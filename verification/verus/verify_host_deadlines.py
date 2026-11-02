#!/usr/bin/env python3
"""Verify host deadlines/reply IDs and pin their admission/ownership bridges."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil

from proof_runner import item, run

ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = ROOT / 'rust/crates/module-host/src/protocol.rs'
SESSION = ROOT / 'rust/crates/module-host/src/session.rs'


def tokens(text):
    text = re.sub(r'//[^\n]*', '', text)
    return re.sub(r',(?=[)\]}])', '', re.sub(r'\s+', '', text))


def method(source, declaration):
    if source.count(declaration) != 1:
        raise RuntimeError('one method required: ' + declaration)
    start = source.index(declaration)
    opening = source.index('{', start)
    end, depth = opening + 1, 1
    while depth:
        if source[end] == '{':
            depth += 1
        elif source[end] == '}':
            depth -= 1
        end += 1
    return source[start:end]


def check_bridge(protocol, session):
    getter = method(protocol, 'pub fn effective_deadline(')
    expected_getter = '''pub fn effective_deadline(&self, request_deadline: Option<u64>) -> Option<SystemTime> {
        effective_deadline_millis(self.deadline_unix_ms, request_deadline).map(from_unix_millis)
    }'''
    if tokens(getter) != tokens(expected_getter):
        raise RuntimeError('wire deadline conversion bridge changed')
    context = method(protocol, 'pub fn into_context(')
    if 'let deadline = self.effective_deadline(request_deadline);' not in context or len(re.findall(r'\bdeadline\b', context)) != 2 or not re.search(r'deadline,\s*}\s*}$', context):
        raise RuntimeError('engine context no longer carries the same effective deadline')
    invoke = method(session, 'async fn handle_invoke(')
    start = invoke.index('let permit: Option<OwnedSemaphorePermit>')
    end = invoke.index('let lease =', start)
    bridge = invoke[start:end]
    expected = '''let permit: Option<OwnedSemaphorePermit> = if request.context.nesting_depth == 0 {
        let acquire = Arc::clone(&state.calls).acquire_owned();
        let expired = || { module_error(&module_id, &request.function,
            ModuleError::BudgetExceeded("invocation deadline elapsed during root admission".to_owned(),)) };
        let acquired = match request.context.effective_deadline(deadline_unix_ms) {
            Some(deadline) => {
                let remaining = deadline.duration_since(SystemTime::now()).map_err(|_| expired())?;
                if remaining.is_zero() { return Err(expired()); }
                tokio::time::timeout(remaining, acquire).await.map_err(|_| expired())?
            }
            None => acquire.await,
        };
        Some(acquired.map_err(|_| { WireError::retryable(codes::SHUTTING_DOWN, "module host is shutting down") })?)
    } else { None };'''
    if tokens(bridge) != tokens(expected):
        raise RuntimeError('root admission deadline/permit bridge changed')
    if invoke.index('let context = request.context.into_context(active, deadline_unix_ms);') < end:
        raise RuntimeError('generation context must follow root admission')
    return bridge


def check_ledger(session):
    flat = tokens(session)
    if 'running:Mutex<HashMap<u64,Option<oneshot::Sender<()>>>>' not in flat or len(re.findall(r'\.running\b', session)) != 4:
        raise RuntimeError('request reservation storage or access sites changed')
    cancelled = method(session, 'fn cancel(')
    if tokens(cancelled) != tokens('''fn cancel(&self, request_id: u64) {
        drop(self.running.lock().expect("request ledger lock").get_mut(&request_id).and_then(Option::take));
    }'''):
        raise RuntimeError('cancellation must retain the original reservation')
    if tokens(method(session, 'fn finish(')) != tokens('''fn finish(&self, request_id: u64) {
        self.running.lock().expect("request ledger lock").remove(&request_id);
    }'''):
        raise RuntimeError('request completion changed its reservation target')
    registration = '''let registered = {
        let mut running = connection.running.lock().expect("request ledger lock");
        if let Entry::Vacant(entry) = running.entry(id) {
            entry.insert(Some(cancel)); true
        } else { false }
    };'''
    if tokens(registration) not in flat or flat.count('connection.finish(id)') != 1 or 'connection.send(frame).await;connection.finish(id);' not in flat:
        raise RuntimeError('request registration/completion ordering changed')
    guard = method(session, 'impl Drop for PendingHostCall')
    if tokens(guard) != tokens('''impl Drop for PendingHostCall<'_> {
        fn drop(&mut self) { self.connection.pending.lock().expect("host call ledger lock").remove(&self.id); }
    }'''):
        raise RuntimeError('host reply cleanup changed its originating target')
    call = method(session, 'async fn host_call(')
    begin = call.index('let id =')
    end = call.index('if self', begin)
    expected = '''let id = self.next_host_call.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
        |id| id.checked_add(1)).map_err(|_| HostError::Failed("module host call identity exhausted".to_owned()))?;
        let (reply, response) = oneshot::channel();
        self.pending.lock().expect("host call ledger lock").insert(id, reply);
        let _pending = PendingHostCall { connection: self, id };'''
    if tokens(call[begin:end]) != tokens(expected) or len(re.findall(r'\b_pending\b', call)) != 1:
        raise RuntimeError('reply identity or cancellation guard placement changed')
    expression = re.search(r'\|id\|\s*(.*?)\s*\)\s*\.map_err', call[begin:end], re.S)
    if not expression:
        raise RuntimeError('one checked reply counter callback required')
    return {'registration': registration, 'cancel': cancelled, 'replyDrop': guard,
            'replyAllocation': call[begin:end], 'counterExpression': expression[1].strip()}


SPECS = '''
spec fn earliest_spec(context_deadline: Option<u64>, request_deadline: Option<u64>) -> Option<u64> {
    match (context_deadline, request_deadline) {
        (Some(left), Some(right)) => Some(if left < right { left } else { right }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}
'''
CONTRACT = '''-> (result: Option<u64>)
    ensures
        result == earliest_spec(context_deadline, request_deadline),
        result.is_none() == (context_deadline.is_none() && request_deadline.is_none()),
        match (result, context_deadline) { (Some(value), Some(limit)) => value <= limit, _ => true },
        match (result, request_deadline) { (Some(value), Some(limit)) => value <= limit, _ => true },
{'''
LAWS = '''
fn symmetric(context: Option<u64>, request: Option<u64>) {
    let left = effective_deadline_millis(context, request);
    let right = effective_deadline_millis(request, context);
    assert(left == right);
}
fn finite_bound_never_becomes_unbounded(limit: u64, other: Option<u64>) {
    let left = effective_deadline_millis(Some(limit), other);
    let right = effective_deadline_millis(other, Some(limit));
    assert(left.is_some() && right.is_some());
}
fn zero_is_expired_not_unbounded(other: Option<u64>) {
    let left = effective_deadline_millis(Some(0), other);
    let right = effective_deadline_millis(other, Some(0));
    assert(left == Some(0) && right == Some(0));
}
fn unbounded_is_preserved() {
    let result = effective_deadline_millis(None, None);
    assert(result == None);
}
fn finite_result_is_an_input(context: u64, request: u64) {
    let result = effective_deadline_millis(Some(context), Some(request));
    assert(result == Some(context) || result == Some(request));
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verus', default=os.environ.get('VERUS_BIN') or shutil.which('verus'))
    args = parser.parse_args()
    if not args.verus:
        parser.error('pinned Verus required')
    protocol, session = PROTOCOL.read_text(), SESSION.read_text()
    bridge = check_bridge(protocol, session)
    ledger = check_ledger(session)
    structural_controls = []
    for name, target, before, after in [
        ('wrong-root-depth', 's', 'request.context.nesting_depth == 0', 'request.context.nesting_depth != 0'),
        ('ignore-root-deadline', 's', 'request.context.effective_deadline(deadline_unix_ms)', 'None'),
        ('unbounded-finite-wait', 's', 'tokio::time::timeout(remaining, acquire)', 'tokio::time::timeout(Duration::MAX, acquire)'),
        ('lose-zero-refusal', 's', 'if remaining.is_zero()', 'if false'),
        ('timer-for-unbounded', 's', 'None => acquire.await', 'None => return Err(expired())'),
        ('different-engine-deadline', 'p', 'let deadline = self.effective_deadline(request_deadline);', 'let deadline = None;'),
    ]:
        text = session if target == 's' else protocol
        if text.count(before) != 1:
            raise RuntimeError('bridge control source changed: ' + name)
        candidate = text.replace(before, after)
        try:
            check_bridge(candidate if target == 'p' else protocol, candidate if target == 's' else session)
        except RuntimeError:
            structural_controls.append(name)
        else:
            raise RuntimeError('broken source bridge escaped: ' + name)
    for name, before, after in [
        ('lose-reservation', '.get_mut(&request_id)\n                .and_then(Option::take)', '.remove(&request_id).flatten()'),
        ('allow-occupied-id', 'if let Entry::Vacant(entry) = running.entry(id)', 'if let Entry::Occupied(entry) = running.entry(id)'),
        ('drop-reply-guard', re.search(r'let _pending = PendingHostCall \{.*?};', session, re.S)[0], ''),
        ('wrong-reply-target', '.remove(&self.id)', '.remove(&0)'),
    ]:
        if session.count(before) != 1:
            raise RuntimeError('ledger bridge control changed: ' + name)
        try:
            check_ledger(session.replace(before, after))
        except RuntimeError:
            structural_controls.append(name)
        else:
            raise RuntimeError('broken ledger bridge escaped: ' + name)
    body = item(PROTOCOL, 'fn effective_deadline_millis(')
    marker = '-> Option<u64> {'
    if body.count(marker) != 1:
        raise RuntimeError('wire deadline selection signature changed')
    def harness(code):
        return SPECS + code.replace(marker, CONTRACT, 1) + LAWS
    variants = [('production', harness(body), True)]
    for name, before, after in [
        ('choose-maximum', 'left <= right', 'left >= right'),
        ('ignore-context', 'Some(if left <= right { left } else { right })', 'Some(right)'),
        ('ignore-request', 'Some(if left <= right { left } else { right })', 'Some(left)'),
        ('drop-context-only', '(Some(value), None) | (None, Some(value)) => Some(value),', '(Some(value), None) => None, (None, Some(value)) => Some(value),'),
        ('drop-request-only', '(Some(value), None) | (None, Some(value)) => Some(value),', '(Some(value), None) => Some(value), (None, Some(value)) => None,'),
        ('bound-unbounded', '(None, None) => None', '(None, None) => Some(0)'),
        ('zero-becomes-unbounded', 'Some(if left <= right { left } else { right })', 'if left == 0 || right == 0 { None } else { Some(if left <= right { left } else { right }) }'),
    ]:
        if body.count(before) != 1:
            raise RuntimeError('numeric control source changed: ' + name)
        variants.append((name, harness(body.replace(before, after)), False))
    def counter_harness(expression):
        return '''fn reply_id_next(id: u64) -> (next: Option<u64>)
            ensures
                next.is_none() == (id == u64::MAX),
                match next { Some(value) => value as int == id as int + 1 && value > id, None => true },
            { ''' + expression + ' }'
    variants.append(('reply-counter-production', counter_harness(ledger['counterExpression']), True))
    for name, expression in [
        ('reply-counter-wraps', 'Some(if id == u64::MAX { 0 } else { id + 1 })'),
        ('reply-counter-stalls', 'Some(id)'),
        ('reply-counter-rejects-last', 'if id >= u64::MAX - 1 { None } else { id.checked_add(1) }'),
        ('reply-counter-never-progresses', 'None'),
    ]:
        variants.append((name, counter_harness(expression), False))
    receipt = run(args.verus, args.output, ROOT, [PROTOCOL, SESSION], variants,
        'Actual pure u64 wire-deadline minimum body and checked-add callback used by host reply ID allocation. Exact minimum, finite/zero/unbounded meaning and strict nonwrapping ID successor with MAX exhaustion. Closed source checks for native conversion, root admission, vacant request reservations, cancellation/completion order and originating reply Drop guard. SystemTime, clock sampling, Rust/standard library atomic/Map/Option/Drop semantics, Tokio timers/semaphores, V8 execution and complete async refinement remain trusted or require native regressions.', Path(__file__))
    receipt['structuralBridgeControlsRejected'] = structural_controls
    receipt['rootAdmissionBridge'] = bridge
    receipt['ledgerBridge'] = ledger
    (args.output/'receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print('Wire deadline and reply-ID sources verified; eleven broken numeric policies and ten changed admission/ownership bridges rejected')


if __name__ == '__main__':
    main()
