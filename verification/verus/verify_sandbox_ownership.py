#!/usr/bin/env python3
"""Prove sandbox identity/retirement decisions and pin native permit ownership."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil

from proof_runner import item, run
from verify_host_deadlines import method, tokens

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'rust/crates/runtime/src/sandbox.rs'


def check_bridge(source):
    cleanup = method(source, 'fn cancel_active_execution(')
    expected = '''fn cancel_active_execution(workspaces: &mut BTreeMap<String, Workspace>, sandbox_id: &str, execution_id: &str) {
        let Some(workspace) = workspaces.get_mut(sandbox_id) else { return; };
        if !execution_is_current(workspace.active.as_deref(), execution_id) { return; }
        workspace.active = None;
        if let Some(execution) = workspace.executions.get_mut(execution_id) {
            execution.status = "cancelled".to_owned();
            execution.finished_at = Some(Utc::now());
            execution.error = "sandbox execution cancelled".to_owned();
        }
    }'''
    if tokens(cleanup) != tokens(expected):
        raise RuntimeError('cleanup guard or originating workspace/execution effects changed')
    drop = method(source, 'impl Drop for ExecutionOwnership')
    expected_drop = '''impl Drop for ExecutionOwnership {
        fn drop(&mut self) {
            if !self.armed { return; }
            let inner = self.inner.clone();
            let sandbox_id = self.sandbox_id.clone();
            let execution_id = self.execution_id.clone();
            self.runtime.spawn(async move {
                let mut workspaces = inner.workspaces.lock().await;
                cancel_active_execution(&mut workspaces, &sandbox_id, &execution_id);
            });
        }
    }'''
    if tokens(drop) != tokens(expected_drop):
        raise RuntimeError('drop must clean its captured originating reservation')
    body = method(source, 'async fn run(')
    expected_owner = '''let mut ownership = ExecutionOwnership {
        inner: self.inner.clone(), sandbox_id: sandbox_id.clone(), execution_id: execution_id.clone(),
        runtime: tokio::runtime::Handle::current(), armed: true,
    };'''
    begin = body.index('let mut ownership')
    end = body.index('let permit =', begin)
    if tokens(body[begin:end]) != tokens(expected_owner) or body.count('ownership') != 2:
        raise RuntimeError('ownership capture or armed lifetime changed')
    if '};letmutownership' not in tokens(body) or body.index('let permit =') > body.index('let response ='):
        raise RuntimeError('reservation must be owned before semaphore and worker waits')
    if 'ownership.armed=false;Ok(execution_json(&sandbox_id,&execution_id,execution))' not in tokens(body):
        raise RuntimeError('normal completion must disarm only after terminal bookkeeping')
    admission_start = body.index('let permit =')
    admission_end = body.index('let request =', admission_start)
    admission = body[admission_start:admission_end]
    expected_admission = '''let permit = tokio::select! {
        biased;
        _ = cancel.changed() => None,
        permit = self.inner.admission.clone().acquire_owned() => {
            Some(permit.map_err(|_| "sandbox is shutting down".to_owned())?)
        }
    };'''
    expected_response = '''let response = if let Some(permit) = permit {
        self.execute_worker(request, timeout, &mut cancel, permit).await
    } else { Err("sandbox execution cancelled".to_owned()) };'''
    if tokens(admission) != tokens(expected_admission) or tokens(expected_response) not in tokens(body):
        raise RuntimeError('queued cancellation must win before worker dispatch')
    worker = method(source, 'async fn execute_worker(')
    expected_worker = '''async fn execute_worker(
        &self, request: Value, timeout: Duration, cancel: &mut watch::Receiver<bool>,
        permit: OwnedSemaphorePermit,
    ) -> Result<Value, String> {
        let binary = self.inner.config.worker_binary.as_ref()
            .ok_or_else(|| "sandbox worker binary is not configured".to_owned())?;
        let input = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
        let output = async move {
            let child = Command::new(binary).kill_on_drop(true)
                .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null()).spawn()
                .map_err(|error| format!("start sandbox worker: {error}"))?;
            let mut child = SandboxChild::new(child, permit);
            let mut stdin = child.child().stdin.take()
                .ok_or_else(|| "sandbox worker stdin is unavailable".to_owned())?;
            stdin.write_all(&input).await.map_err(|error| error.to_string())?;
            drop(stdin);
            let mut stdout = child.child().stdout.take()
                .ok_or_else(|| "sandbox worker stdout is unavailable".to_owned())?;
            let mut bytes = Vec::new();
            tokio::try_join!(child.child().wait(), stdout.read_to_end(&mut bytes))
                .map_err(|error| error.to_string())?;
            child.reaped();
            Ok::<_, String>(bytes)
        };
        tokio::pin!(output);
        let bytes = tokio::select! {
            biased;
            _ = cancel.changed() => return Err("sandbox execution cancelled".to_owned()),
            _ = tokio::time::sleep(timeout + Duration::from_secs(2)) => {
                return Err("sandbox execution timed out".to_owned());
            }
            result = &mut output => result?,
        };
        if bytes.len() > self.inner.config.max_output_bytes {
            return Err("sandbox output exceeds runtime policy".to_owned());
        }
        serde_json::from_slice(&bytes).map_err(|error| format!("invalid sandbox response: {error}"))
    }'''
    if tokens(worker) != tokens(expected_worker):
        raise RuntimeError('worker stdin must remain inside cancellation/watchdog ownership')
    expected_process = '''impl SandboxChild {
        fn new(child: Child, permit: OwnedSemaphorePermit) -> Self {
            Self { state: Some((child, permit)), runtime: tokio::runtime::Handle::current() }
        }
        fn child(&mut self) -> &mut Child {
            &mut self.state.as_mut().expect("owned sandbox child").0
        }
        fn reaped(&mut self) { drop(self.state.take()); }
    }'''
    process = method(source, 'impl SandboxChild')
    expected_retire = '''impl Drop for SandboxChild {
        fn drop(&mut self) {
            if let Some((mut child, permit)) = self.state.take() {
                let _ = child.start_kill();
                self.runtime.spawn(async move {
                    let result = child.wait().await;
                    finish_child_retirement(result, permit);
                });
            }
        }
    }'''
    retire = method(source, 'impl Drop for SandboxChild')
    expected_finish = '''fn finish_child_retirement(
        result: std::io::Result<std::process::ExitStatus>, permit: OwnedSemaphorePermit,
    ) {
        if retirement_requires_closure(result.is_ok()) { permit.semaphore().close(); }
        if let Err(error) = result {
            tracing::error!(error = %error, "sandbox child retirement failed; admission closed");
        }
        drop(permit);
    }'''
    finish = method(source, 'fn finish_child_retirement(')
    if tokens(process) != tokens(expected_process) or tokens(retire) != tokens(expected_retire) or tokens(finish) != tokens(expected_finish):
        raise RuntimeError('native process and permit retirement ownership changed')
    if source.count('Command::new(') != 1 or source.count('.spawn()') != 1:
        raise RuntimeError('unexpected native process creation site')
    flat = tokens(source)
    if 'state:Option<(Child,OwnedSemaphorePermit)>' not in flat or 'admission:Arc<Semaphore>' not in flat:
        raise RuntimeError('native process and capacity must have one owner')
    if source.count('.state') != 3 or source.count('SandboxChild::new(') != 1 or source.count('child.reaped()') != 1:
        raise RuntimeError('unexpected child/permit release site')
    return {'cleanup': cleanup, 'drop': drop, 'capture': body[begin:end],
            'admission': admission, 'worker': worker, 'process': process, 'retire': retire, 'finish': finish}


CONTRACT = '''-> (result: bool)
    ensures result == match active { Some(value) => value@ == expected@, None => false },
{'''
LAWS = '''
fn no_active(expected: &str) {
    let result = execution_is_current(None, expected);
    assert(!result);
}
fn same_active(expected: &str) {
    let result = execution_is_current(Some(expected), expected);
    assert(result);
}
fn other_active(active: &str, expected: &str)
    requires active@ != expected@,
{
    let result = execution_is_current(Some(active), expected);
    assert(!result);
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verus', default=os.environ.get('VERUS_BIN') or shutil.which('verus'))
    args = parser.parse_args()
    if not args.verus:
        parser.error('pinned Verus required')
    source = SOURCE.read_text()
    bridge = check_bridge(source)
    controls = []
    for name, before, after in [
        ('omit-active-guard', 'if !execution_is_current(workspace.active.as_deref(), execution_id)', 'if false'),
        ('wrong-workspace', 'workspaces.get_mut(sandbox_id)', 'workspaces.get_mut("different")'),
        ('wrong-execution-record', 'workspace.executions.get_mut(execution_id)', 'workspace.executions.get_mut("different")'),
        ('wrong-captured-id', 'execution_id: execution_id.clone(),', 'execution_id: "different".to_owned(),'),
        ('unarmed-capture', 'armed: true,', 'armed: false,'),
        ('omit-drop-cleanup', 'cancel_active_execution(&mut workspaces, &sandbox_id, &execution_id);', ''),
        ('ignore-admission-cancel', '_ = cancel.changed() => None', '_ = std::future::pending::<()>() => None'),
        ('ignore-worker-cancel', '_ = cancel.changed() => return Err("sandbox execution cancelled".to_owned())', '_ = std::future::pending::<()>() => return Err("sandbox execution cancelled".to_owned())'),
        ('ignore-worker-watchdog', 'tokio::time::sleep(timeout + Duration::from_secs(2))', 'std::future::pending::<()>()'),
        ('release-before-native-wait', 'let result = child.wait().await;\n                finish_child_retirement(result, permit);', 'drop(permit); let _ = child.wait().await;'),
        ('leak-retiring-permit', 'finish_child_retirement(result, permit);', 'std::mem::forget(permit);'),
        ('omit-native-kill', 'let _ = child.start_kill();', 'let _ = ();'),
        ('ignore-unknown-exit', 'if retirement_requires_closure(result.is_ok())', 'if false'),
        ('wrong-exit-confirmation', 'retirement_requires_closure(result.is_ok())', 'retirement_requires_closure(true)'),
        ('unowned-process-path', 'impl SandboxManager {', 'impl SandboxManager { fn unbudgeted(&self, binary: &Path) { let _ = Command::new(binary).spawn(); }'),
    ]:
        if source.count(before) != 1:
            raise RuntimeError('structural control source changed: ' + name)
        try:
            check_bridge(source.replace(before, after))
        except (RuntimeError, ValueError):
            controls.append(name)
        else:
            raise RuntimeError('broken ownership bridge escaped: ' + name)
    body = item(SOURCE, 'fn execution_is_current(')
    marker = '-> bool {'
    if body.count(marker) != 1:
        raise RuntimeError('one production ownership policy required')
    def harness(code):
        return code.replace(marker, CONTRACT, 1) + LAWS
    variants = [('production', harness(body), True)]
    for name, before, after in [
        ('allow-any-active', 'value == expected', 'true'),
        ('deny-all-active', 'value == expected', 'false'),
        ('invert-identity', 'value == expected', 'value != expected'),
        ('allow-absent', 'None => false', 'None => true'),
    ]:
        if body.count(before) != 1:
            raise RuntimeError('identity source policy changed: ' + name)
        variants.append((name, harness(body.replace(before, after)), False))
    retirement = item(SOURCE, 'fn retirement_requires_closure(')
    retirement_contract = '''-> (close: bool) ensures close == !reaped, {'''
    retirement_laws = '''
fn successful_exit_preserves_admission() {
    let close = retirement_requires_closure(true);
    assert(!close);
}
fn unknown_exit_closes_admission() {
    let close = retirement_requires_closure(false);
    assert(close);
}
'''
    def retirement_harness(code):
        return code.replace('-> bool {', retirement_contract, 1) + retirement_laws
    variants.append(('retirement-production', retirement_harness(retirement), True))
    for name, expression in [('retirement-never-close', 'false'), ('retirement-always-close', 'true'), ('retirement-wrong-result', 'reaped')]:
        if retirement.count('!reaped') != 1:
            raise RuntimeError('one actual retirement error policy required')
        variants.append((name, retirement_harness(retirement.replace('!reaped', expression)), False))
    receipt = run(args.verus, args.output, ROOT, [SOURCE], variants,
        'Actual Option<&str> identity and Boolean unknown-exit closure decisions, verified using Verus string views and native Boolean logic. Closed source correspondence checks captured IDs, mutex-protected conditional cleanup, Drop scheduling, guard placement, cancellation-first admission and request-write/response watchdog placement. Native child and owned permit remain paired through wait; a failed wait closes admission before returning its credit. Rust string/Map/Drop, Tokio linear permits/closed semaphore/process wait, Linux kill/exit and scheduling progress are trusted. Native exit-held schedules complement these source checks; no whole async or arbitrary OS-failure proof is claimed.', Path(__file__))
    receipt['bridge'] = bridge
    receipt['structuralControlsRejected'] = controls
    receipt['bridgeParserSha256'] = hashlib.sha256(Path(__file__).with_name('verify_host_deadlines.py').read_bytes()).hexdigest()
    (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print('Actual sandbox identity and retirement policies verified; seven wrong policies and fifteen changed cleanup/retirement bridges rejected')


if __name__ == '__main__':
    main()
