#!/usr/bin/env python3
"""Extract and verify the full-range numeric bodies called by CallBudget."""
import argparse
import os
from pathlib import Path
import re
import shutil

from proof_runner import item, run

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'rust/crates/module-runtime-v8/src/budget_core.rs'
ADAPTER = ROOT / 'rust/crates/module-runtime-v8/src/budget.rs'
SPECS = '''
spec fn valid_option(value: Option<u128>) -> bool {
    match value { None => true, Some(value) => value <= MAX_DURATION_NANOS }
}
spec fn remaining_spec(invocation: Option<u128>, elapsed: u128) -> Result<Option<u128>, ()> {
    if elapsed > MAX_DURATION_NANOS || !valid_option(invocation) { Err(()) }
    else { match invocation {
        None => Ok(None),
        Some(limit) => if elapsed >= limit { Err(()) }
            else { Ok(Some((limit - elapsed) as u128)) },
    } }
}
spec fn selected(ceiling: Option<u128>, remaining: Option<u128>) -> Option<u128> {
    match (ceiling, remaining) {
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value),
        (Some(limit), Some(left)) => Some(if limit <= left { limit } else { left }),
    }
}
spec fn execution_spec(invocation: Option<u128>, ceiling: Option<u128>, elapsed: u128)
    -> Result<Option<u128>, ()>
{
    if !valid_option(ceiling) { Err(()) }
    else { match remaining_spec(invocation, elapsed) {
        Err(()) => Err(()),
        Ok(remaining) => Ok(selected(ceiling, remaining)),
    } }
}
// Errors are exhausted/invalid, None is unbounded, Some is finite.
spec fn no_greater(later: Result<Option<u128>, ()>, earlier: Result<Option<u128>, ()>) -> bool {
    match (later, earlier) {
        (Err(()), _) => true,
        (_, Err(())) => false,
        (_, Ok(None)) => true,
        (Ok(None), Ok(Some(_))) => false,
        (Ok(Some(left)), Ok(Some(right))) => left <= right,
    }
}
'''
REMAINING = '''-> (result: Result<Option<u128>, ()>)
    ensures
        result == remaining_spec(invocation, elapsed),
        match result { Ok(Some(value)) => value > 0 && value <= MAX_DURATION_NANOS, _ => true },
        valid_option(invocation) && elapsed <= MAX_DURATION_NANOS ==>
            (result.is_err() == match invocation { None => false, Some(limit) => elapsed >= limit }),
{'''
EXECUTION = '''-> (result: Result<Option<u128>, ()>)
    ensures
        result == execution_spec(invocation, ceiling, elapsed),
        match result { Ok(value) => valid_option(value), _ => true },
{'''
LAWS = '''
fn elapsed_never_creates_budget(invocation: Option<u128>, ceiling: Option<u128>,
    earlier: u128, later: u128)
    requires valid_option(invocation), valid_option(ceiling),
        earlier <= later, later <= MAX_DURATION_NANOS,
{
    let remaining_before = remaining_nanos(invocation, earlier);
    let remaining_later = remaining_nanos(invocation, later);
    assert(no_greater(remaining_later, remaining_before));
    let timeout_before = execution_nanos(invocation, ceiling, earlier);
    let timeout_later = execution_nanos(invocation, ceiling, later);
    assert(no_greater(timeout_later, timeout_before));
}
fn queue_preserves_standalone_execution_ceiling(ceiling: Option<u128>, elapsed: u128)
    requires valid_option(ceiling), elapsed <= MAX_DURATION_NANOS,
{
    let remaining = remaining_nanos(None, elapsed);
    assert(remaining == Ok(None));
    let result = execution_nanos(None, ceiling, elapsed);
    assert(result == Ok(ceiling));
}
fn exact_subtraction(invocation: u128, elapsed: u128)
    requires invocation <= MAX_DURATION_NANOS, elapsed < invocation,
{
    let result = remaining_nanos(Some(invocation), elapsed);
    assert(match result {
        Ok(Some(left)) => left as int + elapsed as int == invocation as int,
        _ => false,
    });
}
fn finite_timeout_is_the_minimum(invocation: u128, ceiling: u128, elapsed: u128)
    requires invocation <= MAX_DURATION_NANOS, ceiling <= MAX_DURATION_NANOS, elapsed < invocation,
{
    let result = execution_nanos(Some(invocation), Some(ceiling), elapsed);
    assert(match result {
        Ok(Some(timeout)) => timeout <= ceiling && timeout as int <= invocation as int - elapsed as int
            && (timeout == ceiling || timeout as int == invocation as int - elapsed as int),
        _ => false,
    });
}
fn zero_ceiling_stays_zero(invocation: Option<u128>, elapsed: u128)
    requires valid_option(invocation), elapsed <= MAX_DURATION_NANOS,
        match invocation { None => true, Some(limit) => elapsed < limit },
{
    let result = execution_nanos(invocation, Some(0), elapsed);
    assert(result == Ok(Some(0)));
}
'''


def check_native_bridge():
    # These bodies are inspected, not proved. Pin them so changing the native
    # conversion/capture/link requires explicit review of this proof boundary.
    expected = [
        '''fn duration_from_nanos(nanos: u128) -> Duration {
            debug_assert!(nanos <= numeric::MAX_DURATION_NANOS);
            Duration::new((nanos / 1_000_000_000) as u64, (nanos % 1_000_000_000) as u32,)
        }''',
        '''fn remaining_after(&self, elapsed: Duration) -> Result<Option<Duration>, ModuleError> {
            numeric::remaining_nanos(self.invocation.map(|value| value.as_nanos()), elapsed.as_nanos())
                .map(|remaining| remaining.map(duration_from_nanos)).map_err(|()| expired())
        }''',
        '''fn execution_after(&self, elapsed: Duration) -> Result<Option<Duration>, ModuleError> {
            numeric::execution_nanos(self.invocation.map(|value| value.as_nanos()),
                self.ceiling.map(|value| value.as_nanos()), elapsed.as_nanos(),)
                .map(|timeout| timeout.map(duration_from_nanos)).map_err(|()| expired())
        }''',
        '''pub(crate) fn capture(deadline: Option<SystemTime>, ceiling: Option<Duration>,)
            -> Result<Self, ModuleError> {
            let admitted = Instant::now();
            let invocation = deadline.map(|deadline| {
                deadline.duration_since(SystemTime::now()).map_err(|_| expired())
            }).transpose()?;
            Ok(Self { admitted, invocation, ceiling, })
        }''',
        '''pub(crate) fn invocation_remaining(&self) -> Result<Option<Duration>, ModuleError> {
            self.remaining_after(self.admitted.elapsed())
        }''',
        '''pub(crate) fn execution_timeout(&self) -> Result<Option<Duration>, ModuleError> {
            self.execution_after(self.admitted.elapsed())
        }''',
    ]
    text = ADAPTER.read_text()
    # Ignore whitespace and optional trailing commas only. Executable tokens,
    # clock calls, casts and argument order must remain the inspected bridge.
    def tokens(code):
        code = re.sub(r'\s+', '', code)
        return re.sub(r',(?=[)\]}])', '', code)
    if '#[path = "budget_core.rs"]\nmod numeric;' not in text:
        raise RuntimeError('production numeric module link changed; review adapter')
    declaration = item(ADAPTER, 'pub(crate) struct CallBudget {')
    if tokens(declaration) != tokens('''pub(crate) struct CallBudget {
        admitted: Instant, invocation: Option<Duration>, ceiling: Option<Duration>,
    }'''):
        raise RuntimeError('CallBudget representation changed; review adapter')
    for code in expected:
        declaration = code[:code.index('(', code.index('fn ')) + 1]
        if text.count(declaration) != 1:
            raise RuntimeError('native bridge declaration changed: ' + declaration)
        start = text.index(declaration)
        opening = text.index('{', start)
        depth = 1
        end = opening + 1
        while depth:
            if text[end] == '{':
                depth += 1
            elif text[end] == '}':
                depth -= 1
            end += 1
        if tokens(text[start:end]) != tokens(code):
            raise RuntimeError('native bridge changed; review proof boundary: ' + declaration)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verus', default=os.environ.get('VERUS_BIN') or shutil.which('verus'))
    args = parser.parse_args()
    if not args.verus:
        parser.error('pass --verus with the pinned verifier')
    check_native_bridge()
    constant = re.findall(r'^pub\(crate\) const MAX_DURATION_NANOS: u128 = [0-9_]+;$', SOURCE.read_text(), re.M)
    if len(constant) != 1:
        raise RuntimeError('duration bound declaration changed; review adapter')
    # The bound itself is checked independently in the Verus harness.
    bound_law = '''proof fn full_duration_range() {
        assert(MAX_DURATION_NANOS as int == (u64::MAX as int + 1) * 1_000_000_000 - 1);
        assert(MAX_DURATION_NANOS < u128::MAX);
    }'''
    remaining = item(SOURCE, 'pub(crate) fn remaining_nanos(')
    execution = item(SOURCE, 'pub(crate) fn execution_nanos(')
    marker = '-> Result<Option<u128>, ()> {'
    for function in (remaining, execution):
        if function.count(marker) != 1:
            raise RuntimeError('production signature changed; review adapter')

    def harness(r, e):
        # Verus rejects pub(crate) const at the harness root; only visibility changes.
        return constant[0].replace('pub(crate) const', 'const', 1) + SPECS + bound_law + r.replace(marker, REMAINING, 1) + e.replace(marker, EXECUTION, 1) + LAWS

    variants = [('production', harness(remaining, execution), True)]
    for name, target, before, after in [
        ('accept_exhausted_boundary', 'r', 'elapsed >= limit', 'elapsed > limit'),
        ('reject_last_nanosecond', 'r', 'elapsed >= limit', '(elapsed >= limit || limit - elapsed == 1)'),
        ('forget_elapsed', 'r', 'Ok(Some(limit - elapsed))', 'Ok(Some(limit))'),
        ('truncate_submilliseconds', 'r', 'Ok(Some(limit - elapsed))', 'Ok(Some(((limit - elapsed) / 1_000_000) * 1_000_000))'),
        ('ignore_invalid_elapsed', 'r', 'if elapsed > MAX_DURATION_NANOS', 'if false && elapsed > MAX_DURATION_NANOS'),
        ('ignore_invalid_invocation', 'r', 'limit > MAX_DURATION_NANOS || ', ''),
        ('remove_subtraction_guard', 'r', 'if limit > MAX_DURATION_NANOS || elapsed >= limit', 'if limit > MAX_DURATION_NANOS'),
        ('choose_maximum', 'e', 'if limit < left', 'if limit > left'),
        ('spend_execution_ceiling_in_queue', 'e', '(Some(limit), None) | (None, Some(limit)) => Some(limit),',
            '(Some(limit), None) => Some(if elapsed < limit { limit - elapsed } else { 0 }),\n        (None, Some(limit)) => Some(limit),'),
        ('zero_means_unbounded', 'e', '(Some(limit), Some(left)) => Some(if limit < left { limit } else { left }),',
            '(Some(limit), Some(left)) => if limit == 0 { None } else { Some(if limit < left { limit } else { left }) },'),
        ('ignore_invalid_ceiling', 'e', 'if limit > MAX_DURATION_NANOS', 'if false && limit > MAX_DURATION_NANOS'),
        ('unbounded_becomes_zero', 'e', '(None, None) => None', '(None, None) => Some(0)'),
    ]:
        body = remaining if target == 'r' else execution
        if body.count(before) != 1:
            raise RuntimeError('negative control no longer matches: ' + name)
        mutated = body.replace(before, after, 1)
        variants.append((name, harness(mutated if target == 'r' else remaining,
                                       mutated if target == 'e' else execution), False))
    receipt = run(args.verus, args.output, ROOT, [SOURCE, ADAPTER], variants,
        'Extracted executable full-range u128 nanosecond bodies called by CallBudget. Exact exhaustion/subtraction/minimum, no budget creation at later valid elapsed values, queue preserves standalone ceiling, all u128 invalid inputs handled without overflow. Native Duration as_nanos/new, conversion casts, capture clock sampling, Instant elapsed, scheduling, V8 dispatch, liveness and compiler remain outside proof. Native adapters require runtime regression tests.', Path(__file__))
    print(f"Budget numeric bodies and controls: {len(receipt['checks'])} expected outcomes passed.")


if __name__ == '__main__':
    main()
