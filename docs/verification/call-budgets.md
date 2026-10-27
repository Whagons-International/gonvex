# CallBudget numeric policy proof

`module-runtime-v8/src/budget.rs` calls `budget_core.rs` for invocation remainder
and dispatch timeout selection. Both helpers use `u128` total nanoseconds, covering
every native `Duration`, including submillisecond values and `Duration::MAX`.
The upper bound is exactly `18_446_744_073_709_551_615_999_999_999` nanoseconds.
This adds no new duration restriction. `CallBudget` still stores native `Duration`.

The extractor copies the actual production helper bodies into Verus, adds
contracts and checks laws. It does not verify a parallel mathematical
implementation. Native bridge token checks pin the conversion, the calls to the
numeric helpers, the `CallBudget` fields and the existing capture/elapsed bodies.
Changing those bridges requires reviewing the proof adapter. Whitespace and
trailing commas can change without invalidating the bridge check. Source,
extractor, shared runner and generated harness hashes appear in the receipt.

For every pair of `u128` inputs, remainder selection rejects out-of-range numeric
inputs before doing unsafe arithmetic. For valid native-duration values, it
errors exactly when a finite invocation limit is reached or exceeded. Otherwise
the remainder is exactly `limit - elapsed`, with nanosecond precision. No
invocation deadline produces an unbounded remainder. Execution timeout is the
minimum of finite invocation remainder and configured ceiling, with `None`
meaning unbounded. A zero ceiling stays `Some(0)` for a live invocation.
Exhausted invocations error even if the ceiling is zero.

Separate law checks prove that increasing valid elapsed inputs cannot create
budget, that the finite selected timeout is a minimum, and that waiting in the
queue does not spend a standalone execution ceiling. The production proof has
no input preconditions; law harnesses constrain elapsed to the native duration
range and compare ordered elapsed values. Invalid numeric inputs cannot arrive
through `Duration::as_nanos()`, but the numeric helpers reject them anyway.

Twelve well-typed negative controls must produce verification errors. They accept
exhaustion equality, reject the last nanosecond, forget elapsed, truncate to
milliseconds, bypass each range guard, remove the subtraction guard, choose a
maximum, spend a standalone ceiling while queued, treat zero as unbounded, or
turn an unbounded timeout into zero. Compiler/type errors do not count as
successful controls. No proof escape, admitted fact or additional standard
library specification is introduced.

## Native and external boundaries

The pinned Verus release has no usable specification for
`std::time::Duration::as_nanos`; a direct probe fails as unsupported. The native
`Duration::as_nanos`, `Duration::new`, division/remainder and narrowing casts in
`duration_from_nanos` remain outside the formal proof. The cast is safe because
verified outputs are at most `Duration::MAX.as_nanos()`. That argument and the
native library's meaning remain review obligations, rather than admitted Verus
facts. Native tests check the MAX constant, exact adapter round trips across
seconds/nanoseconds boundaries, second borrowing, nanosecond exhaustion, zero,
maximum values, invalid numeric inputs and comparison with the old native policy
over 648 boundary combinations.

Capture is unchanged. It samples `Instant::now()` and then computes a relative
invocation duration from `SystemTime::now()` once. Later queries use admitted
monotonic elapsed time. Changes to wallclock after capture do not refresh that
stored invocation limit. The two clock reads are separate external effects; the
proof does not establish atomic clock sampling, clock accuracy, the relationship
between the two clocks or correct behavior of any native clock implementation.

Tokio timers, queue admission and cancellation, V8 interruption, dispatch,
scheduler fairness and liveness remain external. The existing queue/V8
regressions must still run. This is a proof of extracted production numeric
bodies conditional on the native adapters, not a proof of the whole production
runtime. Verus, its SMT solver, vstd, the extractor/specification, and both Rust
compiler toolchains remain trusted.

## Run and integration

Run `python3 verification/verus/verify_budgets.py --output /path/to/proof` with
Verus `0.2026.10.18.7d82994`, or pass `--verus`. Require `receipt.json` success
with 13 checks: production plus all 12 failing controls. The production harness
reports nine verified obligations with zero errors. Run under the job's existing
resource controller; the script does not create or enlarge a resource grant.

The required policy CI job runs this adapter and its negative controls. Run Rust
formatting, module-runtime-v8 budget tests and existing queue/V8 regressions.
The implementation child did not execute Cargo, generators, native tests or
benchmarks. Compare this representation against the native Duration baseline
before treating it as a measured performance improvement. The core adds no
allocation or clock read, but full-range u128 conversion/arithmetic and guards
can change CPU cost. Scheduling and end-to-end throughput need separate checks.
