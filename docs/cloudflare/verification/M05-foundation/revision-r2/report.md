# M05 boundary and privacy correction after the 493 recheck

Tested source: **6ac203fbb45f8828dd26ca4cb447ba4252679b83** (full source hash is authoritative in receipt.json).
PR base: **c1fc7847a23f2096e3850223171be592c1726ca8**; already merged normally, still current before publication.
Checkout `/home/gabriel/.t3/worktrees/gonvex/t3-9ef46623-057033777841386e`, branch `cf/M05-monitoring-foundation`.
This appended evidence commit changes no implementation bytes.

[The completed independent COMMENT recheck](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5880928422)
is pinned to49399492911e2655d7295f7e9668d2fb911d0475. It independently confirmed prior F1/F3 and the original
F2 counterexample fixed, while reporting three remaining P2 foundation blockers. Those prior dispositions,
all445/493 evidence/raw records, both older test files and private artifact hashes remain unchanged. This
is implementation-owner correction evidence, **not source clearance**. A NEW independent recheck belongs
to lead; neither completed reviewer was polled, interrupted or reused. No descendant, watch or merge.

## R1: operation eligibility precedes enclosing-record selection

Replay computes candidate intervals for **every operation on every scoped record**, using its actual
attempted timestamp and declared clock uncertainty, before selecting scheduled/check observations.
Potentially in-window operations cannot disappear because their check starts outside the window. The exact
accepted repro (window[0,100), healthy slot0, check100..105 with uncertainty1 and failed create attempted99)
remains valid and now reports **attempted1/failed1**, retained first failure, and **100ms UNKNOWN, lower0/
upper1**. It does not invent an entire interval of service failure from a boundary symptom.

Clock/status/kind/window tests include creates/updates, successful/failed records, checks beyond the end,
uncertainty spanning the opening/end boundaries and duplicate operation identity. Ambiguous membership
stays counted with explicit window uncertainty; it cannot certify successful membership. Exact out-of-window
attempts with zero uncertainty remain explicitly outside, preserving the half-open window. Valid failed
attempts remain failed; they are never reclassified as malformed to evade accounting. Legacy-v1 attempt-time
and deadline fallbacks remain explicitly counted/labeled rather than silently dropped.

## R2: elapsed evidence must fit the operation's remaining observation

Every present elapsed value fits the enclosing monotonic duration. With an actual attempted wall timestamp,
it also fits the remaining observation and wall end under **the declared** clock tolerance. New task records
carry `attemptedAfterMs`, measured from the same monotonic origin as the check; offset+elapsed must fit
exactly, and its wall/monotonic readings must agree within declared uncertainty. Date.now quantization plus
measured alignment-read time is now explicitly included in producer clock uncertainty; no unrecorded
sub-millisecond wall-clock precision is assumed.

The exact contradictory input (check0..100/duration100, attempt95, durable2/applied10, deadline10, asOf100)
throws fixed integrity UNKNOWN **before any availability object exists**. Matrix cases cover exact completion
at the boundary, overdue application, consistent coarse-clock tolerance, monotonic contradiction, future
cutoff and old-v1 no-timestamp fallback. A clock-consistent application whose latest possible time exceeds
`asOf` stays UNKNOWN, never early success; once both observation and uncertainty bounds are satisfied it
can count. Late failed timing may exceed the operation deadline but must still fit its actual observation.
Original raw outcomes are not rewritten to make these inputs pass.

## R3: one strict producer/retention/replay boundary, independent of status

`checks/raw-result.mjs` is dependency-free and shared one-way by producer and replay (the existing public
replay export remains). **Every present** durable/applied/attempt-offset/attempt timestamp field on BOTH
pass and fail records must be a finite, nonnegative, representably bounded numeric value in its domain.
Missing failure timing stays absent; null/string/bool/NaN/infinite/negative/huge/nested values are rejected.
Deadline and observation ordering are checked separately, retaining legitimate late failure measurements.

The validator snapshots own data descriptors, rejects accessors/custom serializers/prototypes/unknown fields,
and returns independent plain records of approved scalar fields. It never returns the untrusted input or
retains a raw operation spread. Arbitrary getter/proxy/foreign-error payloads cannot escape through an error:
rejection always creates a fresh fixed UNKNOWN integrity error with no input, headers, contents or cause.
A validated snapshot cannot later be rewritten by mutating the original object.

`runCheck` validates before returning raw output for retention/export. Unsafe output aborts/pauses new
synthetic writes and rejects the whole observation; it is **not** stripped into zero attempts or green
coverage. Valid failed attempts remain in output and force a failed producer outcome even when an adapter
fulfills. Actual settlement, charged work, cleanup uncertainty and the monotonic deadline remain separate.
Replay validates before filtering and never emits a partial healthy report for invalid data. Exported
component/selector labels are scalar/allowlisted. CLI integrity/parser errors print only a fixed code, with
**no stdout availability object**, parser context, input values, arbitrary error, body or path. A caller
must display UNKNOWN/integrity failure, never catch rejection as an empty healthy report.

The 22 new tests include112 type/status/field combinations,16 clock/status/kind cases, the exact R1/R2
counterexamples, failed absent timing retention, safe snapshots, serializer/accessor/proxy errors, producer
pause/outcome behavior and actual CLI subprocess rejection. These are author local input/adaptor fixtures,
not provider credentials, real application faults or hosted proof. The existing62 tests remain byte-identical.

## Verification and measurement

**84 foundation tests PASS (62 unchanged +22 new),29 unchanged conformance PASS, zero failures/skips.**
Both exact approved CI commands ran from cloudflare/conformance on pinned Node24.13.1. SQLite schema
generator/freshness, explicit owned MDX/Next typegen/tsc, CLI audit/estimate/replay and actual isolated Node
SQLite handler capture pass. Source/tool/config/raw/artifact hashes, full commands and phase data are in
receipt.json. New raw input matrix identifies its constructed fixture domain; the actual local SQLite
capture identifies local executor/source/config/region and is never called SDK/workerd/Whagons proof.

Runtime import cycles remain **1→1**,417→430 nodes against the exact current base. The new independent
validator adds one node, with imports kept one-way. Existing type/test/fixture exclusions and unresolved
module warnings remain explicit. No serving import, root entry/config, package/lock, protocol, K15/K22,
owner/gateway/placement or Rust/module source changed. Approved CI still has exactly its original
conformance command followed by the observability glob; full diff classification is recorded separately.

Every heavy phase passed build-headroom and coach-resources kernel-v1 heavy.slice/heavy-workloads.slice
admission. Cycle analysis and EACH docs compile/type command used the installed **unmodified four-slot
coach-compile** wrapper; no cap or other worker was changed. Earlier historical phases are not retroactively
certified. Tools were pinned and resolved read-only; generated docs/cache/tmp stayed under the existing
owned /mnt tree. No pnpm/install/global cleanup, browser/server/Container/platform/account operation.

The initial combined regression run retained82PASS/2FAIL: it caught legacy cutoff-accounting and prior
error-code compatibility regressions. Code was corrected without changing any prior assertion. Subsequent
84-test passes and final source-bound proof are retained separately; no old pass/failure was erased or
relabelled as proof. Process CPU/wall/peak figures in receipt.json measure only local verification. Source
bytes include tests/docs/fixtures and are NOT serving bundle/gzip/rows/CPU/isolate/overhead/cost gains.
Final phase gonvex-m05-foundation-20261112-phase-6a530ee1bfb2.service measured7.883s wall,11,943,216 CPU microseconds and595,345,408 bytes peak memory, with zero contained OOM kills. The22 implementation/workflow files total144,484 source bytes. All serving measurements and actual-duration requirements remain unexecuted/unproved.

## Holds and limitations

Finite foundation acceptance still needs NEW independent review and current-head CI under lead ownership.
Full M05 remains stage2/wave4, depends ONLY P01, done_when exactly **“Each of the five paging conditions fires
in a drill”**. Canonical P1–P5 identities/thresholds/windows remain unresolved; additional conditions never
replace them. Full application/auth/visibility/receipt/second-client/browser, exact assembled dev, hosted
three-region/provider/heartbeat/status, serving async traces, original/fault/authorized-alert drills,
security/capacity/cost and real14d/30d gates remain **UNMET**. K22/X01 and every original V02/V05/V03/V04/R01
hold stay unchanged. Prior-slot heartbeat-window and historical global-store/literal-link evidence caveats
remain explicit. No activation, message sending, publication, provisioning or serving promotion occurred.
