# M05 finite foundation: correction of the three 445695ee findings

Tested implementation: **74a435a30404a36b434156777ecc7c4bb0cd2401**. Exact PR base:
**c1fc7847a23f2096e3850223171be592c1726ca8**, incorporated by ordinary merge `b9517b5e`.
This evidence revision changes no runtime bytes. Checkout and branch remain
`/home/gabriel/.t3/worktrees/gonvex/t3-9ef46623-057033777841386e`, `cf/M05-monitoring-foundation`.

The [formal COMMENT review](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5880429032)
requested changes to the finite foundation at **445695ee9f4002a571a7aef71a6bda91183490f8**.
All three P2 findings were accepted and corrected by the implementation owner. This is **author evidence**,
not independent clearance. Lead must arrange a **NEW** independent recheck of the new immutable pin;
the completed reviewer was neither interrupted, polled nor reused. No child agent/watch/merge/activation.

## F1: monotonic completion deadline and honest settlement

`runCheck` now checks actual monotonic elapsed time after the adapter promise race, so 40ms synchronous
work cannot emit PASS under a 10ms deadline merely because the timer callback was starved. This also
covers microtask and asynchronous turn boundaries. A timeout pauses new writes and preserves first-attempt
metadata, elapsed time and the pre-enforcement outcome/code. The record explicitly identifies actual
adapter settlement (fulfilled/rejected/unsettled), optional settlement elapsed and cleanup ledger state.
An observation ending/AbortSignal firing never certifies underlying cancellation or cleanup settlement.
Returned records remain unchanged if unresolved work later completes. Task/file mutator admission checks
monotonic deadline before starting another write; cleanup still runs under its separate contract. There
is no automatic retry or inferred non-invocation of an ambiguous writer.

Regressions exercise real event-loop blocking, microtask/turn boundaries, late rejection, unresolved work
that later settles, no write after synchronous setup overruns, and a legitimate on-time completion.
`regression-raw.json` contains the source-bound sync overrun FAIL and unsettled timeout records.

## F2: actual attempted operations independent of the schedule grid

Replay now scans every eligible scoped raw record independently of expected schedule slots. Actual
operation attempts, including off-grid/retry/late/unconfirmed attempts, remain in the denominator. New
operation records timestamp invocation with `attemptedAt`; legacy records retain the enclosing actual
check interval as an explicit timing fallback, conservatively count attempts at uncertain window edges,
and flag uncertainty rather than claim a passing operation belonged to the window.

`recordAccounting` and `observations` expose off-grid, late, out-of-window, pending-at-cutoff, unsettled,
first failures and retries separately from expected-slot sample statistics. An unexpected/conflicting
observation removes the affected slot's healthy certificate and makes its coverage UNKNOWN. It does not
invent a full interval of failure from one off-grid symptom. An exact on-time failed first sample remains
failed. No cross-region quorum or component averaging was added.

The direct review input (slot0 PASS plus scheduledAt1 failed create) now yields **attempted1/success0/
failed1**, one off-grid first failure and **100ms UNKNOWN, lower0/upper1**. The failed record and the
original slot0 PASS remain visible. A passing retry cannot erase the failure. A not-yet-due attempt stays
pending in the denominator; unconfirmed outcomes cannot count as successes. Pending/boundary-unknown
counts explain any gap between attempted and success+failed; the raw outcomes are not rewritten.

Duplicate samples/operation identities, malformed timing/operation records and contradictory elapsed/
widened-deadline data fail input integrity explicitly. An invalid report throws and emits **no availability
object**; consumers must show UNKNOWN/integrity failure, never convert rejection to empty healthy data.
Range-delta coverage accounting bounds overlapping-anomaly work by input records plus scheduled slots.

Regressions include the exact review counterexample, actual in-window attempts whose schedule lies outside
the window, late reports, future completion/cutoff censoring, retries, window-straddling exact/legacy timing,
duplicate/malformed input, pending attempts and regional isolation. Contradictory per-operation timing/
deadline checks are additional input-integrity coverage, not newly independently rated findings.

## F3: actual numeric job elapsed evidence

Job success still requires actual durable completed state and the matching documented job-specific
source/deadline. It now separately requires `Number.isFinite(elapsedMs)` and `elapsedMs >= 0` before the
deadline comparison. Undefined/missing, null, empty/numeric strings, booleans, NaN/infinities, negatives,
objects and arrays produce **UNKNOWN/job-elapsed-unknown**. Numeric zero and the exact deadline pass;
overdue numeric evidence fails. Wrong source, incomplete terminal state and absent durable proof still fail.
The null-evidence review counterexample is retained as a source-bound UNKNOWN result in the new raw file.

## Verification, provenance and preserved history

- **62 foundation tests PASS**: original31 are byte-identical to445, plus31 new regressions. No assertions,
  discovery paths or skips were weakened. **29 unchanged conformance tests PASS**. Both exact approved CI
  commands ran with cwd `cloudflare/conformance` on pinned **Node24.13.1**.
- SQLite schema generator and freshness check PASS; explicit owned fumadocs/Next typegen/tsc PASS after the
  integration merge. No pnpm, install, new dependency materialization or global cleanup was used.
- Runtime cycles **1→1**,417→429 nodes against exact current integration base; original graph nodes byte-
  compared to base. Same type/test/fixture exclusions and unresolved-module warnings remain in `cycles.json`.
- CLI audit/estimate/replay and actual isolated Node SQLite handler capture PASS. All source/tool/config/
  raw/artifact hashes and commands are pinned by `receipt.json`. New local raw source is74a435a3, region
  **local**, domain **Node SQLite handler fixture only**, never real SDK/workerd/Whagons/hosted proof.
- Every original445 verification document and original private artifact hash was checked unchanged.
  Original passes, tooling deviation/limitations and the review's adverse failure evidence remain immutable.
  New evidence is appended here, not substituted for the old reports.
- Complete current-base diff is classified separately; approved workflow remains exactly the original
  conformance command plus observability glob. Serving/root/package/lock/protocol/K15/K22 are untouched.
  The ordinary merge imported only the lead's already-approved documentation changes.

Final phase `gonvex-m05-foundation-20261112-phase-103dff05d9ee.service`: **19.844s wall**,21,817,930 CPU
microseconds, **611,233,792 bytes** process peak, zero contained OOM kills. Build-headroom passed and normal
coach-resources/kernel-v1 heavy.slice/heavy-workloads.slice admission applied. No native compile/browser/
server/platform campaign was started. Tool/cache paths and docs output links resolved inside the existing
owned `/mnt/data/work/gonvex-m05-foundation` tree before writing. No other checkout/process was touched.

Current implementation/workflow source is **122,817 bytes across20 files**, including tests/docs/fixtures.
This is not a bundled/gzip Worker measurement. Serving bundle/rowsRead/rowsWritten/CPU/isolate memory/
throughput/cost/telemetry overhead were **not executed/measured**; no serving improvement is asserted.

The review's unrated future-cutoff diagnostic is covered by actual-attempt censoring, and contradictory
per-operation timing is rejected. The **existing interval-wide prior-slot heartbeat witness window is
unchanged**; this remains an unrated input-contract diagnostic requiring a declared independent heartbeat
schedule/grace policy and real outage proof. It is not misrepresented as a new verified finding or hosted
monitoring acceptance. Original global-store effects and literal pre-relocation link spelling remain
unverified; this revision does not repair historical evidence by assertion.

## Holds remain

M05 stays stage2/wave4, depends **only P01**, exact done_when **“Each of the five paging conditions fires in
a drill”**. P1–P5 identities/thresholds/windows remain unresolved; added checks never replace them.
All real Whagons/SDK/browser/auth/visibility/receipt/second-client, assembled dev, external three-region,
serving-trace/async continuity, original/fault/authorized-alert, independent heartbeat/status, security,
capacity/cost and real14d/30d requirements remain **UNMET**. K22/X01 and the original V02/V05/V03/V04/R01
holds remain unchanged. Nothing was activated, sent, provisioned or promoted. Lead owns integration,
current-head CI/review and any later activation; author tests do not clear the independent-review hold.
