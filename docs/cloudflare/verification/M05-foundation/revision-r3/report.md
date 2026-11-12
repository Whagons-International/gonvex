# Bounded correction of the1b exception and legacy-timing findings

Source checkpoint: `54443a752d47edaf566abfab09404787443f04ca`; code/test commit
`a9550d7255e7639402ff6b4f9d2f6d3a446ae934` is identified in receipt.json by its full Git SHA. Integration87726e80 was merged normally;
its additions are documentation only. This report/evidence is not independent source clearance.

The fresh [COMMENT5881664350](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5881664350)
pins1b3455bb and reports two P2 findings while clearing the exact493 cases. Its report, receipt, raw
reproductions and all previous445/493/1b source/tests/evidence remain unchanged. The lead made only the
two bounded source corrections and new regression coverage in its own checkout; no implementation or
completed reviewer delegate was started for these small fixes. A NEW independent exact-head review remains
required before recommending finite foundation merge. Full M05 acceptance remains separately UNMET.

## N1: classify exceptions without invoking adapter error behavior

Locally constructed CheckError objects now have private immutable scalar classification metadata in a
module-local WeakMap. Classification never uses instanceof, prototype traversal, arbitrary property reads
or coercion on an adapter exception. For a branded local Error, own data descriptors must still match its
original code/status; replacing a field with an accessor or another value is unsafe, without invoking it.
Wrapping/revoked proxies, arbitrary exceptions, invalid code input and altered declared errors cannot
recover that private classification. They yield fixed adapter-error outcomes and pause/abort unsafe
execution. No original exception, headers, contents or cause enter retained output. Intact declared
failures keep their existing code/status and cleanup behavior. Constructor code input is not coerced.

Tests exercise actual dispatch with hostile prototype/get traps, revoked proxies, code/status accessors,
coercing code objects, ordinary errors and a valid declared failure. They assert no attacker getter/
coercion call, no private synthetic marker in output, and the intended guard pause. These are local
adapter-error fixtures, not an observed deployed credential incident or hostile-module sandbox proof.

## N2: legacy and offset-only elapsed proof must fit the wall observation

For every present durable/application elapsed value, the existing monotonic bound remains. When no
attemptedAt is available, its earliest possible attempt is now also projected from startedAt plus the
declared monotonic offset, if any, and must fit finishedAt under the declared clock uncertainty. A long
durationMs alone cannot certify application beyond the observation/cutoff. No timestamp is fabricated,
no operation/attempt is erased, and no tolerance beyond the recorded uncertainty is added.

The exact started0/finished5/duration100/uncertainty0/elapsed80–90 contradiction rejects integrity on both
pass/fail records, including offset-only and timestamp forms. Actual CLI output is empty with its fixed
integrity code. Valid legacy data and valid late failures still count; a clock-consistent uncertain result
cannot become successful before its cutoff permits it. Existing493 edge/accounting/deadline/privacy
assertions remain unchanged. No general RPC normalization applies to this pure raw-data boundary.

## Verification domains and retained failures

100 foundation tests pass:84 existing byte-unchanged tests plus16 new regressions.29 unchanged conformance
units pass. An actual pre-fix1b archive run of the first15 new cases yields **11 failures/4 controls**;
the first corrected run passes99 foundation tests, followed by100 after adding the constructor control.
An earlier commentary estimated12 failures; the recorded actual summary11 is authoritative. Original
assertions were not edited. Two initial resource-wrapper invocations found no relative test paths because
of their working directory; no tests executed in those attempts. Those logs remain distinct from the
actual before/after proof. Later commands use explicit scripts/absolute roots and exact CI test commands.

Pinned Node24.13.1, no pnpm/install or dependency materialization. Normal fresh headroom/coach-resources
kernel-v1 phases and the unchanged four-slot coach-compile wrapper apply. Exact source/tool/command/raw/
resource hashes and generator/cycle results are in receipt.json. Local process CPU/memory figures are
verification costs only, not serving/isolate/capacity measurements. Fixtures and prior source-domain
limitations remain: local Node SQLite/adapters/CLI are not deployed SDK/Whagons/browser/provider proof.

Generators/freshness, root/control types and docs typecheck pass. Runtime cycles remain1→1,417→430
tracked nodes against current integration87726e80; every baseline graph edge is unchanged. Initial cycle
inputs differed: the working tree included pre-existing ignored generated harness bundles, and the next
archive omitted two referenced Rust SQL/JSON leaves. Those failed strict graph comparisons/raw artifacts
are retained. The final comparison uses verified tracked source archives with all referenced inputs;
the strict graph/cycle assertions were not weakened. Unresolved virtual/alias imports remain limitations.

No serving entry/config/package/lock/protocol/Rust/module, K15/K22/placement/outlet source changed. No
serving bundle, SQL behavior or measured runtime-memory improvement is claimed. No live monitor, alert,
status publication, credential/account/deployment/image or staging/production operation occurred. Original
M05 P01-only dependency and exact five paging-drill done_when, unresolved P1–P5 source definitions, real
application/hosted regions/serving traces/heartbeat/drills/security/cost/capacity and actual14d/30d gates
remain UNMET. The current independent reviewer was not reused; lead owns new review, checks and merge.
