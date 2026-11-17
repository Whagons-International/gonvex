# Failed-offset validation and replay-origin correction

Tested source checkpoint `7b8bf781105886f65ede38469e826ace1a0f6dc3`, code/test commit
`14ca580b8c2d57da64d12a28894be688f45354d9`. Current integration0ee475cc was merged normally;
the merge adds only lead documentation. Evidence appended after this checkpoint changes no implementation.

Fresh [COMMENT5882133354](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5882133354)
pins a59c7631 and reports two P2 timing defects while clearing the exact prior1b fixes. The lead made
these bounded corrections in its own checkout. No implementation delegate or completed reviewer was
started/reused for them. This is implementation evidence; NEW independent exact-head review remains
required. Full M05 acceptance and activation remain separately UNMET.

## R1: validate the attempt itself even when completion evidence is absent

Every present attemptedAfterMs already had to be scalar, finite and within the monotonic duration. It
must now independently project to a representable time within finishedAt plus the recorded clock bound.
This check runs even for failed attempts with no durableMs/appliedMs. The exact started0/finished5/
duration100/U0/offset80 failure is integrity UNKNOWN: actual CLI emits no availability object or input
and its fixed integrity code. No attemptedAt is fabricated and no operation is stripped into green.
Valid absent completion timings, exact boundaries and declared uncertainty remain supported.

## R2: one attempt origin for eligibility, expiry and application cutoff

The validated raw leaf exports one attempt-origin projection: supplied attemptedAt, otherwise startedAt
plus supplied attemptedAfterMs. Replay uses it consistently for time-window/cutoff eligibility, deadline
expiry and application proof. An offset80/deadline10 remains pending at85 and expires at90 under U0,
matching equivalent timestamp80. Before its earliest possible time it is explicitly outside the eligible
window; the retained raw/check observation and outside-window count remain. It is not erased as malformed.

No extra tolerance, retry or success is invented. Declared clock uncertainty delays expiry and preserves
uncertain membership; observed completion is still required. First/retry and create/update paths share
the same rules. A truly legacy record with NEITHER time form retains the existing explicitly labeled
legacy fallback/window uncertainty; offset projection is not mislabeled a directly measured wall timestamp.
The existing no-attemptedAt diagnostic remains compatible. Raw snapshots/fields are not rewritten.

## Proof and limits

115 foundation tests pass:100 byte-unchanged prior tests plus15 new regressions.29 unchanged conformance
units pass. The new15 cases against immutable a59 record **14 failures/1 control**, preserved separately.
Eight create/update/first/retry/offset20–80 cases compare timestamp-equivalent eligibility, pending/expired
results and uncertainty at multiple cutoffs. Controls cover declared clock bounds, zero offset, observed
success, late failure, shifted time origin and legacy uncertainty. Actual CLI negative/pending/future/
expired cases exercise retention-to-report behavior without mocking its result.

Schema/freshness, root/control types, docs metadata/typecheck and strict tracked-source runtime cycle
comparison are recorded in receipt.json, with exact hashes/tools/commands/resource phases. Baseline and
candidate share the same graph edge policy; no assertion or generated output is hand-merged. All earlier
445/493/1b/a59 reports, raw failures, source/tool receipts and100 prior tests remain unchanged. The local
tests are raw-input/Node/SQLite/adaptor/CLI fixtures, not SDK/Whagons/workerd/hosted-region proof. Source
bytes include tests/docs and are not serving bundle, SQL rows, isolate memory or cost measurements.

Every heavy phase uses fresh headroom/normal coach-resources kernel-v1 and the unmodified four-slot
coach-compile wrapper. Direct pinned Node24/tools only; no pnpm/install/dependency materialization, account,
credentials, live resources, alert/status activation or staging/production activity. Source scopes remain
observability/evidence and the original approved CI command, with no serving/root/config/package/lock/
protocol/Rust/module/K15/K22/placement/outlet changes. No source clearance or merge follows from passes.

Original stage2/wave4/P01-only dependency and exact five paging-drill done_when/source-definition gap,
real app/SDK/browser/second-client/three-region/heartbeat/trace/alert/status/security/cost/capacity and actual
14d/30d gates remain UNMET. Lead owns the NEW independent review and effective merge lane afterwards.
