# X01: fresh source windows before pressure rebalance

Base: `bc247341d17f3c50481a920c922f7c38c211d672` / tree `60828f48cb35c9a373fb15f4c2f172644d8cb231`.
This fixes a reproduced source defect, not an attribution of the independent browser failures.
No live sampling, deployment, provider/config changes, app writes or load campaign was performed.

## Reproduced defect

The new workerd regression accepts a real socket, records one transient high delivery observation, models
31 seconds without new activity/lease cleanup/cooldown using local durable test data, evicts the controller,
and runs its actual alarm. The base allocated a second holder although the actual holder was quiet:
expected one slot, observed two. The original red assertion/log is retained under
`/mnt/data/work/gonvex-x01/rebalance-bc247-20261117/red.log`. An earlier reserved-alarm helper error is separate
and retained; it is not runtime failure evidence. The same assertion passes with the fix.

Previously, pressure maturity came from the controller's elapsed wall clock. Snapshot maxima could also
survive multiple observations with a fresh `at`. Neither constitutes new measurement history. The controller
checked replacement health without first revalidating the supposedly hot holder, so stale data could cause
allocation and drain publication. Thresholds, ceilings, deadlines, 10-second pressure duration, 60-second
cooldown, 120-second quiet grace and delayed safe drain remain unchanged.

## Exact source data and rollout

`GatewayMetrics` lazily seals fixed five-second buckets. There is no new timer or idle polling. `rotate()`
updates report cadence; it cannot create a bucket, erase the current bucket or re-age completed work.
The existing admission scalars retain current/recent maxima. Rebalance evidence is a SEPARATE immutable
completed bucket, never a label applied to mixed admission maxima. `currentPressure` is 0/1 computed from
actual in-progress latency counters and the current supplied socket count. Throughput rates also use the
latest completed bucket for at most one five-second reporting interval: a report immediately after the
boundary must not mistake newly reset counters for a quiet sustained stream. This does not create or
re-age a completed window, carry old latency/socket pressure, or mint another interval of source evidence.
The next completed quiet bucket, a gap or a clock reset removes that throughput signal. Admission's longer
recent-max retention remains separate.

All additive wire properties are numeric so the exact previous controller's `Object.values(...).every`
number/finite/nonnegative gate continues to accept new reports:

- `windowInstance`: nonzero 52-bit random identity per metrics instance;
- `windowSequence`, `windowStart`, `windowEnd`: safe integers; exactly one SAMPLE_MS interval;
- `windowFramesPerSecond`, `windowBytesPerSecond`, `windowDeliveryMs`, `windowQueueAgeMs`, `windowRpcMs`:
  values from that exact completed bucket;
- `windowSocketSamples`, `windowSockets`: count and minimum of explicitly supplied socket observations
  INSIDE the bucket. Seal/advance occurs BEFORE recording a later supplied count.

Zero socket samples means UNKNOWN, with a numeric zero sentinel excluded from socket pressure. It is not
measured zero/quiet/empty. The sampled minimum is a finite-observation policy signal, NOT a bound on
continuous occupancy: it can exceed the unobserved true minimum and says nothing about an unobserved
boundary. Admission and scale-in/retirement use actual current authoritative counts separately.

A constant-size state-only `pressureEvidence` links the slot epoch, all exact window measurements/metadata
as a fixed-length numeric tuple, and streak
start. A legacy wall-clock `pressuredSince` is never trusted merely because an old controller later stored a
new-format sample; an old write changing that link resets/refuses maturity on adoption.

New controllers validate explicit base fields and complete-or-absent window groups, bound cardinality and
numbers, and copy/freeze observations before storing them. Legacy flat senders genuinely omit optional
properties. Their scalars still support admission/growth; missing source history cannot mature a pressure
rebalance. Tests run the frozen actual bc247 metrics and controller implementations, including rollback
reading state-v1 with new numeric optional fields. These fixtures change only relative import paths.
An old controller remains subject to its old pressure bug until it adopts the new source; numeric wire
compatibility is not a claim that mixed-version old decision code is fixed.

## Decision and resource bounds

Pressure rebalance requires distinct adjacent completed pressured buckets spanning at least 10 seconds,
from one instance, with a fresh last bucket (at most 2*SAMPLE_MS old) and real current pressure. Repeating
one bucket never adds duration. Gaps, instance replacement, rollback/regression, revised same-sequence
measurements and expired evidence reset/refuse maturity. No unreported window is invented.

A mature candidate is probed authoritatively BEFORE replacement allocation or drain publication. The actual
accepted probe/window/current pressure must still qualify; old, quiet, failed or refused probes cannot leave
cached maturity in charge. Quiet scale-in also rechecks its candidate and replacement after the existing grace.
Confirmed empty plus no pending admission can retire immediately; failure never means empty. Existing held
sockets remain until the unchanged safe drain closes them. No Session/host/delivery/router/config edit.

Maintenance work remains bounded: at most two `capacity` probes per event, separately at most one existing
`drain` command RPC. The command's existing deferred gateway health/session work is not counted as a probe.
An already draining holder is handled separately with one ownership probe. Admission still probes at most
two candidates. Existing lease/replay/epoch fencing, 64 slots/cell, 256 admission leases, 64-event history and
120-KiB routing-state ceiling remain. History is one immutable completed bucket and one source streak per
holder; no per-tenant labels, SQL or new logs. Other isolate state and actual serving memory remain unmeasured.

## Verification and limits

Runnable resource payloads, results and logs are under the artifact root above. Normal headroom and one
resource controller at a time were used. Relevant commands:

```sh
cd cloudflare
pnpm exec vitest run --root kernel/gateway
pnpm exec vitest run kernel/session/test/socket.test.ts kernel/delivery/test/delivery.test.ts --maxWorkers 1
pnpm exec vitest run --root kernel/assembly
pnpm typecheck
pnpm types
(cd control && ../node_modules/.bin/wrangler types --include-runtime=false)
node scripts/sync-sqlite-schema-options.mjs # from repository root
pnpm --filter @gonvex/docs typecheck       # from repository root
```

Coverage includes the original positive pressure test/assertions with actual sustained metrics buckets,
controller restart, legacy/unowned timer adoption, rollback-mutated evidence, bounded state refusal, positive quiet out/in and ownership after close, duplicate probes, stale alarms,
source gaps/instances/regressions, immutable/revised/unsafe groups, old/new telemetry and state readers,
failed hot re-probe and separate maintenance-probe/drain-command counts. Virtual clock/durable data are
local test fixtures only, not a runtime clock or deadline policy. The old controller constructor is unchanged;
tests use its frozen methods on already-started native DO state to avoid constructing a second concurrent
controller against the same storage. Helper construction errors were retained as author test history.

Final local checks: 55 gateway tests (including all 13 original controller cases), 46 session/delivery tests,
16 assembly tests and 49 conformance unit tests pass. Docs/schema/Worker type generators and full Cloudflare
typechecking pass with no generated drift. Madge reports the same single existing local-runtime cycle at
base and head; no runtime cycle was added. Framework dry-run bundle: 1,728,565 to 1,735,131 bytes raw
(+6,566), 413,686 to 415,249 gzip (+1,563). These are source bundle sizes, not an assembled app deployment.

The controlled integer fixture in `state-overhead.json` serializes 265 extra bytes for the 12 additive sample
fields and 92 bytes for the marker object, or 377 extra bytes per slot including property/envelope overhead.
That is an illustrative exact JSON fixture measurement, not live heap or storage usage. Each marker has
one 11-number tuple plus epoch and streak start; it cannot retain an old-instance list. The unchanged
122,880-byte routing-state guard has a regression proving refused admission is not published.
KV operations still use the same controller state access path; actual backing SQL row/billing effects and
shared-isolate peak RAM are not measured by these local regressions. The added numeric group is bounded;
the routing-state byte ceiling still fails closed. No cost, FIN, OWNER, capacity or full-V01 acceptance claim.

Next: ROOT's independent source review/CI and permitted integration merge, then its dev release and NEW
runtime verification. Correlate actual close reasons and fresh window/decision data on adopted source.
The bc247 report's unknown 503 reason, `session recovery requires reconnect`, K22 publication work and
all historical conformance/capacity/load gaps remain separate and open.


The separate ROOT fixture correction `03f0f6d6` is cherry-picked as a single test-file commit. Its unrelated
parent/43-file architecture is NOT included. The failed-request assertion now identifies its own lease,
preserves a deliberately seeded unrelated lease, and compares the baseline count; no admission requirement
is weakened. Final gateway checks rerun that actual admission test.

One broader parallel session/delivery iteration hit the unchanged 0063 test's five-second harness timeout
(45 pass, one timeout). Final qualification serializes those two test files with `--maxWorkers 1`, retaining
all assertions and original timeout. This is an explicit local concurrency caveat, not remote acceptance.
