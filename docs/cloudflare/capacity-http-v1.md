# Bounded serving capacity HTTP v1 (K22 → X01 driver)

`GET /__assembly/capacity/v1` uses the **existing** explicit dev fixture
`GONVEX_ASSEMBLY_FIXTURE_KEY` bearer gate. Missing gate, missing bearer or wrong
bearer returns empty404 before validation/RPC. No new secret, serving class,
namespace, provider collector or deployment setting is introduced. This source
change does not enable the gate on any deployment.

The driver contract is
[`capacity-contract.ts`](../../cloudflare/kernel/assembly/capacity-contract.ts).
No SDK/generated-reference change is involved. Endpoint implementation imports
only existing controller/holder ports; it never calls acquire, claim, release,
observe, reconcile, drain, alarms, reset or eviction.

| Query | Strict decimal bounds / meaning |
| --- | --- |
| `cell` | Required0..31; exactly one authoritative controller cell per request |
| `cursor` | Slot array offset0..64, default0 |
| `limit` | Slots1..4, default4 |
| `eventCursor` | Retained event array offset0..64, default0 |
| `eventLimit` | Events1..16, default16 |
| `fresh` |0(default cached) or1(explicit induced holder sampling) |
| `runId` | Optional1..64 ASCII letters/digits/underscore/hyphen; correlation only |

Unknown or duplicate fields, signs, fractions, leading zeros, unsafe integers,
out-of-range numbers and query strings over512characters return400 `BAD_QUERY`.
Offsets beyond the returned source arrays return400 `BAD_CURSOR`. Authorized
non-GET returns405/Allow:GET. There is no caller-supplied holder name, tenant,
provider URL, SQL, arbitrary state or expected-binding assertion.

Each200response is `CapacityPage`v1; `pageStatus` describes this **one page**,
never fleet readiness. A successful controller read includes epoch, recorded
pending lease count, readAt and SHA256 of its exact bounded source status.
Slots/events have returned/total/nextCursor/hasMore. A client assembling pages
must compare `controller.value.snapshotSha256`; a changed digest invalidates a
single-snapshot join. It is not a lock or server-retained cursor. At most64slots
and the last64events exist in this source interface. `total` is the current
array cardinality, not an all-time event count. Even fewer than64events cannot
prove losslessness: retained oldest/newest timestamps are reported and loss is
explicitly UNKNOWN. Other31cells, earlier events and inter-page changes are
outside this page. The bridge keeps no additional history.

Cached mode calls only `GatewayControllerObject.status()` once. It copies
actual slot modes, original sample.at/window identity, pressure evidence and
event facts. It does not restamp sample ages or prune expired leases. Null
samples remain UNKNOWN. Staleness is computed against the HTTP service's wall
clock: sample age over30s is STALE; apparent future time over1s is CLOCK_SKEW.
Completed windows older than2×existing5s SAMPLE_MS are STALE. Service clocks
are explicitly **not synchronized**; these labels are apparent recency, not a
measured cross-isolate clock bound. Missing windows are UNKNOWN.

Fresh mode selects only non-retired slots from this validated authoritative
page. At most4holders are contacted. Each receives existing `capacity(epoch)`
and, only after a valid matching-epoch capacity reply, `drainStatus()`.
Capacity epoch mismatch, malformed/unknown fields, RPC refusal and timeout are
typed failures. Retired slots are marked RETIRED without waking them. No
failure becomes a zero socket count or an optimistic positive. A stale fresh
reply retains its original sample and makes the page PARTIAL.

This observer is **not passive**: controller status can wake/read its DO;
capacity can wake the holder, restore its existing lifecycle and invoke
`GatewayMetrics.snapshot`. Snapshot can seal/advance a bucket and records a
socket sample in the current bucket. The reply records actual capacity/drain
call counts and these effects. A completed `windowSockets` is the minimum of
actual in-bucket socket observations, with `windowSocketSamples`; zero samples
means UNKNOWN, not measured empty/quiet. It is not continuous occupancy.
Window instance/sequence/start/end are copied; skipped history is not invented.
`drainStatus` is the last tracked result with **no result timestamp**. Its
remaining/closed values are not a fresh empty-holder, terminal or OWNER proof.
Normal wake/recovery work can have ordinary side effects even though this
facade invokes only read ports. It never refreshes a lease or changes scaler
thresholds, maturity, cooldown, capacity ceilings or drain eligibility.

Budgets are source constants: controller1500ms, each holder RPC750ms, overall
request5000ms, at most2RPCs in flight, at most1controller+8holder calls. A holder
timeout/abort stops scheduling further calls, including drainStatus, so an
uncancellable timed-out RPC does not permit unlimited replacement calls.
Workers RPC cancellation is unavailable here: in-flight calls may finish after
HTTP refusal; they are not claimed cancelled. There is no async observer queue,
global log or retained per-run state. Pre-abort returns499; later abort becomes
typed ABORTED. Timers/listeners are removed after each call. A raw exception is
never serialized. All replies are no-store JSON, capped at32768UTF8bytes; a
budget overflow returns503 `RESPONSE_BUDGET`. Input ports enforce64slots,
64events, known field/type/name/cell bounds and96KiB controller/4KiB holder
budgets. Unknown/malformed fields are refused before projection, never echoed.
The existing controller120KiB persisted-state guard is unchanged.

Identity reports the HTTP Worker's **actual** existing WORKER_VERSION metadata
when present, the nonsecret configured runtime label, generated catalog version,
project count and SHA256 over sorted `[project,hash,generation]` tuples. IDs and
manifests are not returned. Catalog identity is bounded to2048entries with
bounded scalar fields. Config facts/digest cover only controller-binding
presence and existing static shard fallback; they are **not** a provider
settings/binding-namespace audit. No `assembled:true` or `autoscaling:true`
assertion substitutes for measurements. The existing version port is HTTP
code identity, not proof of inner DO code adoption. Holder/controller code
versions have no existing source port and are explicitly UNAVAILABLE.

CPU, isolate memory, SQL, Control/auth/limiter service measurements, billing,
cost joins, event-loss counters and drain-result age are UNAVAILABLE with named
missing source interfaces. `/dev/metrics` placeholder zeros/empty maps/logs are
not consumed. `runId` is not a provider-meter join. A separate explicit actual
provider collector or clearly modeled downstream calculation is required for
cost; this endpoint does not fabricate either.

Tests run locally in workerd against the real Controller and AssemblyGateway
through the actual fixture HTTP handler, including an accepted native socket,
controller restart, original state/alarm preservation and induced sample count.
Separate adapters are explicitly LOCAL DATA for malformed, foreign, timeout,
abort and concurrency cases; none enter a serving deployment. Test-only
capacity namespaces isolate automatic admission from unrelated composition
fixtures. No serving namespace migration is involved. Qualification is source
evidence only; real deployment, V01/V03, managed Cloudflare, load, cost and OWNER
gates remain open.
