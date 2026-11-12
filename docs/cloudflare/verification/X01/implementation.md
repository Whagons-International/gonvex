# X01 implementation artifact — acceptance still open

Base: e72d3a437b65bea5a8524064e3e04904fc4a1f7b (K10 merged; K20 9a69e2b3).
Owned checkout: `/home/gabriel/.t3/worktrees/gonvex/t3-298eec64-3e15b422e94aa466`.
Branch: `cf/X01-autoscaling`. No Session/engine/assembly/root serving configuration edits.

The original done_when is **unmet** until the integrated K22+X01 runtime passes V03 at 1,000 users,
including the 26-tenant campaign while scaling. A PR, package harness, or configured limit is not this proof.
This artifact does not partition tenant SQLite: one tenant still has one serialized writer.

## Lead integration fixes after independent review

The review of immutable head `60be3adc` requested three source changes. The lead ordinary-merged canonical
planning base c97b35a7, then fixed membership-only byte admission, empty-discard idle/host-credit cleanup,
and late K16 admission after router timeout. Instance/recovery text and every membership/account string
now consume estimated credits before retention. Empty queues settle their outstanding idle promise and
release safe metadata; failed recovery keeps its fence. The router passes its actual admission expiry to
the limiter, which checks it before/after audit awaits and before writing slots, so an abandoned request
cannot install late reservations. This adds no per-subscriber SQL or new persisted timer/tombstone map.

Affected-source checks pass: 390 root workerd tests (one worker, unchanged assertions/timeouts), 22 gateway,
32 public, two public-Reducer tests and 29 conformance unit tests; generators/typecheck/bundles pass and
runtime cycles remain the same one existing local-runtime cycle. The timeout regression runs in workerd
with a deliberately delayed audit seam and real router/guard/budget code; it proves slot cleanup and next
admission, not deployed cross-RPC lifetime or full capacity. See `verification.json` and raw
`/mnt/data/work/gonvex-x01/lead-review-fixes/`. First test-only typecheck failure is retained and corrected.
Original full-suite receipts and failed parallel reruns remain historical evidence, labeled by their head;
they are not relabeled as a fresh full-package pass. A new independent exact-head review is required.
All assembled/deployed, V03/V04/X02 and platform guard gaps below remain open.

## Routing and recovery

The router validates method/upgrade/extensions before allocating. A random pre-auth connection ID selects
one of 32 controller cells by FNV-1a; tenant identity still arrives only in the unchanged SDK auth frame.
Each cell owns a bounded durable pool, versioned holder epochs, and expiring single-use admission tokens.
Assignments bind connection, holder, cell and epoch. The gateway checks the authoritative controller claim,
not a client-supplied header. The router replaces that header and preserves K16's actual-holder audit.
Assignment claims survive controller restart. TTL expiry cleans abandoned admission, never accepted ownership.
Claimed leases conservatively count alongside socket samples until expiry, so admissions can overestimate
occupancy for 30 seconds; this spends headroom rather than over-admitting a holder.

Existing ownership stays in gateway socket attachments, with the durable holder epoch separate from the
admission TTL. A reused holder name receives a greater epoch. Retirement requires an authoritative empty
holder probe and no pending lease; failure never means empty. Fixed names bound historical object allocation.
A 64-entry decision ring per cell exposes allocations/drains/retirements without per-member telemetry.

No deployment/env change selects a larger pool. Socket pressure or outbound frames/bytes, handler delay,
tenant queue age and previous holder RPC latency influence admission; sustained delivery pressure also
creates/probes a replacement and drains an existing hot holder. These are protective initial thresholds,
not measured full-app capacity. Receiver-applied latency and CPU/isolate memory require the V03/V04 driver.
No static fallback runs during controller outage: new upgrades receive 503; established delivery continues.
The absent-binding static adapter exists for old package harnesses only.

Scale-in requires 120 seconds quiet and a 60-second holder cooldown. At most one holder per cell drains.
A replacement is probed before publication and before every cohort; controller authorization, batched
Control revalidation/public resolution and at most two address-limiter reads gate deliberate reconnect.
Unknown/unhealthy dependencies pause the cohort. After a 30-second natural-disconnect grace, at most two
sessions are considered per roughly five seconds, staggered by holder hash; a fully blocked cohort waits
30 seconds. K22's authoritative synchronous `Session.gatewayDrain()` decides eligibility and flushes before
1012 `gateway capacity rebalance`; this artifact never closes through an unresolved call or direct Action.
Blocked reasons and remaining counts are exposed by `drainStatus()`. No timer blindly replays a call.
The cell drain record and holder alarm state survive wake/restart. Lease cleanup and finite transitions use
alarms; a stable empty cell/holder does not start an idle sampling/retry timer.

## One ordered delivery channel and byte credits

Tenant commits, K22 live-query operations, membership handoff, snapshots and recovery use the existing
`ReplicaDelivery` holder channel. Pressure fields are internal and additive, with no SDK wire change.
`GatewayOutlet` maintains one in-flight RPC per holder and releases discarded pending payloads immediately.
`DeliveryCapacityError.code` is `GONVEX_DELIVERY_CAPACITY`; no over-budget normal payload is retained.
K22 must suppress normal sends/new opens on that error and perform whole-holder recovery, not log/drop it.

`reset(host,payload)` reserves one same-channel barrier after the existing in-flight RPC; it discards queued
normal work (settles undefined), and coalesces repeated reset calls into the same completion promise.
Payload is `{v:1,instance,shared:[],sockets:{},recovery:{reason:'change-feed-lagged'}}`. Completion requires
`ReplicaReceipt.recoveryAck === 1`. An ordinary old-holder receipt rejects with
`GONVEX_DELIVERY_RECOVERY_UNSUPPORTED`, retains recovery-required state and blocks normal work. RPC failure
also retains recovery-required state; only an explicit new reset retries. LocalOutlet obtains acknowledgment
from its actual receiver, never fabricates it. K22 engine must not advance accepted revision/delivered on an
undefined discarded receipt. K22 additionally owns serialized live-query catch-up before normal sends resume.

The gateway awaits K22's `Session.recoverReplica(tenantId,payload)` for EVERY applicable session before ack.
An absent old-version hook gets no ack; a failed hook rejects. K22 supplies reset of installed replicas,
pending/failed opens and retained live queries, preserving auth and pending direct Actions. The hook is not
present in this independent base; missing semantics fail closed rather than pretending package parity.

Byte/count/RPC credits are module-scope, shared by all outlet instances in one Worker isolate:

| Initial safeguard | Value | Meaning |
| --- | ---: | --- |
| Controller cells | 32 | Distributed allocation owners, not an account-capacity claim |
| Holder names per cell | 64 | At most 2,048 automatic holders, plus up to eight legacy holders during skew |
| Holder socket admission | 125 | S3 starting point, not a measured safe app user count |
| Admission leases per cell | 256 | 30-second TTL; controller waiting queue 64, gateway admission queue 16 |
| Scale-in / cooldown | 120 s / 60 s | Protective hysteresis, not empirically calibrated |
| Normal pending credits | 8 MiB/holder, 16 MiB/isolate | Includes text/copy/envelope estimates and active normal work |
| Normal items | 256/isolate | Includes in-flight normal work |
| Recovery reserve | 8 MiB/isolate | Constant-size universal recovery payloads, separate from normal credits |
| Active holder RPCs | 8/isolate | Scheduling bound, not a memory guarantee |
| Queue metadata | 2,056 hosts/isolate | Includes recovery-required holders |
| Large legal payload | Sole normal singleton with all byte credits empty | Does not impose an 8 MiB wire limit |

Credits conservatively charge UTF16 text plus allowance for two worst-case UTF8 serialization/transport
copies and envelope objects. This is an estimate, not a heap allocator or an actual peak measurement.
A sole legal near-32-MiB frame may still exceed 128-MB shared isolate memory when all serving state/copies
are included. Do not reduce protocol assertions to hide that potential platform blocker. Weak owner records
permit a bounded sweep of GC-collected outlet metadata at the host ceiling; active RPC/payload credits stay
held until finally/discard. GC timing, DO eviction and aggregate shared-isolate memory still require proof.
At a limit, reject/back off/recover; no unlimited scaling claim is made.

## K22 integration patch (lead owns serving files)

1. Export `GatewayControllerObject` from `kernel/gateway/controller` in the serving entry.
2. Append `GATEWAY_CONTROLLER` binding, class `GatewayControllerObject`, and a NEW SQLite migration tag.
   Preserve every prior migration and K22 assembly binding/export. Regenerate Wrangler types.
3. Keep `GATEWAY` and `PUBLIC_LIMITER` available to the gateway; existing K22 subclass/actionFinished,
   telemetry and publicGuard injection remain untouched. The base reads the new namespace directly.
4. Use K22 Session drain/recovery hooks and engine backlogged/capacity/reset integration. Merge the narrow
   additive protocol fields, not competing whole Session/engine files. Remove the static shard-count var
   from the assembled serving config so it cannot be mistaken for the automatic path.
5. Re-run the composed regression/conformance suites before any bounded development deployment.

## Reproduction and evidence boundary

Normal arch3 headroom/resource wrappers apply. Payload scripts and sanitized logs/results are under
`/mnt/data/work/gonvex-x01`; they write durable result JSON. Worktree owner is `gonvex-x01`.

```sh
cd cloudflare
pnpm typecheck
pnpm test                          # includes the independent gateway workerd suite
pnpm exec vitest run --root kernel/gateway
node --test conformance/test/*.test.mjs
pnpm exec wrangler deploy --dry-run --outdir /mnt/data/work/gonvex-x01/bundle
```

The X01 test Worker uses K05 stand-ins intentionally. Tests backdate durable state to exercise hysteresis
without a two-minute wall-clock wait; this is state-machine regression, not measured workload scaling.
No representative tenant data, real SDK capacity workload, remote region, or shared-service throughput is
claimed here. No staging/production activity or account changes occurred.

## Remaining proof matrix

| Gate | Status / required next evidence |
| --- | --- |
| Independent workerd routing/claims/epochs/TTL/empty retirement | Local regression evidence; see result artifact |
| K22 whole-holder recovery, active/unkeyed/direct-Action drain | Integration required; absent hooks fail closed |
| Real SDK resume/receipt/checksum/query+replica ordering during rebalancing | Composed local + exact remote proof required |
| 1,000-user V03 and 26-tenant mixed campaign while actually scaling | UNMET; real app handlers/copies and driver pending |
| Bursts, reconnects, deployment skew, controller/drain restart/fault | Independent cases only; composed campaign outstanding |
| p50/p95/p99 durable confirmation AND receiver-applied delay | UNMEASURED, not interchangeable |
| Writer ceiling; offered/dispatched/achieved throughput; quiet neighbors | UNMEASURED; gateways do not split the writer |
| Shared Control/auth/revalidation, NAT limiter, routing, builder/deploy/Workflow/sandbox | Existing services preserved; aggregate bottlenecks UNMEASURED |
| 128-MB shared isolate, SQL/rows, bandwidth, peak singleton copies, wake | V04 measurements outstanding; credits are not platform proof |
| X02 cold-boot 1,000-client writer p95 <150 ms | UNMEASURED; evaluate after integration and pull X02 forward if needed |
| Account quota/headroom and actual billable usage/cost | UNVERIFIED/UNMEASURED; Paid plan alone is insufficient |

There is no dollar-cost or safe-user-count estimate without measured usage and actual account rates.
The envelope above bounds new resource identities and retained control work. It does not bound unrelated
Control/Workflow/Container costs or prove this account can provision the entire envelope.

Final control safeguards: routing state refuses publication above a 120-KiB serialized ceiling. Allocation
has at most two candidate probes; each maintenance event considers at most one scale-in candidate. A draining
cell checks held ownership every 30 seconds until an authoritative empty probe retires it, including when the
last close report was lost. This stops on retirement; it is not an empty-cell polling loop. Gateway drain ticks
are single-flight so duplicate controller commands cannot multiply reconnect cohorts. `deliveryPressure()`
exposes isolate credit totals and capacity/unsupported-recovery counts. Weak-owner cleanup does not promise
timely GC or prove liveness under collection/hibernation; that remains an explicit proof gap.

K22's separate unchanged `errors.spin` caller-quarantine/local-workerd blocker (lead ruling 0057) is not
covered by this base/package test pass. Do not describe these results as a full assembled-runtime pass.

Coordinated protocol fence: `SocketWork.subscriptionEpoch?: number` is additive. K22 owns stamping every
normal/split work part and rejecting stale epochs before applying data or instance state after reauth or
same-tenant member/scope change. K22 also owns ordered initial query snapshots and the server-only recovery
socket marker; X01 does not parse a client recovery bypass or modify split/Session/engine logic.

Validation caveat: two default-parallel affected-root reruns hit the unchanged migration-resume test's
5-second harness timeout. An earlier full package run passed it; all 390 root assertions subsequently pass
with `--maxWorkers 1` and the original timeout/assertions unchanged. This is reported, not hidden as an
all-default exact-head pass. Non-root package checks and the final gateway suite are recorded separately.
