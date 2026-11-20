# 04-hang: tenant deliveries stopped after a CPU-limit reset (dev, 2026-11-20 14:30-14:50Z)

On the integration dev deployment, logins on `devcopy-stg1117-04` hung from about 14:29 to 14:57Z. The tenant object
accepted `replica.open`/`openMany` frames, ran the opens, and delivered nothing back. A redeploy at 14:50Z cleared it.

## Verdict

This is a real liveness bug in the replica delivery outlet (`cloudflare/kernel/delivery/outlet.ts`). It is not a
problem in the engine's queues, the attach check, output gates or alarms.

- **Trigger:** at 14:30:41.133Z the platform reset the -04 tenant object: `Durable Object exceeded its CPU time
  limit and was reset`. One invocation had used 7,500 ms CPU against `cpu_ms = 5000`. About eight delivery RPCs to
  gateways were in flight at that moment.
- **Bug:** the outlet's RPC slots (`MAX_RPCS = 8`) and its byte credits are module globals shared by every tenant
  object in the isolate. The isolate survived the reset. The reset object's RPC continuations belong to its dead I/O
  context, so they never run, and `run()`'s `finally` never returned their slots. With all 8 slots held this way, no
  outlet in the isolate could start another delivery RPC.
- **Effect:** the new instance of the -04 object (it woke at 14:30:52Z) ran every open it was sent and queued the
  payloads. Its holder queues grew to 6 items. No delivery RPC completed for 20 minutes, and no error was logged.
  The client waited for replies that never came. The 14:50Z redeploy replaced the isolate and cleared it.

Reproduced in workerd, and fixed: PR into `cloudflare/integration`.

## Timeline (UTC, Workers Logs, object `f6011e77` = devcopy-stg1117-04)

| Time | Evidence |
| --- | --- |
| 13:20 | x18r (diag) deployed: runtime `eba04be3`. |
| 14:28:31-14:28:48 | `release-x18m/run-back3.out`: kernel, runtime and actions roll back to `ca4a2b89`. |
| 14:29:07 | -04 wakes on the new version (`tenant.wake`). |
| 14:30:33-14:30:41 | A burst of concurrent cold loads after the deploy: at least 5 clients, an empty snapshot cache, `tasks.recentWorkspaceTasks` reading about 3,300 SQL rows per open. The object uses about 11 s CPU in 8 s. One invocation records **7,500 ms CPU over 7,811 ms wall** (`exceededCpu`). |
| 14:30:40-14:30:41.120 | Last `tenant.delivery` spans of the old instance. Eight RPCs complete in the last batch; the channel is busy. |
| 14:30:41.133 | **`Durable Object exceeded its CPU time limit and was reset`** (14 log lines). Every invocation in flight ends `exceededCpu`. |
| 14:30:52 | New instance (`tenant.wake`). Opens run (`tenant.open` with `queuedAfter` 1-6), snapshot-cache misses as usual. **No `tenant.delivery` span from here until 14:51.** The span is logged in `run()`'s `finally`, so a failed RPC would still log one. |
| 14:31-14:50 | 2,121 invocations all end `ok`. 2 alarms ran. No error or warn lines, no `delivery.attach.refused`. The holder gateways never log `gateway.receive` or `replica batch sent` for -04. |
| 14:40:44, 14:42:37 | X17's login probes (`logs/broken04-open.jsonl`, `logs/broken04.jsonl`): gateway `tenant` auth succeeds, then the two first opens (`members.listMembers`, `settings.plugins`) run with `queuedAfter: 2`. Nothing reaches the client, so it never sends its cohort. |
| 14:50:11-14:50:31 | `run-back4.out`: the same build is redeployed (new isolate). |
| 14:51 | -04 delivers again: 211 opens, 70 `tenant.delivery` spans. |

Another tenant (`321cab79`, possibly in another isolate) delivered normally the whole time. The -04 log pulls are a
read-only Workers Logs telemetry query filtered on `$workers.durableObjectId starts_with f6011e77`, one minute per query.

## Mechanism (code)

`OrderedOutlet` keeps `credits = { normal, recovery, count, running, hosts }` and the `ready` set at module scope:
"Shared by EVERY outlet/tenant in this module's Worker isolate". `startOne` takes a slot (`credits.running++`), and
only `run()`'s `finally` gives it back once the holder RPC settles. `schedule()` starts nothing while
`credits.running >= MAX_RPCS`.

When workerd resets an object but keeps its isolate, module state survives and the object's I/O context is
discarded. Promises from that context's RPCs never settle in JS, so their `finally` blocks never run. The new instance
gets a fresh engine and outlet but the same module-level `credits`, with `running` still at 8.

The engine's stall rule (`HOLDER_STALL_MS`, BUG-4b) cannot see this. Its ticker runs only while some holder is
deferred, and the items never even start an RPC. `claimTick` returns only byte credits, never RPC slots.

## Reproduction (workerd, real Durable Objects)

`cloudflare/kernel/gateway/test/outlet-reset.spec.ts` with `OutletSender` and `OutletHolder` in the gateway test
Worker. The sender owns a real `GatewayOutlet`; the holder keeps delivery RPCs in flight.

1. The sender `fill(8)`: 8 RPCs in flight, `running` 0 -> 8.
2. `ctx.abort()` resets the sender. The holder then answers all 8 RPCs.
3. The same object comes back and sends one payload.

Before the fix: `timeout`, with `deliveryPressure()` = `{ running: 8, count: 9, hosts: 9 }` after the holder answered
everything. That matches the dev incident. The second test (another tenant, with the reset object never coming back)
also fails: no RPC can start.

After the fix: both pass.

```
before:  × a tenant reset with every isolate RPC slot in flight does not wedge its next instance or other tenants
         AssertionError: expected 'timeout' to be 'delivered'
         × a reset object that never comes back holds the isolate's RPC slots only up to the stale bound
after:   ✓ both (kernel/gateway 93/93, root 589/589 incl. kernel/delivery, kernel/assembly 141/141)
```

The #550 review added accounting coverage (`outlet-reset.spec.ts` test 3, `outlet-settle.spec.ts`). Each mutation
below fails at least one test:

| Mutation | Fails |
| --- | --- |
| Stale rule releases only in-flight credits (the first head's behavior) | dead object's queued credits; live stale-then-settle |
| `credits.running--` unconditional in `run`'s `finally` | live stale-then-settle (`running` goes to -1) |
| `release` not idempotent | live stale-then-settle; retired late settlement |
| No retired guard in `finally` | retired late batch refusal (resends `b`, `c`) |

Removing the recount survives these tests: releasing the queued credits at stale time already restores the totals,
and the recount is defense in depth against drift and for outlets the GC has collected.

No separate dev Worker was needed. The shared dev Worker was not touched (no deploys, read-only log queries).

## Fix

The fix is in `outlet.ts` and is bookkeeping only. Nothing changes what a live holder receives or its order.

1. **Retire the reset instance's outlet (deterministic).** `GatewayOutlet` takes the owning object's id
   (`ctx.id.toString()`, passed by `ReplicaTenantObject`). A Durable Object id has at most one live instance, so a
   new outlet for an id that already has one in this isolate means the earlier instance is gone. Its slots, byte
   credits, host entries and claim are returned at once, and it leaves the scheduler. If one of its RPCs did settle
   late, the slot is not returned twice (`slotReturned`, idempotent `release`). This covers the incident, where the
   same tenant came back 11 s later in the same isolate.
2. **Stale slots (bounded, other tenants).** An RPC that has held a slot for `RPC_SLOT_STALE_MS` (60 s) stops counting
   against `MAX_RPCS`. The credits of its batch, and of every payload queued behind it on that holder, are returned:
   none of them can move until that RPC settles. The isolate's `normal`/`count`/`recovery`/`hosts` totals are then
   recounted from the unreleased payloads of live outlets (`WeakRef`s, as `sweepOwners` does for `hosts`). This covers
   a reset object that never comes back to this isolate: its RPC slots and its queued byte and item credits come back
   within 60 s. Its stuck queues still count in `hosts` until GC collects the outlet, and they are bounded by
   `MAX_HOSTS` with the existing sweep. Its holder's ordered channel is unchanged: that holder still waits for the RPC.
   **Bound when the RPC was only slow** and settles later: the payloads that were queued behind it at that moment go
   out uncounted. That is at most one holder backlog (`HOST_PENDING_BYTES` = 8 MiB, or one legal singleton) per stale
   RPC. The isolate may run more than `MAX_RPCS` RPCs, at most one per holder, growing by at most 8 per 60 s.
   A late settlement of a **retired** outlet returns its slot at most once and returns early: it never requeues,
   rotates or re-enters the scheduler.
3. **Watchdog.** Each reclaim logs one warn line with counts only (no ids):
   - `delivery: outlet of a reset object retired { slots, running, count }`
   - `delivery: stale RPC slots returned { slots, running, count }`

   Each reclaim is also counted in `deliveryPressure()` (`retiredOutlets`, `retiredSlots`, `staleSlots`). `schedule()`
   sweeps only once the oldest held slot's deadline has passed (one comparison per call otherwise), so a delivery RPC
   stuck for more than 60 s shows up even below the cap. `byObject` drops dead entries in `sweepOwners` and whenever
   it reaches 1,024 ids.

Runtime bundle (`wrangler deploy --dry-run -c kernel/assembly/runtime.wrangler.jsonc`): 1109.30 -> 1114.39 KiB
(gzip 276.58 -> 277.92 KiB). No SQL, no rows read or written, no new runtime imports.

## Ruled out

| Candidate | Why not |
| --- | --- |
| Engine `serial` / admission chain left pending | Opens kept running after the reset (22 `tenant.open` spans 14:31-14:50), so the serial queue moved. |
| Paused, fenced or deferred holder that never recovers | The opens ran: `nextOpen` skips paused and deferred holders, so the holder was neither. `queuedAfter` grew because the outlet had items no RPC ever started. |
| Attach check (#543) or member read throwing | No `delivery.attach.refused` and no `could not read the session Member` line. Gateway `tenant` auth succeeded and the opens were admitted. |
| Output gate / `blockConcurrencyWhile` | Every invocation from 14:31 to 14:50 ended `ok` with no broken-gate errors, and calls answered in 1-8 s wall at about 1 ms CPU. |
| Alarm stopped re-arming | Two alarms ran 14:31-14:50, and the outlet path does not depend on alarms. |
| Gateway side (receive, batch apply) | The holders logged no `gateway.receive`, which is logged at the start of every apply. The RPCs were never sent. |
| The 14:28 deploy itself | -04 woke and served normally from 14:29. The wedge starts at the 14:30:41 CPU reset, and the identical 14:50 redeploy cleared it. |

## Production risk

Production users today are not affected: staging and production are served by the Rust runtime, and the Cloudflare
port runs only on dev. Without this fix, the Cloudflare port would be exposed in production:

- **Trigger is reachable under ordinary load.** `TENANT_CPU_LIMIT_MS = 5000` is enforced for every runtime deploy
  (`kernel/assembly/deploy.mjs`). Dev logged CPU-limit resets on 5 tenant objects from 2026-11-17 to 2026-11-20
  (36 lines). Any platform reset that keeps the isolate counts, not only CPU: a broken output gate, or "storage is no
  longer accessible".
- **A reset only wedges when RPCs are in flight.** `321cab79` was reset at 2026-11-19 18:45 and delivered normally
  afterwards. But in-flight RPCs are most likely during load bursts, which is also when CPU overruns happen.
- **The blast radius is the isolate,** not one tenant: every tenant object sharing it stops delivering until the
  isolate is recycled. Subscriber keep-alive and reconnecting clients keep the isolate busy, so it does not recycle
  on its own.
- **A partial leak** (1-7 slots) is silent: the isolate just has fewer delivery RPC slots. I did not check whether
  this explains any earlier dev latency anomaly.

With the fix, a returning tenant recovers at once, and other tenants of the isolate recover within 60 s.

## What is not fixed (follow-ups)

- **The CPU overrun itself.** Only an inference, not proven: the single invocation that used 7,500 ms CPU is most
  likely the delivery engine's `run()` loop. `kick()` starts it from whichever frame first finds it idle, and it keeps
  going while other invocations add work. Its continuations run in, and are charged to, that invocation's context.
  Snapshot-cache hits and small opens never yield, because SQL is synchronous and the outlet sends are not awaited.
  Under a burst of concurrent cold loads, one invocation can exceed `cpu_ms`. A CPU budget per loop invocation (stop,
  then resume from an alarm or self-RPC, as X10 did for migration alarms and transfers) would remove this trigger.
  Owner: the perf loop (X17).
- **A delivery RPC that never settles in a live instance** (a holder that never answers) still blocks that one
  holder's ordered channel. The engine's stall reset cannot help: its recovery barrier queues behind the same RPC.
  This is not observed. The stale-slot warn line now makes it visible.
- **`hosts` entries** of a never-returning object's stuck queues stay counted until GC collects its outlet (recount
  and `sweepOwners` use live `WeakRef`s). `AuthorityAdmission` uses the same pattern.

## Monitoring that would catch a recurrence

1. **Workers Logs alert** on `delivery: outlet of a reset object retired` or `delivery: stale RPC slots returned`
   (runtime service). Either means a reclaim happened; repeats on one deployment mean resets during load.
2. **Workers Logs alert** on `Durable Object exceeded its CPU time limit and was reset` for `AssemblyTenant`. It is
   the trigger, and frequent on dev today.
3. **Liveness probe (synthetic):** for a test tenant, the time from a socket's first `replica.open` to its first
   replica frame. Alert when it exceeds 30 s. This catches any delivery wedge whatever the cause, including ones that
   log nothing, as this one did.
4. **Dev evidence:** `deliveryPressure()` (isolate totals) belongs in the tenant's dev stats endpoint, next to
   `deliveryStats`. A `running` value pinned at 8 while `count > 0` is this signature.
