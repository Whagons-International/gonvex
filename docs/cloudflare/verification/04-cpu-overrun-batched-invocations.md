# What used 7,500 ms CPU on -04 (dev, 2026-11-20 14:30:41Z), and why `cpu_ms = 5000` is reachable

Follow-up to `04-hang-delivery-slots-after-reset.md` (PR #550), reviewer finding 4. The CPU-limit reset that started
the -04 hang happened on the regular build (`ca4a2b89`, after the 14:28 rollback), so production would see it too.

## Verdict

There was **no single long loop.** The 7,500 ms belonged to one Durable Object *invocation* into which the platform
had batched many incoming calls while the tenant was saturated.

On Cloudflare, calls that queue at a busy object are delivered together as one invocation with **one `cpu_ms`
budget**. While the object stays saturated, that invocation keeps absorbing calls. During the burst of concurrent
cold loads after the 14:28 deploy, the tenant's backlog of work exceeded 5 s of CPU, and the platform reset the object.

The #550 doc guessed that the delivery engine's `run()` loop was charged to the invocation that kicked it. That guess
is wrong. Locally, loop chains under the same burst last at most 440 ms, and on Cloudflare a self-RPC handoff is
batched into the same invocation (below). Splitting or yielding engine loops would not prevent the reset.

## Evidence

### The incident invocation (Workers Logs, read-only)

| Field | Value |
| --- | --- |
| Outcome | `jsrpc`, `AssemblyTenant`, `exceededCpu` |
| CPU / wall | `cpuTimeMs 7500`, `wallTimeMs 7811`: 96% busy for 7.8 s |
| Calls recorded | `rpcCallCount 0`: killed before its calls were recorded, so no method name |
| Caller | its trace (`75cfba5f…`) leads to a gateway alarm flushing deferred calls (`gateway.deferred`) |
| The object over the same 8 s | about 11 s of CPU across all invocations; 5+ clients loading at once with an empty snapshot cache; `tasks.recentWorkspaceTasks` reading about 3,300 SQL rows per open |

### Platform behavior, measured on a throwaway Worker

Worker `gonvex-cf-dev-cpuprobe-20261120`: deployed, measured and deleted 2026-11-20 16:47–17:12Z. Source in
`cloudflare/kernel/assembly/verify/cpu-batching-probe/`. One SQLite Durable Object with `limits.cpu_ms = 1000`.
"Call" means a Workers RPC call to the object. CPU is per invocation, from Workers Logs.

| Run | Shape | Result |
| --- | --- | --- |
| E1 | 40 concurrent independent calls (≈160 ms CPU each) from one caller | **Reset.** One invocation carried the calls and was killed at a reported 3,500 ms; 28/40 calls answered. |
| io | 40 separate callers (40 fetches); each call chains a step on a shared promise queue (burn, then a 2 ms timer) | **Reset.** Invocations of 33 and 7 calls; the 33-call one killed at 3,500 ms; 8 reset lines. |
| ioh | as io, but the queue continues in a self-RPC every 8 steps | Not a fix. The self-RPC (`resume`) was batched into the same invocation (1,096 ms, 40 calls); the chain then stalled. |
| E2 | the same 40 calls as E1, one at a time with 20 ms idle gaps | No reset. 40 invocations of 152–252 ms. |
| E3 | one call starts about 3 s of timer-chained background work and returns at once | No reset. The call's invocation shows 294 ms; the rest is charged to later invocations as they happen. |
| E4 | the same work in alarm invocations, 3 steps per alarm | No reset. Each alarm has its own budget (436, 286, 195 ms). |
| E6 | 2 staggered callers keep calls overlapping for 10 s (10 ms CPU and 20 ms waiting per call; 6.3 s CPU in total) | No reset. 203 invocations, at most 2 calls and 171 ms each. |

What this shows:
- **The budget is per invocation**, and an invocation can hold many calls: those that queued at the object
  together (E1, io). Concurrency without saturation does not batch (E6), and idle gaps split (E2).
- **A saturated object accumulates.** When CPU demand exceeds supply, calls queue, the queued calls are delivered as
  one invocation, and its CPU is the backlog's CPU. The kill comes after the configured limit, at 3.5× `cpu_ms` in
  the probe and 1.5× (7,500 vs 5,000) on -04, so the exact threshold is not a stable platform contract.
- **A self-RPC is not a way out** (ioh): it is just another call to the same busy object. Alarms (E4) and work
  continuing after a call returned (E3) are charged to other invocations.

### Local cost of the burst (real -04 data, workerd)

A copy of X17's local harness on my own ports and state. N clients load at once with the frontend's 90 captured
opens each, against a freshly started (cold) assembly. Runtime-isolate CPU comes from a V8 profile; spans are read
over CDP. Local workerd enforces no `cpu_ms` (checked: a 2,000 ms burn under a 100 ms limit answers normally) and has
no per-invocation attribution. So this measures demand, not the reset.

| N | Load ready (ms after the opens) | Runtime CPU | Longest engine loop chain | Heaviest opens (sum over clients) |
| --- | --- | --- | --- | --- |
| 6 | 4,747–5,380 | 4.1 s | 404 ms (73 opens, 8,148 SQL rows) | `floatingChat.listActiveMessages` 351 ms, `analytics.listKpiCards` 294, `tasks.recentWorkspaceTasks` 242 |
| 10 | 6,497–8,996 | 9.7 s | 440 ms (130 opens, 2,399 rows) | `analytics.listKpiCards` 542 ms, `tasks.recentWorkspaceTasks` 468, `floatingChat.listActiveMessages` 317 |

The largest single units are live-query subscribes (`tenant.query`) at about 500 ms each.

Cold -04 demand grows roughly linearly in concurrent cold loads, at 0.7–1 s of CPU per client. That puts a burst of
5–7 concurrent cold loads at the 5 s budget, which matches the incident.

## Production risk

- **Rust today: none.** Staging and production run the Rust runtime.
- **Cloudflare port: real.** `TENANT_CPU_LIMIT_MS = 5000` is enforced on every runtime deploy
  (`kernel/assembly/deploy.mjs`, 0089). Ruling 0089 and X10 budget each *event* at ≤ 3.5 s. On the platform, an
  event is a batch of whatever queued while the object was saturated, so the budget actually limits **backlog**.
  - **Triggers:** a large tenant's login peak, a reconnect storm after a deploy (every client reloads while snapshot
    caches are cold), or one expensive reducer arriving while other calls are queued.
  - **Frequency:** dev logged CPU-limit resets on 5 tenant objects from 2026-11-17 to 2026-11-20, one of them about
    hourly for a day (`e52c716a`).
  - **Cost of each reset:** every call in the batch fails. The tenant's queued output is dropped and every holder
    goes through full recovery. Until #550, a reset with delivery RPCs in flight could also wedge the isolate.

## What would prevent it (decision for the lead)

Bounding engine loops does not help: they are short, and the budget covers the batch, not the loop. The options
that do help:

1. **Raise the tenant runtime's `cpu_ms`** (`TENANT_CPU_LIMIT_MS`, ruling 0089), e.g. to the platform default of
   30,000. This removes the trigger for bursts of up to about 30 s of backlog. Runaway protection would then rest on
   the in-code budgets X10 added (migration alarms and P03 transfer pages stop at a CPU budget) plus reducer limits.
   This is a one-line policy change, but 0089 is the lead's ruling to revisit.
2. **Bound the backlog the tenant accepts.** When open/subscribe work queued in the object exceeds a work budget,
   answer new opens with a retryable "busy" instead of queueing them. This keeps any batch under the limit. It needs
   a protocol-level retry signal and a check of how the client handles it (not verified here), so it is a design task.
3. **Cut per-load CPU** (X17's loop): live-query subscribes of about 500 ms, and the heavy open paths above. This
   helps whichever option is chosen, but on its own it only moves the threshold.

My recommendation is (1) now and (3) continuing. Take (2) only if keeping `cpu_ms` at 5,000 is required.

## How to reproduce

```sh
# Platform behavior (throwaway Worker, gonvex-cf-dev-* only; delete it afterwards)
cd cloudflare/kernel/assembly/verify/cpu-batching-probe
whagons-dev credentials exec full-cloudflare-access -- node run.mjs deploy
U=https://gonvex-cf-dev-cpuprobe-20261120.whagons-international.workers.dev
curl "$U/?mode=calibrate&n=40&ms=150&obj=e1"          # E1: reset ("exceeded its CPU time limit")
curl "$U/?mode=calibrate&n=40&ms=150&obj=e2&seq=20"   # E2: 40/40 ok
curl "$U/?mode=steady&ms=50&secs=10&obj=e6"           # E6: no errors
whagons-dev credentials exec full-cloudflare-access -- node run.mjs delete
```

Per-invocation CPU comes from Workers Logs (`$workers.cpuTimeMs`, `$workers.event.rpcMethods`, `outcome`).
