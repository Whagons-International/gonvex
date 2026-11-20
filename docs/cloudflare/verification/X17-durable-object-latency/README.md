# X17: Durable Object latency that sets a floor under the cold workspace load

Question (X17 brief, step 5): which part of the remaining CF-vs-Rust cold-load gap is an irreducible Cloudflare
platform cost, and which part is ours? This note gives the platform numbers from a minimal repro, the two platform
semantics that were costing us without our knowing (and are fixed in #525), and the floor they set.

All numbers are medians, measured from arch3 on 2026-11-19 against the dev account. The repro Worker is
`gonvex-cf-dev-x17-repro` ([`worker.mjs`](worker.mjs), [`wrangler.jsonc`](wrangler.jsonc)). It binds the dev
Worker's controller and Control classes for read-only probes only (`drainReady`, `moduleInfo`, `sessionPublicTenant`).
Clients: [`step.mjs`](step.mjs) (one Gonvex-shaped client step over a WebSocket) and [`rtt.mjs`](rtt.mjs) (WebSocket
round trips).

## 1. Platform semantics (both measured; both applied in #525)

| Finding | Evidence (repro endpoint, colo) | Consequence for Gonvex |
|---|---|---|
| **The output gate covers the whole object.** While one request has a confirmed write in flight, every other reply of the same object waits for it. | `/gate`: read RPC alone 8 ms; while another request writes in a loop 57 ms; while it writes `allowUnconfirmed` 14 ms (SJC). A second object: 16 / 26 / 16 ms. Again at 15:45Z SJC: 8 / 55 / 11 ms. | The single global Control object took telemetry inserts and error-session upserts during every sign-in, which delayed every concurrent `auth`. #525 moved performance telemetry off the hot path and stopped notifying gateways that have no `control.support.*` readers. |
| **`allowUnconfirmed` is ignored inside `transactionSync`.** The transaction commits as a confirmed write. | `/txgate`: two `allowUnconfirmed` puts inside `transactionSync`: the call itself 52 / 32 ms (DFW), 14 ms (SJC), and a concurrent read on the same object as long. The same two puts outside a transaction: 5 / 5 ms (DFW), 2 ms (SJC). A confirmed `kv.put`: 50 / 35 ms (DFW), 15 ms (SJC). | #523's holder-recovery writes ("unconfirmed") still gated every gateway frame and delivery receipt: gateway delivery invocations took 52–93 ms wall at 8–29 ms CPU. #525 issues them outside a transaction, still atomic through the platform's write coalescing. Tenant → gateway delivery RPC median: 78 → 46 ms. |

## 2. Platform latencies

| Cost | SJC (normal routing) | Notes |
|---|---|---|
| ICMP to the anycast edge | 9 ms | `ping` |
| TCP connect / TLS handshake | 12 / 21 ms | `curl` timings, `/healthz` |
| Client ↔ a fresh DO, one WebSocket frame | 12–14 ms | `step.mjs` "direct" |
| Client ↔ the dev gateway DO, one frame | 21–25 ms | `rtt.mjs`: unauthenticated `query.call` → `query.error`, no I/O; the ping auto-response, answered at the DO's host, gives the same number |
| One frame that the DO answers after an RPC to a second DO (gateway → tenant) | 38–39 ms (+25) | `step.mjs` "hop" |
| One frame answered only when the second DO calls the first back (the Gonvex step shape: dispatch, then delivery) | 47–51 ms (+35) | `step.mjs` "deliver" |
| Worker → DO read RPC / with a confirmed write | 4–14 / 17–26 ms | `/` |
| DO → DO read / with a confirmed write | 16–27 / 44–59 ms | `/` (`doToDo`) |
| `setAlarm(now)` → `alarm()` | 40 ms (79 ms under DFW routing) | `/alarm`. X02 CallOrigin: a gateway call made while a peer RPC is the newest request waits for this wake. |
| Confirmed write latency, per object | 27 vs 65 ms (`release`, two controller cells) | `/acquire`. The same call on two objects differs by 2.4x. |
| Dev objects' distance from SJC | controllers 6–19 ms, Control 15 ms, tenants 4–16 ms | `/cells`, `/control`, `/tenant`. Placement is not the problem. |

**Edge routing is not stable.** From about 15:02Z to 15:16Z, arch3's HTTP and WebSocket traffic to the dev Worker
was served by DFW, intermittently, while ICMP to the anycast address stayed at 9 ms
([`colo-2026-11-19.log`](colo-2026-11-19.log): 6 of 12 one-minute samples DFW). Under DFW, client ↔ gateway DO was
80–96 ms and a raw socket open took 841–1044 ms. No CF measurement in b27 ran under DFW: each window checked the
colo, and the router's `/ws` logs show SJC.

## 3. The floor for a cold workspace load

The Rust reference does a client step in-process (Rust_RTT adds 10 ms per round trip). On Cloudflare, every dependent
client step that reads tenant data is client → gateway DO → tenant DO → (delivery RPC) → gateway DO → client.

- **Per step:** at least ~35–40 ms more than Rust_RTT, even for fresh, co-located objects with no write in the path
  (`step.mjs`: 47–51 ms vs 12–14 ms direct; Rust_RTT about 10 ms).
- **Steps:** a cold load of -11 or -04 has 10–13 such dependent steps before task rows. These are the client "waves"
  in the b27 timelines: auth, exchange, auths, then the frontend's successive `replica.openMany` batches.
- **Before the first step:** the connect pays TLS (21 ms) plus the router → controller → limiter → gateway chain (3–4
  DO hops). The sign-in exchange makes one Firebase `accounts:lookup` (100–240 ms), which the sanitized Rust reference
  skips (its Firebase admin credentials are nulled, b26), plus Control → tenant admits and one confirmed session write.

That sums to roughly **+600–900 ms over Rust_RTT** before any CPU and before any write that must be durable (tenant
reducer commits, the exchange's session row, a new gateway slot). The readiness budget is +300 ms. So cold workspace
load cannot pass the matched-RTT rule with one Durable Object hop pair per client step. Closing it would take an
architecture change: for example, the socket held by the tenant object itself, or the gateway answering from a
replica it holds.

What is not a platform floor, and remains ours:
- The rest of the measured per-step cost: ~85–100 ms per small `openMany` on dev against the ~35–50 ms floor.
- Tenant CPU for large open cohorts: about 520 ms for the -04 55-open cohort, against ~40 ms for the same phase on
  Rust, which also runs the queries in V8.
- The frontend's ordering on -12 (b27).

## 4. Where the gap stands after b28 (2026-11-20), and what would close it

### b28 and the cuts since
Batch b28 measured integration `bcb23689` (snapshot cache #528, exchange admission cache #530, Control notices #532,
one-hop connect #533/#535) against Rust_RTT in the same hours (report:
`/home/gabriel/work/gonvex-c3-walkthrough-20261117/batches/b28.md`). Expected savings of the cuts merged or in review
since b28, from profiled -12 loads (`/home/gabriel/work/gonvex-x17/logs/`):

| Cut | What it removes | Expected saving (cold load) |
|---|---|---|
| #537 exchange admits during `accounts:lookup` | the lookup and the tenant admissions no longer run back to back | ~60–110 ms |
| #538 colo-shared warm holder | socket 2's controller `acquire`: router 233 → 120–132 ms (x18k profile) | ~100 ms |
| #539 identity leases (≤10 s, pushed revocation) | socket 1's repeated control-only auth (66 ms → ~0) | ~60 ms |
| #540 auth reuses the sign-in's admission | socket 2's tenant auth: one Control → tenant round trip | ~30 ms |

None of these touch the realtime-create path.

### Projection per failing cell (ms; gap = CF median − (Rust_RTT + 300))

| Cell | b28 CF | Rust_RTT + 300 | b28 gap | Expected saving | Projected gap | Closes? |
|---|---|---|---|---|---|---|
| load -11 cold | 4054 | 3032 | +1022 | ~250–300 | ~+720 | no |
| load -04 cold | 4524 | 3678 | +846 | ~250–300 | ~+550 | no |
| load -12 cold | 4531 | 3564 | +967 | ~250–300 | ~+670 | no |
| create -11 cold | 3253 | 2294 | +959 | 0 | +959 | no |
| create -11 warm | 2410 | 2203 | +207 | 0 | +207 | no |
| create -04 warm | 2690 | 2656 | +34 | 0 | +34 | within noise, not reliably |
| create -12 cold | 2771 | 2736 | +35 | 0 | +35 | within noise, not reliably |
| create -12 warm | 2469 | 2150 | +319 | 0 | +319 | no |

Warm loads pass on every tenant (-12 warm included) and -04 cold create passes.

### What the remaining gap is made of
- **Cold load: the per-step Durable Object hop floor.** A cold load is about 13 client-sequenced steps (the frontend
  sends each after the previous answer; Rust's timeline shows the same client idle gaps). On Cloudflare each step is
  at least two one-way DO legs: 40–100 ms per answer on dev against 1–31 ms on Rust_0 (+~20 ms for Rust_RTT).
  CF reaches the big `openMany` at 2347 ms after sign-in, Rust at 1403 (+944, x18h cache-hit profile). With the
  snapshot cache, the cohort itself is roughly at par (first open → last ready ~1.3 s CF vs ~1.2 s Rust).
- **Not a stall we can fix.** Tested and ruled out on the platform (`/bp`, `/persist`, `/seq`, this directory):
  5 MB queued to a socket does not slow later RPCs to that object (11–18 ms); `serializeAttachment` and unconfirmed
  puts do not hold replies; back-to-back 95 KB delivery RPCs to a socket-holding object take 15–28 ms; deflate is
  negotiated. An upper-bound A/B with every X02 deferral removed (dev-only) did not make the -12 cold load faster
  (median ~5.35 s vs ~4.74 s baseline, n=4 each, SJC), so an inline `openMany` reply was not built.
- **Create.** Most of `assistant.rewriteText` is the external LLM call (openrouter.ai, 1.17–1.43 s) on both
  backends; CF adds ~150–250 ms for the Action's two tool round trips (ActionHost → tenant) and 174–433 ms (vs
  111–172) from the rewrite to the `tasks.create` result: dispatch, the tenant commit's durability wait (43–52 ms,
  Rust waits for its commit too) and the delivery back.

### Options that would remove the floor (decision for Gabriel)

| Option | What it changes | Expected effect | Rough cost | Risk |
|---|---|---|---|---|
| A. The tenant DO holds the client's tenant socket | Every tenant step is client ↔ tenant DO, in-process like Rust (no gateway hop, no delivery RPC). The router needs the tenant at upgrade time: today it only arrives in the first `auth` frame, so this needs the tenant in the socket URL (frontend change) or a redirect/reconnect after auth. Recovery, presence fan-out, the outlet and capacity move into the tenant. | Removes ~2 legs per tenant step: the bulk of the cold-load gap (est. −400 to −700 ms) and the create delivery hop | Several weeks; protocol and frontend change | High: one single-threaded object per tenant carries every socket and all its CPU; recovery and capacity are redesigned |
| B. The gateway answers tenant reads from a replica it holds | `openMany` served at the gateway from deliveries it already receives | Removes the tenant leg for opens (~6 steps) | Weeks | High: visibility/ACL evaluation and consistency at the gateway |
| C. Gateway caches Control reads (`tenants.mine`, `developer.status`, `publicSettings`) with notify invalidation, like the identity lease | Removes 2 legs for ~3–4 Control steps | est. −100 to −150 ms | Days | Medium: bounded staleness (same model as #539) |
| D. Frontend: fewer sequential waves (send the cohort's opens in fewer frames, earlier) | Removes whole steps; each saves ~45–55 ms on CF | est. −200 to −400 ms | Days, but the frontend (678bdc80) is frozen for this rule | Low technically; changes the measured client |
| E. Re-baseline cold load | Accept the platform floor in the rule (e.g. Rust_RTT + 300 ms + measured floor) | Closes the cells by definition | None | Product decision |

Recommendation: C is the remaining in-scope cut and is worth doing; it will not close cold load alone. Closing cold
load under the current rule needs A (or D with a frontend change). Create cells within noise (-04 warm, -12 cold)
need a larger n to settle; -11 cold create is dominated by the external LLM call's variance.

### Reproduce (additions)
```
U=https://gonvex-cf-dev-x17-repro.whagons-international.workers.dev
node bp-client.mjs 20 262144 1      # 5 MB queued to a socket, then ping latency to that object
node persist-client.mjs             # serializeAttachment / unconfirmed put / confirmed put reply latency
node seq-client.mjs                 # back-to-back delivery-shaped RPCs to a socket-holding object
curl "$U/cache?put=x"; curl "$U/cache"   # colo Cache API persistence (X17i)
```

## Reproduce

```
cd docs/cloudflare/verification/X17-durable-object-latency
whagons-dev credentials exec full-cloudflare-access -- bash -c 'CLOUDFLARE_API_TOKEN=$(jq -r .apiToken "$WHAGONS_CREDENTIAL_FILE") CLOUDFLARE_ACCOUNT_ID=$(jq -r .accountId "$WHAGONS_CREDENTIAL_FILE") npx wrangler deploy'
U=https://gonvex-cf-dev-x17-repro.whagons-international.workers.dev
curl "$U/gate?n=20"; curl "$U/txgate?n=15"; curl "$U/alarm?n=12"; curl "$U/?n=20&o=a&o2=b"
node step.mjs "" 25 run1      # direct / hop / deliver medians
node rtt.mjs                  # ping vs gateway frame on the dev Worker
curl -s "$U/cdn-cgi/trace" | grep colo   # record the serving colo with every run
```
