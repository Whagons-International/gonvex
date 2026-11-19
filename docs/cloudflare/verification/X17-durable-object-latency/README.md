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
