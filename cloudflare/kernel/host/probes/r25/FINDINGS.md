# R25 findings: a CPU-runaway Reducer on the platform

Question: 0012 R25 / 0016 G0-11. Probe: `worker.js`, driver `run.mjs` (see `README.md`). Runs on 2026-11-11:
platform `2026-11-11T08-14-35` (three CPU variants) and `2026-11-11T08-27-44` (follow-up), local
`2026-11-11T08-09-30` (wrangler 4.155.0, workerd 1.20261023.1, compatibility_date 2026-10-23). Raw JSON is in
`.results/`, which git ignores.

**Setup.** The runaway Reducer runs `for(;;){}` inside `ctx.storage.transaction(async …)` after inserting one row.
It is called by Worker → tenant RPC. During the overrun the driver keeps this load:

- 10 sockets held by the tenant (`ctx.acceptWebSocket`), each pinging every 500 ms.
- 10 sockets on 2 gateway objects. Each alternates a gateway-local ping with a tenant `info()` call through the
  gateway.
- 4–8 sibling tenant objects with 1 socket each.
- Queued Worker → tenant calls at +300, +400 and +5,000 ms, and one gateway → tenant `callTenant` at +500 ms.

**Labels.** *Measured* means taken from the raw results. *Inferred* is marked as such.

## Measured numbers (platform, development account)

| # | What | Default (30 s) | `limits.cpu_ms` 10000 | `limits.cpu_ms` 2000 | Local workerd |
|---|---|---|---|---|---|
| 4, 7 | Runaway call: wall time until the caller sees the error | **36.4 s, 33.6 s** (2 runs) | **14.2 s** | **5.3 s** (retries: 5.2–5.8 s) | No limit. Hung until wrangler reloaded workerd at ~249 s. |
| 4 | Runaway error seen by the caller (Worker RPC) | `Durable Object exceeded its CPU time limit and was reset.` with `overloaded: true`, `durableObjectReset: true`, `retryable` and `remote` unset | same | same | — |
| 7 | Does `limits.cpu_ms` apply to Durable Objects? | — | **Yes** | **Yes** | No CPU limit locally |
| 1 | Tenant-held sockets closed | 10/10 at **+7.0–7.5 s** (before the reset), **1013 `"Service overloaded; retry later."`** | 10/10 at +7.0 s, 1013, same reason | 10/10 at **+5.26 s, at the reset**, **1006 `""`** | 10/10 at 249 s, 1006 |
| 1 | Pongs on tenant sockets during the overrun | 0 after t0 | 0 | 0 | 0 |
| 1 | New tenant socket opened *during* the overrun | 0/2 attempts opened (each waited 15 s with no answer) | not run | not run | — |
| 1 | Reconnect after the overrun ends: open + first pong | 90–207 ms, first attempt (10/10) | 99–139 ms | 106–157 ms | 122–153 ms |
| 2 | Gateway-held sockets closed | **0/10** (2 runs) | **0/10** | **0/10** | 10/10, 1006 (same isolate) |
| 2 | Gateway-local pings during the overrun | Kept answering; max gap 1.07 s / 1.22 s | 1.09 s | 1.04 s | Max gap 8.6 s (busy-loop run) |
| 2 | Gateway → tenant RPC in flight at the start | Waited 35.8 s, then `Durable Object reset because its code was updated.` (both runs) | 13.6 s, then `Durable Object exceeded its CPU time limit and was reset.` | 4.8 s, same text | — |
| 2 | Gateway → tenant RPCs made during the overrun | From +6.5 s each failed within ≤ 105 ms: `Durable Object is overloaded. Requests queued for too long.` (305 and 276 calls) | Same from +6.5 s (102 calls) | No shedding (overrun shorter than 6.5 s) | — |
| 3 | Queued calls to the tenant (+300 ms, +400 ms, +5 s) | All blocked for the whole overrun, then failed with `Durable Object reset because its code was updated.` (sic, both runs) | Blocked 13.9 s, then `…exceeded its CPU time limit and was reset.` | Blocked to the reset, same text | All blocked |
| 5 | Constructor count before → after; in-memory instance id | 1 → 2; id changed | 1 → 2; changed | 1 → 2; changed | 1 → 2; changed |
| 5 | Committed rows (50 seeded) / runaway's partial row | **50 intact / 0 (rolled back)** | 50 / 0 | 50 / 0 | 50 / 0 |
| 6 | Isolate ids (module-level random) | Every object had its own isolate id: 8 siblings, 2 gateways and the Worker, in each of 4 runs. **Colocation not observed**; siblings were unaffected (max pong gap 0.6 s, instance ids unchanged). | same | same | **One isolate for everything.** The runaway froze the Worker, gateways and siblings, and the eventual reload reset all of them (1006). |
| 8 | `Date.now()` / `performance.now()` across 3×10⁸ loop iterations (1.4 s locally), a 3×10⁶-row recursive CTE, 2,001 `sql.exec` calls, a microtask and a nested `transaction()` | **+0 ms on all of them.** They first moved after `await setTimeout(0)`: +2,162 ms. | — | — | Advance normally (1,369 ms and 495 ms) |

Wall time runs about 3–6 s past the configured CPU limit: 2 s → 5.3 s, 10 s → 14.2 s, 30 s → 33.6–36.4 s. On these
runs, an input gate held longer than about **6.5–7 s** causes these effects whether or not the object resets:

- New calls are shed with `Durable Object is overloaded. Requests queued for too long.`
- Tenant-held sockets that send traffic are closed with **1013**.
- The 1013 also happened in the Worker Loader no-limit run below, where the tenant was **not** reset.
- Idle sockets were not tested.

## Per-call guards (question 9)

| Candidate | Measured | Verdict |
|---|---|---|
| (a) Clock check at host-call boundaries | `Date.now()` stayed frozen through 2×10⁹ iterations and 20,000 host calls (`sql.exec`), about 14 s of CPU; the 1 s clock budget never fired. | **Does not work on the platform.** It works locally, so local tests would give false confidence. |
| (a) Counter check at host-call boundaries | 1,001 host calls after 10⁸ iterations threw `budget_exceeded`. The transaction rolled back (partial 0), the object was not reset, and the client saw 1.0 s. | Works, but only bounds code that makes host calls. A pure `for(;;){}` never reaches the check. |
| `limits.cpu_ms` (Worker-wide backstop) | Applies to the DO: 2000 → 5.3 s, 10000 → 14.2 s. The object still resets and tenant-held sockets still drop (1006 at the reset when it comes before 7 s, otherwise 1013 at 7 s). | Bounds the blast duration only. It is per event for every handler of the script, so it must sit above the largest legitimate event (wake, migrations, `pragma`-heavy work). |
| (b) Worker Loader (dynamic isolate), `limits: { cpuMs: 1000 }` | **The binding is available** on the account. The runaway failed after 3.2 s with `Error: Worker exceeded CPU time limit.` (surfaced as our `budget_exceeded: …`, `remote: true`). **No reset**: constructor 2 → 2, rows intact, 0 partial. Tenant sockets stayed open (gate held 3.1 s, max pong gap 3.1 s), and queued calls succeeded after 3.2 s. The next loader call worked. | Is a real per-call CPU guard, but see the blocker below. |
| (b) Worker Loader without `cpuMs` | Dynamic-isolate default ≈ 30 s CPU: error after 35.5–36.4 s with the same text. The tenant's own 5 s `setTimeout` wall timer **did not fire** during the spin. Tenant not reset (constructor 1 → 1), but its sockets were closed **1013 at +7 s** and no new socket opened during it (0/3). | Always set `cpuMs`. *Inferred*: the dynamic isolate shares the tenant's thread, because the tenant's timer starved while it spun. |
| (b) Cost | Worker-measured round trip per Reducer, n = 30: in-object p50 38–39 ms, p90 43–45. Via loader p50 40–41 ms, p90 51–52. First (cold) call 57–64 ms vs 40 ms. In-object timing cannot resolve it (the clock is frozen, 0 ms). | About **+1–2 ms p50, +7 ms p90, +20 ms cold**, for calls with **no host calls**. |
| (b) **Blocker: host calls back into the tenant** | Module code in the loader reaches tenant SQL through an `RpcTarget` passed from the tenant. With the tenant inside `ctx.storage.transaction` (input gate held), **0 of 1, 2 or 5** host calls completed on the platform. All hit the 3 s wall timeout (locally the first call got through, the second hung). Without a transaction, 1/2/5 calls completed (46–91 ms total, including cold start). Under `blockConcurrencyWhile` the wall timeout reset the object (local). | **Not usable with K04's one-transaction-per-Reducer design.** Callbacks into an object that holds its input gate deadlock. No measured way around it. |
| (c) Retry storm, no marker | Same command id retried: 2 attempts → **2 resets** (constructor 1 → 3), 5.8 s + 5.2 s at `cpu_ms` 2000. | Every retry resets the tenant again. |
| (c) Caller-side quarantine | The Worker saw the overrun error, called `markPoison(cmd)` on a fresh stub (worked straight after the reset), and the tenant refuses poisoned ids. 3 attempts → **1 reset**. Retries failed in **76–104 ms** with `budget_exceeded: command poisoned-1 is quarantined (caller saw overrun)`. | Works. The happy path costs one indexed lookup (0 ms measurable). It depends on the caller surviving to record the mark, and queued callers can see the misleading `…reset because its code was updated.` text. |
| (c) Durable in-flight marker | The tenant wrote `inflight(cmd)` and awaited `ctx.storage.sync()` before the transaction, then deleted it afterwards. Attempt 2 found the stale marker and quarantined the id. 3 attempts → **1 reset**, retries refused in 41–149 ms. | Works without trusting the caller, but `storage.sync()` costs **27–28 ms on every Reducer** (p50 28, n = 20). |

## Recommendation for K04

1. **Accept that a runaway Reducer resets the tenant**, and keep its blast radius small. Measured:
   - Committed data survives and the runaway's writes roll back.
   - No other object was affected: no colocation was observed. That is not proven for colocated objects.
   - Gateway sockets (G0-1) were never dropped. Only calls in flight failed, and they map to transient errors.
   - Tenant-held sockets are dropped (1013 at about 7 s, or 1006 at the reset). Clients were back in about 100–200 ms after the
     overrun ended, but could not connect during it.
2. **Map the errors.** Treat these texts, received on the tenant stub, as `budget_exceeded` for the command that
   caused the overrun and as transient (`overloaded`) for everyone else:
   - `Durable Object exceeded its CPU time limit and was reset.`
   - `Durable Object reset because its code was updated.` (when no deploy happened)
   - `Durable Object is overloaded. Requests queued for too long.`

   `e.durableObjectReset === true` and `e.overloaded === true` are set on the first two. Keep the Rust error text toward
   the client.
3. **Guard inside the object with a host-call counter, not a clock.** The platform clock does not advance during
   synchronous work. Keep the existing 3 s and 10 s wall budgets only as `setTimeout`/`Promise.race` checks across real
   awaits.
4. **Set `limits.cpu_ms` as the backstop** (measured to apply to DOs). It should sit as low as the largest
   legitimate event allows, ideally under about 4 s so the overrun ends before the about 7 s shed point (2000 gave a
   5.3 s outage). K04 must first measure its heaviest legitimate event (wake, catalog build).
5. **Quarantine the command at the caller** (gateway or Worker): on an overrun error, call `markPoison(commandId,
   path)` on a fresh stub. The tenant answers `budget_exceeded` for that id without running it. Measured: 1 reset
   instead of one per retry, and no happy-path cost. Use the durable in-flight marker only if callers cannot be
   trusted to record the mark, because it costs about 28 ms per Reducer.
6. **Worker Loader with `limits.cpuMs`** is the only true per-call CPU guard found: no reset, sockets kept, about 1–2 ms
   p50. It cannot be adopted until module host calls work while the tenant holds its transaction, and here they
   deadlocked. Hand this to S5 (Worker Loader spike) as an open question: Reducer execution outside the input gate with
   an in-object Reducer queue would be needed. That is a design change, not measured here.
