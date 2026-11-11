# S5: Worker Loader spike

**Question.** Can `/dev/sync` swap a module generation without dropping sockets? That is, can the app bundle be
loaded at runtime through Dynamic Workers (the Worker Loader binding) instead of being baked into the deployed
Worker, so that deploys keep `system.reload` semantics? Decides 0016 G0-2 for K19, and answers ruling 0022 (K04 R25).

**Verdict.** **Partly. The Worker Loader can switch a module generation without dropping a socket, but only
if the tenant's whole data path runs as a Durable Object Facet. Keep 0016 G0-2 for stage 1.**

- **Plain Worker Loader is out.** In this layout the app runs in a Dynamic Worker and every host call is an RPC back
  into the tenant object. On the platform the first call into an object holding its storage transaction completes;
  the **second deadlocks**, and the object resets after 30 s. Reducers make 1–14 host calls, so S2's
  one-transaction model cannot run this way. That also answers ruling 0022.
- **Facet layout works.** S2's host and the app bundle load together as a Durable Object Facet per generation, on the
  facet's own SQLite. On the platform it matched the baked build on all 39 S2 steps, with identical row counts and
  identical rows read. Generation switches kept every socket open: 1,000 tenant-held sockets through 5 switches of the
  real bundle, and 500 through 18 switches with a Reducer in flight. Every socket got `system.reload` on each switch.
- **What the facet layout costs:**
  - A **0.4–1.1 s generation load on every object wake**. No reuse across objects was observed, 22 of 22 times. The
    baked build starts in about 90 ms.
  - All tenant-data kernel code moves into the loaded generation.
  - A switch protocol (drain, `facets.abort`, broadcast) that **failed intermittently**. In about 100 switches: 2
    tenant-object resets and 1 facet stuck in `internal error` until its object restarted.
  - Dynamic Workers are in **open beta** (since 2026-03-24, no GA).
- **Not measured.** The runaway under `limits.cpuMs` in a facet (c), the switch with gateway-held sockets, the
  Worker-timed round trip and a split-script deploy were not run. `whagons-dev credentials` was down from about
  09:00Z to at least 10:15Z on 2026-11-11 (origin 521). The probes are written; see §11.

## Two layouts

| Layout | What runs where | Host calls |
|---|---|---|
| **Runner** | Tenant object (deployed): SQLite, sockets, S2's host. Dynamic Worker: S2's bootstrap and the app bundle. | Every host call is a Workers RPC from the dynamic isolate back into the tenant object (`RpcTarget` passed per invocation). |
| **Facet** | Tenant object (deployed): sockets, routing, generation pointer. A [Durable Object Facet](https://developers.cloudflare.com/dynamic-workers/usage/durable-object-facets/) loaded from the Dynamic Worker: S2's host and bootstrap **plus** the app bundle, on the facet's own SQLite. | Local synchronous `sql.exec` inside the facet, exactly S2's model. One RPC per invocation (tenant object → facet). |
| Baked (baseline) | The 0016 G0-2 design: the bundle is part of the deployed Worker version (S2). | Local synchronous SQL. |

All three use S2's kernel files unchanged (`bootstrap.js`, `host.js`, `sql.js`, `engines.js`, `runtime-globals.js`)
and S2's Query/Reducer split bundle (whagons5-client `2457bb65f`, 4,159,267 B, 1,519 exports). The generation's
code (bundle and function manifest) is stored in a `Bundles` object and fetched by the `LOADER.get(id, callback)`
callback. Generation ids are `<code>:<label>:<run>`, unique per code version, as the API requires.

## Numbers

Platform numbers are from the development account (colo SJC, client on arch3), 2026-11-11, wrangler 4.158.0,
compatibility date 2026-11-01. Client times subtract the measured round trip to the object unless marked
*Worker-timed*. Inside an object `Date.now()` does not advance during synchronous work (K04 R25), so in-object
timings only count waits.

| What | Runner layout | Facet layout | Baked (G0-2) |
|---|---|---|---|
| Deployed Worker upload | 182.7 KiB, 45.6 KiB gzip | same | 5,825.6 KiB, 1,038.7 KiB gzip |
| Worker startup time (wrangler) | 3–5 ms | same | 87–93 ms |
| S2 workload, 39 steps, 8 fresh tenants | not run: deadlocks (§1) | **39/39 outcomes identical to baked**, every table's row count identical | reference |
| Host call inside the Reducer's storage transaction | **deadlock from the 2nd call**, object reset at 30 s | local sync SQL | local sync SQL |
| Host call RPC round trip, outside a transaction | 0.1 ms (no-op), 0.2 ms (`SELECT 1`) | n/a | n/a |
| Cold generation load, real bundle (n = 6) | 378–884 ms, p50 774 ms (fetch from the store 79–323 ms, p50 130) | 529–671 ms, p50 654 ms to the first facet answer | paid in Worker startup (87–93 ms) |
| Cold load, empty generation (kernel only) | 24–45 ms, p50 43 ms | | |
| Warm (same object, same generation) | 0 ms in-object, 2–19 ms client | 2–12 ms client | — |
| Second object, same generation id | callback ran **22/22** times: no cross-object reuse observed | same | — |
| Process memory per extra loaded generation (local workerd RSS) | +38 to +42 MB (first: +86 MB) | +29 to +39 MB per facet generation | +76 MB for the bundle (S2) |
| Generation switch, 1,000 sockets on the tenant object, active | **0 closed**, 5/5 switches, 1,000/1,000 got `system.reload` each time | same object | all drop on deploy (S3: 1006, back in 4–6 s) |
| Preload of the new generation inside the switch | 679–1,030 ms (real bundle), 18–164 ms (empty) | | |

## 1. Runner layout: host calls over RPC deadlock inside the transaction (ruling 0022)

S2's design runs every Query and Reducer in one `ctx.storage.transaction(async …)`. In the Runner layout, module
code in the dynamic isolate makes each host call as an RPC on an `RpcTarget` the tenant passed in, while the tenant
holds that transaction.

| Probe (platform, fresh object, generation warmed outside the transaction) | Result |
|---|---|
| `hostLoop` no-op, n = 1, inside `transaction()` | completes, 30–35 ms client |
| n = 2, 3, 5 inside `transaction()` | **hang**; HTTP 500 after 30.0–30.3 s; the object is reset (`blockConcurrencyWhile` timeout) |
| Real path `s5.slow` through S2's dispatcher, n = 10, 100, 1,000, 3,000 host calls | all hang 30.0–30.4 s, then reset |
| Same with a concurrent request to the object | the concurrent request waits too; the long call resets; its row is lost with it |
| n = 300 outside any transaction | 0.1 ms per no-op, 0.2 ms per `SELECT 1` round trip (5 reps, tiny and real generations) |
| Local workerd 1.20261104.1 | hangs from the **first** call |

The `limits.cpuMs` variant for ruling 0022 (`test/txcalls.mjs`, n = 0/1/2/5 with `cpuMs` 1000) was not run
(credential outage). `cpuMs` limits CPU, not waiting, so it should not change a deadlock; K04 R25 saw 0 of 1, 2 or 5
calls complete under `cpuMs` 1000. K04's probe gave up at a 3 s wall timeout. This one waits for the platform's
30 s reset and sees the first call complete. Both agree from the second call on.

So the first callback into an object that holds its input gate gets through on the platform, and every later one
waits for the gate the caller holds. Every Whagons Reducer makes 1–14 host calls (S2: up to 37 statements), so the
Runner layout cannot run S2's transaction model. Without the transaction (`rpc-notx`, local) the calls are cheap but
the model breaks the way S2's implicit mode does: 8 of 39 steps ended in deferred-FK object resets or failed
savepoints. A deadlock also costs every socket on the tenant object (reset).

The Runner layout is therefore out for Reducers and Queries. It still serves cases with no host calls, such as K04's
CPU guard for pure computation.

## 2. Facet layout: same behavior as the baked build

The facet runs S2's `invoke` unchanged, with its own `ctx.storage`: one storage transaction per invocation, savepoints as
nested transactions, the deferred-FK guard, pinned clock and seeded random.

- **S2 workload** (`workload-cf.json`): 8 fresh tenants per mode, 2,060 DDL statements and 933 rows each (13 objects
  refused, as in S2). All 39 steps give the same outcome text, commit/rollback decision and result value in the facet
  and in the baked build. That includes the deferred-FK rollbacks (steps 03, 11, 20, 23), the savepoint cases S1–S4,
  and `centers.askJev`'s two `applyDataWrites` savepoints. Every table's row count matches after the run. Repeated
  Reducers: 120/120 ok in each mode.
- **Rollback and isolation** (`gate-long-cf.json`): a Reducer making 30,000 host calls and then throwing. A
  supervisor write sent 400 ms into it waited for it in both layouts (965 → 996 ms facet, 972 → 991 ms baked), and it
  survived the Reducer's rollback.
- Provisioning the schema subset through the facet took 1.75–3.66 s (baked 0.81–2.58 s). It is 2,060 statements
  sent in one RPC; the difference is the RPC and facet start, not SQL.

## 3. (a) Load time and memory

**Cold load** (`load-cf.json`, 6 reps; `share-cf.txt`, 16 more objects). Each rep uses a new generation id on a new object:

| Generation | Cold (in-object, includes fetch) | Of which fetch from `Bundles` | Warm, same object | Another object, same id |
|---|---|---|---|---|
| Real bundle, Runner | 378, 556, 681, 774, 877, 884 ms | 79–323 ms | 0 ms (callback not run, 6/6) | 369–830 ms, callback ran 6/6 |
| Empty app (kernel only), Runner | 24–45 ms | 11–31 ms | 0 ms | 21–84 ms, callback ran 6/6 |
| Real bundle, facet (to the first facet answer, client) | 529–671 ms | — | 2–12 ms | — |
| 8 more objects each (`share-cf.txt`) | real: 464–1,089 ms; empty: 29–84 ms | | | callback ran **16/16** |

- The real bundle's compile and evaluate costs about 300–750 ms on the platform per cold load. The deployed-Worker
  path costs 87–93 ms (wrangler's startup time for the baked build). Why dynamic code starts so much slower was not
  investigated (a compile cache for uploaded scripts would explain it).
- Every new object paid the full load, so plan for **one load per object wake**, not one per machine. All 24 objects
  landed in different parent isolates, so colocated reuse was never tested. The docs say reuse "is not guaranteed".
- After a Worker-version deploy, the object's generation was cold again (callback ran, 465 ms), although the object
  itself had not restarted yet (`sockets-deploy-cf.json`).
- For a tenant this adds **≈ 0.4–1.1 s to every cold wake**, on top of S1's ~1 s schema wake, unless the kernel
  starts the load in the constructor in parallel with the wake. That is a G3b cold-boot cost.

**Memory per generation.**

- *Platform* (`ballast/ballast-{1,2,3}-cf.json` plus one earlier run, 4 MB steps): the ballast reached before the
  isolate was replaced was 68–108 MB in a Runner with the real bundle, 52–92 MB with the empty app, 56–120 MB in a
  facet, and 56–128 MB in the tenant object (loader build) vs 60–108 MB (baked). The platform replaces isolates well below the limit, so this
  probe cannot resolve a 13 MB bundle. It shows only that a loaded generation leaves at least about 100 MB of headroom
  in its own isolate.
- The generation runs in **its own isolate**, not the tenant object's: Runner and facet of one generation share an
  isolate id, and the tenant object's isolate is separate.
- *Local workerd RSS* (`rss-whagons-local.json`): each further generation loaded in one object added 38–42 MB (the
  first 86 MB), and each facet on a new generation 29–39 MB. Old generations stay resident until the loader evicts
  them. S2 measured the bundle's V8 heap at 13.5 MB idle and 25.2 MB after Reducers.

## 4. (b) Generation switch and sockets

The switch (`activate`) preloads the new generation, replaces the generation reference, aborts the facet so its
next call starts on the new class (storage is kept), and sends `system.reload {reason: "module generation changed",
artifactHash}` to every socket.

| Run | Sockets | Switches | Closed | `system.reload` received | Object instance |
|---|---|---|---|---|---|
| Tenant-held, real bundle, writer at 2/s (`sockets-cf.json`) | 1,000 | 5 | **0** | 1,000/1,000 each time, 3–64 ms before the activate response arrived | unchanged |
| Tenant-held, empty app, facet Reducer running at each switch (`sockets-load-cf.json`) | 500 | 6 | **0** | 500/500 each time | unchanged |
| Same, first run (`sockets-load-run1-cf.json`) | 500 | 12 | **0** | 500/500 each time | unchanged, but the facet failed every call after switch 1 (§5) |
| Tenant-held, real bundle, then a Worker-version deploy (`sockets-deploy-cf.json`) | 500 | 2 + deploy | 0 in the 40 s watched | 500/500 | unchanged during the 40 s |
| Gateway-held sockets (G0-1) across a facet switch | — | — | **not run** (credential outage); `test/gateway-sockets.mjs` | | |

The switch itself never touched a socket. Compare a Worker-version deploy (S3 §7): hibernated objects keep their
sockets; active objects close all of them with 1006 at the deferral limit (300 s by default, or at once with
`immediate`), and clients are back in 4–6 s. The default deploy above kept the 500 sockets for the 40 s watched,
which agrees with S3 (they would drop at 300 s).

A switch never restarts a gateway object, so gateway-held sockets can only be hurt by a tenant reset (§5, anomaly 1).
K04 R25 measured that a tenant reset closes 0 of 10 gateway sockets and fails only the calls in flight. This was not
measured for a facet switch.

## 5. In-flight calls during a switch, and `facets.abort`

The facet keeps running its old class until `ctx.facets.abort()`. `abort` fails the calls running in the facet
and keeps its storage.

| Case (`inflight-cf.json`, `abort-*-cf.json`) | Result |
|---|---|
| Runner layout: call running on generation A, switch to B | the call finishes on A's isolate; the next call runs on B |
| Facet, synchronous Reducer (40,000 host calls, ~1 s) running when the switch arrives | the switch waits for it (the object's thread is busy); the call commits on A; the next call runs on B |
| Facet, Reducer that yields on a timer mid-transaction, switch with abort | the switch lands mid-call; the call fails with `module generation changed`; **both its writes are rolled back**; the next call runs on B (6/6) |
| Same with **drain** (wait for in-flight facet calls, then abort) | drain 1,080 ms; the call commits on A with both rows; the next call runs on B |
| Abort during the facet's first start (real bundle, 150 ms in) | the starting call fails with `module generation changed`; the next calls run on B (5/5) |
| One object, 10 switches in a row (no load; sync load; yielding load) | 30/30 switches clean |

**Anomalies.** About 100 switches with `abort` were run on the platform. Most were clean, but three things failed
intermittently:

1. **Object reset on switch, 2 cases.** A switch queued behind a ~1 s synchronous facet Reducer got HTTP 500 with an
   empty body. Workers Logs shows `Network connection lost.` on the tenant call. The object restarted, the generation
   pointer write was lost (the next call ran on A), and every socket on that object would close. Seen in 2 of 16
   such tries: 1 of 8 in `abort-behind-sync-cf.txt`, 1 of 2 in the `inflight` runs, 0 of 6 in `abort-matrix-cf.json`.
2. **Facet stuck after a switch, 1 case.** In `sockets-load-run1-cf.json` the first switch aborted a running facet call. From
   then on every facet call on that object failed at once with `internal error; reference = …` (12 switches; fresh
   objects were fine). The object did not reset and its 500 sockets stayed open. The facet worked again only after the
   object restarted. Not reproduced in 6 + 18 + 30 + 5 later tries.
3. **One driver run hung** for 11 minutes with no response (no client timeout then). Cause unknown.

Drain, then abort between invocations, is the only protocol here that loses no work. Anomaly 1 also happened with
nothing in flight from the supervisor's view (the call had just returned), so draining does not rule it out.

## 6. (c) Runaway under `limits.cpuMs` in a facet

**Not run** (credential outage). The probe is `test/runaway.mjs`. It runs a facet Reducer that writes one row and
spins, on a generation with `limits.cpuMs` 1000 and with none, while 20 tenant sockets echo and three calls are
queued. Related measurements:

- K04 R25: a Runner call (no host calls) under `cpuMs` 1000 stopped after 3.2 s with
  `Worker exceeded CPU time limit.`. The tenant was not reset, sockets stayed open and queued calls ran afterwards.
- K04 R25: without a limit, tenant-held sockets that send traffic closed with 1013 once the gate was held for about 7 s.
- This spike: a synchronous facet Reducer holds the tenant object's thread. A supervisor request waited for it (§2), so
  a facet runaway blocks the whole object's events, as in the baked build.

Whether `cpuMs` in a facet's `WorkerCode` stops the facet's call without resetting the facet or its storage is open.

## 7. (d) Reducer round trip vs the baked build, with rows read

S2's 10 committed Reducers, one call per request, 8 fresh tenants per mode (`workload-cf.json`). Client time minus the
object round trip:

| Step | Reducer | Facet p50 / p90 ms | Baked p50 / p90 ms | Rows read (of which host catalog) | Rows written |
|---|---|---|---|---|---|
| 01 | `teams.create` | 46.7 / 90.0 | 53.8 / 95.9 | 2,135 (2,099) | 5 |
| 08 | `spots.create` | 38.6 / 78.0 | 42.3 / 65.5 | 2,174 (2,109) | 9 |
| 19 | `cleaningStatuses.create` | 36.3 / 60.8 | 36.3 / 46.9 | 2,112 (2,083) | 3 |
| 22 | `cleaningStatuses.create` | 21.7 / 41.4 | 31.8 / 56.6 | 2,112 (2,083) | 3 |
| 24 | `cleaningReports.create` | 30.0 / 52.9 | 41.4 / 56.5 | 449 (437) | 8 |
| 26 | `centers.askJev` | 40.6 / 58.0 | 48.3 / 81.3 | 3,448 (3,334) | 11 |
| 27 | `settings.togglePlugin` | 38.9 / 42.7 | 37.9 / 61.6 | 474 (419) | 7 |
| 28 | `boards.create` | 27.1 / 36.9 | 34.6 / 51.1 | 849 (834) | 3 |
| 29 | `boards.createMessage` | 42.2 / 58.1 | 49.4 / 71.8 | 1,717 (1,664) | 7 |
| 30 | `boards.createComment` | 32.2 / 70.4 | 38.1 / 58.3 | 1,277 (1,247) | 5 |

Rows read and written are identical in both layouts (same SQL). The facet's median step was 7 ms *faster* than baked,
inside the network noise of this method (client round trip 20–38 ms). The first run's 3 tenants had the facet 3 ms
slower. Back-to-back calls in one request are another matter: 20 calls in one request took 18.4 ms per call in the
facet vs 6.0 ms baked (p50, n = 24 batches). The tenant object waits for each facet call's commit before starting the
next, while the baked build commits a batch together.

A Worker-timed comparison (`test/roundtrip.mjs`, the front Worker times the call into the object, with no client
network in the number) is written but was not run (credential outage).

## 8. Isolation with `globalOutbound: null`

From module code in the dynamic isolate (`isolation-cf.json`): `fetch()` to any host and `connect()` throw "This worker
is not permitted to access the internet via global functions"; `caches.default` throws "Cache API is not yet supported
for dynamically-loaded workers"; `eval` and `new Function` throw `EvalError`; `env` is empty. The facet's SQLite is
separate from the tenant object's, and module code can reach the tenant only through what the tenant passes in. The
contract's network rule for Queries and Reducers therefore holds by construction. Actions are not in this bundle.

## 9. (e) Platform status, limits and pricing

- **Status.** Dynamic Workers have been in **open beta** since 2026-03-24 (changelog). No GA announcement as of
  2026-11-11. Durable Object Facets are documented (page updated 2026-10-12) with no separate beta or GA statement
  and no published limits of their own. Local workerd 1.20261104.1 behaves differently from the platform (§1), and
  `facets.abort` showed intermittent failures (§5). Facets also have an undocumented `clone(src, dst)` in the type
  definitions.
- **Plan.** Workers Paid only. The development account has it.
- **Limits.** In-flight Dynamic Workers: 10 distinct per Durable Object (4 per Worker request); one generation counts once.
  CPU and subrequests per invocation follow the plan unless `limits` lowers them. No module-size limit is documented;
  a 4.2 MB module plus a 752 KB JSON module loaded fine.
- **Pricing** (docs updated 2026-06-11, billed since 2026-05-26):
  - $0.002 per unique Dynamic Worker per day beyond 1,000 a month. A Dynamic Worker is one (id, code) pair, so one
    generation id used by every tenant counts once a day.
  - Requests and CPU at Workers Standard rates. Each RPC call into a Dynamic Worker is a request.
  - **Startup CPU is billed.** At about 0.5 s per cold load and 1,000 tenant wakes a day, that is about 15M CPU-ms a
    month, inside the included 30M.
- **Facet storage.** The facet's SQLite is stored with the object. Whether it shares the 10 GB per-object limit with
  the supervisor's database is not documented and was not tested.

## 10. What the facet layout would change

- The kernel code that touches tenant SQLite (K01 storage, K02 migrations, K03 capture, K04 host, Query evaluation for
  replicas) runs **inside the loaded generation**. The deployed Worker keeps sockets, routing and the generation
  pointer. Kernel fixes then ship as generations too, without a Worker deploy.
- Tenant data lives in the facet's database, not the object's. P03 export/import, provisioning (K02) and the
  supervisor's own tables move accordingly.
- Each object wake loads the generation (§3); K19 would start the load in the constructor.
- The switch protocol is: drain, abort, broadcast. §5's anomalies make it a beta dependency on the
  tenant's whole data path.

## 11. Recommendation for the lead (0016 G0-2, K19)

1. **Keep G0-2 for stage 1.** K19 uploads a Worker version per `/dev/sync`. Hibernated objects keep their sockets;
   active objects reconnect in 4–6 s (S3).
2. **Do not adopt the Runner layout for Queries or Reducers** (§1). For K04's per-call CPU guard (R25), only module
   code with no host calls can use it.
3. **Treat the facet layout as a stage-3 candidate**, behind a capability flag, after:
   - Dynamic Workers reach GA;
   - the `facets.abort` anomalies are reproduced and fixed or explained (to Cloudflare, with the references in
     `abort-*-cf.*`);
   - runaway containment under `cpuMs` is measured;
   - K19's constructor-time preload brings the wake cost under G3b.
4. **Not measured, for whoever picks up K19.** With G0-1, gateway objects could live in a different Worker script
   from tenant objects (DO bindings take a `script_name`). `/dev/sync` would then deploy only the tenant script, and
   gateways would send `system.reload` when the tenant answers with a new build. That would keep sockets open without
   the Worker Loader. The probe is `split/` and `test/split-deploy.mjs`; it was deployed but not run because of the
   outage.

### Still to run when credentials return

Redeploy both builds first (the deployed versions predate `Gateway`, the timed route and `cpuMs`). Then run `test/runaway.mjs` (c), `test/gateway-sockets.mjs` (b, gateways), `test/roundtrip.mjs` (d, Worker-timed),
`test/split-deploy.mjs`, `test/txcalls.mjs` (0022 with `cpuMs`), then delete `gonvex-cf-dev-s5`,
`gonvex-cf-dev-s5-baked`, `gonvex-cf-dev-s5-gw` and `gonvex-cf-dev-s5-tn`. These four Workers are still deployed on the
development account (idle, no secrets beyond a random admin key) because deleting them also needs the credential.

## Not done

- No staging data was used; nothing to delete.
- Memory per generation on the platform could not be resolved below the platform's isolate-replacement noise (§3).
- Alarms, hibernation of the supervisor with a running facet, and facet storage limits were not tested.
- Anomalies 2 and 3 (§5) were not reproduced on demand.

## Reproduce

See `cloudflare/spikes/s5/README.md`. Inputs: Gonvex `cloudflare/integration` at `bf59f9ff`, S2's artifact
(whagons5-client `2457bb65f`), wrangler 4.158.0 / workerd 1.20261104.1. Raw results are in `cloudflare/spikes/s5/results/`.

