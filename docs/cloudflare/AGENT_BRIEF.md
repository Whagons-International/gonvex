# Gonvex on Cloudflare: agent brief

Read this before taking a package from `work-packages.json`. Commit both files under `docs/cloudflare/` in the Gonvex repo.

## Goal

Run the Gonvex backend on Cloudflare Workers and Durable Objects with the contract unchanged. Clients, `@gonvex/module-sdk` and the Whagons modules must work without edits. One Durable Object per tenant holds that tenant's SQLite database, sockets, change log and timers.

All Cloudflare work collects on the `cloudflare/integration` branch, which was cut from `staging`. Branch from it, name your branch `cf/<package id>-<slug>`, and open pull requests against it. Pushes to `cloudflare/integration` deploy nothing; only `main` and `staging` trigger deploys. `cloudflare/integration` is the long-lived branch for this work and is fully independent of `staging`: nothing from it goes into Gonvex `staging` or the Whagons app repository before cutover. When and how it reaches `staging` is decided at cutover, by Gabriel. Work that would need an app-repo change (for example the bridge part of K21) is prepared on a branch and lands only at cutover. The Rust runtime at `0bf3ad13419d25d9370607e1470ae58da4c43235`, the `staging` head on 2026-11-10 and what staging runs, is the reference. The Go runtime is out of scope.

## Stages

0. Prove: four spikes, the contract freeze, the conformance harness, export/import, the Reducer log feed, baseline metrics.
1. Kernel to contract parity, in three waves.

   1b. Feel responsive before any customer moves (R01–R06): an outside-in probe of today's staging and
   production sets the baseline. The kernel then gets a wake budget, a server-side schema diet, a catalog
   cache, a lean change log and kernel timers. Gate **G3b**, in front of G4: on the same scenarios, the kernel
   equals or beats R01's baseline on every user-facing number.
2. Migration by tenant and by ring.
3. Toward whagons-lite principles, one capability-flagged change at a time. The client-side items (L06
   packed pages, L07 columnar replica) are built in parallel behind capability flags and ship after migration.

A gate separates each stage. Gates are in `work-packages.json`.

## What the contract is

- The wire protocol as `packages/protocol/src/index.ts` defines it. The Rust crate `rust/crates/protocol` is a superset. The native-rows binary snapshot path is out of scope because the shipped SDK never negotiates it.
- The module host calls in `rust/crates/module-runtime/src/lib.rs` and `rust/crates/module-runtime-v8/src/gonvex_bootstrap.js`, including the pinned clock and seeded random.
- The SQL rules in `docs/sqlite-storage.md`.
- The `control.*` functions in `rust/crates/runtime/src/control.rs` and `packages/client/src/control.ts`.
- The deploy flow through `POST /dev/sync` and the operator routes in `rust/crates/runtime/src/operations.rs`.

Caches, lanes, offload threads and tuning variables in the Rust runtime are not part of the contract.

## Intended behavior the kernel implements instead of copying

- Private files: check the owner on every download URL and delete, not only in sandbox imports.
- Actions: a maximum attempt count and a dead-letter state. Today they retry forever.
- Reducer receipts: expire after the client's offline window. Today the table is never pruned.
- Webhooks: one ingress Worker for all of them.

## Platform limits to design for

- Durable Object SQLite: 100 KB per statement, 100 bound parameters per query, 100 columns per table, 2 MB per row, no custom SQL functions, no hooks. `BEGIN` and `SAVEPOINT` go through the storage transaction API.
- 128 MB of memory per isolate. 30 s of CPU per event by default, 5 minutes at most.
- 10 GB of storage per object.
- Received WebSocket messages cap at 32 MiB. A Worker cannot send protocol pings.
- Queue consumers stop at 15 minutes. A Workflow step has no wall-clock limit.

## Known risks, each with a spike

1. The tenant schema has about 6,450 triggers and 473 views. Stock SQLite 3.40 refuses it with a parser stack overflow. S1 finds out whether a Durable Object loads it, and at what wake time and memory.
2. Module code awaits every host call. S2 proves a Reducer can run in one transaction with rollback and savepoints.
3. Presence liveness relies on server pings. S3 finds the replacement.
4. Agent Actions have no deadline. S4 proves the Workflow path.
5. Mobile clients connect through the Go bridge. It must pass its suite against the kernel (K21).

## Credentials and data

- Cloudflare: use `whagons-dev credentials exec full-cloudflare-access` (fields `accountId`, `apiToken`). Gabriel approved it for this work; a scoped token comes later. Create only resources whose names start with `gonvex-cf-dev-`, and never touch other Workers, DNS, zones or R2 buckets in the account.
- Staging tenant data is a copy of production made for testing. Agents may copy staging tenant SQLite files to arch3 and load them into `gonvex-cf-dev-*` resources. Never commit them, never put them in a public place, and delete local copies when a package finishes.

## Rules

1. The contract comes first. If it is wrong or incomplete, write a note in `docs/cloudflare/contract-changes/` and wait for the lead.
2. The Rust runtime's behavior is the reference, except for the intended-behavior items above. Where code and docs disagree, code wins.
3. Every package adds conformance scenarios for what it implements. Never weaken a scenario or an assertion to make a run pass.
4. Stay inside the paths your package owns. Shared files change through the lead.
5. Do not change the Rust runtime or the Whagons modules. P04 and K21 are the two named exceptions, and both stay off `staging` and the app repository until cutover. Schema incompatibilities are handled by the kernel's schema transform (A01), never by editing the app's migrations.
6. Do not touch production or live staging. Use local workerd, a local Rust runtime and the development Cloudflare account. The one exception is R01's synthetic probe: read-only browser sessions against a designated test tenant on staging and production, with no app change and no load beyond one user's.
7. If a platform limit blocks you, stop and report it. Do not design around it on your own.
8. Every pull request states the numbers it moved: bundle bytes, rows read and written for the functions it touched, memory where it matters.
9. Credentials come only through `whagons-dev credentials exec`. Never print a value.
10. Pull requests are never drafts. Put the package id in the title.

## Roles

- Lead: owns `CONTRACT.md`, `work-packages.json` and the decision log. Reviews and merges. Does not implement packages.
- Package agent: one package, one branch, one pull request.
- Verifier: a fresh agent that did not write the code. Runs the harness, checks work against the contract, signs off spikes and gates. Reports and does not fix.

## Design rules that hold from the first line

These come from whagons-lite's `docs/DOD.md` and cost nothing to follow now.

- No SQL inside a loop over rows or members. Read by id list, once per batch.
- Work per change stays flat as member count grows. Encode once per visibility class and send the same bytes to every socket in it.
- Every loop over rows works in pages and yields between pages.
- Nothing ephemeral is written to SQLite.
- Write delivery against one interface with two placements, in the tenant object or in a gateway object, so fan-out shards are a routing change.

## Initial staging-candidate capacity requirement (Gabriel, 2026-11-12)

X01 automatic gateway scaling is required before an initial staging candidate is ready. It must scale out
with cross-tenant and hot-tenant delivery demand without deployments or manual shard-count changes, and
scale in safely with drain, measured hysteresis/cooldown and resumable recovery. Sockets, outbound frames
and bytes, delivery lag, queue age and RPC delay drive bounded telemetry; CPU alone is insufficient.
Keep stable existing-connection ownership and ordered live-query/replica delivery, bounded buffers and
control work, idle hibernation and an explicit resource/cost envelope. Audit shared Control/auth,
revalidation, limiters, routing and deployment capacity too. Gateway scaling does not repartition a
tenant database: each tenant still has one serialized SQLite writer whose ceiling must be measured.

V03/V04 keep every original gate and add reproducible capacity curves on the exact assembled dev deployment,
including 26 tenants and 1,000 users while autoscaling occurs. Locate the first backend or load-generator
bottleneck with bounded admitted sweeps, distinguish measurements/platform limits/account quotas/estimates,
and publish raw artifacts and rerun commands. Evaluate X02 snapshot offload from storm evidence and pull it
forward if required by the original gates. No staging/production cutover or load there is authorized.
See decision 0053 and verification/V03-load-brief.md and V04-budgets-brief.md.

Assembly evidence precision: 0060 applies the existing 0028 JSON-number comparison, retains real-deploy raw
socket failures and defines separate unchanged-SDK recovery proof; original gates remain. Existing K20 image
metadata alone does not close its local-source provenance gap. 0061 permits an explicitly acknowledged empty
server-epoch bootstrap after tenant restart, preserving the unchanged restart test and all scope fences.
0062 records the existing per-holder telemetry limiter domain: one-holder canonical parity keeps exact
assertions, with separate real multi-holder evidence and mandatory final X01-enabled capacity/security
verification. It does not introduce a global account budget or weaken K16's per-address enforcement.
0063 permits digest-only resume checker reuse only from a verified same-socket/scope window, preserving
all digest assertions. It also requires unrelated targeted delivery to leave other sockets untouched,
while preserving genuine whole-holder/global scope barriers. Frozen failing campaigns remain evidence.
0064 defines server-only operation-bound initial completion and fail-closed ordered terminal errors.
0065 permits explicit provisional pending/authority admission with honest refusals and no false quiescence.
0066 separates credential-free hosted image builds from Vault-only daemonless publication; skipped preview
steps are not deployment proof. [K22 review evidence](verification/K22-review-a957.md) records the six
unresolved review findings.
0067 fences intermediate initial pages and strips unbound split progress without changing normal commits.
0068 keeps Action reply generations and legacy deployment-skew gaps explicit; no lost history is fabricated.
0069 permits bounded owner-authority prototyping; terminal release, cleanup and availability remain open.
0070 permits recovery-only exact-subset correlation cleanup, never progress or normal-completion fallback.
0071 permits one owner refresh only after an exact, typed zero-invocation rejection; ambiguous calls are
never automatically replayed. The original first-writer restart test and owner-lifecycle gates remain.
0072 records the both-backend invitation receipt collision quirk; preserve outside-gate behavior in K22.
Generic receipt path validation and the separate migration-gate read-only exception remain unchanged.

## Uptime and tracing planning (0055, 2026-11-12)

The canonical observability-uptime-plan.md adds independent external monitoring and explicit Gonvex
WebSocket/async operation correlation, truthful regional/component/task-operation accounting and dev fault/
alert/recovery proof. Preserve K15/R01/V02/V05/M05 original gates and real-duration soak/shadow requirements.
Cadences and 99.9% monthly are owner proposals, not measured uptime/SLA. No new paid account/subscription,
DNS/status publication, unsolicited alerts or live staging/production monitoring activation is authorized.
Production R01 remains read-only single-user; warm monitoring never substitutes for cold-wake experiments.
