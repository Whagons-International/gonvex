# 0036: Deploy and operator routes (K19): findings and adaptations

Date: 2026-11-11. Status: **proposed** (K19), for the lead. Numbered 0036 after 0032's reservations (0033-0035).
Affects: K07, K10, K11, K13, K15, K20, M05,
`CONTRACT.md` D.0-D.3. Code: `cloudflare/control/operator/`, `cloudflare/router/dev/`.

Measured in local workerd (wrangler 4.155.0, workerd 1.20261023.1) and with the repository's own CLI deploying
whagons5-client `2457bb65f` to the local kernel. No platform run: the credentials vault was down (521), see
"Platform follow-ups".

## 1. The sync body never crosses RPC (finding)

The Whagons `/dev/sync` body is **44.7 MB** of JSON: `module.files` 26.7 MB (migrations), `module.javascript`
8.4 MB, `module.reducerBundle` 5.6 MB, top-level `functions` 1.6 MB. Workers RPC caps an argument at 32 MiB
(0020 §6) and a row is capped at 2 MB, so neither the router nor the control object can hold or store it.

**Done:** the router streams the body into R2 (`incoming/<id>.json`, deleted after the sync) and passes only the
key. The release builder parses, validates and splits it (`releases/<project>/<hash>/{manifest,view,app,…}`) and
builds the K02 plan. Measured: transform 10.0 s, prepare 19.6 s, Node heap 207 MB, 163 plan chunks. This is the
Container of 0028 §3.1; locally it is a Node process, in tests its core runs in-process. A Container cannot bind
R2, so the builder reaches the bucket through the router's `/_gonvex/k19/blobs/<key>` route with a shared token
(`GONVEX_RELEASE_BUILDER_TOKEN`). **Lead:** keep that route, or give the Container R2 S3 credentials.

## 2. `gonvex_runtime_manifests.manifest` holds a reduced manifest (adaptation)

Rust stores the whole manifest (45 MB) in the row. The kernel stores `{project, generatedAt, functions:
{path: {kind, internal}}, module: {language, generation, hash, entrypoint, invitationAcceptanceReducer, crons,
permissionGrants?, release}}`: what K06's `projectModule` reads, under the 2 MB row cap. The full manifest is in
R2 (`releases/<p>/<h>/manifest.json`). `GET /dev/manifest` serves `functions`, `schema` and `visibility` from
`view.json` in R2, with Rust's body shape and `module: {hash, generation}`.

## 3. A wrong sync key is refused before the body is stored (deviation)

Rust parses the body before checking the key. The router checks the key against the project named in
`x-gonvex-project-id` first, so an unauthenticated caller cannot write 64 MiB into R2. Observable only when the
header is present, the key is wrong **and** the body is malformed: the kernel answers 401 where Rust answers
400/415/422. The CLI always sends the header. Without the header the body is parsed first, as in Rust.

## 4. Activation is a Worker version with every project's bundle (0016 G0-2, as ruled)

The builder generates one module (`releases.js`, the kernel's `gonvex:releases` import) that imports each
project's active Query/Reducer bundle (`reducerBundle` when split, else `javascript`) wrapped with K04's prelude
(0022 §3), plus its function contracts. Stored bundles import the prelude as `gonvex:module-globals`, resolved by
the build's alias, so a release in R2 does not depend on a builder's file layout. A tenant object installs its
project's release into a K04 `ModuleRegistry` on first use. Measured locally: the Whagons release (1,997
functions) installs with no error.

- **Who deploys.** A Worker deploy restarts active objects, and the control object is the one holding the
  `/dev/sync` request. So the control object only builds the version; the router deploys it after the control
  object answered, then answers the client. On the platform the client therefore sees the 200 after the version
  is live, as in Rust.
- **Local only:** `wrangler dev` restarts workerd on the swap, which cuts every request in flight, including
  the sync response. The local builder swaps 1.5 s after answering, so locally the new code serves shortly
  *after* the 200. The kernel's own drift check (`GET /dev/manifest`) reads the control object, which is
  already up to date.
- **Size.** Every project's bundle is in one upload. With Whagons alone that is about 1 MB gzip (S2); a second
  large app would approach the 10 MB limit. That is the split-script question of S5 §11.4 and stays open.

## 5. Rollout, deadline, lock (0012 R1, R4, R5, as ruled)

- One queue message per tenant, sent one at a time in `tenant_id` order; the first failure answers 422 and no
  later tenant is touched. Measured in tests: a changed applied migration fails at the first tenant with Rust's
  text and leaves the active release as it was.
- K02's `applyRelease` restarts a plan that is already done (every release re-runs the prelude). Callers must
  call it once and then poll `status()`; `tenant/port.ts` `waitForRelease` does, for the queue consumer and for
  tenant creation. **K07/K10 take note** if they drive migrations themselves.
- K02's failure text carries the unit (`migration 0001_x.sql: …`) and, locally, a restore-point remark; the
  kernel answers Rust's `DatabaseError` text without them, and SQLite failures in sqlx's form.
- After `GONVEX_SYNC_DEADLINE_MS` (default 10 minutes) the sync answers **503** `tenant migration rollout did not
  finish within N s`. Rust has no deadline. **Lead:** confirm 503.
- The lock row expires at the deadline plus a minute, so a reset control object never keeps it.

## 6. Operator authentication: Local only on explicit opt-in (CONTRACT D.3)

Rust's "Local" operator (no credential configured → every caller is an admin) is available only with
`GONVEX_LOCAL_OPERATOR=true` in addition to Rust's conditions. No deploy script sets it. **Lead:** have the
deploy scripts refuse it outside `gonvex-cf-dev-*`, like the emulator gate of 0024 §3d.

## 7. Routes whose Rust values are process state (adaptation)

| Route | Kernel |
|---|---|
| `/dev/metrics` | Same JSON shape. `functions` from the active release and `gonvex_performance_events`; `websocket.connections` and `websocket.tenants` from the socket owners' `connectionState` reports; process pools, V8 heaps, caches and profiler are zeros or `{}`; `scheduler` is zeros until K11 reports (scheduled jobs live in tenant objects, 0012 R6). Adds `kernel.activeRelease`. M05 replaces the zeros with Analytics Engine numbers |
| `/dev/profile`, `?format=profile` | `{available:false, reason}` (D.1: defer) |
| `/dev/cache` | No-op with Rust's response (D.1: replace) |
| `/dev/storage/files` | `{configured:false, files:[]}` until K13 binds R2 file listing |
| `/healthz` diagnostics | Rust's keys; `database`, `moduleHost` and `sandbox` describe the kernel |
| `DELETE /dev/projects/{p}`, `/dev/tenants/{t}` | `deleteAll` on each tenant object, then the rows (no files) |

## 8. Dashboard sockets on the control object (adaptation)

`/dev/dashboard/ws`, `/dev/metrics/stream` and `/dev/logs/stream` are hibernatable sockets of the control
object, with state in attachments and previous results as SHA-256 hashes. Reads re-run after any successful
`/dev` write (dashboard.rs `invalidate`), every 30 s (3 s for logs and errors) on the object's alarm, and when a
tenant object reports a commit through `operatorDataChanged(projectId, tenantId)`. **K07 and K10 must call it**
after commits (the local tenant object does, from K03's `ChangeWake`); until then data reads refresh on writes
made through `/dev` and on the 30 s tick. The metrics stream ticks every 2 s and the log stream every 500 ms
while such sockets exist; the alarm is shared with K06's hourly prune.

## 9. Data browser on Durable Object SQLite (adaptation)

- The platform's `_cf_*` tables (and local workerd's `__miniflare_do_name`) are hidden, and `pragma_*` reads
  skip them: the authorizer refuses pragmas on `_cf_KV` (`not authorized`).
- `replace_references` stages its map in an ordinary scratch table created and dropped in the same tenant
  transaction (`TEMP` is not authorized, A01 T4).
- Operator writes are tenant transactions: one K01 invocation (deferred-FK check, G0-5) with K03 capture and
  empty provenance, as Rust's `TenantTransaction` without a command id. They wait (503) while a migration holds
  the schema gate (0028 §3.3).
- `LIMIT`/`OFFSET` are inlined validated integers: the platform binds JS numbers as REAL.
- A search binds its pattern once and reuses it (Rust binds it per column), which keeps wide tables under 100
  parameters.

## 10. K01's foreign-key model and `_cf_KV` (found independently; fixed by K07)

K01's `ForeignKeyModel` read `pragma_foreign_key_list` for every table, including the platform's `_cf_KV`,
which exists once an object has used `ctx.storage.kv` (K02's progress, K05's gateway registrations); the pragma
fails with `not authorized` and every writing invocation failed. K19 hit it in the local tenant object's operator
writes. K07 (#358) merged the same fix and a test first, so this PR carries no change to K01. K19's own
`pragma_*` reads (`list_tables`, `replace_references`) skip `_cf_*` too.

## 11. Confirmations asked of K19

- **0024 §3f:** no operator route reads `gonvex_auth_sessions.last_seen_at` (operations.rs, operator_data.rs,
  dashboard.rs). The dashboard's "last seen" values are error groups' `last_seen`.
- **0028 §3.1:** the build runs outside every object, keyed by release hash (a prepared release is reused), and
  the plan is stored next to the bundle (`plans/<planId>/…` through K02's `R2PlanStore`).

## K13's request (0032)

0032 asks that `/dev/storage/files` return `url: null` for private rows. Until K13 provides the file listing the
route answers `{configured:false, files:[]}` (§7); the listing call goes through K13's port when it lands, and the
`url: null` rule belongs to that port's rows.

## Not in K19

- `scheduler.sync_project` (crons) is a port the coordinator calls; K11 implements it.
- The IB2 dead-Action routes (`/dev/actions/dead`) belong with K11/K12.
- `/dev/internal/e2e/members` reuses K06's helper; it needs the tenant object's `TenantPort` (K07).
- The local tenant object (`router/dev/local/tenant.ts`) is an assembly for the local kernel and K19's tests.
- **Wiring to K07's tenant object (follow-up, merged while K19 was open).** K07's `ReducerTenantObject`
  (`kernel/reducer/object.ts`) expects a Worker-version subclass with `bundle(hash)` and `plans()` and an
  `activate(projectId, manifest, planId)` call from the deploy path. The kernel Worker's tenant class becomes that
  subclass: `bundle()` from `gonvex:releases`, `plans()` = `R2PlanStore(RELEASES)`, and the `OperatorTenantPort`
  methods delegating to `TenantOperator`. The rollout consumer then calls `activate` instead of
  `operatorApplyRelease`, and `ModuleReloaded` goes to K07's generation switch. One small PR once this merges.

## Platform follow-ups (blocked by the credentials outage)

1. Wire the builder as a Container (K20's infrastructure) with `GONVEX_BUILDER_TARGET=wrangler`, and measure a
   Worker-version deploy of the kernel with the Whagons bundle (upload size, deploy time, sockets dropped).
2. Create `gonvex-cf-dev-k19-releases` (R2) and `gonvex-cf-dev-k19-rollout` (Queue); run `/dev/sync` of Whagons
   against a preview with two tenants; record rows written per tenant and the rollout time.
3. Confirm `limits.cpu_ms = 5000` holds for the operator routes on the largest staging tenant (`list_tables`
   counts every table).
