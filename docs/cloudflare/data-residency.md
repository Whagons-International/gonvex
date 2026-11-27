# Data residency on Cloudflare

A Gonvex Cloudflare stack can keep all of its stored data in one jurisdiction (`eu` or `fedramp`), so a customer whose
data must stay in the EU gets a stack whose Durable Objects and R2 buckets are created and kept in the EU.

## What the switch does

Deploy with `stack: { jurisdiction: 'eu' }` (`deployAssembly`) or pass `jurisdiction: 'eu'` to `servingConfigs`:

| Piece | Effect |
| --- | --- |
| Durable Objects | `GONVEX_JURISDICTION=eu` on all five Workers. Every exported class and default handler goes through `resident` / `residentHandler` (`kernel/assembly/residency.ts`). Each `env` they receive has every Durable Object namespace replaced once by `namespace.jurisdiction('eu')`, so every `getByName` / `idFromName` / `get` addresses an EU object. Call sites stay unchanged. |
| Fail closed | An unknown `GONVEX_JURISDICTION` value throws. A resident object whose own id carries another jurisdiction, or none, refuses to construct. |
| R2 | Every `r2_buckets` binding gets `"jurisdiction": "eu"`. `deployAssembly` creates `<name>-releases`, `<name>-files` (and `<name>-backups`) with `cf-r2-jurisdiction: eu`, and refuses a same-named bucket that exists elsewhere. |
| Workers Logs / observability | Off (`observability.enabled=false`, logs off), because their storage is not restricted to the jurisdiction. `wrangler tail` still streams live logs without storing them. Opt back in with `observability: true`. |
| Analytics Engine telemetry | The `TELEMETRY_EVENTS` dataset binding is dropped (its storage is not restricted). Opt back in with `telemetry: true`. |
| Release builder | Its Container image redeploys the runtime from the source baked into the image. The builder Worker passes `GONVEX_STACK_OPTIONS` to the Container, and `targets.mjs` keeps those settings on every redeploy. If the image may predate this, `/dev/sync` is refused on a jurisdiction stack unless the deploy marks the image with `builderResidency: true`. Without that guard, an older image would redeploy the runtime without the jurisdiction. |

Evidence: `GET /_gonvex/residency?tenant=<id>` (admin key) returns the jurisdiction of the Control and tenant ids,
measured from the namespaces, plus each object's own `gonvexResidency()`: its id's jurisdiction and the
`cdn-cgi/trace` colo the object runs in.

Local workerd does not implement jurisdictions ("Jurisdiction restrictions are not implemented in workerd"). The unit
tests (`kernel/assembly/residency-test`) therefore use fake namespaces. Real placement is proven on a deployed stack
through the route above.

## Tenant regions in the control catalog

`gonvex_cf_tenant_regions` (`control/operator/regions.ts`) records the region a tenant was created for:

- `POST /dev/tenants` accepts `region`. When `region` is absent, the tenant gets the stack's region.
- A region the stack cannot place is refused (422) before anything is written: any region on a stack without a
  jurisdiction, or a region that differs from the stack's.
- `GET /dev/tenants` reports `region` when there is one. A stack without a jurisdiction keeps the exact reference
  tenant shape.

### Next step: several regions on one stack (designed, not built)

1. **Routing.** Resolve the tenant route's `region` along with the route itself (Control already returns
   `TenantRoute`), and pick the namespace as `tenantNamespace(env, region)`, which is `env.TENANT.jurisdiction(region)`
   or the stack default. Do this in the same places `rpc(env.TENANT, …)` is used today, behind one helper. The region
   is immutable after creation, so a cached route never goes stale.
2. **Files.** Add one files binding per region (`FILES_EU` …), chosen by the same helper. `_gonvex_files.bucket`
   already records the bucket of each file.
3. **Control stays home.** Accounts and memberships are global, so Control itself stays in the stack's home
   jurisdiction. A stack that hosts EU tenants keeps the EU home if any of its accounts belong to EU customers.
4. **Moving a tenant between regions** is a snapshot and restore (below) into the other namespace, followed by a
   region update in the same Control write that repoints the route.

## Snapshots, backups and restore

`kernel/assembly/snapshot.ts` writes a consistent, checksummed copy of one SQLite Durable Object (its tables and its
KV) to R2:

- **Consistent.** Every read of a snapshot runs inside one `blockConcurrencyWhile`, after every queued Reducer of a
  tenant. The compressed parts are uploaded after the block, so R2 latency never blocks the object. The manifest
  records `blockedMs`.
- **Layout.** `manifest.json` is written last, so a snapshot without it is incomplete. The rows are in
  `part-NNNNNN.json.gz` files (P03 wire format, at most 4 MiB of JSON each, SHA-256 of each part in the manifest), and
  `kv.json.gz` holds a tagged JSON that is exact for structured-clone values; anything it cannot represent throws.
- **Checksummed.** The manifest records each table's row count and content SHA-256, using the P03 `RowHasher` that the
  tenant import also verifies.
- **Restore** (`restoreStep`, one part per call) only fills an **empty** object. It verifies each part's SHA-256,
  recreates the schema, rows, sequences, triggers and FTS, and restores KV. It skips stack-local KV
  (`gateway:`, `control/watchers/`, `session-recovery*`, `autoscale-*`). It then checks every table checksum against
  the manifest, the schema, `quick_check` and FTS integrity. Only then does it report `done`, and the object resets.

Kernel routes (admin key; 404 without it):

```
POST /_gonvex/backups/run       {"prefix"?}       Control + every live tenant now
GET  /_gonvex/backups/latest                      the last nightly index
POST /_gonvex/backups/restore   {"object":"control"|"tenant","tenant"?,"prefix"}
POST /_gonvex/backups/drill     {"prefix"}          restore into a scratch object, verify, wipe it
GET  /_gonvex/residency?tenant=<id>
```

Nightly backups are enabled with `stack: { backups: true }` (or `{ bucket, jurisdiction, cron, prefix }`). This binds
`BACKUPS` to the kernel and the runtime and adds a cron trigger (default `17 2 * * *` UTC) that writes
`nightly/<time>/{control,tenants/<id>}` plus `index.json` and `nightly/latest.json`. Retention is an R2 lifecycle rule
on the backups bucket.

**Restore drills (tested restores).** After every nightly backup the cron restores the Control snapshot (and on
Sundays UTC every tenant snapshot) into a fresh scratch object `restore-drill-*`. It verifies checksums, schema,
`quick_check` and FTS, then wipes the object. The result is in `index.json` → `drills`, and `ok` is false if any drill
failed. `GONVEX_BACKUP_DRILL=0` turns this off. Run one by hand with `POST /_gonvex/backups/drill`.

**Moving a stack into a jurisdiction.** Bind the old stack's `BACKUPS` to a bucket in the target jurisdiction
(`backups: { bucket, jurisdiction: 'eu', prefix: 'migration' }`). Stop client traffic to the old stack
(`GONVEX_MAINTENANCE=1` on its kernel: every request but the admin `/_gonvex/` routes gets 503) and run a
backup there. Deploy the new stack with `jurisdiction: 'eu'` and the same bucket, then restore Control and each
tenant. The data goes from the old objects to EU R2 to the new EU objects, and never leaves Cloudflare. Copy the
releases and files buckets object by object, verifying SHA-256.

**Deploying this to an existing stack.** The kernel's objects use the default code-update strategy, so a busy
Control can keep running the previous code (and answer "does not implement the method") until it goes idle.
Deploy the kernel with `durable_objects.code_update_strategy: { mode: "immediate" }` to restart it at once.

## What still leaves the jurisdiction

- **Compute.** Workers code, including a resident object's stateless callers, runs in the Cloudflare data centre
  nearest the request. Requests from outside the EU are processed outside it. Stored data does not move.
- **Edge cache.** Responses Cloudflare caches (static assets) live at the colos that serve them.
- **Workflows and Queues.** These have no jurisdiction control. `ACTION_WORKFLOW` instance payloads and `rollout`
  queue messages are stored wherever Cloudflare places them. Keep personal data out of Workflow params (pass ids).
- **Workers Logs, observability and Analytics Engine,** if they are re-enabled.
- **Container images and Containers.** The sandbox and builder containers run wherever Cloudflare schedules them. The
  builder holds no tenant data. The sandbox reads tenant files while a query runs.
- **The operator's machine and CI** whenever data is pulled out through the operator routes.
