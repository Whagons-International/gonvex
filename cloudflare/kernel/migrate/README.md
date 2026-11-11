# Migrations and provisioning (K02)

A tenant object gets its schema the way Rust's `provision_tenant_database` gives a tenant file its schema, and with
the same `gonvex_migrations` ledger:

1. the runtime prelude: tenant identity (`members` and its Control Plane outbox triggers), the reference ledger,
   schema options (`_gonvex_schema_capabilities`, table options, identities, record views), and the migration ledger;
2. every tenant-scope migration not yet in `gonvex_migrations`, in name order, through the A01 transform. Each one ends
   with Rust's `after_migration`, K03's `finalize` (provenance `migration:<name>`, channel `system`) and the ledger row;
3. the change-feed install from K03 (`planChangeFeedInstall`/`applyInstallUnit`), with Rust's `_gonvex_sync_bootstrap`
   receipts.

## Interface

```ts
// Once per release, outside any tenant object (K19's /dev/sync path):
import { buildReleasePlan, migrationsFromFiles } from "./build.ts" /* or ./index.ts */;
const plan = await buildReleasePlan(await migrationsFromFiles(files)); // throws TransformError, MigrationFileError
await new R2PlanStore(bucket).write(plan);                               // or any PlanStore

// In the tenant object (imports migrator.ts and plan.ts only):
const migrator = new TenantMigrator({ storage: ctx.storage, plans, capture, abort: (r) => ctx.abort(r),
  schemaChanged: () => storage.invalidateSchema(), publish: (n) => wake.publish(n) });
await migrator.applyRelease(plan.id);  // starts or continues; one event's worth of work; returns MigrationStatus
// alarm(): `await migrator.alarm()` while `migrator.wantsAlarm()`; the object multiplexes its one alarm.
migrator.gateClosed();                  // true while a unit is partly applied: no Reducers or other writers
migrator.status(); migrator.ledger();
```

`applyRelease` is the per-object step behind `/dev/sync` rollout (CONTRACT.md D.2.4): K19's queue consumer sends it to
every tenant object and waits for `status().state` to leave `running`. A new tenant object gets the active release
the same way before it is marked ready.

## Why plans are built outside the object

The transform over the whole Whagons history (12 migrations, 13.8 MB) takes about 8–10 s of CPU and peaks above 128 MB of
heap in Node (it does not finish with `--max-old-space-size=128`). A tenant object has 128 MB and, per 0022 §3b,
`cpu_ms = 5000`. So the transform runs once per release and the plan is stored: a manifest plus one JSON array of
statements per chunk (`<unit>/<index>`). An object reads the manifest once and one chunk per transaction.

## Chunks, resume and restore points

- Each chunk (A01 T7: ≤ 200 statements, ≤ 1 MB) is one `transactionSync`. Progress lives in `ctx.storage.kv`
  (`gonvex/migrate/progress`) and commits in the same transaction, so a reset loses at most the chunk in flight.
- Each event runs at most `maxChunksPerEvent` transactions and stops starting new ones after `maxMsPerEvent`. A watchdog
  alarm is set at the start of every event; when work remains the next alarm is set for now.
- Before each transactional migration, and before the change-feed install, the object takes a point-in-time bookmark.
  A migration that fails after one of its chunks committed is restored to it: `onNextSessionRestoreBookmark`, then the
  next event aborts the object. Rust's semantics follow: the failing migration rolls back, earlier ones stay.
  A `restore-point` marker is written just before the bookmark, so after a restore the object reads "failed and
  rolled back" instead of starting the migration again.
- A chunk that resets the object `maxAttempts` (3) times without committing fails the migration; tries are counted
  durably before each chunk.
- Transient errors (a plan fetch, a storage call) leave the progress as it was and retry after a second.

## Directives and ledger (CONTRACT.md E.a.6)

| Rule | Kernel |
|---|---|
| Names `NNNN_[A-Za-z0-9_-]*.sql`, ≥ 10 bytes, only directly under `migrations/`, sorted | `migrationsFromFiles`, same error texts as `modules.rs` |
| `-- gonvex:scope` required, leading comment block only | same; control-plane migrations are not in the tenant plan |
| Checksum: SHA-256 hex of the file text; mismatch fails with Rust's text | same |
| `-- gonvex:no-transaction` | statements run outside a transaction, earlier ones stay after a failure, ledger row last; `VACUUM` is refused with a clear error |
| `-- gonvex:foreign-key-rebuild` (any line) | foreign keys off (after a storage sync: the pragma is a no-op inside the event's implicit transaction), re-set on every new session; then Rust's `pragma_foreign_key_check`, **one table at a time over several events** (the whole-database form runs out of SQLite memory on the Whagons schema and resets the object); violation → Rust's text and a restore; refused with no-transaction |
| `install_tenant_identity` refuses `members.user_id` | same text |
| `reference_integrity::after_migration` | no-op without a declared catalog; a declared one fails loudly (K07 owns the ledger engine; Whagons declares none) |
| Transform version (0014 rule 1) | KV `gonvex/migrate/applied/<name>`, beside Rust's table |

## Record capture across chunks

On a tenant that already has capture, a migration's writes stage first-before record images in
`_gonvex_record_pending`. Its deferred guard foreign key fails any commit before `finalize`, and on the platform a
deferred-FK failure at commit resets the object. Between the chunks of one migration the rows wait in
`_gonvex_st_migrate_record_stash`, which exists only mid-migration, and the next chunk puts them back. The migration
still flushes once, with the first images, as Rust's single transaction does.

## Files

| File | |
|---|---|
| `migrations.ts` | parsing, directives, checksums |
| `build.ts` | `buildReleasePlan` (imports the transform) |
| `plan.ts` | plan types, `MemoryPlanStore`, `R2PlanStore` |
| `migrator.ts` | `TenantMigrator`, the per-object step |
| `capture-port.ts` | the K03 interface it calls |
| `runtime-sql.ts`, `runtime-prelude.sql` | generated from the Rust sources (`verify/generate-runtime-sql.mjs`); `test/runtime-sql.test.ts` fails on drift. The A01 drift job uses `runtime-prelude.sql`. |
| `verify/` | `rust-reference/` (a fresh tenant provisioned by `gonvex-store`), `worker/` (verification Worker with K03's capture), `provision.mjs` (provision and compare) |
