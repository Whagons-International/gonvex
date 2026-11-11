# SQL migrations

Gonvex applies versioned SQL migrations during the same `/dev/sync` operation that installs an application's schema and functions. Projects without a `migrations/` directory retain the previous sync behavior.

## Files and scope

Put migrations in the project-root `migrations/` directory and name them `NNNN_description.sql`, for example `0001_add_search_vector.sql`. Gonvex includes these files in the source bundle and bundle hash, validates them, and applies them in lexical filename order.

Every file must begin with one of these header comments before its first SQL statement:

```sql
-- gonvex:scope tenant
```

```sql
-- gonvex:scope control-plane
```

The parser's safe fallback is tenant scope, but sync rejects a missing
directive so the intended database is reviewable. A tenant migration runs
against every registered tenant database for the project. A Control Plane
migration runs once against the project's Control Plane database.

## Tracking and edits

Each database has a `gonvex_migrations` table with the migration filename, SHA-256 checksum, application timestamp, and duration in milliseconds. An applied filename is never run again. Never edit an applied migration: if its current checksum differs from the recorded checksum, sync fails. Add a new migration instead.

Each migration runs in one transaction by default. A statement failure rolls back both its SQL and tracking row.

During sync Gonvex first reconciles the additive declarative schema, then runs SQL migrations, and activates the new function bundle only after both succeed. A tenant database created later receives the deployed tenant schema and all tenant migrations before it is marked ready.

Migrations are SQLite SQL (see the dialect rules in [SQLite storage](sqlite-storage.md)). Operations that cannot run inside a transaction, such as `VACUUM`, require this header:

```sql
-- gonvex:scope tenant
-- gonvex:no-transaction

VACUUM;
```

In no-transaction mode Gonvex executes top-level statements individually and records the migration only after every statement succeeds. Earlier statements can remain after a later failure, so every statement in such a migration should be safely retryable (`IF EXISTS`/`IF NOT EXISTS` where SQLite supports it). Gonvex's splitter understands quoted strings, quoted identifiers, comments, and trigger bodies.

Tenant rollout continues after an individual tenant fails so healthy tenant databases are not blocked. Gonvex collects every failure, identifies its tenant, and fails the overall sync with HTTP 422; the response's `tenants` array gives each tenant's `status` (`provisioned` or `failed`, with its `error`). The runtime manifest is not activated when any migration fails. Control Plane failure stops before tenant rollout.

Applied migrations cannot be rolled back, so after a partial failure the healthy tenants run the previous function bundle on the new schema until the next successful sync. Write migrations and bundles expand/contract compatible: the previous bundle must work on the migrated schema. To recover, repair the failed tenant's data and sync again; tenants that already applied a migration skip it.

At startup the runtime provisions every tenant with the saved manifest. A tenant whose provisioning fails is logged and taken out of service: sessions and other requests for it are refused with `tenant "<id>" is unavailable until an operator repairs its database and redeploys`, background work skips it, and the authenticated `/healthz` diagnostics list it under `runtimeManifests.unavailableTenants`. The other tenants keep serving. A later successful sync brings the tenant back. Startup still fails if every tenant of a project fails, since that indicates a broken runtime build or migration.

A tenant migration containing the line `-- gonvex:foreign-key-rebuild` runs with foreign keys disabled, must be transactional, and must leave no `pragma_foreign_key_check` violation. Inside the same transaction Gonvex validates `_gonvex_table_options` and reinstalls change capture, so an invalid declaration rolls the migration back. Membership triggers on `members` are restored after every batch that applies a migration.

A rebuild copies the tables it rebuilds and leaves the old pages in the file as free pages, which makes scans and replica snapshot builds slower until the file is vacuumed. After a batch that applied a rebuild, while it still holds the tenant's writer, Gonvex runs `VACUUM` on its own connection (temporary copy on disk, not in memory) when free pages exceed 20% of the file and 8 MiB, then truncates the WAL. Files whose live data exceeds `GONVEX_REBUILD_VACUUM_MAX_BYTES` (default 2 GiB) are skipped. Each compaction or skip is logged with its sizes and time. It is best effort: a failed VACUUM leaves the committed migration and the free pages in place.

Applied migrations are logged and returned in the `/dev/sync` JSON response. To inspect pending migrations without applying SQL, schema changes, or a runtime bundle, run:

```sh
gonvex dev --once --dry-run
```

Dry run still connects to each target database to compare tracking records and checksums.

## Empty undeclared columns

The declarative schema can optionally remove a column that the current schema no longer declares, but only when the column contains no non-NULL values. A constant, redundant, false, zero, or empty-string value is data and is never considered empty. Non-empty undeclared columns remain in place and produce the existing warning. Declared columns are never candidates.

This behavior is disabled by default. Enable it on the runtime with:

```sh
GONVEX_DROP_EMPTY_UNDECLARED_COLUMNS=true
```

The opt-in is deliberately runtime-side: an older or rolled-back application bundle must not gain destructive behavior merely by being deployed. Operators should enable it only where deploy ordering and rollback policy make declarative cleanup acceptable. Explicit destructive or data-aware cleanup belongs in a versioned SQL migration.

For tenant tables, Gonvex enumerates every registered tenant database and approves a drop only when the same undeclared column is empty in all of them. It then rechecks emptiness immediately before each physical drop. This fleet-wide rule avoids intentionally creating per-tenant schema drift. If any tenant has a non-NULL value, every tenant keeps the column. Control Plane tables receive the same undeclared-and-empty checks against the single Control Plane database and are also protected by the opt-in flag.

## Primary keys

Primary keys are immutable outside migrations. A row's key is its identity in the change feed and in every client replica, so changing it in place would orphan both.

- `ctx.db.update(table, id, patch)` ignores a patch entry that repeats the row's own key (callers often pass a whole record) and rejects one that changes it. A patch that only repeats the key writes nothing and returns the row.
- An SQL `UPDATE` that changes the primary key of a captured table fails with `gonvex_capture: an UPDATE cannot change the primary key`.
- Migration transactions are exempt, so a repair migration can still re-point keys.

To change a key, delete the row and insert a new one.
