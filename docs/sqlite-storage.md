# SQLite storage

Gonvex stores each tenant in its own SQLite file and the Control Plane
(landlord) in one SQLite file. The runtime process owns those files. There is
no database server, connection pool budget, or network round trip on the
query path.

This document is the contract for the storage layer and for every SQL
statement in the runtime, the module SDK and application migrations.

## Why

Measured on the same two cores with a Gonvex-shaped workload (2026-11-02,
`/mnt/data/work/storage-bench`):

| | Postgres (sqlx) | SQLite (in process) |
|---|---|---|
| Point read CPU | 117–182 µs | 4–29 µs |
| 100-row grid read CPU | 620–785 µs | 127–180 µs |
| Write (read, update, change row) CPU | 308–445 µs | 19–67 µs |
| Open connection + first query | 12.5 ms | 0.9 ms |
| 26 tenants, users acting every ~2 s | ~10,000 users held | ~20,000 users held |

The connection-pool failure modes (exhaustion, churn, `too many
connections`) disappear because there is no pool to exhaust.

## Files and routing

- `GONVEX_DATA_DIR` (default `./data`) holds every database.
- Control Plane: `{data}/control.db`.
- Multi-tenant project: `{data}/projects/{project}/tenants/{tenant}.db`.
- Single-database project: `{data}/projects/{project}/project.db`.
- `TenantRoute.database_url` and project/tenant rows store
  `sqlite://{absolute path}`. Browser and module inputs never carry it.
- One runtime process owns a data directory. It takes an exclusive lock file
  (`{data}/.gonvex.lock`) at start and refuses to start if another process
  holds it. Deploys stop the old process before the new one opens the files.
  Rolling deploys that run two runtimes on one volume are not supported.

## Connections and transactions

Every database file has:

- **one writer connection** (`max_connections = 1`). All writes and every
  Reducer transaction run here, one at a time. SQLite allows one writer;
  serializing in-process avoids `SQLITE_BUSY` entirely.
- **a reader pool** (`read_only = true`, default 4 connections). Queries,
  replica snapshots and Live Queries run here. In WAL mode each read
  transaction sees one consistent snapshot, which replaces Postgres
  `REPEATABLE READ READ ONLY`.

Pragmas on every connection: `journal_mode=WAL`, `foreign_keys=ON`,
`busy_timeout=5000`, `synchronous` from `GONVEX_SQLITE_SYNCHRONOUS`
(`full` default; `normal` is allowed and loses at most the last commits on
power loss, never on a process crash), `cache_size=-65536`,
`temp_store=MEMORY`. Automatic checkpoints stay on (1,000 pages).

`TenantTransaction::commit` finalizes the change feed inside the same
transaction (see below), commits, and only then publishes the revision. A
caller never sees a revision that did not commit.

## Change feed

1. Every application table gets `AFTER INSERT/UPDATE/DELETE` triggers that
   append to `_gonvex_sync_changes` with `revision IS NULL`. Row images are
   built with `json_object` using each column's declared type (below), so a
   JSON column is embedded as JSON and a BOOLEAN as `true`/`false`.
2. Before `COMMIT`, the transaction checks for unrevisioned changes (partial
   index on `revision IS NULL`). If any exist it increments
   `_gonvex_sync_clock.revision`, stamps revision, ordinal and command id on
   those rows with one `UPDATE … FROM (row_number() OVER …)`, and inserts the
   `_gonvex_sync_transactions` provenance row from values held on the
   `TenantTransaction` (this replaces Postgres GUCs and `txid_current`).
3. After `COMMIT`, the registry publishes `{epoch, revision, tables}` on the
   database's in-process broadcast channel (this replaces `pg_notify`).
   `ChangeFeedHub` subscribes to that channel instead of `LISTEN`.
4. Pruning (30-day retention) is unchanged in meaning.

All tenant writes must go through a `TenantTransaction`; a write that
bypasses it would leave unrevisioned rows for the next commit to adopt.

## Locks and notifications

- Advisory locks become `PoolRegistry::lock(database, key)`, an in-process
  keyed async mutex held for the critical section.
- `FOR UPDATE` and `SKIP LOCKED` are removed. The single writer already
  serializes claims; a claim is `UPDATE … WHERE id IN (SELECT … LIMIT n)
  RETURNING …`.
- `ControlPlane::notify` / `listener` keep their signatures and use the
  in-process broadcast channels.

## SQL dialect rules

| Postgres | SQLite |
|---|---|
| `jsonb` / `json` column | declared `JSON`, stored as JSON text; read through the type-aware decoder or `json(col)` |
| `boolean` | declared `BOOLEAN`, stored `0`/`1` |
| `bigint`, `integer` | `INTEGER` |
| `double precision`, `real` | `REAL` |
| `numeric` | `REAL` (or `TEXT` when exactness matters) |
| `timestamptz` / `now()` | `INTEGER` Unix milliseconds; `now()` is `CAST(unixepoch('subsec')*1000 AS INTEGER)` (`gonvex_store::sql::NOW_MS`) |
| `interval '5 minutes'` | millisecond arithmetic (`- 300000`) |
| `text[]`, `= ANY($1)` | JSON array text; `IN (SELECT value FROM json_each($1))` |
| `jsonb_build_object`, `to_jsonb(row)`, `row_to_json` | `json_object(...)`, generated by `gonvex_store::json_row` from declared types |
| `jsonb_agg` | `json_group_array` |
| `->`, `->>` | same operators (SQLite 3.38+) |
| `@>` containment | `EXISTS (SELECT 1 FROM json_each(col) WHERE value = $n)` or generated equivalent |
| `ILIKE` | `LIKE` (ASCII case-insensitive); `ESCAPE '\'` is supported |
| `strpos(a,b)` | `instr(a,b)` |
| `GREATEST`/`LEAST` | scalar `max()`/`min()` |
| `md5(random()::text)` | `lower(hex(randomblob(16)))` |
| `bigserial` / identity | `INTEGER PRIMARY KEY` (`AUTOINCREMENT` only where ids must never be reused) |
| `::type` casts | removed, or `CAST(x AS type)` |
| `CREATE DATABASE` | create the file |
| `information_schema`, `pg_catalog`, `to_regclass` | `sqlite_schema`, `pragma_table_info(t)`, `pragma_index_list(t)` |
| `pg_advisory_*` | `PoolRegistry::lock` |
| `LISTEN`/`NOTIFY` | in-process broadcast |
| `DO $$ … $$`, plpgsql functions | Rust code, or SQLite triggers |
| `ADD COLUMN IF NOT EXISTS` | check `pragma_table_info` first |

`$1`-style numbered placeholders work unchanged (verified: reuse and
out-of-order binding). `ON CONFLICT … DO UPDATE`, `EXCLUDED`, `RETURNING`,
`UPDATE … FROM`, window functions, CTEs and `json_each` all work on the
bundled SQLite 3.46.

## Declared types and JSON rows

The declared column type from `pragma_table_info` is the source of truth for
encoding a row as JSON, both in SQL (`json_object`) and in Rust
(`gonvex_store::decode`):

| Declared type contains | JSON value |
|---|---|
| `JSON` | parsed JSON |
| `BOOL` | `true` / `false` |
| `INT` | integer |
| `REAL`, `DOUB`, `FLOA`, `NUMERIC` | number |
| anything else | string |

## Migrations

Application migrations are SQLite SQL files with the existing
`-- gonvex:scope` header. Applied scripts run with `sqlx::raw_sql`, so
trigger bodies need no statement splitter. Checksums and the
`gonvex_migrations` ledger are unchanged in meaning. Whagons regenerates one
SQLite baseline from `gonvex/_generated/tenant/schema.ts`; its Postgres
migration history is not replayed.

## Moving existing data

`gonvex-admin export-sqlite` moves a Postgres deployment into a data
directory: the Control Plane becomes `control.db`, and every project or tenant
database becomes its file under `projects/`.

```sh
gonvex-admin export-sqlite \
  --control-plane-url "$OLD_GONVEX_CONTROL_PLANE_DATABASE_URL" \
  --data-dir /srv/gonvex/data \
  --migrations ./migrations            # the app's SQLite migrations
# options:
#   --migrations PROJECT_ID=DIR   per-project migrations (repeatable)
#   --project ID / --tenant ID    export only these databases (repeatable)
#   --no-control-plane            keep an existing control.db; export databases only
#   --force                       replace existing control.db / database files
#   --allow-drop-columns          skip Postgres columns that have no SQLite column
#   --allow-drop-tables           skip Postgres tables that have no SQLite table
#   --keep-epoch                  keep the source sync epoch (see below)
#   --sample-rows N               rows compared per table (default 200; 0 = off)
#   --source-url-map FROM=TO      rewrite a source URL prefix (e.g. a Docker host)
#   --report FILE                 also write the JSON report to FILE
```

Stop the Postgres runtime first. The tool reads each source database in one
`REPEATABLE READ READ ONLY` snapshot, holds the data-directory lock while it
runs, and refuses to overwrite `control.db` or a database file without
`--force`.

**Schema.** The tool never translates DDL. It builds each file with the
runtime's own provisioning: `ControlPlane::connect` (Control Plane schema)
plus the control-plane-scoped migrations, then `provision_tenant_database`
(members, reference ledger, the app's tenant migrations, change capture).
Each file is exactly what the runtime would create; only rows are copied.

**Routing.** `gonvex_runtime_projects` rows point at
`projects/{project}/project.db`; a tenant routed to its project's database
(single-database projects) shares that file; any other tenant gets
`projects/{project}/tenants/{tenant}.db`. `database_url` becomes the
`sqlite://` URL and `database_name` the path relative to the data directory,
so no Postgres URL or password survives in `control.db`. Deleted tenants are
rewritten but not exported. Two live tenants sharing one Postgres database
cannot be split into files and are refused.

**What is copied.**

| Copied | Not copied |
|---|---|
| Every Control Plane table | `gonvex_migrations` (the SQLite ledger lists the SQLite migrations) |
| Every application table, including `members` | `_gonvex_sync_changes`, `_gonvex_sync_transactions` |
| `_gonvex_action_outbox` (pending and in-flight rows) | `_gonvex_reference_*` (rebuilt, below) |
| `_gonvex_reducer_idempotency` (or legacy `_gonvex_mutation_idempotency`) | |
| `_gonvex_control_plane_membership_outbox`, `_gonvex_files` | |

Rows that migrations seeded into a copied table are replaced by the source
rows. A Postgres table or column with no SQLite counterpart stops the export
unless `--allow-drop-tables` / `--allow-drop-columns` is given; a SQLite
column missing in Postgres takes its default (a `NOT NULL` column without a
default stops the export).

**Change feed.** History is not copied. Old change rows hold Postgres-encoded
row images (ISO timestamps, `t`/`f`) that would disagree with the converted
rows, and replaying them would need Postgres transaction ids. The clock keeps
the source revision with `retained_revision = revision` and an empty
`_gonvex_sync_changes`, and a new epoch, so every client cursor is outside the
retained window and the client resyncs with a snapshot of converted rows.
`--keep-epoch` keeps the epoch; clients already at the head revision then keep
their cached (Postgres-encoded) rows, so use it only when nothing changed
encoding.

**Triggers.** Change-capture, reference-staging and membership-projection
triggers are dropped for the copy (so copied rows create no change, staging or
outbox rows) and restored verbatim in the same transaction. Application
triggers stay active (FTS indexes fill as rows arrive); an application trigger
that writes into another copied table makes that copy fail with a duplicate
key. After the copy the store's installer runs again and must accept every
file as current.

**Reference ledger.** The catalog the SQLite migrations declared (or, if they
declared none, the Postgres catalog) is re-declared with
`reference_integrity::declare`, which inventories the copied rows. Legacy
dangling references stay visible in `gonvex_reference_violations` without
blocking.

**Conversion** is decided by the SQLite column's declared type:

| Postgres | SQLite column | Stored |
|---|---|---|
| `timestamptz`, `timestamp` (as UTC) | INTEGER | Unix ms (microseconds floored) |
| `timestamptz` | TEXT | RFC 3339 (`…Z`) |
| `date` | INTEGER / TEXT | Unix ms of UTC midnight / `YYYY-MM-DD` |
| `boolean` | BOOLEAN / INTEGER | `0` / `1` |
| `jsonb`, `json`, arrays | JSON | minified JSON text (`json()`); arrays via `array_to_json` |
| `numeric` | REAL / TEXT / INTEGER | number / exact text / integer only if integral |
| `uuid`, enums, `text`, other | TEXT | `::text` |
| `bytea` | BLOB | bytes |

A value that cannot be stored exactly (a fractional numeric in an INTEGER
column, `NaN`, an integer above 2^53 in a REAL column, unparsable text in a
BOOLEAN column) stops the export with the table and column. Text in a JSON
column is kept if it is valid JSON and stored as a JSON string otherwise.
Change capture and the reference ledger encode rows with `json_object`, which
rejects BLOB values, so application tables must not declare BLOB columns.

**Verification.** For every copied table, against the same snapshot: row
counts must match, and `--sample-rows` rows (stable pseudo-random order by
primary-key hash) are converted again and compared with the stored SQLite row
as canonical JSON. Each file must pass `PRAGMA integrity_check` and
`PRAGMA foreign_key_check`, have no staged change rows, and be the file
`control.db` routes its tenant to. The tool prints a JSON
report and exits non-zero on any finding.

## Module SDK statements

The structured data helpers in `@gonvex/module-sdk` (`data.ts`,
`data-write.ts`) send only identifiers they validated and `$n` parameters.
The runtime's `ctx.db.query` must provide:

- **Parameter binding.** `null` → NULL, `true`/`false` → `1`/`0`, integers →
  INTEGER, other numbers → REAL, strings → TEXT, arrays and objects → JSON
  text. The SDK already passes every list as one JSON text parameter
  (`IN (SELECT value FROM json_each($n))`), never as a bound array.
- **`regexp(pattern, value)`** registered on every connection, so
  `column REGEXP $n` works (`matches` predicates). Pattern syntax is the
  portable subset the SDK validates.
- **Savepoints.** `applyDataWrites` runs its operations as separate
  statements between `SAVEPOINT gonvex_data_writes` and
  `RELEASE gonvex_data_writes` (`ROLLBACK TO` on error) inside the Reducer
  transaction, so a caught failure leaves no partial batch.
- **Declared types in results.** Rows, including `RETURNING *`, are decoded
  by declared type. Expression columns (`EXISTS (…)`, `count(*)`) have none
  and arrive as integers; the SDK accepts `0`/`1` for booleans.

`lockData` and `DataRead.lock: "update"` emit no SQL: one writer per tenant
already serializes Reducers. Batched writes convert JSON input per column
with `pragma_table_info`: a column whose declared type contains `JSON` takes
`value -> '$.col'` (JSON text, JSON null as SQL NULL); others take
`value ->> '$.col'`. `packages/local-runtime/test/sqlite-reference.ts` runs
these statements on real SQLite against the portable engine.
