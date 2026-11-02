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

`gonvex-admin export-sqlite` reads a Postgres tenant or Control Plane
database and writes the equivalent SQLite file, converting timestamps to
milliseconds, booleans to integers and `jsonb` to JSON text, then verifies row
counts per table.
