# Kernel schema transform (A01)

Decision 0014: the app's migrations stay unchanged, and the kernel rewrites them into DDL that a Durable Object's
SQLite accepts. This directory is that transform. It is pure TypeScript with no SQLite dependency, so it runs the
same way in the Worker, in Node and in CI.

## Interface (for K02 and K03)

```ts
import { SchemaTransform, TransformError, TRANSFORM_VERSION } from "./index.ts";

const transform = new SchemaTransform();              // one instance per schema history
for (const m of migrations) {                         // in ledger order, every time (it is cheap and deterministic)
  const out = transform.migration(m.name, m.sql);     // throws TransformError
  // out.chunks: string[][]  -> one storage transaction per chunk (T7)
  // out.foreignKeyRebuild   -> `-- gonvex:foreign-key-rebuild`: apply with foreign keys off, then check
  // out.directives          -> every `-- gonvex:` line, verbatim
  // out.version             -> TRANSFORM_VERSION, recorded in the ledger metadata
}
const rt = transform.runtime("capture", ddl, { records, tables });   // runtime-provisioned DDL (K03)
transform.addedObjects();                             // names M02 and checksum tools exclude
```

| Export | Use |
|---|---|
| `SchemaTransform` | Stateful over one schema history. `migration(name, sql)` for migration files, `runtime(name, sql, catalog?)` for DDL the runtime generates, `image(name, sqliteSchemaRows, catalog?)` for an existing tenant file's schema. Each returns a `TransformedMigration`. |
| `TransformedMigration` | `statements` (apply order), `chunks` (T7), `directives`, `foreignKeyRebuild`, `sourceStatements`, `rewrites` (rule and object per rewrite), `version`. |
| `TransformError` | `migration`, `object`, `limit` and `detail`. Every unhandled construct, and any output that still breaks a limit, throws one. Nothing is skipped. |
| `transformMigrations(list)` | Convenience: a fresh transform over a whole list. |
| `checkStatement(sql)` / `LIMITS` | The static limit checker every output statement passes through. |
| `recordFlushSql(record)`, `RECORD_PENDING_NAMES_SQL`, `RECORD_PENDING_CLEANUP_SQL` | T5's commit-time flush, replacing Rust's `FLUSH_SQL` (see below). |
| `rowImage`, `captureValueSql`, `recordImage`, `jsonKind`, `recordView` | T5 building blocks: plain-SQL row and record images typed as Rust's `cell_json` types them. |
| `ADDED_PREFIX` | `_gonvex_st_`: every object the transform adds starts with it (`_cf_` is reserved by the platform). |

The catalog (`CatalogInput`) matters only for T5. `records` are `_gonvex_record_views` rows. The transform also learns them
from `INSERT INTO _gonvex_record_views VALUES (...)` in migrations. `tables` are column shapes
(`pragma_table_info` order) of tables whose capture triggers call `gonvex_capture`.

Determinism: the output is a function of the input statements, the options and the catalog. It is the same byte for
byte on every run, which is tested. Apply only the chunks of migrations that are not yet in the ledger, but always feed
the transform the whole history first, because later statements depend on what earlier ones created.

## Rewrites

| # | Construct | Rewrite |
|---|---|---|
| T1 | Compound SELECT over 5 terms | Left-nested chain: `SELECT * FROM (t1 … t5) op t6 …`, repeated. |
| T2 | View over 100 KB effective (quotes doubled) | Partition a CTE union view by branch into `__partN` views under the limit; the view becomes its final SELECT over their union. |
| T3 | Expression depth over 100 (deep `replace()` chains) | Views and trigger statements: a recursive CTE fold applying the same pairs in order. Index: chained VIRTUAL generated columns of at most 90 levels; the index keeps its name, `CASE` and `WHERE`, so the uniqueness error is unchanged. `DROP INDEX` drops the columns again. |
| T4 | `CREATE TEMP TABLE` | An ordinary table. It is dropped at the end of the migration if the migration did not drop it, and T7 keeps its lifetime in one chunk when it fits. |
| T5 | `gonvex_record_value`, `gonvex_capture` | `json_object` images in column order. In triggers, the Rust function's errors (non-text key, duplicate keys) are raised with the same text. |
| T6 | Statements that read a reference union view | See below. |
| T7 | A migration too large for one storage operation | Chunks of at most 200 statements and 1 MB. Atomicity across chunks is K02's restore point. |

### T6: per-branch reference statements

For a view `V` of the reference-integrity shape (a CTE chain whose last CTE is a `UNION ALL` of k ≥ 2 branches, read by a
final aggregate), the transform emits:

- `_gonvex_st_V__b0 … bk-1`: one view per branch, each with only the CTEs that branch needs (`MATERIALIZED` kept);
- `_gonvex_st_V__rows`: a scratch table with the union's columns, with no declared types, so values are stored unchanged;
- `_gonvex_st_V__queue`, with `_gonvex_st_V__fillN` AFTER INSERT triggers. Each trigger runs one `INSERT INTO rows SELECT * FROM bi`
  per branch, for a few branches each, so every trigger program stays small;
- `_gonvex_st_V__expected`: `V`'s own final aggregate, over the scratch rows;
- `V` itself, redefined as the final aggregate over the union of the branch views, for any other reader.

A statement that reads `V` (a trigger body statement or a top-level backfill) becomes:

```sql
DELETE FROM rows; INSERT INTO queue(n) VALUES (NULL); DELETE FROM queue;   -- fill: one INSERT per branch
<the statement, reading expected instead of V>;
DELETE FROM rows;                                                        -- clear
```

The fill reads the same inputs `V` would read at that moment, so each statement sees what `V` would have returned. A
trigger `WHEN` clause cannot fill, so the guards on `referenceBindings_*` read `expected` while the statement that
writes the bindings table holds the scratch rows. The transform fails loudly if a statement writes a guarded table
without filling for the guard's view. No statement reads more than one branch.

## Commit-time flush (T5, for K03)

Rust's `FLUSH_SQL` calls `gonvex_record_value(table_name, row_id)` with a dynamic record name. Without custom functions,
the flush is one statement per record name:

```ts
for (const { table_name } of sql.exec(RECORD_PENDING_NAMES_SQL)) sql.exec(recordFlushSql(records.get(table_name)));
sql.exec(RECORD_PENDING_CLEANUP_SQL);
```

`_gonvex_record_pending` is `WITHOUT ROWID` with key `(table_name, row_id)`, so Rust's single statement emits events in
that key order. Running the names in ascending binary order and each name in `row_id` order gives the same order.

## Verification

Tests: `pnpm test` in `cloudflare/` runs `test/*.test.ts` inside workerd against real Durable Object SQLite. For each
rewrite, the original construct is refused and the transformed DDL loads and returns the same results. For T6, a
12-branch reference layer gives identical write outcomes, bindings and guard errors to the untransformed layer.
Another test checks that no chunk ends inside a deferred-FK span or a T6 fill.

`verify/` holds the checks behind the A01 report. Tenant files are copied read-only, kept outside the repository and
deleted afterwards.

| Script | Checks |
|---|---|
| `transform-migrations.mjs <dir> [--prelude f]` | Transforms a migrations directory; exits 1 with the `TransformError`. The CI job `transform-drift` runs it on whagons5-client `staging`. |
| `verify-tenant.mjs <tenant.db> --work d --rust-bin b` | Builds the transformed schema over a copy of the tenant's rows and compares it with the original. Every rewritten view must give the same rows (multiset, with storage classes), with every owner in scope, and T6's `expected` view must match after a fill. Rewritten index keys must match on every row and on a 10,000-string Unicode fuzz corpus. Writes generated from the file are replayed on both: on the original through the Rust runtime's own capture (`rust-replay/`, `gonvex-store` `capture::install` plus `feed::finalize`), on the transformed schema through T5's flush. The replay requires the same outcome and error for every write, including a duplicate live role name, the same change rows, and then the same content in every table. Values taken from the clock during the run are masked on both sides. |
| `rust-replay/` | The Rust side of that replay. It is a standalone crate, like P03's stage tool, with the runtime's serde_json features (`preserve_order`, the vendored patch). |
| `load-object.mjs migrations\|compile <base> <object> …` | Loads the transformed migrations into an object (one storage transaction per T7 chunk), then the runtime-generated objects. `compile` prepares every table's INSERT, UPDATE and DELETE and selects from every view. It uses the S1 spike Worker's routes. |
| `platform-writes.mjs <base> <object> <tenant.db>` | Write cost on an object: record writes with T5's flush, timed, with rows written. `verify-tenant.mjs --build-only --no-t6` builds the baseline without T6. |
| `app-source-checks.mjs <client>` | Part of the drift job (0022 §2a, §5). Every REGEXP pattern in the app's module sources must compile with K01's translator (`kernel/storage/regexp.ts`). No module source may name a storage table that carries T3's generated columns. Run it with `node --experimental-transform-types`, because K01 uses parameter properties. |
| `export-roundtrip.mjs <tenant.db> …` | P03 export from a kernel-provisioned object (0022 §5). Push the transformed file, `pull`, `stage` out, then require exactly Rust's schema, no added objects or hidden columns, no stage findings, and equal checksums. P03's `stage` skips `_gonvex_st_*` tables like its other never-copied tables. |
| `build-transformed.mjs` | Shared: turns a copy of a tenant file into the kernel's schema in place. |
| `strict-probe.mjs <base> <object>…` | Reads the 22 reference views S1 saw fail on strict hosts, to tell whether an object sits on one. |
| `strict-hosts.mjs <base> <n>` | On fresh objects, a 12-branch reference layer with T1 only and then with T6: same writes, same bindings. |

The P02 harness fixture goes through the same check: run `node src/cli.mjs --keep` in `cloudflare/conformance` with a
`gonvex-runtime` build, then run `verify-tenant.mjs` on each `rust-a/data/projects/*/tenants/*.db`. Those tenants use
`gonvex_capture` table capture, so the replay compares T5's row images with the pre-update hook's. See
`docs/cloudflare/contract-changes/0023-*` for the one difference (columns added by `ALTER TABLE` on old rows).
