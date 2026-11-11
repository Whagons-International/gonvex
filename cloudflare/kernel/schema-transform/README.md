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

- `test/*.test.ts` run inside workerd against real Durable Object SQLite (`pnpm test` in `cloudflare/`). For each
  rewrite, the original construct is refused and the transformed DDL loads and returns the same results. For T6, a
  12-branch reference layer gives identical bindings and guard errors to the untransformed layer.
- `verify/transform-migrations.mjs <dir> [--prelude f]` transforms a migrations directory and prints a summary. It
  exits 1 with the `TransformError` on an unhandled construct.

The tenant-data checks (all 15 staging tenants: views `EXCEPT` both ways, index keys and a Unicode fuzz corpus, reference
bindings and change rows against the Rust runtime on the same writes, platform load and strict-host compile) and the CI
drift job land in the second A01 pull request.
