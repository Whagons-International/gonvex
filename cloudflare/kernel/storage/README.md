# Kernel storage layer (K01)

The tenant object's SQLite with the Rust runtime's module SQL semantics, on Durable Object SQLite. It is the
storage half of the module host: K04 builds the host calls on it, K03 feeds it change capture, K02 calls
`invalidateSchema()` after migrations.

```ts
const storage = new TenantStorage(ctx.storage);           // one per tenant object
const result = await storage.invoke("reducer", async (db) => {
  const rows = await db.query("SELECT * FROM tasks WHERE id = $1", [id]); // module SQL (db.query)
  db.run("INSERT INTO _gonvex_action_outbox (id, args) VALUES (?1, ?2)", [jobId, args]); // kernel SQL
  return rows;
});
// { ok, value | error, committed, stats: { statements, rowsRead, rowsWritten, catalog, schema, savepoints, foreignKeyCheck } }
```

| File | What | Reference |
| --- | --- | --- |
| `session.ts` | `TenantStorage.invoke`, `StorageSession`: transactions, savepoints, the aborted rule, read-only Queries, row counters, the FK check before commit, kernel SQL and IN-list chunking | `host_calls.rs` `SqlCalls`, `store/src/lib.rs` commit |
| `lexer.ts`, `admission.ts` | SQLite-faithful tokenizer; one statement only; no transaction control; ATTACH/DETACH/PRAGMA/VACUUM refused; read heads; write targets | `sql_admission.rs`, `host_calls.rs` |
| `parameters.ts` | `$N`/`?N`/`?` bound by number with Rust's rules; integers bound as INTEGER; 100-parameter limit | `store/src/direct.rs` `Stmt::bind`, 0011 §1 |
| `values.ts` | decoding by declared type; parameter conversion | `store/src/rows.rs`, `direct.rs` `write_json`, `sql_parameters.rs` |
| `projection.ts` | result typing through aliases, joins, subqueries, CTEs | `sql_projection.rs` |
| `catalog.ts` | table catalogs (record views, `pragma_table_info`), cached across invocations | `host_calls.rs` `catalog_table` |
| `regexp.ts` | `REGEXP` without a custom function | sqlx `regexp`, `registry.rs` `with_regexp` |
| `foreign-keys.ts` | deferred-FK model and the targeted check | 0016 G0-5 |
| `errors.ts` | sqlx's error texts | sqlx Display |

## Behavior

- **Transactions (G0-3).** One `ctx.storage.transaction(async …)` per invocation. Module `SAVEPOINT`, `RELEASE`
  and `ROLLBACK TO` become nested storage transactions on a LIFO stack; `ROLLBACK TO` reopens its savepoint, as
  SQLite does; savepoints still open at the end are released on commit and rolled back otherwise. After a failed
  statement every call fails with `current transaction is aborted…` until `ROLLBACK TO`, and a Reducer that
  returns with a failure outstanding is not committed (Rust's `ABORTED`). Admission, parameter and catalog-shape
  errors do not abort, as in Rust. A Query never commits. `discard()` is Rust's `finish(false)`.
- **Deferred foreign keys (G0-5).** A Reducer that wrote is checked before its transaction ends; a violation fails
  it with Rust's commit text, `error returned from database: (code: 787) FOREIGN KEY constraint failed`, and the
  object stays up (without the check, the platform's commit breaks the input gate and resets the object; a test
  shows both). Only deferred keys are checked (deferrability is read from each table's `CREATE TABLE`; SQLite
  fails immediate keys at the statement). Only what was written is checked: a written table as a child
  (insert/update) and as a parent (update/delete), closed over the schema's triggers and `ON DELETE/UPDATE`
  actions. In `writeTracking: "statements"` mode (today) each table is checked with
  `pragma_foreign_key_check(table)` restricted to its deferred keys. In `"rows"` mode, K03's capture reports rows
  with `noteRows(table, op, rowids)` and child rows are checked by rowid only.
- **Statements.** One statement per call (`sql.exec` would run all of them). Refusals and their texts are Rust's.
  `$N` binds by number in any order, repeated or not (0011 §1); a missing value binds NULL, and so does every
  later parameter (`direct.rs` breaks); `:name` and `$name` fail with sqlx's text. Durable Object SQL binds every
  JS number as REAL, so integer parameters are written as `+CAST(?k AS INTEGER)`: an INTEGER with no affinity,
  which stores, compares and uses indexes exactly like `sqlite3_bind_int64` (an integer into a TEXT column is
  `"3"`, not `"3.0"`). Unaliased parameter columns keep the module's name (`$1`).
- **Read-only Queries (0011 §2).** A Query statement must start with `SELECT`, `WITH`, `VALUES` or `EXPLAIN`;
  `WITH … INSERT/UPDATE/DELETE` is refused before it runs with Rust's read-only-connection text, `error returned
  from database: (code: 8) attempt to write a readonly database`, and `rowsWritten > 0` is checked after every
  Query statement as a backstop.
- **100 bound parameters (0011 §3).** Module SQL over 100 parameters fails with an error naming the limit, except
  a FROM-less `SELECT expr, expr, …` (what `existsDataRows` builds), which runs in groups of items under 100 and
  is joined into one row. Kernel IN lists use `runInChunks` (runtime `in`/`oneOf`).
- **Values.** Rows decode by declared type exactly as `write_query_rows`: the projection's kinds, else the write
  target's catalog for `RETURNING`, else expression-cell decoding (JSON-shaped TEXT parsed). JSON columns parse,
  BOOLEAN columns become booleans, BLOB is lowercase hex, non-finite REAL is null.
- **REGEXP.** `x [NOT] REGEXP p` with `p` a bound parameter or literal is compiled from the pattern into GLOB,
  `length` and `substr`: NULL unless both sides are TEXT (sqlx), otherwise Rust's `is_match`. Supported: anchors,
  literals and escapes, `.`, bracket classes, quantifiers, plain groups, with at most one variable-length part
  between `^` and `$`. That covers Whagons' `^PR-[0-9]+$` (`purchasing/common.ts:164`). Other patterns fail with
  an error that says why.
- **Row counters.** Every statement's `rowsRead`/`rowsWritten` add to `stats`, with catalog and schema reads
  also counted on their own; `run`/`explain` return per-statement counts.

## Differences from Rust that the platform forces

1. Module SQL binding more than 100 parameters fails (0011 §3), except the FROM-less SELECT split above.
2. `REGEXP` covers the subset above; Rust accepts any `regex` pattern.
3. No `sqlite3_column_decltype`. Where Rust's projection inference leaves a column open and decltype would name a
   type, this layer adds two inferences decltype would agree with (a column shared through `USING`/`NATURAL`, and
   `json_each`/`json_tree` columns) and otherwise decodes the column as an expression.
4. Integers outside ±2^53 are not exact in a JS number (module code sees JS numbers in Rust too).
5. In statement mode, the table-level FK check also sees a violation that predates the invocation in a table the
   invocation wrote (SQLite's commit counter only counts new ones). Row mode (K03) does not.
6. The catalog cache cannot check `schema_version` (not authorized on Durable Objects): module DDL invalidates it,
   and anything else that changes the schema must call `invalidateSchema()`.
7. A statement that mixes `$1` and `?1` (one SQLite slot) gets both columns named `$1`.

## Numbers

Deferred-FK check, local workerd, 100,000 child rows, 1,000 parents (`test/foreign-keys.test.ts`, "measure it"):

| Check | Statements | Rows read (billed) | Time |
| --- | --- | --- | --- |
| Table level, one child inserted (statement mode) | 1 | 0 | ≈ 16 ms |
| Parent side, one parent deleted (scans the children) | 1 | 0 | ≈ 16 ms |
| Row level, one child reported (row mode) | 1 | 4 | < 1 ms |
| Whole database `pragma_foreign_key_check` (for comparison) | 1 | 0 | ≈ 15 ms |

The FK model (foreign keys, deferrability, trigger SQL) is read once per schema generation: one pass over
`sqlite_schema` joined with `pragma_foreign_key_list`, one over the triggers. Catalogs are cached across
invocations, so a warm Query reads only its own rows (S2 measured 95–99 % of a Reducer's rows read in catalog
lookups).

## Tests

`pnpm test` in `cloudflare/` runs them in workerd against real SQLite-backed Durable Objects:

- `test/admission.test.ts`: the Rust admission tests, Query heads, statement shapes.
- `test/statements.test.ts`: `$N` binding, parameter types, the 100-parameter limit, errors, decoding, catalogs.
- `test/regexp.test.ts`: 29 patterns × 42 strings against a regex oracle, NULL rules, refusals.
- `test/transactions.test.ts`: commit, rollback, the aborted rule, savepoints (S2's cases), read-only Queries.
- `test/foreign-keys.test.ts`: the G0-5 check, the reset it prevents, triggers, cascades, cost.
- `test/sdk/`: the SDK's SQL suites on this layer (`packages/module-sdk/test/data-write-sqlite.test.mjs`,
  `packages/local-runtime/src/index.test.ts` and `portable-conformance.test.ts` with
  `packages/local-runtime/test/sqlite-reference.ts` ported to `do-reference.ts`), and SDK helpers that reach
  the platform limits. They run the SDK from its sources and, like the originals, are not type-checked under
  `cloudflare/tsconfig.json`.

Conformance: `cloudflare/conformance/scenarios/storage-sql.mjs` with the fixture module
`fixtures/app/gonvex/storage.ts`.
