# X10: migration alarms and P03 transfer pages inside a CPU budget

Goal: every legitimate tenant-runtime event fits the ruled `cpu_ms = 5000` (0022 §5b, 0089), so the lead can lower
`TENANT_CPU_LIMIT_MS` from 30000. This PR changes no `cpu_ms`.

## Cause (reproduced locally)

- The migrator stopped an event after `maxMsPerEvent` (1500 ms) of `Date.now()`. On the platform the clock does not
  advance during synchronous work (R25 #8), so the check never fired and each alarm ran `maxChunksPerEvent` (4) whole
  plan chunks. On a schema of ~6.7k objects one DDL statement costs about 9 ms locally, so a 200-statement chunk is
  about 1.8 s and an alarm about 7 s. Dev measured 3,970–10,588 ms for these alarms (0089).
- The change-feed install re-planned the whole catalog for every transaction (about 1.4 s locally per plan).
- P03 `checksum`/`verify` hashed up to 60 whole tables per request and `schema` ran 400 DDL statements per request,
  so their cost grew with the tenant (dev max 5,006 ms).

## Design

Time is read only after a macrotask yield (`setTimeout(0)`), when the platform clock includes the CPU spent before it.

Migrations (`kernel/migrate/migrator.ts`):

- A plan chunk runs in one or more storage transactions of `batch` statements, with the statement offset in the
  progress record, committed in the same transaction. `batch` starts at 8 and adapts toward 300 ms per transaction
  (at most doubling, at most 50). An event stops starting transactions after 1000 ms and runs at most 4.
- Unchanged: the restore point per migration (a failure after any committed transaction restores it), one finalize and
  the ledger row in the migration's last transaction with its last statements, attempt counting, the gate, foreign-key
  rebuild handling, no-transaction semantics.
- The change-feed plan is reused across transactions while `sqlite_schema` is as the migrator left it, planned again
  after the infrastructure unit (applied alone), in a new instance, and at the end. A fresh plan starts its own event.
  "Does not converge" now checks every receipt installed since the previous plan, not only the last batch's first.
- Unsplittable steps: a single statement over the event budget, or a plan over it, logs
  `gonvex.migrate.unsplittable` in Workers Logs.

P03 transport (`tools/tenant-transfer/src/transport.mjs`, `kernel/assembly/tenant-transfer.ts`):

- `schema`, `checksum` and `check` stop at a 1 s budget. `schema` answers `consumed`, the others `next`. A checksum
  cursor carries the running SHA-256 state (`src/sha256.mjs`, standard SHA-256 with saved state), so the digest equals
  node:crypto's one-pass digest and every existing checksum comparison is unchanged. `check` runs quick_check, then
  `pragma_foreign_key_check(table)` (the per-table sum is the whole-database count), then each FTS5 integrity-check.
- In the assembled runtime the `check` and `verify` cursors stay in the import state; a client cursor is ignored and any
  write clears them. `verify` answers `pending` until its page is hashed. The CLI loops on all of these.
- `insert` stays one transaction per request, bounded by the client's page (4,000 rows / 1.5 MB).
- `schema` clears the cached table plans after every transaction and at the end: a `catalog`, `read` or `checksum`
  that runs while it yields can no longer keep plans of the old schema (#471 review; on 0f1e9801 a catalog after the
  request listed 8 of 300 new tables).

## Atomicity of a split chunk (lead requirement, #471)

A plan chunk was never the unit of atomicity: A01 already cuts a migration into chunks of 200 statements, and a
migration is atomic through its restore point (0014 T7, `chunk.ts`). X10 cuts chunks further at the same kind of
boundary, and keeps every rule that made the 200-statement boundary safe:

| Requirement | How | Test (`kernel/migrate/test/migrate.test.ts`, "split chunks keep each migration atomic") |
|---|---|---|
| The restore point covers every sub-transaction | The bookmark is taken once, before the migration's first transaction (`restorePoint`, unchanged), and any failure after a committed transaction restores it: `restore` is `chunk > 0 \|\| statement > 0` (and `committedBefore` in the last transaction) | a statement throws after four committed two-statement transactions: one bookmark, taken before the first; it is the one scheduled; gate closed; next event aborts |
| A retry resumes at the saved position and never re-runs applied DDL | The statement offset commits in the same transaction as the statements; `attempt()` syncs the try count before each one | a reset emulated inside the transaction at statement 4 (its writes rolled back, nothing after it kept): a new instance resumes at 4; non-idempotent `CREATE TABLE`/`INSERT` all exist once; schema and ledger equal an uninterrupted run |
| The ledger never records a partial step | The ledger row, `after_migration`, finalize and the `applied/` KV marker are still written only in the migration's last transaction, with its last statements | both tests: no ledger row and no `applied/` marker until the end; one finalize per migration |
| Statements that depend on each other stay in one transaction | `cut()` never ends a transaction inside a unit. Units are A01's atomic ranges from the plan (a TEMP table's lifetime T4, a statement the transform expanded, a deferred-FK span T7; `atomic` per chunk, plan format `k02.2`) merged with what the migrator finds itself in the chunk (`dependentStatements`): an INSERT into a table with a `DEFERRABLE INITIALLY DEFERRED` key up to the next DELETE from it, a rebuild from `CREATE TABLE n` to `ALTER TABLE n RENAME TO o`, and an index, view or trigger dropped and created again under the same name. A chunk that sets `PRAGMA defer_foreign_keys`, or a plan without `atomic`, is applied whole. A unit longer than the batch is kept whole and logged `gonvex.migrate.unsplittable` | a deferred-FK INSERT/DELETE pair with a batch of 2 commits as one 4-statement transaction, both with the plan's ranges and with them removed (on 0f1e9801 the split commit reset the object: `FOREIGN KEY constraint failed`); a rebuild and an index pair run as transactions of 4 and 3 and are logged unsplittable; a TEMP-table migration runs in one transaction; a plan without `atomic` applies each chunk whole; a pure test of the grouping, quoted names included |

Table rebuilds (`-- gonvex:foreign-key-rebuild`) need no single transaction: foreign keys stay off and the gate
closed from the first transaction to the ledger, the check runs after all statements, and a violation restores the
bookmark, exactly as across the existing 200-statement chunks. `-- gonvex:no-transaction` migrations keep Rust's
semantics (statements outside a transaction, ledger row last).

## Local measurements (arch3, local workerd; one event's wall time = its CPU locally)

Synthetic tenant (`kernel/migrate/test/large-fixture.ts`): 300 tables, 6,682 schema objects, 114,900 rows; release 2
adds a column, view and 4 triggers on every table (2,700 DDL statements) and rewrites rows of the 85k-row table.

| Migration of release 2 | Events | Max per event | Total |
|---|---|---|---|
| Before, clock frozen as on the platform (`maxMsPerEvent: Infinity`) | 9 | **7,714 ms** | 49.3 s |
| Before, local clock (the check works only here) | 27 | 2,884 ms | 49.5 s |
| After (defaults) | 51 | **1,799 ms** | 48.6 s |

Slowest steps after: the change-feed plan 1,426–1,468 ms (one K03 call over the whole catalog; it gets an event of its
own), the 0002 finalize-and-ledger transaction 946 ms, DDL transactions ≤ 605 ms.

| P03 import, same schema + 20k FTS docs | Before max | After max |
|---|---|---|
| `checksum` (60-table page / budgeted) | 1,181 ms | 1,028 ms (7 requests) |
| `verify` | 1,136 ms | 1,065 ms |
| `schema` | 135 ms (400 statements) | 1,006 ms (budget; fewer, larger requests) |
| `check` | 74 ms | 751 ms (yields between tables; wall, not CPU) |
| `insert` (4,000 rows) | 130 ms | 85 ms |

Before, the checksum and verify cost grows with table size (dev: 5,006 ms); after, it stops at the budget.

Platform estimate: dev measured about 1.5x local for the same chunks, so the worst event after this PR is about 2.7 s
(the change-feed plan alone about 2.2 s), under the 3.5 s that 5000 needs (70 % headroom), above the 2 s target of
0022 §5b. Reducing the plan's cost is K03 work (`planChangeFeedInstall` re-derives every table's trigger SQL).

## Dev verification plan (deploy agent)

Do not touch `devcopy-stg1117-12`: it is a live browser-matrix tenant. Use a new copy name.

1. Release this PR's head to the integration dev runtime (`gonvex-cf-dev-integration-*-runtime`); keep `cpu_ms` 30000.
   Add `devcopy-stg1117-12x` to `GONVEX_DEV_TRANSFER_TENANTS`.
2. Import `devcopy-stg1117-12x` from the same staging snapshot as -12 with `--complete-import --keep-object`; record
   every `assemblyTransfer` invocation's CPU (Workers Logs events view, unsampled `cf-worker-event` rows, filtered on
   that object id) and the six checksum legs of the P03 report.
3. `operatorApplyRelease` on `devcopy-stg1117-12x`, then poll `operatorMigrationStatus` until `done`. Record every
   `alarm` and `operatorApplyRelease` invocation's CPU for that object and every `gonvex.migrate.unsplittable` line.
4. Pass: the six checksum legs pass; the ledger equals the release plan's tenant migrations (compare, read-only, with
   -12's `operatorLedger`); every invocation is ≤ 3,500 ms CPU (expected max about 2.7 s, in the change-feed plan
   event). Then the lead may set `TENANT_CPU_LIMIT_MS` to 5000 and rerun `reducer-errors`.
5. If an event exceeds 3,500 ms, the `unsplittable` lines and the event's method name say whether it is the plan, a
   finalize, an atomic range or a single statement; report it instead of lowering the limit.
6. Delete `devcopy-stg1117-12x` after measuring (`DELETE /dev/tenants` for it, i.e. `operatorDeleteAll`), remove it from
   `GONVEX_DEV_TRANSFER_TENANTS`, and delete local copies of the snapshot.

Rerun locally: `pnpm vitest run kernel/migrate/test/budget.test.ts` and
`pnpm vitest run --root kernel/assembly test/tenant-transfer-budget.spec.ts` (in `cloudflare/`).
