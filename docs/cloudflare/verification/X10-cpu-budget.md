# X10: migration alarms and P03 transfer pages inside a CPU budget

Goal: every legitimate tenant-runtime event fits the ruled `cpu_ms = 5000` (0022 §5b, 0089), so the lead can lower
`TENANT_CPU_LIMIT_MS` from 30000. This PR changes no `cpu_ms`.

## Cause (reproduced locally)

- The migrator stopped an event after `maxMsPerEvent` (1500 ms) of `Date.now()`. On the platform the clock does not
  advance during synchronous work (R25 #8), so the check never fired and each alarm ran `maxChunksPerEvent` (4) whole
  plan chunks. On a schema of ~6.7k objects one DDL statement costs about 9 ms locally, so a 200-statement chunk is
  about 2 s and an alarm about 7 s. Dev measured 3,970–10,588 ms for these alarms (0089).
- The change-feed install re-planned the whole catalog (about 1.4 s locally) for every transaction.
- P03 `checksum`/`verify` hashed up to 60 whole tables per request and `schema` ran 400 DDL statements per request, so
  their cost grew with the tenant (dev max 5,006 ms).

## Design (lead ruling on #471: no new split points)

Migrations (`kernel/migrate/migrator.ts`):

- **Transactions are exactly the existing ones.** A plan chunk is one storage transaction; the restore point, attempt
  counting, gate, foreign-key rebuild, no-transaction rules, finalize and ledger row are untouched. Earlier rounds of
  this PR split chunks further; that is withdrawn.
- **The frozen-clock bug is fixed.** Time is read after a macrotask yield (`setTimeout(0)`), when the platform clock
  includes the CPU of the transaction just run. Another transaction starts in the same event only if the clock visibly
  moved and the time so far plus the last transaction's time fits `maxMsPerEvent` (1000 ms). With a frozen clock an
  event runs one transaction instead of four.
- A transaction that alone exceeds the budget is logged `gonvex.migrate.unsplittable` with its time.
- The change-feed install commits whole receipt units, which the migrator always committed separately, at most 50
  statements per transaction instead of 200. Its plan is reused for those batches while `sqlite_schema` is unchanged,
  is taken again after the infrastructure unit (applied alone), in a new instance and at the end, and gets an event of
  its own; an object replaced every event still progresses (`planOnly`). "Does not converge" checks every receipt
  installed since the previous plan.
- The foreign-key check step (read-only) yields between tables and checks at least one table per step.

P03 transport (`tools/tenant-transfer/src/transport.mjs`, `kernel/assembly/tenant-transfer.ts`):

- `schema`, `checksum` and `check` stop at a 1 s budget. `schema` answers `consumed`, the others `next`. Import DDL was
  already applied one statement per transaction when a batch failed, so smaller batches add no new boundary.
- A checksum cursor carries the running SHA-256 state (`src/sha256.mjs`), so the digest equals node:crypto's one pass.
- **Quiescence is enforced, not assumed.** Every write renews a write token. A cursor carries the token it started
  with; a request that sees a different one, before resuming or after any yield, is refused (409, `stale: true`). The
  assembled runtime runs `checksum`, `check` and `verify` only while the import is in progress (no module, no
  migrations, every transfer request serialized), keeps the `check`/`verify` cursors in the object, and clears them on
  any write. No correctness under concurrent writes is attempted.
- `schema` clears the cached table plans after every transaction and at the end.

## Local measurements (arch3, local workerd; one event's wall time = its CPU locally)

Synthetic tenant (`kernel/migrate/test/large-fixture.ts`): 300 tables, 6,682 schema objects, 114,900 rows; release 2
adds a column, view and 4 triggers on every table (2,700 DDL statements in 200-statement chunks) and rewrites rows of
the 85k-row table.

| Migration of release 2 | Events | Max per event | Total |
|---|---|---|---|
| Before, clock frozen as on the platform | 9 | **7,714 ms** | 49.3 s |
| After | 31 | **2,108 ms** | 38.9 s |

After, every one of the slowest events is a single existing 200-statement chunk (1,795–2,099 ms). That chunk is the
unsplittable unit and is logged as such.

**Platform estimate.** Dev measured about 1.5x local for the same work, so the worst event is about 3.2 s here. The dev
alarms themselves were 4 chunks in 3,970–10,588 ms, i.e. at most about 2.65 s per Whagons chunk, which is what one
alarm now runs. Both are below 3.5 s, with little margin. If dev measures above 3.5 s, `cpu_ms` stays 30000 and the
remaining fix is smaller chunks in the A01 plan builder, which is a transform ruling, not part of this PR.

| P03 import, same schema + 20k FTS docs | Before max | After max |
|---|---|---|
| `checksum` (60-table page / budgeted) | 1,181 ms (grows with table size; dev 5,006) | about 1 s (stops at budget) |
| `verify` | 1,136 ms | about 1 s |
| `insert` (4,000 rows) | 130 ms | 85 ms |

## Dev verification plan (deploy agent)

Do not touch `devcopy-stg1117-12`: it is a live browser-matrix tenant. Use a new copy name.

1. Release this PR's head to the integration dev runtime; keep `cpu_ms` 30000. Add `devcopy-stg1117-12x` to
   `GONVEX_DEV_TRANSFER_TENANTS`.
2. Import `devcopy-stg1117-12x` from the same staging snapshot as -12 with `--complete-import --keep-object`; record
   every `assemblyTransfer` invocation's CPU (Workers Logs events view, unsampled, filtered on that object id) and the
   six checksum legs of the P03 report.
3. `operatorApplyRelease` on -12x, then poll `operatorMigrationStatus` until `done`. Record every `alarm` and
   `operatorApplyRelease` invocation's CPU for that object and every `gonvex.migrate.unsplittable` line (its `ms` is
   the chunk's time).
4. Pass: the six checksum legs pass; the ledger equals the release's tenant migrations (compare, read-only, with -12's
   `operatorLedger`); every invocation is ≤ 3,500 ms CPU. Then the lead may set `TENANT_CPU_LIMIT_MS` to 5000 and rerun
   `reducer-errors`. Otherwise keep 30000 and record the measured worst chunk.
5. Delete -12x after measuring (`DELETE /dev/tenants`, i.e. `operatorDeleteAll`), remove it from
   `GONVEX_DEV_TRANSFER_TENANTS`, and delete local copies of the snapshot.

Rerun locally (in `cloudflare/`): `npx vitest run kernel/migrate/test/budget.test.ts` and
`npx vitest run --root kernel/assembly test/tenant-transfer-budget.spec.ts`.
