# S2: execution spike

**Question.** Can real Whagons Reducers run inside a Durable Object with the exact Gonvex module contract?

**Verdict.** The execution model works. One `ctx.storage.transaction(async …)` per Reducer gives one
atomic transaction around module code that awaits every host call. Rollback works on thrown errors, on
failed host calls and on deferred foreign keys. Module savepoints (`applyDataWrites`, `SAVEPOINT`,
`ROLLBACK TO`, `RELEASE`) work when they are mapped onto nested storage transactions. The 34
runtime-contract vectors pass in workerd. Three of them need a one-line Ed25519 shim; without it, 31 of
34 pass.

The work-package target of 20 real Reducers is **not met inside the Durable Object**. The cause is the
schema, not execution. DO SQLite refuses 13 of the 2,072 objects in the Reducers' schema subset. It
allows 5 terms per compound SELECT, an expression depth of 100 and 100 KB per statement. The refused
objects sit on the task, workspace, status, priority, tag and approval write paths. So 10 of the 25
distinct Whagons Reducers run end to end in the DO, and 15 are blocked. All 25 pass on the reference with
the full schema, using the same host code. Rule 7 applies: I report these limits and do not work around
them. S1 owns the full-schema count.

There is one more hazard, which K01 and K04 must design for. A deferred foreign-key violation surfaces
only at the platform's implicit commit, after the event. The request then fails with HTTP 500 and the
**Durable Object resets**. The Whagons schema has 1,372 deferred foreign keys across 448 tables.

## Numbers

| What | Value |
|---|---|
| App artifact (whagons5-client `2457bb65f`, `gonvex dev` build path, 32 s) | 859 Queries, 1,000 Reducers, 138 Actions |
| Query/Reducer split bundle (what the tenant object loads) | 4,159,267 B raw, 876,160 B gzip, 1,519 exports |
| Complete bundle (Actions included) | 6,315,165 B raw, 1,220,078 B gzip |
| Worker upload (`wrangler deploy --dry-run`, spike worker + split bundle) | 5,751.6 KiB, **1,019.4 KiB gzip** (limits: 3 MB free, 10 MB paid) |
| Worker upload with the complete bundle | 8,207.9 KiB, 1,395.2 KiB gzip |
| workerd startup to first response (median of 7; none / split / complete bundle) | 91 / 261 / 364 ms, so the split bundle adds ~170 ms of compile and evaluate |
| First Durable Object request after startup (constructor) | 27 ms (no difference between bundles) |
| V8 isolate heap, idle (none / split / complete) | 2.8 / 13.5 / 24.7 MB used (limit 128 MB) |
| V8 isolate heap after provisioning and Reducers (split bundle) | 25.2 MB used |
| workerd process RSS (none / split / complete) | 60 / 136 / 174 MB |
| Schema subset for the workload | 165 of 530 tables, 104 of 474 views, 1,522 of 6,453 triggers, 281 of 881 indexes; 2.16 MB DDL |
| Subset objects DO SQLite refuses | **13** (4 for expression depth, 6 for compound SELECT terms, 3 for statement length) |
| Provisioning the subset in the DO | 2,060 statements and 933 rows in 0.33–0.53 s; database 4.0 MB |
| Committed Reducer, warm, in the object (median of 12) | 11–23 ms depending on machine load (`spots.create`, `boards.createMessage`, `centers.askJev`) |
| Failed or read-only Reducer in the object | 1–3 ms |
| Rows read per Reducer | 400–3,450, of which 95–99 % are host catalog lookups (`sqlite_schema`, `pragma_table_info`) |
| Rows written per committed Reducer | 3–13 (triggers included) |
| Runtime-contract vectors in workerd | **34/34** with the Ed25519 shim, 31/34 raw; two rounds in one isolate |
| DO rows compared with the like-for-like reference | 30 tables, 972 rows: 26 tables identical; 4 FTS5 shadow tables already differ right after provisioning |
| Deferred-FK guard (`pragma_foreign_key_check`) | about 6 ms per 100k and 55 ms per 1M child rows; counts 0 `rowsRead` |

The machine was shared with other agents' Rust runtimes, with load averages of 7–14 on 14 cores.
Latencies are local workerd on local disk and indicate only rough size.

## Method

1. **Artifact.** `scripts/build-artifact.mjs` calls `buildModuleArtifact` from
   `packages/gonvex/src/module-artifact.ts` with the same inputs as `gonvex dev`: module sources,
   migrations, `gonvex.json` entrypoint and `split: true`. The tenant object loads the Query/Reducer
   bundle, as the split Rust engine does for Queries and Reducers.
2. **Schema.** `scripts/provision-reference.mjs` provisions a tenant in node:sqlite the way
   `rust/crates/store/src/provision.rs` does. It applies `reference_integrity/schema.sql`,
   `schema_options.sql`, `TENANT_IDENTITY_SQL`, the 12 tenant migrations (foreign keys off around
   `gonvex:foreign-key-rebuild`) and `SYNC_INFRASTRUCTURE_SQL`. The result has 530 tables, 474 views,
   6,453 triggers and 881 indexes. It does not reproduce two things that K02 and K03 own: the generated
   reference-integrity staging triggers and the per-table capture triggers.
   `scripts/subset-schema.mjs` then takes the closure of every object the workload's statements and host
   calls name. That closure includes triggers on those objects, the objects their SQL names, foreign-key
   parents, indexes, all `_gonvex_*` tables, and the rows provisioning inserted.
3. **Host.** `src/host.js` ports the database host calls of `rust/crates/runtime/src/host_calls.rs` at the
   reference commit:
   - single-statement admission;
   - JSON parameter binding;
   - `pragma_table_info` and `_gonvex_record_views` catalogs;
   - typed `RETURNING` rows and record-view reads;
   - the "aborted until `ROLLBACK TO`" rule;
   - deterministic outbox and scheduler ids (`intent_deferred_id`);
   - the scheduler outbox payload, including `coalesce` for `wakeAt`.

   `src/bootstrap.js` ports the `ctx` construction and the dispatcher of `gonvex_bootstrap.js`.
   Argument validation reuses `packages/local-runtime`'s `validateValue`.
4. **Workload.** `test/scenarios.mjs` has 35 steps on one tenant. The only fixture rows are the runtime's
   `members` table; everything else is created through the app's own Reducers. That covers 25 distinct
   Reducers:
   - teams, workspaces, categories, statuses, priorities, spots and tags;
   - approvals and approvers, and `tasks.decideApproval`;
   - `tasks.create`, `tasks.update`, `tasks.assign`, `tasks.setFlag` and `tasks.archive`;
   - `taskResources.createNote` and the board message and comment Reducers;
   - two `wakeAt` users: `cleaningAutomation.create` and `cleaningReports.create`;
   - an outbox enqueuer that also uses `applyDataWrites` savepoints: `centers.askJev`;
   - an `applyDataWrites` multi-table fan-out: `priorities.remove`;
   - six rollback cases.

   `test/savepoint-module.js` adds four synthetic Reducers on the real SDK's `applyDataWrites` for
   savepoint cases that no Whagons Reducer triggers on purpose.
5. **Two references on node:sqlite, using the same host code with real `BEGIN`/`SAVEPOINT`:**
   - The full schema shows what each step does when the whole schema loads. All 39 steps pass.
   - The like-for-like reference loads exactly the DDL the Durable Object accepted. It is used for
     step-by-step outcome and row comparison.

The Rust runtime itself was not run. A Reducer call on it needs the runtime, the control plane, auth and
a `/dev/sync` deploy, which is the P02 harness's job. `test/scenarios.mjs` is written so that the harness
can replay it against Rust.

## Results per step

The table comes from `scripts/report-table.mjs`. "Blocked" means the step fails in the DO because DO
SQLite refused a schema object it needs, or because an earlier step it depends on was blocked. Each
blocked step fails the same way on the like-for-like reference.

| Step | Function | Purpose | In the Durable Object | Same outcome as like-for-like reference | Full-schema reference | Statements | Rows read (host catalog) | Rows written | Savepoints | One JS turn |
|---|---|---|---|---|---|---|---|---|---|---|
| 01 | `teams.create` | team (insert, record view) | pass | yes | pass | 22 | 2135 (2099) | 5 | 0 | yes |
| 02 | `workspaces.create` | workspace | blocked: `referenceExpected_workspaces` refused | yes | pass | 24 | 2557 (2534) | 0 | 0 | yes |
| 03 | `categories.create` | category | blocked: a prerequisite step was blocked | yes | pass | 25 | 2655 (2548) | 10 | 0 | yes |
| 04 | `statuses.create` | status | blocked: `whTaskGridProjection` refused | yes | pass | 20 | 2116 (2095) | 0 | 0 | yes |
| 05 | `statuses.create` | status | blocked: `whTaskGridProjection` refused | yes | pass | 20 | 2116 (2095) | 0 | 0 | yes |
| 06 | `priorities.create` | priority | blocked: `whTaskGridProjection` refused | yes | pass | 20 | 2102 (2081) | 0 | 0 | yes |
| 07 | `priorities.create` | priority (to be removed) | blocked: `whTaskGridProjection` refused | yes | pass | 20 | 2102 (2081) | 0 | 0 | yes |
| 08 | `spots.create` | spot | pass | yes | pass | 21 | 2174 (2109) | 9 | 0 | yes |
| 09 | `tags.create` | tag | blocked: `whTaskTagSearchDocuments` refused | yes | pass | 20 | 2098 (2077) | 0 | 0 | yes |
| 10 | `approvals.create` | approval | blocked: `referenceExpected_approvals` refused | yes | pass | 20 | 2126 (2105) | 0 | 0 | yes |
| 11 | `approvals.createApprover` | approval approver | blocked: a prerequisite step was blocked | yes | pass | 22 | 2189 (2083) | 11 | 0 | yes |
| 12 | `tasks.create` | task create | blocked: a prerequisite step was blocked | yes | pass | 4 | 435 (435) | 0 | 0 | yes |
| 13 | `tasks.create` | task create with approval | blocked: a prerequisite step was blocked | yes | pass | 4 | 435 (435) | 0 | 0 | yes |
| 14 | `tasks.update` | task update | blocked: a prerequisite step was blocked | yes | pass | 8 | 920 (920) | 0 | 0 | yes |
| 15 | `tasks.assign` | task assign | blocked: a prerequisite step was blocked | yes | pass | 8 | 920 (920) | 0 | 0 | yes |
| 16 | `tasks.setFlag` | task flag | blocked: a prerequisite step was blocked | yes | pass | 8 | 920 (920) | 0 | 0 | yes |
| 17 | `taskResources.createNote` | task note (comment) | blocked: a prerequisite step was blocked | yes | pass | 8 | 920 (920) | 0 | 0 | yes |
| 18 | `tasks.decideApproval` | approval decision | blocked: a prerequisite step was blocked | yes | pass | 4 | 505 (505) | 0 | 0 | yes |
| 19 | `cleaningStatuses.create` | cleaning status | pass | yes | pass | 21 | 2112 (2083) | 3 | 0 | yes |
| 20 | `cleaningAutomation.create` | rollback: deferred foreign key fails at commit after an insert and a wakeAt | pass (expected error, rolled back) | yes | pass | 11 | 903 (844) | 13 | 0 | yes |
| 21 | `statuses.create` | status shared with a cleaning status | blocked: `whTaskGridProjection` refused | yes | pass | 20 | 2116 (2095) | 0 | 0 | yes |
| 22 | `cleaningStatuses.create` | cleaning status | pass | yes | pass | 21 | 2112 (2083) | 3 | 0 | yes |
| 23 | `cleaningAutomation.create` | wakeAt user (scheduler.wakeAt -> coalescing outbox job) | blocked: a prerequisite step was blocked | yes | pass | 11 | 903 (844) | 13 | 0 | yes |
| 24 | `cleaningReports.create` | wakeAt user (cleaning report schedule -> coalescing cleaning.tick job) | pass | yes | pass | 7 | 449 (437) | 8 | 0 | yes |
| 25 | `priorities.remove` | applyDataWrites (savepoint) multi-table fanout | blocked: a prerequisite step was blocked | yes | pass | 20 | 2106 (2081) | 0 | 0 | yes |
| 26 | `centers.askJev` | outbox enqueuer (applyDataWrites savepoints) | pass | yes | pass | 37 | 3448 (3334) | 11 | 2 | no |
| 27 | `settings.togglePlugin` | enable a powerup (plugins upsert) | pass | yes | pass | 6 | 474 (419) | 7 | 0 | yes |
| 28 | `boards.create` | board | pass | yes | pass | 10 | 849 (834) | 3 | 0 | yes |
| 29 | `boards.createMessage` | board message | pass | yes | pass | 17 | 1717 (1664) | 7 | 0 | yes |
| 30 | `boards.createComment` | board comment | pass | yes | pass | 14 | 1277 (1247) | 5 | 0 | yes |
| 31 | `tasks.create` | rollback: handler throws before writing | pass (expected error, rolled back) | yes | pass | 4 | 435 (435) | 0 | 0 | yes |
| 32 | `approvals.create` | rollback: JS throw after two inserts | blocked: `referenceExpected_approvals` refused | yes | pass | 20 | 2126 (2105) | 0 | 0 | yes |
| 33 | `approvals.create` | rollback: UNIQUE failure from a host call after an insert | blocked: `referenceExpected_approvals` refused | yes | pass | 20 | 2126 (2105) | 0 | 0 | yes |
| 34 | `tasks.assign` | rollback: deferred foreign key fails at commit after five writes | vacuous: failed earlier, on a blocked object | yes | pass | 8 | 920 (920) | 0 | 0 | yes |
| 35 | `tasks.archive` | task archive | blocked: a prerequisite step was blocked | yes | pass | 8 | 920 (920) | 0 | 0 | yes |
| S1 | `s2.caughtBatchFailure` | caught mid-batch failure | pass | yes | pass | 12 | 843 (806) | 6 | 3 | no |
| S2 | `s2.uncaughtBatchFailure` | uncaught mid-batch failure | pass (expected error, rolled back) | yes | pass | 11 | 842 (806) | 4 | 3 | no |
| S3 | `s2.nestedSavepoints` | nested savepoints, ROLLBACK TO, continue | pass | yes | pass | 12 | 846 (806) | 6 | 5 | no |
| S4 | `s2.caughtSqlErrorWithoutRecovery` | caught SQL error without ROLLBACK TO | pass (expected error, rolled back) | yes | pass | 5 | 402 (401) | 2 | 0 | yes |

These Reducers run end to end inside the Durable Object:

- `teams.create`, `spots.create`, `cleaningStatuses.create` and `settings.togglePlugin`;
- `cleaningReports.create`, a `wakeAt` user that leaves a coalescing `cleaning.tick` job in the outbox;
- `centers.askJev`, which enqueues to the outbox and uses `applyDataWrites` with two savepoints;
- `boards.create`, `boards.createMessage` and `boards.createComment`;
- `cleaningAutomation.create`. It writes 13 rows, its `wakeAt` row among them, then fails a deferred
  foreign key and rolls all of it back.

Blocked steps that write and then fail, such as steps 03 and 11, also exercise the deferred-FK guard. The
DO detects the violation, rolls back and keeps the object alive. The tenant object kept one instance
through all 39 steps. It was constructed twice in the run: once on first use, and once after idle
eviction while the driver spent about a minute on the reference.

## Rollback

Every invocation runs inside one `ctx.storage.transaction()`. A handler error, a commit refusal (see
S4) or a deferred-FK violation calls `txn.rollback()`. Each expected-error step takes a full snapshot of
every table before and after, and checks that nothing changed except the step's own reads. The rollback
evidence inside the DO comes from four steps:

- Step 20 is a real Reducer. It wrote 13 rows, including the record view, trigger-maintained rows and the
  scheduler outbox row, then failed `recordStorage_cleaningResetRules -> recordStorage_statuses`. All
  of it was rolled back.
- S2 wrote 4 rows, failed a UNIQUE constraint mid-batch, and rethrew. All of it was rolled back.
- S4 caught a SQL error and returned normally. The commit was refused and both rows were rolled back.
  This is the Rust store's behavior: DO SQL does not mark a transaction failed after an error, so the
  host must do it.
- Steps 32–34 are the Whagons versions of "throw after writes". In the DO they fail earlier, on a refused
  object. They are marked vacuous or blocked, and the full-schema reference passes them.

Without the transaction API (`mode: "implicit"`), the same code shows why the API is needed:

- S2 leaks its writes. `s2SavepointRows` keeps the row written before the throw.
- Step 20's deferred-FK violation is thrown at the implicit commit. The response is HTTP 500, the object
  resets (constructor count 3 → 4), and the writes are lost.

## Savepoints

`exec` refuses `SAVEPOINT`, `RELEASE` and `ROLLBACK TO`, as well as `BEGIN`, `COMMIT` and `ROLLBACK`. The
error says to use `transaction()` or `transactionSync()` instead. Module SQL uses them anyway:
`applyDataWrites` issues `SAVEPOINT gonvex_data_writes … RELEASE`, or `ROLLBACK TO` and `RELEASE` and a
rethrow. The host maps them onto nested storage transactions, which ActorSqlite implements as SQLite
savepoints:

- `SAVEPOINT x` opens `ctx.storage.transaction(async () => { await gate })`. The engine waits until the
  callback has started and pushes `{ name, gate }` onto a stack.
- `RELEASE x` resolves the gates from the top of the stack down to and including x. Each nested
  transaction commits into its parent.
- `ROLLBACK TO x` rejects the gates from the top down to x, which rolls back each nested transaction.
  The engine then reopens x, because SQLite keeps the savepoint after `ROLLBACK TO`.
- At the end of the invocation, gates still open are released on success and rejected on failure.
  Rust's `COMMIT` and `ROLLBACK` do the same.

Every case behaves like SQLite and like the reference:

- S1: `applyDataWrites` fails mid-batch and the module catches the error. The rows before and after the
  batch commit, and the batch row does not.
- S3: a manual `SAVEPOINT` contains an `applyDataWrites` savepoint, then `ROLLBACK TO`, then more writes,
  then `RELEASE`. Only the row written after the rollback survives.
- `centers.askJev` commits two `applyDataWrites` savepoints through the nested path inside the DO.
  `workspaces.create` and `priorities.remove` do the same on the reference.

## How async module code maps onto the transaction API

Module code awaits every host call. `transactionSync()` cannot hold an `await`. `transaction(async)`
can: on SQLite-backed objects it opens a savepoint and runs the callback inside `blockConcurrencyWhile`.
These facts come from the probes in `probe/probe.js`:

- **Atomic across awaits.** Writes before and after `await Promise.resolve()` and
  `await crypto.subtle.digest()` roll back together when the callback throws. A rejected `transaction()`
  does **not** reset the object; the constructor ran once across repeated rejected transactions.
- **Input gate.** A transaction held open across a real 300 ms timer blocks a second request to the same
  object until the transaction finishes. Even if a Reducer awaited real I/O, no other event for this
  tenant could interleave. That costs latency for every socket on the tenant, so host calls should stay
  synchronous.
- **Does any await cross real I/O?** The driver pumps microtasks against the handler (`invoke.js`,
  `settleWithinMicrotasks`). Every Reducer without savepoints settled within a single JS turn, after 8
  to 38 microtask hops. Host calls are synchronous `sql.exec` wrapped in resolved promises, and WebCrypto
  digests resolve without a turn. Reducers that open a savepoint (`centers.askJev`, S1–S3) cross turns,
  because each nested `transaction()` goes through `blockConcurrencyWhile` and yields to the event loop.
  That yield is not I/O, and the outer transaction's input gate still holds. Other objects in the same
  isolate can run in that window.
- **Invocation state must be per async context.** The Rust bootstrap keeps one `activeRuntimeRequest` per
  isolate. A workerd isolate is shared by every object of the class, so the spike scopes the pinned
  clock, the seeded random and the entropy gate with `AsyncLocalStorage` (`nodejs_als`). S3 checks
  `Date.now() === ctx.now` and two `Math.random()` values after five savepoint turns. Both match the
  reference exactly.
- **Output gate.** Reducers have no network, so no Reducer output can escape before commit. Deliveries
  happen after commit through the outbox, as in Rust.
- **Time limits.** `blockConcurrencyWhile` resets the object if a callback runs longer than 30 s. The
  Rust Reducer limit is 10 s (`V8Config::execution_timeout`), so the host must enforce its own deadline
  below 30 s.

## Platform facts that shape the kernel

| Fact | Evidence | Who handles it |
|---|---|---|
| A deferred-FK violation surfaces at the implicit commit, giving HTTP 500 and an **object reset**, with every transaction API | probe `/fk-async`, `/fk-sync`, `/fk-implicit`; implicit step 20 | K01/K04: check before leaving `transaction()`. The spike's full `pragma_foreign_key_check` costs 55 ms per 1M rows and does not scale; check only the rows written, using K03's change capture |
| `$name` parameters bind by order of first appearance (`$2 … $1` swaps values); `?N` binds by index | probe `/all` `dollarNamed` | K01: rewrite `$n` to `?n` outside literals (`src/sql.js`) |
| No `sqlite3_column_decltype`; values arrive as JS numbers, strings, `ArrayBuffer` or `null` | probe `rawTypes` | K01: port `sql_projection.rs`. The workload needed decltype for 0 columns with write-target typing plus a simple projection, but other SQL will |
| Integers above 2^53 lose precision (`9007199254740993` → `…992`) | probe `rawTypes` | K01 or contract note: Rust returns i64 |
| No `REGEXP` (no custom functions), and `sqlite_version()` is not authorized | probe `regexp`, `version` | K01: rewrite `REGEXP`. One app file (`purchasing/common.ts`) uses `matches` |
| 5 terms per compound SELECT, expression depth 100, 100 KB per statement | probe `/limits`; 13 refused subset objects | S1 and the lead: schema generator or K02 decision |
| `exec` refuses transaction control; a failed statement does not poison the transaction | probe `exec BEGIN…`, `errorThenContinue` | K04: savepoint mapping and the Rust "aborted" rule |
| Error text: `<message>: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_UNIQUE)` | step logs | K04: normalize to Rust's `error returned from database: (code: N) <message>`, done in `engines.js` |
| Ed25519 `verify` throws `OperationError` for a signature that is not 64 bytes, where WebCrypto and Rust return `false` | vectors `ed25519-*` | K04: the shim in `runtime-globals.js` |
| `pragma_*` table-valued functions are not counted in `rowsRead` | `fkGuardRowsRead` = 0 | Billing and CPU note |
| Catalog lookups dominate rows read: each table's first use scans `sqlite_schema` (~2,000 rows) | rows read (host catalog) column | K04: cache catalogs across invocations per schema generation |

## Recommended host-call design for K04

1. **One `ctx.storage.transaction(async txn => …)` per Query or Reducer invocation**, created by the
   host around dispatch. Do not use `transactionSync`, which cannot await, or implicit coalescing, which
   leaks on throw and resets on a foreign-key violation.
2. **Synchronous host calls.** `db.*`, `permissions.has`, `actions.enqueue` and `scheduler.*` run
   `sql.exec` synchronously and return already-resolved promises. A Reducer must never await real I/O.
   Anything it needs, such as file metadata for `storage.getMetadata` or permission bits, lives in the
   tenant's SQLite or in memory.
3. **Savepoints as nested storage transactions**, held open by a gate promise on a LIFO stack. Use the
   mapping in `src/engines.js` (`DurableObjectEngine`). `ROLLBACK TO` reopens the savepoint. Settle any
   open savepoints at the end of the invocation.
4. **The Rust failure rules in the host.** After a SQL error, every call fails with "current transaction
   is aborted…" until `ROLLBACK TO`. A caught error without recovery refuses the commit.
5. **A deferred-FK check before commit**, targeted at written rows. Without it a bad reference resets the
   tenant object and drops every socket.
6. **Invocation state in `AsyncLocalStorage`**: the pinned `Date`, the seeded `Math.random` and the
   entropy gate (`src/runtime-globals.js`). Install the globals in a module imported before the app
   bundle.
7. **Contract shims:** the Ed25519 `verify` length check, and SQL error text normalized to the Rust
   format.
8. **Catalog cache across invocations**, invalidated by DDL or a new bundle generation. This removes
   95–99 % of the rows read.
9. **The tenant object loads the Query/Reducer split bundle.** It costs 1.0 MB gzip upload, ~170 ms of
   startup and 13.5 MB of heap. Actions run elsewhere, with the complete bundle.
10. **Base it on this spike's `host.js`, `bootstrap.js` and `sql.js`, not on
    `packages/local-runtime`'s `PortableReducerRuntime`.** Server Reducers use raw `db.query` SQL
    throughout; every step in the workload did. The portable runtime is the client's structured predictor.
    K04 can reuse local-runtime's `validateValue`, as the spike does, and share column-kind rules.

## Decisions for the lead

- **Schema limits.** S2 cannot meet its 20-Reducer target in a Durable Object until the 13 refused
  objects, and their counterparts in the full schema, fit DO SQLite's limits. All of them are generated:
  - 10 `referenceExpected_*` views are compound SELECTs and recursive CTEs. They could be split into
    helper views of 5 terms or fewer and under 100 KB each.
  - 3 `whTask*SearchDocuments`/`whTaskGridProjection` views and 1 unique index fold accents with about
    200 nested `replace()` calls. With no custom functions available, that needs staged generated columns
    or a host-computed column.

  These are written workarounds, not a design. They belong to S1, K02 or the Whagons schema generator.
- **Deferred foreign keys** (1,372 clauses) need the targeted pre-commit check in K01/K04 before G1.
  Otherwise one stale id from a client resets the tenant.
- **Observation, not S2's business.** `cleaningAutomation.create` checks `targetStatusId` against
  `cleaningStatuses`, but the storage foreign key points at `statuses`. A cleaning status that is not also
  a task status fails at commit on the Rust runtime too (step 20 on the reference).

## Not done

- No run against the Rust runtime (see Method). Rows are compared with node:sqlite references that share
  the host code. `test/scenarios.mjs` is the input for a P02 replay against Rust.
- Local workerd only. Production isolates have different CPU, timer and clock behavior; startup is
  measured as local compile and evaluate time.
- No staging data was copied, so there is nothing to delete.

## Reproduce

See `cloudflare/spikes/s2/README.md`. Inputs: Gonvex `cloudflare/integration` at `368300f9` (Rust at
`0bf3ad13`), whagons5-client `2457bb65f`, workerd `1.20261023.1` and wrangler `4.153.0`.
