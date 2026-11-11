# Kernel change capture (K03)

The tenant object's change log, as the Rust runtime writes it (`rust/crates/store/src/feed.rs`, `capture.rs`,
`record_views.rs`, `provision.rs` `install_change_feed_locked`, `runtime/src/change_feed.rs` pruning;
`docs/sqlite-storage.md` "Change feed"; CONTRACT E.a.2 c1–c10). Durable Object SQLite has no custom functions and
no hooks, so row images are plain SQL, from A01's T5 (0010, 0016 G0-4).

```ts
import { CaptureState, ChangeWake, directSql, finalize, installChangeFeed, invokeCaptured, planChangeFeedInstall,
  applyInstallUnit, pruneChangeLog } from "./index.ts";

// Provisioning (K02): units of statements + their _gonvex_sync_bootstrap receipt, applied in chunks.
for (const unit of planChangeFeedInstall(directSql(ctx.storage.sql))) applyInstallUnit(sql, unit);

// Every invocation (K04/K07) through K01: one revision per committed Reducer, then a wake-up.
const capture = { state: new CaptureState(), wake: new ChangeWake() };
const result = await invokeCaptured(tenantStorage, "reducer", provenance, body, capture);
capture.wake.subscribe((wake) => delivery.readAfter(cursor));      // K10

// A migration's last transaction (K02).
finalize(directSql(ctx.storage.sql), { commandId: `migration:${name}`, channel: "system" });

// The alarm (hourly, CONTRACT E.d).
await pruneChangeLog(ctx.storage, { maxPages: 50 });
```

| File | What | Rust |
| --- | --- | --- |
| `infrastructure.ts` | `SYNC_INFRASTRUCTURE_SQL` and `schema_options.sql`, byte for byte (the receipt digest depends on it) | `feed.rs`, `schema_options.sql` |
| `triggers.ts` | The capture scripts Rust generates, byte for byte ("Rust form", still calling `gonvex_capture`/`gonvex_record_value`): entity, logical record, owned, record dependency | `feed.rs` `capture_triggers_sql`, `record_views.rs` |
| `images.ts` | **The row-image step**, isolated for R05: Rust form → plain SQL (A01 T5), the no-op delete and the `[old, new, changed]` split | `capture.rs`, `feed.rs` finalize |
| `install.ts` | `planChangeFeedInstall` / `applyInstallUnit` / `installChangeFeed`: capture classes, receipts, stale drops, Rust's errors | `provision.rs` `install_change_feed_locked` |
| `finalize.ts` | One revision per commit inside the transaction: record flush, clock, ordinals, command id, provenance, row reports for K01 | `feed.rs` `finalize_known`, `record_views.rs` `flush` |
| `provenance.ts` | `_gonvex_sync_transactions` values and Rust's defaults, `originCommandId` included | `feed.rs` `Provenance` |
| `invoke.ts` | `invokeCaptured`: finalize as the last step of a K01 invocation; publish only after commit | `store/src/lib.rs` commit order |
| `wake.ts` | `ChangeWake`: the in-object wake-up after commit, coalesced, placement-neutral | `registry` broadcast, `ChangeFeedHub` |
| `prune.ts` | 30-day retention in pages, `retained_revision` first | `change_feed.rs` `prune_retained` |
| `digest.ts`, `sql.ts` | Synchronous SHA-256; the SQL surface (`CaptureSql`, K01's `StorageSession.run` shape) | |

## Behavior

- **Capture classes.** Every application table gets AFTER triggers with row images (`entity`); `owned` tables and
  logical record storage get BEFORE triggers that stage the record's pre-image in `_gonvex_record_pending`; record
  dependencies get their own named triggers. **Derived tables** (`_gonvex_table_options.capture = 'derived'`) get no
  capture, their old capture and receipt are dropped, and they must declare their owner foreign key, as Rust's
  derived-table-capture-v1. `metadata` tables, `_gonvex*`, `gonvex_*`, `sqlite_*`, FTS5 tables and their shadow tables
  are not captured, nor are the platform's `_cf_*` tables or local workerd's `__miniflare_*` tables.
- **Receipts.** `_gonvex_sync_bootstrap` holds the SHA-256 of the Rust-form script, so it equals Rust's (checked
  against a Rust tenant file: every receipt, including `infrastructure` and the dependency receipts). A unit is planned
  only when its receipt or its installed trigger SQL differs, so a warm wake plans nothing.
- **One revision per commit.** Rust's finalize statements, in order, inside the committing storage transaction. The
  `[old, new, changed]` split runs in the stamping UPDATE (one row write per change saved; same rows). Record flush is
  one INSERT per record name, in BINARY name order, which is the order of Rust's single statement.
- **Provenance.** `origin_command_id` is the invocation's command id; root and root channel default to it and to the
  channel; the channel defaults to `system`; empty actor fields are NULL. `command_id` on the change rows is NULL when
  there is no command id.
- **Commit and wake-up.** `invokeCaptured` finalizes as the body's last step, unless the invocation is a Query, has
  an outstanding statement failure, or was discarded (none of those commit). The notice is published to `ChangeWake`
  only after K01 reports the transaction committed. Listeners run in a later microtask, so commits published in one
  turn coalesce into one wake; delivery then reads from its own cursor, as Rust's feed task does.
- **Pruning.** Rust's end state (changes older than 30 days up to their highest revision R; orphaned provenance rows
  up to R; `retained_revision = max(retained_revision, R)`), reached in pages of 1,000 rows, one storage transaction
  each, yielding between pages. `retained_revision` is raised in the first transaction, before any delete, so no
  reader can see a partly pruned revision; a stopped run is finished by the next one.

## K01 integration

Capture is typed against K01 structurally (`CapturedSession`: `run`, `noteRows`, `kind`, `aborted`, `discarded`).
`test/capture-parity.test.ts` runs on K01's real `TenantStorage` in mixed write tracking (0028 §2e, wired by K07); the
other tests still use the stand-in in `test/helpers.ts`. The Reducer path (K07) finalizes through `invokeCaptured`
inside its storage transaction (`kernel/reducer/storage.ts`).

**Row reports (0022 §2c).** With `reportRows: true`, finalize reports to `noteRows` the rowids its revision inserted
or updated in captured entity tables, and deletes as table-level entries. It cannot report writes it does not see:
logical-record storage (the change row names the record), derived and metadata tables, and runtime `_gonvex_*`
tables. K01's `"rows"` mode relies on `noteRows` alone, so it would miss deferred foreign keys on those tables. K01
needs a mixed mode: capture's rows for captured entity tables, statement tracking for everything else. That is for
K01's owner to rule on; capture already reports what it can.

## Differences from Rust (note 0026)

1. **REAL number text.** Rust's hook reads the stored record, where SQLite keeps an integral REAL within ±2^47 as an
   integer (`5`), and formats other REALs with Rust's float formatter, then SQLite re-renders them (`1e+300`, `1e-7`,
   `0.000025`). The kernel's triggers see the REAL through SQL and SQLite writes `5.0`, `1.0e+300`, `1.0e-07`,
   `2.5e-05`. The JSON values are equal. In the parity fixture, 16 of 114 change rows differ in this way and in no
   other way.
2. **`WITHOUT ROWID` entity tables.** Rust's capture passes `NEW.rowid`, so in Rust every write that reaches a captured
   `WITHOUT ROWID` table fails with `no such column: NEW.rowid`. The kernel keeps a reference to `NEW.rowid` in those
   triggers (`keepRowidReference`) and fails the same writes with the same message. Whagons is not affected: all its
   `WITHOUT ROWID` tables are `derived` or `metadata`.
3. `application_tables` is computed without a 7-term `UNION ALL` (the platform refuses compound SELECTs over 5 terms)
   and over `sqlite_schema` rather than `pragma_table_list` (0016 G0-12). Same result.

## Verification

`pnpm test` in `cloudflare/` runs `test/*.test.ts` in workerd against Durable Object SQLite.

- **`test/capture-parity.test.ts` (the `done_when`).** `test/golden/rust-change-capture.json` is the Rust runtime's
  output for the conformance scenario `change-capture` (`cloudflare/conformance/scenarios/change-capture.mjs`): 18
  Reducer transactions covering every column kind and edge values, no-op updates, a caught failure inside a savepoint,
  a savepoint rollback, an app trigger updating its own row, trigger-maintained tables, a composite `WITHOUT ROWID`
  key, a derived table, a cascade, a logical record with an owned table and a record dependency, a record no-op, a
  rejected Reducer, bound parameters, and 40 rows in one statement. The test provisions the same schema the kernel way
  (infrastructure, the fixture migration through A01, the capture install), replays the transactions through
  `invokeCaptured`, and requires the same outcome per transaction, the same bootstrap receipts and trigger names, and
  the same change log: 114 change rows, 13 revisions, 13 provenance rows and the clock. The change rows must be equal
  field for field, with the old and new images byte-equal apart from REAL number text (difference 1).
- Regenerate the golden with a `gonvex-runtime` build: `node src/capture-golden.mjs` in `cloudflare/conformance`.
  The scenario itself runs in the harness (`node src/cli.mjs --scenario change-capture`) on any backend with the
  `changeLog` capability, so the kernel backend is compared with Rust there once it exists.
- `test/capture.test.ts`: SHA-256 and the Rust infrastructure digest, the Rust-form scripts, install planning (warm
  plan empty, derived-table rules, Rust's errors, `_cf_*` and FTS exclusion), finalize (revision, ordinals, no-op,
  provenance defaults, migrations), invocations (commit, throw, aborted, Query; wake only after commit), wake
  coalescing, paged pruning.

## Numbers

Rows written per transaction, local workerd, from `capture-parity.test.ts` (`rowsWritten` as billed; the "with"
column includes finalize):

| Transaction | Change rows | Without capture | With capture | Of which finalize |
| --- | ---: | ---: | ---: | ---: |
| insert, 1 row + 1 trigger row | 2 | 4 | 21 | 10 |
| update, 3 rows | 3 | 3 | 28 | 14 |
| no-op update (no revision) | 0 | 1 | 6 | 1 |
| logical record insert (storage + 2 owned rows + label) | 3 | 10 | 39 | 23 |
| record dependency update | 3 | 2 | 30 | 19 |
| 40 inserts + 40 trigger rows, one statement | 80 | 122 | 685 | 322 |

A change row costs about 3 rows written in its trigger (the row, its indexes, `sqlite_sequence`) and about 4 in
finalize (the stamp moves it between partial indexes), plus 2 per revision (clock, provenance). Folding the split
into the stamp saved one row write per change (finalize for the 40-row statement: 402 → 322). R05 removes the row
images; this table is its baseline.

Capture is not wired into the Worker entry yet (K04/K07 do that), so the bundle does not change.
