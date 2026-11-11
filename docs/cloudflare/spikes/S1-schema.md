# S1: the Whagons tenant schema and data in a Durable Object

Package S1, stage 0. Measured on 2026-11-10 in local workerd (wrangler 4.155.0,
workerd 2026-10-23) and on the platform as the Worker `gonvex-cf-dev-s1`
(development account, colo SJC). Code: `cloudflare/spikes/s1/`. Raw results:
`cloudflare/spikes/s1/results/`.

Numbers this PR moves (rule 8): no runtime code changes. The spike Worker is
6.15 KiB (2.19 KiB gzip). Rows read and written for the measured writes are in
`results/platform-capture.json`; memory is under "Memory".

## Verdict

**Workaround, with one blocker that needs a design decision.**

The schema loads, the full migration set applies, the largest staging tenant
imports exactly and wakes in about 1 s. Platform limits break parts of the schema;
six have a mechanical, verified, semantics-preserving rewrite. One does not: the
reference-integrity layer (96 `referenceExpected_*` views read by triggers on
194 tables) exceeds the per-statement nesting check that some platform machines
enforce, so on those machines writes to 136 of 522 tables fail to compile (on
other machines all compile). That layer has to be restructured (generator or
kernel) before G0.

| Question | Answer |
|---|---|
| Does the full migration set apply? | Yes, with the rewrites below, on the platform: 26,532 statements after rewrites plus 1,161 runtime-generated objects, 0 failures, 135 s of requests. Not as one transaction per migration: 0003 alone used 45 s CPU / 65 s wall and the platform reset the object ("storage operation exceeded timeout"). Chunks of 200 statements stay under 2.4 s each. |
| Can the real tenant data be imported? | Yes. 225,854 rows of 522 tables, 0 row-count mismatches, 0 foreign-key violations, `quick_check` ok, `databaseSize` 100.0 MB. 37.7 s of requests on the platform (14.8 s local). |
| Cold wake | Platform: median 997 ms, max 1,851 ms (client-observed, 8 restarts, 63 ms empty-object round trip). Warm: 93 ms. Local: 221 to 340 ms cold, 50 ms warm. |
| Memory | Local workerd RSS rises 69 MB (113 to 182 MB) when the object wakes; native SQLite reports a 56.2 MB parsed-schema heap. On the platform the loaded object has the same JS headroom as an empty one (isolate restarts at 200 to 224 MiB held in both): the tenant's SQLite state did not reduce it. |
| Statements over 100 KB | 3 by text length, 4 once SQLite doubles quotes (below); 6 in the migration files. |
| Tables over 100 columns | 0 (widest table 43, widest view 51 to 100: 2 views). |
| Rows over 2 MB | 0 (widest row 205 KB, `recordStorage_tenantBranding`). |
| Queries over 100 bound parameters | Runtime, not schema: live-query `in`/`oneOf` (one `$n` per value; Whagons sends 500-id chunks), operator `oneOf`, SDK `existsDataRows` (256 reads per statement). |
| `regexp()` | 0 uses in the schema; 1 in Whagons (`gonvex/purchasing/common.ts:164`, SDK `matches`); the runtime registers it on every tenant pool. DO has no `REGEXP`. |
| FTS5 trigram | Works, local and platform, including external content with `'rebuild'` and `case_sensitive 0 remove_diacritics 1`. Both Whagons FTS tables rebuilt. |
| `json_object` trigger change capture | Works. All 1,140 capture triggers that call the custom `gonvex_record_value()` were rewritten to plain SQL; real updates on 5 records captured the right change rows on the platform (95 to 140 ms per transaction). |

## Data and schema

Source: `tenant_a937cee33b408f92699da9c7320393ea.db`, the largest staging tenant
(125.6 MB), copied with `sqlite3 -readonly … .backup` inside the app container
(0.28 s), streamed to arch3, server temp deleted at once. The local copy lives
outside the repo; it was deleted at the end of the package, together with the
deployed Worker and its objects (`wrangler delete`), so nothing holds tenant data
now. Redeploy from `cloudflare/spikes/s1/README.md` to reproduce.

| | Migrations only | Real tenant file |
|---|---|---|
| Tables | 525 | 535 (522 with rows to copy, 156 non-empty) |
| Indexes | 926 | 941 (884 with SQL) |
| Views | 474 | 474 |
| Triggers | 6,453 | 7,596 (+1,143 the runtime generates: capture, record dependencies) |
| DDL bytes | | 6.31 MB |
| Rows | | 225,854 (59.2 MB of values; `_gonvex_sync_changes` 33.4 MB) |

Migrations: whagons5-client `2457bb65f` (identical to today's `origin/staging`),
12 files, 13.9 MB, 26,499 statements after splitting, plus the runtime's static
prelude (`scripts/runtime-prelude.sql`, from `0bf3ad13`). Native SQLite 3.53 applies
them in 37 to 69 s on arch3 (load-dependent); the tenant file's own
`gonvex_migrations` ledger records production taking 27.4 s for 0003, 25.4 s for
0005, 14.5 s for 0007 and 15.5 s for 0009.

## Platform limits, measured

Same on local workerd and the platform unless noted (`results/probes-*.json`).

| Limit | Value | Whagons objects affected |
|---|---|---|
| SQL statement length | 100,000 bytes | 3 views by length; 4 by effective length (next row) |
| DDL after quote doubling | `CREATE` text with every `'` doubled + ~100 bytes ≤ 100,000. SQLite re-parses DDL inside `INSERT INTO sqlite_schema … '%q'`; a 98.5 KB view failed with `SQLITE_TOOBIG`. | `referenceExpected_tasks` 171 KB, `_templates` 184 KB, `_workflowVersions` 102 KB (97.9 KB raw), `_workplanItems` 250 KB |
| Compound SELECT terms | 5 | 22 views, all `referenceExpected_*`, 6 to 114 `UNION ALL` terms |
| Expression depth | 100 (nested calls; parentheses alone do not count) | 3 views and 1 unique index: a Unicode case fold written as 198 to 223 nested `replace()` |
| Opcodes per program | ≈21.5k local (a program grows by doubling; 21,504 fits, 43,008 does not); larger on the platform (13,884 vs 10,745 VALUES rows) | Local: 24 of 1,569 write statements (insert/update `tasks`, `templates`, `workplanItems`, `workflowVersions`, `taskRecurrences`, …); natively, with an opcode limit of 25,000, the *unmodified* schema fails 22 of them. Whole-database `quick_check`, `foreign_key_check` and `ANALYZE` hit it locally; on the platform they passed on the measured object (P03 saw NOMEM only locally too). |
| View/subquery nesting | A query may read a chain of at most 99 CTEs or nested views on the platform; local workerd allows 1,000+. Platform only. | See "Blocker". |
| Temp schema | `CREATE TEMP TABLE` not authorized | 27 migration guard tables (0003, 0004, 0005, 0006), each dropped in its own migration |
| Bound parameters | 100 | runtime queries only (see table above) |
| Columns (tables, result sets) | 100 | none |
| Function arguments | 127 | none (max 108: `json_object` over a 54-column record) |
| LIKE/GLOB pattern | 50 bytes | no literal patterns in the schema; any user search string over ~48 characters fed into `LIKE '%'||?||'%'` |
| Row / value size | Platform and local accepted 8 MiB (docs say 2 MB) | none |
| Custom SQL functions | none; `sqlite_version()` not authorized | `gonvex_record_value` in 1,140 triggers; `gonvex_capture`, `regexp` in the runtime |
| Hooks | none | preupdate, commit, rollback, WAL hooks in `rust/crates/store` (capture, checkpoints) |
| Pragmas | `integrity_check`, `journal_mode`, `cache_size`, `ATTACH` not authorized; `foreign_keys`, `defer_foreign_keys`, `quick_check`, `foreign_key_check`, `table_info`, `optimize`, `ANALYZE` work | runtime pragmas in `docs/sqlite-storage.md` |
| Transactions | SQL `BEGIN`/`SAVEPOINT` refused; use `transactionSync`. A deferred FK violation at commit resets the object and rolls it back. `PRAGMA foreign_keys=OFF` works and stays set for the connection. | FK-rebuild migrations 0003, 0005; SDK `SAVEPOINT gonvex_data_writes` |
| One storage operation | A `transactionSync` running 45 s CPU / 65 s wall was killed: "Durable Object storage operation exceeded timeout which caused object to be reset." | migrations 0003, 0005, 0007, 0009 as single transactions |

## Rewrites that keep results identical

Each is implemented in `cloudflare/spikes/s1/scripts/sql.mjs` and checked on
native SQLite against the real tenant data (`results/verify-workarounds.json`):
all 25 rewritten views return identical rows, and the rewritten index expression
equals the original on every row. They are proposals for the owners of the
generator and the kernel, not schema changes made here.

1. **Compound SELECT over 5 terms → left-nested chain.** `t1 op … op tn` becomes
   `SELECT * FROM (t1 op … t5) op t6 …`, repeated. SQLite evaluates compounds
   left to right, so this holds for any mix of operators. Cost: none measured
   (single-owner queries ±15%).
2. **DDL over 100 KB → partition by union branch.** The 4 oversized views are a
   chain of up to 404 `MATERIALIZED` CTEs ending in one `UNION ALL`. Each part view
   keeps only the CTEs its branches need (so `MATERIALIZED` still holds) and
   the original view becomes its final `GROUP BY` over a `UNION ALL` of parts:
   2 or 3 parts each, largest 94.8 KB raw. Same speed (`tasks` single owner
   34.7 → 37.7 ms). Hoisting each CTE into its own view was also tried: same
   rows but 10 to 16× slower, because views lose `MATERIALIZED`.
3. **Expression depth.** In the 3 views, each 198-level `replace()` chain becomes
   a recursive CTE that applies the same pairs in the same order (≈400 rows
   read per folded value; `whTaskGridProjection` full scan 99 → 473 ms). The
   unique index on `recordStorage_roles` cannot hold a subquery, so its chain is
   cut into two virtual generated columns of 90 levels each. Smaller and faster:
   fold case in FTS5 (`trigram case_sensitive 0` already folds Unicode case) or
   store a folded column written by the kernel.
4. **Temp guard tables → ordinary tables** created and dropped inside the same
   migration (all 27 are).
5. **Custom functions and hooks → plain SQL capture.**
   `gonvex_record_value('<record>', key)` becomes
   `(SELECT json_object(…) FROM "<record>" WHERE "<key>" = key)` with JSON
   columns as `json(col)` and booleans as `true`/`false`, the SQL twin of
   `record_views::image`. The commit-time flush becomes one `FLUSH_SQL` per record
   name present in `_gonvex_record_pending`. 1,140 triggers rewritten (largest
   7,050 bytes), 0 calls left, applied in 46.8 s on the platform (6.8 s local).
   Changed-column lists come out right (`["updatedAt"]`). This matches
   contract-changes 0010.
6. **Migrations → chunked transactions.** Apply each migration in transactions
   of ≤200 statements (≤2.4 s each on the platform). Atomicity then needs a
   restore point: take a point-in-time bookmark before the migration and restore
   it on failure, or build the new schema in a fresh object and switch.

## Blocker: the reference-integrity layer

On the platform object built from the tenant file's final schema, 22 of 96
`referenceExpected_*` views fail as a plain `SELECT count(*)` with "VIEWs and/or
subqueries nested too deep", and 301 of 1,566 write statements (insert 118,
update 135, delete 48) on 136 of 522 tables fail to compile, among them every
insert and update on `tasks`, `approvals`, `categories`, `workspaces` and the
`members` delete. Dropping a table's 4 or 5 `referenceBindings_*` triggers makes
its writes compile; dropping the capture triggers does not, so plain-SQL capture
is not the cause. The failure also occurs on views the rewrites never touched
(`goals`, `taskFindings`, `spotCustomFieldValues`, `taskCustomFieldValues`), and
splitting the approvals union into parts does not help: any single statement
that reads ten or more of its branches fails, while each part alone works.

`results/platform-capture*.json` and `results/platform-reference-views.json`
have the lists.

**The platform runs two behaviours, chosen by host.** On an object built by
replaying the migrations (`mig-full`), all 96 views work and all 1,566 write
statements compile, also after a restart. That is not a property of the schema:
its exact DDL replayed into fresh objects works, and the same DDL replayed into
a failing object after `deleteAll()` (same object id, so the same machine) fails
the same 22 views again. Statistics, data, object order and plain-SQL capture
were each ruled out. All objects share the 99-CTE-chain cap; the stricter
machines also refuse these view shapes. Treat the strict behaviour as the one
that will win: Cloudflare rolls runtime builds out machine by machine. The
objects that failed were placed in SJC on 2026-11-10.

Independently of the platform nesting check, native SQLite with a 25,000-opcode
limit cannot compile 22 write statements of the unmodified schema
(`insert recordStorage_tasks` needs a 43,008-op program, `templates` and
`workplanItems` 86,016). A trigger program inlines whole reference views, twice
for updates.

P03's findings on the same limits are recorded as contract-changes 0013; this
spike confirms them on the platform with the corrections above (22 compound
views, 4 over the effective length, host-dependent nesting).

Smallest workaround, to be decided by the lead: move the expected-reference
computation out of single statements. Either the Whagons generator
(`scripts/schema/reference-sql.mjs`) emits one staging insert per group of at
most 5 branches, each in its own trigger fired through a small queue table so
every trigger program stays under the opcode and nesting limits, followed by one
grouping step; or the kernel computes expected references in JS from the
catalog after each write. Either changes the Whagons schema or the runtime
contract, so per rule 7 this spike stops here.

## Migration timings

| Migration | Statements | Native 3.53 (arch3) | Local workerd, 1 tx | Platform, 200-statement tx |
|---|---|---|---|---|
| prelude | 44 | 0.1 s | 0.2 s | 0.2 s |
| 0000 baseline | 879 | 0.2 s | 0.4 s | 0.8 s |
| 0003 relationship integrity | 2,954 | 5.7 s | 7.6 s | 13.3 s (one tx: reset at 65 s; in 250-statement chunks on another machine: 36.7 s) |
| 0004 document relationships | 1,779 | 1.0 s | 2.0 s | 2.6 s |
| 0005 compact record storage | 11,739 | 10.8 s | 10.9 s | 23.3 s |
| 0007 bounded identity projections | 5,106 | 29.1 s | 18.2 s | 49.1 s |
| 0009 indexed public relationship filters | 3,691 | 21.1 s | 21.0 s | 37.5 s |
| others (0001, 0002, 0006, 0008, 0010, 0011) | 307 | 1.2 s | 1.9 s | 1.7 s |
| runtime-generated objects | 1,143 triggers + 46 others | | 5.5 s | 6.6 s |
| **total** | 26,499 + prelude | 37 to 69 s | 68 s | **135 s** |

Time goes to schema reloads: each `DROP VIEW`, `ALTER TABLE` and table rebuild
re-reads a schema of several thousand triggers (a single `DROP VIEW` costs 45
to 95 ms natively). Locally, 3 backfill statements (`INSERT INTO
referenceBindings_workplanItems SELECT … FROM referenceExpected_workplanItems`)
fail on the opcode limit even on empty tables; on the platform they pass. On
the platform an earlier 250-statement run failed once on the 0004 approvals
backfill with the nesting error; the 200-statement run, on another object and machine, did not (the host split below).

## Snapshot import

| Step | Local workerd | Platform |
|---|---|---|
| Create 524 tables (incl. 2 FTS5) | 0.2 s | 0.8 s |
| 225,854 rows, 163 requests, FK off | 14.8 s | 37.7 s |
| FTS5 rebuild (2 tables) | 0.1 s | 0.3 s |
| 886 indexes | 0.8 s | 5.5 s |
| 483 views (after rewrites) | 0.4 s | 3.5 s |
| 7,596 triggers | 7.9 s | 73.8 s (4 requests) |
| `ANALYZE` | opcode NOMEM | 0.3 s |
| Verify: row counts, FK check, `quick_check` | opcode NOMEM | 0 mismatches, 0 violations, ok |
| `databaseSize` | 99.8 MB | 100.0 MB |

## Memory

- Native SQLite 3.54: the parsed tenant schema takes 56.2 MB of SQLite heap
  (58.4 MB total after open). Preparing `referenceExpected_workplanItems` peaks
  at 76.5 MB.
- Local workerd: the user-worker process goes from 113 MB to 182 MB RSS (peak
  190 MB) when the loaded object wakes and answers one grid query: +69 MB.
- Platform: there is no memory metric, so the spike measured headroom. A freshly
  woken object holds JS typed arrays in 8 MiB steps until the platform restarts
  its isolate (the request succeeds; the next one lands on a new instance).
  Empty object: 200, 224, 224, 224 MiB. Loaded tenant object: 200, 224, 224,
  224 MiB. The tenant's SQLite state does not reduce the isolate's JS headroom,
  and no request in the spike failed on memory.
- So the 128 MB isolate limit is not what binds this schema: statement
  compilation limits are (opcodes and nesting, above).

## Files

- `cloudflare/spikes/s1/src/index.js`: the Worker and Durable Object.
- `scripts/probes.mjs`, `nesting.mjs`: limits and features.
- `scripts/analyze.mjs`: static checks and the native migration replay.
- `scripts/sql.mjs`: splitter and rewrites; `verify-workarounds.mjs` proves them.
- `scripts/load.mjs`: migrations and snapshot loads; `capture.mjs`: plain-SQL
  capture and the write-path compile; `reference-views.mjs`, `wake.mjs`,
  `headroom.mjs`, `cte-depth.mjs`.
