# 0014: Ruling on the schema objects Durable Object SQLite refuses (0013)

Date: 2026-11-11 (revised the same day). Status: **decided (lead, per Gabriel's direction)**.
Affects: A01, K02, K03, K07, P02, G1.

## Revision

The first version of this note put the fix in the Whagons app's schema generator. Gabriel directed that
nothing from this work goes into the app repository or Gonvex `staging` before cutover. **The fix is a
deterministic schema transform inside the kernel** (package A01, `cloudflare/kernel/schema-transform/`).
Its input is the app's migrations, unchanged. No app PR is opened. (An app-repo agent had been started; it
was stopped before it committed or pushed anything.)

## What has to be transformed (P03, S1 and S2, measured on the platform)

| # | Construct | Objects in the current app schema | Transform (S1 verified each on the largest staging tenant's data) |
|---|---|---|---|
| T1 | Compound SELECT over 5 terms | 22 `referenceExpected_*` views | Left-nested chain of at most 5 terms per level |
| T2 | Statement over 100 KB (effective length; SQLite doubles quotes in `sqlite_schema`) | 4 views | Partition by union branch, keeping `MATERIALIZED` |
| T3 | Expression depth over 100 | 3 `whTask*` views; unique index `roles_live_name_key_unique` (Unicode `lower()` emulated by about 200 nested `replace()` calls) | Views: recursive CTE fold (under the 99-CTE-chain cap). Index: chained VIRTUAL generated columns, with an expression index of the same name, `CASE` and `WHERE`, so the uniqueness error text is unchanged |
| T4 | `CREATE TEMP TABLE` (not authorized) | 27 migration guard tables | Ordinary tables created and dropped inside the same migration chunk |
| T5 | Custom SQL functions and hooks (`gonvex_capture`, `gonvex_record_value`) | 1,140 capture and record triggers | Plain-SQL capture triggers (`json_object` row images). S1 showed correct change rows on the platform (also settles 0010) |
| T6 | Trigger bodies that read a reference union view | `referenceBindings_*_source_insert/_source_update` on 136 tables (incl. every `tasks` write) | **Per-branch statements.** One `INSERT` per union branch into a scratch table, then one aggregate statement from the scratch table into `referenceBindings_<table>`. No statement reads a union view. See below |
| T7 | One migration too large for one storage operation | 0003 (45 s CPU) | Apply in chunks of at most 200 statements with a restore point (S1: 135 s for the full set) |

**Why T6 exists.** S1 found that some platform hosts refuse any single statement that reads ten or more
branches of a reference union ("VIEWs and/or subqueries nested too deep"), however the view is split. Trigger
programs inline the views, so on those hosts 301 write statements on 136 tables fail to compile. Cloudflare
rolls out runtime builds host by host, so the strict behavior is the one to design for. T1 and T2 fix
loading; only T6 fixes writes on strict hosts.

## Rules for the transform

1. **Deterministic and pure.** The same migrations always give the same output, byte for byte. The output is
   versioned with the transform's own version, and the version is recorded in the tenant object's migration
   ledger metadata.
2. **Fail loudly.** A construct the transform cannot handle, or output that still breaks a limit, stops
   provisioning or migration with an error naming the migration, the object and the limit. Nothing is ever
   skipped silently. The output is checked against every limit (statement length, compound terms,
   expression depth, union branches read per statement, TEMP, custom functions) before it is applied.
3. **Same results.** For every staging tenant, the transformed schema and the original return identical
   results:
   - every rewritten view: `EXCEPT` in both directions is empty;
   - every rewritten index key: equal on every row, plus a 10,000-string Unicode fuzz corpus;
   - reference bindings: identical rows after replaying the same writes;
   - change rows: identical to Rust's capture for the same writes (K03's `done_when`).

   The P02 harness runs Rust against the kernel on the same writes.
4. **Rust is untouched.** The Rust runtime keeps the original schema. Objects the transform adds (scratch
   tables, generated columns, partition views) are listed by name. The shadow (M02) and checksum tools
   exclude exactly that list, and nothing else.
5. **CI.** A job runs the transform over the app's current `staging` migrations (read-only fetch of
   `whagons5-client` `staging`) on every PR to `cloudflare/integration` and nightly. It fails on any new
   unhandled construct, so app drift shows up before cutover.

## Package

**A01** (`cloudflare/kernel/schema-transform/`, stage 1 wave 1, depends on S1 and P01; blocks K02, K03 and
K07). The `done_when` is in `work-packages.json`. Not a G0 blocker, because this note is the written
workaround. It **is a G1 blocker**: until T3 and T6 land, a task write cannot commit in an object.
