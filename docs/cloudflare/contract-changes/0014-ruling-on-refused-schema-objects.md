# 0014: Ruling on the 26 schema objects Durable Object SQLite refuses (0013)

Date: 2026-11-11. Status: **decided (lead); needs Gabriel's approval for the app-repo exception A01**.
Affects: A01 (new), S1, K02, K03, K07, G1.

## What the 26 objects are (read from `whagons5-client` `migrations/` and `scripts/`)

All 26 are on the **write path**, not only the read side. The triggers that read them fire when they run,
so in a Durable Object a write to the source table fails ("no such table") once its trigger runs:

| Group | Objects | Read by | Cause |
|---|---|---|---|
| D (depth) | unique index `roles_live_name_key_unique` on `recordStorage_roles`; views `whTaskGridProjection`, `whTaskCustomFieldSearchDocuments`, `whTaskTagSearchDocuments` | the index on every role write; the views by the `taskGridRows` and search-document projection triggers on every task, tag and custom-field write (`migrations/0001_sqlite_projections.sql:47,84,279-324`, `0003_sql_relationship_integrity.sql:10686-10692`) | One generator construct: Postgres `lower()` emulated as about 200 nested `replace()` calls, one per non-ASCII capital (`scripts/generate-sqlite-projections.mjs:7,42`), because SQLite's `lower()` folds ASCII only. Depth about 200-250 > 100. |
| R (reference) | 22 `referenceExpected_*` views (19 over the 5-term compound limit, 3 also over 100 KB) | `referenceBindings_<table>_source_insert` / `_source_update` triggers on the 22 source tables, including `tasks`, `templates`, `workplanItems` (`migrations/0004_sql_document_relationships.sql`, e.g. 4873-4875) | `scripts/schema/reference-sql.mjs:75-81` emits one `UNION ALL` term per JSON reference path (6 to 114) plus a `GROUP BY ownerId, path`. |

Consequence: without a fix, a Reducer that writes a task cannot commit in a Durable Object. That is a
**G1 blocker** (G1 needs a real Reducer to commit). It is **not a G0 blocker**, because G0 asks for a written
workaround, which this note is.

## Ruling: smallest fix per group, in the app's schema generator

The kernel never rewrites application DDL (it would silently diverge from Rust and break shadow checksums,
M02). Reference checks do not move into kernel code in stage 1 (that is L03's direction and much larger).
Instead, the **generator** emits SQL that loads on both stock SQLite and Durable Object SQLite and gives the
same results, and the change ships as an ordinary app migration on the **Rust runtime first**, so both
backends run one schema.

**Group D: stage the Unicode fold.**
- The views: compute the fold in at most three nested subquery stages (`SELECT …, <stage 2>(x) FROM (SELECT
  …, <stage 1>(name) AS x FROM …)`), each stage under 80 `replace()` deep. The views keep their names and
  output columns.
- The index: add VIRTUAL generated columns on `recordStorage_roles` that hold the staged fold (each under
  80 deep, the second built on the first). Keep `CREATE UNIQUE INDEX "roles_live_name_key_unique"` as an
  **expression** index with the same `CASE … 'owner' THEN 'tenant_owner' …` and the same `WHERE`, over the
  last stage. Same index name, so the module-visible error stays
  `UNIQUE constraint failed: index 'roles_live_name_key_unique'`.
- The run-collapse step (`generate-sqlite-projections.mjs:83`) stays as it is; it is already 3 deep.

**Group R: split each compound view into a tree of views.**
- Each leaf view holds at most 5 `UNION ALL` terms and stays under 90 KB. Each inner view unions at most 5
  child views. The top view keeps the name `referenceExpected_<table>`, its columns and its
  `GROUP BY ownerId, path` aggregation. So every trigger that reads it is unchanged.
- Shared `WITH RECURSIVE … MATERIALIZED` prefixes are repeated in each leaf that needs them.

## Proof required before A01 merges

1. On stock SQLite (the Rust runtime), for each of the 15 staging tenants: every rewritten view returns the
   same rows as the original (`EXCEPT` both ways is empty), and the generated index key equals the
   original expression for every `recordStorage_roles` row plus a fuzz corpus of 10,000 Unicode names.
2. On the platform (`gonvex-cf-dev-*`): the full tenant schema loads with **zero** refused objects, and S1's
   cold-wake and memory numbers are re-measured with it.
3. A duplicate live role name is refused in a Durable Object with the same error text as in Rust.
4. A task insert through the projection and reference triggers commits in a Durable Object.

## Package

New package **A01** (`work-packages.json`): a named app-repo exception like K21, owned in `whagons5-client`
`scripts/schema/`, `scripts/generate-sqlite-projections.mjs` and one new migration. It needs Gabriel's approval
because rule 5 forbids changing the Whagons app. It is released to Rust staging through the normal app release.
It depends on S1 and blocks K02, K03, K07 and G1.

`gonvex_record_value()` / `gonvex_capture()` in 1,153 insert triggers is a separate problem (0010) and is
not fixed by A01.
