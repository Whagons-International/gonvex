# 0013: Tenant schema objects that Durable Object SQLite refuses

Date: 2026-11-10. Status: **ruled in 0014** (revised: kernel-side schema transform, A01). Reported by P03
(measured in local workerd with the real Whagons tenant schema). Affects: S1, P03, K02, K03, K09, L03.

## Finding (P03, local workerd)

The tenant schema loads except 26 objects:

| Limit | Objects |
|---|---|
| `SQLITE_LIMIT_COMPOUND_SELECT` = 5 terms ("too many terms in compound SELECT") | 19 `referenceExpected_*` views, 6 to 114 `UNION` terms |
| Statement text ≥ 100,000 bytes (`SQLITE_TOOBIG`) | `referenceExpected_tasks` (163 KB), `referenceExpected_templates` (176 KB), `referenceExpected_workplanItems` (240 KB); these also exceed the compound limit |
| Expression depth 100 ("Expression tree is too large") | views `whTaskGridProjection`, `whTaskCustomFieldSearchDocuments`, `whTaskTagSearchDocuments`; unique index `roles_live_name_key_unique` on `recordStorage_roles` (a nested `replace()` chain about 250 deep) |

Also:
- 1,153 `_gonvex_sync_*_insert` triggers call `gonvex_record_value()`, and capture triggers call
  `gonvex_capture()` (see 0010). A DO cannot register either, so those triggers fail when they fire.
- A whole-database `PRAGMA quick_check` returns `SQLITE_NOMEM` inside the object (per-table works);
  `integrity_check` is not authorized.
- Local workerd accepted a 2.1 MB row (the documented limit is 2 MB).

## Consequences

- `roles_live_name_key_unique` not loading means role-name uniqueness is **not enforced** in a DO. That is a
  semantic loss and blocks K02 until rewritten.
- All 26 objects are read by write-path triggers (see 0014), so writes to `tasks` and 21 other tables fail
  in a Durable Object until they are fixed.

## Rules until decided

- No package rewrites these objects on its own (brief rule 7). P03 reports them and still transfers every
  row; its checksums cover data, not refused schema objects.
- S1 confirms each item on the deployed platform and states, per object, whether a semantics-preserving
  rewrite exists (split compound views, flatten the `replace()` chain into a generated column or a
  kernel-side normalization, move capture into plain-SQL triggers or the kernel write path).
- Integrity checks in P03 and M06 use per-table `quick_check`.
