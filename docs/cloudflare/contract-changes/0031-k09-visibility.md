# 0031: K09 visibility: adaptations and one finding for K10

Date: 2026-11-11. Status: proposed (K09). Affects: K07, K08, K10, R03/R04, V06.

K09 ports visibility masks v2 in all three forms (SQL, interpreter, class programs), the parent ACL
indexes, the principal cache and the permission bits (`cloudflare/kernel/visibility/`). The Rust corpus
(`visibility_corpus.rs`) passes in full against Durable Object SQLite. No contract change is needed. This
note records the adaptations so the lead can rule on them, and one Rust behavior K10 must know about.

## Adaptations (no observable change)

| # | Rust | Kernel | Why |
|---|---|---|---|
| 1 | Index appliers are tokio tasks subscribed to the in-process feed before a REPEATABLE READ load | `ensure` reads the clock and the rows in one synchronous step, then brings every index it returns to the snapshot's clock, from transactions delivery pushed (`registry.apply`) or by reading the log for that table (paged) | Durable Object SQL is synchronous: no commit can fall between the clock and the rows. Poison rules (epoch, gap, pruned log, changed projection) are kept. |
| 2 | Classes are freed when their last `Arc` drops | `MemberVisibility.release()` / `HandleCache.clear()` | No deterministic destructors in JS. **K10 must release a binding on unsubscribe and on rebind**; an unreleased binding keeps its class registered (memory, not correctness). |
| 3 | Principal cache: capture classes from `gonvex_capture(` in trigger SQL, cached per `PRAGMA schema_version` | Entity tables are those whose three triggers write `_gonvex_sync_changes` (plain-SQL T5 capture); dropped by `invalidateSchema()` | `schema_version` is not authorized on the platform (K01 README §6). The tenant object calls `registry.invalidateSchema()` beside K01's. |
| 4 | `ctx.permissions.has` waits until the bits reach the Reducer's revision | The tenant object calls `registry.permissions.sync(session)` inside the Reducer's transaction before the first `has` (K04 port: `"rebuilding"` until then) | Same committed state; nothing to wait for in one object. |
| 5 | Fingerprints and stable class ids are SHA-256 of serde JSON | SHA-256 of canonical JSON | Server-side only (resume anchors; the batch header's class id is informational). |
| 6 | `shared_parent_resolution` deduplicates identical parent resolutions in memory | Not ported | Memory optimization only; the principal cache shares the SQL part. Listed for R03/R04. |
| 7 | A JSON number keeps integer/float identity (`1` ≠ `1.0` in the interpreter) | JS numbers do not (`1` = `1.0`, token text `1`) | Inherent to JS. SQLite agrees with the kernel; Whagons plans compare no REAL column. |

## Platform limit checked

The SQL form binds one JSON array per `inSet` (`json_each`), as Rust does. Over the 17 real Whagons plans
(`perf_fixture/whagons_module.json`), the largest predicate binds **48** parameters (a task child plan with its
inherited `tasks` rule), under the 100-parameter limit. A future plan that crosses 100 fails with K01's error
that names the limit. That would be a stop-and-report under rule 7.

## Finding for K10: Rust does not reveal an orphan's children when the parent appears

While writing the `visibility-masks` scenario: a note whose `task_id` names a missing task is visible only
through `onMissing` (its author). Then a task with that id is inserted that another Member can see (its
creator). **Rust never delivers the note to that Member** (no frame within 15 s, twice). The in-memory rule
says it is visible (`rowMatches` is true), and a fresh snapshot shows it. So the gap is in Rust's delivery
routing for a parent insert, not in the visibility semantics. The scenario covers only paths Rust delivers
deterministically (a parent moving out of reach, a parent deleted under its notes). The lead should rule on
whether K10 copies this gap for parity or delivers the row (a stage-3 fix in Rust).
