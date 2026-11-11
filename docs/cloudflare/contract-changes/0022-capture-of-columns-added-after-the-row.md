# 0022: Rust's hook capture reports a column added by ALTER TABLE as NULL on old rows

Date: 2026-11-11. Status: **open** (for the lead; numbered 0022 because open PRs #338 and #340 both use 0020). Reported by A01. Affects: K03, P02, M02.

## Finding

Plain-table capture (`feed::capture_triggers_sql`, `gonvex_capture`) reads row values through SQLite's pre-update
hook. When a table gains a column through `ALTER TABLE … ADD COLUMN … DEFAULT d`, rows written before the ALTER do not
store that column. For such a row, `sqlite3_preupdate_old` returns NULL for the new column, while SQL (`OLD.col`,
`SELECT`) returns `d`. So the first UPDATE of such a row, even `SET x = x`, gives a Rust change row with:

- `old_value.<col>` = `null` (the row's logical value is `d`);
- `changed_columns` containing `<col>`;
- a change row at all, where no logical value changed.

Seen in the P02 fixture: `board.label TEXT NOT NULL DEFAULT 'v1'` is added in generation 2 (scenario
`receipt-upgrade`). After `UPDATE board SET title = title WHERE id = 'old'`, Rust emits
`update ["label"]` with `old.label = null`. A01's plain-SQL capture (T5) reads `OLD."label"` = `'v1'` and emits nothing.
That is the only difference in A01's replay of 13 harness tenants (120 writes) and 15 staging tenants (4,394 writes).

Whagons record capture is not affected. It reads images through the record view (`record_views::image`), so the
default applies. The current Whagons schema has no `gonvex_capture` triggers.

## Why the transform does not emulate it

Plain SQL cannot tell whether a column is stored in the record or filled from the default, so it cannot reproduce the
NULL. The plain-SQL result is the row's logical value, which is also what a client reading the row sees.

## Options

1. Treat Rust's output as a defect and the kernel's as intended behavior (add to the brief's intended-behavior list).
   Conformance scenarios that update rows from before an `ADD COLUMN` would then mask this one difference.
2. Fix Rust's hook capture to substitute the column default when `sqlite3_preupdate_old` has no value for an
   added column. That is a Rust change, so it waits for cutover.
3. Keep Rust as the reference. K03 would then need a way to see stored record length, and the platform offers none.

A01 recommends option 1.
