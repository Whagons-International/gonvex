# 0010: Change capture relies on a custom SQL function and SQLite hooks

Date: 2026-11-10. Status: **decided in 0016 G0-4** (plain-SQL capture via the A01 transform, proven by S1). Affects: S1, K02, K03, L03.

## Finding

Tenant capture triggers call the custom function `gonvex_capture`, which is fed through SQLite hooks
(`rust/crates/store/src/capture.rs:378-440`). Logical record views call `gonvex_record_value`. Durable Object
SQLite allows neither custom functions nor hooks. The older design used plain `json_object` triggers; Rust
moved away from it because the trigger bodies totalled about 2.4 MB of SQL.

## What S1 must report in addition to its `done_when`

- Whether the tenant schema loads with plain-SQL capture triggers (`json_object` row images) in place of
  `gonvex_capture`, the total trigger SQL bytes, the largest single statement (against the 100 KB limit), and
  cold wake time and memory with them.
- The same for views rewritten without `gonvex_record_value`.

## Options for K03

1. Plain-SQL triggers, as above. Same change rows; costs schema size and wake time.
2. Capture in the kernel's write path (the L03 direction pulled forward): the storage layer records changes
   for statements it executes. Needs every write to go through the kernel, which holds for module SQL but not
   for trigger-driven derived tables, so derived-table capture still needs triggers.

K03's `done_when` (change rows equal Rust's for the same writes) applies whichever option is chosen.
