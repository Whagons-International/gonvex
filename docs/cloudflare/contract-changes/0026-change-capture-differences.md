# 0026: Two places where kernel change capture cannot equal Rust byte for byte

Date: 2026-11-11. Status: **open** (for the lead). Reported by K03. Affects: K03, K10, M02, P02.

K03's parity check replays the conformance scenario `change-capture` (18 Reducer transactions) through the kernel's
capture and compares with the Rust runtime's change log (`cloudflare/kernel/capture/test/capture-parity.test.ts`).
The bootstrap receipts, the 13 revisions, the 13 provenance rows and all 114 change rows match. 98 rows are
byte-identical. The other 16 differ only in the two ways described below.

## 1. REAL number text in row images

Rust's `gonvex_capture` reads values through the pre-update hook, which returns the **stored** record value:

- SQLite stores an integral REAL within ±2^47 as an integer. The hook sees `Integer(5)`, and the image says `5`. SQL
  (`NEW.amount`, `typeof` = `real`) sees `5.0`.
- Other REALs are formatted by Rust's float formatter, then re-rendered by SQLite in finalize's split
  (`new_value -> '$[1]'`). The results are `1e+300`, `1e-7`, `0.000025` and `1e+16`.

The kernel's plain-SQL images (A01 T5) see the value through SQL, and SQLite's `json_object` writes `5.0`,
`1.0e+300`, `1.0e-07`, `2.5e-05` and `10000000000000000.0`. `-0.0` is `0` in Rust and `0.0` in the kernel.

**As JSON values the two are equal**, and the digits are the same shortest round-trip digits. Clients parse the
values, and row hashes and digests are computed from parsed values, so no client sees the difference. Rust itself
writes the same value both ways: snapshots use SQL's `printf('%!.17g')` (`rows.rs` `exact_real`), while change rows
use the hook. Plain SQL cannot tell how SQLite stored a REAL, and emulating Rust's formatter in SQL would make every
REAL column's trigger expression several hundred bytes long.

**Proposal.** Accept this. Where conformance compares change rows or frames as text, compare JSON numbers by value,
with a named, unit-tested rule (K03's parity test does this in `test/numbers.ts`). Strings, keys, key order and
integer tokens stay exact. M02's shadow comparison should do the same.

## 2. Captured WITHOUT ROWID tables

Rust's entity capture trigger passes `NEW.rowid` / `OLD.rowid` to `gonvex_capture`. A `WITHOUT ROWID` table has no
rowid, so in Rust **every write that reaches a captured `WITHOUT ROWID` table fails** with `error returned from
database: (code: 1) no such column: NEW.rowid`. The fixture's `capture_pairs` shows it (`k03-09-pairs`). Rust's
module host also refuses module writes to a composite key outright, so only triggers reach such a table.

The transform drops the rowid argument, so the kernel would accept these writes. To keep Rust's behavior, K03 keeps a
reference to `NEW.rowid` in those triggers (`keepRowidReference`), and the kernel fails the same writes with the same
SQLite message.

Whagons is not affected. All of its `WITHOUT ROWID` tables (`schemaRelationshipOwners`, `formDefinitionKeys`,
`formFieldUsages`, `formSubmissionUsages`, `formVersionOwners_*`, `pollOptionKeys`, `pollSelections`) are declared
`derived` or `metadata`, so neither runtime installs capture triggers on them.

**Proposal.** Keep Rust's behavior, as K03 does. If the lead prefers the kernel to accept these writes, delete the
`keepRowidReference` call in `install.ts`. The parity test then flags `k03-09-pairs` as the one intended difference.
