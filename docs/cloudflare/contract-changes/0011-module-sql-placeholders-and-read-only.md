# 0011: Module SQL placeholders and read-only Queries

Date: 2026-11-10. Status: decided (lead). Affects: K01, K04.

1. **`$N` placeholders.** Module SQL may use `$1, $2, …` in any order, and Rust binds them by number (sqlx).
   Durable Object SQLite binds by position, so `SELECT $2, $1` would swap values. K01 rewrites `$N` to `?N`
   before `exec` (outside string literals and comments) and has a test for out-of-order and repeated
   placeholders.
2. **Read-only Queries.** In Rust a Query is read-only because it runs on a read-only connection; the
   statement head-word check alone admits `WITH … DELETE`. The kernel runs Queries on the writer, so K01 must
   refuse a Query statement that writes: it checks the cursor's `rowsWritten` and aborts the Query's
   transaction with the same error class Rust returns for a write on a read-only connection.
3. **100 bound parameters.** Rust allows more. K01 chunks `IN (…)` lists that the kernel builds itself; module
   SQL that binds more than 100 parameters fails with a clear error naming the limit, and S1/S2 count how
   many Whagons queries do so.
