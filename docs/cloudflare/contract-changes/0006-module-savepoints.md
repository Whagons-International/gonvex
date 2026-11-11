# 0006: Module SQL that opens savepoints

Date: 2026-11-10. Status: **decided in 0016 G0-3** (nested storage transactions, proven by S2). Affects: K01, K04, K07, S2.

## Finding

The Rust SQL admission lets module code run `SAVEPOINT`, `RELEASE` and `ROLLBACK TO`
(`rust/crates/runtime/src/sql_admission.rs`), and the SDK's batch-write helper depends on it
(`packages/module-sdk/src/data-write.ts:332`: `SAVEPOINT gonvex_data_writes`). Durable Object SQLite refuses
these statements in `sql.exec`; nested transactions go only through `ctx.storage.transactionSync()`.

## Rule for the kernel

The contract is the module-visible behavior: a `SAVEPOINT name` / `RELEASE name` / `ROLLBACK TO name`
statement issued by module SQL behaves as in Rust. The kernel's storage layer (K01) intercepts these three
statement forms and maps them onto nested `transactionSync` calls or an equivalent the S2 spike proves. If S2
shows that a statement failure inside a module `try` leaves the DO transaction unusable, K01 must restore
Rust's semantics (failed statement rolls back only itself) or this note escalates to Gabriel before stage 1.
