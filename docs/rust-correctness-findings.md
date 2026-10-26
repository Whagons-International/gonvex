# Rust correctness fixes found during the Bend experiment

These 12 failures were reproduced against staging commit
`c233a96cb198669540fa29bb638fd7860611e5b8`. This branch extracts their Rust fixes
onto current staging without the Bend implementation. A reproduced library
failure does not establish a deployed incident. No deployment is part of this PR.

Paths below are relative to `rust/crates/`. Each fix retains ordinary Rust tests.
PostgreSQL regressions need a disposable `GONVEX_TEST_POSTGRES_URL`; without it,
the test functions return early. CI supplies PostgreSQL.

| ID | Reproduction and corrected behavior | Permanent regression |
| --- | --- | --- |
| FRAME-EMPTY-ROUNDTRIP | Writing an empty frame succeeded although reading it failed. Reject empty payloads before writing any bytes. | `module-host/src/framing.rs`: `rejected_empty_frame_writes_no_bytes` and valid round trips. |
| GENERATION-EXHAUSTION | Reserve `u64::MAX`, then reserve automatically. The old code panicked in debug or wrapped in release. Return exhaustion without changing the counter. | `server/src/lib.rs`: `automatic_reservation_rejects_exhaustion_without_poisoning`; `server/src/generation.rs` boundary tests and Verus proof. |
| GENERATION-REUSE-AFTER-UNLOAD | Reserve, unload, reserve could issue 1 twice. Keep an issued-generation ledger across unload and shutdown. | `server/src/lib.rs`: `reservations_survive_unload_and_shutdown`, manual activation and concurrent reservations. |
| HOST-JSON-NUMERIC-ROUNDTRIP | Host JSON changed binary64 bits `659474afcab3011b` to `659474afcab3011a`. Enable serde JSON `float_roundtrip`. | `module-runtime-v8/src/dispatch.rs`: `host_payload_numbers_survive_json_lowering`. |
| POSTGRES-ABORTED-COMMIT | Catch a SQL error, then finish a reducer successfully. SQLx reported COMMIT success while PostgreSQL rolled back. Check transaction usability and await rollback on failure. Query snapshots retain their rollback/fallback behavior. | `postgres/src/transaction_commit_tests.rs`: aborted commit, healthy commit, explicit rollback, read-only fallback and deferred constraints. |
| POSTGRES-RAW-TRANSACTION-CONTROL | Raw `COMMIT` or `END` persisted writes before the invocation's outer rollback. Reject outer transaction commands while retaining savepoints and ordinary prepared statements. | `runtime/src/sql_admission.rs` and `host_calls_parameter_tests.rs`: unit admission, actual host rollback/savepoint histories and Query fallback after a caught SQL error. |
| V8-QUEUE-DEADLINE | A call expired in the isolate queue and still executed. Bound admission and recheck the remaining budget before dispatch. | `module-runtime-v8/src/lib.rs`: `queued_invocation_respects_deadline_without_retiring_healthy_isolate`. |
| V8-INITIALIZATION-CANCELLATION | Cancelled startup returned capacity before the native heap was disposed. Initializers could also run beyond their budget. Hold capacity through disposal and bound CPU/async initialization. | `module-runtime-v8/src/pool.rs`: gated teardown, startup timeout, cancellation and later-invocation tests. |
| V8-HANDLER-LOOKUP-DEADLINE | An exported getter ran before the watchdog and could hang lookup. Arm the watchdog before resolving the handler and disarm every early return. | `module-runtime-v8/src/lookup_deadline_tests.rs`: lookup timeout, retirement, healthy reuse and invalid budgets. |
| V8-PENDING-SERIAL-HOST | A pending serial host future hid a ready native result. A pending host promise could also miss its deadline wakeup. Select native completion while host work waits and use the same deadline for the async event loop. | `module-runtime-v8/src/host_wait_tests.rs`: `pending_serial_host_cannot_swallow_deadline` and `pending_serial_host_cannot_swallow_native_result`. |
| POSTGRES-PARAMETER-CACHE-TYPE-CONFUSION | Reuse `SELECT $1` with integer 0, then text `abcdefgh`. The old cache returned bigint `7017280452245743464`. SDK CRUD with numeric and eight-character string IDs could target unrelated numeric rows. Describing varchar/char arrays also poisoned later text-array binds. Partition SQLx execution cache keys by actual bound OIDs and separate description keys. | `runtime/src/sql_parameters.rs`, `sql_statement_key.rs`, `host_calls_parameter_tests.rs`: changing scalar types, described arrays and actual SDK update/delete/deleteMany. |
| SQL-STATEMENT-ADMISSION | Valid dollar quotes, escape strings, trailing comments and leading comments were rejected. CR-only comments could hide a batch from admission, although PostgreSQL's extended protocol rejected that batch. Use one lexical pass for statement count and the first keyword. | `postgres/src/sql_lexer.rs` and `runtime/src/host_calls_parameter_tests.rs`: literals, nested/CR comments, continued escape strings and healthy transactions after local batch rejection. |
| MIGRATION-SQL-LEXING | The staging migration splitter combined statements after CR-only comments, rejected valid escape strings, and split Unicode dollar quotes at embedded semicolons. Share the lexical scanner with runtime admission while retaining separate execution policies. | `postgres/src/sql_script.rs` regression cases and `provision.rs::migrations_keep_postgres_lexical_boundaries_in_both_transaction_modes`: actual migration execution and receipt-based replay in both transaction modes. |
| VISIBILITY-NULL-NOT-PARITY | A NULL status under `not(eq(status, "blocked"))` passed the Rust row evaluator but failed the SQL predicate. Compile negation as the complement of Boolean admission with `IS NOT TRUE`, including SQL UNKNOWN. | `runtime/src/visibility.rs::nullable_negation_matches_postgres_admission`: real PostgreSQL and Rust decisions across nine nullable/nonnull rows and thirteen nested Boolean expressions. |

## Exact database examples

On a reused SQLx connection, `SELECT $1 AS cache_probe` with parameters `0`, then
`"abcdefgh"`, previously returned `0`, then `7017280452245743464`. The cached
statement expected INT8 and PostgreSQL decoded the eight UTF-8 bytes as an integer.
The corrected path binds TEXT under a distinct statement key. Describe and
execution have distinct prefixes even for zero parameters.

`SELECT $1::varchar[] AS n` previously failed when describe populated a VARCHAR[]
entry and `Vec<String>` supplied binary TEXT[] elements. The same issue affected
`char[]`. The owned parameter plan derives both cache identity and encoding from
the actual SQLx Rust types. Raw SDK SQL, CRUD and structured live-query,
visibility and replica reads use this plan.

These single statements were incorrectly rejected before the lexer fix:

```sql
SELECT $$semi;colon$$ AS n
SELECT $$'$$ AS n
SELECT 42 AS n; -- trailing comment
SELECT E'a\';b' AS n
-- leading comment
SELECT 42 AS n
```

The string `SELECT 1 -- comment\r; SELECT 2`, with an actual CR, was admitted
locally but rejected by PostgreSQL as multiple statements. This was an admission
mismatch, not evidence that SQLx executed multiple statements.

## Verification boundaries and follow-ups

The [Verus pilot](verification/verus.md) proves allocation arithmetic against the
production function body. It does not prove registry concurrency, SQL parsing,
SQLx encoding, PostgreSQL transactions or V8 cancellation. Their ordinary Rust
regressions remain necessary.

The shared lexer assumes PostgreSQL `standard_conforming_strings=on` and is
not a full SQL parser. The migration adapter consumes statement ranges from the
same scanner used for runtime admission. Migrations permit batches, while module
invocations retain their single-statement and transaction-ownership checks.
Custom session string modes remain outside this scanner's contract. See
[PostgreSQL lexical rules](https://www.postgresql.org/docs/current/sql-syntax-lexical.html).

Migration lexer regression tests failed before the fix on staging
`0832985abbf13e43cb777459c215d754333d4294`. The PostgreSQL migration test
executes scripts containing CR-only comments, escaped quotes and Unicode dollar
tags with transaction mode both enabled and disabled, then checks values and
replay receipts. No deployed occurrence has been established. SQL lexing remains
covered by regression tests, with no formal proof claimed.

The aborted-commit guard adds a database round trip to successful reducer commit.
Typed statement keys add allocation and produce distinct cache entries per type
sequence. Benchmark these costs in representative application workloads before
performance promotion. The admission CPU microbenchmark is available as
`python3 rust/tools/benchmark_sql_admission.py --build-dir /path/to/build`.
It measures the guards alone, not end-to-end throughput.

Run full application flows for live-query, visibility and replica behavior before
production promotion. Tests that require object storage or an external module-host
binary must be reported separately from the database component tests. Preserve
this findings log regardless of whether the Bend experiment continues.

## Admission benchmark snapshot

The [Rust-only benchmark receipt](../verification/results/sql-admission-benchmark.json)
compares staging `f28b6cd18a1a9ee5ae13b2c35e8735023c02cc8a` with this lexer.
Median admission time over nine samples was 171 to 78 ns for a short query,
403 to 220 ns for a join, and 176,317 to 26,246 ns for a 100 KB literal.
A separate Rust build was active and CPU isolation was not used. These results
measure only admission CPU cost on valid samples; they establish neither a
server throughput result nor the cost of commit preflight or typed cache keys.

The [shared scanner benchmark](../verification/results/sql-lexer-benchmark.json)
compares the scanner with staging `0832985abbf13e43cb777459c215d754333d4294`.
It uses separate binaries with the same harness, warm-up, and nine paired rounds
with alternating process order. Local medians were 54 to 40 ns for a short query,
197 to 161 ns for a join, and 29,025 to 10,078 ns for a 100 KB literal.
The shared scanner borrows the first word and searches ordinary quoted contents
without visiting every byte in the Rust loop. Runtime read admission compares
that word case-insensitively. The quote search preserves doubled quotes and
keeps escape strings on their existing backslash-aware path.
These CPU-only samples use an active workstation without isolation and do not
measure cross-crate call overhead, database execution or server throughput.
Reproduce with `python3 rust/tools/benchmark_sql_lexer.py --build-dir /path/to/build`.

## Nullable visibility negation

Visibility negation complements the Rust row admission decision. A nullable
comparison to a non-null literal does not admit a NULL-valued row; negating that
comparison admits it. SQL `NOT` retains UNKNOWN and excluded the row instead.
The compiler now emits `(child) IS NOT TRUE`, which complements the child's
admission result, evaluates the child once, and composes with nested AND/OR/NOT.
See [PostgreSQL comparison predicates](https://www.postgresql.org/docs/current/functions-comparison.html).

The live PostgreSQL regression compares thirteen expressions over nine complete
rows covering NULL, blocked/allowed text, and nullable Boolean values. It failed
on the original staging implementation and passes after the fix. This is a
regression check for the recorded negation bug, not a formal proof of all SQL
types, missing fields, or visibility dependencies. No deployed occurrence has
been established.
