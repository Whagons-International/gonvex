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
| POSTGRES-RAW-TRANSACTION-CONTROL | Raw `COMMIT` or `END` persisted writes before the invocation's outer rollback. Reject outer transaction commands while retaining savepoints and ordinary prepared statements. | `runtime/src/sql_admission.rs`: transaction boundary and savepoint/prepared-statement tests. |
| V8-QUEUE-DEADLINE | A call expired in the isolate queue and still executed. Bound admission and recheck the remaining budget before dispatch. | `module-runtime-v8/src/lib.rs`: `queued_invocation_respects_deadline_without_retiring_healthy_isolate`. |
| V8-INITIALIZATION-CANCELLATION | Cancelled startup returned capacity before the native heap was disposed. Initializers could also run beyond their budget. Hold capacity through disposal and bound CPU/async initialization. | `module-runtime-v8/src/pool.rs`: gated teardown, startup timeout, cancellation and later-invocation tests. |
| V8-HANDLER-LOOKUP-DEADLINE | An exported getter ran before the watchdog and could hang lookup. Arm the watchdog before resolving the handler and disarm every early return. | `module-runtime-v8/src/lookup_deadline_tests.rs`: lookup timeout, retirement, healthy reuse and invalid budgets. |
| V8-PENDING-SERIAL-HOST | A pending serial host future hid a ready native result. A pending host promise could also miss its deadline wakeup. Select native completion while host work waits and use the same deadline for the async event loop. | `module-runtime-v8/src/host_wait_tests.rs`: `pending_serial_host_cannot_swallow_deadline` and `pending_serial_host_cannot_swallow_native_result`. |
| POSTGRES-PARAMETER-CACHE-TYPE-CONFUSION | Reuse `SELECT $1` with integer 0, then text `abcdefgh`. The old cache returned bigint `7017280452245743464`. SDK CRUD with numeric and eight-character string IDs could target unrelated numeric rows. Describing varchar/char arrays also poisoned later text-array binds. Partition SQLx execution cache keys by actual bound OIDs and separate description keys. | `runtime/src/sql_parameters.rs`, `sql_statement_key.rs`, `host_calls_parameter_tests.rs`: changing scalar types, described arrays and actual SDK update/delete/deleteMany. |
| SQL-STATEMENT-ADMISSION | Valid dollar quotes, escape strings, trailing comments and leading comments were rejected. CR-only comments could hide a batch from admission, although PostgreSQL's extended protocol rejected that batch. Use one lexical pass for statement count and the first keyword. | `runtime/src/sql_statement_lexer.rs` and `host_calls_parameter_tests.rs`: literals, nested/CR comments, continued escape strings and healthy transactions after local batch rejection. |

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

The admission lexer assumes PostgreSQL `standard_conforming_strings=on` and is
not a full SQL parser. The separate migration parser in `postgres/src/sql_script.rs`
has not been replaced. Review custom session string modes and migration parsing
before claiming general PostgreSQL lexical support.

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
