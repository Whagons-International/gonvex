# Rust correctness fixes found during the Bend experiment

The first twelve failures were reproduced against staging commit
`c233a96cb198669540fa29bb638fd7860611e5b8` and fixed in Rust separately from Bend.
Later entries record additional regressions found during modularization and
verification work. A reproduced library failure does not establish a deployed
incident. Source fixes do not establish deployment.

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
| VISIBILITY-NULL-NOT-PARITY | A NULL status under `not(eq(status, "blocked"))` passed the Rust row evaluator but failed the SQL predicate. Compile negation as the complement of Boolean admission with null-safe predicates, including SQL UNKNOWN. | `runtime/src/visibility.rs::nullable_negation_matches_postgres_admission`: real PostgreSQL and Rust decisions across nine nullable/nonnull rows and thirteen nested Boolean expressions. |

| LIVE-QUERY-EXACT-COUNT-CACHE-TYPE | On one cached connection, exact-count live queries with integer 0, then float 1.0, returned the correct rows but total 0 for row 1. The count branch omitted the typed statement key used by the row branch. Use one owned `Parameters` plan for count identity and binding. | `runtime/src/live_query.rs::tests::exact_count_parameter_type_changes_match_rows_on_one_connection`: actual row/count calls with integer, float and integer parameters on one transaction. |
| SYNC-EARLY-FINALIZATION-LATE-WRITE | A Reducer inserts A, fires deferred constraints early, returns them to deferred, then inserts B. Both application rows commit, but B has NULL feed revision/ordinal and is omitted from revision-based reads. Reject tracked writes after this transaction has finalized, using its actual provenance row. | `runtime/src/host_calls_parameter_tests.rs::sync_finalization_late_write_has_no_unpublished_events`: provisioned triggers, admitted host queries, completion and durable rows/feed/provenance. |
| SYNC-SESSION-GUARD-SUPPRESSION | A Reducer commits a session-level `gonvex.sync_revision` setting. A later transaction on the same physical pooled connection commits a normal row with NULL feed revision and no transaction metadata. Finalization now consults current-transaction provenance; the writable setting is no longer authority. | `runtime/src/host_calls_parameter_tests.rs::sync_finalization_session_guard_does_not_poison_next_transaction`: explicit same-backend reuse and complete feed capture. |

## Exact database examples

On a reused SQLx connection, `SELECT $1 AS cache_probe` with parameters `0`, then
`"abcdefgh"`, previously returned `0`, then `7017280452245743464`. The cached
statement expected INT8 and PostgreSQL decoded the eight UTF-8 bytes as an integer.
The corrected path binds TEXT under a distinct statement key. Describe and
execution have distinct prefixes even for zero parameters.

`SELECT $1::varchar[] AS n` previously failed when describe populated a VARCHAR[]
entry and `Vec<String>` supplied binary TEXT[] elements. The same issue affected
`char[]`. The owned parameter plan derives both cache identity and encoding from
the actual SQLx Rust types. Raw SDK SQL, CRUD, structured live-query row and exact-count reads, visibility
and replica reads use this plan.

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
Negated literal equality uses `IS DISTINCT FROM`, or `IS NOT NULL` for a null
literal. Remaining negation uses `(child) IS NOT TRUE`. Each form complements the
child's admission result and evaluates it once. Boolean double negation lowers
directly to the original child, preserving indexable equality predicates.
See [PostgreSQL comparison predicates](https://www.postgresql.org/docs/current/functions-comparison.html).

The live PostgreSQL regression compares thirteen expressions over nine complete
rows covering NULL, blocked/allowed text, and nullable Boolean values. It failed
on the original staging implementation and passes after the fix. This is a
regression check for the recorded negation bug, not a formal proof of all SQL
types, missing fields, or visibility dependencies. No deployed occurrence has
been established.

The [query benchmark](../verification/results/visibility-negation-benchmark.json)
uses a temporary 31,000-row fixture with non-null values and equal admitted row
sets. The baseline and corrected forms use the same scan/index node types.
Nine paired rounds of 100-query batch means on one CPU gave median execution
times of 1.846/1.688 ms for negated equality, 1.089/1.084 ms for double negation,
and 2.174/2.173 ms for a negated conjunction. These are explicit SQL templates
matching the lowering rules, not SQL emitted by the Rust compiler during the
benchmark. They do not establish server throughput, parameter binding costs or
performance when the corrected predicate admits additional NULL-valued rows.

Reproduce with `GONVEX_TEST_POSTGRES_URL` pointing at a disposable PostgreSQL
instance and `python3 verification/benchmarks/visibility_negation.py --output
/path/to/result.json --repetitions 100`.


## Exact-count live-query statement reuse

The test-only regression failed against staging
`8bae698880d6a7e63472596b1a22c5dc1ced0ebc`. It pins one physical PostgreSQL
connection in a transaction, primes both actual row/count statements with an
integer predicate, then runs a float predicate and the original integer again.
The float remains a JSON float and reaches the exact-count branch. The observed
middle call returned row ID 1 but total 0. All row assertions passed before the
middle count assertion failed. This is a native library reproduction, with no
observed deployed occurrence or write-corruption claim for this count path.

The row query already included actual parameter OIDs in its SQLx statement key.
The separate count binder reused an unprefixed cached statement. The count now
uses `Parameters::scalars` for both the ordered key OIDs and the encoded values,
then decodes the count column as bigint. It retains the predicates and bindings
from before LIMIT/OFFSET, so the total still counts the full filtered relation.
The obsolete duplicate scalar binder is removed. SQL, pagination and public
items/total/offset/limit fields retain their meaning.

The regression returns to the integer type after the float call and checks all
three row results and totals. It uses a normal enabled SQLx statement cache and
runs in the required PostgreSQL CI suite. This test covers the remaining caller
omission; it is not a formal proof of SQLx, PostgreSQL or cache-key serialization.

## Sync finalization phase and pooled settings

Both sync findings were reproduced on staging `8bae698880d6a7e63472596b1a22c5dc1ced0ebc`
through `DatabaseHostCalls` with Reducer capability, production provisioning and
`TenantTransaction` completion. Neither reproduction used an external live tenant.
The late-write case committed A and B, but only A received revision 1. In the
session-setting case, backend PIDs matched across transactions and the later
application row committed while its feed revision and ordinal were NULL, the clock
remained zero, and transaction provenance was absent.

The correction uses the existing unique transaction-ID index on
`_gonvex_sync_transactions` for both stage admission and repeated-finalizer
recognition. A late tracked row change raises SQLSTATE `55000` with an explicit
sync-finalization error. Unrecovered errors roll back the invocation; savepoint
recovery must preserve complete capture of the surviving work. Early constraint
flushing can close the transaction to subsequent tracked DML, including after
constraints return to DEFERRED. Unrelated named constraints remain available.
The session setting may still exist, but it cannot suppress finalization.

The singleton clock update, one-batch drain, event-ID ordinals, command attribution
and notification stay in PostgreSQL's transaction. Actual SQL changes alter the
bootstrap digests, so the installer must upgrade both infrastructure and table
triggers atomically. No TypeScript, client storage or wire-format change is needed.
This does not protect against a database owner deliberately rewriting internal
provenance, functions, triggers or search paths.

The guards add indexed PostgreSQL checks per staged event and per finalizer call.
They add no network round trip but can increase bulk and serialized COMMIT work.
Their cost requires measurement; correctness tests do not establish maintained
throughput. These PostgreSQL behaviors are regression-tested boundaries, with no
formal verification of PL/pgSQL, MVCC, WAL durability or notification delivery
claimed. The independently observed clock-lock write bottleneck and held metrics
PR are separate from these missing-feed correctness findings. No deployed
occurrence or historical CPU-spike cause has been established.
