# Direct SQLite data path (workstream 8b)

Branch `perf/sqlite-direct` = `perf/native-reducers` (in-process V8) + `perf/sqlite-writer` +
`perf/fixture` + `perf/replica-state` (232ee8de, includes lab 3524878). Goal: run module SQL,
change-feed finalize and snapshot reads without sqlx's per-statement thread hand-off and
machinery, and without JSON encode → decode → encode of host-call results.

## Design

### One lease on the transaction's own connection (`store/src/direct.rs`)

sqlx stays the owner of every connection. `TenantTransaction::direct()` takes the connection
out of sqlx's worker for as long as the transaction uses it (`lock_handle`: one round trip, the
worker thread then parks on the connection lock) and returns a `DirectSession` (`Clone + Send`).
`session.run(|conn| ...)` executes statements through the C API on the calling thread:

- Prepared statements are cached per writer connection across transactions
  (`WriterStatements`, LRU, `GONVEX_SQLITE_DIRECT_STATEMENTS`, default 256). Reader sessions keep
  a per-transaction cache (reader connections close when idle). In the Whagons bridge run, 57
  statements were prepared once and reused 8,903 times.
- Bind rules are sqlx's (`$N`/`?N`/`?`, missing values stay NULL); errors are
  `sqlx::Error::Database` with sqlx's exact text and extended code
  (`error returned from database: (code: 2067) UNIQUE constraint failed: t.v`), so module code
  and error classification see the same failures.
- The lease ends before any sqlx use of the transaction (`transaction()` releases it), before
  commit/rollback, and on drop (field order: lease, then statement cache, then the sqlx
  transaction). After release `run` returns `None`; nothing can touch a connection sqlx owns again.
- Statements are finalized before sqlx can close the connection (sqlx panics on `sqlite3_close`
  with live statements): on `PoolRegistry::close`/`close_database`, and when the last owner of the
  cache drops (always before the pool's connection). The writer is opened in serialized mode so
  that finalize is safe even while sqlx runs an unrelated statement.
- Schema changes: `PRAGMA schema_version` is checked when a writer session starts and the cache
  is cleared on change; DDL run through module SQL clears it immediately. Column metadata read
  from cached statements (decltypes) is therefore never stale.
- Failure marks are shared: `DirectSession::mark_failed` sets the owning transaction's mark, so
  the store still refuses to commit a transaction whose SQL failed even if the caller takes the
  transaction out and commits it directly.

### Commit path

`commit_with_revision` now runs the reference-integrity check ("anything staged?") and
`feed::finalize` (`finalize_direct`, same statements in the same order) on the lease, then
releases it and lets sqlx issue `COMMIT` (sqlx tracks transaction depth). A staged reference
ledger (rare) still flushes through sqlx. Durability barrier, change-feed notify, revision
publication after commit, admission, the checkpoint thread and the cache budget are unchanged.
Pools no longer ping the connection on every acquire (`test_before_acquire(false)`: a local
SQLite connection does not drop like a socket); that removed one more worker round trip per
transaction.

### Module SQL on the isolate thread (`runtime/src/host_calls.rs`)

`DatabaseHostCalls` keeps its async API, but module SQL (`dbQuery/Insert/Update/Delete/
DeleteMany`) now lives in `SqlCalls`, behind a mutex shared with `SqlInline`, an
`InlineHost` (new trait in `module-runtime`) that the in-process engine calls **synchronously on
the isolate thread**:

- The V8 op checks capabilities first (unchanged), then offers the call to the inline host only
  when no ordered host call is outstanding (so calls never overtake each other). Before the
  Reducer's transaction is open, and for every non-SQL call (storage, outbox, scheduler, fetch),
  the call goes through the existing async path. The first SQL call of a Reducer therefore opens
  the lazy writer transaction exactly as before (admission, idempotency claim), locks the lease,
  and later calls run inline.
- Results are written as JSON bytes straight from the SQLite columns, with the same decoding
  rules as before (declared type, write-target catalog for `RETURNING`, JSON-shaped heuristic only
  for expression columns), and handed to V8 as the response bytes. The op embeds those bytes into
  its envelope (`{"status":"ok","value":<json>}`) instead of escaping them as a JSON string, and
  the bootstrap parses once. (Before: `Value` → `to_vec` → escaped into the envelope →
  `JSON.parse` envelope → `JSON.parse` value.)
- V8 never gets a handle: `SqlInline` only accepts the same frames, re-checks write capability,
  and runs the same admission (`sql_admission`) and SQL as the async handler.
- Statement failure semantics are unchanged: a failed statement aborts the invocation's
  transaction until `ROLLBACK TO`, and a caught failure never commits.

### Why the isolate thread and not a dedicated writer thread per database

Measured in place (bridge bench, `BRIDGE_BENCH_INLINE_THREAD=1` routes every inline call to one
dedicated SQLite thread with a blocking hand-off, i.e. "one writer thread per database, a job per
statement"):

| 30 real point reads per invoke (synthetic, 1 CPU) | base (sqlx) | dedicated writer thread | isolate thread |
|---|---:|---:|---:|
| invoke p50 | 1.53 ms | 0.78 ms | **0.28 ms** |
| marginal cost per host call | ~47 µs | ~19.5 µs | **~2.5 µs** |

A writer thread can only run whole transactions as one job when the transaction has no
JavaScript in the middle; a Reducer interleaves JS and SQL, so a writer thread means a hand-off
per statement (two switches, ~10 µs on one CPU plus wake-up latency). On the isolate thread the
statement runs where the request is made. Several isolates and one tenant: the writer
transaction (sqlx pool of one, FIFO) still admits one Reducer at a time per database, exactly as
before; the waiting isolate idles in its async op as before (1 isolate per generation on a 1-vCPU
box, so in production there is no second isolate to wait). Different tenants' isolates use
different connections, and reader sessions are per transaction.

### Snapshot/live-query reads (`store/src/json_rows.rs`)

`TenantTransaction::stream_json_rows(sql, binds)` runs the query on a blocking thread on the
transaction's reader connection and streams the first column (JSON text from `json_object`) in
`JsonRowBatch`es (≤128 rows / 256 KiB per allocation). The row type is described in
`json_rows.rs`: a row is the borrowed `&[u8]` UTF-8 JSON text SQLite produced, valid JSON,
not re-encoded or parsed; consumers choose how to materialize it. Dropping the stream interrupts
the query (progress handler), so releasing the lease never waits behind a long scan.
`replica::scan_window` (perf/replica-state's snapshot path) and `live_query` use it and still
decode each row with `serde_json::from_slice` into `Value`, because the replica code consumes
`Value`; the bytes are available for an encode-once consumer (perf/delivery) without changes here.

### What still uses sqlx

Control-plane queries, migrations/provisioning, admin, storage metadata, scheduler, visibility
and membership projections, `BEGIN`/`COMMIT` of every transaction (one round trip each, plus the
lease round trip), the reference-ledger flush when rows are staged, and the socket module host
transport. With `GONVEX_MODULE_HOST_IN_PROCESS=false`, module SQL runs directly on the lease but
on the Tokio task (no isolate thread to run it on).

## Measurements

arch3, shared with other agents' builds (load average 10–17 on 14 cores); everything pinned with
`taskset -c 5`, one Tokio worker. Base = the same tree without this branch's commits
(`perf/native-reducers` + `perf/sqlite-writer` + `perf/fixture` [+ `perf/replica-state` for the
snapshot rows]). p95/p99 values are noisy; compare medians, CPU and switch counts.

### Reducers, real Whagons bundle, in-process V8 (bridge bench, each invoke rolls back)

`BRIDGE_BENCH_MODE=in-process`, `v8-lab/manifest.json`, lab tenant DB, 200 samples:

| Reducer (host calls) | invoke p50 base → new | writer hold p50 base → new | hold p95 base → new | CPU/invoke base → new |
|---|---|---|---|---|
| tasks.update (27) | 3.97 → **2.75 ms** (−31%) | 3.88 → **2.66 ms** | 5.53 → 4.82 ms | 4.50 → 3.59 ms |
| tasks.updateDescription (27) | 3.98 → **2.62 ms** (−34%) | 3.94 → **2.52 ms** | 6.68 → 4.39 ms | 4.55 → 3.18 ms |
| taskResources.createNote (27) | 3.69 → **1.98 ms** (−46%) | 3.61 → **1.88 ms** | 5.40 → 2.80 ms | 4.18 → 2.27 ms |
| tasks.create (31) | 4.39 → **2.12 ms** (−52%) | 4.19 → **1.93 ms** | 6.31 → 2.81 ms | 5.32 → 2.55 ms |

Variants (same binary): direct sessions without the inline path (all calls via the Tokio task):
tasks.update hold 3.81 ms; dedicated writer thread: 3.33 ms. The remaining ~1.0–1.6 ms of handler
time per tasks.update is SQLite executing the bundle's statements (per-statement trace,
`BRIDGE_BENCH_TRACE=1`: 7–110 µs per query, ~300 µs for the task UPDATE with capture trigger, ~290 µs
for the notifications `INSERT … SELECT`); the next gains there are query plans/indexes, not
transport. Null handler (bridge only, envelope change): 30 calls 0.77 → 0.74 ms p50.

Not measured here: commit (each invoke rolls back; commit is measured below).

### Host-call / statement cost with real SQLite (store `txn` mode, Durability::Normal, 20,000 ops)

| | base | new |
|---|---:|---:|
| point read inside a write transaction, sqlx | 9.7 µs p50, 11.1 µs CPU, 1.95 switches | (same path) |
| point read on the direct session | — | **0.9 µs p50, 1.2 µs CPU, 0 switches** |
| heartbeat txn (BEGIN, UPDATE, finalize, COMMIT), UPDATE via sqlx | 403 µs p50, **432 µs CPU, 30.6 vol. switches** | 276 µs, 293 µs CPU, 12.6 switches |
| heartbeat txn, UPDATE on the direct session (the Reducer path now) | — | **263 µs p50, 270 µs CPU, 10.6 switches** |

(Run-to-run spread on this machine ~±15% for CPU/op.) Remaining switches: pool acquire,
`BEGIN`, lease lock, `COMMIT`, connection return task, checkpoint thread wake-ups.
Fixture `reducer/control.support.heartbeat/js_0us` (native control-plane path,
`GONVEX_SQLITE_SYNCHRONOUS=normal`): 233 → 212 µs median, 139 → 122 allocations/op.

### Commits/s, reducer-shaped writes (store `commit` mode, 5 s per point, closed loop)

`new` = TenantTransaction with sqlx statements (finalize direct); `direct` = statements on the
lease too (what module SQL does now). Commits/s · CPU per commit · voluntary switches per commit:

| durability | writers | base | new | direct |
|---|---:|---|---|---|
| full (barrier) | 1 | 42 · 1,316 µs · 38.8 | 48 · 1,166 µs · 20.2 | 42 · 1,159 µs · 16.7 |
| | 10 | 212 · 657 µs · 30.4 | 224 · 505 µs · 12.6 | 60 · — (one 7 s fsync stall in the run) |
| | 50 | 775 · 459 µs · 28.8 | 757 · 335 µs · 10.9 | 833 · 332 µs · 8.5 |
| | 200 | 1,848 · 385 µs · 28.2 | 2,082 · 303 µs · 10.5 | 1,715 · 283 µs · 8.2 |
| normal | 1 | 2,401 · 345 µs · 27.1 | 2,225 · 338 µs · 10.2 | 1,996 · 331 µs · 7.7 |
| | 10 | 2,502 · 330 µs · 27.5 | 1,860 · 339 µs · 9.9 | 2,231 · 303 µs · 7.5 |
| | 50 | 2,061 · 365 µs · 27.3 | 2,010 · 342 µs · 9.8 | 2,405 · 287 µs · 7.8 |
| | 200 | 2,383 · 332 µs · 27.4 | 1,873 · 339 µs · 9.7 | 2,400 · 292 µs · 7.7 |

Throughput is bound by fsync (full) or by this machine's shared disk and CPU (normal) and moves
±25% between runs; the stable effects are 3–4x fewer context switches per commit and 10–25% less
CPU per commit. In the runtime the CPU saving per Reducer is larger because every module
statement now skips sqlx (table above).

### Snapshot reads (fixture, on top of perf/replica-state)

Query + decode of the 9,000-row admin statement (`json_object` over all task columns), 15 iterations:

| variant | median | allocations/op | allocated/op | peak heap |
|---|---:|---:|---:|---:|
| sqlx `fetch` + `serde_json::from_str` → `Value` (before) | 214 ms | 722,536 | 138 MiB | 73.0 MB |
| direct stream + `from_slice` → `Value` (what `scan_window` does now) | 198 ms | 694,861 | 153 MiB | 73.1 MB |
| direct stream, bytes only (what an encode-once consumer would read) | **84 ms** | **326** | 33 MiB | **2.5 MB** |

Snapshot open through the runtime (`replica_state` bench, perf/replica-state's `scan_window`),
base → new, median / allocations / peak heap: Admin 459 → 499 ms (noise; p50 of 10, load 13–17),
1,170,229 → 1,140,390 allocs, 73.3 → 73.5 MB; TeamAllSpots 162 → 162 ms, 449,853 → 439,010,
27.8 → 28.0 MB; TeamSpots 57.0 → 55.2 ms; GrantOnly 25.2 → 25.6 ms. The fixture's
`snapshot_open/…/Admin` bench: 511 → 365 ms median, 1,171,572 → 1,141,294 allocs.
The reader change removes sqlx's ~3 allocations per row and moves the query off the async worker;
time and peak heap of an open stay dominated by `Value` materialization and hashing in the replica
code, which the bytes-only row type lets the next step remove.

### Not measured

V8 + commit end to end (bridge bench rolls back), the 1,950-member tenant, concurrency of several
isolates on one tenant (pool size 1 in all runs), memory of the writer statement cache on the
Whagons schema (57 statements in the bridge run). The fixture's `reducer/presence.beat` and
`reducer/tasks.*` benches fail on both base and new after the replica-state/lab merge
(`function_not_found` from the fake host); they were not used.

## Risks

- Unsafe code: the `'static` extension of sqlx's `LockedSqliteHandle` (sound because the lease is
  always released before the transaction, see `LockedHandle` in `store/src/lib.rs`), raw statement
  handles, and finalize-before-close. If sqlx ever closed the writer for another reason (it does
  not with `idle_timeout(None)`, `max_lifetime(None)` and no ping), its `Drop` would panic on our
  statements.
- A long module SQL statement now runs on the isolate thread; the V8 watchdog cannot interrupt
  native code, so the deadline is enforced when the statement returns (before, the statement kept
  running on sqlx's thread after the isolate was terminated). Snapshot streams are interruptible.
- Byte-limited collections (`max_bytes`) still measure rows through `stable_json`, unchanged.
- Client/protocol contract: unchanged. Host-call results and errors are byte-for-byte the same
  JSON values (tests compare inline vs async results and sqlx vs direct decoding).

## Tests

- `store/tests/direct_session.rs`: change-feed rows and statement reuse across transactions;
  sqlx and direct statements in one transaction; rollback, cancellation (drop with a held lease),
  error isolation with interleaved committing transactions; revision publication after commit and
  in commit order; statement-cache invalidation after a sqlx migration and after DDL through a
  session; direct JSON = sqlx `decode_value` byte for byte; a dropped row stream stops its query.
- `runtime/src/in_process.rs`: module SQL runs on the isolate thread with the same results as the
  async path; failed statement aborts until `ROLLBACK TO`; a caught failure never commits.
- Existing `host_calls`, `host_calls_parameter_tests`, writer durability and replica tests pass
  unchanged.

## Reproduce

```sh
cd rust
cargo test --release -p gonvex-runtime --lib --no-run   # bridge bench binary
BRIDGE_BENCH_MODE=in-process BRIDGE_BENCH_MANIFEST=.../v8-lab/manifest.json \
  BRIDGE_BENCH_DB=.../v8-lab/tenant/<tenant>.db taskset -c 5 <test binary> bridge_bench_whagons --ignored --nocapture
#   BRIDGE_BENCH_INLINE_THREAD=1 (dedicated thread), BRIDGE_BENCH_NO_INLINE=1, BRIDGE_BENCH_TRACE=1
cargo build --release -p gonvex-store --example sqlite_writer_bench
BENCH_DIR=... taskset -c 5 sqlite_writer_bench txn                # BENCH_PATH=sqlx for sqlx only
BENCH_DIR=... BENCH_SYNC=barrier|normal [BENCH_PATH=direct] taskset -c 5 sqlite_writer_bench commit
BENCH_FILTER=snapshot_ cargo bench -p gonvex-runtime --features perf-fixture --bench baseline
BENCH_FILTER=snapshot_open cargo bench -p gonvex-runtime --features perf-fixture --bench replica_state
```
