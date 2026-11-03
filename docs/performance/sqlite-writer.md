# SQLite tenant writer: thread model, group commit, checkpoints, cache

Workstream 8 of the 5,000-user SQLite runtime effort. All numbers come from
`rust/crates/store/examples/sqlite_writer_bench.rs` (store level, no runtime,
no V8) on arch3: Linux 7.2, NVMe under btrfs (`compress=zstd:3`, volatile
write cache with flushes), the process pinned to one CPU (`taskset -c 5`)
and one tokio worker to mirror the 1-vCPU box. Other agents were building on
the same machine, so disk-bound numbers vary between runs by up to 3x; each
comparison below was run back to back, and repeated where it mattered.

```sh
B=$CARGO_TARGET_DIR/release/examples/sqlite_writer_bench
BENCH_DIR=/path BENCH_OPS=20000 taskset -c 5 $B thread
BENCH_DIR=/path BENCH_SYNC=full|barrier|normal BENCH_WRITERS=1,10,50,200 taskset -c 5 $B commit
BENCH_DIR=/path BENCH_SYNC=normal BENCH_WRITERS=10 BENCH_RATE=400 BENCH_SECONDS=20 \
  GONVEX_SQLITE_CHECKPOINT=writer|background taskset -c 5 $B commit
BENCH_DIR=/path BENCH_TASKS=9000|90000 GONVEX_SQLITE_CACHE_BUDGET_MB=0|128 taskset -c 5 $B cache
```

The tenant database has the production change feed (`SYNC_INFRASTRUCTURE_SQL`,
pre-update-hook capture triggers), 2,000 presence rows and 9,000 tasks with
~400-byte descriptions. A "reducer write" is 9 of 10 times a heartbeat
(`UPDATE presence ...`) and 1 of 10 a task update (read, update, insert
event), each through `begin_transaction` → statements → `commit_with_revision`
exactly as the runtime runs them.

## What the code does today (resolved)

`registry.rs` sets `synchronous=NORMAL` on every connection, and that is
intended: since `51788b0` ("batched WAL sync barrier") durability comes from
`GONVEX_SQLITE_SYNCHRONOUS` (default `full`) through `sync_barrier`: after
COMMIT (writer already released) the transaction waits for one shared
`fdatasync` of the WAL before it is acknowledged or its revision published.
`docs/sqlite-storage.md` still described `synchronous=full` in the pragma and
`cache_size=-65536`; the code uses 8 MiB per connection
(`GONVEX_SQLITE_CACHE_KIB`) plus a 256 MiB shared mmap. The doc is corrected
on this branch.

## 1. Thread model: sqlx worker thread vs rusqlite

Per statement, and per heartbeat transaction (BEGIN, UPDATE, the ~11
statements of `reference_integrity::flush` + `feed::finalize`, COMMIT;
`synchronous=OFF` to isolate CPU), same connection, 20,000 ops, 1 CPU:

| path | mean | p99 | CPU/op | voluntary ctx switches/op |
|---|---:|---:|---:|---:|
| point read, sqlx | 14.9 µs | 25.9 µs | 14.7 µs | 2.0 |
| point read, rusqlite on a dedicated thread (job per statement) | 9.9 µs | 17.0 µs | 9.8 µs | 2.0 |
| heartbeat txn, sqlx (~14 round trips) | 272 µs | 557 µs | 262 µs | 22 |
| heartbeat txn, rusqlite thread, job per statement | 248 µs | 446 µs | 246 µs | 28 |
| heartbeat txn, rusqlite thread, **one job per transaction** | **147 µs** | 277 µs | 146 µs | 2 |
| heartbeat txn, inline on the caller (no thread at all) | 134 µs | 269 µs | 132 µs | 0 |
| sqlx BEGIN/UPDATE/COMMIT + finalize inline on the locked raw handle | 298 µs | 512 µs | 295 µs | 8 |

Breakdown of the inline transaction (µs): BEGIN 0.8, heartbeat UPDATE with
capture trigger and hook 15.8, finalize statements 26 in total (largest: the
ordinal `UPDATE ... row_number()` 9.3, JSON split 4.3, clock 3.6),
**COMMIT 95** (writing ~8 dirty pages to the WAL: presence, the change row
and its 4 indexes, clock, `_gonvex_sync_transactions`).

Findings:
- The handoff costs ~5 µs per statement on one CPU. A transaction executed as
  one job on a dedicated thread is 1.85x cheaper than sqlx (272 → 147 µs CPU,
  22 → 2 context switches). Most of that is sqlx's per-statement machinery
  (argument encoding, row channel, statement-cache lookups), not the thread
  itself: rusqlite with one job per statement is only 9 % faster.
- The narrow variant (keep sqlx, run finalize on `lock_handle()`) is
  *slower*, because a borrowed raw handle has no persistent prepared
  statements: re-preparing 11 statements costs more than the 9 handoffs it
  saves. Not kept.
- In absolute terms this is not the 5k bottleneck: at ~270 µs CPU per
  heartbeat, 100 heartbeats/s cost 2.7 % of one CPU. The commit path is
  dominated by fsync (section 2) and checkpoints (section 3).

**Recommendation (not implemented): migrate the writer to rusqlite on one
dedicated thread per database**, executing each write transaction's
store-owned part (claim, finalize, COMMIT) as one job. Plan:
1. `Database` gets a `WriterThread` (std thread + channel of
   `FnOnce(&mut rusqlite::Connection)` jobs); capture hook, WAL hook and
   pragmas install on it; `rusqlite =0.32.1` links the same
   `libsqlite3-sys 0.30.1`.
2. `TenantTransaction` keeps its API but owns a writer lease; `finalize`,
   `reference_integrity::flush`, `claim_reducer`, `store_reducer_result` and
   COMMIT become one job at commit.
3. Module SQL from `ctx.db` (host_calls.rs, ~40 sqlx call sites) moves last;
   with workstream 10(d) (run JS on a read snapshot, validate + commit on the
   writer) the writer only ever runs short "validate + apply + finalize +
   commit" jobs, which is exactly the one-job-per-transaction shape measured
   above, so the migration composes with it. Readers can stay on sqlx.
4. Reduce WAL pages per commit (COMMIT is 70 % of the inline cost): the change
   row carries four indexes; dropping `created_at` (prune can use `event_id`
   ranges) would remove a page write per commit.

## 2. Group commit on the tenant writer

### The reverted attempt (`bf1e6ad`, reverted in `4b45e72`)

`bf1e6ad` made concurrent write transactions *share one SQLite transaction*:
each member ran in a savepoint, the group committed once. Its commit message
lists the costs: a member dropped without finishing (a cancelled reducer
future) "leaves partial writes in the shared transaction that cannot be
undone from `Drop`, so it aborts the open group and every waiting member
receives an error"; members read each other's uncommitted writes; a failed
COMMIT fails every member; finished members were held open up to 25 ms
waiting for later members. Six minutes after the revert, `51788b0` replaced it
with the sync barrier: every transaction stays its own SQLite transaction and
only the *fsync* is shared. (The revert commit has no message; this is
inferred from the two commit messages and the timing.)

### Design kept: share the fsync, not the transaction

The existing barrier already is group commit and avoids every failure mode
above: COMMIT runs with `synchronous=NORMAL` (no sync while holding the
writer), then the transaction waits for the next `fdatasync` of the WAL that
starts after its COMMIT. Atomicity, ordering (revision assigned inside the
writer, in commit order), error isolation and cancellation are those of a
plain SQLite transaction; the revision is published only after the sync.
It composes with the 10(d) "validate + commit in microseconds" writer: the
writer job is short and never waits on the disk; durability waits happen off
the writer. This branch adds tests for it (`tests/writer_durability.rs`:
shared syncs with one revision and provenance per transaction; failed and
cancelled transactions interleaved with 20 committing ones leave exactly the
committed rows and no orphaned feed rows) and a `WAL_SYNCS` counter.

### Measurements (1 CPU, closed loop, 5 s per point)

| durability | writers | commits/s | p50 | p99 | CPU/commit |
|---|---:|---:|---:|---:|---:|
| `synchronous=FULL` in COMMIT (before `51788b0`) | 1 | 39 | 23.5 ms | 43.6 ms | 1.31 ms |
| | 10 | 29 | 286 ms | 760 ms | 1.45 ms |
| | 50 | 17 | 2.9 s | 3.0 s | 1.44 ms |
| | 200 | 37 | 5.1 s | 6.8 s | 1.34 ms |
| barrier (current default, `full`) | 1 | 124 | 7.3 ms | 15.4 ms | 1.01 ms |
| | 10 | 404 | 21.4 ms | 88 ms | 0.67 ms |
| | 50 | 970 | 45 ms | 251 ms | 0.47 ms |
| | 200 | 1,133 | 170 ms | 269 ms | 0.48 ms |
| `normal` (no sync before ack) | 1 | 1,529 | 0.31 ms | 17.6 ms | 0.38 ms |
| | 10 | 1,606 | 3.3 ms | 32 ms | 0.38 ms |
| | 50 | 1,677 | 23 ms | 56 ms | 0.38 ms |
| | 200 | 1,622 | 121 ms | 178 ms | 0.39 ms |

The barrier turns a disk-bound 17-39 commits/s into 1,133/s at 200 writers
(29x), within 70 % of no-sync throughput. A repeat run under heavier disk
contention from other agents gave barrier 27/133/420 commits/s at 1/10/50
writers; the shape holds, absolute values follow the disk.

## 3. Checkpoints off the writer (implemented)

SQLite's automatic checkpoint runs inside the COMMIT that crosses 1,000 WAL
frames: it copies the WAL to the database and syncs both (two fsyncs, even
with `synchronous=NORMAL`) while the tenant's other writers wait.

New `store/src/checkpoint.rs`: the writer gets a WAL hook (which replaces the
automatic checkpoint). Every 1,000 frames it asks one process-wide thread to
run `PASSIVE` on its own connection (never takes the write lock, commits
continue). Finding while building it: under a steady write stream the
background checkpoint never catches up completely (commits land while it
syncs), and only a fully copied WAL restarts from frame 0, so without a
fallback the WAL grew without bound (881 MB in a 24 s saturation run). The
writer therefore still checkpoints inline once the WAL reaches
`GONVEX_SQLITE_WAL_CAP_PAGES`; by then the thread has copied nearly
everything, so that inline checkpoint copies a small tail, but it still pays
two fsyncs. The cap sets how often that happens:

`sync=normal`, 10 writers offered 400 commits/s (achieved ~300-370/s), 20 s,
two runs each:

| checkpoint | commits/s | mean | p50 | p99 | p999 | max | WAL file |
|---|---:|---:|---:|---:|---:|---:|---:|
| in the writer (SQLite default, before) | 295 / 280 | 7.9 / 9.9 ms | 1.08 / 1.05 ms | 105 / 123 ms | 122 / 492 ms | 128 / 493 ms | 10 MB |
| background, cap 4,000 frames | 346 / 358 | 2.9 / 1.9 ms | 0.81 / 0.74 ms | 88 / 34 ms | 199 / 78 ms | 219 / 96 ms | 16 MB |
| **background, cap 16,000 frames (default)** | 363 / 367 | **1.5 / 1.2 ms** | 0.70 / 0.70 ms | **10.6 / 10.9 ms** | 160 / 55 ms | 179 / 63 ms | 64 MB |

p99 drops ~10x (105-123 ms → 11 ms) and mean latency 6-8x; p999 still sees
the rare inline cap checkpoint and the disk's fsync tail. With the barrier
(`full`) the checkpoint gain is hidden by the per-commit sync wait (10
writers at 100/s offered: p99 137-138 ms in the writer vs 139-215 ms
background, within run-to-run noise): commit latency then is the WAL fsync
(below).

Costs and risks: a busy tenant's WAL file reaches ~64 MB on disk (it is
reused, not grown further); readers look pages up in a larger WAL index and
read WAL-resident pages with `pread` instead of the mmap (not measured
separately). One extra connection per database is held by the checkpoint
thread (64 KiB cache); it is closed on `close_database`/`close`.
`GONVEX_SQLITE_CHECKPOINT=writer` restores the old behaviour.

## 4. `synchronous=normal` vs `full`

`fdatasync` after a 32 KiB append on this disk: p50 19 ms, p99 38 ms
(btrfs + a flushed volatile write cache, with other agents' IO). fsync
dominates every durable commit here: a heartbeat costs ~0.4 ms of CPU and
~20 ms of waiting for the disk. The barrier amortises it (section 2), so
`full` costs 30 % of peak throughput at 200 writers and ~7-20 ms of added
latency per commit at low concurrency.

What `normal` + WAL loses: nothing on a process crash or OOM kill (the WAL is
in the OS page cache); on power loss or kernel crash, the commits since the
last WAL sync, which is the last checkpoint or `fdatasync`, i.e. the last
seconds of acknowledged writes. The database stays consistent; clients that
saw those revisions would see them disappear. On the Mac lab (Docker on
macOS) `fsync` inside the Linux VM goes to a virtual disk and does not force
the host's drive cache (no `F_FULLFSYNC`), so syncs are much cheaper there
than here and also weaker; production numbers should be taken on the
production volume.

Recommendation: keep `full` (barrier) as the default as Gabriel prefers.
The fsync is off the writer and shared, so it costs latency, not writer
capacity. Note one existing subtlety: readers can see a commit (WAL snapshot)
before its barrier sync finishes; only the revision publication waits. A
snapshot taken in that window can include a row that a power loss would undo.

## 5. Page-cache memory (implemented: process budget)

`GONVEX_SQLITE_CACHE_BUDGET_MB` (default 128, 0 disables) sets
`sqlite3_soft_heap_limit64` once per process: SQLite's whole heap (page caches,
schemas, statements of every connection of every tenant file) recycles page
cache instead of growing past the budget. It is a soft limit; no allocation
fails. Idle readers already close after 15 s (runtime default) and pools open
readers lazily.

27 tenant files (one with `BENCH_TASKS` tasks, 26 with 300), 4 readers each
opened and scanned (135 connections), then 4 concurrent readers on the big
tenant, `cache_size` 64 MiB per connection (the brief's value):

| big tenant | mmap | budget | SQLite heap after opening | SQLite heap at end | range read p50 / p99 (1/40 of tasks) | full snapshot p50 |
|---|---:|---:|---:|---:|---:|---:|
| 90,000 tasks (~45 MB file) | 0 | none | 203 MB | 208 MB | 18.3 / 22.9 ms | 609 ms |
| | 0 | 128 MB | 79 MB | 14 MB | 28.1 / 55.1 ms | 728 ms |
| | 256 MiB (default) | none | 108 MB | 108 MB | 15.0 / 21.0 ms | 578 ms |
| | 256 MiB (default) | 128 MB | **30 MB** | **12 MB** | **14.3 / 22.5 ms** | 671 ms |
| 9,000 tasks (fixture size) | 256 MiB | none | 29.8 MB | 29.9 MB | 1.00 / 1.55 ms | 64.8 ms |
| | 256 MiB | 128 MB | 29.8 MB | 29.9 MB | 1.00 / 1.58 ms | 63.5 ms |

With the default mmap, the budget cuts SQLite's heap 108 → 12-30 MB at no
cost to point or range reads (point reads ~95-105 µs p50 in every
configuration, dominated by sqlx and tokio); full scans of a 90k-row table
were 0-16 % slower, within this machine's noise. Without mmap the budget
costs range reads ~1.5x (pages re-read with `pread`). At fixture size the
whole heap is ~30 MB (220 KB per connection: schema + statements), under the
budget, so nothing changes. Not measured: the Whagons schema, whose parsed
size per connection is far larger than this synthetic one (the Mac lab saw
311 MB of SQLite heap at 200 users); that is the case the budget is for.
Process RSS figures from this harness are dominated by the rows the harness
itself fetched and are not reported as SQLite's.

## 6. Writer hold time across V8 (documented and measured; owned by 10(d))

Where the tenant writer is taken and released on the Reducer path:

1. `execution.rs` `execute_tenant_reducer_with_access` builds `DatabaseHostCalls::lazy_reducer(pending)` (no
   connection yet) and calls `module_host.invoke(invocation, &mut handler)`.
2. The first `ctx.db` host call reaches `DatabaseHostCalls::ensure_transaction`
   (host_calls.rs) → `ControlPlane::begin_tenant_transaction` →
   `store::begin_transaction`: takes a runtime-wide admission permit
   (`pools.admit`), then `writer.begin()`, which takes the database's single
   writer connection (pool of one) and sends `BEGIN`. From here every other
   write transaction of the tenant waits in the writer pool's acquire queue
   (`acquire_timeout`, 30 s). The idempotency claim `INSERT` runs here too.
3. The writer stays held while V8 runs the rest of the reducer: every further
   `ctx.db` call is a JSON round trip over the module-host socket, and JS
   compute between calls holds it as well.
4. After `invoke` returns: `store_reducer_result` (if an idempotency key),
   then `finish_with_revision(true)` → `commit_with_revision`:
   `reference_integrity::flush`, `feed::finalize`, `COMMIT`; the writer
   connection returns to the pool when COMMIT completes. The `sync_barrier`
   wait and the change-feed `notify` happen after the writer is released.
   On failure `finish(false)` rolls back and releases.

Simulated JS time inside the transaction (`BENCH_HOLD_US=1500`, i.e. a
~1.5-2 ms reducer, as measured in the Mac lab), barrier durability, 1 CPU:

| writers | hold 0: commits/s, p99 | hold 1.5 ms: commits/s, p99 |
|---:|---:|---:|
| 1 | 50/s, 38 ms | 40/s, 43 ms |
| 50 | 852/s, 168 ms | 330/s, 247 ms |

With JS inside the transaction the writer's capacity falls to ~1/(JS time +
~1 ms): 2.6x lower here. Workstream 10(d) owns the fix (run JS on a read
snapshot, then a short validate + commit job on the writer); nothing on this
branch restructures the reducer path.

## Summary of changes on `perf/sqlite-writer`

Implemented:
- `store/src/checkpoint.rs`: background PASSIVE checkpoints with an inline
  cap fallback (`GONVEX_SQLITE_CHECKPOINT`, `…_CHECKPOINT_PAGES`,
  `…_WAL_CAP_PAGES`), counters for observability.
- Process-wide SQLite heap budget (`GONVEX_SQLITE_CACHE_BUDGET_MB`, default
  128), `sqlite_memory_used()`.
- `WAL_SYNCS` counter on the sync barrier; tests in
  `store/tests/writer_durability.rs` (group-commit sharing and per-transaction
  revisions/provenance, failed and cancelled transactions isolated, checkpoint
  thread runs and the WAL stays bounded, budget applied).
- `docs/sqlite-storage.md` corrected (synchronous, cache, checkpoints).
- Benchmark harness `store/examples/sqlite_writer_bench.rs`.

Measured / recommended only: rusqlite writer-thread migration (section 1),
fewer WAL pages per commit, keeping `full`.
