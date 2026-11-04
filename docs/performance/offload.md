# Offload: heavy replica work off the network thread

Workstream 6 of the 5k-users program. Goal: on 1 vCPU / 2 GB, no poll over
5 ms on the network worker, bounded memory during a reconnect burst, snapshot
pages streamed instead of materialized.

## What changed

`#[tokio::main]` gives the process one tokio worker per CPU, so in a 1-vCPU
container every socket shares one thread. Before this change that thread also
built replica snapshots (SQL rows, JSON decode, interning, visibility), applied
feed events per connection (visibility, deltas), encoded snapshot pages and
freed closed subscriptions. All of it now runs on a pool
(`rust/crates/runtime/src/offload.rs`):

| Work | Call site | Where it runs now |
| --- | --- | --- |
| Snapshot preparation (`open_replica_negotiated`) | `ReplicaOpenQueue::start` (`replica_open_queue.rs`) | pool (`offload().spawn`), still cancelled when the queue drops it |
| Feed application per connection (`apply_feed_event`) | socket loop, `Runtime::apply_feed_event_offloaded` | pool when the table set's recent max cost is >= 250 us; subscriptions move to the job and back, the socket awaits it, so ordering is unchanged |
| Paged snapshot encoding (`SnapshotPages`) | `send_replica_open`, legacy page delivery | pool, one page at a time (`PageEncoder`): the page iterator moves to the job and back |
| Native snapshot frames (`SnapshotDelivery::pump`) | `send_snapshot_frames` | pool when a pending snapshot has >= 64 rows left |
| Non-paged snapshot frame JSON | `send_replica_open` | pool for >= 64 rows |
| Freeing removed/closed subscriptions | socket `replicas` is an `offload::RetiringMap` (`clear`, `remove`, drop) | pool (`offload::retire`) |

Snapshot admission (`SnapshotAdmission`, replaces `replica.rs`'
`snapshot_permits`): a count bound (`GONVEX_REPLICA_SNAPSHOT_CONCURRENCY`,
default CPUs, min 2, unchanged) plus a byte budget
(`GONVEX_REPLICA_SNAPSHOT_BUDGET_BYTES`, default 128 MiB of estimated decoded
heap). A build first takes a count slot, then reserves its estimate: the last
observed size of the same collection for the same member, else the largest of
the table, else 8 MiB; sizes are sampled JSON bytes x 5 (an Admin's 9,000 task
rows: ~16 MB JSON, +76 MB heap). Both semaphores are FIFO; an estimate larger
than the budget reserves the whole budget, so that build runs alone instead of
never. `collect_snapshot_rows` no longer serializes every row to count bytes
(only when `maxBytes` is set, and without allocating).

Edits to replica.rs / lib.rs / snapshot_delivery.rs are call-site wrapping
only (plus the admission call and `SnapshotPages::remaining_rows`), so the
replica-state, visibility and delivery workstreams merge over them. The
visibility parent-ACL applier and every pool job are wrapped with
`gonvex_profiler::instrument` (kinds `feed`, `snapshot`, `other`).

Configuration: `GONVEX_OFFLOAD` = `runtime` (default) | `inline` (previous
behaviour) | `blocking` | `threads`; `GONVEX_OFFLOAD_THREADS` (default CPUs,
1..=4); `GONVEX_OFFLOAD_NICE` (default 0); `GONVEX_OFFLOAD_MIN_US` (250).

## Measuring: the stall driver

`rust/crates/runtime/benches/stalls.rs` + `src/perf_fixture/stalls.rs`, on the
shared fixture (9,000-task tenant, 78 members). The network runtime has one
worker; the process is pinned to one CPU (`taskset -c 13`), so the pool sizes
itself to 1 thread and snapshot concurrency to 2, like a 1-vCPU container.

```sh
cd rust   # so rust/.cargo/config.toml applies
cargo bench -p gonvex-runtime --features perf-fixture --bench stalls --no-run
GONVEX_OFFLOAD=inline STALL_SCENARIOS=idle,opens,fanout,burst STALL_BURSTS=200,1500 \
  taskset -c 13 $CARGO_TARGET_DIR/release/deps/stalls-<hash>
# STALL_FANOUT=presence, STALL_BURST_RETAIN=0, STALL_SECONDS, STALL_OPEN_RATE, STALL_UPDATE_RATE
```

Probes (all on the network runtime): a 1 ms sleep loop recording wake lateness
(`lag`), a loopback TCP echo served by a network task and pinged every 5 ms by
an OS thread (`echo`), every poll of the driver's socket-like tasks and of
inline pool jobs timed on the network thread, as wall time and as **thread
CPU time** (`CLOCK_THREAD_CPUTIME_ID`), plus the profiler's `tokio.sched_lag`
(10 ms probe) and long polls by task kind. Scenarios: (a) `opens`: 10 opens/s
of `tasks.recentWorkspaceTasks` cycling over all 78 members (socket queue,
install, paged encode, close), (b) `fanout`: every member holds all 10
subscriptions in its own task and 4 task updates/s (or 20 presence beats/s) go
through `apply_feed_event_offloaded` + JSON encode, (c) `burst`: N concurrent
opens across members, subscriptions retained (or dropped, for transient memory).

**The machine was shared** (arch3, ~60% busy on every CPU from other agents'
builds; the bench ran in a low-weight cgroup). Wall-clock lag and poll maxima
include tens of ms of preemption by other processes (idle lag max 15-40 ms with
no load at all). Thread-CPU poll times are immune to that and are the primary
"no poll over 5 ms" metric. Throughput and delivery latency varied +-40%
between identical runs; treat them as indicative.

## Results (final run, `GONVEX_OFFLOAD=inline` = before, `runtime` = after)

Network thread, thread-CPU time per poll (max, and polls over 5 ms):

| Scenario | Before: CPU poll max / >5 ms | After: CPU poll max / >5 ms |
| --- | --- | --- |
| idle | 0.08 ms / 0 | 0.09 ms / 0 |
| (a) opens 10/s | 147.8 ms / 85 | 2.5 ms / 0 |
| (b) fan-out 4 task updates/s | 96.9 ms / 317 | 0.75 ms / 0 |
| (b) fan-out 20 presence beats/s | 0.69 ms / 0 | 0.83 ms / 0 |
| (c) burst 200 opens | 135.1 ms / 164 | 0.97 ms / 0 |
| (c) burst 1,500 opens | 151.4 ms / 1,177 | 1.7 ms / 0 |
| (c) burst 200, subscriptions dropped | 124.2 ms / 233 | 0.72 ms / 0 |

Scheduler lag (1 ms probe; p99 / max) and echo round trip (p99 / max), ms:

| Scenario | Before lag | After lag | Before echo | After echo | Profiler `sched_lag` p99/max before -> after |
| --- | --- | --- | --- | --- | --- |
| idle | 4.0 / 16.7 | 3.8 / 36.3 | 3.8 / 19.4 | 4.4 / 50.6 | 5.9/14.1 -> 6.3/27.4 |
| (a) opens 10/s | 31.2 / 182.3 | 4.2 / 42.1 | 59.9 / 177.0 | 1.9 / 37.4 | 77.2/177.1 -> 8.4/37.3 |
| (b) task updates 4/s | 371.0 / 454.7 | 3.1 / 28.6 | 436.8 / 451.1 | 2.4 / 12.0 | 489.9/510.0 -> 4.6/28.3 |
| (b) presence 20/s | 17.6 / 65.6 | 3.5 / 9.6 | 19.5 / 62.6 | 3.9 / 7.6 | 27.3/57.3 -> 4.9/8.8 |
| (c) burst 200 | 23.4 / 142.3 | 3.1 / 26.2 | 32.3 / 137.0 | 1.3 / 10.1 | 46.1/133.3 -> 4.2/114.3 |
| (c) burst 1,500 | 25.1 / 175.8 | 2.9 / 67.5 | 34.1 / 174.4 | 1.0 / 56.5 | 49.4/176.1 -> 3.1/59.8 |

After the change the remaining wall-clock maxima coincide with idle-run maxima
of the same size (other processes on the CPU); the network thread's own polls
stay under 2.5 ms of CPU. The profiler's long-poll ring shows the heavy polls
moved to `snapshot` (up to ~80-140 ms), `feed` (60-130 ms) and `other` (page
encode, 15-50 ms) on the pool thread, and no `ws_connection` poll over 5 ms
in CPU terms.

Work completion (same runs; noisy):

| Scenario | Before | After |
| --- | --- | --- |
| (a) open latency p50 / p99 (opens queue up: 10/s exceeds 1 CPU) | 3.2 / 3.8 s | 5.5 / 6.3 s |
| (b) task update -> last connection encoded, p50 / p99 | 163 / 757 ms | 917 ms / 2.8 s (another run: 88 / 188 ms vs 120 / 557 ms inline) |
| (b) presence beat delivery p50 / p99 | 9 / 50 ms | 7 / 14 ms |
| (c) burst 200 completion | 24.4 s | 29.2 s (another run: 16.3 s) |
| (c) burst 1,500 completion | 186 s | 156 s (another run: 107 s) |

The pool does the same CPU work, so total throughput on one CPU is unchanged
within the noise; per-delivery latency of offloaded work depends on the pool's
queue (all 78 feed jobs of an update interleave on one pool thread).

## Strategy comparison (`GONVEX_OFFLOAD`, one run each, pool nice 10 at the time)

| Strategy | Empty job round trip p50 / p99 | Max network CPU poll (a / b / c) | Notes |
| --- | --- | --- | --- |
| `inline` | 0.2 us (spawn) | 114 / 118 / 108 ms | previous behaviour |
| `runtime` (separate tokio runtime) | 6.3 / 12.6 us | 4.0 / 1.7 / 1.5 ms | chosen |
| `blocking` (`spawn_blocking` + semaphore + `Handle::block_on`) | 9.0 / 21.1 us | 0.5 / 15.3 / 1.2 ms | blocking threads cannot be niced back; futures' timers/IO driven by the network runtime |
| `threads` (fixed OS threads, `block_on`, one job each) | 6.8 / 11.5 us | 11.4 / 1.7 / 0.7 ms | a thread idles while its job waits on SQLite |

All three isolate the network thread. `runtime` was chosen: lowest overhead
(tied with `threads`), jobs interleave while waiting on SQLite instead of
holding a thread, no coupling to the network runtime's timer/IO driver, and
cancellation is a plain `JoinHandle::abort`. In that run `blocking`/`threads`
completed the 200-burst faster (15.9 / 20.9 s vs 36.8 s); later `runtime` runs
took 16-33 s, so that difference is within this machine's noise and was not
resolved.

Pool thread priority: an early run with nice 10 isolated as well as nice 0
did later; nice was not shown to help and risks starving the pool's cgroup
share on a shared host, so the default is 0 (`GONVEX_OFFLOAD_NICE`).

## Memory during bursts

Heap from the counting allocator (process-wide). "Transient" = peak minus what
is still live at the end. Snapshot concurrency default (2 on one CPU).

| Burst | Mode | Peak heap | Retained at end | Transient |
| --- | --- | --- | --- | --- |
| 200 opens, retained | before | 175.6 MB | 109.3 MB | 66.3 MB |
| 200 opens, retained | after | 175.5 MB | 109.3 MB | 66.2 MB |
| 1,500 opens, retained | before | 386.9 MB | 316.9 MB | 70.0 MB |
| 1,500 opens, retained | after | 387.1 MB | 315.8 MB | 71.2 MB |
| 200 opens, dropped | before | 166.3 MB | 3.4 MB | 162.9 MB |
| 200 opens, dropped | after | 161.0-183.9 MB | 30.5 MB | 130-153 MB |
| 200 opens, dropped, concurrency 16, budget 128 MiB | after | 226.1 MB | 30.5 MB | 195.5 MB |
| 200 opens, dropped, concurrency 16, no budget | after | 226.3 MB | 30.5 MB | 195.7 MB |

- The transient peak does not grow with the burst (66 -> 71 MB from 200 to
  1,500 opens): builds are bounded process-wide and pages are encoded one at a
  time per socket, never materialized as a full encoded snapshot. The retained
  part is the per-subscription mirror (~0.2 MB per `tasks` subscription here),
  which workstream 5 removes; it, not the burst, is what does not fit 2 GB at
  5k users.
- 1,500 users with all 10 subscriptions each: not run (projection: the
  fixture's 2.12 MB/member retained -> ~3.2 GB, above the box regardless of
  offload until the mirror goes).
- The byte budget did not change the measured peak on this fixture: with 16
  permits, actual concurrency is already capped by the 4 SQLite readers of the
  one big tenant and the single pool thread (in-flight estimates reached
  2.6 GB unbudgeted vs 134 MB budgeted, actual heap 196 MB both). Those runs
  used a 10x estimate factor and an older admission order (estimate taken
  before the count wait, so a burst queued at startup used the 8 MiB default);
  both were fixed afterwards (5x, estimate after the count wait) and were not
  re-measured. The budget matters with many tenant files, where the reader cap
  no longer binds; the unit tests prove its bound (`burst_never_exceeds_the_byte_budget`,
  `oversized_build_runs_alone_instead_of_never`).
- After offload the "dropped" variant retains ~27 MB more at the end: freeing
  is now asynchronous on the pool and had not finished when the burst ended.

## Other findings

- Closing a subscription freed up to ~100 ms of rows on the network thread
  (the last holder drops the interned rows); found by the CPU-time probe, now
  `RetiringMap`/`retire`.
- Per-connection feed cost is heavy-tailed: the same task update costs
  ~0.1 ms for most members and 20-45 ms for a few (rebuilds). An average-based
  inline threshold let those run inline (18-45 ms network polls); the decision
  now uses a slowly decaying maximum per table set.
- `collect_snapshot_rows` and the snapshot interning loop still `yield_now`
  every 32 rows; harmless on the pool, left as is (replica-state workstream).
- The row interner's global `std::sync::Mutex` is held for one hash lookup per
  row, plus an O(n) sweep when the map size reaches a power of two; only pool
  threads take it now. The parent-ACL index `std::sync::RwLock` is held per
  lookup. Neither was measured as a stall.
- Not offloaded: per-message JSON / patch / compact encoding of feed deltas
  (microseconds per message; delivery workstream), WebSocket per-message
  deflate inside the frame encoder (a 256 KB page compresses in ~2-3 ms on the
  network thread; delivery workstream owns the encoder), live-query feed
  application.

## Tests

- `cargo test -p gonvex-runtime` (lib: offload strategy round trips,
  cancellation, panics; budget bound; oversized-alone; estimate fallback).
- `cargo test -p gonvex-runtime --features perf-fixture --test offload_equivalence`:
  every member's 10 collections opened through the pooled socket queue match a
  direct open (hashes, cursor, rows); pool-encoded pages equal inline pages;
  6 task/presence events applied pooled vs inline to every member give
  identical messages, hashes and cursors.
- `native_snapshot_socket_backpressures_until_durable_ack` waited 100 ms for
  the first frame; frames are now encoded on the pool, so it waits up to 10 s
  for the first one (the credit-window assertion is unchanged).
