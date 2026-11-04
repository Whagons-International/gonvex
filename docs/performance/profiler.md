# Runtime profiler

The SQLite runtime has an always-on, low-overhead profiler (`rust/crates/profiler`,
crate `gonvex-profiler`). It answers "where does a p99 heartbeat or save spend its
time", "is the tokio worker stalled, and by what", and "which queue is backing up",
live, on a loaded server.

It is built for the 5,000-user / 1 vCPU target: recording never allocates, never takes
a lock, never hashes a string or touches a map. Each stage boundary reads the
monotonic clock once and does two or three relaxed atomic adds on a per-thread shard
of a static histogram.

## Switches

| Env | Default | Meaning |
| --- | --- | --- |
| `GONVEX_PROFILER` | on | `0`/`false`/`off` turns every recording site into one relaxed load, and the watchdog thread and probe task are not started |
| `GONVEX_PROFILER_LONG_POLL_MS` | 5 | Instrumented task polls at least this long go to the long-poll ring buffer |
| `GONVEX_PROFILER_STALL_MS` | 25 | Watchdog threshold: probe silent this long, or an instrumented poll still running this long, is reported |
| `GONVEX_PROFILER_PROBE_MS` | 10 | Scheduler-lag probe sleep interval |
| `GONVEX_PROFILER_SLOW_MS` | 100 | Reducers at least this slow keep a per-call stage breakdown (slow traces) |

## Endpoints

Both need an operator token with `projects:read` (the Whagons bench `admin` bearer works).

- `GET /dev/profile`, or `GET /dev/metrics?format=profile`: the full dump
  (`"format": "gonvex-profile/1"`). It contains per-stage `count`, `sumNs`, `maxNs`,
  `p50Ns`/`p90Ns`/`p99Ns`/`p999Ns`, sparse `buckets` (`[index, count]` pairs, so that
  clients can compute percentiles over any interval from two dumps),
  `bucketLowerBoundsNs`, `stageInfo` (descriptions), `gauges`, `writerQueues` (top 10),
  `tokio`, `watchdog`, `longPolls` (total plus the newest 64 of a 256-entry ring),
  `slowTraces` (`fields` plus the newest of a 128-entry ring of slow reducer calls,
  each with its function `project/path` and its own per-stage values), `crons`,
  `functions` (per-function counters, see below), `v8Isolates` (per live isolate:
  heap, calls served, GC pause counts and time) and `actions`.
- `GET /dev/metrics?project=…`: `functions.<path>` now carries server-side counters
  from the per-function table (`calls`, `errors`, `cancelled`, `averageDurationMs`,
  `maxDurationMs`, `lastDurationMs`, `totalDurationMs`, `lastCalledAt`,
  `source: "server"`, since process start). Before, these fields came only from
  browser telemetry (`gonvex_performance_events`), which the load bench never
  sends, so they stayed 0. Browser telemetry, when present, is under `client`.
- `GET /dev/metrics?project=…`: the existing payload, plus a compact `profiler` key
  (per-stage p50/p99/max, gauges, tokio metrics, watchdog). The bench harness already
  scrapes this endpoint every 10 s.

## CLI

`bench/profiler/top.py` uses only the Python standard library.

```sh
# live: every 5 s, rates and percentiles over the last interval (bucket deltas)
bench/profiler/top.py --url http://127.0.0.1:3210 --token admin
# cumulative since start
bench/profiler/top.py --url … --once
# record a window: dump, run load, dump, diff
bench/profiler/top.py --url … --save before.json
bench/profiler/top.py --url … --save after.json
bench/profiler/top.py --delta before.json after.json
```

### Reading the output

- The header shows tokio workers, the worker busy ratio over the last second, the
  global (injection) queue depth and its peak, the number of alive tasks, the
  watchdog's view (how long the probe has been silent, and how many stalls it has
  counted) and the number of long polls.
- `gauges` shows `name=current/peak`. The peak is the highest value in the current
  10-second window (or the previous window, early in a window).
- `writer queues` shows, per tenant database: transactions waiting for the single
  writer now, total acquisitions, mean wait and maximum wait.
- The stage table is grouped `reducer | store | delivery | transport | snapshot |
  scheduler | background`. Each row gives the rate per second, then mean, p50, p90,
  p99, p999 and max. `max` is cumulative since start. `invoke.host_call_count`,
  `delivery.frames` and `sqlite.checkpoint_pages` are counts and
  `snapshot_cache.evict` is bytes, not times.
- **"where did a heartbeat reducer spend its time"** is shown for each of `heartbeat`,
  `presence` and `reducer` (other reducers, i.e. saves). Rows follow the reducer path in
  order, and indented rows are contained in the row above them. `share` is the stage
  mean divided by the `fn.total` mean. The unindented rows add up to the total; the
  remainder is printed as "unaccounted". p99 values do not add up. The last column
  (stage p99 ÷ total p99) shows which single stage reaches the tail on its own: a stage
  near 100% *is* the tail.
- **"slowest calls"** answers the tail question exactly instead of from aggregates.
  Every reducer slower than `GONVEX_PROFILER_SLOW_MS` keeps its own breakdown:
  prepare, connect, first_frame (with isolate_wait inside it), js, host_calls,
  writer_wait, sql, finish and the host-call count, plus its function name. The table
  gives, per label and per function, the mean share of each stage in those calls' own
  totals, followed by the newest few calls. In live/delta mode, only calls from the
  current window are shown.
- **"isolate queue"** (in-process V8): requests waiting for an isolate now (and the
  10 s peak), and per label the `invoke.isolate_wait` mean/p99/max and its share of
  `invoke.total`. A request whose deadline passed while it waited is included with its
  whole wait, so the "reducers waiting 3-9 s for an isolate" failure shows here
  instead of only as a `budget_exceeded` error.
- **"background work"**: time per second of wall clock (and events per second) of
  work no request is attached to: WAL checkpoints, V8 GC pauses, isolate starts and
  stops, `malloc_trim`, snapshot-cache builds and offload-pool busy time; then one row
  per live V8 isolate (heap, calls, GC count and ms). Use it to explain CPU spikes
  with no instrumented request behind them.
- **"functions by total time"**: the per-function table (rate, calls, errors,
  cancelled, mean, max, share of all function time); deltas in live/delta mode.
- `recent long polls / stalls` shows `poll` entries (a poll that finished and took this
  long) and `watchdog` entries (`probe_stalled`: the scheduler-lag probe did not wake;
  or a task family seen still inside one poll after the stall threshold, which is
  reported while the worker is still blocked).

## Stages

Labels: reducer-path stages carry a function class: `heartbeat` (`presence.beat`,
`*heartbeat`), `presence` (other `presence.*`), `reducer` (everything else), `query`,
`action`, and `cron` for everything the scheduler runs (crons and `runAt`/`wakeAt`
jobs, including Reducers an Action awaits inline; set from a task-local in
runtime/src/cron_profile.rs, not by `classify`). It is assigned by `Label::classify` with a few byte comparisons on the path.
The other stages are unlabelled (`-`).

### Reducer path (labelled)

| Stage | Boundary (code) |
| --- | --- |
| `fn.total` | `ScopeTimer` covering `execute_tenant_reducer_with_access` (runtime/src/execution.rs); queries and actions record their totals here too (`execute_tenant_query_with_access`, `execute_tenant_action_with_access`) |
| `fn.prepare` | From the start of `execute_tenant_reducer_with_access` to just before `module_host.invoke` (module lookup, control-plane read, idempotency check, validation, handler setup) |
| `invoke.connect` | `ModuleHost::request` (runtime/src/module_host.rs): `connect()` (status + socket connect) and request frame write. In-process: the status check only |
| `invoke.first_frame` | Socket: `dispatch_frames_profiled` / `InvokeProfile::frame_arrived`: request written → first frame read (the module host's isolate wait + JS up to the first host call + IPC). In-process: `DirectProfile` (runtime/src/in_process.rs): request → the invoking task picks up the first host call (isolate wait + JS up to it + the channel hop). With zero host calls, it is the whole run |
| `invoke.isolate_wait` | In-process only, inside `invoke.first_frame`: request → the engine leased an isolate. Root admission (`max_concurrent_calls`), the isolate pool's permit queue (`isolate_pool_size`) and a cold isolate start. Hook: `ModuleHost::isolate_acquired` (module-runtime), called by the V8 engine right after `pool.acquire()` (module-runtime-v8 lib.rs). A request that hit its deadline before getting an isolate records its whole duration here. The socket bridge cannot see this (it would need a protocol field) |
| `invoke.js` | Socket: sum over one invocation of the gaps between a host-call reply being written and the next frame arriving. In-process: (end − start) − first_frame − host_calls, i.e. JS after the first host call plus the isolate↔task channel hops |
| `invoke.host_calls` | Socket: sum over one invocation of frame parse → host-call reply written (`InvokeProfile::host_call_answered`). In-process: serial calls answered by the invoking task (`DirectHost::call`, lock + `handle_bytes`) plus SQL answered on the isolate thread (`SqlInline`, via `HostCallHandler::inline_profile`). Overlapping network calls (Action `fetch`) are not counted on either transport |
| `invoke.host_call_count` | Host calls per invocation (count), both paths above |
| `invoke.total` | `ScopeTimer` covering `ModuleHost::request` (both transports) |
| `db.writer_wait` | `DatabaseHostCalls::ensure_transaction` (runtime/src/host_calls.rs): `begin_tenant_transaction` + provenance + receipt claim, on the first database call |
| `db.sql` | `DatabaseHostCalls::handle` for `DbQuery/Insert/Update/Delete/DeleteMany`, minus `db.writer_wait` spent inside the same call |
| `host.other` | `DatabaseHostCalls::handle` for every other host call |
| `fn.finish` | `handler.finish_with_revision(true)` in execution.rs: the whole commit |
| `commit.finalize` | `TenantTransaction::commit_with_revision` (store/src/lib.rs): reference-integrity flush + `feed::finalize` |
| `commit.commit` | `self.transaction.commit()` (SQLite COMMIT) |
| `commit.fsync` | `pools.durable()`: the WAL sync barrier (`GONVEX_SQLITE_SYNCHRONOUS=full`) |
| `commit.publish` | `pools.notify()` of the change-feed notice |

With the in-process V8 bridge (the default on this line), `invoke.isolate_wait`
separates the isolate queue from JS. With the separate module-host process it is not
separated from JS in `invoke.first_frame` (that would need a timing field in its
protocol). `module_host.inflight` (gauge) is the number of invocations queued or
running; `module_host.isolate_waiting` the in-process ones still waiting for an isolate.

### Store (unlabelled, every caller)

| Stage | Boundary |
| --- | --- |
| `db.admission` | `PoolRegistry::admit` (store/src/registry.rs): process-wide admission semaphore |
| `db.writer_acquire` | `begin_transaction` (store/src/lib.rs): `writer.begin()`, i.e. the single-writer acquire + BEGIN |
| `db.reader_begin` | `begin_transaction`, read-only: `reader.begin()` |

### Delivery path (unlabelled)

| Stage | Boundary |
| --- | --- |
| `feed.notify_lag` | Stamp in `commit_with_revision` after notify → `run_feed` (runtime/src/change_feed.rs) woke for that revision |
| `feed.read` | `deliver_committed`: read the clock + changes and broadcast |
| `delivery.queue_wait` | Stamp in `deliver_committed` just before `sender.send` → this connection starts applying the event (runtime/src/lib.rs, `ScheduledTransport::Feed`). This is the per-connection send-queue / scheduling wait |
| `delivery.visibility` | `runtime.apply_feed_event` (live queries are applied before it and count under `delivery.send`) |
| `delivery.send` | From after visibility to the end of the feed-event arm: encode + write every frame and the watermark |
| `delivery.total` | Broadcast stamp → the connection finished the event |
| `delivery.frames` | Frames per connection per transaction (count) |

### Transport (unlabelled)

| Stage | Boundary |
| --- | --- |
| `ws.inbound_wait` | Compressed transport only: the reader task read the frame → the connection task took it (rust/crates/websocket/src/lib.rs). Inbound requests wait here while the connection task is busy |
| `ws.encode` | `send_json` (`serde_json::to_string`), `row_patches.prepare`, `encode_compact_replica_transaction` (runtime/src/lib.rs) |
| `ws.compress` | `compress_text` (websocket crate): one permessage-deflate compression |
| `ws.write` | `WebSocket::send` / `send_best_text`: compression + frame write + flush, including backpressure |

### Snapshot path (unlabelled)

All of these are in `replica_snapshot_for_resume` (runtime/src/replica.rs), except `snapshot.send`.

| Stage | Boundary |
| --- | --- |
| `snapshot.permit_wait` | `snapshot_permits().acquire()` |
| `snapshot.acl` | Control-plane read + parent ACL preparation |
| `snapshot.begin` | Read transaction + clock |
| `snapshot.visibility` | `visibility::resolve*` |
| `snapshot.query` | `query_rows(...)`: SQL + per-row JSON decode + row admission (decode is not split out, to keep replica.rs hooks to one-line timers while the shared-replica work lands) |
| `snapshot.intern` | Row key/hash + `intern_row` loop |
| `snapshot.build` | Whole build, including the permit wait |
| `snapshot.send` | `send_replica_open` in runtime/src/lib.rs (encode + write of frames/pages) |

### Scheduler

| Stage | Boundary |
| --- | --- |
| `tokio.sched_lag` | Probe task (runtime/src/profiling.rs): how late `sleep(GONVEX_PROFILER_PROBE_MS)` woke |
| `poll.ws_connection` | Every poll of the WebSocket connection task (the callback in `websocket_upgrade`). Reducers, feed apply and snapshot send all run inside these polls |
| `poll.ws_reader` | Every poll of the compressed transport's frame reader task |
| `poll.snapshot` | Every poll of a replica-open preparation task (`replica_open_queue.rs`) |
| `poll.feed` | Every poll of a change-feed reader task (`change_feed.rs`) |
| `poll.outbox` | Every poll of the post-commit action-outbox drain task (execution.rs) |

### Background (unlabelled)

Work with no request attached. These explain CPU spikes the request stages do not.

| Stage | Boundary |
| --- | --- |
| `sqlite.checkpoint` | Checkpoint thread (store/src/checkpoint.rs `run`): one `sqlite3_wal_checkpoint_v2(PASSIVE)` |
| `sqlite.checkpoint_pages` | Same checkpoint: WAL frames copied back (count, not ns) |
| `sqlite.checkpoint_inline` | `wal_hook`: the writer's own checkpoint inside COMMIT once the WAL reached its cap |
| `v8.gc_minor`, `v8.gc_major` | One V8 GC pause (scavenge / minor mark-compact; mark-sweep-compact), any isolate: GC prologue → epilogue callbacks installed in `ModuleIsolate::create` (module-runtime-v8 isolate.rs). Incremental-marking steps are not pauses and are not counted. rusty_v8 130 binds only the prologue; the epilogue is registered through V8's own `Isolate::AddGCEpilogueCallback` symbol (Itanium ABI, Linux/macOS; on other platforms only the prologue is installed and nothing is recorded) |
| `v8.isolate_start` | `ModuleIsolate::create`: new isolate + bundle parse + top-level evaluation (prewarm, on-demand pools, replacements of retired isolates) |
| `v8.isolate_stop` | Isolate teardown (`JsRuntime` dispose) when its worker loop ends (retired, recycled, evicted, shutdown) |
| `mem.malloc_trim` | `trim_native_heap` (`malloc_trim(0)`): after isolate start/stop and module load |
| `snapshot_cache.build` | `SnapshotCache::entry` (runtime/src/snapshot_cache.rs): a miss or an expired entry rebuilt from SQL |
| `snapshot_cache.evict` | One entry evicted over the cache budget (value is the entry's bytes, not ns) |
| `offload.queue_wait` | Offload pool (runtime/src/offload.rs `JobTrack`): job submitted → first run on a pool thread |
| `offload.busy` | Offload pool: time a job spent running (sum of its polls; for a closure, its run) |
| `offload.run` | Offload pool: first run → finished (wall, including awaits) |

`v8Isolates` in `/dev/profile` has one row per live isolate (`fields`:
`totalHeapBytes`, `usedHeapBytes`, `physicalBytes`, `externalBytes`, `mallocedBytes`,
`calls`, `gcMinor`, `gcMinorNs`, `gcMajor`, `gcMajorNs`), refreshed after each call
(the existing heap tally). Gauges `offload.queued` / `offload.running` give the pool's
queue depth and busy jobs.

### Slow traces

`execute_tenant_reducer_with_access` assembles them on the success path, after the
commit. `fn.prepare` and `fn.finish` come from its own timer. `invoke.*` comes from
`InvokeProfile` in module_host.rs (socket) or `DirectProfile` in in_process.rs, handed
to the handler through `HostCallHandler::invoke_profiled` (a default no-op method).
`db.writer_wait` and `db.sql` are accumulated on `DatabaseHostCalls`. Each trace also
stores the function's slot (below); the dump looks its `project/path` up at dump time,
never on the call path. `invoke.isolate_wait` is the last field (older dumps lack it).
When the call is fast, assembling the trace costs one comparison.

## Per-function table

`/dev/profile` `functions` and `/dev/metrics` `functions`: one row per function that
ran, totals since start. Slots are assigned once per `(project, path)` at module install
(`gonvex_profiler::function::register` from `artifact_from_manifest`, stored as
`FunctionDefinition::profile_slot`); a fixed 2,048-slot table of relaxed atomics
(128 KiB `.bss`, the last slot collects overflow). Every Query, Reducer and Action
execution (`execute_tenant_{query,reducer,action}_with_access` in execution.rs) holds a
`function::Call` guard: two clock reads and four or five relaxed adds, no allocation,
lock or map. Fields: `invocations`, `errors` (the call returned an error), `cancelled`
(the caller dropped it before it finished), `totalNs`, `maxNs`, `lastNs`, `lastAtNs`
(profiler clock; `/dev/metrics` converts it to `lastCalledAt`). A call that fails
before its function is found is not counted.

## Per-cron table

`/dev/profile` has a `crons` section: `fields` plus one row per cron that ran, with
totals since start. Slot numbers are assigned once per cron name at module install
(`gonvex_profiler::cron::register`); a finished run adds its totals to a fixed
64-slot table of relaxed atomics (no allocation, lock or map on the run path).
Slot 0 is `(one-shot)` (runAt/runAfter/wakeAt work).

| Field | Meaning |
| --- | --- |
| `invocations`, `errors`, `skipped` | Runs, failed runs, runs answered by a `dueWhen` pre-check without V8 |
| `totalNs`, `maxNs` | Run time from start (after the lane admitted it) to finish |
| `invokeNs` | Outermost module-host (V8) request time, including isolate wait |
| `writerHoldNs`, `maxWriterHoldNs` | Tenant writer held: write transaction open → commit/rollback |
| `hostCalls`, `rowsRead` | Host calls; rows returned by `ctx.db` queries |
| `rowsWritten`, `revisions` | Change-feed rows and revisions published |
| `fanout` | Change-feed subscribers (connections) at publish time, summed |
| `startDelayNs`, `maxStartDelayNs` | Due time → start (claim latency, lane queue, user-first yield) |

`bench/profiler/crontable.py PROFILE [EARLIER]` prints the table (or the delta
of two dumps); see docs/performance/scheduled-load.md.

## Gauges

| Gauge | Meaning |
| --- | --- |
| `module_host.inflight` | Module-host requests in flight (queued for an isolate + executing), both transports |
| `module_host.isolate_waiting` | In-process requests waiting for root admission or a V8 isolate |
| `offload.queued`, `offload.running` | Offload pool jobs submitted but not started; running |
| `db.writer_waiting` | Write transactions waiting for a tenant writer, over all tenants. Per tenant: `writerQueues` (atomics on the registry's per-database entry; the registry lock is taken only by the reader) |
| `db.writer_held` | Write transactions open |
| `db.admission_waiting` | Waiters on the process-wide pool admission semaphore |
| `snapshot.permit_waiting`, `snapshot.active`, `snapshot.permits_available` | Snapshot semaphore queue, holders, free permits |
| `ws.sending` | Connections inside a socket write |
| `ws.outbound_frames` | Frames built for feed delivery but not yet started, summed over connections |
| `ws.outbound_batch` | Peak only: the largest per-connection delivery batch in the window |

## Tokio metrics

The probe samples the stable `RuntimeMetrics` APIs once per second: `num_workers`,
`num_alive_tasks`, `global_queue_depth` (sampled every probe tick, with a peak),
`worker_total_busy_duration` (→ busy ratio over the last second) and
`worker_park_count`. Per-worker local queue depth, poll-time histograms, steal and
poll counts, blocking-queue depth and scheduling-latency histograms need
`RUSTFLAGS="--cfg tokio_unstable"`. They are not used, and the dump lists them under
`unavailableWithoutTokioUnstable`.

## Stall detection

- Each instrumented task (`gonvex_profiler::instrument(TaskKind, future)`) times every
  poll. A poll at or above the long-poll threshold is written to a 256-slot ring
  buffer: one sequence-stamped slot per entry, no lock, and readers skip a slot that is
  being rewritten.
- On entry to a poll, the wrapper writes `(task kind, start time)` into a per-thread
  slot. The `gonvex-profiler-watchdog` OS thread wakes every 10 ms. It reports any slot
  older than the stall threshold, so a blocked worker is seen while it is still blocked
  and attributed to a task family. It also reports when the probe task has not ticked
  for the stall threshold (`probe_stalled`), which covers stalls in uninstrumented
  tasks.

## Overhead

`cargo run --release -p gonvex-profiler --example overhead` (arch3 workstation,
2026-11-03, while other builds were running, load average ~5):

| Operation | ns/op |
| --- | --- |
| clock read (`now_ns`) | ~28 |
| `record` with profiler disabled / `Timer::lap` disabled | 0.3 |
| `record` (value given) | 4.1 |
| `Timer::lap` (clock read + record) | 32 |
| `function::Call` start + finish (2 clock reads + slot counters) | 50 |
| instrumented poll (2 clock reads + slot swap + record) | 54 |
| `Timer::lap`, 2 / 4 / 8 threads on the same stage | 33 / 34 / 34 per thread |
| `Timer::lap`, 16 threads (two threads per shard) | 78 per thread |

An in-process invocation adds about 5 clock reads and 7 records (~170 ns) plus 2 clock
reads per host call answered by the invoking task, and the function counter (50 ns):
about 0.02% of a 1.3 ms invoke. GC, checkpoint, isolate and offload hooks run once per
event (a GC pause, a checkpoint, a pool job), not per row.

At 50,000 stage boundaries per second this costs 0.14% of one core, and 50,000
instrumented polls per second cost 0.27%. A heartbeat reducer crosses about 12
boundaries (fn ×3, invoke ×4, host-call stages ×2 per call, commit ×4), plus about 2
polls per message. At the 5k-user target (~1,000 reducer calls/s plus ~45k delivered
rows/s, which are batched into far fewer per-connection feed events), the estimated
cost is well under 1% of one vCPU. Fixed costs:

- the probe wakes every 10 ms;
- the watchdog thread wakes every 10 ms;
- static tables (58 stages × 7 labels × 8 shards × 896 B ≈ 2.9 MB of zero-initialised
  `.bss`, plus the 128 KiB function table). Only (stage, label, shard) combinations
  that are used get touched, so the resident set is about 7 KB per active stage/label.

## Off-box profilers

- `bench/profiler/perf-oncpu.sh <pid> [seconds]` builds an on-CPU flamegraph with
  `perf record -g` → inferno or FlameGraph.
- `bench/profiler/offcpu.sh <pid> [seconds]` builds an off-CPU flamegraph. It uses
  bpftrace (`finish_task_switch` blocked-time per stack, needs root), or falls back to
  `perf record -e sched:sched_switch` (switch counts, not time).

Requirements: `perf`, plus `bpftrace` for off-CPU time, plus `inferno`
(`cargo install inferno`) or FlameGraph's `stackcollapse-perf.pl`/`flamegraph.pl`.
On arch3 (2026-11-03) none of these were installed, so both scripts are syntax-checked
only. Build with `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only` for readable stacks.
