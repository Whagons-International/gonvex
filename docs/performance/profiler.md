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
  `tokio`, `watchdog`, `longPolls` (total plus the newest 64 of a 256-entry ring) and
  `slowTraces` (`fields` plus the newest of a 128-entry ring of slow reducer calls,
  each with its own per-stage values).
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
  scheduler`. Each row gives the rate per second, then mean, p50, p90, p99, p999 and
  max. `max` is cumulative since start. `invoke.host_call_count` and
  `delivery.frames` are counts, not times.
- **"where did a heartbeat reducer spend its time"** is shown for each of `heartbeat`,
  `presence` and `reducer` (other reducers, i.e. saves). Rows follow the reducer path in
  order, and indented rows are contained in the row above them. `share` is the stage
  mean divided by the `fn.total` mean. The unindented rows add up to the total; the
  remainder is printed as "unaccounted". p99 values do not add up. The last column
  (stage p99 ÷ total p99) shows which single stage reaches the tail on its own: a stage
  near 100% *is* the tail.
- **"slowest calls"** answers the tail question exactly instead of from aggregates.
  Every reducer slower than `GONVEX_PROFILER_SLOW_MS` keeps its own breakdown:
  prepare, connect, first_frame, js, host_calls, writer_wait, sql, finish and the
  host-call count. The table gives, per label, the mean share of each stage in those
  calls' own totals, followed by the newest few calls. In live/delta mode, only calls
  from the current window are shown.
- `recent long polls / stalls` shows `poll` entries (a poll that finished and took this
  long) and `watchdog` entries (`probe_stalled`: the scheduler-lag probe did not wake;
  or a task family seen still inside one poll after the stall threshold, which is
  reported while the worker is still blocked).

## Stages

Labels: reducer-path stages carry a function class: `heartbeat` (`presence.beat`,
`*heartbeat`), `presence` (other `presence.*`), `reducer` (everything else), `query`,
`action`. It is assigned by `Label::classify` with a few byte comparisons on the path.
The other stages are unlabelled (`-`).

### Reducer path (labelled)

| Stage | Boundary (code) |
| --- | --- |
| `fn.total` | `ScopeTimer` covering `execute_tenant_reducer_with_access` (runtime/src/execution.rs); queries and actions record their totals here too (`execute_tenant_query_with_access`, `execute_tenant_action_with_access`) |
| `fn.prepare` | From the start of `execute_tenant_reducer_with_access` to just before `module_host.invoke` (module lookup, control-plane read, idempotency check, validation, handler setup) |
| `invoke.connect` | `ModuleHost::request` (runtime/src/module_host.rs): `connect()` (status + socket connect) and request frame write |
| `invoke.first_frame` | `dispatch_frames_profiled` / `InvokeProfile::frame_arrived`: request written → first frame read. This is the module host's isolate wait + JS up to the first host call + IPC. With zero host calls, it is the whole JS run |
| `invoke.js` | Sum over one invocation of the gaps between a host-call reply being written and the next frame arriving |
| `invoke.host_calls` | Sum over one invocation of frame parse → host-call reply written (`InvokeProfile::host_call_answered`) |
| `invoke.host_call_count` | Host calls per invocation (count) |
| `invoke.total` | `ScopeTimer` covering `ModuleHost::request` |
| `db.writer_wait` | `DatabaseHostCalls::ensure_transaction` (runtime/src/host_calls.rs): `begin_tenant_transaction` + provenance + receipt claim, on the first database call |
| `db.sql` | `DatabaseHostCalls::handle` for `DbQuery/Insert/Update/Delete/DeleteMany`, minus `db.writer_wait` spent inside the same call |
| `host.other` | `DatabaseHostCalls::handle` for every other host call |
| `fn.finish` | `handler.finish_with_revision(true)` in execution.rs: the whole commit |
| `commit.finalize` | `TenantTransaction::commit_with_revision` (store/src/lib.rs): reference-integrity flush + `feed::finalize` |
| `commit.commit` | `self.transaction.commit()` (SQLite COMMIT) |
| `commit.fsync` | `pools.durable()`: the WAL sync barrier (`GONVEX_SQLITE_SYNCHRONOUS=full`) |
| `commit.publish` | `pools.notify()` of the change-feed notice |

Isolate wait in the module host is not separated from JS in `invoke.first_frame`. The
module host is a separate process, and splitting the two would need a timing field in
its protocol. `module_host.inflight` (gauge) is the number of invocations queued or
running there.

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

### Slow traces

`execute_tenant_reducer_with_access` assembles them on the success path, after the
commit. `fn.prepare` and `fn.finish` come from its own timer. `invoke.*` comes from
`InvokeProfile` in module_host.rs, handed to the handler through
`HostCallHandler::invoke_profiled` (a default no-op method). `db.writer_wait` and
`db.sql` are accumulated on `DatabaseHostCalls`. When the call is fast, assembling the
trace costs one comparison.

## Gauges

| Gauge | Meaning |
| --- | --- |
| `module_host.inflight` | Module-host requests in flight (queued for an isolate + executing) |
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
2026-11-03, while other builds were running):

| Operation | ns/op |
| --- | --- |
| clock read (`now_ns`) | ~25 |
| `record` with profiler disabled / `Timer::lap` disabled | 0.2 |
| `record` (value given) | 3.5 |
| `Timer::lap` (clock read + record) | 29 |
| instrumented poll (2 clock reads + slot swap + record) | 55 |
| `Timer::lap`, 2 / 4 / 8 threads on the same stage | 35 / 35 / 36 per thread |
| `Timer::lap`, 16 threads (two threads per shard) | 79 per thread |

At 50,000 stage boundaries per second this costs 0.14% of one core, and 50,000
instrumented polls per second cost 0.27%. A heartbeat reducer crosses about 12
boundaries (fn ×3, invoke ×4, host-call stages ×2 per call, commit ×4), plus about 2
polls per message. At the 5k-user target (~1,000 reducer calls/s plus ~45k delivered
rows/s, which are batched into far fewer per-connection feed events), the estimated
cost is well under 1% of one vCPU. Fixed costs:

- the probe wakes every 10 ms;
- the watchdog thread wakes every 10 ms;
- static tables (45 stages × 6 labels × 8 shards × 896 B ≈ 1.9 MB of zero-initialised
  `.bss`). Only (stage, label, shard) combinations that are used get touched, so the
  resident set is about 7 KB per active stage/label.

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
