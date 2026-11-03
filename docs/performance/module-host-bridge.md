# In-process module isolates (workstream 10e)

Branch `perf/native-reducers` (the 10b inventory is in `native-reducers.md`; the scope moved to
the JS path). Decision (Gabriel, 2026-11-03): run V8 inside the runtime process instead of only
improving the socket protocol.

## What changed

- `GONVEX_MODULE_HOST_IN_PROCESS` (default `true`) makes `runtime::module_host::ModuleHost` serve
  every request (`load`, `activate`, `invoke`, `ping`, `shutdown`) in this process through
  `gonvex_module_host::session::handle_local`. That function runs the **same request handlers**
  as the separate host: artifact decoding and hash check, `V8ModuleEngine` creation, **prewarm
  before staging**, atomic generation activation with draining and reaping of the retired
  generation, root admission (`GONVEX_MODULE_HOST_MAX_CONCURRENT_CALLS`), invocation deadlines,
  generation checks and the wire error codes. Callers (`execution.rs`, `modules.rs`,
  `control.rs`) did not change, and error classification sees the same `ModuleHostError::Remote`
  codes and messages.
- The only new piece is the transport (`runtime/src/in_process.rs`): the engine's
  `ModuleHost::call` goes straight to the invocation's `HostCallHandler`
  (`DatabaseHostCalls` for Reducers and Queries, `ActionHostCalls` for Actions). There is no
  socket, no connect per invoke, no frame encode/decode and no second process. Failed host calls
  keep the socket transport's text (`db_read host call failed [host_call_failed]: ...`).
- `GONVEX_MODULE_HOST_IN_PROCESS=false` restores the separate `gonvex-module-host` process with
  no other change. `GONVEX_MODULE_HOST_MAX_HEAP_MB` (default 64, the module host's own default)
  sets the per-isolate heap ceiling in process.
- `runtime/src/main.rs` initializes the V8 platform before Tokio starts (the module host does the
  same; V8 requires the platform's parent thread to be the same for every isolate thread).
- Transaction semantics are unchanged: `execution.rs` and the store were not touched, so the
  lazy writer, receipts, change feed and commit path are the same as before (10d can merge on top).

## Threads on 1 vCPU

JavaScript never runs on a Tokio worker. Each isolate owns a dedicated thread from the engine pool
(`GONVEX_MODULE_HOST_ISOLATE_POOL_SIZE`, default `min(CPUs, 4)`, so 1 on a 1-vCPU container) and a
watchdog thread. A database host call goes:

isolate thread → (unbounded channel) → the invoking Tokio task → `DatabaseHostCalls` →
sqlx's SQLite connection thread → back the same way.

No step blocks the network worker: the Tokio task only awaits. Running SQLite directly on the
isolate thread would remove two more thread hops per call, but it needs a synchronous connection
(rusqlite) on that thread. That is the `perf/sqlite-writer` question, so the database access stays
behind `TenantTransaction`.

## Sandbox properties kept

- The isolate gets only the capability-checked host functions of `gonvex_bootstrap.js`. It never
  sees a pool, transaction or file handle. Capabilities are checked by the isolate op against the
  invocation's capabilities, and again by the handler (a Query cannot write).
- Deadlines: the engine watchdog calls `TerminateExecution` at the call deadline. The isolate is
  retired and replaced, and the runtime keeps serving (test
  `runaway_and_heap_hungry_scripts_fail_without_taking_the_runtime_down`).
- Heap: `heap_limits(0, max_heap_bytes)` plus a near-heap-limit callback that terminates the call,
  gives V8 50% headroom to unwind, and retires the isolate (same test, `budget_exceeded` /
  "heap limit").
- Bounded output (`max_result_bytes`), result schema validation, recycle after 10,000 calls,
  per-generation isolate pools and prewarm: unchanged, same engine code.
- Arguments are validated in Rust (`validate_portable_schema` in `execution.rs`) for Queries,
  Reducers and Actions before V8 is entered. That was already the case on the base. The engine
  validates again inside the host.

## Isolation lost compared with the separate process, and mitigations

| Lost | Effect | Mitigation |
|---|---|---|
| Crash containment | A V8 fatal error (a CHECK failure, a V8 bug, or an allocation that exceeds even the raised limit, which ends in `FatalProcessOutOfMemory`) aborts the runtime process and drops every WebSocket. Before, only the module host died and the runtime reported not-ready. | Per-isolate heap limit with the near-heap-limit callback (terminate and retire) covers ordinary runaway allocation. Keep `GONVEX_MODULE_HOST_IN_PROCESS=false` as an operational switch. Clients already reconnect and resume. |
| Memory accounting | Isolate heaps count in the runtime's RSS. A container OOM kill now takes down the runtime, not only the host. The total is the same in the 2 GB cgroup (both processes were in it). | One isolate per generation on 1 vCPU. The heap ceiling per isolate is enforced. The reducer-only bundle (`perf/v8-memory`) shrinks the resident heap. |
| Separate address space | A memory-safety bug in V8 or in a native op could corrupt runtime memory. | Same V8 build and ops as the host; no new native ops. |
| Independent restart | A module host restart used to reset V8 state without dropping sockets. | Isolates are recycled after `recycle_after_calls` and retired on any budget failure; generation activation swaps engines in place. |

## Measurements

Harness: `rust/crates/runtime/src/bridge_bench.rs` (ignored tests, release build). It runs the
runtime's own `ModuleHost` client with one Tokio worker, pinned to one CPU (`taskset -c 5`, so the
runtime, the isolate thread and, in socket mode, the module-host process share one core, as in a
1-vCPU container). Socket mode is the unchanged base transport (one Unix socket connection per
invoke, JSON frames, base `gonvex-module-host` binary). Host calls are answered by the real
`DatabaseHostCalls` on a copy of the lab tenant SQLite file (`v8-lab/tenant`, 9k-task tenant) and
the real Whagons bundle (`v8-lab/manifest.json`, the complete bundle). Each invoke **rolls back**,
so every sample sees the same rows. Commit/fsync time is therefore not in the hold time (it is the
same in both modes). The writer hold runs from the first database host call (when the lazy
writer opens) to the end of the transaction. "handler" is time inside host-call handlers (SQLite
through sqlx). "bridge+JS" is invoke time minus handler time. CPU counts the runtime process plus
its module-host child. 300 samples after 20 warm-up calls per function. The workstation was
shared with other agents' builds (load average 11-14 on 14 cores), so p95 values are noisy.
The `perf/fixture` fake module host was not used; the synthetic bundle below plays that role.

### Per host call (synthetic Reducer: `n` awaited `ctx.db.query`, 1 CPU)

| | socket (base) | in-process | speed-up |
|---|---:|---:|---:|
| Marginal cost of one host call, null handler (no SQLite) | 82 µs | 14 µs | 5.9x |
| 30 queries, null handler, invoke mean | 2.74 ms | 0.52 ms | 5.3x |
| 30 real SQLite point reads, invoke mean | 4.84 ms | 1.51 ms | 3.2x |
| 30 real SQLite point reads, writer hold p50 | 4.28 ms | 1.41 ms | 3.0x |

### Whagons Reducers (complete bundle, 1 CPU)

Host calls per invoke are the same in both modes: tasks.update 27 (24 dbQuery, 1 dbUpdate,
1 dbInsert, 1 actionEnqueue), tasks.updateDescription 27, taskResources.createNote 27
(24 dbQuery, 2 dbInsert, 1 actionEnqueue), tasks.create 31 (28 dbQuery, 2 dbInsert, 1 schedule).

| Reducer | invoke p50 socket → in-process | writer hold p50 | writer hold p95 | bridge+JS per invoke | CPU per invoke (runtime + host) |
|---|---|---|---|---|---|
| tasks.update | 8.35 → 4.92 ms (-41%) | 7.93 → 4.84 ms (-39%) | 11.59 → 6.96 ms | 5.23 → 1.90 ms | 9.22 → 5.72 ms (-38%) |
| tasks.updateDescription | 10.36 → 5.14 ms (-50%) | 9.81 → 5.08 ms (-48%) | 13.46 → 7.63 ms | 6.04 → 1.91 ms | 10.72 → 5.72 ms (-47%) |
| taskResources.createNote | 8.46 → 3.95 ms (-53%) | 7.86 → 3.87 ms (-51%) | 11.13 → 5.84 ms | 5.29 → 1.62 ms | 8.75 → 4.47 ms (-49%) |
| tasks.create | 10.65 → 5.81 ms (-45%) | 9.83 → 5.56 ms (-43%) | 11.88 → 7.82 ms | 7.22 → 2.62 ms | 10.97 → 6.50 ms (-41%) |

An earlier run with the same binaries at lower machine load gave the same picture: tasks.update
p50 7.87 → 4.56 ms, hold 7.40 → 4.49 ms, CPU 8.78 → 5.19 ms.

With the reducer-only bundle from `perf/v8-memory` (`v8-lab/split/whagons-split.json`) in process:
tasks.update p50 4.69 ms, hold 4.59 ms; createNote 3.62 / 3.53 ms. That is within noise of the
complete bundle; the split matters for memory, not latency.

### Memory (1 isolate, after load and 1,280 calls)

| | runtime RSS | module-host RSS | total |
|---|---:|---:|---:|
| socket (base) | 173 MB | 181 MB | 354 MB |
| in-process, complete bundle | 268 MB | 0 | 268 MB (-86 MB) |

The runtime RSS includes the SQLite pools and caches opened by the calls (+58 MB in socket mode
between load and the end of the calls). In process, the isolate and its bundle add about 95 MB to
the runtime (268 vs 173 MB). The `perf/v8-memory` merge adds `malloc_trim` after isolate start/stop
and after Load, and the in-process Load path calls it too. The split-bundle run is not comparable on
RSS: the harness holds the 22 MB manifest and the split artifact as parsed JSON (+90 MB before
load). The V8-memory agent measured the V8 heap per isolate at 38 MB (complete) and 25 MB (reducer
bundle).

### What is left in the writer hold

For tasks.update in process, about 64% of the hold is inside the host-call handlers (24 SQLite
reads through sqlx, about 110 µs each including the sqlx thread hand-off). About 36% is JavaScript
plus the remaining bridge: isolate thread → Tokio task → handler and back, two cross-thread wake-ups
per call, plus JSON encoding in the op. Next steps, in order of size:

1. SQLite on a dedicated connection thread without sqlx's per-query hand-off (`perf/sqlite-writer`),
   or run the reads on the isolate thread itself. That removes the remaining two hops per call.
2. The double JSON encoding of host-call results in `op_gonvex_host_call` (the value travels as a
   JSON string inside a JSON envelope).

**OCC (10d).** After in-process execution, a task edit holds the writer for about 4.5-5 ms
(rollback measured, plus the commit). At the 5k budget's ≈23 task writes/s in the largest tenant,
that is ≈11% writer utilization before commit, with presence moved out of V8. Queueing on the
writer is small at that utilization, so OCC is not needed for that rate. Recommend it only if
commit time (`synchronous=full` fsync, not measured here) or bursty writes push utilization well
above that, and do (1) first, because it cuts the same hold time with no semantic change.
