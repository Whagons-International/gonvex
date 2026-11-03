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

See the tables below (filled from `bridge_bench.rs`).
