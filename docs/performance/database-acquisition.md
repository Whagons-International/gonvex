# Database acquisition diagnostics

The runtime metrics API and metrics stream expose two acquisition stages under
`database.processAdmission` and `database.sqlxAcquire`. The first measures the
shared `PoolRegistry` admission semaphore. The second measures SQLx connection
acquisition. Registry clones share these counters across projects and tenants.
They reset when the registry is recreated.

These are acquisition suspension measurements. A future starts a wait on its
first pending poll and settles it when acquisition completes or the future is
dropped. Both stages include cooperative executor yields and scheduling delay.
SQLx also includes connection creation, idle-connection validation and hooks.
A nonzero SQLx wait count does not establish database-slot contention. Neither
stage includes query execution, transaction lifetime or `BEGIN` execution.

Each stage reports:

| Field | Meaning |
| --- | --- |
| `waitCount` | Acquisition attempts that suspended, counted once per attempt. |
| `currentWaiters` | Attempts still suspended in this stage. |
| `waitDurationMs` | Total settled duration, including failures and cancellation. |
| `maxWaitDurationMs` | Longest settled duration. |
| `acquiredCount` | All successful acquisitions, including immediate ones. |
| `timeoutCount` | Native acquisition deadlines that expired. |
| `cancelledCount` | Suspended acquisition futures dropped before returning. |
| `errorCount` | Other failures, including closure. |

An outer timeout that drops acquisition counts as cancellation. A never-polled
future contributes nothing. Nanoseconds accumulate before conversion to whole
milliseconds, so short waits contribute to the total. Active elapsed time is
absent until the wait settles. The independently sampled relaxed atomics are
approximate and do not provide cross-field arithmetic identities.

Initial pool connection, listener connection/reconnection and direct raw
`PgPool` callers outside registry helpers remain unmeasured. The API lists these
exclusions under `database.unmeasuredAcquisitions`.

## Legacy compatibility

`database.waitCount` and `database.waitDurationMs` remain zero placeholders.
`database.legacyWaitMetricsSupported` is false. Older dashboards interpret these
fields as proof of waiting for a free database slot. Feeding broader SQLx
suspension into those fields would give a false diagnosis during ordinary
connection validation. Their zero values do not establish absence of waits.
Use `database.sqlxAcquire` for the measured values.

The old foreground, reactive and bootstrap lane counters also remain unsupported
placeholders. `queryAdmission.legacyLaneMetricsSupported` is false.
`queryAdmission.processAdmission` duplicates the measured shared admission stage;
it is not a tenant queue length.

## Investigation

Compare counter deltas over a workload interval alongside current waiters,
connection occupancy, CPU, resident memory, request errors and latency. A rise
in SQLx suspension with spare capacity can be connection I/O. Even occupancy at
the configured limit does not attribute all suspension time to capacity waits.
Use PostgreSQL activity/wait events and profiles to investigate further.

Native tests cover acquisition ownership, cancellation, deadlines, FIFO order,
cooperative scheduling and transaction/query release. Required PostgreSQL CI also
runs the explicitly ignored database cases with an isolated database. These are
runtime tests, not formal verification or evidence of instrumentation overhead.
