# Bounded authoritative replica opens

The authoritative path now takes a nonblocking RAII permit after the
runtime-validated XOR resume attempt and before `SnapshotCache.entry`. This
bounds cold same-key single-flight waiters as well as distinct-key builders,
keyed inline misses, cache hits requiring authoritative evaluation, and
cache-disabled/fresh SQL fallbacks. The permit lasts until the ordinary open
returns or its future is dropped. Errors and cancellation release ownership.
Clearing or disabling the cache does not replace the admission owner.

The existing snapshot count/byte admission and FIFO short/long lanes remain
inside this bound. Cache entry acquisition still precedes the ordinary read
transaction. Failed fast validation closes its transaction and drops its read
permit before falling back into this path. No reader is held while waiting for
a separate cache builder to obtain a reader.

## Configuration and limits

`GONVEX_REPLICA_OPEN_MAX_WAITING` is an additional open-lifetime budget, default
256. The total accepted authoritative-open limit is the actual configured
`SnapshotAdmission` active-slot capacity plus this budget. Zero means active
slots only. Invalid/unparseable or overflowing budgets fall back to the default
(or the remaining representable semaphore capacity). Configuration is read when
the process-wide guard is first initialized, not on each acquisition. The total
is conservatively clamped to Tokio's representable semaphore maximum.

The active-slot capacity comes from `GONVEX_REPLICA_SNAPSHOT_CONCURRENCY`
(default available CPUs, minimum two). The existing
`GONVEX_SNAPSHOT_MAX_WAITING` build-lane limit remains 4096 by default, with its
existing positive-value semantics and 30-second default
`GONVEX_SNAPSHOT_ADMISSION_TIMEOUT_MS`. It bounds build-lane waiters, whereas the
new budget bounds the whole authoritative open, including time before build
admission. Neither budget creates another asynchronous semaphore queue.

256 is an initial conservative budget informed by earlier pure-open-future
measurements: about 3.8 MB for 256 warm waiting futures versus 52 MB for 4096;
5000 cold futures previously bypassed either build-waiting setting. Those
measurements do not establish socket RSS or a production capacity point.
Operators should tune against measured refusal/recovery latency and memory,
without relaxing the latency objective to call an overloaded run a pass.

Each runtime's snapshot cache owns a handle to the same process-wide guard.
Independent Runtime instances in the process share the limit; clearing or dropping
one cache cannot reset live ownership or create another admission budget. The
existing builder admission is process-wide too. There is no new tenant or
connection map, no strict cross-tenant reservation, and no starvation guarantee
across retrying tenants. The existing FIFO lanes apply once an accepted open
reaches build admission.

## Retry and readiness contract

Refused opens return the existing `ReplicaError::Overloaded`, transported as
`replica.error` with the original subscription id/path. The SDK retries this
existing error contract with 250 ms exponential backoff capped at five seconds;
the thin client adds bounded downward jitter. Refusal emits no snapshot, ready,
or new cursor. Existing cached membership/cursors remain available for retry.
The SDK can advertise its cached cursor/digest on retry. The existing thin
benchmark deliberately cold-opens normal retries; it retains its saved cursor
for explicit full-integrity reconciliation. This change preserves that driver
behavior. No client-provided digest creates runtime validation proof.

A supported current runtime-validated XOR resume returns before this guard,
without full-list evaluation. Tampered, pruned or permission-invalidated proof
falls through to authoritative admission after the fast read is released.
The module lookup/metadata prefix, cheap validation read/task queue, per-socket
preparation backlog, completed
snapshot results, encoding, and outbound socket queues are **outside** this
bound. This change does not prove those aggregate backlogs bounded. It also
cannot bound futures callers allocate but never poll or refuse to drop after
completion.

## Metrics

The existing profiler gains appended stable stage/gauge IDs:

- `replica.cold_open_active`: accepted guarded authoritative opens, including
  cache single-flight and admission waits, until completion/cancellation.
- `replica.cold_open_lifetime`: that guarded lifetime, once per accepted open.
- `replica.cold_open_refused`: one count per refused acquisition (value is one,
  not nanoseconds).

Admission counters remain authoritative even when profiling is disabled;
`SnapshotCache` stats expose the limit, active count, lifetime peak and refused
count. Hot acquisition uses atomic counters without allocation, mutexes, string
formatting or environment lookup. Profiler gauge ownership is captured at
acceptance and decremented even if profiling is disabled before cancellation.

Cold-open gauge ownership uses a dedicated profiler helper with one enabled
snapshot. The existing generic `Gauge::guard` double-check race during concurrent
profiling toggles remains outside this change; this is not a general profiler
underflow fix. The new guard removes its counted gauge lifetime before publishing
free admission capacity, with acquire/release ordering for replacement races.

## Reproducing and merging

`rust/crates/runtime/tests/open_admission_probe.rs` runs unchanged against the
mature baseline and candidate. Compile the test binary with `--release --features
perf-fixture --test open_admission_probe --no-run` through the machine's resource
and compiler gates. Run that prebuilt binary separately with build waiting budgets
256 and 4096. Set `GONVEX_OPEN_PROBE_BASELINE=1` only for an actual baseline source
build; this changes test expectations, not production admission. The probe uses
independent first-poll budgets; a separate test spawns 5000 real Tokio tasks behind
a barrier while 78 large-tenant connections and small-tenant subscriptions remain
installed. No sleeps coordinate admission.

The raw source baseline is `2b7e2ff9f6950ea73a4659c284c9204262314574`.
The isolated future-size fix `bc526fbd2a8aa5e808348f25e1caac3a15606655` is consumed
as `1c94c8ac` on this branch. Apply that separate fix and then the guard change;
do not merge experimental perf ancestry. The only ordinary-open hook follows
`resume::try_open` and `drop(module)`, before cache arguments, keyed lookup,
single-flight, or fresh-read work. No `lib.rs`, starter, protocol, client,
permissions, store hooks, or application function implementation changes are
required by the guard.

The profiler additions append stable IDs and add the dedicated cold-open gauge
helper. SnapshotCache retains a handle to process-wide state across cache clear,
toggle, generation changes and Runtime replacement. No reader is held by this
guard. Completed snapshots and transport queues retain their existing ownership.

Required generators refreshed the dashboard's previously stale bundled SDK cron
constructor (`dueWhen` support already present in source). Bundles were generated
from a canonical source mirror so machine-specific symlink paths do not enter
bundle comments/hashes. On a conflicting integration, rerun generators against
the merged source; never hand-merge the embedded generated code.

## Local measurements (2026-11-05)

Candidate branch: `perf/bounded-open-admission`; base and isolated future fix are
listed above. The frozen candidate Rust source manifest SHA-256 is
`849b37af4c7a8d386c63bc2a65eb3af9cec5a0e3c6a585a536b59008f83a3836`.
Raw logs, source manifests, aggregate JSON and actual-cgroup receipts are under
`/mnt/data/work/gonvex-open-admission`. Builds used release optimizations, thin
LTO and four codegen units. Every heavy phase verified `memory.max=6442450944`
and `cpu.max=200000 100000`, MemAvailable >=8 GiB, both I/O PSI avg10 values <20%,
and build-headroom on the actual /mnt target. Other workers/caps were unchanged.

The same prebuilt module probe runs against raw 2b7 and the candidate, with both
actual snapshot slots held before offering 5000 opens. Fixture: synthetic
9000-task/78-member tenant plus three 200-task/5-member tenants, fake module host,
real runtime/cache/admission. Live bytes below are requested Rust allocations
retained after polling; C allocator activity is excluded. They are not socket
RSS or encoded snapshot memory.

| Source | Build waiting cap | Open extra budget | Keys | Pending | Refused | Rust live bytes |
|---|---:|---:|---|---:|---:|---:|
| 2b7 | 256 | absent | same | 5000 | 0 | 68,615,344 |
| 2b7 | 256 | absent | distinct keyed | 256 | 4744 | 3,794,866 |
| 2b7 | 4096 | absent | same | 5000 | 0 | 68,615,344 |
| 2b7 | 4096 | absent | distinct keyed | 4096 | 904 | 59,526,394 |
| Candidate | 4096 | 256 | same | 258 | 4742 | 3,632,703 |
| Candidate | 4096 | 256 | distinct keyed | 258 | 4742 | 3,850,464 |
| Candidate | 256 | 256 | same | 258 | 4742 | 3,632,018 |
| Candidate | 256 | 256 | distinct keyed | 256 | 4744 | 3,805,106 |
| Candidate | 4096 | 0 | same | 2 | 4998 | 112,306 |
| Candidate | 4096 | 0 | distinct keyed | 2 | 4998 | 109,598 |

The same-key cache single-flight gap persists at either old build waiting cap.
The distinct keyed ordinary path already honors that cap once actually polled;
the direct cache-entry pending-builder test is a different case. At cap 256,
the candidate adds a small guard cost to the already-bounded distinct path.

Resident memory was measured separately. For the 4096 build-cap same-key case,
raw 2b7 RSS was 128,692,224 -> 170,733,568 bytes; candidate RSS was
126,513,152 -> 126,660,608 bytes. Allocator reuse and fixture memory affect these
totals, and later modes reuse the same process. The candidate still allocated
136,772,017 cumulative Rust bytes across 5000 offers: callers allocate/drop their
futures even when refused. The bound concerns retained accepted ordinary work.

With the default open budget, serial same-key offers used 57.77 ms CPU / 58.01 ms
wall on raw 2b7 and 50.66 ms CPU / 50.63 ms wall on the candidate. These are module
polling costs, including request construction, not transport latency percentiles.
The real spawned-task case held 78 large-tenant connections and small-tenant
subscriptions, cleared the cache, then offered 5000 cold opens: 258 stayed
accepted and 4742 refused in 69.32 ms wall / 147.40 ms process CPU. All tasks were
drained after cancellation; admission and gauge returned to zero, with exactly
4742 refusal samples and 258 lifetime samples. Small-tenant recovery plus a fresh
SQL-oracle comparison took 10.01 ms wall / 8.84 ms CPU for 200 rows.

One million accepted guard/drop iterations measured 12.36 ns/op with profiling
disabled and 68.84 ns/op enabled, with zero calling-thread allocations in both
cases. Refusal measured 8.90 / 13.69 ns/op respectively. These wall-clock samples
include guard/metric work under the two-CPU quota; threads were not pinned and
other lanes remained active.

A conservative next bounded-lab configuration is the new default open budget
256 alongside build waiting cap 256, with existing slot/byte limits and FIFO
lanes retained. The lower build cap also limits the separate fast-validation
admission queue; it does not bound task/module-prefix or outbound queues. The
zero-open-budget boundary is tested, not proposed as a production setting.
No full 1k load, all-socket RSS measurement, production capacity certification or
latency-SLO pass is claimed. The separate generator-budget decision remains
outside this lane.

Validation passed:

- Runtime release library: 410 passed with perf fixture; 405 passed with default
  production features. Each retains eight pre-existing ignored tests.
- Profiler: 11 passed, including both profiling-toggle ownership directions.
- Snapshot-cache, principal-cache, replica-state, validated resume, queued
  permission-revocation and socket admission/feed oracles: all passed.
- Module probes at build caps 256/4096 and zero open wait budget: passed.
- Real SDK required-set/backoff/cached-cursor and resume/lifecycle/integrity:
  31 passed; thin client: 18 passed.
- Dashboard/template codegen and agent-api outputs, docs metadata, full workspace
  typecheck, owned Rust formatting and diff checks: passed.
- Runtime TypeScript import graph: two inherited cyclic edges before/after,
  zero added. No existing cycles were waived or changed.
- Raw 2b7 WebSocket future independently measured 29,664 bytes and failed its
  existing 28,672-byte test budget. The isolated upstream three-await fix plus
  this change measures 23,128 bytes with both fixture and production features.
  The budget and its tests are unchanged.
