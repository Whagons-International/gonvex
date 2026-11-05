# Gate6 snapshot-path baseline and waiting-future probes

2026-11-04 Pacific / 2026-11-05 UTC. Work branch `perf/open-cost`, based
exactly on integration `38e600f6`. **No runtime optimization was applied.**
Gabriel withdrew the design/implementation briefs and requested that this
unchanged-runtime evidence be preserved. There is no optimized after result,
no generic speed-up claim, and no full-app capacity result here.

## Revisions and fixture

- Runtime source: `38e600f6`; benchmark/oracle source used for CPU results:
  `6f9def68` (the only subsequent code change is the queue harness polling fix,
  `85aa831b`). Production runtime files are unchanged from the integration base.
- CPU baseline binary, kept at
  `/mnt/data/work/gonvex-open-cost/before-bench`, SHA-256
  `937de909ff5ec578b8e1182b40cc785ef1c8b71a3bf4b1c2218463dada781a40`.
- Full synthetic offline DB input:
  `/home/gabriel/work/gonvex-perf/lab-5k-new/baseline-data`, copied in its
  entirety, including WAL/SHM, to
  `/mnt/data/work/gonvex-open-cost/offline-data`. Only the copy's routing URLs
  were rewritten; copied pending scheduled/outbox work was cleared before
  runtime startup. No shared lab DB or endpoint was changed or contacted.
- Stored app module hash:
  `da98a6665b3b2f834f67e7a5eee99d43708abe36f415dafdfc20bcf47439c069`.
- Capture: `bench/whagons/app-opens-7d97617cf.json`, SHA-256
  `288a7fcb58566adc7eb4bdaa0d03d402bd58562b051491109cbc4f85b15b47dd`.
  Its 130 requests plus the old gate's deduplicated metadata/task requests
  produce **131 opens**, preserving all args and substituting `$member`.
  This is neither the actual first-visit 177–178 opens nor a verified capture
  of the currently shipping app. The shipping recapture is owned elsewhere.
- Eight real synthetic admitted members: two admins and two ordinary members
  in each of the two largest tenants. These cover different user IDs, roles,
  row sets and tenant boundaries. Equal roles are **not proof** of equivalent
  visibility for every collection; member-specific collections differ. No new
  cross-user sharing or class-equivalence optimization was implemented.
- CPU affinity CPU 3, one Tokio worker, two readers, two snapshot build slots;
  optimized Cargo bench profile, default snapshot/principal cache settings.
  Counting allocator wraps mimalloc. Process CPU is user + system `getrusage`.

The offline constructor starts the real Runtime against the owned DB copy and
uses a fake module host. Setup, module activation and session admission happen
**outside** the timed regions. Timed work is sequential `open_replica` over the
collection set, including constructing and dropping messages/subscriptions.
It excludes HTTP, Firebase/auth exchange, WebSocket orchestration/input queues,
wire serialization, paging, compression, transport, client installation and
browser work. These are snapshot-path CPU per synthetic collection-set visit,
not complete end-to-end sign-in CPU. They cannot explain the full ~1.5 CPU-s
first visit or ~0.75 CPU-s actual page reload measured in Step 16.

## CPU results, unchanged runtime

First means one cold-cursor pass for that member. The first member in each
tenant also warms that tenant's shared cache. Later members benefit from this
and from earlier SQL-oracle runs. Reload is the median of three repeats with
**a cursor for every collection**, with no intervening writes. The actual app
capture had cursors for only 89/130 opens: this is an idealized all-cursor
reload, not an exact reproduction of its mixed reload. It does not measure
changed-log catch-up, a reconnect burst, or a concurrent cold-cache storm.

| Synthetic session | First CPU ms | All-cursor reload CPU ms | First allocations | Reload allocations |
| --- | ---: | ---: | ---: | ---: |
| Tenant 0 admin 0, tenant cold | 639.394 | 27.345 | 988,002 | 105,082 |
| Tenant 0 admin 1 | 34.562 | 22.686 | 175,895 | 104,491 |
| Tenant 0 member 2 | 44.580 | 22.417 | 173,466 | 105,953 |
| Tenant 0 member 3 | 49.360 | 22.261 | 176,751 | 106,735 |
| Tenant 1 admin 0, tenant cold | 236.956 | 21.516 | 696,551 | 104,598 |
| Tenant 1 admin 1 | 32.316 | 24.391 | 176,685 | 104,609 |
| Tenant 1 member 2 | 35.254 | 25.899 | 170,922 | 106,496 |
| Tenant 1 member 3 | 37.888 | 21.521 | 176,539 | 107,124 |

For each user and each of the 131 collections, a separate, untimed cached
cold open's exact projected row set was compared with `open_replica_fresh`
(the SQL oracle): **1,048 comparisons passed**, no need-hashes result or
missing subscription. This checks the existing path, not a new fast path.
Raw measurements: [CPU and oracle JSONL](evidence/open-cost-baseline.jsonl).

Available attribution is partial. The first cold tenant-0 pass constructs
15,986 cached raw rows (126,700 attributed allocations, 101.7 ms in that site's
calling-thread scope), and decodes their ACL columns (89,062 attributed
allocations, 22.3 ms). Tenant 1: 11,165 cached rows, 52.3 ms row construction,
11.0 ms ACL decode. Warm passes construct no cached raw rows; their cache-view
scope is roughly 1.4–4.1 ms per collection-set pass. These site scope durations
are not a complete or additive process CPU partition. SQLite connection/schema
work, principal/ACL setup and other code account for unpartitioned work.

`perf record` failed both inside the admitted service and when attaching as an
external read-only observer (`Failure to open any events for recording`,
`perf_event_paranoid=2`). A trivial child-process `perf stat cycles:u true`
worked, but did not make recording the admitted runtime possible. No policy,
capability or kernel setting was changed. No usable CPU flamegraph was obtained.
Errors remain under `/mnt/data/work/gonvex-open-cost/*perf*.log` and
`before-{profile,cycles}.log`.

## Waiting memory: existing configuration only

A separate 9,000-task / 78-member shared fixture provides 17 whole-table
collections using its real schemas and visibility plans. The probe holds both
build slots, polls 5,000 actual `open_replica` futures without Tokio cooperative
budget artifacts, and counts pending futures, immediate errors and allocator
live/peak growth. All pending futures are then cancelled, the slots released,
and a new open must successfully install a subscription. Every variant passed
this recovery/conservation check. The probe uses a shared borrowed session and
excludes enclosing WebSocket/offload futures and connection/socket/input queues.

| Cache/key state | Existing max-waiting config | Pending opens | Admission waiters | Immediate refusals | Heap live growth, decimal MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Warm, 17 distinct keys | 4,096 | 4,096 | 4,096 | 904 | 52.368 |
| Cold, 17 distinct keys | 4,096 | 5,000 | 17 | 0 | 63.190 |
| Warm, 17 distinct keys | 256 | 256 | 256 | 4,744 | 3.830 |
| Cold, 17 distinct keys | 256 | 5,000 | 17 | 0 | 63.190 |
| Cold, one key | 256 | 5,000 | 1 | 0 | 62.581 |

The existing cap bounds **build-admission waiters**, not complete open
preparations. Cold opens can wait on `SnapshotCache::entry`'s per-key
single-flight mutex before reaching build admission. Reducing the configured
cap therefore does not bound the cold pending-open count. The probe's full
5,000-attempt allocated bytes stayed about 90–93 MB across variants; smaller
retained heap is not proof of cheaper opens, lower total allocations or higher
capacity. It deliberately records refusals instead of hiding failed opens.

These pure-future numbers **do not reproduce or explain** gate6's reported
~1.2 GB for 4,096 waiting builds. They omit substantial enclosing connection
state and use the 17-table fixture, not the full app's socket path. There is no
claim that all runtime memory is bounded or that 256 is a measured production
optimum. No queue default, fairness behavior, admission lane, failure contract
or retry implementation was changed.
Raw results, in the table's order: [queue JSONL](evidence/open-cost-queue.jsonl).
The earlier cooperative-budget-affected probe is invalid and is not included.

## Current source inventory (no design proposal)

- `snapshot_cache.rs` already stores projected raw JSON bytes and row hashes,
  shared across users by content/args/database, with a default 256 MiB process
  budget and 900-second SQL-build age. The cache is lazy and rolls/replays logs;
  it is not an always-maintained, finished authorized snapshot for each class.
- `cached_view` checks read epoch/revision, active membership and index readiness;
  it binds the member and evaluates visibility over shared cached rows. Opens
  still take read transactions and resolve/check their principals.
- V2 compiled visibility has class atoms and separate member atoms, including
  inherited parents; a class ID alone does not establish equal authorized rows
  when member atoms exist. Unsupported expressions use the interpreted/SQL
  fallback. AND/OR, ancestry and member-dependent joins cannot be replaced by
  an unproven bitwise-AND test. Reopen/principal semantics were left to their
  assigned child; no principal code was edited here.
- Resume paths can use cursors and the raw change log, but still have integrity,
  dependency-change, index-readiness and fallback work. This experiment used
  no writes and does not establish their cost or correctness under revocation.
- No persisted columns, new hot cache, ready-made snapshots, ACL memo or early
  lifetime guard was applied. The unapplied candidate patch was deleted after
  the explicit hold. No app lazy-opening changes or gate-set reduction occurred.

## Commands, artifacts and resource handoff

Build (admitted with coach-resources/coach-compile, headroom checked on the true
target immediately before each build):

```sh
cd rust
CARGO_TARGET_DIR=/mnt/data/work/gonvex-open-cost/target \
  cargo bench -p gonvex-runtime --features perf-fixture --bench snapshot_raw --no-run
```

Baseline executable environment (the command was inside an admitted service):

```sh
GONVEX_DB_MAX_OPEN_CONNS=2 GONVEX_REPLICA_SNAPSHOT_CONCURRENCY=2 \
BENCH_OFFLINE_DATA=/mnt/data/work/gonvex-open-cost/offline-data \
BENCH_OFFLINE_SESSIONS=/mnt/data/work/gonvex-open-cost/sessions.json \
BENCH_OFFLINE_OPENS=/absolute/path/bench/whagons/app-opens-7d97617cf.json \
BENCH_ITERS=3 BENCH_ORACLE=1 \
  taskset -c 3 /mnt/data/work/gonvex-open-cost/before-bench
```

Queue series script and exact grants/logs:
`/mnt/data/work/gonvex-open-cost/queue-series.sh`, `queue-series.log`,
`build-before*.log`, `build-queue-2.log`. Queue source `85aa831b`; the first
build caught a harness `Context` crossing an await, fixed before measurements.
The final queue harness excludes the earlier cooperative-budget artifact.
Formatting of all three touched Rust files and `git diff --check` passed.
No runtime tests were rerun because production runtime code did not change;
the optimized benchmark, SQL oracle and cancellation/recovery assertions were
executed successfully. No CI, PR, push, deploy, load driver, staging endpoint or
browser was used.

All new builds, fixture copies, caches and outputs are on `/mnt/data`.
The resource wrapper's legacy path rule required the real source checkout as
`--build-dir` metadata/cwd; explicit `CARGO_TARGET_DIR` remained on `/mnt/data`.
No caps, wrapper policy, running worker or integration branch was changed.
The own lab lock was acquired atomically after heap's explicit release at
03:42 UTC and removed after all jobs exited at 03:54 UTC, with an ownership
assertion. Reopen and integration were notified directly; reopen is next.
There are no runtime-file overlaps with the reopen child and no integration
merge was performed. These harness additions may overlap only another change
to `snapshot_raw.rs` or `perf_fixture/mod.rs`.
