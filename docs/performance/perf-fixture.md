# Perf fixture: shared Whagons-shaped benchmark harness

`gonvex_runtime::perf_fixture` (cargo feature `perf-fixture`) starts a real
`Runtime` over Whagons-shaped SQLite tenants, with a fake in-process module
host instead of V8. Benchmarks call the same runtime functions the websocket
path calls, so a speed-up measured here is a speed-up of that code.

## Run

```sh
cd rust
export CARGO_TARGET_DIR=...            # your own target dir (see the perf brief)
cargo bench -p gonvex-runtime --features perf-fixture --bench baseline
# one group only, fewer iterations, JSON lines to a file:
BENCH_FILTER=fanout/ BENCH_ITERS=10 BENCH_JSON_OUT=/tmp/bench.jsonl \
  cargo bench -p gonvex-runtime --features perf-fixture --bench baseline
# keep the fixture working:
cargo test -p gonvex-runtime --features perf-fixture --test perf_fixture_smoke
```

Environment knobs:

| Variable | Effect |
| --- | --- |
| `BENCH_FILTER` | Run only benches whose name contains this substring. |
| `BENCH_ITERS`, `BENCH_WARMUP` | Override every bench's iteration / warmup count. |
| `BENCH_JSON_OUT` | Append one JSON object per bench to this file. |
| `BENCH_WORKERS` | Tokio worker threads of the bench (default 1, like a 1-vCPU container). |
| `GONVEX_*` | Passed to the runtime config (e.g. `GONVEX_DB_MAX_OPEN_CONNS`). |
| `GONVEX_PERF_FIXTURE_DIR` | Template cache root (default `~/.cache/gonvex-perf-fixture`). |
| `GONVEX_PERF_FIXTURE_KEEP=1` | Keep the run directory (SQLite files) after the bench. |

Every bench prints one `BENCH_JSON {...}` line (name, iterations, median/p90/p99/
mean ns, allocations/op, bytes/op, heap-live bytes/op, `extra` fields) and a
readable table at the end. Allocation counts are process-wide (they include
SQLite worker and tokio threads) and come from a counting wrapper around
mimalloc, the production allocator.

## Fixture shape

`FixtureOptions::whagons()`:

| Tenant | Tasks | Members |
| --- | --- | --- |
| `big` (index 0) | 9,000 (~400-byte description) | 78 |
| `small-1..3` | 200 each | 5 each |

- **Schema**: the real Whagons table definitions (`src/perf_fixture/whagons_schema.sql`,
  extracted from Whagons `migrations/0000_sqlite_baseline.sql` with foreign keys
  stripped) for the 20 tables the subscriptions and visibility plans touch,
  applied as the module's tenant migration; Whagons' extra `members` columns.
  Not included: the task-grid / tag-search projection triggers of
  `migrations/0001_sqlite_projections.sql` (task writes here are cheaper than
  in Whagons by that trigger work).
- **Module**: the real Whagons replica collection definitions of the ten
  collections a user opens in `bench/whagons/run.ts`, the `presence.*` and
  `tasks.update*` reducer definitions, and the version 2 visibility plans of
  their tables, including `taskUsers`/`taskTags` inheriting from `tasks`
  (`src/perf_fixture/whagons_module.json`). Regenerate both files with
  `python3 src/perf_fixture/extract_whagons.py <whagons checkout>` and bump
  `FIXTURE_VERSION`.
- **Legacy visibility** (`FixtureOptions::visibility = FixtureVisibility::Legacy`):
  the same tables' legacy (unversioned) plans as Whagons staging deployed them
  before the mask migration (`src/perf_fixture/whagons_visibility_legacy.json`,
  written by `extract_whagons.py <v2 checkout> <legacy manifest>`). Used by the
  v1-vs-v2 parity test and the `visibility_v2` bench
  (`docs/performance/visibility-v2-migration.md`).
- **Subscriptions per member** (`WHAGONS_SUBSCRIPTIONS`): `presence.list`,
  `workspaces.list`, `statuses.statuses`, `tags.tags`, `members.listMembers`,
  `members.listMemberTeams`, `taskResources.taskUsers`,
  `taskResources.taskTags`, `workspacePreferences.workspacePreferences`,
  `tasks.recentWorkspaceTasks`.
- **Members** (`MemberKind`), for 78 members: 4 `Admin` (role `admin`,
  `tasks.viewAll`: every task), 6 `Manager` (`view-users`, teams,
  `see_all_spots`), 20 `TeamAllSpots` (teams + `see_all_spots`: every task of
  their teams' workspaces), 27 `TeamSpots` (teams + 1-3 `memberSpots` at root,
  area or leaf level: tasks at or below their spots plus spotless tasks), 21
  `GrantOnly` (no team: ~40 `taskUsers` assignments and 5 `taskShares`).
- **Data**: 7 teams, 11 workspaces (each linked to 1-2 teams), a 104-spot tree
  (4 roots x 5 areas x 4 leaves), 8 statuses, 4 priorities, 40 tags, one
  presence row and two workspace preferences per member, ~0.75 assignments and
  ~0.6 tags per task, a few team shares and approval instances. 15% of tasks
  have no spot, 5% a blank spot. All content comes from a seeded SplitMix64
  (`data.rs`), so it is identical on every machine.
- **Storage**: the SQLite files (Control Plane + one file per tenant, as in
  production) are built once per fixture version and options into the cache
  (about 2 s for the small shape, see below for the full one), then copied
  (reflinked on btrfs) into a fresh run directory per `Fixture::build`. The
  seed rows are committed through the change feed, so tenants have a real
  change log. Runtime configuration uses production defaults
  (`Config::from_env` rules) plus `GONVEX_*` overrides.

## Fake module host

`fake_host.rs` is a Unix-socket server speaking module-host protocol v2
(Ping/Load/Activate/Invoke, host calls in the opposite direction). It runs on
its own thread and tokio runtime, like the separate host process. Scripted
functions issue the same `ctx.db` host calls the SDK issues on the server:

- `presence.beat` / `presence.transition`: `SELECT "_id" FROM "userPresence" WHERE "userId" = $1 LIMIT $2`, then `dbUpdate` (or `dbInsert`/`dbDelete`).
- `tasks.updateDescription` / `tasks.update`: read the task, the caller's
  `memberTeams`, the task's status, then one `dbUpdate` (4 host calls;
  `TaskUpdate { extra_reads }` adds more).

**Reducers always run on the fake socket host.** The fixture sets
`GONVEX_MODULE_HOST_IN_PROCESS=false` (and ignores any `GONVEX_MODULE_HOST_BINARY`),
so on branches with the in-process V8 host (`perf/native-reducers`, where
`IN_PROCESS` defaults to true) `reducer/*` benches still measure the socket
bridge with scripted Rust reducers. Without that override the runtime would
load the fixture's placeholder bundle (`export {};`) into in-process V8 and
every reducer would fail with `function_not_found`. The fixture cannot run its
reducers in-process: that needs a real JavaScript bundle. Measure in-process
reducer cost with that branch's own `bridge_bench` instead, and compare the
socket numbers here against it.

`fixture.host.set_js_time(Duration, JsTimeMode::Spin | Sleep)` adds simulated
JavaScript time to every invoke. `host.invocations()` / `host.host_calls()`
count the traffic. Add a function by implementing `ScriptedFunction` and
inserting it into `fake_host::whagons_functions()` (it must also exist in
`whagons_module.json` for the runtime to dispatch it).

## API

```rust
let fixture = Fixture::build(FixtureOptions::whagons()).await?;
let session = fixture.session(tenant, member);                 // admitted TenantSession
let opened = fixture.open_replica(&session, id, path).await?;  // websocket open path
let one = fixture.connect(tenant, member).await?;              // all 10 subscriptions
let mut all = fixture.connect_all(tenant).await?;              // every member
let event = fixture.update_task(tenant, task, json!({"description": "x"})).await?;
let event = fixture.beat_presence(tenant, member).await?;      // row write only
let event = fixture.commit(tenant, sql, binds).await?;         // any statement -> FeedEvent
let stats = fixture.deliver(&mut all, &event, Encode::JsonDeflate).await?;
let reply = fixture.call_reducer(&session, "presence.beat", json!({})).await;
let reply = fixture.call_control_reducer(&session, "conn", "control.support.heartbeat", args).await;
let admitted = Fixture::evaluate_visibility(&all, "tasks.recentWorkspaceTasks", &change);
```

- `commit`/`update_task` write through a `TenantTransaction` on the runtime's
  own pools and return the committed transaction as a `FeedEvent` (read back
  from the change feed).
- `deliver` clones the event per connection and calls `apply_feed_event`, then
  (per `Encode`) `serde_json::to_string` and per-message deflate
  (`compress_text`, threshold from config), as the websocket send path does.
- `call_reducer` goes through the websocket handler's `call_reducer` (module
  dispatch, fake host round trip with host calls, tenant writer, commit) with
  an idempotency key, as the SDK sends.
- `fixture.runtime`, `fixture.pools()` and `fixture.tenants[..]` (routes,
  generated members and tasks) are public for anything else.

Inside the crate the fixture can reach `pub(crate)` items; if a bench needs a
private function, expose it to the fixture with a `#[cfg(feature = "perf-fixture")]`
wrapper rather than changing its visibility in production builds.

## Add a bench

Create `rust/crates/runtime/benches/<name>.rs` and register it in
`rust/crates/runtime/Cargo.toml`:

```toml
[[bench]]
name = "<name>"
harness = false
required-features = ["perf-fixture"]
```

```rust
use gonvex_runtime::perf_fixture::harness::{BenchConfig, CountingAlloc, Report};
use gonvex_runtime::perf_fixture::{Fixture, FixtureOptions};

#[global_allocator]
static GLOBAL: CountingAlloc<mimalloc::MiMalloc> = CountingAlloc(mimalloc::MiMalloc);

fn main() {
    tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all()
        .build().unwrap().block_on(async {
            let fixture = Fixture::build(FixtureOptions::whagons()).await.unwrap();
            let mut report = Report::new();
            report.run_simple("my/bench", BenchConfig::new(5, 100), 1.0, "op", |_| async {
                // work under test
            }).await;
            report.finish();
        });
}
```

`Report::run` takes a per-iteration `setup` (untimed) and `op` (timed);
`run_sync` times synchronous code; `harness::retained(future)` reports the
heap-live and RSS bytes still held by what the future returned (keep it
alive); `report.record(name, extra)` stores non-timing numbers. Extra fields
go into `result.extra` (the `run*` calls return `Option<&mut BenchResult>`).

## Measure any commit of `claude/sqlite-storage-lab`

The fixture only touches production code through feature-gated hooks
(`Config::perf_from_lookup`, `gonvex_websocket::perf_compress_text`, the
`perf_fixture` module itself), so it merges onto later lab commits:

```sh
git worktree add /home/gabriel/work/fixture-at-X <lab-commit>
cd /home/gabriel/work/fixture-at-X
git merge --no-edit perf/fixture        # or: git cherry-pick the perf/fixture commits
cd rust && CARGO_TARGET_DIR=<your target dir> \
  BENCH_JSON_OUT=/tmp/X.jsonl cargo bench -p gonvex-runtime --features perf-fixture --bench baseline
```

Compare two runs by bench `name` in the JSON lines. If a lab commit changes
replica internals, the only places to adapt are `subscription_rows` and
`Fixture::evaluate_visibility` in `src/perf_fixture/mod.rs`; benches read
`MemberConnection::rows` and `Delivery` and never `ReplicaSubscription`
fields.

### Stable bench names and fields

| Question | Bench | Fields |
| --- | --- | --- |
| Bytes per subscriber / per user after opening | `retained/all_members_10_subscriptions`, `retained/one_member_10_subscriptions/TeamAllSpots` | `liveBytesPerMember`, `liveBytesPerSubscription`, `liveBytes`, `peakHeapBytes`, `rssBytes`, `rowsPerSubscription` |
| Snapshot open of one member (9k tenant) | `snapshot_open/tasks.recentWorkspaceTasks/{Admin,TeamAllSpots,TeamSpots,GrantOnly}`, `snapshot_open/all_10_subscriptions/TeamAllSpots` | `medianNs`, `p99Ns`, `allocationsPerOp`, `bytesPerOp`, `peakHeapBytes`, `extra.rows` |
| Apply-feed-event per change over all 78 subscribers | `fanout/task_update/78_members/apply`, `fanout/presence_beat/78_members/apply+json` | `medianNs` (per change), `extra.applyNsPerSubscriber`, `extra.visibilityChecksPerChange` (`in_memory_row_checks`), `extra.applyNsPerVisibilityCheck`, `extra.visibilitySqlPerChange`, `extra.collectionSnapshotsPerChange` |
| Encode per delivered change | `fanout/task_update/78_members/apply+json`, `.../apply+json+deflate` | `extra.encodeNsMedian`, `extra.encodeNsPerMessage`, `extra.jsonBytes`, `extra.wireBytes`, `extra.messages` |
| Visibility of one change | `visibility/task_update/78_members` | `medianNs`, `extra.admitted` |
| Compression | `ws/compress_text/task_delta` | `medianNs`, `extra.payloadBytes`, `extra.compressedBytes` |
| Reducers | `reducer/{presence.beat,tasks.updateDescription}/js_{0,1500}us`, `reducer/control.support.heartbeat/js_0us` | `medianNs`, `p99Ns`, `allocationsPerOp`, `extra.hostCallsPerInvoke` |

## Baseline

Base: `claude/sqlite-storage-lab` at 72e6d30 (merged into `perf/fixture`), no
other runtime changes. Machine: arch3, AMD Ryzen 7 5800X (16 threads, `nproc`
14 for this user), 31 GiB, btrfs NVMe; bench with 1 tokio worker (the fake
host on its own thread). The machine was running other agents' builds at the
same time, so p99s are noisy; compare medians and allocation counts. Bench
profile (thin LTO). Template build of the full fixture: 4.3 s (cached
afterwards; `Fixture::build` from the cache: ~1 s).

| Bench | Median | p99 | Allocs/op | Bytes allocated/op | Notes |
| --- | --- | --- | --- | --- | --- |
| `snapshot_open/tasks.recentWorkspaceTasks/Admin` | 175.5 ms | 214.2 ms | 1,283,147 | 232.3 MiB | 9,000 rows, peak heap +76.4 MB |
| `snapshot_open/tasks.recentWorkspaceTasks/TeamAllSpots` | 70.2 ms | 86.4 ms | 492,563 | 88.3 MiB | 3,411 rows, peak heap +29.0 MB |
| `snapshot_open/tasks.recentWorkspaceTasks/TeamSpots` | 26.8 ms | 32.2 ms | 104,518 | 18.0 MiB | 652 rows |
| `snapshot_open/tasks.recentWorkspaceTasks/GrantOnly` | 13.7 ms | 15.6 ms | 16,979 | 2.1 MiB | 45 rows |
| `snapshot_open/all_10_subscriptions/TeamAllSpots` | 167.4 ms | 191.1 ms | 920,388 | 123.1 MiB | 8,528 rows, peak heap +38.6 MB |
| `fanout/task_update/78_members/apply` | 131.8 ms | 251.3 ms | 82,336 | 109.0 MiB | 1.69 ms/subscriber; 78 receivers, 284 messages, 858 in-memory row checks, 0 visibility SQL / snapshots |
| `fanout/task_update/78_members/apply+json` | 142.2 ms | 235.4 ms | 83,118 | 109.4 MiB | JSON encode 0.35 ms per change (1.2 us/message), 209.5 KB |
| `fanout/task_update/78_members/apply+json+deflate` | 186.3 ms | 266.4 ms | 83,304 | 123.2 MiB | encode+deflate 1.46 ms per change (5.1 us/message), 122.0 KB on the wire |
| `fanout/presence_beat/78_members/apply+json` | 7.75 ms | 20.7 ms | 53,097 | 7.9 MiB | 99 us/subscriber, 234 messages, 106 KB |
| `visibility/task_update/78_members` | 0.50 ms | 2.0 ms | 6,339 | 494 KiB | 6.4 us/subscriber, 25 of 78 admit the row |
| `ws/compress_text/task_delta` | 9.8 us | 16.2 us | 3 | 374 KiB | 3,836-byte delta -> 1,200 bytes; a new compressor (~370 KiB) per message |
| `reducer/presence.beat/js_0us` | 8.2 ms | 22.8 ms | 1,855 | 224 KiB | 2 host calls; time is mostly the durable commit |
| `reducer/tasks.updateDescription/js_0us` | 24.3 ms | 67.6 ms | 3,463 | 437 KiB | 4 host calls |
| `reducer/control.support.heartbeat/js_0us` | 19.0 ms | 38.4 ms | 146 | 15 KiB | native Control Plane path, no module host |
| `reducer/presence.beat/js_1500us` | 24.4 ms | 70.5 ms | 1,849 | 224 KiB | 1.5 ms simulated JS (spin) |
| `reducer/tasks.updateDescription/js_1500us` | 24.2 ms | 67.8 ms | 3,457 | 432 KiB | |

Retained memory after opening (heap-live from the counting allocator):

| Measurement | Value |
| --- | --- |
| One `TeamAllSpots` member, 10 subscriptions (8,528 rows, 3,411 tasks) | 37.9 MB live (peak 38.6 MB) |
| All 78 members, 780 subscriptions (308,649 rows, 120,611 task rows) | 165.7 MB live: **2.12 MB per member, 212 KB per subscription**; RSS +145.7 MB |

Reducer timings on this machine are dominated by the durable SQLite commit
(`synchronous=full` fsync on a shared NVMe) and vary run to run (an earlier run
on 49b9882 measured `presence.beat` at 40.8 ms median); use their allocation
counts and host-call counts for code changes, or set `GONVEX_SQLITE_SYNCHRONOUS=normal` to isolate CPU work. Not measured here: concurrency (several
reducers or opens at once), the websocket transport, V8, and the Whagons
projection triggers.
