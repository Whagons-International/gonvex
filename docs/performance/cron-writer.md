# Long cron/job Reducers vs. the tenant writer and the only isolate

Branch `perf/cron-writer` (gonvex) and `perf/grant-projection-incremental` (Whagons).

## Problem (perf/delivery-loss, new-200: 200 users, 1 vCPU)

77 saves across 13 Reducers failed with `invocation deadline elapsed while waiting for a module isolate`.
On 1 vCPU the engine has one warm isolate (`isolate_pool_size = clamp(CPUs, 1, 4)`), and every tenant's Reducers queue for it.
The per-cron table in `/dev/profile` (`lab-5k-new/new-200/profile`) showed:

| cron | runs | errors | mean | max | V8 | writer hold max |
|---|---:|---:|---:|---:|---:|---:|
| grantProjection.reconcile-hourly | 11 | 2 | 3,190 ms | 17,973 ms | 2,777 ms | 17,972 ms |
| (one-shot) | 54 | 0 | 830 ms | 8,911 ms | 758 ms | 1,003 ms |

The only one-shot function in that run is `tasks.notifyIfUnassigned`, which `tasks.create` schedules.

## Where the 18 s goes

Harness: `bridge_bench_whagons` (`rust/crates/runtime/src/bridge_bench.rs`, release build, in process, `taskset -c 5`).
Database: a copy of the largest new-lab tenant (`lab-5k-new/bench-db`): 2,031 members (1,951 active), 2,444 memberTeams, 8,851 tasks.
Bundle: the real Whagons bundle, `gonvex/_generated/manifest.json` of the branch. Each invoke rolls back.
The bench gained the workloads `grantProjection.reconcile` and `tasks.notifyIfUnassigned` (on the scheduler channel), plus `BRIDGE_BENCH_WARMUP`.

| `grantProjection.reconcile`, one invocation | value |
|---|---:|
| invoke p50 (5 samples) | **6,457 ms** |
| time inside host-call handlers (SQLite) | 6,369 ms |
| JavaScript + bridge | 92 ms |
| host calls | 11 (5 reads, `SAVEPOINT`, 4 `UPDATE`, `RELEASE`) |
| members written | 1,951 (4 batches of up to 500) |
| writer hold | 6,457 ms |

So the JavaScript loop is not the cost.
It is about 1.6 s for each of the four batched `UPDATE`s (`BRIDGE_BENCH_TRACE=1`: 1.70 / 1.73 / 1.67 / 1.52 s).
The SDK's `updateDataRows` (`packages/module-sdk/src/data-write.ts`) emitted:

```sql
UPDATE "members" SET ... FROM json_each($1) AS source
WHERE "members"."id" = source.value ->> '$.id'
```

SQLite cannot index `source.value ->> '$.id'`.
It plans this as `SCAN members` with `SCAN source` per row: 2,031 × 500 ≈ 1M JSON extractions per statement.
The cost is O(table × batch), so it grows with tenant size.
On the loaded 1-vCPU container it was ~3× slower again (≈18 s), past the 10 s deadline.

The watchdog terminates JavaScript, but it cannot stop a statement already running in SQLite.
So the run failed, rolled back, and held the writer until the last statement returned.
Because it never commits, **every hourly run on the big tenant repeats the same 1,951-row rewrite and fails again**.
The lab tenant shows it: no member has `__fromRoles`.

`tasks.notifyIfUnassigned` is cheap on its own: 44–68 ms p50 for ~1,000–1,460 inline notification inserts, 1 CPU.
Its 8.9 s in new-200 was time spent waiting for the isolate behind other work.

## Fixes

### 1. SDK: batched updates look rows up by key (gonvex `packages/module-sdk`)

`updateDataRows` sends the batch as a JSON object keyed by row ID:

```sql
UPDATE "t" SET col = <$1 -> CAST("t"."key" AS TEXT) ...> WHERE "t"."key" IN (SELECT key FROM json_each($1))
```

- Rows are found through the primary key.
- Each row reads its fields with one lookup against SQLite's cached parse of `$1`.
- IDs that `->` would read as path syntax fall back to a correlated `json_each` lookup per row. The safe set is `/^[A-Za-z0-9_:.@/-]+$/`; anything else (a leading `$`, quotes, brackets, spaces, non-ASCII) uses the fallback.

The semantics are unchanged, including JSON columns, NULLs, integer keys and missing rows.
`test/data-write-sqlite.test.mjs` runs the old and new statements against real SQLite (`node:sqlite`) and compares the rows.
It covers both ID classes, integer keys, and a query-plan assertion.

| 2,031-row members table | old | new |
|---|---:|---:|
| 500-row batch (python sqlite3 3.54, stub capture function) | 447 ms | 13 ms |
| 2,000-row batch | 1,889 ms | 60 ms |
| full reconcile in the bench (unchunked app, SDK fix bundled) | 6,457 ms | **303 ms** (SQL 211 ms) |

### 2. App: chunked reconcile (Whagons `gonvex/grantProjection.ts`)

`grantProjection.reconcile({after?, batch?})` works in steps:

- It projects every member (reads are cheap).
- It writes at most `RECONCILE_BATCH = 100` changed members after `after`, ordered by ID.
- When more remain, it schedules itself with `ctx.scheduler.runAfter(0, 'grantProjection.reconcile', {after: lastWritten, batch})`.

The cron still fires hourly with `{}`.

- **Steady state:** a tenant that is up to date costs one short invocation that writes nothing.
- **Chain length:** a chain covers 32 durable hops × 100 = 3,200 stale members.
- **Larger tenants:** a tenant with more stale members finishes over the following hours, because every step recomputes what is still stale.

Also:

- `pendingMemberGrants` indexes team memberships by member. `memberPermissionNames` scanned every membership for every member, O(M×T); it now receives only the member's own rows, so the result is the same. JS time fell 92 → 42 ms.
- `tasks.notifyIfUnassigned` now reads only the memberTeams of the workspace's teams (`teamId in`), instead of every membership.

Equivalence test (`gonvex/module/grantProjection.test.ts`, "reaches the same rows as one full sync"):

- **Setup:** two copies of a 640-member tenant (more than 2 batches stale), mixing global roles, team roles, hand-set flags, removed members and withdrawn grants.
- **Result:** one `syncMemberGrants` on one copy and the reconcile chain on the other produce identical `members` rows.
- **Step count:** `ceil(writes / batch)` steps, each at most `batch` writes.
- **Idempotence:** a further run writes and schedules nothing.

A second test covers a custom batch size, and a member that goes stale behind the cursor, which the next run picks up.

| one reconcile step, 1 CPU, same tenant | invoke p50 | SQL | JS | writer hold |
|---|---:|---:|---:|---:|
| before (one invocation, all 1,951) | 6,457 ms | 6,369 ms | 92 ms | 6,457 ms |
| chunked, batch 100, published SDK (artifact ccacddb37617) | 466 ms | 400 ms | 58 ms | 466 ms |
| chunked, batch 200, published SDK (1bbf5855d687) | 709 ms | 676 ms | 42 ms | 709 ms |
| chunked, batch 200, fixed SDK (dff282988471) | **62.5 ms** | 23 ms | 43 ms | 62.7 ms |

`RECONCILE_BATCH` is 100 because Whagons pins the published `@gonvex/module-sdk 0.5.2-sqlite.0` until the SDK fix is released.
After the release, a 100-member step costs about 30 ms.

### 3. Runtime guard (gonvex `crates/module-runtime-v8`)

Three mechanisms were evaluated and implemented:

- **(b) Priority admission** (`admission.rs`, default on, `GONVEX_MODULE_HOST_ISOLATE_PRIORITY`):
  - The isolate pool's FIFO semaphore is replaced by a priority queue.
  - The lane comes from the invocation channel and root channel: user (UI/API/agent) first, then `System` (outbox Actions and the calls they make), then `Scheduler` (crons, one-shot jobs and what they call).
  - Rank = arrival + lane delay (0 / 1 s / 5 s). That bounds starvation: scheduled work that has waited 5 s goes ahead of user calls arriving later.
  - Cancel-safe: a timed-out waiter leaves the queue, and a permit granted to a canceled waiter passes on.
- **(a) Scheduled time slice** (default 3,000 ms, `GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS`, 0 = off):
  - Applies to scheduled Queries and Reducers at depth 0. It is counted from isolate dispatch, not arrival, so queueing behind users does not eat it.
  - The error is `budget_exceeded`: "scheduled reducer grantProjection.reconcile exceeded its 3000 ms time slice for scheduled work (GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS). A scheduled run holds the tenant writer and the module isolate while it runs: process a bounded batch per run and schedule the rest with ctx.scheduler.runAfter".
  - The scheduler logs it and finishes the job, as for any failed run.
- **(c) Scheduled isolate** (default **off**, `GONVEX_MODULE_HOST_SCHEDULED_ISOLATE=true`):
  - Scheduled data calls at depth 0 run in their own one-isolate pool from the same bundle.
  - It starts on demand and is released after `GONVEX_MODULE_HOST_ON_DEMAND_IDLE_MS` (60 s) idle.

All three knobs are read by the in-process host (`runtime/src/in_process.rs`) and by the separate module host (`module-host/src/main.rs`), through `V8Config::with_scheduling_env`.

Measurement setup: the `bridge_bench_contention` micro-driver (ignored test).

- **Crons:** `BRIDGE_BENCH_CRONS` loops run `grantProjection.reconcile` back to back on the scheduler channel, each on its own copy of the tenant.
- **Users:** `tasks.update` arrives open-loop every 50 ms (20/s) on another copy, with 10 s deadlines like production.
- **Duration:** 20 s, 1 CPU.
- **Isolation:** the user calls never wait for the cron's writer, only for the isolate.
- **Metric:** *wait* = invoke start → first host call, i.e. isolate admission plus the JavaScript before the first `ctx.db` call (well under 1 ms).
- **Failures:** about 64% of the user calls fail with "Task not found" (tasks outside the admin's spaces, same as in the step log). Those are counted apart from `budget_exceeded`.
- **"off" baseline:** this branch's binary with all three knobs off, which is the base behaviour (FIFO, no slice, shared isolate).

User wait (ms), `tasks.update`:

| app (artifact) | cron loops | guard | wait p50 | wait p99 | wait max | user `budget_exceeded` | cron runs / errors | cron writer hold p50 / max |
|---|---:|---|---:|---:|---:|---:|---|---|
| before (4b99197ec80b) | 1 | off | 3,403 | 6,510 | 6,694 | 0 | 4 / 0 | 6,375 / 6,694 |
| before | 1 | priority | 3,511 | 6,710 | 6,905 | 0 | 3 / 0 | 6,712 / 6,931 |
| before | 1 | priority + slice 3 s | 1,970 | 3,680 | 3,751 | 0 | 6 / 6 (slice) | 3,507 / 3,567 |
| before | 1 | + scheduled isolate | 0.26 | 4.6 | 8.5 | 0 | 6 / 6 (slice) | 3,494 / 3,583 |
| before | 2 | off | — | ≥ 10,000 | — | **398 / 398** | 5 / 4 (isolate wait) | 0 / 8,196 |
| before | 2 | priority, no slice | 8,684 | 9,343 | 9,343 | 368 | 5 / 4 | 0 / 9,350 |
| before | 2 | priority + slice 3 s | 2,348 | 4,437 | 4,473 | 0 | 6 / 6 (slice) | 4,110 / 4,253 |
| before | 2 | + scheduled isolate | 0.4 | 4.9 | 12.4 | 0 | 6 / 6 (slice) | 4,156 / 4,566 |
| chunked b=100 (ccacddb37617) | 1 | off | 225 | 506 | 566 | 0 | 47 / 0 | 401 / 552 |
| chunked b=100 | 1 | priority + slice | 224 | 514 | 571 | 0 | 44 / 0 | 424 / 578 |
| chunked b=100 | 1 | + scheduled isolate | 0.5 | 4.8 | 20.9 | 0 | 43 / 0 | 463 / 674 |
| chunked b=100 | 2 | off | 698 | 1,228 | 1,349 | 0 | 44 / 0 | 415 / 675 |
| chunked b=100 | 2 | priority | 230 | 546 | 591 | 0 | 44 / 0 | 430 / 652 |
| chunked b=100 | 2 | priority + slice | 213 | 522 | 581 | 0 | 47 / 0 | 406 / 589 |
| chunked b=100 | 2 | + scheduled isolate | 0.5 | 3.9 | 10.1 | 0 | 43 / 0 | 469 / 587 |
| chunked b=200 + SDK fix (dff282988471) | 1 | priority + slice | 26.6 | 66.6 | 122 | 0 | 335 / 0 | 51.5 / 135 |
| chunked b=200 + SDK fix | 2 | off | 90.5 | 172 | 245 | 0 | 336 / 0 | 51.0 / 129 |
| chunked b=200 + SDK fix | 2 | priority + slice | 25.8 | 74.9 | 121 | 0 | 327 / 0 | 52.4 / 136 |

The cron loops run back to back, which is a worst case.
In production the steps of a chain are separated by the scheduler's claim latency and `GONVEX_SCHEDULER_YIELD_MS`.
Each loop is one tenant's chain, and the scheduler runs at most `GONVEX_SCHEDULER_CONCURRENCY = 2` scheduled jobs at once.

Reading:

- **What fixes the problem:** the app and SDK changes. The writer hold per scheduled run goes 6.5 s → 0.47 s (published SDK) → 0.06 s (fixed SDK), and user isolate wait p99 goes 6.5 s → 0.5 s → 0.07 s.
- **Priority:** it matters when scheduled runs queue. With two chains, user p99 is 1,228 → 546 ms on the chunked app, and 172 → 75 ms with the SDK fix. It cannot help against a run that already holds the isolate (one loop: no change), because the isolate is not preemptive.
- **The slice:** it bounds any remaining long scheduled run. User p99 is 6.5 s → 3.7 s with one loop. With two loops, the base behaviour fails every user call (398/398), which is the production symptom; with the slice none fail. The run's writer hold is slice + the SQL statement in flight (3.5–4.5 s here), because SQLite statements are not interrupted.
- **The scheduled isolate:** it removes isolate waiting behind scheduled work entirely (user p99 4–5 ms in every case). Its cost is one more isolate while scheduled work runs: a reducer-bundle isolate heap is ≈ 25 MB (perf/v8-memory) plus its thread, and an isolate start each time it was idle for 60 s. Not measured here: the start cost on 1 vCPU, and the RSS delta in a long run. The bench RSS figures are single end-of-run samples and too noisy to attribute. It stays off by default because memory is the binding 5k constraint. Turn it on where there is headroom.
- **Same-tenant waits are not covered by the guard:** a user Reducer on the tenant the cron is writing waits for the writer, not the isolate. Only the app/SDK fix shortens that wait. The slice bounds it to about slice + one statement.

## Tests

- gonvex SDK: `cd packages/module-sdk && pnpm test` → 56 pass. New: `test/data-write-sqlite.test.mjs` (4 tests).
- gonvex Rust: `cargo test -p gonvex-module-runtime-v8 -p gonvex-module-host` → 51 + 15 pass. New:
  - `admission::tests`: priority order, FIFO when off, aging, and cancellation that never strands capacity.
  - `scheduling_tests`: the slice error and its text; user calls not sliced; zero slice falls back to the ordinary ceiling; queued user before outbox before cron, FIFO when off; a scheduled isolate lets a user call run while a cron holds its isolate, and with a shared isolate it waits.
- `cargo test -p gonvex-runtime` → 371 lib + all integration targets pass.
- `cargo clippy -p gonvex-module-runtime-v8 --tests -- -W clippy::redundant_clone`: no new warnings.
- Whagons:
  - `vitest run --config vitest.gonvex.config.ts gonvex/module/grantProjection.test.ts` → 8 pass, including the equivalence test. Also `findingTeamNotifications`, `voiceTaskDraftsAtomic` (notifyIfUnassigned).
  - `tsc -p tsconfig.gonvex-v2.json`, `pnpm gonvex:functions:check` and `pnpm arch:check` pass.
  - `pnpm test:gonvex:host-call-loop-audit` reports three "new" loops in `gonvex/testing/*` (not touched here), which is pre-existing on the base.

## Reproduce

```sh
# bench binary: cargo test --release -p gonvex-runtime --lib --no-run
BRIDGE_BENCH_MODE=in-process BRIDGE_BENCH_MANIFEST=<manifest.json> BRIDGE_BENCH_DB=<tenant.db> \
  BRIDGE_BENCH_ONLY=grantProjection.reconcile,tasks.notifyIfUnassigned BRIDGE_BENCH_CALLS=20 BRIDGE_BENCH_WARMUP=2 \
  taskset -c 5 <gonvex_runtime test binary> bridge_bench_whagons --ignored --nocapture
GONVEX_MODULE_HOST_ISOLATE_PRIORITY=1 GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS=3000 GONVEX_MODULE_HOST_SCHEDULED_ISOLATE=0 \
  BRIDGE_BENCH_CRONS=2 BRIDGE_BENCH_MANIFEST=... BRIDGE_BENCH_DB=... \
  taskset -c 5 <binary> bridge_bench_contention --ignored --nocapture
```

Results: `/home/gabriel/work/gonvex-perf/cw-results/bridge-*.json`. Manifests: `cw-results/manifest-*.json`.

## Risks

- **Slice default (3 s):** a scheduled Reducer that legitimately needs longer now fails with the slice error. Before, it had the full 10 s, counted from when the run started. In the zero-user lab (`scheduled-load.md`), the cron p99 was 1.3 s and the max V8 times were queueing. Raise or disable the slice per deployment with `GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS`.
- **Priority:** under sustained user saturation, scheduled work waits up to its 5 s lane delay longer than before. Outbox Actions run in their own pool family and are not delayed by it, but the Reducers they call are, by up to 1 s. Scheduled runs still have the 10 s invocation deadline from their start, so a run can fail with "waiting for a module isolate" sooner than before when users saturate the isolate.
- **SDK statement shape:** it changes for every `updateDataRows` caller. The equivalence test covers TEXT and INTEGER keys, JSON and plain columns, NULLs, missing rows and path-like IDs. The SDK must be republished, and Whagons rebuilt against it, before the 62 ms step applies in deployments.
- **Reconcile chain:** a tenant with more than 3,200 stale members needs several hourly runs. A chain step that fails, for example on the slice, ends that chain until the next hour, so a batch-100 step must stay well under the slice.
- **Not done:** interrupting SQLite (`sqlite3_interrupt`) at the deadline, so that a slice also bounds the statement in flight.
