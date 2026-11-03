# Scheduled (zero-user) load

What the SQLite runtime does when nobody is connected, why it came in herds, and
what changed. Workstream 11 of the 5k-user program (branch `perf/scheduled-load`).

## What runs with nobody connected

The Whagons module declares 14 `tenantCron`s. In the 26-tenant lab each runs once
per tenant:

| Period | Crons | Runs per hour (26 tenants) |
| --- | --- | --- |
| every minute | `agents.dispatch` (interval), `analyticsDelivery.processQueue` (`* * * * *`), `deepResearch.tick` (interval), `journeySync.processDue` (interval, an Action) | 6,240 |
| every 15 min | `boards.generateBirthdayMessages`, `centerClassification.queue` (`*/15`), `billing.synchronize` (interval, an Action) | 312 |
| hourly | `cleaning.tick`, `slaExecution.evaluate`, `workflowRuntime.resumeDueDelays` (`0 * * * *`), `grantProjection.reconcile` (`7 * * * *`) | 104 |
| 6 h / daily | `workplans.reconcileSchedules`, `taskRecurrences.reconcileRecurringSchedules` (`0 */6`), `tools.checkOverdueItems` (`0 8 * * *`) | — |

Runtime housekeeping on top: the scheduler's claim loop (wakes at least every 30 s),
the Action-outbox sweep over every tenant every 30 s, completed-job pruning every
10 min, and the membership projector, which scans every tenant's membership outbox
at most every 10 s and on every Reducer commit.

Before this branch, `scheduler.rs` had no jitter. Five-field crons fired for all
tenants at the same instant, and interval crons were anchored to process start
(`next = now + interval` at cron sync), so they also lined up across tenants. The
claim loop spawned every claimed job at once (batches of 16, looping while full),
so a herd of 26–180 V8 invocations hit the module host (one isolate) together.

## Instrumentation

* Profiler label `cron` for everything the scheduler runs (task-local, see
  `runtime/src/cron_profile.rs`); `module_host` uses it for `invoke.*` too.
* Per-cron table in `/dev/profile` (`crons`), keyed by a slot assigned once per
  cron name at module install (`gonvex_profiler::cron`): invocations, errors,
  skipped, total and max time, V8 time (outermost module-host request), writer
  hold and max, host calls, rows read, change rows written, revisions, fan-out
  (feed subscribers at publish time) and start delay. Relaxed atomics only; see
  docs/performance/profiler.md.
* `bench/profiler/crontable.py PROFILE [EARLIER]` prints it.

## Lab and method

`/home/gabriel/work/gonvex-perf/lab-idle*`: reflinked copy of the prepared
26-tenant lab (`gonvex-sqlite-lab`), runtime + module host in a 1 vCPU / 2 GB
container (`gvperf-idle-*`, its own compose project, volume and Unix-socket proxy),
local-load build of this branch. No users, no generator. `collect.py` records the
container cgroup `cpu.stat` and memory every second, process RSS every 5 s and
`/dev/profile` every 10 s; `analyze.py`, `crontable.py`, `stages.py` reduce them.
Every run starts from the same pristine data copy. arch3 was running other agents'
builds throughout (load average ≈ 16 on 14 cores); container CPU is cgroup CPU
time, so it is not inflated by host contention, but latencies can be.

Variants:

* **baseline**: lab base + profiler + cron instrumentation (commit 67c531fe).
* **A**: fixes in the runtime only (stagger, lane, wake gating, finalize fast path;
  commit 389ae99d), unchanged module.
* **B**: A plus the `dueWhen` pre-checks of the Whagons `perf/cron-due` branch,
  applied to the lab's installed manifest (`lab-idle/patch-duewhen.py`, which
  recomputes the canonical artifact hash).
* **C**: head (5eb8137b) without `dueWhen`, 30 min, for the per-call prepare cost.

## Results (2026-11-03, arch3, 0 users, 26 tenants, 1 vCPU / 2 GB)

Windows: baseline 12:35–13:42 (67 min, then extended to 14:18 to include a second
:00), A and B 13:07–14:14 (67 min), C 13:16–13:41 (25 min). The first 5 min of
each run are skipped. perf (`-g --call-graph dwarf`, on-CPU and context-switch)
was attached to all three labs at 13:59:50–14:01:14, so the 14:00 window is
perturbed equally in every variant. One host-wide stall (tokio.sched_lag ≈ 5 s
at the same wall time in independent containers, 13:37:2x) shows up in
baseline, A and C; it is host contention, not the scheduler.

### Idle CPU, spikes, throttling, stalls (67-min windows)

| | baseline | A (runtime fixes) | B (A + dueWhen) |
| --- | --- | --- | --- |
| mean cores | 0.052 | 0.053 | **0.047** |
| median / p99 cores (1-s samples) | 0.036 / 0.49 | 0.042 / 0.35 | 0.038 / **0.30** |
| container CPU-seconds | 209.6 | 212.3 | **187.3** (−11%) |
| cgroup throttled time | 42.4 s | 2.5 s | **0.1 s** |
| seconds ≥ 0.25 cores (spikes) | 94 | 36 | 35 |
| hottest second-of-minute | :31 = 0.47 cores, :00 = 0.25 | :42 = 0.08 | :42 = 0.07 |
| per-minute CPU-s median / p90 / max | 2.66 / 4.95 / 8.12 | 2.82 / 4.74 / 9.16 | 2.71 / 3.42 / 5.51 |
| watchdog stalls / long polls (> 5 ms) | 194 / 255 | 75 / 77 | **48 / 50** |
| tokio.sched_lag probes late ≥ 5 / 10 / 25 / 50 ms | 573 / 332 / 226 / 105 | 398 / 179 / 80 / 49 | 364 / 130 / 57 / 32 |

The baseline's hottest second is :31, not :00: the four per-minute interval crons
were anchored to process start (12:29:31), so 104 V8 invocations landed in the
same second every minute. The stagger removes that herd. Total CPU barely moves
(cron work is spread, not removed); what disappears is the queueing:
throttling falls from 42 s to 2.5 s, and stalls and long polls drop by
roughly 60–70%. `dueWhen` (B) then removes most of the work itself.

### :00, :15 and :30 windows (container CPU, 1-s samples)

| window | baseline | A | B |
| --- | --- | --- | --- |
| 13:00:00 +60 s (no perf in A/B; they started 13:03) | 6.35 CPU-s, peak 0.91 cores, 3 s ≥ 0.5 | — | — |
| 14:00:00 +60 s (perf attached in all) | 6.33 CPU-s, **peak 0.97**, 5 s ≥ 0.5 | 4.10 CPU-s, peak 0.31, 0 s ≥ 0.5 | 3.35 CPU-s, **peak 0.15**, 0 s ≥ 0.5 |
| 14:00 → 14:10 (stagger window) | 38.1 CPU-s, 18 s ≥ 0.5 cores | 38.4 CPU-s, 7 s ≥ 0.5 | 30.2 CPU-s, 2 s ≥ 0.5 |
| 13:30:00 +60 s | 6.51 CPU-s, peak 0.99 | 3.54, peak 0.11 | 3.05, peak 0.08 |
| 13:45:00 +60 s | 4.18 CPU-s, peak 0.56 | 2.74, peak 0.09 | 2.52, peak 0.07 |

`:00` window 13:59:50–14:03:30 from `/dev/profile` (`lab-idle/window.sh`):

| | baseline | A | B |
| --- | --- | --- | --- |
| cron runs (V8 entered) | 468 (468) | 434 (434) | 434 (**23**) |
| cron wall time / V8 time / writer hold | 397 s / 368 s / 26.9 s | 129 s / 88.5 s / 65.0 s¹ | 39.8 s / 1.6 s / 1.1 s |
| fn.total (cron) p50 / p99 | 464 ms / 5.5 s | 4.0 ms / 2.7 s | 16.8 ms / 1.3 s |
| watchdog stalls / long polls | 79 / 166 | 44 / 45 | 22 / 24 |
| sched_lag late ≥ 5 / 50 ms | 246 / 37 | 213 / 28 | 168 / 14 |
| start delay mean / max | 3.4 s / 10.8 s (herd queueing) | 5.6 s / 37 s (stagger + host stall¹) | 4.5 s / 34 s |

¹ A's writer-hold total includes two runs (36 s and 16.6 s) that straddled the
perf/host stall at 14:01:08–14:01:17, which hit all three containers at the same
wall time. Without them A holds the writer for ≈ 12 s.

On-CPU thread shares, 13:59:50–14:01:14 (`perf-1400/oncpu.svg` in each run dir):

| thread | baseline | A | B |
| --- | --- | --- | --- |
| sqlx-sqlite worker | 45.8% | 24.3% | 39.6% |
| tokio-rt-worker (sockets, timers) | 34.1% | 36.8% | 38.3% |
| gonvex-scheduled (new lane) | — | 20.0% | 12.2% |
| module host isolate + V8 workers | 16.9% | 11.7% | **0.5%** |
| profiler | 2.8% | 6.6% | 8.9% |

In the baseline :00 profile, 5.4% of samples are V8 *parsing* (lazy function
compilation in the isolate as 26 tenants × 9 crons arrive at once); it is gone
in A and B. Runtime user frames mostly do not unwind past libc in these dwarf
captures (kernel symbols are hidden by kptr_restrict), so the thread table is
the reliable part. Flamegraphs: `lab-idle/runs/baseline/perf/oncpu.svg` (13:00),
`lab-idle/runs/baseline/perf-1400/oncpu.svg`, `lab-idle-a/runs/after/perf-1400/oncpu.svg`,
`lab-idle-b/runs/after/perf-1400/oncpu.svg`.

### Per-cron table (67 min, 26 tenants)

Totals in seconds (runs × mean). "V8" is the outermost module-host request, so in
the baseline it includes queueing behind the herd on the single isolate.

| cron | runs | baseline total / V8 / hold | A total / V8 / hold | B total / V8 / hold (skipped) |
| --- | --- | --- | --- | --- |
| agents.dispatch (1 min) | 1,742 | 401 / 358 / 23.3 | 129 / 77.7 / 61.8¹ | 17.6 / 0 / 0 (all) |
| analyticsDelivery (1 min) | 1,742 | 98.9 / 75.1 / 16.7 | 27.7 / 10.3 / 20.4 | 5.6 / 0 / 0 (all) |
| deepResearch.tick (1 min) | 1,742 | 393 / 336 / 24.7 | 33.1 / 21.1 / 28.2¹ | 14.6 / 0 / 0 (all) |
| journeySync (1 min) | 1,742 | 475 / 418 / 19.0 | 32.4 / 30.0 / 16.0 | 7.0 / 0 / 0 (all) |
| billing.synchronize (15 min) | 104–112 | 18.7 / 17.1 / 0.7 | 6.3 / 5.9 / 1.0 | 3.2 / 2.3 / 1.9 |
| boards.birthdayMessages (15 min) | 104–112 | 28.6 / 23.2 / 4.8 | 1.0 / 0.1 / 0.8 | 3.8 / 1.4 / 1.8 |
| centerClassification (15 min) | 104–115 | 32.8 / 24.6 / 8.7 | 5.4 / 2.1 / 5.1 | 3.9 / 0.7 / 3.6 |
| cleaning.tick (hourly) | 26–30 | 2.7 / 1.9 / 0.9 | 0.3 / 0.1 / 0.3 | 0.04 / 0 / 0 (all) |
| slaExecution (hourly) | 26–31 | 3.0 / 2.1 / 0.8 | 2.2 / 1.6 / 0.9 | 0.7 / 0 / 0 (all) |
| workflowRuntime.resumeDueDelays (hourly) | 26–32 | 3.9 / 2.8 / 1.1 | 0.5 / 0.1 / 0.4 | 0.06 / 0 / 0 (all) |
| grantProjection.reconcile (hourly :07) | 26–44 | 11.1 / 10.4 / 1.4 | 1.6 / 0.6 / 1.5 | 1.6 / 0.5 / 1.5 |
| **total** | 7,384–7,444 | **1,469 / 1,270 / 102** | **240 / 150 / 136**¹ | **57.9 / 4.9 / 8.9** (7,061 of 7,444) |

Rows written, revisions and fan-out: every cron except `grantProjection.reconcile`
wrote 0 rows and published 0 revisions in all variants (fan-out is 0 with no
users). The reconcile wrote 1,160–1,184 member rows (≈ 45 per tenant, one
revision per tenant) in every variant. That is a one-time catch-up: each lab
starts from the same pristine copy, whose seeded members never had their role
grants projected (`{"e2e": true}` → `{"__fromRoles": [...], ...}`). The next
hourly run wrote nothing (checked in a tenant's change feed). `rows read` counts
host-call rows only; reconcile reads ≈ 60–67 rows per tenant here.

6-hourly and daily crons (`workplans`, `taskRecurrences`, `tools.checkOverdueItems`)
did not fire in any window.

### Memory (end of window)

| | baseline (67 / 105 min) | A | B | C (25 min) |
| --- | --- | --- | --- | --- |
| runtime RSS | 590 / 662 MB | 531 MB | 475 MB | 410 MB |
| module host RSS | 390 / 468 MB | 555 MB | 381 MB | 439 MB |
| cgroup anon | 857 / 989 MB | 1,016 MB | 788 MB | 823 MB |

Memory was still drifting up at the end of every run (V8 heap growth and
SQLite page cache across 26 tenants), so these are not steady-state values. B is
lowest because it barely enters V8. A's higher module-host RSS is within that
drift and was not investigated further.

### Cron :00 window at 200 users

Pending: `integrate/docs/performance/step-log.md` has no such section yet. The
integration agent's `base-cron-150` case (lab-5k, 150 users) was running when
this was written.

## Fixes

### 1. Per-tenant stagger (`scheduler.rs`, `Stagger`)

Each (project, tenant, cron) gets a fixed 64-bit key (SHA-256 prefix).

* Interval crons run at Unix-time phase `key mod interval`: consecutive runs are
  exactly one interval apart, the 26 tenants spread over the whole interval, and a
  restart lands on the same lattice (so no extra run per interval, unlike the old
  `now + interval` anchoring, which also lined every tenant up on process start).
* Five-field crons run `key mod min(gap, spread)` after each nominal occurrence, where
  `gap` is the time to the next occurrence and `spread` is
  `GONVEX_SCHEDULER_CRON_SPREAD_MS` (default 600,000). A run stays inside its own
  period, so the cron still runs at most once per period. **Maximum delay added:
  `min(period, 10 min) - 1 ms`**: up to 59.999 s for `* * * * *`, up to 10 min for
  `*/15`, hourly, 6-hourly and daily crons. A process that starts inside an
  occurrence's window still runs that occurrence.
* Missed runs are still skipped, never replayed. `GONVEX_SCHEDULER_STAGGER=0`
  restores nominal times.

### 2. Low-priority lane (`scheduled_lane.rs`)

* Scheduled runs execute on a dedicated OS thread (`gonvex-scheduled`, a
  current-thread tokio runtime) with nice +5 (`GONVEX_SCHEDULER_NICE`), never on the
  tokio worker that polls sockets (`GONVEX_SCHEDULER_THREAD=0` turns this off).
* At most `GONVEX_SCHEDULER_CONCURRENCY` (default 2) runs at once process-wide, one
  per tenant. The claim loop claims only as many jobs as it has free permits, so a
  claimed job never waits behind the cap while its two-minute lease runs down.
* Users first: a run waits while user Reducers/Queries/Actions are executing
  (a relaxed atomic counter plus a `Notify` when it drops to zero), at most
  `GONVEX_SCHEDULER_YIELD_MS` (default 2,000) per run, so a busy server still makes
  progress. Outbox-drained Actions count as user calls for this purpose.
* The lane decides *when* a run starts; it sits in front of either V8 bridge
  (module-host process today, `in_process.rs` on perf/native-reducers) and in front
  of the tenant writer. With the in-process bridge, the V8 work of scheduled runs
  would also land on the niced thread.
* Risk: under sustained CPU saturation a niced run holding a tenant writer runs
  slower (priority inversion for user writes to that tenant). Nice +5 (not 19) and
  the per-run yield bound keep this small; set `GONVEX_SCHEDULER_NICE=0` to disable.

### 3. `dueWhen` pre-checks (SDK + runtime)

A cron may declare one read-only SQL statement. The runtime evaluates it on a
tenant **reader** (no writer, no V8) with `$1` = now (Unix ms); the run enters V8
only when the first column of the first row is truthy (not NULL, 0, '' or no row).
A failing check (missing table, SQL error) runs the cron (fail open). Skipped runs
are counted (`skipped`) and still complete their job row and schedule the next run.

### 4. No-op commits

The change feed already published nothing for a transaction that changed no tracked
rows, and finalize already drops UPDATEs that changed no column
(docs/sqlite-storage.md, "Change feed"), so no-op cron writes never produced a
revision or fan-out. What a no-op cron commit still did:

* it woke the membership projector, which then scanned **every** tenant's
  membership outbox (26 reader transactions per wake);
* it spawned an Action-outbox drain for its tenant (a writer `UPDATE` claim), which
  in the lab also re-ran due retries of permanently failing Actions;
* finalize ran 4 statements (schema lookup, no-op delete, normalisation, pending check).

Now `CommitOutcome` reports `members_changed` (the revision touched `members`) and
`outbox_written` (an outbox row was inserted in the transaction). The projector is
woken and the drain spawned only then; the projector's periodic pass (≤10 s) and the
30-s outbox sweep remain the safety nets. Finalize remembers per database that the
feed exists and, when nothing is staged (`revision IS NULL` partial index probe),
returns after one statement. A skipped revision cannot hide a change: the probe is
the same predicate the old code checked after normalising, and staged rows (even
from a write that bypassed `TenantTransaction`) are still adopted by the next commit.
The fsync barrier still runs for no-write commits (a read-only Reducer may return
data committed by another transaction whose sync is pending; skipping it would
weaken acknowledgement durability). Test:
`no_op_commits_publish_no_revision_and_report_their_outcome`.

### 5. Per-call function-set copy (found in the :00 on-CPU profile)

Every Reducer call copied all ~2,000 function names of the Whagons module into a
fresh `BTreeSet<String>` (`with_schedulable_functions`) to validate `scheduler.*`
targets. The handler now looks the target up in the leased module's function table.
This is on every user Reducer too (heartbeats included).


## `dueWhen` inventory (Whagons `perf/cron-due`, df1be6e06)

10 of the 14 crons declare a check; 4 have no cheap SQL condition.

| cron | dueWhen (abridged) | skipped in lab B |
| --- | --- | --- |
| agents.dispatch | active agent with `nextRunAt <= now` or a `task_created` trigger; stale queued/running runs; runs older than 90 days | all |
| analyticsDelivery.processQueue | requested or stale queued deliveries; active schedules with `nextSendAt` due or unparsable | all |
| deepResearch.tick | assignment `running` or `paused` | all |
| journeySync.processDue | any `journeySyncConfigurations` row | all |
| cleaning.tick | active reset rule or report schedule that is armed | all |
| slaExecution.evaluate | any `taskSlaStates.nextEventAt` | all |
| workflowRuntime.resumeDueDelays | any `workflowDelays` waiting | all |
| workplans.reconcileSchedules | unarmed, or armed but overdue by > 1 h, recurring items | did not fire |
| taskRecurrences.reconcileRecurringSchedules | same pattern on `taskRecurrences` | did not fire |
| tools.checkOverdueItems | overdue active checkout, or overdue without reminder | did not fire |
| boards.generateBirthdayMessages | none: depends on today's date against member birthdays and per-board settings | — |
| billing.synchronize | none: an Action that reconciles with the billing provider | — |
| centerClassification.queue | none: fingerprints workspaces in JS | — |
| grantProjection.reconcile | none: the diff is computed in JS (see below) | — |

Hourly repair sweeps use "any armed due row" rather than "due now", so a lost
wake is still repaired on the next hour. The checks fail open: a missing table or
SQL error runs the cron. Contract: `dueWhen` must be one literal read-only
`SELECT`/`WITH`/`VALUES` statement (validated by the artifact parser in
`packages/gonvex/src/module-artifact.ts`), `$1` is now in Unix ms, and only the
first column of the first row is read. Older runtimes **reject** a manifest that
carries `dueWhen` (`CronSpec` is `deny_unknown_fields`), so the runtime must be
deployed before a module that declares it; the SDK's older artifact parser drops
the field, so the Whagons commit is inert until the SDK is upgraded.
Generated manifests in the Whagons branch were not regenerated.

## Hourly reconcile jobs: full scans

* `grantProjection.reconcile` reads all members, roles, permissions,
  rolePermissions and memberTeams of a tenant in V8 under the tenant writer, then
  writes only members whose projected grants differ. Here: ≈ 60–67 rows per
  tenant, 35 ms per tenant (34 ms of it holding the writer) after the stagger
  (427 ms in the baseline herd). It scales with tenant size: for a 1,950-member
  tenant expect a few thousand rows and some hundreds of ms of writer hold once
  an hour. Steady state writes nothing. Making it incremental needs a watermark
  (max `updated_at` over the five tables, stored after each run) that a `dueWhen`
  could compare against; the reducers that change roles already call
  `syncMemberGrants` in the same transaction, so the hourly pass is only a safety
  net and could also move to every 6 h. Not changed here (app decision).
* `slaExecution.evaluate`, `cleaning.tick` and `workflowRuntime.resumeDueDelays`
  read ≤ 5 rows in the lab and now skip V8 entirely through `dueWhen` when nothing
  is armed.
* `workplans` / `taskRecurrences` reconcile (6 h) did not fire during the runs;
  their cost was not measured.

## Risks

* Stagger delays five-field crons by up to `min(period, 10 min)`. Daily
  `tools.checkOverdueItems` (08:00) may run at up to 08:09:59.999. Anything
  that needs exact wall-clock firing must set `GONVEX_SCHEDULER_STAGGER=0` (global);
  there is no per-cron opt-out.
* Concurrency cap 2 plus the user-first yield (≤ 2 s per run) means a busy server
  delays crons. Start delay max reached 37 s in A during the host stall; jobs are
  claimed only when a permit is free, so leases do not expire.
* Niced lane holding a tenant writer under CPU saturation (priority inversion),
  see fix 2.
* `dueWhen` correctness is the app's responsibility. A wrong check silently skips
  work until the data changes. Fail-open covers errors, not wrong logic.
* The no-op commit fast path relies on the `revision IS NULL` probe; covered by
  `no_op_commits_publish_no_revision_and_report_their_outcome`.

## Not measured

* The 200-user :00 window (pending, see above).
* Runtime user-space frames in the :00 flamegraphs (dwarf unwinding stopped at
  libc; kernel symbols hidden).
* Memory at steady state (all runs were still drifting).
* 6-hourly and daily crons.
* The lane with the in-process V8 bridge (perf/native-reducers).

## Tests

* `cargo test -p gonvex-runtime -p gonvex-store` (from `rust/`, after merging
  `claude/sqlite-storage-lab` af869fb1): runtime lib 328 passed / 4 ignored, store
  lib 40 passed, every integration suite passed, 0 failed.
* `pnpm test` in `packages/module-sdk` (52 passed) and `packages/gonvex` (67 passed).
* rustfmt 1.94 (CI's toolchain) clean for the runtime, store and profiler crates.
