# Action depth, the Action cap and the outbox drainer

Follow-up to "Actions at :00" in `scheduled-load.md`, for the staging line
(`deploy/sqlite-staging` + `perf/scheduled-load`, in-process V8 by default).
Branch `perf/action-depth`.

## Problem

At :00 with 150 users the module host went from 395 to 788 MiB. The 2 GiB cgroup
hit its limit, direct reclaim took the vCPU, and one `outbox` poll held the only
tokio worker for 33.7 s. Three causes:

1. **Durable hops counted as nesting.** Each nesting depth has its own isolate
   pools (9 pools for depths 0–8). Durable work added one to the depth: cron runs
   ran at depth 1, cron-started Actions and `runAfter` jobs at 2, their tool
   Reducers at 3, and so on. Every depth that was ever used kept a full-bundle
   isolate alive (≈ 80–105 MB each). A self-rescheduling `processQueue` chain
   failed at depth 8.
2. **No cap on Actions.** The outbox drain spawned one task per committing
   Reducer, plus the 30-s sweep, all on the network worker. The scheduler's lane
   did not cover the outbox.
3. **Actions shared or multiplied full isolates.** On an unsplit artifact, an
   Action that awaits `fetch` held the warm isolate that user Reducers need.

With in-process V8 all of this sits in runtime RSS, so an OOM kills the runtime.

## Changes

### 1. Durable hops start at depth 0 (`module-runtime`, `scheduler.rs`, `execution.rs`)

`InvocationProvenance.durable_hops` (serde `durableHops`, omitted when 0, so the
stored shape of ordinary calls does not change) counts scheduled-job and
outbox-Action hops since the chain's root.
`InvocationProvenance::durable_hop(max)` adds one hop, sets `depth = 0`, and fails
once `max` hops have been used. `max` is `MAX_DURABLE_HOPS` = 32, overridable
with `GONVEX_MAX_DURABLE_HOPS`.

* Scheduler run (`execute_job`): a run past the bound fails. Its job completes
  with an error log and is not retried, which was already the case for every
  failed scheduled run.
* Outbox delivery: a row past the bound is **deleted** with an error log.
  Before, a depth-exceeded Action was retried with backoff forever.
* Synchronous nesting (Action tools, `ctx.functions.invoke`) keeps `depth` as
  before and is still bounded by `MAX_INVOCATION_DEPTH` (8). It inherits the
  parent's hop count.
* Rows written before this change carry only `depth`. They read as 0 hops, and
  their first hop resets the depth.

### 2. Isolate pool selection (`module-runtime-v8`)

* Only Actions start nested calls. A Query or Reducer in a pool family that never
  serves Actions has no ancestor holding an isolate of that family, so it always
  uses slot 0. An Action's tool Reducers now share the warm isolate with user
  calls instead of getting a warm isolate per depth. Families that serve Actions
  are still indexed by depth: a nested Action never waits on its parent's isolate.
* Unsplit artifacts now run Actions in **on-demand complete-bundle isolates**,
  released after `on_demand_idle_ttl` (60 s), the same way split artifacts
  already did. Queries and Reducers keep the warm isolate, so an Action awaiting
  `fetch` no longer blocks user Reducers. `GONVEX_MODULE_HOST_ON_DEMAND_ACTIONS=0`
  restores the old sharing. The in-process bridge now also reads
  `GONVEX_MODULE_HOST_ON_DEMAND_IDLE_MS`.
* `gonvex_module_runtime_v8::live_isolates()` returns `(all, complete-bundle)`
  isolate threads in the process.

The Whagons lab artifact is **unsplit**. On it, Action memory at :00 is the warm
isolate plus one Action isolate.

### 3. One Action at a time (`runtime/src/action_gate.rs`)

`ActionGate` is a process-wide permit for **root** Actions. The capacity is
`GONVEX_ACTION_CONCURRENCY`, defaulting to 1 when `available_parallelism() <= 2`
(the cgroup quota counts) and 2 otherwise.

* Priority: direct user Actions first, then outbox deliveries, then
  scheduler-started Actions (`cron_profile::active()`). Order is FIFO within a
  class.
* The gate is cancel-safe: a permit handed to a dropped waiter passes on to the
  next waiter.
* Nested Actions (depth > 0) run inside their root's permit, so a nested Action
  cannot deadlock against the cap.
* The permit is held for the whole invocation, including `fetch`. On 1 vCPU,
  memory binds, not CPU.
* For anonymous Actions, waiting for the permit counts against their deadline.
  It does not count as V8 time in the cron profile.

### 4. One outbox drainer (`execution.rs`, `scheduler.rs`)

* Commits that wrote the outbox, and the 30-s sweep, only queue the tenant
  (`OutboxQueue`, deduplicated by database) and wake **one** drainer task. The
  drainer runs on the `gonvex-scheduled` lane thread (nice +5), never on the
  network worker. It holds only a `Weak` runtime while idle.
* One tenant turn:
  1. Run a read-only due probe (`ControlPlane::outbox_due`), so an idle tenant
     costs no writer claim.
  2. Promote due `runAfter`/`runAt` rows without a permit
     (`claim_outbox(Path(SCHEDULE_OUTBOX_PATH))`). They are cheap, and they must
     not wait behind a slow Action.
  3. Take the Action permit, and only then claim **one** Action
     (`claim_outbox(NotPath(..))`).
  4. Deliver the Action with the permit passed in through `ExecutionAccess`.
  5. If the tenant may have more, put it at the back of the queue.

  The backlog therefore stays in SQLite, and a claimed row never waits for a
  permit while its 5-minute lease runs. Tenants rotate.
* `/dev/profile` has a new `actions` section: capacity, and running / waiting /
  admitted per class, queued tenants and live isolates. There are new gauges
  `action.running` and `action.waiting`.

### 5. Two existing bugs found by the end-to-end tests

* `actions.enqueue` as a Reducer's first write failed with "the invocation
  transaction is closed". It used `transaction()` instead of opening the lazy
  writer (`ensure_transaction()`). The bug is on staging today: any Reducer whose
  first database call is `ctx.actions.enqueue` throws.
* Jobs queued by system work (a cron run's `runAfter`) store the actor
  `_gonvex_system`. The scheduler then tried to resolve that as an account, which
  fails with `InvalidSession`, so **cron-started `runAfter` chains failed at the
  first hop**. It now uses the system session for that actor. It was not checked
  whether staging's `processQueue` chain ever ran past its first hop. These logs
  were not inspected.

## Measurements (arch3, 2026-11-03)

Lab: `/home/gabriel/work/gonvex-perf/lab-ad`. It is a reflinked copy of the
26-tenant scheduled-load lab data (unsplit Whagons artifact, 14 crons per
tenant), with no users, in-process V8 and a 1 vCPU container. Each run starts
from the same pristine data.

The burst (`inject.py`) fires about 150 s after start. Every tenant gets the same
work, due at the same instant:

* 8 outbox rows, queued as by a cron Reducer (provenance depth 1, a real member
  actor of the tenant): `journeySync.requestTaskReconciliation`,
  `roles.getTaskPermissionsForTasks` and `taskVideoProcessing.backfill` (dry run).
* 2 one-shot scheduled jobs running `journeySync.requestTaskReconciliation`.

That is 208 outbox Actions and 52 scheduler-started Actions. Each enters V8 and
calls 1–2 tool Reducers or Queries.

The lab data also holds about 4,200 permanently failing outbox rows
(`pushNotifications.deliverScheduled` and `centerClassification.classify` need
unconfigured secrets). Every variant retries them. They fail before V8.

Binaries: before = `332e57a6` (staging + scheduled-load merge), after = `3963259e`.
Both are `local-load` builds. Windows: 60 s before the burst, and 300 s from the
due time.

| | before (2 GiB) | after (2 GiB) | before (800 MiB) | after (800 MiB) |
| --- | --- | --- | --- | --- |
| V8 isolate threads, idle → burst max | 3 → **4** | 2 → **2** | 3 → 4 | 2 → 2 |
| runtime RSS, burst start / max (MiB) | 664 / 750 | 614 / **658** | 660 / 794 | 608 / 654 |
| cgroup memory max in burst (MiB) | 843 | 966¹ | **800 (at limit)** | 647 |
| anon max in burst (MiB) | 560 | 477 | 619 | 477 |
| container CPU-s, burst window | 19.0 | 15.4 | 19.6 | 20.0 |
| network-thread on-CPU samples (burst +25..+155 s, 499 Hz) | 1,176 | **488** | — | — |
| `gonvex-scheduled` on-CPU samples | 381 | 719 | — | — |
| long polls > 5 ms / watchdog stalls | 0 / 0 | 0 / 0 | 0 / 0 | 3 (`outbox`, max 8 ms, lane thread) / 0 |
| `tokio.sched_lag` max | 201 ms² | 201 ms² | 182 ms² | 90 ms² |
| tool Reducer calls within 30 s of the due time | 184 (one sweep) | 284 | 184 | — |

¹ Page cache (file pages). Anon was lower than before.
² Cumulative max since start, not per window. All variants had one startup-era
maximum near 200 ms.

* **Isolates.** Before, the lab sits at 3 isolates with no users: depth 0 warm,
  depth 1 for cron runs, depth 2 for cron-started Actions' tools. The burst adds
  depth 3 (the outbox Actions' tools). After, there are 2 isolates: the warm one,
  and the on-demand Action isolate, which never closes because the failing
  retries keep it busy. Runtime RSS is about 90 MiB lower at the burst peak, and
  about 50 MiB lower at idle.
* **800 MiB.** Before sits at the cgroup limit through the burst. After keeps
  about 150 MiB of headroom.
* **Network thread.** The outbox work moved off `tokio-rt-worker`: −58% of its
  on-CPU samples in the burst window, with the work now on `gonvex-scheduled`.
  Total sampled CPU in that window was 7,093 → 5,307 samples. The 3 long polls
  after the change are drainer polls on the lane thread, not the network worker.
* **Throughput.** The burst's Actions are short: 1–5 ms of V8 time each, and
  `fn.total action` p50 is in milliseconds in both variants. With the cap of 1,
  the after variant ran the burst's first attempts within 20 s of the due time.
  It also went through about 5,900 permit admissions in 15 min, most of them the
  failing retry rows. Before ran them in the next 30-s sweep. Neither variant
  built up a backlog. **Not measured:** long Actions (seconds of `fetch`) under
  the cap. That is where the cap costs latency.
* **Loop stop.** The end-to-end test `a_self_rescheduling_reducer_runs_past_depth_eight_and_stops_at_the_hop_bound`
  shows that a `runAfter(0, self)` chain now runs 1 + 32 times and stops. Before,
  it would have died at depth 8, or, started from a cron, at its first hop (the
  system-actor bug above).

## The 33.7 s `outbox` poll

The poll was **not reproduced**. Neither the 2 GiB nor the 800 MiB lab (no users)
produced a long poll on the network worker in either variant. At 800 MiB the
before variant sat at the cgroup limit but kept about 168 MiB of page cache, so
it reclaimed cache without thrashing.

The sched tracepoints (`perf record -e sched:sched_switch …`) did not work on
arch3 this session. Perf wrote files it could not read back ("broken or missing
trace data"), even for `perf record … -- sleep 0.2`. Only on-CPU profiles were
taken.

Most likely cause, from code and the step-log evidence:

* The old drain ran the **whole delivery inside one task on the network worker**.
  That covers the module-host invoke, every host call of the Action, and every
  tool Reducer the Action calls, with their frame encoding and decoding.
* At 14:02 the cgroup was at `memory.max`, with `memory.events max` climbing to
  24k, page cache evicted down to 15 MiB, and 26% of samples in direct-reclaim
  spinlocks.
* With the page cache that low, the runtime's own text pages and the SQLite pages
  are refaulted. Any poll that touches a lot of code or allocates then stalls in
  reclaim. The outbox poll, the largest code path on that thread, is the one that
  shows it.

This is a page-fault/reclaim stall in whatever ran on the only worker, not a
33-s synchronous loop. Nothing in the drain path does seconds of synchronous CPU
work. The largest synchronous pieces are JSON encode/decode of host-call frames
and tool results.

The fix does not depend on which of the two it was. The drain no longer runs on
the network worker, and the memory that caused the reclaim (extra depth isolates,
unbounded concurrent Actions) is capped.

## Tests

* `cargo test -p gonvex-runtime -p gonvex-store -p gonvex-module-runtime -p gonvex-module-runtime-v8 -p gonvex-module-host`
  (from `rust/`): 500 passed, 0 failed, 7 ignored. The runtime lib alone: 351
  passed, 6 ignored.
* New tests:
  * `module-runtime`: `durable_hops_reset_depth_count_hops_and_stop_at_the_bound`,
    `stored_provenance_keeps_its_shape_without_hops_and_reads_old_rows`.
  * `module-runtime-v8`: `nested_reducers_reuse_the_warm_isolate_instead_of_a_pool_per_depth`,
    `unsplit_engines_run_actions_in_on_demand_isolates`.
  * `runtime` `action_gate`: priority order with FIFO per class, a cancelled
    waiter does not lose the permit, capacity 2.
  * `runtime` `tests::action_depth`, end to end with in-process V8:
    * outbox Actions never overlap, at most one row is `processing` while the
      rest wait as `pending` rows, and at most 2 isolates are live;
    * a waiting user Action runs before queued outbox work;
    * the `runAfter` loop stops at the hop bound.

## Risks

* **Action latency under the cap.** Every root Action in the process runs one at
  a time on 1 vCPU. A long Action (an agent turn, a slow `fetch`, up to the
  15-min Action timeout) delays every other Action, including user Actions, which
  only get priority in the queue.
  * Agent-profile Actions have no deadline, so a user Action can wait
    indefinitely behind one.
  * Mitigation: `GONVEX_ACTION_CONCURRENCY=2` trades about one more Action
    isolate (on-demand, about 80 MB) for less waiting.
* **Cron-started Actions** wait for the permit while holding a scheduler lane
  permit (cap 2) and their tenant's lane lock. Behind a long Action, cron
  Reducers can be delayed too. Job leases are renewed while waiting, so no work
  is lost.
* **First Action after 60 s idle** starts an isolate (≈ 360 ms for the Whagons
  bundle, `v8-memory.md`). Before this change, Actions on an unsplit artifact
  were warm.
* **Hop bound semantics.** At 32 hops, a fan-out chain (each run scheduling 2)
  is much less bounded than the old depth 8. Linear loops stop. An outbox Action
  past the bound is deleted, not retried.
* **Permanently failing outbox rows.** They now each go through the permit
  (about 5,900 admissions in 15 min in the lab) and the drainer. Every turn adds
  a read-only probe per tenant. It is cheap, but it is still per-row work for
  rows that fail before V8.
* **Behaviour change for direct `ctx.actions.enqueue`-first Reducers** (bug
  fix): those calls used to fail and now succeed.

## Not measured

* A loaded :00 (users plus crons at :00 with real external `fetch`). Only an
  idle burst of short Actions was measured.
* The out-of-process module host. All runs used in-process V8.
* Split artifacts in the lab. They are covered by the unit tests only.
* Off-CPU stacks: the sched tracepoints were unusable this session.
* User-Action latency while outbox work is running, beyond the ordering test.
