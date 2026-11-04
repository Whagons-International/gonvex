# "Task update was not delivered before teardown": harness, not delivery loss

Branch `perf/delivery-loss` (from perf/integrate-5k 18d101d6). Lab `lab-loss` (containers `gvloss-*`, a copy of
`lab-5k-new`), runtime `bin-resume-lazy`, Whagons artifact 83669147 (the integration lab's current one), generator on
the MacBook (`app-new`), 200 users, 2 shards, thin clients with 8 SDK users, `LOAD_PRESENCE_V2=1`, batch frames on,
60 s ramp and 180 s plateau.

## Verdict

**No delivery loss was found in the runtime.** The error came from the generator's teardown check in
`bench/whagons/run.ts`. Four defects in that check turned correct deliveries into errors. Each false expectation was
counted once **per observer**, meaning every user of the tenant whose replica holds the task, so a handful of bad
expectations became hundreds of errors:

1. **Comparing two hosts' clocks (the main cause).** The `recordTaskViewByTaskId` expectation was
   `row.firstViewedAt >= at`. Here `firstViewedAt` is the runtime's `ctx.now` (arch3 clock) and `at` is the
   generator's `Date.now()` (Mac clock). The clocks disagree by tens of milliseconds, and the sign changes between
   runs. When the Mac was ahead, every first view failed the check.
2. **Failed saves kept their expectations.** `timed()` swallows the error, so a save that failed with
   `budget_exceeded` still waited for a change that was never committed. Expectations were also set before the save,
   so the failure could not remove them.
3. **No delivery grace.** Each user checked as soon as its own loop ended. Edits other users made in the last seconds
   of the plateau were still in flight. Under a backlog (500 users: `taskDelivery` p95 63 s), these were counted as
   lost.
4. **View expectations that could never be met.** The check read `t` before the detail window's wait of up to 30 s,
   and it expected a change even when the viewer was the task's creator. The app skips that case.

The server matched in every case checked: the runtime committed the value (`_gonvex_sync_changes`), the visibility
oracle found 0 mismatches, and under the fixed rule every observer's replica held the value.

## Evidence

Old rule and new rule were measured in the same runs: the new rule computes the old rule's count at teardown.

| run | ops | failed saves | old-rule errors | new-rule undelivered | late (within grace) | oracle | error rate |
|---|---:|---:|---:|---:|---:|---|---:|
| loss-200 (Mac clock ≈ 9–11 ms **ahead**) | 403 | 0 | **1,123** | 0 | 0 | 9/9 converged | 0.44 %* |
| loss-200b (Mac clock ≈ 20 ms behind) | 402 | 0 | **0** | 0 | 0 | 9/9 converged | 0.44 % |
| loss-200c (final rules) | 403 | 0 | – | **0** | 0 | 9/9 converged | **0.44 %** |

\* The error rate uses the new rule. With the old rule, loss-200 would have reported 1,123 extra errors.

The workload was the same in all three runs (seeded per user, 402–403 expectations). The old rule counted 1,123 errors
in one run and 0 in the next. The only difference between those runs is clock skew:

- Clock alignment: each server-side `taskViews.viewedAt` (`ctx.now`) was matched with the generator's save start. In
  loss-200, the server time came **before** the save start on 85 of 93 views (median −9.1 ms), which is impossible
  unless the Mac clock was ahead. In loss-200b the median was +23 ms. A direct echo measurement (2–4 ms RTT) gave
  Mac − arch3 of −12.7 ms, then −21.5 ms, then −23.3 ms over about 20 minutes. The Mac clock drifts and gets stepped.
- Server truth for loss-200 (`after-loss-200`, tenant cbda0ae1): every viewed task had exactly one view during the
  run. `firstViewedAt` equals the commit time (`_gonvex_sync_changes.created_at` − 2 ms). There were no
  concurrent-viewer races and no no-op views.
- Integration new-200 (`lab-5k-new/new-200.json`): 77 saves failed with `budget_exceeded … waiting for a module
  isolate`. Defect 2 multiplied these into errors as well. My runs had no failed saves, because `bin-resume-lazy` is
  faster than `bin-action-depth`.
- Why the "old app" showed 17 errors while the new app showed 443: the two runs were at different clock skews and had
  different numbers of failed saves and backlogs. Nothing in the server's task delivery differs. Both apps share
  `recordTaskView` (`firstViewedAt` only when unset and the viewer is not the creator) and the same
  `tasks.recentWorkspaceTasks` plan (same columns, `taskGridRows` index). The skew at the time of the old run was
  never recorded, so the exact split cannot be reconstructed.

Not reproduced here: the 1,567 errors at 500 users and the 6,304 on the new artifact at 500. Those are dominated by
defects 2 and 3: about 113 failed `recordTaskViewByTaskId` saves, plus a 13–60 s delivery backlog at teardown. Rerun
them with this harness to confirm.

## Fix (harness only; the runtime and the thin client are unchanged)

- `bench/whagons/task-expectations.mjs` (tests: `task-expectations.test.mjs`, run in the runtime-regression workflow):
  - First views match on `firstViewedAt > 0` (`firstViewed`). No timestamps are compared across hosts.
  - `viewSetsFirstViewedAt(row, member)`: expect a change only when the view sets the field (the app skips views by
    the creator and views of already-viewed tasks). `run.ts` re-reads the row after the detail window's wait.
  - `unmetTaskUpdate`: failed saves expect nothing. A later edit excuses an earlier one when the server's `updatedAt`
    ≥ `at` − `LOAD_CLOCK_TOLERANCE_MS` (250 ms).
- `run.ts`:
  - `saveFor()` records each save's outcome.
  - Teardown keeps the subscription open for up to `LOAD_TEARDOWN_DELIVERY_MS` (30 s) and reports these counters
    separately:
    - `lateDelivered`
    - `pendingSaveAtTeardown` (error "Task update save still pending at teardown")
    - true `undelivered`, with diagnostics: path, args, at, savedAt, observer and the observer's row
  - These land in `result.taskExpectations` (merged by `merge-shards.mjs`).
- `undelivered-check.py <result.json> <data copy>`: for each remaining undelivered op, checks whether the committed
  row carries the value (a delivery gap) or not (a harness or app issue). Not needed in these runs (0 ops).
- The thin client and its `unsupported` map are untouched (`unsupported: {}` in loss-200c).

Error counting stays **per observer**, so one real loss to N users still counts N times.

## The `budget_exceeded` errors in `taskResources.recordTaskViewByTaskId`

These are not specific to that reducer. It is simply the most frequent one (15 % of foreground actions). In new-200 the
same error hit 13 reducer paths, for 77 failures in total. The message is `invocation deadline elapsed while waiting
for a module isolate` (`rust/crates/module-runtime-v8/src/lib.rs:497-503`):

- Each Reducer gets a deadline of `GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS`, which defaults to 10 s
  (`runtime/src/execution.rs` `install_execution_deadline`). The clock starts at admission, and the call fails if it
  cannot get an isolate in time.
- The isolate pool has `clamp(available_parallelism, 1, 4)` isolates, which is **1** on the 1-vCPU container. All
  Reducers from all tenants queue for that one isolate.
- In new-200, `grantProjection.reconcile-hourly` held the big tenant's writer for 17.97 s (`crons` table in
  `/dev/profile`: maxNs = maxWriterHoldNs = 17.97 s, 2 errors). One-shot jobs ran for up to 8.9 s and wrote 55k
  rows. While that happened, the Reducers behind them in the queue reached their 10 s deadline: `fn.total reducer`
  p999 was 10.05 s, and `db.writer_acquire` max was 16.31 s.
- Nothing ran and nothing was committed (the writer is taken only at the first DB call). The expectations were
  therefore false, which is defect 2.

The fix belongs to the runtime or the app, and the integration thread owns it: keep long crons from occupying the only
isolate and the writer, for example by chunking or yielding in `grantProjection.reconcile-hourly`, or by giving crons
their own pool.
