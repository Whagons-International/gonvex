# SQLite WASM research status

Paused by the owner on 2026-10-23 while another agent evaluates improving
IndexedDB. The owner requested commits and draft PRs so the work can be resumed
later. This supersedes the October 20 checkpoint's statement that no PR should
be opened. Keep this experiment unmerged until the owner resumes it and the
remaining qualification is complete.

## Preserved branches

- SDK, runtime, protocol, native assets and benchmarks: Gonvex branch
  `perf/sqlite-ingest-credit-batches`.
- Whagons integration and generated application bindings: whagons5-client
  branch `impl/sqlite-large-qualification`.

These branches preserve the full experiment, including intermediate fixes and
diagnostics. Their size is intentional for this paused snapshot. They are not
a proposal to roll the experiment into production as one change. The app
branch selects SQLite as its default, so it must remain a draft alongside this
SDK branch. Existing staging has continued independently; reconcile it only
when the investigation resumes.

## Latest evidence

The final browser diagnostic used SDK commit
`fd939096b866af0c3a5d7113ca1db0bdc1312340`, app commit
`bc598792f3cab0a8f07956746d8ab22d8af50317` plus its preserved working-tree
changes, and the `vertical-slice-r45-native` build. It completed create, edit,
reload, offline create and edit, reconnect with outbox drain, and a second tab.
It then failed on Everything with `Native ingress wait budget exhausted` and
a route error page. Extra instrumentation makes this a correctness diagnostic,
not a performance sample.

The final SDK patch retains the normalized feed failure for readers already
waiting on that feed. Admission pressure now reaches these readers as
recoverable `PAGE_CHANGED`, matching the reconnect callback, while permanent
payload errors keep their original classification. Queue limits and committed
rows remain unchanged.

The new regression failed before the fix, receiving `BUDGET` instead of
`PAGE_CHANGED`. After the fix, this command from `packages/client` passed all
58 native feed tests in 4.36 seconds:

```sh
./node_modules/.bin/vitest run src/sqlite-native-feed.test.ts --maxWorkers=1
```

These tests use real SQLite WASM in Node with in-memory storage. They do not
verify browser OPFS or demonstrate successful end-to-end recovery. The final
patch has not had the full SDK suite, package build, fresh browser build, or
real UI recheck. The prior browser build does not contain it. App generators
and browser captures were not rerun when creating the paused snapshot.

## What remains before any rollout

- Rebuild exact SDK and app sources, regenerate checked artifacts, inspect
  import cycles, and run their required checks.
- Reproduce the Everything failure, prove recovery with the final patch, and
  complete the real UI walkthrough and repeated reload/edit soak.
- Resolve remaining preview/editor timing and lifetime failures. Verify
  normal multi-tab ownership, background tabs, offline intents, reconnect and
  owner loss without disabling browser throttling.
- Run a matched, alternating IndexedDB/SQLite cohort across task counts and
  related-table fanout. Include all failures, timeouts and slow samples.
- Record actual loaded worker and WASM identity. An earlier 31k native-labeled
  result exercised a legacy adapter and cannot qualify SQLite.
- Measure normal renderer private memory, whole-browser memory, SDK memory,
  startup phases, worker messages, disk work and correct UI paint separately.
  Forced-GC diagnostics cannot establish normal memory use.

The owner's latest preference was below 200,000,000 bytes for the largest
settled renderer private-memory metric, prioritizing load, reload and
interactions. The separate design targets in `client-sync-engine.md` include
cached correct paint p95 at most 500ms and 139MiB combined three-tab SDK memory.
These metrics are not interchangeable. No completed production-representative
scaling cohort qualifies the final candidate against them.

Broad task and related-data subscriptions, row copies and derived UI collections
may dominate memory independently of storage. Scale these independently and
measure their contributions. Current evidence does not establish that browser
architecture is the sole bottleneck or that SQLite can meet the goals.

## Local evidence

Private diagnostic profiles and logs remain on arch3 under
`/mnt/data/work/wh-sqlite-study-20261023` and
`/mnt/data/work/wh-memory-impl/visibility-private`. The former contains the
protocol, pause notes and `runs/study-trace-01` / `runs/study-trace-02`. The
walkthrough r71 and later drivers correct the retained-search assertion: a
reload with a one-task search must validate that task, rather than require
twelve visible tasks. Keep the full-grid checks when there is no search.

Do not upload browser profiles, environment files, credentials or raw heap
snapshots. No new investigation jobs should start without an owner resume.
