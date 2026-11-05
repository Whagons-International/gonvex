# Per-open visibility resolution and per-task detail opens (perf/open-visibility)

Branch `perf/open-visibility` (from `perf/cache-heap` 36c440d6). Module-level work on the
shared perf fixture (`perf/fixture`, extended here) plus a 1k-plateau replay driver; no lab
load run. Bench: `cargo bench -p gonvex-runtime --features perf-fixture --bench
open_visibility` (scenarios and env in the file header). Oracle:
`cargo test -p gonvex-runtime --features perf-fixture --test principal_cache_oracle`.

## Where the 66 snapshot builds/s at 1k come from (ch-1k-prod-b, step-log 8d39925b)

From `lab-5k-new/ch-1k-prod-b/dev-metrics.jsonl`, plateau window (167.6 s), deltas of the
profiler stage counts and the snapshot cache's per-path counters:

| counter | per s | what |
|---|---:|---|
| `snapshot.build` | 67.6 | ScopeTimer in `begin_collection_read` **and** in `build_cache_entry` |
| `snapshot.visibility` (= `begin_collection_read`) | 40.8 | every open's own read |
| ⇒ `build_cache_entry` entered | ≈ 26.7 | separate admitted read that builds a cache entry |
| snapshot cache entries actually built | 6.06 | 6.0/s of them per-task detail keys (`{taskId}` / `{id}`) |
| detail-path cache builds + hits | 6.9 | the 8 detail collections that reached the cache layer |
| login-path cache hits | ≈ 34 | `statuses`/`tags`/`workspaces` ≈ 4.4/s each (≈ 4.4 logins/s), `taskUsers`/`taskTags` ≈ 9/s, … ; 0 builds |

* **Detail windows are about half of the 66/s.** Client side: `recordTaskViewByTaskId` (sent
  after a window became current) 2.44/s plus ≈ 1.8/s windows that never became current
  (494 "Task detail queries never became current") ≈ **4.3 windows/s ≈ 34 detail opens/s**.
  Server side: ≈ 26.7/s `build_cache_entry` entries (only per-task keys miss the cache; login
  keys were all hits) + 6.9/s detail opens reaching their own read ≈ 34/s.
* ≈ 20.7/s `build_cache_entry` calls ended **without** an entry. Inference (not verified):
  detail-window opens dropped while queued for a snapshot permit (`snapshot.permit_wait`
  p50 26 s) when the client gave up or closed the window; a dropped future still records its
  ScopeTimer. Every detail open that did run paid **two** permits and two read transactions
  (the separate cache build, then its own read), and per-task entries were hit only 0.1–0.3
  times per build.
* The other ≈ 33/s are login/reconnect opens (≈ 4.4 logins/s × 11 collections, all cache
  hits) and workspace-switch / notification windows.

The fixture now carries the 8 detail collections and `notifications.listNotifications`
(extracted from the same Whagons manifest, existing plans byte-identical; `FIXTURE_VERSION`
2), and the bench's driver replays that plateau: **4.4 logins/s (reconnects over 1,000
members, 11 collections) + 4.3 detail windows/s (8 collections) + 10 task writes/s (0.3/s of
them `taskUsers` assignments)**, one tenant, pinned to one core.

## What changed

### 1. Shared principal resolutions (`rust/crates/runtime/src/principal_cache.rs`)

Each version 2 open used to resolve the member's principal with SQL in its own read
transaction: the member row, every active principal/grant set of the plan (plus grant join
sets and derived sets), then recursively the same for every inherited parent, and a
fingerprint that serializes the whole plan and hashes it (twice). A Whagons detail window is
`tasks.get` (tasks plan: 31 sets) plus 7 collections inheriting `tasks` (9 grant sets each,
plus the tasks resolution again): **403–451 SQL statements per window** (fixture, statement
profile), all for the same answer.

Now the SQL part of a resolution (`Core`: member row, principal/grant/derived sets) is kept
per **(database, tenant, account, plan set definitions)**:

* **Key by set definitions, not plan identity.** `sets_signature` = SHA-256 of the plan's
  version and `sets` (memoized on the plan). The 7 task-child plans carry identical grant
  sets, so they share one entry; the login's `taskUsers`/`taskTags` resolution serves every
  detail child. A plan reuses an entry only when its active sets for the member's role equal
  the entry's (active sets also depend on the predicate). The fingerprint hashes the whole
  plan, so it is kept per plan in the entry (computed on a plan's first use of the entry).
* **Validity by the change log, in the open's own read transaction.** An entry is valid over
  a revision range `[low, revision]`. An open at another revision reuses it only after one
  probe: `SELECT (EXISTS(… _gonvex_sync_changes WHERE table_name = $t AND revision > $1 AND
  revision <= $2) OR …)` over the tables the resolution read (`members` plus each active
  set's base and join tables), an index seek per table on `gonvex_sync_changes_table`. The
  log rows are written in the committing transaction (capture triggers + `finalize`) and
  every row after `retained_revision` is kept, so the probe is exact. A range starting
  below `retained_revision` is a miss. After a clean probe the range widens.
* **Only captured tables.** A resolution is stored only if every input table has all three
  capture triggers (checked per database at its `PRAGMA schema_version`).
* **Single flight per key**: concurrent opens of one member wait for the first SQL
  resolution instead of all resolving.
* Not shared: developer sessions (possible ephemeral principal), legacy (non-v2) plans,
  live queries (they pass `None`). Parent ACL indexes and inherited parents are attached
  per open exactly as before; only SQL results are shared. `plan.validate()` still runs on
  every resolution.
* Bounds: `GONVEX_PRINCIPAL_CACHE_ENTRIES` (8,192), `GONVEX_PRINCIPAL_CACHE_TTL_SECS` (120,
  unused entries dropped), least recently used eighth dropped over the bound.
  `GONVEX_PRINCIPAL_CACHE=0` disables (the A/B switch in every table below).
  `/dev/metrics` → `memory.principalCache` (entries, bytes, hits, probedHits, misses,
  invalidated, uncacheable, evictions).
* `resolve_derived` now runs inside the SQL part, before parents and indexes are attached;
  it reads only the member's sets and direct context, so the result is unchanged.

Precision: invalidation is per table, not per member. Any write to a grant source table of
the tasks plan (`taskUsers`, `taskShares`, `taskWorkspaceContexts`, `taskApprovalInstances`)
or to a principal table (`memberTeams`, `memberSpots`, `workspaceTeams`, `spots`,
`workspaces`, `personalWorkspaceMembers`, `members`, `globalSettings`) invalidates every
member's tasks-plan entry in that tenant, and any `tasks` write invalidates `workspaces`
entries (its derived `activePendingTasks`). A detail window or a login is a burst of opens
within a second, so it still shares; reuse across bursts drops with the tenant's grant-write
rate (see the driver's `invalidated` counts).

### 2. Keyed snapshot-cache entries built inside the opening read (`snapshot_cache.rs`, `replica.rs`)

Per-task detail collections are snapshot-cache keys with equality-filter arguments
(`{taskId}`). Before, a miss built the entry in a **separate** admitted read (its own
snapshot permit and transaction: `build_cache_entry`), then the open took a second permit
for its own read and evaluated the entry in memory. At 1k users those entries were hit
0.1–0.3 times per build, so most detail opens paid two permits and two reads.

Now a keyed miss builds the entry **inside the open's own read transaction**, at its clock
(`build_entry`: the equality filters' index, a few rows, no visibility SQL), installs it,
and evaluates it in memory with the compiled class program like any cached entry. The view
path is unchanged (same parent-index and member checks, falls back to the SQL scan when
indexes moved). Principal-free keys (`{}`: the login collections) keep the separate shared
build. `GONVEX_SNAPSHOT_CACHE_KEYED_INLINE=0` restores the old path; new profiler stage
`snapshot.cache_inline`; `memory.snapshotCache.keyedInlineBuilds`.

This is the cheap form of "serve detail collections from cached rows plus a filter": the
rows come from one indexed read of the task's own rows (there is no tenant-wide cached
parent set that covers them: `tasks.recentWorkspaceTasks` is bounded by `maxRows`, and
notes/logs/views/approvals/findings/custom values have no tenant-wide entry), and the
visibility check is the in-memory compiled program instead of SQL `IN (json_each(…))`
predicates over the member's token sets. Measured alternative that lost: skipping the cache
for keyed keys (SQL scan with visibility predicates) cost **more** CPU per window than
building + filtering in memory (e.g. TeamSpots 3.7 → 8.6 ms user per window), though it
did cut tail latency by removing the second permit; the inline build keeps both gains.

## Results

Host: arch3, shared with other agents' builds (load average 6–11 throughout; every core
~50 % busy). Cross-process comparisons of the same binary moved by up to 2× between runs, so
the decisive per-open numbers come from the **in-process A/B** (the `ab` scenario alternates
sharing off/on per member in one process). CPU = process user CPU (all threads, incl. SQLite
workers); system time is excluded because host page compaction under memory pressure was
charged to whichever process faulted (perf showed `migrate_pages`/`compaction` in it).

### Per detail window (8 collections) and per cold login (11 collections), in-process A/B

300-member fixture tenant (9,000 tasks), 100 members per mode (all kinds, alternating), a
cold login then 2 detail windows on different tasks each; inline keyed builds on in both
modes; two runs:

| per member | sharing off | sharing on | change |
|---|---:|---:|---:|
| detail window user CPU (median) | 14.6 / 12.5 ms | 4.7 / 4.0 ms | **3.1×** |
| detail window CPU incl. system | 17.6 / 15.5 ms | 5.2 / 4.7 ms | 3.3× |
| detail window wall | 19.2 / 16.8 ms | 6.0 / 5.4 ms | 3.2× |
| detail window allocations | 109,670 | 31,981 | −71 % |
| cold login user CPU | 57.1 / 47.3 ms | 51.1 / 44.6 ms | −6 to −11 % |
| cold login allocations | 109,688 | 93,754 | −15 % |

Same A/B with the separate keyed build (`GONVEX_SNAPSHOT_CACHE_KEYED_INLINE=0`): detail
window 14.6/14.4 → 5.2/5.6 ms user, wall 19.8 → 15.6/16.4 ms. The inline build saves ≈ 1 ms
CPU and ≈ 10 ms wall per window (one read transaction and one permit fewer per open).

SQL per window / login (statement profile, sharing only, separate keyed builds, 78-member
fixture):

| | off | on |
|---|---:|---:|
| detail window statements (Admin, TeamAllSpots / TeamSpots, GrantOnly) | 451 / 403 | 80 / 32 |
| detail window SQLite VM steps | 49k–77k | 0.3k–4.3k |
| cold login statements | 192 | 107 |
| with a task write before every open: window / login | 412 / 204 | 56 / 127 |

Per kind (sequence scenario, 4 members per kind and mode, medians of 2 runs per config,
separate processes, so ±30 % noise): detail window user CPU before → after (sharing +
inline) Admin 15.6 → 6.4 ms, TeamAllSpots 16.1 → 5.9, TeamSpots 12.5 → 6.0, GrantOnly 11.8 →
5.4 quiet; 26–31 → 11–16 ms with a write before every open (each open then probes the change
log; grant writes invalidate). The sequence's cold-login medians showed sharing slower in
some runs (e.g. 38.7 → 62.0 ms TeamSpots quiet) and faster in others (66.0 → 51.1 ms
TeamSpots writes); the in-process A/B above resolved this as run-to-run drift (perf of both
modes: the cache's own functions are < 0.5 % of samples).

### 1k plateau replay (4.4 logins/s + 4.3 detail windows/s, 1,000 members, 60 s, one core)

264 logins, 257 detail windows, ≈ 530 writes per run; A = before (`GONVEX_PRINCIPAL_CACHE=0
GONVEX_SNAPSHOT_CACHE_KEYED_INLINE=0`), B = sharing, D = sharing + inline keyed builds (the
branch default), C = sharing + keyed opens deferred to SQL (discarded variant):

| run | config | user CPU | login p50 / p99 | detail window p50 / p99 | snapshot cache builds |
|---|---|---:|---|---|---:|
| v5 | A | 28.2 / 26.4 s | 114 / 365, 103 / 386 ms | 49 / 712, 38 / 468 ms | 2,075 |
| v5 | B | 22.0 / 20.1 s | 94 / 133, 92 / 286 | 22 / 129, 34 / 340 | 2,075 |
| v5 | C | 22.8 / 20.3 s | 100 / 191, 84 / 268 | 15 / 98, 14 / 78 | 11 |
| v6 | A | 24.5 / 23.2 s | 104 / 416, 112 / 468 | 50 / 583, 63 / 624 | 2,075 |
| v6 | B | 25.7 / 24.9 s | 118 / 290, 104 / 172 | 48 / 375, 33 / 179 | 2,075 |
| v6 | D | 29.3 / 25.0 s | 132 / 465, 107 / 233 | 21 / 146, 16 / 100 | 11 + 2,064 inline |

* **Detail-window tail: p99 468–712 ms → 100–146 ms (D)** in every run; p50 38–63 → 16–21 ms.
* Total process CPU of the replay is dominated by the 11-collection logins (their snapshot
  scans, not visibility) and the writes; the per-window saving (≈ 10 ms × 4.3/s ≈ 4–5 % of a
  core) is inside the replay's run-to-run spread (v5 shows −22 %, v6 none). Use the
  in-process A/B for CPU per open.
* Principal cache at 1,000 members: 1,446 entries, 5.96 MB; per run ≈ 5,300–5,600 hits at
  the same revision, ≈ 750–820 after a clean change-log probe, 1,446 misses (each member's
  first login: 4 distinct set signatures), ≈ 135 invalidated by the 0.3/s grant writes.

### 500-user ramp driver (500 logins over 60 s + 2 detail windows/s, 23 writes/s, one core)

v4 binary (sharing only), three runs each, user CPU 37.1 / 49.0 / 44.4 s off vs 40.6 / 43.4 /
30.3 s on; login p99 11.2 / 5.2 / 2.4 s off vs 1.7 / 1.5 / 1.2 s on; detail p99 14.4 / 8.2 /
3.4 s off vs 2.1 / 2.6 / 2.0 s on (the one-core pin was shared with host load: tails are
queueing). Principal cache: 2,000 entries, 7.7 MB at 500 members.

## Memory

≈ 3.9–4.1 KB per entry (sets of tokens, member row, per-plan fingerprints), 4 entries per
member (tasks plan, task-child grant plans, workspaces, set-less plans): ≈ 6 KB per member
in the fixture; bounded by 8,192 entries (≈ 32 MB worst case) and the 120 s TTL. Keyed
snapshot-cache entries are the same as before (built in a different place).

## Tests

* `cargo test -p gonvex-runtime -p gonvex-profiler --features gonvex-runtime/perf-fixture
  --no-fail-fast`: all pass (runtime lib 374 passed / 6 ignored, every oracle suite:
  `principal_cache_oracle` (new), `snapshot_cache_oracle`, `delivery_oracle`,
  `raw_rows_oracle`, `class_sharing_oracle`, `visibility_fanout`, `offload_equivalence`, …).
* `cargo test -p gonvex-runtime -p gonvex-store --no-fail-fast` (featureless): 471 passed,
  0 failed, 6 ignored.
* `cargo clippy -p gonvex-runtime --features perf-fixture --benches --tests`: no warnings in
  the touched code (pre-existing ones remain).
* New oracle `principal_cache_oracle`: for every member of the small fixture and every
  cold-login and detail-window collection, an open with sharing (hits at the same revision
  and after probes) equals an open resolved with SQL (rows, order, hashes, digest) after 12
  steps (data writes, assignment/unassignment, share, note, team gain/loss, spot loss, role
  and permission change, presence) and with writes racing the opens; it asserts the
  expected hit/probe/invalidation counts per step, and compares snapshot-cache opens of the
  8 detail collections (inline-built keyed entries, then hits after writes) to fresh SQL.

## Risks

* Validity rests on the change log covering every write of an input table after
  `retained_revision`. Tables without all three capture triggers are never cached; a write
  path that bypassed the triggers (none known: one writer connection per database, triggers
  on every tracked table) would serve a stale principal until the TTL.
* Invalidation is per table: a tenant with frequent grant-table writes re-resolves more
  often (correct, just less sharing). Not precise per member.
* Entries are process-wide keyed by database URL + tenant + account; an epoch change
  (database reset) misses. Developer sessions are never shared.
* `resolve_derived` now runs before parent indexes are attached (it reads only sets).
* Keyed inline builds run inside the open's read transaction while it holds its snapshot
  permit: the read is a few rows through the equality filters' index; a keyed collection
  whose filter column is not indexed would make the open's read longer (before, the build
  was a separate read). `GONVEX_SNAPSHOT_CACHE_KEYED_INLINE=0` reverts.
* Fixture change (`FIXTURE_VERSION` 2, 9 more collections and 7 more tables) is additive, but
  rebuilds every fixture template once and adds empty tables to other benches' databases.

## Not measured

* No lab run (the integration thread owns e2e); the 1k numbers above are a module-level
  replay of the observed pattern in one tenant, not the 7-tenant skew.
* The ≈ 20.7/s `build_cache_entry` calls without an entry at 1k are inferred to be cancelled
  queued opens; the lab metrics do not distinguish cancellation from build errors (logged at
  debug only).
* Live queries do not use sharing (they pass `None`).
* Heap by owner with jemalloc profiling was not rerun.
