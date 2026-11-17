# Staging findings of 2026-11-04: reader churn and snapshot rebuild volume

Branch `perf/staging-findings` (from `perf/integrate-5k` 96c408c0). Evidence: staging `/dev/profile` at 19:01 UTC
(1.94 h uptime) and runtime logs around 18:57 UTC (copies in `/home/gabriel/work/gonvex-perf/staging-evidence/`).
Staging runs **sqlite.1** (`deploy/sqlite-staging` 24055f50, nice 10, 12 tokio workers) with the Whagons
v2-visibility client 0f83220ce (`@gonvex/client` 0.5.2-sqlite.1). Staging itself was not touched; its environment
(Coolify variables) was not read.

## Finding 1: reader connection churn

### What staging runs (a)

- sqlite.1 reader pool (`store/src/registry.rs` at 24055f50): `min_connections(0)`,
  `idle_timeout(Some(GONVEX_DB_IDLE_TIMEOUT_MS))` per connection, up to `GONVEX_DB_MAX_OPEN_CONNS` readers
  (code default = CPUs clamped to 2..4, so 4 on staging's 12 vCPUs). Every reader closes after the idle timeout,
  so the first burst after a quiet spell opens a new reader for every concurrent read, up to the cap.
- The idle timeout default in code is **15 s** (lab commit 49e69c5e). The Postgres-era PRs set different values
  only in deployment files: #261 (`9fda6355`, 1 s, 2 conns) and #279 (`88d94c06`, 60 s) put
  `GONVEX_DB_MAX_OPEN_CONNS=2`, `GONVEX_DB_MAX_IDLE_CONNS=2`, `GONVEX_DB_IDLE_TIMEOUT_MS=60000` into
  `docker-compose.yml` and `.env.example`. They still apply to the SQLite pools when a deployment uses those files.
  The runtime no longer has a Postgres path (Postgres variables fail startup, `config.rs`), so these knobs now
  mean only SQLite readers. Either way (15 s or 60 s), every reader is gone after a minute's quiet.
- The log matches: at 18:56:59 eight read-only connects start together (`PRAGMA foreign_keys = ON; synchronous;
  cache_size; mmap_size; temp_store` = sqlx's reader setup, no `journal_mode`, so readers, not writers). Each takes
  1.1–2.5 s, and four acquires report 2.44–2.55 s. Profile: `snapshot.begin` max 2,555 ms / p99 341 ms,
  `db.reader_begin` max 2,554 ms, presence `fn.prepare` 895 ms at 18:57:02 (slow trace #3).
- RC (`perf/integrate-5k`): readers are swept by `sweep_readers` (reader-warm): one warm reader per database
  for 10 min after its last read, extra readers close after `GONVEX_DB_IDLE_TIMEOUT_MS` (still 15 s), and reads
  wait up to 25 ms for a busy reader before opening another. Warm readers count against
  `GONVEX_SQLITE_CACHE_BUDGET_MB`.

### Why a connect costs seconds (c)

`MODE=setup` of `store/examples/reader_warm_bench.rs`, 26 real Whagons tenant files (2.9 MB parsed schema each):

| step | alone, 1 CPU | 8 at once, 1 CPU | 8 at once, 4 CPUs |
|---|---|---|---|
| thread spawn + join (sqlx worker) | 0.08 ms | 0.25–1.6 ms | 0.18 ms |
| `open_v2` | 0.14–0.37 ms | 1–6 ms | 0.5 ms |
| first schema-reading pragma (schema parse) | 11–24 ms p50, 46 ms p99 | 114–121 ms | 205 ms |
| `mmap_size` pragma | 2 µs | 2 µs | 2 µs |
| total | 12–29 ms | 134–138 ms | 207 ms |

- The whole cost is SQLite parsing the schema, once per connection. Concurrent opens multiply it, and they get
  slower with more cores (205 ms on 4 CPUs vs 114 ms on 1), which fits SQLite's global memory-statistics mutex
  on every allocation (reader-warm doc).
- `mmap_size` 256 MiB costs nothing at setup: setting it to 0 changed the 8-concurrent total from 134 ms to 138 ms
  (noise). It is an address-space reservation only.
- Thread spawn is at most 1.6 ms.
- Staging's 1.1–2.5 s is the same work (8 concurrent parses) on a nice-10 process sharing a throttled host. It is
  about 10× the lab's 1-CPU figure. That factor is not measured.

### Remaining churn in the RC, and the change (b)

Measured with the new `MODE=burst` driver: the staging pattern. All 26 tenant writers are open, the default
128 MB budget applies, and 2 tenants each get 14 concurrent read transactions (one page load's collection
opens) after 30 s of idle. Each read runs one query and holds its reader 5 ms more. There are 4 rounds, with
readers capped at 4 per database (staging's cap). One CPU (`taskset`), host shared with other agents.

| config | readers opened per burst (2 tenants) | `snapshot.begin` p50, rounds 1–3 | p99, rounds 1–3 | begins > 2 s |
|---|---|---|---|---|
| sqlite.1-equivalent (no warm reader, no patience, 15 s) | 8 | 121–173 ms | 137–186 ms | 0 |
| RC (warm reader, patience 25 ms, 15 s extras) | 6 | 52–72 ms | 89–121 ms | 0 |
| **this branch** (+ 120 s extras, one open at a time) | **2** | **12–24 ms** | **66–77 ms** | 0 |

Round 0 is cold for every config (6–8 opens, p99 128–182 ms). No config reaches 2 s in the lab: the seconds
need staging's slower CPU share. The structural change is the open count: sqlite.1 opens 4 readers per tenant per
burst, which is exactly the ~8 connections in the 18:57 log. The RC still opens 3 extras per tenant on every burst
more than 15 s after the last one.

The change, on this branch:

1. **`GONVEX_DB_IDLE_TIMEOUT_MS` default 15 s → 120 s** (`runtime/src/config.rs`). It applies only to readers
   beyond the warm one. The budget sweep still closes them earlier when connection heap passes ¾ of
   `GONVEX_SQLITE_CACHE_BUDGET_MB`, so memory stays inside the budget: heap stayed at 120–128 MiB in every run
   above. A compose deployment still gets 60 s from `docker-compose.yml` unless that default is changed too
   (left alone here; deployment files are the coordinator's call).
2. **One reader open at a time per database, at most `GONVEX_DB_READER_OPENS` (default 2) process-wide**
   (`store/src/registry.rs` `begin_read`). A read that finds no idle reader while this database is already
   opening one waits for that open, then waits the usual patience for a release, instead of starting another
   schema parse. `GONVEX_DB_READER_OPENS=0` restores the RC behaviour. Test:
   `store/tests/reader_open_single_flight.rs` (12 short reads on a cold database open ≤ 2 readers).
3. **`GONVEX_DB_WARM_READERS`** (default 1, capped at the pool size): readers kept in the warm window.

Fully cold bursts (the warm reader is gone too: more than 10 min idle, or evicted by the budget) measured with
a 20 s warm window and 15 s extras:

| config | 1 CPU: opens / p50 / p99 | 4 CPUs: opens / p50 / p99 |
|---|---|---|
| sqlite.1-equivalent | 32 / 137 / 186 ms | 32 / 165 / 193 ms |
| RC | 32 / 88 / 130 ms | 32 / 99 / 170 ms |
| this branch | 24 / 85 / 173 ms | 22 / 92 / 182 ms |

Single-flight cuts cold opens by 25–31 % (fewer schema parses: CPU and 2.9 MB each) but does not improve cold
latency. p99 is 7–33 % worse because opens are serialized. The latency win comes from (1) and the RC's warm
reader. If the coordinator prefers latency over parse count for cold bursts, set `GONVEX_DB_READER_OPENS=0`.

**Budget caveat (unchanged, still open):** with 26 tenants, the writers alone hold ~75 MB of schema. Under the
default 128 MB budget, only about 6 databases keep a warm reader (reader-warm doc). The two burst tenants above
kept theirs because they were the most recently read. A tenant read only every few minutes is likely to be cold
on staging. `GONVEX_SQLITE_CACHE_BUDGET_MB=256` was the reader-warm recommendation.

Raw outputs: `/home/gabriel/work/gonvex-perf/sf-lab/results/{burst,burst4,setup}-*.txt`.

```sh
B=$CARGO_TARGET_DIR/release/examples/reader_warm_bench
LAB_DATA=/copy MODE=burst GAP_S=30 ROUNDS=4 GONVEX_DB_WARM_READER_MS=0 GONVEX_DB_READER_PATIENCE_MS=0 \
  GONVEX_DB_READER_OPENS=0 IDLE_MS=15000 taskset -c 3 $B          # sqlite.1-equivalent
LAB_DATA=/copy MODE=burst GONVEX_DB_READER_OPENS=0 IDLE_MS=15000 taskset -c 5 $B   # RC
LAB_DATA=/copy MODE=burst IDLE_MS=120000 taskset -c 7 $B                          # this branch
LAB_DATA=/copy MODE=setup CONCURRENT=8 [MMAP_BYTES=0] taskset -c 13 $B
```

## Finding 2: snapshot rebuild volume

### Cause

- **The web client does resume.** On reload and on reconnect, SDK 0.5.2-sqlite.1 sends `{cursor, digest(xor1)}`
  for every stored window. The windows persist in IndexedDB across reloads. The scope is the server's
  `visibilityScope` (project, tenant, account, permissions hash): no per-load or per-tab id
  (`modules.rs:551-558`, `index.ts:3943-3946`, `indexeddb-replica.ts:428`). Every tab has its own 2 sockets
  (the main client plus the control-only client, `GonvexClientProvider.tsx`) and its own opens.
- **On sqlite.1 the server rebuilds although the client resumed.** `open_replica_negotiated` (replica.rs at
  24055f50) runs `begin_collection_read`, the full visibility query, before it checks the cursor. The
  `Aggregate` resume path then streams the whole visible set to verify the digest. Every resume therefore
  costs a full per-member snapshot build on the network worker (sqlite.1 has no offload). This is the 7–27 ms
  `snapshot` long polls in the profile.
- **The profile can't split resumes from cold opens directly**, but it bounds them:
  - `snapshot.send` (2,830) is never recorded for cold snapshots. The SDK negotiates paged, non-native
    delivery, and those pages go through `legacy_snapshot_delivery` with no profiler stage. So the 2,830 sends
    are resume deltas or readies, `needHashes`, and errors. **At least 2,830 of 6,823 builds (41 %) served
    clients that already had the data.**
  - The rest are cold opens, `needHashes` second builds (a resume can build twice: once for cursor + digest,
    then again for the keys + hashes reopen), cancelled opens (the build timer records on drop), and agent tool
    reads of replica collections. Browser one-shot or live queries cannot read replica paths (`execution.rs`
    rejects them).
- **Why whole groups of ~13–15 reopen on a live socket**, from sqlite.1 code. Which one dominates on staging is
  not measured.
  - `needHashes` to every affected subscription when a shared visibility dependency changes
    (`replica.rs` `resync`). The client then reopens each one with keys + hashes: large messages, which fits the
    9–45 ms `ws_connection` polls just before each group.
  - `session-expired` from the 30 s revalidation, or a re-authentication that cannot be preserved (opens in
    flight, member row changed). Access tokens last 15 min. These reopens use cursor + digest.
  - A module deploy sends `system.reload`.
  - Feed lag or reset sends `replica.reset`. The client deletes the windows, so these reopens are cold full
    snapshots (the most expensive case).
  - The groups in the profile's long-poll ring (18:57:09 and 18:58:05) are 56 s apart, with no early EOF in
    between, so they were reopens on live sockets, not reconnects.
- **Early EOF closes are a smaller source:**
  - The logs show 25 EOFs in 1.9 h: about 12 page closes (2 sockets each) or tab-discard and network events.
  - At ~14 builds each, that is ≈170 builds, about 2.5 % of the total.
  - The SDK's `close()` sends a Close frame, and the server's liveness timeout closes with code 4000, also a
    Close frame. So an EOF means the transport was cut: a page torn down before the frame went out, a
    back/forward-cache or mobile suspend, a network switch, or the proxy.
  - Whagons' `connectionDiagnostics.ts` records close codes per socket on the client. That is the place to
    confirm.

### What the RC already fixes (measured on 96c408c0 + this branch; the change does not touch this path)

`snapshot_cache` bench, one CPU, el-rey-sized fixture (10,500 tasks, 78 members). "Before" is the per-member SQL
scan that sqlite.1 does on every resume. "After" is the RC's log-only resume over the shared cache
(`resume-lazy`, `resume-fix`):

| resume, `tasks.recentWorkspaceTasks` | before | after |
|---|---|---|
| Admin, 0 changes since cursor | 147 ms / 79,966 allocs | 8.5 ms / 1,394 allocs |
| Admin, 200 changes | 160 ms | 24 ms |
| TeamAllSpots, 0 changes | 45 ms | 4.4 ms |
| GrantOnly (45 rows), 0 changes | 11.4 ms | 11.5 ms (sparse key: per-member SQL is already the cheap path) |

Reconnect storm, resume mode: 150 sockets × 10 collections = 1,500 cursor resumes in 10 s, with 4 task writes/s
during the storm. It goes through the socket path, `enqueue_replica_open`, which runs on the offload pool
(`replica_open_queue.rs:250`):

| | before (cache off) | after (RC) |
|---|---|---|
| open latency p50 / p99 | 2.22 s / 4.24 s | 17 ms / 280 ms |
| time to complete (arrivals end at 10 s) | 14.3 s | 10.0 s |
| process CPU | 14.0 s | 7.6 s |
| full-collection SQL reads | 1,500 per-member scans | 13 shared entry builds (1,409 hits) |
| `needHashes` | 0 | 0 |
| network-thread polls > 5 ms | 0 (max 2.1 ms) | 1 (max 25.7 ms), p99 0.24 ms |

Correctness: `cargo test -p gonvex-runtime --features perf-fixture --test resume_log_only --test
snapshot_cache_oracle` passes. Resumes read no per-member rows, and cached base + log delta equals a fresh
snapshot. In the RC every snapshot build and resume runs on the offload pool, never on the network worker (the
"before" column above runs there too, which is why its network polls are short; on sqlite.1 the same work runs on
the network worker). Raw: `/home/gabriel/work/gonvex-perf/sf-lab/results/snapshot-cache.{txt,jsonl}`.

### Not fixed here (post-gate)

- Which reopen trigger dominates on staging. Add a per-reason counter for opens: resume, cold,
  `needHashes` reopen, after `replica.reset`, after `session-expired`. Today the profile records a single
  `snapshot.build`.
- `needHashes` on shared visibility-dependency changes still makes clients upload keys + hashes for every
  affected collection. In the RC the comparison reads the cache, not SQL, but the large inbound message and the
  second round trip remain. Candidate: a server-side aggregate over the cached entry plus the log, so the client
  needs no hashes.
- `replica.reset` (feed lag) deletes client windows and forces cold snapshots. In the RC those are cache hits,
  but they still send full snapshots on the wire.
- A Whagons-side check of close codes for the early EOFs (`connectionDiagnostics`).
- Unrelated error in the same logs: cron `workplans.generateScheduledItem` fails with
  `table taskUsers has no column named _creationTime` (18:09:05). That is a Whagons module/schema mismatch on
  staging.

## Not measured

- Staging itself, the full runtime binary with V8, and the real staging CPU share. The "seconds" in the logs are
  not reproduced, only the open counts and their relative costs.
- Which env values staging sets for `GONVEX_DB_*` and the cache budget. The 15 s vs 60 s question is answered
  only from the code and the compose files.
- The 120 s idle default under the 1k gate load (statement re-preparation share before/after). Only the burst
  driver was measured.

## Follow-up: preview 96c408c0 on staging, 21:18–21:48 UTC (23 connections, ~1,590 subscriptions)

Reported numbers (coordinator): `snapshot.build` 32,227 (p99 2.9 s), `snapshot.begin` p99 2.5 s,
`db.reader_begin` 115,584 (p99 2.58 s), `snapshot.cache_inline` 1,003, `snapshot_cache.build` 910 (p99 9.2 s),
140 watchdog stalls, 1,537 slow acquires. Triggers: three restarts and a client deploy at ~21:29. Host load was
32–41 on 12 cores, `GONVEX_DB_MAX_OPEN_CONNS=16`, reader nice 10. The 21:48 profile and logs were not in
`staging-evidence/` when this was written. The numbers below come from code, the module manifest, the fixture
and the local app capture in `/home/gabriel/work/gonvex-perf/app-opens/`.

### (1) Did the cache serve few opens? Mostly a reading of the stage names, plus four real misses

- **`snapshot.build` is not a SQL build.**
  - In the RC it times `begin_collection_read`, which **every** open runs, hit or miss: permit wait, ACL,
    begin and principal resolution (`replica.rs` 1096). It also covers each shared-entry build
    (`build_cache_entry`, 1051) and each re-read when an index is moving (up to 3 per open).
  - 32,227 is therefore the number of opens and re-reads.
  - SQL row scans are only `snapshot_cache.build` (910 shared entries), `snapshot.cache_inline` (1,003 keyed
    entries), and per-member fallbacks.
  - Cache hits have no profiler stage. They are in `/dev/metrics` → `memory.snapshotCache.{hits, builds,
    fallbacks}`, and that is the number to save next time.
- **Upper bound on hits:** 32,227 − 910 − 1,003 ≈ 30,300 opens served from an entry. The real figure is lower
  by the fallbacks and the index re-reads.
- **Eligibility of the 807 Whagons replica collections** (module.json on `feat/sqlite-staging-deploy`):
  - All 807 use version 2 plans, so the v1 bounded-plan fallback never applies.
  - 677 use shared entries; 130 are keyed (equality-filter arguments: one task's detail collections).
  - Keyed collections build per distinct arguments inside the open (the 1,003). The earlier measurement put
    these at 0.1–0.3 hits per build, as expected for per-task windows.
  - The unordered-truncation fallback needs a collection to exceed its `maxRows`. That is rare at Whagons sizes,
    but a 1–20-row `__local.*` collection whose table holds more rows (e.g. `deepResearchSettings` maxRows 1)
    takes it on every open. Not quantified.
- **Real misses, in order:**
  1. **A deploy invalidated every entry.** The key included the module **generation**, so after 21:29 every
     (tenant, collection, args) missed, although the client's cursors stayed valid. The cursor is (database
     epoch, revision) and no deploy changes it. The SDK answers `system.reload` with cursor+digest resumes
     (`resumeReplicaSubscriptions`). The epoch-changed path (`activateReplicaDirective`) keeps the stored
     windows. **Fixed below.**
  2. **Restarts start with an empty cache.** Three restarts and one deploy are four full key sets. 910 builds
     over ~4 rounds is ~230 distinct shared keys per round, about what 2–3 active tenants × ~100–170
     collections give. Single-flight works per key (database, project, content, path, args) across members and
     connections; it can't share across collections or tenants, by design. Each build reads the whole table
     (no visibility predicate). Under load 32–41 with niced readers that took up to 9.2 s, and every open of
     that key waits for it.
  3. **Reopens without cursors.** In a local capture of the real web client (`app-opens/reload-opens.json`, one
     user, reload 15 s after the first visit), **41 of 130 reload opens (32 %) carried a digest but no cursor**:
     40 small `__local.*` collections plus `members.listMembers`. The SDK only sends a cursor when a stored
     window exists (`replicaOpenRequest`), so these windows were not persisted: empty windows, or not yet
     flushed at 15 s. They are cold opens (full snapshot sent) but still cache hits on the server.
  4. **Index-moving re-reads and fallbacks.** An open re-reads up to 3 times, waiting up to 2 s each, while a
     parent ACL index is behind its clock. Under a write load on an overloaded host this inflates
     `snapshot.build` and `db.reader_begin` (115,584 reader begins for ~32k opens). Unmeasured on staging: needs
     `fallbacks` from `/dev/metrics`.
- **Opens per connection:** 32,227 / 23 ≈ 1,400 per connection in 30 min, about 9 full reopen rounds of ~150.
  The 4 known triggers account for fewer than half. The rest (re-authentication not preserved, watchdog-related
  reconnects, `needHashes`) needs the logs.

### (2) Does a deploy force full re-snapshots? Not for clients; it did for the server cache. Fixed

- **Client side:** the cursor survives the deploy, and the reopen is a log-only resume.
- **Server side:** the shared entry for that key was keyed by generation, so the first resume of each key
  rebuilt it from SQL. Every other resume waited on that build (single-flight).
- **Fix:** `snapshot_cache::content_hash`. Entries are keyed by SHA-256 of (collection definition, visibility
  plan, the table's schema entry) instead of the generation, memoized per (project, generation, path).
  - The plan is in the key because `build_entry` reads the plan's ACL columns into every row.
  - Decoded ACL rows are re-decoded whenever the class scope changes (pointer compare), so a new generation's
    compiled program never uses the old decode.
  - A deploy that changes a collection's definition, plan or table schema still misses that collection only.
  - Correctness relies on the same contract as client cursors: the change log is authoritative across deploys.
- **Tests:**
  - `tests/redeploy_cache.rs`: cold open → redeploy as generation 2 with a different bundle → write → resume.
    It asserts no `needHashes`, no snapshot, 0 member rows and 0 cache-fill rows read, cache builds unchanged,
    the digest equal to a fresh SQL snapshot's, and a second member's cold open after the deploy served from the
    same entry.
  - With the generation put back into the key, the same test fails with `(0, 2500)` rows read: the old full
    refill.
  - Unit test `content_hash_ignores_the_generation_but_not_what_rows_depend_on`.

### (3) Reader nice 10 on a busy host

- `GONVEX_SQLITE_READER_NICE` sets each reader's sqlx worker thread to base + N (`store/src/reader_priority.rs`).
  Nice only weighs threads against others in the same CPU cgroup (under cgroup v2 a container is one group; other
  containers compete through `cpu.weight`).
- So a reader at nice 10 has weight 110 against 1,024 for each of the runtime's own nice-0 threads: tokio
  workers (12 on staging), V8 isolates, writers, checkpoint.
- In a reopen storm those nice-0 threads are runnable, and readers get about a tenth of their share: slow
  queries, and **schema parses of new connections at nice 10**. With `GONVEX_DB_MAX_OPEN_CONNS=16`, up to 16 of
  those per tenant at once.
- **Measured** (`MODE=burst`, 2 tenants × 14 reads after an idle gap, one CPU shared with two nice-0 busy threads
  standing in for the runtime's network workers, 3 rounds):

  | reader nice | `snapshot.begin` p50 | p99 | burst wall |
  |---|---|---|---|
  | 10 | 605 ms | 707 ms | 623–714 ms |
  | 0 | 154 ms | 188 ms | 169–194 ms |

  3.9× slower at nice 10 whenever the runtime's own nice-0 threads are busy, which they are in a storm.
  Host-wide oversubscription (other containers) slows the whole container, but does not change this ratio.
- **Recommendation for staging:**
  - `GONVEX_SQLITE_READER_NICE=0`. Reader nice was tuned for the 1-vCPU lab, where readers had to yield to the
    writer and the isolate.
  - Keep `GONVEX_OFFLOAD_NICE` as is unless the profile shows offload starvation.
  - `GONVEX_DB_MAX_OPEN_CONNS=4` instead of 16. Each reader holds 2.9 MB of parsed schema, and 16 concurrent
    parses per tenant are exactly the slow-acquire pattern. `GONVEX_DB_READER_OPENS` (this branch) serializes
    opens per database either way.
  - `GONVEX_SQLITE_CACHE_BUDGET_MB=256` so warm readers survive across ~26 tenants.

### Still needed from staging (not readable here)

`/dev/metrics` → `memory.snapshotCache` (hits, builds, fallbacks, rollForwards), the 21:48 profile and the logs.
These confirm the hit count, split the fallbacks, and count reopens by trigger.

## Follow-up 2: steady-state churn, memory, slow statements (staging 22:12–22:20 UTC)

Evidence: `/dev/metrics` at 22:12 and 22:20, profiles at 21:48, 22:12 and 22:20, runtime logs **21:18:30–21:57:06**
(the saved log ends there; 22:24 is not in it), RSS samples every minute 21:49–22:28, and the page-load collection
list. Staging runs preview 96c408c0: cache keyed by module generation, no per-reason open counters.

### (1) Steady-state reopen churn

- **Rate:** `snapshot.begin` +6,980 in 8.47 min = **824 opens/min**. The cache counted +5,656 hits, +491 shared
  builds and +356 keyed inline builds = 6,503 lookups; per path, hits + builds = 6,147.
- **By path** (`snapshotCache.byPath` delta, 22:12 → 22:20). It is concentrated, not ~1 reopen per subscription
  every 2 min:

  | collection | opens in 8.5 min | entries | note |
  |---|---|---|---|
  | `tasks.get` (keyed by task id) | 800 (152 builds) | 390 | detail windows |
  | `members.listMembers` | 669 | 6 | `{}` args everywhere in the app |
  | `members.listMemberTeams` | 574 | 6 | |
  | `taskResources.getTaskAcknowledgmentProgressForTask` (keyed) | 542 | 156 | detail |
  | `taskWorkspaceContexts.listForTask` (keyed) | 514 | 156 | detail |
  | `taskResources.listTaskAckReads` (keyed) | 513 | 156 | detail |
  | `spots.list` / `taskWorkspaceContexts.contexts` / `statuses.statuses` / `tasks.list` | 141 / 103 / 102 / 95 | | |
  | all 40 `__local.*` | 400 (6.5 %) | | |

- **Decomposition, as far as the evidence goes:**
  - **New sockets (page loads, reconnects):** at least 5 new main connections appeared between the two
    snapshots, holding 582 subscriptions: **≥ 9.5 %** of opens, more counting the connections that came and went
    in between (13 disappeared). This includes ~40 `__local.*` opens each: the 400 `__local.*` opens are almost
    exactly 5 × ~40 + another ~5 page loads, so **`__local.*` is reload traffic, not churn of its own.**
  - **Task detail windows as users browse:** the keyed collections (`tasks.get` plus the
    `taskResources.*`/`listForTask`/`listTaskAckReads` family and smaller per-task collections) are
    **≈ 3,000 opens, ~half**. 356 inline builds means 356 first-time (task, collection) pairs. The other ~2,600 are
    reopens of tasks already opened, about **4–5 collections per task view × ~120 task views per 8 min** across ~10
    users. Expected client behaviour, cheap per open: 8.8 MiB for all 1,296 keyed entries.
  - **`members.listMembers` / `listMemberTeams`: ~1,240 opens (20 %), unexplained.** The app opens them with `{}`,
    so each client holds one deduplicated subscription, and the SDK closes one only 60 s after its last listener
    (`replicaSubscriptionRetentionMs`). Five new connections explain about 10 of these opens. The rest
    (~25 per connection in 8 min) must be server-driven: `needHashes`, which the client answers by reopening with
    keys and hashes. Code candidates:
    - a resume answers `needHashes` when **any** row of a visibility-dependency table changed since the cursor
      (`table_change_relevance(...).1`, replica.rs);
    - feed delivery resyncs (`Resync::Principal / Consistency / Window`).
    - `presence.transition` (78 of the last 200 operations) writes only `userPresence`, so it is not the trigger.
  - **Re-authentication, feed-lag resets:** `feedBacklog.lagResumes` 0 and `fillResets` 0, so feed-lag resets
    are unlikely. Re-auth is invisible in these logs (INFO is off; only `sqlx` WARNs are logged).
- **The server cannot tell these apart today.** Added on this branch (`runtime/src/open_reasons.rs`):
  `/dev/metrics` → `memory.replicaOpens` with
  - every open by request shape: `resume`, `reconcile` (keys + hashes), `coldDigest`, `cold`, `fullIntegrity`;
  - `reopenOnSocket`: the same socket opening the same (path, args) again;
  - `clientClose`;
  - every `needHashes` the server sent, by reason: `needHashesResumeDependency`, `needHashesResumeDigest`,
    `needHashesResumeWholeSet`, `needHashesPrincipal`, `needHashesConsistency`, `needHashesWindow`;
  - every reset: `resetFeedLagged`, `resetFeedReset`;
  - `topPaths` (40 paths: opens, reopensOnSocket, reconciles).

  Two 8-minute snapshots of it decompose the churn directly. Relaxed atomics plus a per-socket
  `HashSet<(path, args)>`, about 150 entries per connection.

### (2) Memory: RSS 1.39–1.65 GB with ~10 users

At 22:20 RSS was 1,450 MB (VmHWM 1,708 MB). Accounted by the metrics:

| owner | 22:12 | 22:20 | note |
|---|---|---|---|
| snapshot cache | 128 MiB / 1,854 entries | 129 MiB / 2,076 entries | 613 entries empty; keyed entries 8.8 MiB |
| …of which pre-deploy duplicates (21:29 generation change) | | **≥ 34 MiB** | 16 of the top 40 entries are the old generation's copies of the same (db, path, args), never hit again (evictions 0) |
| SQLite allocator | 104 MiB | 110 MiB | budget 128; up to 16 readers per tenant (`GONVEX_DB_MAX_OPEN_CONNS=16`) at 2.9 MB of schema each |
| V8 heaps | 122 MiB (5 isolates) | 97 MiB (4) | heap only; code and isolate overhead not counted |
| principal cache | 2.2 MiB | 2.3 MiB | |
| feed backlog | 0 (peak 1 MB) | 0 (peak 2.5 MB) | |
| **accounted** | **≈ 356 MiB** | **≈ 338 MiB** | |
| **unaccounted** | | **≈ 1,080 MiB** | Rust heap (per-connection replica state for ~1,680 subscriptions, delivery state, retained freed memory), V8 non-heap, thread stacks, mmapped tenant-file pages (`mmap_size` 256 MiB per file; touched pages count in RSS as file pages) |

- The unaccounted part cannot be split from these files. The RSS log has total RSS only, no
  `RssAnon`/`RssFile`.
- Lab reference: the same app shape idles at 546–640 MB RSS (anon 408–522 MB). A loaded lab plateau sat at
  ~1.1 GiB (anon ~0.9 GiB, page cache 0.35–0.42 GiB).
- `mem.malloc_trim` ran 5 times in 8 min, with p99 1.23 s.
- Next capture: `/proc/<pid>/smaps_rollup` (anon vs file), `/proc/<pid>/status`, and per-connection replica
  bytes.
- **Does a 256 MiB LRU hold the working set?**
  - On staging today, yes: 129 MiB including ≥ 34 MiB of dead duplicates, so ≲ 95 MiB live. That is
    effectively two active tenants: `tenant_a937c` ≥ 59 MiB in its top entries alone (one
    `notifications.listNotifications` entry is 17.7 MB / 13,185 rows), `tenant_754db` ~10 MiB.
  - The content-keyed cache (previous follow-up) stops deploys from duplicating entries.
- **Production, 26 tenants:** the cache scales with active tenants × their table sizes, not with users. Two
  el-rey-sized tenants (39 % + 39 % of users) at ~60–100 MiB each, plus 24 small ones at ≤ 10 MiB, is about
  **150–450 MiB**. That fits the 256 MiB LRU only at the low end. The small tenants' entries will be evicted and
  rebuilt (cheap for them), which is the intended behaviour. Estimate, not measured.
- **1k users:** the cache stays bounded by the budget. SQLite stays near its budget (raise it to 256 MiB for
  warm readers, previous follow-up). Per-connection state is the open question. It is the part of the
  unaccounted ~1 GB that grows with users, and these files cannot measure it; with 10 users it cannot even be
  separated from the baseline. The lab's 1k plateau (≈ 1.1 GiB) is the better data point.

### (3) Slow statements and slow acquires

- **The log covers 21:18:30–21:57:06**, so the 22:24:08 burst is not in it. The RSS sampler saw 528 % CPU at
  22:24:06, consistent with a burst.
- Of **2,454 slow-statement WARNs**, **2,286 (93 %) are reader connection setup** (`PRAGMA foreign_keys = ON;
  cache_size; mmap_size; temp_store`): p50 2.69 s, max 10.5 s. **Yes, it is the reader-open pattern again.**
- The rest are control-plane reads starved behind it:
  - `SELECT id,tenant_id,account_id,release,environment,last_seen…` ×121, p50 1.7 s;
  - session and membership checks ×16, 6–10 s;
  - `_gonvex_sync_clock` ×11.
- **1,578 slow-acquire WARNs.** Their per-minute counts follow the reader setups (21:28–21:45: 59–137/min). Only
  9 fall after 21:50 within the logged window (21:54).
- Reader opens keep happening in steady state: `sqliteReaderOpens` 5,717 → 6,115 between 22:12 and 22:20 is
  **47 opens/min with no restart**. That is `GONVEX_DB_IDLE_TIMEOUT_MS=15 s` (RC default) closing extra
  readers, plus up to 16 readers per database reopening in parallel at nice 10.
- The statements carry no database path, so the tenants can't be named from these logs.
- **Fix:** the settings from the previous follow-up (`GONVEX_SQLITE_READER_NICE=0`,
  `GONVEX_DB_MAX_OPEN_CONNS=4`, `GONVEX_SQLITE_CACHE_BUDGET_MB=256`) plus this branch's reader changes
  (120 s extra-reader idle, one open per database at a time).
