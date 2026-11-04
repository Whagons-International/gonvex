# 500 users: snapshot cache thrash, SQLite CPU and heap by owner (perf/cache-heap)

Branch `perf/cache-heap` (from `perf/integrate-5k` 18d101d6). Lab: own copy `lab-heap`
(containers `gvheap-*`, project `gvperf-heap`), same baseline data and app as `lab-5k-new`
(artifact 83669147), generator on the Mac, rl-500 parameters: thin clients + 8 SDK users,
500 users, 4 shards, 60 s ramp, 180 s plateau, presence v2, runtime 1 vCPU / 2 GiB.
Driver: `lab-heap/run.sh <label> [VAR=…]`; tables from `lab-heap/summarize-ch.py`.

## Diagnostics added (all off by default except the cache report)

| what | how |
|---|---|
| Snapshot cache entries | `/dev/metrics` → `memory.snapshotCache.top` (40 largest: db, path, args, rows, bytes, hits, builds, observed/admitted/scanned), `byPath`, `keysSeen`, `sparseDrops`, `sparseSkips` |
| SQLite time per statement | `GONVEX_SQLITE_STATEMENT_PROFILE=1`: auto-extension + `sqlite3_trace_v2(PROFILE)` on every connection; `memory.sqliteStatements` (count, total/max ns, VM steps, full-scan steps). Times are SQLite's wall clock, so they stretch under CPU starvation. A nested `pragma_table_info(...)` reports the whole outer statement's duration (its trace fires when the outer statement ends): read `PRAGMA table_info=…` rows as overlap, not extra work. |
| Reader churn | `memory.sqliteReaderOpens` |
| Heap by call site | feature `gonvex-runtime/jemalloc-prof` (jemalloc, sampled profiling): `_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19`, `GONVEX_HEAP_DUMP_PREFIX=/state/heap/h`, `GONVEX_HEAP_DUMP_SECS=30`; read with `jeprof --text --cum --show_bytes <binary> <dump>` (rewrite `/binhost/` in the dump's maps to the host path first) |

perf inside the container still does not resolve runtime frames (libc/kernel only); attribution
below comes from the cache report, the statement profile and the heap profiles.

## 1. Cache: root cause and fix

Entries at the eviction point (before, `ch-diag-500`, 883 entries, 259 MiB):

| path | entries | MiB | rows | hits | builds |
|---|---:|---:|---:|---:|---:|
| `notifications.listNotifications` | 5 | **182** | 142,599 | 45 | 5 |
| `tasks.recentWorkspaceTasks` | 5 | 50 | 21,501 | 457 | 5 |
| `roles.memberTeams` / `members.*` | 11 | 18 | 15,170 | 1,376 | 11 |
| per-task detail paths (`tasks.get`, `listTask*`, …, keyed by `taskId`) | ~900 | <0.5 | ~180 | ~260 | ~900 |

* **Root cause: `notifications.listNotifications`.** The load writes ~1,100 notification rows/s
  (the app's `pushNotifications.ts` fan-out: 100k–185k single-row `ctx.db.insert` plus ~400 bulk
  `insertDataRows` per plateau). The principal-free entry of a large tenant reached **66,435 rows /
  89 MB** while each member's visibility (`userId = member.id`) admits ~0.1 % of it. Three tenants'
  notification entries filled 70 % of the 256 MiB budget and evicted everything else
  (1,591 evictions in rl-500, 553–787 here). Every open also evaluated all 66k rows and every
  roll-forward copied them; their decoded ACL rows (`with_acl`) were another ~170 MiB of heap.
* Keys are **not** split per user: the 11 standard subscriptions all use `{}`; the per-task detail
  windows key by `taskId` as their equality filter requires (small, mostly 0 hits).
* Fix (52170e24): **sparse-entry bypass.** Each open records rows admitted vs. rows evaluated per key.
  An entry of ≥ 1,000 rows whose opens admit < 1/50 of it (after 3 opens) is dropped and its key
  reads SQL (visibility as an indexed predicate) for 10 min
  (`GONVEX_SNAPSHOT_CACHE_SPARSE_MIN_ROWS`/`_RATIO`/`_HOLD_SECS`; ratio 0 disables). Keys now use
  only the arguments the equality filters read (`cache_arguments`), so extra args cannot split
  an entry.

* **Per-member sparse decisions** (perf/resume-fix). The rule above was per key. On a
  mixed collection a few narrow opens (GrantOnly members see 45 of 10,500 tasks) dropped
  the entry and sent *every* member's open and resume back to the full SQL scan for
  10 minutes. Broad members lost the log-only resume: el-rey Admin resume 7.9 → 132 ms
  (`snapshot_cache` bench). Now:
  * a member whose opens admit < 1/50 of a ≥ 1,000-row entry is marked narrow, and its
    opens read SQL (as before, but only that member's);
  * a member admitting more is marked broad and always uses the cache;
  * a key is dropped and made sparse only when no open in its window (≤ 64 opens) was
    broad;
  * on a sparse key, members not known yet read SQL; one whose SQL result is broad
    revives the key.

  Members are remembered for `GONVEX_SNAPSHOT_CACHE_SPARSE_HOLD_SECS`, at most 16,384 per
  key. All-narrow keys such as notifications behave as before. Test: `tests/resume_log_only.rs`
  (valid-cursor resumes read 0 SQL rows, counted by `raw_sites`, on a mixed key and after a
  sparse drop).

Cache over the plateau:

| case | hit rate | builds/s | evictions | peak bytes | sparse skips |
|---|---:|---:|---:|---:|---:|
| before (ch-before-500) | 77 % | 7.7 | 553 | 255 MiB | – |
| after (ch-after-500 / -500b) | 81 % / 83 % | 8.1 / 7.5 | 0 / 0 | 74 / 75 MiB | 1,432 / 870 |
| after2 + nice (or10 / or10b) | 84 % / 86 % | 6.7 / 7.2 | 0 / 0 | 75 MiB | 959 / 1,059 |

Builds/s stays ~7: almost all remaining builds are per-task detail keys (cheap index lookups,
few hits). The omit-nulls null-free cache text (perf/omit-nulls) was not needed: bytes are no
longer the eviction problem (74 MiB of 256).

## 2. Heap by owner (jemalloc sampled live heap, MiB, peak of the plateau)

| owner (call site) | before (bcfd9109) | after (651a5df1) |
|---|---:|---:|
| change feed events (`deliver_committed → log_change_from_row`, parsed `LogChange` images held until the slowest connection of the tenant has received them; broadcast ring of 1,024 txns) | **363** | **278** |
| snapshot cache: decoded ACL rows (`CacheEntry::with_acl`) | 169 | 26 |
| snapshot cache: views / log rows of opens (`cached_view` minus ACL) | 24 | 2 |
| snapshot cache: entry rows (`build_cache_entry`) | 86 | 64 |
| `ParentAclRegistry::ensure_needed` (parent ACL indexes, fixed per tenant) | 84 | 84 |
| rest of `begin_collection_read` (resolved principals) | 27 | 38 |
| websocket connection tasks | 52 | 36 |
| delivery memo prewarm / `apply_feed_event_offloaded` (at the peak dump) | 19 / 24 | 36 / 44 |
| module artifact | 29 | 28 |
| **total sampled live heap** | **946** | **686** |

Process: anon peak 1,670 → 1,039–1,086 MiB, RSS 2,141 → 1,505–1,612 MiB (mimalloc runs).
The ~1.2 GiB "rest of the Rust heap" in rl-500 was mostly the notification cache entries plus their
decoded ACLs and the feed backlog, not per-connection replica state. **Not fixed: the feed
backlog** (bounded by the 1,024-transaction ring, not by bytes; a lagged receiver resets all of
its replicas, so shrinking the ring trades memory for reopen storms). Options for a follow-up:
keep change images as JSON text and parse lazily (most consumers read a few columns), or a byte
budget on the ring with backpressure on `deliver_committed`.

## 3. SQLite: top statements and fixes (whole run, wall seconds)

| statement | before | after2 | fix |
|---|---|---|---|
| change-log replay `… FROM _gonvex_sync_changes WHERE revision > $1 AND revision <= $2 AND table_name IN (json_each)` | 116 s, 7.3k calls, **1.0 G VM steps** | gone from the top | new index `gonvex_sync_changes_table (table_name, revision, ordinal) WHERE revision IS NOT NULL`; `resume_changes` binds one parameter per table so the planner picks it |
| visibility resolution `… memberTeams WHERE memberId = $1` (+ joins) | 39.5k calls, 84 M full-scan steps | indexed | **stale `sqlite_stat1`** (memberTeams recorded as 1 row) made the planner scan; writer runs `PRAGMA optimize=0x10002` at open and `PRAGMA optimize` 60 s after start then hourly (`GONVEX_SQLITE_OPTIMIZE_SECS`) |
| `SELECT DISTINCT table_name … WHERE revision = $1` (commit path) | – | 301 M VM steps in after (regression from the new index with stale stats) | `INDEXED BY gonvex_sync_changes_revision` |
| outbox claim / due (`status='pending' … OR status='processing' …`) | 47 M VM steps, 21 s | 2.2 M, 6 s | two index-backed branches; partial indexes `_gonvex_action_outbox_pending_path`, `_gonvex_action_outbox_processing` |
| notification inserts (single-row and bulk) | 33 s + 29 s | 63 s + 24 s | app workload (client repo); the bulk form's `pragma_table_info` per column appears as `PRAGMA table_info='notifications'` (overlap) |

Reader opens: 212–243 per plateau (≈1.3/s), unchanged.

## 4. Hasher

`foldhash::fast::RandomState` (seeded randomly per process; already in the lock) replaces SipHash
on per-event / per-row maps: `EventShare` decisions/outcomes/images, the delivery memo, the event
layout index, the cache view's replaced-key set and build duplicate check, and the store's
result-kind cache (which also no longer copies the SQL text per lookup). Keys are one tenant's
row ids, table names, argument JSON and server stream tokens; the maps live for one event / open
or are size-bounded; the random seed keeps offline collision crafting out. Not measured in
isolation (perf cannot resolve frames in the lab).

## 5. Priority (integration-thread question)

`GONVEX_OFFLOAD_NICE` (existing) and new `GONVEX_SQLITE_READER_NICE` (auto-extension on read-only
connections renices the sqlx worker thread). After2 binary, same load:

| case | errors | save p50/p95/p99 s | taskDelivery p50/p99 s | reducer fn.total p50/p99 | budget_exceeded | snapshot.build p50/p99 | CPU |
|---|---:|---|---|---|---:|---|---:|
| no nice (after2, repeat) | 1.7 % / 7.8 % | 6.0/31.4/43.4, 3.0/46.2/64.2 | 2.3/26.5, 2.2/40.6 | 0.83/3.27 s, 1.14/4.98 s | 0 / 0 | 6.8/11.1 s, 5.2/20.8 s | 1.00 |
| offload nice 10 | 29.8 % | 9.3/50.5/69.2 | 2.8/46.5 | 1.33/12.2 s | 13 | 6.9/26.7 s | 0.96 |
| offload + reader nice 10 (or10, or10b) | **0.2 % / 0.2 %** | **0.8/6.8/11.7, 1.1/14.5/21.7** | 0.7/6.5, 0.9/12.5 | 0.39/1.85 s, 0.41/3.30 s | 0 / 0 | 0.19/5.3 s, 2.1/6.4 s | 0.84 / 0.86 |

The isolate wait is the unaccounted share of reducer `fn.total` (94–99 % in slow traces), so
reducer `fn.total` is the isolate-wait signal here. Offload alone is worse (its snapshot SQL still
runs at normal priority on reader threads while the offload half waits); both together let the
writer and the isolate run first. Recommendation: `GONVEX_OFFLOAD_NICE=10
GONVEX_SQLITE_READER_NICE=10` in the lab compose / deployment; code defaults stay 0.

## All cases

| case | binary | errors | save p50/p95/p99 s | taskDelivery p50/p99 s | presence p50/p99 s | snapshot.build p50/p99 | reducer p50/p99 | delivery.queue_wait p50/p99 | anon peak | RSS |
|---|---|---:|---|---|---|---|---|---|---:|---:|
| ch-before-500 | bcfd9109 | 82.2 % | 29.7/86.5/112.1 | 13.6/81.6 | 0.71/15.27 | 7.57/39.4 s | 5.37/10.42 s | 8.72/98.2 s | 1,670 | 2,141 |
| ch-after-500 | 52170e24 | 0.6 % | 1.3/17.8/27.9 | 1.2/17.1 | 0.61/1.44 | 2.52/10.7 s | 0.60/1.80 s | 0.10/20.7 s | 1,073 | 1,580 |
| ch-after-500b | 52170e24 | 2.3 % | 1.9/20.3/32.1 | 1.5/17.9 | 0.61/1.49 | 4.83/9.2 s | 0.76/2.13 s | 0.11/17.1 s | 1,049 | 1,558 |
| ch-after2-500 | 651a5df1 | 1.7 % | 6.0/31.4/43.4 | 2.3/26.5 | 0.64/1.53 | 6.83/11.1 s | 0.83/3.27 s | 0.40/24.6 s | 1,086 | 1,612 |
| ch-nice-off-500 | 651a5df1 | 7.8 % (482 users: 19 client `fetch failed`) | 3.0/46.2/64.2 | 2.2/40.6 | 0.74/2.06 | 5.16/20.8 s | 1.14/4.98 s | 0.22/37.1 s | 1,039 | 1,505 |
| ch-nice-o10-500 | 651a5df1 | 29.8 % | 9.3/50.5/69.2 | 2.8/46.5 | 0.71/9.17 | 6.86/26.7 s | 1.33/12.2 s | 0.45/50.3 s | 1,286 | 1,682 |
| ch-nice-or10-500 | 651a5df1 | 0.2 % | 0.8/6.8/11.7 | 0.7/6.5 | 0.60/1.45 | 0.19/5.25 s | 0.39/1.85 s | 0.015/8.2 s | 991 | 1,473 |
| ch-nice-or10-500b | 651a5df1 | 0.2 % | 1.1/14.5/21.7 | 0.9/12.5 | 0.64/1.42 | 2.06/6.37 s | 0.41/3.30 s | 0.086/11.8 s | 1,033 | 1,587 |

Every case is still above the pass rule (p95 < 500 ms, p99 < 1 s) at 500 users. Oracle: 0 leaked
rows in every case (missing rows only in backlogged cases). One run per case except where
repeated; run-to-run spread is large (after vs after-b, after2 vs nice-off). after2's outbox fix
lets the drainer claim ~60 % more actions per plateau (8.7k vs 5.4k), which competes with user
work on the single vCPU; that may explain why after2 sits slightly behind after.

## Tests

* `cargo test --workspace --no-fail-fast` (debug, featureless): **646 passed, 0 failed, 7 ignored**.
* `cargo test -p gonvex-runtime --features perf-fixture --no-fail-fast`: 401 passed, 1 failed,
  8 ignored; every oracle suite passes (`snapshot_cache_oracle`, `delivery_oracle`,
  `raw_rows_oracle`, `class_sharing_oracle`, `visibility_fanout`). The failure,
  `live_query::tests::exact_count_parameter_type_changes_match_rows_on_one_connection`
  ("row and count statements must both remain cached"), passed in the featureless run and 5/5
  alone. It is flagged as load-sensitive, not explained.
* New unit tests: `snapshot_cache::tests::sparse_entries_are_dropped_and_their_key_skips_the_cache`,
  `cache_arguments_keep_only_equality_filter_arguments`, `statement_profile::tests::normalizes_whitespace`.

## Risks

* Sparse bypass moves those opens to SQL: correct by construction (it is the existing fallback),
  but a collection whose SQL visibility is *not* index-backed would get slower. Thresholds are env
  knobs; ratio 0 disables.
* `PRAGMA optimize` at writer open and hourly runs ANALYZE when row counts moved ~10×: a schema
  cookie bump makes every connection re-prepare once. With `analysis_limit=400` it is bounded.
* New indexes on `_gonvex_sync_changes` and `_gonvex_action_outbox` add write cost per change
  row / outbox row; they are created at boot through the infrastructure bootstrap (hash change).
* `INDEXED BY` fails the statement if the index were missing; the infrastructure check requires it.
* Store timing tests `connection_budget` / `writer_durability` failed once under a parallel
  workspace run and pass 3/3 alone (known load-sensitive).
