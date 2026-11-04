# Change-feed backlog: byte-bounded, catch-up from the change log (perf/feed-backlog)

Branch `perf/feed-backlog` (from `perf/cache-heap` 36c440d6). Follow-up A of `tasks/next-500.md`:
the largest heap owner at 500 users (default mix, cache-heap) was the change-feed backlog: 278–363
MiB of parsed change images held for the slowest connection of a tenant, bounded at 1,024
transactions, not bytes. At 1,000 users (integration step log 8d39925b) the "rest" of the heap was
~1.2 GiB, with a 24 s delivery queue and saves at 91 s p50: a call completes only when the
replica watermark covering its commit reaches the caller.

**Result in one paragraph.** The feed's parsed images are now byte-bounded (64 MiB estimate,
~85–97 MiB sampled at 1k, previously up to 208 MiB at 1k and 278–363 MiB at 500 on the default
mix). When connections keep up the feed holds ~nothing (as before). A connection behind the
budget, or whose ring slot was overwritten, catches up from the durable change log by its own
cursor instead of resetting all its subscriptions or pinning memory for everyone. In the
micro-driver a stalled connection costs 70 MiB instead of 825–1,649 MiB and receives every
transaction instead of losing 476 and resetting. **At 1,000 users the runtime stays CPU-bound
(1.00 core in every run) and outcomes vary from 2 % to 63 % errors between identical runs; across
2 base and 11 after runs no variant improves save or delivery latency reproducibly.**

## What changed

1. **The ring carries signals, not images** (`change_feed/backlog.rs`). Each tenant's broadcast
   ring (4,096 slots, `GONVEX_FEED_RING_CAPACITY`) holds a ~40-byte `FeedSignal` (revision,
   epoch, `Weak` to the parsed images, an `unread` token). Strong references live in a per-tenant
   `FeedStore`:
   * budget: process-wide `GONVEX_FEED_BACKLOG_BYTES` (default 64 MiB of *estimated* heap;
     jemalloc measures ~1.4–1.5× the estimate) and ≤ 1,024 transactions; the newest always stays;
   * release: a transaction every receiver has taken is dropped at once (the ring frees a slot's
     value when its last receiver reads it, killing the token; the last reader sweeps the store).
2. **Catch-up from the change log by cursor** (`FeedReceiver`, now returned by
   `ChangeFeedHub::subscribe`). A receiver whose event was evicted re-reads `(cursor, …]` from
   `_gonvex_sync_changes` (`read_changes`, revision index) in 16-revision chunks, one event per
   revision, in order. On `Lagged` it fills the gap the same way instead of resetting every
   subscription. Cursor before `retained_revision`, changed epoch or read error → `Reset` (the
   previous behaviour). Refills of a tenant are single-flight and land in a shared refill cache
   (`GONVEX_FEED_REFILL_BYTES`, default 32 MiB), so receivers at about the same position read the
   log once. `recv` is cancel-safe (refills run in a spawned task); `try_recv` (catch-up merge
   path) starts a refill and reports empty; `len`/`is_empty` include held/refilling events.
3. **Row images are shared** (`LogChange.old_value/new_value: Arc<Value>`): catch-up merges
   (`merge_transactions`, up to 256 events per catching-up connection) deep-copied every image —
   up to 212 MiB sampled in one 1k run (after3-1k-b).
4. **Event shares** (`feed_share.rs`, lead from the fast-hash thread: lagging connections recompute
   each event's memo, decisions, `Layout` and outcomes once it leaves the 4-event window). The
   window is now `GONVEX_EVENT_SHARES_MAX` (default **64**, was a fixed 4) with a by-revision index;
   within it a share is dropped as soon as no one else holds its images (front sweep per creation,
   full sweep every 64), and a connection's own merged event gets an unstored share. Measured with
   a 1,024 window: the recompute does go away (1,489–2,423 shares created vs 0.6–1.1 M found per
   plateau when connections kept up), but shares then held 170–205 MiB instead of 69–82 and
   latency did not improve reproducibly, hence the default of 64.

Metrics: `/dev/metrics` → `memory.feedBacklog` (`bytes`, `peakBytes`, `entries`, `evicted`,
`released`, `fills`, `fillRevisions`, `fillEvents`, `fillChanges`, `fillMs`, `lagResumes`,
`fillResets`, `sharedRefillEvents`, `refillBytes`) and `memory.eventShares` (`created`, `found`,
`live`).

Protocol / client contract: unchanged. Internal API: `FeedReceiver` replaces
`broadcast::Receiver<FeedEvent>` (connection loop, `merge_waiting_feed`, parent-ACL and
permission appliers, dashboard); `From<broadcast::Receiver<FeedEvent>>` keeps plain channels for
tests. `LogChange` images are `Arc<Value>` (deref to `&Value`; constructors wrap).

## Micro-driver (`benches/feed_backlog.rs`, shared fixture, 1,500 transactions of task updates)

`FEED_FAST` receivers take every transaction as broadcast; `FEED_SLOW` are stalled during the
writes, then commit their own "save" and drain concurrently. *Save visible* = time until that
receiver's feed reaches the save's revision (its watermark, hence its call result). Base =
36c440d6 + the same bench; after = 0a549c62 (bench paths unchanged since).

| scenario | case | heap held while stalled | stalled receivers got | save visible on a behind connection | fast p99 |
|---|---|---:|---|---:|---:|
| 500-shape: 40 fast, 10 stalled, 50 rows/txn | base | 825 MiB | 1,025 of 1,501: **Lagged** (476 lost → reset + reopen of every subscription) | 17–140 ms + reopen (not measured) | 188 ms |
| | after | **70 MiB** (118 in drain) | all 1,501 in order | **4.2 s** (replays 1,414 evicted txns: 89 log reads, 12,726 events shared) | 10 ms |
| 1k-shape: 80 fast, 20 stalled, 100 rows/txn | base | 1,649 MiB | 1,025 of 1,501, Lagged | 37–344 ms + reopen | 329 ms |
| | after | **70 MiB** (131 in drain) | all 1,501 | **8.8 s** (92 log reads, 141k changes re-parsed, 27,683 shared) | 25 ms |
| keeping up: 40 fast, 0 stalled | base / after | 1 / 1 MiB | – | – | 6 / 4 ms |

Without single-flight sharing, 10 concurrent stalled receivers took 35.5 s (each read the log);
with it 4.2 s and the same 89 reads as one. Replay cost is ~55 µs per change (SQL read + JSON
parse of both images). Fast-receiver p99 is noisy under host load.

## Lab: 500 and 1,000 users, production mix

Own lab `lab-feed` (copy of lab-heap, containers `gvfeed-*`); harness = integration
`bench/whagons` at 8d39925b (production mix, preforked proxy); runtime 1 vCPU / 2 GiB;
`GONVEX_OFFLOAD_NICE=10 GONVEX_SQLITE_READER_NICE=10`; jemalloc-prof builds on both sides; 180 s
plateau; 500 = 4 shards / 60 s ramp, 1k = 8 shards / 90 s ramp. Before = 651a5df1 (cache-heap
code). Driver `lab-feed/run*.sh`, table `lab-feed/summarize-ch.py`, raw data in
`lab-feed/<case>/`. Commits: de32b28e backlog; 0a549c62 + release; dfa4f604 + 1,024 share
window; d6bfe832 + shared images; 719f1d7b + share sweeps (noarc = 719f1d7b without d6bfe832).

**500 users**

| case | binary | errors | save p50/p95/p99 s | delivery.queue_wait p50/p99 | snapshot.build p50/p99 | reducer fn.total p50/p99 | CPU | anon peak MiB |
|---|---|---:|---|---|---|---|---:|---:|
| fb-before-500 | before | 0.1 % | 0.0/0.2/0.5 | 1.2 ms/198 ms | 6 ms/392 ms | 21 ms/282 ms | 0.39 | 1,008 |
| fb-before-500h | before | 0.1 % | 0.1/0.5/0.9 | 1.7 ms/339 ms | 11 ms/524 ms | 36 ms/570 ms | 0.52 | 853 |
| fb-after-500 | de32b28e | 0.1 % | 0.0/0.3/0.9 | 1.1 ms/242 ms | 6 ms/315 ms | 23 ms/327 ms | 0.36 | 1,180 |
| fb-after2-500 | 0a549c62 | 0.1 % | 0.1/0.7/1.2 | 1.8 ms/449 ms | 10 ms/2.93 s | 39 ms/518 ms | 0.52 | 861 |
| fb-after3-500 | dfa4f604 | 0.1 % | 0.0/0.3/0.5 | 1.3 ms/174 ms | 7 ms/321 ms | 27 ms/316 ms | 0.43 | 949 |

At 500 (production mix) no connection falls behind: base held 1–5 MiB of feed images; the first
after build held its whole budget (~95 MiB sampled) until the release token (0a549c62: 0 MiB,
0 refills). Sampled heap: before-500h 372–396 MiB, after3-500 376–396 MiB.

**1,000 users** (every run at 0.97–1.00 cores; anon peak in MiB)

| case | binary | errors | save p50/p95/p99 s | delivery.queue_wait p50/p99 | refills (changes re-read) | anon |
|---|---|---:|---|---|---|---:|
| fb-before-1k | before | 21.4 % | 32.2/99.1/118.7 | 11.2 s/95.0 s | – | 1,316 |
| fb-before-1k-b | before | 11.3 % | 6.0/59.9/82.2 | 72 ms/49.3 s | – | 1,244 |
| fb-after-1k | de32b28e | 19.9 % | 26.2/90.2/115.5 | 8.4 s/80.7 s | 723 (435k) | 1,232 |
| fb-after3-1k | dfa4f604 | 2.3 % | 0.6/24.2/38.2 | 6 ms/21.4 s | 3 (1.6k) | 1,257 |
| fb-after3-1k-b | dfa4f604 | 10.5 % | 6.6/52.3/77.9 | 766 ms/41.6 s | 56 (24k) | 1,711 |
| fb-after4r-1k | d6bfe832 | 31.4 % | 52.9/135.1/162.1 | 14.2 s/116.9 s | 169 (96k) | 1,792 |
| fb-after4r-1k-b | d6bfe832 | 44.4 % | 64.4/162.4/191.5 | 14.9 s/113.4 s | 845 (458k) | 1,681 |
| fb-after5-1k | 719f1d7b | 19.0 % | 33.2/106.1/135.5 | 8.4 s/98.6 s | 949 (489k) | 1,369 |
| fb-after5-1k-b | 719f1d7b | 38.5 % | 37.0/86.2/94.3 | 16.4 s/120.9 s | 1,246 (789k) | 1,297 |
| fb-noarc-1k | noarc | 29.6 % | 39.3/116.3/142.9 | 14.2 s/102.9 s | 1,122 (613k) | 1,393 |
| fb-noarc-1k-b | noarc | 23.3 % | 37.0/108.7/134.6 | 11.3 s/102.9 s | 1,333 (836k) | 1,320 |
| fb-after5-256-1k | 719f1d7b, 256 MiB budget | 63.1 % | 78.9/162.3/187.9 | 15.3 s/129.1 s | 0 | 1,599 |
| fb-after5-256-1k-b | 719f1d7b, 256 MiB budget | 17.6 % | 30.4/93.4/121.6 | 11.3 s/83.9 s | 0 | 1,552 |

Invalid and left out: fb-after2-1k (overlapped this thread's test builds), two fb-after4 attempts
(host memory-pressure stop; 248 users never connected). Host memory PSI ≤ 1.5 % in the runs above.

Reading: identical binaries differ by 2–4× in errors and p50s (after3 2.3 % vs 10.5 %; 256 MiB 63 %
vs 18 %). Runs either keep up (queue wait p50 in ms, few refills) or fall into a lag spiral (queue
wait p50 8–16 s, 0.4–0.8 M changes re-read). Neither shared images, share sweeps nor a 4× budget
(0 refills) moved the band, so log refills are not what drives the spiral; the integration
thread's binding constraint at 1k (CPU in snapshot builds, then delivery) applies. Oracle: 0
leaked rows in every case; the recurring "236 missing" mismatch at 1k appears on the base too.

### Heap by owner at 1k (jemalloc sampled live heap, MiB, 6 plateau dumps, min–max)

| owner | before-1k | before-1k-b | after-1k | after3-1k | after4r-1k | after5-1k | noarc-1k-b |
|---|---:|---:|---:|---:|---:|---:|---:|
| change-feed images (`deliver_committed`) | 7–208 | 5–166 | 81–95 (+~30 refills) | 8–82 | 92–176 | 87–97 | 85–92 |
| delivery / event shares (`apply_feed_event_offloaded`) | 69–82 | 48–79 | 75–80 | 106–147 | 96–272 | 98–204 | 99–185 |
| catch-up merges (`merge_transactions`) | 0 | 0 | 0 | 1–19 | 11–83 | 0 | 0 |
| `begin_collection_read` | 133–152 | 135–151 | 131–148 | 133–145 | 138–145 | 144–152 | 129–136 |
| **total sampled** | **518–743** | **509–699** | **588–650** | **541–649** | **680–1,105** | **677–797** | **671–772** |

(after3-1k-b, not tabled: merges up to 212 MiB, total up to 1,054 — fixed by item 3. The
share columns are with the 1,024 window; the default is now 64.)

## Call completion when a connection is behind

* A call resolves when the caller's watermark covers its commit, i.e. after its feed has applied
  everything up to it. Base: behind by more than 1,024 transactions → `Lagged` → every
  subscription reset and reopened (calls wait for the reopened snapshots); within the ring it
  waits behind the in-memory backlog (1k: queue wait p50 0.07–11 s, saves 6–32 s p50).
* After: never a lag reset while the cursor is retained. The behind connection replays in order,
  from memory while within the budget and from the log beyond it. In the micro-driver that is
  ~2.8 ms (50-row) / ~6 ms (100-row) per evicted transaction: 4.2 s / 8.8 s for 1,414.
* The replay is dominated by parsing both images of every change (~55 µs per change). Not done:
  a collapsed catch-up read (one image per row per range, first old / last new, as
  `merge_transactions` does after the fact), which would cut parse work by the number of times a
  row changed in the range; for a connection very far behind, a reset may be cheaper than a replay.

## Tests

* `cargo test -j 3 -p gonvex-runtime --features perf-fixture --no-fail-fast -- --test-threads=4`
  on the final code: **403 passed, 0 failed, 8 ignored**. (On 0a549c62 the load-sensitive
  `call_watermark_latency::a_call_whose_commit_sends_the_caller_no_rows_completes_within_50_ms`
  failed once at 61.7 ms with the lab running; 3/3 alone.)
* `cargo check --workspace --tests`: clean. `cargo clippy -p gonvex-runtime --features perf-fixture`:
  no `redundant_clone` in touched files (the lint is already on for the crate).
* New `tests/feed_backlog.rs` (1-byte budget, 32-slot ring): evicted transactions come back from
  the log; lagged receivers resume without reset; `recv` and `try_recv`; every delivered image
  equals the log; the store keeps at most the newest transaction.

## Risks

* Beyond the budget, a behind connection costs SQLite reads and JSON parsing on the shared reader
  pool instead of memory (at 1k: up to 1,333 refills / 0.84 M changes per 180 s plateau).
* The budget is an estimate (~1.4–1.5× undercount).
* A connection behind by a long way replays every retained transaction (merged in batches by the
  existing catch-up merge) instead of resetting.
* `LogChange` images are `Arc<Value>`: an in-place edit would need `Arc::make_mut` (only tests do).
* Event-share window 64 (was 4): up to 64 events' decisions/outcomes per tenant stay alive while
  referenced.

## Not measured

* Reopen cost of a base connection after `Lagged`; the thin-client generator does not report
  replica resets, so whether lab connections hit it is unknown.
* CPU by function in the lab (container perf does not resolve runtime frames): the share effect is
  shown by counters and profiler stages, not by `Layout` samples.
* The final default (64-event share window) in the lab: the 1k lab runs used the
  1,024 window; the micro-driver and tests cover the final code.
* Default (non-production) mix at 500; mimalloc builds.
