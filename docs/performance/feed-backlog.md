# Change-feed backlog: byte-bounded, log catch-up, long-lived event shares (perf/feed-backlog)

Branch `perf/feed-backlog` (from `perf/cache-heap` 36c440d6). Follow-up A of `tasks/next-500.md`:
the largest heap owner at 500 users was the change-feed backlog (278–363 MiB of parsed change
images held for the slowest connection of a tenant, bounded at 1,024 transactions, not bytes).
At 1,000 users (integration step log, 8d39925b) the "rest" of the heap was ~1.2 GiB with a
24 s delivery queue, and saves took 91 s at p50 because a call completes only when the replica
watermark covering its commit reaches the caller.

## What changed

1. **The ring carries signals, not images** (`change_feed/backlog.rs`). Each tenant's
   broadcast ring (now 4,096 slots, `GONVEX_FEED_RING_CAPACITY`) holds a ~40-byte
   `FeedSignal`: revision, epoch, a `Weak` to the parsed images and an `unread` token.
   Strong references live in a per-tenant `FeedStore`:
   * **budget**: process-wide `GONVEX_FEED_BACKLOG_BYTES` (default 64 MiB of *estimated*
     heap; jemalloc measures ~1.4–1.5× the estimate) and at most 1,024 transactions. The
     newest transaction always stays.
   * **release**: a transaction every receiver has taken is dropped at once (the ring frees a
     slot's value when its last receiver reads it, so the `unread` token dies; the last reader
     sweeps the store). A feed whose connections keep up holds almost nothing, like the base.
2. **Catch-up from the change log by cursor** (`FeedReceiver`, returned by
   `ChangeFeedHub::subscribe`). A receiver that finds its event evicted re-reads
   `(cursor, …]` from `_gonvex_sync_changes` (the existing `read_changes`, revision index) in
   chunks of 16 revisions, one event per revision, in order. `Lagged` (ring overwritten) no
   longer resets every subscription when the receiver knows its cursor: it fills the gap from
   the log the same way. A cursor older than `retained_revision`, a changed epoch or a read
   error yields `FeedEvent::Reset` (the previous behaviour).
   * Refills of one tenant are **single-flight** (a per-tenant async lock) and their events go
     into a shared refill cache (`GONVEX_FEED_REFILL_BYTES`, default 32 MiB, oldest first):
     receivers at about the same position read the log once.
   * `recv` is cancel-safe (it runs in `select!`): a received signal and a running refill
     (spawned task) stay in the receiver.
   * `try_recv` (the catch-up merge path, `delivery::merge_waiting_feed`) starts a refill and
     reports empty; `len`/`is_empty` count held, stashed and refilling events.
3. **Event shares live as long as their event** (`feed_share.rs`, lead from the fast-hash
   thread). `EventShares` kept the 4 newest events per tenant, so a connection more than 4
   events behind recomputed the event's eval memo, class decisions, fan-out `Layout` and shape
   outcomes. Shares now stay while their images are held anywhere else (store, refill cache, a
   connection still applying them), with a by-revision index, a cap of 1,024 per tenant and
   the 4 newest always kept. Counters: `/dev/metrics` → `memory.eventShares`
   `{created, found, live}`.

Metrics: `/dev/metrics` → `memory.feedBacklog` (`bytes`, `peakBytes`, `entries`, `evicted`,
`released`, `fills`, `fillRevisions`, `fillEvents`, `fillChanges`, `fillMs`, `lagResumes`,
`fillResets`, `sharedRefillEvents`, `refillBytes`).

Protocol and client contract: unchanged. The connection loop, the delivery code, parent ACL /
permission appliers and the dashboard take a `FeedReceiver` instead of a
`broadcast::Receiver<FeedEvent>` (`From<broadcast::Receiver<FeedEvent>>` keeps tests and benches
on plain channels).

## Micro-driver (`benches/feed_backlog.rs`, shared fixture tenant, 1,500 transactions)

`FEED_FAST` receivers take every transaction as it is broadcast; `FEED_SLOW` are subscribed but
stalled during the writes, then make their own commit (a "save") and drain concurrently. Save
visible = time until that receiver's feed reaches the save's revision (when its watermark, and
so its call result, can be sent). Base = 36c440d6 with the same bench file.

| scenario | case | heap held while stalled | stalled receivers got | save visible on a behind connection | fast p99 |
|---|---|---:|---|---:|---:|
| 500-shape: 40 fast, 10 stalled, 50 rows/txn | base | 825 MiB | 1,025 of 1,501, **Lagged** (476 lost → reset + reopen of every subscription) | 17–140 ms (+ reopen, not measured) | 188 ms |
| | after | **70 MiB** (peak in drain 118) | all 1,501 in order, 0 lag | **4.2 s** (replay of 1,414 evicted txns, 89 log reads, 12,726 events shared) | 10 ms |
| 1k-shape: 80 fast, 20 stalled, 100 rows/txn | base | 1,649 MiB | 1,025 of 1,501, Lagged | 37–344 ms (+ reopen) | 329 ms |
| | after | **70 MiB** (peak in drain 131) | all 1,501, 0 lag | **8.8 s** (92 log reads, 141k changes re-parsed, 27,683 shared) | 25 ms |
| keeping up: 40 fast, 0 stalled | base | 1 MiB | – | – | 6 ms |
| | after | 1 MiB | – | – | 4 ms |

One stalled receiver alone: 3.4–4.2 s drain for 1,414 refilled transactions (70,700 changes,
~55 µs per change for SQL read + JSON parse of both images). Before single-flight sharing, 10
concurrent stalled receivers took 35.5 s (each read the log itself); with it 4.2 s and the
same 89 reads as one. Fast-receiver latencies in this bench are noisy under host load (the
base's high p99 is partly allocation pressure of the 0.8–1.6 GiB backlog, partly noise).

## Lab: 500 and 1,000 users, production mix

Own lab `lab-feed` (copy of lab-heap, containers `gvfeed-*`), harness = integration
`bench/whagons` at 8d39925b (production mix, preforked proxy), runtime 1 vCPU / 2 GiB,
`GONVEX_OFFLOAD_NICE=10 GONVEX_SQLITE_READER_NICE=10`, jemalloc-prof builds of both sides,
180 s plateau; 500 = 4 shards / 60 s ramp, 1k = 8 shards / 90 s ramp. Before = 651a5df1
(cache-heap code, `bin-after2-je`); after = 0a549c62 (backlog only) and dfa4f604 (+ event
shares). Driver `lab-feed/run.sh`, table `lab-feed/summarize-ch.py`.

| case | binary | errors | save p50/p95/p99 s | taskDelivery p50/p99 s | delivery.queue_wait p50/p99 | snapshot.build p50/p99 | reducer fn.total p50/p99 | CPU | anon peak MiB |
|---|---|---:|---|---|---|---|---|---:|---:|
| fb-before-500 | before | 0.1 % | 0.0/0.2/0.5 | 0.1/0.6 | 1.2 ms/198 ms | 6 ms/392 ms | 21 ms/282 ms | 0.39 | 1,008 |
| fb-before-500h (repeat) | before | 0.1 % | 0.1/0.5/0.9 | 0.1/1.0 | 1.7 ms/339 ms | 11 ms/524 ms | 36 ms/570 ms | 0.52 | 853 |
| fb-after-500 | de32b28e | 0.1 % | 0.0/0.3/0.9 | 0.1/0.9 | 1.1 ms/242 ms | 6 ms/315 ms | 23 ms/327 ms | 0.36 | 1,180 |
| fb-after2-500 | 0a549c62 | 0.1 % | 0.1/0.7/1.2 | 0.1/1.1 | 1.8 ms/449 ms | 10 ms/2.93 s | 39 ms/518 ms | 0.52 | 861 |
| fb-after3-500 | dfa4f604 | 0.1 % | 0.0/0.3/0.5 | 0.1/0.6 | 1.3 ms/174 ms | 7 ms/321 ms | 27 ms/316 ms | 0.43 | 949 |
| fb-before-1k | before | 21.4 % | 32.2/99.1/118.7 | 13.5/91.5 | 11.2 s/95.0 s | 17.3 s/31.2 s | 86 ms/1.53 s | 0.98 | 1,316 |
| fb-after-1k | de32b28e | 19.9 % | 26.2/90.2/115.5 | 10.1/82.5 | 8.4 s/80.7 s | 15.5 s/25.5 s | 99 ms/1.70 s | 0.98 | 1,232 |
| fb-after2-1k ¹ | 0a549c62 | 68.9 % | 36.0/150.6/183.3 | 12.3/149.5 | 9.4 s/66.7 s | 17.4 s/34.8 s | 86 ms/2.87 s | 1.00 | 1,410 |
| **fb-after3-1k** | **dfa4f604** | **2.3 %** | **0.6/24.2/38.2** | **0.3/21.5** | **6.4 ms/21.4 s** | **4.5 s/9.5 s** | **34 ms/1.56 s** | **0.89** | 1,257 |
REPEATS

¹ Ran while this thread's full test suite and oracle builds loaded the host; the same
overload signature as the integration thread's ch-1k-prod-b (saves pending at teardown,
metadata subscriptions never current). Not counted.

At 500 (production mix) no connection falls behind: before the backlog held 1–5 MiB, the first
after build held its whole 64 MiB budget (fixed by the release token in 0a549c62: 0 MiB, 836
releases, 0 refills). At 1k with the backlog alone (after-1k) connections still fell behind:
723 refills re-read 435k changes over the plateau (26 s of refill wall time, 19.9k events
served from the shared refill cache), heap fell but latency barely moved. With long-lived event
shares (after3-1k) the plateau created **1,669 shares and found them 1,056,887 times**; the
connections stayed current (queue wait p50 6 ms), the log path was nearly idle (3 refills) and
the runtime was no longer saturated (0.89 cores).

### Heap by owner (jemalloc sampled live heap, MiB, plateau dumps every 30 s, min–max)

| owner | before-500h | after3-500 | before-1k | after-1k | after3-1k |
|---|---:|---:|---:|---:|---:|
| change-feed images (`deliver_committed → log_change_from_row`) | 1–5 | 0–4 | 81–208 | 81–95 (+~30 refill copies) | 8–82 |
| delivery / event shares (`apply_feed_event_offloaded`) | – | 38–69 | 69–82 | 75–80 | 106–147 |
| `begin_collection_read` | 106–114 | 109–113 | 133–152 | 131–148 | 133–145 |
| parent ACL (`ensure_needed`) | 80 | 81 | 77 | 71 | 76 |
| snapshot cache ACL rows (`with_acl`) | 23–31 | 24–32 | 29–64 | 26–41 | 28–31 |
| **total sampled** | **372–396** | **376–396** | **518–743** | **588–650** | **541–649** |

Longer-lived shares cost 30–65 MiB more in `apply_feed_event_offloaded` at 1k (decisions,
layouts, outcomes of up to 751 live events); the feed images they replace were up to 208 MiB.

## Call completion when a connection is behind

* A call resolves when the connection's watermark covers its commit, i.e. when that
  connection's feed has applied every transaction up to it. Behind by N transactions means
  waiting for N applies plus, now, re-reading the evicted ones from the log.
* Base: a connection behind by more than the 1,024-transaction ring loses the overwritten
  events (`Lagged`) and resets every subscription (`ReplicaReset change-feed-lagged`); its
  calls then wait for the reopened snapshots (not measured here). Within the ring, it waits
  behind the in-memory backlog (1k: queue wait p50 11 s → save p50 32 s).
* After: no reset; the behind connection replays from the log in order. The bench puts that at
  ~2.8 ms (500-shape) / ~6 ms (1k-shape) per evicted transaction of 50/100 rows (4.2 s / 8.8 s
  for 1,414). In the 1k lab the large gain came from not falling behind in the first place
  (event shares), not from faster catch-up: after3-1k save p50 0.6 s, p95 24 s, p99 38 s.
* The tail (p95 24 s) is still connections that fall behind; their replay is dominated by
  parsing both images of every change (~55 µs per change). Not done here: a collapsed catch-up
  read (one image per row and range, first old / last new, as `merge_transactions` does after
  the fact) would cut that by the number of times a row changed in the range.

## Tests

* `cargo test -j 3 -p gonvex-runtime --features perf-fixture --no-fail-fast -- --test-threads=4`
  (on 0a549c62): 402 passed, 1 failed, 8 ignored. The failure,
  `call_watermark_latency::a_call_whose_commit_sends_the_caller_no_rows_completes_within_50_ms`
  (call 0 at 61.7 ms, the others 38–47 ms, debug build, host loaded by the lab run), passed 3/3
  alone. Load-sensitive timing test.
* After the event-share change (dfa4f604): `class_sharing_oracle`, `delivery_oracle`,
  `visibility_fanout`, `snapshot_cache_oracle`, `raw_rows_oracle`, `feed_backlog`, lib
  `feed_share` tests: all pass. FINALTESTS
* New: `tests/feed_backlog.rs` (1-byte budget, 32-slot ring): evicted transactions come back
  from the log; lagged receivers resume without reset; `recv` and `try_recv` paths; every
  delivered image equals the log; the store holds at most the newest transaction.
* Lab oracle: 0 leaked rows in every case (the "236 missing" visibility mismatch at 1k also
  appears on the base).

## Risks

* A connection behind the budget now costs SQLite reads and JSON parsing (on the shared reader
  pool) instead of memory; at 1k with shares it was rare (3 refills), with the backlog alone it
  was 723 refills / 435k changes in 180 s. `GONVEX_FEED_BACKLOG_BYTES` trades memory for that.
* The byte estimate undercounts (~1.4–1.5× real heap); the budget is in estimated bytes.
* Refills read `read_changes` after the commit; a change pruned meanwhile (30-day retention)
  yields a reset, as before.
* Event shares are kept while their images are referenced; a connection holding an event for
  a long time keeps its share (cap 1,024 per tenant, head-of-line sweep from the oldest).
* A behind connection now applies every transaction (merged in batches by the existing catch-up
  merge) instead of resetting; for a connection behind by hours that is slower than a reopen.
  Bounded by `retained_revision` only.
* `delivery.visibility` per applied event rose at 1k after3 (p50 0.6 → 3.3 ms): connections now
  apply events one by one instead of merging long backlogs (frames per event p50 6 → 9.5). Not
  analysed further.

## Not measured

* Reopen cost of a base connection after `Lagged` (snapshot reopen of every subscription); the
  thin-client generator does not report replica resets, so whether lab connections hit it is
  unknown.
* CPU by function for the lab runs (perf inside the container does not resolve runtime
  frames); the share effect is shown by the share counters and profiler stages, not by
  `Layout` samples.
* Default (non-production) mix; mimalloc builds (all lab runs are jemalloc-prof).
