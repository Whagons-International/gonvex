# Bursts and overload control (perf/burst-control)

Branch `perf/burst-control` (from `perf/integrate-5k` 165b5a2f). Brief: step 13 section C
(`tasks/step13.md`). At 1k users (production mix, step 13) the runtime averages 0.64 cores, but
tails come from snapshot opens waiting 5–8 s (p90) for a permit and a delivery queue p99 of 7–11 s,
and runs are bistable.

**Result in one paragraph.** The delivery-queue tail was not slow delivery. It was a socket
*refusing* to deliver: the connection loop deferred every feed event while any of its replica
opens was pending, including opens that were only queued for one of the 2 process-wide snapshot
permits. A user who opens a task window and saves it waits for their own save's watermark until
the whole window has been admitted and built, which takes seconds under a burst. Burst control
removes that coupling (a socket defers its feed only while an open may hold a read clock),
coalesces the feed for connections that fall behind (one merged apply instead of one per
transaction, then the user's inbound frames), and bounds the snapshot admission queue with a
fail-fast retryable error. On the burst micro-driver (1 pinned CPU, overloaded):
**save p95 3.3–3.6 s → 96–110 ms, save p99 3.6–4.1 s → 138–173 ms, `delivery.queue_wait` p99
3.8–4.2 s → 23–25 ms**. The spread between runs is also smaller. Snapshot opens stay CPU-bound:
they wait about as long as before, but now they are the only thing that waits. At light load
nothing changes (slightly better). Short-first snapshot lanes are implemented but opt-in, because
they tripled heavy-reconnect p99.

**Round 2 (after integration step 14): merge by reference.** In the lab the first round's merge cost
~22 % CPU at 500 users (default mix 0.65 → 0.79 cores). Each lagging connection built its own
merged copy of the change lists, so it found none of the shared per-event work: decision shares
and the encoded-item memo (share reuse fell from ~307 to ~2.5 per share). Lagging connections now
take the waiting events **by reference**: the broadcast's own `Arc` change lists, unmerged, in
commit order. They apply them one after another through each event's existing share, in one
offload job, and the batcher still sends one frame per tick. On the driver: share reuse 2.7–6.1
(copy) → **440–470** found per created, shared decisions ×1.4–2.5, CPU per save −4 % (mid load)
and −7 % (heavy), save p95 at the heavy point 345–376 → **178–185 ms**, with 6–7 % more saves
completed. See "Round 2" below.

## What was happening (the coupling)

`websocket()` in `lib.rs`: `defer_feed = preparing_replicas || …`. While
`replica_opens` was non-empty the loop read only socket frames, and feed events stayed in the
receiver (the broadcast ring and the backlog). The reason is correctness. A new subscription's
snapshot is taken at clock R; an event with revision > R must not be applied to the connection
before the subscription is installed, or the new subscription would miss it. But a preparation
that is only *waiting for snapshot admission* has no read transaction or clock yet. In the 1k
runs that wait is 5–8 s (p90; `snapshot.permit_wait`), so:

* `delivery.queue_wait` p99 7.4 s (rf-1k-prod-2) = sockets with a queued open;
* save p95/p99 4.2 s / 9.5 s: a call completes when the watermark covering its commit reaches the
  caller (the SDK's read-your-writes). In `run.ts` the "detail window" action opens 8
  collections and then saves (`recordTaskViewByTaskId`), so the busiest savers are exactly the
  connections with pending opens;
* feedback: deferred sockets accumulate feed; merging or lag work then lands at once.

Reproduced on the driver (below): idle observers' delivery p99 ~150–220 ms, while active
users' delivery p99 is 4.3–4.8 s and save p95 3.3 s on the same runtime at the same time.

## What changed

1. **Feed keeps flowing while opens wait for admission** (`replica_open_queue.rs`
   `OpenProgress`, `admission_wait`; `replica.rs` `begin_collection_read`,
   `build_cache_entry`; `lib.rs` connection loop). Each started preparation counts as *reading*
   from its first poll until the socket takes its result (the `Reading` guard travels with the
   result, and the socket installs the subscription before its next await). The exception is
   while it awaits `SnapshotAdmission::admit` (task-local `OPEN` state). The loop defers its feed
   only while some preparation is reading (`defer_feed = reading_replicas || …`), and a
   `settled` notification re-evaluates it when the last reader goes back to waiting. Race: a
   preparation marks itself reading (SeqCst) *before* its clock read. After taking a feed
   event (broadcast after its commit), the socket re-checks `reading()` (SeqCst) and, if set,
   parks the event in `pending_feed` for after installation. If the check sees no reader, any
   later clock read sees that commit, so the snapshot already contains the event and the new
   subscription skips it by cursor. The cache paths are covered: a shared cache entry has its own
   transaction and is rolled forward to the open's own clock; waiting on another task's cache
   build counts as reading (conservative). Toggle: `GONVEX_FEED_DURING_ADMISSION=0` (previous
   behaviour).
2. **Lagging connections batch by reference** (`delivery::feed_lagging`, `take_waiting_feed`,
   `Runtime::apply_feed_events_offloaded`, connection loop). A connection that takes a feed event
   while `GONVEX_FEED_MERGE_LAG` (default 1) more events already wait in its receiver is behind.
   It takes up to 64 of them as they are (the broadcast's `Arc` change lists, no copy) and applies
   them in commit order: the delivery v4 fan-out serves each event it can inline, and from the
   first miss on, the rest go through `apply_feed_event_with` one after another in **one** offload
   job. Every event finds the decision share and the encoded items that other connections built for
   the same transaction (`feed_share` keys shares by the change list's address; the tenant frame
   memo keys items by revision). Then the feed-first scheduler gives the socket's inbound frames
   (the user's own calls) the next turn (`FeedFirstScheduler::merged`). Before, it drained every
   queued event first, one offload round trip each. Clients see every intermediate state, in one
   batch frame per tick, as with a non-lagging connection that receives several commits per tick.
   `0` disables it. `GONVEX_FEED_LAG_MERGE=copy` restores round 1 (a merged copy per connection;
   for A/B only). A socket whose *writes* are backlogged still merges by copy (delivery v3,
   unchanged: fewer bytes for a slow reader). Its mergeability check now reads the headers (same
   epoch, later revision) instead of building and dropping a merged copy for every pair, so the
   check is no longer O(n²) in clones. Both take loops re-check, before every take, whether one of
   the socket's preparations started reading (`OpenProgress`), so no event after a new clock is
   taken early. Round 1 had left this race open for the merge path.
3. **Bounded snapshot admission with fast fail** (`offload.rs` `SnapshotAdmission`). The count
   semaphore is replaced by a cancel-safe slot queue. At most `GONVEX_SNAPSHOT_MAX_WAITING`
   (4,096) builds wait, and a build waits at most `GONVEX_SNAPSHOT_ADMISSION_TIMEOUT_MS`
   (30 s). Beyond either it fails with `ReplicaError::Overloaded` → `replica.error` ("replica
   snapshot admission overloaded (…); retry"), which the SDK already retries with backoff
   (`scheduleReplicaRetry`: 250 ms·2ⁿ, ≤ 5 s). Before, a queue could grow without bound
   behind the 2 permits. `/dev/profile` gauges: `snapshot.waiting_short`, `snapshot.waiting_long`,
   `snapshot.shed_total`. New stage `snapshot.permit_hold` (admitted → released: the service
   time under admission).
4. **Short-first lanes (opt-in, `GONVEX_SNAPSHOT_LANES=1`).** Builds whose learned size estimate
   is ≤ `GONVEX_SNAPSHOT_SHORT_BYTES` (256 KiB) go first. Large ones hold at most `count − 1`
   slots, with aging (4× the short head's wait, or 1 s). Estimates are now learned for cached
   views too, and keyed (per-task) collections are estimated apart from collection-wide ones of
   the same table (`tasks.get` vs `tasks.recentWorkspaceTasks`). Measured as not worth it by
   default (below; measured before the aging rule got its 1 s floor, which only delays aged
   large builds further).

Not changed: the permit count (2 on 1 vCPU). With 4 permits on the driver, hold times doubled
and nothing improved (one run each, save p95 93 vs 88 ms, open p95 2.26 vs 2.18 s): opens are
CPU-bound. A CPU token bucket in front of the permits was not built. The offload/SQLite reader
threads already run at nice 10 in the lab, so snapshot work only gets CPU that the network
thread and V8 leave over, and the server CPU on the driver stays at 0.85–0.89 busy in every
variant. Pacing would only add idle time. The priority that mattered is user calls and their own
deliveries ahead of everything else, which changes 1 and 2 give.

## Driver

`cargo bench -p gonvex-runtime --features perf-fixture --bench burst_control`
(`benches/burst_control.rs`). Real WebSockets through the runtime's router into the perf fixture
(9,000-task tenant, fake module host). The runtime (tokio worker, offload pool, SQLite threads)
is pinned to one CPU (`BURST_SERVER_CPU`), so `available_parallelism` is 1: 2 snapshot permits
and 1 offload thread, as in the container. Clients run on other CPUs. Lab env:
`GONVEX_OFFLOAD_NICE=10`, `GONVEX_SQLITE_READER_NICE=10`, `synchronous=normal`, and the batch
idle delay set to the tick (50 ms) so watermarks measure delivery. Users are e2e members, of which
10 % get the fixture Admin's permissions (heavy login sets: all 9,000 tasks). Active users loop:
think exp(mean), then 50 % open a task detail window (8 keyed collections) *and* save that task
at once, 50 % save only. Idle observers hold the login set (10 collections). At 30 % of the run,
half the active users reconnect (10 cold opens each). Measured on the client: save (sent →
result + covering watermark), delivery (commit result → watermark on every other socket),
window open (8 ready), reconnect (10 ready), plus server stages. A/B is one binary with the
toggles (`off` = `GONVEX_FEED_DURING_ADMISSION=0 GONVEX_FEED_MERGE_LAG=0`, the pre-change
behaviour; lanes off). The machine is shared with other agents' builds, so variants are
interleaved per repetition.

Regression test: `cargo test -p gonvex-runtime --features perf-fixture --test burst_control`.
The only snapshot slot is held, a socket opens `tasks.get` (it waits) and saves that task. The
save must complete within 2 s while the open waits (it fails with
`GONVEX_FEED_DURING_ADMISSION=0`). After release, the snapshot contains the save and a later save
reaches the new subscription.

## Results (burst driver, ms)

Overloaded: 150 active users + 100 observers, think 1 s, 30 s, interleaved, 2 repetitions each
(`burst-control-results/m1.jsonl`). The server CPU is 0.83–0.89 busy in every variant.

| variant | save p50 | save p95 | save p99 | delivery to active p99 | delivery to observers p99 | window open p50 / p95 | reconnect p50 / p99 | `delivery.queue_wait` p99 | `snapshot.permit_wait` p90 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| off (before) | 247 / 248 | 3,317 / 3,557 | 3,641 / 4,086 | 4,271 / 4,763 | 221 / 154 | 1,915–2,159 / 3,418–3,853 | 5,380–5,712 / 7,299–8,730 | 3,755 / 4,156 | 3,270 / 3,531 |
| + feed during admission | 39 / 42 | 154 / 176 | 357 / 352 | 161 / 158 | 144 / 142 | 1,625–2,146 / 2,942–3,513 | 4,922–5,917 / 9,039–9,705 | 100 / 116 | 2,588 / 3,276 |
| **+ merge when lagging (default)** | 37 / 41 | **96 / 110** | **138 / 173** | **128 / 129** | 113 / 113 | 1,687–1,713 / 3,351–3,365 | 5,260–5,590 / 8,644–10,110 | **23 / 25** | 2,980 / 3,035 |
| + lanes (opt-in) | 42 / 41 | 139 / 113 | 230 / 185 | 163 / 169 | 129 / 146 | 1,366–1,888 / 2,596–3,253 | 6,587–7,148 / **20,205–20,317** | 41 / 32 | 2,643 / 2,294 |

Light load (60 + 40 users, think 2 s; 0.26 cores; `burst-control-results/light.jsonl`): off → default: save p95
35/28 → 24/23, p99 107/64 → 43/44; window open p95 69/38 → 43/48; reconnect p99 516/379 →
220/402; `delivery.queue_wait` p99 10.6/8.0 → 3.6/4.2. No regression.

Heavier overload (220 + 150 users, think 0.8 s, 40 s; CPU 0.94–0.96; `burst-control-results/heavy.jsonl`, 2 reps each):

| variant | save p50 | save p95 | save p99 | delivery to active p99 | delivery to observers p99 | window open p50 / p95 | reconnect p99 | `delivery.queue_wait` p99 | saves done | windows done |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| off (before) | 2,050 / 1,612 | 7,169 / 6,833 | 8,443 / 7,627 | 11,641 / 10,600 | 1,082 / 936 | 6,365–5,994 / 7,841–7,345 | 21,777 / 20,602 | 9,802 / 9,307 | 1,868 / 1,983 | 944 / 991 |
| default | **53 / 52** | **424 / 411** | **569 / 570** | **446 / 464** | 420 / 412 | 5,376–5,262 / 7,682–7,657 | 15,566 / 19,239 | **36 / 37** | 2,066 / 2,104 | 1,046 / 1,063 |

The pre-change runtime degrades the way the 1k lab runs do: everyone who opens anything waits
seconds for their own saves, and delivery to *idle* observers also passes 1 s. With burst control,
saves and deliveries stay under 0.6 s p99 at the same overload. Only the opens wait, and the
same CPU completes ~7–11 % more work (merged applies). No errors, timeouts or resets in any run.
Run-to-run spread of save p95: 4.9 % (off) vs 3.1 % (default) here; 7 % vs 14 % in the 150-user
matrix, so both are small once the machine is quiet.

Earlier single runs of the same comparison, during heavier machine contention (`burst-control-results/r1.jsonl`, before
the merge existed): off save p95 2.7–3.2 s vs feed-during-admission 2.0–2.1 s. Under contention the
remaining tail was the socket draining queued feed one event at a time (`ws.inbound_wait` p99
1.35 s), which is what change 2 removes.

## Risks and contract

* No protocol change. Clients see the same frames. A lagging connection gets its queued
  transactions in one batch frame (every revision, in order). A write-backlogged socket gets
  merged transactions as before (delivery v3). Batching stays limited to batch connections
  without live queries.
* Offload work per job is bounded by 64 events. One job applying 64 events holds the
  connection's subscriptions for its duration, as 64 separate jobs would in sequence.
* New error: a snapshot open (or replica collection query) that waits > 30 s or finds 4,096
  builds waiting gets `replica.error` and the SDK retries. This never triggered on the driver
  (shed 0). In a spiral it bounds the queue instead of growing it.
* Correctness of change 1 rests on "no clock is held while in `admission_wait`" (checked for both
  call sites; documented at `admission_wait`). A future open path that reads a clock *before*
  `admit` would have to stay out of `admission_wait`. The regression test checks the event
  applied before the clock is in the snapshot, and that the next one is delivered.
* Opens are not faster: under overload, window opens still wait 1.4–2.1 s p50 and reconnects
  5–6 s p50 on the driver. CPU, not admission, is the limit there.

## Round 2: merge by reference (burst driver)

Same driver, the same binary with toggles, variants interleaved, 2 repetitions each. "off" =
`GONVEX_FEED_DURING_ADMISSION=0 GONVEX_FEED_MERGE_LAG=0`, "copy" = `GONVEX_FEED_LAG_MERGE=copy`
(5ca42585), "ref" = the defaults. CPU = the runtime threads' user + system time over the measured
window (threads pinned to the server CPU), per completed save. Shares = `feed_share` shares
created / found over the window (`burst-control-results/v2-*.jsonl`).

Mid load (150 + 100 users, think 2 s, 30 s; ≈ 0.65 of the CPU: the unsaturated, 500-like point):

| variant | saves | save p50 / p95 / p99 | `delivery.queue_wait` p99 | CPU s (ms per save) | shares created / found (found per created) | shared decisions |
|---|---:|---|---:|---|---|---:|
| off | 1,608 / 1,578 | 40–43 / 1,467–1,719 / 1,953–2,227 | 1,413 / 1,794 | 21.8 / 21.9 (13.6 / 13.9) | 10.8k / 792k (74); 12.1k / 777k (64) | 387k / 378k |
| copy | 1,569 / 1,589 | 22–24 / 75–78 / 110–119 | 16 / 15 | 20.9 / 20.8 (13.3 / 13.1) | 94.2k / 557k (5.9); 93.1k / 571k (6.1) | 272k / 280k |
| **ref** | 1,603 / 1,599 | **19–20 / 68–70 / 98–100** | 14 / 13 | **20.6 / 20.4 (12.9 / 12.7)** | **1.7k / 734k (440); 1.7k / 734k (444)** | **391k / 390k** |

Heavy (220 + 150 users, think 0.8 s, 40 s; CPU-bound):

| variant | saves | save p50 / p95 / p99 | delivery to active p99 | `delivery.queue_wait` p99 | CPU s (ms per save) | shares created / found (per) | shared decisions |
|---|---:|---|---:|---:|---|---|---:|
| off | 1,943 / 2,043 | 1,956–2,063 / 6,478–6,996 / 7,582–9,024 | 11.8 s / 9.7 s | 9.7 s / 8.5 s | 36.1 / 41.3 (18.6 / 20.2) | 301k / 1.13M (3.8); 312k / 1.19M (3.8) | 403k / 430k |
| copy | 2,184 / 2,236 | 50 / 345–376 / 508–534 | 346 / 372 | 34 / 36 | 39.0 / 39.0 (17.9 / 17.5) | 266k / 707k (2.7); 270k / 722k (2.7) | 330k / 338k |
| **ref** | **2,348 / 2,374** | **44–45 / 178–185 / 231–299** | **203 / 199** | 50 / 51 | 38.8 / 38.7 (**16.5 / 16.3**) | **3.0k / 1.40M (468); 3.1k / 1.42M (457)** | **818k / 829k** |

By reference restores sharing to beyond the pre-burst-control level ("off" also loses it here, to
write-backlog merges and log refills). It also does the most work per CPU second. The CPU
difference on this driver is smaller than the lab's 22 %: the driver's e2e members see no task
rows, so a private decision is cheap here, while the Whagons default mix decides team and
personal visibility per row. The share and decision counts are the transferable signal. The
regression test (`tests/burst_control.rs`) now also bursts 120 saves while the observer is busy
with 40 calls of its own: the observer takes by-reference batches (79 batches of 159 events in
the runs here; asserted > 0), ends at the newest state, covers every revision, and has no reset.

## Not measured

* The Mac-generator lab at 1k (only the integration agent runs e2e steps). Expected effect there:
  the save and `delivery.queue_wait` tails tied to `snapshot.permit_wait` should go. Snapshot open
  tails will remain.
* The production-mix resume path (cheap log-only resumes) under lanes; resumes are not in the
  driver.
* Memory at 1k (a batch holds ≤ 64 references to broadcast change lists the ring holds anyway; a
  write-backlogged merge still holds ≤ 256 copies, as delivery v3 does).
* Round 2 in the lab (default mix 500, production 1k): expected to remove the +0.14 cores at 500
  that came from lost sharing. Not run here.
