# Delivery coverage: who takes the shared fan-out path, and why not

Branch `perf/delivery-coverage`, from `perf/integrate-5k` 165b5a2f (step 13). Read
`delivery-v4.md` first (shapes, outcomes, the shared path).

The step-13 cost model put delivery at 37 % of runtime CPU at 500 users
(production mix): ~141 receiving connections per change, ~0.14 ms each, while
delivery v4's shared path measured ~2.3 µs per connection on the fixture. This
branch counts, per connection and transaction, how the delivery was decided,
measures the split on the lab workload, and extends the shared path to the cases
that kept the lab off it.

Code: `replica_fanout.rs` (paths, shape, claims, outcome key), `replica_mask.rs`
(`member_unmoved`, shared concern/ACL helpers), `visibility.rs`
(`principal_change_possible`, allocation-free scalar comparisons),
`visibility_class.rs` (`parent_default_flip`), `feed_share.rs` (claims,
principal memo), the feed branch of the connection loop in `lib.rs`, profiler
stages `delivery.shared` / `delivery.individual` / `delivery.noop`. Tests:
`tests/class_sharing_oracle.rs` (extended). Bench: `benches/delivery_coverage.rs`.
Lab report: `bench/whagons/coverage-report.py LAB LABEL`.

## 1. Counters

`/dev/profile` → `deliveryCoverage`: one count per batch connection per
transaction (`replica::fanout::Path`), plus `waits`, `wakeups`, `noopWakeups`.

| path | meaning |
|---|---|
| `shared.advance` | served from its shape, nothing to decide (cursors advance) |
| `shared.data` | served from the shape's recorded outcome |
| `full.representative` | first connection of its shape: full path, records the outcome |
| `full.unclean` | the shape's first connection could not record an outcome |
| `full.inherited` | a plan the shape cannot follow (`inheritsAny`, nested inherits) |
| `full.legacy` | version 1 plan |
| `full.principal_moved` | a changed row moves this member's principal, grants or derived sets |
| `full.parent_acl` | a parent row's admission flips for this member (children re-decided) |
| `full.parent_and_source` | source rows plus a created / visibility-moved parent in one transaction |
| `full.hit` | a changed row hits a member atom (assignment, own row) |
| `full.index_waiting`, `full.cursor_epoch`, `full.truncated`, `full.not_compiled`, `full.many_rows`, `full.duplicate_stream`, `full.serve_rejected`, `full.no_share`, `full.not_batch`, `empty` | as named |

Stages: `delivery.shared` (fan-out call, inline), `delivery.individual` (whole
`apply_feed_event`, wall, on the offload pool when offloaded), `delivery.noop`
(whole feed arm of a wakeup that added no data item, wall).

## 2. The real split (lab, 500 users, production mix)

Before (counters only, 727063ff, run `dc-before2-500-prod`): **100 % full path,
reason `full.inherited`** (152,209 of 152,237 decisions over the plateau; 834/s,
141.6 per transaction). Every lab user holds `taskResources.taskUsers` and
`taskResources.taskTags` (inherited from `tasks`), and `shape()` rejected any
connection with an inherited plan before per-row work. The fixture micro-driver
shows the same (100 % `full.inherited` for every kind of transaction).

What the next layers of exclusion were, found one lab run at a time:

| step | lab shared | largest remaining reason |
|---|---:|---|
| counters only | 0.0 % | `full.inherited` 100 % |
| inherited plans + per-member principal checks (after1) | 14.8 % | `full.representative` 71.5 % (every shape distinct), `full.parent_and_source` 12.5 % |
| claims + unmoved parents (after2) | 14.8 % | `full.representative` 83.5 %, 0 waits: shapes really were unique |
| outcome key without skipped subscriptions (after3) | 89.1 % | `full.representative` 9.2 % |
| filters-only skip for detail windows (after6) | 97.1 % | `full.representative` 1.6 %, `full.parent_acl` 0.7 % |
| + burst-control merge, shares by revision range (after7) | 96.7 % | merged events: 89 % shared |

## 3. What changed

1. **Inherited plans** (`taskUsers`, `taskTags`, `taskLogs`, … inheriting `tasks`):
   plain `inherits` with parents that do not inherit again are followed.
   - Untouched concern tables (`replica_mask::concern_tables`) → `Skip`, exactly
     the mask step's result.
   - Parent rows: an update of no ACL-index column (or a parent insert) cannot flip
     children; otherwise the member's parent class decides old/new admission once
     per (parent class, row) (`MemberVisibility::parent_default_flip`, memoized
     in the event's memo). No flip → the mask step decides nothing. Flip →
     `full.parent_acl`; parent member hit → `full.hit`.
   - Source rows decided by the compiled class through the shared parent index
     (class default per class and row, as for plain plans); parent indexes must
     be at the event's revision now and not ahead.
   - Source rows together with parent updates that change no visibility column
     share too (overlay and index agree on everything visibility reads).
2. **Principal checks per member, not per table.** The static "slow table" test
   (any change to a set, join or permission table sent every connection down the
   full path: every task write, because `workspaces` has a derived set over
   `tasks`) is replaced by the mask step's own per-change checks
   (`member_unmoved`: `principal_change_concerns`, derived and grant deltas),
   run only for tables that can fire each check (per plan combination), and with
   the principal check skipped when no plan's principal can move for the change
   (`principal_change_possible`: member- and role-independent, once per event and
   plans). The lab's most frequent transaction (`members` + `peopleDirectory`
   presence updates) pays one hash lookup per subscription for it.
3. **Claims.** In the connection loop all connections of a shape handle an event
   at about the same time while the first one's full path runs on the offload
   pool, so with v4 every one of them missed the outcome. Now the first miss
   claims the shape (`EventShare::claims`); the others wait for the claim
   (`fan_out_waiting`) and are then served from the recorded outcome. A claim
   released without an outcome marks the shape failed (`full.unclean`); a dropped
   claim (cancelled connection) releases the waiters.
4. **Outcome key.** A shape's key now holds only the subscriptions the event
   decides or asks (collections of changed tables), in subscription id order; the
   ones it only advances stay out. Lab users keep task detail windows open
   (`tasks.get {id}`, `listTaskNotes {taskId}`, …): with every subscription's
   identity in the key, every connection was its own shape.
5. **Filters-only skip.** A collection whose own filters
   (`collection_filters_admit`: argument equalities, exclusions) admit neither
   image of any changed row of its table decides and holds none of them (a detail
   window on another task): it advances and stays out of the key, after the same
   per-member and index checks as before (the full path would resync or bind the
   mask the same way).
6. Also: index readiness awaited before the shared attempt (`await_indexes`: the
   full path waited there anyway); a parent overlay no longer marks the
   representative unclean when the shape guarantees it decided nothing new;
   `grant_contribution`, `principal_change_concerns` and `derived_change_concerns`
   compare scalars without allocating; `active_grant_sets_iter`.

Wire: none. Every batch frame, unbatched frame, digest, cursor and row count of
the fan-out equals the connection deciding alone (oracle below), including the
batch header's informational class id (the shape binds masks exactly where the
full path would).

## 4. Results

### Lab (500 users, production mix, 1 vCPU / 2 GiB, same lab copy, back to back)

Same lab copy (`lab-dc`, containers `gvdc-*`), 500 users, production mix, thin
clients, 180 s plateau, `LOAD_PERF=0` (no perf capture in the window), generator
on the Mac. `before5` = counters only (727063ff on 165b5a2f); `after6c` = 0517eaad
(this design, before the burst-control merge); `after7` = 76afa9e4 (merged with
perf/integrate-5k 9bdaf828, burst control in).

| | before5 | after6c | after7 (+ burst control) |
|---|---:|---:|---:|
| deliveries decided per transaction | 141.3 | 141.3 | 133.9 |
| shared | 0.0 % | 97.1 % | 96.7 % |
| full-path runs (`delivery.individual`) | 835/s | 24/s | 26/s |
| full path per transaction (wall, offloaded) | 57.3 ms | 12.6 ms | 22.5 ms |
| shared path per transaction (inline) | 0 | 2.24 ms (16 µs per connection) | 2.06 ms |
| offload pool busy (feed applies + snapshot jobs) | 0.077 cores (854 jobs/s) | 0.053 cores (109 jobs/s) | 0.043 cores (102 jobs/s) |
| runtime cgroup CPU | 0.30 cores | 0.29 cores | 0.28 cores |
| errors | 0.08 % | 0.08 % | 0.16 % |
| save p50 / p95 / p99 ms | 33 / 100 / 238 | 31 / 124 / 293 | 35 / 233 / 852 |
| taskDelivery p50 / p95 / p99 ms | 91 / 181 / 364 | 91 / 197 / 365 | 95 / 292 / 799 |

What this shows and what it does not:

- The full path now runs for ~3 % of connection deliveries (24/s instead of 835/s,
  about 35× fewer), and the offload pool's jobs fell 854/s → ~105/s.
- **Total runtime CPU at 500 users did not fall measurably** (0.30 → 0.28–0.29
  cores, within run-to-run spread; the earlier perf-instrumented pairs show the
  same, 0.39 vs 0.35). In this lab the offloaded feed applies were a smaller
  share than the step-13 cost model's 29 % (its attribution came from a
  frame-pointer build; these binaries resolve self time only, so the
  remaining delivery CPU could not be split from snapshot jobs and the network
  thread). The ≥5× target for delivery CPU per change is therefore **not
  demonstrated**; what remains per change is ~141 connection wakeups on the
  network thread (16 µs shared call each, plus watermark and poll), 61 % of them
  sending nothing (§5).
- `after7`'s p99s were taken during a Tailscale blip (RTT max 4.3 s, p95 31 ms vs
  ~10 ms); one earlier run of the same binary as after6c (`dc-after6b`) showed
  p95s of 22–24 s for every kind, saves included, while the runtime's own stages
  stayed normal (queue wait ~3 ms, reducers 15–40 ms) and the RTT had a 1.5 s
  spike; its rerun (`after6c`) was normal. Treat single-run tails as noisy.

Burst control (merged catch-up events, `after7`): 6,983 connection deliveries of
merged events over the plateau, 6,229 served shared (89 %), 754 individual. Shares
are now also found by revision range (`EventShares::share`: epoch, first and last
revision, change count), so two lagging connections that merged the same range
share one outcome although each built its own change list; the full path then
decides on the share's change list (row memos are keyed by image address). Merges
of different ranges still do not share (that needs a canonical merged event per
range, owned by burst control).

Progression (perf-instrumented runs, same lab): shared 0 % (before2) → 14.8 %
(after1) → 89.1 % (after3) → 97 % (after6). Full-path wall per transaction 44 ms
→ 33 ms → 21 ms → 11–13 ms.

### Fixture micro-driver (`delivery_coverage`)

78-member Whagons fixture tenant; every connection holds the 10 lab collections
plus a task detail window (8 collections, one of 20 tasks). Whole feed path per
connection (decide + push + watermark + compressed frame), one tokio worker, the
full path inline (no offload), median of 2 rounds, A/B alternated
(`bench-base-d` = 727063ff with this bench file):

| transaction | before µs/change | after µs/change | speed-up | shared |
|---|---:|---:|---:|---:|
| task edit (+ log) | 3,707 | 1,870 | 2.0× | 95 % |
| task view | 1,128 | 700 | 1.6× | 100 % |
| note (+ log) | 1,130 | 767 | 1.5× | 100 % |
| notification | 711 | 747 | 1.0× | 100 % (advance) |
| assign (+ log) | 4,630 | 2,531 | 1.8× | 95 % |
| tag | 1,733 | 1,135 | 1.5× | 95 % |
| spot move (+ log) | 8,642 | 4,010 | 2.2× | 91 % |
| presence | 2,120 | 1,223 | 1.7× | 99 % |
| **production-weighted mix** | **2,613** | **1,421** | **1.8×** | 97 % |

In the fixture the full path runs inline (~20–50 µs per connection); in the lab
it is offloaded (job, subscription map move, scheduling) and cost ~94–130 µs
per delivery, so the lab gains more from each shared connection.

## 5. Wakeups (for the conn-idle lead)

Every connection of a tenant wakes for every transaction of the tenant: 141.7
wakeups per transaction at 500 users (≈ Σ tenant² / N), i.e. one wakeup per write
per connection. In `dc-after5-500-prod` **61.5 % of those wakeups added no data
item** (only the watermark): 512/s of 832/s. Their cost on the network thread is
the shared-path call (`delivery.shared` ≈ 15 µs wall per connection, inline,
including the shape over ~10–18 subscriptions) plus the watermark and the task
poll; `delivery.noop` (1.07 ms mean) is wall time and mostly waiting for the
shape's claim, not CPU. With burst control merged (`after7`): 133.9 wakeups per transaction, 60.3 %
no-op, `delivery.noop` 547 µs mean wall. A no-op wakeup also writes a batch frame eventually
(watermark-only batches wait for the idle delay; `ws.write` stays ≈ 840/s). A
token → class index (candidate classes and subscriptions per change) could skip
those connections entirely; the counters here are the baseline for it.

## 6. Tests

- On 76afa9e4 (with burst control): `cargo test --workspace`: 661 passed, 0 failed,
  8 ignored; fixture set below: 564 passed, 0 failed, 9 ignored. Before the merge:
  658 / 0 / 8 and 560 / 0 / 9.
- `cargo test -p gonvex-runtime -p gonvex-store -p gonvex-protocol -p gonvex-websocket
  -p gonvex-profiler --features gonvex-runtime/perf-fixture,gonvex-websocket/perf-fixture`:
  (includes `class_sharing_oracle`,
  `delivery_oracle`, `replica_state`, `visibility_fanout`, `offload_equivalence`,
  `raw_rows_oracle`, `feed_backlog`, `resume_log_only`).
- Fan-out oracle (`class_sharing_oracle::fan_out_frames_equal_each_connections_own_frames`)
  now: 4 subscription sets (all 10 collections plus a detail window on one of 5
  tasks; tasks only; presence/statuses/tags/tasks; `taskTags` + `taskUsers`
  only), 47 events including tag insert/delete, two tags in one transaction,
  views, notifications, edit + log, spot move + log, task created with a tag, a
  tag of a missing task (onMissing), tag + task touch, tag + task move, workspace
  move of a tagged task, deletion of a tagged task, member availability and
  see-all-spots changes, and a merged range of three transactions delivered to
  every connection as its own private copy (asserts shares found by range and
  merged deliveries served shared). Every other event is delivered to all connections
  concurrently (claims and waits exercised: 453 waits). Asserts inherited
  connections are served decided rows and that waits happened.
- `cargo clippy -p gonvex-runtime -p gonvex-profiler --features gonvex-runtime/perf-fixture
  --lib --tests --benches -- -W clippy::redundant_clone`: no `redundant_clone`;
  no new warnings in the touched code.

## 7. Risks

- Correctness rests on the shape mirroring the mask step (`member_unmoved` keeps
  the per-change loop's checks in one place; the oracle compares every frame).
  A future check added to `mask_step` must be added to `member_unmoved` too.
- Claims serialize a shape's connections behind its first connection's full
  path (they would otherwise each run it). A slow representative delays them;
  a cancelled one releases them (drop guard).
- `principal_change_possible` is a superset by construction (all non-derived
  sets regardless of role, no anchors); `moving` and the ACL-unchanged rule
  assume followed plans whose parents do not inherit again (checked statically).

## 8. Not done / not measured

- 1k users, the default mix, and the quadratic projection were not run (the
  generator host was shared with the integration and burst-control series).
- The ≥5× target is not reached: 1.8× on the fixture mix, and no measurable
  change in total runtime CPU in the lab at 500 users (§4). What remains per connection is the shape walk over all of a
  connection's subscriptions and the wakeup itself: the next lever is not waking
  connections an event cannot concern (§5).
- `full.parent_acl` (parents whose visibility flips: spot/workspace moves) and
  `full.hit` (assigned members) still decide alone, by design.
- Lab perf captures without frame pointers resolve self time only.

## 9. Targeted wakeups (skip no-op wakeups)

Code: `rust/crates/runtime/src/wake.rs`, `FeedStore::publish` (`change_feed/backlog.rs`),
`replica_fanout::{wake_interest, wake_key, wake_skips}`, the feed branch of the
connection loop (`lib.rs`). Off with `GONVEX_FEED_TARGETED_WAKE=0`; tenant flush
`GONVEX_FEED_WAKE_FLUSH_MS` (default 5,000).

- The tenant feed decides once per transaction, **before** broadcasting its signal,
  which idle connections it could affect (`Interest::affected`), marks their slots
  and wakes only those. An idle connection (batch delivery, replica subscriptions
  only, no call result waiting for its watermark, no open, snapshot or catch-up in
  flight) waits on its slot instead of the ring, runs the feed arm for marked
  revisions and takes the others without deciding anything (cursors stay older,
  which a resume tolerates). Entering the mode or changing the interest treats every
  revision dispatched so far as marked.
- Conservative decision: a transaction is skipped only when every changed row is
  provably irrelevant to every subscription (table not read; the mask step's
  principal / grant / derived checks cannot fire: member-independent, or from the
  member's derived source tokens; parent update of no visibility column; source row
  rejected by the collection's own filters, or by the member's compiled class in both
  images). Inherited source rows, plans with shared indexes, uncompiled members,
  legacy plans, grant rows and membership changes wake.
- Watermarks of skipped revisions are lazy: sent with the connection's next frame,
  or at the tenant flush (one timer per tenant, not per connection). A connection
  whose call waits for its watermark is not in the mode (call completion unchanged:
  `call_watermark_latency` median gap 0.5 ms).

Tests: `class_sharing_oracle` checks, before each event, whether every connection
would be skipped and that a skipped connection's own full path sends nothing
(1,202 of 2,304 connection-events skipped; the only frames tolerated are 4
consistency resyncs the full path issues because the oracle's shared index is
ahead of the event: a skipped connection decides no row against it). New
`tests/targeted_wake.rs` (real socket): an unrelated commit is skipped and its
watermark arrives within one flush; an own-collection change arrives in ~70 ms; a
heartbeat-like call completes with its watermark within 250 ms.

Lab (same lab copy, frame-pointer builds at 500, `LOAD_PERF=0` at 1k):

| | 500 before | 500 after (5 s flush) | 1k before | 1k after |
|---|---:|---:|---:|---:|
| feed-arm wakeups per transaction | 125.2 | 77.7 | 257.9 | 150.1 |
| of them no-op | 58.4 % | 32.8 % | 59.3 % | 30.2 % |
| tenant flush wakeups | – | 15,832 / plateau | – | (not recorded) |
| network thread CPU (fp attribution) | 0.119 cores (20.2 ms/txn) | 0.114 cores (19.4 ms/txn) | – | – |
| runtime cgroup CPU | 0.29 | 0.29 | 0.56 (400/1,776 throttled) | 0.52 (304/1,778) |
| full path per transaction (wall) | 10.6 ms | 13.7 ms | 85.4 ms | 26.7 ms |
| save p95 / p99 ms | 170 / 269 | 128 / 274 | 1,430 / 6,488 | 711 / 1,021 |
| taskDelivery p95 / p99 ms | 208 / 391 | 197 / 429 | 945 / 3,866 | 708 / 979 |
| query p95 / p99 ms | 102 / 256 | 107 / 236 | 5,939 / 8,413 | 1,334 / 2,332 |
| delivery p99 ms | 1,050 | 1,070 | 3,601 | 1,160 |
| errors | 0.08 % | 0.08 % | 0.05 % | 0.05 % |

At 500 the network thread is dominated by socket encode/compress/write (~0.055
cores), not by decisions; skipping saves little CPU there. At 1k the single pair
shows lower CPU (−7 %), fewer throttled periods and much shorter tails, but 1k
tails vary 3–6× run to run and the 1k before run had a 3.6 s RTT blip: one pair,
not yet conclusive. One 500 run of an intermediate build (`dc-wake-after3-500`)
spiralled (11.8 % errors, snapshot admission overload, watchdog stalls, a visibility
check timing out with rows missing); the counters-only build failed the same way
once this morning (`dc-before-500`), before any wake code, so it reads as the known
overload instability, not a skip: still, worth watching in integration.
