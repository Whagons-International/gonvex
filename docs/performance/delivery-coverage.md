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
| filters-only skip for detail windows (after6) | see §4 | |

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

LAB_TABLE

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
shape's claim, not CPU. A no-op wakeup also writes a batch frame eventually
(watermark-only batches wait for the idle delay; `ws.write` stays ≈ 840/s). A
token → class index (candidate classes and subscriptions per change) could skip
those connections entirely; the counters here are the baseline for it.

## 6. Tests

- `cargo test --workspace`: 658 passed, 0 failed, 8 ignored.
- `cargo test -p gonvex-runtime -p gonvex-store -p gonvex-protocol -p gonvex-websocket
  -p gonvex-profiler --features gonvex-runtime/perf-fixture,gonvex-websocket/perf-fixture`:
  560 passed, 0 failed, 9 ignored (includes `class_sharing_oracle`,
  `delivery_oracle`, `replica_state`, `visibility_fanout`, `offload_equivalence`,
  `raw_rows_oracle`, `feed_backlog`, `resume_log_only`).
- Fan-out oracle (`class_sharing_oracle::fan_out_frames_equal_each_connections_own_frames`)
  now: 4 subscription sets (all 10 collections plus a detail window on one of 5
  tasks; tasks only; presence/statuses/tags/tasks; `taskTags` + `taskUsers`
  only), 47 events including tag insert/delete, two tags in one transaction,
  views, notifications, edit + log, spot move + log, task created with a tag, a
  tag of a missing task (onMissing), tag + task touch, tag + task move, workspace
  move of a tagged task, deletion of a tagged task, member availability and
  see-all-spots changes. Every other event is delivered to all connections
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

- 1k users, the default mix, and the quadratic projection were not run.
- The ≥5× target is not reached in the fixture (1.8× on the mix); see §4 for
  the lab. What remains per connection is the shape walk over all of a
  connection's subscriptions and the wakeup itself: the next lever is not waking
  connections an event cannot concern (§5).
- `full.parent_acl` (parents whose visibility flips: spot/workspace moves) and
  `full.hit` (assigned members) still decide alone, by design.
- Lab perf captures without frame pointers resolve self time only.
