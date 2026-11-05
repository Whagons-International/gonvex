# Replica delivery v3: per-class decisions, presence in the batch, batches under backpressure

Branch `perf/delivery-v3`. Builds on delivery v2 (`docs/performance/delivery.md`:
encode once, compress once, `replica.batch` per connection per tick), replica-state
(`replica-state.md`), compiled visibility classes (`../visibility-masks.md`),
sqlite-direct, offload, presence v2 (`presence-v2.md`) and the ws-buffers slow-reader
fix (`websocket-memory.md`). Merged base: those branches plus
`origin/deploy/sqlite-staging` f482033c (216976c9 + 684330d8), then `perf/ws-buffers`
a0a33ff4.

Code: `rust/crates/runtime/src/feed_share.rs` (new), `replica.rs` / `replica_mask.rs`
(feed apply), `delivery.rs` (batcher, prewarm, merge of waiting events),
`change_feed.rs` (`merge_transactions`), the feed/presence branches of the connection
loop in `lib.rs`, `rust/crates/websocket/src/{lib,connection}.rs` (batch transport on
the bounded writer queue). Bench: `rust/crates/runtime/benches/delivery_v3.rs`.

## 1. Decide once per visibility class (`feed_share`)

A feed event's row images are one `Arc<Vec<LogChange>>` per broadcast, so their
addresses are identical in every connection's copy. `EventShare` (one per committed
transaction and tenant, the last 4 kept, found by epoch + revision + `Arc::ptr_eq`)
holds that `Arc` and, keyed by row address:

- one `EvalMemo` for the tenant: each change row's ACL columns are decoded once and
  each class default is evaluated once for all connections (was: once per connection);
- per-(class, collection, args, row) `RowDecision`s. A subscription reuses a decision
  only if its compiled class is the one that computed it (`VisibilityClass::id()`,
  process-local, never reused), the definition (`Arc` address) and exact argument JSON
  match, the row hits no member atom of its member (`MemberVisibility::hit_memo` on
  the old and new image), its principal did not move in this event, its window is
  complete (not truncated) and the row is a change row (not an affected-row read).
  For such a subscription `decide_row` is a function of exactly those inputs (pure in
  definition, args, plan, principal, window and images; a non-hit member's compiled
  visibility is its class default), so its own decision would be identical;
- one shared copy of each parent before-image used for overlays (was a deep copy per
  subscription).

Plans with `inherits`/`inheritsAny` never share decisions (their answer also reads the
parent index at evaluation time). Fallback (interpreted) plans decide alone.
`GONVEX_REPLICA_CLASS_SHARING=0` turns sharing off.

Two per-subscription costs the profile showed dominating the apply path were removed in
`mask_step` (they are not class work, but every subscription paid them for every
change):

- **Concern tables.** Each subscription caches every table its plan, sets, joins,
  parents and `inheritsAny` read (recursively through inherited parents). A change to
  any other table skips the principal / derived / ancestor / grant checks (before:
  `match_expression`, `parent_admits`, `active_sets` led the flat profile).
- **Parent ACL columns.** An update of a parent row (`taskUsers`/`taskTags` see every
  task change as a parent change) that changed none of the parent ACL index columns
  (`visibility::index_columns`), under an unchanged principal, cannot flip
  `parent_admits`: both interpreted checks are skipped.

`replica.batch.class` is now the real class: the `stable_id()` of the connection's
compiled binding (16 hex; a hash of the sorted ids when the connection's collections
span several classes, one per plan; an uncompiled binding contributes its principal
fingerprint). Informational: which bytes a connection gets is decided per
subscription, never by the header.

Encode on the offload pool: when the feed apply is offloaded, `delivery::prewarm`
encodes the transaction and delta items (default stream token) and compresses them for
the connection's window there, so the network thread's `push` finds them memoized.
The per-connection work left on the network thread is the binding, the digest tail and
the append.

What is still per connection: `mask_step`'s checks for changes of tables the plan does
read, the digest/window update and the normalized entity change (`replica.transaction`)
of the connection (memoized by content as in v2). A frame computed once per (class,
collection) and pushed to every connection of the class without any per-connection
apply is not done (see "Not done").

## 2. Presence in the batch

A connection that negotiated `replicaBatchFrames` gets its drained presence change
(`presence.patch`, or the resync `presence.snapshot`) as an item of the tick's
`replica.batch` (`ReplicaBatcher::push_item`); it counts as data, so the batch goes at
the next tick. Presence keeps its own cadence (the slot is drained once per presence
tick, 1 s) and the connection gets one frame per tick for replica and presence
together. Other connections get `presence.*` frames as before. No new capability: the
SDK (`expandReplicaBatch`) and the thin client (`onBatch`) already dispatch any
non-replica item as a frame. `ReplicaBatchItem` (protocol types) now lists
`presence.patch | presence.snapshot`. Tests: `tests/presence_v2.rs`
`presence_patches_ride_in_the_replica_batch` (real socket), SDK `presence.test.ts`,
thin client test; `client.thin.unsupported` stays empty.

## 3. Batches under backpressure (ws-buffers)

The websocket crate is now ws-buffers' (owned writer task, bounded outbound queue,
256 KiB default, send-stall reset with 4008) with the delivery batch transport ported
onto it: `Outbound::Batch(BatchFrame)` is written by the writer as is (precompressed
body through the encoder's `precompressed` slot, or plain text); `WebSocket` keeps one
pending `OutboundBatch` that every `send`/`send_best_text`/`try_send` queues first
(order unchanged); `flush_batch` waits for space, `try_flush_batch` does not.

Connection loop policy:

- While `SocketWatch::is_backlogged()` (64 KiB queued or a write blocked) the
  connection takes no feed event, drains no presence and does not queue its batch (the
  batch deadline is suspended). It wakes on `writable()`.
- Once writable, the feed events that waited are merged into one event
  (`delivery::merge_waiting_feed` → `change_feed::merge_transactions`: changes in commit
  order, the last revision). Applying it collapses every row to its first before-image
  and last after-image, so each row's newest state is delivered once. Merging only
  happens when no subscription's cursor lies inside the merged range (a subscription
  skips whole events at or before its cursor) and no live query is open; a waiting
  reset, an epoch change or receiver lag stops the merge and is processed next.
- Lagging the broadcast ring still resets and resyncs (cursor + digest), as before.

`tests/slow_reader_delivery.rs` drives that policy with the production pieces (real
compressed loopback socket with a 64 KiB / 64-frame cap, fixture subscriptions,
`apply_feed_event`, `ReplicaBatcher`, `merge_waiting_feed`): a client that stops
reading during 300 incompressible 24 KB task writes received **13 batches carrying 38
versions of the task** (one merge absorbed ~263 waiting events), the last at the last
revision; the queue never held more than the cap except a single frame on an otherwise
empty queue (max 150 KB, which the transport allows by design).

## Wire changes beyond delivery v2

1. `replica.batch.class` is a real visibility class id (see above).
2. `replica.batch.items` may contain `presence.patch` / `presence.snapshot`.
3. A connection that fell behind may receive one `replica.transaction` / delta / ready
   covering several revisions (cursor = the last); intermediate row states are not
   sent. The watermark semantics are unchanged.

Nothing else changes; old clients get the old frames.

## Benchmarks

arch3 (Ryzen 7 5800X, shared with other agents' builds, load average 9-15, so medians
move ±30% between runs; allocation counts are stable). Benches pinned with
`taskset -c 3`, one tokio worker. Before = the green merged base df5e2606 (same bench
file, binary saved before any change); after = 0f12c65d (`perf/delivery-v3`, code identical to the pushed head).
"before (paired)" is the before binary re-run right after the after run.

`cargo bench -p gonvex-runtime --features perf-fixture --bench delivery_v3`

| benchmark | before | before (paired) | after | change |
| --- | --- | --- | --- | --- |
| one task update → 78 members × 10 collections, json: median | 3.09 ms | 4.10 ms | **2.47 ms** | −20% / −40% |
| same: allocations / bytes allocated per change | 33,693 / 3.83 MB | 33,706 / 3.83 MB | **17,430 / 2.45 MB** | −48% / −36% |
| same, native rows: median | 3.43 ms | 3.70 ms | **2.34 ms** | −32% / −37% |
| same, native: allocations / bytes | 32,426 / 3.63 MB | 32,398 / 3.62 MB | **15,939 / 2.23 MB** | −51% / −39% |
| frames / wire bytes per change (json) | 19.45 / 53.5 KB | | 19.45 / 53.5 KB | unchanged |
| 1,950 members (`tasks.recentWorkspaceTasks`), 23 writes/s, real compressed socket: CPU (share of one core) | 0.390 | 0.441 | **0.372** | −5% / −16% |
| same: busy ms per write / allocations per write | 16.9 / 174k | 19.2 / 174k | **16.2 / 147k** | −4..16% / −16% |
| same: frames/s, wire MB/s | 6,335, 14.4 | | 6,335, 14.4 | unchanged |
| reconnect burst, 200 connections × 10 collections (78-member tenant): peak heap | 113.2 MB | 111.4 MB | 104.5 MB | −7% (noise level) |
| snapshot open, 9,000 tasks, Admin (`replica_state`): time / peak heap | 174 ms / 73.5 MB | | 169 ms / 73.5 MB | unchanged (step 3 not done) |
| retained per member, 78 members × 10 collections (`replica_state`) | 377 KB | | 378 KB | unchanged |
| marginal member Admin / TeamAllSpots / TeamSpots / GrantOnly | 63 / 63 / 59 / 38 KB | | 60 / 63 / 59 / 38 KB | unchanged |
| network-thread polls > 5 ms (`stalls`, fanout 4 updates/s; burst of 200 opens) | 0 ; 0 | | 0 ; 0 | stays 0 |

Notes:

- The 78-member gain is mostly the `mask_step` work removed for unrelated tables and for
  parent changes that move no ACL column, plus the shared memo and decisions; frames and
  bytes are unchanged by design (sharing never changes what a connection receives).
- The 1,950-member driver opens only `tasks.recentWorkspaceTasks` (no inherited plans),
  so the concern-table and parent skips barely apply; what is left there is the
  per-connection apply (≈ 8 µs per member per write: `mask_step` for the tasks table,
  digest update, entity normalization, batch push). At 23 writes/s that is 0.37 of a
  core for 1,950 members: within budget, but not "only the append" per connection.
- "As many reconnects as fit" was not measured beyond 200 (the fixture tenant has 78
  members; the 200-connection burst re-opens members round-robin).

## Tests

- `cargo test -p gonvex-runtime -p gonvex-store -p gonvex-protocol -p gonvex-websocket
  --features gonvex-runtime/perf-fixture,gonvex-websocket/perf-fixture`: 512 passed,
  0 failed (includes `delivery_oracle`, `replica_state`, `perf_fixture_smoke`,
  `visibility_fanout`, `presence_v2`, `offload_equivalence`, and the new
  `class_sharing_oracle` and `slow_reader_delivery`).
- `tests/class_sharing_oracle.rs`: two identical connection sets of every member of the
  small Whagons fixture; set A with class sharing, set B deciding alone; 32 events
  (edits, inserts, deletes, spot/workspace moves, grant assign/unassign, team gained and
  lost, presence beats): all 704 frames identical, 137 decisions reused.
- `tests/replica_state.rs` (SQL oracle, sharing on): every client window equals SQL
  through all steps.
- `visibility_fanout`'s statement budgets need `GONVEX_TEST_MODULE_HOST_BINARY`; without
  it they report "not measured" (as on the base).
- SDK: `packages/client` vitest 1,054 of 1,055 pass; `indexeddb-engine.test.ts` "batches
  stationary membership hashes…" fails only in the full parallel run on the loaded
  machine and passes alone (39/39), untouched by this branch. Thin client
  `node --test bench/whagons/thin-client.test.mjs`: 9/9.

## Not done

- **Raw JSON rows end to end (task step 3).** Snapshot and change-feed rows are still
  decoded into `serde_json::Value` (snapshot open unchanged: 9,000 rows, 73.5 MB peak).
  Blocker for hashing SQLite's bytes directly: hashes must stay byte-identical to
  `stable_json(parse(row))`, and SQLite's `json_object` writes REALs with 17 significant
  digits while the canonical form uses serde's shortest form (`0.10000000000000001` vs
  `0.1`); keys must also come out sorted. A raw fast path is possible for rows whose
  bytes are already canonical (no REAL tokens, serde-compatible escapes, sorted
  columns), falling back to parse; it also needs the snapshot message rows (protocol
  crate) to carry `RawValue`s and the change feed to keep `json_object` bytes. Not
  started.
- **Per-class frames without a per-connection apply.** Decisions, decode and class
  defaults are shared, but every connection still runs its own `apply_feed_event`
  (subscription loop, digest, entity normalization). The next step is a per-tenant
  fan-out that produces the (class, collection) delta once and only appends it per
  connection, with the per-member path for hit members; the class id contract for that
  is in place.
- Presence items are inline per connection (not `SharedText`), so they are compressed
  per batch, not once.
