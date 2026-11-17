# Replica delivery v2: encode once, compress once, one frame per tick

Workstreams 1-3 of the 5k-users program (branch `perf/delivery`). Code:
`rust/crates/websocket/src/shared.rs` (shared segments, compressor reuse,
batched messages), `rust/crates/runtime/src/delivery.rs` (memo, stream
bindings, per-connection batcher), the feed branch of the connection loop in
`rust/crates/runtime/src/lib.rs`, the SDK (`packages/protocol`,
`packages/client`) and the thin load client (`bench/whagons/thin-client.mjs`).
Benchmarks: `rust/crates/runtime/benches/delivery.rs`.

## What changed for every client (no negotiation)

- **Compressor reuse.** `compress_text` (every permessage-deflate message)
  used to build a new zlib stream per message (~370 KiB allocated and freed).
  It now resets one compressor per thread and window (`Compress::reset`). The
  output bytes are identical (tested against a fresh compressor), so this is
  invisible on the wire.

## Delivery v2 (`replicaBatchFrames`), negotiated

Both sides advertise version 1: the client in `auth.capabilities`
(`replicaBatchFrames: 1`), the server in `session.ready.capabilities`
(`replicaBatchFrames: 1`). A client that does not advertise it gets exactly the
old frames (old clients are unchanged). The SDK (`@gonvex/client`) and the thin
load client advertise it; the thin client can turn it off with
`LOAD_THIN_BATCH_FRAMES=0` (or `batchFrames: false`) for A/B runs.

### Old wire (per connection, per committed transaction)

For each transaction the connection's subscriptions see, in this order, one
WebSocket message each:

1. `replica.transaction` (normalized entity changes; `oldValue` omitted with
   `replicaNoOldValues`; `replica.transactionEncoded` with native rows; or a
   connection-local `replica.rowPatch` with field patches),
2. `replica.delta` per changed subscription (`id` = the client's subscription
   id),
3. `replica.ready` per changed subscription (`id` = subscription id),
4. `replica.watermark {revision}` (also when nothing was visible).

For the fixture's task update that is 284 messages for 78 members (one per
member at least, for the watermark), each encoded and compressed separately.

### New wire (per connection, per tick)

One text message per connection per tick holds everything the connection's
feed produced since the last one, in the same order:

```json
{"type":"replica.batch","tenant":"<tenant id>","class":"<16 hex>","from":41,
 "items":[
   {"type":"replica.transaction","cursor":{"epoch":"e","revision":42},"originCommandId":"c1","changes":[...]},
   {"type":"replica.stream","id":"<subscription id>","stream":"~3f2a9c0d11e4b7a8"},
   {"type":"replica.delta","id":"~3f2a9c0d11e4b7a8","path":"tasks.recentWorkspaceTasks","cursor":{...},"upserts":[...],"deleted":[],"digest":"...","truncated":false},
   {"type":"replica.ready","id":"~3f2a9c0d11e4b7a8","path":"tasks.recentWorkspaceTasks","cursor":{...},"digest":"..."}
 ],
 "watermark":42}
```

- `items` are the old frames, unchanged except that a `replica.delta` /
  `replica.ready` `id` is a **stream token** (`~` + 16 hex digits, optional
  `.n` suffix) instead of the subscription id. A stream is a collection path
  plus its arguments; the token contains nothing about the socket. (Key order
  inside an item can differ from the old frame: a delta's `digest` and
  `truncated` come last.)
- `replica.stream {id, stream}` binds a subscription id to a token **on this
  socket**, before the first item that uses it. Bindings live until the socket
  closes; a new binding for the same id replaces the old one. If one socket has
  two subscriptions with the same path and arguments, the second gets its own
  token (`.2`), so every item reaches exactly the subscriptions it did before.
- `watermark` replaces the per-transaction `replica.watermark` frames: it is
  the highest revision through which this connection's frames are complete and
  applies after all items. (The SDK already coalesced adjacent watermarks.)
- `transaction`/`delta`/`ready` items always omit old row images (the batch
  client applies from its local base, like `replicaNoOldValues`). With native
  rows negotiated, items use the native encodings
  (`replica.transactionEncoded`, `canonicalUpserts`) as before. Field patches
  (`replica.rowPatch`) are not used on the v2 path (they are per-connection by
  construction); compress-once replaces them.
- `tenant`, `class` and `from` describe the stream for a future edge layer and
  for diagnostics; clients ignore them. `class` is today a stand-in (hash of the
  connection's resolved visibility fingerprints) until shared visibility
  classes (workstream 7) provide stable class ids; `from` is the previous
  batch's watermark on this connection.
- Every other server message (snapshots, call results, errors, resets, live
  queries, control frames) is sent as before, and **the pending batch is always
  sent first**, so the total order of frames is the old order; only adjacent
  replica feed frames are grouped.

Client rule (implemented by `expandReplicaBatch` in `@gonvex/protocol`, and in
`ThinClient.onBatch`): process items in order; `replica.stream` updates the
token map; `replica.delta`/`replica.ready` are dispatched once per subscription
bound to the token, with `id` set to that subscription id; other items are
dispatched as is; then `watermark` is dispatched as `replica.watermark`. Items
for an unbound token are dropped.

### Tick

- A batch with changed rows (a transaction or delta item) is sent at most
  `GONVEX_REPLICA_BATCH_TICK_MS` (default 50 ms) after it opened, or
  immediately when it reaches `GONVEX_REPLICA_BATCH_MAX_BYTES` (default
  512 KiB) or when anything else is sent on the socket.
- A batch without changed rows (only `replica.ready` items, which re-state a
  collection's cursor and digest, and/or the watermark: the transaction changed
  nothing this connection sees) waits `GONVEX_REPLICA_BATCH_IDLE_MS` (default
  1 s), unless a reducer/action result on this connection waits for that
  revision; then it goes at the next tick. This keeps frames/s near
  "connections that received rows" instead of "all connections x commits".
  Everything still arrives in order; only the cursor confirmation is later.
- `GONVEX_REPLICA_BATCH_TICK_MS=0` sends each transaction's batch at the next
  loop turn (no added latency, still one frame per transaction instead of 2-4).

Latency: replica data waits up to one tick (50 ms by default) longer than
before. The writer's own call result is not delayed (it flushes the batch).

## Encode once

Before: every connection serialized its own frames (`serde_json`, 9.4% of
runtime CPU in `format_escaped_str` at 200 users) and the native path also
re-rendered canonical row strings per connection.

What is shared: a delta's rows and the transaction's entity changes. What is
not: the collection `digest` (and `truncated`) of a delta or ready frame
describes the connection's whole window, so two members who see different rows
have different digests even for the same changed row. The first version keyed
whole delta frames and shared almost nothing (`privateEncodesPerOp` 178 of
~270 items per change). Now a delta item is a shared segment holding everything
but the closing `,"digest":..,"truncated":..}`, which each connection appends
as a few inline bytes; `replica.ready` items (about 150 bytes, all per
connection) are encoded inline per connection.

Each shared part is encoded through a per-tenant memo keyed by (revision, item
kind, stream token, native) holding up to 16 distinct contents. A connection
reuses an encoded item **only when its own frame is equal (`==`) to the
memoized frame**. The frame each connection would have sent is still computed
by its own visibility-filtered apply; the memo only removes duplicate encoding
and compression. Consequences:

- No visibility risk from sharing: equal content means the connection would
  have sent these exact bytes itself. Class ids never decide who gets what.
- Sharing crosses classes: members of different classes that see the same
  change get the same bytes.
- Bound: 8 MiB of encoded items per tenant memo (FIFO eviction by key).

Interface for workstreams 5/7: `ReplicaBatcher::push(batch, hub, tenant,
native, subscriptions, frame)` takes one replica frame (transaction / delta /
ready) of one connection; `watermark(...)` closes the connection's revision.
When the feed path produces deltas once per (class, collection), it can call
`TenantFrames` once per (class, collection) and push the same `Arc` to every
connection of the class without the equality check (the memo then degenerates
to a lookup).

## Compress once

The server always negotiates `server_no_context_takeover` (`negotiation.rs`,
every accepted offer), so the bytes of a compressed segment depend only on its
content, the server window (9..=12, chosen per connection) and the level.
`SharedText` caches the DEFLATE form per window (at most 7 forms) and every
connection with that window reuses it.

A batch is assembled from compressed segments, without compressing anything per
connection: each shared segment is a complete run of non-final DEFLATE blocks
ending with the sync-flush empty stored block, and starts from an empty
dictionary, so segments concatenate into one valid raw DEFLATE stream (a later
segment's back-references never reach before its own start). Per-connection
bytes (envelope, commas, stream bindings, watermark) are written as stored
blocks. The message ends with an empty stored block whose `00 00 ff ff` is
removed (RFC 7692 7.2.1). Batches below the compression threshold are sent as
plain text. Connections without permessage-deflate get the concatenated text.

Cost of this choice: no cross-item back-references inside one batch, so a batch
of many small items compresses a little worse than compressing the whole batch
per connection. Measured below.

## Benchmarks

`cargo bench -p gonvex-runtime --features perf-fixture --bench delivery`
(1 tokio worker, fixture of `docs/performance/perf-fixture.md`, window 12,
threshold 1024). `apply_feed_event` runs untimed: only the send path is timed.

Machine: arch3 (Ryzen 7 5800X) with other agents' builds running, so compare
medians and allocation counts rather than p99s. "legacy" re-creates the
pre-branch send path in the same binary (per-frame `serde_json`, a new zlib
stream per compressed frame, one watermark frame per change);
"legacy_reuse" is what old clients get now (same frames, reset compressor).
Raw numbers: `BENCH_JSON` lines of the `delivery` bench.

### One task update fanned out to 78 members (send path only)

`delivery/task_update/78_members/*`, 20 iterations, per change:

| Rows | Path | Median | Allocs | Bytes allocated | Frames | JSON bytes | Wire bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| json (`replicaNoOldValues`) | legacy | 730 us | 1,140 | 10.5 MiB | 350.9 | 148 KB | 112 KB |
| | legacy_reuse | 645 us | 1,061 | 640 KiB | 345.8 | 137 KB | 106 KB |
| | **v2** | **589 us** | 2,438 | **633 KiB** | **17.7** | 89 KB | **57 KB** |
| native rows | legacy | 1,251 us | 4,686 | 10.9 MiB | 350.3 | 152 KB | 113 KB |
| | legacy_reuse | 891 us | 3,987 | 1.0 MiB | 343.9 | 138 KB | 106 KB |
| | **v2** | **699 us** | 2,577 | **690 KiB** | **19.4** | 104 KB | **65 KB** |

- v2 encodes 2 shared items per change (4.0 KB of JSON: the transaction and the
  task delta) and reuses them 33 times; the old path serialized ~4.5 frames for
  every one of the 78 members.
- 60 of 78 members see no changed row (the task is not visible to them) but
  still get a `replica.ready` for `tasks.recentWorkspaceTasks` (their digest
  and cursor) and a watermark. In v2 those wait for the idle delay and share
  one frame per second with later changes, so they are not counted per change
  above (JSON/wire bytes of v2 exclude them). Those per-member ready frames are
  the largest remaining per-connection cost; removing them belongs to the
  replica-state work (cursor without per-subscription digests).
- More allocations in v2 (envelope, stream bindings, ready items, digest tails
  are small per-connection allocations) but 16x fewer bytes allocated.

Re-run after merging `perf/replica-state` (232ee8d, no per-subscription row
mirror): same frames and item sharing; json v2 677 us vs legacy 1,167 us
(legacy_reuse 621 us), native v2 828 us vs legacy 1,401 us (legacy_reuse
804 us), 17.7 / 19.4 frames per change, wire 58 / 66 KB vs 113 / 114 KB.
Socket replay: 0.37 vs 0.75 of one core, 10,944 vs 200,431 frames/s. The
machine was busier in this run; medians move 10-40% between runs.

### Compressor reuse (all clients)

| Bench | Before (09f1784) | After | Note |
| --- | --- | --- | --- |
| `ws/compress_text/task_delta` | 12.8 us, 3 allocs, 374 KiB | 8.6 us, 2 allocs, 6 KiB | different sample payload (3,836 vs 3,833 bytes); output bytes are identical to the old compressor (unit test) |
| `fanout/task_update/78_members/apply+json+deflate`, `encodeNsMedian` | 1.80 ms | 1.07 ms | old per-message path, includes `serde_json` |
| same, bytes allocated per change | 123.2 MiB | 110.7 MiB | mostly `apply_feed_event` |

### 1,950 subscribers, 23 task writes/s (micro-driver)

`delivery/driver/1950_subscribers/23_writes_per_s/*`: the 78 real connections'
frames for 92 task writes (4 simulated seconds), each connection replicated 25
times; data batches flush every 50 ms, row-less batches every 1 s. Excludes
`apply_feed_event` and sockets.

| Path | Frames/s | CPU (share of one core) | Bytes allocated per write | Wire MB/s |
| --- | --- | --- | --- | --- |
| legacy, json | 200,431 | 0.375 | 281 MB | 63.1 |
| v2, json | **10,944** | 0.409 | **41 MB** | 62.2 |
| legacy, native | 200,431 | 0.640 | 295 MB | 64.2 |
| v2, native | **10,944** | **0.391** | **42 MB** | 63.3 |

`delivery/socket_send/78_streams_x25/*` replays the same json streams through a
real compressed WebSocket (ratchet framing, permessage-deflate, loopback writes,
client draining on another thread), scaled x25:

| Path | Frames/s | CPU (share of one core) | Wire MB/s |
| --- | --- | --- | --- |
| legacy | 200,431 | 0.618 | 63.8 |
| v2 | **10,944** | **0.360** | 62.2 |

- Frames/s drop 18x, well under the ~20k frames/s budget.
- Without sockets, v2 json costs about the same CPU as legacy json: most legacy
  json frames are below the 1 KiB compression threshold and cost only
  `serde_json`, while v2 sends larger, compressed batches. The saving shows up
  in the native path (canonical row rendering per connection) and per frame at
  the socket (ratchet framing and a write per frame): -42% at the socket.
- Wire bytes barely change: the per-member `replica.ready` digests and ids
  (hex, UUIDs) dominate and do not compress; shared segments compress
  individually (no cross-item back-references).

### Presence beats

`delivery/presence_beats/10_per_tick/78_members/*` (10 beats inside one tick,
every member sees every presence row):

| Path | Median | Allocs | Frames per tick | Wire bytes per tick |
| --- | --- | --- | --- | --- |
| legacy | 3.75 ms | 7,800 | 3,120 | 956 KB |
| v2 | 4.88 ms | 11,725 | **78** | 855 KB |

v2 is slower here in CPU (each beat produces one per-member `replica.ready`
item, encoded inline, plus compression of the batch) but sends 40x fewer frames;
presence transitions themselves are moving to the presence workstream's
in-memory path.

### Correctness

- `cargo test -p gonvex-runtime --features perf-fixture --test delivery_oracle`:
  for every member of the small Whagons fixture, 12 ticks of 1-3 commits each
  (description edits, spot and workspace moves that add/remove rows from
  members' visible sets, presence beats), json and native rows, plain and
  compressed: the expanded `replica.batch` equals, item for item, the frames
  that connection's own apply produced. Memo hits are asserted, so sharing is
  exercised.
- `gonvex-websocket` tests: segment assembly inflates to the concatenated text
  for windows 9..=15 (incompressible, tiny and large segments, >64 KiB inline
  runs); batches flush before any other frame on the wire, plain and
  compressed (windows 9 and 12).
- SDK: `packages/client/src/replica-batch-frames.test.ts`; thin client:
  `bench/whagons/thin-client.test.mjs`.

## Not done / limits

- Changed rows still go SQLite JSON text -> `serde_json::Value` -> JSON text.
  With encode-once that happens once per distinct item instead of once per
  connection; keeping raw `json_object` bytes through the change feed is left
  for when workstream 5 makes the feed path produce per-class deltas.
- `apply_feed_event` still runs per connection. Workstream 5 made it cheap
  (78 us per subscriber) and pure (`replica::decide_row` gives every
  subscription of one collection and visibility class the same answer); the
  next step is to decide once per (class, collection) per event and push the
  same shared item to every connection of the class (the memo equality check
  then becomes a lookup). Not done here: it needs workstream 7's stable class
  ids in the connection loop.
- `replica.ready` items and delta digests stay per connection (aggregate
  digests are always on after workstream 5); they are the largest per-socket
  bytes left.
- Live-query, control and presence frames are not batched (they flush the
  batch). Presence transitions can be added as items when the presence agent's
  per-connection pending slot lands.
- No edge layer; the frame format only avoids socket-specific data in shared
  items.
