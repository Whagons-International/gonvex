# Replica delivery v4: per-tenant fan-out, raw JSON snapshot rows, call completion

Branch `perf/delivery-v4`, from `perf/delivery-v3` a924e623 (+ its review fix
adb6bcd9) and `origin/deploy/sqlite-staging` (export-sqlite only). Read
`delivery-v3.md` first.

Code: `rust/crates/runtime/src/replica_fanout.rs` (new), `canonical_row.rs` (new),
`replica.rs` (recording, raw scan, pages), `feed_share.rs` (layout and outcomes per
event), `delivery.rs` (shared pushes, waiting calls), `visibility_class.rs`
(`class_default_memo`, allocation-free context atoms), the feed and call branches of
the connection loop in `lib.rs`, `protocol/src/lib.rs` (`JsonRow`). Tests:
`tests/class_sharing_oracle.rs` (new fan-out oracle), `tests/raw_rows_oracle.rs`,
`tests/call_watermark_latency.rs`, unit tests in `canonical_row.rs`, `delivery.rs`,
`visibility_class.rs`.

## 1. Per-tenant fan-out (`replica_fanout`)

One committed transaction is applied in full by the first connection of each
*shape*; every other connection of that shape appends the shared, already-encoded
items and updates only its own digest, window counters and cursor.

A connection's shape is, per subscription in id order: whether the event reaches it
(cursor ahead / source untouched / source changed) and, for collections of tables the
event changed, two bits per changed row (does its compiled visibility admit the old
and the new image: its class default, memoized once per class and row), its
collection definition, its arguments (interned) and its stream identity. `decide_row`
is a function of exactly those visibility answers plus definition, arguments, images
and window, so members of *different* classes that see the changed rows the same way
share one outcome. (Keying by class id, the first attempt, gave ~740 distinct shapes
per write for 1,950 members and was slower than v3.) Most members of a tenant see
none of a given write, so they share one "advance the cursor" shape.

A connection has a shape only when its result is a function of it:

- every subscription the event reaches has a version 2 plan without inherited
  visibility (`inherits` / `inheritsAny`) and a compiled binding, and no table its plan
  reads besides its source (members, any set or join table, permission tables)
  changed: its step is `Skip` or `Apply` with an unchanged principal, no overlay and
  no affected-row read;
- its member is not *hit* by a changed row (no member atom true on the old or new
  image);
- its windows are complete (not truncated), its cursors are of the event's epoch, its
  shared indexes are at the event's revision right now (not waiting, not ahead);
- its streams have distinct identities (default stream tokens on the socket); a
  bounded window stays within budget after the outcome.

Everything else (personal grants and other hit members, principal changes, truncated
windows, inherited plans, waiting indexes) takes the full path as before. The full
path of the first connection of a shape records the outcome only when it ran clean
(no resync, default context); shapes hold their definitions and arguments by `Arc`,
so an address cannot be reused while an outcome keyed on it lives (4 events per
tenant, as v3's shares).

Per connection on the shared path: shape (no per-row work for connections rejected
by static checks), one hash lookup, digest add/remove of the outcome's hashes, window
counters, cursor, and for the receivers the delta item (shared `SharedText`) plus
their own tail `,"digest":…,"truncated":…}` and a ready text assembled from shared
prefix/suffix around their digest. Byte-identical to the full path: the delivery
oracle compares every batch frame.

Where it runs: in the connection loop the shared path runs inline on the network
thread (it is microseconds); a miss runs the offloaded full path, which records the
outcome. `GONVEX_REPLICA_FANOUT=0` (or class sharing off) disables it.

Also removed from every path: `hex_digest` formatted each byte into its own
`String` (32 allocations per row hash); member context atoms formatted numeric
values into a new string per member per row (`scalar_eq`).

## 2. Raw JSON snapshot rows (`canonical_row`, `JsonRow`)

Snapshot rows stay the JSON text SQLite produced (`json_object` over the collection's
columns) from the reader thread to the wire: `ReplicaSnapshot.result` and
`ReplicaSnapshotPage.result` are `Vec<JsonRow>` (a `Box<RawValue>`, serialized
verbatim). Only the key and order columns are parsed (byte ranges of the top-level
members).

Row hashes stay byte-identical to `stable_json(serde_json::from_str(row))`: one pass
over the bytes checks that the row is canonical up to (a) the order of its top-level
columns and (b) the spelling of non-integer numbers. Number tokens that serde_json
would not read back as an unchanged i64/u64 (a REAL written by SQLite with 17
significant digits such as `0.10000000000000001`, `1.0`, `1E5`, `-0`, integers beyond
64 bits) are parsed as f64 (correctly rounded, as serde_json with `float_roundtrip`)
and replaced by `ryu_js`'s output, exactly what `stable_json` writes. The canonical
text is then the row itself (borrowed) or the row re-assembled in a reused scratch
buffer (sorted members, replaced numbers). Anything else not provably canonical (an
escape serde would not write, U+2028/U+2029, unsorted or duplicate nested object
keys, whitespace, nesting beyond 64, a non-finite number) falls back to parse +
`stable_json`. Native pages (`canonicalRows`) carry the canonical text, as before.

Proof: `canonical_row` unit tests (targeted REAL / integer-range / escape / unicode /
surrogate / ordering / nesting cases, plus 113 rows written by SQLite for every
column kind with REAL, BOOLEAN, JSON, BLOB, unicode and control characters, both
hashes compared); `tests/raw_rows_oracle.rs`: every row of every table of both
fixture tenants (2,811 rows, all declared columns) hashes identically (71% of them
on the borrowed/re-assembled path, the rest are internal tables with nested JSON).

Not streamed: the snapshot start frame carries the digest of the whole window, so
rows are still collected before the first page; they are held as raw text (≈ wire
size) instead of `Value` trees.

## 3. Call completion never waits for the batch timer

Gate finding (delivery-v3 integration, heartbeat p99 1.4 s): with batch frames, a
call whose commit sends its caller no rows completed only when the watermark-only
batch was sent. Two defects in the batcher:

1. it kept a single `urgent_through` = the *maximum* waiting revision; a second call
   (higher revision) made the batch that already covered the first call's revision
   look non-urgent, so it waited for the 1 s idle delay;
2. a covered call still waited for the 50 ms tick, and found reducer-receipt
   results (`QueryResult` of the receipt query) never marked the batch urgent.

Now the batcher keeps every waiting revision (`BTreeSet`), `call_waiting()` is true as
soon as the open batch's watermark covers the smallest one, the connection flushes
the batch right after queuing such a watermark, and `deadline()` returns "now" for
it. `tests/call_watermark_latency.rs`: real WebSocket, `replicaBatchFrames`, one open
collection, 8 `presence.transition` calls (each commits the caller's presence row,
which reaches none of its collections): result + covering watermark in 17–28 ms each
(debug build, `synchronous=full`); with the old tick behaviour forced the same test
measures 61–76 ms and fails. Unit tests cover the masking case.

## Benchmarks

arch3 (Ryzen 7 5800X shared with other agents, load average 8–12), benches pinned
with `taskset -c 3`, one tokio worker. Before = `perf/delivery-v4` at e00488ba
(= delivery-v3 + review fix + staging, bench file with the same feed/flush split
timers added), after = this branch (code identical to the head for every measured
path). Two paired rounds; the table shows round 2, round 1 agreed within noise.

`cargo bench -p gonvex-runtime --features perf-fixture --bench delivery_v3` (and
`--bench replica_state`, `--bench stalls`)

| benchmark | before | after | change |
| --- | --- | --- | --- |
| 1,950 members (`tasks.recentWorkspaceTasks`), 23 writes/s, real compressed socket: CPU share of one core (2 runs) | 0.383 / 0.481 | **0.226 / 0.225** | −41..53% |
| same: busy ms per write (feed + flush) | 16.7 / 20.9 | **9.8 / 9.8** | −41..53% |
| same: feed only, ms per write / ns per member per write | 14.6 / 7,504 ; 18.3 / 9,361 | **7.3 / 3,728 ; 7.1 / 3,631** | −50..61% |
| same: shared fan-out path, ns per connection (97.5% of member-writes) | (no fan-out) | **2,270 / 2,514** | |
| same: full path, ns per connection (48.7 connections per write: hit members, recordings) | | 59,532 / 45,837 | |
| same: allocations / bytes allocated per write | 146,562 / 23.0 MB | **30,024 / 5.44 MB** | −80% / −76% |
| same: frames/s, wire MB/s | 6,334.5, 14.37 | 6,334.5, 14.37 | unchanged |
| same: flush (socket) ms per write | 2.0 / 2.7 | 2.6 / 2.7 | unchanged |
| one task update → 78 members × 10 collections, json / native: median | 2.55 / 2.67 ms | 2.68 / 2.55 ms | unchanged (noise) |
| same: allocations per change, json / native | 17,470 / 15,923 | 16,315 / 14,900 | −7% |
| snapshot open, 9,000 tasks, Admin: time / peak heap | 176 ms / 73.5 MB | **156 ms / 17.7 MB** | −11% / **−76%** |
| same: allocations | 1,140,316 | **338,061** | −70% |
| snapshot open TeamAllSpots (3,411 rows): time / peak | 80.5 ms / 28.0 MB | 72.5 ms / 7.0 MB | −10% / −75% |
| snapshot open TeamSpots (652) / GrantOnly (45): peak | 5.68 / 0.55 MB | 1.71 / 0.28 MB | −70% / −50% |
| reconnect burst 200 connections × 10 collections: peak heap / seconds | 180.1 MB / 22.4 | **70.1 MB / 17.7** | −61% / −21% |
| reconnect burst 800: peak heap | 144.9 MB | **70.3 MB** | −51% |
| reconnect burst 1,600: peak heap | 185.7 MB | **114.7 MB** | −38% |
| open all 78 members × 10 collections: peak heap | 97.4 MB | 41.6 MB | −57% |
| retained per member (78 × 10 collections) | 377,222 B | 377,323 B | unchanged |
| stall driver, burst of 200 opens of 9k tasks: peak heap (3 runs) | 134 MB | 35 MB | −74% |
| network-thread polls > 5 ms: fan-out 4 updates/s; burst of 200 opens | 0 ; 0 (×3) | 0 ; 0 (×3) | stays 0 |
| same with `STALL_BATCH=1` (batch connections, fan-out inline on the network thread) | n/a | 0 in 5 of 5 instrumented runs; longest inline fan-out call 448 µs | |
| snapshot wire bytes, 9,000 tasks | 16,205,721 | 16,231,088 | +0.16% (REAL spelling, see below) |

Notes:

- Per-member cost on the shared path is ~2.3–2.5 µs, not ≪ 1 µs. It includes the
  bench's own per-connection work that the old path has too (the event clone, the
  batch header and watermark formatting of `ReplicaBatcher::watermark`: about half of
  the ~7.5 allocations per connection) and is dominated by touching each connection's
  scattered state (subscription, member binding, batcher) on a loaded machine. The
  rest of the feed cost per write is the ~49 hit members per write that decide alone
  (~50 µs each, by design).
- The 78-member benchmark is unchanged on purpose: every connection there opens the
  inherited `taskUsers` / `taskTags` collections, which the fan-out does not serve
  (rejected before any per-row work).
- One earlier batch-mode stall run (1 of 4, before the instrumented runs) showed a
  single 6.5 ms connection poll that the 5 instrumented runs did not reproduce
  (longest inline fan-out 448 µs, longest push 173 µs); the machine was at load ~8.
- "As many reconnects as fit in 2 GB": 1,600 concurrent reconnects (16,000 collection
  opens) peak at 115 MB; snapshot admission bounds concurrent builds, so the peak does
  not grow linearly. Larger bursts were not run (the fixture tenant has 78 members;
  bursts reopen members round-robin).

## Wire changes

1. Snapshot rows (`replica.snapshot` / `replica.snapshotPage` `result`) are SQLite's
   JSON text verbatim. Key order is unchanged (serde_json `preserve_order` already kept
   the column order). Values parse identically; the text differs only where SQLite and
   serde spell the same value differently: REAL with 17 significant digits
   (`0.10000000000000001` instead of `0.1`), numbers inside JSON columns as stored
   (`1E5`, `1.0`). Client-visible hashes are unchanged (the SDK hashes parsed values;
   native pages carry the canonical text as before; proven by the oracles).
2. Fan-out: none (every batch frame byte-identical, oracle).
3. Calls: a watermark-only batch covering a waiting call is sent immediately instead
   of at the tick / idle deadline.

## Tests

- `cargo test -p gonvex-runtime -p gonvex-store -p gonvex-protocol -p gonvex-websocket
  --features gonvex-runtime/perf-fixture,gonvex-websocket/perf-fixture`: 521 passed,
  0 failed, 8 ignored (includes `class_sharing_oracle` with the new
  `fan_out_frames_equal_each_connections_own_frames`, `delivery_oracle`,
  `replica_state` SQL visibility oracle, `raw_rows_oracle`, `call_watermark_latency`,
  `slow_reader_delivery`, `offload_equivalence`, `presence_v2`, `visibility_fanout`).
- Fan-out oracle: three subscription sets per member (all 10 Whagons collections;
  tasks only; presence + statuses + tags + tasks), half the connections native rows,
  32 events (edits, spot/workspace moves, presence, insert, grant assign/unassign, team
  gained/lost, delete): 1,152 frames, every frame, unbatched frame, digest, cursor and
  row count identical to the connection deciding alone; 449 connections served by the
  fan-out (395 with decided rows), 114 outcomes recorded.
- `cargo check -p gonvex-runtime -p gonvex-protocol -p gonvex-store -p gonvex-websocket
  --all-targets` (no features) compiles; every fixture test has `required-features`.
- `cargo clippy -p gonvex-runtime -p gonvex-protocol --features
  gonvex-runtime/perf-fixture --lib --tests --benches -- -W clippy::redundant_clone`:
  no warnings in the new code, no `redundant_clone` in touched files.
- SDK `packages/client` vitest: 1,055 / 1,055. Thin client `node --test
  bench/whagons/thin-client.test.mjs`: 9 / 9.

## Not done

- Feed row images are still decoded into `Value` (once per transaction per tenant,
  shared by every connection through the event `Arc`); only snapshot rows are raw.
- Inherited plans (`taskUsers`, `taskTags` and other `inherits` collections) never use
  the fan-out; a connection holding one takes the full path for events that reach it.
- Snapshot pages are not streamed while SQL runs (the start frame carries the whole
  window's digest); rows are held as raw text until paged.
- Presence items in the batch are still per connection (not shared `SharedText`).
