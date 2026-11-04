# Omit null-valued columns on the replica wire (`replicaOmitNulls`)

Branch `perf/omit-nulls`, from `perf/resume-lazy` d93290ec (delivery v3 + v4 +
shared snapshot cache). Option 1 of `resume-and-bounded-replicas.md` (c).

Code: `rust/crates/protocol/src/omit_nulls.rs` (new: raw-text stripper, message
transform, reference restore), `ClientCapabilities` / `ServerCapabilities`
`replica_omit_nulls`, `runtime/src/lib.rs` (negotiation, open and feed paths),
`replica.rs` (`omit_open_nulls`, `columns` only on the first snapshot page),
`delivery.rs` (`RowEncoding`: shared items per encoding), `replica_fanout.rs`
(outcome texts per encoding), `packages/protocol/src/index.ts`
(`restoreOmittedNulls`, `restoreReplicaFrameNulls`), `packages/client/src/index.ts`
(restoration on receive, option `replicaOmitNulls`), `bench/whagons/thin-client.mjs`
(JSON rows + restoration). Bench: `benches/omit_nulls.rs`,
`bench/omit-nulls/decode.mjs`, `bench/omit-nulls/lab.py`.

## Why

A Whagons `tasks.recentWorkspaceTasks` row has 52 columns; 26–38 of them are
null in practice, and their `"column":null` pairs are a third (fixture) to
three fifths (lab data) of the row's JSON. They cost wire bytes, compression
input, client parse time and IndexedDB/heap space for nothing.

## Wire contract

### Negotiation

- Client `auth.capabilities.replicaOmitNulls: 1`; server
  `session.ready.capabilities.replicaOmitNulls: 1`. The runtime omits only when
  the client advertised version 1 **and** did not negotiate `replicaNativeRows`
  (native canonical strings are unchanged).
- Other versions or absence: today's rows, byte for byte.
- An `auth` that changes the connection's row encoding (omission on ↔ off)
  never preserves subscriptions (`subscriptionsPreserved` is not set); the
  client reopens and learns columns again.
- The SDK advertises it by default (`new GonvexClient(url, { replicaOmitNulls:
  false })` turns it off). A new SDK against an old runtime receives full rows
  and passes them through unchanged.

### What may be omitted

A sender **may** omit any subset of a row's top-level members whose value is
`null`; omitting none is valid. The runtime omits:

| frame | rows | omitted members |
| --- | --- | --- |
| `replica.snapshot`, `replica.snapshotPage` | `result[]` | every top-level `null` |
| `replica.delta` (plain or a `replica.batch` item) | `upserts[]` | every top-level `null` |
| `replica.transaction` (plain, batch item, inside `replica.rowPatch`) | `changes[].newValue` of `operation: "update"` | top-level `null` members **not** in `changedColumns` (if `changedColumns` is empty: none) |

Nested values (JSON columns) are never changed. `oldValue`, inserts, deletes,
`replica.snapshotEncodedPage` / `canonicalUpserts` / `replica.transactionEncoded`
(native) are unchanged. Field patches (`replica.rowPatch`) are computed over the
omitted rows: a column that becomes null is an `unset`, null → value a `set`.

### Columns

Every message answering a `replica.open` carries the subscription's projected
columns, in row order:

- `replica.snapshot`: `columns` (top level, like the other snapshot metadata);
- `replica.snapshotPage`: `columns` on the page with `sequence: 0` only;
- `replica.delta` of an open (a resume): `columns`;
- `replica.ready` of an open: `columns`.

Feed frames (deltas, readies, batch items) never repeat it. The client keeps
`columns` per subscription id for the socket (feed frames for a subscription
are sent only after its open answer; the server already holds feed events
while a snapshot is paged).

### Client restoration

Before any other handling of a frame (after `replica.batch` expansion and
`replica.rowPatch` decoding): learn and remove `columns`; then every row of a
snapshot, snapshot page or delta of a subscription with known columns becomes
`{ ...nullTemplate(columns), ...row }`: every column in column order, `null`
where absent, then any other members (none in practice). Transactions are not
restored: the SDK merges `newValue` into the entity it holds
(`{...held, ...newValue}`), where an omitted (unchanged, null) column already is
`null`. From then on the SDK hashes, stores, persists and publishes exactly the
rows a runtime without the capability sends; IndexedDB records are unchanged,
so no migration.

### Examples (old → new)

Snapshot (one row; `columns` abbreviated to 5 of 52):

```json
{"type":"replica.snapshot","id":"tasks","result":[{"_id":"t1","name":"Fix","spotId":null,"dueDate":null,"flagColor":null}],"cursor":{"epoch":"e","revision":9},"key":"_id","digest":"xor1:…"}
{"type":"replica.snapshot","id":"tasks","result":[{"_id":"t1","name":"Fix"}],"cursor":{"epoch":"e","revision":9},"key":"_id","columns":["_id","name","spotId","dueDate","flagColor"],"digest":"xor1:…"}
```

Paged snapshot: page 0 adds `"columns":[…]`; later pages only lose their nulls.

Ready of an open (resume without changes):

```json
{"type":"replica.ready","id":"tasks","cursor":{"epoch":"e","revision":12},"digest":"xor1:…","truncated":false}
{"type":"replica.ready","id":"tasks","cursor":{"epoch":"e","revision":12},"digest":"xor1:…","truncated":false,"columns":["_id","name","spotId","dueDate","flagColor"]}
```

Feed batch for a task update (`flagColor` set to null, `dueDate` stays null):

```json
{"type":"replica.batch",…,"items":[
 {"type":"replica.transaction","cursor":{"epoch":"e","revision":13},"changes":[{"entity":"tasks","id":"t1","operation":"update","newValue":{"_id":"t1","name":"Fix 2","spotId":null,"dueDate":null,"flagColor":null},"changedColumns":["name","flagColor"]}]},
 {"type":"replica.delta","id":"~5f…","cursor":{"epoch":"e","revision":13},"upserts":[{"_id":"t1","name":"Fix 2","spotId":null,"dueDate":null,"flagColor":null}],"deleted":[],"path":"tasks.recentWorkspaceTasks","originCommandIds":[],"digest":"xor1:…","truncated":false},…]}
{"type":"replica.batch",…,"items":[
 {"type":"replica.transaction","cursor":{"epoch":"e","revision":13},"changes":[{"entity":"tasks","id":"t1","operation":"update","newValue":{"_id":"t1","name":"Fix 2","flagColor":null},"changedColumns":["name","flagColor"]}]},
 {"type":"replica.delta","id":"~5f…","cursor":{"epoch":"e","revision":13},"upserts":[{"_id":"t1","name":"Fix 2"}],"deleted":[],"path":"tasks.recentWorkspaceTasks","originCommandIds":[],"digest":"xor1:…","truncated":false},…]}
```

## Hashes are unchanged

Row hashes and collection digests stay SHA-256 / `xor1` / `sum128` over the
canonical **full** projection (nulls included), on both sides; the server never
hashes an omitted row (omission is the last step of encoding for one
connection, after hashing and digest updates), and the client hashes only
restored rows. Restoration is exact because the server's rows always contain
every column of the projection (SQLite `json_object` over the declared
columns; feed rows from `project_row`, which iterates the same columns) and
the restored object has the same members, values and order.

Proof by test:

- `tests/raw_rows_oracle.rs::omitted_null_rows_restore_to_identical_text_and_hashes`:
  every row of every table of both fixture tenants (2,811 rows; 2,406 with a
  null member), read with the snapshot encoding over all declared columns:
  stripped by the runtime's raw-text pass, restored with the column list,
  re-serialized: identical text (key order included), identical raw-path and
  `Value`-path hashes.
- `omit_nulls` unit tests: all-null rows, rows without nulls (unchanged),
  escapes and `"null"` strings, nested nulls kept, whitespace, unparseable
  rows left alone, transaction `changedColumns` rule, inserts unchanged.
- `tests/replica_state.rs::omit_nulls_clients_hold_exactly_the_visible_rows`:
  the full client-model scenario (10 members × 10 collections, edits, a column
  becoming null, null becoming a value, insert, moves, grants, principal
  changes, deletes, resumes from old cursors, 200-change resume, divergence
  repair via `needHashes`, cursor outside retention) with every message encoded
  as the runtime sends it to such a client, round-tripped through its JSON text
  and restored: no digest mismatch, rows equal to a fresh SQL snapshot after
  every step (82,910 null members restored).
- `tests/class_sharing_oracle.rs`, `tests/delivery_oracle.rs`: batch frames of
  `RowEncoding::OmitNulls` connections from the fan-out equal the connection's
  own full-path frames, and equal the pre-batch per-connection frames with the
  omission applied.
- `tests/omit_nulls_socket.rs`: real WebSocket, an omitting and a plain socket
  of one member open all 10 collections (paged, batch frames); restored rows
  equal the plain socket's rows as text.
- SDK `replica-omit-nulls.test.ts`: digests verified by the SDK itself (no
  inline hashes) over restored rows, paged snapshots with `columns` on page 0,
  batch deltas, compact transaction merge, key order, `__proto__` columns.

## Results

arch3, `taskset -c 3`, machine load 5–7 (other agents' builds); Node 22.23.
"Before" is the JSON encoding of the same process (code identical to the base
for that path). Raw JSON: `/home/gabriel/work/gonvex-perf/omit-nulls-out/`.

`cargo bench -p gonvex-runtime --features perf-fixture --bench omit_nulls`
(Admin member, cache-hit open, pages as the connection loop pages them,
DEFLATE per message as the socket compresses: no context takeover, window 15).

| benchmark | before | after | change |
| --- | --- | --- | --- |
| fixture shape (9,000 tasks, 400 B descriptions): Admin tasks snapshot, raw | 16,264,417 B | 10,777,046 B | −33.7 % |
| same, deflated | 3,612,027 B | 3,254,803 B | −9.9 % |
| el-rey (10,500 tasks, 400 B): tasks snapshot raw / deflated | 18,974,698 / 4,214,053 B | 12,571,959 / 3,797,504 B | −33.7 % / −9.9 % |
| same, pages | 73 | 49 | |
| el-rey, 16 B descriptions: raw / deflated | 14,937,013 / 2,651,592 B | 8,535,128 / 2,215,152 B | −42.9 % / −16.5 % |
| lab tenant `8ec9f43e…` (9,181 real-schema rows, `lab.py`): raw / deflated | 12,550,950 / 182,950 B | 5,216,427 / 112,487 B | −58.4 % / −38.5 % |
| el-rey 400 B: all 10 collections, raw / deflated | 22,999,624 / 5,632,533 B | 16,433,657 / 5,206,799 B | −28.5 % / −7.6 % |
| small collections: `statuses` / `tags` / `listMembers` / `listMemberTeams` raw | 3,201 / 8,031 / 55,950 / 20,188 B | 2,526 / 6,498 / 46,652 / 17,867 B | −21 / −19 / −17 / −11 % (deflated ±1 %) |
| collections with few nulls (`presence.list`, `workspaces.list`, `workspacePreferences`) | | +52–83 B (`columns`) | +0.5–6 % |
| task update, batch frame per receiving member (el-rey, 78 members × 10 collections, 1,628 frames): raw | 3,891 B | 2,676 B | −31.2 % |
| same, deflated | 1,493 B | 1,195 B | −20.0 % |
| same, update that sets a column to null | 3,875 / 1,482 B | 2,632 / 1,184 B | −32 % / −20 % |
| same, fixture shape (1,580 frames) raw / deflated | 3,882 / 1,495 B | 2,666 / 1,196 B | −31 % / −20 % |
| server: omission pass per Admin tasks open (10.5k rows) | 0 | 17–18 ms (scan 11.8 + `JsonRow` validation 5.1), 76,639 allocations | **+1.6 µs/row** |
| server: page JSON encode, same open | 3.8 ms | 3.5 ms | |
| server: DEFLATE of the pages, same open (noisy) | 24.8 / 22.6 / 19.8 ms | 26.0 / 19.1 / 13.3 ms | (el-rey 400 / fixture / 16 B) |
| server: batch frame encode per receiving member, unshared (upper bound) | 19.1 µs | 21.5 µs | +2.4 µs |
| client: `JSON.parse` + restoration of the Admin tasks pages (Node 22) | 27.1 ms | 23.4 ms (parse 18.5 + restore 4.9) | −14 % |
| same, 16 B descriptions / lab rows | 27.3 / 20.6 ms | 22.3 / 11.6 ms | −18 % / −44 % |
| server snapshot-cache entry, el-rey tasks (estimate) | 27.07 MB | 27.07 MB | unchanged |
| same: row text in it (with nulls / without) | 18.92 MB | (12.53 MB if stored without nulls) | (−6.39 MB, −24 % of the entry; not done) |
| client stored rows (IndexedDB, heap) | | unchanged | rows are restored before storage |

Notes:

- Compressed savings are much smaller than raw savings: DEFLATE already
  encodes the repeated `"column":null` pairs cheaply. The raw savings still
  apply to the client's parse input, string memory and `maxBytes`-free paths;
  the compressed ones are what the network carries.
- Server CPU: the omission pass costs ~1.6 µs per row on a cold open (snapshot
  rows are SQLite's text; the pass scans it and builds new text). It runs on
  the offload pool inside the open, not on the network thread. DEFLATE time is
  dominated by match search and drops only a little with the smaller input, so
  a cold Admin open costs ~15–20 ms more CPU (≈ +45–60 % of a cache-hit
  open's total: open ≈ 5 ms + page encode 3.8 ms + DEFLATE 25 ms). Resumes (the normal reload) send only log deltas and pay
  nothing measurable. Feed items are encoded once per (tenant, revision,
  content, encoding) and shared, as before; the omitted variant is a separate
  shared item, so a tenant whose connections use both encodings encodes both.
- The client restores with a per-column-list template (`{...template,
  ...row}`): V8's object-clone fast path is faster than parsing the nulls.
  The first version (building each row key by key) took 41 ms for the same
  pages; in-place `row[c] = null` took 30 ms.

## Risks

- **Server CPU on cold opens** (above). Follow-up: keep the null-free text in
  the shared snapshot cache entry instead of the full text (−24 % entry
  memory, no per-open pass; legacy and native opens would then re-insert the
  nulls, and visibility/ACL decoding, which reads the entry text, must treat
  an absent column as null). Not done here.
- **Transactions** rely on `changedColumns` being exact (it is computed by the
  capture trigger with `OLD.c IS NOT NEW.c`, collapsed changes take the union);
  an empty list keeps every null. If a client entity holds a stale non-null
  value for a column that only a collection no longer holding the row
  projects, a legacy `newValue` would reset it to null one frame earlier than
  the omitted one (the delta of a collection that holds the row again carries
  the full row in the same batch).
- **A server row that lacks a declared column** (not null: absent, e.g. a
  change-log image older than an `ALTER TABLE ADD COLUMN`) is restored as
  null and then hashes differently from the server's hash: the client sees a
  digest mismatch and reopens (fails safe). Today such a row already hashes
  differently in snapshots and feeds.
- **Missing columns on the client** (a bug, or a frame for a subscription whose
  open answer was lost) leave rows unchanged: digest mismatch → reset →
  reopen with `columns`. Never silently wrong rows.
- **`columns` costs bytes on open answers**: ~0.9 KB for the 52-column tasks
  collection on its snapshot (first page) and on every open's `ready`,
  including a resume without changes.
- Native-rows clients (and the thin client's default native mode) are
  unchanged.

## Tests

- `cargo test -p gonvex-runtime -p gonvex-store -p gonvex-protocol -p gonvex-websocket
  --features gonvex-runtime/perf-fixture,gonvex-websocket/perf-fixture`: 530
  passed, 0 failed, 8 ignored (includes `raw_rows_oracle`, `replica_state` ×2,
  `class_sharing_oracle`, `delivery_oracle`, `snapshot_cache_oracle`,
  `omit_nulls_socket`).
- `cargo test --workspace --all-targets --no-run` (no features) compiles.
- SDK `packages/client` vitest: 1,059 / 1,059 (4 new). `packages/protocol`,
  `packages/client` typecheck. Thin client `node --test
  bench/whagons/thin-client.test.mjs`: 10 / 10 (1 new).

## Not done / not measured

- Snapshot cache entries still hold rows with nulls (see Risks).
- Client IndexedDB size: unchanged by design (restored rows are stored).
- No browser measurement; client decode measured in Node 22 only.
- Real staging data was not read; the lab tenant is synthetic load data.
- `replica.transaction` inserts keep their nulls (exact entity key order).
