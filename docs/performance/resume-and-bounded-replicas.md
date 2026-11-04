# Resume without full-set queries, shared snapshot cache, bounded task replicas

Branch `perf/resume-lazy` (from `perf/delivery-v3` adb6bcd9, with
`perf/delivery-v4` d7254de0 merged in). Code:
`rust/crates/runtime/src/snapshot_cache.rs` (new module), the open path in
`replica.rs` (`open_replica_with`, `collection_rows`, `cached_view`,
`build_cache_entry`), `visibility_index::{exactly_at, usable_revision}`,
`visibility_class::{ClassScope::decode_owned, AclRow::refresh, AclRow::stale}`,
and `gonvex_protocol::JsonRow`, which now holds `Arc<RawValue>` instead of
`Box<RawValue>`. Clones share the text; the wire format is unchanged. Tests:
`tests/snapshot_cache_oracle.rs`, unit test in `snapshot_cache.rs`, the
existing `tests/replica_state.rs` (its SQL oracle now uses
`open_replica_fresh`). Bench: `benches/snapshot_cache.rs`.

## (a) What resume did

Staging (2026-11-03, one user) showed CPU spikes of `SELECT json_object(...)`
returning 10,501 rows (1.1–4.4 s). That is `tasks.recentWorkspaceTasks`
(`mode: "progressive"`, no `maxRows`, 52 columns, `maxBytes` 200 MiB).

Yes: a resume with a valid cursor ran the full visibility query. In
`open_replica_negotiated`, the `xor1`/`sum128` branch (`DigestRequest::Aggregate`)
read the change log since the cursor, and then streamed the *whole* visible
collection from SQL (`scan_window`). It canonicalized and SHA-256-hashed
every row (before delivery v4 it also parsed each one into
`serde_json::Value`), only to compute the current aggregate that verifies the
client's digest. The
`WholeSet` and keys+hashes (`Reconcile`) branches scan too. The web client
resumes on every reload and every reconnect (see (b)), so each reload of each
member paid a full snapshot build of every collection on the server, while
the wire carried only the delta.

## Design: always base + log delta

The base is one of two things:

- **The client's own copy** (a resume with a valid cursor). Changes since the
  cursor come from `_gonvex_sync_changes`, filtered by visibility, as before.
  The integrity check no longer scans SQL. The current aggregate is computed
  in memory from the shared cache entry below plus the log.
- **A shared cached snapshot** (a cold open, or the verification of a
  resume). The entry is keyed by (tenant database, project, module
  generation, collection path, arguments). It holds every row the
  collection's own filters admit at revision `R`, in snapshot order, with
  each row's projection as SQLite's raw JSON text (delivery v4's `JsonRow`,
  sent as is, shared by every open), canonical hash, byte size, its
  (order value, key) and, once the first compiled open needs them, its
  decoded ACL columns. Building it parses only the key and order columns
  (`canonical_row`), like delivery v4's scan. It applies no visibility.

An open at clock `C`:

1. takes the entry (built if missing: single-flight per key; concurrent opens
   wait for the one build) *before* its read transaction, so `R ≤ C`;
2. reads the source table's log `(R, C]` in its read transaction; rows at `C`
   are the entry's rows with every logged key replaced by its last
   after-image (deleted keys removed, new keys merged in snapshot order);
3. filters those rows with the member's principal at `C` in memory: the
   compiled class program for version 2 plans (`MemberVisibility::visible`),
   `visibility::row_matches` otherwise, plus the live-membership check that
   the SQL predicate contains (one `EXISTS` query);
4. applies `maxRows`/`maxBytes` in order exactly like `collect_window`
   (row then byte budget, boundary = last row's (order value, key)).

The result is what a fresh SQL snapshot at `C` returns. A cold open sends it
as the snapshot. A resume uses its aggregate to verify the client's digest
(`client == current − hash(new images) + hash(old images)`, unchanged) and
sends the log delta, unchanged.

Why the entry is per collection and not per visibility class: a class shares
only class atoms. Members of one class still differ by member atoms (task
assignment, authorship, shares), so a class-level row set is not any
member's set. Sending it and then correcting it would put rows on the wire
that the member cannot see. One principal-free entry per collection serves
every class and member, is smaller than one entry per class (×45 classes),
and needs no invalidation on principal or visibility changes. Every open
evaluates its own principal at its own clock.

### When the full SQL query still runs

- No entry yet for the key (first open after start, a module redeploy that
  changes the generation, eviction, maximum age): one SQL read of the
  collection *without* a visibility predicate, shared by every open that
  waits for it.
- The open falls back to the old per-member SQL scan (`stats.fallbacks`) when:
  - the entry is from another database epoch, or older than the log's
    retention (`R < retained_revision`). The entry is dropped and the next
    open rebuilds;
  - a version 1 plan has a `maxRows`/`maxBytes` budget whose sets stay in SQL
    (`bounded_sql_visibility`);
  - a parent ACL or derived-set index the principal evaluates through is not
    exactly at `C` after up to 3 tries, or it advanced while the open
    evaluated. In-memory visibility is only equal to SQL visibility at the
    index's revision. When an index is behind or ahead, the open commits its
    read transaction, waits for the index applier (up to 2 s) *without*
    holding a connection, and begins a new read at a newer clock. An earlier
    version waited while holding the read connection: under a write storm that
    starved the pool the change feed needs (`PoolTimedOut` in the storm bench).
  - a budget would truncate an unordered collection (SQL's choice of rows is
    not reproducible);
  - the cache is disabled (`GONVEX_SNAPSHOT_CACHE=0`), or the build failed.
- `needHashes` (a client reconcile with keys and hashes) is unchanged in when
  it happens: a principal or visibility input changed since the client's
  cursor, the digest disagrees, or a truncated window moves. Its streamed
  comparison now also reads the cache rather than SQL.

No protocol or SDK change: the messages, digests, inline hashes and windows
are byte-identical to the fresh path (the oracle compares them).

### Staleness policy and knobs

| Variable | Default | Effect |
| --- | --- | --- |
| `GONVEX_SNAPSHOT_CACHE` | on | `0` disables the cache (old behaviour). |
| `GONVEX_SNAPSHOT_CACHE_BYTES` | 256 MiB | Estimated heap of all entries; least recently used entries are evicted beyond it. An entry larger than the budget is used by its open and not kept. |
| `GONVEX_SNAPSHOT_CACHE_ROLL_CHANGES` | 256 | An open whose delta has at least this many log rows replaces the entry with its merged rows at `C` (roll-forward from the log: no SQL). |
| `GONVEX_SNAPSHOT_CACHE_ROLL_REVISIONS` | 512 | ... or that is this many revisions behind. |
| `GONVEX_SNAPSHOT_CACHE_MAX_AGE_SECS` | 900 | Rebuild from SQL when the last SQL build is older (bounds the effect of any incremental bug; roll-forwards keep the original build time). |

Builds and views run inside the replica open, which already runs on the
offload pool (`replica_open_queue`), not on the network thread. A build holds
a snapshot admission permit like a snapshot build. Opens wait for a build
without holding one, so this cannot deadlock.

Compared with the brief's sketch, two choices differ:

- **Shared rows, per-connection page frames.** Every page frame carries the
  subscription id, snapshot id and cursor, so page bytes differ per
  connection. Sharing compressed pages would need a protocol change
  (content-addressed page bodies). Rows are shared raw text (`JsonRow` is
  `Arc<RawValue>`): nothing is re-read, re-decoded or re-hashed, but page
  encoding is still per connection.
- **The collect-before-send step is cheap on a cache hit**, without a
  precomputed digest. The first frame needs the member's window digest. A
  per-class digest cannot be precomputed: members of one class differ by
  member atoms, so windows and digests are per member. On a hit, the window
  and its digest come from the in-memory filter of the cached rows (~5 ms for
  10,500 rows). "Collecting" the rows is pushing `Arc` clones, so the start
  frame goes out without waiting on SQL. Only a miss still streams SQL first,
  once per collection and tenant.
- **No per-class invalidation.** The entry holds no visibility decision, so
  there is nothing to invalidate.

## Results

`taskset -c 3 cargo bench -p gonvex-runtime --features perf-fixture --bench snapshot_cache`
(one CPU, one tokio worker), on the merged base (delivery v4 underneath
*both* sides). Fixture: one el-rey-sized tenant, 10,500 tasks with the real
Whagons 52-column schema and ~400-byte descriptions, 78 members, 10
collections each. "Before" is delivery v4's SQL path (`open_replica_fresh`;
the storm switches the cache off), measured in the same process. Medians of 5
iterations. Allocations are process-wide. The machine is shared with other
agents' builds, so wall times vary by about ±30 % between runs; allocation
counts are stable. Raw JSON:
`/home/gabriel/work/gonvex-perf/resume-lazy-out/cache-merged.jsonl`.

### Resume (a): `tasks.recentWorkspaceTasks`, client cursor + `xor1` digest

| member (rows) | changes since cursor | before: time / allocs | after: time / allocs |
| --- | --- | --- | --- |
| Admin (10,500) | 0 | 156 ms / 422,453 | 8.0 ms / 8,940 |
| Admin | 10 | 159 ms / 426,779 | 13.8 ms / 18,898 |
| Admin | 200 | 186 ms / 496,228 | 26.5 ms / 104,690 |
| TeamAllSpots (2,004) | 0 | 47.7 ms / 90,066 | 6.6 ms / 8,992 |
| TeamAllSpots | 10 | 56.0 ms / 93,243 | 11.6 ms / 17,924 |
| TeamAllSpots | 200 | 57.2 ms / 141,210 | 41.3 ms / 81,618 |
| GrantOnly (45) | 0 | 15.2 ms / 11,877 | 4.1 ms / 8,233 |
| GrantOnly | 10 | 18.1 ms / 14,742 | 9.6 ms / 16,715 |
| GrantOnly | 200 | 45.2 ms / 56,717 | 19.0 ms / 74,892 |

Rows read from SQL per resume: before, every row the member sees (10,500 /
2,004 / 45) plus the log since the cursor; after, only the log since the
cursor (0 / 10 / 200 task rows plus dependency-table rows). No `needHashes`
on either side. With changes, the remaining cost is parsing the log's full
row images (52 columns each, as `Value`).

Before the merge (delivery v3 rows as `Value`) the same bench gave, for Admin
at 0 changes, 288 ms / 1.35M allocations before and 10 ms / 8,936 after
(`cache5.jsonl`).

The 9k `replica_state` bench (TeamAllSpots, 0 / 10 / 200 changes) moves less:
128 / 185 / 195 ms (cache off, merged base) to 88 / 143 / 162 ms (cache on),
with ~2.1–3.2M allocations on both sides. Its timed block also builds the
client model's request, and `ClientModel::open_request` → `window.digest()`
re-hashes every row the model holds; that client work dominates the
measurement. The el-rey bench above times only the server open.

### Cold open (no client copy), and cache costs

| member | fresh SQL (delivery v4) | cache miss (build + view) | cache hit | hit + 1k-change delta | fresh after 1k changes |
| --- | --- | --- | --- | --- | --- |
| Admin: time | 163 ms | 199 ms | 5.0 ms | 85.5 ms | 192 ms |
| Admin: allocations | 427,180 | 773,139 | 13,643 | 298,399 | 427,164 |
| Admin: peak heap | 20.8 MB | 33.6 MB | 1.8 MB | 22.3 MB | 20.8 MB |
| TeamAllSpots: time / peak | 48 ms / 4.6 MB | 200 ms / 32.6 MB | 6.5 ms / 0.9 MB | 82 ms / 21.2 MB | 72 ms / 4.7 MB |
| GrantOnly: time / peak | 15.5 ms / 0.4 MB | 199 ms / 31.9 MB | 3.1 ms / 0.3 MB | 66.5 ms / 21.0 MB | 29 ms / 0.4 MB |

- A miss reads the whole table once for everybody (and decodes the ACL
  columns for the compiled program), so it costs a narrow member more than
  its own predicate-filtered scan. It happens once per collection per tenant
  (single-flight), not per open.
- The "+1k changes" open replays 1,000 log rows (one revision, parsed as
  `Value`) and rolls the entry forward. The opens after it are plain hits.
- Profiler stages during the resume runs: the read transaction and
  principal resolution take ~1.9 ms per open (`snapshot.visibility` p50
  1.79 ms); the in-memory filter of 10.5k cached rows over the decoded ACL
  columns takes ~3 ms.

### Reconnect storm: 1,500 opens across 48 visibility classes in 10 s

150 connections (78 members, cycled) start uniformly over 10 s. Each opens all
10 collections through the socket path (`enqueue_replica_open` on the offload
pool, install, page encoding). The writer adds 4 task updates/s. One CPU
(`taskset`). Merged base.

| | cold: before | cold: after | resume (+200 task writes since cursor): before | resume: after |
| --- | --- | --- | --- | --- |
| time to complete (arrivals end at 10 s) | 15.5 s | 10.0 s | 17.5 s | 10.3 s |
| process CPU | 15.2 s | 8.0 s | 17.2 s | 9.5 s |
| open latency p50 / p99 | 2.9 s / 5.5 s | 7 ms / 251 ms | 3.7 s / 7.3 s | 54 ms / 376 ms |
| peak RSS (over start) | 400 MB (+36 MB) | 367 MB (+115 MB) | 396 MB (+135 MB) | 422 MB (+116 MB) |
| network-thread polls > 5 ms (longest poll) | 0 (2 ms) | 0 (3 ms) | 0 (2 ms) | 0 (1 ms) |
| SQL full-collection reads | 1,500 per-member scans | 10 entry builds | 1,500 per-member scans | 10 entry builds |
| cache hits / roll-forwards / fallbacks | — | 1,490 / 1 / 1 | — | 1,490 / 1 / 3 |
| wire bytes | 631 MB | 631 MB | 12.1 MB | 11.5 MB |

- After the change, both storms keep up with arrivals. The cold storm's
  remaining CPU is mostly encoding 631 MB of snapshot pages per connection.
- The resume storm's remaining CPU is the 200-change log replay that every
  resume parses, plus principal resolution.
- The fallbacks are opens whose parent/derived-set index was still moving
  after 3 re-reads during the writes. They were served by the SQL scan.
- Peak RSS counts the cache entries (43 MB for all 10 collections) against
  the before runs' transient per-open copies.
- On the pre-merge base (rows as `Value`): cold 20.3 s → 10.8 s,
  p50 5.3 s → 0.77 s; resume 21.0 s → 13.0 s (`cache5.jsonl`).

### Cache memory, eviction

Measured live heap of one entry, including its decoded ACL columns (counting
allocator, el-rey tenant, merged base):

| collection | rows | entry heap | per row |
| --- | --- | --- | --- |
| tasks.recentWorkspaceTasks | 10,500 | 31.8 MB | 3.0 KB |
| taskResources.taskUsers | 8,809 | 8.3 MB | 0.95 KB |
| taskResources.taskTags | 6,410 | 5.7 MB | 0.88 KB |
| members.listMembers | 78 | 0.11 MB | 1.5 KB |
| the other 6 | ≤ 78 each | 0.36 MB together | |

- All 10 collections of the tenant take 43 MB (estimate). Before the merge,
  with rows held as `Value`, tasks alone took 94.8 MB (9.0 KB/row).
- The tasks row text is ~1.8 KB; the rest is the hash, key, order values and
  decoded ACL columns.
- The estimate the budget uses (`row_heap_bytes`) is within about ±15 % of the
  measurement (27.1 MB estimated vs 31.8 MB measured for tasks).
- The entry is per collection, not per class: 48 classes share it, so the
  per-class cost is 0.
- With the default 256 MiB budget, entries are evicted least recently used.
  An entry larger than the budget is used by the open that built it and is
  not kept. Unit test: `snapshot_cache::tests::evicts_least_recently_used_entries_beyond_the_budget`.
- At 2 GB for 5,000 users the budget has to be set together with the
  per-connection budgets: the largest tenant needs ~45 MB and seven tenants
  roughly their size share. A smaller budget means more SQL builds. It never
  changes results.

### Correctness

`tests/snapshot_cache_oracle.rs` (fixture `small`: 400 tasks, 12 members)
compares a cached open with a fresh SQL open at the same clock, for every
member × all 10 collections: rows (in order for ordered collections), the
snapshot metadata (aggregate digest, inline hashes), the `ready` digest, the
window row count and truncation. It runs:

- after the first open;
- after each of 13 writes: description edit, task moved to another workspace,
  spot removed, insert, grant-only assignment and its removal, an edit while
  assigned, a member gaining and losing a team (principal changes), a delete,
  a soft delete, a presence beat;
- in 3 rounds racing a writer (40 commits during the opens; only opens at
  equal clocks compare);
- after the log's retention passes the entry. That open must fall back
  (fallback counter +1) and still be equal; the next open must rebuild
  (build counter +1).

Roll-forwards are forced (`GONVEX_SNAPSHOT_CACHE_ROLL_CHANGES=3`) and asserted
to happen. Normal opens must take zero fallbacks. `tests/replica_state.rs`
runs its client-model scenario through the cache (opens, resumes, principal
changes, `needHashes` reconciles) against `open_replica_fresh`.

## (b) What a reload costs on the staging web client

Read from the code (SDK in this worktree, which matches the published
`0.5.2-sqlite.0` Whagons pins; Whagons `origin/feat/sqlite-staging-deploy`).
Not run in a browser.

- **Persistence is on.** Whagons passes `localReplica.storage =
  IndexedDBLocalReplicaStorage` (Dexie) from `browserUpgradeStorage`
  (`src/providers/GonvexClientProvider.tsx:55-76`, `browser-upgrades.ts:26`).
  There is no flag to turn it off, and no Worker or cross-origin isolation is
  needed. The OPFS replica is the separate experimental entry and is not used.
  The SDK default (no storage) would be memory-only.
- **On reload** the SDK loads each window record of the scope (ids, cursor,
  `hashes`, `aggregateDigest`) and up to 2,048 rows / 4 MB of row data. It
  renders cached rows at once (`source: cache`), then sends `replica.open`
  with `{cursor, digest}`, where the digest is the stored `aggregateDigest`.
  The digest is not recomputed from the rows. The client only produces
  `xor1`, never `sum128`. Keys and hashes are sent only when the window has
  no stored hashes, or after `needHashes`.
- **Server cost of that reload, before this branch:** one full SQL snapshot
  build per collection per member (the 10,501-row `json_object` scans in the
  staging logs). Wire: only the delta.
- **After:** a log read plus an in-memory filter of the cached rows (numbers
  above); the wire is unchanged.
- **Full download** happens only on an epoch change, a cursor outside the
  30-day retention, a digest mismatch on `ready` (treated as reset), or a new
  scope. The scope hashes project, tenant, account and the permissions hash,
  so a permission change starts an empty partition. A module redeploy does
  not invalidate cursors.
- **Client cost, estimated from the code:**
  - parse ~1 MB of window JSON per 10k-row window (ids plus 64-hex hashes);
  - page through the rows table (256 rows per read) with `JSON.parse` per row;
  - after any cold snapshot over 2,048 rows, a SHA-256 pass over every row
    (the server sends inline hashes only up to 2,048).

## (c) Bounded `tasks.recentWorkspaceTasks`: options

Definition today (Whagons `gonvex/tasks/replicaQueries.ts:22-38`):

- `table: "tasks"`, `key: "_id"`, 52 columns;
- `orderBy: "id"` descending, `mode: "progressive"`;
- `maxBytes` 200 MiB, no `maxRows`.

Every member replicates every task they can see. On staging that is 10,501
rows for the largest tenant.

### Real column sizes

JSON bytes per row, as the snapshot encodes them (`"column":value`, 52
columns). Measured with `json_object` and `json_quote` over the `tasks` table.

| source | rows | row | `description` | `audienceRules` | `"col":null` pairs | always-null columns |
| --- | --- | --- | --- | --- | --- | --- |
| lab tenant `8ec9f43e…` (`gonvex-sqlite-lab/runtime`) | 9,181 | 1,335 B | 16 B (empty strings) | 20 B (null) | **799 B** | 35 of 52 |
| perf fixture `big` (synthetic 400-byte descriptions) | 9,000 | 1,787 B | 419 B | 45 B | **633 B** | 26 of 52 |

In both, the largest part of a task row is not `description` but the keys of
columns whose value is null (`"workflowTriggeredByUserId":null`, …). The lab
data is synthetic load data: its descriptions are empty. **Real staging
description and `audienceRules` sizes were not measured** (no production data
was read). They decide option 2.

### Options

Wire and client storage scale with rows × bytes per row. The server
snapshot-cache entry scales with rows × heap per row: 3.0 KB per tasks row
with delivery v4's raw text (it was 9.0 KB as `Value`). A cached open or
resume scales with the rows it filters (≈ 0.3–0.5 µs per row in memory, plus
the log rows it replays).
Admin = sees all 10,500 tasks.

| option | contract change | bytes/row (lab / fixture) | Admin snapshot (lab / fixture) | server cache entry | runtime risk |
| --- | --- | --- | --- | --- | --- |
| 0. today | — | 1,335 / 1,787 | 14.0 / 18.8 MB | 31.8 MB | resume scans fixed by this branch |
| 1. omit null-valued columns on the wire (absent = null; hashes still over the full canonical projection, the SDK re-adds nulls before hashing) | protocol + SDK, no app change | 536 / 1,154 (−60 % / −35 %) | 5.6 / 12.1 MB | unchanged (or −45 % if stored without nulls) | low: one encode/decode rule |
| 2. lazy heavy columns: drop `description`, `audienceRules` from the replica; a per-task live query loads them when a task opens | app (Whagons UI + one query) | 1,299 / 1,323 (−3 % / −26 %) | 13.6 / 13.9 MB | −3 % / −26 % | none; worth it only if real descriptions are large |
| 3. recent window: `maxRows` N (e.g. 2,000) by `id desc` plus a live query for older tasks | app | unchanged | 2,000 rows: 2.7 / 3.6 MB (−81 %) | unchanged (entry holds all rows) | **high today**: a truncated window reconciles whenever a row enters or leaves it; with `id desc` every task insert moves the boundary, so every member with a truncated window gets `needHashes` and uploads ~200 KB of keys and hashes per insert |
| 4. time window (e.g. updated in the last 90 days, or open tasks) | app + runtime | — | — | — | not expressible: collections only have equality filters and `excludeWhenSet`; a time-relative filter lets rows leave without a write, so the feed cannot decide it from row images |
| 1 + 2 | protocol + app | 500 / 690 | 5.3 / 7.2 MB | | |

### Recommendation

1. Land this branch. It removes the cost that caused the staging spikes
   (every reload re-ran the 10.5k-row query per collection) with no contract
   change.
2. Next, omit null-valued columns on the wire (option 1). It is the largest
   byte saving for every member and collection, needs no Whagons change, and
   is independent of the description question.
3. Measure staging `length(description)` and `length(audienceRules)` before
   deciding option 2. On lab data it saves 3 %.
4. Do not adopt `maxRows` for tasks until the runtime slides a truncated
   window (evict the boundary row, admit the new one, without a reconcile).
   Until then option 3 turns every task insert into a per-member reconcile.
   Bounding memory is better done in the runtime (cache entries as text, null
   omission) than by truncating what members see.
