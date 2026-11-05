# Replica subscription state without a per-client mirror (workstream 5)

Branch `perf/replica-state`. Code: `rust/crates/runtime/src/replica.rs`,
`replica_mask.rs`, one hook in `visibility.rs` (parent before-image overlay,
shared plans), REAL encoding in `store/src/rows.rs`. Bench:
`rust/crates/runtime/benches/replica_state.rs`. SDK client model and SQL
oracle: `rust/crates/runtime/tests/support/replica_client.rs`,
`tests/replica_state.rs`.

## What a subscription keeps

Before: every `ReplicaSubscription` held `rows: BTreeMap<key, Arc<interned
row>>` and `hashes: BTreeMap<key, hash>` for every row its client held, plus
its own clone of the collection definition and visibility plan (~2.5 MB per
Whagons member at 9,000 tasks).

Now it keeps:

| field | size |
| --- | --- |
| id, path, args, visibility scope, cursor | strings of the request |
| `definition: Arc<ReplicaCollectionDefinition>` | shared per (project, module generation, path) |
| `visibility: Arc<VisibilityPlan>` | shared per module plan (also by inherited parents) |
| `digest: ReplicaAggregate` | 32 bytes (`xor1`) or a boxed `sum128-v1` state (4.1 KB, native clients) |
| `window: ReplicaWindow` | rows, canonical bytes, truncated, boundary `(order value, key)` |
| `resolved: ResolvedVisibility` | the member's principal (token sets). Not shared yet: workstream 7 classes |

The row interner is gone. Cold snapshots stream rows from SQL into the
snapshot message and keep nothing afterwards.

## Feed deltas

`apply_feed_event` collapses the event per row (first before-image, last
after-image) and calls `decide_row` for each source row of each subscription:

- `visible(before)` uses the principal *before* this transaction's grants and
  the parents' before-images (`visibility::with_parent_overlay`: a thread-local
  overlay consulted by `InheritedParent::row`, so the shared, already-advanced
  parent index answers with the old parent rows);
- `visible(after)` uses the current principal and index;
- both / neither / only one decide update / nothing / insert or delete; the
  aggregate removes `hash(project(before))` and adds `hash(project(after))`.

A delete for a row the client never held is never published (its before-image
was not visible), so no keys of invisible rows leak. `decide_row` is a pure
function of (definition, args, plan, principal before/after, window, images):
every subscription of the same collection and visibility class gets the same
`RowDecision`, so a delivery layer (perf/delivery) can decide once per class
and apply the result per subscriber (`row_step` is that apply step).

Rows whose visibility moved without the row changing (a grant added, a derived
set moved, a parent became visible or hidden) are read by one targeted query
on the source table *without* a visibility predicate (rows whose FK/set column
is one of the moved tokens), at the event's revision (rows written later are
rewound with the first later before-image from `_gonvex_sync_changes`), and
decided before/after in memory like source rows. The read does not depend on
the principal, so it is shared by every connection the event touches the same
way (one read per event, not per connection).

Normalized entity changes (`replica.transaction`) union the projections of
every collection of the connection that holds the row after the event, decided
from the after-image; an entity delete is sent only when no collection of the
connection holds it (the SDK removes a deleted entity from every window of
that entity).

## Unchanged subscriptions send nothing

A subscription whose client rows did not change in a revision (most members
cannot see a given task) sends no frame for it: no `replica.ready`, no delta.
Its cursor stays where the client last saw a change, or advances with the
connection's `replica.watermark` (sent once per revision, and once per batch
in perf/delivery's `replica.batch`), which the SDK applies to settled windows.
Clients without the watermark capability keep the older cursor; a resume from
it replays the change log since that cursor and verifies the client's digest
against SQL, so an older cursor is always safe (`tests/replica_state.rs`:
"an unaffected subscription receives nothing, then resumes"). A changed
subscription still sends its delta followed by `replica.ready`.

One task update to the 78 fixture members (`apply_feed/task_update`):

| | before | after |
| --- | --- | --- |
| messages | 284 (209 of them readies to members whose rows did not change) | 75 (25 receivers × transaction, delta, ready) |
| connections receiving frames | 78 | 25 |
| JSON bytes (uncompressed) | 211,321 (2,709 per member) | 153,475 (1,967 per member) |
| projected to 1,950 members | 7,100 messages, 5.28 MB | 1,875 messages, 3.84 MB |

The remaining bytes are the task row itself (encoded per receiver here; once
per visibility class in perf/delivery).

## What cannot be decided from images: reconcile with the client's hashes

The runtime sends `replica.needHashes` for that collection and drops the
server subscription; the SDK reopens it with its cursor, keys and row hashes
(`fullIntegrity`), and the runtime streams the collection from SQL and sends
only rows whose hash differs plus keys SQL no longer returns. Cases:

- this member's principal changed (permission rows naming the member, grant
  removal, ancestor grants, `inheritsAny` associations);
- a shared index is not complete through, or is already ahead of, the event
  (as before, where it re-snapshotted);
- a truncated window would move (a row enters or leaves the window, or the
  boundary row moves) or a complete window crosses `maxRows`/`maxBytes`;
- version-1 plans whose visibility inputs changed.

### Principal change: why reconcile instead of a server-side before/after SQL diff

A server-side diff needs the set the client holds. Re-running the collection
query with the *old* principal on *current* data is not that set when the same
transaction (or one since the client's cursor) also changed a parent or row the
old principal depended on: a row visible to old-principal+old-parent, hidden to
old-principal+new-parent and to the new principal would never be deleted (a
leak). The client's own keys and hashes are exact, and the SDK already
implements the reopen (the same path it uses when a resume digest disagrees).
It does not drop the window, so rows stay rendered while it reconciles.

Measured (TeamSpots member, team gained/lost, 10 collections, 4 of which
reconcile): 214 ms per toggle including the client's reopen vs 152-167 ms for
the old server-only rebuild; client→server 362 KB (row hashes of the 4
collections), server→client 38 KB vs 808 KB before. Principal changes are
rare (admin writes); the cost is per affected member.

## Bounded and ordered collections

A complete bounded window (all Whagons collections at fixture sizes) is
maintained with row/byte counters (`maxBytes` now counts canonical JSON bytes,
the bytes that are hashed). Crossing a budget reconciles. A truncated ordered
window keeps its boundary `(order value, key)`: rows after it are outside the
window; changes outside are ignored, updates in place are applied, and
anything that moves the window's edge reconciles. Snapshot SQL now orders by
`orderBy, key` so the boundary is exact (SQLite ordering of NULL < numbers <
text is mirrored by `sql_order`).

## Resume

- Cursor outside retention, other epoch, or no cursor: snapshot (unchanged).
- `xor1`/`sum128` digest only: changes since the cursor from
  `_gonvex_sync_changes` (source and dependency tables); visibility inputs
  changed since the cursor → `needHashes`. Otherwise the current aggregate is
  computed by streaming the collection from SQL (nothing retained), the
  client's digest is verified against it minus the logged changes, and the
  delta is the logged rows' net changes. Disagreement → `needHashes`.
- keys/hashes (`fullIntegrity`): streamed comparison (upserts of differing
  rows, deletes of missing keys).

## Wire and SDK contract

No new message types or fields. Behaviour changes:

1. `replica.needHashes` can now arrive for an *open* subscription after its
   `replica.ready` (not only in reply to `replica.open`). `@gonvex/client`
   handles it (it sets `forceFullIntegrity` and reopens; covered by
   `sdk-lifecycle.test.ts` "reconciles a live window on an unsolicited
   needHashes"). The server delivers nothing for that collection until the
   reopen.
2. `replica.delta`/`replica.ready` digests are always aggregates: `xor1:` unless
   `sum128-v1` was negotiated. A client that opened without a digest used to
   get the whole-set SHA-256 digest; every SDK since #112 picks the
   verification from the digest's prefix. The snapshot's inline `digest` for
   small collections is unchanged (whole-set over the inline hashes).
3. Snapshot rows of a tie in `orderBy` are ordered by key.
4. REAL values are encoded with 17 significant digits (exact double) in every
   `json_object` row encoding (`store::rows::json_value_expr`); SQLite's
   default 15 digits disagreed with row images, which carry the exact double,
   so a hash of a snapshot row and of the same row's feed image differed.
5. `maxBytes` budgets count canonical JSON bytes (previously serde's encoding;
   the two differ only for U+2028/U+2029 and number formatting).

## Benchmarks

`cargo bench -p gonvex-runtime --features perf-fixture --bench replica_state`
(BENCH_FILTER, BENCH_ITERS). Fixture: 9,000-task tenant, 78 members, 10
collections per member. Before = `claude/sqlite-storage-lab` 72e6d30 + fixture
(own target dir, same bench file). One tokio worker. The machine is shared
with other agents: timings varied up to ±50 % between runs; the table uses
the quietest run of each side, memory and allocation counts are stable.

| benchmark | before | after |
| --- | --- | --- |
| retained, 78 members × 10 collections (incl. tenant-shared indexes) | 196.1 MB (2.51 MB/member, 251 KB/subscription) | 33.3 MB (426 KB/member, 42.6 KB/subscription) |
| retained, one more member once indexes are warm (Admin / TeamAllSpots / TeamSpots / GrantOnly) | 2.52 MB / 1.31 MB / 687 KB / 517 KB | 128 KB / 131 KB / 123 KB / 83 KB |
| projection to a 1,950-member tenant (marginal × 1,950, Admin) | 4.92 GB | 249 MB |
| snapshot open, 9,000 tasks (Admin): time / peak heap | 158 ms / 73.6 MB | 141 ms / 73.3 MB |
| snapshot open, 3,411 tasks (TeamAllSpots) | 70 ms / 27.9 MB | 55 ms / 27.8 MB |
| feed apply, one task update to 78 members × 10 collections | 124.5 ms (1.60 ms/subscriber), 82 k allocs, 284 messages / 211 KB | 6.1 ms (78 µs/subscriber), 75 k allocs, 75 messages / 153 KB |
| feed apply, one presence beat | 6.6 ms | 3.2 ms |
| resume TeamAllSpots tasks, 0 changes: time / peak heap | 159 ms / 29.0 MB | 167 ms / 0.79 MB |
| resume, 10 changes | 222 ms / 29.2 MB | 185 ms / 0.81 MB |
| resume, 200 changes | 489 ms / 32.9 MB, needHashes + 495 KB upload | 249 ms / 4.0 MB, no needHashes |
| principal change (team toggle, 10 collections) | 167 ms server, 808 KB down | 214 ms incl. client reopen, 362 KB up / 38 KB down |

Per-collection marginal bytes after (Admin): taskUsers 46 KB, taskTags 44 KB,
tasks 17 KB, workspaces 6 KB, others ≤ 2.3 KB. What remains is the member's
resolved principal (`ResolvedVisibility`, including each inherited parent's
resolution: taskUsers and taskTags each re-resolve the tasks principal). That
is workstream 7's shared principal classes.

## Not done here

- Snapshot rows are still decoded into `serde_json::Value` for the snapshot
  message (peak heap of a 9,000-row open unchanged at ~73 MB). Next step: keep
  SQLite's JSON text as `Box<RawValue>` rows (parse only to hash) once the
  delivery pipeline (workstream 1) settles the row payload type.
- Live queries (section 3): `LiveQuerySubscription` still keeps `last_result`.
  The e2e workload opens no live queries.
- Collections whose shared index is ahead of the event reconcile (as it re-snapshotted before);
  under sustained writes with inherited-parent collections this is the main
  remaining source of reconcile traffic and should be measured with the load
  driver.
