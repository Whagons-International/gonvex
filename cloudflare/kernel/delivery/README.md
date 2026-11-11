# Replica delivery (K10)

Replica Collections end to end in the tenant object: `replica.open`, `replica.openMany` and `replica.close`;
`{epoch, revision}` cursors and resume; paged snapshots; the `xor1`, `sum128-v1` and legacy whole-set digests;
`replica.needHashes`; `replica.reset`; deltas, `replica.batch` frames, stream bindings and watermarks; eager and
progressive collections; row and byte budgets; open admission. The reference is the Rust runtime at `0bf3ad13`:
`replica.rs`, `replica_mask.rs`, `replica_cursor.rs`, `delivery.rs`, `snapshot_delivery.rs` and the socket loop in
`lib.rs` (1840-2420, 3585-3692, 4188-4300). `replica_fanout.rs`, `replica_resume.rs`, `snapshot_cache.rs` and
`feed_share.rs` are caches and are not ported as such; the shared row decisions are (`EventShare`).

```text
            tenant object (ReplicaTenantObject = K07's ReducerTenantObject + this package)
K03 ChangeWake ──▶ ReplicaDeliveryEngine ── reads the change log after its cursor, one task at a time under K07's `serial`
                    ├ subscription.ts  open: cold snapshot (pages) or resume (aggregate / whole-set / reconcile)
                    ├ feed.ts          one transaction per connection: legacy_step / mask_step, row decisions, entity changes
                    └ outlet.ts        DeliveryOutlet: one ordered channel per socket holder
                                          ├ GatewayOutlet ──RPC deliverReplica──▶ gateway object (default placement)
                                          └ LocalOutlet   ──call────────────────▶ sockets in this object
holder (any object):  Session ─ SocketDelivery (socket.ts) ─ ReplicaBatcher (batcher.ts) ─▶ WebSocket
```

| File | What | Rust |
| --- | --- | --- |
| `engine.ts` | Connections, open queues and admission, the commit feed, bootstrap and per-commit watermarks, `change-feed-reset` / `change-feed-lagged`, membership changes, the call floor for K07 | `lib.rs` socket loop |
| `subscription.ts` | `replica.open`: resolution, cursor rules, the three digest requests, snapshot pages, the two-pass scan of a large window | `open_replica_with`, `SnapshotPages`, `cursor_resumable` |
| `feed.ts` | `apply_feed_event_with`: `legacy_step`, `mask_step`, `affected_rows`, `decide_row_with`, `row_step`, `normalize_entity_changes`; IB5 | `replica.rs` 1312-2300, `replica_mask.rs` |
| `scan.ts` | `scan_window`'s statement and `WindowCollector`, read in keyset pages of 256 rows | `scan_window`, `WindowCollector` |
| `window.ts`, `rules.ts`, `canonical.ts`, `definition.ts` | Filters, projection, keys, `sql_order`, boundaries; relevance; `stable_json`, row hashes, `xor1` and `sum128-v1`, `hashes_digest`; the manifest's definitions | `replica.rs` helpers, `replica_digest.rs`, `modules.rs` |
| `frames.ts` | Every replica frame in serde's byte order, the compact transaction rule | `protocol`, `replica_wire.rs` |
| `log.ts` | Committed transactions with provenance, `collapse` | `change_feed.rs` `CHANGE_COLUMNS`, `raw_log.rs` |
| `protocol.ts` | The delivery payload between the tenant and a socket holder (versioned, additive) | |
| `socket.ts`, `batcher.ts` | The holder side: ops in order, `replica.batch` with stream bindings and the 50 ms / 1000 ms / urgent timing, monotonic watermarks, instance resets | `delivery.rs` `ReplicaBatcher` |
| `outlet.ts` | `GatewayOutlet` and `LocalOutlet`: ordered, one payload in flight per holder | |
| `object.ts` | `ReplicaTenantObject`: wiring into K07's tenant object | |
| `harness/` | The conformance Worker (`cloudflare-delivery` backend) and `platform.mjs` (deploy to `gonvex-cf-dev-*`, delete) | |
| `bench/platform.mjs` | G1 latency and a large window on a deployed harness | |

## Behavior

- **Opens.** Opens are queued per connection (only the last open of an id in one frame survives, re-opening an id
  cancels the earlier one) and run one at a time, round robin across connections, each under the tenant's invocation
  queue. Before an open reads, every commit up to the clock is delivered, so the subscription joins at the clock and
  later transactions skip it until they pass its cursor. Refusals keep Rust's texts: `individual replica open
  request is too large`, `too many pending replica opens` (256 per socket, 4,096 per object: Rust's limits are
  process-wide), and every `ReplicaError` of `replica.rs`.
- **Snapshots.** A socket with `replicaSnapshotPages` gets `replica.snapshotPage` frames of at most 256 KiB or 512
  rows (one oversized row alone), with `scope` and `visibilityScope`, the digest on the last page only, then
  `replica.ready`. Rows are the JSON text SQLite produced. A socket without pages gets one `replica.snapshot` with
  inline `hashes` and the whole-set `digest` up to 2,048 rows; a window that would cross the platform's outbound
  limit is refused with a clear error instead of being dropped (0018).
- **128 MB isolates.** The scan reads keyset pages of 256 rows while the tenant's invocation queue is held, so every
  page sees one state without a cursor across awaits. Up to 4 MiB of row text an open keeps its rows and pages them
  after the scan (truncation and digest must be known before page 0). A larger window is scanned twice: once for its
  digest, boundary and truncation, once to stream its pages, 2 MiB at a time, each chunk handed to the holder before
  the next is read; the second pass hashes nothing. Measured on the platform: a 144 MB window (5,000 rows × 30 KB)
  streamed through 128 MB isolates in 625 pages (15-35 s, network bound) while the object kept serving (0040 §3).
- **Resume.** `cursor_resumable` (same epoch, at or below the clock, at or above `retained_revision`). An aggregate
  digest (`xor1:`, or `sum128-v1:` when negotiated) replays the log since the cursor and verifies the client's claim;
  a whole-set digest and keys with hashes reconcile against SQL. Visibility inputs moving since the cursor, a
  truncated window that changed, or a digest that disagrees answer `replica.needHashes`.
- **Feed.** Each committed transaction is read once from `_gonvex_sync_changes` (with `_gonvex_sync_transactions`),
  pushed to K09's indexes, and applied to every connection: version 1 plans through `legacy_step`, version 2 through
  `mask_step` (principal, grant, derived, parent and `inheritsAny` routing, affected-row reads). Frames come in
  Rust's order: one `replica.transaction` (the union of every collection's projection, `oldValue` of held rows), then
  a delta and a ready per changed collection, `replica.needHashes` for each collection that must reconcile (removed
  until the client reopens). Row decisions are shared per visibility class for one event (`EventShare`, Rust's
  `feed_share`); a delta's body is encoded once and only its `digest`/`truncated` tail is per socket.
- **Watermarks.** Every socket of the tenant gets each commit's watermark (a plain `replica.watermark`, or the batch
  tail), after that commit's frames. A bootstrap watermark at the lowest cursor follows a cohort of opens once its
  snapshots are out. K07's call watermark: immediate when the socket has no subscription; otherwise it comes with the
  feed, and the holder's batch carrying it is sent at once. Holders never send a watermark below the last one.
- **Batches.** `replica.batch` envelopes are built where the socket lives: transaction items, delta bodies plus tails,
  ready items, `replica.stream` bindings (`~<16 hex>`, `.n` for a second subscription of the same collection on one
  socket; bindings are forgotten on every `auth`), the watermark in the tail. Any other frame flushes the open batch
  first. Presence rides in it (leading edge).
- **Resets.** `change-feed-reset` when the log no longer covers what follows the delivery's cursor (pruned, or a
  database at an earlier revision); `change-feed-lagged` when a holder falls 256 payloads behind (its queue is
  discarded and its sockets resync). A new database epoch with the log intact is delivered as Rust delivers it: the
  transaction carries the new epoch, collection cursors keep theirs, and resumes from the old epoch reload. A socket
  whose collections span two epochs is closed (1011), as Rust ends that socket's task.
- **Tenant restarts.** Subscriptions live in the tenant object's memory. A holder tracks its sockets' collections and
  the tenant instance serving them; when a delivery comes from a new instance, it sends `replica.reset
  change-feed-reset` for each collection the old one served, and the client resumes by cursor and digest.
- **Membership.** A commit that changes a session's own Member in `account_id`, `status`, `role`, `permissions` or
  `membership_revision` (after its admission) drops that connection's subscriptions; the holder sends
  `auth.error membership-changed` and the watermark (lib.rs `membership_affects`).
- **IB5** (0032): an orphan child is re-decided when its parent appears or disappears, so it reaches whoever can now
  see it (Rust skips the children of a parent its transaction created).

## Differences from Rust

See contract-changes 0040. In short: the scan holds the tenant's invocation queue while it pages (writers wait for
it, never longer than the read itself); a window over 4 MiB is read twice; watermarks are monotonic on every socket
(Rust can send a socket without subscriptions a call's watermark before the previous commit's); a restarted tenant
object resets the collections it served; IB5. Caches (`snapshot_cache`, `resume_validation`, delivery v4 shapes,
targeted wakeups) are not ported: they change cost, not frames.

## Tests, conformance, numbers

```sh
pnpm vitest run kernel/delivery                     # in cloudflare/: workerd
cd conformance && node src/cli.mjs --backend rust=rust --backend cf=cloudflare-delivery \
  --scenario thin-sdk-parity --scenario reducer-second-client --scenario replica-resume --scenario visibility-masks \
  --scenario replica-windows --scenario replica-reset --scenario replica-orphan-reveal --scenario replica-tenant-restart \
  --scenario replica-reconnect-wave
```

- `test/delivery.test.ts`: pages (sizes, sequences, digest on the last page), the two-pass scan of a 9,000-row window
  (statements = 2 × ⌈rows / 256⌉), truncation and boundary moves, shared texts and per-socket tails, the call floor,
  subscription epochs, `sum128-v1` against the protocol crate's vector, the batcher's timing and bindings, instance
  resets, presence in batches.
- Conformance: every replica scenario passes Rust against the kernel locally and against the kernel deployed on the
  platform, with each backend's digests and watermarks self-checked (0024 §2). `reducer-second-client` is G1: a
  writer's Reducer commit reaches a second client's collection through the real `@gonvex/client` SDK (and the thin
  client).
- Platform (0040 §3, `harness/platform.mjs`, `bench/platform.mjs`): every replica scenario passes Rust against the
  deployed kernel; G1 reader latency p50 74 ms, p95 97 ms from this machine (call → the reader's delta).
- Cost per commit: two statements (the clock, one page of change rows joined with their transaction); rows read ≈ 1 +
  2 × change rows; nothing written. A cold open: the clock, the principal (one statement with a warm principal cache),
  ⌈rows / 256⌉ scan statements; rows read ≈ the window. No delivery state is written to SQLite.
