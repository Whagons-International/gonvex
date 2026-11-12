# 0046: K10 replica delivery: adaptations, findings and proposals

Date: 2026-11-11. Status: **proposed** (K10). Affects: K05 (session and gateway files edited additively), K07
(tenant object hook), A01 (capture SQL), K08, K14, P02 (harness), R02, R05, X01, V01.
Code: `cloudflare/kernel/delivery/` (README has the details).

Replica Collections are implemented to `replica.rs`, `replica_mask.rs`, `delivery.rs`, `snapshot_delivery.rs` and the
socket loop at `0bf3ad13`. Every replica scenario passes Rust against the kernel, locally and against the kernel
deployed on the platform (`gonvex-cf-dev-k10-*`, deleted afterwards), with each backend's digests and watermarks
self-checked. G1 holds: `reducer-second-client` runs a writer's Reducer through the real `@gonvex/client` SDK and a
second SDK client (and a thin client) receives it through its collection.

## 1. Adaptations (no frame a client can observe changes)

| # | Rust | Kernel | Why |
|---|---|---|---|
| 1 | A snapshot reads one statement in a repeatable-read transaction | Keyset pages of 256 rows, all read while the tenant's invocation queue (K07 `serial`) is held | One connection and no snapshot across awaits in a Durable Object. Writers wait for the read itself, never for the network: pages go to the holder chunk by chunk and the next chunk is read when the holder accepted the last one. |
| 2 | `SnapshotPages` pages a materialized row list | Up to 4 MiB of row text an open keeps its rows; a larger window is scanned twice (digest, boundary and truncation first, then the pages) | Every page carries `truncated` and the last one the digest, so both must be known before page 0, and 128 MB cannot hold a large window. The second pass reads the same state (1) and hashes nothing. |
| 3 | Subscriptions live in the socket task | In the tenant object, per connection; the socket's holder (a gateway) builds `replica.batch` envelopes, bindings and the batch timing | Gateway placement (0016 G0-1). One interface, two placements: `DeliveryOutlet` (`GatewayOutlet` over RPC, `LocalOutlet` for sockets in the tenant object). |
| 4 | Feed frames and call replies leave one socket loop in order | One payload at a time per holder, applied at once on arrival (not queued behind frames the socket is still handling); K07's immediate call watermark only when the socket has no subscription, or the holder already accepted the commit's payload | Workers RPC orders nothing across separate calls; a reply must never overtake the commit frames it certifies. |
| 5 | Delivery v4 shapes, `feed_share`, `snapshot_cache`, `resume_validation`, targeted wakeups | Row decisions shared per visibility class per event; delta bodies and transaction items encoded once per payload | Caches change cost, not frames. |
| 6 | `DefaultHasher(path, args)` stream tokens | FNV-1a 64 of path and arguments | The token is opaque to the SDK; equality is what makes Rust's "phantom" bindings after a re-auth (0015 S0-10), and the kernel keeps it. |
| 7 | `too many pending replica opens` against process-wide admission | 256 waiting opens per socket, 4,096 per object | DO-local bounds (CONTRACT C.5 "replace with DO-local bounds"). |

## 2. Findings for the lead

1. **Rust's watermarks are not monotonic on a socket without subscriptions** (0024 §2). A writer's call reply carries
   the immediate watermark R, and the feed's watermark of its previous commit R−1 arrives after it
   (`replica-resume`, `replica-windows`, `replica-reconnect-wave`, all on Rust). The self-check is therefore scoped as
   the diff already scopes watermarks: to sockets that subscribed to something. On those, both backends pass all three
   rules. The kernel's holders never send a watermark below the last one, on any socket. **Proposal:** accept; the
   kernel is stricter than Rust here.
2. **Rust ends the task of a socket whose collections span two database epochs** (`lib.rs` 1859-1866: a client that
   reopens one collection in a new epoch under a new id while an old-epoch one is open). The kernel closes that socket
   with 1011 (A.1.9 convention for Rust's silent ends).
3. **A new epoch with the log intact does not reset collections in Rust**: the next transaction carries the new epoch,
   deltas keep the subscription's cursor epoch, and resumes from the old epoch reload (probed on Rust; scenario
   `replica-reset`). The kernel does the same. It sends `change-feed-reset` only when the log cannot cover what
   follows its cursor (pruned, or a database at an earlier revision), where Rust's feed resets.
4. **The tenant object can restart under open gateway sockets** (eviction, crash, a redeploy of the object), and its
   subscriptions are in memory (0033 §4 for live queries). Holders track each socket's collections and the tenant
   instance serving them; a payload from a new instance makes the holder send `replica.reset` with
   `change-feed-reset` for each one, and the client resumes by cursor and digest (`replica-tenant-restart`, kernel
   only). Rust has no such restart. **Proposal:** accept. K08's live queries need the same treatment when K08 is
   wired into delivery (not done here; see §4).
5. **Intended behavior IB5 is implemented** (0032): a parent that appears or disappears re-decides its children, so an
   orphan note reaches its task's creator and leaves its author (`replica-orphan-reveal`, kernel only).
6. **Pre-existing on the integration base: no tenant could be created on any kernel backend.** K08's `tasks` fixture
   table (55 columns) made A01's capture trigger a left-deep chain of 110 `OR`s, which A01's own expression-depth
   check refuses ("expression depth about 111"). Fixed in `schema-transform/capture.ts`: the `OR` and `||` runs are
   balanced trees of at most 8 (associative, same value). Whagons tables wider than ~50 columns hit the same wall.
7. **K05 left the WebSocket close handshake unanswered.** With the hibernation API the object must answer a client's
   close frame; until it did, a client that closed its socket waited for the TCP connection to time out before it
   could reconnect (`replica-resume` stalled). `SocketHost.closed` now echoes the code (1000 for 1005/1006/1015).
8. **The `replica.batch` envelope's `from`** is the last watermark sent in a batch (A.4 row 1, `delivery.rs`
   `last_watermark`, 0 before the first). K05 used the admission revision; K14's test asserted that and is corrected.
9. **Harness normalization (P02).** The SDK groups the integrity reopens it sends after `replica.needHashes` into one
   `replica.openMany` or several depending on whether the `needHashes` frames arrived in one tick: Rust and the kernel
   produced the same episodes grouped differently (`visibility-masks`). The normalizer now expands outbound
   `replica.openMany` into one frame per open, as it expands every other batch frame, and its no-op rule handles a
   batched reopen. Each rule has a unit test.

## 3. Platform measurements (deployed harness, this machine as the client)

| Measure | Result |
|---|---|
| G1, 40 commits: writer call → `reducer.result` | p50 68.7 ms, p95 83.6 ms |
| G1, 40 commits: writer call → reader's `replica.delta` | p50 74.1 ms, p95 96.9 ms |
| Cold open of a 48 MB window (2,500 rows × 20 KB) | 193 pages, 19.9 s (network bound); the object kept serving (next call 98 ms) |
| Cold open of a 144 MB window (5,000 rows × 30 KB), larger than the 128 MB isolate | 625 pages in 15.5 s and 35.4 s (two runs, network bound); 41 statements and 20,119 rows read (two passes); the object kept serving (next call 111-182 ms); `wrangler tail` showed no exception and no exceeded limit |
| The same, first attempt | stalled: no frame for 10 minutes. It ran before the tail was attached and before the second pass stopped hashing, so the cause is not known. **Follow-up:** repeat under tail in V01 or R05. |
| Reconnect wave (`replica-reconnect-wave`, six SDK clients drop at once with commits in the outage) | passes on the platform |

## 4. Obligations from the decision log

| Obligation | Where |
|---|---|
| 0032 IB5: deliver the row a parent's appearance reveals; kernel-only scenario | `feed.ts` `mask_step` (parents that appear or disappear re-decide their children); `replica-orphan-reveal` (Rust skips it, so it is outside the Rust-vs-kernel comparison) |
| 0031 §2 / 0032: release class bindings on unsubscribe and rebind | `HandleCache.clear()` on close, reopen of an id, needHashes, reset, drop, and when a transaction changes the principal |
| 0024 §2: per-backend watermark self-check | `conformance/src/watermark-check.mjs`, scoped to subscribed sockets (§2.1) |
| 0030 #4: `subscriptionFloor(socket)` for the Reducer path | `ReducerTenantObject.subscriptionFloor` (hook) → `ReplicaDeliveryEngine.subscriptionFloor` |
| 0033 #3: per-socket frames ordered before replica frames | The delivery payload is per socket and ordered (`SocketWork.ops`); live query frames go there first once K08 is wired (§5) |
| 0033 #4: resubscribe after a tenant-object restart | Replica collections: the holder resets them on a new instance (§2.4). Live queries: with K08's wiring (§5) |
| 0036 §8: `operatorDataChanged` after commits | `ReplicaTenantObject.operatorSink()`: one call per 100 ms of delivered commits once the Worker version binds the operator object (K19 follow-up of 0037) |
| 0022 §4.4 / 0018: payloads under 32 MiB, no frame ≥ 32 MiB − 1 KiB | `splitPayload` (24 MiB of text per payload, sockets and ops split in order); snapshot pages ≤ 256 KiB; a non-paged snapshot over the limit is refused with an error; the holder closes 1011 rather than send a larger frame (K05) |
| K14 follow-up: presence rides in an open `replica.batch` | `ReplicaBatcher.presence` (leading edge, `delivery.rs` `push_item`) |

## 5. Not in this package

- **K08 live queries are not wired into delivery.** No tenant object imports K08's engine yet; its `live.applyFeed`
  hook, `bySocket` frames and the live-query part of the call floor and bootstrap watermark are the next integration
  step (K08 README "Integration").
- Native rows (`replica.snapshotAck`, binary pages) stay out of scope (A.0).
- `operatorDataChanged` (0037 K19 §8): the engine has a `delivered(revision)` hook; the K19 wiring belongs with the
  kernel's tenant class (0037 follow-up for K19).
- Outbound backpressure on the gateway: workerd's `WebSocket.send` has none (A.1.8), so a slow client's snapshot is
  buffered by its gateway; the tenant is protected by the per-holder lag bound only.

## 6. Review fixes (ruled in 0048 §1)

| Request | Change |
|---|---|
| `connectionReset` reached tenant objects without it | No-op `connectionReset` on K07's `ReducerTenantObject` and K05's `TenantStubObject` (`ReplicaTenantObject` overrides it); `TenantRpc.connectionReset` is optional. `pnpm test` ends with no unhandled errors. |
| Rebase onto K17 | K17's `schema-transform/capture.ts` kept; every `package.json` suite kept; `receiptCrash` stays last under its original comment, after `replicaReconnectWave` and `servicePrincipals` |
| No SQL per subscriber (design rule) | `affected_rows` reads go through one `TransactionReads` per transaction: the clock once, whole rows once per (table, column, values), later changes once per table, shared by every subscription of every connection. A parent flip that re-decides 50 children for every subscriber (`delivery.test.ts`): **4 statements / 108 rows read per commit for 1, 10 and 100 subscribers** (before: 4 / 108, 22 / 585, 202 / 5,355). |
| `rust.mjs` | `resetEpoch` moved out from between `readChangeLog`'s doc comment and the method |
| §1a one commit channel | `ReplicaTenantObject` sends K07's own-membership hand-off as a `ReplicaDelivery` on the ordered holder channel, so it cannot overtake a commit's frames; the tenant object with delivery no longer uses `DeliveryBatch` for commits. K08's live-query frames join the per-socket list when K22 wires K08 (§1c). |

