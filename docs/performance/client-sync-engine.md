# Off-main-thread SQLite client sync engine

> Historical: written while Gonvex stored data in PostgreSQL. Storage moved to
> SQLite on 2026-11-02; see [SQLite storage](../sqlite-storage.md).

Design proposal, reviewed against `95da6c930211e47b798fd66bc4b65ca42d0a5cd7` on `staging`, SDK `0.5.2-staging.28`. This PR changes documentation only. Targets below are acceptance gates, not measured results.

## Decision

Use one leader-elected dedicated worker per origin, runtime endpoint, project, account and tenant, within the browser's storage partition. The worker owns transport, decoding, integrity checks, persistence, subscriptions, portable reducer execution and outbox scheduling. Other tabs call it through addressed BroadcastChannel RPC. Use transferred MessagePorts where tabs can establish a direct window handshake, but do not require an opener relationship. BroadcastChannel cannot transfer a MessagePort.

Use official `@sqlite.org/sqlite-wasm` with the `opfs-sahpool` VFS in that worker. Only the elected owner opens the database. UI threads retain mounted subscription results and a bounded row cache. Native uses the same logical schema and query compiler through `@gonvex/expo-sqlite`, with a different driver and lifetime policy.

Ship the worker with IndexedDB first. Keep the current client as the default until asynchronous UI readiness, crash recovery and mobile behavior have passed the gates below. SQLite and a reduced UI cache are separate switches. Do not change Whagons' `client-contract.json` version or publish packages from this design PR.

## Evidence and corrections

Source links refer to this checkout. They identify the remaining costs rather than promising savings from moving every existing operation unchanged.

| Finding | Code checked | Qualification |
| --- | --- | --- |
| Client transport decodes JSON on its calling thread | [`index.ts`](../../packages/client/src/index.ts), `connect`, message handler around 1736 | Worker placement is not the default. Each `GonvexClient` opens a socket; the two-socket Whagons topology is application configuration, not two sockets inherently created by every SDK instance. |
| Each client owns a `LocalReplica`; browser peers catch up from disk | [`index.ts`](../../packages/client/src/index.ts), constructor and peer refresh; [`indexeddb-replica.ts`](../../packages/client/src/indexeddb-replica.ts), constructor and `readChanges` | Peers share durable data, not a resident replica. |
| Table-map copies and retained metadata scale with corpus size | [`local-replica.ts`](../../packages/client/src/local-replica.ts), `residentRows`, `trimResidentRows`, `readSnapshots`, `markEntityChanged` | Frozen rows, incremental byte accounting and indexed notifications already exist. Preserve them. `rowVersions` grows during a scope's lifetime but clears on invalidation/disposal. Eviction remains insertion-based. |
| Hydration is bounded but can still decode a full retained scope | [`indexeddb-replica.ts`](../../packages/client/src/indexeddb-replica.ts), `loadWorkingSet`; [`local-replica.ts`](../../packages/client/src/local-replica.ts), `cloneEntities` | IndexedDB already pages reads and enforces residency limits. Alphabetical startup is adapter traversal, not an intentional priority policy. |
| Membership persistence rewrites window JSON | [`indexeddb-replica.ts`](../../packages/client/src/indexeddb-replica.ts), `writeWindows`, `applyTransaction`, `applyWindowDelta` | Not every single-row transaction rewrites every window. Changed window payloads are serialized whole; deletes inspect persisted windows. Watermark-only commits already have a metadata path. |
| Scalar indexes amplify writes | [`indexeddb-read-view.ts`](../../packages/client/src/indexeddb-read-view.ts), `entityRecord`; [`expo-sqlite/index.ts`](../../packages/expo-sqlite/src/index.ts), lookup triggers | IndexedDB indexes finite numbers, booleans and strings up to 256 characters, not nulls or arbitrary scalar values. Mobile also stores authority separately. |
| Reducers have a disk read path today | [`index.ts`](../../packages/client/src/index.ts), `runLocalReducer`; [`portable.ts`](../../packages/local-runtime/src/portable.ts), `ReducerReadView`; [`read-view.ts`](../../packages/local-runtime/src/read-view.ts) | Reads are Promise-based and transaction-consistent. RAM failure can retry disk. SQLite removes residency failure, not genuine coverage failure. |
| Reducer worker support is reusable but is not the sync engine | [`worker-client.ts`](../../packages/local-runtime/src/worker-client.ts), [`worker.ts`](../../packages/local-runtime/src/worker.ts), [`native-bridge.ts`](../../packages/local-runtime/src/native-bridge.ts) | The endpoint abstraction supports web workers too; native is an existing consumer. `serveLocalReducerWorker` disables global network APIs and cannot be used unchanged in a worker that also owns sockets. |
| Protocol has monolithic replica snapshots and per-field authority | [`protocol/index.ts`](../../packages/protocol/src/index.ts), `ServerMessage`; [`replica-record.ts`](../../packages/client/src/replica-record.ts), `mergeReplicaRecord` | Live Queries already have patch messages and `windowRevision`. Preserve their separate resume identity. |
| React batches paint and indexes row subscriptions | [`paint-external-store.ts`](../../packages/react/src/paint-external-store.ts), [`react/index.tsx`](../../packages/react/src/index.tsx) | Keep animation-frame batching and unchanged row identities. Worker notifications alone do not fix full-table subscriptions. |
| Upgrade fencing adds per-operation coordination | [`browser-upgrades.ts`](../../packages/client/src/browser-upgrades.ts), `guarded`, `fenced`, `reserveId` | This protects old tabs. Remove repeated reads only for a separately fenced engine store, never from the shared legacy counter or adapter. |

The existing [F1 report](replica-residency-f1.md) measured a 50k-row RAM upsert at 5.480 ms after its improvements, but that excludes real browser storage and large-window updates. It is useful context, not this proposal's baseline. The Whagons 200 MiB setting and big-tenant script come from the worker brief; the Whagons checkout was not inspected here.

## Worker ownership and recovery

The scope key includes a normalized endpoint and project as well as account and tenant. Hash it for channel and directory names; do not include credentials. Same-origin access is not authentication. The worker validates the authenticated scope, artifact hash, protocol version and engine generation before accepting each connection. Separate storage partitions cannot share a worker or lock.

Every engine-capable tab starts a small worker candidate. The candidate requests an exclusive Web Lock named `gonvex-engine:<scope-key>` and holds it in the worker for its entire ownership lifetime. Acquire the lock before initializing the VFS or connecting sockets. Followers keep only proxy state; close unused candidates after discovery and recreate them when ownership changes. Use Web Locks for exclusivity, not a heartbeat lease. A heartbeat is only a discovery/liveness hint, and never grants permission to steal a lock.

The owner announces a random generation. Each RPC carries client ID, generation, request ID, scope and method. Subscription batches carry a monotonically increasing local commit sequence. Ignore late generations. Use timeouts to reconnect discovery, not to repeat a reducer with new entropy. Bound each client's queued messages to 2 MiB; coalesce subscription updates or replace them with a resnapshot marker on overflow. Requests and durable intents are never dropped to relieve notification pressure. Chunk large replies to at most 64 KiB. Where a direct MessagePort exists, use transferable buffers for large replies.

A new follower first registers its interest, then receives a consistent result and sequence, then accepts later deltas. Registration and snapshot capture share the engine transaction lane. This avoids missing a commit between initial read and subscription. Subscription IDs are reference-counted across tabs, so the socket retains the union of authorized collection and Live Query interests.

On graceful departure, stop admission, finish the current durable transaction, close sockets and SQLite handles, pause/release the pool handles, then release the Web Lock. Give another connected tab priority through a voluntary transfer announcement. On abrupt closure, worker destruction releases ownership; the next owner opens the database, recovers the journal, rebuilds overlays, and resumes from committed cursors. If a worker remains alive it keeps its lock, so a second writer cannot appear. Never delete or recreate the database on a handle-acquisition error.

A live WebSocket cannot move between workers. Handoff preserves the logical session using cached session identity, refresh credentials supplied by a connected UI and persisted replica/outbox state. It reconnects a physical socket. Commands sent before a crash retry with the same idempotency key. A server acknowledgement without durable local persistence is not proof that the command can be removed. Keep the intent until authoritative replay and the associated local checkpoint are durable. Target healthy foreground handoff within 2 seconds; browser suspension can delay it indefinitely. Do not promise uninterrupted background sync.

Send provider-specific token fetching back to one attached UI through RPC. Never structured-clone callback functions or circulate tokens over the discovery channel. The elected tab can deliver credentials over its private worker endpoint; follower credentials need a direct authenticated connection or a narrowly scoped RPC, not broadcasts to every listener. Same-origin hostile scripts remain outside the SDK's security boundary. Scope changes clear UI caches before accepting new data. Visibility loss invalidates subscriptions and quarantines old cached rows before rendering the new scope.

Whagons' service worker caches the versioned engine JS, wasm and reducer artifact as one manifest. It does not own SQLite handles, sockets or outbox drain. It may wake a page or advertise an update. Do not depend on Background Sync or service-worker lifetime for offline durability. Keep old manifests while open pages use them. Incompatible artifact/engine versions use legacy mode rather than joining an owner with incompatible reducer semantics.

### Browser fallback matrix

Feature probes run in a dedicated worker and perform an actual write, flush, close and reopen in a disposable probe directory. Probe API names alone are insufficient. Never probe destructively in the real pool.

| Browser/context | Recommended engine | Fallback |
| --- | --- | --- |
| Current Chrome/Edge desktop | Dedicated owner, Web Locks + BroadcastChannel, SAH-pool | Worker + IndexedDB if OPFS initialization fails |
| Chrome Android, including versions without SharedWorker | Same dedicated owner | Worker + IndexedDB; foreground recovery after OS suspension |
| Firefox desktop/Android with successful OPFS probe | Same dedicated owner | Worker + IndexedDB |
| Safari/iOS 16.4+ with successful SAH probe | Same dedicated owner; test installed PWA separately | Worker + IndexedDB; older Safari uses IndexedDB |
| Private mode, restricted iframe, embedded WebView | Select by real storage/lock probes | Legacy client if coordination is unavailable; if durable storage fails, report the existing persistence error and do not acknowledge an offline edit |
| CSP blocks workers or wasm | Legacy IndexedDB client | Existing supported behavior and explicit diagnostic |
| No Web Locks or BroadcastChannel | Legacy client with its existing coordination requirements | No multi-tab SQLite writer; do not invent unsafe timeout election |
| Expo native | One native engine per app/account/tenant with native SQLite | Existing Expo adapter and reducer bridge until native migration passes |

`createSyncAccessHandle` is dedicated-worker-only. SharedWorker availability is not the deciding constraint. MDN's current compatibility data lists Chrome Android support from 148, correcting the brief's blanket absence claim; Safari support begins at 16. Older deployed Android versions still matter. See [MDN access handles](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemFileHandle/createSyncAccessHandle) and [MDN SharedWorker compatibility data](https://github.com/mdn/browser-compat-data/blob/main/api/SharedWorker.json).

Rejected alternatives:

- SharedWorker as the sole database owner cannot directly use sync access handles. A SharedWorker coordinator plus dedicated storage child adds another lifetime and RPC boundary. Revisit only if measured handoff disruption warrants it.
- One SQLite connection per tab repeats work and contends on OPFS handles. The `opfs` VFS requires SharedArrayBuffer isolation; SAH-pool fits a single owner without that requirement. See [SQLite persistence](https://sqlite.org/wasm/doc/trunk/persistence.md).
- Service-worker ownership depends on a lifetime the application cannot control.
- Keeping whole tables in every UI thread leaves most of the memory cost intact.

## Durable storage

Use a pool directory unique to the scope key and engine storage format. Never use one default pool directory for several independently elected tenant workers. Pin the wasm package version, self-host assets, and verify that its initialization-failure behavior preserves existing files. Pool ownership is exclusive. Start with rollback journaling and `synchronous=FULL`; benchmark WAL separately, since browser WAL does not supply multi-reader concurrency. See [SQLite pool management and WAL](https://sqlite.org/wasm/doc/trunk/persistence.md#vfs-opfs-sahpool-concurrency).

The following is the logical schema, shared by web and native. SQL identifiers are fixed SDK names. Scope remains explicit in every key even when a database currently holds only one scope. Revisions use canonical decimal text at the driver boundary if they can exceed safe JS integers; ordering uses a lossless revision comparator, not lexicographic TEXT order.

```sql
CREATE TABLE replica_rows (
  scope TEXT NOT NULL, entity TEXT NOT NULL, id TEXT NOT NULL,
  value TEXT, authority_epoch TEXT NOT NULL, authority_json TEXT NOT NULL,
  deleted_revision TEXT, local_sequence INTEGER NOT NULL,
  PRIMARY KEY (scope, entity, id)
) WITHOUT ROWID;
CREATE TABLE replica_windows (
  scope TEXT NOT NULL, signature TEXT NOT NULL, entity TEXT NOT NULL,
  generation TEXT NOT NULL, metadata_json TEXT NOT NULL,
  PRIMARY KEY (scope, signature)
) WITHOUT ROWID;
CREATE TABLE replica_members (
  scope TEXT NOT NULL, signature TEXT NOT NULL, generation TEXT NOT NULL,
  entity TEXT NOT NULL, id TEXT NOT NULL, order_key TEXT NOT NULL,
  row_hash BLOB,
  PRIMARY KEY (scope, signature, generation, id)
) WITHOUT ROWID;
CREATE INDEX replica_member_row ON replica_members(scope, entity, id);
CREATE INDEX replica_member_order
  ON replica_members(scope, signature, generation, order_key, id);
CREATE TABLE replica_checkpoints (
  scope TEXT NOT NULL, stream TEXT NOT NULL, epoch TEXT NOT NULL,
  revision TEXT NOT NULL, digest BLOB, window_revision TEXT,
  PRIMARY KEY (scope, stream)
) WITHOUT ROWID;
CREATE TABLE engine_meta (
  scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY (scope, key)
) WITHOUT ROWID;
CREATE TABLE intent_outbox (
  scope TEXT NOT NULL, command_id TEXT NOT NULL, admission_sequence INTEGER NOT NULL,
  state TEXT NOT NULL, next_attempt_at INTEGER, intent_json TEXT NOT NULL,
  execution_json TEXT, patches_json TEXT, dependencies_json TEXT,
  PRIMARY KEY (scope, command_id), UNIQUE (scope, admission_sequence)
) WITHOUT ROWID;
CREATE INDEX intent_ready ON intent_outbox(scope, state, next_attempt_at);
CREATE TABLE intent_dependencies (
  scope TEXT NOT NULL, command_id TEXT NOT NULL, dependency_id TEXT NOT NULL,
  PRIMARY KEY (scope, command_id, dependency_id)
) WITHOUT ROWID;
```

`authority_json` stores field-to-revision authority beside the row, preserving `mergeReplicaRecord` behavior and avoiding mobile's second metadata-row write. Missing projected columns remain absent, never become null. Row hashes remain hashes of the collection's projected row, not the union row merged from other collections. Store their 32-byte representation locally without changing current wire hex strings. `metadata_json` contains query plan, coverage, result skeleton/scalar if applicable, completeness, freshness and visibility/artifact binding, but no membership arrays or hash maps.

Membership is one record per window and ID. One row update changes its row, affected membership hashes and checkpoint, in one transaction. Delete looks up owners by `replica_member_row` rather than parsing every window. Collection exclusion removes only that membership; delete/pruning must preserve other owners and projected field authority. A high-fanout row still costs O(owning windows), not O(collection size).

Collections without server ordering use deterministic ID order. Ordered Live Queries use stable fractional order keys between neighbors, preserving the exact server sequence and duplicate/result-skeleton semantics. Rebalance only a bounded segment when keys grow too long; a complete reorder can remain O(n). Changes to sort fields can change positions and limits. Never renumber 50k memberships for a one-row update. Full snapshots stage a new generation and atomically switch `replica_windows.generation` when complete. Readers join only the active generation. Cleanup of retired generations runs in small idle transactions.

Portable reads compile a shared `DataRead` plan into parameterized SQLite plus residual portable evaluation. Existing `compileDataRead` emits server SQL and must not be pasted into SQLite unchanged. Preserve null rules, JS numeric/string distinctions, timestamp normalization, deterministic tie ordering and `numericSuffix` behavior. Unsupported PostgreSQL expressions retain online-only behavior. SQL optimizations must pass parity fixtures against portable and IndexedDB reads. Native's async driver can implement the same Promise-based `ReducerReadView`; synchronous wasm reads do not imply synchronous native bridge calls.

### Declared indexes and coverage

`ReplicaCollectionDefinition` has `equalFilters`, `orderBy` and projected columns today, but no `indexes` declaration. Add optional `localIndexes` metadata in a separate module-sdk/codegen PR. A proposed value is `[{ columns: ["workspace_id", "status", "id"] }]`. This is a local performance hint, never an authorization rule or client-contract change. Derive initial indexes from equality filters plus declared order/key; require explicit declarations for reducer scans over other columns.

Build only declared, typed SQLite expression indexes over `value` for known entity/column definitions, with scope/entity restrictions. Use generated safe identifiers and trusted schema column names; arguments are bound values. Rebuild an affected index on artifact change in the worker. Unsupported collation/transform/type combinations use bounded SQL candidate reads and portable residual evaluation. No delete-and-reinsert lookup trigger over every scalar. Require `EXPLAIN QUERY PLAN` fixtures for representative equality/range/ordering reads on both drivers.

Keep complete slices, columns and their cursor/visibility bindings in window metadata. Having a row on disk does not prove a query is complete. Reducers still require `readIsCovered` or complete primary-key coverage. Partial, truncated or unauthorized data cannot answer an absence query. Only RAM eviction ceases to cause `IncompleteReplicaError`; genuine missing data still queues the intent without prediction or produces today's documented fallback. Do not fill missing data through unauthenticated local queries.

### Compaction

Do not delete tombstones by age alone. A delayed projected snapshot can otherwise resurrect a row. Compact only below a persisted minimum accepted replay revision for the epoch, after all active/staged windows and in-flight snapshot requests are newer, no pending intent depends on that record, and the server confirms the replay retention floor. Reject older input below the floor and require an authorized fresh snapshot. Old servers without that negotiation keep tombstones until an epoch replacement. Engine-only local notification history can compact after all connected subscribers acknowledge it; disconnected clients resnapshot rather than demand an unlimited peer log. Idle cleanup has a 5 ms scheduling budget and bounded deletes; never place a full VACUUM on the interactive lane.

### IndexedDB migration and mixed SDK versions

Backend format version is independent of the application contract. Never reinterpret the existing upgrade journal as a backend switch or bump the contract to fence old SDKs. Old clients must still connect to the runtime, and already-open old tabs must retain their durable journal.

Start SQLite as a derived read cache while `whagons-v2`, `whagons-v2-outbox` and `whagons-v2-upgrades` remain canonical. This compatibility mode is deliberate. Under existing shared/exclusive upgrade coordination, capture an IDB checkpoint and copy bounded pages of entity values, per-field authority, tombstones, window membership/hash records, cursor and cached session metadata. Record migration progress in SQLite; never advance a resume cursor beyond the imported data. Release the fence between pages, then replay IDB sequences. If a reset occurs or sequence history cannot reconstruct a consistent checkpoint, restart the affected scope import. The current IDB sequence tracks latest record states; it is not an immutable transaction log. Do not assume it can reconstruct an arbitrary historical snapshot.

Finish with a bounded exclusive fence: catch up latest entity/window records, verify epoch and sequence, commit the complete imported generation, and mark it ready. Until this fence succeeds, cached SQLite rows are not authoritative. Old clients continue using the unmodified IDB schema. Catch up peer changes under the existing locks before admitting a reducer. New engine writes first commit IDB, then update SQLite and publish; a crash between them rebuilds from canonical IDB. A SQLite failure cannot turn a committed IDB edit into a reported rejection: return its durable admission identity and recover the cache. This mode saves read work and UI memory but carries dual-write cost. Benchmark it separately.

Keep the IDB outbox canonical throughout this mode, including its shared numeric ID reservation and intent/drain locks. SQLite's outbox tables are a restartable mirror, never a second scheduler. Do not copy an intent, drain both journals, or delete the source before verifying durable destination and ownership. New reducers can persist their read/write metadata as optional entry fields, with conservative dependencies when legacy entries lack them.

SQLite-authoritative mode requires a new opt-in storage namespace and engine-aware participants. Retain a legacy recovery adapter for the old IDB namespace. Existing pending legacy intents drain through that adapter using their original scope, IDs, artifact and idempotency keys before the new namespace admits dependent edits. If legacy peers still write, stay in compatibility mode. Legacy peers do not implement a trustworthy presence handshake, so inactivity or a heartbeat timeout is insufficient proof. Whagons should initially deploy compatibility mode; pure SQLite activation is a controlled all-pages-reload rollout with explicit recovery of any later-discovered legacy intents. Until that rollout can prove exclusive legacy admission fencing without a contract bump, pure SQLite remains opt-in for isolated/fresh namespaces only.

In a pure namespace, SQLite atomically commits intent, prediction metadata and dependencies before UI acknowledgement. Rolling back to IDB uses an exclusive, resumable export and selects it only after all rows, authority, memberships, checkpoints and live intents are durable. A feature flag cannot simply abandon the only journal containing offline work. Corrupt replica data may be discarded for authorized resync; corrupt/unreadable intent data requires recovery and blocks destructive reset. Missing legacy hashes or coverage invalidates that window's resume certificate, not unrelated imported rows. Server refusal to resume triggers today's scoped resync. Never infer completeness from an import count alone.

## UI memory and notifications

The worker keeps SQLite page cache, bounded statement/query-plan caches, active query dependency indexes and a small decoded-row LRU. It does not construct a second `entityTables` corpus. Hash only changed projected rows and maintain the digest incrementally under the existing integrity algorithm. Do not change canonical serialization or digest math as part of the thread move.

Each tab has a row cache keyed by scope, entity, ID and projection/authority version. Mounted hooks increment refcounts. A batch with overlapping IDs shares the same row objects. Patch changed fields and preserve unchanged nested references after the message clone boundary; freeze published rows. Deletes and absent rows have explicit versioned entries. Unmount decrements refcounts; zero-ref entries enter true LRU eviction. Evict version and negative-entry metadata with the row. No lifetime-long `rowVersions` map remains on the UI side.

| Consumer | Engine registration | UI result |
| --- | --- | --- |
| `useEntity` | Exact row key and required projection | Loading until the first reply, then row/absence with scope-bound version |
| `useReplicaEntities` | Deduplicated key set | Restore caller order and duplicates; reuse unchanged row objects and array when nothing selected changed |
| `useReplicaCollection` | Collection signature, arguments and requested projection | Exact ordered result, completeness and freshness; delta changes affected rows/positions |
| `useLiveQuery` / state hook | Existing server query signature plus portable offline plan if supported | Authoritative server membership/order online; labelled partial result offline when coverage is partial |
| Broad imperative reads | Explicit retained query or awaited engine read | Legacy synchronous reads stay on legacy client; worker reads have an asynchronous readiness contract |

A full collection hook still requests a full collection. Keeping 50k rows mounted cannot meet a small per-tab memory target without changing what the application requests. Preserve the old hook's full-result behavior. Add an optional paged collection hook/retained query API for virtualized views; migrate Whagons' visible task list to it before claiming viewport-sized memory. Other selectors that need all rows continue to retain all rows or move aggregation into the engine. Never silently slice a collection.

Each committed transaction produces changed row keys, old/new values needed for predicate decisions, changed memberships, scope/coverage changes and origin commands. Index subscriptions by exact keys, windows and predicate dependency columns. For predicate queries, both old and new values participate, so an off-screen row can enter a result. Limit/order queries reevaluate the indexed boundary when candidates cross it. Joins and aggregates track relation dependencies; unsupported dependency analysis invalidates the affected query conservatively. Do not claim changed row keys alone can maintain arbitrary query results.

The worker recomputes only affected results and sends inserted/deleted/moved IDs, changed projected fields and state changes once per committed sequence. UI applies a whole batch before notifying, then keeps existing paint scheduling. A sequence gap triggers a subscription resnapshot. A background tab receives a bounded latest state rather than an unbounded stream. Retention of a server subscription is distinct from retention of decoded UI rows.

Budget every owned allocation: decoded rows, result arrays, memberships, hashes, versions, negative entries, optimistic patches, read/write dependency sets, query plans, snapshot staging, RPC buffers, telemetry, wasm memory and SQLite page cache. Expose per-category estimates plus actual measured worker/tab heap. Initial targets are 16 MiB idle UI cache per tab, 64 MiB worker managed memory including wasm/page cache, and 2 MiB queued RPC per tab. Mounted results are pinned and counted; when pins exceed the soft budget, report the overage and recommend pagination rather than evicting live values. Pending intents remain durable on disk and page into memory as needed. Limit speculative execution and decoded frames; do not let a 500-intent journal become 500 retained full snapshots. DOM/React/browser overhead is measured separately from SDK estimates.

## Reducers and dependency-aware outbox

Reuse `PortableReducerRuntime` with generated, statically imported reducer bindings in the engine bundle. Functions cannot cross RPC. Bootstrap the engine's private transport factories and wasm before installing global network denial. Engine socket/reconnect code uses a captured WebSocket constructor through a private closure; reducers never receive that closure. Deny global fetch, WebSocket and XMLHttpRequest before executing reducers, matching the existing reducer worker's policy. Generated artifact validation rejects direct external I/O and imports outside the approved reducer graph. Do not call `serveLocalReducerWorker` unchanged, because its denial would also break today's transport code. Prove this arrangement in phase 2 with attempted reducer network access and successful engine reconnect. If it cannot preserve existing restrictions, keep the feature disabled until a separate reducer compartment with a transactional read host is implemented.

The engine serializes reducer admission and authoritative application on one transaction lane. Execute against a stable confirmed SQLite read transaction plus the ordered pending overlay. The existing async `ReducerReadView.select` wraps synchronous wasm selects; all reducer awaits remain inside the logical lane. Do not yield the database transaction to another admission or server commit. Stage writes and publish nothing on failure. Bound reducer time; abort pathological execution and preserve today's error behavior. Long snapshot import work yields between independent pages, never halfway through a reducer commit.

Capture exact reads, projected fields and versions; also record predicate/range reads, ordering/limit boundaries and absence reads. Writes include entity/ID/field keys plus insert/delete effects. A key-only set is unsafe when a previous intent inserts a row matching a later predicate. Unknown query semantics use a table dependency, and legacy entries retain the conservative local-intent chain. Feed these sets into the concurrent dependency-aware outbox scheduler, preserving its admission order, retry state, batch wire negotiation and conflict rules. Do not introduce a second batching implementation.

RPC reducer requests have a stable command ID, timestamp, entropy and artifact hash, reused after timeout/handoff. Deduplicate durable admission by command ID and return the stored outcome on retries. In compatibility mode the IDB intent commits before any prediction appears; apply its overlay in SQLite afterward and rebuild on restart. In pure SQLite mode one transaction commits the intent, dependency edges and overlay metadata. Confirmed rows remain separate from optimistic patches.

A send or acknowledgement does not remove an optimistic layer. Authoritative replay with the origin command ID and committed checkpoint retires it; rebasing later dependent intents uses the same execution metadata and server-authoritative base. Rejected predecessors rebase or invalidate dependents under existing semantics. Server visibility and validation decide acceptance. Local completeness cannot authorize a write.

## Wire and server changes

Move today's text decode into the worker first, with unchanged `ServerMessage` validation, merge semantics and capabilities. Preserve control-only mode and telemetry batching. The engine owns one data socket and, only when needed, one control socket for the scope; followers do not create another pair. Sharing control across different tenant engines can wait until account-wide routing semantics are explicit.

Later add optional auth capabilities, proposed as `replicaSnapshotPages: 1` and `binaryReplica: 1`. Names and codec belong to a separate protocol PR. The auth frame and negotiation response remain JSON. Switch replica frames only after the server explicitly accepts a capability. Never send new frames merely because a client advertises support. Old mobile/SDK clients receive existing JSON, monolithic snapshots and resume behavior. Mixed clients share the same runtime safely. Do not bump the application contract.

Use a versioned MessagePack envelope for binary replica data, with entity/column dictionaries per negotiated session and lossless revision representation. Preserve absent-versus-null fields, JSON values, per-column authority, ordering and origin IDs. Keep control/error frames readable JSON initially. Benchmark parse CPU, bytes and peak allocation against JSON before shipping a codec. Impose frame/decode limits and validate before committing. Binary encoding has no permission to skip integrity checks.

Paged snapshots carry snapshot ID, scope/visibility generation, epoch/revision, page sequence, last-page marker and final digest. Pin a consistent authorized server snapshot or materialized snapshot generation across pages. Store pages in an inactive SQLite generation; interruption discards/restarts staging or resumes only under a server-issued valid token. Buffer or replay later deltas with bounds, never expose a mixed snapshot plus newer cursor. Atomically activate rows, membership and checkpoint at final validation. Progressive rendering remains explicitly partial; eager complete subscriptions publish only after completion. Cross-collection atomic transactions retain their single commit boundary.

Server work is in [`rust/crates/runtime/src/replica.rs`](../../rust/crates/runtime/src/replica.rs), [`live_query.rs`](../../rust/crates/runtime/src/live_query.rs), [`change_feed.rs`](../../rust/crates/runtime/src/change_feed.rs) and the runtime socket routing in [`lib.rs`](../../rust/crates/runtime/src/lib.rs), with types/codecs in `packages/protocol`. Add targeted old/new handshake and interrupted-page tests. Never run the full cargo workspace for these PRs.

The companion single per-tenant commit-version proposal fits `replica_checkpoints.stream = 'tenant'`. Today's change feed already has a transaction revision; that does not establish that every delivered collection/window shares one resumable completeness frontier. Continue storing per-stream epoch/cursor/digest and Live Query `windowRevision` now. Adopt a tenant cursor only after a negotiated server barrier proves every active visible stream is covered. Persist that barrier atomically with its rows/memberships. New subscriptions may still need their own snapshot boundary. Never take the maximum collection cursor as the tenant frontier.

## Public API and Whagons adoption

Keep `GonvexClient`, current generated bindings and all existing hook signatures source-compatible on the legacy path. Worker mode is opt-in. Authentication, reducer promises, queued outcomes, errors, source/completeness/freshness and offline expiry checks retain their meaning. SSR and tests can use the legacy implementation without workers.

Synchronous imperative `localReplica.entity()` and whole-table reads cannot fetch cold worker-owned rows synchronously without blocking the UI. This is a real compatibility boundary. Leave those methods and activation semantics unchanged on the default client. Add a separate typed engine-backed client factory and awaited query/retention APIs; migrate callers explicitly. A worker-backed hook can keep its call signature but has an asynchronous first-result boundary. Existing hooks that return undefined while loading can reuse that state; hooks whose readiness contract promises hydrated results must wait at provider initialization. Do not label a pending lookup as a confirmed absent row. Add a distinct pending state if current public types cannot express it.

Generated code adds a worker module URL and engine artifact manifest without removing `localRuntime.create()`. Custom adapters/executors continue using legacy mode until they opt into the new driver contract. The provider consumes the same watch abstraction after asynchronous engine readiness; direct `LocalReplica` consumers are not transparently replaced by a proxy.

Whagons adoption steps:

1. Take the independently released control-only, cached-startup, outbox, resume and telemetry changes first. No app change is required by this document PR.
2. Inventory synchronous replica reads and all full-collection hooks in the Whagons checkout. Move derived counts/filtering to engine queries; retain exact-ID hooks for details and add paged subscriptions for the virtualized task view.
3. Generate the engine bundle, cache it with the matching PWA artifact manifest, and enable worker + IDB compatibility mode for a small tenant cohort. Preserve the existing database names and journal.
4. Enable SQLite read-cache mode after import/resume/crash fixtures pass. Compare costs separately from pure SQLite; do not describe dual writes as eliminated IO.
5. Enable pure SQLite only for fresh/isolated namespaces or after the legacy admission/recovery gate above is solved. Native stays on its own tested rollout.

Proposed future integration, illustrative only; these exports/options do not exist in staging:

```ts
import { createEngineClient } from "@gonvex/client";
import { engineWorkerUrl, engineManifest } from "./generated/gonvex-engine";

const client = await createEngineClient(runtimeUrl, {
  engine: { workerUrl: engineWorkerUrl, manifest: engineManifest },
  auth: existingAuthOptions,
  storage: {
    mode: "sqlite-read-cache",
    legacyReplicaName: "whagons-v2",
    legacyOutboxName: "whagons-v2-outbox",
  },
});
```

Exact app edits require the later API PR and Whagons call-site review. Do not paste this into the app today or change its client-contract version.

## Delivery plan

Each row is an independently reviewable PR. Every implementation PR extends focused tests beside touched code, runs only those files plus the changed packages' typecheck/build, and preserves a legacy-path smoke fixture. No full-suite or workspace-wide cargo run. Keep switches independent so a storage regression does not require undoing transport work.

| Phase | Scope and dependencies | Measurable win / gate | Rollback |
| --- | --- | --- | --- |
| 0 | Define driver, RPC/watch and readiness contracts after the four in-flight SDK branches land; add deterministic benchmark fixtures | Record real-browser baseline, separate worker/DOM/storage costs; prove cold/partial read semantics | Documentation/fixture-only; no default change |
| 1 | Membership-as-rows in a new opt-in IDB engine database; reverse membership index, metadata-only checkpoints, delta storage API returning committed changes | Single-row membership update writes no full ID/hash array; no own-write `readChanges` round trip; latency independent of 50k window length | Existing adapter stays default. Export the opt-in store before switching a populated namespace; never upgrade legacy Dexie in place |
| 2 | Single-tab dedicated engine using IDB; static portable reducer binding, auth RPC, decoding/hash work off UI | 1,000-row burst has no SDK-caused main-thread task above 50 ms; durable reducer RPC retry and network restriction fixtures pass | Opt-in off after in-flight RPCs settle; same canonical journal. Multi-tab detected: retain legacy mode until phase 3 |
| 3 | Worker-owned Web Lock, follower proxies, union subscriptions, bounded RPC and handoff | Three tabs use one data/control pair per scope, one engine; kill owner at every journal boundary without losing/duplicating intents | Stop owner, recover durable work, reconnect legacy clients. Old tabs remain outside engine sharing |
| 4a | SQLite driver, declared local index metadata, import and IDB-derived cache; native schema/query parity | Indexed equality reads and cold result hydration meet targets; crash/reimport never loses source data | Discard only the derived cache; canonical IDB rows and outbox remain |
| 4b | Pure SQLite journal for fresh/isolated namespaces; lossless export and legacy recovery tooling | Eliminate compatibility dual writes; atomic intent/admission survives worker/process kill | Export live journal under exclusive ownership before IDB selection; cannot abandon pending SQLite work |
| 5 | UI row cache and engine query notifications replace whole-table proxy state; optional paged hook | Meet three-tab SDK memory target with 250 visible rows; unchanged-row identity and query-entry/exit fixtures pass | Re-enable legacy hydration/watch path after durable store export if required; full-result hook behavior preserved |
| 6 | Negotiated snapshot paging, then separately negotiated binary codec and retention-floor compaction | Bounded snapshot peak memory; binary must reduce replica bytes at least 25% and decode CPU at least 20% versus same JSON fixture | Disable new capabilities; server continues legacy JSON/snapshot path. Invalidate unsupported staging tokens, never active durable data |

Phase 1 uses a separate namespace because adding Dexie tables/version in the shared legacy database can make old tabs fail with VersionError. Its opt-in deployment must migrate/export safely, and does not accelerate old tabs. Phase 2 can run against the unmodified canonical legacy adapter for Whagons compatibility; it need not depend on phase 1's new format. This prevents the early membership improvement from blocking thread isolation. Phase 5's watch protocol and refcount design begin in phase 0; it ships after the engine can serve cold reads, not after a fake synchronous facade is introduced. The pure SQLite gate can remain closed while phases 5 and 6 deliver benefits in compatibility mode.

### Concurrent workstreams

| Workstream | Engine placement and conflict boundary |
| --- | --- |
| `perf/sdk-control-telemetry` | Keep control-only clients free of replica/outbox/runtime initialization. Move telemetry batching into owner, merge follower counters with tab attribution and bounded queues. Import its transport flags rather than recreate them. |
| `perf/sdk-cached-startup` | Cached session bootstrap runs before worker subscriptions publish; account/tenant/artifact validation and offline expiry remain gates. Auth provider stays UI-owned. Reuse its readiness state machine. |
| `perf/sdk-outbox` | Worker hosts its dependency scheduler, durable admission and negotiated send batches. Add read sets through optional metadata; unknown/legacy entries use conservative dependencies. IDB compatibility mode retains existing coordination. |
| `perf/sdk-resume` | Worker stores cursor+digest and `windowRevision` certificates. Import the exact persisted certificate format; do not recompute a certificate from partial UI cache. Resume completes before declaring current freshness. |

Avoid editing `index.ts`'s transport/admission paths concurrently with these branches. First land the contracts and membership adapter separately; extract the settled methods behind the engine interface afterward. Measurements compare the combined settled baseline, so the engine cannot claim another worker's telemetry or resume savings.

## Benchmarks and acceptance

Add a serial Playwright benchmark under `packages/client/bench/sync-engine/` in phase 0, with a local runtime and deterministic seed. The storage-only portion now lives in that directory; its README lists the native browser cases, confidence intervals and executable gates. Runtime, worker, paint, wire and recovery gates below still need their own end-to-end fixtures. Use a committed seed and artifact hash, 50,000 task rows with 50 mixed columns averaging approximately 2 KiB serialized, five related tables, a 250-row visible task view and 1,000 exact-ID subscriptions. Record actual encoded bytes. Configure the current 200 MiB/unlimited-row legacy budget. Use an isolated origin/profile with production-built assets and separate fresh/persisted profiles. No production data or load test against a shared runtime.

Measure Chromium first on the same Linux workstation with one browser and three tabs, one benchmark worker and no background builds. Run 5 warmups and 20 measured iterations, serially; alternate baseline and candidate. Record browser/SDK/runtime commits, CPU/RAM/OS, cache state, network shaping and raw samples. Use fixed 50 ms RTT and 20 Mbit/s for online cases; keep local storage timings separate. Run smaller parity/kill fixtures on Firefox and Safari/iOS before rollout; Safari QA requires a Mac/device and is not represented by Playwright WebKit alone. A Whagons `pnpm gonvex:big-tenant` run is a later integration check after inspecting that script's scope and fixture safety.

| Metric | Definition | Acceptance target |
| --- | --- | --- |
| Cached cold start | Fresh JS process with populated durable data and empty UI cache; navigation to first correct 250-row cached paint, including wasm/worker startup; offline and online separately | p95 <= 500 ms and at least 50% below settled legacy baseline; no network wait for valid cached render |
| True first install | Empty durable store to first correct partial/complete result under shaped network | No more than 10% regression before paging; report wasm download/compile separately |
| Three-tab memory | Same scope, 250 mounted task rows each; steady state after workload/GC plus peak during 1,000-row burst; attribute worker exactly once | SDK-owned steady memory <= 25 MiB per UI tab + 64 MiB shared engine, <= 139 MiB combined; at least 60% below baseline. Report total browser process memory too |
| Burst long tasks | 1,000-row delta delivered over 1 s after startup; PerformanceObserver plus main-thread trace | Zero SDK-caused UI tasks > 50 ms; p95 frame interval <= 32 ms on reference hardware; no missing atomic notification |
| Single-row delta | Change a middle/end member of a 50k-row collection; decode through durable commit and subscriber delivery | p95 durable apply <= 10 ms, visible delivery <= 32 ms; no full window serialization; 50k vs 10k apply time <= 1.25x with equal owner count |
| 500-intent drain | 500 independent covered edits, then a 500-entry dependent chain; start offline, reconnect on shaped network | Independent case <= 5 s and >= 2x settled baseline; dependent chain no > 10% regression; measure durable admission separately, p95 <= 20 ms |
| Wire/paging | Same authorized rows, dictionaries, compression settings and transport | Binary gates from phase 6; staging memory <= two 256 KiB pages plus configured SQLite cache, excluding old monolithic fallback |
| IO/network/battery | Record IDB/SQL statements, native write counts, transport bytes/socket count and 10-minute foreground/idle CPU | One owning data/control pair; no follower peer re-read loop; single-row writes proportional to changed rows and owning memberships. CPU time >= 40% lower on burst/replay workload; device energy measurements reported separately |

These absolute times are reference-hardware goals, not contractual behavior on every phone. If a gate fails, publish the result and keep the feature opt-in. SDK memory estimates do not establish a total Whagons-tab budget; React/DOM and unrelated application caches can dominate. Native benchmarks report bridge/SQLite costs on a physical phone, including suspend/resume. Do not infer battery savings solely from JavaScript timing.

Correctness acceptance is mandatory even if performance improves:

- Kill the owner before admission, after durable admission, after send, after server acknowledgement and during authoritative checkpoint persistence. Recover all acknowledged local intents with original IDs, and verify one server effect per ID.
- Fail one database write in a cross-entity transaction. No subscriber sees partial confirmed rows or advanced cursor. Reducer failure publishes no prediction; persistence failure never reports offline success.
- Switch account/tenant and change server visibility mid-snapshot. No row from the prior scope flashes; incomplete or revoked coverage cannot drive local absence decisions.
- Replay older projected rows after a delete, including epoch changes and tombstone compaction boundaries. Preserve field authority and prohibit resurrection.
- Exercise predicate phantoms, empty results, ordered limit boundaries, joins, offline partial collections, duplicate requested IDs and overlapping projections. Compare worker and legacy results.
- Open three tabs with overlapping/different subscriptions, flood a background follower, kill the owner, and reopen. Cursor gaps resnapshot; no duplicate sockets among engine participants or unbounded notification queue.
- Import interrupted IDB data with a concurrent legacy writer, stale digest and pending outbox; export back after new admissions. The selected backend has a consistent checkpoint and every pending intent remains recoverable.
- Connect an old SDK/mobile fixture without new capabilities to the new runtime, and a new engine to an old runtime. JSON fallback, visibility and authoritative replay remain intact.

No benchmark or feasibility spike ran in this documentation PR. Phase 2 must prove reducer isolation and worker bundle compatibility; phase 4 must prove pinned SAH-pool initialization/recovery behavior and migration fencing before it becomes a supported backend. Those are the remaining implementation risks, along with Safari suspension, full-result app subscriptions, wasm startup cost, SQL parity and mixed-version journal recovery.
