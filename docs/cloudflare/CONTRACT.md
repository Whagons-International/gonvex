# Gonvex contract for the Cloudflare kernel

Status: **frozen for stage 0** (package P00). Owner: the Cloudflare lead. Changes go through
`docs/cloudflare/contract-changes/NNNN-<slug>.md` and a PR that edits this file.

Reference: the Rust runtime at `0bf3ad13419d25d9370607e1470ae58da4c43235` (the `staging` head on
2026-11-10, identical to what staging runs). Every `file:line` below is at that commit. Where a doc and
the code disagree, the code is the contract and the row says so. The Go runtime is out of scope.

## How to read this document

Each item carries one disposition:

| Disposition | Meaning |
|---|---|
| **keep** | The kernel reproduces the observable behavior byte for byte (frames, status codes, error strings, ordering). Conformance compares the kernel with Rust directly. |
| **adapt** | Same observable behavior, different Cloudflare mechanism. The row names the mechanism. Conformance still compares observable behavior. |
| **replace** | The behavior changes on purpose. The row says to what and why. Clients and modules must not notice unless the row says otherwise. |
| **defer** | Not built in stage 1. The row says why and when it returns. A deferred item must fail closed (a clear error), not silently. |

"Not contract" marks implementation detail (caches, lanes, offload threads, tuning variables, pool sizes)
that the kernel may ignore. Numbers marked *tuning* are Rust defaults, not promises; numbers marked
*contract* are observable by clients or modules and must match.

### Cloudflare mechanisms named in this document

| Name | Meaning |
|---|---|
| Tenant DO | One Durable Object per tenant: SQLite (`ctx.storage.sql`), sockets (hibernation API), change log, one alarm. |
| Control DO | One Durable Object holding the control-plane schema (sessions, refresh tokens, accounts, tenants, memberships). |
| Router | The front Worker: routes `/ws`, HTTP routes and operator routes to the right object. |
| Ingress Worker | One Worker for every webhook, resolving the tenant and forwarding the raw body. |
| Action Worker | Stateless Worker that runs Actions with fetch, secrets, tools and storage. |
| Workflow | Cloudflare Workflows; a step holds an agent Action with no wall-clock limit. |
| Queue | Cloudflare Queues (migration rollout, outbox fan-out where named). |
| R2 | Object storage for files and module bundles. |
| Container | Cloudflare Containers (sandbox worker with DuckDB, media). |
| `transactionSync` | `ctx.storage.transactionSync()`; the only way to get `BEGIN`/`SAVEPOINT` semantics on DO SQLite. |
| alarm | `ctx.storage.setAlarm()`; one per object, so every timer multiplexes onto it. |

### Platform limits every row is checked against

DO SQLite: 100 KB per statement, 100 bound parameters, 100 columns per table, 2 MB per row, no custom SQL
functions, no hooks, `BEGIN`/`SAVEPOINT` only through the transaction API, no `ATTACH`/`PRAGMA`
(beyond the allowlist)/`VACUUM`. Isolate: 128 MB memory, 30 s CPU per event by default (5 min max).
Storage: 10 GB per object. WebSocket: 32 MiB per received message; a Worker cannot send protocol
pings. Queue consumers: 15 minutes. Workflow steps: no wall-clock limit.

## Intended behavior (the kernel implements this instead of copying Rust)

| # | Area | Rust today | Kernel behavior | Contract-change note |
|---|---|---|---|---|
| IB1 | Private files | Owner checked only for sandbox `importFile` (E.d.4) | Check the owner of a private file on **every** URL mint, signed download, metadata read and delete; `ownerId` on upload must be the caller (0001; metadata and proxy: 0012 R12, R13) | 0001, 0012 |
| IB2 | Actions | `actions.enqueue` jobs retry forever (backoff from 1 s, capped at 15 min); scheduled jobs never retry (E.b) | Outbox Actions: at most 8 attempts, then `dead`. Scheduled jobs: still one attempt, a failure ends `dead` not `completed` (E.b.5) | 0001 |
| IB3 | Reducer receipts | Receipt table never pruned (E.receipts) | Receipt results expire after the client's offline window; a tombstone refuses late duplicates | 0001, 0009 |
| IB4 | Webhooks | No runtime route: external adapters verify and invoke an Action (D.1 Webhooks) | One Ingress Worker for every webhook, forwarding the raw body; signatures verified in the invoked Action or the Ingress Worker (0045) | 0001, 0002 |
| IB5 | Visibility delivery | Rust never delivers a row that becomes visible when its parent is inserted later, though a fresh snapshot shows it (0031) | The kernel delivers it, converging on the snapshot state | 0032 |
| IB6 | Sandbox workspace | Rust creates the chroot workspace as root and runs the worker as uid 65534, so DuckDB cannot write `./analysis.duckdb` (15 of 19 staging calls fail) | The kernel chowns the workspace to the worker's uid | 0050 |

Out of scope by decision of the brief: the native-rows binary snapshot path (the shipped SDK never
negotiates it), the Go runtime, and Rust caches, lanes, offload threads and tuning variables.

## Contents

- A. WebSocket wire protocol
- B. Module host calls, capability sets and determinism
- C. Control Plane functions (`control.*`), auth, sessions and admission
- D. HTTP routes, the `/dev/sync` deploy flow and operator auth
- E. SQL rules, scheduler, outbox, storage, presence v2, Reducer receipts, timers
- F. Decision log and open decisions

Each section starts with its own path abbreviations.

## A. WebSocket wire protocol

Reference: Rust runtime at `0bf3ad13`. Paths are relative to the repo root. `lib.rs` means `rust/crates/runtime/src/lib.rs`. `proto` means `rust/crates/protocol/src/lib.rs`. `ts` means `packages/protocol/src/index.ts`. `sdk` means `packages/client/src/index.ts`. `ws/` means `rust/crates/websocket/src/`.

Dispositions: **keep** means the same bytes and behavior on Cloudflare. **adapt** names the Cloudflare mechanism. **replace** names the substitute. **defer** says until when.

### A.0 What the shipped SDK negotiates (scope decision)

The SDK `auth` frame is built at `sdk:5695-5711`. It sends these client capabilities: `replicaNoOldValues:1`, `replicaSnapshotPages:1` (on unless `options.replicaSnapshotPages === false`, `sdk:829`), `replicaFieldPatches:1` (only when `options.replicaFieldPatches === true`, default off, `sdk:830`), `tenantControl:1`, `telemetryBatch:1`, `replicaReadyMany:1`, `replicaWatermark:1`, `replicaBatchFrames:1`, `queryPagePatch:1`, `queryObjectPatch:1`, `queryOrderDelta:1`, `queryFanout:1`, `queryResultBatch:1`, `preserveSubscriptions:1` and `presence:1`.

- It never sends `replicaNativeRows` or `replicaSetDigest`. `git grep` finds them only in `bench/`, `scripts/bench-websocket/`, docs and Rust. Native-rows mode needs all three of `replicaNativeRows`, `replicaSetDigest` and `replicaSnapshotPages` at version 1 (`snapshot_delivery.rs:13-19`). So the binary snapshot path, `replica.snapshotAck`, `replica.snapshotStart`, `replica.snapshotEncodedPage`, `replica.transactionEncoded`, `canonicalUpserts` and `replica.closed` are never used by the SDK. **Confirmed out of scope.**
- Because `replicaSetDigest` is never sent, SDK sockets always use the legacy digest: `require_legacy_digest`, `lib.rs:2914-2920`, `replica.rs:155-159`.
- Because `replicaBatchFrames:1` is always sent, every replica feed frame of an SDK socket goes through `replica.batch` (`lib.rs:2230-2270`). The non-batch paths are never reached by the SDK: `replica.rowPatch`, compact `replica.transaction` and standalone feed `replica.watermark` (`lib.rs:2271-2332`). That holds even when `replicaFieldPatches` is on.
- With paging on, snapshots are sent as `replica.snapshotPage` text frames through `LegacySnapshotDelivery` (`lib.rs:2012-2016`, `snapshot_delivery.rs:295-362`).
- The SDK reads text frames only: `JSON.parse(String(event.data))` at `sdk:2209`. It never sets `binaryType`. A binary frame becomes "[object Blob]", fails to parse and is dropped.
- The SDK sends one close code of its own: `4002 "replica row patch recovery"` (`sdk:2231`).

### A.1 Connection lifecycle

#### A.1.1 Upgrade

| Item | Rust behavior | Source | Disposition |
|---|---|---|---|
| Path | `GET /ws`. Operator dashboard socket: `GET /dev/dashboard/ws` (separate protocol, see last row). | `lib.rs:555-556` | adapt: a Worker router forwards the upgrade to the object that accepts the socket (see A.1.2) |
| Query params | `public=1` selects the public transport. Any other value or no value selects the authenticated transport. No other parameter is read. The SDK sets it when `publicTenant` is set (`sdk:2141-2146`). | `lib.rs:1580` | keep |
| Headers read | `Sec-WebSocket-Extensions`, used for permessage-deflate. Invalid extension syntax returns HTTP 400. The client IP comes from `GONVEX_CLIENT_IP_HEADER` (default `x-real-ip`) behind trusted proxies. No subprotocol (`SubprotocolRegistry::default()`). No Origin check. | `ws/lib.rs:101-121`, `ws/negotiation.rs:14-109`, `public_functions.rs:178-200` | adapt: client IP comes from `CF-Connecting-IP`. Compression: see A.1.6 |
| Upgrade rejections | HTTP 429 `pending connection limit exceeded` when the IP already has 100 unauthenticated sockets (`GONVEX_PENDING_CONNECTIONS_PER_IP`). HTTP 429 `public connection limit exceeded` when the IP has 20 public sockets (`GONVEX_PUBLIC_CONNECTIONS_PER_IP`). HTTP 400 on a bad handshake or a bad extension header. | `lib.rs:1581-1608`, `ws/lib.rs:101-116` | adapt: counters per IP across objects need a limiter object or the Workers rate-limit binding. The Worker returns the 429 before the upgrade |
| Dashboard socket | `/dev/dashboard/ws` has its own `auth.result`/`query.*` JSON (`dashboard.rs:190-326`). It is not part of the SDK contract. | `dashboard.rs:45` | defer to the operator-routes section |

#### A.1.2 Handshake order and routing

1. Right after the upgrade, before any client frame, the server sends `session.ready` with `project`, `tenant` and `replica` absent and only `capabilities` set (`lib.rs:1758-1791`). It is never sent again on that socket: `SessionReady` has no other construction site in the runtime.
2. The client sends `auth` (`sdk:2165`, on `open`). The tenant and project are known only from this frame. The URL does not carry them.
3. The server replies with `auth.result`, or `auth.error` and keeps the socket open. Exceptions: `client.updateRequired` closes, and a public/transport mismatch closes.
4. The client sends subscriptions and calls.
5. Re-auth: any later `auth` frame on the same socket re-authenticates in place. It may switch tenant or project (A.1.4).

- SDK note: `session.ready.replica` is always absent from Rust, so the SDK takes the directive from `auth.result.result.replica` (`sdk:2389`). The SDK's `session.ready` replica branch (`sdk:2253-2268`) is unused with Rust.
- **Routing consequence (adapt, open: contract-changes 0004):** a Worker cannot pick the tenant Durable Object at upgrade time. The tenant arrives in the first frame and can change on re-auth. Control-only sockets have no tenant. Pending sockets (no `auth` yet) belong to no tenant. Changing the SDK to put the tenant in the URL is not allowed by the brief. The options are:
  - (a) A gateway object (per connection or per project shard) accepts the socket, sends `session.ready`, parses `auth` and talks to the tenant object over RPC. This is the "gateway placement" in the brief's design rules.
  - (b) The tenant object accepts the socket after a peek-and-forward. This does not support in-place tenant switching without a reconnect, which changes SDK-visible behavior.

  Decision tracked in `contract-changes/0004-socket-routing-tenant-in-auth-frame.md`.

#### A.1.3 `session.ready` contents (`lib.rs:1760-1786`)

The frame is `{"type":"session.ready","capabilities":{...}}`. Server capabilities, serialized in struct order (`proto:567-622`):

| Capability | Value | Read by SDK? | Disposition |
|---|---|---|---|
| `replicaNoOldValues` | 1 | no (type only) | keep |
| `replicaFieldPatches` | 1 | yes (`sdk:2221`, before decoding a `replica.rowPatch`) | replace (0012 R20): not advertised by the kernel. Unused by the SDK while `replicaBatchFrames` is on |
| `replicaSnapshotPages` | 1 | — | keep |
| `replicaSetDigest` | 1 (**Rust-only**) | no | replace: do not advertise. sum128 is not negotiated by the SDK |
| `replicaNativeRows` | 1 (**Rust-only**) | no | replace (0012 R20): not advertised by the kernel |
| `replicaResume` | 1 | yes (`sdk:4133`, decides whether `replica.open.digest` is sent) | keep |
| `queryResume` | 1 | — | keep |
| `preserveSubscriptions` | 1 | — | keep |
| `telemetryBatch` | 1 | — | keep |
| `protocolVersion` | `PROTOCOL_VERSION = 2` (`proto:91`) | — | keep |
| `runtimeVersion` | `config.runtime_version`, the Git SHA | — | adapt: the Worker version id or the Git SHA from a deploy binding |
| `replicaBatch` | 1 | — | keep |
| `replicaIntegrity` | 1 | — | keep |
| `queryBatch` | 1 | — | keep |
| `queryResultBatch` | 1 | — | keep. Rust never emits `query.batch` (A.3), so this is harmless |
| `reducerBatch` | 1 | — | keep |
| `replicaWatermark` | 1 | yes (`sdk:2530-2531`; watermarks are ignored without it) | keep |
| `reducerReceiptLookup` | 1 | — | keep |
| `controlWatermark` | 1 | — | keep |
| `replicaBatchFrames` | 1 | — | keep |
| `presence` | 1 only when `GONVEX_PRESENCE_V2` is on (default on) | yes (`sdk:2242`) | adapt: the S3 spike decides liveness. Advertise only when presence is implemented |

#### A.1.4 Auth and re-auth

Disposition for every row in this subsection: **keep** (frames, codes and order); the session state lives in the object that owns the socket (0004).

The handlers are `lib.rs:2714-2989` and `authenticate` at `lib.rs:4464-4780`.

| Case | Condition | `auth.result.result` / `auth.error` | Source |
|---|---|---|---|
| Service principal | `token` starts with `gvx_svc_` and `public` is false | `{"projectId","tenantId","accountId":"","servicePrincipal","functions":[...],"maxDelegationSeconds"}`. The socket is then limited to `handle_service_frame` (A.2 notes). Any failure returns `auth.error code:"transient"` with error `service credential is invalid or not permitted for this project`, `auth session store is unavailable` or `tenant is not available`. | `lib.rs:2714-2774`, `service_principal.rs:248-345,348-447` |
| Public | `public:true` (it must match the `?public=1` transport, otherwise `auth.error` "public authentication requires the public WebSocket transport" and close) and no token, not control-only, project and tenant set | `{"projectId","tenantId","accountId":"","public":true,"artifactHash"}`. Failure: `auth.error transient` "public tenant session is unavailable". The scope is pinned per socket: changing it sends `auth.error transient` "public connection scope is fixed" and close 1008. The scope is rechecked every 30 s, and immediately before a tenant-code frame. | `lib.rs:2622-2637,2819-2866,4490-4532,3877-3906` |
| Control-only, no token | `controlOnly:true`, no token | Requires `project` (`auth.error transient` "project is required for public Control Plane calls"). Result `{"projectId","accountId":"","tenantId":""}`. **The pending flag is not cleared, so the socket still closes at the 10 s auth deadline (1008).** | `lib.rs:4533-4569,2867-2871` |
| Developer or impersonation | token `gvx_imp_*` or `gvx_dev_*` | `{"projectId","accountId","tenantId","impersonationId","impersonatorId","developerSessionToken","localIdentity":{auth:{account:{id,email,name,avatarUrl},developerSession},tenant:{id},member:{id,accountId,status,role,displayName,permissions}},"artifactHash","replica"}`. Control-only without a tenant: `auth.error` "developer grants require an active tenant". | `lib.rs:4580-4664` |
| Control-only with token, no tenant | | `{"projectId","accountId","tenantId":"","artifactHash"}` | `lib.rs:4665-4709` |
| Session | token plus tenant | `{"projectId","accountId","tenantId","artifactHash","replica":ReplicaDirective,"localIdentity":{auth:{account:{...}},tenant:{id},member:{...}}}` | `lib.rs:4710-4780` |
| No token | | `auth.error transient` "a Gonvex app session is required" | `lib.rs:4570-4579` |
| Control-only output | `controlOnly:true` | `replica` and `localIdentity` are removed from the result (`suppress_control_replica`) | `lib.rs:2897-2899,4438-4445` |
| `tenantControl` | `controlOnly:true` with `capabilities.tenantControl:1` | The tenant is kept for control-only auth. Without the capability it is dropped. | `lib.rs:4412-4425` |
| `subscriptionsPreserved` | `preserveSubscriptions:1`, same account, route, member, delegation and impersonation, not anonymous, not control-only, no opens or legacy pages in flight | `"subscriptionsPreserved":true` is added to `result`, and the subscriptions and feed are kept. Otherwise all subscriptions, opens, pages, live queries and control queries are dropped and the feed is re-subscribed. | `lib.rs:2926-2966,5212-5232` |
| Auth timeout | Each `auth` is wrapped in `timeout_at(auth_deadline or now+10 s)` | On timeout the loop breaks and the socket closes with no frame | `lib.rs:2788-2811` |
| Revalidation | A connection with a token is revalidated every 30 s (`auth_revalidation::INTERVAL`), batched process-wide | When invalid: drop all subscriptions, then `auth.error {id:"session-expired",code:"transient",error:"authentication expired or was revoked"}`. The socket stays open. | `lib.rs:2039-2083`, `auth_revalidation.rs:30` |
| Membership change | The feed (or a re-read after the connection's own commit) shows the session Member's `account_id`, `status`, `role`, `permissions` or `membership_revision` changed | `discard_tenant_session!` runs, then `auth.error {id:"membership-changed",code:"transient",error:"tenant membership changed; authenticate again"}`, then `replica.watermark` for the transaction | `lib.rs:2183-2191,3322-3337,4250-4347` |

`auth.error` code mapping (`lib.rs:4453-4462`): `InvalidSession` and `SessionProjectMismatch` map to `credential_rejected`. `MemberNotFound` and `NoTenantMembership` map to `member_removed`. Everything else maps to `transient`. The SDK also acts on `revoked` and `account_disabled` (`sdk:2287,2436-2438`). **Rust never emits these two at this ref.**

`ReplicaDirective` (`modules.rs:507-567`) is `{protocolVersion:1, scope, visibilityScope, epoch}`. Each is a SHA-256 hex of JSON. **`epoch` hashes `database_url`.** If a Durable Object computes it from a different database identifier, every client's stored replica epoch changes and all replicas are reset at cutover. **adapt:** reproduce the Rust `database_url` string per tenant, or accept one full reload per tenant at migration. Ruled: reproduce it (`contract-changes/0008-replica-epoch-database-url.md`).

Disposition for auth overall: **adapt**. The 10 s deadline and the 30 s revalidation become timers in the object that holds the socket: `setTimeout` while awake, plus one DO alarm multiplexed over all sockets so hibernated sockets are still revalidated. Session and membership lookups go to the control-plane store over RPC.

#### A.1.5 Version and update gating

- `client.updateRequired {reason:"CLIENT_UPDATE_REQUIRED", contract:<module.client_contract>}` is sent in three cases, each followed by an immediate close with no close frame (loop `break`):
  - (a) At auth, when it is not control-only, a tenant was resolved and `auth.clientContract != module.client_contract` (`lib.rs:2873-2896`).
  - (b) Before handling **every** inbound frame while a tenant session exists and the contract differs (`lib.rs:2534-2556`).
  - (c) On a module reload event, when the contract changed (`lib.rs:2479-2496`).
- `system.reload {reason:"module generation changed", artifactHash}` is sent on `RuntimeEvent::ModuleReloaded` for the socket's project. All replicas, opens, pages and live queries are dropped first (`lib.rs:2479-2508`). The SDK resubscribes (`sdk:2275-2282`).
- `reducer.call.artifactHash` and `reducer.call.clientContract` are checked per call. A mismatch returns `reducer.error class:"update_required"` with `STALE_REDUCER_ARTIFACT: ...` or `CLIENT_UPDATE_REQUIRED: ...` (`execution.rs:57-60`, `error_class.rs:123-125`). The SDK regex-matches these prefixes for legacy errors without a class (`sdk:5992`).
- Disposition: **adapt**. The deploy flow (`POST /dev/sync`) must notify every live tenant or gateway object of the project, through a project registry and RPC fan-out, to emit `system.reload` and `client.updateRequired`. Hibernated objects must be woken to do so. Frame shapes are **keep**.

Stage-1 baked deployment has the active-socket reconnect adaptation already accepted in 0016 G0-2;
0028 §1 defers Facets. Decision 0060 preserves raw same-socket failures separately from actual unchanged-SDK
upgrade recovery evidence. Neither a local Wrangler reload nor a unit suite proves platform upgrade recovery.
Decision 0061 permits only an empty, explicitly acknowledged, server-epoch bootstrap on the ordered delivery
channel after tenant restart; unscoped data never becomes authorized by retagging it with a returned epoch.

#### A.1.6 Compression

| Item | Rust | Source | Disposition |
|---|---|---|---|
| Negotiation | Only permessage-deflate, on by default (`GONVEX_WS_COMPRESSION=true`). The server always answers `server_no_context_takeover; client_no_context_takeover; server_max_window_bits=min(offer,12)`, plus `client_max_window_bits=N` when offered. Invalid parameters decline that offer. Invalid syntax returns 400. | `ws/negotiation.rs:14-109`, `config.rs:259-263` | adapt: workerd negotiates permessage-deflate itself under the `web_socket_compression` compatibility flag (verify in S3). The server cannot pick window bits or no-context-takeover |
| Threshold | Only messages of at least 1024 bytes are compressed (`GONVEX_WS_COMPRESSION_THRESHOLD`). Level 1. | `config.rs:264-268`, `ws/shared.rs:23`, `ws/lib.rs:407` | replace: platform policy. Not observable by the SDK |
| Compress once | Shared DEFLATE segments are concatenated into one `replica.batch` frame (`SharedText`, sync-flush segments). | `ws/shared.rs:1-18`, `delivery.rs:20-21,326-329` | replace: encode the text once per visibility class and send the same string to every socket. Compression is the platform's job |
| Best-of-two | `send_best_text(full, patch)` sends whichever of the full row-patch frame and the patch frame compresses smaller. | `ws/lib.rs:388-404`, `lib.rs:2283` | defer with row patches |
| Compact transaction choice | Without batch frames, `oldValue` images are kept when they make the message cross the compression threshold. | `protocol/src/replica_wire.rs:10-43` | defer: not reached by the SDK (batch frames on) |

#### A.1.7 Size limits

| Limit | Value | Behavior when exceeded | Source | Disposition |
|---|---|---|---|---|
| Inbound message, authenticated transport | 64 MiB message, 16 MiB frame | Transport read error, break, no application close (plain tungstenite sends no close frame; the compressed ratchet path sends 1002; 1009 never occurs, test at `ws/lib.rs:1107-1150`) | `lib.rs:1609-1618`, `ws/lib.rs:146-155` | adapt: the platform caps received messages at 32 MiB. Document 32 MiB as the new ceiling. SDK frames are far below it |
| Inbound, public transport | 64 KiB message and frame (`GONVEX_PUBLIC_MAX_PAYLOAD_BYTES`) | The code attempts close 1009 "public transport limit exceeded", but clients observe **1002** (the transport closes with a protocol error first; measured by K05, ruled in 0022 §4.3). The kernel closes 1002 | `lib.rs:1609-1618,2523-2533` | keep: check `message.length` in `webSocketMessage` |
| Inbound text or binary on an unscoped or anonymous socket | 64 KiB | Close **1009** "public payload too large". A public `auth` over 64 KiB also closes 1009. | `lib.rs:2583-2619,2652-2660` | keep |
| Inbound binary frames | Ignored after the size check (no handler) | — | `lib.rs:3855` | keep |
| `query.subscribeMany` | at most 256 | `query.error {id:"",error:"query batch cannot contain more than 256 subscribes"}` | `lib.rs:3155-3169` | keep |
| `reducer.callMany` | at most 256 | `reducer.error {id:"",class:"rejected",retryable:false,error:"reducer batch cannot contain more than 256 calls"}` | `lib.rs:3351-3366` | keep |
| `replica.openMany` | at most 256 (`REPLICA_OPEN_BATCH_LIMIT`) | `replica.error {id:"",error:"replica batch cannot contain more than 256 opens"}` | `lib.rs:154,3593-3603` | keep |
| One `replica.open` | `json_len*2+1024 <= 32 MiB` | `replica.error` "individual replica open request is too large" | `lib.rs:5122-5126,5147-5150`, `replica_open_queue.rs:117` | keep |
| `telemetry.batch` | at most 50 events, and only when `telemetryBatch:1` was negotiated | The whole batch is silently dropped | `lib.rs:3789-3809` | keep |
| `error.envelope` | 1-20 events, each at most 32 KiB, total at most 256 KiB | `error.ack {error:"invalid error envelope"/"error envelope is too large"}` | `telemetry.rs:20-22,224-288` | keep |
| Legacy snapshot page | at most 256 KiB or 512 rows. One oversized row is sent alone. | — | `replica.rs:3017-3018,3189-3222` | keep. Pages stay far below 32 MiB, which matters for 128 MB isolates |
| Legacy paged subscriptions in flight | 256 per socket | `replica.error` "legacy snapshot subscription limit exceeded" | `snapshot_delivery.rs:319-325` | keep |
| `replica.batch` | Flushed at 512 KiB uncompressed (`GONVEX_REPLICA_BATCH_MAX_BYTES`) | — | `delivery.rs:41-71,1003-1005` | keep |
| Non-paged `replica.snapshot` | Unbounded single frame. Used only when `replicaSnapshotPages` is not negotiated. | — | `lib.rs:5306-5318` | adapt: must stay under the platform's outbound limit (verify in S3) and isolate memory. Page or reject large windows |
| Row-patch dictionary | 4 MiB / 4096 rows per socket | Eviction (`evicted`) | `protocol/src/row_patch.rs:12-13` | defer with row patches |

#### A.1.8 Keepalive, liveness, slow consumers

| Mechanism | Rust | Source | Disposition |
|---|---|---|---|
| Client Ping | Answered with Pong carrying the same payload | `lib.rs:3849-3853` | keep: the platform answers automatically |
| Server Ping | Every 20 s (`GONVEX_PRESENCE_PING_SECONDS`), **only on sockets that negotiated `presence:1`** | `lib.rs:1710-1712,2424-2441`, `presence.rs:63-65,94` | replace: a Worker cannot send protocol pings. Liveness must come from the platform's close and error events, or from an application frame the SDK already sends. The SDK sends no periodic frame under presence v2. S3 decides |
| Liveness timeout | 60 s with no inbound frame (Pong included). Server back-pressure and write progress count as activity. | Close **4000** "presence liveness timeout" | `lib.rs:2430-2437`, `presence.rs:66-69` | replace (S3). Without pings the server cannot detect a silent peer. Presence offline relies on `webSocketClose`/`webSocketError` plus the 10 s offline grace (`presence.rs:60-62`) |
| Outbound queue | 256 KiB or 1024 frames per socket (`GONVEX_WS_OUTBOUND_MAX_*`). `send` waits while full. Backlogged at 64 KiB queued or when a write is blocked. | `ws/connection.rs:39-62,600-605`, `config.rs:630-677` | replace: workerd `WebSocket.send` has no back-pressure signal. Bound per-socket pending batches in object memory and drop or resync instead of queueing |
| Send stall | No write progress for 30 s (`GONVEX_WS_SEND_STALL_TIMEOUT_SECONDS`): **no close frame is sent**; the socket is dropped with TCP RST (SO_LINGER 0) (`ws/connection.rs:191-215,381-386`). `4008` appears only in a log field (`ws/connection.rs:396-401`) | `ws/connection.rs:35-37,322-392` | replace: not observable on the platform. Rely on the platform closing dead peers. Note it in S3 |
| Coalescing while backlogged | Feed events stay in the broadcast ring and are merged into one transaction when caught up. Presence merges per Member. Ring lag sends `replica.reset reason:"change-feed-lagged"` to every subscription and clears them (and live queries). | `lib.rs:1891-1903,2129-2155,2367-2386`, `delivery.rs:334-517` | adapt: the change log lives in the tenant object's SQLite. Per-socket cursors and a lag bound produce the same `replica.reset` |
| Batch tick | A batch holding rows is sent within 50 ms of opening. A ready- or watermark-only batch waits 1000 ms. It is sent at once when a call result waits for the batch's watermark (`batcher.urgent`). | `delivery.rs:41-71,617-657`, `lib.rs:1970-1973,3034-3051` | adapt: `setTimeout` inside the awake object. No alarm is needed because a batch only exists while the object is handling a write |
| Pending auth deadline | 10 s (`GONVEX_AUTH_TIMEOUT_SECONDS`) until an identity, tenant or service grant is established | Close **1008** "authentication deadline exceeded" | `lib.rs:1680-1681,2029-2034`, `config.rs:488-492` | adapt: `setTimeout` while awake plus the DO alarm. Hibernation must not lose the deadline |
| Hibernation state | All per-socket state is task-local in Rust: subscriptions, cursors, batch bindings, `sent_replica_revision`, row-patch dictionary, presence slot, contract and capabilities | `lib.rs:1688-1756` | adapt: DO WebSocket Hibernation (`acceptWebSocket`, `serializeAttachment`). The attachment is small (2 KiB in docs; verify), so only identity, capabilities, contract and a session id fit. Subscriptions must be rebuilt from SQLite or the object must stay awake while subscriptions exist. S3 decides. Nothing ephemeral goes to SQLite (brief) |

#### A.1.9 Close codes

| Code | Reason string | When | Source | Disposition |
|---|---|---|---|---|
| 1008 | `authentication deadline exceeded` | No successful identity, tenant or service auth within 10 s of upgrade | `lib.rs:2029-2034` | keep (verify workerd accepts server-sent 1008) |
| 1008 | `public connection scope is fixed` | A public socket re-auths to another project or tenant (after the `auth.error`) | `lib.rs:2844-2850` | keep |
| 1002 (observed; code path names 1009 `public transport limit exceeded`) | protocol error | Read error (oversize or protocol) on a `?public=1` socket | `lib.rs:2523-2533` | keep **1002**, what clients see (0022 §4.3) |
| 1009 | `public payload too large` | Text or binary over 64 KiB on an unscoped or anonymous socket, or a public `auth` over 64 KiB | `lib.rs:2583-2619,2652-2660` | keep |
| 1003 | `invalid Gonvex protocol frame` | A text frame fails `ClientMessage` deserialization: unknown `type`, a missing required field, or a type mismatch in any field, including optional ones such as `device.viewportWidth` being non-integer. Also any unparsable frame on a service-principal socket. | `lib.rs:3839-3847,2572-2580` | keep: the validator must be exactly as strict as serde (see A.6) |
| 4000 | `presence liveness timeout` | Presence socket silent for 60 s | `lib.rs:2430-2437` | replace (A.1.8) |
| 1007 / 1002 | transport | Invalid UTF-8 text (ratchet `CloseCode::Invalid`) or a protocol violation | `ws/lib.rs:244-256`, tests `ws/lib.rs:1108-1150` | keep (platform) |
| none (TCP close) | — | After `client.updateRequired`, after an `auth.error` for a public/transport mismatch, a revoked public admission (`auth.error id:"session-expired"`), a running auth timing out, a send failure, or the runtime event bus closing. Queued frames are flushed first. | `lib.rs:2035-2038,2622-2637,2639-2651,2809-2811,2893`, `ws/connection.rs:436-454,355-369` | adapt: `ws.close(1000)` or `1011` after the frame. The SDK reconnects on any close |
| 1012 | `gateway capacity rebalance` | X01 bounded staggered reconnect/drain after a safe serialized session boundary and replacement admission; preserve unresolved direct Actions and existing cursor/receipt recovery | Lead ruling 0054; SDK reconnect path `packages/client/src/index.ts:2170` | adapt (authorized; exact local/deployed recovery evidence required before acceptance) |
| 4002 | `replica row patch recovery` | **Client-initiated** after a row-patch decode failure | `sdk:2231` | keep (client side) |
| 1000, 1001, 1011, 1012, 1013, 3001, 4001, 4999 | application-chosen | The transport passes them through unchanged (test only; not emitted by the runtime) | `ws/lib.rs:1202-1220` | keep (pass through) |

### A.2 Client→server messages

The handler dispatch is `lib.rs:2712-3856`. Rust decodes with `serde(tag="type")` and no `deny_unknown_fields`, so unknown fields are ignored. Fields marked `#[serde(default)]` are optional in Rust even when TS requires them. "TS" means `ts:320-380`.

| `type` | Fields | Response / ack | Rust-only? | Source (TS; Rust type; handler) | Disposition | Notes |
|---|---|---|---|---|---|---|
| `auth` | `id: string`; `clientContract?: number` (u64); `token?: string`; `project?: string`; `tenant?: string`; `controlOnly?: boolean`; `public?: boolean`; `device?: BrowserTelemetryInfo`; `capabilities?: ClientCapabilities` | `auth.result` or `auth.error` (same `id`); or `client.updateRequired` then close | no | ts:321; proto:390-413; lib.rs:2714-2989 | adapt (A.1.2, A.1.4) | `BrowserTelemetryInfo` (`ts:152-167`, `proto:207-238`): `viewportWidth`, `viewportHeight`, `hardwareConcurrency` and `touchPoints` are **u32** in Rust (a non-integer closes 1003). `deviceMemory` is f64; the rest are strings. Client capability keys are listed in A.0 and A.6. Rust re-reads every client capability on each `auth` frame and accepts only the value `1` (`lib.rs:2899-2920`); a failed re-auth tears down the existing session. A service-principal token takes the `gvx_svc_` branch |
| `query.call` | `id: string`; `path: string`; `args: JsonValue` (Rust default null); `scope?: "tenant"\|"control"` | `query.result {id,path,result,reason:"initial"}` or `query.error`. For the receipt query `__gonvex.reducerReceipt` with `found:true`, a `replica.watermark` follows or the batch is made urgent | no | ts:322; proto:414-422; lib.rs:2990-3067 | keep | Without a tenant: `query.error` "authenticate with an active tenant before calling a Query". `scope:"control"` goes to the control plane (another section) |
| `query.subscribe` | `id`; `path`; `args`; `scope?`; `windowRevision?: string` | `query.result {id,path,windowRevision,result,reason,subscriptionRevision{epoch,sequence}}`, or `query.progress {id,path,throughRevision}` when `windowRevision` equals the current one, or `query.error`. Control subscriptions return `query.result {id,path,result,reason}`. Updates follow | no | ts:323; proto:423-437; lib.rs:3068-3154; live_query.rs:984-1029 | keep | A new live query marks the opening cohort, so a bootstrap `replica.watermark` follows once all snapshots are sent (`lib.rs:1848-1875`) |
| `query.subscribeMany` | `subscribes: QuerySubscribeRequest[]` (`{id,path,args,scope?,windowRevision?}`) | One `query.result`, `query.progress` or `query.error` per entry, in order | no | ts:337, ts:218-224; proto:440-443, proto:319-330; lib.rs:3155-3248 | keep | at most 256 |
| `query.unsubscribe` | `id` | none | no | ts:324; proto:438-439; lib.rs:3249-3259 | keep | Removes the live or control subscription |
| `replica.open` | `id`; `path`; `args`; `cursor?: {epoch: string, revision: number}`; `keys?: string[]`; `hashes?: Record<string,string>`; `digest?: string`; `fullIntegrity?: boolean` | Queued. Later: `replica.snapshotPage`(s) then `replica.ready`; or `replica.delta`/`replica.ready` on resume; or `replica.needHashes`; or `replica.error` | no | ts:325-335, ts:240-250; proto:444-445, proto:355-373; lib.rs:3585-3592, 1979-2021, 5128-5184 | keep | **`keys` is accepted and never read** by the runtime. `digest` starting with `xor1:` selects aggregate verification (`replica.rs:584-595`). Re-opening an id cancels the earlier open. Without a tenant: `replica.error` "authenticate with an active tenant before opening a Replica Collection" |
| `replica.openMany` | `opens: ReplicaOpenRequest[]` | As for `replica.open`, per entry | no | ts:336; proto:446-447; lib.rs:3593-3614 | keep | at most 256. Only the last open per id in a frame publishes |
| `replica.close` | `id` | None for the SDK. On native sockets: `replica.closed {id}` | no | ts:338; proto:448-449; lib.rs:3667-3692 | keep | Cancels pending opens and legacy pages for the id |
| `replica.snapshotAck` | `id: string`; `snapshotId: string`; `sequence: u64` | May release `replica.ready` and more binary pages. Error: `replica.error` "native snapshot acknowledgements were not negotiated" or "snapshot acknowledgement exceeds sent sequence" | **yes** | proto:450-457; lib.rs:3615-3666 | defer (native rows out of scope) | A non-native socket that sends it gets `replica.error` and loses that subscription |
| `reducer.call` | `id`; `path`; `args`; `scope?`; `trace?: MessageTrace`; `idempotencyKey?: string`; `artifactHash?: string`; `clientContract?: number`; `receiptPath?: string`; `intentEntropy?: string` | `reducer.result` or `reducer.error`, then `replica.watermark` when the connection has already sent through `committedRevision` (else the batch is made urgent). Control writes: control `query.result` updates, then `reducer.result`, then `control.watermark {id}` | no | ts:339-356, ts:226-238; proto:458-459, proto:332-353; lib.rs:3260-3350, 4884-5101 | keep (execution: other sections) | `trace` is echoed verbatim; the server fills no timing fields. Presence short-circuits: a repeat `control.support.heartbeat` returns `{sessionId:"support_<conn>"}` from memory, and the configured legacy beat paths return `now_ms` with `committedRevision = sent_replica_revision` (`lib.rs:4870-4994`). Without a tenant: `reducer.error class:"unauthenticated"` "authenticate with an active tenant before calling a Reducer" |
| `reducer.callMany` | `calls: ReducerCallRequest[]` | Per call, in order, the same as `reducer.call` | no | ts:357; proto:460-461; lib.rs:3351-3466 | keep | at most 256. Public sockets check admission per call |
| `action.call` | `id`; `path`; `args`; `scope?`; `idempotencyKey?: string`; `trace?` | `action.result {id,path,result,committedRevision?,trace?}` or `action.error`, then a watermark as for reducers. Control: updates, `action.result`, `control.watermark` | no | ts:358; proto:462-478; lib.rs:3467-3584 | adapt: long Actions run in a Workflow (S4); the socket gets the result over RPC. Shape keep | Without a tenant: `action.error` "authenticate with an active tenant before calling an Action" |
| `error.register` | `id`; `release?: string`; `environment?: string` | `error.ack {id, error?}` | no | ts:359; proto:479-486; lib.rs:3693-3711; telemetry.rs:142-222 | keep | Upserts the `gonvex_support_sessions` row. Rate limited per 60 s window keyed by project plus subject: 120 per account, or 20 per connection when anonymous (`telemetry.rs:35-50, 288-296`) |
| `error.envelope` | `id`; `events: JsonValue[]` | `error.ack {id, accepted?, fingerprints?, error?}` | no | ts:360; proto:487-488; lib.rs:3778-3788; telemetry.rs:224-288 | keep | Limits in A.1.7 |
| `error.heartbeat` | `id` | `error.ack {id}` | no | ts:361; proto:489-490; lib.rs:3763-3777 | keep | Same as `register` with no release or environment |
| `presence.set` | `state: "active"\|"idle"\|"offline"` | none | no | ts:363; proto:527-528; lib.rs:3712-3726; presence.rs:105-111 | adapt: presence lives in the tenant object's memory (S3) | Rust trims, lowercases and accepts `gone` as offline. Invalid states, or no tenant Member session (anonymous, service, delegated), are silently ignored |
| `presence.subscribe` | `id` | `presence.snapshot {id,serverTime,members}` or `presence.error {id,error}` | no | ts:364; proto:531-532; lib.rs:3727-3758 | adapt (as above) | Errors: "presence requires an authenticated tenant member session", "presence v2 is disabled" |
| `presence.unsubscribe` | (none) | none | no | ts:365; proto:533-534; lib.rs:3759-3762 | adapt | Rust variant `PresenceUnsubscribe {}` |
| `telemetry.event` | `id: string`; `kind: "query"\|"reducer"\|"action"`; `path: string`; `reason?: "initial"\|"change"\|"recover"`; `outcome: "ok"\|"error"`; `error?: string`; `clientSentAtMs?: number`; `clientReceivedAtMs: number` (required); `clientDurationMs?: number`; `trace?`; `device?` | none | no | ts:367-380; proto:491-519; lib.rs:3810-3838; telemetry.rs:60-90 | adapt: write to Analytics Engine or a queue, not tenant SQLite | Rust types `kind`, `outcome` and `reason` as String and drops invalid values silently (`id` at most 200, `path` at most 500) |
| `telemetry.batch` | `events: Array<telemetry.event fields without type>` | none | no | ts:366; proto:520-523, proto:537-565; lib.rs:3789-3809 | adapt (as above) | Only when `telemetryBatch:1` and 50 or fewer events. `device` falls back to `auth.device` |
| WS Ping / Close | — | Pong / ends the loop | — | lib.rs:3849-3854 | keep (platform) | |
| Binary (any) | — | Ignored | — | lib.rs:3855 | keep | |

Service-principal sockets (`gvx_svc_`) accept only `auth`, `reducer.call`, `action.call` (tenant scope), `query.call` (tenant scope), `query.subscribe` (→ `query.error` "service principals cannot subscribe"), `replica.open` (→ `replica.error` "service principals cannot open replicas"), and the batched frames (→ `reducer.error {id:"",class:"rejected"}` "batched frames are not available to service principals"). Every other frame is ignored. A parse failure closes 1003. A function outside the allowlist returns "function \"<path>\" is not allowed for this service principal". Service `reducer.error` frames carry no `class` or `retryable`. Source: `service_principal.rs:348-489`. Disposition: **adapt** (the Go bridge, K21; the grant check moves into the object).

Public transport rate limits: 30 calls per connection and 120 per IP per 60 s window. A `*Many` frame costs `len`. When the limit is hit, a per-frame error is sent (`public_limit_errors`, error "public call rate limit exceeded"; every frame type other than `auth`, `query.call`/`query.subscribe(Many)`, `reducer.call(Many)`, `replica.open(Many)` and `action.call` (that is `query.unsubscribe`, `replica.close`, `error.*`, `presence.*`, `telemetry.*`, `replica.snapshotAck`) gets `auth.error {id:"public-rate-limit",code:"transient"}`; a rate-limited `auth` frame gets `auth.error` with its own id). Source: `lib.rs:2661-2687,4349-4410`, `public_functions.rs:52-60,153-175`. Disposition: **adapt**: per-object counters in the object that owns the socket (0012 R11).

### A.3 Server→client messages

"Emitted by Rust" tells whether the runtime ever sends the message at this ref. Rust payload maps marked `flatten` serialize keys in alphabetical order after the fixed fields. "TS" means `ts:395-629`.

| `type` | Fields | Responds to | Rust-only? | Source (TS; Rust; emit site) | Disposition | Notes |
|---|---|---|---|---|---|---|
| `session.ready` | `project?`, `tenant?`, `replica?: ReplicaDirective`, `capabilities?: ServerCapabilities` | Upgrade (unsolicited, first frame) | no (two capabilities are Rust-only) | ts:429-435; proto:654-664; lib.rs:1758-1791 | adapt (A.1.2-3) | Rust sends only `capabilities` |
| `auth.result` | `id`, `result: JsonValue` (A.1.4) | `auth` | no | ts:436; proto:665-666; lib.rs:4464-4780, service_principal.rs:332-344 | keep (shape); adapt (source) | `subscriptionsPreserved` is added on preserve |
| `auth.error` | `id`, `error`, `code?` (`transient`, `credential_rejected` or `member_removed`) | `auth`, or unsolicited with id `session-expired`, `membership-changed` or `public-rate-limit` | no | ts:437; proto:667-673; lib.rs:4260-4268, 2035-2038, 2073-2077, 4404-4408 | keep | |
| `query.result` | `id`; then alphabetical: `path`, `reason`, `result`, `subscriptionRevision?`, `windowRevision?` | `query.call`, `query.subscribe`, live updates, control refresh | no | ts:445-456; proto:674-679; lib.rs:4782-4794, live_query.rs:1002-1029, control.rs:4268-4277, service_principal.rs:443-449 | keep | TS `trace`, `replicaScope` and `originCommandIds` are **never sent** by Rust. The service-principal variant omits `reason` |
| `query.progress` | `id`, `path`, `throughRevision` | `query.subscribe` with a matching `windowRevision` | no | ts:457-465; proto:680-685; live_query.rs:984-1000 | keep | TS also has `reason?`, `trace?`, `originCommandIds?`; never sent |
| `query.patch` | `id`, `path?`, `reason?`, `baseRevision`, `subscriptionRevision`, `inserted?`, `updated?`, `deleted?`, `order?`, `prepend?`, `append?`, `replicaScope?`, `windowRevision?`, `trace?`, `originCommandIds?` | live update | no | ts:466-483; proto:686-691 | defer: **never emitted by Rust**. Implement only when the kernel sends incremental live-query patches (stage 3, capability-flagged) | Client advertises `queryOrderDelta` |
| `query.pagePatch` | as `query.patch` plus `result?` | live update | no | ts:484-502; proto:692-697 | defer (never emitted) | Client cap `queryPagePatch` |
| `query.objectPatch` | `id`, `path?`, `reason?`, `baseRevision`, `subscriptionRevision`, `collections: Record<string, KeyedCollectionPatch>`, `replicaScope?`, `windowRevision?`, `trace?`, `originCommandIds?` | live update | no | ts:503-515, ts:293-300; proto:698-703 | defer (never emitted) | Client cap `queryObjectPatch` |
| `query.batch` | `messages: ServerMessage[]` | live updates | no | ts:396-399; proto:704-705 | defer (never emitted) | Client cap `queryResultBatch`. The SDK dispatches nested `query.fanout` and other messages (`sdk:2553-2575`) |
| `query.fanout` | `queryType`, `ids[]`, plus the union of query fields | live updates | no | ts:400-421; proto:706-713 | defer (never emitted) | Client cap `queryFanout` |
| `query.error` | `id`, `path?`, `error` | `query.call`, `query.subscribe` | no | ts:598; proto:714-720 | keep | |
| `control.watermark` | `id` | A successful control-scope reducer or action, after the control `query.result` refreshes | no | ts:599; proto:721-722; lib.rs:5082-5101 | keep | |
| `reducer.result` | `id`, `path?`, `result`, `originCommandId` (= `id`), `committedRevision?`, `trace?` | `reducer.call(Many)` | no | ts:600; proto:723-739; lib.rs:5066-5073 | keep | |
| `reducer.error` | `id`, `path?`, `error`, `class?: rejected\|transient\|update_required\|unauthenticated`, `retryable?`, `trace?` | `reducer.call(Many)` | no | ts:601-607, ts:393; proto:740-754, proto:100-147; lib.rs:5074-5078 | keep | `retryable` is true only for `transient` and `unauthenticated`. The service-principal path omits `class` |
| `action.result` | `id`, `path?`, `result`, `committedRevision?`, `trace?` | `action.call` | no | ts:608; proto:755-769; lib.rs:3488-3510 | keep | |
| `action.error` | `id`, `path?`, `error`, `trace?` | `action.call` | no | ts:609; proto:770-778 | keep | |
| `replica.transaction` | `cursor{epoch,revision}`, `originCommandId?`, `provenance?: PublicInvocationProvenance`, `changes: [{entity,id,operation,oldValue?,newValue?,changedColumns?}]` | feed | no | ts:422-428, ts:81-99; proto:779-791, proto:156-171, proto:633-645 | keep (shape); adapt (fed from the in-object change log) | Inside `replica.batch`, `oldValue` is always stripped (`delivery.rs:802-805`). `operation` is a TS union but a String in Rust |
| `replica.transactionEncoded` | `cursor`, `originCommandId?`, `provenance?`, `canonicalChanges: string[]` | feed (native only) | **yes** | proto:792-805; replica.rs:3258-3290 | defer (native) | |
| `replica.watermark` | `revision: number` | Bootstrap after the cohort's snapshots, per transaction (non-batch), after a call commit, after membership-changed | no | ts:565; proto:806-807; lib.rs:1860-1875, 2318-2322, 3059-3066, 4177-4248 | keep | Inside a batch it is the trailing `watermark` field (A.4). It is sent even without the client's `replicaWatermark` cap |
| `replica.snapshot` | `id`, `result: Row[]` (verbatim JSON), `cursor`, `key`, then alphabetical: `digest?`, `hashes?`, `maxBytes`, `maxRows`, `mode`, `orderBy`, `orderDirection`, `path`, `truncated` | `replica.open` when paging is not negotiated | no | ts:516-531; proto:808-817; replica.rs:2951-3015 | keep (shape); adapt (size, A.1.7) | `pageDigest`, `scope` and `visibilityScope` are stripped by `legacy_snapshot_frame`. `hashes`/`digest` are inline only below 2048 rows (`replica.rs:316,693`) |
| `replica.snapshotStart` | `snapshotId`, `id`, `cursor`, `key`, metadata (`digest`, `maxBytes`, `maxRows`, `mode`, `orderBy`, `orderDirection`, `path`, `scope`, `truncated`, `visibilityScope`) | native open | **yes** | proto:818-829; replica.rs:3082-3105 | defer (native) | |
| `replica.snapshotPage` | A.4 row 6 | `replica.open` with paging | no | ts:532-551; proto:830-843 | keep | |
| `replica.snapshotEncodedPage` | A.4 row 9 (binary) | native open | **yes** | proto:844-857 | defer (native) | |
| `replica.delta` | `id`, `cursor`, then alphabetical: `deleted`, `digest`, `originCommandIds` (always `[]`), `path`, `truncated`, `upserts` | feed, resume | no | ts:552-562; proto:858-864; replica.rs:3292-3313 | keep | TS `hashes?` is never sent in a delta. Native mode replaces `upserts` with `canonicalUpserts: string[]` (Rust-only, defer) |
| `replica.ready` | `id`, `cursor`, `digest`, then alphabetical: `mode`, `path`, `truncated` | After the snapshot's last page, a resume, or a feed change | no | ts:563, ts:252-259; proto:865-872; replica.rs:3315-3332 | keep | Held by the gates until the last page is sent (`snapshot_delivery.rs:329-335,355-359`) |
| `replica.readyMany` | `ready: ReplicaReady[]` | — | no | ts:564; proto:875-876 | defer: **never constructed** by the runtime (only filtered in `gate`, `snapshot_delivery.rs:171-187`) | Client cap `replicaReadyMany` is unused |
| `replica.closed` | `id` | `replica.close` (native only) | **yes** | proto:873-874; snapshot_delivery.rs:79-83 | defer (native) | |
| `replica.needHashes` | `id`, `path?` | `replica.open` resume needing per-row hashes, or a visibility resync | no | ts:582; proto:877-882; replica.rs:596-601, 1602-1605 | keep | |
| `replica.syncing` | `id`, `path?`, `reason` | — | no | ts:585-590; proto:883-889 | keep (never sent): **client-local only**; Rust never sends it | Do not emit |
| `replica.reset` | `id`, `path?`, `reason` | feed reset or lag | no | ts:591-596; proto:890-896; lib.rs:2367-2386, replica.rs:1340-1355 | keep | Rust sends only `reason:"change-feed-lagged"` and `"change-feed-reset"`. **Neither is in the TS `reason` union** (A.6) |
| `replica.error` | `id`, `path?`, `error` | open, feed apply failure, limits | no | ts:597; proto:897-903 | keep | |
| `replica.batch` | `tenant`, `class`, `from`, `items[]`, `watermark?` | feed (`replicaBatchFrames:1`) | no (hand-built) | ts:574-581, ts:631-639; delivery.rs:691-714 | adapt (A.4) | |
| `error.ack` | `id`; alphabetical: `accepted?`, `error?`, `fingerprints?` | `error.register`, `error.envelope`, `error.heartbeat` | no | ts:438; proto:904-909; telemetry.rs:553-571 | keep | |
| `support.command` | `id`, `result: JsonValue` (the whole command object; `id` = `command.id`) | Unsolicited, from a control-plane support action addressed to this `connection_id` | no | ts:439; proto:910-911; lib.rs:2509-2515, control.rs:210 | adapt: the control store routes by `connection_id` to the object holding the socket over RPC (needs a connection registry) | |
| `presence.snapshot` | `id`, `serverTime: i64 ms`, `members: [{memberId,state,since}]` (online only, sorted by `memberId`) | `presence.subscribe`, or a resync after 32 pending batches | no | ts:441, ts:309-318; proto:915-921; presence.rs:795-801 | adapt (S3) | It can ride in `replica.batch` |
| `presence.patch` | `serverTime`, `changes: PresenceMember[]` (`state` may be `offline`) | Coalesced transitions (250 ms window, leading edge) | no | ts:443; proto:923-928; presence.rs:569-602, 851 | adapt (S3) | |
| `presence.error` | `id`, `error` | `presence.subscribe` | no | ts:444; proto:929-930; lib.rs:3740-3749 | keep | |
| `client.updateRequired` | `reason: "CLIENT_UPDATE_REQUIRED"`, `contract: number` | auth, any frame, module reload | no | ts:610; proto:931-932 | keep (frame); adapt (trigger, A.1.5) | Followed by close |
| `system.reload` | `reason: "module generation changed"`, `artifactHash?` | Module reload | no | ts:611; proto:933-942; lib.rs:2497-2507 | adapt (A.1.5) | |
| `replica.rowPatch` | A.4 row 7 | feed (`replicaFieldPatches:1`, non-batch, non-native) | no (TS `ReplicaRowPatchFrame`, outside the `ServerMessage` union) | ts:613-629; protocol/src/row_patch.rs:309-477; lib.rs:2276-2292 | defer: unreachable for the SDK (batch frames on). Do not advertise `replicaFieldPatches` (0012 R20) | |

### A.4 Hand-built and binary frames

| Frame | Exact shape (byte order as written) | When sent | Negotiated by | Source | Disposition |
|---|---|---|---|---|---|
| 1. `replica.batch` envelope | `{"type":"replica.batch","tenant":<JSON string tenant_id>,"class":"<16 lowercase hex u64>","from":<u64 last watermark sent in a batch>,"items":[` + items joined by `,` + tail + `}`. The tail is `]`, or `],"watermark":<u64>` when a watermark was recorded (the maximum seen this batch). One text message per connection per tick. | Feed frames for an authenticated tenant socket. Flushed at the tick, idle or urgent deadline, at 512 KiB, or **before any other outbound frame** (ordering; `delivery.rs:677-689`, `ws/connection.rs:444-447`) | client `replicaBatchFrames:1` | delivery.rs:691-714, 985-1000; ws/shared.rs:302-327 | adapt: build the same string in the object holding the socket. `class` is informational: the SDK ignores it, so any stable visibility-class id works. `from` must be the last watermark sent |
| 2. `replica.stream` item | `{"type":"replica.stream","id":<JSON string subscription id>,"stream":"~<16hex>"}`. A collision on the same socket uses `"~<16hex>.<n>"`, n starting at 2. | Before the first delta or ready item for a subscription id in that socket's lifetime, or after its identity changes | as above | delivery.rs:734-780 | keep shape. The token is opaque to the SDK (`ts:646-673`). Rust derives it from `DefaultHasher(path, serde_json(args))` (`delivery.rs:519-533`); any per-socket-unique token works |
| 3. Delta item | serde `replica.delta` with `id` = stream token and `digest`/`truncated` removed, **closing `}` stripped**, then a per-connection tail `,"digest":<json>,"truncated":<bool>}` (only the keys present; delivery v4 always writes both). Field order: `type,id,cursor,deleted,originCommandIds,path,upserts` + tail. | Feed change to a subscription window | as above | delivery.rs:817-863, 173-178, 236-246; replica_fanout.rs:1171-1189 | adapt: the encode-once rows part is shared across sockets of one visibility class (brief design rule); the tail is per socket |
| 4. Ready item | serde `replica.ready` with `id` = token: `{"type":"replica.ready","id":tok,"cursor":{...},"digest":"...","mode":...,"path":...,"truncated":...}`. v4 splices the per-connection digest between cached prefix and suffix bytes split at `"digest":"`. | After each delta (window certificate) | as above | delivery.rs:864-889; replica_fanout.rs:476-486, 1190-1196 | keep shape |
| 5. Transaction item | serde `replica.transaction` with every `oldValue` removed: `{"type":"replica.transaction","cursor":{"epoch","revision"},"originCommandId"?,"provenance"?,"changes":[{"entity","id","operation","newValue"?,"changedColumns"?}]}`. One shared encoding per (tenant, revision), up to 16 content variants. | Committed transaction visible to the socket | as above | delivery.rs:796-815, 892-904, 162-210 | adapt: encode once per class (memo bounded at 8 MiB per tenant, `delivery.rs:35-39`; size it for 128 MB) |
| 6. Presence item | `presence.patch` or `presence.snapshot` JSON, appended to the open batch as data | Presence slot drained. Sent at once unless the batch already holds rows (leading edge) | `presence:1` plus batch frames | delivery.rs:969-983; lib.rs:2397-2423 | adapt (S3) |
| 7. `replica.rowPatch` | `{"type":"replica.rowPatch","sequence":<u64 per socket, from 1>,"message":<skeleton replica.transaction (object newValue removed) or replica.delta (upserts:[])>,"rows":[{"index","version"?,"value"?,"base"?,"set"?,"unset"?}],"evicted":[<u64>...]}` | Non-batch feed, when `replicaFieldPatches:1` and not native. Full or patch variant, whichever compresses smaller | `replicaFieldPatches:1` | protocol/src/row_patch.rs:185-477; lib.rs:2276-2292 | defer (SDK never reaches it) |
| 8. Compact transaction | `replica.transaction` with `oldValue` removed, except when keeping it crosses the compression threshold | Non-batch feed with `replicaNoOldValues:1` | `replicaNoOldValues:1` | protocol/src/replica_wire.rs:10-43; lib.rs:2294-2306 | defer (SDK uses batch) |
| 9. **Binary** `replica.snapshotEncodedPage` | WS **binary** opcode. The payload is the UTF-8 JSON of serde `{"type":"replica.snapshotEncodedPage","snapshotId","sequence","last","id","canonicalRows":[<string>...],"cursor","key",<metadata alphabetical; digest on last page>}`. **No header, no length prefix, no framing beyond WebSocket.** Page target 64 KiB; a frame over 128 KiB fails with `replica.error` "snapshot row exceeds native frame budget". Preceded once by the text `replica.snapshotStart` (at most 16 KiB). Credit: 8 frames and 512 KiB in flight per socket, released only by `replica.snapshotAck`. At most 256 subscriptions. | Native open | `replicaNativeRows:1 && replicaSetDigest:1 && replicaSnapshotPages:1` | snapshot_delivery.rs:8-19, 85-119, 201-292; lib.rs:5321-5352; replica.rs:3107-3123, 3184-3215 | defer: SDK never negotiates it. Do not advertise `replicaNativeRows` |
| 10. Legacy pages (text) | `{"type":"replica.snapshotPage","snapshotId":<uuid v4>,"sequence":<0..>,"last":<bool>,"id","result":[<rows verbatim SQLite JSON text>],"cursor","key",<alphabetical: digest (last page only; the pageDigest if any, else digest), maxBytes, maxRows, mode, orderBy, orderDirection, path, scope, truncated, visibilityScope>}`. `hashes` dropped. At most 256 KiB or 512 rows per page. One page per socket-loop turn, round-robin across subscriptions. The matching `replica.ready` is held until the last page. Feed events are deferred while pages are pending (`lib.rs:1905`). | `replica.open` with `replicaSnapshotPages:1` | `replicaSnapshotPages:1` | snapshot_delivery.rs:295-362; replica.rs:3017-3146, 3184-3222; lib.rs:2012-2016, 2022-2028 | keep: the page loop yields between pages (brief: page and yield). Rows are spliced as SQLite JSON text without re-parse |
| 11. Presence patch merge | Several pending patches merged per Member (last wins, first-seen order), `serverTime` of the last one | Presence drain | `presence:1` | presence.rs:569-602 | adapt (S3) |

### A.5 Error codes and strings on the wire

Disposition for every row: **keep** the exact strings and codes unless the row names a ruling. New in the kernel: `reducer.error{class:"rejected", retryable:false, error:"Reducer receipt expired"}` for a call that hits an expired receipt (0009).

| Frame and field | Value | Emitted when | Source |
|---|---|---|---|
| `auth.error.code` | `transient` | Store unavailable, missing project or token, public unavailable, scope fixed, rate limit, session-expired, membership-changed, service failures, other database errors | lib.rs:4453-4462 and the sites above |
| `auth.error.code` | `credential_rejected` | `InvalidSession`, `SessionProjectMismatch` | lib.rs:4456-4458 |
| `auth.error.code` | `member_removed` | `MemberNotFound`, `NoTenantMembership` | lib.rs:4459 |
| `auth.error.code` | `revoked`, `account_disabled` | **Never emitted by Rust**; the SDK treats both as definitive (sdk:2481, 2491-2492) | — |
| `auth.error.id` (unsolicited) | `session-expired`, `membership-changed`, `public-rate-limit` | A.1.4, A.2 | lib.rs:2077, 4263, 4406 |
| `auth.error.error` | `auth session store is unavailable`; `public tenant session is unavailable`; `project is required for public Control Plane calls`; `a Gonvex app session is required`; `developer grants require an active tenant`; `impersonation grant is invalid, expired, revoked, or already used: <e>`; `public authentication requires the public WebSocket transport`; `public connection scope is fixed`; `authentication expired or was revoked`; `tenant membership changed; authenticate again`; `public call rate limit exceeded`; `service credential is invalid or not permitted for this project`; `tenant is not available`; or the Display of a database error | | lib.rs:4464-4780, 2631, 2840, 2076, 4264, 4350; service_principal.rs:54, 288-312 |
| `reducer.error.class` / `retryable` | `rejected`/false, `transient`/true, `update_required`/false, `unauthenticated`/true | Classification | proto:100-147; error_class.rs:117-151 |
| `reducer.error.error` prefixes the SDK parses | `STALE_REDUCER_ARTIFACT: expected "<h>", active artifact is "<h>"`; `CLIENT_UPDATE_REQUIRED: expected contract <n>, active contract is <n>`; also `STALE_AGENT_CATALOG: ...` | Stale client | execution.rs:55-60; sdk:5992 |
| Other execution errors | `project "<p>" has no active TypeScript module`; `function "<p>" is not registered`; `function "<p>" is a <k>, not a <k>`; `internal function "<p>" cannot be called by a client`; `module database capability failed: <e>`; `Action "<p>" requires a capability the Rust runtime has not configured: <c>`; `function "<p>" is not classified as interactive`; `function "<p>" arguments do not match its schema: <m>`; `... result does not match its schema: <m>`; `nested function invocation exceeded the maximum depth`; `recursive Action invocation detected for "<p>"` | | execution.rs:31-76 |
| Control errors | `unknown Control Plane function ...`, `authentication is required` (class `unauthenticated`), `tenant administrator access is required`, `developer access is required`, `project administrator access is required`, `project administration is unavailable during impersonation`, `invalid arguments: ...`, ... | | control.rs:101-134 |
| Not-authenticated strings | `authenticate with an active tenant before calling a Query` / `subscribing to a Live Query` / `calling a Reducer` (class `unauthenticated`) / `calling an Action` / `opening a Replica Collection` | No tenant session | lib.rs:3030, 3132, 5045, 3522, 5144 |
| Batch limits | `query batch cannot contain more than 256 subscribes`; `reducer batch cannot contain more than 256 calls`; `replica batch cannot contain more than 256 opens`; `individual replica open request is too large`; `batched frames are not available to service principals` | | lib.rs:3160, 3356, 3595, 5149; service_principal.rs:476 |
| Replica errors | `sum128-v1 replica integrity was not negotiated`; `replica arguments must be an object`; `replica row is missing key "<k>"`; `replica returned duplicate key "<k>"`; `replica SQL identifier "<i>" is invalid`; `visibility plan required for table "<t>"`; `replica function "<p>" is not registered`; `project has no active TypeScript module`; `targeted replica fetch advanced beyond its resolved principal`; `legacy snapshot subscription limit exceeded`; native-only: `native snapshot acknowledgements were not negotiated`, `snapshot acknowledgement exceeds sent sequence`, `native snapshot header encoding failed`, `native snapshot metadata limit exceeded`, `native snapshot subscription limit exceeded`, `snapshot iterator ended without final frame`, `snapshot encoding failed`, `snapshot row exceeds native frame budget` | | replica.rs:57-86; snapshot_delivery.rs:90-99, 133, 236-246, 323; lib.rs:3623 |
| `replica.reset.reason` | `change-feed-lagged`, `change-feed-reset` | Broadcast lag, feed reset | lib.rs:2375; replica.rs:1353 |
| Live-query errors | `structured query arguments must be an object`; `visibility plan required for table "<t>"`; `invalid structured query: <m>` | | live_query.rs:352-365 |
| `presence.error.error` | `presence requires an authenticated tenant member session`; `presence v2 is disabled` | | lib.rs:3743-3746 |
| `error.ack.error` | `error telemetry is unavailable`; `error telemetry rate limit exceeded`; `error store unavailable`; `invalid error envelope`; `error envelope is too large` | | telemetry.rs:150-288 |
| Service principal | `service principals cannot subscribe`; `service principals cannot open replicas`; `service principals cannot call Control Plane Actions` / `Queries`; `function "<p>" is not allowed for this service principal` | | service_principal.rs:395-505 |
| `client.updateRequired.reason` / `system.reload.reason` | `CLIENT_UPDATE_REQUIRED` / `module generation changed` | | lib.rs:2492, 2504 |

Disposition: **keep** for the codes, `class` values, the unsolicited ids, the parsed prefixes (`STALE_REDUCER_ARTIFACT`, `CLIENT_UPDATE_REQUIRED`) and the reset reasons. Free-text errors that embed database or driver Display output (`DatabaseError`, sqlx) are **adapt**: keep the fixed strings, but D1/DO SQLite driver text will differ. Conformance scenarios should match exact strings only for the rows above that are not `<e>` placeholders.

#### Additions from verification

- **`query.result.reason` values outside the TS union:** Rust also emits `"reconnect"` (`live_query.rs:740`), `"control-change"` and `"presence-change"` (`lib.rs:2457, 2472, 3306, 3426, 3548`). Keep: the kernel sends the same strings.
- **Public admission recheck failure:** each blocked frame gets an error with text `public tenant session is unavailable` (`public_admission_errors`), then the socket closes with no close frame (`lib.rs:2689-2709, 3908-3925`). Keep.
- **Replica admission overload:** `replica snapshot admission overloaded (admission deadline|too many waiting builds); retry` (`offload.rs:843, 1086, 1138`; `replica.rs:86`) as `replica.error`. Keep the text; the trigger is a tuning detail.

### A.6 Rust vs TS differences

Disposition for every item: the kernel matches **Rust's** behavior (what Rust sends and accepts), not the TS union, unless an item names a ruling (0012 R18-R20).

1. **Rust-only client message:** `replica.snapshotAck` (proto:450-457).
2. **Rust-only server messages:** `replica.transactionEncoded`, `replica.snapshotStart`, `replica.snapshotEncodedPage` (binary), `replica.closed`, and the `canonicalUpserts` delta variant (proto:792-874; replica.rs:3225-3290).
3. **Rust-only capabilities:** client and server `replicaSetDigest` and `replicaNativeRows` (proto:249-254, 576-581). The server advertises both to every socket (lib.rs:1768-1769).
4. **Client capabilities the server ignores** (present in both TS and the Rust `ClientCapabilities` struct, proto:263-282): `replicaReadyMany`, `replicaWatermark` (the watermark is sent anyway), `queryPagePatch`, `queryObjectPatch`, `queryOrderDelta`, `queryFanout`, `queryResultBatch`. These names exist in the Rust struct but are never read in `lib.rs`.
5. **TS server messages Rust never emits:** `query.patch`, `query.pagePatch`, `query.objectPatch`, `query.batch`, `query.fanout`, `replica.readyMany`, `replica.syncing` (client-local by design). Defined but never sent: `replicaScope` and `trace` on query frames, `originCommandIds` on query frames, `hashes` on `replica.delta`. `replica.delta.originCommandIds` is always `[]` (replica.rs:3307). Server timing fields in `MessageTrace` are never filled; `trace` is echoed.
6. **Enum mismatches:** `replica.reset.reason` from Rust (`change-feed-lagged`, `change-feed-reset`) is outside the TS union (`ts:595`). `auth.error.code` in TS is free `string`; Rust emits three values and the SDK also handles two that Rust never sends.
7. **Typing strictness (it closes the socket):** Rust closes **1003** on any frame serde rejects, while TS types are compile-time only. Rust is stricter in these places:
   - Integer-only `u32` for `device.viewportWidth`, `viewportHeight`, `hardwareConcurrency` and `touchPoints`.
   - `u64` for `clientContract`, `cursor.revision` and `snapshotAck.sequence`.
   - Unknown `type` closes.
   - A required field that is missing closes: `id`, `path`, `events`, `calls`, `opens`, `subscribes`, `clientReceivedAtMs`, `kind`, `outcome`, `state`.

   Rust is looser in these places:
   - `args` may be omitted (default null).
   - `telemetry.kind`, `outcome` and `reason` and `presence.set.state` are free strings (`gone` is accepted).
   - `ReplicaChange.operation` is a free string.
   - Unknown fields are ignored.

   Cloudflare must reproduce this exact accept/close boundary (one decoder with conformance vectors from `proto` tests, proto:945-1188).
8. **Shape wrappers:** Rust `query.*`, `replica.delta`, `replica.ready`, `replica.snapshot*` and `error.ack` carry flattened `BTreeMap` payloads. Optional TS fields appear only when the runtime inserted them, so key order is alphabetical after the fixed fields. JSON consumers do not care; byte-identical conformance must.
9. **`session.ready`:** TS documents `project`, `tenant` and `replica`; Rust sends none of them. The replica directive travels in `auth.result.result.replica`.
10. **`auth.result.result`** is `JsonValue` in TS. Its de-facto schema (A.1.4) is only in Rust and in SDK parsers (`replicaDirectiveFromAuthResult`, `localIdentityFromAuthResult`, `artifactHashFromAuthResult`, `developerSessionTokenFromAuthResult`). It belongs in the contract.
11. **`replica.rowPatch`** is a TS type (`ReplicaRowPatchFrame`) outside the `ServerMessage` union. Rust builds it from private structs (row_patch.rs:185-260), not from the protocol enum.
12. **`replica.batch` / `replica.stream`** exist in TS as types (ts:574-581, 631-639) but in Rust only as hand-written byte strings (delivery.rs:701-708, 774-777). No serde round-trip test covers them in `proto`.
13. **`ReplicaOpenRequest.keys`** is in both, and is read by neither the runtime nor (for routing) the SDK.
14. **`PROTOCOL_VERSION = 2`** (proto:91). The doc comment says "0.5.0 wire contract", the enum comments say "@gonvex/client@0.4.1", and `ReplicaDirective.protocolVersion` is 1. These are three different version numbers on the wire.

## B. Module host-call interface, capabilities and determinism

Reference: Rust runtime at `0bf3ad13`. Paths are abbreviated:
`MR` = `rust/crates/module-runtime/src`, `V8` = `rust/crates/module-runtime-v8/src`, `BS` = `V8/gonvex_bootstrap.js`,
`RT` = `rust/crates/runtime/src`, `MH` = `rust/crates/module-host/src`, `ST` = `rust/crates/store/src`,
`SDK` = `packages/module-sdk/src`, `RC` = `packages/module-sdk/runtime-contract`, `LR` = `packages/local-runtime/src`.

Boundary layers, in order: module JS -> `ctx.*` object built by `BS` -> one op `op_gonvex_host_call(JSON)` -> `HostCallRequest` (`V8/dispatch.rs:103`) -> ABI `HostCall` (`MR/lib.rs:349`) -> capability re-check in the op (`V8/isolate.rs:647`) -> `HostCallFrame` (`MH/protocol.rs:489`, same names, used in-process too) -> `HostCallHandler` (`RT/module_host.rs:107`): `DatabaseHostCalls` for Query/Reducer (`RT/host_calls.rs:110`), `ActionHostCalls` for Action (`RT/action_calls.rs:212`). Op strings below are the JSON `kind` tag (camelCase) used on every layer.

On Cloudflare the JSON op boundary is no longer needed. The contract to keep is the `ctx.*` surface, the op names, argument and result shapes, error envelopes and the capability gating. "Direct call" below means the kernel implements the op as a JS function in the same isolate. The function names the Cloudflare mechanism.

---

### B.1 Function kinds and capability sets

Disposition for every row and note in this subsection: **keep** (kinds, flags and the capability matrix are module-visible).

The ABI has exactly three kinds: `query | reducer | action` (`MR/lib.rs:41-45`, `MH/protocol.rs:323-330`). Everything else is a flag or a channel on those three kinds:

| Variant | How it is expressed | Source |
|---|---|---|
| internal | `FunctionContract.internal`; callable only via an Action tool binding (`allow_internal`) | `MR/lib.rs:51`, `RT/action_calls.rs:331,353` |
| internalReducer | reducer + `internal:true, interactive:false, offline:forbidden`; tool binding kind `internalReducer` | `SDK/index.ts:1552-1559`, `RT/action_calls.rs:344` |
| public (anonymous) | `FunctionDefinition.public`; anon session disables outbox/scheduler, 60 s Action deadline | `RT/modules.rs:26`, `RT/execution.rs:585-586,859-869`, `RT/public_functions.rs:33` |
| interactive / local reducer | reducer with `offline:allowed`, no optimistic: SDK wraps handler with `reducerExecutionContext` | `SDK/index.ts:1533-1540` |
| delivery | query `oneShot`/`live`/`replica` | `SDK/index.ts:449`, `MR/lib.rs:53` |
| action profile | `standard` / `agent` (agent = no execution ceiling, may hold `functions`, `sandbox`) | `V8/lib.rs:740-749`, `RT/action_calls.rs:237-241` |
| cron / scheduled | CronSpec must target reducer or action; runs with channel `scheduler`, lane Scheduled, 3 s slice for Q/R at depth 0 | `RT/modules.rs:212-228`, `V8/admission.rs:34-47`, `V8/lib.rs:718-732` |
| tool / nested | depth +1, channel `agent`, max depth 8 | `RT/action_calls.rs:306-321`, `MR/lib.rs:18` |
| http/webhook | **No such kind.** HTTP callers invoke ordinary functions with channel `api`. | `MR/lib.rs:144-151` |

Effective capability = structural(kind) AND host grant (`MR/capabilities.rs:9-66`). Host grants are computed in `RT/execution.rs:1530-1533` (db), `:585-588` (reducer) and `:898-906` (action).

| Capability (host-call name) | Query | Reducer | Action | Host grant condition |
|---|---|---|---|---|
| db_read (`dbQuery`, `permissionsHas`) | Y | Y | **N** (no db handle) | always for Q/R |
| db_write (`dbInsert/Update/Delete/DeleteMany`) | N | Y | N | reducer |
| action_outbox (`actionEnqueue`) | N | Y | N | `!anonymous` |
| scheduler (`scheduleAfter/At`) | N | Y | Y | R: `!anonymous`. A: declared `scheduler:true` and `!anonymous` (`RT/action_calls.rs:242-244`) |
| storage (`storage`) | N | Y (`getMetadata` only) | Y (all ops) | R: `public \|\| internal \|\| !interactive` (`RT/execution.rs:587-588`). A: declared `storage:true` |
| action_tools (`toolInvoke`) | N | N | Y | declared `tools` non-empty |
| network (`fetch/fetchRead/fetchCancel`) | N | N | Y | declared `networkOrigins` non-empty |
| sandbox (`sandbox`) | N | N | Y | declared `sandbox` (agent only per SDK `SDK/index.ts:374`) |
| secrets (no host call; `ctx.secrets`) | N | N | Y | declared names resolved, non-empty (`RT/execution.rs:903`) |
| functions (`functionInvoke`) | N | N | Y | declared `functions:true` AND profile `agent`. Declaring `functions:true` on a non-agent profile is not a silent deny: every invocation of that Action fails with "interactive function invocation requires an agent Action profile" (`RT/action_calls.rs:237-241`) |
| presence / ai | **none**: there is no presence or AI host call; AI goes through `fetch`, and presence is protocol-level | — |

Kind notes:
- A Query gets `ctx.permissions` from the bootstrap, because it is db_read (`BS:610-620`). The host then refuses it because only Reducers carry `storage_context` (`RT/host_calls.rs:865-867`). Effectively the capability is Reducer-only.
- A Reducer's `ctx.storage` has only `getMetadata` (`BS:696`). The host answers only `getMetadata` (`RT/host_calls.rs:766-781`).
- `secrets` has a capability bit but no `HostCall` maps to it. `check_capability` has no `secrets` arm (`MR/lib.rs:465-478`).

Disposition: **keep** the matrix, the intersection rule and the "method absent when not granted" shape. Query and Reducer run in the tenant DO. Standard Actions run in the Action Worker. Agent Actions run in a Workflow step. The sandbox runs in a Container.

---

### B.2 Host calls (exhaustive)

#### B.2a `op_gonvex_host_call` operations (async, ordered except `fetch`)

The JS error envelope for every op is `{status:"ok",value}` / `{status:"denied",message}` / `{status:"failed",message}` (`V8/dispatch.rs:308-352`). It surfaces as `GonvexHostError` (B.5).

| Op (`kind`) | Args (JSON) | Result | Kinds | Limits / caps | Errors (text) | Source | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| `dbQuery` | `statement:string` (non-empty), `parameters?:JsonValue[]` bound `$1..$n`; arrays/objects bound as JSON text | `Row[]` (JSON objects, decoded by declared type; expression TEXT that looks like JSON is parsed) | Q, R | One statement. No BEGIN/COMMIT/END/ABORT/ROLLBACK(without TO)/START/PREPARE TRANSACTION, ATTACH/DETACH/PRAGMA/VACUUM. Query head must be `select/with/values/explain`. **No row cap** (only the 8 MiB function-result cap and the heap). | "a database statement may not contain more than one statement"; "transaction boundaries belong to the invocation…"; "a Query may only read; …"; "query parameters must be an array"; "parameter $n: …"; aborted-tx text (B.7) | `BS:566-571`, `V8/dispatch.rs:106-110`, `RT/host_calls.rs:1011-1082`, `RT/sql_admission.rs:170-213`, `RT/host_calls.rs:1715-1727` | adapt: direct call into `ctx.storage.sql.exec` inside the Reducer's storage transaction (S2). Queries read the DO SQLite directly. | Module `SAVEPOINT x` / `RELEASE` / `ROLLBACK TO` are **admitted** and used by the SDK (`SDK/data-write.ts:332-340`). DO forbids raw SAVEPOINT in `sql.exec`, so they must be mapped onto the storage transaction API: **contract-change note**. DO: 100 KB statement / 100 bound params apply to module SQL. Return a clear error and never silently split. |
| `dbInsert` | `table`, `row:object`, `generatedId?:string\|null` | inserted row (`RETURNING *`, re-read through the base table for record views) | R | identifiers `^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, optional `schema.table`. Omitted text PK: `generatedId`, else **`Uuid::new_v4`**. Non-text PK omitted and not DB-generated: error. Composite PK allowed on insert. | "insert into … requires primary key …"; "identifier … is not a table or column name"; "a Query cannot modify application tables" | `BS:586-591`, `RT/host_calls.rs:1084-1143,1404-1427,1691-1713` | adapt: direct `sql.exec` in tx | One bound param per column, so >100 columns breaks on DO (DO caps tables at 100 columns anyway). The column catalog comes from `pragma_table_info($1,…)` and `sqlite_schema` (`:1257-1286`). Verify DO allows the `pragma_table_info` table-valued function, or use the manifest schema. |
| `dbUpdate` | `table`, `id:string\|number`, `patch:object` (non-empty), `key?` (wire only; JS never sends it, so host default) | updated row or `null` | R | key: declared PK, else catalog PK, else `id`. Composite PK: error. | "an update requires at least one column"; "table … declares primary key …, not …"; "… composite primary key …"; "a row id must be a string or number" | `BS:592-597`, `V8/dispatch.rs:118-126,209`, `RT/host_calls.rs:1145-1177,1217-1247,1647-1679` | adapt: direct `sql.exec` | cols+1 params: a 100-column patch exceeds DO's 100-param limit |
| `dbDelete` | `table`, `id`, `key?` | `{deleted:n}` | R | as update | as update | `BS:598-602`, `RT/host_calls.rs:1179-1188` | adapt: direct `sql.exec` | SDK type says `Promise<void>`, but the host returns `{deleted}` (`SDK/index.ts:227`) |
| `dbDeleteMany` | `table`, `ids:(string\|number)[]`, `key?` | `{deleted:n}` | R | **≤10,000 ids/call**; one JSON param via `json_each($1)` | "deleteMany cannot delete more than 10000 rows per call"; "deleteMany ids must be an array" | `BS:603-607`, `RT/host_calls.rs:1190-1214` | adapt: direct `sql.exec` | 10k-id JSON text must stay below DO's 2 MB value cap |
| `permissionsHas` | `permission:string` (non-empty), `scope?:string` (default `""` = tenant-wide) | `boolean` | R (Q refused by host) | Waits until the bits cover the tx's `_gonvex_sync_clock.revision`. | "ctx.permissions is available to Reducers with a tenant session"; "permission bits are rebuilding; retry" | `BS:613-619`, `RT/host_calls.rs:845-847,864-887` | adapt: in-memory permission index in the tenant DO | Answers from committed bits, not this Reducer's own uncommitted grant rows (`SDK/index.ts:274-276`) |
| `actionEnqueue` | `function:string` (non-empty), `args?` (`undefined` -> `null`) | `string` action id | R | rejects `_gonvex.scheduler.enqueue`; opens the lazy writer | "actions.enqueue requires an Action path"; "actions.enqueue cannot invoke a Gonvex system operation"; aborted-tx | `BS:622-626`, `RT/host_calls.rs:782-815`, `ST/lib.rs:1659` | **replace** (IB2, 0001): outbox row in the same DO tx; a DO alarm dispatches it to the Action Worker; at most 8 attempts, then `dead` (today: exponential backoff 1 s to 15 min, forever, `ST/lib.rs:1343-1382`) | id = deterministic intent id (B.4) when commandId is present, else UUIDv4. Outbox hops are bounded by `MAX_DURABLE_HOPS=32` (`MR/lib.rs:25`, `RT/execution.rs:1200`). |
| `toolInvoke` | `tool:string` (declared name), `args?` | the tool's Query/Reducer result | A | tool must be in `request.actionTools` (checked in isolate) and in the declared map. depth < 8. | "Action tool … is not declared for this invocation" (denied, sticky); "nested function invocation exceeded the maximum depth"; "Action tool … has unsupported kind …" | `BS:628-634`, `V8/isolate.rs:676-684`, `RT/action_calls.rs:306-371` | adapt: Action Worker -> tenant DO RPC (`query`/`reducer`). Each nested Reducer runs as its own DO transaction. | new commandId `agent-tool-<uuidv4>`, channel `agent`, `onBehalfOfMemberId` = session member |
| `functionInvoke` | `path`, `args?`, `artifactHash` (all required strings) | function result | A (agent profile + `functions:true`) | target must be interactive and active. The artifact hash must match the active one. | "interactive function invocation is not declared for this Action"; `STALE_AGENT_CATALOG: expected artifact …` (`RT/execution.rs:55-56`) | `BS:635-647`, `RT/action_calls.rs:284-304,525-532`, `RT/execution.rs:995` | adapt: Workflow step / Action Worker -> DO RPC | JS sets `code="STALE_AGENT_CATALOG"` when the message contains it (`BS:442`) |
| `scheduleAfter` | `delayMs:safe int ≥0`, `function`, `args?` | `"job_<id>"` | R, A | target must be a registered reducer/action of this module | "scheduled function … is not a registered Reducer or Action"; "scheduler delayMs is outside the supported range" | `BS:650-655`, `RT/host_calls.rs:816-828,889-927`, `RT/action_calls.rs:559-576` | adapt. R: outbox row `_gonvex.scheduler.enqueue` with payload `{jobId,function,args,runAtUnixMs,coalesce}` in the same tx. A: direct enqueue (`RT/scheduler.rs:792`). A DO alarm fires the earliest due job. | **runAt = host `Utc::now()` + delay at call time, not `ctx.now`** (`RT/host_calls.rs:824`). Local runtime uses `execution.now + delay` (`LR/portable.ts:481-482`). Divergence: keep the server behavior and flag it. |
| `scheduleAt` | `atUnixMs:safe int ≥0`, `function`, `args?`, `coalesce?:bool` | `"job_<id>"` | R, A | as above | "scheduler atUnixMs is outside the supported range" | `BS:656-661`, `RT/host_calls.rs:829-844`, `RT/action_calls.rs:577-599` | adapt: as above | |
| (`wakeAt`) | = `scheduleAt` with `coalesce:true` | `"job_<id>"` | R, A | at most one unclaimed wake per (tenant, function, args); a second wake only moves it earlier | — | `BS:662-668`, `SDK/index.ts:240-247` | adapt: coalesce on the DO's job table + alarm | local runtime treats `wakeAt` as `runAt` (`LR/portable.ts:490-493`) |
| `fetch` | `request:{url, method(upper, default GET), headers:{lower:str}, body:string\|null}`. JS: object body -> JSON + content-type. | `{status,statusText,url,headers,body:string}` or `{…,stream:u64}` for `text/event-stream` | A | absolute http(s) only. Origin must be in `networkOrigins` (checked in isolate **and** host). Redirects followed only to declared origins. **≤16 concurrent**. Response ≤8 MiB (content-length and body). connect 30 s. Total timeout = Action deadline remainder (none for agent). | "fetch origin is not declared for this Action" (denied, sticky); "fetch only supports absolute http and https URLs"; "fetch response exceeds the 8388608 byte limit"; "fetch … timed out within the … ms Action request budget"; "Action concurrent network request limit exceeded"; "fetch cannot start after the Action execution deadline" | `BS:671-689,523-543,513-521`, `V8/isolate.rs:654-675,685`, `V8/lib.rs:649-655`, `RT/action_calls.rs:22,159-184,373-490` | adapt: Action Worker global `fetch` with the same origin allowlist, `redirect:"manual"` loop and byte cap. Agent: Workflow step. | Body decoded with **`String::from_utf8_lossy`**, so binary responses are corrupted (`RT/action_calls.rs:488`). Keep that for parity (0012 R24). Denial texts: `fetch origin "<o>" is not declared for this Action` (`RT/action_calls.rs:401-404`), `redirect origin is not declared for this Action` (`:424`). Keep both. Mind Workers subrequest limits. |
| `fetchRead` | `stream:u64` | `{chunk:string,done:false}` or `{done:true}` (unknown id -> done) | A | cumulative 8 MiB. UTF-8 split chars held back. | network-not-declared; size; timeout | `BS:498-511`, `RT/action_calls.rs:80-148,542-544` | adapt: ReadableStream reader on the Worker `fetch` body | serial in the isolate, concurrent in the host handler (`RT/action_calls.rs:507-509`) |
| `fetchCancel` | `stream:u64` | `null` | A | — | — | `BS:509`, `RT/action_calls.rs:545-548` | adapt: `body.cancel()` | errors are swallowed in JS |
| `storage` op `getMetadata` | `{fileId}` | `FileMetadata` (`id,tenantId,ownerId,bucket,objectKey,contentType,sizeBytes,checksum,visibility,status,createdAt,uploadedAt`) | R, A | **Finalizes pending uploads by HEAD on object storage, inside the Reducer's write tx** | "fileId is required"; "file not found"; "storage metadata is unavailable" | `BS:695`, `RT/host_calls.rs:766-781`, `RT/storage.rs:244-248,629-678` | **replace** (IB1, 0012 R12): `_gonvex_files` in the DO, R2 `head()`, owner check on private files | network I/O inside a Reducer tx. No owner check today. **Intended: owner check.** |
| `storage` op `generateUploadUrl` | `{contentType?,size?,visibility?(private\|tenant\|public),ownerId?,expiresMs?}` | `{fileId,url,method:"PUT"\|"POST",headers}` | A | size ≤128 MiB. Default expiry 15 min. Anonymous: visibility forced `tenant`, owner `""`, needs `GONVEX_PUBLIC_URL`. | "upload too large"; "public uploads require GONVEX_PUBLIC_URL…" | `BS:698`, `RT/storage.rs:26-28,151-218,926-943` | **replace** (IB1): R2 presigned PUT or Worker proxy; `ownerId` must be the caller unless a system principal | caller may set any `ownerId` (`RT/storage.rs:934-943`). File id = `uuidv4.simple()`. |
| `storage` op `getUrl` | `{fileId}` | `string` URL (public: object URL; else presigned 10 min or proxy) | A | — | file not found | `BS:699`, `RT/storage.rs:219-231` | **replace** (IB1): R2, owner check for private files. | no owner check today |
| `storage` op `generateDownloadUrl` | `{fileId, ttlMs (0 = default 10 min)}` | `string` URL | A | ttl ms -> s via `div_ceil`. **No max TTL.** | file not found | `BS:700`, `RT/storage.rs:232-243,945-953` | **replace** (IB1): R2, owner check for private files. | |
| `storage` op `delete` | `{fileId}` | `null` | A | — | file not found | `BS:701`, `RT/storage.rs:249-270` | **replace** (IB1): R2 delete plus a row delete in the DO, owner check for private files. | |
| `storage` op `store` | `{contentBase64 (non-empty), contentType?, visibility?, ownerId?}` | `FileMetadata` | A | decoded ≤128 MiB | "storage content is not valid base64"; "storage content exceeds the 134217728 byte limit" | `BS:703`, `RT/storage.rs:271-319` | **replace** (IB1 for `ownerId`; 0012 R26 for size): R2 put | base64 in a JSON string conflicts with the 128 MB isolate on CF (a 128 MiB file is ~171 MiB of base64). Ruled in 0012 R26: the kernel caps `store` at 32 MiB decoded; larger files use `generateUploadUrl`. |
| `storage` op `call(operation,payload)` | any of the above by name | as above | A | unknown -> "unknown storage operation …" | — | `BS:704`, `RT/storage.rs:320` | keep (alias) | |
| `sandbox` op `create` | `{ttlMs?}` (exact fields) | `SandboxHandle {sandboxId,expiresAt,duckdb}` | A | ttl default 30 min, max 2 h. ≤128 total, ≤4 per account. Disabled by default. | "sandbox is disabled by runtime policy"; "sandbox ttl exceeds runtime policy"; "runtime sandbox limit reached" | `BS:711`, `RT/sandbox.rs:194-240`, `RT/config.rs:132-158` | adapt: Container (per sandbox id, scoped to project+tenant+account) | |
| `sandbox` ops `destroy`/`run`/`cancel`/`status`/`readFile`/`readText`/`writeFile`/`writeText` | `destroy {sandboxId}`; `run {sandboxId,code,timeoutMs?}`; `cancel/status {sandboxId,executionId}`; `read* {sandboxId,path}`; `writeFile {sandboxId,path,contentBase64}`; `writeText {…,content}` | see `SDK/index.ts:340-358` | A | run timeout default 30 s, max 120 s. code ≤512 KiB. file ≤64 MiB. workspace ≤256 MiB. output ≤8 MiB. rows 500. heap 64 MiB. concurrency 2. ≤16 executions. Paths relative, no `..`. | "sandbox was not found"; "sandbox path must be relative…"; "sandbox payload contains unsupported field …" | `BS:712-719`, `RT/sandbox.rs:206-214,276,303,486,502,515,539,689-752` | adapt: Container | |
| `sandbox` op `importFile` | `{sandboxId,fileId,filename}` | `{alias,tables:[{tableName,rowCount,columns}]}` | A with `sandbox.duckdb` **and** `storage` | file ≤`max_file_bytes` (64 MiB) | "sandbox importFile also requires the storage capability"; "…requires capabilities.sandbox.duckdb"; "file not found" (private file whose owner ≠ caller) | `BS:720`, `RT/action_calls.rs:612-616`, `RT/sandbox.rs:649-686`, `RT/storage.rs:324-360` | adapt: R2 get -> Container | **This is the only place a private-file owner check exists today** (`RT/storage.rs:341`) |
| `sandbox` ops `worker.readText/writeText/listFiles/query/register` | internal to sandbox-worker | — | sandbox-worker only | — | the Action host rejects them: "unsupported sandbox operation" | `BS:724-730`, `RT/sandbox.rs:218` | adapt: Container-internal only; never routed from an Action | |

Shared mechanics:
- Capability re-check plus sticky violation: a denial fails the invocation even if JS catches it (`V8/isolate.rs:647-653,1326-1338`).
- Ordering: every op except `fetch` is serialized per invocation. Database ops may be answered inline on the isolate thread once the tx is open (`V8/isolate.rs:685-701`, `RT/host_calls.rs:673-723`).
- Wire host-call failure text: `"{capability} host call failed [host_call_failed]: {message}"`. JS sees it with one more prefix: `host call failed: {capability} host call failed [host_call_failed]: {message}`, because `HostError::Failed` adds its own (`MR/lib.rs:441-442`) (`MH/session.rs:158-161`, `RT/in_process.rs:321-323`, `RT/host_calls.rs:715-720`). Keep this text: modules and tests match on it.
- Non-JSON or non-UTF-8 responses become `failed` (`V8/isolate.rs:743-751`, `BS:455-459`).

#### B.2b Synchronous ops (not capability-gated, no host round trip)

| Op | Args -> result | Kinds | Limits / errors | Source | Disposition |
|---|---|---|---|---|---|
| `op_gonvex_random_bytes` | `len` -> JSON byte array | **A only** | Q/R: `InvalidAccessError: secure entropy is available only in Actions; use intentEntropy in Reducers`. len clamped to 65,536. | `V8/isolate.rs:753-772` | replace: `crypto.getRandomValues` of workerd, wrapped by the per-kind guard |
| `op_gonvex_crypto_import` / `_key_info` / `_operation` / `_digest` | WebCrypto: HMAC SHA-1/256, AES-GCM 128/256 (96-bit IV, 128-bit tag), RSASSA-PKCS1-v1_5 SHA-256, Ed25519. Digests SHA-1/256/384/512. | all | DOMException names via the `Name: msg` prefix (`BS:352-359`) | `V8/webcrypto.rs:107-436`, `RC/README.md` | replace: native `crypto.subtle` of workerd. The vectors must still pass (error names for the unsupported sizes). |
| `op_gonvex_base64` | `atob`/`btoa` | all | `InvalidCharacterError` | `V8/webcrypto.rs:436-438`, `BS:360-361` | replace: native |
| `op_gonvex_parse_url` | URL parts JSON | all | `TypeError: Invalid URL` | `V8/isolate.rs:781-810`, `BS:89-113` | replace: native `URL` |
| `op_gonvex_sleep` | ms -> void (real time) | all | delay clamped 0..2^31-1 | `V8/isolate.rs:812-815`, `BS:224-244` | replace: native timers |
| `op_gonvex_sha256` | — | — | registered but unused by the bootstrap | `V8/isolate.rs:774-779,822` | drop |

---

#### B.2c Helper methods built on host calls

| Method | Kinds | Implementation | Source | Disposition |
|---|---|---|---|---|
| `ctx.db.recordColumns(table)` | R | Issues `dbQuery` against `sqlite_schema` and `_gonvex_record_views` to list a logical record's columns | `BS:575-585` | keep (needs `_gonvex_record_views` per 0010) |

### B.3 Globals installed by the bootstrap

Disposition for every row: **keep** the module-visible behavior; the kernel installs the same globals in the module isolate (0005). A row's own note overrides this.

Load order: vendored web-streams-polyfill 3.3.3 -> `createRuntimeGlobals` factory (from `RC/globals.js`) -> `BS` (`V8/isolate.rs:41-45,1085-1096`). No module loader is installed: the bundle must be one self-contained ESM (`V8/isolate.rs:1009-1011`). Most installs use `??=` (only if absent).

| Global | Behavior | Per-kind | Source |
|---|---|---|---|
| `Date` | `Date.now()`, `Date()`, and argument-less `new Date()` read `now()`. Explicit args, `parse`, `UTC` and subclasses are native. The prototype is copied, with no NativeDate leak. | Q/R: pinned to `ctx.now`. A: wall clock. Module init or no active call: **throws** "Time and randomness require an active runtime invocation" | `RC/globals.js:1-27`, `BS:28` (overwrites, not `??=`) |
| `Math` | frozen copy; only `random` replaced | Q/R: sfc32 seeded from `intentEntropy`. Throws "Math.random requires intentEntropy in Queries and Reducers" when it is missing or invalid. A: native. Init: throws. | `RC/globals.js:28-55`, `BS:29` |
| `crypto` | frozen `{getRandomValues, randomUUID, subtle}` | `getRandomValues`/`randomUUID`: A only; Q/R throw `DOMException InvalidAccessError`. `getRandomValues` >65,536 B gives `QuotaExceededError`; integer typed arrays only. `subtle`: all kinds. | `BS:403-435` |
| `CryptoKey` | opaque, native handle in a WeakMap; `Illegal constructor` | all | `BS:371-390` |
| `console` | `log/info/debug` -> stdout, `warn/error/trace` -> stderr, prefix `[gonvex:module]` | all | `BS:44-54` |
| `URL`, `URLSearchParams` | Rust `url` crate parse | all | `BS:58-115` |
| `TextEncoder`, `TextDecoder` (UTF-8 only, `fatal`, `stream`) | JS impl | all | `BS:117-193` |
| `AbortSignal` (`abort`, `timeout`), `AbortController` | minimal | all | `BS:195-222` |
| `setTimeout/clearTimeout/setInterval/clearInterval` | real-time sleep op | all, **not pinned** in Q/R | `BS:224-244` |
| `ReadableStream`, `WritableStream`, `TransformStream`, `ByteLengthQueuingStrategy`, `CountQueuingStrategy`, `TextDecoderStream`, `TextEncoderStream` | polyfill | all | `BS:249-272` |
| `Headers`, `Response` (`json`, `error`, `clone`, `bytes`, `text`, `arrayBuffer`) | JS impl. **No `Request`, no global `fetch`.** | all (`fetch` only as `ctx.fetch` in A) | `BS:281-346` |
| `DOMException`, `atob`, `btoa` | — | all | `BS:348-361` |
| `Deno.core` | left reachable (ops re-check capabilities) | — | `BS:18-21,810` |
| `structuredClone`, `queueMicrotask`, `performance`, `Blob`, `FormData`, `Request`, `fetch`, `WebSocket`, `EventTarget` | **not installed by the bootstrap** (only what bare deno_core/V8 provides) | — | absence in `BS` |

Context object (`BS:549-734`): `kind, function, now, auth{account,developerSession}, tenant, member, invocation (frozen InvocationInfo), intentEntropy? (reducer only), invitationInviter? (reducer only), db?, permissions?, actions?, tools?, functions?, scheduler?, fetch?, secrets? (frozen copy of resolved env), storage?, sandbox?`. Unwrapping of the export: function, `runtimeRegistrations()` by path, `.handler`, `.run`, `.options.run`, `.default` (`BS:742-764`).

Cloudflare: workerd already has `Date`, `Math`, `crypto`, `URL`, streams and the rest. The kernel must **shadow** `Date`, `Math.random`, `crypto.getRandomValues` and `crypto.randomUUID` per kind with the same `RC/globals.js` factory. Use lexical bindings like the generated local bundles do (`packages/gonvex/src/local-bindings.ts:12-20`, `SDK/runtime-globals.ts`). Disposition: **keep** semantics, **adapt** installation. `globalThis` patches are not safe if one isolate serves concurrent invocations.

---

### B.4 Determinism contract

Disposition for every row: **keep** exactly (0012 R23). Replay (M02) depends on it.

| Item | Exact behavior | Source |
|---|---|---|
| Clock value | `ctx.now` = `InvocationContext.now_unix_ms`. The runtime sends `0` (`RT/execution.rs:1535`). The module host stamps `SystemTime::now()` in ms when it decodes the invoke (`MH/protocol.rs:304-307`), before isolate admission. The V8 fallback is the same when it is `0` (`V8/lib.rs:574-580`). | |
| Clock scope | **Per invocation**, not per transaction: the write tx opens lazily later. Nested tool and function calls get their own `now`. Q and R pinned; A real. | `V8/isolate.rs:148-149`, `RC/globals.js:10-13` |
| Clock leaks | DB-side time is not pinned: `NOW_MS` = `CAST(unixepoch('subsec')*1000 AS INTEGER)` in SQL defaults and storage. Scheduler `runAfter` uses `Utc::now()`. Timers are real-time. | `ST/feed.rs:15`, `RT/storage.rs:298,664`, `RT/host_calls.rs:824` |
| Seed source | `intentEntropy`: 64 lowercase hex, generated by the client once per intent (`crypto.getRandomValues(32 bytes)`, `packages/client/src/index.ts:1645`), persisted with the intent and sent with the reducer call. Server validates `^[0-9a-f]{64}$`, else InvalidArguments "Invalid reducer entropy" (`RT/execution.rs:589-601`). **Queries never get entropy** (`RT/execution.rs:1502`), so `Math.random()` always throws in server Queries. Reducers without client entropy (server-originated) also throw. | |
| PRNG | sfc32. `w[i] = parseInt(entropy[8i..8i+8],16)` for i=0..7; `a=w0+w4\|0, b=w1+w5\|0, c=w2+w6\|0, d=w3+w7\|0`; no warm-up. Step: `v=((a+b\|0)+d\|0)>>>0; d=d+1\|0; a=b^b>>>9; b=c+(c<<3)\|0; c=(c<<21\|c>>>11)+v\|0; return v/2^32`. Generator cached per invocation object (WeakMap), so each invocation starts the sequence fresh. | `RC/globals.js:28-52` |
| Vectors | entropy `ab…ab` gives `[0.02352941082790494, 0.7535156221129, 0.8780962645541877]`; `cd…cd` gives `[0.8235294106416404, 0.6871706461533904, 0.5438285011332482]` | `RC/vectors.json` |
| Secure entropy | `getRandomValues`/`randomUUID` denied in Q/R (InvalidAccessError) | `V8/isolate.rs:759-768`, `SDK/runtime-globals.ts:8-22` |
| Row ids (SDK, local reducers) | `reducerRowId(ctx, table, ordinal)` = UUID layout over `SHA-256(JSON.stringify(["gonvex.reducer.ids.v1", tenant.id\|"", account.id\|"", invocation.commandId, table, ordinal]))[0..16]`, with `b[6]=(b[6]&15)\|128` (version nibble **8**) and `b[8]=(b[8]&63)\|128`. Per-table ordinal counter. Explicit allocation passes through. | `SDK/reducer-execution.ts:4-40` |
| Batch ids | `reducerRowId(ctx, "batch:"+table, ordinal)`, counter shared per `ctx.db` | `SDK/data-write.ts:77-87` |
| JSON ids | prefix = first 28 chars of `reducerRowId(ctx,"json:"+ns,0)` + 8-hex ordinal | `SDK/reducer-execution.ts:43-51` |
| Tokens | `hex(SHA-256(JSON.stringify(["gonvex.reducer.tokens.v1", intentEntropy, tenant.id, purpose])))`. With no entropy, falls back to `crypto.randomUUID()`, **which throws in a server Reducer**. | `SDK/reducer-execution.ts:54-61` |
| Deferred ids (host) | `actionEnqueue`/`schedule*` ids = same hash with table `"deferred:"+kind` (`action`/`schedule`), per-kind ordinal, keyed by tenant, actor account and commandId. Schedule id is prefixed `job_`. Without a commandId: UUIDv4. Rust test vectors: `aa14126b-2f16-814a-8623-07601307140c` (action #0) and `c9983b7c-273c-8f8e-91df-ef1b9ba899f6` (schedule #0) for (`tenant-1`,`account-1`,`command-1`). Matches local `LR/portable.ts:470-489`. | `RT/host_calls.rs:62-85,365-379,903,1748-1761` |
| Non-deterministic leftovers | Insert of an omitted text PK without `generatedId` uses `Uuid::new_v4` (`RT/host_calls.rs:1100-1107`). Only "local" reducers get `reducerExecutionContext` (`SDK/index.ts:1533-1540`). Storage file ids are UUIDv4. Tool command ids are `agent-tool-<uuidv4>`. Action command ids are `action-<uuidv4>`. | |
| Replay guarantee | Server execution is authoritative. The idempotency receipt (`claim_reducer` at tx open, `store_reducer_result` before commit) replays the stored result instead of re-running (`RT/host_calls.rs:266-279`, `RT/execution.rs:613-633`). There is no deterministic re-execution on the server. Client prediction and server results can differ in `now`. | |

Disposition: **keep** all algorithms byte for byte; the vectors are the acceptance test. The pinned `now` is stamped by the DO at invocation start.

---

### B.5 Errors across the boundary

Disposition for every row: **keep** (see the closing paragraph for platform-error mapping).

| Layer | Names / codes | Retryable | Source |
|---|---|---|---|
| JS host-call error | `GonvexCapabilityError` (`status:"denied"`, `code:"CAPABILITY_DENIED"`); `GonvexHostError` (`status:"failed"`, `code:"HOST_CALL_FAILED"`, or `"STALE_AGENT_CATALOG"` when the message contains it) | — | `BS:437-446` |
| ABI `HostError` | `CapabilityDenied(cap)` "capability {cap} is not available to this invocation"; `Failed(msg)` "host call failed: {msg}" | — | `MR/lib.rs:437-443` |
| Dispatcher envelope | `{status:"error",kind}`: `dispatch` -> InvalidArtifact; `handler` -> Execution (stack, or `Name: message`); `result` (non-JSON, cyclic) -> Execution "…cannot be encoded as JSON"; `resultSize` -> BudgetExceeded | — | `BS:766-807`, `V8/dispatch.rs:356-403` |
| `ModuleError` | FunctionNotFound, WrongFunctionKind, InvalidArtifact, BudgetExceeded, Unsupported, InvalidArguments, InvalidResult, Execution | — | `MR/lib.rs:521-539` |
| Wire codes (`WireError{code,message,retryable}`) | `bad_request, unsupported, frame_too_large, invalid_artifact, artifact_hash_mismatch, module_load_failed, generation_conflict, unknown_generation, module_not_loaded, function_not_found, wrong_function_kind, invalid_args, invalid_result, budget_exceeded, execution_failed, cancelled, shutting_down, host_call_failed` | `retryable:true` on `shutting_down`, `unknown_generation` (host behind), `module_not_loaded` | `MH/protocol.rs:29-76`, `MH/session.rs:415-425,671-693,751-807` |
| `ExecutionError` | ModuleMissing, FunctionMissing, WrongKind, InternalFunction, Database, ModuleHost, HostCall, CapabilityUnavailable, StaleCatalog (`STALE_AGENT_CATALOG:`), StaleReducerArtifact (`STALE_REDUCER_ARTIFACT:`), ClientUpdateRequired (`CLIENT_UPDATE_REQUIRED:`), NotInteractive, InvalidArguments, InvalidResult, InvocationDepth, RecursiveAction, Transient(wrap) | — | `RT/execution.rs:32-76` |
| Client class (`reducer.error.class`) | `rejected` \| `transient` \| `update_required` \| `unauthenticated`; `retryable = transient \|\| unauthenticated` | yes | `rust/crates/protocol/src/lib.rs:107-147` |
| Classification | Wire code `cancelled/shutting_down/budget_exceeded/module_not_loaded/unknown_generation/generation_conflict/module_load_failed/invalid_artifact/artifact_hash_mismatch/host_call_failed` or `retryable` -> transient; other remote codes -> rejected. Transport failures -> transient. SQLite BUSY/LOCKED/NOMEM/INTERRUPT/IOERR/FULL/CANTOPEN/PROTOCOL (`code&0xff ∈ {5,6,7,9,10,13,14,15}`) -> transient. Constraint/syntax -> rejected. `TransientFault` flag keeps a transient DB cause transient even if the module rethrows. | | `RT/error_class.rs:22-185` `ExecutionError::ModuleMissing` maps to `transient` (`error_class.rs:120`). |
| Sticky failures | Capability denial or bridge loss fails the call even if caught. A failed SQL statement marks the tx aborted (B.7). | | `V8/isolate.rs:609-615,1326-1338`, `RT/host_calls.rs:961-972` |

Disposition: **keep** names, codes, message prefixes and classes. Map platform errors as follows: DO overload, `storage.sql` busy and DO reset go to `transient`. CPU-limit kills go to `budget_exceeded` (transient). DO SQLite limit errors (statement >100 KB, >100 params, row >2 MB) go to `rejected`, with a clear message.

---

### B.6 Generations, loading, reload, budgets

Not contract (tuning): isolate recycle count, transport slack, frame size, queue wait, init timeout, concurrent fetch limit. Contract: result size cap (8 MiB), execution deadline (Query and Reducer 10 s, Action 15 min, public Action 60 s), depth 8 and 32 durable hops, args and result schema validation.

**Loading** (`RT/modules.rs:169-293`, `MH/session.rs:416-512`). The manifest record is validated (migrations, client contract, crons that target only reducer/action, permissionGrants). `Load` decodes the artifact and checks SHA-256 of the JS against `javascript.hash` (and of the reducer bundle) (`MH/artifact.rs:53-95`). It then builds the engine: every function needs valid `args_schema` and `result_schema`, and duplicates are rejected (`V8/lib.rs:261-296`). `prewarm` evaluates the bundle in every warm isolate, so a throwing bundle fails the load (`V8/lib.rs:402-418`). Generation is the host's monotonic counter (`GENERATION_CONFLICT` otherwise).

**Activate**: under the registry activation write-lock, so a call lease (read-lock from manifest selection to completion) never sees a host/manifest mismatch (`RT/modules.rs:240-256,306-313`). Calls already running finish on the old generation. The old generation is reaped after drain ≤30 s (`RT/modules.rs:250`, `MH/session.rs:486-501`).

**Split artifact**: an optional Query/Reducer-only bundle. Those functions resolve by exact path from it. Actions run from the complete bundle in on-demand isolates (idle TTL 60 s) (`V8/lib.rs:202-209,305-355`).

**system.reload**: `RuntimeEvent::ModuleReloaded` makes every socket of the project clear its replicas and live queries. It sends `client.updateRequired` (then closes) if the client contract mismatches, else `{type:"system.reload", reason:"module generation changed", artifactHash}` (`RT/lib.rs:2479-2508`, `rust/crates/protocol/src/lib.rs:933-942`). The client updates `activeArtifactHash` and resumes query and replica subscriptions (`packages/client/src/index.ts:2275-2282`). Invocations may pin a generation: `UNKNOWN_GENERATION` (retryable) if the host is behind. Reducer artifact or agent catalog mismatch gives `STALE_*` -> update_required.

Disposition: **adapt**. The generation lives in the tenant DO (or control DO). Activation is an atomic swap of the module reference plus a broadcast of `system.reload` to that DO's sockets, which keeps the drain semantics. Code loading: Workers forbid `eval`/`new Function`. The bundle must be loaded through a Worker Loader (dynamic isolate) or a per-generation script/version upload. **Open: `contract-changes/0005-module-bundle-loading.md`; S2 reports.**

**Budgets**

| Budget | Value (default) | Overrun | Source |
|---|---|---|---|
| Heap per isolate | 64 MiB (`GONVEX_MODULE_HOST_MAX_HEAP_MB`) | near-limit callback terminates, then `BudgetExceeded "module exceeded the N byte isolate heap limit"` and the isolate is retired | `V8/isolate.rs:1012,1028-1036,1140-1144`, `RT/config.rs:322-326` |
| Init timeout | 10 s | `BudgetExceeded "module initialization exceeded its N ms deadline"` | `V8/lib.rs:141`, `V8/isolate.rs:992-1058` |
| Q/R execution (wall) | 10 s (`GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS`), counted from dispatch and shortened by the invocation deadline (captured before queueing) | watchdog `terminate_execution`; `BudgetExceeded "module exceeded its N ms execution deadline"`; isolate retired | `V8/lib.rs:142,188-194,734-751`, `V8/budget.rs:18-77`, `V8/budget_core.rs:8-44`, `V8/isolate.rs:1163-1187,1297-1324` |
| Scheduled Q/R slice | 3 s (`GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS`), depth 0, lane Scheduled | message "…exceeded its N ms time slice for scheduled work…" | `V8/lib.rs:151,718-732,755-768` |
| Action execution | 15 min (`…ACTION_EXECUTION_TIMEOUT_MS`) | same as Q/R; fetch timeout = remainder | `V8/lib.rs:143`, `RT/config.rs:359-363` |
| Agent Action | **no ceiling** when authenticated; an anonymous agent Action still gets the execution deadline and the 60 s public cap (`RT/execution.rs:857-869`) | — | `V8/lib.rs:740-749`, `RT/execution.rs:857` |
| Public Action | 60 s | wire text `module database capability failed: public Action execution deadline exceeded` (`RT/execution.rs:942` wraps it in `ExecutionError::HostCall`) | `RT/execution.rs:859-869,931-944` |
| Queue wait | counts against the invocation deadline | "invocation deadline elapsed while waiting for a module isolate" | `V8/lib.rs:594-603` |
| Transport slack | +5 s over the deadline | "…did not answer within N ms" | `MH/session.rs:38,712-720` |
| Result size | 8 MiB (JS pre-check in UTF-16 units, host exact bytes) | `BudgetExceeded` | `V8/lib.rs:144`, `BS:804-806`, `V8/isolate.rs:1371-1376` |
| Frame size (socket host) | 64 MiB | `frame_too_large` | `RT/config.rs:340-343` |
| Depth / hops | depth ≤8 (separate isolate pool per depth); durable hops ≤32 | "nested function invocation exceeded the maximum depth"; hop overflow drops the row | `MR/lib.rs:18-25`, `V8/lib.rs:302-304,537-539`, `RT/execution.rs:1200-1206` |
| Concurrent fetch | 16 | "Action concurrent network request limit exceeded" | `V8/lib.rs:650-655` |
| Isolate recycle | 10,000 calls | retire | `V8/lib.rs:145` |
| Args / result schema | portable schema validated before and after | InvalidArguments / InvalidResult | `V8/lib.rs:556-565,702-711`, `MR/lib.rs:544-790` |

Cloudflare: 128 MB per isolate and 30 s CPU by default (5 min max). Wall-clock deadlines must be enforced at host-call boundaries and by `Promise.race`. CPU-bound runaway JS cannot be terminated per call. A CPU-limit hit in the tenant DO resets the whole object, including its sockets. Disposition: **adapt**. Keep the error texts and codes. Q/R in the DO keep the 10 s and 3 s wall budgets. Actions keep 15 min in the Action Worker. Agent Actions go to a Workflow step (no deadline). Per-call heap caps: **replace** with the platform isolate limit plus result and fetch caps.

---

### B.7 Transactions

Disposition for every row: **adapt** onto `transactionSync`, with the savepoint rule in 0006; module-visible rollback semantics are **keep**.

| Aspect | Behavior | Source |
|---|---|---|
| Query | read-only tx (`begin_read`) opened before invoke. Every `dbQuery` shares one snapshot. Always rolled back. | `RT/execution.rs:360-399`, `ST/lib.rs:1476` |
| Reducer open | **Lazy**: the writer tx (`database.writer.begin()`, a deferred BEGIN on the tenant's single writer connection) opens at the first DB/outbox/schedule/getMetadata host call, never while the call waits for an isolate. On open: `set_command_id`, `set_invocation_provenance`, `claim_reducer(subject,key,path)`. A conflict rolls back, then "idempotency key already committed; replaying the stored result" and the stored result is replayed. | `RT/host_calls.rs:49-60,236-288`, `ST/lib.rs:1483,1580,1600`, `RT/execution.rs:537-562,613-633` |
| Commit | Only after the JS handler returns successfully and the result passes schema validation. Then `store_reducer_result`, then `commit_with_outcome`. A Reducer that never touched the DB commits nothing. | `RT/host_calls.rs:433-474`, `RT/execution.rs:612-638`, `ST/lib.rs:1620,1784` |
| Rollback | Handler throw, budget overrun, denial, invalid result or bridge loss: `finish(false)` rolls back. | `RT/host_calls.rs:467-473`, `ST/lib.rs:1840` |
| Statement failure | SQLite rolls back only the failing statement. The host marks the invocation tx **aborted** (`aborted=Some(msg)`, `session.mark_failed`). Further SQL fails with "current transaction is aborted, commands ignored until end of transaction block" until a successful `ROLLBACK TO <savepoint>` clears it. A commit attempted while aborted is rolled back with that error, even if JS caught the exception. | `RT/host_calls.rs:31-32,150-154,961-972,1016-1020,1075-1080,454-462`, `ST/lib.rs:1733` |
| Savepoints | Module SQL may issue `SAVEPOINT`, `RELEASE [SAVEPOINT]` and `ROLLBACK [TRANSACTION|WORK] TO`. Bare BEGIN/COMMIT/ROLLBACK are refused. SDK `applyDataWrites` wraps a batch in `SAVEPOINT gonvex_data_writes` / `ROLLBACK TO` + `RELEASE`, so a caught error never commits half a batch. | `RT/sql_admission.rs:170-190,246-265`, `SDK/data-write.ts:327-340` |
| Nesting | A Reducer cannot call another function (no tools/functions capability). Action -> tool/function Reducer: each nested call is **its own transaction** and commits independently. The Action has no tx. | `MR/capabilities.rs:23-34`, `RT/action_calls.rs:344-366` |
| Outbox / schedule | `actionEnqueue` and Reducer `schedule*` write outbox rows in the Reducer tx, so they are visible only after commit. Action `schedule*` enqueues directly. | `RT/host_calls.rs:782-815,889-927`, `ST/lib.rs:1639-1659` |
| Local runtime | `db.atomic` = snapshot and restore of staged changes. `db.lock` = no-op. `db.query` throws `UnsupportedLocalOperationError`. Deferred work is captured with deterministic ids. | `LR/portable.ts:356-494` |

Disposition: **adapt**. The Reducer tx becomes one DO storage transaction (`ctx.storage.transactionSync` or async `transaction`, per S2), opened lazily with the same receipt claim. Module-issued `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` must be emulated through the storage transaction API (nested `transactionSync`), because DO refuses them in `sql.exec`. **This needs a contract-change note if S2 cannot await host calls inside `transactionSync`.** Keep the aborted-tx rule and its exact message.

---

### Runtime-contract vectors

`packages/module-sdk/runtime-contract/vectors.json`, version 1, origin "F2/F3, 2026-10-18". **34 cases** (24 action, 5 query, 5 reducer), run by `RC/runner.js`:

`digest-SHA-1`, `digest-SHA-256`, `digest-SHA-384`, `digest-SHA-512`, `hmac-SHA-1-rfc-2202`, `hmac-SHA-256-rfc-4231`, `ed25519-rfc-8032-pkcs8-raw`, `ed25519-rfc-8032-jwk`, `rsa-pkcs1-v1_5-pkcs8`, `rsa-pkcs1-v1_5-jwk`, `aes-128-nist-zero-block`, `aes-256-nist-zero-block`, `legacy-sealed-secret-node-to-isolate`, `crypto-key-access-errors`, `webhook-base64`, `query-ctx-now`, `query-random-no-entropy`, `query-secure-entropy-denied`, `query-seeded-random-ab`, `query-seeded-random-cd`, `reducer-ctx-now`, `reducer-random-no-entropy`, `reducer-secure-entropy-denied`, `reducer-seeded-random-ab`, `reducer-seeded-random-cd`, `action-wall-time-secure-entropy`, `undeclared-network-origin`, `json-serialization`, `cyclic-serialization-rejected`, `schema-rejects-invalid-argument`, `cached-hmac-survives-invocation`, `hmac-sha256-jwk`, `ed25519-rfc-8032-spki`, `aes-gcm-authenticated-data`.

The Rust suite repeats them in a recycled isolate (`V8/runtime_contract_tests.rs:38-142`). Disposition: **keep** as the K04 acceptance test.

## C. Control Plane functions, auth, sessions, admission

Reference: Rust runtime at `0bf3ad13`. All Rust paths are `rust/crates/runtime/src/` unless they say otherwise. "control.rs:N" points to the handler body, and "def N" points to the `definition()` entry (control.rs:4323-4533). TS means `packages/client/src/control.ts`: the line of the `ref`, with the schema at lines 57-116.

### C.0 Dispatch rules that apply to every row

Disposition for every rule: **keep**; dispatch runs in the Control DO.

- **Wire.** A Control call is an ordinary `query.call` / `query.subscribe` / `reducer.call` / `action.call` frame with `scope:"control"` (lib.rs:3002, 3080, 3275, 3479, 5014). There is no HTTP route for Control functions.
- **Lookup.** `definition(path)` decides the kind, the authorization and `live` (control.rs:4323). An unknown path returns `unknown Control Plane function "<p>"`. The wrong frame kind returns `Control Plane function "<p>" is not a Query|Reducer|Action` (control.rs:4535).
- **Arguments.** Every function uses `exact_object` (control.rs:4580): an unknown key returns `invalid arguments: unknown field "<k>"`, and a non-object returns `expected an object`. `required_string` trims the value and returns `<k> is required` if it is missing or empty (4599). Below, **E-args** stands for these errors.
- **Writes** (Reducers and Actions) need an idempotency key. A missing key returns `Control Plane write calls require an idempotency key` (control.rs:263). The TS client defaults the key to the call id (`packages/client/src/index.ts:5088`). Rows go to `gonvex_control_idempotency` (PK project, subject, key), with these rules:
  - The subject is the account id. TenantAdmin calls append `|tenant:<id>`. Auth Actions use `auth:<sha256(material)>` (control.rs:4181, 4097).
  - Reusing a key for another path or kind returns `idempotency key was already used for another operation`.
  - A key that is still pending returns `Control Plane operation is still in progress`, except in the saga variant `claim_control_saga`, where a pending row means "resume" (3517).
  - A stored error is replayed, and so is a stored result.
  - **E-idem** stands for these errors.
  - **Rows are never pruned** in Rust. Kernel: rows expire after 24 h (0012 R9).
- **Authorization.** `authorize_control` is at control.rs:2013. The "Who" column uses these levels:
  - `anon` (Public): any socket. That includes a token-less `controlOnly` auth with a `project`, and an anonymous public-transport socket.
  - `acct` (Account): `connection.identity` is set, otherwise `authentication is required`. **Impersonation, developer-mode and service-delegated sockets carry the *target* account's identity**, so Account functions run as the target.
  - `tAdmin` (TenantAdmin): the connection has a tenant, and `admit_member` re-reads the tenant `members` row on **every call**, which must have role `owner|admin`. Otherwise `tenant administrator access is required`. **This is not blocked during impersonation or service delegation.** Inside handlers, an `admin` caller cannot grant or target `owner|admin`.
  - `dev` (Developer): `gonvex_runtime_projects.owner_email`, or a `gonvex_project_members` row with role ∈ {owner, admin, dev}, case-insensitive on email. Otherwise `developer access is required`.
  - `pAdmin` (ProjectAdmin): the same check with roles {owner, admin}. Otherwise `project administrator access is required`.
  - `dev` and `pAdmin` return `project administration is unavailable during impersonation` when `impersonation_id` is set.
  - `svc`: a service-principal socket (C.1b).
- **Live Control Queries.** `open_control_query` (control.rs:1947) refuses anonymous tenant sessions with `authentication is required` and non-live paths with `Control Plane query "<p>" is not subscribable`. It returns `query.result {path,result,reason:"initial"}`. `refresh_control_queries` (1978) re-runs **every** live Control subscription on the socket and sends one only if its JSON changed, with reason `control-change` or `presence-change`. Errors arrive as `query.error`. These events trigger a refresh:
  - Any successful Control write on that socket (lib.rs:3303, 3423, 3545).
  - `RuntimeEvent::ControlChanged{project}`. This event fires after any successful Control write in the process (`notify_control_changed`, lib.rs:4036), after a membership-projector batch (membership_projector.rs:106), and on a cross-process `gonvex_control_changed` notify (lib.rs:3979).
  - `PresenceChanged`, which refreshes only `control.support.listTenants` (control.rs:1986).
- **Common errors.** **E-nf** is `invalid arguments: record not found`, returned when an UPDATE or DELETE touched 0 rows (`affected_result`, control.rs:4171). Every error renders as `invalid arguments: ...` or the variant text in control.rs:102-135.

### C.1 Function table: 60 registered names plus 2 service-principal paths

Kind: Q = Query, Ql = live Query, R = Reducer, A = Action. In the "Who" column, "admin≠owner/admin" means a tenant `admin` cannot grant or target the `owner` or `admin` role.

I diffed the Rust `definition()` names against the keys of `controlFunctionSchemas` in TS. **They match exactly: 60 names = 57 `control.*` plus 3 legacy aliases.** The 2 `control.servicePrincipals.*` paths are dispatched outside `definition()` and do not appear in TS.

| # | Name | Kind | Args | Result | Who | Side effects (tables written) | Errors (besides E-args, E-idem, auth) | Rust | TS | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | control.accounts.me | Q | {} | {id,email,name,avatarUrl} | acct | none (taken from the socket's identity snapshot) | — | control.rs:2139, def 4327 | 180 | adapt: control DO, or answer locally from the socket's identity | Opens a read txn without needing one |
| 2 | control.accounts.updatePassword | R | {currentPassword,newPassword} | {updated} | acct | gonvex_account_passwords upsert; idempotency | `newPassword must contain at least 12 characters`; `password management is owned by the configured external identity provider` (auth_mode ∉ gonvex-native, hybrid); `current password is incorrect` | control.rs:2528, def 4332 | 181 | adapt: control DO (PBKDF2, see C.3) | currentPassword is checked **only if a hash exists**. Other sessions are **not** revoked. Under impersonation it runs as the target |
| 3 | control.accounts.resetMemberPassword | R | {memberId,newPassword} | {updated} | tAdmin, admin≠owner/admin target | reads tenant `members`; gonvex_account_passwords upsert; revokes **all** gonvex_auth_sessions and gonvex_auth_refresh_tokens of the target; idempotency | 12-character rule; auth-mode error; `active tenant member was not found`; TenantAdminRequired | control.rs:568, def 4337 | 182 | adapt: control DO plus a read RPC to the tenant DO | Live sockets of the target drop within 30 s (revalidation) |
| 4 | control.accounts.provisionMemberLogin | R | {email,name,password,role∈owner\|admin\|member,permissions:{}} | {updated,accountId,memberId} | tAdmin, admin≠owner/admin | Control txn: accounts, account_identities (provider `password`, issuer = project, subject = email), gonvex_account_passwords, gonvex_member_login_provisioning. Tenant txn: `members` upsert (command `member-provision:<acct>`), projector wake. Control txn: account_tenant_index, idempotency | `permissions must be an object`; 12-character rule; `role is invalid`; `login provisioning is owned by the configured external identity provider; invite the account instead`; `this account already exists; invite it instead of provisioning its login` | control.rs:667, def 4337 | 183 | adapt: control DO → tenant DO → control DO saga | Three txns. The provisioning row makes a retry resumable. The idempotency row is claimed **last**. `viewer` is not allowed |
| 5 | control.auth.passwordLogin | A | {email,password} | Session `{accessToken,tokenType:"Bearer",expiresIn:900,expiresAt(ms),refreshToken,refreshExpiresAt(ms),account{id,email,emailVerified,name,picture,provider},tenants[],activeTenantId}` | anon | gonvex_auth_sessions and gonvex_auth_refresh_tokens (new `family_*`); idempotency with subject `auth:sha256(email)` | `password authentication is disabled; use the project's external identity provider`; `invalid email or password` (same text for an unknown account and a wrong password) | control.rs:421 (flow 373-566), def 4344 | 187 | adapt: control DO | activeTenantId = the first admitted tenant. `tenants[]` items carry an extra `memberId` (session_result_from_directory, 4001). PBKDF2 runs **while holding the Control writer txn**. **There is no rate limit.** The **idempotency replay check runs before the password check** and returns the stored plaintext tokens (C.3.9) |
| 6 | control.auth.exchangeExternalToken | A | {provider∈firebase\|external-oidc, token, tenantId?, previousRefreshToken?} | Session | anon | accounts and account_identities (create or link), gonvex_auth_identity_events; sessions and refresh tokens; revokes the previous family if `previousRefreshToken` belongs to the same account; idempotency with subject `auth:sha256(provider\0token)` | `provider must be firebase or external-oidc`; `project auth mode "<m>" does not allow provider "<p>"`; `external identity provider is disabled`; `... configuration is incomplete`; external_auth.rs:53-74 errors; `verified email matches more than one account`; `a verified invited email is required`; `account signup requires an active invitation`; `external identity email is required for first login`; `account is unavailable` | control.rs:480 (verification 381-399, resolution 3680), def 4344 | 188 | adapt: control DO. JWKS through Worker `fetch`, cached in DO memory | Token verification (network) happens **before** the writer txn. activeTenantId matches `tenantId` by id or domain, otherwise the first tenant |
| 7 | control.auth.refreshSession | A | {refreshToken} | Session | anon | the old refresh token's `used_at`; new access and refresh tokens in the **same family with the same absolute expiry**; reuse of a token more than 5 s after first use revokes the whole family | `invalid or expired refresh token`; `refresh token was already rotated; use the latest session` (within the 5 s grace); `refresh token reuse detected; this login was revoked` (also stored as a failed idempotency row) | control.rs:520, 3938, def 4344 | 189 | adapt: control DO | activeTenantId = the first tenant. The old access token stays valid until it expires |
| 8 | control.auth.logout | R | {refreshToken, all:boolean} | {updated} | acct | revokes the token's family (sessions and refresh tokens), or with all=true every session and refresh token of the account | `refreshToken is required` (also when all=true) | control.rs:2576, def 4351 | 190 | adapt: control DO | An unknown token still returns {updated:true}. The family lookup is scoped to the caller's account |
| 9 | control.auth.publicSettings | Q | {} | {mode, providers[]} | anon | none | `unsupported project auth mode "<m>"` | control.rs:2149, def 4356 | 186 | adapt: control DO (cacheable in KV) | Adds `password` if mode is native or hybrid, plus enabled providers filtered by mode |
| 10 | control.auth.realms.list | Ql | {} | [{provider,enabled,signupMode,azureTenantId,clientId,hasClientSecret,issuer,audience,jwksUrl,firebaseProjectId,firebaseTenantId,hasAdminCredentials,authMode}] | pAdmin | none | — | control.rs:2181, def 4361 | 192 | adapt: control DO | The first element is a synthetic `password` row. Secrets appear only as `has*` booleans |
| 11 | control.auth.realms.configure | R | {provider∈google\|microsoft\|apple\|firebase\|external-oidc, authMode?, enabled, signupMode∈personal\|inviteOnly, azureTenantId?, clientId?, clientSecret?, issuer?, audience?, jwksUrl?, firebaseProjectId?, firebaseTenantId?, adminCredentials?} | {updated} | pAdmin | gonvex_auth_providers upsert (secrets encrypted with AES-256-GCM; an empty secret keeps the old one); gonvex_runtime_projects.auth_mode; idempotency | `provider is unsupported`; `signupMode must be personal or inviteOnly`; `enabled must be boolean`; `authMode "<m>" does not enable provider "<p>"`; firebaseProjectId, OIDC triple, clientId, azureTenantId and clientSecret requirements; `jwksUrl must be an HTTPS URL without credentials`; `adminCredentials is supported only for Firebase`; Firebase JSON and project_id mismatch errors; `GONVEX_DASHBOARD_SESSION_SECRET is required to store provider credentials` | control.rs:2618, def 4366 | 193 | adapt: control DO, with the key from a Secrets binding | Firebase defaults: issuer `https://securetoken.google.com/<p>`, audience `<p>`, Google securetoken JWKS. Native redirect URIs live in gonvex_auth_redirect_uris, which only operator routes write, and only for Google (operations.rs:2718) |
| 12 | control.auth.memberProviders | Q | {memberIds: string[]} | [{memberId,providers[]}] | tAdmin | none. Reads tenant `members` and Control account_identities | `memberIds must be an array`; `memberIds cannot contain more than 500 values` | control.rs:3370 (dispatch 152), def 4371 | 195 | adapt: control DO plus a tenant DO read | The `>1000` check (3377) is **dead** because `unique_string_array` caps at 500 first. Providers prefer `sign_in_provider` |
| 13 | control.tenants.mine | Ql | {} | [{id,name,role,permissions,domain,timezone,description,profile}] | acct | none | — | control.rs:3308 (dispatch 148), def 4376 | 198 | adapt: control DO, driven by account_tenant_index and then admission per tenant DO | Scans **every tenant of the project** with one `admit_member` (one tenant-DB read) each, on every call and every live refresh. A redeemed developer grant adds its tenant Kernel: answer from `account_tenant_index`, re-admit by batched RPC to tenant DOs, at most 32 concurrent (0012 R15). |
| 14 | control.tenants.create | R | {name, domain?} | {id,name,role:"owner",permissions:{},domain,timezone:"UTC",description,profile:{}} | acct | gonvex_tenant_provisioning (checkpoint keyed by idempotency key); a tenant DB file under GONVEX_DATA_DIR; gonvex_runtime_tenants; migrations; tenant `members` owner row; account_tenant_index; `provisioned=TRUE`; idempotency | `tenant domain must be a lowercase DNS label` (≤63 chars, [a-z0-9-], no leading or trailing `-`); `GONVEX_DATA_DIR is not configured`; `project is not configured for tenant databases` (database_mode ≠ multiTenant); `this project allows tenant creation only through its Control Plane` (any enabled provider is inviteOnly); `tenant domain is already in use`; `tenant provisioning command belongs to another account`; `project module is unavailable` | control.rs:1055, def 4381 | 199 | replace: create the tenant DO (`idFromName(tenantId)`), provision the schema inside it, and write the directory row in the control DO | domain defaults to `slug(name)`. database_alias = `slug-<8 chars of the tenant id>` |
| 15 | control.tenants.getByDomain | Q | {domain} | {id,name,domain} | anon | none | `tenant not found` | control.rs:2227, def 4386 | 200 | adapt: control DO (cacheable in KV) | Filters only `status<>'deleted'`, so **disabled tenants resolve**. Anonymous callers can enumerate tenants with it |
| 16 | control.tenants.updateProfile | R | {name, domain, description} | {updated} | tAdmin | gonvex_runtime_tenants name, domain (lowercased), description | E-nf | control.rs:2813, def 4391 | 201 | adapt: control DO | **No domain format or uniqueness check**, unlike create. `description` is optional in Rust and required in the TS schema |
| 17 | control.tenants.updateTimezone | R | {timezone} | {updated} | tAdmin | gonvex_runtime_tenants.timezone | `timezone must be an IANA timezone or UTC` (≤128 chars and contains `/`, or `UTC`); E-nf | control.rs:2838, def 4391 | 202 | adapt: control DO | |
| 18 | control.tenants.delete | R | {} | {updated} | tAdmin (an admin can do it, not only an owner) | soft delete: status=`deleted`, deleted_at | E-nf | control.rs:2858, def 4391 | 203 | adapt: control DO. Tenant DO data is retained | The DB is not dropped. Sockets fail at the next revalidation because the route stops resolving |
| 19 | control.tenants.setException | R | {tenantId, value:any} | {updated} | pAdmin | gonvex_control_settings(kind `tenant.exception`, scope tenantId) | — | control.rs:2871, def 4398 | 204 | adapt: control DO | Nothing in the runtime reads it |
| 20 | control.tenants.setSeatLimit | R | {tenantId, seatLimit:int≥0\|null} | {updated} | pAdmin | gonvex_runtime_tenants.seat_limit | `seatLimit must be a nonnegative integer or null`; E-nf | control.rs:2885, def 4398 | 205 | adapt: control DO | **Never enforced.** No admission or invitation path reads it |
| 21 | control.invitations.lookup | Q | {token} (Rust also accepts `invitationToken`) | {tenantId,tenantName,email,role,teamIds,allowedAuthProviders,expiresAt} | anon | none | `token is required`; `invitation is invalid or expired` | control.rs:2246, def 4403 | 208 | adapt: control DO | Lookup is by sha256(token) |
| 22 | control.invitations.list | Ql | {} | [{id,email,role,permissions,teamIds,allowedAuthProviders,expiresAt,revoked,accepted,state,createdAt,updatedAt}] | tAdmin | none | — | control.rs:2290, def 4408 | 209 | adapt: control DO | Returns every state |
| 23 | control.invitations.create | R | {email, role∈owner\|admin\|member\|viewer, permissions?:{}, teamIds:[], allowedAuthProviders:[], payload?:any} | {id, token} | tAdmin, admin≠owner/admin | gonvex_auth_membership_invitations upsert on (project, tenant, email): replaces any prior invite, expires now + 7 d (604800000 ms), new token `gvx_invite_*`; idempotency | `email is invalid`; `role is invalid`; `permissions must be an object`; `teamIds must be an array` / `allowedAuthProviders must be an array`; `... cannot contain more than 500 values`; `... must contain strings`; `authentication provider "<p>" is unsupported`; TenantAdminRequired | control.rs:2916, def 4413 | 210 | adapt: control DO | teamIds and allowedAuthProviders are **required arrays**. The plaintext token is stored in the idempotency result |
| 24 | control.invitations.update | R | {id, role, permissions?, teamIds, allowedAuthProviders, payload?} | {updated} | tAdmin, admin≠owner/admin | the invitation row; invited_by = caller | as row 23; E-nf (only unaccepted, unrevoked rows match) | control.rs:2983, def 4413 | 211 | adapt: control DO | Does not extend the expiry |
| 25 | control.invitations.revoke | R | {id?, email?} (one is required) | {updated} | tAdmin | revoked_at | `id or email is required`; E-nf | control.rs:3041, def 4413 | 213 | adapt: control DO | No role check, so an admin can revoke an owner invite |
| 26 | control.invitations.accept | R | {token} (or `invitationToken`) | {tenantId, memberId} | acct; the account email must equal the invite email | Control: invitation `handoff_state=claimed`. Tenant txn (command `invitation:<id>`): reducer receipt `$system.invitation.accept`, `members` upsert, optional module `invitationAcceptance` internal Reducer with the action outbox. Control: invitation `completed` and accepted_at, account_tenant_index. On failure the claim is released (4789) | `token is required`; `invitation is invalid, expired, revoked, or already accepted`; `invitation does not belong to this account`; `invitation provider policy is invalid`; `invitation requires a linked authentication provider`; `invitation application data is invalid`; `invitation acceptance is already in progress`; `invitation inviter can no longer issue these membership grants`; `module must declare invitationAcceptance for application invitation payloads`; `invitation acceptance Reducer is invalid`; module Reducer error text; `invitation was already accepted` | control.rs:1556, def 4420 | 212 | adapt: control DO ↔ tenant DO saga, with the module Reducer running in the tenant DO | Re-checks the inviter's authority inside the tenant txn (4746). Idempotent per (account, key) through the invitation row and the tenant receipt. **No gonvex_control_idempotency row** |
| 27 | control.agentAuth.issue | R | {permissions:string[], expiresInSeconds:1..86400} | {id, token} (`gvx_agent_claim_*`) | pAdmin | gonvex_agent_claim_tokens | `expiresInSeconds must be between 1 and 86400`; array errors | control.rs:3063, def 4425 | 216 | adapt: control DO | |
| 28 | control.agentAuth.claim | R | {token} | {id, permissions} | acct | claimed_at, claimed_account_id | `agent token is invalid, expired, revoked, or already claimed` | control.rs:3086, def 4430 | 217 | adapt: control DO | **A claim grants nothing.** Nothing reads claimed tokens |
| 29 | control.agentAuth.revoke | R | {id} | {updated} | pAdmin | revoked_at | E-nf | control.rs:3108, def 4425 | 218 | adapt: control DO | |
| 30 | control.project.developers.list | Ql | {} | [{email,name,role}] | pAdmin | none | — | control.rs:2317, def 4435 | 221 | adapt: control DO | |
| 31 | control.project.developers.invite | R | {email,name,role∈owner\|admin\|dev} | {updated} | pAdmin | gonvex_project_members upsert | `role is invalid`; `email is invalid` | control.rs:3115, def 4440 | 222 | adapt: control DO | Grants access immediately with no invite flow (gonvex_project_invitations is unused here). An admin can grant `owner` |
| 32 | control.project.developers.remove | R | {email} | {updated} | pAdmin | deletes the gonvex_project_members row | E-nf | control.rs:3128, def 4440 | 223 | adapt: control DO | `owner_email` on the project still grants access |
| 33 | control.developer.status | Ql | {} | {developer, mode, tenantId, grantId} | acct | none | — | control.rs:2337, def 4445 | 226 | adapt: control DO | `developer` is true for **any** project_members role. `mode` is true when impersonating |
| 34 | control.developer.provisionSelf | R | {tenantId} | {updated, tenantId, memberId} | dev | tenant `members` upsert with role admin and `{"developer":true}`; account_tenant_index; idempotency | tenant resolution errors (`TenantDatabaseMissing`) | control.rs:858, def 4450 | 227 | adapt: control DO + tenant DO saga | Creates a **real** membership |
| 35 | control.developer.removeSelf | R | {tenantId} | {updated} | dev | `members` status=revoked; account_tenant_index revoked; idempotency | same as row 34 | control.rs:858, def 4450 | 228 | adapt: control DO + tenant DO saga | Revokes the caller's membership however it was created |
| 36 | control.developer.enter | R | {tenantId} | {id:`devmode_*`, token:`gvx_imp_*`, expiresAt} | dev | gonvex_impersonation_grants (actor = target = caller, reason `developer mode`, expires in 30 min); idempotency | the tenant must pass `resolve_public_tenant` (active directory row) | control.rs:3140, precheck 331, def 4450 | 229 | adapt: control DO | Redeemed through `auth`. Without a membership the caller gets an ephemeral admin member `developer:<acct>` (store/src/lib.rs:1015) |
| 37 | control.developer.exit | R | {grantId} | {updated} | acct (own grants only) | the grant's revoked_at | E-nf | control.rs:3151, def 4457 | 230 | adapt: control DO | The live developer socket drops within 30 s |
| 38 | control.assistant.getDefaults | Ql | {} | any (`{}` if unset) | pAdmin | none | — | control.rs:2360, def 4462 | 233 | adapt: control DO | Reads only scope `''` |
| 39 | control.assistant.setDefaults | R | {scopeId, value} | {updated} | pAdmin | gonvex_control_settings `assistant.defaults` | — | control.rs:3158, def 4467 | 234 | adapt: control DO | A non-empty scopeId is invisible to getDefaults |
| 40 | control.voice.getConfiguration | Ql | {} | [{kind,scopeId,value}] (kind LIKE `voice.%`) | pAdmin | none | — | control.rs:2370, def 4475 | 237 | adapt: control DO | |
| 41 | control.voice.setRateCard | R | {scopeId, value} | {updated} | pAdmin | gonvex_control_settings `voice.rateCard` | — | control.rs:3158, def 4467 | 238 | adapt: control DO | |
| 42 | control.voice.setTenantEntitlement | R | {scopeId, value} | {updated} | pAdmin | `voice.tenantEntitlement` | — | control.rs:3158, def 4467 | 239 | adapt: control DO | |
| 43 | control.voice.setUserOverride | R | {scopeId, value} | {updated} | pAdmin | `voice.userOverride` | — | control.rs:3158, def 4467 | 240 | adapt: control DO | |
| 44 | control.support.listTenants | Ql (also refreshed on presence-change) | {} | [{id,name,domain,status,timezone,seatLimit,createdAt,userCount,activeSessions}] | dev | none. Counts active members in each tenant DB (4 at a time) and adds the process-local connection count | — | control.rs:2082 (dispatch 155), def 4480 | 243 | adapt: control DO fanning out to tenant DOs; activeSessions from each DO's socket count | Reads O(tenants) per call and per refresh. `userCount` is null on error |
| 45 | control.support.listSessions | Ql | {} | [{id,tenantId,accountId,release,environment,lastSeenAt}] (≤1000) | pAdmin | none | — | control.rs:2374, def 4485 | 244 | adapt: control DO plus presence from the socket-owning DOs | Live sessions show lastSeenAt = now (process-local presence) |
| 46 | control.support.listErrors | Ql | {} | {groups:[≤500], releases[]} | pAdmin | none | — | control.rs:2468, def 4485 | 245 | adapt: control DO (rows come from the error-ingestion section) | |
| 47 | control.support.getSession | Q | {id} | {id,tenantId,accountId,connectionId,release,environment,lastSeenAt,createdAt} | pAdmin | none | `support session not found` | control.rs:2415, def 4490 | 246 | adapt: control DO | |
| 48 | control.support.getError | Q | {fingerprint} | group | pAdmin | none | `error group not found` | control.rs:2493, def 4490 | 247 | adapt: control DO | |
| 49 | control.support.getTenant | Q | {tenantId} | {id,name,domain,status,timezone,description,profile,seatLimit,createdAt,updatedAt} | pAdmin | none | `tenant not found` | control.rs:2446, def 4490 | 248 | adapt: control DO | |
| 50 | control.support.pruneSessions | R | {olderThanSeconds:60..31536000} | {deleted} | pAdmin | DELETE gonvex_support_sessions (cascades to commands), excluding live ones | `olderThanSeconds must be between 60 and 31536000` | control.rs:3245, def 4497 | 249 | adapt: control DO | |
| 51 | control.support.heartbeat | R | {release, environment} | {sessionId:`support_<connectionId>`} | acct | gonvex_support_sessions upsert (last_seen_at); idempotency | — | control.rs:3185; in-memory short-circuit lib.rs:4899-4935; def 4504 | 250 | adapt: presence in the socket DO. Write only on change (DOD: nothing ephemeral in SQLite) | A repeat with identical attribution is answered from memory with no write |
| 52 | control.support.sendCommand | R | {sessionId, kind, payload} | {id} | pAdmin | gonvex_support_commands insert | `support session not found` | control.rs:3214, def 4497 | 251 | adapt: control DO. **Keep no push (0012 R8)** | The push (`broadcast_support_command`, control.rs:164) sits in `execute_control_query` and only fires for **Query** dispatch. sendCommand is a Reducer, so `ServerMessage::SupportCommand` (lib.rs:2509) is **never sent** in the reference. Ruled in 0012 R8 |
| 53 | control.support.ackCommand | R | {id} | {updated} | acct (owner of the session) | acknowledged_at | E-nf | control.rs:3235, def 4504 | 252 | adapt: control DO | |
| 54 | control.support.createImpersonation | R | {accountId, tenantId, reason} | {id:`imp_*`, token:`gvx_imp_*`, expiresAt} | pAdmin | gonvex_impersonation_grants, expiry 5 min; idempotency (stores the plaintext token) | `target account is not an active tenant member` | control.rs:987, def 4497 | 253 | adapt: control DO plus a tenant DO admission check | |
| 55 | control.demos.create | R | {tenantId,email,name,password,label} | {accountId, memberId} | pAdmin | Saga with a resumable claim: accounts (re-enabled), account_identities, gonvex_account_passwords, gonvex_demo_accounts. Tenant: `members` (role member). Control: account_tenant_index, idempotency | 12-character rule; `email already belongs to a non-demo account`; tenant resolution | control.rs:1304, def 4509 | 256 | adapt: control DO + tenant DO saga | |
| 56 | control.demos.resetPassword | R | {accountId, password} | {updated} | pAdmin | gonvex_account_passwords (demo accounts only) | 12-character rule; E-nf | control.rs:3269, def 4509 | 257 | adapt: control DO | Does not revoke sessions |
| 57 | control.demos.delete | R | {accountId} | {updated} | pAdmin | Tenant: `members` revoked. Control: delete gonvex_demo_accounts, set accounts.disabled_at, account_tenant_index revoked; idempotency (saga) | `demo account not found` | control.rs:1467, def 4509 | 258 | adapt: control DO + tenant DO saga | A disabled account's sessions fail within 30 s |
| 58 | users.myTenants (legacy) | Ql | {} | string[] (tenant ids) | acct | none | — | control.rs:3308, def 4516 | 261 | adapt: as row 13 | Requires `scope:"control"` |
| 59 | tenants.getInvitationByToken (legacy) | Q | {token?} or {invitationToken?} | {tenantId,invitationToken,userEmail,teamIds,allowedAuthProviders} | anon | none | as row 21 | control.rs:2246, def 4521 | 263 | adapt: as row 21 | Echoes the token back |
| 60 | tenants.acceptInvitation (legacy) | R | {token?} or {invitationToken?} | {tenantId, memberId} | acct | as row 26 | as row 26 | control.rs:1556, def 4526 | 264 | adapt: as row 26 | |
| 61 | control.servicePrincipals.delegate | R, `scope:"control"` on a **service socket only** | {accountId, memberId, reason (≤200 B), expiresInSeconds? (30..principal max, default = max), actor?{kind:[a-z0-9_-]{1,32}, name (1-120 chars), reference? (≤200)}} | {id:`svcdel_*`, token:`gvx_imp_*`, expiresAt (RFC 3339), expiresInSeconds} | svc | gonvex_impersonation_grants (actor `service:<id>`, reason `service:<id>: <reason>`, delegation_actor JSON) | `invalid arguments: <k> is required`; `reason exceeds 200 bytes`; `expiresInSeconds must be between 30 and <max>`; actor field errors; `target is not an active tenant member` (memberId must equal that account's member) | service_principal.rs:578, route 560, const 47 | not exposed in TS | adapt: control DO, with principal config from a Secrets binding | **No idempotency**, so a retry mints a second grant. Absent from `definition()`, so a normal socket gets `unknown Control Plane function`. Service sockets get `service principals cannot call Control Plane Queries` / `Actions` (service_principal.rs:403, 438) |
| 62 | control.servicePrincipals.revokeDelegation | R, service socket only | {id} (exactly one key) | {updated:bool} | svc (own grants in its tenant) | revoked_at | `invalid arguments: id is required` | service_principal.rs:693, const 48 | not exposed in TS | adapt: control DO | An unknown id returns {updated:false} rather than an error |

Notes on the TS schemas. They are not validated at runtime; the client only exposes them (control.test.ts:36). They differ from Rust in these places:

- The session `tenants[]` items carry an extra `memberId`.
- `invitations.lookup` omits `invitationToken`.
- `tenants.updateProfile.description` is required in TS and optional in Rust.
- `teamIds` and `allowedAuthProviders` are required in both. In Rust they are effectively required (`must be an array`).

### C.2 Control schema

Disposition for every table: **adapt** into the Control DO at the placement shown, except `gonvex_scheduled_jobs` and `gonvex_storage_uploads`, which move to the tenant object (0012 R6).

Source: `rust/crates/store/src/control_schema.sql`. **36 tables, 1 trigger** (`gonvex_dashboard_security_change`). Placement means where each table lives: control DO = the control object; *other* = another section's owner.

| # | Table | Purpose | Placement |
|---|---|---|---|
| 1 | gonvex_runtime_projects | Project row: database_mode, auth_mode, owner_email (grants dev and pAdmin), project_key, status | control DO |
| 2 | accounts | Account per auth realm (= project): email, name, avatar, disabled_at | control DO |
| 3 | account_identities | Link from (project, provider, issuer, subject) to an account; verified_email; sign_in_provider | control DO |
| 4 | gonvex_runtime_tenants | Tenant directory: id, domain, status, timezone, profile, seat_limit, database_url/name/alias, deleted_at | control DO (the database_url becomes the DO id) |
| 5 | account_tenant_index | Routing projection (account, tenant) → member, status, revision. **It never grants access** | control DO |
| 6 | gonvex_runtime_manifests | Deployed module manifest and hash per project | other (deploy section) |
| 7 | gonvex_runtime_reducer_logs | Reducer log entries per project | other (logs section) |
| 8 | gonvex_project_members | Project developers by email: role owner, admin or dev | control DO |
| 9 | gonvex_project_invitations | Dashboard project invites (not used by `control.*`) | other (operator) |
| 10 | gonvex_dashboard_notifications | Dashboard inbox | other (operator) |
| 11 | gonvex_dashboard_accounts | Dashboard operator logins, with password_hash and session_revision (the trigger rotates it) | other (operator) |
| 12 | gonvex_account_access_tokens | Operator personal access tokens `gvx_pat_*` (operations.rs:3148) | other (operator) |
| 13 | gonvex_project_env | Project environment variables | other (deploy) |
| 14 | gonvex_auth_providers | Per-project identity provider config, with encrypted client secret and Firebase admin credentials | control DO |
| 15 | gonvex_auth_redirect_uris | Allowed app redirect URIs for native OAuth | control DO |
| 16 | gonvex_auth_transactions | Native OAuth state, PKCE challenge and nonce. Expires in 10 min and is deleted when consumed | control DO (or DO storage with a TTL) |
| 17 | gonvex_auth_codes | Native OAuth one-time codes, 5 min | control DO |
| 18 | gonvex_auth_sessions | Access tokens (sha256 hash): family, expires_at, revoked_at, last_seen_at | control DO |
| 19 | gonvex_auth_refresh_tokens | Refresh tokens (hash): family, used_at, revoked_at | control DO |
| 20 | gonvex_account_passwords | PBKDF2 hashes per (project, account) | control DO |
| 21 | gonvex_auth_identity_events | Audit of external identity resolution (`new` or `verified_email`) | control DO |
| 22 | gonvex_auth_membership_invitations | Tenant invitations with the handoff saga state | control DO |
| 23 | gonvex_member_login_provisioning | Resume checkpoint for provisionMemberLogin | control DO |
| 24 | gonvex_tenant_provisioning | Resume checkpoint for tenants.create | control DO |
| 25 | gonvex_control_idempotency | Control write receipts (results include plaintext tokens) | control DO |
| 26 | gonvex_agent_claim_tokens | agentAuth tokens | control DO |
| 27 | gonvex_control_settings | (project, kind, scope) → JSON: assistant.*, voice.*, tenant.exception | control DO |
| 28 | gonvex_support_sessions | Support session per connection (`support_<conn>`) | control DO |
| 29 | gonvex_support_commands | Support commands sent to a session | control DO |
| 30 | gonvex_scheduled_jobs | Scheduler jobs, crons and wakes | other (moves to tenant DO alarms) |
| 31 | gonvex_impersonation_grants | Support impersonation, developer mode and service delegations; reconnect token hash | control DO |
| 32 | gonvex_demo_accounts | Demo account registry | control DO |
| 33 | gonvex_error_events | Client error events | other (errors section) |
| 34 | gonvex_error_groups | Error groups (inbox) | other (errors section) |
| 35 | gonvex_performance_events | Client performance telemetry | other (telemetry) |
| 36 | gonvex_storage_uploads | Consumed proxy upload keys | other (storage) |

**Tenant-side objects this section depends on:**

- `members`, the authoritative membership.
- `_gonvex_control_plane_membership_outbox`, written by a tenant trigger.
- `_gonvex_sync_clock.revision`, which becomes the `admission_revision`.
- `_gonvex_reducer_idempotency`, used by invitation acceptance.

**Nothing prunes these tables in Rust** (kernel: `gonvex_control_idempotency` rows expire after 24 h, 0012 R9; the others keep Rust's behavior): gonvex_auth_sessions, gonvex_auth_refresh_tokens, gonvex_auth_codes (used or expired), gonvex_impersonation_grants, gonvex_control_idempotency, gonvex_auth_identity_events. Only gonvex_auth_transactions rows are deleted, when consumed (native_auth.rs:321). This matters for growth against the 10 GB per-object limit.

### C.3 Auth semantics

#### C.3.1 Token formats

Disposition for every row: **keep** (prefixes, lifetimes and storage are client-visible or survive migration); the tables move to the Control DO.

Every format is opaque, never a JWT. Only `sha256_hex(token)` is stored. `secure_token(kind)` produces `gvx_<kind>_<uuid32hex>.<base64url(32 random bytes)>` (control.rs:4262). The native OAuth helper produces `gvx_<kind>_<base64url(32 B)>` (native_auth.rs:851).

| Credential | Prefix | Lifetime | Store | Source |
|---|---|---|---|---|
| Access token (session) | `gvx_session_` | **15 min** (`expiresIn:900`) | gonvex_auth_sessions | control.rs:3867-3869, 4084 |
| Refresh token | `gvx_refresh_` | **30 days, absolute per family** (rotation keeps `expires_at`) | gonvex_auth_refresh_tokens | control.rs:475, 494, 3976 |
| Support impersonation grant | `gvx_imp_` (id `imp_*`) | **5 min**; single use | gonvex_impersonation_grants | control.rs:1023-1024 |
| Developer-mode grant | `gvx_imp_` (id `devmode_*`, reason `developer mode`, actor = target) | **30 min** | gonvex_impersonation_grants | control.rs:3144-3146; store/src/lib.rs:294 |
| Service delegation grant | `gvx_imp_` (id `svcdel_*`) | 30 s up to the principal max (default **900 s**, configurable 60..3600) | gonvex_impersonation_grants | service_principal.rs:645; config.rs:84-85 |
| Reconnect credential for any redeemed grant | `gvx_dev_` | until the grant expires; **rotated on every reconnect**; bound to `used_connection_id` | `reconnect_token_hash` | store/src/lib.rs:543-660 |
| Service principal credential | `gvx_svc_` | static; only its digest is configured | `GONVEX_SERVICE_PRINCIPALS` JSON | service_principal.rs:46-127 |
| Invitation token | `gvx_invite_` | 7 days | invitations | control.rs:2950 |
| Agent claim token | `gvx_agent_claim_` | 1..86400 s | gonvex_agent_claim_tokens | control.rs:3076 |
| Native OAuth state / nonce / code | `gvx_oauth_`, `gvx_nonce_`, `gvx_authcode_` | 10 min / – / 5 min | auth_transactions / auth_codes | native_auth.rs:36-37 |
| Operator credentials | `gvx_pat_`, a signed dashboard session, `GONVEX_ADMIN_KEY`, or `gvx_session_` in `dashboard_auth_project_id` | operator section | — | operations.rs:3122-3160 |

#### C.3.2 Passwords

- **Algorithm.** PBKDF2-HMAC-SHA256, **210,000 rounds**, 16-byte random salt, 32-byte key. Stored as `pbkdf2_sha256$<rounds>$<b64url salt>$<b64url hash>` (control.rs:4136). Verification reads the rounds from the string and compares in constant time (4114).
- **Policy.** At least 12 characters, counted in bytes. Allowed only when `auth_mode` ∈ {gonvex-native, hybrid}.
- **Cloudflare.** **Workers WebCrypto PBKDF2 refuses more than 100,000 iterations**, and stored hashes use 210,000. Decided in `contract-changes/0007-password-hash-pbkdf2-rounds.md`: verify with a non-WebCrypto PBKDF2 at the stored round count, never rehash at lower cost, and stop under Rule 7 if one verification costs more than 500 ms CPU.

#### C.3.3 Provider secrets

- AES-256-GCM. The key is `SHA-256("gonvex-control-secret-v1\0" ‖ GONVEX_DASHBOARD_SESSION_SECRET)`, the AAD is `gonvex-auth-provider`, and the stored blob is `nonce(12) ‖ ciphertext` (control.rs:4661-4738).
- On Cloudflare: **adapt** with a Secrets binding and WebCrypto AES-GCM. Keep the byte format so imported rows decrypt.

#### C.3.4 External tokens: Firebase and external-oidc

Source: external_auth.rs.

- **Algorithm and checks.** RS256 only, with a `kid`. Required claims are `exp`, `iat`, `sub`, `iss` and `aud`, with 30 s leeway, and `iat ≤ now + 60`.
- **Firebase extras.** `auth_time` is required and must be ≤ now + 60. `firebase.tenant` must equal the configured `firebase_tenant_id`.
- **Firebase admin check.** If admin credentials are configured, every exchange signs a service-account JWT and calls Google `accounts:lookup`. A disabled user, or `auth_time < validSince`, is rejected (343-446).
- **JWKS cache.** Per URL, using `max-age` clamped to 30 s..1 h (default 5 min). `no-store` / `no-cache` are not cached. An unknown `kid` forces one refetch.
- **JWKS fetch safety.** HTTPS, no userinfo, not `localhost`, resolved to a public IP and pinned, no redirects, 5 s timeout (456-498).
- **Account resolution** (control.rs:3680):
  1. An existing identity row updates email, name and picture.
  2. Otherwise a verified email links to the single account with that verified email. More than one match is an error.
  3. Otherwise a new account is created, subject to `signup_mode`. `inviteOnly` requires a verified email with a pending, unexpired invitation (3898).
- **Cloudflare: adapt.** Worker `fetch`. A pinned-IP resolve is not available, but Workers cannot reach private networks, so keep the scheme and host checks. Cache JWKS in DO memory or KV. RSA verification and signing through WebCrypto.

#### C.3.5 Native OAuth (HTTP, not `control.*`)

Source: native_auth.rs:39-50. CORS is `*`.

- **Routes.** `GET /auth/config?project=` reports `accessTokenLifetimeSeconds` 900 and `refreshTokenLifetimeSeconds` 2592000. The others are `GET /auth/{google|microsoft|apple}/authorize`, `GET|POST /auth/{provider}/callback`, `POST /auth/token` (`grantType` authorization_code or refresh_token, PKCE S256 with a 43-128 character verifier) and `POST /auth/logout`.
- **Redirect rule.** A redirect URI must be registered in gonvex_auth_redirect_uris and use HTTPS, except for localhost.
- **Callback URL.** `GONVEX_AUTH_URL` or `GONVEX_PUBLIC_URL` + `/auth/<p>/callback`. A Google fallback client comes from `GONVEX_GOOGLE_CLIENT_ID` / `_SECRET`.
- **Microsoft emails** are treated as verified (386-388).
- **`/auth/token`** writes no idempotency row.
- **Cloudflare: adapt.** Worker router routes that RPC to the control DO.

#### C.3.6 Refresh and rotation

Disposition: **keep** (lifetimes, rotation and reuse detection are client-visible); state in the Control DO.

- Each refresh marks the old token used and issues a new pair in the same family.
- Reuse within 5 s returns an error and keeps the family.
- Reuse after 5 s revokes the whole family (sessions and refresh tokens; control.rs:3963-3972).
- Refresh does not revoke the old access token.
- `exchangeExternalToken` with `previousRefreshToken` revokes the previous family.

#### C.3.7 Revocation sources

Disposition: **keep** every source; propagation to sockets is **adapt** per C.3.8 item 10 (≤30 s).

All of these take effect on live sockets within 30 s (C.3.8):

- `auth.logout` (one family, or `all`).
- `accounts.resetMemberPassword` (every session of the target).
- `accounts.disabled_at` (set by `demos.delete`).
- Refresh-token reuse.
- Grant `revoked_at`: `developer.exit`, `servicePrincipals.revokeDelegation`.
- Grant expiry.
- Tenant deleted or disabled, and member not active.

`accounts.updatePassword` does **not** revoke anything.

#### C.3.8 Socket authentication and re-authentication in place

Disposition for every item: **keep** (frames, codes, ordering and preservation rules), with the session held by the object that owns the socket (0004). Item 10 states its own adapt.

`auth` frame: `{type:"auth",id,token?,project?,tenant?,controlOnly?,public?,clientContract?,device?,capabilities?}` (protocol index.ts:321). The handler is at lib.rs:4464. Branches:

1. **Service credential.** `token` starts with `gvx_svc_` → C.1b service socket (lib.rs:2714). The principal must list the project, and project and tenant are required. Every failure returns the same error text, `service credential is invalid or not permitted for this project`.
2. **Public.** `public:true` without a token → anonymous tenant session (lib.rs:4489). See C.5.
3. **Control-only without a token.** `controlOnly` with no token → requires `project`. The connection has only a `project_id`, so only `anon` functions work.
4. **No token otherwise** → `a Gonvex app session is required`.
5. **Grant token.** `gvx_imp_` / `gvx_dev_`:
   - Consumes a `gvx_imp_` once, or rotates a `gvx_dev_`, and returns a new `developerSessionToken`.
   - The project and tenant must match the grant.
   - Developer grants use `admit_developer`; all others use `admit_member`.
   - The result includes `impersonationId`, `impersonatorId` and `localIdentity`.
   - The control-only form requires a tenant: `developer grants require an active tenant`.
6. **Control-only session.** `controlOnly` with no tenant (a tenant is kept only with the `capabilities.tenantControl=1` capability, lib.rs:4412) → `load_session_identity`. The token must start with `gvx_session_`, be unexpired, unrevoked and in the requested project, and the account must be enabled.
7. **Full session.** Otherwise `authenticate_session` → identity, then the tenant (the requested one, else `first_admitted_tenant`), then `admit_member`. The result includes `{projectId,accountId,tenantId,artifactHash,replica,localIdentity}`.
8. **Error codes.** `auth.error.code` is `credential_rejected` (InvalidSession, SessionProjectMismatch), `member_removed` (MemberNotFound, NoTenantMembership) or `transient` (lib.rs:4453-4461).
9. **Re-authentication in place.** Any later `auth` frame on the same socket re-runs the whole flow.
   - Subscriptions are kept, and the response adds `subscriptionsPreserved:true`, only when **all** of these hold: the client sent `capabilities.preserveSubscriptions=1`; it is not control-only; it is not anonymous; and the project, identity, impersonation id, route, member, delegation and developer session are all identical (lib.rs:5217).
   - Otherwise replicas, live queries, Control subscriptions and the feed are reset.
   - Each auth attempt is bounded by `GONVEX_AUTH_TIMEOUT_SECONDS` (10 s). On timeout the socket closes silently (lib.rs:2722-2735, 2788-2805).
   - If the module declares a `client_contract` that differs from the client's `clientContract` on a non-control tenant socket, the server sends `client.updateRequired` and closes (lib.rs:2875-2895).
10. **Revalidation, every 30 s** (`auth_revalidation::INTERVAL`; a constant, not an env var).
    - Plain `gvx_session_` sockets join one process-wide batched round, 500 hashes or accounts per statement (`valid_session_hashes`, `active_member_accounts`, store/src/lib.rs:436-508).
    - Impersonation and developer sockets keep a per-socket timer (`revalidate_connection`, lib.rs:3927). `validate_impersonation_session` checks that the grant is not revoked or expired, matches the same `used_connection_id`, and is still admitted. It does not rotate the credential.
    - On failure the server sends `auth.error{code:"transient",id:"session-expired",error:"authentication expired or was revoked"}` and resets the connection completely (lib.rs:2039-2079).
    - The revalidation round also writes `gonvex_auth_sessions.last_seen_at` at most every 5 min. **Nothing reads that column**, so drop it under the DOD rule (adapt).
    - **Cloudflare: adapt.** Each socket-owning DO batches its sessions to the control DO by RPC every 30 s (alarm). The ≤30 s revocation latency is contract.
11. **Service sockets** are not revalidated after auth (service_principal.rs). Delegated member sockets are revalidated like any other session.

#### C.3.9 Other auth hazards in the reference

Ruled in `contract-changes/0012-p00-lead-rulings.md` (R9, R10):

- **Plaintext secrets in idempotency rows.** Auth Actions store the complete session result in `gonvex_control_idempotency.result`, and a replay with the same (subject, key) returns it **before** credentials are checked (control.rs:407-419). The same applies to impersonation, invitation and agent tokens. Each passwordLogin subject is `sha256(email)` plus a client-chosen key.
- **Replays can return dead tokens.** A replay can return tokens that are already expired or revoked.
- **No rate limit on Control auth Actions** on the member transport.

### C.4 Session semantics

Disposition for every item: **keep**; the tenant scan in item 2 is **adapt** per 0012 R15.

1. **Creating a session.**
   - Login (passwordLogin, exchangeExternalToken, refreshSession, or `/auth/token`) returns the Session object.
   - `tenants[]` comes from `account_tenant_index` (status active, tenant not deleted or disabled), **re-admitted against each tenant's `members` row**. Each item includes `memberId`. Sorted by `lower(name), tenant_id` (control.rs:4001).
   - `activeTenantId` matches the requested `tenantId` by id or domain. Otherwise it is the first tenant, or `""`.
   - Login does not open a socket session. The client then sends `auth` with the access token.
2. **Selecting a tenant on a socket.** Use `auth.tenant` (id or domain; `resolve_tenant` prefers an exact id). Without it, the server uses `first_admitted_tenant`: it scans **all** project tenants, directory-listed ones first, then `lower(name)`, and returns the first one `admit_member` accepts (store/src/lib.rs:754). Otherwise `NoTenantMembership` → `member_removed`.
3. **Switching tenants.** Send a new `auth` frame with another `tenant` on the same socket; a different route never preserves subscriptions. Control-only sockets carry a tenant only with `tenantControl=1`.
4. **Admission.** `admit_member` (store/src/lib.rs:981) is the only gate. It reads `members WHERE account_id=? AND status='active'` and `_gonvex_sync_clock.revision` in one read snapshot. The directory only routes. Disabled or deleted tenants fail in `resolve_tenant`. Developer grants fall back to an ephemeral `developer:<acct>` admin member (store/src/lib.rs:1015). Service sockets use a synthetic `_gonvex_service:<id>` member with role `system`.
5. **Membership changes in the middle of a session.**
   - **(a) From the feed.** A committed tenant transaction with revision > `admission_revision` changes a `members` row whose `account_id` or `id` belongs to this session, in one of the columns `account_id`, `status`, `role`, `permissions` or `membership_revision` (lib.rs:4270). The server then discards the tenant scope (replicas, live queries, Control subscriptions, feed and presence) and sends `auth.error{code:"transient",id:"membership-changed",error:"tenant membership changed; authenticate again"}` followed by that transaction's watermark (lib.rs:2185-2192, 4260). This covers role and permission changes, not only removal.
   - **(b) After the socket's own commit.** After a Reducer or Action on the socket, the server re-reads its own Member (`member_by_id`). If access changed, it sends the same frames **before** the watermark (lib.rs:3317-3335, 4311).
   - **(c) Revalidation round.** If the member is no longer active, the round sends `session-expired` within 30 s.
   - **(d) TenantAdmin Control calls** re-admit on every call.
   - After (a) or (b), `control_connection` keeps its identity and token. The client is expected to send `auth` again.
6. **Directory projection.** A tenant trigger writes `_gonvex_control_plane_membership_outbox`.
   - The `MembershipProjector` loops over every project and tenant. It reads ≤1000 rows, upserts `account_tenant_index` with a revision guard, then deletes the outbox rows.
   - Its idle delay backs off 2 s → 10 s, and a `wake()` from Control sagas cuts it short (membership_projector.rs:33-185).
   - It notifies `ControlChanged`.
   - **Cloudflare: adapt.** The tenant DO pushes to the control DO after commit and retries from an alarm.
7. **Developer and support impersonation.** Covered in C.3.1. `developer.status.mode` reports the impersonation flag. `dev` and `pAdmin` calls are blocked while impersonating; `acct` and `tAdmin` calls run as the target.

### C.5 Admission semantics

**K** marks a tuning knob (not contract). **C** marks behavior that is contract (error texts, codes, ordering).

| Limit | Value / variable | Applies to | On exceed | Source | K/C | Cloudflare |
|---|---|---|---|---|---|---|
| Pending (unauthenticated) sockets per IP bucket | `GONVEX_PENDING_CONNECTIONS_PER_IP`=100 | every `/ws` upgrade, both transports, until a successful identity or tenant auth | HTTP 429 `pending connection limit exceeded` | lib.rs:1582-1591; config.rs:483 | K (value), C (429 + text) | adapt: per-object counter in the object that owns the socket (0012 R11) |
| Auth deadline | `GONVEX_AUTH_TIMEOUT_SECONDS`=10 | sockets still pending, measured from connect; also each later `auth` attempt | close 1008 `authentication deadline exceeded`; a timed-out attempt closes silently | lib.rs:1680, 2029-2034 | K, C (1008) | adapt: DO alarm. A token-less control-only socket (the login page) never leaves pending, so **login must finish within 10 s of connecting** or the socket closes |
| Public sockets per IP | `GONVEX_PUBLIC_CONNECTIONS_PER_IP`=20, reserved at upgrade | `/ws?public=1` | HTTP 429 `public connection limit exceeded` | lib.rs:1592-1606 | K/C | adapt (as above, 0012 R11) |
| Public calls per connection | `GONVEX_PUBLIC_CALLS_PER_CONNECTION`=30 per 60 s fixed window | public transport; the cost of a batch is its call count; auth frames count | correlated `... public call rate limit exceeded` error (auth.error / query.error / ...) | public_functions.rs:24-27 (defaults), 52-62 (window); lib.rs:2660-2690, 4349 | K/C | adapt: socket-DO memory |
| Public calls per IP | `GONVEX_PUBLIC_CALLS_PER_IP`=120 per 60 s; windows evicted after 120 s; at most 16,384 buckets, oldest evicted | public transport | same | public_functions.rs:64, 157-180 | K/C | adapt: per-object counter in the object that owns the socket (0012 R11) |
| Public payload | `GONVEX_PUBLIC_MAX_PAYLOAD_BYTES`=65536 (also the WebSocket max message and max frame) | public transport, and any unscoped or anonymous socket | close 1009 `public payload too large` | lib.rs:1608-1617, 2603-2618 | K/C | adapt |
| Member transport frame and message caps | max message 64 MiB, max frame 16 MiB | `/ws` | transport close | lib.rs:1608-1617 | K | adapt: kernel limit 32 MiB, the platform maximum (0012 R21) |
| Public Action deadline | `GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS`=60 | anonymous Actions | Action error | config.rs:493 | K | adapt |
| Public admission recheck | ≤30 s cache; a fresh check before tenant-code frames; background monitor every 30 s; a revoked socket stays revoked | anonymous sessions | `auth.error{code:"transient",id:"session-expired",error:"public tenant session is unavailable"}` then close; call frames get the same text | lib.rs:2851-2864, 2694-2710, 3877; public_functions.rs:215-276 | C (30 s is hard-coded) | adapt: DO alarm |
| Public scope pinning | first successful public auth pins (project, canonical tenant) | public sockets | `public connection scope is fixed` + close 1008 | lib.rs:2823-2850 | C | adapt |
| Transport mismatch | the `public` flag must equal the transport | all | `public authentication requires the public WebSocket transport` + close | lib.rs:2622-2637 | C | adapt |
| Public tenant resolution | needs an **active** directory row (status=`active`, not deleted). Id beats domain; case-insensitive | public auth, `developer.enter` | `public tenant session is unavailable` (identical for every failure) | store/src/lib.rs:965 | C | adapt: control DO lookup, cacheable in KV |
| Client IP | trusted only if the peer is in `GONVEX_TRUSTED_PROXY_CIDRS` ∪ `GONVEX_PUBLIC_TRUSTED_PROXY_IPS`, then `GONVEX_CLIENT_IP_HEADER` (default `x-real-ip`; XFF takes the right-most untrusted). IPv6 buckets are /64; IPv4-mapped addresses become IPv4 | all limiters | — | public_functions.rs:66-79, 182-213 | K | replace: `CF-Connecting-IP` is platform-trusted |
| Reducer batch size | ≤256 calls per `reducer.callMany` | all | `reducer batch cannot contain more than 256 calls` (Rejected) | lib.rs:3352 | C | keep |
| Cold replica opens | active = `GONVEX_REPLICA_SNAPSHOT_CONCURRENCY` (default max(cpus, 2)) + `GONVEX_REPLICA_OPEN_MAX_WAITING` (default 256); non-blocking | replica opens | `too many pending replica opens` | open_admission.rs:23-97; offload.rs:1020 | K | replace with DO-local bounds |
| TCP accept tuning | SO_SNDBUF 256 KiB, TCP_NOTSENT_LOWAT 128 KiB, nodelay, backlog 4096 | listener | — | accept.rs:27-61 | K | replace (platform-owned; not applicable) |
| Session revalidation | 30 s interval, 500 per statement, `last_seen_at` at most every 5 min | authenticated sockets | `session-expired` | auth_revalidation.rs:30-32 | C (≤30 s), K (chunk) | adapt (C.3.8 item 10) |
| Per-account or per-member socket cap | **none** | — | — | — | — | keep (none) |
| Login / Control Action rate limit | **none** on the member transport | — | — | — | — | keep none in stage 1; V06 (0012 R10) |

All limiters are **process-local** (public-functions.md, "Abuse limits"). On Cloudflare they become per-object counters in the object that owns the socket (0012 R11); a deployment-wide budget is not contract today.

## D. HTTP routes and the /dev/sync deploy flow

Reference: Rust runtime at `0bf3ad13`. All paths are relative to `rust/crates/runtime/src/` unless they start with `packages/`, `rust/crates/store/` or `docs/`. Where code and docs disagree, the code is what this section records, and the disagreement is flagged (**DOC≠CODE**).

### D.0 Router shape

Disposition: routes and status codes **keep**; the router is the Router Worker (adapt). `dashboard::invalidate` is **adapt**: any successful non-GET `/dev/*` write notifies dashboard subscriptions in the Control DO. The 64 MiB JSON/form body limit is **keep** up to the Worker request-body limit of the plan; S-series spikes record that limit.

- One axum router, `Runtime::router()` at `lib.rs:552-583`. The process serves nothing else (`main.rs:173-178`). It has no separate metrics or admin listener.
- The router merges `native_auth::router()` (`native_auth.rs:39-50`), `operations::router()` (`operations.rs:46-126`) and `operator_data::router()` (`operator_data.rs:23-35`). There is no `.nest` and no `.fallback`, so an unmatched path gets axum's 404 and a wrong method gets 405.
- Layers, outermost first (`lib.rs:576-581`):
  1. `dashboard::invalidate` (`dashboard.rs:28-43`): any successful non-GET/HEAD/OPTIONS request under `/dev/*` broadcasts `dashboard_changes`, which re-runs every dashboard subscription. This includes `/dev/sync`.
  2. `dev_cors` (`lib.rs:609-643`): every `OPTIONS /dev/*` returns 204 before routing. Responses on `/dev/*` get `ACAO:*` and `Allow-Headers: Authorization, Content-Type, X-Gonvex-Key, X-Gonvex-Project-Key, X-Gonvex-Project-Id, X-Gonvex-Tenant-Id`, plus `Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS` (`lib.rs:627-630`) and max-age 600.
  3. `DefaultBodyLimit` of 64 MiB. It applies to `Json` and `Form` extractors only. The storage upload reads the raw `Body` with its own 128 MiB cap (`lib.rs:818`).
- `/auth/*` adds its own CORS through `auth_cors` (`native_auth.rs:56-74`). `/storage/*` uses `storage_cors` (`lib.rs:863-884`).
- The same router is re-entered in-process by the dashboard socket: `dashboard::read_router` (`dashboard.rs:95-102`) runs `oneshot` GETs against the operations, operator_data, `/dev/projects` and `/dev/tenants` handlers.

**Not HTTP at all.** These are listed so nobody looks for them as routes:
- Public functions run over `/ws?public=1` (`lib.rs:1580`, `public_functions.rs`).
- Telemetry, error envelopes and performance events are WebSocket frames (`lib.rs:3698-3823` → `telemetry.rs:59,142,224`).
- Data ingest is a sandbox import (`sandbox.rs:617` → `data_ingest.rs:21`).
- Password login, Firebase and OIDC token exchange, refresh and logout for tenant users are WebSocket `control.auth.*` calls: `control.auth.passwordLogin` at `control.rs:421`, `exchangeExternalToken` at `control.rs:480`, `refreshSession` at `control.rs:520`, `logout` at `control.rs:2576`.
- **Webhooks and HTTP actions have no route in the runtime.** `packages/module-sdk/README.md:260-263` says an external adapter verifies the webhook and then invokes an Action. Package K18 has no Rust reference.

**Auth legend.**
- **none**: no credential.
- **op(perm)**: operator credential through `operations::authorize` with the PAT permission named (`operations.rs:3184`).
- **op+access** / **op+manage**: an operator credential plus `can_access_project` or `can_manage_project` (`operations.rs:3203,3231`).
- **pk|op(perm,access|manage)**: `authorize_project_resource` (`operations.rs:3046`). A project key sent as Bearer passes. Otherwise it falls back to an operator check.
- **admin-key**: `GONVEX_ADMIN_KEY` as Bearer (`operations.rs:3259`).
- **sync-key**: the project key, or `GONVEX_DEV_SYNC_KEY` when the project has no key. Sent as Bearer or `x-gonvex-key`.
- **svc**: a service principal `gvx_svc_` token.
- **signed URL**: an HMAC query signature.
- **in-band**: authentication happens in the first WebSocket frame.

The full model is in D.3.

### D.1 Route table

Disposition values: keep, adapt (with the Cloudflare mechanism named), replace (with what), defer (with why). Abbreviations used below:
- RW: router Worker.
- CDO: control Durable Object.
- TDO: tenant Durable Object.
- IW: ingress Worker.
- Q: Queues.

#### Health and version

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/healthz` | `health` | none. Full diagnostics need an `admin`-role, non-Local operator holding `admin:projects` | — | 200 or 503 `{ok}`. With diagnostics: `{ok,version,time,databaseSet,database,valkeySet:false,rowsCache:false,runtimeManifests{ready,failedProjects},moduleHost,s3Set,sandbox{enabled,ready,error}}`. Headers `cache-control:no-store`, `vary:Authorization` | `lib.rs:554`, `1499-1563` | adapt: RW answers `{ok}` from a CDO ping. Diagnostics are rebuilt from CDO and R2 state | `ok` = module host ready ∧ control plane present ∧ no failed project manifests ∧ sandbox binary present. `version` comes from `GONVEX_RUNTIME_VERSION`, and the deploy docs use `/healthz.version` (`apps/docs/content/docs/deployment.mdx:108`). Pool, module-host and sandbox fields have no Cloudflare equivalent and need new values |

There is no separate version route.

#### WebSocket upgrades

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET (Upgrade) | `/ws` | `websocket_upgrade` → `websocket` | in-band `auth` frame: session token, project key, svc token or public. Upgrade itself needs none | query `public=1` selects the anonymous transport. Other query params are ignored | 101. 429 `pending connection limit exceeded` or `public connection limit exceeded` | `lib.rs:555`, `1565-1641`; auth frames at `lib.rs:2714`, `2775` | adapt: RW → TDO, or → CDO for `controlOnly` sockets (K05) | **Project and tenant are not in the URL.** They arrive in the first `auth` frame, and a re-`auth` can switch tenant on the same socket (`lib.rs:2967`). The router cannot pick the Durable Object from the upgrade request alone. Limits: non-public 64 MiB per message and 16 MiB per frame. Public sockets use `GONVEX_PUBLIC_*` (`lib.rs:1605-1614`). Per-IP pending and connection slots apply. permessage-deflate is negotiated, and Workers cannot negotiate it. The full protocol is in another section |
| GET (Upgrade) | `/dev/dashboard/ws` | `dashboard::upgrade` → `connection` | in-band `auth{token}` → `operations::authorize(...,"")`. Upgrade needs none | frames `auth`, `query.subscribe{path:"dashboard.read",args:{resource,project?,tenant?,projectKey?}}`, `query.unsubscribe` | `session.ready{capabilities:{protocolVersion:2}}`, `auth.result{result:{system:"dashboard"}}`, `auth.error`, `query.result{reason:initial\|change}`, `query.error` | `lib.rs:556`; `dashboard.rs:45-52`, `149-219` (loop), `222-330` (watch), `66-93` (allowlist) | adapt: CDO re-runs the same read handlers. Change triggers for `/dev/data/*` come from TDO change-log subscription | Max message 64 KiB, at most 64 subscriptions, results capped at 8 MiB, snapshot timeout 15 s. Re-reads on any `/dev` write, on runtime events, on the tenant change feed for data reads, and every 30 s, or every 3 s for logs and errors. Allowlist: `/dev/projects`, `/dev/tenants`, `/dev/manifest`, `/dev/data/tables`, `/dev/storage/files`, `/dev/auth/notifications`, `/dev/auth/tokens`, `/dev/logs`, `/dev/errors/groups`, `/dev/projects/{id}/members`, `/dev/data/tables/{t}/rows`. `/dev/logs` has no GET route (405), so subscribing to it always errors. The `projectKey` arg is set as `x-gonvex-key`, which `project_key_matches` never reads because it reads only Bearer (`operations.rs:3069`), so it has no effect |
| GET (Upgrade) | `/dev/metrics/stream` | `metrics_stream` | op(`projects:read`)+access. The token arrives in `Sec-WebSocket-Protocol: gonvex-dashboard-auth.<token>` and is re-checked every 2 s | query or header `project` | `{type:"metrics",metrics}` every 2 s | `operations.rs:61`, `1814-1876` | replace: metrics from Analytics Engine or per-DO counters (M05), same frame shape | Echoes the subprotocol |
| GET (Upgrade) | `/dev/logs/stream` | `log_stream` | `?key=` must equal the admin key or the project key. **The credential is in the URL.** Operator sessions are not accepted | query `project`, `key`, `replay` (default 1) | `{type:"ready"}`, then `{type:"log",log:{time,operationId,project,tenant,accountId,path,kind,outcome,durationMs,error,reason,source:"client"}}` | `operations.rs:62`, `1878-2008` | adapt: CDO socket. Rows live in the control plane's `gonvex_performance_events` | Polls every 500 ms. Replays the last 200 rows. Used by the CLI (`packages/gonvex/src/index.ts:2049`) |

#### Native OAuth (tenant end users)

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/auth/config` | `auth_config` | none | `?project` | `{project,providers[google\|microsoft\|apple],databaseMode,signupMode,accessTokenLifetimeSeconds:900,refreshTokenLifetimeSeconds:2592000}`. 404 when the project is inactive | `native_auth.rs:41`, `93-163` | adapt: RW → CDO (K06) | Providers are listed only when `auth_mode` is `gonvex-native` or `hybrid` |
| GET | `/auth/{provider}/authorize` | `authorize` | none. PKCE S256 and a 16-512 character state are required | `?project,redirect_uri,state,code_challenge,code_challenge_method=S256` | 307 to the provider: `client_id`, `redirect_uri=GONVEX_AUTH_URL/auth/{p}/callback`, `scope`, `state`, `nonce`. Microsoft adds `response_mode=query`, Apple adds `form_post` | `native_auth.rs:42`, `174-281` | adapt: CDO (K06) | Inserts `gonvex_auth_transactions` with a 10-minute TTL. The redirect URI is normalized and checked against the provider configuration |
| GET, POST (form) | `/auth/{provider}/callback` | `callback_get` / `callback_post` → `finish_callback` | provider callback. Single-use state (DELETE…RETURNING) | query or form `state,code,error` | 307 to `redirect_uri?code=<authcode>&state=` or `?error=` | `native_auth.rs:43-46`, `283-435` | adapt: CDO. Apple's POST must not hit the dev CORS path | Exchanges the code, verifies the id_token and nonce, then resolves or creates the account. Auth code TTL is 5 minutes |
| POST, OPTIONS | `/auth/token` | `token` | grant: PKCE code or refresh token | JSON `{grantType:authorization_code\|refresh_token,project,code?,codeVerifier?,redirectUri?,refreshToken?,tenant?}` | session result JSON from `session_result_from_directory`. 401 on an invalid grant. Reuse of a refresh token revokes the family | `native_auth.rs:47`, `437-518` | adapt: CDO (K06) | |
| POST, OPTIONS | `/auth/logout` | `logout` | optional Bearer access token | JSON `{refreshToken?,all?}` | always `{ok:true}` | `native_auth.rs:48`, `578-625` | adapt: CDO | `all` revokes every session of that account |

#### Storage and files

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/storage/{*key}` | `storage_download` | signed URL: `exp`,`sig`. HMAC-SHA256 keyed `"gonvex-storage:"+S3 secret` over `"{key}\n{exp}"`. `upload` must be absent | `?exp&sig`. `Range` header honoured | 200 or 206 streamed. Forwards `accept-ranges`, `content-length`, `content-range`, `content-type`, `etag`, `last-modified`. Inline allowlist (png/jpeg/gif/webp/avif, audio, mp4/webm/ogg, pdf), everything else `attachment` + `octet-stream`, `nosniff`, a CSP (sandbox when not inline), `cache-control: private, max-age=300`. 403, 404 or 502 | `lib.rs:560-566`, `653-778`; `storage.rs:365-380`, `883-905` | **replace** (IB1, 0012 R13): RW → R2 range GET (K13) after a `_gonvex_files` lookup that refuses a deleted file or a private file whose URL was not minted for its owner | **The signature is the only check, and it is a bearer capability.** URLs are minted by WebSocket `storage.getUrl` and `generateDownloadUrl` (`storage.rs:220-246`), and neither checks the owner. The only owner check is `read_file` for sandbox imports (`storage.rs:341`). Intended behavior: check that the caller owns a private file before minting any download URL and before `delete` (`storage.rs:248-268`). Also `owner()` lets a caller store any `ownerId` (`storage.rs:934-943`). Download URLs last 10 minutes by default |
| POST, PUT | `/storage/{*key}` | `storage_upload` | signed URL with `upload=1`, keyed `"gonvex-storage-upload:"+secret` | raw body, at most 128 MiB. `Content-Type` | `{storageId:<last key segment>}`. 409 `upload already consumed`. 413 `upload too large` (`lib.rs:813-816`). 403 or 502 | `lib.rs:780-841`; `storage.rs:382-492` | adapt: R2 put with `onlyIf` no-etag. The single-use claim moves from control-plane `gonvex_storage_uploads` to a TDO (0012 R6) | Single use: a ticket row is claimed before the PUT and released if the PUT fails. `If-None-Match:*` blocks overwrite. The tenant `_gonvex_files` row must still be `pending`. Upload URLs last 15 minutes. URLs are proxied only when `GONVEX_PUBLIC_URL` is set; otherwise the client gets a presigned S3 URL (`storage.rs:203-206`) |
| OPTIONS | `/storage/{*key}` | `storage_options` | signed upload URL | same query | 204 or 403 | `lib.rs:843-861` | adapt: RW | A preflight without a valid signature returns 403 |
| GET | `/dev/storage/files` | `storage_files` | pk\|op(`projects:read`,access) | `?project` or `x-gonvex-project-id` | `{configured,bucket,files:[{id,key,size,contentType,uploadedAt,url}]}`, at most 1000 | `operations.rs:65`, `1156-1184`; `storage.rs:494-572` | adapt: CDO fans out to TDO pages, or lists R2 by prefix | Loops over every tenant database in turn. Every row gets a signed download URL whatever its visibility. Keep paging |

#### Webhooks and HTTP actions

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| any | (none) | — | — | — | — | absent. See `packages/module-sdk/README.md:260-263` | replace: new IW (K18) that verifies the provider signature on the raw body, resolves the tenant and invokes an Action through a service principal (0002) | Telnyx and GitHub are named in K18's `done_when` |

#### Deploy (developer / CLI)

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| POST | `/dev/sync` | `dev_sync` → `apply_dev_sync` | sync-key (Bearer or `x-gonvex-key`). **Operator credentials are not accepted** | JSON Manifest plus header `x-gonvex-project-id`. See D.2 | 200 `{ok:true,project,functionCount,schemaDefinition,runtimeReloaded:true}`. Errors `{error}` with 400, 401, 422, 500 or 503 | `lib.rs:567`, `1358-1497` | adapt: RW → CDO. Bundle in R2. Tenant migrations through Q → TDO. Activation in the CDO (K19) | See D.2 |
| GET | `/dev/manifest` | `manifest` | svc (when `manifest:true` and the project is listed) or pk or op(`projects:read`)+access | `?project` or `x-gonvex-project-id` | `{project,functions,schema,visibility,module:{hash,generation}}`. A svc caller gets the body without `schema` and `visibility` | `operations.rs:58`, `1473-1546`; `service_principal.rs:214-232` | adapt: CDO (active release) | The CLI polls it to detect drift (`packages/gonvex/src/index.ts:1873-1887`). The API gateway reads it with a svc token |
| POST | `/dev/internal/e2e/members` | `provision_e2e_member` | admin-key | JSON `{projectId,tenantId?,email,name?,password?,accountOnly?}` | `{projectId,accountId}` or `{projectId,tenantId,accountId,memberId}`. 404 or 422 | `lib.rs:575`, `1145-1356` | adapt: CDO, then TDO membership upsert. Test-only | Creates an account (with a password of 12+ characters for native or hybrid auth), upserts a tenant member with role `admin` and `{"e2e":true}`, then `account_tenant_index` |

#### Operator: account, tokens and notifications

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| POST | `/dev/auth/login` | `login` | none (email + password) | JSON `{email,password}` | `{session:{email,name,role,provider:"gonvex",expiresAt,accessToken,sessionRevision,authority}}` | `operations.rs:48`, `196-278` | adapt: CDO | Checks the configured `DASHBOARD_AUTH_ACCOUNT` and password first, then `gonvex_dashboard_accounts` (PBKDF2, 210k rounds). Returns a token HMAC-signed with `GONVEX_DASHBOARD_SESSION_SECRET`, or the admin key when that is unset. Lasts 7 days |
| GET | `/dev/auth/me` | `me` | op (any) | — | `{account:{email,name,role},authentication,permissions}` | `operations.rs:49`, `280-296` | adapt: CDO | `authentication` is one of session, native-session, personal-access-token, admin-key or local |
| GET | `/dev/auth/accounts` | `list_accounts` | op(`admin:projects`) and role `admin` | — | `{accounts:[…]}` | `operations.rs:52-55`, `298-332` | adapt: CDO | |
| POST | `/dev/auth/accounts` | `create_account` | same | JSON `{email,name,password,role}` | `{account:{email,name,role}}` | `operations.rs:52-55`, `341-403` | adapt: CDO | Upserts `gonvex_dashboard_accounts` |
| GET | `/dev/auth/tokens` | `list_tokens` | op(`tokens:read`) | — | `{tokens:[…]}` (own tokens only) | `operations.rs:50`, `1186-1220` | adapt: CDO | |
| POST | `/dev/auth/tokens` | `create_token` | op(`tokens:create`) | JSON `{name,permissions[],expiresAt}` | 201 `{token:{id,name,prefix,permissions,createdAt,expiresAt},accessToken}` | `operations.rs:50`, `1230-1324` | adapt: CDO | Format `gvx_pat_…`, stored as a SHA-256 hash. `normalize_permissions` stops a holder granting `*` or `admin:projects` beyond its own (`operations.rs:3463-3520`) |
| DELETE | `/dev/auth/tokens/{token}` | `revoke_token` | op(`tokens:revoke`) | — | `{ok:true}` | `operations.rs:51`, `1326-1358` | adapt: CDO | Revokes only the caller's own tokens |
| GET | `/dev/auth/notifications` | `list_notifications` | op (any) | — | `{notifications,unread}`, at most 100 | `operations.rs:56`, `1366-1411` | adapt: CDO | |
| POST | `/dev/auth/notifications/read` | `read_notifications` | op (any) | JSON `{ids[]}` | `{ok:true}` | `operations.rs:57`, `1424-1465` | adapt: CDO | |

#### Operator: projects, keys, members and env

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/dev/projects` | `list_projects` | op(`projects:read`). Filtered by access | — | `{projects:[…]}` | `lib.rs:568`, `886-922` | adapt: CDO | |
| POST | `/dev/projects` | `create_project` | op(`projects:create`) | JSON `{name,databaseMode:single\|multiTenant}` | 201 `{project:{id,name,environment,database,databaseMode,storageBucket,status,description},projectKey}` (`rust/crates/store/src/provision.rs:497-505`) | `lib.rs:568`, `924-968`; `rust/crates/store/src/provision.rs:434` | adapt: CDO creates the project and its key | 400 when `default_database_url` is unset. `owner_email` is the actor |
| PATCH | `/dev/projects/{project}` | `update_project` | op(`projects:update`)+manage | JSON `{name?,databaseMode?,errorTrackingEnabled?,status?}`. `status` may only promote to `active` | `{project}` | `operations.rs:66-69`, `2067-2152` | adapt: CDO | |
| DELETE | `/dev/projects/{project}` | `delete_project` | op(`projects:delete`)+manage | — | `{ok:true}` | `operations.rs:66-69`, `2154-2185`; `rust/crates/store/src/provision.rs:554` | adapt: CDO, then delete each tenant DO's storage (`deleteAll`) and the R2 prefix | Deletes the database files |
| POST | `/dev/projects/{project}/key` | `project_key` | op(`projects:keys:read`)+access | — | `{projectKey}` | `lib.rs:569`, `970-1005` | adapt: CDO | Project key format: `gvx_<b64url(projectId)>.<secret>` (`rust/crates/store/src/provision.rs:40-53`) |
| POST | `/dev/projects/{project}/key/rotate` | `rotate_project_key` | op(`projects:keys:write`)+manage | — | `{projectKey}` | `operations.rs:70-73`, `2187-2232` | adapt: CDO | |
| GET | `/dev/projects/{project}/members` | `project_members` | pk\|op(`projects:members:read`,access) | — | `{members,invitations}` | `operations.rs:74-77`, `405-464` | adapt: CDO | Covers project (developer) members, not tenant members |
| POST | `/dev/projects/{project}/members` | `upsert_project_member` | op(`projects:members:write`)+manage (no pk) | JSON `{email,name,role}` | `{member}` | `operations.rs:74-77`, `474-540` | adapt: CDO | |
| POST | `/dev/projects/{project}/invitations` | `create_project_invitation` | op(`projects:members:write`)+manage | JSON `{email,role}` | `{invitation}` | `operations.rs:78-81`, `548-621` | adapt: CDO | |
| GET | `/dev/projects/{project}/env` | `get_project_env` | pk\|op(`projects:env:read`,access) | — | `{variables:[{name,value,source:"project",updatedAt}]}` | `operations.rs:82-88`, `2234-2261` | adapt: CDO (encrypt at rest) | Values come back in plaintext. Env is read when a function runs (`execution.rs:954-990`), so changing it needs no redeploy |
| POST | `/dev/projects/{project}/env` | `set_project_env` | pk\|op(`projects:env:write`,manage) | JSON `{name,value}` | `{ok:true}` | `operations.rs:82-88`, `2267-2304` | adapt: CDO | |
| PUT | `/dev/projects/{project}/env` | `replace_project_env` | same | JSON `{content?(dotenv),variables?[{name,value}]}` | `{ok:true,count}` | `operations.rs:82-88`, `2313-2377` | adapt: CDO | Replaces the whole set |
| DELETE | `/dev/projects/{project}/env` | `delete_project_env` | same | JSON body `{name}` | `{ok:true}` | `operations.rs:82-88`, `2382-2417` | adapt: CDO | DELETE carries a body |

#### Operator: project auth configuration and tenants

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/dev/projects/{project}/auth/providers/{provider}` | `get_auth_provider` | pk\|op(`projects:read`,access) | — | provider JSON | `operations.rs:89-92`, `2424-2466` | adapt: CDO | `supported_provider` is at `operations.rs:2782` |
| PUT | `/dev/projects/{project}/auth/providers/{provider}` | `put_auth_provider` | pk\|op(`projects:update`,manage) | JSON `{authMode:gonvex-native\|firebase\|external-oidc\|hybrid,enabled,signupMode,clientId,clientSecret,issuer,audience,jwksUrl,firebaseProjectId,firebaseTenantId,azureTenantId,adminCredentials}` | `{provider,authMode,enabled,signupMode,updated:true}` | `operations.rs:89-92`, `2468-2614` | adapt: CDO | Holds secrets (client secret, Firebase admin credentials) |
| GET | `/dev/projects/{project}/auth/google` | `get_google_auth` | pk\|op(`projects:read`,access) | — | `{enabled,signupMode,redirectUris,brokerCallbackUrl,ready,issues}` | `operations.rs:93-98`, `2616-2670` | adapt: CDO | |
| PUT | `/dev/projects/{project}/auth/google` | `put_google_auth` | pk\|op(`projects:update`,manage) | JSON `{redirectUri,signupMode}` | same as GET | `operations.rs:93-98`, `2677-2725` | adapt: CDO | |
| DELETE | `/dev/projects/{project}/auth/google` | `delete_google_auth` | same | `?redirectUri` | `{ok:true}` | `operations.rs:93-98`, `2736-2772` | adapt: CDO | |
| GET | `/dev/projects/{project}/auth/accounts` | `project_auth_accounts` | pk\|op(`projects:read`,access) | — | `{accounts:[…]}` | `operations.rs:99-102`, `623-670` | adapt: CDO | Lists the project realm's end-user accounts |
| PATCH | `/dev/projects/{project}/auth/accounts/{account}` | `update_project_auth_account` | pk\|op(`projects:update`,manage) | JSON `{disabled}` | `{ok:true,disabled}` | `operations.rs:103-106`, `676-770` | adapt: CDO, which must revoke live sessions in the TDOs | |
| DELETE | `/dev/projects/{project}/auth/accounts/{account}` | `delete_project_auth_account` | same | — | `{ok:true,disabled:true}` | `operations.rs:103-106`, `685-692` | adapt: CDO | This is a soft disable, not a delete |
| GET | `/dev/projects/{project}/auth/memberships` | `project_auth_memberships` | pk\|op(`projects:read`,access) | `?tenant,member,email` | `{members,invitations}` | `operations.rs:107-112`, `881-981` | adapt: CDO, plus a TDO read | |
| PUT | `/dev/projects/{project}/auth/memberships` | `put_project_auth_membership` | pk\|op(`projects:update`,manage) | query `tenant,member,email`. JSON `{email,role,permissions}` | `{ok:true}` | `operations.rs:107-112`, `983-1029`, `1031-1084` | adapt: TDO member write + CDO index | |
| DELETE | `/dev/projects/{project}/auth/memberships` | `delete_project_auth_membership` | same | query `tenant,member,email` | `{ok:true}` | `operations.rs:107-112`, `1086-1149` | adapt: TDO + CDO | |
| GET | `/dev/projects/{project}/auth/tenants` | `project_auth_tenants` | pk\|op(`projects:read`,access) | — | `{tenants:[…]}` | `operations.rs:113-116`, `772-812` | adapt: CDO | |
| POST | `/dev/projects/{project}/auth/tenants` | `create_project_auth_tenant` | pk\|op(`projects:update`,manage) | JSON `{name,ownerEmail?}` | 201 `{tenant}` | `operations.rs:113-116`, `820-861` | adapt: CDO creates the TDO and applies the active release's migrations | Runs `provision_runtime_tenant` (`lib.rs:244-281`) and an optional owner invitation, then sends `ControlChanged` |
| GET | `/dev/tenants` | `list_tenants` | op(`projects:read`). Without `?project` the caller must be a global admin; with it, access | `?project` | `{tenants}` | `lib.rs:570`, `1012-1053` | adapt: CDO | |
| POST | `/dev/tenants` | `create_tenant_shard` | pk\|op(`projects:update`,manage) | JSON `{projectId,tenantId?\|id?,name}` | 201 tenant JSON. 422 | `lib.rs:570`, `1055-1098` | adapt: CDO + TDO provisioning | Requires the module to be installed first (`lib.rs:263-268`) |
| DELETE | `/dev/tenants/{tenant}` | `delete_tenant_shard` | op(`projects:update`)+manage (no pk) | `?project` | `{ok:true}` | `lib.rs:571-574`, `1100-1143`; `rust/crates/store/src/provision.rs:772-827` | adapt: CDO, then TDO `deleteAll` | Marks the tenant `deleting`, removes the file, then the row |

#### Dashboard data (operator_data)

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/dev/data/tables` | `list_tables` | pk\|op(`projects:read`,access) | `?project,tenant` or `x-gonvex-project-id` / `x-gonvex-tenant-id` | `{tables}` | `operator_data.rs:25`, `247-333` | adapt: TDO | An empty tenant means the project database (`resolve_tenant(project,project)`, `operator_data.rs:147-187`) |
| GET | `/dev/data/tables/{table}/rows` | `read_rows` | same | `?limit,offset,search,sort,direction,filters(JSON),columns,count` | `{table,columns,rows,total,offset,limit}` | `operator_data.rs:26-29`, `519-653` | adapt: TDO | `_gonvex_*` and other internal tables are hidden |
| POST | `/dev/data/tables/{table}/rows` | `insert_row` | pk\|op(`projects:update`,manage) | JSON row | 201 `{table,row}` | `operator_data.rs:26-29`, `655-734` | adapt: TDO, in a captured transaction | Writes go through the change feed |
| PATCH | `/dev/data/tables/{table}/rows/{row}` | `update_row` | same | JSON patch | `{table,row}` | `operator_data.rs:30-33`, `736-819` | adapt: TDO | |
| DELETE | `/dev/data/tables/{table}/rows/{row}` | `delete_row` | **admin-key only** | `?project,tenant` | `{table,row}` | `operator_data.rs:30-33`, `821-868` | adapt: TDO | Unlike insert and update, delete accepts neither the project key nor an operator |
| POST | `/dev/data/references/replace` | `replace_references` | **admin-key only** | `?project,tenant`. JSON `{replacements:{from:to},dryRun?}`, at most 10k entries | `{textRows,jsonRows,columns,dryRun}` | `operator_data.rs:34`, `914-968` | adapt: TDO | |

#### New in the kernel: dead Actions (IB2)

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/dev/actions/dead` | new | op(`projects:read`)+access | query `project`, `tenant`, `limit`, `cursor` | `{items:[{id,function,args,attempts,lastError,deadAt}],cursor}` | none (IB2 has no Rust counterpart) | replace (IB2, 0001) | Lists outbox Actions and scheduled jobs in state `dead` |
| POST | `/dev/actions/dead/{id}/requeue` | new | op(`projects:update`)+manage | — | `{requeued:true}` | none | replace (IB2, 0001) | Resets attempts and makes the row pending again |

#### Metrics, logs, errors and caches

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| GET | `/dev/metrics` | `metrics` | op(`projects:read`)+access. `?format=profile` → `profile` | `?project,format` | `metrics_snapshot` JSON | `operations.rs:59`, `1566-1600`, `1664-1812` | replace: Analytics Engine plus per-DO counters (M05). Keep the JSON shape for the dashboard | Process memory, pools and the V8 heap have no equivalent |
| GET | `/dev/profile` | `profile` | op(`projects:read`) | — | profiler dump | `operations.rs:60`, `1559-1564` | defer: Rust process profiler, out of contract | |
| DELETE | `/dev/logs` | `clear_logs` | pk\|op(`projects:update`,manage) | `?project` | `{cleared:n}` | `operations.rs:63`, `2010-2037` | adapt: CDO | Clears `gonvex_performance_events` |
| DELETE | `/dev/cache` | `clear_cache` | same | `?project` | `{cleared:true,project}` | `operations.rs:64`, `2039-2052` | replace: no-op or broadcast to the TDOs. There is no shared live-query cache | Clears the process-wide cache, not only this project's |
| GET | `/dev/errors/status` | `error_status` | op(`projects:read`)+access | `?project` | `{enabled,project}` | `operations.rs:117`, `2816-2850` | adapt: CDO (K15) | |
| GET | `/dev/errors/groups` | `error_groups` | same | `?project,status,release,level` | `{groups,releases}` | `operations.rs:118`, `2852-2879` | adapt: CDO | |
| GET | `/dev/errors/groups/{fingerprint}` | `error_group` | same | `?project` | group JSON | `operations.rs:119-122`, `2881-2903` | adapt: CDO | |
| PATCH | `/dev/errors/groups/{fingerprint}` | `update_error_group` | op(`projects:update`)+manage | JSON `{status,priority,assignee?}` | group JSON | `operations.rs:119-122`, `2912-2962` | adapt: CDO | |
| GET | `/dev/errors/groups/{fingerprint}/bug-report` | `error_bug_report` | op(`projects:read`)+access | `?project` | `{title,markdown,agentContext{…}}` | `operations.rs:123-125`, `2970-3003` | adapt: CDO | |

#### CORS preflight

| Method | Path | Handler | Auth | Request | Response | Rust | Disposition | Notes |
|---|---|---|---|---|---|---|---|---|
| OPTIONS | `/dev/*` | `dev_cors` | none | — | 204 with CORS headers | `lib.rs:609-620` | keep, in the RW | Answered even for paths that do not exist |

### D.2 The /dev/sync deploy flow

#### D.2.1 Client side (`packages/gonvex/src/index.ts`)

Disposition: **keep**. The CLI is unchanged; the kernel answers it as Rust does.

1. **Starting the CLI.** `gonvex dev` (`runDev` at `631-683`) loads the runtime URL, project id and key (`GONVEX_PROJECT_KEY`) from flags or `.env` (`ensureProjectSettings` at `1893`). With `--once` there is no log stream and no watch loop, and a failed sync exits 1 (`676-681`). With `-- <cmd>` the CLI watches while running the child process. There is no separate `deploy` command: CI and Docker run `gonvex dev --once`.
2. **Deciding when to build.** `watchProject` (`1187-1260`) fingerprints module sources, `migrations/**/*.sql` and `gonvex.json` by mtime and size.
   - It rebuilds when the fingerprint changes.
   - It retries a failed sync after `runtimeSyncRetryMs`.
   - It checks for drift every `runtimeStateCheckMs`: GET `/dev/manifest` and compare `module.hash` and the function count (`1873-1891`), then re-sync if they differ.
3. **Building the manifest.** `buildManifest` (`1286-1306`) returns `{project, generatedAt, functions, schema: emptySchemaDefinition(), module, visibility?}`. The `module` object is a `ModuleArtifact` (`packages/gonvex/src/manifest-types.ts:271-288`):
   - `language`, `generation` (the format version), `hash` (canonical), `entrypoint` and `functions`.
   - `files`: a map from path to base64. It includes `migrations/NNNN_*.sql` and the optional `client-contract.json`.
   - `javascript`, `reducerBundle?`, `visibility`, `crons?`, `invitationAcceptanceReducer?` and `permissionGrants?`.
   - **The JS bundle, migrations and client contract all travel inline in one JSON body.** There is no separate upload, and **env is not part of sync**: it uses the env routes.
4. **Sending it.** `syncRuntime` (`1754-1767`) sends `POST {runtimeURL}/dev/sync` with:
   - `content-type: application/json`
   - `x-gonvex-project-id`, when known
   - `authorization: Bearer <key>` and `x-gonvex-key: <key>`
   - the manifest JSON as the body

   A non-2xx response throws. On success the returned `schemaDefinition` is written back into the generated bindings (`1229-1233`).

#### D.2.2 Server side (`lib.rs:1358-1497`)

Disposition: **adapt** per D.2.4 and 0012 R1-R5 (same validation, ordering, responses and status codes; Control DO, R2 and a Queue as mechanisms).

1. **Body.** axum's `Json` extractor, limited to 64 MiB (`lib.rs:576`). Malformed JSON gets axum's 400 or 422 text, not `{error}`.
2. **Project.** It comes from the body's `project`, or else from the header. When both are present they must match, otherwise 400 `manifest project does not match x-gonvex-project-id` (`1374-1395`). The result is written back into `manifest.project`.
3. **Key.** Bearer, else `x-gonvex-key` (`1396-1406`). The control plane is required, otherwise 503 (`1407-1418`). `project_accepts_sync_key` (`rust/crates/store/src/lib.rs:824-845`) accepts the project's stored `project_key` (constant-time compare) when that key is non-empty. When the project row is missing or has an empty key, it accepts `GONVEX_DEV_SYNC_KEY` instead. Anything else returns 401 `invalid Gonvex sync key` (`1419-1430`).
   - **Unknown project ids sync under the fallback key**, because a missing row counts as an empty key. The module installs and no tenants are migrated.
4. **Validation.** `manifest.module` must be an object, otherwise 422 (`1431-1440`). `RuntimeManifestRecord{project_id, module_hash: module.hash, manifest}` is passed to `modules::validate_manifest_record` (`modules.rs:385-406`):
   - `migrations_from_module` (`modules.rs:425-498`) reads only `files["migrations/*.sql"]`, so nested paths are ignored. Names must match `NNNN_[A-Za-z0-9_-]+.sql`. Content must be base64 UTF-8. The leading comment block must contain `-- gonvex:scope tenant|control-plane` (required, otherwise an error) and may contain `-- gonvex:no-transaction`. Migrations are sorted by name. The checksum is the SHA-256 hex of the SQL text (`rust/crates/store/src/provision.rs:71-84`).
   - `artifact_from_manifest` (`modules.rs:576-…`) checks `module.hash` == record hash == `canonical_artifact_hash(module)`, that a `javascript` bundle is present, the `functions` object, `reducerBundle` and visibility. Any failure returns 422.
5. **Control-plane migrations.** `apply_control_migrations` (`rust/crates/store/src/provision.rs:862-877`) runs against **the single shared Control Plane database**, not a per-project one. A failure returns 422 and stops before any tenant (`1450-1455`).
   - **DOC≠CODE**: `docs/migrations.md:20-22` says "the project's Control Plane database". The ledger is shared, so two projects that use the same migration filename with different SQL collide on checksum.
6. **Ledger semantics.** `apply_migrations_locked` (`rust/crates/store/src/provision.rs:960-1086`) runs under the per-database `SCHEMA_LOCK`:
   - It ensures `gonvex_migrations(name PK, checksum, applied_at, duration_ms)` exists.
   - For each migration of the matching scope, in name order: if a ledger row exists with an equal checksum, skip; if the checksum differs, fail with `migration X checksum mismatch: database has …, artifact has …`.
   - Otherwise it runs the SQL in one transaction, or statement by statement when `no-transaction` is set. Then it runs `reference_integrity::after_migration` for tenant scope. Then it calls `feed::finalize`, with provenance `command_id=migration:<name>`, `channel=system`, so the migration's row changes reach clients. Finally it inserts the ledger row in the same transaction.
   - A line `-- gonvex:foreign-key-rebuild` turns foreign keys off for the run, checks `pragma_foreign_key_check`, and requires a transactional tenant-scope migration. It also retires the pooled connection.
   - The first failing migration rolls back and aborts that database. Migrations already applied in earlier transactions stay.
7. **Tenant rollout.** `tenant_routes(project)` (`rust/crates/store/src/lib.rs:890-…`) returns non-deleted, non-disabled tenants ordered by `tenant_id`. **They are processed sequentially in a `for` loop and the first failure aborts** with 422 and the error text, which does not name the tenant (`lib.rs:1456-1467`).
   - Per tenant, `provision_tenant_database` (`rust/crates/store/src/provision.rs:829-860`) takes the schema lock, then runs `install_tenant_identity` (refusing a pre-identity-v2 `members.user_id`), the reference-integrity install, `SCHEMA_OPTIONS_SQL`, the tenant-scope migrations, `install_change_feed_locked` (sync infrastructure and capture triggers, cached by digest in `_gonvex_sync_bootstrap`), and finally publishes change-feed notices.
   - **DOC≠CODE**: `docs/migrations.md:46` says rollout continues past a failed tenant, collects every failure and names the tenant. The code fails fast. Tenants before the failure are migrated and tenants after it are not. Nothing rolls back.
8. **Module install and activation.** `modules.install` (`modules.rs:169-293`) runs these steps:
   1. Re-parses the migrations.
   2. Reads `client_contract` from `files["client-contract.json"]`: `version` > 0, and `offlineMaxAgeMs` either a positive integer or null (`modules.rs:1236-1262`).
   3. Validates the crons (`modules.rs:333-383`): unique names, scope `project` or `tenant`, exactly one of `intervalMs` or `expression`, a `dueWhen` that passes the read-only SQL admission check, and a target that is a Reducer or Action.
   4. Validates `permissionGrants`.
   5. Sends `host.load` to get a new generation.
   6. Takes the activation write lock (`modules.rs:229`). It blocks new calls, while in-flight calls hold read leases (`project_for_call` at `modules.rs:309-316`).
   7. Calls `host.activate{drain_ms:30000}`. The old generation drains for up to 30 s.
   8. Swaps the registry entry and clears the recorded failure.

   Any failure returns 422. **Schema migrations stay applied while the old generation keeps serving.**
9. **Crons.** `scheduler.sync_project` (`scheduler.rs:717-…`) deletes pending cron jobs and re-inserts the next run per spec and target route, with stagger. A project-scoped cron needs exactly one route. **A failure here returns 422, but the new generation is already live.**
10. **Live queries.** `live_query_cache.clear()` clears the whole process cache, not only this project's (`lib.rs:1479`).
11. **Broadcast `RuntimeEvent::ModuleReloaded{project_id}`** (`lib.rs:1480-1486`). Every socket of that project (`lib.rs:2479-2508`) then:
    - clears replicas, replica opens, snapshot deliveries and live queries.
    - If the new module has a `client_contract` and the socket is authenticated with a different `clientContract`, it sends `client.updateRequired{reason:"CLIENT_UPDATE_REQUIRED",contract}` and closes.
    - Otherwise it sends `system.reload{reason:"module generation changed",artifactHash}` (`packages/protocol/src/index.ts:610-611`; `rust/crates/protocol/src/lib.rs:931-934`).
    - Afterwards every inbound frame on a tenant session re-checks the contract and closes on a mismatch (`lib.rs:2527-2553`).
12. **Persist.** `save_runtime_manifest` (`rust/crates/store/src/lib.rs:847-866`) upserts `gonvex_runtime_manifests(project_id, manifest, module_hash, updated_at)`. **This happens after activation and after the broadcast.** If it fails, the response is 500 while the new code serves until the next restart, which then reloads the previous manifest.
13. **Response.** 200 `{ok:true, project, functionCount: installed.functions.len(), schemaDefinition: manifest.schema (the request's own value echoed; the CLI sends an empty schema), runtimeReloaded:true}` (`lib.rs:1489-1496`).
    - **DOC≠CODE**: `docs/migrations.md:48` says applied migrations are returned. They are not: the `Vec<String>` from `apply_control_migrations` is dropped, and tenant applies return `()`.
    - **DOC≠CODE**: `gonvex dev --once --dry-run` (`docs/migrations.md:50-54`) does not exist in the CLI or runtime.
    - **DOC≠CODE**: `docs/migrations.md:44` describes declarative schema reconciliation before migrations, and lines 56-65 describe `GONVEX_DROP_EMPTY_UNDECLARED_COLUMNS`. Neither appears in `/dev/sync` or anywhere in the Rust source.
14. **Side effects.** The `dashboard::invalidate` layer fires.

#### D.2.3 Other paths into the same state

Disposition: **adapt** as D.2.2.

- **Startup** (`lib.rs:494-541`). For each stored manifest, ordered by `updated_at`, the runtime installs the module, re-applies the control migrations, then provisions every tenant 8 at a time (`STARTUP_TENANT_PROVISION_CONCURRENCY`, `lib.rs:164`). A failed tenant aborts startup.
- **New tenants** (`provision_runtime_tenant` at `lib.rs:244-281`). The tenant is created, given the active module's migrations, then marked provisioned. This fails when no module is installed.
- **Concurrency.** There is no project-level lock around `/dev/sync`. Two concurrent syncs serialize only per database on `SCHEMA_LOCK`, and on the activation gate.

#### D.2.4 Cloudflare mapping (K19, K02)

| Step | Mechanism |
|---|---|
| Auth and project | RW verifies the sync key against the CDO |
| Bundle | Hash-addressed object in R2 (`bundles/<project>/<hash>.json`). The CDO records a release row (pending) |
| Control migrations | CDO, in its own SQLite, with the same ledger |
| Tenant migrations | One Queue message per tenant → that TDO's `applyRelease(hash)`. Same ledger and change-log provenance. The CDO tracks per-tenant status |
| Activation | Flip the active release in the CDO once every tenant has applied it. That matches today's "no activation unless every tenant migrated". Then fan out to the TDOs, which send `system.reload` or `client.updateRequired` to their sockets |
| New tenant | The TDO applies the active release's migrations before it is marked ready |

**Contract risk.** The CLI expects a synchronous response, and `gonvex dev --once` fails on non-2xx. Two ways to keep that:
- The CDO holds the request open until the Queue rollout finishes, or until a deadline.
- Add a pollable status, which is a contract change.

Ruled: the first option (0012 R4).

**Lead rulings (0012).** Keep fail-fast (R1). Keep the response shape without applied migration names (R2). Keep the unknown-project fallback through `GONVEX_DEV_SYNC_KEY` (R3). Add a per-project sync lock, 409 on a concurrent sync (R5).

### D.3 Operator authorization model

#### Credential resolution

Disposition: **keep** the order and every credential kind; verification runs in the Control DO with secrets as Worker secrets.

`operations::authenticate` (`operations.rs:3122-3182`) tries these in order. It reads the token from `Authorization` with an optional `Bearer ` prefix (`operations.rs:3447-3461`).

1. **Signed dashboard session** (`verify_signed_session`, `operations.rs:3347-3403`).
   - Format: `base64url(json).base64url(HMAC-SHA256)`. The key is `GONVEX_DASHBOARD_SESSION_SECRET`, or `GONVEX_ADMIN_KEY` when that is unset (`operations.rs:3431-3438`).
   - The session must not be expired and must carry a `sessionRevision`.
   - Authority `configured`: the email must equal `DASHBOARD_AUTH_ACCOUNT`, and the revision must equal the HMAC of the configured account and password. The role is `admin`.
   - Authority `database`: the revision must equal `gonvex_dashboard_accounts.session_revision`, and the password hash must not start with `!`. The role comes from that row.
   - Credential `Session`, permissions `*`.
2. **Native session** (`gvx_session_…`). Requires `GONVEX_DASHBOARD_AUTH_PROJECT_ID`. The runtime loads the session in that project's realm and requires a verified email. That email must exist in `gonvex_dashboard_accounts` or equal the configured account (`actor_for_email`, `operations.rs:3269-3305`). Credential `NativeSession`, permissions `*`.
3. **Personal access token** (`gvx_pat_<id>.<secret>`). Looked up in `gonvex_account_access_tokens` by id and SHA-256 hash, not revoked, not expired. Each use updates `last_used_at`. Its owner must still have a dashboard account. Credential `PersonalAccessToken` with the token's permission list (`operations.rs:3307-3345`).
4. **Admin key** (`GONVEX_ADMIN_KEY`). Actor `admin@gonvex.local`, role `admin`, credential `AdminKey`.
5. **Local**, only when `dashboard_auth_optional` holds: `!GONVEX_REQUIRE_AUTH`, no auth project, no session secret or admin key, and no configured account (`operations.rs:3440-3445`). Every request becomes `local@gonvex.dev`, an `admin` with every permission.
6. Otherwise 401 `dashboard sign-in or personal access token is required`.

#### Permission checks

Disposition for every row: **keep**; operator credentials are checked in the Control DO.

| Check | Rule | Where |
|---|---|---|
| `authorize(perm)` | Only PATs are restricted. They need `*`, an exact match, or a `prefix*` match. Every other credential passes any permission | `operations.rs:147-159`, `3184-3201` |
| Global admin | role `admin` ∧ (AdminKey ∨ Local ∨ a PAT holding `*` or `admin:projects`). **A session-based admin is not a global admin** | `operations.rs:161-170` |
| `can_access_project` | Local ∨ global admin ∨ the project's `owner_email` is the actor (or `owner_email` is empty and the actor is an `admin`) ∨ a row in `gonvex_project_members` with any role | `operations.rs:3203-3229` |
| `can_manage_project` | Local ∨ global admin ∨ owner ∨ a `gonvex_project_members` row with role in (`owner`, `admin`) | `operations.rs:3231-3257` |
| Project key | `authorize_project_resource` accepts the project's own key as Bearer before any operator check. The project key therefore grants read and write for env, auth providers, tenant creation, membership edits, data-row insert and update, and clearing logs and cache, but not project member or key management | `operations.rs:3046-3078` |
| Admin-key-only routes | `delete_row`, `replace_references`, `/dev/internal/e2e/members`. The `/healthz` diagnostics need an `admin`-role non-Local actor | — |

#### Developer and tenant-admin roles on the WebSocket (`control.*`)

Disposition: **keep**.

These are separate from HTTP operators. `authorize_control` (`control.rs:2013-2080`), with the `Authorization` enum at `control.rs:45-51`, decides:
- **Public**: anyone.
- **Account**: any authenticated account.
- **TenantAdmin**: the caller's tenant `members.role` is `owner` or `admin`, admitted through the control plane.
- **Developer** or **ProjectAdmin**: no active impersonation, and the account's email is the project's `owner_email`, or appears in `gonvex_project_members` with role in (`owner`, `admin`, `dev`) for Developer, or (`owner`, `admin`) for ProjectAdmin.

**Identity is joined by email.** A tenant-realm account becomes a project developer because its email appears in the same `gonvex_project_members` table that HTTP operators use. Developer self-provisioning (`control.developer.provisionSelf`) writes a tenant member with role `admin` and `{"developer":true}` (`control.rs:889-895`).

**Cloudflare placement.** All of the above lives in the CDO: dashboard accounts, PATs, project members, project keys, sessions. The RW forwards `Authorization` unchanged. The TDO trusts only principals that the CDO has resolved, never raw headers. Local mode must be impossible to enable in deployed Workers.

## E. SQL rules, scheduler, outbox, storage, presence v2

Reference: Rust runtime at `0bf3ad13`. Code wins where docs disagree; each disagreement is marked **DOC≠CODE**.

Path legend: `R:` = `rust/crates/runtime/src/`, `S:` = `rust/crates/store/src/`, `MR:` = `rust/crates/module-runtime/src/`, `V8:` = `rust/crates/module-runtime-v8/src/`, `P:` = `packages/`, `D:` = `docs/`.

Dispositions: **keep** (same behavior, same mechanism class), **adapt** (same behavior, Cloudflare mechanism named), **replace** (different mechanism, behavior kept or contract note needed), **defer** (not needed for parity, or a spike decides), **drop** (Rust-only tuning, not contract).

DO SQLite limits used below: 100 KB/statement, 100 bound params, 100 columns/table, 2 MB/row, no custom SQL functions, no hooks, `BEGIN`/`SAVEPOINT` only via `ctx.storage.transactionSync`, no `ATTACH`/`PRAGMA`/`VACUUM` via `sql.exec`.

---

### E.a SQL rules

#### E.a.1 Connection, durability and engine rules

| # | Rule | What Rust does | Source | DO SQLite compatibility | Disposition |
|---|---|---|---|---|---|
| a1 | One writer, reader pool per file | Writer pool `max_connections(1)`; reader pool `read_only=true`, default 4 | `S:registry.rs:654`, `S:registry.rs:583-621`; `D:sqlite-storage.md:42-50` | DO is single-threaded per object; one connection | **adapt**: DO input gates serialize; no pools |
| a2 | Connection pragmas | `journal_mode=WAL`, `foreign_keys=ON`, `busy_timeout=5000`, `synchronous=NORMAL` (writer), `cache_size=-{GONVEX_SQLITE_CACHE_KIB}`, `mmap_size`, `temp_store=MEMORY`, statement cache 128 | `S:registry.rs:594-620`, `S:registry.rs:186-194`; `D:sqlite-storage.md:52-56` | `PRAGMA` not issuable | **drop** all except FK enforcement; **verify** in S1 that DO enforces FKs incl. `DEFERRABLE INITIALLY DEFERRED` at commit (a10, a11 depend on it) |
| a3 | Durability before ack | COMMIT never fsyncs under writer; `synchronous=full` default waits on batched WAL sync (`sync_barrier`) before ack/publish | `S:lib.rs:1808-1814`; `D:sqlite-storage.md:58-66` | DO output gate holds outgoing messages until writes are durable | **adapt** (DO output gates) |
| a4 | Off-writer checkpoints | WAL hook + no-op custom function `gonvex_wal_state`; PASSIVE every 1,000 frames; inline at 16,000 | `S:checkpoint.rs:308-345`; `D:sqlite-storage.md:68-72` | Hook and custom function both forbidden; DO manages WAL | **drop** |
| a5 | Heap budget, reader sweep, nice levels | Soft heap limit 128 MB; idle readers close 15 s; nice 10 | `S:registry.rs:136-153`; `D:sqlite-storage.md:74-87` | n/a | **drop** |
| a6 | Planner statistics | `PRAGMA analysis_limit=400; PRAGMA optimize=0x10002` on writer open; `PRAGMA optimize` every `GONVEX_SQLITE_OPTIMIZE_SECS` (3600, first ~60 s after start) | `S:registry.rs:155-169`, `S:registry.rs:735-760` | `PRAGMA` not issuable | **defer**: S1 checks whether DO allows `PRAGMA optimize`/`ANALYZE`; stale `sqlite_stat1` turned indexed lookups into scans in Rust |
| a7 | Statement interrupt | `sqlite3_progress_handler` every 1,000 VM steps stops a dropped stream | `S:direct.rs:836-855` | No progress handler | **replace**: page every row loop (design rule); 30 s CPU limit is the backstop |
| a8 | Statement cache key | `/* gonvex.parameter-types.v1:oids */` prefix | `R:sql_statement_key.rs:1-49` | n/a | **drop**: dead module (declared `R:lib.rs:62`, no callers; Postgres OIDs) |
| a9 | Advisory locks | `PoolRegistry::lock(database,key)` in-process keyed mutex (`SCHEMA_LOCK="gonvex_sql_migrations"`, `_gonvex_sync_changes_prune`) | `S:provision.rs:27`, `R:change_feed.rs:334-336`; `D:sqlite-storage.md:116-117` | n/a | **adapt**: single-threaded DO; `blockConcurrencyWhile` for migrations |
| a10 | Notifications | `{epoch,revision,tables}` on in-process broadcast; payload drops `tables` above 7,000 bytes (pg_notify legacy) | `S:feed.rs:102-123`; `D:sqlite-storage.md:106-108` | n/a | **adapt** (in-DO event dispatch); **drop** 7,000-byte cap |
| a11 | `FOR UPDATE`/`SKIP LOCKED` removed; claims are `UPDATE … WHERE id IN (SELECT … LIMIT n) RETURNING` | | `D:sqlite-storage.md:118-120`; `S:lib.rs:1254-1307` | Standard SQL | **keep** |
| a12 | File layout and process lock | `{data}/projects/{p}/tenants/{t}.db`, `.gonvex.lock` | `D:sqlite-storage.md:27-38` | One DO per tenant | **replace**: `idFromName(project/tenant)` Single-database projects (`{data}/projects/{p}/project.db`, all tenants in one file) map to **one tenant DO per project**, keyed `idFromName(project)`; the Control Plane file `{data}/control.db` maps to the Control DO (`D:sqlite-storage.md:29-34`). |
| a13 | Time | `NOW_MS = CAST(unixepoch('subsec')*1000 AS INTEGER)` in defaults and claims | `S:feed.rs:15` | Needs SQLite ≥3.42 | **keep**; **verify** DO SQLite version and whether its clock is the coarsened Workers clock (claims/leases compare against it) |

#### E.a.2 Change capture (the hard incompatibility)

| # | Rule | What Rust does | Source | DO compatibility | Disposition |
|---|---|---|---|---|---|
| c1 | Capture triggers | Per table `AFTER INSERT/UPDATE/DELETE` trigger `_gonvex_sync_{table}_{op}`: `INSERT INTO _gonvex_sync_changes(table_name,row_id,operation,new_value) VALUES(…, gonvex_capture('{t}', NEW/OLD.rowid, 0/1/2))` | `S:feed.rs:459-497` | Calls a custom function | **replace** |
| c2 | Row images | `gonvex_capture` (custom fn, writer only) returns `[old,new,changed]` from events buffered by `sqlite3_preupdate_hook`; `commit_hook`/`rollback_hook` clear buffer; images typed by declared column type; no-op update → NULL | `S:capture.rs:1-19`, `S:capture.rs:147-214`, `S:capture.rs:216-233`, `S:capture.rs:265-335`, `S:capture.rs:378-452` | Custom function + 3 hooks: all forbidden | **replace**: pure-SQL triggers building `json_object(...)` images per declared type (the pre-hook design; helpers still in `S:feed.rs:403-457`). Rust moved off it because trigger bodies were 2.4 MB and 27 MB parsed schema per connection (`S:capture.rs:3-7`); DO has one connection, so S1 must measure parse memory against the 128 MB isolate and the 100 KB/statement limit per trigger. Alternative for S1: capture in the kernel for structured `ctx.db` writes and keep triggers only for raw SQL and app-trigger cascades |
| c3 | Capture catalog | Digest of `sqlite_schema` tables + capture triggers; shape cache of 16 schemas; refreshed after any script containing `TRIGGER` | `S:capture.rs:496-584`, `S:lib.rs:1909-1921` | Reads `sqlite_schema`, `pragma_table_info` | **adapt** (needed only if c2 keeps a JS-side catalog) |
| c4 | Finalize (pre-COMMIT) | `record_views::flush`; probe `revision IS NULL`; delete no-op updates; split `[old,new,changed]`; `UPDATE _gonvex_sync_clock SET revision=revision+1 RETURNING`; stamp revision/ordinal/command_id via `UPDATE … FROM (row_number() OVER …)`; `SELECT DISTINCT table_name … INDEXED BY`; insert `_gonvex_sync_transactions` | `S:feed.rs:262-401`, `S:lib.rs:1759-1774` | Standard SQL | **keep**; run at the end of the same `transactionSync` |
| c5 | Commit order | reference flush → finalize → COMMIT → durability wait → publish; caller never sees an uncommitted revision | `S:lib.rs:1784-1834` | | **adapt**: publish after the transactionSync returns; output gate covers durability |
| c6 | All tenant writes through `TenantTransaction` | Else unrevisioned rows are adopted by next commit; writer without `gonvex_capture` cannot write a captured table | `D:sqlite-storage.md:111-112`, `S:capture.rs:17-19` | | **keep** (kernel owns every write) |
| c7 | Logical record views | `BEFORE` triggers stage first pre-image into `_gonvex_record_pending` via custom fn `gonvex_record_value(name,key)`; `FLUSH_SQL` resolves post-images and inserts change rows; deferred FK to `_gonvex_record_guard` fails a commit that skipped flush | `S:record_views.rs:13`, `S:record_views.rs:152-178`, `S:record_views.rs:180-209`, `S:capture.rs:337-370`, `S:schema_options.sql:27-31` | Custom function | **replace**: generate per-view SQL subquery (`(SELECT json_object(...) FROM <view> WHERE key=?)`) in triggers and flush; verify deferred-FK guard in S2 |
| c8 | Reference integrity | `AFTER` triggers stage row images into `_gonvex_reference_pending`; Rust `flush` evaluates declarations before COMMIT; deferred FK edges→targets reject dangling refs at COMMIT; guard FK fails unflushed commits | `S:reference_integrity.rs:1-16`, `S:reference_integrity.rs:106-110`, `S:reference_integrity/schema.sql:1-80`, `S:lib.rs:1760-1763` | Triggers OK; deferred FK must fire at transactionSync end | **adapt**: port flush to kernel; S2 verifies deferred FK |
| c9 | Pruning | Hourly per tenant feed task: delete changes `created_at < now-30d` up to max such revision; delete orphan `_gonvex_sync_transactions`; raise `retained_revision`; one unbounded transaction | `R:change_feed.rs:317-322`, `R:change_feed.rs:327-373` | | **adapt**: DO alarm, paged deletes (design rule) |
| c10 | Cursor expiry | `after < retained_revision` → `reset("change feed cursor expired")` | `R:change_feed.rs:381-387` | | **keep** |

#### E.a.3 Module SQL (`ctx.db.query`) and SDK statements

| # | Rule | What Rust does | Source | DO compatibility | Disposition |
|---|---|---|---|---|---|
| m1 | Exactly one statement | SQLite-tokenizer-faithful scanner (strings, `"`/`` ` ``/`[]` identifiers, blobs, TCL vars, `--`/`/* */`); NUL and unterminated rejected; >1 statement rejected | `R:sql_admission.rs:1-14`, `R:sql_admission.rs:42-160`, `R:sql_admission.rs:194-213` | | **keep** (port lexer verbatim with its tests `R:sql_admission.rs:215-338`) Error texts (keep): `a database statement is required` (empty, `R:sql_admission.rs:194-197`), `a database statement may not contain NUL`, `…unterminated literal or comment`, `<HEAD> statements are not allowed in module SQL` (`R:sql_admission.rs:183-188, 198-203`). |
| m2 | No transaction control (exception: `PREPARE TRANSACTION AS` / `PREPARE TRANSACTION (` is not treated as transaction control, `R:sql_admission.rs:177`) | Refuses `COMMIT`, `END`, `ABORT`, `BEGIN`, `START`, bare `ROLLBACK [WORK\|TRANSACTION]`, `PREPARE TRANSACTION` | `R:sql_admission.rs:170-182` | Matches DO | **keep** |
| m3 | No `ATTACH`/`DETACH`/`PRAGMA`/`VACUUM` | Head-word refusal; table-valued `pragma_table_info(...)` admitted | `R:sql_admission.rs:183-188`, `R:sql_admission.rs:267-280` | Matches DO for statements; **verify** DO allows `pragma_*` table-valued functions (SDK batched writes and provisioning rely on them, `D:sqlite-storage.md:352-356`, `S:provision.rs:295,1035,1126,1204,1305,1390,1470`) | **keep**; S1 verifies |
| m4 | Savepoints | `SAVEPOINT x`, `RELEASE [SAVEPOINT] x`, `ROLLBACK [TRANSACTION|WORK] TO x` admitted (`sql_admission.rs:174-176`); SDK `applyDataWrites` wraps writes in `SAVEPOINT gonvex_data_writes` | `R:sql_admission.rs:246-265`; `P:module-sdk/src/data-write.ts:332`; `D:sqlite-storage.md:344-347` | `SAVEPOINT` forbidden in `exec`; only nested `transactionSync` (synchronous callback) | **adapt**: kernel intercepts the three heads and maps to nested savepoint scope; module code awaits between statements, so S2 decides feasibility |
| m5 | Aborted until `ROLLBACK TO` | A failed statement marks the invocation aborted; only `ROLLBACK TO` clears it; never commits | `R:host_calls.rs:150-154`, `R:host_calls.rs:1011-1020`, `R:host_calls.rs:1067-1072` | | **keep** |
| m6 | Query read-only | Query capability: head must be `select`/`with`/`values`/`explain`; and Query runs on a `read_only` connection | `R:host_calls.rs:1013-1015`, `R:host_calls.rs:1715-1727`, `R:scheduler.rs:510-513` | DO has no read-only connection: `WITH … DELETE` passes the head check | **adapt**: head check + reject when cursor `rowsWritten>0` (throw inside transactionSync to roll back); verify `EXPLAIN` is allowed |
| m7 | Module DDL | `create`/`alter`/`drop` allowed in Reducers; clears catalog cache, marks schema changed | `R:host_calls.rs:1054-1068` | DDL allowed | **keep**; verify new tables get capture triggers |
| m8 | Parameter binding | null→NULL; bool→0/1; i64/u64≤i64::MAX→INTEGER (else error); other numbers→REAL; string→TEXT; array/object→JSON text; JSON column binds JSON text even for strings; BOOLEAN column binds 0/1 | `R:sql_parameters.rs:5-55`; `D:sqlite-storage.md:337-340` | JS numbers lose integers >2^53 | **adapt**: same rules; S2 checks int64 round-trip (DO returns JS numbers) |
| m9 | `$N` placeholders | Module SQL is bound by `Stmt::bind`: `$N`/`?N` take the Nth value (reuse and any order allowed); a bare `?` takes the next one; placeholders beyond the supplied values stay NULL with no error; `:name`/`@name` fail with `unsupported SQL parameter format`; `$name` fails with `parameters with non-integer names are not currently supported` | `S:direct.rs:578-635`; `D:sqlite-storage.md:153-156` | `sql.exec` binds positionally; SQLite numbers `$N` named params by first appearance, so `SELECT $2,$1` binds wrong | **adapt**: rewrite `$N`→`?N` with the m1 lexer (outside literals/comments/identifiers) and reproduce every rule and error text in this row (0011) |
| m10 | 100 bound params | Lists pass as one JSON param (`IN (SELECT value FROM json_each($n))`); structured insert binds one per supplied column; update binds columns+1; deleteMany one JSON param | `P:module-sdk/src/data.ts`; `R:host_calls.rs:1117-1141`, `R:host_calls.rs:1158-1175`, `R:host_calls.rs:1209` | Update of all 100 columns = 101 params; raw SQL unbounded | **adapt**: reject >100 with a clear error; S1 reports max column count |
| m11 | 100 KB/statement | No limit on module SQL | `R:host_calls.rs:1011` | Hard limit | **adapt**: reject >100 KB before exec |
| m12 | `regexp(pattern,value)` | Registered on every connection (`with_regexp`, Rust `regex`); SDK `matches` emits `col REGEXP $n` after `validateDataPattern` | `S:registry.rs:617-618`; `P:module-sdk/src/data.ts:109-114`, `P:module-sdk/src/data.ts:419`; `D:sqlite-storage.md:341-343` | No custom functions | **replace**: contract-change note required (rule 1). Options: kernel rewrites the SDK's portable subset to `GLOB`/`LIKE`/`instr`, or evaluates `matches` in JS (breaks `LIMIT` semantics) |
| m13 | Declared-type decoding | `JSON`→parsed, `BOOL`→bool, `INT`→int, `REAL/DOUB/FLOA/NUMERIC`→number, else string; `RETURNING` decoded with target table kinds; expression columns arrive as ints | `D:sqlite-storage.md:158-170`, `D:sqlite-storage.md:348-350`; `R:host_calls.rs:1036-1052` | DO cursor gives no declared type | **adapt**: kernel catalog from `pragma_table_info` (m3) |
| m14 | No BLOB columns in app tables | `json_object` rejects BLOB | `D:sqlite-storage.md:320-321` | | **keep** |
| m15 | `lockData`/`lock:"update"` emit no SQL | One writer serializes | `D:sqlite-storage.md:352-353` | | **keep** |
| m16 | TEMP tables | Only in tests | `R:sql_parameters.rs:225`, `R:visibility.rs:3138` | Unknown | **adapt**: reject `CREATE TEMP` in module SQL unless S1 shows support |
| m17 | Dialect table | Postgres→SQLite rules (JSON text, BOOLEAN 0/1, ms integers, `json_each`, `json_group_array`, `->`/`->>`, `UPDATE … FROM`, window functions, `RETURNING`) | `D:sqlite-storage.md:124-156` | All stock SQLite ≥3.38/3.42 | **keep** |
| m18 | Batched-write JSON extraction | In SDK batched writes a JSON column takes `value -> '$.col'` and every other column takes `value ->> '$.col'` | `D:sqlite-storage.md:353-356` | Stock SQLite | **keep** |

#### E.a.4 Row and statement size risks

| Item | Rust | Source | DO limit hit | Disposition |
|---|---|---|---|---|
| Change row | `old_value`+`new_value` JSON images of the app row; transiently `[old,new,changed]` in `new_value` | `S:feed.rs:29-42`, `S:feed.rs:168-183` | App row above ~0.7 MB breaks the 2 MB row cap | **adapt**: split pre/post images into separate columns at insert; document a ~0.9 MB app-row ceiling |
| Receipt result | `_gonvex_reducer_idempotency.result JSON` unbounded | `S:lib.rs:1153-1165` | 2 MB/row | **adapt**: results larger than 2 MB cannot be stored as one row; K07 reports how many Whagons reducers exceed it. Results also expire after the offline window (0009) |
| Outbox args | `_gonvex_action_outbox.args JSON` unbounded | `S:feed.rs:67-80` | 2 MB/row | **adapt**: cap |
| Module artifact | `gonvex_runtime_manifests` (Control Plane) holds bundle + base64 files | `S:control_schema.sql:59` | 2 MB/row | **replace**: R2 or KV for artifacts (outside this section; flag to lead) |
| Migration scripts | Whole file via `sqlx::raw_sql` (multi-statement) | `S:provision.rs:1017-1027`, `S:lib.rs:1909-1921`; `D:sqlite-storage.md:174-176` | Each statement ≤100 KB; multi-statement `exec` support to verify | **adapt**: split with the m1 lexer (trigger-body aware, as `D:migrations.md:41`); S1 lists any statement >100 KB (views, triggers) |

#### E.a.5 Runtime-owned tables

Disposition for every table: **keep** the name, columns and semantics inside the tenant DO (export/import and replay depend on them). Pruning rules change only where a note says so (0009 for receipts). Exception: `_gonvex_action_outbox` gains `max_attempts` and a `dead` status, dead rows are kept, and the hop guard marks a row `dead` instead of deleting it (IB2).

Tenant database (every tenant DO):

| Table | Purpose | Created | Pruned |
|---|---|---|---|
| `_gonvex_sync_clock` | Singleton `{epoch, revision, retained_revision}`; epoch `lower(hex(randomblob(16)))` | `S:feed.rs:21-28` | n/a |
| `_gonvex_sync_changes` | Change log: `event_id` AUTOINCREMENT, `revision`, `ordinal`, `command_id`, `table_name`, `row_id`, `operation`, `old_value`, `new_value`, `changed_columns`, `created_at`; partial indexes on revision, (table,revision), pending, created_at, command | `S:feed.rs:29-53` | 30 d, hourly (`R:change_feed.rs:327-373`) |
| `_gonvex_sync_transactions` | Provenance per revision: root/origin command, root/invocation channel, actor account/member, on-behalf-of, agent execution | `S:feed.rs:54-66`, `S:feed.rs:377-394` | With its changes |
| `_gonvex_action_outbox` | Durable Action and Reducer-scheduled work (E.c) | `S:feed.rs:67-86` | Row deleted on completion |
| `_gonvex_reducer_idempotency` | Reducer receipts `(subject, idempotency_key)` → path, result (E.receipts) | `S:lib.rs:1140-1168` | **Never** in Rust; kernel expires results and keeps tombstones (0009) |
| `_gonvex_sync_bootstrap` | Digest receipt of installed capture/infrastructure scripts | `S:provision.rs:1104-1170`, `S:provision.rs:1533-1560` | n/a |
| `_gonvex_control_plane_membership_outbox` | `members` triggers queue `(account_id, member_id, status, membership_revision)` for the Control Plane directory | `S:provision.rs:1573-1601` | Deleted after projection |
| `_gonvex_files` | File metadata (E.d) | `R:storage.rs:30-44`, `R:storage.rs:580-601` | Deleted by `storage.delete` only |
| `_gonvex_schema_capabilities` | Feature flags of the schema installer | `S:schema_options.sql:1-3` | n/a |
| `_gonvex_table_options` | Capture class per table: `entity`/`derived`/`owned`/`metadata`, owner table/column/direction | `S:schema_options.sql:4-11` | n/a |
| `_gonvex_id_domains`, `_gonvex_identity_keys` | Public text id ↔ integer key, immutable, never deleted (triggers RAISE) | `S:schema_options.sql:13-16`, `S:schema_options.sql:22-23` | Never (by design) |
| `_gonvex_interned_text` | Immutable interned strings | `S:schema_options.sql:17`, `S:schema_options.sql:24-25` | Never (by design) |
| `_gonvex_record_views`, `_gonvex_record_guard`, `_gonvex_record_pending`, `_gonvex_record_dependencies` | Logical record views over normalized storage (c7) | `S:schema_options.sql:18-37` | Pending emptied each commit |
| `_gonvex_reference_*` (catalog, tables, rules, keys, targets, edges, bindings, configurations, sequence, guard, pending, requests) + view `gonvex_reference_violations` | Declared reference ledger (c8) | `S:reference_integrity/schema.sql:1-80` | Pending emptied each flush |
| `gonvex_migrations` | Migration ledger (E.a.6) | `S:provision.rs:968-980` | Never |
| `members` | Tenant identity (runtime-provisioned) | `S:provision.rs:1563-1572` | App-owned |

Control Plane tables this section touches (location ruled in 0012 R6, R7: scheduled jobs and storage uploads move to the tenant object; support sessions, commands and control idempotency stay in the control object): `gonvex_scheduled_jobs` (`S:control_schema.sql:261-301`), `gonvex_storage_uploads` (`S:control_schema.sql:350-358`), `gonvex_support_sessions`/`gonvex_support_commands` (`S:control_schema.sql:247-260`), `gonvex_control_idempotency` (`S:control_schema.sql:225-232`, never pruned).

#### E.a.5b Moving existing data (`gonvex-admin export-sqlite`)

`docs/sqlite-storage.md:181-330` describes the Postgres-to-SQLite export (`--keep-epoch` and the new-epoch reset, no DDL translation, integrity and sample checks, BLOB rejection). Disposition: **not contract** for the kernel. Tenant transfer from SQLite to a Durable Object and back is P03's tool (`cloudflare/tools/tenant-transfer/`), which must preserve the replica epoch per 0008 and reject BLOB values as this export does.

#### E.a.6 Migrations

| Rule | Rust | Source | DO | Disposition |
|---|---|---|---|---|
| File name | `NNNN_description.sql`, `[A-Za-z0-9_-]` | `R:modules.rs:441-453` | | **keep** |
| Directives | Leading `--` lines only: `-- gonvex:scope tenant\|control-plane` (required at sync; parser rejects missing); `-- gonvex:no-transaction`; `-- gonvex:foreign-key-rebuild` (any line, exact) | `R:modules.rs:464-488`, `S:provision.rs:953-958`; `D:migrations.md:9-22`. **DOC≠CODE**: doc says parser falls back to tenant; code errors on missing scope | | **keep** |
| Order | Lexical by name | `R:modules.rs:497` | | **keep** |
| Ledger | `gonvex_migrations(name PK, checksum SHA-256 hex of file, applied_at ms, duration_ms)`; applied name skipped; checksum mismatch fails sync | `S:provision.rs:968-1003`, `S:provision.rs:72-78`, `S:provision.rs:1049-1055` | | **keep** |
| Transactional | Default: one transaction (script + reference `after_migration` + `finalize` with provenance `command_id="migration:{name}"`, channel `system` + ledger row) | `S:provision.rs:1009-1068` | `BEGIN` only via `transactionSync` | **adapt** (`transactionSync`) |
| No-transaction | Each statement runs alone; ledger row only after all succeed | `S:provision.rs:1016-1019`; `D:migrations.md:32-41` | `VACUUM` forbidden | **adapt**: allow but reject `VACUUM` with a clear error |
| FK rebuild | Tenant + transactional only; `PRAGMA foreign_keys=OFF` around the transaction, `pragma_foreign_key_check LIMIT 1` must be empty, writer statements retired | `S:provision.rs:936-940`, `S:provision.rs:1008-1013`, `S:provision.rs:1019-1046`, `S:provision.rs:1070-1074` | `PRAGMA` forbidden | **adapt**: `PRAGMA defer_foreign_keys=ON` if DO allows it (S1), else **defer** with a contract note |
| Before tenant migrations | `reference_integrity::install` + `SCHEMA_OPTIONS_SQL` | `S:provision.rs:965-968` | | **keep** |
| Rollout | Tenant failures collected, sync fails, manifest not activated; Control Plane failure stops before tenants; new tenants get schema + all tenant migrations before ready | `D:migrations.md:28-30`, `D:migrations.md:43` | One DO per tenant | **adapt**: fan out per tenant DO; aggregate results |
| Dry run | `gonvex dev --once --dry-run` compares ledger/checksums | `D:migrations.md:45-51` | | **keep** |
| Drop empty undeclared columns | Opt-in `GONVEX_DROP_EMPTY_UNDECLARED_COLUMNS`; fleet-wide emptiness check, recheck before each drop | `D:migrations.md:53-65` | Fleet check = every tenant DO | **defer** (off by default) |

---

### E.b Scheduler

`R:wake.rs` is not the scheduler: it is targeted feed wakeups (`GONVEX_FEED_TARGETED_WAKE` default on, flush delay `GONVEX_FEED_WAKE_FLUSH_MS` 5,000 ms, `MAX_MARKS` 1,024; `R:wake.rs:1-30`, `R:wake.rs:51-77`, `R:wake.rs:273`). **defer** to the delivery packages (not contract). `R:cron_profile.rs` is profiler attribution only (`R:cron_profile.rs:1-8`): **drop**.

#### E.b.1 Storage and entry points

| Item | Rust | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Job table | Control Plane `gonvex_scheduled_jobs` (id, project, tenant, function_path, args, run_at, scheduled_for, cron_name, provenance, status `pending\|completed`, claim_sequence, claim_token, lease_until, wake_key, timestamps); indexes for due one-shot, due cron, completed, claim order; unique one pending run per (project,tenant,cron); unique one unclaimed wake per (project,tenant,wake_key) | `S:control_schema.sql:261-301` | | **adapt**: per-tenant table in the tenant DO (drop project/tenant columns, 0012 R6); status gains `dead` (IB2); one DO alarm at min(next due) |
| `ctx.scheduler.runAfter(delayMs, fn, args)` | Host call `scheduleAfter`; `run_at = now + delay` | `V8:gonvex_bootstrap.js:650-655` | | **keep** |
| `runAt(atUnixMs, fn, args)` | Host call `scheduleAt` | `V8:gonvex_bootstrap.js:656-661` | | **keep** |
| `wakeAt(atUnixMs, fn, args)` | `scheduleAt` with `coalesce:true` | `V8:gonvex_bootstrap.js:662-668` | | **keep** |
| From a Reducer | Transactional: writes an outbox row `action_path="_gonvex.scheduler.enqueue"`, payload `{jobId,function,args,runAtUnixMs,coalesce}`; `jobId="job_"+` deterministic intent UUID (`sha256(["gonvex.reducer.ids.v1",tenant,account,command,"deferred:schedule",ordinal])`), random UUID if no intent command; target must be a registered Reducer or Action | `R:host_calls.rs:30`, `R:host_calls.rs:809-843`, `R:host_calls.rs:897-930`, `R:host_calls.rs:62-85`, `R:host_calls.rs:365-379` | | **keep** (transactional in the tenant DO; promotion step becomes a direct insert in the same transaction) |
| Promotion | Drainer claims up to 100 schedule rows per turn, inserts job (no-op if id exists), deletes outbox row; failure → outbox retry backoff | `R:execution.rs:1146-1188`, `R:execution.rs:1299-1343`, `R:scheduler.rs:865-896` | | **replace**: insert job directly in the Reducer's transaction (same DB now) |
| From an Action | Non-transactional direct insert into Control Plane; needs `scheduler` capability | `R:action_calls.rs:554-600`, `R:scheduler.rs:286-339` | Action Worker → tenant DO RPC | **adapt** (DO RPC) |
| Kind check | Only `reducer`/`action` | `R:scheduler.rs:305-307` | | **keep** |

#### E.b.2 Crons

| Item | Rust (exact) | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Spec | `{name, function, args, scope: project\|tenant, intervalMs \| expression, dueWhen?}`; exactly one positive `intervalMs` or non-empty `expression`; unique names | `R:modules.rs:101-120`, `R:modules.rs:347-370` | | **keep** |
| Expression | 5 fields → prefixed `"0 "` (seconds); else passed to `cron` crate as is | `R:scheduler.rs:1146-1156` | | **keep** (same semantics in TS) |
| Sync | On start and on deploy: delete all pending cron rows of the project, insert next run per (cron, target tenant); project scope requires exactly 1 route | `R:scheduler.rs:681-787` | Deploy fans out to every tenant DO | **adapt** |
| Stagger | On by default (`GONVEX_SCHEDULER_STAGGER=0\|false\|off` disables); key = first 8 bytes BE of `sha256("{project}\0{tenant}\0{cron}")` | `R:scheduler.rs:80-133` | | **keep** |
| Interval phase | `phase = key mod interval`; `next = after - ((after - phase) mod interval) + interval` | `R:scheduler.rs:1083-1090` | | **keep** |
| Expression offset | `offset = key mod min(gap to next nominal, spread)`; spread `GONVEX_SCHEDULER_CRON_SPREAD_MS` default 600,000; latest nominal ≤ after is still due at its staggered time | `R:scheduler.rs:98-117`, `R:scheduler.rs:1091-1144` | | **keep** |
| Missed runs | Skipped, never replayed (`next_cron_run`); one pending run per cron/tenant | `R:scheduler.rs:1039-1052`, `S:control_schema.sql:289-296` | | **keep** |
| Job id | `cron_` + hex of first 16 bytes of `sha256("{project}\0{tenant}\0{name}\0{rfc3339}")` | `R:scheduler.rs:1173-1181` | | **keep** |
| Next run | Inserted in `finish` after the run completes | `R:scheduler.rs:658-664` | | **keep** |
| `dueWhen` | One admitted read statement (`select/with/values/explain`), run on a tenant read transaction, `$1` bound; runs V8 only if first column of first row is truthy (not NULL, 0, 0.0, '', '0'); errors fail open; skip still finishes the job | `R:modules.rs:111-116`, `R:modules.rs:372-378`, `R:scheduler.rs:493-534`, `R:scheduler.rs:1054-1071`, `R:scheduler.rs:443-448`. **DOC≠CODE**: comment says `$1` = run's due time; code binds `Utc::now()` (`R:scheduler.rs:520`) | | **keep** (bind now) |

#### E.b.3 Claim, leases, execution

| Item | Rust (exact) | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Claim | `UPDATE … claim_sequence+1, claim_token="{owner}:{seq}", lease_until=now+120000` for `pending ∧ run_at≤now ∧ (unclaimed ∨ lease expired)`, order one-shot before cron, then run_at, id; `LIMIT min(free permits,16)` | `R:scheduler.rs:44-60`, `R:scheduler.rs:26`, `R:scheduler.rs:256-268` | | **adapt**: in-DO claim; lease kept only for work handed to the Action Worker |
| Lease renewal | Every 30 s while running, guarded by claim_token | `R:scheduler.rs:422-434`, `R:scheduler.rs:467-491` | | **adapt** (alarm re-check) |
| Idle | Sleep until next due, max 30 s (`MAX_IDLE_SLEEP`); woken by enqueue/sync/finish | `R:scheduler.rs:27-29`, `R:scheduler.rs:1027-1037` | | **adapt** (DO alarm = min next due) |
| Concurrency | `GONVEX_SCHEDULER_CONCURRENCY` 2 process-wide; 1 per tenant; waits for user calls up to `GONVEX_SCHEDULER_YIELD_MS` 2,000 ms; thread nice 5 | `R:scheduled_lane.rs:1-22`, `R:scheduled_lane.rs:98-125`, `R:scheduler.rs:437-439` | | **drop** process caps (lane = not contract); **keep** one run per tenant at a time |
| Provenance | Stored provenance; empty root → fresh system provenance; `parent_command_id=command_id`, `command_id=job.id`, channel `Scheduler`; one durable hop | `R:scheduler.rs:571-590` | | **keep** |
| Actor | `_gonvex_system` or absent → system session; else re-resolve Member (`tenant_session_for_account`) | `R:scheduler.rs:550-563` | | **keep** |
| Retries | None. `finish` marks `completed` whether the run succeeded or failed (`_success` ignored); only a crash (lease expiry after 120 s) re-runs it, so at-least-once with no dedupe (scheduled Reducer runs without idempotency key) | `R:scheduler.rs:627-669`, `R:scheduler.rs:603-613` | | **replace** (IB2): still one attempt and no new retries, but a failed run ends `dead`, not `completed` (E.b.5) |
| Retention | Completed rows deleted after 7 d; prune every 600 s, batches of 5,000, max 20 batches per pass | `R:scheduler.rs:21-25`, `R:scheduler.rs:385-415`, `R:scheduler.rs:999-1016` | | **adapt** (alarm, paged) |
| Hop limit | `MAX_DURABLE_HOPS=32` (`GONVEX_MAX_DURABLE_HOPS` overrides, >0); `durable_hop` fails at ≥ max; scheduler: job error then completed | `MR:lib.rs:25`, `MR:lib.rs:243-251`, `R:execution.rs:1395-1408`, `R:scheduler.rs:586-590` | | **keep** the hop limit; a scheduled job that fails on it ends `dead`, not `completed` (IB2, E.b.5) |

#### E.b.4 `wakeAt` coalescing

| Item | Rust | Source | Disposition |
|---|---|---|---|
| Key | `wake_` + hex first 16 bytes of `sha256("{function}\0" + canonical args (object keys sorted, recursive))` | `R:scheduler.rs:898-938` | **keep** |
| Merge | `INSERT … WHERE NOT EXISTS(id) ON CONFLICT(project,tenant,wake_key) WHERE pending ∧ claim_token='' ∧ wake_key<>'' DO UPDATE run_at=min, scheduled_for=min`; returns surviving id | `R:scheduler.rs:860-896`, `S:control_schema.sql:297-301` | **keep** |
| Mid-run | A claimed wake leaves the unique index, so a wake during the run queues one more | `S:control_schema.sql:297-298` | **keep** |

#### E.b.5 Intended behavior (Actions)

Today: outbox Actions retry forever with backoff capped at 15 min (E.c); scheduled Actions never retry. Intended (IB2, `contract-changes/0001-intended-behavior.md`): **replace**. Outbox Actions get `max_attempts` (default 8) and a terminal `dead` status with `last_error`. Scheduled jobs keep their single attempt (no new retries), but a failed run ends `dead` instead of `completed`. Dead rows are kept for operators and listed through an operator route; they never run again unless an operator requeues them. Long agent Actions run in **Workflows** (S4); others in the stateless Action Worker.

---

### E.c Outbox

#### E.c.1 Action outbox (`_gonvex_action_outbox`)

| Item | Rust (exact) | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Enqueue | `ctx.actions.enqueue(fn,args)` (Reducers only; rejects empty path and `_gonvex.scheduler.enqueue`); `INSERT … (id, action_path, args, actor_user_id NULLIF '', actor_email NULLIF '', provenance)` in the Reducer's transaction; id = deterministic intent UUID (`deferred:action`); sets `outbox_written` | `V8:gonvex_bootstrap.js:624`, `R:host_calls.rs:782-808`, `S:lib.rs:1659-1688` | Same DO SQLite transaction | **keep** |
| Wake | After commit, if `outbox_written`: tenant queued for the single drainer | `R:execution.rs:696-712`, `R:execution.rs:1091-1102` | DO alarm (set to now) | **adapt** |
| Safety sweep | Every 30 s all tenants queued (`OUTBOX_SWEEP_INTERVAL`) | `R:scheduler.rs:30-32`, `R:scheduler.rs:211-232`, `R:execution.rs:1345-1363` | Alarm per DO | **adapt** |
| Drain turn | Read-only `outbox_due` probe; promote schedules; acquire Action permit; claim one Action (`status='processing'`, `locked_at=now`, `attempts+1`), oldest `available_at, created_at`, or `processing` with `locked_at < now-300000`; tenant re-queued at the back (round-robin); idle drainer waits 5 s | `R:execution.rs:1104-1144`, `R:execution.rs:1366-1393`, `S:lib.rs:1214-1307` | | **adapt**: alarm drains in the DO; dispatches to the Action Worker (service binding) or **Workflows** for long agent Actions; **Queues** optional for fan-out (15 min consumer cap) |
| Lease | 300,000 ms; renewed every 30 s, attempt-guarded (`attempts=$2`); failed renewal abandons the delivery without touching the row | `S:lib.rs:1309-1327`, `R:execution.rs:1262-1276` | | **adapt** |
| Hop guard | `durable_hop(32)` fails → row deleted (`complete_action`) with an error log | `R:execution.rs:1197-1214` | | **replace**: mark `dead` instead of delete |
| Actor | `_gonvex_*` → fallback system session; else Member re-resolved; resolve failure is a delivery error (retried) | `R:execution.rs:1215-1227` | | **keep** |
| Provenance | `deadline_unix_ms=None`, `parent_command_id=command_id`, `command_id="outbox-{id}"`, channel `System`; missing provenance → fresh with `durable_hops=1` | `R:execution.rs:1228-1250` | | **keep** |
| Success | `DELETE` row | `S:lib.rs:1329-1341`, `R:execution.rs:1277-1282` | | **keep** |
| Failure | `status='pending'`, `available_at=now+retry_delay_ms(attempts)`, `last_error`; delay `1000·2^(attempts-1)` ms capped 900,000 (reached at attempt 11); **no maximum: retries forever** | `S:lib.rs:1343-1383`, `R:execution.rs:1283-1295` | | **replace** (E.b.5: max attempts + dead state) |
| Guarantee | At-least-once after commit; never before commit; Action runs without idempotency key, so a lease loss can duplicate side effects; order per tenant by `available_at, created_at`, one in flight per tenant drain turn | as above | | **keep** |
| Export | Pending and in-flight rows copied; `_gonvex_*` pseudo-actors stripped; legacy ids mapped | `D:sqlite-storage.md:220-230`, `D:sqlite-storage.md:263` | P03 | **keep** |

#### E.c.2 Membership outbox (`_gonvex_control_plane_membership_outbox`)

| Item | Rust | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Enqueue | `members` AFTER INSERT/UPDATE/DELETE triggers upsert per account, revision-guarded; delete writes `revoked`, revision+1 | `S:provision.rs:1573-1601` | Triggers OK | **keep** |
| Drain | Read up to 1,000 rows; per row: Control Plane upsert into `account_tenant_index` (revision-guarded), then delete row (`membership_revision<=rev`); one transaction per row; yield between batches | `R:membership_projector.rs:124-180` | Tenant DO → Control DO RPC | **adapt**: one batched RPC per alarm (per-row transactions violate the "no SQL in a loop" rule) |
| Cadence | Wake when a commit changed `members`; else backoff 2→4→8→10 s cap, reset to 2 s after work | `R:execution.rs:703-705`, `R:membership_projector.rs:30-60`, `R:membership_projector.rs:78-84` | Alarm | **adapt** |
| Guarantee | At-least-once, idempotent via revision guard; crash-safe | `R:membership_projector.rs:1-6` | | **keep** |

---

### E.d Storage and files

#### E.d.1 Model

| Item | Rust (exact) | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Backend | S3-compatible via SigV4 (`reqwest`), path- or host-style | `R:storage.rs:1-4`, `R:storage.rs:705-760`, `R:storage.rs:854-872` | R2 binding in the DO/Worker; S3 API only for presigned URLs | **adapt** (R2) |
| Metadata table | `_gonvex_files(id, tenant_id, owner_id, bucket, object_key, content_type, size_bytes, checksum, visibility private\|tenant\|public, status pending\|uploaded, created_at, uploaded_at, deleted_at)` + indexes on tenant_id, owner_id; DDL run on every storage call | `R:storage.rs:30-44`, `R:storage.rs:580-601` | Tenant DO SQLite | **keep** table; **adapt**: provision once |
| Object key | `{safe(project)}/{safe(tenant)}/{fileId}`; fileId = UUID v4 simple | `R:storage.rs:874-881`, `R:storage.rs:996-1010`, `R:storage.rs:162` | | **keep** |
| Callers | Actions with `storage` capability: all ops; Reducers: `getMetadata` only (in-transaction) | `R:action_calls.rs:601-615`, `R:host_calls.rs:765-780` | | **keep** |
| Visibility | `tenant`/`public` as requested, anything else `private`; anonymous sessions forced `tenant` | `R:storage.rs:163-167`, `R:storage.rs:926-932` | | **keep** |
| Owner | Anonymous → `''`; else request `ownerId` if non-empty, **else caller's account id**: any caller can assign another account as owner | `R:storage.rs:934-943` | | **replace** (IB1): owner = caller account; only a system principal (scheduler, migrations, no account) may set another owner |

#### E.d.2 Operations

| Op | Rust | Source | Disposition |
|---|---|---|---|
| `generateUploadUrl` | Anonymous requires `GONVEX_PUBLIC_URL`; declared `size` ≤128 MiB (`MAX_DIRECT_STORE_BYTES`); inserts `pending` row; if public URL set: registers ticket in Control Plane `gonvex_storage_uploads` and returns proxy `POST` URL, else presigned `PUT`; TTL `expiresMs` (ceil to s) default 900 s; returns `{fileId,url,method,headers}` | `R:storage.rs:151-217`, `R:storage.rs:25-27` | **replace** (IB1: owner = caller): R2 presigned PUT or Worker proxy (Worker body limit 100 MB on Free/Pro < 128 MiB: proxy path needs presigned/multipart); ticket moves into tenant DO |
| `getUrl` | `metadata(finalize=true)`; proxy URL (10 min) if public URL set; else unsigned object URL when `public`; else presigned GET 10 min | `R:storage.rs:219-231` | **replace** (IB1: owner check for private files) |
| `generateDownloadUrl` | `metadata(finalize=false)`; TTL `ttlMs` default 600 s; proxy or presigned GET | `R:storage.rs:232-243` | **replace** (IB1: owner check for private files) |
| `getMetadata` | `metadata(finalize=true)` | `R:storage.rs:244-248` | **replace** (0012 R12: owner check for private files) |
| `delete` | `metadata(false)`; DELETE object (404 ok); then DELETE row (not atomic with object) | `R:storage.rs:249-270`, `R:storage.rs:687-703` | **replace** (IB1: owner check; R2 `delete`) |
| `store` | base64 ≤128 MiB decoded; PUT; insert `uploaded` row with sha256 checksum | `R:storage.rs:271-318` | **adapt**: 128 MiB base64 through V8 exceeds a 128 MB isolate; cap lower and route large blobs via upload URLs |
| Finalize on read | `pending` + `finalize` → HEAD; if present, set `uploaded`, size, content type, ETag checksum. HEAD runs **inside an open tenant write transaction** | `R:storage.rs:604-680`, `R:storage.rs:642-683` | **adapt**: HEAD first (R2 `head`), then `transactionSync` (no I/O inside it) |
| Tenant scoping | Every metadata read filters `id` and `tenant_id` | `R:storage.rs:645-650` | **keep** |
| `list_project_files` (operator) | Per route, uploaded and not deleted, newest first, limit | `R:storage.rs:494-572` | **adapt** (fan-out over tenant DOs) |
| Sandbox `importFile` | `read_file`: owner check, `uploaded` required, size ≤ `sandbox.max_file_bytes` | `R:sandbox.rs:661-680`, `R:storage.rs:324-363` | **keep** |

#### E.d.3 Signing, single use, downloads

| Item | Rust (exact) | Source | Disposition |
|---|---|---|---|
| Proxy URL | `{GONVEX_PUBLIC_URL}/storage/{key}?exp={unix s}&sig={hex}[&upload=1]` | `R:storage.rs:883-892` | **keep** URL shape |
| Proxy signature | `hex(HMAC-SHA256(key="gonvex-storage:"|"gonvex-storage-upload:"+secret_access_key, "{key}\n{exp}"))`; verify: `exp > now`, non-empty, constant-time compare | `R:storage.rs:365-371`, `R:storage.rs:894-904` | **keep** (secret from Worker secret) |
| Proxy TTL | Unclamped (caller TTL used as given) | `R:storage.rs:236`, `R:storage.rs:883-884` | **adapt**: clamp like presign |
| Presign | SigV4 query, `UNSIGNED-PAYLOAD`, `X-Amz-Expires` clamped 1–604,800 s | `R:storage.rs:796-838` | **adapt** (R2 S3 API) |
| Single-use upload (proxy mode) | Ticket `gonvex_storage_uploads(object_key PK, project, tenant, file_id, consumed_at)`; reject if consumed or row not `pending`; claim via upsert `consumed_at=now WHERE consumed_at IS NULL` committed before PUT; PUT with `If-None-Match: *` (412/409 → "upload already consumed" → HTTP 409); failed PUT releases claim; tickets never pruned | `S:control_schema.sql:350-358`, `R:storage.rs:382-470`, `R:storage.rs:472-490`, `R:lib.rs:829-841` | **adapt**: ticket in tenant DO; R2 conditional put (`onlyIf`) |
| Single-use upload (presign mode) | None: presigned PUT reusable until expiry; no size condition | `R:storage.rs:203-207` | **replace**: always ticketed |
| Upload route | `POST`/`PUT /storage/{*key}` requires `upload=1` and valid upload signature; body read ≤128 MiB (`body::to_bytes(…,128<<20)`) else 413; returns `{storageId}`; router also has `DefaultBodyLimit` 64 MiB (does not apply to a raw `Body`) | `R:lib.rs:560-576`, `R:lib.rs:780-841` | **adapt** (Worker route; body limit per plan) |
| Download route | `GET /storage/{*key}`: rejects `upload` param or bad signature (403); forwards `Range`; passes 200/206; forwards `accept-ranges, content-length, content-range, content-type, etag, last-modified`; `Cache-Control: private, max-age=300`; 404 otherwise; streams body | `R:lib.rs:662-720` | **replace** (0012 R13: file-row lookup first; then R2 `get` with `range`) |
| Inline allowlist | Inline only: `image/png, image/jpeg, image/gif, image/webp, image/avif, audio/mpeg, audio/ogg, audio/wav, audio/webm, audio/mp4, video/mp4, video/webm, video/ogg, application/pdf`; else `application/octet-stream` + `attachment`; always `nosniff`; CSP `default-src 'none'; base-uri 'none'` (inline) or `sandbox; default-src 'none'; base-uri 'none'` | `R:lib.rs:724-778` | **keep** |
| CORS | `*`; `GET, POST, PUT, OPTIONS`; header `Content-Type`; OPTIONS requires valid upload signature | `R:lib.rs:843-885` | **keep** |
| Size limits summary | Declared upload ≤128 MiB; proxy body ≤128 MiB; `store` ≤128 MiB; sandbox import ≤ `sandbox.max_file_bytes` | `R:storage.rs:26`, `R:storage.rs:158`, `R:storage.rs:274-280`, `R:lib.rs:815-818` | **adapt** to Worker/isolate limits |

#### E.d.4 Ownership and visibility checks: where Rust checks today

| Path | Tenant check | Owner/visibility check today | Intended |
|---|---|---|---|
| Sandbox `importFile` → `read_file` | yes | **yes**: `private ∧ owner_id ≠ caller account` → "file not found" (`R:storage.rs:341-343`) | keep |
| `getUrl` | yes | **no** (`R:storage.rs:219-231`) | owner check for `private` |
| `generateDownloadUrl` | yes | **no** (`R:storage.rs:232-243`) | owner check for `private` |
| `getMetadata` (Action) and Reducer `ctx.storage.getMetadata` | yes | **no** (`R:storage.rs:244-248`, `R:host_calls.rs:765-780`) | owner check (0012 R12) |
| `delete` | yes | **no** (`R:storage.rs:249-270`) | owner check |
| `GET /storage/{key}` proxy | signature only; no `_gonvex_files` lookup (deleted-row or status not checked) | **no** (`R:lib.rs:662-720`) | **replace** (IB1, 0012 R13): look up the `_gonvex_files` row on every GET; refuse a deleted file, and a private file whose URL was not minted for its owner |
| `ownerId` on create | n/a | caller may set any owner (`R:storage.rs:934-943`) | owner = caller |
| `public` visibility | n/a | only changes `getUrl` in presign mode (unsigned URL); in proxy mode ignored | keep |

Dispositions: **replace** with the intended owner check on every download URL and delete. Rule: `visibility='private'` requires `owner_id = caller account` on every read, URL mint, metadata read and delete. No session kind is exempt on reads (Rust's `read_file` compares the owner with the session account id and has no exemption, `R:storage.rs:341-343`). See IB1 and 0012 R12, R13.

---

### E.e Presence v2

#### E.e.1 Parameters (code defaults)

Disposition: the 250 ms coalescing window and the `presence.*` frame shapes are **keep**; liveness numbers (ping interval, liveness timeout) are **open until S3** (0012 R22); the rest are tuning.

| Parameter | Default | Env | Source | Doc |
|---|---|---|---|---|
| Enabled | on | `GONVEX_PRESENCE_V2` | `R:presence.rs:48-50`, `R:presence.rs:87-101` | `D:performance/presence-v2.md:400-411` |
| Coalescing window | 250 ms | `GONVEX_PRESENCE_TICK_MS` | `R:presence.rs:51-55` | §3.4 (**DOC≠CODE** in §9 table: "tick 1 s (default)" is historical) |
| Leading edge | on | `GONVEX_PRESENCE_LEADING_EDGE` | `R:presence.rs:56-59`, `R:lib.rs:1648-1669` | |
| Offline grace | 10,000 ms (0 allowed) | `GONVEX_PRESENCE_OFFLINE_GRACE_MS` | `R:presence.rs:60-62`, `R:config.rs:712-716` | |
| Server Ping interval | 20 s | `GONVEX_PRESENCE_PING_SECONDS` | `R:presence.rs:63-65` | |
| Liveness timeout | 60 s, forced ≥ 2× ping | `GONVEX_PRESENCE_LIVENESS_SECONDS` | `R:presence.rs:66-69`, `R:config.rs:688-693` | |
| Max pending publications per connection | 32 → resync snapshot | none | `R:presence.rs:156-169`, `R:presence.rs:97` | |
| Support last-seen refresh | 300 s, first at random offset | `GONVEX_SUPPORT_LAST_SEEN_REFRESH_SECONDS` | `R:presence.rs:81-84`, `R:lib.rs:400-415` | |
| Closed-session flush | every 5 s | none | `R:lib.rs:386-399` | |
| Shutdown flush | ≤3 s | none | `R:lib.rs:4819-4820`, `R:lib.rs:436-450` | |
| Legacy beat / transition paths | empty | `GONVEX_PRESENCE_LEGACY_BEAT_PATHS`, `…_TRANSITION_PATHS` | `R:presence.rs:70-80` | |

#### E.e.2 State and transitions

| Item | Rust | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Scope | `(project, tenant)`; Member id = tenant Member id | `R:presence.rs:112-125` | One tenant DO = one scope | **keep** |
| Eligible sessions | Authenticated tenant Member; excludes anonymous, `delegation` sessions, empty member id, `_gonvex_service:` members | `R:lib.rs:4798-4815` | | **keep** |
| Per-connection state | `active`/`idle`; `parse_state`: `active`, `idle`, `offline`, `gone`(=offline), case-insensitive, else ignored | `R:presence.rs:104-111` | In-memory → lost on hibernation | **adapt**: `ws.serializeAttachment` (≤2 KB) holds state, member, subscription id; rebuild on wake from `getWebSockets()` (nothing ephemeral in SQLite) |
| Aggregate | any connection `active` → active; all `idle` → idle; none → offline (after grace) | `R:presence.rs:191-207` | | **keep** |
| `presence.set` | Moves the connection if scope/member differs (no grace); `offline` removes this connection now, no grace; else upsert and clear grace; marks dirty | `R:presence.rs:414-474`, `R:lib.rs:3712-3726` | | **keep** |
| Disconnect / re-auth | Socket close (guard drop) or re-auth: detach with grace; also unsubscribes; support sessions queued for last-seen flush | `R:presence.rs:541-567`, `R:presence.rs:760-775`, `R:presence.rs:871-893`, `R:lib.rs:2742` | `webSocketClose`/`webSocketError` | **adapt** |
| Publication | Due = max(ready, last publication + window); ready = now or grace end; publish dirty Members whose aggregate differs from published; offline removes Member; one encoded patch shared by all subscribers; no per-connection timer; one scheduler sleeps to earliest due | `R:presence.rs:219-261`, `R:presence.rs:619-638`, `R:presence.rs:803-868`, `R:lib.rs:1648-1669` | | **adapt**: `setTimeout` while awake for the 250 ms window; grace end on the DO alarm (multiplexed) |

#### E.e.3 Frames

| Frame | Shape | Source | Disposition |
|---|---|---|---|
| Capability | `session.ready.capabilities.presence=1`; client `auth.capabilities.presence=1` (per auth; re-auth without it turns Ping liveness off) | `D:performance/presence-v2.md:66-92`, `R:lib.rs:2817` | **keep** |
| `presence.set` | `{type,state}` fire-and-forget; ignored without eligible session | `P:protocol/src/index.ts:363`, `R:lib.rs:3712-3726` | **keep** |
| `presence.subscribe` | `{type,id}` → `presence.snapshot` (same id) of online Members sorted by id, then patches; ineligible → `presence.error` "presence requires an authenticated tenant member session" or "presence v2 is disabled" | `P:protocol/src/index.ts:364`, `R:presence.rs:491-522`, `R:presence.rs:777-801`, `R:lib.rs:3727-3758` | **keep** |
| `presence.unsubscribe` | `{type}` | `R:presence.rs:524-539`, `R:lib.rs:3759-3762` | **keep** |
| `presence.snapshot` | `{type,id,serverTime,members:[{memberId,state,since}]}` | `P:protocol/src/index.ts:441` | **keep** |
| `presence.patch` | `{type,serverTime,changes:[…]}`; `since` = publication wall time; lagging connection merges batches (last state per Member wins) | `P:protocol/src/index.ts:443`, `R:presence.rs:569-604` | **keep** |
| In batches | Rides inside `replica.batch` for batch connections | `P:protocol/src/index.ts:638-639` | **keep** |

#### E.e.4 Liveness

| Item | Rust | Source | Cloudflare | Disposition |
|---|---|---|---|---|
| Server Ping | Every 20 s to connections with `presence_negotiated`: protocol Ping (opcode 0x9, empty) | `R:lib.rs:1710-1712`, `R:lib.rs:2424-2439` | **A Worker cannot send protocol pings** | **replace**: S3 decides |
| Alive signal | `max(last inbound frame, socket write/backpressure activity)`; ≥ liveness timeout at a Ping tick → Close `4000 "presence liveness timeout"` and drop; detection 60–80 s | `R:lib.rs:2424-2436` | Hibernation API reports close/error only; half-open sockets go undetected | **replace** options for S3: (1) client sends an app-level ping string, `setWebSocketAutoResponse` answers without waking the DO and `getWebSocketAutoResponseTimestamp(ws)` checked on the alarm (needs a client change → contract note; brief says clients must work unedited); (2) rely on edge TCP close events plus any existing periodic client frame; measure half-open detection time |
| Client Ping | Server answers client Pings with Pong | `R:lib.rs:3849-3853` | Runtime answers | **keep** |

#### E.e.5 Legacy beat paths and support commands

| Item | Rust | Source | Disposition |
|---|---|---|---|
| Legacy beat | If path in beat list, non-control scope, connection already got a replica watermark (`sent_replica_revision>0`), eligible session: answer `reducer.result` with `result=now ms`, `committedRevision=last sent watermark`; no V8, no write; registers caller `active` only if not yet present | `R:lib.rs:4937-4956`, `R:presence.rs:476-488` | **keep** |
| Legacy transition | If path in transition list and connection has no `presence:1`: run Reducer normally; on success apply `args.state` to the feed | `R:lib.rs:4958-4995` | **keep** |
| `control.support.heartbeat` | First call per connection (or changed tenant/account/release/environment) upserts `gonvex_support_sessions` (`id="support_{connectionId}"`); repeats answered from memory with same `{sessionId}` (no write, no idempotency row, no notify) | `R:lib.rs:4884-4935`, `R:presence.rs:656-726`, `R:control.rs:3185-3212` | **adapt**: support sessions live in the Control DO (0012 R7) |
| `error.register`/`error.heartbeat` | Same dedupe; `error.ack` | `D:performance/presence-v2.md:323-326`, `R:lib.rs:3763-3770` | **adapt** |
| Last seen | Not refreshed per heartbeat; open sessions reported as now; written on close (5 s batches), every 300 s for open sessions (≤500 per statement, one transaction), and on clean stop | `R:lib.rs:381-450`, `R:lib.rs:4822-4845` | **adapt**: alarm-driven batch RPC to Control DO |
| `control.support.sendCommand` | ProjectAdmin; session must exist; inserts `gonvex_support_commands`; `notify("gonvex_support_command")` is called only on the Query dispatch path, so for this Reducer it never fires and no `support.command` frame is sent (C.1 row 52) | `R:control.rs:164-169`, `R:control.rs:172-215`, `R:control.rs:3214-3234`, `R:control.rs:4497-4502` | **keep** no push (0012 R8); rows stored in the Control DO |
| `control.support.ackCommand` | Account; only the session's own account; sets `acknowledged_at` once | `R:control.rs:3235-3244`, `R:control.rs:4504-4508` | **keep** |
| `control.support.pruneSessions` | ProjectAdmin; `olderThanSeconds` 60–31,536,000; never deletes live session ids | `R:control.rs:3245-3268` | **adapt** (live ids come from tenant DOs) |
| list/get sessions, tenants, errors | `listSessions`/`listErrors` ProjectAdmin live; `listTenants` Developer live; `get*` ProjectAdmin | `R:control.rs:4480-4496`, `R:control.rs:2374-2500` | **keep** |
| Durability | None: restart → all offline until clients re-announce | `D:performance/presence-v2.md:353-362` | **keep** (DO eviction = restart) |

---

### E.receipts Reducer receipts

Disposition for every row: **keep** except pruning, which is **replace**: per 0009 for `_gonvex_reducer_idempotency`, per 0012 R9 (24 h expiry) for `gonvex_control_idempotency`.

| Item | Rust | Source |
|---|---|---|
| Table | `_gonvex_reducer_idempotency(subject TEXT DEFAULT '', idempotency_key, path, result JSON, created_at ms, PK(subject,idempotency_key))`, index on `created_at`; legacy `_gonvex_mutation_idempotency` renamed | `S:lib.rs:1140-1168` |
| Claim | `INSERT … ON CONFLICT DO NOTHING` in the Reducer transaction; `subject` = caller account id | `S:lib.rs:1600-1617`; the receipt lookup query is `R:execution.rs:198-215` |
| Result | `UPDATE … SET result` in the same transaction | `S:lib.rs:1620-1637` |
| Replay | Same key → stored result; path mismatch → `IdempotencyPathMismatch` | `S:lib.rs:1172-1199` |
| Invitation accept exception | Its special unclaimed branch does not call generic replay/path validation; an existing foreign-path receipt can finish the Control invitation without changing the member role. **Keep** observed outside-gate reference behavior for K22; hardening deferred in 0072. | `S:lib.rs:1600`; `R:control.rs:1701`, `R:control.rs:1899`; both-backend probe in 0072 |
| Pruning | **None.** No `DELETE FROM _gonvex_reducer_idempotency` anywhere in `rust/`, `go/` or `packages/`; the `created_at` index has no reader. Same for Control Plane `gonvex_control_idempotency` | grep; `S:lib.rs:1163-1164` |
| Client offline window | App-declared `client-contract.json` `{version, offlineMaxAgeMs}`: positive integer ms or `null` (= unlimited); no SDK default (no contract → no limit); validated at build and in the runtime; test example 604,800,000 (7 d) | `P:client/src/client-upgrades.ts:6`, `P:gonvex/src/module-artifact.ts:218-224`, `R:modules.rs:1252-1260`, `R:modules.rs:1268-1276` |
| What the window gates | Only creating new offline intents (`Date.now()-lastOnlineAtMs > offlineMaxAgeMs` → "Offline editing window expired"); queued outbox entries have no expiry and replay whenever the client reconnects | `P:client/src/index.ts:1652`, `P:client/src/index.ts:1674`, `P:client/src/outbox.ts:232`, `P:client/src/outbox.ts:614` |

Disposition: **replace** (intended, IB3). Rule in `contract-changes/0009-receipt-expiry.md`: results expire after `offlineMaxAgeMs + 24 h` and leave a tombstone; a call that hits a tombstone gets `reducer.error{class:"rejected"}` instead of running again; a `null` window keeps receipts as in Rust; tombstones are deleted after 365 days.

### E.timers One alarm per tenant DO

Disposition: **adapt** (every Rust timer listed here multiplexes onto the tenant DO's single alarm; handlers stay idempotent).

Every periodic task in this section multiplexes onto the single DO alarm, set to the minimum of: next scheduled job `run_at`; next outbox `available_at` or lease expiry; change-feed prune (hourly); completed-job prune (600 s); receipt prune; membership projection backoff (2–10 s); presence grace end (when hibernating); support last-seen flush (5 s close batch, 300 s refresh); targeted-wake flush (5 s). DO alarms are at-least-once, so every handler must be idempotent (the Rust handlers already are: claims, revision guards, conditional deletes).

## F. Decision log and open decisions

Every ruling below lives in `docs/cloudflare/contract-changes/`. A package that disagrees with a ruling writes a
new note and waits for the lead.

| Note | Subject | Status | Blocks |
|---|---|---|---|
| 0001 | Intended behavior IB1-IB4 | decided | K07, K11, K12, K13, K18 |
| 0002 | Webhooks have no runtime route; the Ingress Worker replaces external adapters | decided | K18 |
| 0003 | P01 scaffold decisions (vitest plugin, lockfile churn, CI secrets) | decided | none |
| 0004 | Tenant arrives in the first `auth` frame; socket routing | decided in 0016 G0-1 (gateway objects) | K05, K06 |
| 0005 | Loading module bundles without `eval` | decided in 0016 G0-2 (bundle in the Worker version; S5 tests the Worker Loader) | K04, K19 |
| 0006 | Module SQL that opens savepoints | decided in 0016 G0-3 (nested storage transactions) | K01, K04, K07 |
| 0007 | PBKDF2-SHA256 at 210,000 rounds | decided, K06 verifies | K06 |
| 0008 | Replica epoch hashes the stored `database_url` (cutover duties in M03) | decided | P03, K10, M03 |
| 0009 | Reducer receipt expiry with tombstones | decided | K07 |
| 0010 | Change capture without custom functions or hooks | decided in 0016 G0-4 (plain-SQL capture via A01) | K02, K03 |
| 0011 | `$N` placeholders, read-only Queries, 100-parameter limit | decided | K01, K04 |
| 0012 | Lead rulings R1-R26 on items found during the freeze (R22 closed by 0016 G0-6; R25 open) | decided except R25 | K05, K06, K11, K12, K13, K14, K19 |
| 0013 | Tenant schema objects DO SQLite refuses | ruled in 0014 | A01 |
| 0014 | Kernel-side deterministic schema transform of the unchanged app migrations (package A01) | decided | A01, K02, K03, K07, G1 |
| 0015 | Rulings from stage-0 package reviews (error text, digests, P03/P04 acceptance, stream bindings) | decided | P02, K15, M02, M03 |
| 0016 | Rulings at gate G0 from the spikes | decided except G0-11 (R25) | K01-K05, K11-K14, K19 |
| 0017 | Presence liveness without server pings | decided in 0016 G0-6 (no SDK change in stage 1) | K14 |
| 0018 | Platform WebSocket message caps | decided in 0016 G0-7 | K05, K10, P02 |
| 0019 | Stage 1b (R01–R06) and gate G3b before G4; L02–L05 removed | decided (Gabriel) | R01–R06, G4 |
| 0020 | K05 session-layer findings (proposed by K05) | ruled in 0022 §4 | K05, K10 |
| 0021 | K04 module-host interfaces and R25 (proposed by K04) | ruled in 0022 §3 | K01, K04, K19, S5 |
| 0022 | Wave-1 review rulings: K01/K04 ownership, K01 deviations, R25, K05 findings, A01 part 1, harness flake | decided | K01-K05, K08, S5 |
| 0023 | Hook-capture quirk on columns added after the row (proposed by A01) | ruled in 0024 §1 | K03, M02 |
| 0024 | Second wave-1 pass: A01 complete, watermark self-check for K10, K06 findings, numbering | decided | K03, K06, K10, K19, R03, R04, V03 |
| 0025 | K06 control object findings (proposed by K06) | ruled in 0024 §3 | K06, V03 |
| 0026 | K03 change-capture differences (proposed by K03) | ruled in 0028 §2 | K03, K07, M02 |
| 0027 | K02 migration findings (proposed by K02) | ruled in 0028 §3 | K02, K04, K07, K19 |
| 0028 | Third wave-1 pass: S5 (G0-2 stands; Facet is a stage-3 candidate), K03 and K02 rulings | decided | K02, K03, K04, K07, K19, M02, R05 |
| 0029 | K06 merged over a known external CI failure (GitHub runner DNS outage) | decided | none |
| 0030 | K07 Reducer path findings | ruled in 0032 | K07, K10, K11 |
| 0031 | K09 visibility findings | ruled in 0032 (IB5) | K08, K10 |
| 0032 | First wave-2 review: K09, K07, K08, K11, K13 rulings; IB5; note renumbering | decided | K07, K08, K10, K11, K13, K19, V01 |
| 0033 | K08 live-query findings | ruled in 0032 | K08, K10 |
| 0034 | K11 scheduler findings | ruled in 0032 | K11, K12, R06 |
| 0035 | K13 files findings | ruled in 0032 | K13, K19, V01 |
| 0036 | K19 deploy and operator routes findings | ruled in 0037 | K02, K07, K10, K19 |
| 0037 | Second wave-2 review: K08, K11, K19, K14, K12 rulings; numbering | decided | K02, K11, K12, K14, K19, G3b |
| 0038 | K14 presence findings (dead-peer detection) | ruled in 0037 | K14, G3b |
| 0039 | K12 Action runtime findings (renumbered by K12) | ruled in 0037 | K11, K12 |
| 0040 | K21 bridge support findings | ruled in 0045 | K21, V02 |
| 0041 | K15 telemetry findings | ruled in 0045 | K15 |
| 0042 | K16 public functions findings | ruled in 0045 | K16, V01 |
| 0043 | K18 ingress findings | ruled in 0045 (IB4 amended) | K17, K18, V01 |
| 0044 | K17 service principals findings | ruled in 0045 | K17, V02 |
| 0045 | Batch-C review rulings; numbering; assembly package proposal | decided | K15-K18, K21, V01, V02 |
| 0046 | K10 replica delivery findings | ruled in 0048 | K10, K22 |
| 0047 | K20 sandbox findings (Dynamic Workers as L09 candidate) | ruled in 0048; L09 pull-forward for Gabriel | K20, L09 |
| 0048 | K10 and K20 rulings; one commit channel; K22 assembly added | decided | K05, K08, K10, K20, K22, V01 |
| 0049 | Batch-C re-review: K15, K16, K18, K21; test-harness backends; flake protocol | decided | K22, V01 |
| 0050 | Final rulings K10, K15, K16, K20; IB6 sandbox workspace; staging DuckDB defect for Gabriel | decided (§2a Gabriel) | K10, K20, K22, L09 |
| 0051–0052 | K20 landed and K10 combined integration merge | decided | K10, K20, K22 |
| 0053 | Required automatic fleet scaling and assembled capacity curves | required; proof open | X01, V03, V04, staging candidate |
| 0054 | Sharded placement and bounded reconnect drain | design decided; assembled proof open | X01, K22 |
| 0055 | External uptime, tracing and operational proof plan | plan accepted; no live activation or uptime claim | K15, R01, V02, V05, M05 |
| 0056 | Bounded ordered outlet and acknowledged recovery | design decided; final proof open | X01, K22 |
| 0057 | Local errors.spin guard failure | affected local proof blocked; private-platform proof required | K22, V01 |
| 0058–0059 | Semantic recovery metadata and oversized identity | design authorized; lifecycle/fit proof remains | K22 |
| 0060–0063 | Evidence boundaries, empty scope bootstrap, telemetry domains and verified resume checker | decided; original failures/gates retained | K22, V01, V03, V04 |
| 0064–0065 | Generation-bound initial completion and finite admission | decided; no false progress/quiescence | K22, X01 |
| 0066 | Credential-free source-image CI and Vault-only publication | initial archives/publication verified; final K22 build/deploy open | K20, K22 |
| 0067–0068 | Intermediate initial-page fences and Action reply skew | decided; final source/rollout proof open | K22 |
| 0069 | Tenant-instance owner authority and terminal drain | bounded prototype permitted; lifecycle UNMET | K22 |
| 0070 | Recovery-only exact-subset processing facts | follow-up permitted; final independent/deployed proof open | K22 |
| 0071 | One refresh after proven zero invocation | bounded prototype permitted; ambiguous replay prohibited | K22 |
| 0072 | Invitation foreign-receipt reference quirk | preserve observed behavior; separate hardening deferred | K22, later hardening |
| 0073 | Explicit admission for ordered holder rebuild | permitted under0061/0069; no client retry credit or lifetime approval | K22 |
| 0074 | Sequenced owner barrier, control-only challenge and exact pre-application support | bounded implementation permitted; native/SDK/deploy proof open | K22 |
| 0075 | Same-queue owner control and bounded reply credit | permitted; preserves recovery fence and original memory/capacity gates | K22, V03, V04 |
| 0076 | Terminal drain for a durably bound lost admission reply | narrow interface permitted; unbound uncertainty and owner-lifecycle gates remain | K22 |
| 0077 | Observable builder-only startup diagnostic | artifact preparation permitted; cause UNKNOWN, live release pending concrete review | K22, lead |
| 0078 | Versioned exact-echo owner-intent callbacks | leaf validation permitted; all durable/lifecycle/activation gates remain | K22 |

Nothing from `cloudflare/integration` reaches Gonvex `staging` or the app repository before cutover
(`AGENT_BRIEF.md`). G1 cannot pass until A01 lands.
