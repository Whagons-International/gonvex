# Presence v2: in-memory, transition-only presence

Status: implemented on branch `perf/presence` (runtime, protocol, `@gonvex/client`,
`@gonvex/react`). Whagons app changes are on `whagons5-client` branch
`perf/presence-v2`. This document is the wire contract: a client (or load
simulator) can implement presence v2 from this page alone.

## 1. Why

Under the Whagons load profile, 89% of all Reducer calls were presence traffic.
Every connected user sent, every 30 seconds:

| call | what it did on the server |
|---|---|
| `presence.beat` (application Reducer) | V8 invoke; 2 `ctx.db` host calls (SELECT the Member's `userPresence` row, UPDATE `lastSeenAt`/`state`, or INSERT); the tenant's single SQLite writer; `synchronous=FULL` commit; change feed; the changed row encoded and sent to every subscriber of `presence.list`, i.e. every connected Member of the tenant |
| `control.support.heartbeat` (runtime Control Reducer) | Control Plane writer: idempotency row + `gonvex_support_sessions` upsert + commit; then the caller's Control queries refresh and **every connection of the project** re-runs its live Control queries (`notify_control_changed`) |
| `error.heartbeat` (SDK error reporter, not a Reducer) | Control Plane upsert of the error session + `notify_control_changed` |

plus `presence.transition` (idle/active, mean gap 101 s) with the same cost as a
beat. The presence fan-out alone is Σ(tenant size)² row deliveries per 30 s.

The UI does not need any of this. It needs one bit per Member (online or not)
and whether they are idle, and it needs it only when it changes.

## 2. What the Whagons UI shows (and keeps showing)

Surveyed in `whagons5-client` (`src/hooks/usePresence.ts` and its callers):

* An availability dot per user: header avatar/status menu
  (`useHeaderNotifications`), `UserProfilePopover`, mention chips, and the
  assignee/user picker in the task dialog (`useBasicTabTemplateOptions`).
* The dot is `getEffectiveAvailability(manualStatus, presence)`:
  manual `offline` wins → no presence → `offline`; presence `idle` →
  `away` (manual `busy` still wins); presence `active` → manual `busy`/`away`
  or `available`.
* The signed-in user's own dot uses the locally reported state
  (`OwnPresenceContext`), not the server row.
* `lastSeenAt` is never displayed. Nothing shows "last seen N minutes ago",
  who is viewing a task, or support-agent availability from `userPresence`.
  (Whiteboard, workbench and break-room presence are separate tables and
  Reducers and are out of scope.)
* Presence visibility is tenant-wide (`userPresenceVisibility` is `public`
  within the tenant database).
* The support dashboard (`TechSupport.tsx`, Control Plane
  `control.support.listSessions`) shows a "Last seen" column per support
  session.

Presence v2 preserves all of this: `usePresence()` still returns
`Map<memberId, {state, lastSeenAt, expiresAt}>` and the availability rules are
unchanged. Two behaviors become *more* correct (intended behavior that the old
path did not deliver):

* A tab that crashes or loses network without `pagehide` used to stay online
  forever (the client-side lease used `expiresAt`, a column the Replica
  Collection never delivered). Now it goes offline 60–80 s after the socket
  dies (liveness) plus the 10 s grace.
* Multiple tabs: the old single row was last-writer-wins (a hidden tab could
  mark an active user idle; one tab's `gone` deleted the row while another tab
  was open). Now the Member's state aggregates all their connections.

## 3. Protocol

All frames are JSON text frames on the existing Gonvex WebSocket
(`/ws`). Field names are exact; unknown fields must be ignored.

### 3.1 Capability negotiation

Server → client, first frame on every socket (unchanged frame, new field):

```json
{"type":"session.ready","capabilities":{"protocolVersion":2,"controlWatermark":1,"presence":1}}
```

`capabilities.presence === 1` means the runtime runs presence v2
(`GONVEX_PRESENCE_V2` on). Absent means: keep the legacy Reducer path.

Client → server, in `auth` (unchanged frame, new capability):

```json
{"type":"auth","id":"a1","token":"…","project":"whagons-5","tenant":"t_123",
 "capabilities":{"replicaNoOldValues":1,"presence":1}}
```

`capabilities.presence: 1` tells the server that this connection's liveness may
be checked with WebSocket Pings (section 3.4) and that the client does not send
periodic presence traffic. The capability is per `auth`; a re-auth without it
turns Ping liveness off.

A client may only use `presence.*` frames when the server advertised the
capability. Doing so on a runtime without presence v2 yields
`{"type":"presence.error",…}` for subscribe and is ignored for set.

### 3.2 Client → server

**`presence.set`** — this connection's own state. Fire-and-forget (no reply).

```json
{"type":"presence.set","state":"active"}
{"type":"presence.set","state":"idle"}
{"type":"presence.set","state":"offline"}
```

* Send on transitions only, and once after **every** `auth.result` (the
  server forgets a connection's state on re-auth, see 3.5). Never periodically.
* `offline` removes this connection at once, without grace (use it on
  `pagehide`). `gone` is accepted as an alias of `offline`. Any other value is
  ignored.
* Only an authenticated tenant **Member** session counts. Sent on a socket
  without one (anonymous/public, control-only, service principal, before
  `auth.result`), it is silently ignored.

**`presence.subscribe`** — receive the tenant's feed.

```json
{"type":"presence.subscribe","id":"presence-1"}
```

The server answers with `presence.snapshot` carrying the same `id`, then sends
`presence.patch` frames. Must be re-sent after every `auth.result`. Without a
tenant Member session the answer is
`{"type":"presence.error","id":"presence-1","error":"presence requires an authenticated tenant member session"}`.

**`presence.unsubscribe`** — stop the feed.

```json
{"type":"presence.unsubscribe"}
```

### 3.3 Server → client

**`presence.snapshot`** — every *online* Member of the tenant. Replaces the
client's whole map.

```json
{"type":"presence.snapshot","id":"presence-1","serverTime":1730650000123,
 "members":[{"memberId":"m_1","state":"active","since":1730649990000},
            {"memberId":"m_2","state":"idle","since":1730649000000}]}
```

**`presence.patch`** — Member transitions since the previous frame, coalesced.

```json
{"type":"presence.patch","serverTime":1730650001000,
 "changes":[{"memberId":"m_2","state":"active","since":1730650001000},
            {"memberId":"m_7","state":"offline","since":1730650001000}]}
```

* `state` is `active`, `idle` or `offline`. `offline` means remove the Member.
* `since` is the server time (Unix ms) at which the Member entered `state`
  (the publication that carried it). It is the value the app exposes as
  `lastSeenAt`.
* A patch never repeats a state the scope had already published. Within one
  publication a Member appears at most once (last state wins); a flap that
  returns to the published state inside one coalescing window is not sent
  at all. (A connection that fell behind and gets several publications merged
  into one patch may see a Member's current state again; apply it as is.)
* The snapshot reflects state published up to the subscribe; transitions not
  yet published arrive in the next patch. Patches are applied in order.
* A connection that falls more than 32 publications behind gets a fresh
  `presence.snapshot` (same subscribe `id`) instead of the missed patches.
* Timing (3.4): leading edge, then at most one patch per 250 ms window.

**`presence.error`**

```json
{"type":"presence.error","id":"presence-1","error":"presence v2 is disabled"}
```

### 3.4 Timing and liveness

| parameter | default | env |
|---|---|---|
| coalescing window | 250 ms | `GONVEX_PRESENCE_TICK_MS` |
| leading edge | on | `GONVEX_PRESENCE_LEADING_EDGE` (off = old fixed tick) |
| offline grace after the last connection closes | 10 s | `GONVEX_PRESENCE_OFFLINE_GRACE_MS` (0 allowed) |
| server Ping interval (connections that advertised `presence:1`) | 20 s | `GONVEX_PRESENCE_PING_SECONDS` |
| liveness timeout (no inbound frame) | 60 s, never < 2 × Ping | `GONVEX_PRESENCE_LIVENESS_SECONDS` |
| max pending publications per connection before resync | 32 | — |

**Publication timing (leading edge + trailing window).** Publication is per
`(project, tenant)` scope, and every subscribed connection of the scope gets
every publication, so the rule holds per connection too:

* **Leading edge.** If the scope published nothing in the last 250 ms, a
  change is published at once: the server publishes it as soon as it
  processed the `presence.set` (or the close / liveness failure), and the
  subscriber's socket sends it immediately. In a `replica.batch` connection
  the patch is an item of a batch flushed at once, unless that connection's
  open batch already holds row changes (then it rides in that batch, which
  leaves within the 50 ms batch tick).
* **Trailing window.** Changes arriving within 250 ms of the last publication
  are coalesced (latest state per Member wins; flaps back to the published
  state vanish) and published together when the window ends. That publication
  opens the next window. A scope therefore publishes at most one patch per
  250 ms, and each connection receives at most one presence emission per
  250 ms.
* **Offline after grace** is published when the grace ends (or at the end of
  the window then open), not on a polling tick.
* Expected delivery latency: ~0 ms + socket write for a change in a quiet
  scope, at most 250 ms + socket write for a change inside a busy window.
  Measured (conn_idle driver, section 9): p95 214–258 ms, p99 238–274 ms
  from 500 to 1,950 users in one tenant.
* There is no timer per connection. One process-wide scheduler sleeps until
  the earliest due scope (window end or grace end) and does nothing while no
  change is pending.

* **Liveness is the WebSocket.** A connection is present from its first
  `presence.set` until (a) it sends `presence.set offline`, (b) the socket
  closes for any reason, (c) its session changes (re-auth, revocation,
  membership change), or (d) it fails liveness.
* **Liveness check:** every 20 s the server sends a WebSocket Ping (opcode
  0x9, empty payload) to connections whose `auth` advertised `presence:1`.
  *Any* inbound frame — Pong, text, binary — counts. If nothing arrived for
  60 s at a Ping tick, the server sends Close `4000 "presence liveness
  timeout"` and drops the connection. Detection therefore takes 60–80 s.
  Browsers and Node `ws` answer Pings automatically; a simulator only has to
  keep reading the socket. There is no application-level ping frame.
* **Aggregation per Member** within one `(project, tenant)`: `active` if any of
  their present connections reported `active`; `idle` if all report `idle`;
  `offline` when none is present. Multiple tabs/devices are just multiple
  connections.
* **Grace:** when the last connection of a Member goes away by (b), (c) or (d),
  the Member stays published for 10 s. A reconnect (new socket, `auth`,
  `presence.set`) inside the grace window produces no patch at all. After the
  grace the Member is published `offline` at the next tick, so offline lands
  10–10.25 s after the close. Explicit `offline` (a) skips the grace.
* **Latency:** see the publication timing above: immediately in a quiet scope,
  at most 250 ms (the window) plus the socket write otherwise.

### 3.5 Sessions, reconnects and tenant switches

* Re-auth on the same socket (token refresh, tenant switch) drops the
  connection's state and subscription server-side (with grace). The SDK sends
  `presence.set <current>` and `presence.subscribe` right after `auth.result`,
  so a same-tenant refresh shows no transition.
* A new socket is a new connection: same rule.
* A tenant switch moves the connection: it leaves the old tenant (offline after
  grace if it was the last one) and joins the new one.

### 3.6 Audience

* Scope is `(project, tenant)`; the Member id is the tenant Member id
  (`ctx.member.id`), the same id the app's `userPresence.userId` held.
* A patch goes to every connection of the same `(project, tenant)` that is
  subscribed, including the Member's own connections. Nothing crosses tenants
  or projects (separate in-memory scopes; covered by tests).
* Anonymous/public sessions and service principals have no presence and cannot
  subscribe. Support/developer impersonation sessions count as the
  impersonated Member, as they did with the old Reducer.
* Workspace- or team-scoped presence is not implemented: Whagons presence is
  tenant-wide today.

### 3.7 Support and error sessions (`control.support.heartbeat`, `error.*`)

The `gonvex_support_sessions` row stays (the support dashboard and support
commands need it); only its refresh changes. With presence v2 on:

* `control.support.heartbeat` (unchanged frame, Control scope): the first call
  per connection — and any call with a changed tenant, account, `release` or
  `environment` — writes as before. Repeats are answered from memory: same
  `reducer.result` (`{"sessionId":"support_<connectionId>"}`) followed by the
  usual `control.watermark`, but no Control Plane write, no idempotency row, no
  Control query refresh and no project-wide notification.
* `error.register` / `error.heartbeat` (SDK error reporter): the same rule; a
  repeat is acknowledged with `error.ack` without a write.
* `last_seen_at` is no longer refreshed per heartbeat. While the connection is
  open, `listSessions`/`getSession` report it as seen *now*, and
  `pruneSessions` never deletes it. The stored value is written:
  * when the connection closes (its final value, batched every 5 s);
  * every `GONVEX_SUPPORT_LAST_SEEN_REFRESH_SECONDS` (default 300) for all
    open sessions, in one batched transaction (≤ 500 sessions per statement),
    with no idempotency rows, no Control change notification and no query
    refresh. The first refresh runs at a random offset inside the interval so
    it does not coincide with other periodic work;
  * on a clean stop (SIGTERM or Ctrl-C), as `now` for every open session plus
    any closed session not yet flushed, in one batched transaction before the
    pools close (bounded to 3 s).
* Clients on a presence v2 runtime should register the support session once
  per connection (on connect/reconnect) instead of every 30 s, and the SDK
  error reporter stops its 30 s `error.heartbeat`.

### 3.8 Compatibility with old clients (Reducer presence)

Old clients do not send `capabilities.presence` and keep calling
`presence.beat`, `presence.transition` and `control.support.heartbeat` every
30 s while subscribed to `presence.list` (the `userPresence` table).

| server setting | effect on an old client |
|---|---|
| `GONVEX_PRESENCE_LEGACY_BEAT_PATHS=presence.beat` | Beats are answered in Rust: `reducer.result` with `result` = server time (ms) and `committedRevision` = the last replica watermark already sent to that connection (so the client retires its optimistic local write); then the usual `replica.watermark`. No V8, no write, no fan-out. The caller is marked present (`active` if it was not yet present; an existing state is kept). Only applies once the connection has received a replica watermark; before that the beat runs normally. |
| `GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS=presence.transition` | Transitions still run normally (so the table stays correct for other old clients) and, from a connection without `presence:1`, also update the in-memory feed (`args.state`: `active`/`idle`/`gone`). |
| (always) | Support/error heartbeat repeats are answered from memory (3.7). |

What each side sees during the window:

* **Old viewer:** renders `userPresence` rows exactly as before. v2 clients keep
  writing their *transitions* (never keepalives) to the table while the app's
  `MIRROR_LEGACY_PRESENCE_TABLE` is on, so old viewers still see v2 users.
  With the beat short-circuit on, `lastSeenAt` in the table is refreshed only
  by transitions; nothing displays it.
* **v2 viewer:** sees old clients through the legacy path settings above.
  Without them, old clients are invisible to v2 viewers.
* An old client that crashed stays in the old table (as before); a v2 viewer
  sees it offline once its socket is gone.

Default of `GONVEX_PRESENCE_V2`: **on**. It is additive for old clients: the
capability is only used by clients that advertise it, and the heartbeat
de-duplication keeps every observable result (`sessionId`, the dashboard's
last-seen, prune safety). The legacy path lists default to **empty** because
they name application Reducers; the runtime must not assume an application's
function names. For Whagons set both (see 5).

## 4. Durability

Presence is not durable. After a runtime restart every client reconnects and
re-announces within its reconnect delay; until then Members appear offline.
Whagons displays no last-seen time, so nothing needs to survive a restart.
Support sessions keep their durable row (written on first heartbeat). Their
`last_seen_at` is written when the connection closes, refreshed every 5
minutes while open, and written for every open session on a clean stop. A
crash (SIGKILL, OOM, power loss) therefore loses at most one refresh interval.

Clean stop and deploys. The runtime handles SIGTERM as well as SIGINT. It runs
as PID 1 in its container with no init, and the kernel does not apply a
default action to signals sent to PID 1, so before this change SIGTERM was
ignored: every stop-first redeploy waited out the stop grace and ended in
SIGKILL without closing pools. Now, after the signal, the listener stops
accepting, in-flight HTTP requests get at most 4 s (open WebSockets are not
waited for), support-session end times are written (at most 3 s), then the
module host stops and the pools close. Measured with an open WebSocket and an
empty data directory: exit 0 in 0.03 s after SIGTERM. The worst case (about
7 s plus pool close) fits the Docker default stop timeout of 10 s that a
Coolify Dockerfile Application gets unless configured otherwise, and the 30 s
`stop_grace_period` in `docker-compose.production.yml`.

## 5. Migration (Whagons)

1. Deploy a runtime with this change (`GONVEX_PRESENCE_V2` defaults on) and set
   `GONVEX_PRESENCE_LEGACY_BEAT_PATHS=presence.beat`,
   `GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS=presence.transition`.
   Effect immediately, before any client release: beats and repeated
   support/error heartbeats cost no V8, no write and no fan-out.
2. Publish the Gonvex SDK release with presence v2 (`@gonvex/client`
   `presenceSupported/setPresence/subscribePresence/presenceSnapshot`,
   `@gonvex/react` `usePresence`) and bump Whagons to it.
3. Ship Whagons branch `perf/presence-v2`: it detects the SDK API and the
   server capability at runtime (`src/lib/gonvex/usePresenceFeed.ts`). With
   both, `PresenceProvider` reports transitions with `setPresence`, sends no
   keepalive and does not subscribe to `presence.list`; `usePresence` reads the
   feed; `TechSupportSessionProvider` registers once per connection. With
   either missing it runs exactly the old code path.
4. After old releases age out of the support window: set
   `MIRROR_LEGACY_PRESENCE_TABLE = false`, then remove `presence.beat`,
   `presence.transition`, `presence.list` and the `userPresence` table, and
   clear the legacy path settings.

## 6. Flags

| env | default | meaning |
|---|---|---|
| `GONVEX_PRESENCE_V2` | `on` | capability, `presence.*` frames, Ping liveness, heartbeat de-duplication |
| `GONVEX_PRESENCE_TICK_MS` | `250` | coalescing window (at most one patch per scope per window) |
| `GONVEX_PRESENCE_LEADING_EDGE` | `on` | publish the first change after a quiet window at once; `off` restores the fixed process-wide tick (rollback; use with `TICK_MS=1000` for the old behavior) |
| `GONVEX_PRESENCE_OFFLINE_GRACE_MS` | `10000` | grace after the last connection closes |
| `GONVEX_PRESENCE_PING_SECONDS` | `20` | Ping interval |
| `GONVEX_PRESENCE_LIVENESS_SECONDS` | `60` | liveness timeout (≥ 2 × Ping) |
| `GONVEX_PRESENCE_LEGACY_BEAT_PATHS` | empty | comma list of keepalive Reducers answered in Rust |
| `GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS` | empty | comma list of `{state}` transition Reducers mirrored into the feed |
| `GONVEX_SUPPORT_LAST_SEEN_REFRESH_SECONDS` | `300` | batched refresh of open support/error sessions' stored `last_seen_at` |

## 7. Simulator checklist (one v2 user)

The load generator for user-count step-ups is the thin protocol client
`bench/whagons/thin-client.mjs`, which sends frames byte-identical to the SDK
and implements presence v2 (`presenceSupported`, `setPresence`,
`subscribePresence`, `presenceSnapshot`; counters in
`ThinClient.stats.presence`). The heartbeat timings of the old SDK-based
generator (`bench/whagons/README.md`: presence and support heartbeats every
30 s, transitions every 101 s on average) describe the **legacy** path only and
are superseded by the thin client: against a presence v2 runtime a simulated
user sends no `presence.beat`, no periodic `control.support.heartbeat` and no
`error.heartbeat`; it sends `presence.set` on its idle/active transitions
(the same 101 s mean model) and registers its support session once per
connection.


1. Open `/ws`, read `session.ready`, check `capabilities.presence === 1`.
2. Send `auth` with `capabilities.presence: 1`; wait for `auth.result`.
3. Send `{"type":"presence.set","state":"active"}` and
   `{"type":"presence.subscribe","id":"<any>"}`; read `presence.snapshot`.
4. On each simulated idle/active transition send `presence.set` (nothing
   else; no beats). On page hide send `presence.set offline`.
5. Keep reading the socket (Pongs are automatic in `ws`/browsers).
6. Register the support session once per connection:
   `reducer.call` `control.support.heartbeat` with `scope:"control"`, an
   `idempotencyKey`, `args:{release,environment}`; complete at
   `control.watermark`.
7. After a reconnect or re-auth, repeat 3 and 6.
8. Delivery latency of a transition = receipt time of the `presence.patch`
   containing the Member minus the sender's `presence.set` time; expect
   ≈ network alone when the tenant's presence was quiet for 250 ms, and
   ≤ 250 ms + network otherwise (section 3.4). A simulator that models the
   server should publish per tenant: immediately if the tenant published
   nothing in the last 250 ms, else at last publication + 250 ms with the
   latest state per Member.

## 8. Implementation map

* Runtime: `rust/crates/runtime/src/presence.rs` (hub, aggregation, tick,
  per-connection slot, support sessions), connection loop and Reducer
  short-circuits in `rust/crates/runtime/src/lib.rs`
  (`call_reducer_with_presence`, `presence_member_scope`), error heartbeat in
  `telemetry.rs`, support session list/get/prune in `control.rs`, config in
  `config.rs`. Frames in `rust/crates/protocol/src/lib.rs`.
* Per-connection output is a `PresenceSlot` filled by a publication (one
  encoded frame per scope per publication, shared by all subscribers) and
  drained by the socket task on the slot's notify into its `replica.batch`.
  Scheduling: `Scope::schedule`/`reschedule` keep each scope's due time;
  `PresenceHub::run_due` publishes due scopes and returns the next due time
  for the `presence_scheduler` task in `lib.rs`; `PresenceHub::tick`
  force-publishes everything (tests, benchmarks, and the
  `GONVEX_PRESENCE_LEADING_EDGE=0` fixed tick).
* SDK: `packages/client/src/presence.ts` (`PresenceFeed`), wiring in
  `packages/client/src/index.ts`, `usePresence` in `packages/react/src/index.tsx`,
  types in `packages/protocol/src/index.ts`.
* Tests: `presence.rs` unit tests, `rust/crates/runtime/tests/presence_v2.rs`
  (real sockets), `packages/client/src/presence.test.ts`,
  `packages/react/src/presence.test.tsx`.
* Benchmark: `rust/crates/runtime/examples/presence_bench.rs` (section 9).

## 9. Measurements

`cargo build --release -p gonvex-runtime --example presence_bench && target/release/examples/presence_bench`
(arch3 workstation, 2026-11-03, while ~12 other agent builds were running, so
absolute times are noisy; base `claude/sqlite-storage-lab` 72e6d30 merged).
1,950 users in one tenant. The old path excludes V8: the Reducer's invoke and
its two `ctx.db` host calls over the module-host socket are not in these
numbers (measured separately at ~1.5 ms per Reducer on the Mac lab).

Per beat / per transition:

| path | host calls | tenant writer hold | latency (mean / p95 / p99) | allocations |
|---|---|---|---|---|
| old `presence.beat`: admission + SELECT + UPDATE + capture + `synchronous=FULL` commit | 2 (+1 invoke) | 41.3 ms mean, p95 76 ms, p99 117 ms | 41.6 / 76.5 / 117 ms | 227 |
| old fan-out: encode the replica frame for 1,950 subscribers (lower bound: no visibility, no socket) | — | — | 5.8 / 7.1 / 8.3 ms | 101,400 |
| new: v2 client beat | none sent | 0 | 0 | 0 |
| new: old client's beat short-circuited in Rust (hub lookup) | 0 | 0 | 138 ns / 170 ns / 291 ns | 1 |
| new: `presence.set` transition (hub update) | 0 | 0 | 482 ns / 621 ns / 922 ns | 2 |
| new: one tick publishing 1 / 5 / 20 transitions to 1,950 subscribers (encode once + 1,950 slot pushes) | 0 | 0 | 95 / 88 / 95 µs | ~1,960–2,040 |
| new: draining all 1,950 slots (1 / 5 / 20 transitions per frame) | 0 | 0 | 145 / 131 / 261 µs | 1,950 |

A first run (same build) measured the old writer hold at 30.9 ms mean
(p99 76 ms). On this disk, under this load, one tenant writer completes ~25–30
beat commits per second; the 1,950-user tenant needs 84/s of presence commits
alone.

Resident hub state: 1.67 MB for 1,950 online Members, each with one
subscribed connection (858 B per user).

1,950-user tenant, 30 s beats, transitions with 101 s mean gaps, 30 simulated
minutes:

| | old | new, tick 1 s (default) | new, tick 250 ms |
|---|---|---|---|
| durable commits/s for presence + support | 149.3 (65 beats + 65 support + 19.3 transitions) | 0 | 0 |
| presence deliveries/s | 164,387 row deliveries (126,750 from beats + 37,637 from transitions) | 1,950 frames carrying 37,282 Member changes | 7,734 frames carrying 37,539 Member changes |
| presence bytes/s on the wire (before compression) | not measured (~250 B per row frame → ~41 MB/s estimated) | 2.1 MB/s | 2.5 MB/s |
| hub CPU per wall second (set + tick + drain) | — | 0.35 ms | 0.90 ms |

Not measured: V8 time, socket writes and compression, visibility evaluation
on the old fan-out, the Control Plane side of the old support heartbeat
(`notify_control_changed` re-runs every project connection's live Control
queries), and end-to-end latency under the full Whagons load.

### 9.1 Leading edge + 250 ms window (perf/presence-edge, 2026-11-04)

Driver: `benches/conn_idle.rs` (real WebSockets; runtime pinned to one CPU
with `taskset -c 13`, production config defaults; clients in a child process
on the other CPUs). Each socket authenticates with presence v2 and
`replicaBatchFrames` (`CONN_IDLE_BATCH_FRAMES=1`, as the SDK and thin client
do), subscribes, and toggles `presence.set` with 101 s mean gaps (production
rate). Latency is measured per viewer: receipt time of the patch carrying the
Member's state minus the time the Member's socket sent that `presence.set` (one
client process, one clock), over every subscriber of the tenant, in a 60 s
window after a 10 s warm-up. Before = the same binary with
`GONVEX_PRESENCE_LEADING_EDGE=0 GONVEX_PRESENCE_TICK_MS=1000` (the old fixed
1 s tick, presence items waiting for the batch tick); A/B runs interleaved. arch3
was shared with other agents' builds; CPU numbers move ±15% between runs (the
1k pair was repeated: second run in parentheses).

| sockets (tenants) | mode | latency p50 / p95 / p99 / max (ms) | presence frames/s (all sockets) | server CPU µs/s per socket | server cores |
|---|---|---|---|---|---|
| 500 (production skew) | 1 s tick | 541 / 1,003 / 1,045 / 1,055 | 372 | 21.0 | 0.011 |
| 500 (production skew) | leading edge | **3.8 / 214 / 238 / 254** | 712 | 29.5 | 0.015 |
| 1,000 (production skew) | 1 s tick | 572 / 1,023 / 1,049 / 1,067 (539 / 1,011 / 1,052) | 886 (880) | 20.5 (17.8) | 0.021 (0.018) |
| 1,000 (production skew) | leading edge | **68 / 236 / 252 / 261** (78 / 235 / 251) | 2,439 (2,457) | 37.4 (32.8) | 0.037 (0.033) |
| 1,950 in one tenant | 1 s tick | 552 / 1,027 / 1,072 / 1,113 | 1,950 | 20.9 | 0.041 |
| 1,950 in one tenant | leading edge | **150 / 258 / 274 / 298** | 7,767 | 50.8 | 0.099 |

Every run: all sockets connected, 0 auth errors, the same transition rate
(4.9 / 9.9 / 19.7 per second) and the same number of measured deliveries.

* p95 is now bounded by the window (250 ms) instead of the 1 s tick; p50 is
  near zero where tenants are quiet (500 users) and rises toward half a window
  as a tenant's transition rate approaches one per window (1,950 users:
  ~20 transitions/s, so almost every change lands inside a window).
* Cost: more frames, since a busy tenant now publishes up to 4 times per
  second instead of once. At 1k that is +0.012–0.016 cores; for the 1,950-user
  tenant +0.06 cores (7,767 vs 1,950 frames/s). The remaining cost per frame is
  mostly the socket write (`sendto`, see conn-idle.md). The window
  (`GONVEX_PRESENCE_TICK_MS`) trades this directly: 500 ms halves the frames of
  a busy tenant and gives p95 ≈ 500 ms.
* Not measured here: compression cost per frame separately (the driver runs
  the runtime's default compression settings as configured), the full
  Whagons mix (replica traffic in the same batches would let some presence
  items ride along instead of costing their own frame), and network RTT.
