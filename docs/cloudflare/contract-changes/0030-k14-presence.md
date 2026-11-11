# 0030: K14 presence: placement, memory loss and the measured dead-peer gap

Date: 2026-11-11. Status: **proposed** (K14). Affects: K05 (session files edited), K07/K10 (the tenant object
embeds presence), K06 (support sessions), V01. Code: `cloudflare/kernel/presence/` (README has the details).

Presence v2 is implemented to `presence.rs` and `presence-v2.md`. The `presence` scenario and the new
`presence-edges` scenario pass Rust against the kernel. Rows 1-5 below are adaptations that need a ruling.
Row 6 is the G0-6 measurement.

## 1. Where presence lives (gateway placement)

The hub for a scope is in the tenant object; each socket's own state (state, subscription id, snapshot
delivered, last change time) is in its `SessionRecord` attachment on the gateway, written only when it changes.
A publication is encoded once and sent once per gateway that holds a subscriber, over one ordered channel per
gateway (one RPC in flight). Rust's per-connection slot (merge pending patches, snapshot past 32) moves to that
channel: the merge and the 32 limit apply per gateway, not per connection, because workerd's `send` has no
back-pressure to queue behind (A.1.8). A subscribe's snapshot is sent on that channel, one tenant-to-gateway hop
after the frame, not inline as in Rust. A patch can never overtake it, but other replies on the same socket
may arrive in a different order than they do on Rust. Proposed: accept. Frame shapes and the timing rules are
unchanged.

## 2. Memory loss of the tenant object

A tenant object holds no sockets, so it is evicted when idle (measured locally: about 20 s) and restarted by a
deploy. Rust loses presence only on a restart, which also drops every socket. Here the sockets survive on the
gateways, so the first presence event after a wake rebuilds the hub from the gateways' attachments (one RPC per
registered gateway). Every presence RPC carries the socket's state from before the event, so the connection
that woke the object is restored first. Without that, an idle user's tab close (or an explicit `offline`)
arriving at an evicted object would never be published (found and fixed in this package, with a test).
Subscribers are resynchronized with a snapshot only when a grace may have been pending at the loss: the alarm
fired on a cold object, or the object reports an armed alarm (`lostPending`). An idle eviction leaves nothing
pending, sends nothing, and keeps the conformance traces equal to Rust's. Deviations:

- A restored Member's `since` is the earliest state change of the sockets holding its state, not the lost
  publication time. Only new subscribers see it. Whagons displays no last-seen time from presence (`presence-v2.md` §2).
- The real tenant object multiplexes its alarm (E.timers). Its `lostPending` must say whether presence's grace
  end was among the pending times. Answering "an alarm is set" is safe but sends a snapshot round on each wake
  of a tenant with crons. **K07/K10/K11: implement `lostPending` precisely when the alarm multiplexer lands.**

## 3. Gateways that restart lose sockets without close events

An active gateway restarted by a deploy loses its sockets without `webSocketClose`. While presence is in use,
at most once a minute and on each alarm, the tenant asks the gateways that hold its connections which still
exist; the others leave with grace. No timer keeps an idle tenant awake. Proposed: accept.

Related K05 fix in this PR: a close that wakes a hibernated gateway was dropped. workerd does not list the closing
socket in `getWebSockets()` at that wake (measured), so the session was never found, and neither presence nor the
gateway's tenant index learned of the close. `SocketHost.closed` now restores the session from the closing
socket's attachment.

## 4. K05 files changed

Presence needs hooks in the session layer, so this PR edits K05's files. The edits are additive, with no
behaviour change outside presence and the close fix in row 3:

- `session.ts`: three optional record fields (`presence`, `support`, `watermark`), `SocketPresence` per session,
  presence frames routed to it, `reducer.call`/`callMany` through presence's in-memory answers, presence leave on
  auth, discard and close, `presenceDelivery`, and `forward` returning the frames it sent.
- `host.ts`, `placement/gateway.ts`: the presence config, the `waitUntil` hook, the gateway RPCs `presence` and
  `presenceConnections`, and the close fix.
- `ports.ts`, `rpc.ts`, `bindings.ts`: the optional `TenantPort.presenceLeave`, `ControlPort.connectionState` and
  `SocketView.presencePrevious`.
- `stub/tenant.ts`: the stub tenant object embeds `TenantPresence`, as the real one must.
- `protocol.ts`: `echoTrace` is exported.

The conformance normalizer gets one rule: presence Members are sorted by their aliased Member id, because each
backend assigns ids at random and the wire order is by id. Each backend's raw order is asserted in
`presence-edges`. Rust against Rust flaked on this before the rule (2 runs in 4).

## 5. Smaller choices

- **Legacy beat condition.** Rust short-circuits once `sent_replica_revision > 0`. The kernel uses the last
  watermark delivered by the tenant since auth (`record.watermark`), not K05's 20 s liveness watermark, which
  Rust does not have.
- **Support heartbeat.** The repeat is answered on the gateway from the socket's record, which holds the same
  key as Rust's `support_is_current`. Registration is reported through K06's `connectionState` when the port has
  it. The main Worker still binds K05's control stub, which answers `control.support.heartbeat` as unknown, so
  the short-circuit is exercised by unit tests until K06 is wired in.
- **Configuration.** Rust's names: `GONVEX_PRESENCE_V2`, `_TICK_MS`, `_LEADING_EDGE`, `_OFFLINE_GRACE_MS`,
  `_LEGACY_BEAT_PATHS`, `_LEGACY_TRANSITION_PATHS`. An invalid value keeps the default instead of failing the
  start (a Worker cannot refuse to start).
- `error.register` / `error.heartbeat` de-duplication stays with K15.

## 6. G0-6: dead-peer detection for an idle socket (measured locally; the platform run is a follow-up)

`kernel/presence/bench/run-dead-peer.sh` runs workerd and the clients in an unprivileged network namespace. Once
the victim has reported `active`, it drops the victim's packets in both directions (no FIN, no RST) and times the
watcher's offline. workerd serves the socket directly, so the server-side TCP stack is the host kernel's.

| Server `tcp_retries2` | Drop → offline | What happens |
| --- | --- | --- |
| 3 | 33.1 s (2 runs) | 20 s until K05's liveness watermark, ~3 s of retransmission, workerd reports close 1006 to the gateway, then the 10 s grace |
| 8 | 136.5 s | 20 s, then about 105 s of exponential retransmission, close 1006, 10 s grace |
| 15 (Linux default) | **not detected within 30 min** | still retransmitting when the run gave up |

- **The 10 s grace after detection holds.** From workerd's close event to the watcher's offline it takes the grace
  plus one window, as in Rust.
- **Detection itself is the gap.** Rust detects a silent peer in 60-80 s (server Pings). The kernel detects it only
  when the server's TCP stack gives up retransmitting K05's 20 s watermark. With Linux defaults that took more than
  30 minutes locally. Without the watermark, an idle dead peer is never detected.
- **The platform number is not measured.** Clients' TCP ends at Cloudflare's edge, whose retransmission policy we
  do not control, and the vault was down (no deploy). Follow-up: run `dead-peer.mjs` against a `gonvex-cf-dev-*`
  deploy, with the victim's port dropped by the client host's firewall.
- **Recommendation.** If the platform number is also minutes, bring the stage-3 SDK ping
  (`capabilities.ping`, 0017) forward. It gives 60-80 s detection with the auto-response, at no wake cost (S3).
