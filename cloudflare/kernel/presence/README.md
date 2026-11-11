# Presence v2 (K14)

In-memory, transition-only tenant presence on the gateway placement (0016 G0-1): Member transitions,
`presence.subscribe` / `presence.snapshot` / `presence.patch`, the in-memory answers for legacy keepalive
Reducers and repeated support heartbeats, and liveness without server pings (0016 G0-6). The reference is
`rust/crates/runtime/src/presence.rs` and the presence parts of the socket loop in `lib.rs` (3712-3772,
4807-4995) at `0bf3ad13`; the wire contract is `docs/performance/presence-v2.md` and `CONTRACT.md` E.e.

```text
client ──presence.*──▶ gateway (SocketPresence, per socket)  ──dispatch / presenceLeave──▶ tenant object (TenantPresence → PresenceHub)
client ◀─snapshot/patch── gateway.presence(delivery)          ◀──one ordered channel per gateway──┘
```

| File | What |
| --- | --- |
| `hub.ts` | `PresenceHub`: the port of Rust's hub for the scopes of one tenant object. Aggregation, grace, leading edge, 250 ms window, `publish_scope`, snapshot and patch encoding in serde order, `parse_state`, config from Rust's environment names. Pure: no I/O, no timers. |
| `tenant.ts` | `TenantPresence`, embedded by the tenant object: scheduling (`setTimeout` while awake, grace ends also on the alarm), the per-gateway delivery channel, rebuild after a memory loss, the sweep of restarted gateways. |
| `socket.ts` | `SocketPresence`, owned by each K05 `Session`: per-socket state in the attachment, `presence.*` frames, leave on auth / discard / close, the short-circuits of `call_reducer_with_presence`, deliveries to the socket. |
| `bench/dead-peer.mjs`, `bench/run-dead-peer.sh` | Dead-peer detection for an idle socket (G0-6): a victim whose packets vanish (no FIN, no RST), timed to the watcher's offline. Runs unprivileged in a network namespace. |
| `test/` | Hub (Rust's unit tests), tenant side (channel, merge, resync, rebuild, sweep, alarm), socket side (through a real `Session`), end to end in workerd (evicted tenant and gateway objects). |

## Behaviour

- **State.** The tenant object owns the hub for its scope. Each socket's own state (`active`/`idle`/none),
  subscription id, whether its snapshot arrived, and the time its state last changed live in the socket's
  `SessionRecord` (`presence`), written only when they change. Nothing is written to SQLite.
- **Publication.** As Rust: a change in a scope quiet for 250 ms is published at once; later changes in the
  window coalesce into one trailing patch; offline lands when the 10 s grace ends. Each publication is
  encoded once by the tenant and sent **once per gateway** that holds a subscriber; the gateway sends the same
  string to each subscribed socket (inside `replica.batch` for batch-frame sockets, as Rust drains presence
  into the batch). Subscribe snapshots are sent bare, as in Rust.
- **Ordering.** Each gateway has one ordered channel with at most one RPC in flight. A subscribe's snapshot
  travels on it, so no patch overtakes it; the gateway drops patches for a subscription whose snapshot has not
  arrived. Publications that queue behind an in-flight delivery are merged (last state per Member wins,
  `drain`); past 32 pending, or after a failed delivery, the gateway's subscribers get a snapshot instead
  (Rust's per-connection slot, moved to the gateway hop). A failed gateway is retried after 1 s, never at once.
  On the socket side, presence RPCs are chained, so a leave sent on re-auth reaches the tenant before the
  state the client re-announces.
- **Leave.** Every `auth`, a discarded tenant scope and the socket's close remove the connection with grace
  (Rust's `reset` and `ConnectionGuard`). An explicit `offline` (or `gone`) removes it without grace.
- **Memory loss.** A tenant object without sockets is evicted when idle (about 20 s in local workerd) and
  restarted on a deploy. The first presence event after a wake rebuilds the hub from the gateways'
  attachments (one RPC per registered gateway). The connection whose event woke the object is restored
  from the state it had before the event, which travels with every presence RPC (`presencePrevious`). An idle
  eviction leaves nothing pending, so the rebuilt state is what subscribers already have and nothing is sent.
  When a grace may have been pending (the alarm fired on a cold object, or the object reports a set alarm via
  `lostPending`), every subscriber gets one snapshot. A restored Member's `since` is the earliest state change
  of the sockets that hold its state (Rust's publication time is gone with the memory).
- **Restarted gateways.** A gateway that restarts (a deploy of an active object) loses its sockets without
  close events. At most once a minute while presence is in use, and on every alarm, the tenant asks the
  gateways holding its connections which still exist; the others leave with grace. No timer runs while
  presence is idle, so idle tenants are not kept awake.
- **Hibernated gateways.** A close that wakes a hibernated gateway is handled from the closing socket's
  attachment: workerd does not list the closing socket in `getWebSockets()` at that wake (measured).
- **Liveness (G0-6).** No SDK change: K05 sends a watermark to a tenant socket after 20 s without outbound
  frames, so the server's TCP stack retransmits to a dead peer and eventually fails the connection, which
  workerd reports as close 1006. See "Dead-peer detection" below.
- **Legacy paths.** `GONVEX_PRESENCE_LEGACY_BEAT_PATHS` (answered on the gateway once the socket has
  received a replica watermark: `reducer.result` with the server time and `committedRevision` = that
  watermark, then the watermark; the caller is registered `active` only if it reported nothing) and
  `GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS` (after a successful call from a socket without `presence:1`, its
  `args.state` is applied to the feed). Both default to empty, as in Rust.
- **Support heartbeats.** A repeated `control.support.heartbeat` with the same attribution (connection,
  project, tenant, account, release, environment) is answered on the gateway from memory:
  `reducer.result {sessionId}` then `control.watermark`, with no control call. The first one and any change go
  to the control object; on success the attribution is kept in the socket's record and reported with K06's
  optional `connectionState` (also on close, for K06's last-seen write).

## Dead-peer detection (G0-6)

`bench/run-dead-peer.sh [tcp_retries2]` runs `wrangler dev` and the driver in an unprivileged user and network
namespace (`unshare -rn`), drops the victim's packets in both directions with nft once it has reported
`active`, and prints when the watcher sees it offline. workerd serves the port directly (no Node proxy), so
the server-side TCP stack is workerd's host kernel.

Measured on arch3 (local workerd 1.20261023.1, loopback, 2026-11-11), victim idle after `presence.set active`:

| `tcp_retries2` | Drop → offline at the watcher | Breakdown |
| --- | --- | --- |
| 3 | 33.1 s, 33.1 s | 20 s until the liveness watermark, ~3 s of retransmissions, close 1006, 10 s grace |
| 8 | see `results.md` | |
| 15 (Linux default) | see `results.md` | |

Without the liveness watermark nothing is ever sent to an idle socket and the peer is never detected. On the
platform the client's TCP connection ends at Cloudflare's edge, whose retransmission policy is not ours to set
and was not measured (the vault was down): that run is a follow-up, with the driver pointed at a
`gonvex-cf-dev-*` deploy and the victim's port dropped by the client host's firewall.

## Owned elsewhere

| Item | Package |
| --- | --- |
| The real tenant object (embeds `TenantPresence`, routes `presence.*` from `dispatch`, `presenceLeave`, its alarm) | K07/K10 tenant object; the K05 stub does it today |
| Presence items riding in an open `replica.batch` with row changes | K10 batcher (presence sends its own envelope today) |
| `error.register` / `error.heartbeat` de-duplication | K15 |
| K06 wiring of the gateway (`connectionState` on auth, live support ids) | K05/K06 integration |
| An SDK ping (`capabilities.ping`) for detection that does not depend on TCP | stage 3 (0016 G0-6) |
