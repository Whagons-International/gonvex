# 0017: Presence liveness needs a client ping frame

Date: 2026-11-11. Status: **decided in 0016 G0-6** (no SDK change in stage 1). Reported by S3 (measured on the platform,
`docs/cloudflare/spikes/S3-sockets.md` §6). Affects: K05, K14, R22 in 0012, `packages/client`, `packages/protocol`.

## Finding

Rust detects a silent presence socket with a server WebSocket Ping every 20 s and a 60 s inbound timeout
(close 4000 `presence liveness timeout`, CONTRACT.md A.1.8). On the platform:

- A Durable Object cannot send protocol Pings.
- The edge does not ping clients either, and it does not close idle sockets. A socket that sent and received
  nothing stayed open for the whole 20-minute test (`results/idle-cf.json`).
- Client protocol Pings are answered by the runtime, but the object never sees them and they leave no
  auto-response timestamp.
- The current SDK sends no periodic frame under presence v2 (`packages/client/src/error-reporter.ts:166-177`).

So with today's SDK a client whose network dies without a FIN or RST stays "present" on an idle tenant until
the edge's TCP stack gives up. That happens only once the object sends it data, and it may never happen. Rust shows
offline 70–90 s after the socket dies.

## What works (measured)

`ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'))`
answers a text frame exactly equal to the request without waking the object and without billing. 1,000
sockets pinging every 20 s (50 pings/s) never woke a hibernated object, and
`ctx.getWebSocketAutoResponseTimestamp(ws)` returned the last ping time for every socket after the next
wake. An alarm every 20 s closes sockets whose last ping or message is older than 60 s. Silent sockets were
closed 60.1 s after their last ping. Detection is 60–80 s, the same as Rust, and the 10 s offline grace follows.

## Proposal

1. Server: `session.ready.capabilities.presence` stays `1`. Add `capabilities.ping = 20000` (the client ping
   interval in ms) when the server wants application pings. Old servers never send it.
2. Client (`@gonvex/client`): when `ping` is advertised and the socket negotiated `presence:1`, send the exact
   text `{"type":"ping"}` every `ping` ms, whether or not other traffic flows. Ignore `{"type":"pong"}`.
   Treat a missing pong as nothing, because liveness is the server's decision.
3. Server: close 4000 `presence liveness timeout` when a `presence:1` socket has sent neither a ping nor
   another frame for 60 s, checked by an alarm every 20 s while such sockets exist. Rust keeps its protocol
   Pings. The new frame is additive and needs no Rust change unless the lead wants Rust to accept and answer it
   for parity in the conformance harness. Rust's strict decoder would close 1003 on an unknown `type`
   (0012 R17), so either Rust learns `ping`, or the SDK sends it only when the server advertised `ping`.
   Proposal 2 already does the latter.
4. Old SDKs, until they age out: the kernel cannot detect a silent dead socket. Presence for such a Member goes
   offline only on `webSocketClose`/`webSocketError`. To make the edge notice a dead peer, the kernel may push a
   harmless frame to a non-pinging socket every 60 s. An empty `presence.patch` would do for subscribed
   sockets. Detection then depends on the edge's TCP retransmission timeout, which S3 did not measure (it needs
   packet loss injection, which needs root on the load host).

## Why this is a contract change

The client sends a new frame, and the brief requires the shipped SDK to work unchanged. It does work unchanged.
Only dead-peer detection is worse for old SDKs, and the lead must accept that as a deviation for the support
window.
