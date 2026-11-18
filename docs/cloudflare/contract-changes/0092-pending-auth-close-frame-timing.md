# 0092: the pending-auth deadline is measured on the close frame (decided by lead 2026-11-18)

Platform adapt, raised by X03 (criterion 5, run7 session-frames).

## Observation

The kernel enforces the 10 s pending-auth deadline on time. Instrumented on local workerd, the holder's alarm fires
10–45 ms after the deadline, `close(1008, "authentication deadline exceeded")` is sent from an empty session queue,
and the client's close reply reaches the gateway 80–240 ms later.

What arrives late is the WHATWG **close event** of Node's built-in `WebSocket` (undici), which the harness's raw
sockets used. Per the WebSocket spec it fires only once the TCP connection is torn down after the closing
handshake. On Cloudflare that teardown can trail the close frame by about 10 s, or more than 40 s on dev: 3 of 3
idle sockets saw no close event within 40 s, while the `ws` client saw the frame at 9.9 s. The Rust reference
drops the TCP connection right after its close frame, so the same client sees its close event at 10.0 s.

## Decision

For the pending-auth deadline assertion only, the harness measures **when the close frame is received**: its
code, reason and arrival time. The close event is not used. Everything else is unchanged: the 10 s deadline, the
9.5–12.5 s tolerance and the compared outcome text (`closed 1008 authentication deadline exceeded` / `open`).

## Implementation

- `RawSocket` takes an opt-in `{ closeFrame: true }` (`ctx.raw(tag, { closeFrame: true })`). Such sockets use the
  `ws` client, whose receiver reports the close frame with its code and reason. They record every frame exactly
  like other raw sockets and expose `closeFrame` and `whenCloseFrame()`.
- `session-frames` opens its three deadline sockets that way and asserts on `whenCloseFrame()`. No other socket
  or scenario changes client.
- `test/raw-close-frame.check.mjs` covers a server that sends 1008 and never tears the connection down: the
  frame, code and reason are reported on arrival while the close event is still pending. It needs built packages,
  like `mutation.check.mjs`.
- Local run: session-frames passes, and the Rust reference passes under the same measurement.
