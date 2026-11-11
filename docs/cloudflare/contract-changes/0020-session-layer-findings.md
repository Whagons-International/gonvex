# 0020: Session layer findings (K05)

Date: 2026-11-11. Status: **proposed**; items 1-3 need a lead ruling, items 4-7 record what K05 did.
Reported by K05. Affects: CONTRACT.md A.1.7, A.1.9, 0018, K10, K16, K17, P02.

Measured against the Rust runtime at `0bf3ad13` (conformance scenarios `session-handshake` and
`session-frames`, plus direct probes) and on the platform (`gonvex-cf-dev-hib`, deleted).

## 1. A silent socket's server close completes about 10 s late on the platform

On a hibernatable socket on which the client never sent a frame, `ws.close(code, reason)` from the object
(in an alarm, a timer or a handler) delivers the close frame, and the client's close reply reaches the object
(`webSocketClose` fires), but the edge closes the TCP connection only about 10 s later (12.3 s after the open
for a close sent at 2.3 s). Clients fire `onclose` only on TCP close, so they see the close about 10 s late.
Local `wrangler dev` did not close it within 8 s. As soon as the client has sent any frame, TCP closes
immediately after the close handshake. No server-side variant helped (a second `close`, a reply `close` in
`webSocketClose`, a frame before the close).

Effect: the 10 s pending-auth deadline (A.1.8, close 1008) reaches a client that never sends a frame about
20 s after it connected, not 10 s. The close code and reason are right. The SDK sends `auth` on open whenever
it has a token, tenant, project or token fetcher (`packages/client/src/index.ts` `sendAuth`); only an SDK
configured with none of them stays silent, and it then reconnects about 10 s later than on Rust. Conformance shows it as one differing observation, `session-frames` step
"the pending-auth deadline", `outcomes.none` (Rust: closed 1008 at about 10 s; kernel: open at 13 s).

**Proposed:** accept as a platform deviation (only clients that never speak are affected), and keep the
observation in the scenario as the record of it. The alternative is to drop the deadline for silent sockets,
which would change nothing a client sees sooner.

## 2. Rust refuses single frames over 16 MiB on member sockets

A.1.7 says the authenticated transport accepts 64 MiB messages and 16 MiB frames. In practice a client sends a
message as one frame, and Rust drops the connection (no close frame, 1006 at the client) on any frame over
16 MiB (16,777,217 bytes refused, 16,777,216 accepted). 0018 accepted "≤ 32 MiB − 1 KiB is accepted" for
conformance; Rust does not meet that. The kernel accepts up to the platform's 32 MiB, as 0018 decided.

**Proposed:** conformance asserts 16 MiB on both backends (done in `session-frames`) and the kernel-only
workerd test asserts 32 MiB − 1 KiB. The kernel is more permissive than Rust between 16 and 32 MiB; SDK frames
are far below both.

## 3. An oversize frame on the public transport closes 1002, not 1009

A.1.9 lists 1009 "public transport limit exceeded". Rust's compressed transport fails the read and sends 1002
with an empty reason; the handler's 1009 never reaches the client (Node's client negotiates
permessage-deflate, as browsers do). Code wins over docs (brief rule 2): the kernel closes 1002 and the
scenario compares 1002.

**Proposed:** amend A.1.9 row "1009 public transport limit exceeded" to "1002, empty reason (observed)".

## 4. Close codes where Rust sends no close frame

Rust ends the task (TCP close, the client sees 1006) after `client.updateRequired`, after a public/transport
mismatch, when an auth runs past its deadline, and on a send failure. The platform always sends a close frame.
The kernel uses 1000 for the first two and 1011 for the others (A.1.9 "adapt"). Conformance reports all of
them as "closed without a protocol code" and compares only the codes the runtime sends on purpose.

## 5. `{"type":"ping"}`

The kernel answers it with `{"type":"pong"}` by auto-response (0016 G0-6). Rust closes 1003 (unknown type).
The decoder still refuses it; the auto-response runs before the object sees the frame.

## 6. Workers RPC caps a serialized argument at 32 MiB

`Serialized RPC arguments or return values are limited to 32MiB` (workerd). Every tenant-to-gateway delivery
batch is one RPC argument, so it must stay under 32 MiB, the same order as the outbound frame cap of 0018. K10
splits large batches; the gateway also refuses to send any frame of 32 MiB − 1 KiB or more and closes 1011.

## 7. Placement storage and hand-offs

- Gateway registrations live in the tenant object's KV storage (`gateway:<name>`, one write when a gateway
  gets its first socket of the tenant, deleted when a delivery finds none). Not in the tenant's SQL schema.
- Not built in K05 and stubbed behind ports: per-address 429 limits, the public admission recheck and public
  rate limits (K16); `gvx_svc_` service principals (K17, every service credential gets Rust's refusal text);
  `error.*` and `telemetry.*` (K15); presence frames (K14); everything K06 owns (`stub/control.ts`).
