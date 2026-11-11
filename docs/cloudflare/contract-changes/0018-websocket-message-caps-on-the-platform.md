# 0018: The platform's WebSocket message caps are not exactly 32 MiB

Date: 2026-11-11. Status: **decided in 0016 G0-7** (accepted). (amends 0012 R21). Reported by S3, measured on the platform
(`docs/cloudflare/spikes/S3-sockets.md` §5, `cloudflare/spikes/s3/results/big-cf.json`,
`big-threshold-cf.json`). Affects: K05, K10, P02 (conformance), 0012 R21.

## Finding

Client → object (inbound), one message per row, on a hibernatable socket:

| Message size | Result |
|---|---|
| ≤ 32 MiB − 128 B (33,554,304) | delivered to `webSocketMessage` |
| 32 MiB − 64 B … 32 MiB (33,554,368 … 33,554,432) | **lost**; the socket dies with **1006** and no close frame (text: no reply for 60 s, then 1006 on the next send) |
| > 32 MiB | close **1009** `Message is too large: N > 33554432` |

Local workerd accepts exactly 33,554,432 and closes 33,554,433 with 1009. On the platform, the edge adds 64–128 B of
overhead before the object's limit is checked.

Object → client (outbound): frames up to 32 MiB − 1 KiB arrive. Frames of 32 MiB, 33 MiB, 64 MiB and 100 MiB were
**silently dropped**. `ws.send()` did not throw, no close followed, and the client received nothing (90 s timeout).
Local workerd delivers all of them.

## Proposal

1. R21's conformance boundary becomes "≤ 32 MiB − 1 KiB is accepted; > 32 MiB is refused". The band between is
   platform-defined and is not asserted.
2. The kernel never sends a frame ≥ 32 MiB − 1 KiB. Anything that could approach it (`replica.batch`,
   `query.result`, Action results) is split or fails with a clear error, because the platform drops it silently.
   Today's pages (≤ 256 KiB) are far below this.
3. SDK frames are far below both caps, so no client change is needed.
