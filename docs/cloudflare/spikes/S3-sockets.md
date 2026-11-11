# S3: 1,000 WebSockets on one Durable Object

Package S3, stage 0. Measured 2026-11-11 (UTC) locally (wrangler 4.158.0 / workerd 1.20261104.1) and on the
platform as Worker `gonvex-cf-dev-s3` (compatibility date 2026-11-01, development account), with load driven
from arch3 (edges SJC and LAX; 106 MB/s from the edge to arch3). Code: `cloudflare/spikes/s3/`.
Every number below comes from a file in `cloudflare/spikes/s3/results/`. Object CPU and active time come from
GraphQL Analytics (`durableObjectsPeriodicGroups`, `results/analytics-cf.json`). Billed requests come from
`durableObjectsInvocationsAdaptiveGroups` (`results/requests-cf.json`). The deployed Worker and its objects
are deleted.

The spike is one Worker with two SQLite-backed classes that share one delivery class (`SocketHub`):
`TenantDO` (the change log, sockets, liveness alarm) and `GatewayDO` (sockets only). Delivery works the same in
either placement. Each socket is accepted with the Hibernation API and carries the attachment
`{m: memberId, c: class, cur: cursor, live, at}` and the tags `c<class>` and `m:<member>`. A change is a
`replica.transaction`-shaped JSON frame of about 2 KB (1,885 bytes on the wire), encoded once per visibility class
(8 classes, 125 sockets each), and the same string goes to every socket of the class.

## Verdict

**One object holds 1,000 sockets easily, but it cannot fan out 30 writes/s to them.** At 1,000 sockets
the ceiling is about 10 fan-outs per second. Gateway objects of about 125 sockets carry the 30/s burst. Liveness
needs a small SDK change (contract-change 0015).

| Question | Answer (platform unless marked) |
|---|---|
| Memory, 1,000 sockets | **≈8.5 MB** in an awake isolate (local inspector: 5.8 MB V8 heap + 2.7 MB embedder, ≈6.7 KB per socket, linear from 250 to 1,000). A hibernated object holds nothing. The platform ballast probe still had more than 75 MB of headroom with 1,000 sockets attached (the reset point varies 80–136 MB, too noisy to resolve a few MB). |
| CPU to fan one 2 KB change to 1,000 (8 classes) | **≈3.8 ms** CPU and ≈15 ms active with a per-class `Set` index (≈4 µs per send). With `getWebSockets(tag)` per class: ≈9.9 ms. Encoding the 8 class frames: ≈0.2 ms. With permessage-deflate: ≈9.6 ms (compression is billed to the object). |
| Time to fan one change to 1,000 | Writer send → all 1,000 receivers on arch3, single object: delivery p50 123 ms, last socket p50 224 ms (echo RTT 31 ms). The writer's own ack waits ≈100 ms behind the 2 MB of fan-out frames. With 8 gateways: last socket p50 157 ms. With deflate: 180 ms. |
| 30 writes/s for 60 s, one object | **No.** It sustains ≈10 fan-outs/s (≈10k frames/s at 1.9 KB, ≈14k/s at 440 B or with deflate). Above that the queue grows without any signal to the object (latency 9 s at 15/s, 20 s at 20/s). At 60/s the platform closed **all** 1,000 sockets with `1013 Service overloaded; retry later`. The object used under 20% CPU throughout: the limit is outbound frame throughput, not CPU. |
| 30 writes/s for 60 s, 8 gateways × 125 | **Yes**: 1,800,000/1,800,000 frames, latency p50 89 ms, p99 342 ms, max 1.08 s. 4 × 250 holds 20/s (p99 1.2 s) but not 30/s (p90 12 s; with deflate p99 1.8 s, and one gateway still more than 30 s behind at the end). |
| permessage-deflate | **Supported.** workerd negotiates it and echoes the client's offer (default offer: context takeover, 15-bit window; it cannot impose Rust's `no_context_takeover; max_window_bits=12`). 4.4× fewer bytes (1,885 → 424 per frame). It raises the per-object ceiling ≈1.5× (10/s: p50 121 ms instead of 1.6 s) at 2.5× the CPU. |
| 32 MiB inbound cap | Accepted ≤ 32 MiB − 128 B. Between 32 MiB − 64 B and 32 MiB the message is lost and the socket dies with 1006. Above 32 MiB: 1009 `Message is too large`. Outbound frames ≥ 32 MiB are **silently dropped** (≤ 32 MiB − 1 KiB arrive). Contract-change 0016. |
| Liveness without server pings | `setWebSocketAutoResponse('{"type":"ping"}' → '{"type":"pong"}')` works while hibernated, costs nothing and wakes nothing. It records a timestamp per socket. A 20 s alarm sweep closed silent sockets at the first sweep 60 s after their last ping, which gives the same 60–80 s detection as Rust, plus the 10 s grace. **Requires the SDK to send the ping frame** (0015). Old SDKs: a dead idle socket is never detected. The edge sent no pings and closed nothing in 20 minutes. |
| Deploy with sockets open | Default strategy (deferred, 300 s) on a hibernated object: **0 of 1,000 sockets dropped**, and the object woke on the new build. Active object: every socket closed **1006** at `max_delay` (30.0 s in the test) or at once with `immediate`. All 1,000 were back in 4.1–6.0 s (accept ≈0.7 ms CPU each). |
| Hibernation, state rebuilt | Hibernates 5–10 s after the last event with sockets open. Pings don't prevent it. A wake adds ≈25–50 ms (p50) to the first round trip at 1,000 sockets, against ≈12–30 ms at 1 socket. Rebuilding every attachment eagerly (even 1,000 × 15 KB) adds nothing measurable. Attachment limit: **16,384 serialized bytes**. Sockets survive wakes. An isolate memory reset closes them all (1006). |
| Cost of 1,000 sockets | Socket layer ≈ **$0.26/month** per 1,000-user tenant on the workload below, mostly inbound frames at 20:1. Pings 0 (auto-responses are free; they would otherwise cost ≈$0.30). Gateways add ≈ **$1.25/month** at 1 commit/s, mostly RPC requests. §9. |
| Gateway hop | Tenant → gateway RPC, empty delivery: **6–17 ms p50 in the same colo** (min 4 ms), 15–20 ms SJC↔LAX. Under the 30/s burst, including the gateway's 125–250 sends: 59–83 ms p50. |

## 1. Memory

Local workerd, V8 heap read through the inspector after a forced GC (`load/memory.mjs`, `results/memory-local.json`),
with the object awake and its socket index built:

| Sockets | V8 used | Embedder heap | Total |
|---:|---:|---:|---:|
| 250 | 2.60 MB | 0.86 MB | 3.46 MB |
| 500 | 3.72 MB | 1.48 MB | 5.20 MB |
| 1,000 | 5.79 MB | 2.71 MB | 8.50 MB |

That is ≈4.3 KB of V8 heap and ≈2.5 KB of embedder heap per socket (JS `WebSocket` wrappers, attachments
deserialized into the index, tags). While hibernated, the sockets live in the runtime outside the isolate.

The platform has no memory API (`actorMemoryMb` in analytics was 0 for every object). The ballast probe
(`ballast?kind=heap`, 4 MB steps until the instance id changes) reset the isolate between 80 and 136 MB with
1,000 sockets attached, and between 104 and 128 MB with none (`results/memory-probes-cf.json`). The spread is larger
than the effect being measured, so the local number stands.

**When the isolate exceeds its memory, every socket of the object is closed with 1006** (1,000 of 1,000).
Socket-heavy objects must stay far from 128 MB. This is also an argument for gateways: they limit the blast radius.

## 2. Fan-out CPU and time

Per variant, a separate tenant with 1,000 sockets ran 300 fan-outs (30 events of 10) of one change, and nothing else
happened in it. CPU and active time are the analytics totals minus an identical idle tenant
(1,000 accepts and closes: 729 ms CPU, 1,447 ms active). Source: `load/cpu.mjs`, `results/cpu-cf.json`,
`results/analytics-cf.json`.

| Variant | CPU per fan-out | Active per fan-out | Per send |
|---|---:|---:|---:|
| Per-class `Set` index in memory (`cache`) | 3.8 ms | 15 ms | 3.8 µs |
| `getWebSockets("c<n>")` per class (`tags`) | 9.9 ms | 36 ms | 9.9 µs |
| `deserializeAttachment()` per socket (`attach`) | 2.7 ms | 7 ms | 2.7 µs |
| Encode only (8 class frames, no send) | 0.18 ms | — | — |
| `cache` with permessage-deflate | 9.6 ms | 37 ms | 9.6 µs |

`attach` and `cache` are within the noise of each other; tags are clearly slower. A kernel should keep an in-memory
index rebuilt on wake (§8). The analytics also show where the burst limit comes from. In the single-object
30/s run the object pushed 621,609 frames in one minute using 4.7 s of CPU, and in the 10/s → 20/s sweep
1,004,023 frames using 20.7 s. No CPU, memory or fatal errors were counted in any run, including the runs that
ended in 1013.

End-to-end time for a single commit: the writer sends `write` and receives `ack`; 1,000 receivers record
`now − writerSendTime` on the same host, so no clock sync is involved. `results/fanout-cf.json` and the others:

| Placement | Delivery p50 | p99 | Last socket p50 | Writer ack p50 | Echo RTT |
|---|---:|---:|---:|---:|---:|
| One object | 123 ms | 692 ms | 224 ms | 129 ms | 31 ms |
| One object, deflate | 110 ms | 228 ms | 180 ms | 118 ms | 34 ms |
| 4 gateways × 250 | 91 ms | 603 ms | 233 ms | 144 ms | 17 ms |
| 8 gateways × 125 | 81 ms | 443 ms | 157 ms | 125 ms | 32 ms |

## 3. Burst: 30 writes/s for 60 s

The writer socket sends `write` at a fixed rate. Each write appends a row to the SQLite change log, encodes 8 class
frames and delivers them. The burst ends when every ack has arrived or after a 30 s drain.

**One object, 1,000 sockets, 2 KB** (`results/sweep-cf.json`, `results/fanout-cf.json`):

| Rate | Achieved | Delivery p50 | p99 | Frames delivered |
|---:|---:|---:|---:|---:|
| 5/s | 5.0/s | 166 ms | 345 ms | 100% |
| 10/s | 9.5/s | 1.6 s | 3.0 s | 100% |
| 15/s | 10.8/s | 9.1 s | 12.1 s | 100% (after drain) |
| 20/s | 9.5/s | 19.2 s | 22.3 s | 99.4%, then the object restarted and the writer socket closed |
| 30/s (60 s) | — | 24.9 s | 30 s+ | 43% within the window |

**One object, 440-byte frames** (`results/small-cf.json`): 10/s → p50 914 ms, 20/s → 13/s achieved, 30/s →
13.8/s achieved. At 60/s every socket, the writer included, was closed with **1013 `Service overloaded; retry later.`**

Fitting the two frame sizes gives about **66 µs per frame plus 18 ns per wire byte** of outbound capacity per object:
≈10k frames/s at 1.9 KB, ≈14k/s at 0.4 KB. It is not CPU (above). `WebSocket.send` gives no back-pressure
signal: frames queue invisibly until the platform sheds the object.

**Gateways, 2 KB** (`results/gw4-cf.json`, `results/gw8-cf.json`, `results/gw4-deflate-cf.json`):

| Layout | Rate | Achieved | Delivery p50 | p90 | p99 | Max | Frames delivered | Hop p50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 × 250 | 10/s | 9.8/s | 90 ms | 126 ms | 254 ms | 831 ms | 100% | 78 ms |
| 4 × 250 | 20/s | 19.8/s | 106 ms | 389 ms | 1.2 s | 1.6 s | 100% | 97 ms |
| 4 × 250 | 30/s × 60 s | 15.5/s | 379 ms | 12.3 s | 16.0 s | 16.7 s | 92% in window | 404 ms |
| 4 × 250, deflate | 30/s × 60 s | 18.5/s | 112 ms | 846 ms | 1.8 s | 2.5 s | 98% in window | 83 ms |
| **8 × 125** | **30/s × 60 s** | **29.8/s** | **89 ms** | **168 ms** | **342 ms** | **1.08 s** | **100%** | 69 ms |

At 8 × 125 each gateway sends 3,750 frames/s, ≈40% of one object's ceiling. The tenant object's CPU in that run was
≈5 s per minute (encode, SQLite append, 8 RPCs per commit). Workers Logs for the 4 × 250 deflate run:
tenant commit CPU p50 2 ms, p99 13 ms; gateway delivery (250 sends, deflate) CPU p50 5 ms, p99 36 ms.

## 4. permessage-deflate

- Negotiated by workerd under the default compatibility flags. The client's offer is echoed:
  `permessage-deflate` → `permessage-deflate; client_max_window_bits=15` (context takeover).
  `client_no_context_takeover; server_no_context_takeover; server_max_window_bits=10` is honoured as offered.
  The object cannot choose parameters. It can only refuse compression, by stripping `Sec-WebSocket-Extensions`
  in the Worker before the upgrade reaches the object.
- 1,885 → 424 bytes per frame (4.4×) on this JSON. Local workerd saturated at 10 writes/s with deflate
  (`results/fanout-deflate-local.json`); the platform did not.
- Platform, one object (`results/deflate-cf.json`): 5/s p50 102 ms; 10/s p50 121 ms (1.6 s without);
  15/s p50 495 ms (9.1 s without). CPU per fan-out 9.6 ms instead of 3.8 ms.
- Recommendation: leave it on (it is the platform default and what the SDK offers). It raises the ceiling and
  cuts egress. Its CPU cost is small next to the frame-rate limit.

## 5. Message caps

`load/big.mjs`, `load/big-threshold.mjs`; contract-change 0016.

| Direction | Size | Platform | Local workerd |
|---|---|---|---|
| in | ≤ 32 MiB − 128 B | delivered | delivered |
| in | 32 MiB − 64 B … 32 MiB | lost, socket 1006 | delivered |
| in | 32 MiB + 1, 33 MiB | 1009 `Message is too large: N > 33554432` | same |
| out | ≤ 32 MiB − 1 KiB | delivered | delivered |
| out | 32 MiB, 33 MiB, 64 MiB, 100 MiB | **silently dropped** (`send` does not throw) | delivered |

## 6. Liveness without server pings

Measured (`load/liveness.mjs`, `results/liveness-cf.json` for the 1,000-socket run, `live-*` rows in
`results/analytics-cf.json` for both runs, `results/liveness-local.json`, `load/idle.mjs`, `results/idle-cf.json`):

- 1,000 sockets sending `{"type":"ping"}` every 20 s (3,007 pongs in 60 s): the object hibernated anyway.
  After the next wake, `getWebSocketAutoResponseTimestamp` returned a timestamp for 1,000/1,000 sockets
  (ages 0–20 s). Billed requests did not move: thousands of pings, 0 requests.
- Silent sockets (TCP open, no pings): with an alarm every 20 s and a 60 s timeout, 10 of 10 were closed with
  `4000 presence liveness timeout` at the first sweep 60 s after their last ping. That was 40.1–60.1 s after they
  stopped, because their last pings were spread over the preceding 20 s. None of the 990 pinging sockets was closed.
  The 100-socket run gave the same result (5 of 5).
- One sweep of 1,000 sockets costs ≈17 ms CPU and ≈55 ms active, including the wake (3 per minute while sockets
  exist). The object hibernates between sweeps.
- 20 minutes idle: a socket that sends nothing was not closed, and the edge sent it no protocol Pings.
  Client protocol Pings were answered (59/59) but leave no auto-response timestamp, so they are invisible to the object.

Design for K14 (and the grace setting R22 waits for):

1. The constructor calls `setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}','{"type":"pong"}'))`.
2. Sockets that negotiated `presence:1` on a server advertising `capabilities.ping` send the ping frame every 20 s (0015).
3. Last activity = max(auto-response timestamp, last inbound frame in memory, connect time in the attachment).
   Nothing is written per ping or per frame.
4. The tenant alarm (shared with the scheduler, R6) also runs a sweep every 20 s while presence sockets exist,
   and closes sockets idle for more than 60 s with 4000. Detection: 60–80 s after the last ping, as in Rust.
5. The offline grace stays 10 s after `webSocketClose`/`webSocketError`/4000. A silent client is therefore shown
   offline 70–90 s after it dies, the same as Rust. **K14 can keep Rust's 10 s grace.**
6. Old SDKs (no ping): offline only on close or error. A dead idle socket stays present until the edge's TCP gives up,
   which needs outbound data and was not measured (packet-loss injection needs root on arch3). 0015 proposes
   an optional 60 s no-op push to non-pinging sockets.

## 7. Deploys

`load/deploy.mjs`, `results/deploy-cf.json`: 1,000 sockets that reconnect with 0–1 s jitter, three successive deploys.

| Case | Strategy | Sockets closed | When | Back |
|---|---|---:|---|---|
| A. Object hibernated (no traffic) | default (deferred, 300 s) | **0** | — | the next event ran the new build with 1,001 sockets attached |
| B. Object active (2 writes/s) | `deferred 30s` | 1,000, code 1006 | 29.9–30.0 s after the deploy | all in 6.0 s (reconnect p50 3.2 s, p99 5.3 s) |
| C. Object active | `immediate` | 1,000, code 1006 | as the deploy returned | all in 4.1 s (p50 2.4 s, p99 3.1 s) |

The code-update strategy (changelog 2026-10-30) means **an idle tenant deploys without dropping its sockets,
but only if its objects can hibernate**. The kernel must therefore avoid `setTimeout`/`setInterval` (alarms only)
and avoid outbound sockets. Busy tenants drop all their sockets at `max_delay`. With gateways, only the active
gateways drop.

During a rollout, a fresh object can still run the previous version. A new tenant called a gateway created a
second earlier, and the gateway answered from the old code (`The RPC receiver does not implement the method "colo"`).
**Tenant ↔ gateway RPC must be versioned and tolerate both versions.**

## 8. Hibernation and per-socket state

`load/hibernate.mjs`, `load/attach.mjs`; `results/hibernate-*-cf.json`, `results/attach-cf.json`. Two tenants run side
by side, one with 1,000 sockets and one with only the probe socket. Wake overhead = first echo after 60 s idle
minus the next echo:

| Run | 1,000 sockets | 1 socket |
|---|---:|---:|
| eager rebuild (deserialize every attachment in the constructor) | 42–73 ms, p50 50 | 24–35 ms, p50 31 |
| eager rebuild, second run | 48–75 ms, p50 50 | 18–41 ms, p50 27 |
| lazy (no rebuild until needed) | 21–46 ms, p50 35 | 10–14 ms, p50 12 |
| eager, attachments padded to 15 KB (15 MB in total) | 48–80 ms, p50 58 | — |
| eager, same object, small attachments | 39–189 ms, p50 68 | — |

- The object hibernates between 5 and 10 s after its last event (`results/hibernate-eager-cf.json`, idles 5/10/15/20/30/60 s).
- The ≈25 ms extra at 1,000 sockets is the runtime restoring hibernated sockets. Deserializing the attachments,
  even 15 KB each, adds nothing measurable. Rebuild the index eagerly.
- `serializeAttachment` limit: **16,384 bytes serialized**. 16,320 B of padding fit; 16,330 B failed with
  `A WebSocket 'attachment' cannot be larger than 16384 bytes`. CONTRACT.md A.1.8 assumed 2 KiB. Rewriting all
  1,000 attachments costs about 5 µs each (`touch` probe).
- Recommendation for A.1.8 ("S3 decides"): keep identity, capabilities, contract, session id, cursor at attach
  and a compact form of the socket's subscriptions in the attachment (up to 16 KiB). Rebuild in-memory indexes
  in the constructor, let the object hibernate, and never rewrite attachments per commit: a cursor is implied by
  what was sent, and a reconnecting client resumes from its own cursor. A socket whose subscription state does not
  fit in 16 KiB is a lead decision: keep the object awake for it, or treat the subscription definitions as durable
  session state in SQLite.
- Idle sockets occasionally drop: 3, 22, 3, 3 and 0 of 1,000 over 4–8 minute holds, all with 1006 where recorded
  (`results/hibernate-hold-cf.json`). The cause is not identified. The SDK must keep reconnecting with backoff, as it does.

## 9. Cost

Pricing as published 2026-11-10: $0.15 per million requests (connection, RPC call, alarm, HTTP; inbound WebSocket
messages count 20:1; outgoing frames and `setWebSocketAutoResponse` replies are free). Duration $12.50 per million
GB-s at a fixed 128 MB, billed only while an event runs or the object is not eligible to hibernate.
`setAlarm` counts as a row written ($1.00 per million). Account-wide allowances (1M requests, 400k GB-s, 50M rows)
are ignored below.

Measured units (`results/requests-cf.json`, `results/analytics-cf.json`):

| Unit | Requests | Active time |
|---|---:|---:|
| Socket accept | 1 | ≈1.4 ms |
| Socket close event | counted as 1 invocation | — |
| Ping (auto-response) | 0 | 0 |
| Fan-out of one change to 1,000 sockets | 0 | ≈15 ms (37 ms with deflate) |
| Liveness sweep of 1,000 sockets (alarm, incl. wake) | 1 + 1 row written | ≈55 ms |
| Tenant → gateway delivery | 1 per gateway | ≈30 ms in the gateway at 125 sockets |

Worked month for one tenant with 1,000 users connected 10 h on 22 days (792,000 s). Assumptions: 5 connections per
user per day, 1 app frame per user per minute plus presence transitions every 101 s, and 1 commit per second on
average.

| Item | Volume | Cost |
|---|---:|---:|
| Connections (+ close events) | 110,000 (+110,000) | $0.03 |
| Inbound frames | 21.1M frames → 1.06M requests | $0.16 |
| Liveness alarms | 39,600 requests + 39,600 rows | $0.05 |
| Pings | 39.6M frames, 0 requests (≈$0.30 if they were ordinary frames) | $0.00 |
| Duration: fan-outs, sweeps, accepts | ≈14,200 s → 1,775 GB-s | $0.02 |
| **Socket layer, one object** | | **≈$0.26** |
| With 8 gateways: delivery RPCs | 6.3M requests | $0.95 |
| With 8 gateways: gateway duration | ≈190,000 s → 23,800 GB-s | $0.30 |

Executing the Queries and Reducers behind those frames is not counted here (S2/K04). One hour at the full 30 writes/s
burst through 8 gateways costs ≈864,000 RPC requests ($0.13) plus ≈3,200 GB-s ($0.04). Coalescing commits into one
gateway RPC per 50–100 ms tick caps RPCs at 10–20 per second per gateway whatever the commit rate.

## 10. Tier-1 gateway sketch

Measured hop (`load/hop.mjs`, `results/hop-cf.json`): RPC tenant → gateway with an empty delivery, 20 calls each.

| Gateways | Same colo as tenant | Other colo (SJC↔LAX) | First call (creates the object) |
|---|---|---|---|
| created by the tenant | 4–8 ms p50 (min 4 ms) | 16 ms p50 | 238–333 ms |
| created by the edge (first client) | 9–17 ms p50 | 15–20 ms p50 | 44–113 ms |

Objects are placed near their first caller, but not strictly in its colo: one of four tenant-created gateways
landed in the other colo.

Design:

1. **One delivery interface, two placements**: `deliver(framesPerClass: string[], seq)`. The spike's `SocketHub`
   is the same code in `TenantDO` (local placement) and `GatewayDO` (RPC placement).
2. **Routing** follows 0004 option 2. The upgrade lands on a gateway shard chosen without the tenant. After
   `auth`, the gateway registers "tenant T has sockets here" with T's object (once per tenant per gateway, not
   per socket), and unregisters when the last one leaves. The tenant keeps the set of gateways in memory, rebuilt on wake by
   asking them. It is not written per socket.
3. **Per commit**: the tenant appends to its change log, encodes once per visibility class, and makes one RPC per
   registered gateway with the class frames (coalesced per 50–100 ms tick). The gateway sends the same string to
   every socket of the class.
4. **Sizing by frame rate, not CPU** (X01's trigger): keep each object under ≈4k frames/s (40% of the measured
   ceiling). At 30 commits/s that means ≈125 sockets per gateway. A gateway of ≈250 is fine up to ≈15 commits/s.
   The object counts frames it sends per second; CPU stays under 20% at the ceiling and is the wrong signal.
5. **Back-pressure** comes from the RPC: `deliver` returns after its sends are queued. When a gateway's round trip
   exceeds a bound (for example 1 s), the tenant stops sending it individual commits. It sends one coalesced
   frame per class, or `replica.reset reason:"change-feed-lagged"` to the gateway's sockets (CONTRACT.md A.1.8).
   Nothing else signals the object before the platform's 1013.
6. **Gateways hold no durable state**. A reset or deploy closes only that gateway's sockets, and clients resume
   from their own cursors. Idle gateways hibernate and adopt new code without dropping sockets (§7).
7. **Versioning**: RPC methods are additive and both sides tolerate the other version during a rollout (§7).
8. The alternative in 0004 option 1 (router Worker proxy) was not measured.

## 11. Recommendation

- Use the **tenant object placement up to ≈4k frames/s** (for example 1,000 sockets at ≤4 commits/s, or
  250 sockets at ≤15/s). Switch a tenant to gateways of ≈125 sockets when its frame rate needs it (X01). That
  covers the 1,000-user, 30 writes/s target with p99 342 ms.
- Keep permessage-deflate on.
- Adopt the ping frame and alarm sweep for liveness (0015). K14 keeps the 10 s grace, and R22 resolves to
  "60–80 s detection by client ping, same as Rust; old SDKs detected on close only".
- Keep hibernation working: no timers, alarm only, attachments ≤16 KiB, eager index rebuild. Deploys then drop
  sockets only on busy objects.
- The kernel bounds its own send rate per object and never sends a frame ≥ 32 MiB − 1 KiB (0016). Neither failure
  is reported to the object.

## Reproduce

See `cloudflare/spikes/s3/README.md`.
