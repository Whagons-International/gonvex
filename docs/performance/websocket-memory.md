# WebSocket per-connection memory

Branch `perf/ws-buffers`, measured 2026-11-03 on arch3 (Linux 7.2, loopback, cgroup v2).
Baseline is `claude/sqlite-storage-lab` at 3524878 with only the benchmark hooks added.

The 5,000-users-in-2-GB target leaves about 260 KB per user for everything, replica
state included. This page measures what one mostly idle authenticated WebSocket costs,
layer by layer, and records the fixes on this branch.

## Results

Bytes per connection, server process only. "Heap" is live bytes from a counting
allocator wrapped around mimalloc (exact). "Anon" is the server cgroup's `memory.stat`
`anon` delta per connection (resident, includes allocator overhead). "Kernel" is the
`slab` delta (TCP socket, file, inode and epoll objects). `sock` (socket buffer memory)
was 0 in every non-stalled phase, because the driver reads every reply.

Runtime path, compressed (permessage-deflate negotiated, as every browser does):

| Phase | N | Heap before | Heap after | Anon before | Anon after | Kernel slab |
|---|---:|---:|---:|---:|---:|---:|
| idle, authenticated | 1,000 | 47,252 | 31,833 | 58,683 | 39,309 | 3,946 |
| idle, authenticated | 5,000 | 47,155 | 31,737 | 49,404 | 35,096 | 3,937 |
| after one 2 KB message each way | 5,000 | 51,100 | 33,624 | 54,821 | 37,456 | 3,937 |
| after one 64 KB message each way | 1,000 | 180,237 | 35,789 | 211,320 | 52,649 | 3,946 |
| after one 64 KB message each way | 5,000 | 180,139 | 35,689 | 212,374 | 40,774 | 3,937 |

Runtime path, plain (no compression offered or `GONVEX_WEBSOCKET_COMPRESSION=false`):

| Phase | N | Heap before | Heap after | Anon before | Anon after |
|---|---:|---:|---:|---:|---:|
| idle, authenticated | 5,000 | 171,097 | 32,966 | 172,123 | 36,868 |
| after 2 KB | 5,000 | 172,949 | 38,913 | 174,773 | 46,268 |
| after 64 KB | 5,000 | 361,145 | 166,098 | 380,837 | 180,528 |

Transport only (the `gonvex_websocket::upgrade` path with an echo task, no runtime state):

| Phase | Compressed before | Compressed after | Plain before | Plain after |
|---|---:|---:|---:|---:|
| idle | 8,932 | 4,860 | 132,783 | 5,807 |
| after 2 KB | 12,880 | 6,838 | 134,757 | 7,781 |
| after 64 KB | 141,786 | 9,004 | 318,019 | 136,687 |

(5,000 connections; 1,000 connections give the same figures within a few bytes, except
plain-before after 64 KB: 329,316.)

At 5,000 compressed connections that have each exchanged one 64 KB message, the server
holds 900 MB → 178 MB of heap for connections (1.06 GB → 204 MB of resident anon),
plus 20 MB of kernel slab.

## Where the bytes are (after the fixes, idle, compressed, runtime path)

| Layer | Bytes | Notes |
|---|---:|---|
| Connection task future (`websocket()` in runtime/src/lib.rs) | 25,832 | tokio task cell; was 36,968 |
| Session state (strings, maps, control connection, metrics entry) | ~3,000 | many small allocations, 25 under 8 B |
| Transport: hyper upgrade + tokio I/O + websocket reader task + channels | ~4,900 | was 8,932 (4,096 BufWriter) |
| Kernel: TCP socket, file, inode, epoll item (slab) | ~3,940 | constant, not tunable from user space |
| Kernel: socket buffers (`sock`) | 0 idle | bounded per stalled client, see "Slow and stuck readers" |

Before the fixes, two more layers mattered:

- Receive buffers retained after a large client frame: Ratchet's frame read buffer grows
  to the largest frame ever received and keeps it, and the reader task's `BytesMut::split()`
  left an empty view that kept the whole message allocation alive. Together that was
  about 130 KB per connection after one 64 KB client message, kept for the rest of the
  connection. Clients do send large frames, for example `replica.open` with resume hashes.
- The tungstenite fallback (plain connections) allocates a 128 KiB read buffer per
  connection by default and zero-fills it on every read.

Compression state is not a per-connection cost on this transport: the negotiation is
`server_no_context_takeover; client_no_context_takeover`, the encoder builds a compressor
per message and the decoder builds an inflater per compressed message, at the end of the
message. Neither is kept between messages (`Deflate` is 48 bytes when idle). Their cost is
per message, below.

## Fixes on this branch

1. **Bounded read buffer for large frames** (`websocket/src/frame_limits.rs`). `FrameLimits`
   already parses every client frame header to enforce the frame limit before Ratchet
   allocates. It now hands masked data frames above 4 KiB to Ratchet as 4 KiB fragments
   (first fragment keeps opcode and RSV1, last keeps FIN, every fragment keeps the
   client's masking key, which is valid because fragments are multiples of 4 bytes).
   Ratchet reassembles the message as it does for any fragmented message, so its read
   buffer never exceeds 4 KiB plus a header. The frame limit still applies to the wire
   frame, the message limit still applies to the reassembled message, and control frames,
   unmasked frames and reserved opcodes reach Ratchet unchanged so its errors are the same.
   Covered by `large_masked_data_frames_are_refragmented_with_the_same_key` and
   `frames_larger_than_the_read_fragment_round_trip` (plain, compressed, fragmented with a
   ping in between, uneven sizes, 7,919-byte writes).
2. **Reader task hands over the whole buffer** (`std::mem::take` instead of `split()`).
3. **No idle write buffer.** `FrameLimits` replaces `BufWriter::with_capacity(4096)`
   with the same coalescing policy (small header and payload writes go out together,
   writes of 4 KiB or more bypass the buffer) but frees the buffer after each flush.
4. **4 KiB tungstenite read buffer** for the plain fallback (was 128 KiB). Larger frames
   still grow it; see "Not done".
5. **Smaller connection task future.** Reducer calls, tenant and control queries, live
   query opens, auth, revalidation, service frames and error-session registration are
   boxed at their await inside `websocket()` (4-18 KB futures each; one allocation per
   such request). Feed delivery (`apply_feed_event`, `apply_live_query_feed_event`, 7 KB)
   stays inline because it runs per event per connection; boxing it too would save
   another 3.5 KB per connection at the price of a 7 KB copy and allocation per delivery.
   36,776 → 25,640 bytes. `connection_future_sizes` (runtime lib tests) prints the sizes
   and fails above 28 KiB.

Protocol behaviour is unchanged: same negotiation, same frame and message limits, same
close codes. Wire bytes sent by the server are identical.

## Compressor cost per message (baseline for the "compress once" work)

`ws_memory compress-cost`, release build, zlib-rs via flate2, level 1, 12-bit window,
pseudo-JSON identifier text (`compress_text` exactly as the encoder runs it):

| Payload | Compressed | ns/message | Allocations/message | Peak transient bytes |
|---:|---:|---:|---:|---:|
| 256 B | 206 B | 4,702 | 3 | 265,569 |
| 2 KB | 1,017 B | 8,209 | 3 | 266,368 |
| 16 KB | 7,304 B | 43,687 | 3 | 273,536 |
| 64 KB | 28,825 B | 212,067 | 3 | 298,112 |

- Building one compressor allocates 265,344 bytes (one allocation, ~3 µs) even at
  `window_bits=12`; zlib-rs sizes deflate state from memLevel 8, not from the window.
  A 15-bit compressor would be 380,032 bytes.
- An inflater is 47,552 bytes and is built per compressed client message.
- Small messages are dominated by compressor construction (more than half of the 2 KB
  cost). With fan-out every recipient currently pays this for an identical payload; the
  shared-compression work can build it once per payload. Nothing here keeps compressor
  state per connection, so that work is not blocked: it can hand the encoder pre-compressed
  bytes (the existing `alternative` slot already carries a precomputed choice per send).

## Slow and stuck readers (no container sysctls needed)

A client that stops reading while the server has data for it used to pin whatever the
kernel's autotuned buffers allowed (up to 4 MB send queue per socket with the host and
staging defaults `tcp_wmem = 4096 16384 4194304`) and was never disconnected. The runtime
now bounds and ends that case itself, per connection:

1. **Per-socket kernel caps** on every accepted socket (`runtime/src/accept.rs`,
   `TunedListener`), no sysctl or privilege needed:

   | Env | Default | Effect |
   |---|---|---|
   | `GONVEX_TCP_SEND_BUFFER_BYTES` | 262144 | `SO_SNDBUF` (kernel doubles it); `0` = platform autotuning |
   | `GONVEX_TCP_NOTSENT_LOWAT_BYTES` | 131072 | `TCP_NOTSENT_LOWAT` (Linux); `0` = platform default |
   | `GONVEX_TCP_RECEIVE_BUFFER_BYTES` | unset | `SO_RCVBUF`; unset keeps receive autotuning (fast uploads) |

   `TCP_NODELAY` is set as before. The listener is bound with a 4096 backlog
   (`accept::LISTEN_BACKLOG`, clamped by `somaxconn`; tokio's `bind` uses 1024) for
   reconnect waves, and the runtime raises its soft `RLIMIT_NOFILE` to the hard limit at
   startup (staging containers start with soft 1024 / hard 524288).
2. **Bounded outbound queue** (`websocket/src/connection.rs`). Every connection has an
   owned writer task; the connection task only queues. Queued plus in-flight payload is
   capped at `GONVEX_WS_OUTBOUND_MAX_BYTES` (default 262144) and
   `GONVEX_WS_OUTBOUND_MAX_FRAMES` (1024); one larger message is still accepted into an
   empty queue. `send` waits at the cap (backpressure for this connection only).
   Large messages are moved to exact-size allocations before they wait anywhere
   (`text_message`, and the reader's reassembly buffer): growing writers double their
   buffer, so a 65 KB frame used to sit in a 128 KiB allocation and the real pin was ~2x
   the configured cap.
3. **Coalescing while behind.** `SocketWatch::is_backlogged()` is true at 64 KiB queued or
   while a write is blocked on a full socket. The connection task then produces nothing
   new: presence keeps merging per member in its slot (latest state wins), feed events
   stay in the shared per-tenant broadcast ring, and a subscription that lags past the ring
   resets and resyncs from its cursor + digest. It resumes on `writable()`.
4. **Send-stall timeout.** If a write makes no byte progress for
   `GONVEX_WS_SEND_STALL_TIMEOUT_SECONDS` (default 30, checked every 1 s), the writer drops
   the queue and resets the socket (`SO_LINGER 0` + close), so the kernel frees the unsent
   data at once. It logs `websocket send stalled` with close code **4008**
   (`CLOSE_SEND_STALLED`); a close frame is not sent because it would queue behind the
   unread data, so the client sees an abnormal closure (1006 / `ECONNRESET`) and reconnects
   with its cursor. This also covers dead TCP peers without a FIN.
5. **Presence liveness.** Presence v2 still closes with **4000** after 60 s without
   evidence the client is alive, but evidence now includes transport facts: inbound frames,
   a blocked write resuming (the peer acknowledged data), the reader holding a message the
   server has not taken yet (the server's own read backpressure, e.g. while replica opens
   are queued), and frames still queued for the peer. So a client draining a large replica
   open is not killed by the liveness timeout, and a stuck one is ended by the send-stall
   timeout instead. A client that reads nothing while nothing is queued for it still fails
   liveness.

### API for the per-tick batch builder

`WebSocket::watch()` returns a cheap `SocketWatch` (clone, no borrow of the socket):
`backlog()` (bytes, frames, write_blocked), `is_backlogged()`, `writable().await`,
`is_closed()`, `send_stalled()`, `last_activity()`. `WebSocket::try_send(message)` queues
without waiting and returns `TrySendError::Full(message)` at the cap. The batch path
(perf/delivery*) should: build at most one frame per connection per tick; if
`is_backlogged()`, keep merging that connection's pending rows/members into its per-row
latest-state map instead of encoding; on `writable()` encode the merged state once and
`try_send` it; on `Full`, take the message back, keep merging, retry on the next
`writable()`; if a subscription's pending state exceeds its own budget, drop it and send
the resync signal (cursor + digest) rather than the rows.

### Measurement: 1,000 clients that stop reading

`STALL=32`: each of 1,000 clients sends 32 × 64 KB requests whose replies are ~64 KB and
reads nothing. Server in a 2 GB `systemd-run` scope (`MemoryMax=2G`, no swap), arch3
loopback. Peak per connection; before = 67cc513f (pre-fix), after = this branch with
defaults. `sock` = cgroup socket buffer memory, anon = cgroup anon (user space, resident).

| Path | sock before | sock after | anon before | anon after | heap after | disconnected before | disconnected after | cgroup peak before → after |
|---|---:|---:|---:|---:|---:|---|---|---|
| runtime, compressed | 880,611 | 461,144 | 434,630 | 699,789 | 596,382 | never (120 s) | 45.9 s | 1.2 G → 1.1 G |
| runtime, plain | 1,636,909 | 361,287 | 420,532 | 811,216 | 572,297 | never (120 s) | 36.8 s | 1.9 G → 1.1 G |
| transport, compressed | 977,793 | 487,702 | 58,548 | 502,890 | 498,510 | never (120 s) | 36.0 s | 1.0 G → 0.97 G |
| transport, plain | 1,992,257 | 346,501 | 141,463 | 588,472 | 487,688 | never (120 s), one run OOM-killed at 11 s | 36.1 s | 2.0 G → 0.91 G |

- Before, user space was small because the connection task blocked inside the socket
  write and the kernel held the data; the kernel share was unbounded by anything but
  autotuning and the client was kept forever. After, the total per stalled connection is
  bounded (~0.9-1.2 MB, roughly: send buffer ~150 KB, the flooding client's receive queue
  ~200-300 KB, 256 KiB queue + one in-flight reply, up to three 64 KB inbound requests) and
  every such connection is gone 30 s after its writes stop moving (36-46 s after the flood
  starts, because the queue first has to fill).
- Before the exact-size fix the same run held 1.58 MB heap per connection (200-client
  diagnostic), with a 512 KiB queue of 128 KiB allocations.
- The remaining kernel share is mostly the client's own uploads sitting in the server's
  receive queue. `GONVEX_TCP_RECEIVE_BUFFER_BYTES=65536` cut `sock` to 238,731 per
  connection (cgroup peak 951 MB) in the same run; it is not the default because it
  disables receive autotuning for large client frames on long links.
- After close, resident memory is not returned at once (mimalloc page retention; heap is
  back to ~130 B per connection).

Normal clients (same driver, 1,000 connections, one request each way, two alternating
rounds per binary on a shared machine): wire bytes per connection identical in every mode
(1,218 / 28,946 compressed runtime, 2,293 / 65,787 plain runtime, 995 / 28,714 and
1,932 / 65,420 transport). Round-trip p50/p95 are within run-to-run noise of each other
(e.g. runtime compressed 2 KB p50 4.5-6.1 ms before vs 5.4-7.5 ms after; 64 KB plain
transport p99 9-77 ms before vs 10-113 ms after, tails dominated by machine load).

### Container settings

No sysctls are required any more: the caps above are per socket and work with the stock
Docker namespace defaults. Checked read-only on 2026-11-03: the staging SQLite runtime
container (`gonvex-runtime:sqlite-f482033c` on the Whagons host, Docker 29.8, cgroup v2,
no `daemon.json` ulimit or sysctl overrides) has `somaxconn` 4096, `tcp_wmem` max 4 MB,
`tcp_rmem` max 6 MB, and `nofile` soft 1024 / hard 524288 (another Coolify-managed runtime
container on the same host has soft 524288). The runtime now raises the soft limit to the
hard limit itself; `somaxconn` 4096 matches the new listen backlog.

## Not done / not measured

- No runtime-level test asserts that feed delivery pauses while a connection is
  backlogged; that path is covered by the transport tests (`outbound_tests.rs`) and the
  presence draining/stuck tests (`tests/presence_v2.rs`). The batch builder's use of
  `try_send` is documented, not implemented here (other workstream).
- The stall experiment used 1,000 clients on loopback; 5,000 stalled clients and real
  RTT/loss were not measured.
- The plain (tungstenite) fallback still keeps its read buffer at the size of the largest
  frame received (256 KiB after a 64 KB frame because of its doubling) and its write
  buffer at the largest frame sent. Fixing that means running the plain path through the
  same `FrameLimits` shim (for example via Ratchet without an extension); not done to keep
  the fallback's behaviour identical.
- The remaining ~19 KB of the connection future is state the 1,660-line `websocket()`
  function keeps across awaits, not any single awaited call (boxing every await only
  reached 21,112 bytes). Splitting the per-message handlers out of that function would
  remove most of it; that is a larger refactor of a function other workstreams edit.
- Measured on loopback with one client process; real networks with RTT and loss will show
  non-zero `sock` for in-flight data even with prompt readers. Not measured on the Mac lab
  container.
- Heap figures are exact; resident figures include mimalloc page retention (memory freed
  after close is not returned at once, see the `closed` phase in the raw output).

## Reproduce

```sh
cd rust
cargo build --release -p gonvex-runtime --example ws_memory
../bench/ws-memory/run.sh target/release/examples/ws_memory /tmp/ws-mem            # 1k and 5k, plain and compressed, transport and runtime
SIZES= STALL=32 ../bench/ws-memory/run.sh target/release/examples/ws_memory /tmp/ws-stall 'runtime transport' 'on off' 1000
SIZES= STALL=32 SERVER_ARGS="--rcvbuf 65536" ../bench/ws-memory/run.sh target/release/examples/ws_memory /tmp/ws-stall-rcv runtime on 1000
# the old binary as server, the new one as client: CLIENT_BIN=<new ws_memory> run.sh <old ws_memory> ...
target/release/examples/ws_memory compress-cost
cargo test --release -p gonvex-runtime --lib connection_future_sizes -- --nocapture
```

`run.sh` starts the server in its own `systemd-run --user --scope -p MemoryMax=2G` so
`memory.stat` is per server, raises `ulimit -n` for the bench processes only, and prints,
per phase, heap / RSS / cgroup `anon`, `sock`, `slab`, `kernel` deltas per connection,
summed `ss -tm` skmem, and live allocations by size class. The runtime mode signs in with
a real native session (password login on a temporary SQLite control plane); no V8 is
needed.
