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
| Kernel: socket buffers (`sock`) | 0 idle | up to ~1-2 MB per stalled client, see below |

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

## Kernel socket memory

Idle and promptly-read connections hold no socket buffer memory (`sock` = 0, `ss -tm`
skmem r/t/w = 0). The risk is slow or stalled readers: the kernel queues unsent data up to
the send buffer, and with autotuning the per-socket limits are large (host here:
`tcp_wmem = 4096 16384 4194304`, `tcp_rmem = 4096 131072 33554432`). cgroup v2 charges
this memory to the container (`memory.stat sock`), and it counts against `memory.max`.

Stalled-reader measurement (`STALL=32`: each client sends 32 × 64 KB messages to the echo
transport and reads nothing; 1,000 connections):

| Setting | Path | `sock` per connection | Heap per connection |
|---|---|---:|---:|
| default autotuning | compressed | 1,026,547 | 52,515 |
| default autotuning | plain | 1,994,575 | 136,685 |
| SO_SNDBUF = SO_RCVBUF = 64 KiB | compressed | 266,108 | 234,398 |
| same caps, 5,000 connections | compressed | 164,592 | 172,797 |

With default autotuning, 1,000 stalled plain clients pushed the server's 2 GB cgroup into
the OOM killer. With 64 KiB caps the kernel share drops 4-6x; in this echo driver more
data then waits in user space (the echo task holds received messages), which the runtime's
connection task does not do (it stops reading while a write is blocked; fan-out backlog
lives in the shared per-tenant broadcast ring, not per connection).

## Recommendations

For a 2 GB container holding 5,000 sockets:

- **Per-connection budget now**: ~36-41 KB resident (user) + ~4 KB kernel slab for an idle
  compressed connection, i.e. ~200 MB at 5,000 users before any replica state. Replica
  state is the remaining budget item, not the transport.
- **Container sysctls** (all per network namespace; set with `docker run --sysctl` or the
  compose `sysctls:` key, not on the host):
  - `net.ipv4.tcp_wmem = 4096 16384 262144` caps a slow reader's queued send data at
    roughly 0.5 MB of charged memory instead of 4 MB+.
  - `net.ipv4.tcp_rmem = 4096 16384 262144`: clients send small frames; this caps what a
    flooding client can park in the server's receive queue.
  - `net.ipv4.tcp_notsent_lowat = 131072`: keeps unsent data small so backpressure reaches
    the connection task sooner.
  - `net.core.somaxconn = 4096` and a listen backlog to match, for reconnect waves
    (5,000 clients reconnecting after a deploy).
  - Leave `tcp_mem` alone (global, not namespaced); the memcg limit applies anyway.
  - `ulimit -n` ≥ 16,384 for the runtime process.
  Prefer these sysctls over `SO_SNDBUF`/`SO_RCVBUF` in code: setting the socket options
  disables autotuning for that direction, which hurts fast clients on long links.
- **Application backstop** (not done here): close or resync a connection whose send has
  been blocked for more than a few seconds, so the worst case is bounded by time as well as
  by buffer size.
- Keep permessage-deflate enabled: the plain fallback still retains its largest frame (below).

## Not done / not measured

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
SIZES= STALL=32 ../bench/ws-memory/run.sh target/release/examples/ws_memory /tmp/ws-stall transport on 1000
SIZES= STALL=32 SNDBUF=65536 RCVBUF=65536 ../bench/ws-memory/run.sh target/release/examples/ws_memory /tmp/ws-stall-cap transport on 1000
target/release/examples/ws_memory compress-cost
cargo test --release -p gonvex-runtime --lib connection_future_sizes -- --nocapture
```

`run.sh` starts the server in its own `systemd-run --user --scope -p MemoryMax=2G` so
`memory.stat` is per server, raises `ulimit -n` for the bench processes only, and prints,
per phase, heap / RSS / cgroup `anon`, `sock`, `slab`, `kernel` deltas per connection,
summed `ss -tm` skmem, and live allocations by size class. The runtime mode signs in with
a real native session (password login on a temporary SQLite control plane); no V8 is
needed.
