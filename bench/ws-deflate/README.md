# Runtime WebSocket compression benchmark

This compares JSON WebSocket messages with and without RFC 7692 permessage-deflate. The fixture comes unchanged from PR #118, `perf/sdk-binary-replica` at `d99b60092c9082648a8483e65fdd5d2650354d11`, originally `packages/client/bench/wire-codec/fixture.mjs`. It contains 20,000 task rows with 50 mixed columns in 128-row pages, a 1,000-row transaction, and one delta.

The release-mode Rust example uses the exact `gonvex-websocket` transport called by the runtime's `/ws` handler. It preloads serialized JSON. It does not exercise database queries, visibility filtering, application reducers, or JSON serialization. Those are identical on both paths. The separate memory script runs the complete `gonvex-runtime` binary and opens 1,000 unauthenticated sockets per trial, with the auth timeout raised locally and the pending connection limit raised to 2,000. No live database is used.

## Reproduce

From the repository root, install the locked JavaScript dependencies with `pnpm install --frozen-lockfile`. The root already declares Playwright and `ws`. Install the Playwright Chromium binary if it is missing.

```sh
export TMPDIR=/home/gabriel/.cache/sdk-wave1/tmp
mkdir -p "$TMPDIR"
export CARGO_BUILD_JOBS=2
CARGO_TARGET_DIR=rust/target-websocket cargo build --manifest-path rust/Cargo.toml -p gonvex-websocket --release --example ws_deflate
node bench/ws-deflate/run.mjs
cargo build --manifest-path rust/Cargo.toml -p gonvex-runtime --bin gonvex-runtime
node bench/ws-deflate/memory.mjs
node bench/ws-deflate/active-memory.mjs
node bench/ws-deflate/probe.mjs
```

`BENCH_BINARY` and `RUNTIME_BINARY` can override the two executables. The benchmark writes `results.json`; the full runtime memory script writes `runtime-memory.json`. The probe prints handshake metadata, without authenticating, and can be redirected to `live-handshakes.jsonl`.

## Measurements

- Wire bytes are observed TCP bytes received by Node `ws`, excluding the HTTP upgrade but including WebSocket headers and the final `done` marker. They exclude TCP/IP/TLS overhead. Each workload is transferred on an already-open connection. The browser also uses an already-open connection. The browser receives text and calls native `JSON.parse`; JavaScript never decompresses anything.
- Server CPU is the single-thread example process's Linux `schedstat` running time, divided by decimal MB of original JSON input. It includes framing, copying and compression. It excludes the Node client process. The plain path gives the non-compression baseline. Tiny delta measurements are dominated by fixed per-message costs, so they are not a compressor throughput estimate.
- Chromium time runs from sending the workload request to receiving and parsing every data message and its completion marker. Every iteration checks the expected row count. This includes server compression, loopback delivery, native browser decompression, event dispatch, and JSON parsing. It does not measure pure decoding or peak client heap.
- Each workload has three warmups per mode and 30 paired samples, alternating mode order. Mean CPU and latency CIs use Student's t with 29 degrees of freedom. The relative latency CI uses a paired percentile bootstrap with 10,000 resamples and a fixed seed. The snapshot gate requires at least 50% fewer bytes and the upper 95% CI of latency increase to be at most 10%.
- Full-runtime memory uses five fresh processes per mode, alternating trial order. RSS is sampled after startup and after 1,000 completed upgrades. CIs use Student's t with four degrees of freedom. RSS includes allocator overhead and all runtime connection state; it excludes kernel TCP buffers and the separate Node client. It measures idle sockets after the initial `session.ready` frame, not active snapshots or a universal hard per-connection ceiling. The codec windows and input/output message limits provide the bounds for active traffic.

These are loopback results from a shared Linux workstation, not a WAN or mobile battery benchmark. Raw browser and CPU samples, configuration, CPU model, and a SHA-256 of the optimized binary are recorded in `results.json`.

## Live path, checked 2026-10-14 UTC

A real Node `ws` handshake requesting permessage-deflate returned HTTP 101 on both endpoints. Staging returned no `Sec-WebSocket-Extensions`; production returned `permessage-deflate; server_no_context_takeover; client_no_context_takeover`. See `live-handshakes.jsonl`.

Read-only Cloudflare DNS inspection found `proxied=false` for both runtime A records. Cloudflare's HTTP proxy is therefore outside these two current paths. Read-only Coolify inspection found the staging and production runtime applications on their expected branches, both using `Dockerfile.runtime`, with Traefik upgrade routing and no configured extension-header rewrite. Production's observed handshake demonstrates extension pass-through on its existing path. Staging's missing response does not by itself prove whether the request extension header reaches the origin; the Rust origin code also lacks compression support.

No infrastructure change was made. No proxy change is indicated by the current configuration. After deploying the runtime, repeat the staging probe to verify the negotiated extension end to end. Traefik's ordinary HTTP gzip middleware is separate from permessage-deflate. If DNS proxying changes later, repeat the same handshake check through that new path.

The RFC negotiation and bounded incoming-message decoding are implemented by [ws-pmd](https://github.com/odedlaz/ws-pmd-rs), with [Ratchet](https://github.com/swimos/ratchet) handling WebSocket framing. Outgoing messages use flate2 with the same zlib-rs backend. Both codecs are allocated only while processing a compressed message and dropped before returning. Messages below 1 KiB never allocate a compressor; uncompressed client frames never allocate an inflater. The transport rejects an agreement unless both directions disable context takeover. Compression is negotiated by the optional standard HTTP extension header before auth, not by a new Gonvex message or SDK capability. The application protocol and client contract remain unchanged.

## Round two measurements

Round-one raw measurements are preserved in `round-1-results.json` and `round-1-runtime-memory.json`. They showed an extra 95.86 MB RSS per 1,000 negotiated idle sockets, which blocked merge. Those codecs reset their histories but retained backend allocations. The new connection extension holds only numeric settings and a fragmentation flag, at most 48 bytes on the tested architecture. Deflate/inflate state lives only for the message being processed.

The existing backend already used zlib-rs, and round one already used compression level 1. `level-1.json`, `level-2.json` and `level-3.json` record a release transport sweep with 30 paired samples per workload. To reproduce a level, change `COMPRESSION_LEVEL` in `rust/crates/websocket/src/lib.rs`, rebuild the release example, then run `BENCH_CPU_ONLY=1 BENCH_LEVEL=<compiled level> BENCH_OUTPUT=bench/ws-deflate/level-<compiled level>.json node bench/ws-deflate/run.mjs`. `BENCH_LEVEL` labels the result; it does not change the compiled compressor. Restore level 1 and rebuild before the full browser and memory runs.

| Level | Snapshot compressed wire bytes | Savings | Server CPU ms / original JSON MB |
| --- | ---: | ---: | ---: |
| 1 | 4,892,096 | 85.10% | 1.295 |
| 2 | 3,050,843 | 90.71% | 1.722 |
| 3 | 2,948,552 | 91.02% | 2.940 |

Level 1 remains the choice for server CPU. Level 2 saves another 1.84 MB per snapshot but costs about 33% more CPU. Level 3 saves only another 102 KB while costing about 71% more CPU than level 2. No new compression backend or shared pool is introduced. Allocation on each message has a CPU cost, which is measured rather than assumed away.

The active-memory script uses five fresh release transport processes per mode and 32 concurrent snapshot clients. It samples `/proc/<pid>/status` approximately every 1 ms during transfers and again 100 ms after completion, while keeping all sockets open. It checks 20,000 received rows per client and records the binary hash and sample counts. This measures server process RSS above the already-open idle connections, excluding the fixture baseline, kernel buffers and client memory. Sampling can miss brief peaks. The example is single-threaded, so 32 concurrent transfers do not mean 32 compressors run simultaneously. Retained allocator memory and transport buffers remain visible even after codecs are freed. This is a measured workload, not a universal active-socket ceiling.

## Round two level 1 results

The repeat run passes the snapshot gate, including its conservative upper-CI check. A failed browser gate sets a nonzero benchmark exit status. The 1 KiB threshold and negotiated windows are unchanged. Raw samples, binary hash and platform metadata are in `results.json`.

| Workload | Plain wire bytes | Compressed wire bytes | Savings | Chromium plain mean ms, 95% CI | Chromium compressed mean ms, 95% CI | Relative latency %, 95% CI |
| --- | ---: | ---: | ---: | --- | --- | --- |
| snapshot | 32,833,978 | 4,892,096 | 85.10% | 1597.200, 1573.940 to 1620.460 | 1562.103, 1541.634 to 1582.573 | -2.197, -3.517 to -0.818 |
| transaction | 1,718,757 | 253,319 | 85.26% | 104.087, 96.404 to 111.769 | 103.657, 94.613 to 112.701 | -0.413, -12.104 to 13.368 |
| delta | 1,753 | 524 | 70.11% | 20.553, 12.901 to 28.205 | 20.620, 12.983 to 28.257 | 0.324, -49.078 to 97.973 |

| Workload | Plain server CPU ms / original JSON MB, 95% CI | Compressed server CPU ms / original JSON MB, 95% CI |
| --- | --- | --- |
| snapshot | 0.313, 0.303 to 0.323 | 1.315, 1.265 to 1.365 |
| transaction | 0.484, 0.450 to 0.519 | 1.647, 1.582 to 1.711 |
| delta | 26.705, 21.173 to 32.237 | 41.661, 32.897 to 50.425 |

Round-one snapshot compressed CPU was 1.228 ms per JSON MB. The repeat run is 1.315 ms, with a 95% CI of 1.265 to 1.365. The measured increase is about 7%, so this is a memory improvement with a CPU tradeoff. Both measurements include transport work and were made on a shared workstation. Incoming compressed messages temporarily reconstruct ws-pmd codecs because its public API only builds encoder/decoder pairs from a consumed negotiation. The unused encoder is dropped immediately, and the decoder is dropped after bounded inflation. This adds work only for compressed incoming messages; typical small uncompressed requests bypass it.

| 32 concurrent snapshot clients | Sampled peak RSS above idle, MB | Retained RSS above idle 100 ms later, MB |
| --- | ---: | ---: |
| plain | 7.516 | 7.516 |
| compressed | 9.273 | 0.757 |

All five active trials in each mode observed the same above-idle values, so their sampled mean CIs collapse to those values. Sampling uncertainty and allocator behavior still apply; these are not exact peaks or a hard memory bound. The compressed path adds 1.769 MB to the sampled peak for this workload and retains 6.746 MB less after transfer. No round-one active measurement was recorded.

## Round two full runtime idle memory

Five fresh runtime processes per mode opened 1,000 sockets each and verified HTTP upgrades, negotiated extension presence and `session.ready`. Decimal MB below are incremental process RSS per 1,000 sockets; numerical KB per socket are identical. The script now records the runtime binary hash as well as raw trials.

| Mode | Round one MB / 1,000 idle sockets, 95% CI | Round two MB / 1,000 idle sockets, 95% CI |
| --- | --- | --- |
| plain | 175.384, 175.197 to 175.572 | 175.631, 175.208 to 176.054 |
| compressed | 271.240, 270.782 to 271.697 | 48.104, 47.807 to 48.402 |
| compressed minus plain | 95.855, 95.365 to 96.346 | -127.527, -127.904 to -127.149 |

There is no positive idle RSS overhead relative to the plain transport in this measurement. The negotiated path now uses less memory because its framing/reader buffers differ from axum, and codecs no longer dominate that comparison. This is not a promise of zero total socket memory or immediate allocator return to the OS after every active message. The active-transfer samples above show both transient memory and retained allocator/buffer memory. Server CPU remains higher than uncompressed transport. Old clients retain the same uncompressed axum path, so none of their behavior or allocation changes.

No Whagons code change is needed. After runtime deployment, run `node bench/ws-deflate/probe.mjs` again to confirm the staging route negotiates the extension. No infrastructure writes, SDK protocol changes, client-contract bump, package publication or merge are included.


## Round three supply-chain removal

`ws-pmd` has been removed from the manifest and lockfile. The websocket crate now owns RFC extension parsing, offer selection and bounded raw inflation. Codec allocation remains per message. The outgoing compressor, level, threshold and no-context-takeover policy are unchanged. See [transport compatibility](transport.md) for the axum/tungstenite-to-Ratchet change, the reasons for it, paired transport tests and known wire differences.

Round-two raw data is preserved in `round-2-results.json` and `round-2-runtime-memory.json`. `results.json` and `runtime-memory.json` contain the round-three rerun with binary hashes. Chromium is still 149.0.7827.55, with three warmups and 30 paired samples per workload. Full-runtime memory still uses five fresh processes per mode and 1,000 verified upgrades/session.ready frames per trial.

| Measurement | Round two | Round three |
| --- | ---: | ---: |
| Snapshot plain / compressed wire bytes | 32,833,978 / 4,892,096 | 32,833,978 / 4,892,096 |
| Transaction plain / compressed wire bytes | 1,718,757 / 253,319 | 1,718,757 / 253,319 |
| Delta plain / compressed wire bytes | 1,753 / 524 | 1,753 / 524 |
| Chromium snapshot plain / compressed mean ms | 1597.200 / 1562.103 | 1553.040 / 1503.807 |
| Chromium snapshot relative time, 95% CI | -2.197%, -3.517 to -0.818 | -3.170%, -5.411 to -0.767 |
| Compressed snapshot CPU ms / JSON MB, 95% CI | 1.315, 1.265 to 1.365 | 1.208, 1.198 to 1.218 |
| Plain idle RSS MB / 1,000 sockets, 95% CI | 175.631, 175.208 to 176.054 | 175.634, 175.359 to 175.909 |
| Negotiated idle RSS MB / 1,000 sockets, 95% CI | 48.104, 47.807 to 48.402 | 47.900, 47.581 to 48.219 |
| Negotiated minus plain idle RSS MB / 1,000, 95% CI | -127.527, -127.904 to -127.149 | -127.734, -128.174 to -127.293 |

Wire bytes are identical and the Chromium snapshot gate passes. Full-runtime idle RSS shows no regression. CPU and browser timing vary on this shared workstation; these samples do not establish that the parser replacement itself improves outgoing CPU. The benchmark's ordinary small requests do not exercise compressed incoming inflation, so it does not quantify the removal of per-message negotiation and the unused encoder on that path. Active-transfer RSS and compression-level sweeps were not repeated because the outgoing codec policy is unchanged; their round-two data remains above.
