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

The RFC negotiation is implemented by [ws-pmd](https://github.com/odedlaz/ws-pmd-rs), with [Ratchet](https://github.com/swimos/ratchet) handling WebSocket framing. Compression is negotiated by the optional standard HTTP extension header before auth, not by a new Gonvex message or SDK capability. The application protocol and client contract remain unchanged.

## Recorded results

The snapshot gate passed, including its conservative upper-CI check. Runtime compression defaults to enabled when offered. `GONVEX_WS_COMPRESSION=false` disables it; `GONVEX_WS_COMPRESSION_THRESHOLD=1024` is the default minimum message size.

| Workload | Plain wire bytes | Compressed wire bytes | Fewer bytes | Chromium plain mean, 95% CI | Chromium compressed mean, 95% CI | Relative latency, 95% CI |
| --- | ---: | ---: | ---: | --- | --- | --- |
| 20k-row paged snapshot | 32,833,978 | 4,892,096 | 85.10% | 1581.35 ms, 1548.98 to 1613.71 | 1523.59 ms, 1496.28 to 1550.90 | -3.65%, -5.35% to -1.82% |
| 1,000-row transaction | 1,718,757 | 253,319 | 85.26% | 102.05 ms, 91.63 to 112.47 | 92.22 ms, 84.58 to 99.86 | -9.64%, -22.15% to +5.50% |
| Single-row delta | 1,753 | 524 | 70.11% | 20.59 ms, 12.89 to 28.29 | 20.56 ms, 12.89 to 28.24 | -0.13%, -49.53% to +98.07% |

The delta is too noisy to support a latency claim. Small-message completion-marker and TCP delayed-ACK costs dominate this loopback measurement. No delta CPU or latency gate is claimed.

| Workload | Plain server CPU ms / JSON MB, 95% CI | Compressed server CPU ms / JSON MB, 95% CI |
| --- | --- | --- |
| Snapshot | 0.256, 0.250 to 0.262 | 1.228, 1.219 to 1.237 |
| Transaction | 0.370, 0.342 to 0.399 | 1.447, 1.393 to 1.502 |
| Delta | 18.470, 15.960 to 20.979 | 26.962, 23.024 to 30.900 |

For an output-byte denominator, snapshot compression uses 8.24 ms of server CPU per compressed wire MB, with a 95% CI of 8.18 to 8.30. The benchmark shifts CPU work to the server; it does not claim reduced server CPU.

The complete runtime, across five fresh-process trials of 1,000 idle sockets each, uses an additional 175.38 KB per plain connection, with a 95% CI of 175.20 to 175.57, versus 271.24 KB per compressed connection, with a 95% CI of 270.78 to 271.70. The paired increase is 95.86 KB per connection, with a 95% CI of 95.37 to 96.35, or about 95.9 MB per 1,000 sockets. These are decimal KB/MB and incremental process RSS after receiving `session.ready`. Kernel buffers are excluded.

The memory overhead remains finite with fixed codec windows and no context takeover, but it is real. Capacity planning must include it before enabling compression on very large fleets. Active-message buffers are separately bounded by the existing frame and expanded-message limits; this idle test does not measure their peak.
