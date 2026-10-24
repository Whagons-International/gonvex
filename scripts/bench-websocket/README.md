# WebSocket protocol experiments

This is an isolated benchmark package. Its dependencies do not enter a Gonvex SDK tarball or the production runtime. See [the audit and results](../../docs/performance/websocket-protocol.md).

Install the root workspace with its frozen pnpm lockfile and run `pnpm build:packages`. Install this directory's dependencies with `npm ci --prefix scripts/bench-websocket`. Use the checked-in npm and Cargo lockfiles. Chromium must already be installed for Playwright, as in the repository's existing browser benchmark workflow. `GONVEX_BENCH_CHROMIUM` can select an existing executable.

On arch3, follow AGENTS.md and the resource skill. Put dependencies, Cargo output, browser profiles and temporary files on `/mnt/data`, run `build-headroom` before each install/build/render, and run the commands inside a `coach-resources` job. A symlink from this directory's ignored `node_modules` to a dependency installation on `/mnt/data` works.

From the repository root, with `ARTIFACTS` set to an owned absolute output directory:

```sh
node scripts/bench-websocket/fixtures.mjs "$ARTIFACTS"
CARGO_TARGET_DIR="$ARTIFACTS/target" cargo build --release --locked \
  --manifest-path scripts/bench-websocket/rust/Cargo.toml
"$ARTIFACTS/target/release/gonvex-websocket-bench" matrix \
  "$ARTIFACTS/fixtures.json" "$ARTIFACTS/rust.json"
node scripts/bench-websocket/threshold-sweep.mjs "$ARTIFACTS"
"$ARTIFACTS/target/release/gonvex-websocket-bench" boundaries \
  "$ARTIFACTS/sweep-fixtures.json" "$ARTIFACTS/boundaries.json"
"$ARTIFACTS/target/release/gonvex-websocket-bench" boundaries \
  "$ARTIFACTS/fixtures.json" "$ARTIFACTS/guard-encoding.json"
GONVEX_BENCH_BINARY="$ARTIFACTS/target/release/gonvex-websocket-bench" \
  GONVEX_BENCH_NODELAY=1 TMPDIR="$ARTIFACTS" \
  node scripts/bench-websocket/browser.mjs "$ARTIFACTS"
node scripts/bench-websocket/verify-results.mjs "$ARTIFACTS"
node --test scripts/bench-websocket/patch-model.test.mjs
```

Run `browser.mjs` with `GONVEX_BENCH_SMOKE=1` to measure only the small baseline update in memory. Compare `GONVEX_BENCH_NODELAY=0` and `1` to isolate the accepted socket option. Both proxy sockets always use TCP_NODELAY. The full comparison uses TCP_NODELAY on the Rust listener to keep delayed ACK stalls from hiding codec/storage costs.

`GONVEX_BENCH_RESUME=1` continues an interrupted browser run without repeating completed cases. It requires the same fixture hash and records the new run's bundle/binary hashes. Snapshot equality compares data rather than JavaScript object insertion order. `GONVEX_BENCH_WORKLOADS`, `GONVEX_BENCH_STORES` and `GONVEX_BENCH_CANDIDATES` accept comma-separated filters. Candidate names are `variant/codec/compression`. `GONVEX_BENCH_SKIP_CODECS=1` skips the isolated codec loop for targeted pipeline runs.

The Rust matrix uses release builds, the production JSON/DEFLATE versions and the production `gonvex-websocket` crate. It records encoding, compression, Rust decoding, counted allocation calls and peak additional live allocation bytes. DEFLATE uses a fresh 12-bit raw encoder and sync flush with the RFC 7692 trailer removed, exactly as production does. Codec state allocation is included. Zstd dictionary training uses separate generated records; its 4 KiB distribution cost is excluded from per-message sizes.

The matrix uses protocol-shaped `serde_json::Value` frames. The threshold sweep separately uses the actual typed `ServerMessage` serializer and the production compact encoder. Its envelope field order can change compressed sizes by a few bytes. Boundary tests are necessary: naive omission can move a message below the compression threshold and increase transmitted bytes. The production guard retains the legacy frame for that transition and reuses the selected JSON for sending.

The browser runner opens an owned loopback Rust server and uses native Chromium WebSockets, native permessage-deflate, and the real LocalReplica with either memory or IndexedDBEngineReplicaStorage. Every trial first asserts an empty replica, then starts timing after seeding revision 1. Cleanup discovers and deletes the actual engine database, including its namespace suffix, rather than deleting the caller prefix. It includes Rust encoding, compression, sending, browser decompression/decoding and durable application. Untimed fixture setup and database cleanup occur between trials. A TCP proxy counts actual compressed server WebSocket frames. The verifier compares those bytes against the Rust matrix, excluding benchmark commands, completion trailers, handshakes and TCP/IP/TLS overhead.

Browser codec tests include UTF-8 conversion, dynamic protobuf Value conversion and eager FlexBuffers materialization. The lazy FlexBuffers probe reads only an envelope field and is explicitly incomparable to full decoding. Failed codecs are recorded, not silently replaced with easier input. MessagePack tests also cover signed/unsigned 64-bit boundaries, null and binary data.

Snapshots stage decoded rows in JavaScript, then activate through LocalReplica. Canonical-row snapshot experiments also parse those strings in JavaScript. This does not emulate native direct storage ingestion. The expired-cursor fixture reuses the complete snapshot payload and is included in the Rust matrix. A real reconnect handshake and replay/resume SQL are not benchmarked.

There are 100 measured trials plus five warmups for small/100-row updates, 30 trials for 1,000-row updates, and eight trials for 10k-row snapshots. Every case has five warmups. The recorded run used 100 trials for the memory 1,000-row case before adopting the smaller expensive-write sample count. Snapshot p95/p99 are maxima of a small sample, not reliable tail estimates. CDP reports renderer task duration, GC events, sampled heap and retained heap after forced collection. CPU/GC totals include untimed setup and cleanup. Sampled peak heap is a lower bound and not an allocation count. These are single-machine synthetic experiments, not production capacity or WAN latency claims.

`patch-model.mjs` is an experiment, not a supported protocol. Its seeded model tests exercise nulls, explicit deletion, duplicate delivery, ordering and missing-base recovery. Shipping it also needs persisted per-field authority, epoch/scope binding, projection changes, permission changes, crash/multi-tab races and atomic full-row repair. It is not imported by production code.
