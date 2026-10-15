# Dedicated engine browser benchmark

Run one browser at a time with production-built assets and persistent profiles. The fixture has 50,000 task rows with 50 columns, five related tables, a 250-row mounted view, and 1,000 exact-ID subscriptions. Both modes use a 200 MiB resident budget and unlimited resident row count. Row bytes, browser version, source revision, hardware, load observations, and individual command latencies are recorded.

```sh
PIPELINE=1 START_LOAD_LIMIT=.35 LOAD_LIMIT=.5 WAIT_LOAD=1 BROWSER=chromium WARMUPS=1 ITERATIONS=4 \
  RESULTS=round2-final-chromium-pipelined.json node packages/client/bench/engine-worker/run.mjs
node packages/client/bench/engine-worker/summarize.mjs \
  round2-final-chromium-pipelined.json round2-final-chromium-pipelined-summary.json
node packages/client/bench/engine-worker/bundles.mjs
```

Defaults remain five warmup pairs and 20 measured pairs. `WARMUPS`, `ITERATIONS`, `BROWSER`, `RESULTS`, `PORT`, and `PROFILE_SET` select a run. Browser order alternates within each pair. `SEED_ROWS=1000 SKIP_SNAPSHOT=1 SKIP_DRAIN=1` selects a parity smoke run. `PROFILE_SET` selects the directory under TMPDIR; profiles and built files do not enter the repository.

`KEEP_EDITS=1` retains the Round 1 workload, where synthetic edits accumulate across modes and pairs. Without it, preparation clears synthetic edits before each mode, outside measurement, while retaining the task fixture. The output identifies which workload ran. Compare legacy and worker within the same run; the different preparation policies make absolute results across these policies unsuitable as a before/after comparison.

The four workloads are cached cold paint, a 1,000-row burst, a 20,000-row paged snapshot, and admission/drain of 500 queued intents. The local WebSocket fixture models 50 ms auth/reducer RTT and paces snapshot pages at 20 Mbit/s. The scheduler chooses the actual batch sizes. Each drain records wire frame counts and batch sizes, so causal barriers remain observable. These timings do not represent a deployed Rust runtime or device battery use.

Admission defaults to sequential awaited calls. `PIPELINE=1` issues all 500 promises together and lets negotiated RPC batches group up to 1,024 calls per packet when negotiated, with a 128-call fallback for earlier batching hosts. Both modes disable per-call timeouts for this large benchmark queue. The recorded individual latency runs from calling reducer to its durable acknowledgement. In a pipelined run this includes waiting behind earlier commands, so its latency distribution differs from sequential admission.

`PROFILE_IO=1` enables optional instrumentation of native IDB store/index requests and worker admission RPCs. Use a separate run for these counts; instrumentation changes timings. `ENGINE_REVISION=310d3a4 PROFILE_SET=<separate-profile>` loads the Round 1 client sources into the same fixture for a controlled diagnostic. The extra benchmark read/lifecycle helper stays outside the sampled SDK. IDB request times measure the first native response, overlap, and do not add up to wall time. Worker admission duration excludes UI transport time; subtracting its mean from the UI mean estimates the remaining transfer and scheduling overhead.

The bundle script compares identical dependency builds against the settled `11820f6` baseline and recorded `origin/staging`. It verifies the default minified asset's SHA-256, checks gzip growth, and reports opt-in entry/chunk sizes. Its worker entry excludes profiling code.

Cold sampling ends after the cached visible rows paint. The runner samples CDP around `start()` and then calls `prepare()` outside that interval to register the exact-ID interests, connect/authenticate, and wait for source readiness. New runs record `coldSamplingAligned: true`. Historical samples in `results.json` used different cold script and wall intervals, so their cold script medians and ratios are invalid and omitted from `summary.json`. Their raw values remain for audit; the corrected harness needs a new full measurement run.

The harness checks load before each mode, every ten seconds during long workloads, and after each workload. It rejects a complete pair when one-minute load exceeds half the logical CPU count. LOAD_LIMIT selects a stricter fraction when needed. START_LOAD_LIMIT can reserve headroom before a pair without changing the acceptance cutoff during measurement. WAIT_LOAD=1 waits and repeats that pair; otherwise it stops and records a failure. Failed/partial runs are diagnostic artifacts. Complete runs report 10,000 deterministic bootstrap samples, both independent median ratios and paired median ratios. The drain gate requires the paired 95% upper bound to be at most 1. Chromium's cold-script gate requires at least 40% reduction at that bound.

Chromium uses CDP ScriptDuration. RAF intervals and scheduled click-to-two-RAF paint timings are available across browsers. Long-task and script metrics are explicitly unavailable when the browser lacks the corresponding API; an empty observation array does not establish a pass.

Firefox 151 / Playwright 1.61 has an instrumentation assertion reproducible with a standalone worker WebSocket and no SDK. The Firefox-only loader workaround skips socket-open events whose page request/response records are missing. It leaves tracked page events intact, does not change network behavior, and does not modify installed Playwright files. The patch checks the exact source fragment and fails if it needs review after a Playwright update.

All browsers run the reducer network-denial, durable crash/retry, mid-batch worker termination with original command IDs, private reconnect, and two-tab fallback fixtures. WebKit is a full desktop benchmark, not Safari/iPhone suspension QA. First install, dependent-chain performance, worker CPU/heap, and device energy remain separate work.
