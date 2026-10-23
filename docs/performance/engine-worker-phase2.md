# Dedicated client engine, Phase 2

The opt-in `@gonvex/client/engine` factory hosts the settled Gonvex client in a dedicated worker. WebSocket transport, server JSON decode, integrity verification, canonical IDB storage, subscription evaluation, portable reducers, and the existing outbox scheduler run there. The default `GonvexClient` keeps its synchronous replica API.

This implements the Phase 2 row in [the merged design](client-sync-engine.md). Its audit at `95da6c9` identified transport JSON decode on the caller's thread, scope hydration/decode, and the distinction between a reducer-only worker and a socket-owning sync engine. This branch starts at the settled `11820f6` hot paths; those earlier optimizations are retained rather than reimplemented.

The UI retains mounted results and shared projected rows. Unmount removes their row and negative-entry metadata. Updates use changed-field patches when small and decoded, acknowledged packets otherwise. A committed watch batch completes its result/cache swaps before notifying any subscriber. Large explicitly mounted results remain large; this phase does not introduce pagination or silently trim full-result hooks.

## API and integration

Generation retains `localRuntime.create()` and adds `gonvex-engine.ts`, `gonvex-engine-worker.ts`, and a static portable reducer binding. Use the bundler-generated factory so the worker and its dependencies remain separate assets.

```ts
import { createEngineClient } from "@gonvex/client/engine";
import { createEngineWorker, engineWorkerUrl, engineManifest } from "./gonvex/_generated/gonvex-engine";
import { localRuntime } from "./gonvex/_generated/local-runtime";

const client = await createEngineClient(existingWebSocketUrl, {
  engine: {
    workerUrl: engineWorkerUrl,
    workerFactory: createEngineWorker,
    artifactHash: engineManifest.artifactHash,
  },
  auth: existingAuthOptions, // includes the active project, tenant, account identity and UI fetchToken
  fallbackBinding: localRuntime,
  storage: {
    replicaName: existingReplicaName,
    outboxName: existingOutboxName,
    contract: existingClientContract,
    initialVersion: existingInitialVersion,
  },
  residency: existingReplicaResidency,
});
```

`GonvexProvider`, watch-based hooks, reducer/action/query hooks, and managed auth providers accept the engine client. `useEntityState` distinguishes pending from a completed lookup; `useEntity` retains its undefined loading result. Retained/Live Query state has an explicit optional `pending` flag. Imperative worker reads use `await client.readEntity(...)` or `readEntities(...)`. `useGonvexEngineClient` exposes the engine API. Direct synchronous `localReplica` and legacy `useGonvexClient` consumers require an explicit migration.

Provider token fetching stays on the UI and crosses only the private endpoint. Discovery messages contain no credentials. Engine initialization validates the RPC protocol and reducer artifact; commands are bound to the authenticated journal scope. A scope/account change clears UI results before accepting new data. Same-account refresh retains results.

For application retries, keep the command or use the error's retained request:

```ts
const command = client.createCommand();
await client.reducer(ref, args, { engineCommand: command });
// Reuse command after reopening; it includes timestamp, entropy, artifact and scope.
// For RPC timeout/unavailability:
await client.retryReducer(error);
```

The original outcome persists as flat metadata with the intent before its prediction publishes. Recovery also understands the earlier nested outcome format. Retirement writes a recovery receipt before removing that intent. Failed receipt persistence leaves the canonical entry intact. Control-plane credentials and online-only outcomes are not copied into that receipt store. Server replay and the existing durable checkpoint retire predictions; no new scheduler or wire codec is introduced.

## Rollback and limits

Participating tabs hold Web Locks presence registrations. A second foreground participant makes both clients settle outstanding RPCs, close the worker, and use the legacy host against the same IDB journal. The worker holds a separate exclusive ownership lock before opening storage. This is presence/fallback, not follower sharing or leader handoff. Older SDK tabs do not implement this presence protocol; existing IDB fences and peer synchronization remain in force.

To turn the opt-in off, await `client.close()` before constructing the existing legacy client with the same stores and contract. A crash releases worker ownership; `recover()` reloads the canonical journal. Reducer RPC timeouts abort the worker and preserve the original command identity for retry. A worker initialization timeout reports failure rather than replaying potentially blocked code on the UI. Browser suspension can delay recovery and cutover.

Custom adapters/executors, SQLite, multi-tab engine sharing, paged UI hooks, native migration, and new server capabilities are outside this phase. Existing storage migrations must be statically supplied to a custom worker entry. No Whagons files, application contract, runtime protocol, package version, or published package changed.

## Round 2 validation and measurements

Round 1 reported a Chromium Phase 2 pass; the cold-script claim was later invalidated by the sampling mismatch described below, but its 500-intent wall median regressed from 49,469 to 73,843 ms. Default gzip grew by 590 bytes. Firefox failed in Playwright before validation. The original samples remain in `packages/client/bench/engine-worker/results.json`; these are historical results, not the current gate.

At Round 1 commit `310d3a4`, `engine/receipts.ts:find` performed an additional complete scoped outbox load for every missing recovery receipt. `engine/host.ts` then waited for the entire local admission and an additional receipt write before starting the next command. The canonical entry already contained the outcome. The diagnostic old-source run confirms 500 receipt reads, 500 admission receipt writes, and 1,000 scoped journal reads over 249,500 records for 500 worker calls. Its legacy mode made 500 scoped reads over 124,750 records. The corresponding early Round 2 diagnostic has no receipt reads or writes during newly generated command admission. The old diagnostic UI mean was 25.98 ms versus 25.66 ms inside the host, leaving approximately 0.31 ms for transfer and scheduling; storage and admission serialization dominated. Other shared-store reads vary with in-flight replica work; these diagnostic runs are not headline timing comparisons.

New commands generated by the UI proxy skip recovery lookup. Explicit commands and retries retain lookup, argument checks, original entropy/time, account fencing, and canonical-journal deduplication. Entity reads stay ready through same-account authenticated/restored transitions; account/scope changes and unavailable sessions still force a cold read. Fresh calls start synchronously within a received batch, matching the legacy preparation schedule. Retained command identities also share the batch lease, with their lookup/start order preserved. A start barrier keeps later RPC packets behind a cohort awaiting ownership. Explicit commands use an ordered lookup/start lane that releases before durable acknowledgement so reducer preparation can pipeline. `EngineRpc` groups same-microtask admissions into packets of at most 1,024 commands, with individual request identities, replies and errors. This is enabled only when the host advertises `admissionBatch: 1` in its private init reply. An optional `admissionBatchLimit` advertises the larger limit. Missing limit retains 128-call packets; missing batching support uses individual envelopes. Small RPC replies keep the ordered sender lane and do not wait for their own delivery acknowledgement. They cannot overtake preceding watch batches. Watch events and large packets remain acknowledged. Large text fallbacks retain complete values for older receivers that ignore optional alias metadata; newer receivers restore shared identity. Replies from older hosts without alias metadata are accepted.

Local commands share the existing delivery/local/intent leases. A following awaited RPC can reuse the lease without delaying its durable acknowledgement. Reuse ends after 5 ms idle, 50 ms elapsed enrollment, 1,024 accepted commands, or a control operation. Already accepted commands finish before release. The journal cache reloads once at that ownership transfer, rather than once per command. An explicit retry already awaiting lookup prevents this optimization so later fresh calls cannot jump it. Each admission remains serialized inside the lease and receives its own durable acknowledgement. The established local admission, storage fences, causal dependency scheduler, `reducer.callMany`, and authoritative replay remain in use. Counter reservation and canonical intent insertion still persist per command before its prediction or acknowledgement. Retirement reuses a durable local outcome already in the fenced outbox cache, including the single-ack path that removes its memory entry before calling the store. The temporary reference lasts only until that deletion settles. Remote outcomes and cache misses still read canonical storage. Receipt persistence completes before the original canonical deletion. This change batches retirement receipt writes per tick, rather than staging dependent predictions before a bulk canonical commit. That retains the existing atomic admission semantics and older SDK coordination.

Recovery receipts are capped at 1,024 retired commands across the journal's scopes and expire after 30 days. A private receipt-store upgrade prunes existing unbounded history atomically before updating the retained records. Ongoing pruning frees up to 64 older receipts at a time, keeping the hard cap while avoiding a prune on every server acknowledgement. Expired records are checked on activity at least daily, and lookups enforce the 30-day limit immediately. One durable timestamp floor prevents a pruned outcome from silently becoming a second admission. Explicit retries outside retained recovery history fail closed; outstanding canonical intents retain their original outcomes without that limit. A newly generated command can still be admitted at the same timestamp as a pruning boundary. Clock changes can conservatively reject an ambiguous explicit retry. No application contract or server protocol version changes.

The default minified library asset is byte-identical to the settled `11820f6` baseline, including its SHA-256. The engine-only connection, admission, cold-read and close hooks live behind the opt-in entry. Default runtime code and storage adapters no longer carry the Round 1 engine branches. The current staging comparison includes the later optional membership-store work; its larger size is not an optimization attributable to Round 2.

| Library asset | Minified bytes | Gzip bytes |
| --- | ---: | ---: |
| Default, settled baseline | 350,383 | 98,587 |
| Default, Round 2 | 350,383 | 98,587 |
| Default, recorded staging `1f2a649` | 377,362 | 103,651 |

The hash is `f31f43b017247a5a6977f244267dccf315e2df32b365b8a85e51de2f6680df0b`. Opt-in entries and shared chunks are reported in `bundle-sizes.json`. The bundle guard verifies equality and rejects gzip growth. These are library sizes with identical dependency builds, not a Whagons application build.

The corrected Chromium sequential run at `b6e020d` completed one warmup pair and four measured alternating pairs. It passed cold-script, burst and drain gates. Maximum accepted normalized load was 0.291; an overloaded complete pair was excluded. The later remote rejection fixes do not change this local-success path. Retirement reread removal at `f0461b4` is being revalidated separately.

| Chromium workload | Legacy → worker main script median | Legacy → worker wall median | Paired ratio, bootstrap 95% CI |
| --- | ---: | ---: | --- |
| Cached cold paint | 334.6 → 8.1 ms | 2,415 → 2,468 ms | Script 0.024 [0.021, 0.029] |
| 1,000-row burst | 73.8 → 24.0 ms | 1,542 → 1,550 ms | Script 0.324 [0.284, 0.380] |
| 20k-row snapshot | 574.4 → 4.4 ms | 22,562 → 22,529 ms | Script 0.008 [0.006, 0.013] |
| 500 sequential intents | 1,161.6 → 477.8 ms | 42,085 → 28,851 ms | Wall 0.684 [0.632, 0.710] |

Sequential durable acknowledgement latency is 37.0 → 4.7 ms median and 55.2 → 47.6 ms p95. The median admission phase is 18,176 → 6,668 ms. Burst click-to-two-RAF p95 is 29.3 → 33.2 ms; no input-latency improvement is claimed. Worker maximum observed burst long task is 0 ms.

The pipelined Chromium run at `6e58ccc` completed one warmup pair and two measured pairs, with all three gates passing. Drain median is 47,380 → 23,176 ms; paired wall ratio 0.468 [0.468, 0.489]. Admission phase is 23,780 → 2,473 ms. Individual queued acknowledgement latency is 11,423 → 832 ms median and 23,516 → 2,283 ms p95. These latencies include waiting behind earlier pipelined commands. Maximum accepted load is 0.498. The smaller pair counts limit statistical power; defaults remain five warmups / 20 measured pairs.

Before the retirement reread fix, Firefox completed all four workloads, crash/retry, private reconnect and two-tab fallback in two alternating pairs. Its drain gate failed: 32,504 → 33,283 ms median, paired ratio 1.024 [1.024, 1.067]. This led to removing unnecessary local retirement journal reads. The Firefox harness assertion is resolved. Firefox and WebKit do not expose Chromium's CDP script metric or Long Tasks API; those gates are unavailable rather than passing on empty arrays.

## Final serial validation attempt, 2026-10-14

The checkout was pulled with `git pull --ff-only` and remained at `3e5c3d2`, including `f0461b4`. Product and harness code were unchanged. Chromium ran five warmup pairs and ten of the requested twenty measured alternating pairs. Fourteen attempts were excluded for load. The three-hour cumulative load-wait budget expired before Chromium completed, so Firefox and WebKit were not started. This attempt is **NOISY**, and the PR is **not merge-ready by its final gates**. Numeric gates computed from the accepted Chromium subset pass, but that subset does not replace the required complete run or its final two-tab fixture.

| Browser | Measured pairs | Cold script | Burst long task | Drain | Final verdict |
| --- | ---: | --- | --- | --- | --- |
| Chromium 149.0.7827.55 | 10 / 20 | PASS on accepted subset | PASS on accepted subset | PASS on accepted subset | NOISY, incomplete |
| Firefox | 0 / 20 | Unavailable | Unavailable | Not measured | NOISY, not started |
| WebKit | 0 / 20 | Unavailable | Unavailable | Not measured | NOISY, not started |

The following Chromium medians and paired bootstrap 95% confidence intervals describe only the ten accepted pairs. Ratios are worker / legacy.

| Workload | Legacy → worker script median | Legacy → worker wall median | Paired script ratio, 95% CI | Paired wall ratio, 95% CI |
| --- | ---: | ---: | --- | --- |
| Cached cold paint | 363.2 → 12.0 ms | 2,639 → 2,916 ms | 0.031 [0.028, 0.035] | 1.100 [1.036, 1.184] |
| 1,000-row burst | 73.8 → 23.4 ms | 1,541 → 1,548 ms | 0.327 [0.281, 0.349] | 1.002 [0.999, 1.008] |
| 20k-row snapshot | 643.0 → 9.0 ms | 22,746 → 22,878 ms | 0.014 [0.007, 0.019] | 1.004 [0.992, 1.012] |
| 500 sequential intents | 1434.8 → 557.0 ms | 47,629 → 32,874 ms | 0.378 [0.333, 0.438] | 0.769 [0.679, 0.836] |

Initial `uptime` showed load 16.65 on 16 logical cores. The pre-browser controller saw 12.74 and waited one three-minute interval, then started Chromium at 4.46. The guarded runner retained 35% start headroom and a 50% acceptance cutoff. Maximum accepted load was 7.64, or 0.4775 of cores. Waiting load peaked at 29.38. About 180.5 minutes of cumulative load waiting elapsed before the next monitoring check stopped the run. Process snapshots showed `gabriel` Node, Chrome, compiler and ffmpeg jobs, including Node at 441% CPU and a compiler at 164%. No unrelated process was killed. Only the validation controller and its descendants were terminated at the wait limit.

The Chromium subset passed reducer network denial, durable crash/retry, termination after the first durable acknowledgement in a 32-command batch with the original command IDs, and private reconnect. The final two-tab fixture was not reached. Firefox and WebKit have no timings or paired confidence intervals from this attempt. Existing historical artifacts, including the untracked `round2-final-firefox-full.json`, were preserved.

Raw accepted samples, exclusions, load checks, numeric subset gates and the serial status are in `round2-validation-chromium.json`, `round2-validation-chromium-summary.json`, and `round2-validation-status.json`. Runner and process snapshots are in `round2-validation-chromium.log` and `round2-validation-load.log`. Rerun the same configuration serially on a quiet machine. No application change is required for this validation follow-up. No unit tests, typechecks or builds were rerun for this documentation/results-only change.

```sh
export TMPDIR=/home/gabriel/.cache/sdk-wave1/tmp
START_LOAD_LIMIT=.35 LOAD_LIMIT=.5 WAIT_LOAD=1 BROWSER=chromium WARMUPS=5 ITERATIONS=20 \
  PROFILE_SET=gonvex-final-validation-chromium PORT=48849 RESULTS=round2-validation-chromium.json \
  node packages/client/bench/engine-worker/run.mjs
node packages/client/bench/engine-worker/summarize.mjs \
  round2-validation-chromium.json round2-validation-chromium-summary.json
```

Use fresh result/profile names for the next attempt, and substitute Firefox then WebKit after checking `uptime` before each browser.



The concurrently reviewed correctness fixes exposed a cold-sampling mismatch in the historical runs: wall time stopped at cached paint, while CDP script sampling also included subsequent exact-ID subscription and online preparation. Historical cold script ratios are invalid and their summaries now omit them. The corrected runner calls `prepare()` only after sampling cached paint and marks new runs with `coldSamplingAligned: true`.

Firefox's Playwright 1.61 assertion reproduces with a standalone worker opening a WebSocket and no Gonvex SDK. `_onWebSocketOpened` expects a page request record that worker events do not always provide. The benchmark's Firefox-only loader guard skips only untracked request/response events, preserves tracked page instrumentation, checks the exact patched fragment, and does not edit installed dependencies. No product transport workaround ships.

The machine is shared. A first full attempt crossed the 50%-of-logical-CPUs load cutoff after warmups, before any measured samples. It is archived as `round2-chromium-load-abort.json` and excluded. The final harness checks load before modes, every ten seconds during long workloads, and after each workload. WAIT_LOAD retries an entire excluded pair after load drops; no overloaded mode enters the measured samples. Without it, a partial run records failure.

Focused validation passes 64 client engine tests, four React engine tests and the generator integration test. The tests cover negotiated batching/fallback, reply ordering, pipelining without awaiting the first durable ack, interrupted batches, lost replies, original command IDs on recovery, canonical durability, online server outcomes, retirement races, receipt transaction failures, migration and retention. The browser fixture kills a worker after the first durable acknowledgement in a 32-command batch, then retries all commands with their original identities. Client and React typechecks/builds pass. Generator validation compiles the generated static worker. No full suite, cargo workspace run, npm publish or PR merge was performed.

```sh
pnpm --filter @gonvex/client exec vitest run src/engine/engine.test.ts --maxWorkers=1
pnpm --filter @gonvex/react exec vitest run src/engine.test.tsx --maxWorkers=1
node --test packages/gonvex/test/local-bindings.test.mjs
pnpm --filter @gonvex/client typecheck
pnpm --filter @gonvex/client build
pnpm --filter @gonvex/react typecheck
pnpm --filter @gonvex/react build
node packages/client/bench/engine-worker/bundles.mjs
```

## Final serial validation, 2026-10-17

**Run conditions.** All runs used head `71b48f7`; the gonvex `staging` branch was at `c90765f` and was not merged in. The host was arch3: Ryzen 7 5800X, 16 logical cores, Linux 7.2.4, Playwright browsers.

Each browser ran inside its own capped scope:
- `MemoryMax=3G` and `CPUQuota=300%`;
- `nice 10` and `ionice -c3` (idle-class disk access);
- core dumps disabled.

The browsers ran one at a time. Each started only after these checks passed:
- memory pressure (full avg10) at most 1% and IO pressure (full avg10) at most 5%;
- at least 8 GB of memory available;
- no other heavy work running on the shared sweep slots.

The harness settings were unchanged from the earlier rounds: sequential admission, `START_LOAD_LIMIT=.35`, `LOAD_LIMIT=.5`, `WAIT_LOAD=1`, 5 warmup pairs and 20 measured pairs. Ratios are worker ÷ legacy, as paired medians with 10,000-sample bootstrap 95% confidence intervals.

The CPU cap changes absolute numbers against the earlier uncapped runs. Compare legacy and worker only within the same run.

| Browser | Pairs | Cold wall | Burst wall | 20k snapshot wall | 500-intent drain wall | Verdict |
| --- | ---: | --- | --- | --- | --- | --- |
| Chromium 149.0.7827.55, 50k rows | 20/20, 6 load exclusions | 1.027 [0.880, 1.080] | 0.998 [0.995, 1.001] | 1.001 [0.992, 1.004] | 0.938 [0.783, 0.990] | **PASS**: drain, cold-script and burst gates; correctness; two-tab |
| Firefox 151.0, 50k rows | 20/20, 2 load exclusions | **1.176 [1.039, 1.471]**; p95 5,497 → 13,991 ms | 1.001 [0.994, 1.008] | 0.999 [0.995, 1.011] | 0.955 [0.928, 0.964] | **PASS** on the drain gate; script gates unavailable; correctness; two-tab. **Cold paint regresses.** |
| WebKit 26.5, 50k rows | 0/20 | not measured | not measured | not measured | not measured | **BLOCKED**: per-origin storage quota |
| WebKit 26.5, 25k rows, reduced fixture | 10/20 | 1.062 [0.967, 1.164] | 1.001 [0.999, 1.003] | 1.001 [0.997, 1.007] | 0.976 [0.934, 1.040] | **INCOMPLETE**: the legacy-mode cold paint timed out |

Chromium script time, legacy → worker medians, with the paired ratio:

| Workload | Script time | Paired ratio |
| --- | --- | --- |
| Cold | 337.0 → 8.9 ms | 0.026 [0.025, 0.028] |
| Burst | 61.0 → 22.2 ms | 0.381 [0.351, 0.402] |
| Snapshot | 780.7 → 8.5 ms | 0.011 [0.010, 0.013] |
| Drain | 1,260.4 → 529.8 ms | 0.417 [0.395, 0.434] |

The capped drain improvement (0.938) is smaller than the uncapped 2026-10-14 subset (0.769) and the round-2 run (0.684).

Every browser that reached the fixtures passed every correctness fixture:
- reducer network denial (3 APIs);
- durable crash and retry;
- a 32-command mid-batch termination with a durable prefix of 1 and the same command IDs;
- private reconnect.

Chromium and Firefox both finished the two-tab fixture, and both tabs fell back to legacy as designed.

**Firefox cold paint.** Worker-mode cached paint is slower than legacy on Firefox: median 3,391 → 4,757 ms, p95 5,497 → 13,991 ms. No harness gate covers cold wall time, but this is an open regression for any Firefox opt-in.

**WebKit at 50k rows.** The 50k-row fixture's canonical database grew to 1.16 GB:
- `entities` values took 350 MB in SQLite pages;
- the index tables took 805 MB, mostly the existing `*lookupKeys` multiEntry index, with 1.7M index rows for 50,005 entities.

That exceeds WebKit's per-origin quota of about 1 GB. A `QuotaExceededError` was raised during a **legacy** warmup drain. The index layout predates this PR and is the same on `staging`. Large tenants on Safari face the same limit, which is separate follow-up work.

**WebKit at 25k rows: first attempt.** It stopped during the warmups with `Fixture timed out` in a legacy drain. Two earlier attempts of that pair had been excluded for load. During the failing attempt the one-minute load reached 9.5, above the cutoff of 8, and host IO pressure was high: the IO full average over five minutes was about 36%. With idle-class disk access, the browser's IndexedDB writes can stall for long periods. The harness rethrew the timeout instead of excluding the overloaded pair, so the run aborted.

**WebKit at 25k rows: second attempt.** It ran on a quiet machine (load about 1–4.5) and produced 10 accepted measured pairs. Then legacy cold start in pair 10 did not paint its 250 cached rows within the fixture's 60-second limit, at load 3.6.

The drain gate computed from those 10 pairs is FAIL: the upper bound is 1.040, above the required 1. Drain times also rose late in the run in both modes: legacy p95 was 43.4 s and worker p95 59.6 s, against medians of 35.0 s and 33.8 s. Because the run is incomplete, neither result is a WebKit verdict.

No product code changed. A harness patch was tried locally to exclude workloads that fail while overloaded. It was reverted because its retry path was never exercised.

Raw data:
- `round3-validation-chromium.json` and its `-summary.json`;
- `round3-validation-firefox.json` and its `-summary.json`;
- `round3-validation-webkit-50k-quota-failure.json`;
- `round3-validation-webkit-25k-attempt1-timeout.json`;
- `round3-validation-webkit-25k.json` and its `-summary.json`.

Overall verdict: **not merge-ready**. Chromium passes. Firefox passes its gate but shows a cold-paint regression. WebKit is blocked and incomplete.

## Startup fix and re-validation, 2026-10-19

**Startup changes**

The 2026-10-19 diagnosis found that worker `init` waited for `localSessionReady`, which hydrates the whole local scope, before the UI could register its visible watch. On Firefox at 50k rows that wait was 2.6–3.3 s. Two commits change the startup path:

- `0a85044`:
  - Init now answers immediately.
  - Session transitions reach the UI through the existing `session` event.
  - A failed restore sends `startupError`, which falls back to legacy.
  - The UI keeps a session event that races ahead of the init reply.
  - Host `close` now releases stores, receipts and ownership in `finally`.
- `dc69e75`:
  - The restore's first visibility-scope activation reached the UI after init and superseded RPCs already in flight. WebKit's parity fixture failed on this with "Account or visibility changed while awaiting engine RPC" when running `0a85044`.
  - Only that restore-phase activation is now marked. The UI skips the invalidation for it when its command scope is unchanged.

**Engine tests on `dc69e75`: 68/68 pass.** Each new test fails against the code it guards:
- the race test without the session guard;
- the early-init test with the original host;
- the in-flight reducer test without the restore marker, which reproduces the exact WebKit error.

Client and React typechecks, the client build, and the bundle guard (default asset byte-identical, SHA-256 `f31f43b0…`) pass.

**Benchmark setup**

The workload is unchanged: 50k rows, 5 warmup pairs and 20 measured pairs, sequential admission, `START_LOAD_LIMIT=.35`, `LOAD_LIMIT=.5`. Each browser runs alone in a capped scope (3 GB, 3 CPUs, nice, idle IO) on the shared second build slot. Ratios are worker ÷ legacy, paired, with 95% confidence intervals.

**Round 4: `0a85044` (superseded by `dc69e75`, kept for the record)**

| Browser | Pairs | Cold wall | 500-intent drain wall | Result |
| --- | ---: | --- | --- | --- |
| Chromium 149 | 20/20 | 1.070 [1.043, 1.079] | 0.819 [0.758, 0.929] | Gates pass |
| Firefox 151 | 20/20 | 1.109 [1.051, 1.422] | 0.938 [0.912, 0.953] | Drain gate passes; cold paint still slower |
| WebKit 26.5 | 0/20 | — | — | **FAIL**: the parity fixture hit "Account or visibility changed while awaiting engine RPC". This was the regression fixed in `dc69e75`. |

**Round 5: `dc69e75` (current code)**

| Browser | Pairs | Cold wall | 500-intent drain wall | Result |
| --- | ---: | --- | --- | --- |
| Chromium 149 | **2/20** | 1.049 (n=1) | 0.749 (n=1) | **INCOMPLETE** |
| Firefox 151 | 20/20 | **1.237 [1.113, 1.758]**; p95 5,539 → 8,603 ms | 0.981 [0.953, **1.015**] | **Drain gate FAIL**; cold paint regression persists |
| WebKit 26.5 | 0/20 | — | — | **BLOCKED**: `QuotaExceededError` at 50k rows (existing `*lookupKeys` storage layout); then `Fixture timed out` |

Chromium aborted with `EngineRpcTimeout: listIntents` (20 s) during the pair-1 worker drain. The 1-minute load reached 10.36, above the cutoff of 8. The harness treats any workload error as fatal rather than a load exclusion, so no Chromium verdict exists for this head.

Correctness fixtures pass wherever they ran: denial, crash/retry, the 32-command mid-batch crash, reconnect, and two-tab fallback (Firefox).

**Verdict: not merge-ready.**

- The startup change narrows the Firefox cold-paint gap in some runs (1.109 in round 4) but does not remove it (1.237 in round 5). Firefox's drain improvement is not significant on `dc69e75`.
- WebKit cannot run the 50k fixture in either mode.
- Chromium needs a complete 20-pair run on `dc69e75`.

No workload, threshold or assertion was changed, and every failed run is kept: `round4-validation-*.json` and `round5-validation-*.json`.

## Files and follow-up

- Client: package subpath exports; `auth-scope.ts`, `client-error.ts`; compile-time engine metadata in `index.ts`/`outbox.ts`; `engine/` proxy, host, worker, RPC, patches, receipts, connection/admission hooks, read/close helpers, and focused tests. The default `local-replica.ts` and `browser-upgrades.ts` runtime changes from Round 1 were removed.
- React: `src/index.tsx` and `src/engine.test.tsx`.
- Generator: `src/local-bindings.ts` and `test/local-bindings.test.mjs`.
- Measurements: `packages/client/bench/engine-worker/` source, raw browser samples, summary, and bundle sizes; this report.

Whagons needs an explicit feature-flagged factory switch using the integration code above, matching generated worker assets and the existing stores/contract. Audit direct `localReplica`/`useGonvexClient` consumers before that switch; convert their reads to awaited engine reads or engine hooks. Keep worker and reducer-artifact assets in the application's offline precache/update policy. This PR does not make that application change or bump its client contract.

Recovery metadata has the bounded contract above. Suspended workers/tabs, stale deployed worker assets, and mixed old/new tabs remain rollout risks. The engine-only hooks mirror legacy connection/admission logic and must be kept in sync when those routines change. Full-result subscriptions can still exceed a small UI budget. No worker CPU, memory, battery, production-runtime, dependent-intent-chain, or Safari/iOS suspension improvement is claimed from these browser fixtures.
