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

## Files and follow-up

- Client: package subpath exports; `auth-scope.ts`, `client-error.ts`; compile-time engine metadata in `index.ts`/`outbox.ts`; `engine/` proxy, host, worker, RPC, patches, receipts, connection/admission hooks, read/close helpers, and focused tests. The default `local-replica.ts` and `browser-upgrades.ts` runtime changes from Round 1 were removed.
- React: `src/index.tsx` and `src/engine.test.tsx`.
- Generator: `src/local-bindings.ts` and `test/local-bindings.test.mjs`.
- Measurements: `packages/client/bench/engine-worker/` source, raw browser samples, summary, and bundle sizes; this report.

Whagons needs an explicit feature-flagged factory switch using the integration code above, matching generated worker assets and the existing stores/contract. Audit direct `localReplica`/`useGonvexClient` consumers before that switch; convert their reads to awaited engine reads or engine hooks. Keep worker and reducer-artifact assets in the application's offline precache/update policy. This PR does not make that application change or bump its client contract.

Recovery metadata has the bounded contract above. Suspended workers/tabs, stale deployed worker assets, and mixed old/new tabs remain rollout risks. The engine-only hooks mirror legacy connection/admission logic and must be kept in sync when those routines change. Full-result subscriptions can still exceed a small UI budget. No worker CPU, memory, battery, production-runtime, dependent-intent-chain, or Safari/iOS suspension improvement is claimed from these browser fixtures.
