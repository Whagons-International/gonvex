# F2 IndexedDB replica format, round 2

## Outcome

Reducer reads no longer await index construction. An equality demand on an unindexed column returns its correct scoped scan result and schedules derived work after the read transaction. Reads continue scanning until the index is complete. Metadata seeds common lookup columns during ingestion, and the committed Playwright harness measures the built package in native Chrome IndexedDB.

This round starts at `c4e54d3`. The original `f21f03d` comparison and JSON-versus-object experiment remain below as historical measurements. No row encoding, authority merge, subscription delivery, durable outbox, application migration journal or resident-row budget changed.

## Design and concurrency

The planner still uses string primary-key equality and IN directly, chooses a usable indexed equality inside a conjunction, and applies the complete residual predicate. OR, transformations, NULL tests and unusable scalar values retain their scoped fallback. The read's `finally` block queues demanded columns without awaiting them, including when a Reducer reports incomplete coverage or throws.

A build yields through `requestIdleCallback` with a 100 ms timeout, or a zero-delay timer where idle callbacks are unavailable. It registers a token under `lookupPending:<entity>`, processes at most 32 records per write transaction, then yields again. The existing index-work request budget applies within each batch. `Dexie.ignoreTransaction` keeps scheduled work outside the initiating transaction, including when seeds are discovered during writes.

Ready columns and pending columns serve different purposes. The read planner sees only the ready `lookupColumns:<entity>` policy. Writes load both policies once per entity per native transaction and maintain pending keys as well as ready keys. This preserves inserts behind the build cursor and updates to records already processed. Each build rereads peer policies before its next batch. Final publication merges the newly completed column with peer-ready columns and removes only that column's pending token in one transaction.

This avoids holding a write lock for a whole-entity build. Native IDB still serializes overlapping transactions, so a read or write can wait for an already active 32-row batch. No foreground operation waits for the background job as a whole. Large fallback scans retain their existing CPU and latency cost.

The storage instance deduplicates each scope/entity/column job and runs one builder at a time, avoiding a burst of background write transactions when several columns are demanded. `clear` invalidates its scope generation and deletes pending tokens; `close` cancels scheduled work. Builds stop between transactions. A peer also stops when its durable token disappears. Interrupted or failed builds leave partial derived keys invisible to the planner. Writes continue maintaining pending columns; the next demand retries from the beginning. Row authority, sequence and memberships are untouched by index work.

`waitForIndexBackfills()` is a diagnostic drain for tests and benchmarks. No application read calls it. Derived ready and pending metadata alone do not make an empty scope application data for browser upgrade fencing.

## Metadata policy

`GonvexClient` configures capable storage adapters from its generated local runtime binding. The policy unions foreign-key/reference columns from `localSchema`, row columns named by collection `equalFilters`, and optional per-Reducer `localReadHints`. Primary-key columns use the existing compound primary index. Unrequested scalar columns remain unindexed; learned demands complement these seeds.

Inspection found that the original `LocalColumn` carried only type, nullability and default. The old generator kept `localSchema` inside the runtime factory. This round adds optional `{ table, column }` references from PostgreSQL's `pg_constraint`, maps composite foreign keys by their matching attribute positions, exposes `localSchema` on the generated runtime binding, and increments the schema cache digest to v2. Existing generated clients still seed collection filters; regeneration adds foreign-key seeds.

Current `localDependencies` contains only table-name arrays, with no predicate-column hints. The implementation does not infer columns from those table names. Optional `localReadHints` provides an explicit reducer-to-table-to-columns companion for bindings that have that information; this round does not add a static predicate analyzer.

For an empty entity, the write transaction publishes seed policy before writing its rows. Snapshot replacement therefore writes seeded keys during ingestion without a preceding demand. For an already populated entity, newly introduced seeds become pending and use the same background construction mechanism. Unchanged reconnect projections still register new seeds before bypassing row-key construction.

## Upgrade and durability

The version-six Dexie upgrade from round 1 remains unchanged. It clears blanket `lookupKeys` while preserving values, IDs, authority, sequences, tombstones, windows, cursors and sessions. Older upgrades avoid constructing blanket keys that version six would immediately discard. Durable outbox and application migration journals remain in separate databases. The existing version-three and version-five durability tests still pass.

The Chrome upgrade fixture creates a version-five database containing 50,000 rows and 2,500,000 lookup keys, then times construction/open through `listScopes`. It verifies all 50,000 records remain and the upgraded keys are empty. This is an atomic one-time version change, distinct from the new cancellable demand backfill.

## Native Chrome measurements

The benchmark uses `/opt/google/chrome/chrome`, Playwright already declared in the root package, and esbuild resolved through the existing client Vitest/Vite dependencies. It bundles the built client into a tiny HTTP page and uses native IndexedDB. Both builds run on this Linux workstation with Ryzen 7 5800X and Chrome 153.0.8010.36, using 10k/50k rows with 50 scalar columns and 1,000 `LocalReplica` subscribers. The complete working-set budget is 200 MiB and unlimited rows.

The Chrome baseline is round 1, `c4e54d3`. Its `workspaceId` workload is learned on an empty replica before ingestion. Round 2 seeds that same column before ingestion. `statusId` is first demanded after ingestion. Scoped OR scans measure the same workspace predicate without an index. Indexed equality and scan numbers are five-read medians; snapshot, cold load, demand and upgrade numbers are single samples. The paired builds run sequentially. The CLI's own compiler is paused for this pair and the Node measurements. Other workstation activity is not controlled, so timing differences are not a stable throughput estimate.

| Metric | 10k round 1 | 10k round 2 | 50k round 1 | 50k round 2 |
| --- | ---: | ---: | ---: | ---: |
| Snapshot write ms | 1,036.3 | 1,029.4 | 4,689.6 | 7,457.2 |
| Indexed equality median ms | 4.6 | 5.9 | 20.9 | 28.5 |
| Scoped scan median ms | 229.0 | 276.9 | 1,249.3 | 1,448.6 |
| Cold open + complete load ms | 245.8 | 604.6 | 1,147.5 | 1,398.5 |
| First-demand read ms | 1,160.3 | 276.4 | 6,593.8 | 1,423.3 |
| Next/pending read ms | 25.2 | 290.6 | 92.2 | 1,328.2 |
| Backfill duration ms | 945.3 | 2,482.8 | 5,420.5 | 11,845.6 |
| Lookup entries after demand | 20,000 | 20,000 | 100,000 | 100,000 |

The 50k version-five to six upgrade took 13,562.3 ms in round 1 and 12,887.3 ms in round 2. The migration code is unchanged.

Backfill duration wraps the adapter's actual build method. In round 1 it is awaited by the first-demand read. In round 2 it starts as queued derived work after the scoped read and includes idle turns, batch transactions and any time spent behind foreground transactions. The baseline's next read is already indexed because its first read waited; the round-2 pending read still scans. These timings deliberately show the longer background wall time instead of hiding it inside lookup latency.

The 50k first-demand read fell from 6,593.8 ms to 1,423.3 ms, a 4.6x improvement. Background construction took 11,845.6 ms independently. Snapshot, steady equality and cold load were slower in this pair. This round does not claim a native snapshot throughput improvement over round 1. Exploratory passes varied substantially with other workstation activity. The measured read no longer includes construction, and the suspended-build test proves that independence without relying on timing.

Chrome's native database is outside Node's JS heap. These runs do not measure physical index bytes, renderer retained heap, React hook overhead or compaction. The Node measurements below retain the required `--expose-gc` and `process.memoryUsage().heapUsed` accounting.

## Round-2 Node measurements

The original Node harness now drains derived work explicitly outside measured reads. Known-workload runs learn the policy on an empty entity and drain before ingestion, so their snapshot and steady lookup costs stay comparable with round 1. The same 50-column rows and 1,000 subscribers are retained.

| Metric | 10k round 1 | 10k round 2 | 50k round 1 | 50k round 2 |
| --- | ---: | ---: | ---: | ---: |
| Snapshot write ms | 3,948.8 | 3,526.5 | 22,766.5 | 35,526.5 |
| Indexed equality median ms | 55.9 | 72.0 | 1,218.6 | 1,895.6 |
| Cold open + complete load ms | 218.5 | 256.5 | 1,050.1 | 3,784.0 |
| Retained heap after GC, MiB | 78.84 | 78.85 | 311.53 | 311.52 |
| Lookup entries | 10,000 | 10,000 | 50,000 | 50,000 |

The separate 1,000-row first-demand run has no preceding learned or seeded policy. Its snapshot took 831.9 ms. The first read returned in 4,666.0 ms, compared with the historical round-1 14,802.7 ms. After that read, a diagnostic drain measured 20,147.2 ms of remaining background work. Subsequent equality reads took 1.98 ms median. Fake-IDB timing is noisy here and its replacement implementation is quadratic. The round-2 50k Node write and load timings also regressed in this sample; retained heap and key counts stayed at round-1 levels. No steady-state timing improvement is claimed from these Node results.

Raw results are committed in `packages/client/bench/results/replica-idb-f2-chrome-before.json`, `replica-idb-f2-chrome-after.json`, `replica-idb-f2-round2-node.jsonl` and `replica-idb-f2-round2-demand-node.jsonl`. Round-1 raw results remain in `replica-idb-f2.json`.

## Files changed in round 2

- `packages/client/src/indexeddb-replica.ts`: background jobs, bounded transactions, pending/ready policy separation, peer-safe writes, cancellation and diagnostics.
- `packages/client/src/index.ts` and `local-replica.ts`: metadata policy derivation and optional storage configuration during client construction.
- `packages/client/src/indexeddb-read-view.test.ts`: pending-read and write independence, concurrent builds, cursor races, failure/retry, seed ingestion and cancellation coverage.
- `packages/client/src/local-reducers.test.ts`: metadata union, primary-key exclusion, table-only dependency handling and client-to-storage wiring.
- `packages/local-runtime/src/schema.ts`: optional reference metadata without changing execution semantics.
- `packages/gonvex/src/local-schema.ts` and `local-bindings.ts`: foreign-key metadata, schema cache invalidation and exposed generated runtime schema.
- `packages/gonvex/test/local-bindings.test.mjs`: single/composite reference extraction and generated binding exposure.
- `packages/client/bench/replica-idb-browser.mjs` and `replica-idb-browser-entry.mjs`: reproducible built-package/native Chrome workload and upgrade measurements, including result and complete-policy guards.
- `packages/client/bench/replica-idb.mjs`, `README.md` and results: asynchronous diagnostic draining, reproduction instructions and recorded measurements.
- `docs/performance/replica-idb-f2.md`: this full report.

## Validation

- `nice -n 10 pnpm --dir packages/client test`: passed, 21 files and 386 tests, including the client build. Final duration was 21.50 seconds.
- `nice -n 10 pnpm --dir packages/client typecheck`: passed.
- `nice -n 10 pnpm --dir packages/local-runtime test`: passed, 8 files and 72 tests. Duration was 28.21 seconds.
- `nice -n 10 pnpm --dir packages/local-runtime typecheck`: passed.
- `nice -n 10 env CARGO_BUILD_JOBS=2 pnpm --dir packages/gonvex test`: passed, all 47 tests, with no skips. Duration was 1,146.98 seconds including the runtime helper's cold native dependency build and pauses during measurement.
- `nice -n 10 pnpm --dir packages/gonvex typecheck`: passed.
- Native Chrome 10k/50k runs against both `c4e54d3` and the final implementation: completed with equality, completed-policy, key-count, working-set and 50k upgrade preservation guards passing.
- Node 10k/50k known-workload and 1k first-demand runs: completed with equality and complete-working-set guards passing and explicit post-GC heap measurements.
- Both browser benchmark files passed `node --check`; `git diff --check` passed.

Initial CLI setup failed because the React dependency had not been built and Playwright's cached headless shell was absent. Building that dependency and installing the test browser resolved both. The earlier unbounded native helper build was restarted with two workers; the complete final suite passed. No Rust source changed and no Rust test suite ran. The unchanged CLI artifact test invokes `cargo run` for its real artifact verifier, which required compiling bundled DuckDB. No other package test suite is claimed.

Existing tests changed only where they previously relied on a completed index immediately after a Reducer read. They now drain background work before inspecting keys/policy or measuring indexed deserialization. The failure test's title now describes rollback of the failed batch; a new test proves earlier committed batches stay invisible and retry safely. No application-result assertion was removed. The reference-extraction and generated-schema assertions are additions.

A deliberately suspended background turn proves the first demand and a second read return correct rows without waiting. A write also completes while the gate stays closed, and the next read observes that write. Other tests exercise inserts behind an advanced cursor, peer policy merges, a failed second batch, incomplete Reducers, clear and close cancellation, serialized queued builders, seeded snapshots and unchanged reconnect projections.

## Risks and work left for later

- An unseeded equality still scans the full scoped entity until its index is ready. On 50k rows this remains visible latency. Seed coverage matters for interactive Reducers. No static reducer-column inference is added because current dependencies identify only tables.
- Background building takes longer in wall time than an uninterrupted atomic build. Active batches still hold ordinary IDB locks briefly, and a hot foreground workload can postpone idle work. There is no promise that native operations have zero scheduling delay.
- A closed tab or failed build can leave unpublished derived keys and pending policy. Later writes maintain them and the next demand restarts the build. The build does not persist its cursor, so a retry can redo completed rows.
- A newly requested column remains indexed until its scope is cleared. Policy pruning and removal of obsolete derived keys remain future work.
- Version-five to six migration still rewrites all records under an atomic version-change transaction. Its measured open cost is material and may require temporary disk headroom. Moving demand construction off reads does not change that upgrade.
- Foreign-key seeding can add multiple real lookup columns to a task entity, increasing keys relative to the one-seed benchmark. It still avoids blanket indexing of all 50 scalars. Existing clients gain reference seeds only after regenerating bindings.
- Benchmarks use actual native IDB latency and fake-IDB heap separately. Neither establishes production tab memory, physical database size or Whagons React edit latency. The row-value experiment still favors keeping JSON for this phase.

## Retained round-1 row value experiment

JSON values remain unchanged. `packages/client/bench/replica-values.mjs` compares JSON strings with native structured-cloned objects using identical 50-column rows, a primary-key-only Dexie schema, batches of 32 and read pages of 256. Numbers below are medians of three trials on this workstation with Node and fake-indexeddb, using `--expose-gc`.

| Rows | Encoding | Write ms | Read ms | Stored fake-IDB heap MiB |
| ---: | --- | ---: | ---: | ---: |
| 10,000 | JSON string | 197.3 | 127.0 | 11.91 |
| 10,000 | Object | 261.9 | 115.0 | 13.07 |
| 50,000 | JSON string | 1,070.5 | 614.0 | 59.74 |
| 50,000 | Object | 1,201.7 | 626.8 | 64.97 |

Objects did not improve both read and write time. They increased retained fake-IDB heap by about 9%, made writes 12% to 33% slower, and changed reads by between a 9% improvement and a 2% regression. These measurements do not justify changing the row encoding or byte-budget accounting in this phase.

## Retained round-1 fake-IDB benchmark

Measurements were collected on this Linux workstation with an AMD Ryzen 7 5800X, Node 22.19.0, Dexie 4.4.4 and fake-indexeddb 6.2.5. The baseline client was built from `f21f03d`, the original worktree HEAD. Both builds used the same workspace dependencies and benchmark data. Heavy commands ran with `nice -n 10`.

The main comparison observes `tasks.workspaceId` equality on an empty replica before ingestion. This isolates snapshot and lookup costs once the actual workload is known. No other columns are requested. Each case retains the complete task replica with 1,000 subscriber registrations and the consumer's 200 MiB/unlimited-row budget. Snapshot, cold load and heap are single samples. Equality is the median of five subsequent queries, returning 100 rows at 10k and 500 at 50k.

| Metric | 10k before | 10k after | 50k before | 50k after |
| --- | ---: | ---: | ---: | ---: |
| Snapshot write ms | 30,926.6 | 3,948.8 | 265,892.7 | 22,766.5 |
| Equality median ms | 59.3 | 55.9 | 1,421.2 | 1,218.6 |
| First equality with known policy ms | 66.2 | 68.7 | 1,521.8 | 1,169.1 |
| Cold open + complete working-set load ms | 694.4 | 218.5 | 5,442.8 | 1,050.1 |
| Stored lookup index entries | 500,000 | 10,000 | 2,500,000 | 50,000 |
| Lookup entries + four fixed keys per row | 540,000 | 50,000 | 2,700,000 | 250,000 |
| Retained heap after GC, MiB | 254.69 | 78.84 | 874.59 | 311.53 |

Snapshot writes improved 7.8x and 11.7x. Lookup entries fell 98%; including the fixed keys, total stored key entries fell 90.7%. Cold open/load time fell 68.5% and 80.7%. Retained fake-IDB-plus-replica heap fell 69.0% and 64.4%. Equality reads improved only 5.7% and 14.3%, so the evidence supports write/load/size improvements more strongly than lookup latency.

The four fixed keys are the primary record key plus the `scope`, `[scope+entity]` and `[scope+sequence]` indexes. The lookup count comes directly from native `IDBIndex.count()`; the fixed-key total is derived from the written record count, with sequence present on every benchmark row.

With no preceding Reducer reads, the new format creates zero lookup keys. Default snapshot writes measured 3,124.4 ms at 10k and 19,068.1 ms at 50k; cold open/load measured 166.7 ms and 972.0 ms; retained heap was 74.49 MiB and 293.47 MiB. Those runs intentionally omit equality reads, to avoid folding a new index's construction into the write/load measurement.

The separate first-demand run uses 1,000 existing 50-column rows, no previously learned workload, and 1,000 observers. Its snapshot took 288.9 ms. The first equality read, including the scoped scan and atomic backfill, took **14,802.7 ms** in fake-indexeddb. Subsequent equality reads took 1.81 ms median, and the store had 1,000 lookup entries. This is a material one-time cost; the main known-workload table does not include it. The existing fake-indexeddb update implementation scans each whole index to remove a record's old keys, creating an artificial quadratic component. This is the historical round-1 measurement. The native Chrome and asynchronous round-2 results above replace the earlier open measurement item.

Raw measurements are committed under `packages/client/bench/results/replica-idb-f2.json`.

Reproduce the main comparison against separately built clients:

```sh
nice -n 10 env CLIENT_DIST=file:///tmp/gx-idb-f2-baseline/dist/ node --expose-gc packages/client/bench/replica-idb.mjs
nice -n 10 node --expose-gc packages/client/bench/replica-idb.mjs
nice -n 10 node --expose-gc packages/client/bench/replica-values.mjs
```

See `packages/client/bench/README.md` for dependency builds, baseline setup and separate first-demand/default-write commands.
