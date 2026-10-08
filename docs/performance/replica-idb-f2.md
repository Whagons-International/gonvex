# F2 IndexedDB replica format report

## Design

The previous format serialized each row as JSON and indexed every finite number, boolean and string of at most 256 characters, including the primary key. A fully populated 50-column task wrote 50 multi-entry lookup keys in addition to its primary, scope, entity and sequence keys.

The available generated metadata cannot provide an exact list of Reducer equality columns. `packages/gonvex/src/local-bindings.ts` passes table names, replica collection definitions and table-level `localDependencies` to the client. `local-schema.ts` stays inside the portable runtime factory. `packages/gonvex/src/local-schema.ts` emits primary keys, column types, nullable flags and defaults. It does not emit secondary SQL indexes or foreign keys. Collection `equalFilters` name subscription filters, not every predicate a Reducer can submit. The dependency analyzer in `packages/gonvex/src/local-dependencies.ts` resolves tables, not predicate columns.

A Reducer can pass any structured `DataRead` predicate at runtime. The IndexedDB planner can narrow candidates using string primary-key equality or IN, and one untransformed scalar equality or IN term within a conjunction. OR, transformed comparisons, NULL tests, large strings and other operators need residual evaluation. Declared SQL indexes and foreign keys do not restrict these runtime reads. `paged-read-view.ts` delegates candidate paging to its adapter and evaluates predicates through `memoryReadView`; it has no IndexedDB lookup-key dependency and needs no format change.

Version six indexes columns observed in actual Reducer reads, independently for each scope and entity. New snapshots start with no blanket scalar indexes. String primary-key reads go directly to the existing compound primary key, without loading index policy. If a conjunction has a usable learned column, the planner chooses that term, even if an earlier term is unindexed. Otherwise it scans only `[scope+entity]`, applies the entire predicate, preserves ordering, limits, exclusions and coverage rules, and records the chosen equality-column demand.

After the read transaction finishes, a separate atomic write transaction builds the demanded keys and publishes their policy under `lookupColumns:<entity>` in the scope's existing metadata store. It pages at most 256 records and uses the existing index-work write budget. It never exposes a partially built index. Requests from concurrent peers merge inside that write transaction. Writes reload policy once per entity per native transaction, so a peer cannot use a stale process-wide policy cache. Revisions and no-op records keep the existing merge behavior. Metadata-only commits still avoid entity reads and writes.

Incomplete Reducers also teach their requested columns, which can happen before hydration. An index-build failure rolls back its keys and policy, preserves the original read result or error, and retries after a later scan. Index construction does not change replica authority, sequence, memberships or delivery. Policy-only empty scopes are excluded from `listScopes`, so browser upgrade fencing does not mistake derived metadata for legacy application data.

## Upgrade and durability

The Dexie schema advances from version five to six. Its atomic upgrade clears only `lookupKeys` on existing entity records. Values, IDs, authority, sequence, tombstones, windows, cursors and sessions remain intact. The version-four upgrade no longer builds blanket keys that version six would immediately discard.

The durable outbox and application upgrade journal live in separate databases. The version-five upgrade test seeds those databases and checks that their queued intent and staged journal survive unchanged, alongside the replica's rows and metadata. Existing version-three upgrade coverage also passes through the new format.

## Row value experiment

JSON values remain unchanged. `packages/client/bench/replica-values.mjs` compares JSON strings with native structured-cloned objects using identical 50-column rows, a primary-key-only Dexie schema, batches of 32 and read pages of 256. Numbers below are medians of three trials on this workstation with Node and fake-indexeddb, using `--expose-gc`.

| Rows | Encoding | Write ms | Read ms | Stored fake-IDB heap MiB |
| ---: | --- | ---: | ---: | ---: |
| 10,000 | JSON string | 197.3 | 127.0 | 11.91 |
| 10,000 | Object | 261.9 | 115.0 | 13.07 |
| 50,000 | JSON string | 1,070.5 | 614.0 | 59.74 |
| 50,000 | Object | 1,201.7 | 626.8 | 64.97 |

Objects did not improve both read and write time. They increased retained fake-IDB heap by about 9%, made writes 12% to 33% slower, and changed reads by between a 9% improvement and a 2% regression. These measurements do not justify changing the row encoding or byte-budget accounting in this phase.

## Replica benchmark

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

The separate first-demand run uses 1,000 existing 50-column rows, no previously learned workload, and 1,000 observers. Its snapshot took 288.9 ms. The first equality read, including the scoped scan and atomic backfill, took **14,802.7 ms** in fake-indexeddb. Subsequent equality reads took 1.81 ms median, and the store had 1,000 lookup entries. This is a material one-time cost; the main known-workload table does not include it. The existing fake-indexeddb update implementation scans each whole index to remove a record's old keys, creating an artificial quadratic component. Native browser backfill still requires measurement; do not assume this 14.8-second result predicts Chrome.

Raw measurements are committed under `packages/client/bench/results/replica-idb-f2.json`.

Reproduce the main comparison against separately built clients:

```sh
nice -n 10 env CLIENT_DIST=file:///tmp/gx-idb-f2-baseline/dist/ node --expose-gc packages/client/bench/replica-idb.mjs
nice -n 10 node --expose-gc packages/client/bench/replica-idb.mjs
nice -n 10 node --expose-gc packages/client/bench/replica-values.mjs
```

See `packages/client/bench/README.md` for dependency builds, baseline setup and separate first-demand/default-write commands.

## Files changed

- `packages/client/src/indexeddb-read-view.ts`: explicit indexed-column key construction; learned-column query planning; scoped fallback scans.
- `packages/client/src/indexeddb-replica.ts`: version-six upgrade; persistent per-scope/entity policy; bounded atomic backfill; transaction-local policy cache; write maintenance.
- `packages/client/src/indexeddb-read-view.test.ts`: scoped fallback, selected-column demand, updates, peers, reopen, concurrent demands, failed backfill, incomplete Reducers and version-five durability fixtures.
- `packages/client/bench/replica-idb.mjs`: reproducible snapshot, lookup, open/load, heap and index-count benchmark with 10k and 50k rows and 1,000 observers.
- `packages/client/bench/replica-values.mjs`: isolated JSON versus object measurement.
- `packages/client/bench/README.md`: build and reproduction instructions.
- `packages/client/bench/results/replica-idb-f2.json`: recorded machine, baseline, optimized and row-value measurements.
- `docs/performance/replica-idb-f2.md`: this report.

## Validation

- `cd packages/client && nice -n 10 pnpm test`: passed, 21 test files and all 378 tests, including the package build. The final run took 16.91 seconds.
- `cd packages/client && nice -n 10 pnpm typecheck`: passed.
- Targeted native-request-count test: passed after changing the instrumentation to `IDBObjectStore.prototype.get`.
- Both 10k/50k main benchmark builds, default no-read snapshots, the 1k first-demand case and the three-trial value-encoding benchmark completed successfully.
- A 100-row smoke run passed the benchmark's equality-count and complete-working-set guards.
- `git diff --check`: passed.

No Rust files changed and no Rust tests were run. Only the client package changed, so no other package test suite is claimed. An accidental workspace-wide JavaScript test invocation was stopped; the completed validation above is the client package suite.

The existing parent-lookup test now observes its workload before measuring indexed reads. Its JSON.parse assertion counts row objects, excluding the newly persisted column-policy array. Its timeout accommodates one-time fake-indexeddb backfill. The scalar-key helper test now explicitly requests `statusId`, `active` and `deletedAt`, expecting two keys because NULL remains unindexed and the primary key no longer gets a blanket secondary key. The older version-three test's title describes the new upgrade behavior. No application-behavior assertions were removed.

## Risks and work left for later

- A newly observed column on an already large entity requires one scoped scan and an atomic rewrite of its derived keys. It delays that first read and briefly occupies the replica database's write lock. Snapshot writes after the workload is known avoid this cost. See the separate first-demand measurement above. Fake-indexeddb 6.2.5's `RecordStore.deleteByValue` scans an entire index when replacing an existing record, so full-entity backfill has an artificial quadratic component absent from an ordinary native B-tree update.
- Policies accumulate observed columns within each scope. There is no artifact-based pruning of columns that later Reducer versions stop using. A column actually queried at least once remains indexed until the scope is cleared.
- The upgrade rewrites existing records once, under Dexie's atomic version-change transaction. Physical browser disk compaction and actual open-time reduction need a real browser measurement. Database migration may require temporary disk headroom.
- The heap figures include fake-indexeddb's JavaScript database representation and one complete resident LocalReplica with 1,000 listener registrations. They are not measurements of Chrome's native IDB storage, React hook overhead, renderer memory or physical database bytes.
- No Rust, code generator, SQLite adapter, React API or consumer app changes are needed. Static column analysis or generated read hints could avoid first-demand backfill on more workloads, but the existing metadata is insufficient to make that analysis exact.
