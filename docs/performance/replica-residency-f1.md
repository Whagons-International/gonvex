# F1: Local Replica residency and notification performance

Implemented findings 1–4 on `perf/replica-residency`, against baseline `f21f03d3c1fd60be201f70ae83f53eefd4674c19`, package version `0.5.2-staging.27`. All four findings were present. The changes reduce read allocation and subscriber CPU. With only 250 rows visible, retained heap falls by less than 1 MiB; keeping the complete task corpus still accounts for most of replica memory.

## Design

### Incremental residency accounting

Each immutable table-map version carries an estimated byte total in a `WeakMap`. Insert, replacement and deletion update that total using the old and new row sizes. Copying a touched table uses a native `Map` copy and carries its total forward without revisiting rows or serializing them. Publication sums table totals and counts, so publication accounting is O(number of entities), not O(resident rows). `trimResidentRows()` reads the two scope totals and returns in O(1) below budget. It enumerates candidates and builds pin sets only above budget.

The existing estimate, `JSON.stringify(row).length * 2 + 256`, and eviction policy remain intact. Actual heap is measured separately. Table totals remain attached to speculative maps until the committed state swap, so failed persistence cannot change published totals. Hydration, peer changes, epoch changes, pruning, eviction and disposal use the same accounting. Evicted rows also invalidate their entity/window subscriptions.

Peer catch-up had two additional full-residency operations. The byte scan now reads counters. The bundled IndexedDB adapter opts into a synchronous membership predicate and checks changed IDs against the captured table maps. It no longer materializes every resident ID into arrays and then sets. Retained-window additions and pending IDs still participate. Custom adapters keep the original ID-array path unless they opt into the optional capability. No additional storage round trips were added.

### Immutable rows

The replica owns cloned ingest values and recursively freezes new JSON objects and arrays. Unchanged, already-frozen subtrees are shared. Confirmed rows from `entity()`, `entityBatch()`, `entityRows()` and watch snapshots share the stored frozen object. `visibleRowCopies` is removed. Optimistic journal fields are cloned and frozen at ingestion; projected row objects are frozen before delivery. Optimistic projections can still allocate a shallow object per read, but never clone unchanged confirmed rows.

Ingest values remain detached from caller-owned values. Transactions still copy touched table maps and replace row objects. Old watch snapshots remain valid after changes and rollback. Diagnostic snapshots, storage snapshots, committed integrity reads and reducer execution results retain their existing defensive-copy boundaries.

Audited production client/react call sites with `rg` for entity, batch, table and watch reads, row property assignments, deletes, `Object.assign`, and sorting. `query-expression.ts` filters/slices arrays and sorts a copied array; it does not edit rows. `projectReplicaRows()` writes a new projection. React reads rows and runs selectors. The assignment in `replica-record.ts` writes a newly constructed merge result. No SDK consumer was found mutating a replica-owned row. Frozen-row integration tests and the complete client/react suites support this audit; arbitrary application selectors are outside that proof.

### Indexed notifications

`LocalReplica.subscribe()` accepts an optional entity, row-ID set, or window signature. Existing global subscriptions remain supported. Changes accumulate touched entities/IDs and windows, and publication dispatches directly through those indexes. Multi-ID registrations receive one callback per commit, including the touched-ID set. Independent registrations and idempotent cleanup are covered by tests.

SDK Replica and Live Query watches, `useEntity`, `useReplicaEntities`, retained windows and Live Query state use the index. Scope/epoch invalidation and freshness transitions still broadcast because those changes can affect every snapshot. Computed offline Live Queries opt into broad delivery while offline so relation-table changes remain visible. Global infrastructure subscribers keep their existing behavior.

`paint-external-store.ts` is unchanged. Its animation-frame coalescing, background-tab delivery and cleanup remain covered by its existing tests. Public hook signatures are unchanged.

### Batched entity hooks

`useReplicaEntities` retains an ordered ID list by content comparison, without JSON serialization. Inline arrays with unchanged contents reuse the subscription and retention. The hook tracks a numeric snapshot revision and per-ID versions. Indexed notifications check only touched selected IDs. Rendering the resulting array uses the cached versions and preserves unchanged row identity, duplicate IDs and caller order. Mounting, changing selections and scope invalidation can check the full selection; unrelated table changes do not.

## Benchmarks

Machine: Linux x86_64, AMD Ryzen 7 5800X, Node v22.19.0. Both revisions ran on this same shared workstation under `nice -n 10`. Timings vary with other workloads; these are measured results, not browser performance guarantees.

Rows have 50 scalar columns. The replica allows 200 MiB estimated residency and unlimited row count. Each case retains a complete task collection, watches a 250-row visible window, and holds a 250-row batch read. Heap is the `heapUsed` increase after three explicit GCs. Timing uses 30 warmup upserts, 200 measured single-row upserts, and 100,000 individual `entity()` reads after warmup. There are 1,000 indexed subscribers across 20 entities, 50 watching the changed entity and ID. Callback work counts wake-ups; it does not mount React components or time rendering. Hot writes use an indexed-capable no-op adapter to exercise trimming while excluding database I/O.

Hot-path values below are medians of three alternating before/after process runs. The comparison harness transpiles the unchanged baseline LocalReplica into a temporary sibling of the built package, uses the same dependencies, and removes the temporary file. Cold hydration uses the actual IndexedDB adapter with `fake-indexeddb` and freshly seeded durable rows.

| Metric | Rows | Before | After | Change |
| --- | ---: | ---: | ---: | ---: |
| Retained heap, MiB | 10,000 | 17.315 | 16.592 | -0.723 MiB, -4.2% |
| Retained heap, MiB | 50,000 | 80.658 | 80.060 | -0.598 MiB, -0.7% |
| Single-row upsert, ms | 10,000 | 1.320 | 1.119 | -15.2% |
| Single-row upsert, ms | 50,000 | 9.759 | 5.480 | -43.9% |
| Subscriber callbacks per upsert | either | 1,000 | 50 | -95% |
| Individual entity read, ns | 10,000 | 6,601 | 162 | 40.8× faster |
| Individual entity read, ns | 50,000 | 6,675 | 239 | 27.9× faster |

| Cold operation, ms | Rows | Before | After |
| --- | ---: | ---: | ---: |
| IndexedDB working-set load | 10,000 | 751.6 | 767.2 |
| Full scope activation | 10,000 | 845.8 | 867.6 |
| IndexedDB working-set load | 50,000 | 7,757.3 | 5,015.0 |
| Full scope activation | 50,000 | 6,243.8 | 4,480.3 |

Cold results are single full runs. The 50k timing difference is sensitive to fixture/host conditions and should not be attributed to a JSON parser optimization. The scope still loads all rows.

The first full baseline run also measured upserts at 1.12/6.10 ms and reads at 5.70/5.39 µs for 10k/50k rows. Raw repeat runs preserve the timing variation. The median table is the paired comparison, not a selection of the fastest samples.

### Reproduce

From the repository root, build the dependencies and client:

```sh
nice -n 10 pnpm --filter @gonvex/protocol --filter @gonvex/module-sdk --filter @gonvex/local-runtime build
cd packages/client
nice -n 10 pnpm build
nice -n 10 node --expose-gc bench/replica-residency.mjs
nice -n 10 node --expose-gc bench/compare-replica-residency.mjs
```

Use `--hot-only` on `replica-residency.mjs` to skip durable fixture setup. The comparison harness accepts a baseline commit as its first argument and uses hot-only measurements. Durable 50k-row fixture creation takes several minutes in fake IndexedDB and is excluded from hydration timings. The full-run baseline was captured before implementation from the original compiled client. Raw data is committed in `packages/client/bench/replica-residency.before.json`, `.after.json` and `.comparison.json`.

## Files changed

- `packages/client/src/local-replica.ts`: accounting, frozen sharing and subscription indexes.
- `packages/client/src/index.ts`: read-only view forwarding, subscription type export and indexed SDK watches.
- `packages/client/src/indexeddb-replica.ts`: optional resident-membership lookup for peer catch-up.
- `packages/react/src/index.tsx`: indexed hook subscriptions and cached batch versions/IDs.
- `packages/client/src/replica-performance.test.ts`: frozen ingest/read values, exact accounting, failed persistence, no under-budget scan, selective/deduplicated notifications, cleanup and offline relation delivery.
- `packages/client/src/replica-residency.test.ts`: peer updates without resident-ID enumeration, alongside the existing disk/pinning/budget tests.
- `packages/client/src/local-replica.test.ts` and `entity-rows-projection.test.ts`: updated internal clone/mutation assertions.
- `packages/react/src/index.test.tsx`: real-replica batch integration with touched-ID-only checks, inline arrays, duplicates and reentrant writes.
- `packages/client/bench/`: executable benchmarks and raw results.
- `F1-REPORT.md`: this report.

## Verification

- `cd packages/client && nice -n 10 pnpm test`: **376 passed, 22 files**. Includes package build.
- `cd packages/client && nice -n 10 pnpm typecheck`: **passed**.
- `cd packages/react && nice -n 10 pnpm test`: **81 passed, 3 files**.
- `cd packages/react && nice -n 10 pnpm typecheck`: **passed**.
- `git diff --check`: **passed**.
- Full before/after benchmark and three paired hot-path comparisons: **completed**.

Four existing tests deliberately asserted the replaced defensive-copy implementation. The watch allocation test now expects zero read-time clones for prediction and committed changes. The visible-row test now expects shared stored identity and a `TypeError` on mutation. The table and batch projection tests now expect nested mutation to throw and still verify stored/journal values and duplicate entries. Other existing tests passed unchanged.

No Rust files changed and no Rust suite ran. Early package build/typecheck attempts encountered missing compiled workspace dependencies or a concurrent build clearing client declarations. Dependencies were built and the required package checks subsequently passed. No push, PR, npm publication, stash or reset was performed.

## Risks and work left for later

Freezing intentionally changes attempted writes to returned rows: strict-mode mutations throw instead of modifying detached copies. Client/react do not rely on such writes, but application code that edits read results must create its own draft. Nested values are also immutable. Return types and hook signatures stay compatible, and row values, ordering, completeness and optimistic rollback retain their behavior.

The memory result is modest because only 250 visible rows were duplicated and the entire 50k-row authoritative corpus remains resident. This benchmark does not establish a reduction from a 330–700 MiB Whagons browser tab to a particular target. It excludes browser DOM, React trees, runtime heaps and other caches.

Single-row commits still copy the touched table's native `Map`, O(table rows). Window invalidation still inspects memberships, so an ID near the end of a large membership costs more than the benchmark's first ID. Collection deltas reconstruct membership/row arrays, maintain ownership and can sort the whole collection; watch snapshot rebuilding visits visible IDs. These are distinct remaining costs. Indexing subscriber callbacks does not make the whole transaction O(1). A persistent table-map structure and a window-membership reverse index warrant a later change with separate lifetime/order tests.

`loadWorkingSet()` already pages IndexedDB records and honors the residency budget. With Whagons' complete-replica settings, it still decodes the whole retained scope; activation also clones and freezes rows. Cold measurements are fake-IndexedDB results and are sensitive to the host load. JSON.parse cost alone is not isolated from record enumeration or Dexie overhead.

Per-entity lazy hydration is feasible only with a defined readiness boundary. Today's synchronous `entity()`, `entityRows()`, completeness and reducer reads can observe every restored row after `activateScope()` resolves. Silently deferring a table would change those semantics. A later opt-in design could declare eager entities and explicitly ensure a cold entity before exposing its reads, reusing the existing retained-ID/window loaders and indexed reducer fallback. This PR does not introduce that API or weaken complete local/offline sorting.
