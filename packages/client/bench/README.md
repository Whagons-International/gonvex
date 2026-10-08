# IndexedDB replica benchmarks

From the repository root, build the dependencies and client:

```sh
nice -n 10 pnpm --filter @gonvex/protocol --filter @gonvex/module-sdk --filter @gonvex/local-runtime --filter @gonvex/client build
nice -n 10 node --expose-gc packages/client/bench/replica-idb.mjs
nice -n 10 node --expose-gc packages/client/bench/replica-values.mjs
```

`replica-idb.mjs` writes 10k and 50k task rows with 50 populated scalar columns, measures five equality reads, closes and recreates the storage adapter before loading its complete working set, and retains one complete `LocalReplica` with 1,000 subscriber registrations. The budget is 200 MiB with an unlimited row count. Heap deltas use `process.memoryUsage().heapUsed` after explicit GC. The native IDB `lookupKeys` index count is read directly rather than estimated from input rows.

The default run observes the actual equality request on an empty replica before the snapshot. This measures storage and reads with a known workload. Only `tasks.workspaceId` is needed for that request. The old client still creates all 50 scalar keys. No workload hints are needed in app code; normal Reducer reads learn their demands automatically, including incomplete Reducers before hydration.

Run a separate first-demand case to include the scoped scan and atomic backfill of an already populated entity:

```sh
nice -n 10 env ROWS=1000 LEARN_AFTER_SNAPSHOT=1 node --expose-gc packages/client/bench/replica-idb.mjs
```

Backfill updates are especially slow in fake-indexeddb. Do not hide that cost in a steady-state lookup measurement or extrapolate it to native browser IDB. To measure default snapshots before any Reducer has requested an index, without triggering subsequent backfill:

```sh
nice -n 10 env NO_READS=1 node --expose-gc packages/client/bench/replica-idb.mjs
```

`ROWS` accepts a comma-separated list. `CLIENT_DIST` accepts a directory file URL ending in `/` and lets this committed benchmark run against a separately built baseline client, for example:

```sh
nice -n 10 env CLIENT_DIST=file:///tmp/gx-idb-f2-baseline/dist/ node --expose-gc packages/client/bench/replica-idb.mjs
```

For the F2 report, that baseline was built from commit `f21f03d` using the same workspace dependency builds. A temporary copy of the client's `src` directory and `tsconfig.json` suffices: restore `indexeddb-replica.ts` and `indexeddb-read-view.ts` from that commit with `git show`, point the copied tsconfig's `extends` at the repository's `tsconfig.base.json`, link the installed client `node_modules`, add `{"type":"module"}` as the temporary package.json, and run `nice -n 10 pnpm --dir packages/client exec tsc -p <temporary-tsconfig>`. This does not alter the working branch.

`replica-values.mjs` isolates row encoding with a primary-key-only schema, batches of 32, pages of 256 and medians of three trials. Both encodings use the same task shape.

These scripts measure JavaScript work and fake-indexeddb's retained JS database, not native disk size, browser IDB open latency, actual React hook overhead or Chrome renderer memory. Snapshot/open/heap results are single runs; lookup results are medians of five reads. Use a quiet machine and compare identical builds and benchmark scripts.
