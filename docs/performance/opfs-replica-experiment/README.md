# TypeScript OPFS replica experiment

This is a working database-side prototype behind `@gonvex/client/experimental-opfs`. It is **not a replacement for the production Local Replica**. The default client is unchanged. The measured implementation is smaller in module bytes but slower and larger on disk than the matched SQLite component. A browser memory win has not been established.

## Measured result

`results.json` retains every sample, artifact hashes and the real-worker recovery receipt. Three fresh disposable databases per arm and size, interleaved order, 20 combined queries per database, same Electron 44 / Chromium 152 browser on this Mac. Both arms use dedicated workers and OPFS; SQLite is the other thread's pinned 3.54.1 build at `24cdc5e`, with SAHPool, WAL, exclusive locking, FULL sync, external-content trigram FTS and the same three secondary indexes. Rows have the same content as `packages/client/bench/sqlite-replica/fixture.mjs` on that commit. Fixture generation and index construction are included in ingestion. Search/filter/sort results were checked against an independent expected result.

| Median of three component runs | TypeScript 31,012 | SQLite 31,012 | TypeScript 100,000 | SQLite 100,000 |
| --- | ---: | ---: | ---: | ---: |
| Ingest and indexes, ms | 1901.0 | 550.8 | 6331.3 | 1820.8 |
| Combined query p50, ms | 2.1 | 0.4 | 6.1 | 1.0 |
| Reopen plus first combined query, ms | 8.0 | 1.5 | 14.8 | 2.3 |
| Filter/sort without search, ms | 49.3 | 0.2 | 147.2 | 0.4 |
| Committed database bytes | 33,204,835 | 22,863,872 | 107,152,233 | 75,587,584 |

The TypeScript modules are 73,174 unminified bytes / 20,537 bytes summed per-module gzip. The pinned SQLite JS plus WASM are 2,170,717 / 784,158 bytes. These are artifact sizes, not measured production bundle downloads. TypeScript uses no WASM. The measured SQLite linear memory was 4 MiB at 31k and 5.94 MiB at 100k, not the older report's assumed fixed 16 MiB. Linear memory is not total worker/browser memory. Native Node trials also failed to demonstrate a process-memory advantage and are not substituted for browser memory measurements.

These numbers exclude app/auth/network, real tenants, production bundles and whole-browser memory. All samples, including slow queries, remain in the raw results. This small component cohort is not an application/tail qualification. Do not compare its seconds to the other thread's minutes and claim an engine win.

The latest Arch3 report read for this experiment is `/mnt/data/work/wh-memory-impl/REPORT.md`, section "Small native route crash identified — immutable read wrapper". It attributes a route crash to Whagons spreading a class view and losing `entityVersion`, not to SQLite throughput. Its corrected build still required persistent UI verification. Older invalid native labels and failed-grid memory figures are excluded here.

## Storage and queries

- A dedicated owner holds one exclusive OPFS data handle per opaque deployment/account/tenant scope. A SHA-256 scope name isolates files; the full scope is also checked in the database header. A second owner is refused. No alternate backend or reset is selected on corruption, quota failure or an unknown write outcome.
- Immutable copy-on-write B+tree nodes, 4 KiB maximum encoded node, backwards-only pointers, checksummed frames and two checksummed 4 KiB commit headers. A commit flushes its records/index pages, writes the alternate header, then flushes again. A write/flush failure poisons the handle. Reopen chooses a complete commit; nonempty invalid storage is never silently reset.
- Snapshot rows are encoded once into append-only frames. Row, ordinal and secondary indexes reference those frames. Staged rows remain invisible until the new root commits. Aborting/crashing preserves the old root. Snapshot staging is not yet resumable and append replies are not durable server credit.
- A bounded external sort builds trees bottom-up. Scratch records have checksums. Trigram accumulation is bounded to blocks of 256 input ordinals and spills when its byte budget is reached. Postings choose sparse byte lists or 256-bit bitmaps and retain exact cardinalities. No corpus-sized JS row map or posting map is retained.
- Queries combine AND/OR/NOT, typed comparisons, IN, structural JSON equality, array membership, substring predicates, relations, literal case-insensitive search, arbitrary multi-column scalar sorting and projection. SQL UNKNOWN/null placement and the ascending identity tie-break follow `sqlite-typed-query.ts`. Binary text collation must be explicit. Undeclared/unsupported operations fail rather than being ignored.
- The planner chooses a leading-equality composite index or the rarest search trigram, counting a competing index only as far as that trigram cardinality. Exact text and residual predicates are verified before the page limit. Compatible ascending index orders seek by keyset and stop after the page; other sorts use a bounded top-K heap. Changed/deleted rows invalidate their old acceleration entries and join an exact dirty-row arm until snapshot rebuild. Related-table predicates currently scan their related collection and can hit the explicit work budget.
- Rows, metadata and pending intent receipts can commit under one root. `acceptIntent` rejects key/content reuse and retries an unknown outcome without reapplying local changes. This is a database primitive; it does not replace the SDK's causal scheduler, server idempotency, predicted overlays or settlement protocol.

The default sort buffer is 1 MiB, staging posting dictionary 1 MiB, write buffer 64 KiB, node cache 32 pages, delivery envelope 256 KiB and top-K candidate allowance 4 MiB. These are explicit buffer/queue admission bounds derived from the SDK's bounded-message contract, not promises about V8 heap or process RSS. Oversized keys/values/plans and exhausted work/scratch budgets refuse the operation. An oversized secondary key invalidates that index so exact queries fall back safely. No arbitrary composite-index explosion is attempted.

## Verification and reproduction

```sh
pnpm --filter @gonvex/client test:opfs
pnpm --filter @gonvex/client test
pnpm --filter @gonvex/client bench:opfs typescript 31012
pnpm --filter @gonvex/client bench:opfs sqlite 31012
```

The 19 database checks include a model-based B+tree test, wide keys, every injected commit I/O boundary with torn writes/unknown flushes, crash recovery, corruption/scope refusal, scratch corruption, Unicode/short/literal substring search, stale postings after edits/deletes, null logic, indexed pagination, long-index fallback, immutable intent retries, input/metadata guards and 100 generated query plans compared to SQLite.

For browser reproduction, run `pnpm --filter @gonvex/client bench:opfs:browser` to compile and verify the two pinned SQLite assets in ignored `packages/client/bench/opfs/build/`, serve the repository on localhost, and open `packages/client/bench/opfs/browser.html`. Call `runDatabaseBenchmark({arm:'typescript',rows:31012})` or `arm:'sqlite'` in the console. `browser-proof.mjs` exports `proveWorkerRecovery()`, which uses the public SDK worker API, terminates owners during an uncommitted snapshot and after an acknowledged intent, reopens, verifies exact results and cleans its own files. Benchmark workers also clean their disposable files before reporting completion. No raw heap/auth/customer artifacts are committed.

## Before live-client integration

Reuse the SQLite work's generated admission/membership/visibility contracts, field authority merge, owner broker/fencing, causal intent journal, prediction/rebase, credited feed and sealed-page display. Do not transplant its SQL execution adapter or duplicate the generated reducer catalog into a second runtime. The prototype's worker API is not `LocalReplicaStorage` or a generated `GonvexClient` substitute.

Production work still includes multi-tab broker/failover, complete generated collection membership and coverage, locale/natural collation, label-expanded task search, workspace contexts/custom-field pivots, prediction/reducer integration, migration/fallback with durable transfer, resumable snapshots and garbage collection/compaction of old committed pages. File growth is currently append-only. Snapshot rebuild restores exact indexes but does not reclaim old committed generations. Large unindexed/deep sorted windows may rescan. No feature flag, settings screen or production default is added by this experiment.

The Whagons companion command `pnpm experiment:opfs --sdk-root /path/to/gonvex --rows 31012` compares three combined cases against its actual workspace row selector and tests durable intent retry. It is an integration oracle, not a claim of full UI parity.
