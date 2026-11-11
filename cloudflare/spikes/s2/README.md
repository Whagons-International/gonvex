# S2 execution spike

Runs real Whagons Reducers inside a Durable Object on local workerd, with the
Gonvex module contract. The findings are in `docs/cloudflare/spikes/S2-execution.md`.

This directory is self-contained. It is not part of the pnpm workspace, and it
has its own lockfile. Nothing here talks to Cloudflare.

## Layout

- `src/runtime-globals.js`: pinned `Date`, seeded `Math.random` and the entropy gate,
  scoped per invocation with `AsyncLocalStorage`. It also holds the Ed25519 `verify` shim.
- `src/bootstrap.js`: `ctx` and the dispatcher, ported from `gonvex_bootstrap.js`.
- `src/host.js`: the database host calls with the semantics of `rust/crates/runtime/src/host_calls.rs`.
- `src/sql.js`: admission, `$n` → `?n`, write targets and result typing.
- `src/engines.js`: the Durable Object engine, where savepoints are nested
  `ctx.storage.transaction()` calls, plus the node:sqlite engine used by the reference.
- `src/invoke.js`: runs one invocation in one transaction, with the deferred-FK guard.
- `src/worker.js`: the `Tenant` Durable Object and its routes.
- `test/scenarios.mjs`: the workload, made of real Reducers and rollback cases.
- `test/savepoint-module.js`: synthetic Reducers built on the real SDK's `applyDataWrites`.
- `test/run-spike.mjs`: the driver for workerd, rollback checks, contract vectors and the row comparison.
- `test/run-reference.mjs`: the same steps on node:sqlite, using either the full schema or the subset the DO accepted.
- `scripts/`: the artifact build (as `gonvex dev` does it), reference provisioning, the
  schema subset, startup and size measurements, and V8 heap measurements.
- `probe/`: platform probes for transactions, savepoints, the input gate, foreign keys and SQLite limits.

## Run

```sh
pnpm install                                   # workerd 1.20261023.1, wrangler 4.153.0
node scripts/build-artifact.mjs <whagons5-client checkout>     # 2457bb65f
node scripts/provision-reference.mjs <whagons5-client checkout>
node scripts/prepare.mjs && node test/run-reference.mjs .artifact/reference-run.sqlite --savepoints
node scripts/subset-schema.mjs
node test/run-spike.mjs                         # .artifact/spike-results.json
node scripts/measure-startup.mjs                # .artifact/startup-results.json
node scripts/measure-heap.mjs                   # .artifact/heap-results.json
node scripts/report-table.mjs                   # the per-step table in the report
```

The Gonvex packages must be built first with `pnpm --filter @gonvex/cli... build`.
Generated files go to `.artifact/`, `.build/` and `.storage/`, all of which are git-ignored.
