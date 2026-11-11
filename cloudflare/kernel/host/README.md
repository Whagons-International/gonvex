# Kernel module host (K04)

Runs a project's Queries and Reducers inside the tenant Durable Object with the Gonvex module contract
(`docs/cloudflare/CONTRACT.md` section B). It starts from the host design S2 recommended
(`docs/cloudflare/spikes/S2-execution.md`, `cloudflare/spikes/s2/`). The Rust reference is
`rust/crates/module-runtime`, `module-runtime-v8` (`gonvex_bootstrap.js`, `isolate.rs`, `lib.rs`),
`module-host` and `runtime/src/host_calls.rs` at `0bf3ad13`.

It is built on the K01 storage layer (`cloudflare/kernel/storage`). Ruling 0022 §1: **K01 owns every SQL
semantic** (admission, `$N`, read-only Queries, typing and decoding, catalogs and their cache, the
aborted rule, savepoints, the deferred-FK check). **K04 owns** the `ctx` surface, capabilities,
determinism, the host-call error wrapping, budgets and generations.

## Layout

| File | What | Rust reference |
|---|---|---|
| `src/globals.ts` | Pinned `Date`, seeded `Math.random`, gated `crypto` (WebCrypto plus the Ed25519 length shim), `[gonvex:module]` console, absent globals. The active invocation lives in `AsyncLocalStorage`. | `BS:28-54,403-435`, `RC/globals.js` |
| `src/module-globals.ts`, `src/bundle.ts` | The app-bundle prelude: one import that gives the bundle *lexical* bindings of those globals, so the kernel's own globals stay native | `packages/gonvex/src/local-bindings.ts` |
| `src/capabilities.ts` | Structural capability sets per kind, the host-grant intersection, the op → capability map | `MR/capabilities.rs`, `MR/lib.rs` `HostCall::capability` |
| `src/context.ts` | `ctx` for all three kinds, every host-call payload, `resolveHandler` | `BS:549-764` |
| `src/engine.ts` | One generation's engine: lookup, args/result schema, budgets, sticky denials, serialized ops, result caps, the dispatcher's error envelope | `V8/lib.rs` `execute`, `V8/isolate.rs` `host_call`/`decode`, `BS:766-807` |
| `src/schema.ts` | Portable schema validation with Rust's messages | `MR/lib.rs` `validate_at` |
| `src/errors.ts` | `ModuleError`, wire codes, error classes, platform-error mapping | `MR/lib.rs`, `MH/protocol.rs`, `MH/session.rs`, `RT/error_class.rs` |
| `src/database-host.ts` | Query/Reducer host calls on K01's `StorageSession`: `dbQuery` (K01's `query`), the statements `dbInsert/Update/Delete/DeleteMany`, the outbox and the scheduler build (K01's `runTyped`), `permissionsHas`, `storage.getMetadata`, deterministic deferred ids, Rust's error wrapping | `RT/host_calls.rs` |
| `src/generations.ts` | Load, stage, activate, drain; pinned generations; the `system.reload` event | `MH/session.rs`, `server/src/lib.rs` registry |
| `src/invoke.ts` | `TenantModuleHost.run`: lease, K01 invocation (`TenantStorage.invoke`), host, engine, discard on failure, error mapping; quarantine | `RT/execution.rs` reducer/query paths |
| `test/` | workerd tests: the 34 vectors, host calls on K01, budgets, catalog invalidation, generations, parity with Rust's answers on the conformance fixture | |
| `bench/` | The real Whagons bundle through the host (local workerd), rows read with the cache off and on | |
| `probes/r25/` | The runaway-Reducer measurement on the platform (0012 R25, 0016 G0-11) | |

## What it implements

- **Capability sets per kind (B.1).** Effective = structural(kind) ∧ host grant. A method that isn't
  granted is absent from `ctx`. Each op is checked again, along with Action network origins and tool
  names. A denial fails the call even if the module catches it. The Reducer grant follows
  `RT/execution.rs:585-588`: no outbox and no scheduler for anonymous callers, and `storage` only for
  public, internal or non-interactive Reducers.
- **Host calls (B.2a).** Payloads and error texts match `gonvex_bootstrap.js` and the Rust host,
  including `host call failed: <capability> host call failed [host_call_failed]: <message>`. Every op
  except `fetch` runs serially per invocation. Action-only ops (`fetch*`, the full `storage` and
  `sandbox` surfaces, `toolInvoke`, `functionInvoke`, `secrets`) are built here and gated by capability.
  Their host side is K12, K13 and K20, through the same `ModuleHost` interface.
- **Determinism (B.4).** `ctx.now` is stamped by the object at the start of the invocation (or taken from
  the caller). `Date` is pinned and `Math.random` is sfc32 seeded from `intentEntropy`, through the
  published factory. `getRandomValues` and `randomUUID` throw `InvalidAccessError` in Queries and
  Reducers. `runAfter` uses the server clock. Deferred ids match Rust's vectors.
- **WebCrypto (B.2b).** workerd's native `crypto.subtle`. The one shim: Ed25519 `verify` returns `false`
  for a signature that is not 64 bytes, as WebCrypto and Rust do; workerd throws `OperationError`.
- **Runtime-contract vectors.** All 34 pass in workerd, twice in one isolate (`test/vectors.test.ts`).
- **Errors (B.5).** `ModuleError` with Rust's Display text. Wire errors read
  `module <id> function <fn>: …`; clients see `TypeScript module host returned <code>: …`. Commit
  failures read `module database capability failed: …`. Classes are `rejected` or `transient` per
  `error_class.rs`, plus a transient-fault flag for SQLite codes 5/6/7/9/10/13/14/15. Platform errors
  are classified by B.5's closing paragraph.
- **Caps and budgets (B.6).** 8 MiB result: a UTF-16 pre-check in the dispatcher, then an exact byte
  check. The Q/R wall budget is 10 s, the scheduled slice 3 s with Rust's message, the Action budget
  15 min, and an agent Action has none. Depth 8. The wall budget is enforced at host-call boundaries and
  by a timer race, plus a host-call counter. workerd cannot interrupt running JS: see R25 below.
- **Transactions (B.7).** K01's: one storage transaction per invocation, the aborted rule, the commit
  refusal after a caught SQL error, the deferred-FK check (G0-5), Queries never commit. K04 discards
  the transaction when the engine fails the call (Rust's `finish(false)`) and maps K01's refusals to
  `module database capability failed: …`.
- **Generations and reload (B.6, 0005 → G0-2).** In stage 1 the bundle ships in the Worker version.
  `ModuleRegistry` keeps Rust's semantics anyway:
  - generations are strictly newer (`generation_conflict` otherwise);
  - activation is an atomic swap, and running calls finish on their own generation, which drains;
  - a pin newer than the active generation is `unknown_generation` (retryable);
  - with no generation, calls get `module_not_loaded`;
  - activation emits `{type:"system.reload", reason:"module generation changed", artifactHash}` with the
    client contract, for K05 to send or to turn into `client.updateRequired`.

  If S5's Worker Loader lands, only the loading changes.

## Catalog cache (G0-12, R04)

There is one catalog cache, K01's (`kernel/storage/catalog.ts`). It is cached across invocations and
invalidated by module DDL (K01), by `TenantModuleHost.schemaChanged()` (which K02 calls after
migrations; it delegates to `TenantStorage.invalidateSchema()`) and by a new generation.

This PR changes one thing in K01's catalog, as a separate commit for the K01 owner: **all record
schemas load in one statement per schema generation**. `name COLLATE NOCASE` cannot use the BINARY
primary key of `_gonvex_record_views`, so the per-table lookup scanned the whole table on each table's
first use, about 400 rows each on Whagons. NOCASE is matched with ASCII folding and the first row wins,
as the lookup would. K01's 101 tests pass.

**Measured on real Whagons Reducers** (whagons5-client `2457bb65f` split bundle, S2's schema subset and
35-step workload, local workerd, `node bench/run.mjs`).

- **Before** is Rust's cost: K01 without the patch, with catalogs invalidated before every invocation
  (Rust reads them once per invocation).
- **After** is K01's cache with the patch, in a fresh tenant object, so it includes the object's one-time
  loads.
- K01's deferred-FK model has no Rust counterpart. It is read once per object per schema generation
  (5,256 rows, on the first Reducer after a wake) and is shown separately. Before excludes it.

| Reducer (S2 step) | Before (catalog) | After (catalog) | of which FK model (once) | Fewer, excl. FK model |
|---|---|---|---|---|
| `teams.create` (01, first call in the object) | 2,016 (1,980) | 5,804 (512) | 5,256 | 73 % |
| `spots.create` (08) | 2,055 (1,990) | 111 (46) | 0 | 95 % |
| `cleaningStatuses.create` (19) | 1,993 (1,964) | 49 (20) | 0 | 98 % |
| `cleaningStatuses.create` (22, warm) | 1,993 (1,964) | 29 (0) | 0 | 99 % |
| `cleaningReports.create` (24) | 450 (438) | 52 (40) | 0 | 88 % |
| `centers.askJev` (26) | 3,240 (3,125) | 195 (80) | 0 | 94 % |
| `settings.togglePlugin` (27) | 475 (420) | 77 (22) | 0 | 84 % |
| `boards.create` (28) | 820 (805) | 33 (18) | 0 | 96 % |
| `boards.createMessage` (29) | 1,628 (1,575) | 89 (36) | 0 | 95 % |
| `boards.createComment` (30) | 1,218 (1,188) | 44 (14) | 0 | 96 % |
| **The 10 committed Reducers** | **15,888** | **6,483** | 5,256 | **92 %** (59 % with the one-time FK model) |
| All 37 calls (blocked and rollback steps included) | 51,739 | 2,180 + FK model | | 96 % |

- Without the record-schema patch, K01's cache gives **55 %** on the committed Reducers (15,888 →
  7,099), excluding the FK model.
- The catalog rows left with the cache on are `pragma_table_info` on a table's first use in the object.
  They go to 0 once the cache is warm (step 22).
- Outcomes, rows written and the contents of all 29 application tables are identical with and without
  the cache.
- Median server time for the committed Reducers: 23.0 ms before, 9.5 ms after (local; the machine was
  shared).
- Same outcomes as S2: 10 of 25 distinct Reducers commit. The rest are blocked by schema objects DO
  SQLite refuses, until A01.
- Upload with the Whagons split bundle: 5,747 KiB, **1,016.7 KiB gzip** (measured before the move onto
  K01).

**Queries.** No Whagons Query has a JavaScript handler. All 859 are replica, live or one-shot plans that
the runtime executes itself (K08/K10). Dispatching one through the module host gives Rust's
`invalid_artifact` outcome. See contract-change note 0021. The Query dispatch path is covered by the 10
Query vectors and `test/host.test.ts`.

## Runaway Reducer (0012 R25, 0016 G0-11)

Measured on the platform (development account, probe deleted afterwards): `probes/r25/FINDINGS.md`.

### What happens

- **The runaway call** fails with `Durable Object exceeded its CPU time limit and was reset.`
  (`durableObjectReset`, `overloaded`). It takes 33.6–36.4 s at the default limit, 14.2 s at
  `limits.cpu_ms` 10000 and 5.3 s at 2000. `cpu_ms` applies to Durable Objects. The object resets: the
  constructor runs again, committed rows are intact and the runaway's writes are rolled back.
- **Every other call on the tenant waits for the whole overrun, then fails.** At the default limit the
  text is the misleading `Durable Object reset because its code was updated.` From about 6.5–7 s on, new
  calls are shed in under 105 ms with `Durable Object is overloaded. Requests queued for too long.`
- **Sockets held by the tenant object** are all dropped: 1013 `Service overloaded; retry later.` at
  about 7 s, or 1006 at the reset if that comes sooner. No socket can open during the overrun. Clients
  are back in 90–207 ms once it ends.
- **Sockets held by gateway objects (G0-1)** never dropped: 0 of 10 in every run. Only their calls in
  flight to the tenant failed. No other object was affected (colocation was not observed).
- **The platform clock** (`Date.now`, `performance.now`) does not advance during synchronous work, not
  even across 2,001 `sql.exec` calls. A wall-clock check at a host-call boundary never fires on the
  platform; it only works locally.

### Mitigation

Implemented here:

1. `EngineConfig.maxHostCalls` (default 100,000). A runaway that keeps calling the host is stopped in the
   object with Rust's `module exceeded its 10000 ms execution deadline` (budget_exceeded, transient), and
   rolled back. A pure `for(;;){}` never calls the host, so it still reaches the platform limit.
2. `TenantModuleHost.quarantine`: a command id recorded after an overrun is answered with the same Rust
   deadline error without running. `isPlatformOverrun()` and `deadlineOverrunError()` let the caller map
   the three platform texts. Measured with the probe: 1 reset instead of one per retry, and retries
   refused in 76–104 ms.

Accepted by the lead (0022), from note 0021 §5:

3. The caller (K05 gateway, K07) records the command id on a fresh stub when it sees an overrun.
4. Ruled (0022 §5b): the tenant-object script sets `limits.cpu_ms = 5000`, and K02 chunks migrations
   so that no chunk exceeds 2 s CPU. Revisit at V04.
5. Keep sockets in gateways.
6. The Worker Loader with `limits.cpuMs` is the only true per-call guard: no reset, about +1–2 ms p50.
   But host calls back into a tenant that holds its transaction deadlocked (0 completed). Added to
   S5's scope.

## Run

```sh
pnpm install --filter @gonvex/cloudflare          # from the repo root
cd cloudflare && pnpm typecheck && pnpm test      # includes this package (vitest --root kernel/host)

# Real Whagons bundle (needs S2's artifacts: cloudflare/spikes/s2/README.md, scripts/build-artifact.mjs,
# provision-reference.mjs and subset-schema.mjs on whagons5-client 2457bb65f)
node kernel/host/bench/run.mjs <s2 .artifact dir>   # table above; raw data in bench/.results/
```

## Not here

| Item | Owner |
|---|---|
| Every SQL semantic, the catalog cache and the FK check | K01 |
| Reducer receipts (claim at the first write, stored result, replay) | K07 |
| The permission index behind `PermissionIndex` | K09 |
| `_gonvex_files` and R2 behind `FileMetadataSource` | K13 |
| Outbox and scheduler drain | K11 |
| The Action-side host (fetch, storage, tools, functions, secrets) | K12 |
| Sandbox | K20 |
| Sending `system.reload` / `client.updateRequired` to sockets | K05 |
| Building the Worker version with the prelude | K19 |
