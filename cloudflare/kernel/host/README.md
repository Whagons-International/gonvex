# Kernel module host (K04)

Runs a project's Queries and Reducers inside the tenant Durable Object with the Gonvex module contract
(`docs/cloudflare/CONTRACT.md` section B). It starts from the host design S2 recommended
(`docs/cloudflare/spikes/S2-execution.md`, `cloudflare/spikes/s2/`). The Rust reference is
`rust/crates/module-runtime`, `module-runtime-v8` (`gonvex_bootstrap.js`, `isolate.rs`, `lib.rs`),
`module-host` and `runtime/src/host_calls.rs` at `0bf3ad13`.

## Layout

| File | What | Rust reference |
|---|---|---|
| `src/globals.ts` | Pinned `Date`, seeded `Math.random`, gated `crypto` (WebCrypto plus the Ed25519 length shim), `[gonvex:module]` console, absent globals. The active invocation lives in `AsyncLocalStorage`. | `BS:28-54,403-435`, `RC/globals.js` |
| `src/module-globals.ts`, `src/bundle.ts` | The app-bundle prelude: one import that gives the bundle *lexical* bindings of those globals, so the kernel's own globals stay native | `packages/gonvex/src/local-bindings.ts` |
| `src/capabilities.ts` | Structural capability sets per kind, the host-grant intersection, the op → capability map | `MR/capabilities.rs`, `MR/lib.rs` `HostCall::capability` |
| `src/context.ts` | `ctx` for all three kinds, every host-call payload, `resolveHandler` | `BS:549-764` |
| `src/engine.ts` | One generation's engine: lookup, args/result schema, budgets, sticky denials, serialized ops, result caps, the dispatcher's error envelope | `V8/lib.rs` `execute`, `V8/isolate.rs` `host_call`/`decode`, `BS:766-807` |
| `src/schema.ts` | Portable schema validation with Rust's messages | `MR/lib.rs` `validate_at` |
| `src/errors.ts` | `ModuleError`, wire codes, error classes, sqlx error text | `MR/lib.rs`, `MH/protocol.rs`, `MH/session.rs`, `RT/error_class.rs` |
| `src/database-host.ts` | Query/Reducer host calls: `dbQuery/Insert/Update/Delete/DeleteMany`, `permissionsHas`, `actionEnqueue`, `scheduleAfter/At`, `storage.getMetadata`; admission, typing, the aborted-transaction rule, deterministic deferred ids | `RT/host_calls.rs` |
| `src/catalog.ts` | Table catalogs and the object-wide catalog cache | `RT/host_calls.rs` `catalog_table` |
| `src/sql.ts` | Admission, `$N` numbering, write targets, SELECT projection typing | `RT/sql_admission.rs`, `sql_projection.rs` (simplified) |
| `src/storage.ts` | The storage interface K04 needs from K01, plus S2's stand-in (contract-change note 0020) | `ST/lib.rs` transaction rules |
| `src/generations.ts` | Load, stage, activate, drain; pinned generations; the `system.reload` event | `MH/session.rs`, `server/src/lib.rs` registry |
| `src/invoke.ts` | `TenantModuleHost.run`: lease, storage transaction, host, engine, commit rules, error mapping | `RT/execution.rs` reducer/query paths |
| `test/` | workerd tests: the 34 vectors, host calls, transactions, budgets, the catalog cache, generations | |
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
- **Transactions (B.7).** One storage transaction per invocation (through K01). After a caught SQL error,
  later SQL fails with `current transaction is aborted…` until `ROLLBACK TO`, and the commit is refused.
  The deferred-FK check runs before the commit (G0-5): the object is not reset. A Query never commits.
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

Rust reads `pragma_table_info` and `_gonvex_record_views` once per invocation. In a Durable Object, S2
found these reads are 95–99 % of a Reducer's rows. Two lookups cause most of it:

- `EXISTS(… sqlite_schema …)` scans the whole schema;
- `_gonvex_record_views … COLLATE NOCASE` cannot use the BINARY primary key, so it scans that table for
  every table it looks up.

The cache lives in object memory and is keyed by an epoch:

- the first catalog need loads all record schemas in one scan;
- tables are cached on first use;
- a deploy (new generation) or `schemaChanged()` (migrations, K02) drops everything;
- an invocation that runs DDL, or writes `_gonvex_record_views`, bypasses the cache from that statement
  on and drops it when it ends, whether it commits or rolls back;
- missing tables are never cached.

**Measured on real Whagons Reducers** (whagons5-client `2457bb65f` split bundle, S2's schema subset and
35-step workload, local workerd, `node bench/run.mjs`). Each mode runs in a fresh tenant object, so the
cache-on column includes the object's one-time catalog load:

| Reducer (S2 step) | Rows read, cache off (catalog) | Rows read, cache on (catalog) | Fewer |
|---|---|---|---|
| `teams.create` (01, first call in the object) | 2,140 (2,104) | 548 (512) | 74 % |
| `spots.create` (08) | 2,179 (2,114) | 111 (46) | 95 % |
| `cleaningStatuses.create` (19) | 2,117 (2,088) | 49 (20) | 98 % |
| `cleaningStatuses.create` (22, warm) | 2,117 (2,088) | 29 (0) | 99 % |
| `cleaningReports.create` (24) | 450 (438) | 52 (40) | 88 % |
| `centers.askJev` (26) | 3,457 (3,342) | 154 (72) | 96 % |
| `settings.togglePlugin` (27) | 475 (420) | 77 (22) | 84 % |
| `boards.create` (28) | 851 (836) | 33 (18) | 96 % |
| `boards.createMessage` (29) | 1,721 (1,668) | 89 (36) | 95 % |
| `boards.createComment` (30) | 1,280 (1,250) | 44 (14) | 97 % |
| **The 10 committed Reducers** | **16,787** | **1,186** | **93 %** |
| All 37 calls (blocked and rollback steps included) | 54,545 | 2,124 | 96 % |

- The catalog rows left with the cache on are `pragma_table_info` on a table's first use in the object.
  They go to 0 once the cache is warm (step 22).
- Outcomes, rows written and the contents of all 29 application tables are identical with and without
  the cache.
- Median server time for the committed Reducers: 11.5 ms off, 10.5 ms on (local; the machine was
  shared).
- Same outcomes as S2: 10 of 25 distinct Reducers commit. The rest are blocked by schema objects DO
  SQLite refuses, until A01.
- Upload with the Whagons split bundle: 5,747 KiB, **1,016.7 KiB gzip**. The host alone: 104.8 KiB,
  27.0 KiB gzip (not minified).

**Queries.** No Whagons Query has a JavaScript handler. All 859 are replica, live or one-shot plans that
the runtime executes itself (K08/K10). Dispatching one through the module host gives Rust's
`invalid_artifact` outcome. See contract-change note 0020. The Query dispatch path is covered by the 10
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

Proposed for the lead (note 0020 §5):

3. The caller (K05 gateway, K07) records the command id on a fresh stub when it sees an overrun.
4. Set `limits.cpu_ms` as the backstop, ideally under about 4 s so that an overrun ends before the 7 s shed
   point. This conflicts with S1's 45 s-CPU migration transaction, so K02's chunk size must fit first.
5. Keep sockets in gateways.
6. The Worker Loader with `limits.cpuMs` is the only true per-call guard: no reset, about +1–2 ms p50.
   But host calls back into a tenant that holds its transaction deadlocked (0 completed), so it is an
   open question for S5.

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
| The real storage layer (FK check limited to the rows written, parameter limits, `$N` rewrite) | K01 |
| Reducer receipts (claim at the first write, stored result, replay) | K07 |
| The permission index behind `PermissionIndex` | K09 |
| `_gonvex_files` and R2 behind `FileMetadataSource` | K13 |
| Outbox and scheduler drain | K11 |
| The Action-side host (fetch, storage, tools, functions, secrets) | K12 |
| Sandbox | K20 |
| Sending `system.reload` / `client.updateRequired` to sockets | K05 |
| Building the Worker version with the prelude | K19 |
