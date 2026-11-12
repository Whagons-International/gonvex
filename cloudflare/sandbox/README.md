# Sandbox (K20)

`ctx.sandbox` for Actions on Cloudflare, by the stage-1 contract (CONTRACT B.2a, ruling 0048 §2.1): Rust's sandbox
worker in a Container, DuckDB included, with Rust's limits. It covers:
- the ten calls: `create`, `destroy`, `run`, `cancel`, `status`, `readFile`, `readText`, `writeFile`, `writeText`
  and `importFile`;
- per-workspace files with TTLs;
- the `duckdb` binding inside a run.

The reference is `rust/crates/runtime/src/sandbox.rs`, `data_ingest.rs` and `rust/crates/sandbox-worker/` at
`0bf3ad13`. Findings, numbers and the L09 candidate are in note
`docs/cloudflare/contract-changes/0047-k20-sandbox-findings.md`.

## Layout

| Path | What | Rust reference |
|---|---|---|
| `src/service.ts` | `SandboxService.call(operation, payload, session, duckdb)`: K12's `SandboxPort`. Rust's payload checks, policy and texts, in Rust's order; `importFile`'s read through K13's `ActionFiles.readFile` | `Runtime::sandbox_call`, `SandboxManager::call`, `create`, `run`, `read_file`, `write_file` |
| `src/backend.ts` | `SandboxBackend`, the interface between the contract and what runs code (the L09 candidate implements it too) | — |
| `src/container-backend.ts` | `ContainerBackend`: the registry's counts, then the tenant's Container | — |
| `src/container.ts` | `SandboxContainer`: one Durable Object and Container per tenant; starts it with no Internet, forwards operations, stops it when no workspace is alive | — |
| `src/registry.ts` | `SandboxRegistry`: the deployment's `max_total` and `max_per_account` | `SandboxManager` counts |
| `src/config.ts`, `src/payload.ts` | `GONVEX_SANDBOX_*` with Rust's names and defaults; `exact_fields`, `as_u64`, path and base64 rules | `config.rs:108-158`, `sandbox.rs:712-752` |
| `container/Dockerfile` | The image: `gonvex-sandbox-worker --features duckdb` (unchanged), `k20-ingest`, the manager | `Dockerfile.runtime` |
| `container/ingest/` | `k20-ingest`: Rust's `data_ingest.rs` compiled unchanged (`#[path]`), its lock seeded from `rust/Cargo.lock` | `data_ingest.rs` |
| `container/manager.mjs` | `SandboxManager` as a small HTTP server: workspaces, TTL prune, per-tenant admission, one worker process per run with Rust's request JSON, the ingest | `sandbox.rs` |
| `test/` | workerd tests against the manager, which runs with the Rust binaries, the image, or protocol stubs (below) | |
| `dev/` | Platform proof: a Worker in its own script with the Container, a probe route, `platform.mjs` (deploy, probe, delete), `results/platform.json` | |

## Wiring (K22)

```ts
// The sandbox Worker, its own script (0048 §2.3), wrangler.jsonc as dev/wrangler.jsonc:
//   "containers": [{ "class_name": "SandboxContainer", "image": "<path>/container/Dockerfile",
//                    "image_build_context": "<repository root>", "instance_type": "basic" }],
//   durable_objects: SANDBOX_CONTAINER → SandboxContainer, SANDBOX_REGISTRY → SandboxRegistry,
//   vars: GONVEX_SANDBOX_ENABLED = "true" (and any GONVEX_SANDBOX_* override)
export { SandboxContainer, SandboxRegistry } from "../sandbox/src/index";

// The Action runtime (K12, actions/src/worker.ts); files is K13's ActionFiles for the tenant:
configureActions({ ..., sandbox: (env, tenantId) => sandboxFromEnv(env, { readFile: (s, id, max) => new ActionFiles({ tenant: env.TENANT.getByName(tenantId), bucket: env.FILES, config }).readFile(s, id, max) }) });
```

## How a call runs

```
K12 host call ─► SandboxService (Rust's checks and texts) ─► ContainerBackend
     create ─► SandboxRegistry.admit (128 total, 4 per account) ─► SandboxContainer [project, tenant]
                                                                    │ start({ enableInternet: false }) if stopped
                                                                    ▼
                                    manager.mjs: workspaces /var/lib/gonvex/sandboxes/<id>/{files,imports}
     run        ─► per-tenant admission (concurrency 2) ─► gonvex-sandbox-worker (chroot, uid 65534, seccomp,
                   V8 64 MiB, DuckDB 128 MiB, the deadline) ─► execution record
     importFile ─► K13 ActionFiles.readFile (IB1) ─► bytes streamed ─► k20-ingest ─► imports/import_N.duckdb
```

- The manager keeps Rust's behaviour, with one exception (0047 §2b): it chowns each workspace to the worker's uid.
  Without that, the chrooted worker cannot create `analysis.duckdb`, which is the "Permission denied" every
  current `analyzeInSandbox` call on staging hit.
- Workspaces live on the Container's disk. The object stops the Container 60 s after its last workspace is
  destroyed or expires. A Container that stops loses its workspaces, as a Rust runtime restart does.

## Tests

`pnpm test` runs `vitest run --root sandbox`. `test/global-setup.mjs` decides what the tests' Container object
talks to:

| Setting | Manager | Run |
|---|---|---|
| none (CI) | the manager here, with the protocol stubs in `test/stub/` | 15 tests; 10 need Rust and skip |
| `K20_SANDBOX_BIN=<dir>` with `gonvex-sandbox-worker` and `k20-ingest` | the manager here, with the Rust binaries (unconfined: no root) | 24 tests |
| `K20_SANDBOX_URL=http://127.0.0.1:<port>` | the image under `docker run` (root, chroot, seccomp) | 24 tests |

```sh
docker build -f cloudflare/sandbox/container/Dockerfile -t gonvex-cf-dev-k20-sandbox:local .   # repository root, ~10 min cold
docker run -d --name k20 -p 127.0.0.1:47821:8080 gonvex-cf-dev-k20-sandbox:local
cd cloudflare && K20_SANDBOX_URL=http://127.0.0.1:47821 npx vitest run --root sandbox
```

- `contract.test.ts` (every mode): payload checks and policy; counts per account, total and TTL expiry; code,
  timeout, execution, file and workspace caps; paths; cancel; the deadline; run slots per tenant; isolation by
  account, tenant and project; imports through K13's real `ActionFiles` with a private file of another account
  refused (IB1). In stub mode it also checks that the worker request is Rust's, field by field.
- `rust.test.ts` (Rust only): the seven Whagons call sites with the app's own helper code (`test/whagons.ts`, from
  whagons5-client staging `a6de676951`):
  - `analyzeWorkspaceTasks`, with and without an upload;
  - `ingestAttachment` for CSV, XLSX and XLS;
  - the shared-sandbox read, sample, query and `SUMMARIZE`;
  - snapshots with `analyzeInSandbox`;
  - `runCode`;
  - `parseExcelAttachment`.

  It also covers the V8 heap, deadline, result size and row caps with Rust's texts; no network and no DuckDB file
  access; and an agent Action through K12's runtime.

## Platform results and numbers

`whagons-dev credentials exec full-cloudflare-access -- node sandbox/dev/platform.mjs` (from `cloudflare/`, needs
docker) deploys `gonvex-cf-dev-k20-sandbox` with a `basic` Container (1/4 vCPU, 1 GiB, 4 GB), runs the probes,
writes `dev/results/platform.json`, and deletes the Worker and its Container application. Results from 2026-11-12:

| Measure | Result |
|---|---|
| Cold start (Container to manager listening) | 547–1,036 ms; the first `create` of a new tenant 1.1–1.9 s |
| First run / later trivial run | 276–519 ms / 112–148 ms |
| DuckDB register 1,000 + GROUP BY / `SUMMARIZE` | 213 / 288 ms |
| Import CSV / XLSX / XLS | 848 (first DuckDB load) / 200 / 193 ms |
| 6.7 MB CSV, 200,000 rows: import, GROUP BY | 2.2 s, 181 ms |
| Deadline 1 s / CPU spin 2 s / 64 MiB heap | 1.12 s / 2.14 s / 410 ms, each with Rust's text |
| Network, DuckDB file access, another account or tenant | refused / refused / "sandbox was not found" |
| Memory | manager 57–77 MB RSS; worker peak 49 MB (trivial) to 56 MB (DuckDB), measured locally |
| Cost | $0.00000278/s while a tenant's Container runs (about $0.010/hour); CPU ≤ $0.000001 per DuckDB run; about $0.0005 for a 2-minute assistant turn |

The Worker side adds no dependency and is a few kilobytes; the image is 380 MB. The sandbox reads and writes no
tenant SQLite. Per call, the registry writes one entry at create and deletes it at destroy.
