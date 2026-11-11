# Sandbox (K20)

`ctx.sandbox` for Actions on Cloudflare: the ten calls (`create`, `destroy`, `run`, `cancel`, `status`, `readFile`,
`readText`, `writeFile`, `writeText`, `importFile`), per-workspace files with a TTL, Rust's execution caps, and the
`duckdb` binding (`query`, `register`) inside a run. The reference is `rust/crates/runtime/src/sandbox.rs`,
`data_ingest.rs` and `rust/crates/sandbox-worker/` at `0bf3ad13`; the contract rows are CONTRACT.md B.2a `sandbox`.

**Backend: Dynamic Workers and Durable Object SQLite, no Container.** Note
`docs/cloudflare/contract-changes/0040-k20-sandbox-on-dynamic-workers.md` records the evaluation of both options, why
this one covers every Whagons call site, and each deviation from Rust.

## Layout

| File | What | Rust reference |
|---|---|---|
| `src/service.ts` | `SandboxService.call(operation, payload, session, duckdb)`: K12's `SandboxPort`. Payload checks, policy and texts in Rust's order; `importFile`'s storage read | `Runtime::sandbox_call`, `SandboxManager::call`, `create`, `run`, `read_file`, `write_file` |
| `src/backend.ts` | `SandboxBackend`, the one interface between the contract and what runs code. A Container backend would implement it | — |
| `src/dynamic.ts` | `DynamicWorkerBackend`: routes each call to the sandbox's objects | — |
| `src/workspace.ts` | `SandboxWorkspace` (one per sandbox): files, TTL alarm, executions, the run in a Dynamic Worker, `ExecutionHost` (what module code may call) | `Workspace`, `run`, `execute_worker`, `WorkerHost::dispatch` |
| `src/analysis.ts` | `SandboxAnalysis` (one per sandbox): the `duckdb` binding on SQLite, imports, `SUMMARIZE`/`DESCRIBE`/`SHOW TABLES` | `WorkerHost::query`, `register` |
| `src/registry.ts` | `SandboxRegistry` (one per deployment): `max_total`, `max_per_account`, the run slots (`concurrency`) | `SandboxManager` counts, `admission` |
| `src/program.ts` | Rust's TypeScript wrapper (console, `files`, `duckdb`), type-stripped by Sucrase; the Dynamic Worker's entry module | sandbox-worker `transpile` |
| `src/sql.ts` | Statement split, `$N`, `"import_1"."data"` → the stored import table | — |
| `src/ingest/` | CSV/TSV/TXT (streamed), XLSX/XLSM (sheets inflated as streams), XLS (BIFF8) into tables | `data_ingest.rs`, calamine 0.36 |
| `src/config.ts` | `GONVEX_SANDBOX_*` with Rust's names and defaults (disabled by default) | `config.rs:108-158` |
| `src/files.ts` | `r2FileSource`: `importFile`'s read through K13's owner check, streamed from R2 | `StorageService::read_file` |
| `test/` | workerd tests (below) and the Whagons call-site code they run (`test/whagons.ts`) | |
| `dev/` | Platform proof: a Worker with a probe route, `platform.mjs` (deploy, probe, delete), `results/platform.json` | |
| `scripts/make-fixtures.mjs` | Builds `test/fixtures/workbook.xlsx`, `.xls` (LibreOffice) and `workbooks.ts` | |

## Wiring it in (K12 and K19)

```ts
// Worker that hosts the objects (wrangler.jsonc): a Worker Loader and three SQLite-backed classes.
//   "worker_loaders": [{ "binding": "LOADER" }],
//   durable_objects: SANDBOX_WORKSPACE → SandboxWorkspace, SANDBOX_ANALYSIS → SandboxAnalysis, SANDBOX_REGISTRY → SandboxRegistry
//   "limits": { "cpu_ms": 120000 }   // the longest run timeout; bounds one analysis statement (note 0040 §6)
//   vars: GONVEX_SANDBOX_ENABLED = "true" (and any GONVEX_SANDBOX_* override)
export { SandboxAnalysis, SandboxRegistry, SandboxWorkspace } from "../sandbox/src/index";

// The Action runtime (K12, actions/src/worker.ts):
configureActions({ ..., sandbox: (env) => sandboxFromEnv(env, files) });
// files: K13's `new ActionFiles({ tenant, bucket, config })` as it is, or the streaming
// `r2FileSource({ tenant: (s) => env.TENANT.getByName(s.tenantId), bucket: env.FILES })`.
```

## How a call runs

```
K12 host call ─► SandboxService ─(checks, Rust's order and texts)─► DynamicWorkerBackend
                                                                     │  name = [project, tenant, account] / sbx_<uuid>
     create ─► SandboxRegistry.admit (128 total, 4 per account) ─► SandboxWorkspace.create (alarm at expiresAt)
     run    ─► SandboxWorkspace: record ─► registry run slot ─► Sucrase ─► LOADER.load({ globalOutbound: null,
                env: {}, limits: { cpuMs: timeoutMs } }) ─► runner.run(ExecutionHost) ─┬─ files.* → workspace KV
                                                                                       └─ duckdb.* → SandboxAnalysis SQL
                the answer, the deadline timer, an expired host call or a cancel ends it; the host is closed
     importFile ─► K13 owner check, bytes from R2 ─► SandboxAnalysis.ingest ─► tables "import_N.<table>"
```

- **Files and records** are in the workspace object's key-value storage, in 512 KiB chunks. SQL cannot read `_cf_KV`,
  so module SQL never reaches them.
- **The run** executes in a Dynamic Worker loaded for that one execution. Its only capability is the `ExecutionHost`
  it receives. It has no network (`globalOutbound: null`), no bindings, no `eval` and no cache.
- **The `duckdb` binding** runs in a separate object, so a long statement never blocks the workspace. If a statement
  exceeds the CPU limit, only that object is reset: the open statement rolls back and committed tables stay.
- **The TTL**: every call refuses an expired sandbox ("sandbox was not found"). The alarm then deletes both objects'
  storage and the registry entry. A running execution delays the deletion until it ends, as Rust's prune does.

## Behaviour

**Kept from Rust:**
- Every operation's payload fields and checks, in Rust's order and with Rust's texts (`exact_fields`, `required_string`,
  `as_u64` fallbacks to the defaults, path rules, base64, UTF-8).
- All limits (table below); the execution JSON and its statuses; one active execution per sandbox; and the cancel
  semantics (the answer still says `running`).
- The 4,000-character error bound.
- The worker's wrapper (`console` with 500 lines of 16,384 characters, `files`, `duckdb` only when declared), the
  bootstrap's argument checks, and `ctx.sandbox.*` inside a run answering `unsupported sandbox worker operation`.
- The V8 engine's error texts for a thrown error, a result over the cap and the deadline.
- The worker's errno texts for files.
- `importFile`'s order (storage read, name, size, then the sandbox). Alias numbering (`import_N`). Sheet table names
  and limits. `register`'s types and errors. Query rows capped at `maxRows` with `truncated`. duckdb-rs running every
  statement and answering the last.

**Platform-forced or intended (note 0040):**
- The memory limit is the platform's 128 MB.
- Tables hold at most 100 columns.
- XLSB and ODS are refused (Whagons never sends them).
- DuckDB-only SQL that Whagons' own code does not emit fails with a SQLite error.
- Dates are ISO text instead of `Date32(…)`, and booleans are 1/0.
- Executions survive an object eviction.

| Limit (Rust default) | Mechanism | Proven |
|---|---|---|
| disabled by default | `GONVEX_SANDBOX_ENABLED` | test |
| TTL 30 min, max 2 h | `expiresAt` check on every call; alarm deletes | test (expiry and wipe) |
| 128 total, 4 per account | registry object | test |
| 16 executions | workspace record | test |
| run timeout 30 s, max 120 s | caller-side timer + expired host calls + `limits.cpuMs = timeoutMs` | test (timer); platform (CPU, microtask and host-call spins) |
| heap 64 MiB | platform isolate limit, 128 MB | platform |
| code 512 KiB, file 64 MiB, workspace 256 MiB | service and host checks; SQL growth checked in its transaction | test |
| output 8 MiB, rows 500 | runner and manager caps; cursor stops at `maxRows` | test |
| concurrency 2 | registry run slots | test |
| no network | `globalOutbound: null` | test and platform |

## Tests

`pnpm test` runs `vitest run --root sandbox`: 48 tests in workerd with real objects, a real Worker Loader and local R2.

- `scenarios.test.ts`: **the assistant data-analysis scenarios.** Every Whagons call site runs the app's own helper
  code, copied from whagons5-client staging `a6de676951` into `test/whagons.ts`:
  - `analyzeWorkspaceTasks`, with and without an upload;
  - `ingestAttachment` for CSV, XLSX and XLS;
  - the shared-sandbox data-file tools: `readConversationFile`, `inspectDataFile`, `queryDataFile` (with
    `qualifyImportedTables`) and `profileDataFile` (`SUMMARIZE`);
  - `snapshotTasksData` with `analyzeInSandbox`, including the `console.log` answer path and TypeScript errors;
  - the cloud agent's `runCode`;
  - `parseExcelAttachment` (see note 0040 §7).
- `action.test.ts`: `ctx.sandbox` end to end. K12's `ActionRuntime` and K04's module host run an agent Action whose
  `sandbox` port is this package's service: an analysis with an upload, and Rust's host-call error text, including
  "sandbox importFile also requires the storage capability".
- `isolation.test.ts`: another account, tenant or project reaches nothing, by any operation. Two sandboxes of one
  account share nothing. A private file of another account is refused (IB1). Paths stay inside the workspace. A run
  has no fetch, `connect`, `eval`, `new Function` or cache. SQL cannot read `_cf_KV`, `ATTACH`, `PRAGMA` or files. A
  run past its deadline can no longer write.
- `limits.test.ts`: every policy and cap above, with Rust's texts and order.
- `ingest.test.ts`: the CSV dialect, the XLSX and XLS readers against the fixtures, Rust's number and name formats,
  and atomic imports.
- `numbers.test.ts`: rows read and written per analysis call.
- `sql.test.ts`: the statement rewrite (import names, `$N`, statement split) and the DuckDB-only statements.

Local workerd enforces neither `limits.cpuMs` nor isolate memory. A pure spin locally never ends, so the suite has no
such test; `dev/` proves both on the platform.

## Platform results

```sh
cd cloudflare
whagons-dev credentials exec full-cloudflare-access -- node sandbox/dev/platform.mjs
```

This deploys `gonvex-cf-dev-k20-sandbox`, runs the probes through the real service, writes `dev/results/platform.json`
and deletes the Worker. Results from 2026-11-11:

| Probe | Result |
|---|---|
| `fetch`, `connect`, `eval` in a run | refused: "This worker is not permitted to access the internet…", "Code generation from strings disallowed" |
| another account / another tenant | "sandbox was not found"; the owner still reads its file |
| `await` past a 1 s timeout | failed after 1.2 s, `module exceeded its 1000 ms execution deadline` |
| `for (;;) {}`, 2 s timeout | stopped by `cpuMs`, failed in 4.7 s with the deadline text |
| `for (;;) await null`, 2 s | 5.0 s, deadline text |
| `for (;;) await files.list()`, 2 s | 12.4 s, deadline text: the host calls queue on the workspace object (note 0040 §5h) |
| the same sandbox after a spin | the next run succeeds in 215 ms |
| 60 MB allocated / 400 MB allocated | succeeds / `module exceeded the 134217728 byte isolate heap limit` |
| create; first run (register 1,000 tasks, GROUP BY) | 638 ms for the first run, including the Dynamic Worker load |
| a trivial run | 205 ms |
| import CSV (100 rows) / XLSX / XLS | 160 / 155 / 149 ms |
| import a 6.6 MB CSV (200,000 rows), then GROUP BY | 1.9 s, then 530 ms |

## Numbers

- **Bundle** (esbuild, `src/index.ts`): 51.7 KB minified (17.8 KB gzip) of this package. Sucrase and fflate bring it to
  264 KB (68 KB gzip). The dev Worker's upload is 613 KiB (115 KiB gzip) with source maps and fixtures.
- **Rows on the analysis object** (`numbers.test.ts`; writes include index entries):

| Call | Statements | Rows read | Rows written |
|---|---|---|---|
| `register` 1,000 rows | 3 | 1,001 | 1,002 |
| `GROUP BY` over 1,000 rows | 1 | 2,000 | 0 |
| point query on a registered table (no index, as in DuckDB) | 1 | 1,000 | 0 |
| `SUMMARIZE` of 3 columns, 1,000 rows | 8 | 11,000 | 0 |
| import a 100-row CSV | 2 | 101 | 102 |
| import the fixture XLSX (2 sheets) | 4 | 8 | 10 |

- **Tenant object**: none. The sandbox never touches tenant SQLite. The workspace object uses key-value storage only:
  per file write, one meta write and one entry per 512 KiB chunk; per run, one record write at the start and one at
  the end, plus one meta write each time.
- **Memory**: a CSV import streams, so its memory is flat. An XLSX or XLS import holds the compressed file (64 MiB at
  most) and its shared strings. Worksheet XML is inflated as a stream, twice.
