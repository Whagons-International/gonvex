# 0040: K20 sandbox on Dynamic Workers and SQLite, not a Container

Date: 2026-11-11. Status: **proposed** (K20). Affects: K20, L09, K12 (wiring), K19 (the release builder's Container),
CONTRACT.md B.1/B.2a `sandbox` rows ("adapt: Container").

Number: 0039 is reserved for K12 (0037). This is the next free number.

## 1. Decision

`ctx.sandbox` runs on **Dynamic Workers (Worker Loader, `globalOutbound: null`) and Durable Object SQLite**:
option (b) of the K20 brief. A Container (option a) is not built. The `duckdb` binding keeps its name and its
`query`/`register` calls, and executes on SQLite. Everything stays behind one interface, `SandboxBackend`
(`cloudflare/sandbox/src/backend.ts`), so a Container backend can be added without touching the contract layer.

The brief's rule was: if (b) covers every Whagons call site, prefer it. It does (§3).

## 2. The two options

| | (a) Container running Rust's sandbox worker | (b) Dynamic Worker + DO SQLite (built) |
|---|---|---|
| Parity | Exact: same binary, DuckDB, deno_ast, calamine | Same contract surface. Seven deviations, all listed in §5 |
| What must be built | An image with `sandbox-worker --features duckdb` (V8 + bundled DuckDB, a long C++ build), plus a manager server for the workspaces, TTLs, counts and admission that `SandboxManager` does in-process (`sandbox.rs` is part of the runtime crate) | About 2,800 lines of TypeScript in `cloudflare/sandbox/src` |
| Start | A container cold start per instance (seconds), then about 5 ms per spawn | 640 ms for the first run of a sandbox; 205 ms for a later trivial run (platform, §6) |
| Isolation | chroot, uid drop and seccomp inside the container (needs root and a seccomp-capable runtime), `enableInternet: false` | V8 isolate per execution, no bindings, `globalOutbound: null`; per-sandbox objects named by project, tenant and account. Proven on the platform |
| Limits | rlimits and the V8 watchdog, as Rust | `limits.cpuMs`, the platform's 128 MB, caller-side deadline, host-side caps. Proven on the platform |
| Files and TTL | Container disk, lost when the instance sleeps | Object storage, deleted by an alarm at `expiresAt` |
| R2 for `importFile` | A Container cannot bind R2 (0037 K19 §1), so bytes are pushed in | Streamed from R2 into the analysis object |
| Product direction | DuckDB and the Container are dropped after migration (L09) | Already the L09 shape: JavaScript plus SQLite. L09 is left with renaming |

## 3. Whagons call sites (whagons5-client staging `a6de676951`)

Every call site runs in `cloudflare/sandbox/test/scenarios.test.ts`, with the app's own helper code copied into
`test/whagons.ts`.

| Call site | What it needs | Covered by |
|---|---|---|
| `assistant/executionContext.ts` `analyzeWorkspaceTasks` | create, writeText, run with `files.readText`, `duckdb.register`, a GROUP BY; optional importFile | yes |
| `executionContext.ts` `runSandboxCode` / `waitForSandbox` | run, status, cancel; result or `console.log` lines | yes, including the logs fallback |
| `executionContext.ts` `createTaskSnapshotter` + completion `analyzeInSandbox` | writeText per snapshot; TypeScript reading `files.readText(path)` | yes |
| completion `readConversationFile`, `inspectDataFile` (sample) | `SELECT * FROM "import_N"."table" LIMIT … OFFSET …` | yes: catalog-qualified names map to the stored import table |
| completion `queryDataFile` | the model's SELECT/WITH, qualified by `qualifyImportedTables` | yes for SQL that SQLite also accepts; DuckDB-only syntax fails (§5a) |
| completion `profileDataFile` | `SUMMARIZE SELECT * FROM …` | yes: computed, with DuckDB's 12 columns and formats |
| completion `getSharedSandbox` / finally | create with a 10-minute TTL, destroy | yes |
| `assistantData.ts` `ingestAttachment` | create, importFile → tables, columns, row counts | yes for CSV, TSV, XLSX, XLSM and XLS: every type the client uploads (`DATA_ATTACHMENT_EXTENSIONS`) |
| `assistantExcelActions.ts` `parseExcelAttachment` | importFile of an .xlsx, then `runSandboxQuery` | import yes. Its query fails on both runtimes (§7) |
| `agentToolRuntime.ts` `runCode` | create with a 5-minute TTL, TypeScript run, destroy | yes |

## 4. How the contract maps

- One `SandboxWorkspace` object per sandbox, named `["project","tenant","account"]/sbx_<uuid>`. The stored scope is
  checked again on every call. Files, execution records and results live in its key-value storage, which SQL cannot
  reach. Its alarm deletes everything at `expiresAt`.
- One `SandboxAnalysis` object per sandbox, holding only module tables. Imports are stored as `"import_N.<table>"`.
- One `SandboxRegistry` object per deployment, for `max_total`, `max_per_account` and the `concurrency` run slots.
  Entries carry their own expiry, so a dead run or workspace stops counting by itself.
- A run loads one Dynamic Worker with Rust's TypeScript wrapper, type-stripped by Sucrase. Its only capability is an
  RPC target scoped to that execution, closed when the execution ends.

## 5. Deviations (each needs a ruling)

| # | Deviation | Rust | Kernel | Why |
|---|---|---|---|---|
| a | SQL dialect | DuckDB 1.4.1 | SQLite. Bridged: `catalog.table` and `catalog.main.table` for imports, `$N`, several statements (all run, the last answers, as duckdb-rs `prepare`), `SUMMARIZE`, `DESCRIBE`, `SHOW TABLES` | Product direction (L09). Whagons' own SQL is fully covered. Model-written DuckDB-only syntax (`::`, `ILIKE`, `date_trunc`, `QUALIFY`, list functions) fails with a SQLite error, which the agent's instructions already tell it to correct. The tool description "Run a read-only, bounded DuckDB SELECT" (`assistantServerTools.ts:411`) should say SQLite at cutover. That is an app change, so it waits for cutover. |
| b | Value types | DATE/TIMESTAMP answered as Debug text (`Date32(19723)`); BOOLEAN as `true`/`false`; a value that does not fit the sniffed type fails the import | DATE/TIMESTAMP as ISO text; BOOLEAN as 1/0; a non-fitting value stays text. Sniffing samples 20,000 rows instead of 100,000. `SUMMARIZE` quantiles are exact, not approximate | SQLite has no such types. Rust's Debug dates are a defect nobody can rely on |
| c | Columns per table | 200 per sheet; no CSV limit | 100 per table (sheet: "exceeds the column limit"; CSV and `register`: a clear text) | DO SQLite limit |
| d | XLSB and ODS | supported by calamine | refused: "unsupported data file type .xlsb on this runtime …" | The Whagons client uploads only csv, tsv, xlsx, xlsm and xls |
| e | Heap | 64 MiB V8 cap | the platform's 128 MB; the error names 134217728 | A Dynamic Worker cannot be capped lower |
| f | Workspace bytes | `directory_size` of the worker root (files, imports, DuckDB files) | files plus the analysis database size; table growth beyond the budget rolls back with "sandbox workspace byte limit exceeded" | No file system |
| g | Counts and run slots | per runtime process | per deployment, in one registry object | Same numbers. **Question for the lead:** the global `concurrency` of 2 protected one server's CPU; on Workers it only queues runs. Keep it for parity, or make it per tenant? |
| h | Deadline under a host-call flood | V8 watchdog at the timeout | `cpuMs` stops the isolate. A run that floods host calls queues them on the workspace object: 12.4 s for a 2 s timeout on the platform (pure spins: 4.7–5.0 s) | Platform scheduling |
| i | TypeScript | deno_ast (swc) | Sucrase. Same wrapper and same "TypeScript parse failed:" prefix; the parser's message and stack frames differ | No compiler in a Worker otherwise; deno_ast is not available to JavaScript |
| j | Lifetime of state | in memory: a runtime restart loses every sandbox | object storage: survives eviction. A run whose object instance died reads `cancelled` / "sandbox execution cancelled", as Rust's `ExecutionOwnership` does | Durability is free here |

## 6. Consequences for other packages

- **K12** (merged): `configureActions({ sandbox: (env) => sandboxFromEnv(env, files) })`; `test/action.test.ts` runs an
  agent Action through K12's runtime with this service. `files` is K13's `ActionFiles` as it is, or the streaming
  `r2FileSource`.
- **K19** declares the Worker Loader binding, the three classes and `limits.cpu_ms = 120000` on the Worker that hosts
  them. One analysis statement is bounded by that CPU limit, and the run deadline fires independently in the
  workspace. 0021/0022 set 5,000 ms for the tenant script, so the sandbox classes need their own script, or the
  tenant script's limit needs a ruling.
- **K19's release builder** (0028 §1, 0036 §1) planned to reuse "the K20 container infrastructure". There is none
  now. The builder needs its own Container, or A01's reduced-memory transform in a Workflow step.
- **L09** shrinks to renaming the binding and removing the compatibility layer (`src/sql.ts`) once the app stops
  saying DuckDB. It still depends on the app change at cutover.
- **Conformance:** like K13 (0032), there is no harness scenario yet. The harness runs no Actions on the kernel, and
  Rust's sandbox needs the worker binary with DuckDB. V01 adds it; the workerd suite stands in.

## 7. Findings in Whagons (reported, not changed)

1. `parseExcelAttachment` queries `SELECT … FROM "<sheet table>"` without the import alias. DuckDB 1.4.1 answers
   `Catalog Error: Table with name sales_2024_1 does not exist! Did you mean "import_1.sales_2024_1"?` (checked with
   DuckDB 1.4.1, the version Rust links). The query runs outside the action's try, so the action fails on staging
   today.
2. Qualified, the same query would still yield no rows. `runSandboxQuery` keeps `state.result` only when it is an
   array, and `duckdb.query` answers an object (`{columns, rowCount, rows, truncated}`).
3. Its 2,000-row chunks exceed the 500-row query cap, so each chunk is truncated.

## 8. Platform evidence

`cloudflare/sandbox/dev/results/platform.json`, from `gonvex-cf-dev-k20-sandbox`, which was deleted after the run.
The summary is in `cloudflare/sandbox/README.md` "Platform results".
