# L09 candidate: `ctx.sandbox` on Dynamic Workers and Durable Object SQLite

K20 first built this as its implementation. Ruling 0048 §2.2 moved it here: it is the stage-3 L09 design, and
Gabriel decides whether to pull it forward. K20 itself ships the Container path (`cloudflare/sandbox/`, note 0047).
This is a self-contained spike with its own install, outside the pnpm workspace and CI, like S1–S5. It reuses
K20's contract layer (`cloudflare/sandbox/src`: service, payload checks, config, registry), so both
implementations answer the same checks with the same texts.

## What it is

| Part | Here | Instead of (K20) |
|---|---|---|
| Module code | One Dynamic Worker per run (Worker Loader, `globalOutbound: null`, no bindings, `limits.cpuMs` = timeout), TypeScript type-stripped by Sucrase | `gonvex-sandbox-worker` (V8, deno_ast) in a Container |
| `duckdb.query` / `register` | Durable Object SQLite, one object per sandbox, with a layer for the DuckDB syntax Whagons' own code emits: `catalog.table` imports, `$N`, several statements, `SUMMARIZE`, `DESCRIBE`, `SHOW TABLES` | DuckDB 1.4.1 |
| `importFile` | CSV streamed; XLSX/XLSM (fflate) and XLS (BIFF8) readers in TypeScript | Rust's `data_ingest.rs` (calamine) |
| Files, TTL | the workspace object's key-value storage; an alarm at `expiresAt` | the Container's disk |
| Admission | `RunSlots`, one object per tenant (0048 §2.3) | the manager's per-tenant semaphore |

## Measurements

From `results/platform.json` (measured on 2026-11-11 as `gonvex-cf-dev-k20-sandbox`, before the move; deleted
after the run):

| Measure | L09 candidate | K20 Container (0047 §3) |
|---|---|---|
| First run of a sandbox | 638 ms (Dynamic Worker load) | 1.1–1.9 s for a new tenant's first `create` (Container start), then 276–519 ms |
| Trivial run | 205 ms | 112–148 ms |
| 6.7 MB CSV (200,000 rows): import, GROUP BY | 1.9 s, 530 ms | 2.2 s, 181 ms |
| Heap limit | the platform's 128 MB | Rust's 64 MiB V8 cap |
| Deadline overshoot, 2 s timeout | 4.7–5.0 s for CPU and microtask spins; 12.4 s under a host-call flood | 2.14 s for a CPU spin |
| Cost | Workers CPU and Durable Object requests; no per-tenant idle cost | about $0.010 per hour while a tenant's Container runs |

## The seven scenarios on both

The same Whagons call-site code (`cloudflare/sandbox/test/whagons.ts`) runs here (`test/scenarios.test.ts`) and on
Rust with DuckDB (`cloudflare/sandbox/test/rust.test.ts`). The answers are the same except:

| Where | Rust with DuckDB | L09 |
|---|---|---|
| A boolean column of an import (`Closed`) | `true` / `false` | `1` / `0` |
| `SUMMARIZE` | `avg "20.0"`, `std "10.0"`, interpolated quartiles (`q25 "12"`, `q75 "28"`) | `"20"`, `"10"`, discrete quartiles (`"10"`, `"20"`) |
| Unqualified import table (`parseExcelAttachment`) | `Catalog Error: Table with name sales_2024_1 does not exist!` | `no such table: sales_2024_1` |
| Deadline text | the time left when the run started (`982 ms` of 1,000) | the configured time |
| Date columns | Debug text (`Date32(19724)` for 2024-01-02) | ISO text |

Table names, columns and row counts of the CSV, XLSX and XLS imports are identical. So are the query rows, the
snapshots, `runCode`, the `console.log` answers and the result-size text (aligned with Rust after this
comparison).

## Real model-written SQL (0048 §2.2)

`results/staging-sql.json` counts the assistant tool traces of every staging tenant. It holds aggregates only; the
read-only copies were deleted.

- 24 model-written statements, 10 distinct:
  - 8 run on both DuckDB 1.4.1 and SQLite;
  - 2 use `read_json_auto`, which fails on Rust too: its DuckDB has no json extension, and autoload is off.
- **No statement uses DuckDB-only syntax that Rust runs and SQLite refuses.**
- On staging, no current-era DuckDB call has succeeded:
  - `analyzeInSandbox`: 0 of 19. 15 failed with Rust's chroot "Permission denied" (0047 §2b), 2 with "DuckDB
    analysis is unavailable", 2 because the file was not ready.
  - The image staging runs has no DuckDB (0047 §2a).

The sample is small. Most traffic in the traces came from the earlier Go/JS sandbox tools.

## Run

```sh
cd cloudflare/spikes/l09-sandbox
pnpm install                 # its own lockfile; reflinked from the global store
pnpm test                    # 48 tests in workerd (local workerd enforces neither cpuMs nor isolate memory)
pnpm typecheck
whagons-dev credentials exec full-cloudflare-access -- node dev/platform.mjs   # deploys gonvex-cf-dev-l09-sandbox, probes, deletes
node scripts/make-fixtures.mjs   # rebuilds the spreadsheet fixtures in cloudflare/sandbox/test/fixtures (needs soffice)
```
