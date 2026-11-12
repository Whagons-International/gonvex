# 0047: K20 sandbox findings — the Container path, and Dynamic Workers as the L09 candidate

Date: 2026-11-12. Status: **proposed** (K20), after the rulings in 0048 §2. Affects: K20, L09, K19 (the builder
Container), K22 (wiring), V01 (conformance). Renumbered from 0040 (0048).

## 1. What K20 delivers (0048 §2.1)

`ctx.sandbox` runs Rust's sandbox worker in a Cloudflare Container, with DuckDB and Rust's limits.

- **The image** is `cloudflare/sandbox/container/Dockerfile`. It holds three things:
  - `gonvex-sandbox-worker`, built unchanged with `--features gonvex-sandbox-worker/duckdb`, exactly as
    `Dockerfile.runtime` builds it with `GONVEX_DUCKDB=true`;
  - `k20-ingest`, which is Rust's `rust/crates/runtime/src/data_ingest.rs` compiled unchanged through `#[path]`.
    Its lockfile is seeded from `rust/Cargo.lock`, so calamine 0.36.1 and DuckDB 1.4.1 match the runtime;
  - `manager.mjs`, a port of `SandboxManager` (`sandbox.rs`). It keeps the same workspace layout, request JSON,
    limits, texts and order, and spawns one worker per execution.

  No Rust source changes.
- **One Container per tenant**: a `SandboxContainer` Durable Object, named `[project, tenant]`.
  - It starts with `enableInternet: false`. Inside, the worker keeps Rust's chroot, uid 65534 and network seccomp.
  - Workspaces live on the Container's disk.
  - The object stops the Container 60 s after its last workspace has gone (destroyed or expired). A stopped
    Container loses its workspaces, as a Rust runtime restart does.
- **The Worker side** has three parts:
  - `SandboxService` (K12's `SandboxPort`) makes Rust's payload checks, in Rust's order and with Rust's texts;
  - `ContainerBackend` forwards each operation;
  - `SandboxRegistry` (one object per deployment) enforces `max_total` 128 and `max_per_account` 4.
- **Imports** go through K13's `ActionFiles.readFile` (`kernel/files/rpc.ts`) and its owner checks (IB1). The
  `ActionSession` maps to K13's `FilesSession` as the account id plus the tenant Member id. Bytes stream to the
  Container, so a 64 MiB import never becomes one RPC argument. The local `FilesReadTarget` and its cast are gone.
- **Accepted in 0048 §2.3:**
  - run concurrency is **per tenant**: the manager's semaphore is keyed by tenant;
  - the sandbox runs in **its own script** (`cloudflare/sandbox/dev/wrangler.jsonc` shows it);
  - the tenant script stays at 5,000 ms.

## 2. Findings (rulings requested)

| # | Finding | Proposal |
|---|---|---|
| a | **Staging runs no DuckDB.** The image staging runs (`gonvex-runtime:84bb4faf…`, deployed 2026-11-10) answers `DuckDB analysis is unavailable in this build; enable the duckdb Cargo feature`. Coolify sets no `GONVEX_DUCKDB`, so `Dockerfile.runtime` defaults it to `false`. Staging and production-v2 enable the sandbox; production (`gonvex.whagons.com`) leaves it disabled. Every Whagons call site declares `sandbox: { duckdb: true }`, so **on staging every assistant sandbox `create` fails today**. Traces confirm it: 14 `snapshotTasksData`, 1 `inspectDataFile` and 1 `readConversationFile` failed with that text (`spikes/l09-sandbox/results/staging-sql.json`). | K20 builds DuckDB in, as 0048 rules: the Container path matches the contract, not staging's build. If the lead wants staging's observable behaviour instead, one build argument drops the feature. |
| b | **Rust's chroot cannot write its own workspace.** The runtime creates `<root>/<id>` as root. The worker chroots into it and drops to uid 65534, so it cannot create `analysis.duckdb` or write `files/`. In the K20 image, a root-owned workspace reproduces the exact staging error: `IO Error: Cannot open file "./analysis.duckdb": Permission denied`. That is 15 of the 19 `analyzeInSandbox` calls on staging (the other 4 failed for other reasons, so none succeeded). | The manager chowns the workspace root and `files/` to 65534 (and files the host writes). Without that, no DuckDB run works under chroot. This deviates from Rust's code, toward Rust's intent; it needs a ruling. |
| c | Rust's deadline text names the time **left** when the run started (`module exceeded its 982 ms execution deadline` for a 1,000 ms timeout), and a result over the cap reads `run returned more than the N byte result limit`. | No change; the kernel answers what Rust answers. The L09 candidate was corrected to the same result-size text. |
| d | The manager's texts are Rust's, with two exceptions: OS errors are rebuilt from errno (`No such file or directory (os error 2)`, the same strings), and an unreadable worker response reads `invalid sandbox response: <JSON.parse message>` instead of serde's message. | Accept: neither is reachable with a working worker. |
| e | CI cannot build the image: V8 and bundled DuckDB take about 10 minutes, and CI allows 15. | CI runs the contract suite against the manager with protocol stubs (`test/stub/`). The Rust-only suite runs locally with the binaries (`K20_SANDBOX_BIN`) and against the image (`K20_SANDBOX_URL`, root, chroot). The image runs on the platform via `dev/platform.mjs`. All three passed (§3). |

## 3. Evidence and numbers (rule 8)

**Tests.** `vitest run --root sandbox`, 25 tests, in three modes:

| Mode | Result |
|---|---|
| CI stubs | 15 passed; 10 need Rust and skip |
| Native Rust binaries (unconfined) | 24 passed; 1 stub-only protocol test skipped |
| The image (`docker run`: root, chroot and seccomp) | 24 passed, twice |

The seven Whagons scenarios run against Rust with DuckDB with the app's own helpers (`test/rust.test.ts`), plus
an agent Action through K12's runtime.

**Platform** (`gonvex-cf-dev-k20-sandbox`, `basic` instance: 1/4 vCPU, 1 GiB, 4 GB; deleted after the run;
`cloudflare/sandbox/dev/results/platform.json`):

| Measure | Result |
|---|---|
| Cold start: Container started to manager listening | 547, 967, 1,036 ms |
| First `create` of a new tenant (includes the start) | 1.14, 1.69, 1.93 s |
| First run in a fresh Container / later trivial run | 276–519 ms / 112–148 ms |
| `create` on a warm Container | 184–557 ms |
| DuckDB: register 1,000 tasks + GROUP BY | 213 ms |
| `importFile`: CSV 100 rows (first DuckDB load) / XLSX / XLS | 848 / 200 / 193 ms |
| `SUMMARIZE` of an import | 288 ms |
| 6.7 MB CSV (200,000 rows): import, then GROUP BY | 2.2 s, 181 ms |
| Deadline 1 s (`await`) / CPU spin, 2 s timeout | failed at 1.12 s / 2.14 s, Rust's text |
| 64 MiB V8 heap | failed in 410 ms: `module exceeded the 67108864 byte isolate heap limit` |
| Network | `fetch`/`WebSocket` absent; DuckDB `https://` and `/etc/passwd`: "file system operations are disabled by configuration" |
| Another account / another tenant | "sandbox was not found" |

**Memory.**
- Manager RSS: 57–77 MB.
- Worker peak RSS per run (local, same binaries): 49 MB trivial, 56 MB DuckDB register + GROUP BY, under 56 MB
  for the 200,000-row ingest.
- Rust's caps bound a run at 64 MiB V8 plus 128 MiB DuckDB. Two concurrent runs plus the manager fit the `basic`
  instance's 1 GiB.

**Cost** (Containers pricing as published on developers.cloudflare.com, 2026-11).

What `basic` costs while it runs:
- memory: 1 GiB × $0.0000025/GiB-s;
- disk: 4 GB × $0.00000007/GB-s;
- together $0.00000278/s, about **$0.010 per Container-hour**, billed only while the tenant's Container runs;
- CPU is billed on active use, at $0.000020/vCPU-s.

The CPU a run uses, at most 1/4 vCPU for its wall time:

| Run | CPU cost |
|---|---|
| Trivial | ≤ $0.0000006 |
| DuckDB analysis | ≤ $0.0000011 |
| 200,000-row import | ≤ $0.000011 |

An assistant turn keeps the Container up for the turn plus a 60 s grace. A 2-minute turn costs about **$0.0005**.
The Workers Paid allowance (25 GiB-hours, 375 vCPU-minutes) covers about 25 Container-hours a month.

**Bundle.** The Worker side has no new dependency and is a few kilobytes. The image is 380 MB.

## 4. The L09 candidate (0048 §2.2, for Gabriel)

The Dynamic Worker plus DO SQLite implementation is now `cloudflare/spikes/l09-sandbox/`. It has its own install,
outside the workspace and CI, and its report is that directory's `README.md`. In brief:

- **Measured on the platform:**
  - first run 638 ms;
  - a 200,000-row CSV imports in 1.9 s and groups in 530 ms;
  - the heap limit is the platform's 128 MB;
  - the deadline overshoots: 4.7–5.0 s for spins and 12.4 s under a host-call flood (2 s timeout).
- **The same seven scenarios on Rust with DuckDB and on L09.** The results are identical except the value types:
  - booleans come back as `true` vs 1;
  - error texts differ for a missing table (`Catalog Error: Table with name … does not exist!` vs
    `no such table: …`);
  - the deadline text names the remaining vs the configured milliseconds;
  - `SUMMARIZE` formats and quartiles: Rust gives `avg "20.0"`, `std "10.0"` and interpolated `q25 "12"`,
    `q75 "28"`; L09 gives `"20"`, `"10"`, `"10"`, `"20"`.
- **Real model-written SQL, from every staging tenant:** 24 statements, 10 distinct.
  - 8 run on both DuckDB 1.4.1 and SQLite.
  - 2 use `read_json_auto`, which fails on Rust too: its DuckDB has no json extension.
  - **None uses DuckDB-only syntax that Rust runs and SQLite refuses.**
- **On staging, no current-era DuckDB call has ever succeeded** (§2a, §2b). The dialect change would therefore
  affect no working behaviour today.

The sample is small (13 tenants with traces; most traffic came from the earlier Go/JS sandbox tools). Gabriel
decides whether L09 moves forward.

## 5. Not here

- A conformance scenario: V01 adds it, as for K13 (0048 §2.3).
- Wiring into the kernel Worker: K22 (`sandboxFromEnv(env, files)` for `configureActions`).
- K19's builder Container: it can follow the same `containers` configuration (0048 §2.4).
- The Whagons `parseExcelAttachment` finding goes to the app at cutover (0048).
