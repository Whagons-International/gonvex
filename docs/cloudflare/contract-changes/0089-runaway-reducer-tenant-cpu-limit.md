# 0089: a runaway Reducer on the assembled kernel (`reducer-errors`, `errors.spin`)

Raised by X09 on 2026-11-17. **Status: adapt accepted by lead 2026-11-18 (conditional on measurement).**
**Condition met 2026-11-18 (X10c): the runtime `cpu_ms` is 5000**, pinned by the same deploy guard. The first
measurement failed it (migration alarms up to 10.6 s, imports up to 5.0 s; below). After X10 (#471) and X10b (#492),
re-measured on dev on a fresh import of the largest copy, the worst legitimate tenant event is 1,836 ms (see
"Re-measurement"). The conformance assertion, fixture and 15 s wait are unchanged.

## Observation

`reducer-errors` step "transient: invocation deadline" (X03 run5,
`/home/gabriel/work/gonvex-x03-conformance-20261117/run5/raw/*-reducer-errors.json`):

| Backend | Result | Wall |
|---|---|---|
| Rust reference (`GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS=2000`) | `reducer.error` transient, retryable, `TypeScript module host returned budget_exceeded: module … function errors.spin: module exceeded its 1999 ms execution deadline`; the follow-up `board.add` succeeds | 2.4 s for the step |
| Cloudflare dev (`gonvex-cf-dev-*-runtime`, `limits.cpu_ms` 30000) | no reply: `timed out waiting for errors.spin reply` | 15.0 s (harness wait) |

## Cause

1. `errors.spin` is a pure `for(;;){}`. workerd cannot interrupt JavaScript in its own isolate, and the platform
   clock does not advance during synchronous work (R25 #8), so neither the engine's wall timer
   (`kernel/host/src/engine.ts` `Promise.race` with `setTimeout`) nor its host-call clock/counter checks can fire.
   The only stop is the script's CPU limit, which resets the tenant object (0021 §5c, 0022 §5c).
2. 0022 §5b ruled `limits.cpu_ms = 5000` for the tenant-object script. #436 (K22 runtime isolation) moved
   `AssemblyTenant` into the separate `-runtime` script and gave it `cpu_ms` 30000, with no ruling. The reset
   then comes after 37.6–44.9 s of wall time (measured below), past the 15 s harness wait and the ~7 s shed point.
   R25 had predicted that at the 30 s default the in-flight caller would see `…reset because its code was
   updated.` On the assembled path that did not happen: every runaway was followed by `recordOverrun`, which
   `kernel/assembly/deadline.ts` calls only for the CPU-limit text. **The caller mapping works; timing is the only
   remaining gap.**

## Change in X09 (#460)

- `kernel/assembly/deploy.mjs`: `servingConfigs` (used by the preview deploy, the release builder target and the
  namespace transfer) refuses a runtime config whose `limits.cpu_ms` is not `TENANT_CPU_LIMIT_MS`. The pinned
  value is **30000**, the deployed value. The tenant CPU limit can no longer change without a ruling, as it did in
  #436. The first draft of #460 restored 5000. The measurement below reverted it.

## Measurement (lead condition, 2026-11-18)

Source: Workers Logs (read-only telemetry `query`, events view). Script `gonvex-cf-dev-integration-20261117-runtime`
(the only `-runtime` script with traffic). Window 2026-11-17T02:07:20Z → 2026-11-18T02:10Z: **44,991 invocations**,
every `cf-worker-event` row pulled, not sampled. (The `calculations` view returned about 21k rows and an alarm
max of 708 ms against a visible 10,588 ms event, so it is sampled and was not used.) Raw rows:
`/home/gabriel/work/x09-build/logs/{all,delta}.jsonl`.

| Invocations | n | p50 CPU | p99 CPU | max CPU | > 3.5 s | > 5 s |
|---|---|---|---|---|---|---|
| All | 44,752 | 0 ms | 651 ms | 32,500 ms | 22 | 15 |
| All except the 3 runaways (`exceededCpu`) | 44,749 | 0 ms | 650 ms | **10,588 ms** | 19 | 12 |
| Alarms (`migrator.alarm()` and scheduler) | 2,779 | 3 ms | 819 ms | **10,588 ms** | 15 | 11 |
| `assemblyTransfer` (P03 import) | 807 | 81 ms | 1,763 ms | **5,006 ms** | 4 | 1 |
| First loads (`dispatch` + `deliverReplicaInitial`) | 2,700+ | 23 ms | about 1,040 ms | 1,920 ms | 0 | 0 |
| `admit` | 16,085 | 0 ms | 209 ms | 576 ms | 0 | 0 |
| `operatorMigrationStatus` / `provision` | 321 | | 1,099 / 2,411 ms | 1,335 / 2,411 ms | 0 | 0 |

Classification of every event above 3.5 s:

- **Runaways (not legitimate): 3 × 32,500 ms, `exceededCpu`.** Each is a `dispatch` on the same conformance fixture
  tenant. Before each run (22:55, 23:39, 00:31) that tenant was wiped with `operatorDeleteAll`, and each overrun was
  followed by `recordOverrun`. So these are `errors.spin`, the caller mapping fired, and the reset came at 37.6–44.9 s
  wall time.
- **Migrations: 15 events, 3,970–10,588 ms CPU, 4.8–19.9 s wall.** These are on 15 P03-imported tenants,
  21:54:44–21:57:40, version `ac15d1d4`. Each was the first alarm after `operatorApplyRelease`, which is
  `kernel.migrator.alarm()` (`tenant.ts` `alarm()`) applying the release over the imported rows. Later alarms on
  the same objects are 0–270 ms. 11 of the 15 are above 5 s. 0022 §5b required migration chunks of at most 2 s of
  CPU, and these chunks break that rule on imported data.
- **P03 imports: 4 events, 3,690–5,006 ms CPU,** on one tenant (the largest import, 93 transfer calls), outcome
  `canceled`.
- **First loads stay under 2 s per invocation.** The largest first-load objects are `321cab79…`, `f6011e77…` and
  `7be99976…`. Their max is 1,722, 1,920 and 1,100 ms, still active at 02:09 during the current rerun. Telemetry
  shows object ids, not names, so which object is the 115k-row -12 is inferred (the biggest import plus the most
  first-load CPU points to `321cab79…`). The conclusion does not depend on it: no first-load invocation exceeded 2 s.

**`/__tenant-transfer` runs in the runtime script.** The stable Worker (`src/index.ts` → `handleTenantTransfer`)
checks the token and the 4 MiB body cap only. It forwards every operation over RPC to
`AssemblyTenant.assemblyTransfer` (`kernel/assembly/tenant-transfer.ts`). So imports count against the tenant CPU
limit, and one already took 5,006 ms.

## Measured safe value and path to 5000

- At the lead's 70 % headroom rule, the measured legitimate max (10,588 ms) needs `cpu_ms` ≥ 15,200. An interim
  value would be **16,000** (66 %). It brings no conformance benefit, though: the reset arrives 3–6 s after the CPU
  limit (R25), so about 19–22 s wall, still past the 15 s harness wait. With nothing to gain, 30000 stays until
  the chunking lands. Lowering it to 16,000 only shortens the blast window. That is the lead's call.
- **Path to 5000 (owner K02/K22), done in X10/X10b:** the migration alarm must stop at a CPU budget per chunk on imported data, as
  0022 §5b and 0027 already require, and so must P03 `insert`/`checksum`/`check` pages, either with smaller pages
  or with a resumable operation. 5000 also needs every legitimate event at ≤ 3.5 s, so re-measure the same table
  after those fixes. Then set `TENANT_CPU_LIMIT_MS` to 5000. Expected result from R25: `errors.spin` answers in
  about 8 s with Rust's deadline error, inside the 15 s wait.

## Re-measurement (2026-11-18, runtime e785f303 with #471 and #492)

`devcopy-stg1117-12z`, a fresh copy of the largest staging tenant (115,547 rows, 381 tables, the -12 snapshot),
imported, registered (migration alarms) and deleted under the dev lane lock, 17:03:53Z–17:05:37Z, gate variables off
afterwards; all six P03 checksum legs matched. Workers Logs, events view, unsampled, that object:

| Invocation | n | max CPU | max wall |
|---|---|---|---|
| `alarm` (the first after registration is the largest) | 24 | **1,836 ms** | 2,434 ms |
| `assemblyTransfer` (P03 import pages) | 103 | **1,554 ms** | 1,634 ms |
| `operatorMigrationStatus` / `operatorDeleteAll` / `operatorApplyRelease` | 1 each | 377 / 220 / 29 ms | |

Before: the same alarm was 3,609 ms on #471 alone (`devcopy-stg1117-12x`) and 6,003 ms before X10; imports 5,006 ms.
Runtime events above 3 s over the 24 h before 17:10Z are only those pre-fix alarms and imports and the conformance
`errors.spin` runaways (`exceededCpu`). Every legitimate event is under 3.5 s (70 % of 5000), so 5000 is set. Expected
from R25: `errors.spin` answers with the deadline error after about 5–8 s, inside the harness's 15 s wait; the
caller mapping (`recordOverrun`) is unchanged, and the next operator call after the reset recovers (#492).

## What remains different from Rust (proposal: adapt)

Exact parity is not possible with one in-object storage transaction per Reducer:

| | Rust | Kernel |
|---|---|---|
| Stop point | execution deadline (10 s; 2 s in conformance), wall clock | the tenant CPU limit. Today that is 30 s: measured 37.6–44.9 s wall for `errors.spin`. At 5000 it would be about 8 s wall (R25) |
| Error | `budget_exceeded` transient, `module exceeded its N ms execution deadline` | same code, class and text (N is the contract 10 s) |
| Writes | rolled back | rolled back (reset aborts the transaction; R25 #5) |
| Collateral | the isolate is retired; other calls continue | the tenant object resets: its other in-flight and queued calls fail transient during the overrun; sockets live in gateways and are kept (G0-1) |

The per-call guard that would close the gap (Worker Loader isolate with `limits.cpuMs`) is blocked by S5/R25:
host calls back into a tenant that holds its storage transaction deadlock from the second call. Running Reducers
outside the input gate is a new architecture and needs its own ruling (0057 §6).

Adapt (accepted by lead 2026-11-18, conditional on measurement):

1. The Cloudflare behavior is the table above: the same error frame, code and class, stopped by the tenant CPU limit
   instead of the wall deadline, the tenant reset, and the writes rolled back. The conformance step keeps its
   assertion and its 15 s wait.
2. The condition is not met yet: legitimate migration alarms reach 10.6 s of CPU. The limit stays 30000 until
   the "Path to 5000" work is done and the measurement is repeated.
