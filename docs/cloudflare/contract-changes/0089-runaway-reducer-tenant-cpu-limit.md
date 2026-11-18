# 0089: a runaway Reducer on the assembled kernel (`reducer-errors`, `errors.spin`)

Raised by X09 on 2026-11-17. Restores ruling 0022 §5b in the assembled topology. **The exact-parity gap below
is a proposal for the lead**; no conformance assertion, fixture or harness wait is changed.

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
   `AssemblyTenant` into the separate `-runtime` script and gave it `cpu_ms` 30000, with no ruling. Two effects
   follow, both measured by R25 (`kernel/host/probes/r25/FINDINGS.md`):
   - the reset comes after about 33–36 s, past the 15 s harness wait and the ~7 s shed point;
   - at the 30 s default, an in-flight gateway → tenant RPC fails with `Durable Object reset because its code was
     updated.`, not `…exceeded its CPU time limit and was reset.` The gateway mapping
     (`kernel/assembly/deadline.ts`, 0057 §3) maps only the CPU-limit text, by design, so even after the reset the
     caller cannot answer with Rust's deadline error.

## Change (within existing rulings)

- `kernel/assembly/runtime.wrangler.jsonc`: `limits.cpu_ms` back to **5000**.
- `kernel/assembly/deploy.mjs`: `servingConfigs` (used by the preview deploy, the release builder target and the
  namespace transfer) refuses a runtime config whose `cpu_ms` is not 5000, so the limit cannot drift silently again.
  The stable Worker (gateways, Control) keeps 30000; it holds no tenant objects.

Expected platform behavior at 5000, from R25 (Worker → tenant 2000/10000 runs, gateway → tenant in-flight call at
2000/10000 both saw the CPU-limit text): the runaway call fails after about 5 + 3 s; the gateway maps it to the
Rust deadline error (`budget_exceeded`, `transient`, retryable; numbers are `<n> ms` after normalization),
records the overrun marker and quarantines the command id; the runaway's writes roll back with the reset
transaction; the follow-up call reaches a fresh tenant object. **Not yet measured on the assembled dev Worker:**
this PR makes no deploy. The next X03 run on a builder-deployed runtime is the proof (0057 §5).

## What remains different from Rust (proposal: adapt)

Exact parity is not possible with one in-object storage transaction per Reducer:

| | Rust | Kernel at `cpu_ms` 5000 |
|---|---|---|
| Stop point | execution deadline (10 s; 2 s in conformance), wall clock | 5 s of CPU, about 8 s wall (R25: wall runs 3–6 s past the CPU limit) |
| Error | `budget_exceeded` transient, `module exceeded its N ms execution deadline` | same code, class and text (N is the contract 10 s) |
| Writes | rolled back | rolled back (reset aborts the transaction; R25 #5) |
| Collateral | the isolate is retired; other calls continue | the tenant object resets: its other in-flight and queued calls fail transient during the overrun; sockets live in gateways and are kept (G0-1) |

The per-call guard that would close the gap (Worker Loader isolate with `limits.cpuMs`) is blocked by S5/R25:
host calls back into a tenant that holds its storage transaction deadlock from the second call. Running Reducers
outside the input gate is a new architecture and needs its own ruling (0057 §6).

Proposed adapt, for the lead:

1. Accept the table above as the Cloudflare behavior: same error frame, code and class; stop at the tenant CPU
   limit rather than the wall deadline; the conformance step keeps its assertion and 15 s wait, which a 5000 ms
   limit satisfies.
2. Optionally lower the runtime `cpu_ms` toward 2000–4000 (R25: 2000 → 5.3 s wall, below the 6.5–7 s shed point),
   once the heaviest legitimate tenant event (wake, catalog build, migration chunk, transfer bridge) is measured
   under it on the assembled runtime. Not changed here.
