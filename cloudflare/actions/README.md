# Action runtime (K12)

Runs Gonvex Actions on Cloudflare: Actions run in the Action Worker, and every Action that is handed off runs in a
**Workflow step**, as S4 recommended (`docs/cloudflare/spikes/S4-long-actions.md`) and 0016 G0-8 ruled. Queues are
never used for Actions. The module host is K04's engine. This package is the Action side of it:

- the host behind every Action op;
- Rust's `execute_tenant_action` checks;
- the Workflow that runs Actions K11's scheduler hands over;
- the runs of direct agent calls.

One claimer per table (0037): K11 owns `_gonvex_action_outbox` and `gonvex_scheduled_jobs`. K12 owns only
`_gonvex_st_action_runs`.

The reference is the Rust runtime at `0bf3ad13`: `rust/crates/runtime/src/action_calls.rs` (`ActionHostCalls`,
`fetch`, `OpenFetchBodies`), `execution.rs` (`execute_tenant_action_with_access`, `invoke_interactive_function`,
`deliver_outbox_action`, provenance).

## Layout

| File | What | Rust reference |
|---|---|---|
| `src/runtime.ts` | `ActionRuntime.execute`: generation, function and kind, public access, argument schema, provenance, deadlines (15 min; none for `profile:"agent"`; 60 s for anonymous callers), the capability declaration, project secrets, then K04's engine with the Action host. `tools.*`, `ctx.functions.invoke`, `ctx.scheduler`, `ctx.storage`, `ctx.sandbox`. Every error text is Rust's `ExecutionError` Display | `execution.rs:825-1136`, `action_calls.rs:212-628` |
| `src/fetch.ts` | `ctx.fetch`: the origin allowlist (again on the host), the redirect loop with each hop checked, the 8 MiB cap by content-length and by body, lossy UTF-8, canonical status text, event streams through `fetchRead` in whole UTF-8 characters with a cumulative cap, the request budget (the rest of the deadline), 16 concurrent requests | `action_calls.rs:22-180,373-490`, `V8/lib.rs:649-655` |
| `src/provenance.ts` | `InvocationProvenance`: direct, initial-Action, delegated and tool provenance, durable hops (32), `ctx.invocation`, the persisted form | `module-runtime/src/lib.rs:155-270`, `execution.rs:1593-1706` |
| `src/session.ts` | The tenant session an Action runs as; the system session; the module-facing identity | `execution.rs:1458-1486,1529-1586` |
| `src/handoff.ts` | K11's `ActionHandoff` port on a Workflow (`WorkflowHandoff`), and `runHandoff`: a run K11 claimed, run as the re-resolved Member with K11's provenance, K11's claim renewed every third of its lease, the outcome reported once | `execution.rs:1237-1344`, K11 `ports.ts` |
| `src/calls.ts` | `ActionCalls`, the tenant side of direct agent calls: begin, claim, renew, complete, cancel, the sweep (an alarm source on K11's `TenantAlarm`), receipts | S4 `tenant.ts` |
| `src/runner.ts` | `runCall`: one direct-call run on the Action side: claim, run with a renewal every 30 s, report | |
| `src/worker.ts` | `ActionWorkflow` (both payloads), `ActionWorker.call` (direct `action.call`), `configureActions` | S4 `runner.ts` |
| `src/tenant.ts` | `TenantActions`, what the tenant object exposes by RPC (over `ActionCalls`, K04's host and K11's `Scheduler`); `WorkflowDispatcher` | |
| `src/ports.ts` | The interfaces to the tenant object, K06 (sessions, secrets), K11 (scheduler), K13 (storage), K20 (sandbox) | |
| `src/rpc.ts` | Deploy skew: tenant RPC failures that are retried | 0016 G0-8 |
| `test/` | workerd tests with a real tenant object (K01 storage, K04 host) and a real Workflow | |
| `dev/` | A local Worker and `run-agent.mjs` for the 30-minute agent Action | |
| `scripts/record-golden.mjs` | Records Rust's answers to the conformance scenario `actions` for `test/parity.test.ts` | |

## How an Action runs

```
Reducer ctx.actions.enqueue ─► outbox row (same tx) ─► K11 scheduler (tenant alarm): claim, one durable hop
ctx.scheduler (Action or Reducer) ─► K11 job ─────────► K11 scheduler: claim the due job
                                                              │ ActionHandoff.startAction(run) = WorkflowHandoff
                                                              ▼
            ActionWorkflow "h-<id>-a<attempt>": run ─ renewRun every leaseMs/3 ─ completeRun(claim, outcome) once
                                                              │ K11: done, retry with backoff, or dead (_gonvex_action_dead)

direct action.call (K05/K07) ─► ActionWorker.call ─┬─ standard / public: ActionRuntime.execute inline (15 min / 60 s)
                                                    └─ agent: tenant.beginCall ─► ActionCalls run (_gonvex_st_action_runs)
                                                              ▼
            ActionWorkflow "<id>-d<n>": claimCall (attempt+1) ─ run ─ renewCall every 30 s ─ completeCall (fenced) ─► onFinished
                                                              (ActionCalls' sweep: lost hand-off, expired lease)
```

- **Outbox and scheduled Actions belong to K11.** K11's scheduler claims the row or the job, applies the durable hop and
  the attempt cap, and hands the run over. `WorkflowHandoff` accepts every run: it never runs an Action inline in the
  tenant object. The Workflow resolves the actor (the system session, or the Member re-resolved through K06), renews
  K11's claim every third of its lease, and reports the outcome once. A refused renewal means K11 gave the row to a
  newer delivery: the run stops and reports nothing. K11 decides done, retry or dead.
- **Direct agent calls belong to `ActionCalls`.** A claim writes `status='processing'`, `locked_at=now` and
  `attempts+1`. A renewal extends the lease in memory and writes `locked_at` only when less than half of the 5-minute
  lease is left. The persisted `locked_at` alone decides when a lease has expired, so an alarm that wakes a fresh object
  instance still recovers a dead runner on time (S4's platform bug).
- **Exactly-once answers.** `completeCall` is fenced on the attempt and applied in one statement; a replay answers
  `duplicate`, a zombie attempt's answer `rejected`. A direct call is answered once and never retried (Rust answers the
  socket once); only a runner that died runs it again, and the call fails after 8 such runs. The answer reaches K05/K07
  through `onFinished`.
- **Recovery.** `ActionCalls.alarmSource()` is registered on K11's `TenantAlarm`. Its sweep hands off again a hand-off
  not claimed within one lease (a lost `create()`), and an expired lease. A failed `create()` is retried after one
  renewal interval. The Workflow engine's own step retries cover a crash: the same instance retries, so the same owner
  takes its lease over at once.
- **Deploy skew (G0-8).** Every tenant method takes one object argument with `v: 1` and answers one object. A newer
  peer only adds fields. "RPC receiver does not implement the method" and object resets are transient (`rpc.ts`), so the
  step retries.

## Capabilities

| Capability | Kernel | Tested by |
|---|---|---|
| `fetch` | `src/fetch.ts`; workerd's `fetch` with `redirect: "manual"` | parity steps 2-4; `runtime.test.ts` "network edge cases", deadlines, 16 requests |
| secrets | `ControlPort.projectSecrets` (K06) | parity step 5; `runtime.test.ts` "secrets" |
| `tools.*` | the tenant object's `call` (K04's `TenantModuleHost.run`), each Reducer in its own transaction | parity step 6 |
| `functions.invoke` | Member re-resolved through `ControlPort.tenantSessionForAccount`; Query/Reducer in the tenant object, Actions inline | parity step 7 |
| scheduler | `SchedulerPort.enqueue`, by default the tenant's `schedule` RPC (K11's `Scheduler.enqueue`); target checked against the generation | parity step 8; `runtime.test.ts` "storage and scheduler"; `handoff.test.ts` (a scheduled Action run end to end) |
| storage | `StoragePort.call` (K13); "storage is not configured" without it | parity step 9; `runtime.test.ts` "storage and scheduler" |
| sandbox | `SandboxPort.call` (K20); "sandbox is disabled by runtime policy" without it | `runtime.test.ts` |

`test/parity.test.ts` runs the conformance scenario `actions` step by step through this runtime, against a real tenant
object, and compares every answer with what Rust answered (`test/golden/actions-rust.json`, Rust traces from 3 runs, unchanged on 2 more after each rebase). Three
answers wait on the plan executors and are asserted as the kernel gives them today: a `query` tool on a structured plan
and `ctx.functions` on a replica collection (K08, K10).

## S4's scenarios

`test/scenarios.test.ts` ports the twelve scenarios S4 listed, on a real tenant object and a real Workflow in local
workerd, with leases scaled down. They run along the two paths:

- outbox Actions through **K11's real `Scheduler`** with `WorkflowHandoff`;
- direct agent calls through `ActionCalls`.

Scenarios that stand in for a runner the platform kills record the hand-off, then drive the step body (`runHandoff`,
`runCall`) with the claim or the owner that the engine gives a retried step. `test/handoff.test.ts` runs K11's
hand-off end to end against the real `Scheduler.renew` and `complete`, a scheduled Action included.

| # | Scenario | Outbox (K11 + WorkflowHandoff) | Direct call (ActionCalls) |
|---|---|---|---|
| 1 | Agent Action in a Workflow step, renewals on cadence, completed once | 6 s run, K11's claim renewed on cadence; **30 minutes** with `dev/run-agent.mjs` (below) | 1 s run, delivered once; a failure answered once |
| 2 | An event stream arrives complete and in order | in scenario 1, and parity step 3 | — |
| 3 | 64 MB streamed into an upload; 1 GB without exceeding 128 MB | Not applicable to `ctx.fetch`: the contract caps a response at 8 MiB (B.2a). The cap and the stream path are tested; S4's raw-`fetch` numbers stand | |
| 4 | One request waits for its response | 3 s with no response headers, claim renewed | — |
| 5 | Runner killed mid-run | the step's retry runs under the same claim; the dead run's report is refused | the same owner re-claims as attempt 2 |
| 6 | Zombie: lease expires, new owner, zombie's report refused | K11 re-claims as attempt 2; also with the tenant object reset | the sweep hands off again, after a tenant reset |
| 7 | Hand-off lost | K11 retries the row with backoff, then completes it | the sweep retries the `create()` |
| 8 | Duplicate hand-off | two Workflow instances for one claim; one report applies | refused while leased, then as done |
| 9 | Replayed and stale reports are no-ops | yes | yes |
| 10 | Cancel mid-run | K11 has no cancel | the next renewal is refused within one interval; answered once as cancelled |
| 11 | `MAX_ATTEMPTS` failures end `dead` | K11's backoff, then `_gonvex_action_dead`, then `requeueDead` | a runner that dies too often fails the call (`calls.test.ts`) |
| 12 | Redeploy mid-run | the deploy-skew errors are retried; a real redeploy is a platform follow-up | same |

## The 30-minute agent Action

```sh
cd cloudflare
node actions/dev/run-agent.mjs 1800 /tmp/k12-agent-1800s.json
```

It runs `wrangler dev` on `dev/wrangler.jsonc` from a frozen copy of the sources (`wrangler dev` restarts on every edit,
which kills in-flight work). The agent loop is S4's: 60 event-stream turns of 20 s (100 events every 200 ms) with 10 s
pauses, over the real local network to the conformance upstream. The Action is queued by a Reducer, claimed by K11's
scheduler on the tenant alarm and handed to the Workflow, which renews K11's 5-minute claim every 100 s.

## Numbers

- **Bundle** (esbuild, minified):
  - the Action Worker's K12 code is 28.3 KB, 9.9 KB gzip;
  - with K04's engine it is 38.4 KB, 13.3 KB gzip, before the app bundle;
  - the tenant side (`calls.ts`, `tenant.ts`) is 8.8 KB, 3.2 KB gzip.
- **Tenant rows written** (SQLite `rowsWritten`, index entries included):
  - **direct agent call** (asserted in `calls.test.ts`): begin and hand-off 4, claim 1, renewals 20 per running hour (one
    write every 180 s), completion 2;
  - **outbox Action:** K11's rows, plus about 36 renewal writes per running hour, because K12 renews every 100 s and K11
    writes `locked_at` on each. Rust renews every 30 s: 120 row writes per hour, plus their index entries.
- **Rows read:** one run read per direct-call claim, renewal and completion. The sweep reads only live runs (the open
  direct calls of one tenant) and pages them by 100.
- **Memory:** response bodies stream. Only a non-stream body up to 8 MiB is held, as in Rust.

## Not here

| Item | Owner |
|---|---|
| Routing `action.call` frames to `ActionWorker.call`, and delivering a direct agent call's result | K05, K07 |
| Wiring K07's tenant object as the kernel's tenant class with K11's `Scheduler` (`actions: WorkflowHandoff`), `ActionCalls` on its `TenantAlarm`, and `TenantActions`' methods by RPC (`test/worker.ts` shows the composition) | K19 (0037) |
| The outbox drain, scheduled jobs, dead rows and the operator listing (0034); agent-profile outbox Actions outside the one-in-flight rule, up to 8 per tenant (0037) | K11 |
| Full `InvocationProvenance` in outbox rows (0032's follow-up) | K07 |
| `_gonvex_files` and R2 behind `StoragePort` | K13 |
| Sessions and `gonvex_project_env` behind `ControlPort` | K06 |
| The sandbox behind `SandboxPort` | K20 |
| Building the Worker version with the app bundle and `configureActions`; `limits.cpu_ms = 300000` on the Action Worker | K19 |
| The plan executors behind a `query` tool on a structured plan or a replica collection | K08, K10 |
