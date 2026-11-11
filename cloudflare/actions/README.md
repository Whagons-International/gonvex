# Action runtime (K12)

Runs Gonvex Actions on Cloudflare: Actions run in the Action Worker, and every Action that is handed off runs in a
**Workflow step**, as S4 recommended (`docs/cloudflare/spikes/S4-long-actions.md`) and 0016 G0-8 ruled. Queues are
never used for Actions. The module host is K04's engine; this package is the Action side of it: the host behind
every Action op, Rust's `execute_tenant_action` checks, the lease protocol on the tenant's outbox, and the Workflow.

The reference is the Rust runtime at `0bf3ad13`: `rust/crates/runtime/src/action_calls.rs` (`ActionHostCalls`,
`fetch`, `OpenFetchBodies`), `execution.rs` (`execute_tenant_action_with_access`, `invoke_interactive_function`,
`deliver_outbox_action`, provenance) and `rust/crates/store/src/lib.rs` (`claim_outbox`, `renew_action`,
`complete_action`, `retry_action`).

## Layout

| File | What | Rust reference |
|---|---|---|
| `src/runtime.ts` | `ActionRuntime.execute`: generation, function and kind, public access, argument schema, provenance, deadlines (15 min; none for `profile:"agent"`; 60 s for anonymous callers), the capability declaration, project secrets, then K04's engine with the Action host. `tools.*`, `ctx.functions.invoke`, `ctx.scheduler`, `ctx.storage`, `ctx.sandbox`. Every error text is Rust's `ExecutionError` Display | `execution.rs:825-1136`, `action_calls.rs:212-628` |
| `src/fetch.ts` | `ctx.fetch`: the origin allowlist (again on the host), the redirect loop with each hop checked, the 8 MiB cap by content-length and by body, lossy UTF-8, canonical status text, event streams through `fetchRead` in whole UTF-8 characters with a cumulative cap, the request budget (the rest of the deadline), 16 concurrent requests | `action_calls.rs:22-180,373-490`, `V8/lib.rs:649-655` |
| `src/provenance.ts` | `InvocationProvenance`: direct, initial-Action, delegated and tool provenance, durable hops (32), `ctx.invocation`, the persisted form | `module-runtime/src/lib.rs:155-270`, `execution.rs:1593-1706` |
| `src/session.ts` | The tenant session an Action runs as; the system session; the module-facing identity | `execution.rs:1458-1486,1529-1586` |
| `src/outbox.ts` | The tenant object's side: claim, renew, fail, complete, cancel, the sweep, dead rows, requeue, direct agent calls | `store/src/lib.rs:1214-1383`, S4 `tenant.ts` |
| `src/runner.ts` | One hand-off on the Action side: claim, run with a renewal every 30 s, report | `execution.rs:1237-1344` |
| `src/worker.ts` | `ActionWorkflow` (the Workflow), `ActionWorker.call` (direct `action.call`), `configureActions` | S4 `runner.ts` |
| `src/tenant.ts` | `TenantActions` (what the tenant object composes), `WorkflowDispatcher` | |
| `src/ports.ts` | The interfaces to the tenant object, K06 (sessions, secrets), K11 (scheduler), K13 (storage), K20 (sandbox) | |
| `src/rpc.ts` | Deploy skew: tenant RPC failures that are retried | 0016 G0-8 |
| `test/` | workerd tests with a real tenant object (K01 storage, K04 host) and a real Workflow | |
| `dev/` | A local Worker and `run-agent.mjs` for the 30-minute agent Action | |
| `scripts/record-golden.mjs` | Records Rust's answers to the conformance scenario `actions` for `test/parity.test.ts` | |

## How an Action runs

```
direct action.call (K05/K07) ──► ActionWorker.call ─┬─ standard / public: ActionRuntime.execute inline (15 min / 60 s)
                                                     └─ agent: tenant.beginCall ─┐
Reducer ctx.actions.enqueue ──► outbox row, same tx ──► afterCommit ────────────┤
                                                                                 ▼
                             tenant object: WorkflowDispatcher.create("<id>-d<n>")   (sweep: lost hand-off, expired lease)
                                                                                 ▼
             ActionWorkflow step "run action": claim (attempt+1) ─ run ─ renew every 30 s (fenced on the attempt)
             ActionWorkflow step "apply result": complete (fenced, one transaction, receipt) ──► onFinished (direct calls)
```

- **One lease per attempt, persisted.** A claim writes `status='processing'`, `locked_at=now`, `attempts+1` on Rust's own
  `_gonvex_action_outbox` row. A renewal extends the lease in memory and writes `locked_at` only when less than half
  of the 5-minute lease is left. The persisted `locked_at` alone decides when a lease has expired, so an alarm that wakes a
  fresh object instance still recovers a dead runner on time (S4's platform bug).
- **Exactly-once application.** `complete` is fenced on the attempt and applied in one storage transaction. It deletes the
  outbox row, as Rust does, and leaves a receipt in `_gonvex_st_action_runs`. A replay answers `duplicate`, and a zombie
  attempt's completion answers `rejected`. Execution stays at-least-once, as in Rust: a lost lease can run an Action's side
  effects twice, never apply its completion twice.
- **Retries and the dead state (IB2).** A failed outbox Action goes back to `pending` with Rust's backoff (1 s, doubling,
  15 min cap). Its 8th failure, a durable chain past 32 hops, or `cancel` ends it `dead`. A dead row leaves Rust's table,
  whose CHECK has no `dead`, and keeps its path, arguments, actor, provenance, attempts and last error in the run row.
  `listDead` / `requeue` back the operator routes in CONTRACT.md D.1.
- **Recovery.** The object's alarm runs `sweep()`. It hands off again a hand-off not claimed within one lease (a lost
  `create()`), an expired lease (a zombie or a killed runner), and a retry that is due. The Workflow engine's own step
  retries cover a crash: the same instance retries, so the same owner takes its lease over at once.
- **Direct agent calls** become a run of origin `call` in the same table, run by the same Workflow. Their result is
  delivered once through `onFinished`, for K05/K07 to turn into `action.result` / `action.error`. A failed direct call is
  answered once and not retried, as Rust answers the socket once.
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
| scheduler | `SchedulerPort.enqueue` (K11); target checked against the generation | parity step 8; `runtime.test.ts` "storage and scheduler" |
| storage | `StoragePort.call` (K13); "storage is not configured" without it | parity step 9; `runtime.test.ts` "storage and scheduler" |
| sandbox | `SandboxPort.call` (K20); "sandbox is disabled by runtime policy" without it | `runtime.test.ts` |

`test/parity.test.ts` runs the conformance scenario `actions` step by step through this runtime, against a real tenant
object, and compares every answer with what Rust answered (`test/golden/actions-rust.json`, 6 identical Rust traces). Three
answers wait on the plan executors and are asserted as the kernel gives them today: a `query` tool on a structured plan
and `ctx.functions` on a replica collection (K08, K10).

## S4's scenarios

`test/scenarios.test.ts` ports the twelve scenarios S4 listed, on a real tenant object and a real Workflow in local
workerd, with the lease scaled down. Scenarios that stand in for a runner the platform kills drive the step body
(`runClaimed`) with the owner the engine gives a retried step.

| # | Scenario | Here |
|---|---|---|
| 1 | Agent Action in a Workflow step, renewals on cadence, applied once | 6 s run with 300 ms renewals in the suite; **30 minutes** with `dev/run-agent.mjs` (below) |
| 2 | An event stream arrives complete and in order | in scenario 1, and parity step 3 |
| 3 | 64 MB streamed into an upload; 1 GB without exceeding 128 MB | Not applicable to `ctx.fetch`: the contract caps a response at 8 MiB (B.2a). The cap and the stream path are tested; S4's raw-`fetch` numbers stand |
| 4 | One request waits for its response | 3 s with no response headers (S4: 800 s on the platform), in an agent Action with renewals running |
| 5 | Runner killed mid-run, the same owner re-claims as attempt 2 | yes |
| 6 | Zombie: lease expires, new owner, zombie's completion rejected; also with the tenant object reset | yes, both |
| 7 | Hand-off lost; the sweep hands off after one lease | yes, plus a failing `create()` |
| 8 | Duplicate hand-off | two Workflow instances for one row |
| 9 | Replayed and stale completions are no-ops | yes |
| 10 | Cancel mid-run | the next renewal is refused within one interval |
| 11 | `MAX_ATTEMPTS` failures end `dead` | 3 attempts with backoff through the alarm, then requeue |
| 12 | Redeploy mid-run | the deploy-skew errors are retried; a real redeploy is a platform follow-up |

## The 30-minute agent Action

```sh
cd cloudflare
node actions/dev/run-agent.mjs 1800 /tmp/k12-agent-1800s.json
```

It runs `wrangler dev` on `dev/wrangler.jsonc` from a frozen copy of the sources (`wrangler dev` restarts on every edit,
which kills in-flight work). The agent loop is S4's: 60 event-stream turns of 20 s (100 events every 200 ms) with 10 s
pauses, over the real local network to the conformance upstream, renewing every 30 s on a 5-minute lease.

## Numbers

- **Bundle:** the K12 runtime is 27.3 KB minified, 9.8 KB gzip. With K04's engine, the Action Worker is 37.4 KB, 13.1 KB
  gzip, before the app bundle (esbuild, `src/worker.ts`).
- **Tenant rows written per Action** (SQLite `rowsWritten`, index entries included; asserted in `outbox.test.ts`):
  hand-off 3, claim 4, renewals 40 per running hour (one write every 180 s), completion 3. Rust writes the row on each
  renewal: 120 per hour plus their index entries.
- **Rows read:** one job read (two statements at most) per claim, renewal, failure and completion; the sweep reads open
  rows in pages of 100.
- **Memory:** response bodies stream; only a non-stream body up to 8 MiB is held, as in Rust.

## Not here

| Item | Owner |
|---|---|
| Routing `action.call` frames to `ActionWorker.call`, and delivering a direct agent call's result | K05, K07 |
| The tenant object class that hosts `TenantActions`, calls `afterCommit` after a Reducer commit, and exposes the methods by RPC | K07 |
| The alarm that calls `sweep()`; scheduled Actions handed to `ActionWorkflow` (note 0033 §4); the scheduler port | K11 |
| `_gonvex_files` and R2 behind `StoragePort` | K13 |
| Sessions and `gonvex_project_env` behind `ControlPort` | K06 |
| The sandbox behind `SandboxPort` | K20 |
| Building the Worker version with the app bundle and `configureActions`; the dead-Action operator routes | K19 |
| The plan executors behind a `query` tool on a structured plan or a replica collection | K08, K10 |
