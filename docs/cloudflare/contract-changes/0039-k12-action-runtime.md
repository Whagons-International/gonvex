# 0039: K12 Action runtime: interfaces, deviations and rulings

Date: 2026-11-11. Status: **proposed by K12; §3 and §4 ruled in 0037**. Proposed as 0030, then 0036; numbered 0039
by 0037. Affects: K05, K06, K07, K11, K13, K19, K20, A01, M03, CONTRACT.md B.2a, B.6, E.b, E.c.1, D.1.

Code: `cloudflare/actions/` (README there). Evidence:

- `test/parity.test.ts`: every step of the conformance scenario `actions` equals Rust's recorded answers;
- `test/scenarios.test.ts`: S4's twelve scenarios on a real tenant object and a real Workflow, outbox Actions
  through K11's real `Scheduler` and direct calls through `ActionCalls`;
- `test/handoff.test.ts`: K11's hand-off end to end;
- `test/calls.test.ts` and `test/runtime.test.ts`;
- a 30-minute agent Action run locally with `dev/run-agent.mjs`.

## 1. Interfaces K12 defines (for the lead to record)

One claimer per table (0037, as 0022 §1). K11 claims `_gonvex_action_outbox` and `gonvex_scheduled_jobs`. K12 claims
only `_gonvex_st_action_runs`, the runs of direct agent `action.call`s.

| Port | Implemented by | Methods |
|---|---|---|
| `TenantActions` (tenant RPC) | the tenant object (K19 wires K07's object) | `call` (a tool or `ctx.functions` Query/Reducer); `claimCall`, `renewCall`, `completeCall`, `beginCall` (direct calls); `renewRun`, `completeRun` (K11's `Scheduler.renew` / `complete`); `schedule` (K11's `Scheduler.enqueue`) |
| `ActionHandoff` (K11's port) | K12 `WorkflowHandoff` | `startAction(run)` → `{ accepted: true }`; the run reports through `renewRun` / `completeRun` |
| `ActionCalls.alarmSource()` | registered on K11's `TenantAlarm` | the sweep of direct-call runs |
| `ControlPort` | the control object (K06) | `tenantSessionForAccount(project, tenant, account)`, `projectSecrets(project, names)` |
| `SchedulerPort` | default: the tenant's `schedule` RPC | `enqueue({ id: "job_<uuid>", function, args, runAtUnixMs, coalesce, session, provenance })` (an Action's `ctx.scheduler`) |
| `StoragePort` | K13 | `call(operation, payload, session)`, Rust's `storage.call` |
| `SandboxPort` | K20 | `call(operation, payload, session, duckdb)` |
| `ActionWorker.call` | K05/K07 route `action.call` here | inline for standard and public Actions; `{ pending, id }` for agent Actions, answered once through `ActionCalls`' `onFinished` |

Every tenant method takes one object argument with `v: 1` and answers one object (K11's two take its own shapes), so
a later version only adds fields, and no method is renamed or removed in the version that stops using it (G0-8).
`rpc.ts` treats "RPC receiver does not implement the method" and object resets as transient, and the Workflow step
retries.

K06 does not expose `tenantSessionForAccount` by RPC yet (it is `directory.ts`), and there is no `gonvex_project_env`
in the control object yet. **Needed before G2** for `ctx.functions`, outbox Actions queued by a Member, and secrets on
the platform.

## 2. Deviations (proposed for acceptance)

| # | Rust | Kernel | Why |
|---|---|---|---|
| a | Direct calls hold no state | A direct agent call is a row in **`_gonvex_st_action_runs`** while it runs (`ACTION_OBJECTS` lists the table and its indexes for A01's `addedObjects()`, as 0037 rules). | Accepted in 0037 |
| b | — | A direct-call run persists its lease at half-life: 20 SQLite rows per running hour, index entries included. Outbox Actions renew K11's claim every third of the lease, 100 s on the 300 s outbox lease, which K11 writes each time: about 36 rows per running hour, against Rust's 120 rows plus their index entries. | S4, G0-8 |
| c | Success deletes the row unfenced | Direct calls: completion is fenced on the attempt, and a receipt is kept for 7 days. Outbox rows: K11's attempt-guarded completion (0034 §3c). | Exactly once (S4) |
| d | Redirects: reqwest's custom policy, no limit | 32 hops, then "fetch request failed: error following redirect" | The Worker's subrequest limit |
| e | reqwest's error texts | Kernel network failures map to reqwest's texts: "fetch request failed: error sending request", "fetch response body failed: request or response body error", and the two timeout texts | `0015 S0-1`: the first line is contract |
| f | Agent Actions have no deadline | No deadline inside the Action. The Workflow step that runs it has a timeout of **24 h** (`ACTION_STEP_TIMEOUT`). A step needs one, and the default is 10 min. | Platform. S4 ran 30 min; runs of many hours are unmeasured (S4 open item) |
| g | A request body string is sent as is | Sent as bytes, so workerd adds no `content-type` that Rust would not send | Parity (`echo` step) |
| h | A direct call runs on the socket's request | A direct agent call is answered once and never retried. Only a runner that died runs it again. After 8 such runs the call fails with "Action runner stopped 8 times without finishing". | IB2's cap applied to lost runners |

Kept on purpose, although they read oddly:

- The unconfigured-secret error names the **project id** as the Action (`Action "<project>" requires a capability the
  Rust runtime has not configured: …`): Rust passes `project_id` as `path`, and the text says "Rust runtime".
- The 17th concurrent `fetch` is refused without the capability prefix, as the V8 isolate refuses it.

## 3. Rulings (0037)

1. **Hand-off latency of direct agent calls:** option **(a) accepted** for stage 1. A direct agent call starts about
   2 s later than on Rust, because of Workflow creation and the first claim (S4: `create()` 740 ms p50, first claim
   1.8 s p50). G3b measures confirmation time; if agent turns regress, (b) gets a ruling then.
2. **Short outbox Actions also go through a Workflow:** accepted.
3. **Results of direct agent calls over 1 MiB** are delivered once but not kept for redelivery. One over 512 KiB is
   applied from inside the run step. Accepted.

## 4. K11 owns outbox rows (0037)

Done in this PR:

- `ActionOutbox`'s outbox path is gone: claim, lease, backoff, dead rows, `afterCommit`, `listDead` and `requeue`.
  What is left is `ActionCalls` (`src/calls.ts`), which keeps only direct-call runs and never reads or writes
  `_gonvex_action_outbox`.
- Outbox and scheduled Actions reach the runtime only through K11's scheduler and `WorkflowHandoff`. K11 keeps the
  row, its attempts, retries and dead state (`_gonvex_action_dead`), and the operator routes use `Scheduler.listDead`
  and `requeueDead`.
- S4's ported scenarios run outbox cases through K11's real `Scheduler` with `WorkflowHandoff`, and direct-call cases
  through `ActionCalls`. `handoff.test.ts` runs against K11's real `Scheduler.renew` and `complete`.

For K11 (its follow-up, from 0037): agent-profile outbox Actions do not count toward the one-in-flight rule. They run
concurrently as Workflows, up to 8 per tenant. Until that lands, a long agent Action queued from a Reducer holds the
tenant's other outbox Actions for as long as it runs.

Found while re-pointing the scenarios: K04 still stores a partial provenance in outbox rows (no `artifactHash`). K11
therefore starts a fresh provenance, as Rust's serde would, and the parent command is the row id rather than the
queuing command. This is 0032's existing follow-up for K07, and `scenarios.test.ts` asserts today's behavior.

## 5. Platform follow-ups (blocked by the vault outage on 2026-11-11)

The following are platform runs, not code. Each goes to `gonvex-cf-dev-k12-*` resources, deleted afterwards.

- the 30-minute agent Action on the platform with K12's code;
- a redeploy of the Action Worker in the middle of a run;
- a tenant object on the previous version answering a new method;
- the CPU per MB that K12's `fetchRead` path costs, against S4's 40-100 ms per MB;
- Workflow instance creation under a burst of outbox Actions;
- a run of several hours.

`limits.cpu_ms = 300000` is set in `dev/wrangler.jsonc`. K19's Worker build must set it on the Action Worker (G0-8).
