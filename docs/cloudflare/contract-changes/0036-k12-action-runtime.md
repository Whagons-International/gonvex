# 0036: K12 Action runtime: interfaces, deviations and open questions

Date: 2026-11-11. Status: **proposed by K12**, for the lead. Renumbered to 0036 on rebase: 0033-0035 are K08, K11 and K13 (0032). Affects: K05, K06, K07, K11, K13, K19, K20, A01, M03,
CONTRACT.md B.2a, B.6, E.b, E.c.1, D.1.

Code: `cloudflare/actions/` (README there). Evidence: `test/parity.test.ts` (every step of the new conformance
scenario `actions` equals Rust's recorded answers), `test/scenarios.test.ts` (S4's twelve scenarios on a real tenant
object and a real Workflow), `test/outbox.test.ts`, `test/runtime.test.ts`, and a 30-minute agent Action run locally
with `dev/run-agent.mjs`.

## 1. Interfaces K12 defines (for the lead to record)

| Port | Implemented by | Methods |
|---|---|---|
| `TenantActionPort` | the tenant object (K07), composing `TenantActions` | `call` (a tool or `ctx.functions` Query/Reducer), `claim`, `renew`, `fail`, `complete`, `beginCall` |
| `ControlPort` | the control object (K06) | `tenantSessionForAccount(project, tenant, account)`, `projectSecrets(project, names)` |
| `SchedulerPort` | K11 | `enqueue({ id: "job_<uuid>", function, args, runAtUnixMs, coalesce, session, provenance })` (an Action's `ctx.scheduler`) |
| `ActionHandoff` (K11's port) | K12 `WorkflowHandoff` | `startAction(run)` → `{ accepted: true }`; the tenant exposes `renewRun` / `completeRun` over K11's `Scheduler.renew` / `complete` |
| `StoragePort` | K13 | `call(operation, payload, session)`, Rust's `storage.call` |
| `SandboxPort` | K20 | `call(operation, payload, session, duckdb)` |
| `ActionWorker.call` | K05/K07 route `action.call` here | inline for standard and public Actions; `{ pending, id }` for agent Actions |
| `ActionOutbox` | hosted by the tenant object | `afterCommit()` after a Reducer commit that wrote the outbox; `sweep()` from the alarm (K11); `listDead`/`requeue` for D.1's operator routes (K19) |

Every tenant method takes one object argument with `v: 1` and answers one object, so a later version only adds fields,
and no method is renamed or removed in the version that stops using it (G0-8). `rpc.ts` treats "RPC receiver does not
implement the method" and object resets as transient, and the Workflow step retries.

K06 does not expose `tenantSessionForAccount` by RPC yet (it is `directory.ts`), and there is no `gonvex_project_env`
in the control object yet. **Needed before G2** for `ctx.functions` and secrets on the platform.

## 2. Deviations (proposed for acceptance)

| # | Rust | Kernel | Why |
|---|---|---|---|
| a | Lease state lives only in `_gonvex_action_outbox` | Kernel state (owner, hand-offs, receipts, dead rows, direct-call runs) lives in **`_gonvex_st_action_runs`** (after §4's ruling: direct-call runs only). A01's `addedObjects()` must list it, so that P03 export and M02 leave it out (the precedent is K02's stash table, 0028 §3.4). | Rust's table has a CHECK with only `pending`/`processing`, and must stay byte-compatible for export |
| b | `renew_action` writes `locked_at` every 30 s | `locked_at` is written only when less than half of the 5-minute lease is left: 40 SQLite rows per running hour against Rust's 240 (rows plus index entries). An exported in-flight row's `locked_at` can be up to 150 s older than its last renewal. | S4 G0-8. M03 drains in-flight Actions before cutover, so nothing reads a stale `locked_at` |
| c | Success deletes the row unfenced | Success deletes the row **fenced on the attempt**, in one transaction, and leaves a receipt for 7 days | Exactly once (S4) |
| d | A failing outbox Action retries forever | Dead after 8 attempts (IB2). A chain past 32 hops is dead too (Rust deletes it). Dead rows leave Rust's table and keep everything an operator needs to requeue. Export back to Rust drops them. | IB2, E.c.1 |
| e | Redirects: reqwest's custom policy, no limit | 32 hops, then "fetch request failed: error following redirect" | The Worker's subrequest limit |
| f | reqwest's error texts | Kernel network failures map to reqwest's texts: "fetch request failed: error sending request", "fetch response body failed: request or response body error", and the two timeout texts | `0015 S0-1`: the first line is contract |
| g | Agent Actions have no deadline | No deadline inside the Action. The Workflow step that runs it has a timeout of **24 h** (`ACTION_STEP_TIMEOUT`). A step needs one, and the default is 10 min. | Platform. S4 ran 30 min; runs of many hours are unmeasured (S4 open item) |
| h | A request body string is sent as is | Sent as bytes, so workerd adds no `content-type` that Rust would not send | Parity (`echo` step) |

Kept on purpose, although they read oddly: the unconfigured-secret error names the **project id** as the Action
(`Action "<project>" requires a capability the Rust runtime has not configured: …`, Rust passes `project_id` as `path`).
The text says "Rust runtime". The 17th concurrent `fetch` is refused without the capability prefix, as the V8 isolate
refuses it.

## 3. Open questions for the lead

1. **Hand-off latency of direct agent calls.** A direct agent `action.call` becomes a Workflow run, so it survives
   deploys and has no wall-clock limit. S4 measured `create()` at 740 ms p50 and the first claim at 1.8 s p50 (3.9 s
   max) on the platform. An agent chat turn would start about 2 s later than on Rust, where the socket's own request
   runs it at once. That counts against G3b's confirmation time. Options:
   - (a) accept it;
   - (b) run the first seconds inline in the Action Worker and hand off only if the Action is still running;
   - (c) run agent calls inline and accept the redeploy risk.

   K12 implements (a). (b) needs a ruling, because it splits one attempt across two runners.
2. **Short outbox Actions also go through a Workflow.** Rust's drainer starts them at once. The kernel adds the same
   hand-off latency, which no client sees directly. Proposed: accept. Revisit if V-tests show outbox lag.
3. **Results of direct agent calls over 1 MiB** are delivered once but not kept for redelivery (a DO row holds 2 MB).
   One over 512 KiB is applied from inside the run step, because a step result holds at most 1 MiB. Proposed: accept.
   Whagons agent turns return ids, not documents.

## 4. Relationship with K11 (0034, approved in 0032)

K12 was briefed to persist leases and renew them from the tenant object. K11's approved design also claims outbox rows
in the tenant object (one in flight per tenant, attempt-guarded, dead rows in `_gonvex_action_dead`). That makes two
tenant-side implementations of the same protocol on `_gonvex_action_outbox`. K11's PR (#357) is not merged, so K12
ships both sides, and the boundary is this:

- **K11's hand-off is supported now.** `src/handoff.ts` implements K11's `ActionHandoff` port (`WorkflowHandoff`):
  every Action K11's scheduler claims (an outbox Action or a scheduled Action) is accepted and runs in
  `ActionWorkflow`. The run's claim is renewed through the tenant's `renewRun` (K11 `Scheduler.renew`), and the
  outcome is reported once through `completeRun` (K11 `Scheduler.complete`). K11 keeps the row, its attempts,
  retries and dead state. Tested in `test/handoff.test.ts` against a stand-in of those two methods.
- **Proposed ruling: K11 owns outbox rows.** When K11 merges, the tenant object drains `_gonvex_action_outbox`
  with K11's scheduler and K12's `WorkflowHandoff`. K12's `ActionOutbox` then keeps only its run of origin `call`:
  direct agent `action.call`s, which K11 does not handle. Its outbox-origin path, which S4's scenarios exercise
  here, is removed in a follow-up K12 PR. Its dead rows would otherwise duplicate `_gonvex_action_dead`.
- **Difference to settle between the two.** K11 holds one outbox Action in flight per tenant. A 30-minute agent
  Action queued from a Reducer would then hold every other outbox Action of that tenant for 30 minutes. Rust has
  the same per-turn behavior (E.c.1), but runs Actions inline, so a long one blocks only its own drain lane. S4
  measured exactly this head-of-line blocking on Queues. **Proposed:** agent-profile outbox Actions do not count
  against K11's one-in-flight rule.

## 5. Platform follow-ups (blocked by the vault outage on 2026-11-11)

The following are platform runs, not code. Each goes to `gonvex-cf-dev-k12-*` resources, deleted afterwards.

- the 30-minute agent Action on the platform with K12's code;
- a redeploy of the Action Worker in the middle of a run;
- a tenant object on the previous version answering a new method;
- the CPU per MB that K12's `fetchRead` path costs, against S4's 40-100 ms per MB;
- Workflow instance creation under a burst of outbox Actions;
- a run of several hours.

`limits.cpu_ms = 300000` is set in `dev/wrangler.jsonc`. K19's Worker build must set it on the Action Worker (G0-8).
