# 0037: Rulings from the second wave-2 review (K08, K11, K13, K19, K14, K12)

Date: 2026-11-11. Status: decided (lead).

## Note numbering (0024 §5)

| Number | Note |
|---|---|
| 0036 | K19 deploy and operator routes |
| 0037 | this note |
| 0038 | K14 presence (proposed as 0033; renumbered on merge) |
| 0039 | K12 Action runtime (proposed as 0036; K12 renumbers in its fix) |

## K08: approved

All 0032 items are addressed. Follow-up: `kernelVisibility` should import K09's port types instead of casting
`as unknown as QueryVisibility`, so that drift between the shapes fails typecheck.

## K11: merged

**Follow-up for K11 (new thread):** agent-profile outbox Actions do **not** count toward the one-in-flight rule.
They run concurrently as Workflow steps, up to 8 per tenant. Short Actions keep one in flight per tenant.
Otherwise one 30-minute agent Action would block every other Action of its tenant (K12 0039 §4).

## K19 (note 0036): approved

| § | Ruling |
|---|---|
| 1 | Keep the router's token-guarded blob route for the release builder: a Container cannot bind R2. The token is a Worker secret scoped to that route. |
| 3 | Refusing a wrong project key before storing the body is accepted. When the body is also malformed, the error the caller sees can differ from Rust's. That is accepted as a security improvement, and `operator-routes` asserts the kernel's order. |
| 5 | **503** when the rollout deadline passes is accepted. **Follow-up for K02:** `applyRelease` must be idempotent for a plan that is already complete, so callers don't depend on `status()` polling. |
| 6 | Deploy scripts refuse `GONVEX_LOCAL_OPERATOR` outside `gonvex-cf-dev-*` deployments (the same rule as 0024 §3d). |
| 8 | K07's tenant object and K10's delivery call `operatorDataChanged` after commits. |
| — | **Follow-up for K19:** wire K07's `ReducerTenantObject` as the kernel's tenant class (`bundle()` from `gonvex:releases`, `plans()` = `R2PlanStore`, `OperatorTenantPort` through `TenantOperator`). |
| — | `/dev/storage/files` returns `url: null` for private rows once K13's listing port is wired (0032). |

## K14 (note 0038): approved

- The presence scenarios pass Rust vs kernel. A closed client goes offline after the 10 s grace.
- **Dead-peer detection** depends on the TCP stack: locally 33 s, 136 s, or more than 30 minutes at Linux's
  default `tcp_retries2`. **Ruling:** accepted for stage 1. The platform measurement (`dead-peer.mjs`) is a
  follow-up. **If the edge does not detect a silent dead peer within 120 s, the lead escalates to Gabriel** a
  capability-flagged SDK ping (0017's design). That would be an SDK change and needs his approval.
- K05 files changed additively, plus a real fix (a close that wakes a hibernated gateway was dropped). Accepted.
- The presence normalizer rule (members sorted by aliased id) is accepted. Each backend's raw sort order is
  still asserted.
- `session-frames` step 6 fails against local `wrangler dev` on the integration base too, from before this PR.
  K05's owner or V01 investigates.

## K12 (note 0039): changes requested

- **K11 owns `_gonvex_action_outbox`.** K12 removes its outbox-origin path in this PR. Outbox and scheduled
  Actions reach the runtime only through K11's scheduler and `WorkflowHandoff`. `ActionOutbox` keeps only
  direct agent `action.call` runs. One claimer per table.
- §3.1: **(a) accepted**: direct agent calls start about 2 s later because of Workflow creation. G3b measures
  confirmation time, and (b) is ruled on if agent turns regress.
- §3.2 and §3.3 accepted. `_gonvex_st_action_runs` goes into A01's `addedObjects()`.

## K13 (note 0035): approved in 0032, merged after the others in this pass.
