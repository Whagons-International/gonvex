/**
 * The rollout queue consumer (CONTRACT D.2.4): one message per tenant applies
 * the release's tenant plan to that tenant object (K02 `applyRelease`, one
 * event's slice per call, the object's alarm continuing in between), waits
 * until it is done or failed, and reports to the control object, which then
 * sends the next tenant (fail-fast in `tenant_id` order, 0012 R1).
 *
 * A consumer that dies mid-tenant is retried by the queue: `applyRelease` is
 * resumable and idempotent per plan. Queue consumers stop at 15 minutes, so a
 * tenant still running after `CONSUMER_BUDGET_MS` is retried as a fresh
 * delivery that continues where the object is.
 */
import { rustTenantError, waitForRelease, type OperatorTenantPort } from "../../control/operator/tenant/port.ts";
import type { RolloutMessage, RolloutReport } from "../../control/operator/release.ts";
import { CONTROL_NAME, type DevRouterEnv } from "./routes.ts";

export interface RolloutEnv extends DevRouterEnv {
  TENANT: DurableObjectNamespace;
}

const CONSUMER_BUDGET_MS = 12 * 60 * 1000;
const POLL_MS = 250;

export async function runRolloutStep(message: RolloutMessage, env: RolloutEnv, budgetMs = CONSUMER_BUDGET_MS): Promise<RolloutReport | "retry"> {
  const tenant = env.TENANT.getByName(message.tenant) as unknown as OperatorTenantPort;
  let status;
  try {
    status = await waitForRelease(tenant, message.planId, message.project, Date.now() + budgetMs, POLL_MS);
  } catch {
    // An object reset (a chunk over the CPU limit, a deploy) surfaces here; the plan resumes on retry.
    return "retry";
  }
  if (status === "timeout") return "retry";
  if (status.state === "done") return { syncId: message.syncId, tenant: message.tenant, index: message.index, state: "done", error: null };
  return { syncId: message.syncId, tenant: message.tenant, index: message.index, state: "failed", error: rustTenantError(status.error) };
}

export async function handleRolloutBatch(batch: MessageBatch<RolloutMessage>, env: RolloutEnv): Promise<void> {
  const control = env.CONTROL.getByName(CONTROL_NAME);
  for (const message of batch.messages) {
    const report = await runRolloutStep(message.body, env);
    if (report === "retry") {
      message.retry();
      continue;
    }
    await control.rolloutReport(report);
    message.ack();
  }
}
