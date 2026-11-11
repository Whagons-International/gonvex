// Shared helpers for the scenario tests: a configured tenant object, the
// Action runtime against it, and waiting for a Workflow instance.
import { env } from "cloudflare:test";
import { ActionRuntime } from "../src/runtime.js";
import type { CallJob } from "../src/calls.js";
import { handoffInstanceId, runHandoff, type DurableRun, type HandoffJob, type HandoffResult, type HandoffTenantPort } from "../src/handoff.js";
import type { TenantActionPort } from "../src/ports.js";
import { PROJECT } from "./fixture.js";
import { registry, testControl, testFetcher, type TestTenant } from "./worker.js";

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export type TenantConfig = Parameters<TestTenant["configure"]>[1];

export async function tenantFor(name: string, config: TenantConfig = {}) {
  const tenantId = `${name}-${crypto.randomUUID().slice(0, 8)}`;
  const tenant = env.TENANT.getByName(tenantId);
  await tenant.configure(tenantId, config);
  const port = tenant as unknown as TenantActionPort & HandoffTenantPort;
  const runtime = new ActionRuntime({ registry: registry(), tenant: port, control: testControl, fetcher: testFetcher });
  const callJob = (id: string, n = 1): CallJob => ({ v: 1, kind: "call", projectId: PROJECT, tenantId, id, n });
  const handoffJob = (run: DurableRun): HandoffJob => ({ v: 1, kind: "handoff", projectId: PROJECT, tenantId, run });
  /** Runs a recorded K11 hand-off by hand, as the Workflow step would. */
  const runRecorded = (run: DurableRun, options: { stopRenewing?: (elapsed: number) => boolean; on?: HandoffTenantPort } = {}): Promise<HandoffResult> =>
    runHandoff({ runtime, tenant: options.on ?? port, control: testControl, stopRenewing: options.stopRenewing }, handoffJob(run));
  return { tenant, tenantId, runtime, port, callJob, handoffJob, runRecorded };
}

export async function until<T>(check: () => Promise<T | undefined | false>, label: string, ms = 30_000): Promise<T> {
  const deadline = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
    await sleep(50);
  }
}

/** The output of a Workflow instance once it completes. */
export async function workflowOutput(instance: string, ms = 30_000): Promise<Record<string, unknown>> {
  return until(async () => {
    try {
      const status = await (await env.ACTION_WORKFLOW.get(instance)).status();
      if (status.status === "errored" || status.status === "terminated") throw new Error(`workflow ${status.status}: ${JSON.stringify(status.error)}`);
      return status.status === "complete" ? (status.output as Record<string, unknown>) : undefined;
    } catch (error) {
      if (String(error).includes("workflow ")) throw error;
      return undefined;
    }
  }, `workflow ${instance}`, ms);
}

/** K11 hands an outbox row over as `h-<id>-a<attempt>`. */
export const outboxInstance = (id: string, attempt = 1) => handoffInstanceId({ id, claim: { source: "outbox", id, attempt, leaseMs: 0 } });

type Stub = Pick<TestTenant, "handedOffRuns" | "listDead">;
/** K11 hand-offs the tenant recorded (typed: the RPC stub's inferred type loses them). */
export const handedOff = async (tenant: unknown): Promise<DurableRun[]> => (await (tenant as Stub).handedOffRuns()) as unknown as DurableRun[];
export const deadItems = async (tenant: unknown) =>
  ((await (tenant as Stub).listDead()) as unknown as { items: Array<{ id: string; source: string; function: string; attempts: number }> }).items;
