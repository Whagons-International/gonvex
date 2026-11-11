import type { TenantObject } from "./tenant";
import type { RunnerObject } from "./runner-object";

/** How the tenant hands a committed outbox row to a runner. */
export type Runner = "queue" | "workflow" | "alarm" | "alarm-await";

export interface Job {
  tenant: string;
  id: string;
}

export interface Env {
  TENANT: DurableObjectNamespace<TenantObject>;
  RUNNER: DurableObjectNamespace<RunnerObject>;
  ACTIONS: Queue<Job>;
  ACTION_WF: Workflow<Job>;
  UPSTREAM_URL: string;
  S4_TOKEN?: string;
  LEASE_MS?: string;
  RENEW_MS?: string;
  MAX_ATTEMPTS?: string;
  /** "1" under `wrangler dev`: the OOM crash becomes a thrown error. */
  LOCAL?: string;
}

export const tenantStub = (env: Env, tenant: string) => env.TENANT.get(env.TENANT.idFromName(tenant));
