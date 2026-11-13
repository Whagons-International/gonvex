/**
 * `query.call` with tenant scope: `Runtime::execute_tenant_query`
 * (`execution.rs` 130-330) and the frame around it (`lib.rs` 2990-3067).
 *
 * The checks run in Rust's order with Rust's texts. Three branches belong to
 * other packages and arrive through `CallPorts`: the Reducer receipt Query
 * (K07), Replica Collection Queries (K10) and public Query handlers run by
 * the module host (K04). Without a port the branch answers with Rust's
 * "no active module" style error rather than hang.
 */
import type { StorageSession } from "../storage";
import type { TenantSession } from "../session/ports";
import { UNAUTHENTICATED } from "../session/protocol";
import { executePlan } from "./execute";
import { callResult, queryError } from "./frames";
import { rustDebug } from "./json";
import type { QueryFunction, QueryModule } from "./module";
import { LiveQueryError, validatePlan } from "./plan";
import type { SnapshotRunner } from "./live";
import type { QueryVisibility } from "./visibility";

/** `execution::REDUCER_RECEIPT_QUERY`. */
export const REDUCER_RECEIPT_QUERY = "__gonvex.reducerReceipt";

/** The result of a branch another package executes: the result as serde_json text (see `json.ts`), or Rust's error text. */
export type CallOutcome = { ok: true; text: string } | { ok: false; error: string };

export interface CallPorts {
  readonly visibility: QueryVisibility;
  readonly module: (projectId: string) => QueryModule | null;
  readonly run: SnapshotRunner;
  /** `validate_portable_schema`: null when `args` match, else the message (K04 `validateValue`). */
  readonly validateArgs: (schema: unknown, args: unknown) => string | null;
  /** K07: `reducer_receipt_status`. */
  readonly receipt?: (session: TenantSession, args: unknown) => Promise<CallOutcome>;
  /** K10: `execute_replica_collection_query`. */
  readonly replica?: (session: TenantSession, path: string, fn: QueryFunction, args: unknown) => Promise<CallOutcome>;
  /** K04: a public Query handler in the module host. */
  readonly handler?: (session: TenantSession, path: string, args: unknown) => Promise<CallOutcome>;
}

/** `ExecutionError::HostCall`. */
const hostCall = (message: string) => `module database capability failed: ${message}`;

class CallError extends Error {}

/** The reply frame for a tenant-scope `query.call` (`tenant` null: no tenant session). */
export async function callQuery(ports: CallPorts, tenant: TenantSession | null, id: string, path: string, args: unknown): Promise<string> {
  if (!tenant) return queryError(id, path, UNAUTHENTICATED.query);
  try {
    const value = await executeQuery(ports, tenant, path, args);
    return callResult(id, path, value);
  } catch (error) {
    return queryError(id, path, error instanceof Error ? error.message : String(error));
  }
}

async function settle(outcome: Promise<CallOutcome>): Promise<string> {
  const result = await outcome;
  if (!result.ok) throw new CallError(result.error);
  return result.text;
}

export async function executeQuery(ports: CallPorts, session: TenantSession, path: string, args: unknown, access: { allowInternal?: boolean; delegatedAgent?: boolean } = {}): Promise<string> {
  if (path === REDUCER_RECEIPT_QUERY) {
    if (!ports.receipt) throw new CallError(`function ${rustDebug(path)} is not registered`);
    return settle(ports.receipt(session, args));
  }
  const module = ports.module(session.identity.projectId);
  if (!module) throw new CallError(`project ${rustDebug(session.identity.projectId)} has no active TypeScript module`);
  const fn = module.functions.get(path);
  if (!fn) throw new CallError(`function ${rustDebug(path)} is not registered`);
  if (fn.internal && !access.allowInternal) throw new CallError(`internal function ${rustDebug(path)} cannot be called by a client`);
  if (fn.kind !== "query") throw new CallError(`function ${rustDebug(path)} is a ${fn.kind}, not a query`);
  if (session.anonymous && (!fn.public || fn.replica !== null || fn.delivery === "live" || fn.delivery === "replica")) {
    throw new CallError(hostCall("authenticate with an active tenant before calling a non-public query"));
  }
  const mismatch = ports.validateArgs(fn.argsSchema, args);
  if (mismatch !== null) throw new CallError(`function ${rustDebug(path)} arguments do not match its schema: ${mismatch}`);
  if (!access.delegatedAgent && fn.delivery && fn.delivery !== "oneShot") {
    throw new CallError(hostCall(`query ${rustDebug(path)} is delivered as ${fn.delivery} and cannot be called as a one-shot Query`));
  }
  const plan = fn.liveQueryPlan;
  if (plan && !fn.public && !session.anonymous) {
    return ports.run((db: StorageSession) => {
      try {
        validatePlan(plan);
        const visibilityPlan = module.visibility.get(plan.table);
        if (!visibilityPlan) throw LiveQueryError.visibilityMissing(plan.table);
        return executePlan({ db, session, module, visibility: ports.visibility }, plan, visibilityPlan, args).text;
      } catch (error) {
        throw new CallError(hostCall(error instanceof Error ? error.message : String(error)));
      }
    });
  }
  if (fn.replica !== null) {
    if (!ports.replica) throw new CallError(hostCall(`replica collection query ${rustDebug(path)} is not available in this kernel build`));
    return settle(ports.replica(session, path, fn, args));
  }
  if (!fn.public) throw new CallError(hostCall(`one-shot query ${rustDebug(path)} requires a structured live query plan`));
  if (!ports.handler) throw new CallError(`project ${rustDebug(session.identity.projectId)} has no active TypeScript module`);
  return settle(ports.handler(session, path, args));
}
