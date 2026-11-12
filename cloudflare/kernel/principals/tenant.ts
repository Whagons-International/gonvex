/// <reference path="../host/src/shims.d.ts" />
/**
 * A service principal's allowlisted calls inside the tenant object:
 * `service_tenant_call` (service_principal.rs:491-558).
 *
 * The call runs as the principal's system identity (`serviceSession`), with
 * `allow_internal` and the `api` invocation channel, through the same path a
 * member's call takes (K07 for Reducers), so receipts, capture provenance,
 * commit order and error texts are the member path's. The allowlist was
 * checked by the object that holds the socket; the tenant object trusts the
 * grant it is handed, as Rust's handler trusts its `ServiceGrant`.
 *
 * Unlike a member's call, the reply carries no watermark and never discards
 * the session: a service socket has no change feed.
 */
import type { ReducerCallRequest } from "../session/decode";
import type { KernelTenant } from "../reducer/tenant";
import { NOT_IMPLEMENTED } from "../reducer/tenant";
import type { ServiceTenantCall, ServiceTenantOutcome } from "./frames";
import { serviceSession, type ServiceGrant } from "./grant";

/**
 * One allowlisted call. Reducers run on K07's path. One-shot Queries (K08)
 * and Actions (K12) are not composed into the tenant object yet; they answer
 * the tenant object's "not implemented" text, as a member's call does today,
 * until the package that composes them routes `kind` here.
 */
export async function serviceTenantCall(kernel: KernelTenant, grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome> {
  const session = serviceSession(grant);
  if (call.kind !== "reducer") return { ok: false, error: NOT_IMPLEMENTED };
  const request: ReducerCallRequest = {
    id: call.id, path: call.path, args: call.args,
    ...(call.idempotencyKey !== undefined ? { idempotencyKey: call.idempotencyKey } : {}),
  };
  const outcome = await kernel.reducers.serial(() => kernel.reducers.execute(session, request, { allowInternal: true, channel: "api" }));
  if (!outcome.ok) return { ok: false, error: outcome.failure.message };
  return outcome.committedRevision === undefined
    ? { ok: true, result: outcome.result }
    : { ok: true, result: outcome.result, committedRevision: outcome.committedRevision };
}
