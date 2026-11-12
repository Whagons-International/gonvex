/**
 * How the Ingress Worker runs an Action (K18; 0043 §4, ruled in 0045).
 *
 * K17's tenant service path is the only service-call implementation: the
 * tenant object's `serviceCall(grant, call)` (`kernel/principals/tenant.ts`,
 * `service_tenant_call` in Rust). It runs the call as the principal's system
 * identity `_gonvex_service:<id>` with `allow_internal`, on the channel the
 * principal's configuration sets (`system` for the ingress, 0045 §3; never
 * taken from the request). Like K17's gateway object, the ingress checks the
 * allowlist before the call: the tenant object trusts the grant it is handed.
 *
 * Actions reach that path once the assembly composes K12 into the tenant
 * object (0045 "Assembly"), with K12's inline option: providers wait on the
 * HTTP answer, so an agent Action must not become a pending Workflow run.
 */
import type { ServiceTenantCall, ServiceTenantOutcome } from "../../kernel/principals/frames.ts";
import type { ServiceGrant } from "../../control/src/types.ts";

export interface ServiceTenantPort {
  serviceCall(grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome>;
}

/** The Action's result JSON value, or its error text as Rust puts it on a service socket. */
export type ServiceActionReport =
  | { ok: true; value: unknown }
  | { ok: false; error: string; transient?: boolean };

/** The Rust service session's member id (`service_principal.rs:71`). */
export const serviceMemberId = (principalId: string): string => `_gonvex_service:${principalId}`;

/** The call's frame id, which is its command id (`ingress_<uuid>`). */
export const commandId = (): string => `ingress_${crypto.randomUUID().replaceAll("-", "")}`;

/** The tenant object of a grant's tenant, from the `TENANT` namespace. */
export function tenantPort(namespace: DurableObjectNamespace | undefined, grant: ServiceGrant): ServiceTenantPort | null {
  if (!namespace) return null;
  return namespace.getByName(grant.route.tenantId) as unknown as ServiceTenantPort;
}

/** K17's outcome as the ingress reads it: the result text parsed once. */
export function reportOf(outcome: ServiceTenantOutcome): ServiceActionReport {
  if (!outcome.ok) return { ok: false, error: outcome.error };
  try {
    return { ok: true, value: JSON.parse(outcome.result) };
  } catch {
    return { ok: false, error: "the tenant answered a result that is not JSON", transient: true };
  }
}
