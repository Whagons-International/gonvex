/**
 * The authenticated state of one service socket, and the identity its calls
 * run under. Reference: `rust/crates/runtime/src/service_principal.rs` at
 * `0bf3ad13` (`ServiceGrant`, `ServiceGrant::session`, constants 46-54).
 */
import type { TenantRoute, TenantSession } from "../session/ports";

/** `TOKEN_PREFIX`: a `gvx_svc_` credential, configured by its SHA-256 digest. */
export const SERVICE_TOKEN_PREFIX = "gvx_svc_";
export const DELEGATE_PATH = "control.servicePrincipals.delegate";
export const REVOKE_PATH = "control.servicePrincipals.revokeDelegation";
/** `SERVICE_PRINCIPAL_ACTOR_PREFIX`: grant rows name the principal `service:<id>`. */
export const SERVICE_ACTOR_PREFIX = "service:";
/** The system identity allowlisted calls run as (`_gonvex_service:<id>`). */
export const SERVICE_IDENTITY_PREFIX = "_gonvex_service:";

/** Every `gvx_svc_` auth failure but two returns this text (`AUTH_FAILURE`). */
export const SERVICE_AUTH_FAILURE = "service credential is invalid or not permitted for this project";
/** `authenticate_service_principal`: the route did not resolve. */
export const SERVICE_TENANT_UNAVAILABLE = "tenant is not available";

/**
 * `ServiceGrant`: the principal, the one tenant the socket is bound to, its
 * function allowlist and its delegation ceiling. The same shape as K06's
 * `ServiceGrant` (`control/src/types.ts`), so it crosses RPC unchanged. It is
 * kept in the socket's `SessionRecord` (hibernation attachment).
 */
export interface ServiceGrant {
  principalId: string;
  route: TenantRoute;
  functions: string[];
  maxDelegationSeconds: number;
}

/** `ServiceGrant::allows`: exact paths, no wildcards (rejected at configuration). */
export function allows(grant: ServiceGrant, path: string): boolean {
  return grant.functions.includes(path);
}

/**
 * `ServiceGrant::session`: the synthetic tenant session allowlisted calls run
 * as. Role and status `system`, empty permissions, no delegation: the
 * principal's own calls are not a delegated member session.
 */
export function serviceSession(grant: ServiceGrant): TenantSession {
  const id = `${SERVICE_IDENTITY_PREFIX}${grant.principalId}`;
  return {
    identity: {
      projectId: grant.route.projectId,
      account: { id, email: "", emailVerified: false, name: grant.principalId, avatarUrl: "", provider: "service" },
    },
    route: grant.route,
    member: {
      id, accountId: id, status: "system", displayName: grant.principalId, avatarUrl: "", role: "system",
      permissions: {}, membershipRevision: 0,
    },
    admissionRevision: 0,
    delegation: null,
    developerSession: null,
    anonymous: false,
  };
}

/** `auth.result.result` of a service socket, in the reference's `json!` key order. */
export function serviceAuthResult(grant: ServiceGrant): Record<string, unknown> {
  return {
    projectId: grant.route.projectId,
    tenantId: grant.route.tenantId,
    accountId: "",
    servicePrincipal: grant.principalId,
    functions: grant.functions,
    maxDelegationSeconds: grant.maxDelegationSeconds,
  };
}
