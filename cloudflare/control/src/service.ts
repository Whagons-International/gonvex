/**
 * Service-principal Control calls (service_principal.rs:542-760; C.1 rows 61-62).
 * Only `scope:"control"` Reducers on a service socket reach these. There is no
 * idempotency: a retry mints a second grant, as in the reference.
 */
import type { Ctx } from "./context";
import { admitMember } from "./directory";
import { debugStr } from "./errors";
import { NOW_MS } from "./sql";
import type { DelegationActor, ServiceGrant } from "./types";
import { byteLength, rfc3339, secureToken, sha256Hex, uuidSimple } from "./util";

export const DELEGATE_PATH = "control.servicePrincipals.delegate";
export const REVOKE_PATH = "control.servicePrincipals.revokeDelegation";

/** Plain error text, as the reference returns `Err(String)` from these paths. */
export class ServiceError extends Error {}

const actorOf = (grant: ServiceGrant): string => `service:${grant.principalId}`;
// Rust `char::is_control`: Unicode general category Cc.
const hasControl = (value: string): boolean => /\p{Cc}/u.test(value);

function delegationActor(value: unknown): DelegationActor | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) throw new ServiceError("invalid arguments: actor must be an object");
  const object = value as Record<string, unknown>;
  const unknown = Object.keys(object).find((key) => !["kind", "name", "reference"].includes(key));
  if (unknown !== undefined) throw new ServiceError(`invalid arguments: unknown actor field ${debugStr(unknown)}`);
  const kind = typeof object.kind === "string" ? object.kind : "";
  if (!kind || kind.length > 32 || !/^[a-z0-9_-]+$/.test(kind)) throw new ServiceError("invalid arguments: actor.kind must be 1-32 characters of [a-z0-9_-]");
  const name = typeof object.name === "string" ? object.name.trim() : "";
  if (!name || [...name].length > 120 || hasControl(name)) {
    throw new ServiceError("invalid arguments: actor.name must be 1-120 characters without control characters");
  }
  const referenceError = () => new ServiceError("invalid arguments: actor.reference must be a string of at most 200 characters without control characters");
  let reference: string | undefined;
  if (object.reference !== undefined && object.reference !== null) {
    if (typeof object.reference !== "string") throw referenceError();
    const trimmed = object.reference.trim();
    if ([...trimmed].length > 200 || hasControl(trimmed)) throw referenceError();
    if (trimmed) reference = trimmed;
  }
  return reference === undefined ? { kind, name } : { kind, name, reference };
}

/** `delegate_service_session`. */
export async function delegateServiceSession(ctx: Ctx, grant: ServiceGrant, args: unknown): Promise<Record<string, unknown>> {
  if (!args || typeof args !== "object" || Array.isArray(args)) throw new ServiceError("invalid arguments: expected an object");
  const object = args as Record<string, unknown>;
  const unknown = Object.keys(object).find((key) => !["accountId", "memberId", "reason", "expiresInSeconds", "actor"].includes(key));
  if (unknown !== undefined) throw new ServiceError(`invalid arguments: unknown field ${debugStr(unknown)}`);
  const field = (name: string): string => {
    const value = typeof object[name] === "string" ? (object[name] as string).trim() : "";
    if (!value) throw new ServiceError(`invalid arguments: ${name} is required`);
    return value;
  };
  const accountId = field("accountId");
  const memberId = field("memberId");
  const reason = field("reason");
  if (byteLength(reason) > 200) throw new ServiceError("invalid arguments: reason exceeds 200 bytes");
  const maximum = grant.maxDelegationSeconds;
  let seconds = maximum;
  const raw = object.expiresInSeconds;
  if (raw !== undefined && raw !== null) {
    if (!(typeof raw === "number" && Number.isInteger(raw) && raw >= 30 && raw <= maximum)) {
      throw new ServiceError(`invalid arguments: expiresInSeconds must be between 30 and ${maximum}`);
    }
    seconds = raw;
  }
  const actor = delegationActor(object.actor);
  let admitted = false;
  try {
    admitted = (await admitMember(ctx, grant.route.projectId, grant.route.tenantId, accountId)).member.id === memberId;
  } catch {
    admitted = false;
  }
  if (!admitted) throw new ServiceError("target is not an active tenant member");
  const grantId = `svcdel_${uuidSimple()}`;
  const token = secureToken("imp");
  const expires = Date.now() + seconds * 1000;
  ctx.sql.tx(() =>
    ctx.sql.run(
      `INSERT INTO gonvex_impersonation_grants
       (id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at,delegation_actor)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      grantId,
      grant.route.projectId,
      sha256Hex(token),
      actorOf(grant),
      accountId,
      grant.route.tenantId,
      `${actorOf(grant)}: ${reason}`,
      expires,
      actor === null ? null : JSON.stringify(actor),
    ),
  );
  return { id: grantId, token, expiresAt: rfc3339(expires), expiresInSeconds: seconds };
}

/** `revoke_service_delegation`. */
export function revokeServiceDelegation(ctx: Ctx, grant: ServiceGrant, args: unknown): Record<string, unknown> {
  const object = args && typeof args === "object" && !Array.isArray(args) ? (args as Record<string, unknown>) : null;
  const id = object && Object.keys(object).length === 1 && typeof object.id === "string" ? object.id.trim() : "";
  if (!id) throw new ServiceError("invalid arguments: id is required");
  const affected = ctx.sql.tx(() =>
    ctx.sql.affected(
      `UPDATE gonvex_impersonation_grants SET revoked_at=${NOW_MS}
       WHERE project_id=$1 AND tenant_id=$2 AND id=$3 AND actor_account_id=$4 AND revoked_at IS NULL`,
      grant.route.projectId,
      grant.route.tenantId,
      id,
      actorOf(grant),
    ),
  );
  return { updated: affected > 0 };
}

/** `service_control_call`. */
export async function serviceControlCall(ctx: Ctx, grant: ServiceGrant, path: string, args: unknown): Promise<unknown> {
  if (path === DELEGATE_PATH) return delegateServiceSession(ctx, grant, args);
  if (path === REVOKE_PATH) return revokeServiceDelegation(ctx, grant, args);
  throw new ServiceError(`Control Plane function ${debugStr(path)} is not available to service principals`);
}
