/**
 * The service socket's frame boundary: `Runtime::handle_service_frame`
 * (service_principal.rs:359-489) and `service_control_call` (560-573).
 *
 * A service socket accepts `auth` (handled by the normal auth path),
 * `reducer.call`, `action.call` and `query.call`. Tenant-scope calls must name
 * an allowlisted function; control-scope Reducers reach only the two
 * delegation paths. Subscriptions, replicas and batches are refused with
 * Rust's texts; every other frame is ignored. A frame that does not decode is
 * closed 1003 by the session layer's decoder before it gets here.
 *
 * Frames are written in serde's key order (`ServerMessage` in
 * rust/crates/protocol/src/lib.rs). Service `reducer.error` frames carry no
 * `class` or `retryable` (the reference renders the error to a string first),
 * except the batch refusal, which is classed `rejected`.
 */
import type { ActionSession } from "../../actions/src/session";
import type { ClientFrame, Json } from "../session/decode";
import { frames } from "../session/protocol";
import { rustDebug } from "../reducer/errors";
import { allows, DELEGATE_PATH, REVOKE_PATH, type ServiceGrant } from "./grant";

export type ServiceCallKind = "reducer" | "action" | "query";

/** One allowlisted tenant call, as the tenant object receives it. */
export interface ServiceTenantCall {
  kind: ServiceCallKind;
  /** Stamped by the socket owner, never decoded from a client frame. */
  reply?: ActionSession["reply"];
  /** The frame id: the command id of a Reducer (row ids derive from it). */
  id: string;
  path: string;
  args: Json;
  idempotencyKey?: string;
}

/** A tenant call's outcome: the result as JSON text (spliced, never re-parsed), or the reference's error text. */
export type ServiceTenantOutcome =
  | { ok: true; result: string; committedRevision?: number; pending?: boolean }
  | { ok: false; error: string };

/** A control Reducer's outcome (`control.servicePrincipals.*`). */
export type ServiceControlOutcome = { ok: true; value: Json } | { ok: false; error: string };

/** What the boundary needs: the control object (delegation) and the tenant object (allowlisted calls). */
export interface ServiceCalls {
  control(grant: ServiceGrant, path: string, args: Json): Promise<ServiceControlOutcome>;
  tenant(grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome>;
}

export const SERVICE_ERRORS = {
  subscribe: "service principals cannot subscribe",
  replica: "service principals cannot open replicas",
  batch: "batched frames are not available to service principals",
  controlActions: "service principals cannot call Control Plane Actions",
  controlQueries: "service principals cannot call Control Plane Queries",
  notAllowed: (path: string) => `function ${rustDebug(path)} is not allowed for this service principal`,
  controlUnavailable: (path: string) => `Control Plane function ${rustDebug(path)} is not available to service principals`,
} as const;

/** Whether `path` is one of the two Control paths a service socket may call (others never reach the control object). */
export const isServiceControlPath = (path: string): boolean => path === DELEGATE_PATH || path === REVOKE_PATH;

const q = JSON.stringify;

/** `reducer.result`: `id`, `path`, `result`, `originCommandId` (= `id`), `committedRevision?`; never a trace. */
export function serviceReducerResult(id: string, path: string, result: string, committedRevision?: number): string {
  return `{"type":"reducer.result","id":${q(id)},"path":${q(path)},"result":${result},"originCommandId":${q(id)}`
    + (committedRevision === undefined ? "" : `,"committedRevision":${committedRevision}`) + "}";
}

/** `reducer.error` without `class` and `retryable`. */
export const serviceReducerError = (id: string, path: string, error: string): string =>
  `{"type":"reducer.error","id":${q(id)},"path":${q(path)},"error":${q(error)}}`;

export function serviceActionResult(id: string, path: string, result: string, committedRevision?: number): string {
  return `{"type":"action.result","id":${q(id)},"path":${q(path)},"result":${result}`
    + (committedRevision === undefined ? "" : `,"committedRevision":${committedRevision}`) + "}";
}

export const serviceActionError = (id: string, path: string, error: string): string =>
  `{"type":"action.error","id":${q(id)},"path":${q(path)},"error":${q(error)}}`;

/** `query.result` with the payload map `{path, result}` (BTreeMap order; no `reason`). */
export const serviceQueryResult = (id: string, path: string, result: string): string =>
  `{"type":"query.result","id":${q(id)},"path":${q(path)},"result":${result}}`;

/**
 * `service_tenant_call`: the allowlist check, then the tenant object. The
 * refusal happens here, in the object that holds the socket, so a refused
 * call never reaches the tenant.
 */
async function tenantCall(calls: ServiceCalls, grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome> {
  if (!allows(grant, call.path)) {
    console.warn(`service principal ${grant.principalId}: ${call.kind} ${call.path} is outside its allowlist (tenant ${grant.route.tenantId})`);
    return { ok: false, error: SERVICE_ERRORS.notAllowed(call.path) };
  }
  return calls.tenant(grant, call);
}

/**
 * One decoded frame on an authenticated service socket. Returns the frames to
 * send (empty: ignored), or `null` for an `auth` frame, which the normal auth
 * path handles (`ServiceFrame::PassThrough`).
 */
export async function handleServiceFrame(calls: ServiceCalls, grant: ServiceGrant, frame: ClientFrame): Promise<string[] | null> {
  switch (frame.type) {
    case "auth":
      return null;
    case "reducer.call": {
      const { id, path } = frame;
      if (frame.scope === "control") {
        const outcome = isServiceControlPath(path)
          ? await calls.control(grant, path, frame.args)
          : { ok: false as const, error: SERVICE_ERRORS.controlUnavailable(path) };
        return [outcome.ok ? serviceReducerResult(id, path, JSON.stringify(outcome.value)) : serviceReducerError(id, path, outcome.error)];
      }
      const outcome = await tenantCall(calls, grant, {
        kind: "reducer", id, path, args: frame.args, ...(frame.idempotencyKey !== undefined ? { idempotencyKey: frame.idempotencyKey } : {}),
      });
      return [outcome.ok ? serviceReducerResult(id, path, outcome.result, outcome.committedRevision) : serviceReducerError(id, path, outcome.error)];
    }
    case "action.call": {
      const { id, path } = frame;
      if (frame.scope === "control") return [serviceActionError(id, path, SERVICE_ERRORS.controlActions)];
      const outcome = await tenantCall(calls, grant, {
        kind: "action", id, path, args: frame.args, ...(frame.idempotencyKey !== undefined ? { idempotencyKey: frame.idempotencyKey } : {}),
      });
      if (outcome.ok && outcome.pending) return [];
      return [outcome.ok ? serviceActionResult(id, path, outcome.result, outcome.committedRevision) : serviceActionError(id, path, outcome.error)];
    }
    case "query.call": {
      const { id, path } = frame;
      if (frame.scope === "control") return [frames.queryError(id, path, SERVICE_ERRORS.controlQueries)];
      const outcome = await tenantCall(calls, grant, { kind: "query", id, path, args: frame.args });
      return [outcome.ok ? serviceQueryResult(id, path, outcome.result) : frames.queryError(id, path, outcome.error)];
    }
    case "query.subscribe":
      return [frames.queryError(frame.id, frame.path, SERVICE_ERRORS.subscribe)];
    case "replica.open":
      return [frames.replicaError(frame.id, frame.path, SERVICE_ERRORS.replica)];
    case "query.subscribeMany":
    case "replica.openMany":
    case "reducer.callMany":
      return [frames.reducerError("", undefined, SERVICE_ERRORS.batch, "rejected")];
    default:
      return [];
  }
}
