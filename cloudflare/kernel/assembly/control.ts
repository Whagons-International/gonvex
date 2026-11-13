// K22: K17 control adapter promoted to the serving runtime.
// All auth, Control writes and operator routes run K06/K19.
import { tenantSessionForAccount as resolveActionSession } from "../../control/src/directory";
import { actionSession } from "./action-session";
import { OperatorControlObject } from "../../control/operator/object";
import type { AuthOutcome } from "../../control/src/auth";
import type { ControlConnection } from "../../control/src/types";
import type { ClientFrame, Json } from "../session/decode";
import { echoTrace } from "../session/protocol";
import { replicaEpoch } from "../session/directive";
import type { Failure, FailureKind, Impersonation, ModuleInfo, Outcome, RevalidationItem, ServiceAuthOutcome, SessionIdentity, SocketView, TenantRoute, TenantSession } from "../session/ports";
import { type GatewayRpc, rpc } from "../session/rpc";
import type { ServiceControlOutcome } from "../principals/frames";
import type { ServiceGrant } from "../principals/grant";

/** K05's auth branches prefix this to an impersonation failure; K06's text already carries it. */
const IMPERSONATION_PREFIX = "impersonation grant is invalid, expired, revoked, or already used: ";

/** `auth.error.code` → the store error class K05 maps back to the same code (`authErrorCode`). */
function failureKind(code: string): FailureKind {
  if (code === "credential_rejected") return "InvalidSession";
  if (code === "member_removed") return "MemberNotFound";
  return "Other";
}

const failed = (outcome: AuthOutcome, strip = ""): Failure => {
  const message = outcome.message as { error?: string; code?: string };
  const text = message.error ?? "";
  return { ok: false, kind: failureKind(message.code ?? "transient"), message: strip && text.startsWith(strip) ? text.slice(strip.length) : text };
};

/** K05's tenant session shape (`anonymous` replaces K06's empty-account convention). */
function sessionOf(tenant: NonNullable<ControlConnection["tenant"]>): TenantSession {
  return { ...(tenant as unknown as TenantSession), anonymous: tenant.identity.account.id === "" };
}

function connectionOf(socket: SocketView): ControlConnection {
  const c = socket.connection;
  return {
    connectionId: socket.connectionId, projectId: c.projectId, identity: c.identity as ControlConnection["identity"],
    tenant: c.tenant as unknown as ControlConnection["tenant"], impersonationId: c.impersonationId, authToken: c.authToken,
  };
}

export class SessionControlObject extends OperatorControlObject {
  private readonly assemblyInstance = crypto.randomUUID();
  async runtimeInstance(): Promise<string> { return this.assemblyInstance; }
  private auth(request: { token?: string; project?: string; tenant?: string; controlOnly?: boolean; public?: boolean; connectionId?: string }): Promise<AuthOutcome> {
    return this.authenticate({
      id: "", token: request.token ?? null, project: request.project ?? null, tenant: request.tenant ?? null,
      controlOnly: request.controlOnly ?? false, public: request.public ?? false, connectionId: request.connectionId ?? "",
    });
  }

  // ---- K17: service principals ------------------------------------------------------------

  /** `authenticate_service_principal` through K06's `auth` branch for `gvx_svc_`. */
  async serviceAuthenticate(token: string, project: string | undefined, tenant: string | undefined): Promise<ServiceAuthOutcome> {
    const outcome = await this.auth({ token, ...(project !== undefined ? { project } : {}), ...(tenant !== undefined ? { tenant } : {}) });
    if (outcome.service) return { ok: true, grant: outcome.service as ServiceGrant };
    return { ok: false, error: (outcome.message as { error?: string }).error ?? "" };
  }

  /** `service_control_call` (K06 `serviceCall`), as K05's port shape. */
  async serviceControl(grant: ServiceGrant, path: string, args: Json): Promise<ServiceControlOutcome> {
    const outcome = await this.serviceCall(grant, path, args);
    return outcome.ok ? { ok: true, value: outcome.value as Json } : { ok: false, error: outcome.error };
  }

  // ---- K05 ControlPort ----------------------------------------------------------------------

  async sessionProject(projectId: string): Promise<Outcome<null>> {
    const outcome = await this.auth({ project: projectId, controlOnly: true });
    return outcome.message.type === "auth.result" ? { ok: true, value: null } : failed(outcome);
  }

  async sessionIdentity(token: string, projectId: string | undefined): Promise<Outcome<SessionIdentity>> {
    const outcome = await this.auth({ token, ...(projectId !== undefined ? { project: projectId } : {}), controlOnly: true });
    if (outcome.message.type !== "auth.result" || !outcome.connection.identity) return failed(outcome);
    return { ok: true, value: outcome.connection.identity as SessionIdentity };
  }

  async sessionAuthenticate(token: string, projectId: string | undefined, tenant: string | undefined): Promise<Outcome<TenantSession>> {
    // An empty tenant asks for the first admitted one (`first_admitted_tenant`), as in K06's own branch.
    const outcome = await this.auth({ token, ...(projectId !== undefined ? { project: projectId } : {}), ...(tenant !== undefined ? { tenant } : {}) });
    if (outcome.message.type !== "auth.result" || !outcome.connection.tenant) return failed(outcome);
    return { ok: true, value: sessionOf(outcome.connection.tenant) };
  }

  async sessionImpersonation(token: string, projectId: string | undefined, tenant: string | undefined, connectionId: string): Promise<Outcome<Impersonation>> {
    const outcome = await this.auth({ token, ...(projectId !== undefined ? { project: projectId } : {}), ...(tenant !== undefined ? { tenant } : {}), connectionId });
    if (outcome.message.type !== "auth.result" || !outcome.connection.tenant) return failed(outcome, IMPERSONATION_PREFIX);
    const result = outcome.message.result as { impersonatorId?: string };
    return {
      ok: true,
      value: {
        session: sessionOf(outcome.connection.tenant),
        grantId: outcome.connection.impersonationId,
        actorAccountId: String(result.impersonatorId ?? ""),
        reconnectToken: outcome.connection.authToken,
      },
    };
  }

  async sessionPublicTenant(projectId: string, tenant: string): Promise<Outcome<TenantRoute>> {
    const outcome = await this.auth({ project: projectId, tenant, public: true });
    if (outcome.message.type !== "auth.result" || !outcome.connection.tenant) return failed(outcome);
    return { ok: true, value: outcome.connection.tenant.route };
  }

  /** One revalidation round: K06's per-connection `revalidate` (C.3.8 item 10). */
  async sessionRevalidate(items: RevalidationItem[]): Promise<boolean[]> {
    return Promise.all(items.map((item) => {
      const account = { id: item.accountId, email: "", emailVerified: false, name: "", avatarUrl: "", provider: "" };
      const tenant = item.tenantId
        ? ({ identity: { projectId: item.projectId, account }, route: { projectId: item.projectId, tenantId: item.tenantId, databaseUrl: "" } } as unknown as ControlConnection["tenant"])
        : null;
      return this.revalidate({
        connectionId: item.connectionId, projectId: item.projectId, identity: { projectId: item.projectId, account },
        tenant, impersonationId: item.impersonationId, authToken: item.token,
      });
    }));
  }

  /**
   * `scope:"control"` frames: one-shot Queries and writes through K06, with
   * the reference's completion (`ordered_control_completion`: result, then
   * `control.watermark`). Subscriptions answer their initial result only.
   */
  async sessionDispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    const connection = connectionOf(socket);
    const j = JSON.stringify;
    switch (frame.type) {
      case "query.call":
      case "query.subscribe": {
        const check = frame.type === "query.subscribe" ? await this.checkSubscribe(connection, frame.path) : { ok: true as const, value: null };
        const outcome = check.ok ? await this.query(connection, frame.path, frame.args ?? {}) : check;
        return [outcome.ok
          ? j({ type: "query.result", id: frame.id, path: frame.path, reason: "initial", result: outcome.value })
          : j({ type: "query.error", id: frame.id, path: frame.path, error: outcome.error })];
      }
      case "reducer.call":
      case "action.call": {
        const kind = frame.type === "reducer.call" ? "reducer" : "action";
        const trace = frame.trace ? { trace: echoTrace(frame.trace) } : {};
        const outcome = await this.write(connection, frame.path, frame.args ?? {}, frame.idempotencyKey ?? "", kind);
        if (!outcome.ok) {
          return [kind === "reducer"
            ? j({ type: "reducer.error", id: frame.id, path: frame.path, error: outcome.error, class: outcome.errorClass, retryable: outcome.errorClass === "transient" || outcome.errorClass === "unauthenticated", ...trace })
            : j({ type: "action.error", id: frame.id, path: frame.path, error: outcome.error, ...trace })];
        }
        const result = kind === "reducer"
          ? j({ type: "reducer.result", id: frame.id, path: frame.path, result: outcome.value, originCommandId: frame.id, ...trace })
          : j({ type: "action.result", id: frame.id, path: frame.path, result: outcome.value, ...trace });
        return [result, j({ type: "control.watermark", id: frame.id })];
      }
      default:
        return [];
    }
  }

  // ---- K05 ModulePort -------------------------------------------------------------------------

  protected manifestOf(projectId: string): Record<string, unknown> | null {
    const text = this.sql.value<string>("SELECT manifest FROM gonvex_runtime_manifests WHERE project_id=$1", projectId);
    return typeof text === "string" ? (JSON.parse(text) as Record<string, unknown>) : null;
  }

  /** The deployed manifest (`/dev/manifest`). */
  async moduleManifest(projectId: string): Promise<Record<string, unknown> | null> {
    return this.manifestOf(projectId);
  }

  async moduleInfo(projectId: string): Promise<ModuleInfo | null> {
    const manifest = this.manifestOf(projectId);
    if (!manifest) return null;
    const module = (manifest.module ?? {}) as { hash?: string; files?: Record<string, string> };
    const contract = module.files?.["client-contract.json"];
    return { artifactHash: String(module.hash ?? ""), clientContract: contract ? (JSON.parse(atob(contract)) as { version: number }).version : null };
  }

  async moduleEpoch(projectId: string, databaseUrl: string): Promise<string> {
    const manifest = this.manifestOf(projectId) ?? {};
    const module = (manifest.module ?? {}) as { hash?: string };
    return replicaEpoch({
      projectId, databaseUrl, functions: (manifest.functions ?? null) as Json, schema: (manifest.schema ?? null) as Json, moduleHash: String(module.hash ?? ""),
    });
  }

  async tenantSessionForAccount(projectId: string, tenantId: string, accountId: string) {
    const session = await resolveActionSession(this.operatorCtx(), projectId, tenantId, accountId);
    return actionSession({ ...session, anonymous: session.identity.account.id === "" } as TenantSession);
  }
  async projectSecrets(projectId: string, names: readonly string[]): Promise<Record<string, string>> {
    return Object.fromEntries(this.sql.all<{ name: string; value: string }>(
      "SELECT name,value FROM gonvex_project_env WHERE project_id=$1 AND name IN (SELECT value FROM json_each($2))", projectId, JSON.stringify(names),
    ).map((row) => [row.name, row.value]));
  }

}
