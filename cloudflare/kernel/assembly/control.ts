// K22: K17 control adapter promoted to the serving runtime.
// All auth, Control writes and operator routes run K06/K19.
import { resolveTenant, tenantSessionForAccount as resolveActionSession } from "../../control/src/directory";
import { actionSession } from "./action-session";
import { OperatorControlObject } from "../../control/operator/object";
import type { AuthOutcome } from "../../control/src/auth";
import type { ControlConnection } from "../../control/src/types";
import type { ClientFrame, Json } from "../session/decode";
import { echoTrace } from "../session/protocol";
import { span } from "../delivery/spans";
import { sha256Hex } from "../../control/src/util";
import { replicaEpoch } from "../session/directive";
import { deployedManifest, derivedOf } from "../../control/src/manifest-cache";
import type { Failure, FailureKind, Impersonation, ModuleInfo, Outcome, RevalidationItem, ServiceAuthOutcome, SessionIdentity, SocketView, TenantRoute, TenantSession } from "../session/ports";
import { type GatewayRpc, rpc } from "../session/rpc";
import { rememberBounded } from "../session/prewarm";
import type { ServiceControlOutcome } from "../principals/frames";
import type { ServiceGrant } from "../principals/grant";

/** `prewarmTenant`: one wake per tenant per interval per Control instance, a bounded map. */
export const PREWARM_TENANT_INTERVAL_MS = 10_000;
export const PREWARM_TENANT_MAX = 4_096;
/** Co-tenant memory (`noteCoTenants`): at most this many other tenants per tenant, each for at most a day. */
export const COTENANTS_PER_TENANT = 8;
export const COTENANT_TTL_MS = 24 * 60 * 60 * 1000;
const COTENANT_PERSIST_MS = 60 * 60 * 1000;
const COTENANT_KEY = "control/cotenants/";

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
  protected override publicationVersionReady(version: string): Promise<boolean> {
    return this.catalog().ready(version);
  }
  protected override async publicationVersionAbsent(version: string): Promise<boolean> {
    const serving = await this.catalog().serving(version);
    return !serving.runtime && !serving.actions;
  }
  private catalog(): import("./catalog-port").CatalogPort {
    const catalog=(this.env as unknown as {RUNTIME_CATALOG?:import("./catalog-port").CatalogPort}).RUNTIME_CATALOG;
    if(!catalog)throw new Error("runtime catalog service is not bound");
    return catalog;
  }
  protected override async runtimeNamespaceReady(version:string):Promise<boolean>{
    const probe="gonvex-topology-namespace";
    const identity=await this.catalog().identity(),active=this.syncs.activeReleases();
    if(identity.version!==version||Object.keys(identity.releases).length!==active.length
      ||active.some(release=>identity.releases[release.project]!==release.hash))return false;
    return await this.catalog().tenantId(probe)===this.env.TENANT.idFromName(probe).toString();
  }
  /** Tenant id -> when this instance last woke it for a page (`prewarmTenant`). */
  private readonly prewarmed = new Map<string, number>();
  /** The bound of `prewarmed` (a field so a test can lower it). */
  protected prewarmMax = PREWARM_TENANT_MAX;
  /**
   * The router asks this, without waiting, when a page of a tenant's subdomain opens its first socket
   * (kernel/session/prewarm.ts): the tenant object wakes while the Firebase sign-in runs, so the exchange's admission
   * of that tenant does not wait for a cold object. The selector resolves as `resolveTenant` does (exact id, else the
   * domain, of a provisioned, non-deleted, non-disabled tenant of the project; the same function). The tenant only
   * answers `ping()`.
   * At most once per tenant per `PREWARM_TENANT_INTERVAL_MS` per Control instance; unknown selectors do nothing.
   */
  async prewarmTenant(projectId: string, selector: string): Promise<"warming" | "recent" | "unknown"> {
    if (typeof projectId !== "string" || typeof selector !== "string" || !projectId || !selector || projectId.length > 128 || selector.length > 63) return "unknown";
    // The same resolution as every tenant auth (`resolveTenant`): its refusals (unknown, deleted, disabled, no
    // database) all mean "nothing to wake".
    let tenantId: string;
    try { tenantId = resolveTenant(this.context(), projectId, selector).tenantId; } catch { return "unknown"; }
    const now = Date.now();
    if (!this.wake(tenantId, now, "page")) return "recent";
    // A multi-tenant account's sign-in also admits its other tenants: wake the ones recent sign-ins brought along.
    for (const other of this.recentCoTenants(tenantId, now)) this.wake(other, now, "co-tenant");
    return "warming";
  }

  /** One background `ping()` of `tenantId`, unless this instance woke it within `PREWARM_TENANT_INTERVAL_MS`. */
  private wake(tenantId: string, now: number, reason: "page" | "co-tenant"): boolean {
    const last = this.prewarmed.get(tenantId);
    if (last !== undefined && now - last < PREWARM_TENANT_INTERVAL_MS) return false;
    rememberBounded(this.prewarmed, tenantId, now, this.prewarmMax);
    const started = Date.now();
    this.ctx.waitUntil(rpc<{ ping(): Promise<number> }>(this.env.TENANT, tenantId).ping()
      .then(() => span("control.prewarm", { reason, ms: Date.now() - started }), () => span("control.prewarm", { reason, ms: Date.now() - started, failed: true })));
    return true;
  }

  /**
   * Tenant id -> the other tenants recent sign-ins admitted together with it as the active tenant, and when.
   * Tenant ids only (no account), bounded: `COTENANTS_PER_TENANT` per tenant, `COTENANT_TTL_MS`; kept in memory and
   * in this object's KV (`control/cotenants/<tenant>`).
   * `prewarmTenant` pings these too: a multi-tenant account's exchange admits every tenant of the account and waits
   * for each cold one, but the page names only the active tenant before the sign-in.
   */
  private readonly coTenants = new Map<string, Map<string, number>>();
  protected noteCoTenants(result: unknown): void {
    const value = result as { activeTenantId?: unknown; tenants?: unknown } | null;
    const active = typeof value?.activeTenantId === "string" ? value.activeTenantId : "";
    const tenants = Array.isArray(value?.tenants) ? value.tenants : [];
    if (!active || tenants.length < 2) return;
    const now = Date.now();
    const others = this.loadCoTenants(active);
    const before = [...others.keys()].sort().join("\u0000"), oldest = Math.min(...others.values());
    this.coTenants.delete(active);
    for (const tenant of tenants) {
      const id = (tenant as { id?: unknown } | null)?.id;
      if (typeof id !== "string" || !id || id === active) continue;
      others.delete(id);
      others.set(id, now);
    }
    while (others.size > COTENANTS_PER_TENANT) others.delete(others.keys().next().value!);
    this.coTenants.set(active, others);
    if (this.coTenants.size > PREWARM_TENANT_MAX) this.coTenants.delete(this.coTenants.keys().next().value!);
    // Survives an evicted or restarted Control. Written only when the set changes, or hourly to keep it inside the
    // TTL, so a sign-in's reply does not routinely wait for a confirmed write.
    if ([...others.keys()].sort().join("\u0000") !== before || now - oldest > COTENANT_PERSIST_MS) {
      this.ctx.storage.kv.put(`${COTENANT_KEY}${active}`, [...others]);
    }
  }
  private loadCoTenants(tenantId: string): Map<string, number> {
    let others = this.coTenants.get(tenantId);
    if (!others) {
      const stored = this.ctx.storage.kv.get<Array<[string, number]>>(`${COTENANT_KEY}${tenantId}`);
      others = new Map(Array.isArray(stored) ? stored.filter(([id, at]) => typeof id === "string" && typeof at === "number") : []);
      this.coTenants.set(tenantId, others);
      if (this.coTenants.size > PREWARM_TENANT_MAX) this.coTenants.delete(this.coTenants.keys().next().value!);
    }
    return others;
  }
  /** The live co-tenants of `tenantId`, most recent first. */
  private recentCoTenants(tenantId: string, now: number): string[] {
    const others = this.loadCoTenants(tenantId);
    for (const [id, at] of others) if (now - at > COTENANT_TTL_MS) others.delete(id);
    return [...others.keys()].reverse();
  }

  private readonly assemblyInstance = crypto.randomUUID();
  async runtimeInstance(): Promise<string> { return this.assemblyInstance; }
  private auth(request: { token?: string; project?: string; tenant?: string; controlOnly?: boolean; public?: boolean; connectionId?: string; liveOnly?: boolean }): Promise<AuthOutcome> {
    return this.authenticate({
      id: "", token: request.token ?? null, project: request.project ?? null, tenant: request.tenant ?? null,
      controlOnly: request.controlOnly ?? false, public: request.public ?? false, connectionId: request.connectionId ?? "",
      ...(request.liveOnly ? { liveOnly: true } : {}),
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
    return { ok: true, value: outcome.connection.identity as SessionIdentity, ...this.sessionExpiry(token) };
  }

  /** X17j: the session's `expires_at`, so a gateway identity lease never outlives the token. */
  private sessionExpiry(token: string): { expiresAt?: number } {
    const trimmed = token.trim();
    if (!trimmed.startsWith("gvx_session_")) return {};
    const at = this.sql.value<number>("SELECT expires_at FROM gonvex_auth_sessions WHERE token_hash=$1 AND revoked_at IS NULL", sha256Hex(trimmed));
    return typeof at === "number" && Number.isFinite(at) ? { expiresAt: at } : {};
  }

  /** `options.liveOnly` (#540 x #539): admit the tenant live, not from a just-made sign-in admission. */
  async sessionAuthenticate(token: string, projectId: string | undefined, tenant: string | undefined, options?: { liveOnly?: boolean }): Promise<Outcome<TenantSession>> {
    // An empty tenant asks for the first admitted one (`first_admitted_tenant`), as in K06's own branch.
    const started = Date.now();
    const outcome = await this.auth({ token, ...(projectId !== undefined ? { project: projectId } : {}), ...(tenant !== undefined ? { tenant } : {}),
      ...(options?.liveOnly ? { liveOnly: true } : {}) });
    span("control.rpc", { method: "sessionAuthenticate", ms: Date.now() - started });
    if (outcome.message.type !== "auth.result" || !outcome.connection.tenant) return failed(outcome);
    return { ok: true, value: sessionOf(outcome.connection.tenant), ...this.sessionExpiry(token) };
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
    const started = Date.now();
    try { return await this.dispatchFrame(socket, frame); }
    finally { span("control.rpc", { method: "sessionDispatch", type: frame.type, path: "path" in frame && typeof frame.path === "string" ? frame.path : "", ms: Date.now() - started }); }
  }

  private async dispatchFrame(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    const connection = connectionOf(socket);
    if ((frame.type === "query.subscribe" || frame.type === "query.call") && frame.path.startsWith("control.support.")) this.noteSupportInterest(connection.projectId, socket.host);
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
        if (frame.path === "control.auth.exchangeExternalToken" || frame.path === "control.auth.refreshSession" || frame.path === "control.auth.passwordLogin") this.noteCoTenants(outcome.value);
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

  /** X15: parsed once per manifest row version (`control/src/manifest-cache.ts`). Shared: never mutate it. */
  protected manifestOf(projectId: string): Record<string, unknown> | null {
    return deployedManifest(this.sql, projectId)?.manifest ?? null;
  }

  /** The deployed manifest (`/dev/manifest`). */
  async moduleManifest(projectId: string): Promise<Record<string, unknown> | null> {
    return this.manifestOf(projectId);
  }

  async moduleInfo(projectId: string): Promise<ModuleInfo | null> {
    const deployed = deployedManifest(this.sql, projectId);
    if (!deployed) return null;
    return derivedOf(deployed, "moduleInfo", (): ModuleInfo => {
      const module = (deployed.manifest.module ?? {}) as { hash?: string; files?: Record<string, string> };
      const contract = module.files?.["client-contract.json"];
      return { artifactHash: String(module.hash ?? ""), clientContract: contract ? (JSON.parse(atob(contract)) as { version: number }).version : null };
    });
  }

  /**
   * X15: a gateway's `watch` and the module read that must follow it, in one call. The holder is registered
   * before the read in this same turn, so a publication after the read still notifies it.
   */
  async watchModule(projectId: string, binding: string, id: string): Promise<ModuleInfo | null> {
    await this.watch(projectId, binding, id);
    return this.moduleInfo(projectId);
  }

  async moduleEpoch(projectId: string, databaseUrl: string): Promise<string> {
    const deployed = deployedManifest(this.sql, projectId);
    const epoch = (manifest: Record<string, unknown>) => {
      const module = (manifest.module ?? {}) as { hash?: string };
      return replicaEpoch({
        projectId, databaseUrl, functions: (manifest.functions ?? null) as Json, schema: (manifest.schema ?? null) as Json, moduleHash: String(module.hash ?? ""),
      });
    };
    if (!deployed) return epoch({});
    // A rejected hash is not kept: the next call computes it again.
    const key = `epoch\u0000${databaseUrl}`;
    const pending = derivedOf(deployed, key, () => epoch(deployed.manifest));
    return pending.catch((error) => { deployed.derived.delete(key); throw error; });
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
