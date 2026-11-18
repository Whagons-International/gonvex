/**
 * The control object with the operator surface (K19).
 *
 * `OperatorControlObject` extends K06's `ControlObject` and adds, without
 * changing it:
 *
 * - `fetch` for `/dev/*` (routes.ts) and `/healthz`; `/auth/*` stays K06's;
 * - the hibernatable operator sockets: `/dev/dashboard/ws` (dashboard.rs),
 *   `/dev/metrics/stream` and `/dev/logs/stream` (operations.rs);
 * - RPC for the router: `devSync`, `rolloutReport`, `syncKeyAccepted`;
 * - RPC for tenant objects: `operatorDataChanged`;
 * - one alarm multiplexed between K06's hourly prune and the socket ticks
 *   (alarms only, never timers: 0016 G0-9).
 *
 * The deployed Worker exports this class as the control object.
 */
import { resetTolerantTenantPort } from "./tenant/reset";
import { ControlObject, type ControlEnv } from "../src/control-object";
import type { Ctx } from "../src/context";
import type { TenantPort } from "../src/tenant-port";
import type { TenantRoute } from "../src/types";
import { OperatorAuth, bearer } from "./auth";
import { operatorConfig, type OperatorConfig, type OperatorEnv } from "./config";
import { DASHBOARD_TAG, LOGS_TAG, METRICS_TAG, OperatorSockets } from "./sockets";
import { error, json, HttpError } from "./http";
import { HttpReleaseBuilder, type DeploymentRef, type DeploymentGrant, type ReleaseBuilder, type ReleaseSummary, type RolloutMessage, type RolloutReport } from "./release";
import { handleOperatorRoute, type Op } from "./routes";
import { OPERATOR_SCHEMA, SyncCoordinator, type SyncOutcome, type SyncRequest } from "./sync";
import { installDeploymentSchema } from "./deployment";
import { deleteTenantStorage, rustTenantError, waitForRelease, type OperatorTenantPort } from "./tenant/port";

export interface OperatorObjectEnv extends ControlEnv, OperatorEnv {
  /** Releases, plans and incoming bodies (release.ts). */
  RELEASES?: R2Bucket;
  /** The rollout queue (D.2.4). */
  ROLLOUT?: Queue<RolloutMessage>;
  /** The release builder: a Container or service binding... */
  RELEASE_BUILDER?: Fetcher;
  /** ...or a URL (local development: the Node builder). */
  GONVEX_RELEASE_BUILDER_URL?: string;
  GONVEX_RELEASE_BUILDER_TOKEN?: string;
}

const PRUNE_DUE_KEY = "k19/prune-due";
const PRUNE_EVERY_MS = 60 * 60 * 1000;

/** The operator identity the admin key acts as (`OperatorAuth`). */
const ADMIN_KEY_OPERATOR = "admin@gonvex.local";

export class OperatorControlObject extends ControlObject {
  readonly operatorConfig: OperatorConfig;
  readonly syncs: SyncCoordinator;
  readonly sockets: OperatorSockets;
  private builderOverride: ReleaseBuilder | undefined;

  constructor(state: DurableObjectState, env: OperatorObjectEnv) {
    super(state, env);
    this.operatorConfig = operatorConfig(env);
    state.blockConcurrencyWhile(async () => {
      this.sql.script(OPERATOR_SCHEMA);
      installDeploymentSchema(this.sql);
    });
    this.syncs = new SyncCoordinator({
      sql: this.sql,
      builder: {
        inspect: (...a) => this.builder().inspect(...a),
        prepare: (...a) => this.builder().prepare(...a),
        activate: (...a) => this.builder().activate(...a),
        deploy: (...a) => this.builder().deploy(...a),
      },
      enqueue: async (message) => {
        const queue = (this.env as OperatorObjectEnv).ROLLOUT;
        if (!queue) throw new Error("the rollout queue (ROLLOUT) is not bound");
        await queue.send(message, { contentType: "json" });
      },
      versionReady: (version) => this.publicationVersionReady(version),
      versionAbsent: (version) => this.publicationVersionAbsent(version),
      activateTenants: async (projectId, tenants, hash, generation) => {
        for (const tenant of tenants) if (!await this.activatePublicationTenant(tenant, { projectId, hash, generation })) return false;
        return true;
      },
      moduleReloaded: async (projectId, _tenants, artifactHash, _generation, clientContract, publicationId) => {
        // Durable watches include tenant and control-only sockets, even after a reset.
        for (const { binding, name } of this.watchersFor(projectId).values()) {
          if (binding !== "GATEWAY") continue;
          const namespace = this.env[binding] as DurableObjectNamespace;
          const stub = namespace.get(namespace.idFromString(name)) as unknown as import("../../kernel/session/rpc").GatewayRpc;
          await stub.moduleReloaded(projectId, { artifactHash, clientContract, publicationId });
        }
      },
      syncCrons: async () => {
        // K11 owns `scheduler.sync_project`; the kernel scheduler re-plans crons from the active release.
      },
      deadlineMs: this.operatorConfig.syncDeadlineMs,
      devSyncKey: this.operatorConfig.devSyncKey,
      acceptsKey: (project, key, fallback) => this.operatorAuth().projectAcceptsSyncKey(project, key, fallback),
    });
    this.sockets = new OperatorSockets({
      ctx: state,
      operatorContext: () => this.operatorContext(),
      operatorAuth: () => this.operatorAuth(),
      armAlarm: () => this.armAlarm(),
      operatorConfig: this.operatorConfig,
    });
  }

  /** Generic K19 fixtures have no baked catalog; the serving assembly overrides this. */
  protected publicationVersionReady(_version: string): boolean | Promise<boolean> { return true; }
  /** True only when no component serves the version; without a serving composition nothing can prove it. */
  protected publicationVersionAbsent(_version: string): boolean | Promise<boolean> { return false; }
  protected activatePublicationTenant(tenant: string, release: {projectId:string;hash:string;generation:number}): Promise<boolean> {
    return this.tenantPort(tenant).operatorActivateRelease(release);
  }
  protected async runtimeNamespaceReady(_version:string):Promise<boolean>{return false;}
  async runtimeTopology(action:"status"|"freeze"|"thaw",version?:string) {
    let failure:{status:number;error:string}|undefined;
    try{
      if(action==="freeze")this.syncs.deployments.freezeTopology();
      if(action==="thaw"){
        if(!version||!await this.publicationVersionReady(version))throw new HttpError(503,"runtime catalog version not ready");
        if(!await this.runtimeNamespaceReady(version))throw new HttpError(409,"runtime tenant namespace or active catalog differs");
        this.syncs.deployments.thawTopology();
      }
    }catch(cause){if(!(cause instanceof HttpError))throw cause;failure={status:cause.status,error:cause.message};}
    return {frozen:this.syncs.deployments.topologyFrozen(),deployment:this.syncs.deployments.current(),releases:this.syncs.activeReleases(),...failure};
  }
  /** X03 dev/test fixture: drop the admin-key operator's REVOKED account tokens (the list keeps revoked tokens,
   * so a persistent dev deployment would otherwise grow one per conformance run). Active tokens are untouched. */
  async fixturePurgeRevokedTokens(): Promise<{ deleted: number } | { error: string }> {
    if (!(this.env as { GONVEX_ASSEMBLY_FIXTURE_KEY?: string }).GONVEX_ASSEMBLY_FIXTURE_KEY) return { error: "assembly fixture control disabled" };
    const deleted = this.sql.tx(() => this.sql.affected("DELETE FROM gonvex_account_access_tokens WHERE owner_email=$1 AND revoked_at IS NOT NULL", ADMIN_KEY_OPERATOR));
    return { deleted };
  }
  async publishRelease(syncId: string): Promise<{ready:boolean;error?:string;status?:number}> {
    try { return await this.syncs.publish(syncId); }
    catch (cause) {
      if (cause instanceof HttpError) return {ready:false,error:cause.message,status:cause.status};
      throw cause;
    }
  }
  async publicationReference(id:string):Promise<DeploymentRef> { return this.syncs.deployments.reference(id); }
  async currentDeployment():Promise<DeploymentRef|null> {return this.syncs.deployments.current();}
  async deploymentRequest(action:"authorize"|"settle"|"abort", ref:DeploymentRef, result:{ok?:boolean;error?:string;retain?:boolean;superseded?:boolean}):Promise<{ok:true;grant?:DeploymentGrant}|{ok:false;status:number;error:string}> {
    try {
      if (action === "authorize") return {ok:true,grant:await this.syncs.deployments.authorize(ref)};
      if (action === "settle") await this.syncs.deployments.settle(ref,{ok:result.ok===true,retain:result.retain===true,...(result.error?{error:result.error}:{})});
      else await this.syncs.deployments.abort(ref,result.error ?? "publication aborted",{superseded:result.superseded===true});
      return {ok:true};
    } catch (cause) {
      if (cause instanceof HttpError) return {ok:false,status:cause.status,error:cause.message};
      throw cause;
    }
  }

  /** Tests swap the builder for an in-process one. */
  setReleaseBuilderForTest(builder: ReleaseBuilder | undefined): void {
    this.builderOverride = builder;
  }

  private builder(): ReleaseBuilder {
    if (this.builderOverride) return this.builderOverride;
    const env = this.env as OperatorObjectEnv;
    if (env.RELEASE_BUILDER) {
      const fetcher = env.RELEASE_BUILDER;
      return new HttpReleaseBuilder((path, body) =>
        fetcher.fetch(`https://release-builder${path}`, {
          method: "POST",
          headers: { "content-type": "application/json", ...(env.GONVEX_RELEASE_BUILDER_TOKEN ? { authorization: `Bearer ${env.GONVEX_RELEASE_BUILDER_TOKEN}` } : {}) },
          body: JSON.stringify(body),
        }),
      );
    }
    if (env.GONVEX_RELEASE_BUILDER_URL) return HttpReleaseBuilder.forUrl(env.GONVEX_RELEASE_BUILDER_URL, env.GONVEX_RELEASE_BUILDER_TOKEN);
    throw new Error("no release builder is configured (RELEASE_BUILDER or GONVEX_RELEASE_BUILDER_URL)");
  }

  /** A per-call context, as K06's private `context()` builds it. */
  operatorCtx(): Ctx {
    const env = this.env;
    return {
      sql: this.sql,
      config: this.config,
      tenant: (route: TenantRoute) => env.TENANT.getByName(route.tenantId) as unknown as TenantPort,
      controlChanged: (projectId: string) => this.controlChanged(projectId),
      fetch: (input, init) => fetch(input, init),
      tenantCalls: 0,
    };
  }

  operatorAuth(): OperatorAuth {
    return new OperatorAuth(this.operatorCtx(), this.operatorConfig);
  }

  tenantPort(tenantId: string): OperatorTenantPort {
    return this.env.TENANT.getByName(tenantId) as unknown as OperatorTenantPort;
  }

  /** K06's `notify` (ControlChanged to watching socket owners). Private there; called by name. */
  private controlChanged(projectId: string): void {
    (this as unknown as { notify(projectId: string): void }).notify(projectId);
  }

  operatorContext(): Op {
    const ctx = this.operatorCtx();
    return {
      ctx,
      auth: new OperatorAuth(ctx, this.operatorConfig),
      config: this.operatorConfig,
      sync: this.syncs,
      releases: (this.env as OperatorObjectEnv).RELEASES,
      // A reset tenant (CPU limit, eviction) must not answer the next operator call with a 500 (X10, run9).
      tenant: (tenantId) => resetTolerantTenantPort(tenantId, () => this.tenantPort(tenantId)),
      controlChanged: (projectId) => this.controlChanged(projectId),
      tenantCounts: (projectId) => this.tenantCounts(projectId),
      provisionTenant: (projectId, tenantId, planId) => this.provisionTenant(projectId, tenantId, planId),
      deleteTenantStorage: (tenantId) => deleteTenantStorage(this.tenantPort(tenantId)),
    };
  }

  /** Applies the active release to one tenant object and waits until it is done or failed. */
  async provisionTenant(projectId: string, tenantId: string, planId: string): Promise<void> {
    const status = await waitForRelease(this.tenantPort(tenantId), planId, projectId, Date.now() + this.operatorConfig.syncDeadlineMs);
    if (status === "timeout") throw new Error(`tenant ${tenantId} provisioning did not finish within the deadline`);
    if (status.state === "failed") throw new Error(rustTenantError(status.error));
  }

  // ------------------------------------------------------------------ HTTP

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") return this.health(request);
    if (url.pathname === "/dev/dashboard/ws") return this.sockets.acceptDashboard(request);
    if (url.pathname === "/dev/metrics/stream") return this.sockets.acceptMetrics(request, url);
    if (url.pathname === "/dev/logs/stream") return this.sockets.acceptLogs(request, url);
    if (url.pathname.startsWith("/dev/")) {
      const response = await handleOperatorRoute(this.operatorContext(), request);
      // dashboard.rs `invalidate`: a successful /dev write re-runs every dashboard read.
      if (!["GET", "HEAD", "OPTIONS"].includes(request.method) && response.status >= 200 && response.status < 300) {
        this.ctx.waitUntil(this.sockets.refresh({ reason: "write" }));
      }
      return response;
    }
    return super.fetch(request);
  }

  /**
   * `/healthz` (lib.rs `health`): `{ok}`; full diagnostics only for an
   * `admin`-role, non-Local operator holding `admin:projects`.
   */
  private async health(request: Request): Promise<Response> {
    const headers = { "cache-control": "no-store", vary: "Authorization" };
    let diagnostics = false;
    if (request.headers.has("authorization")) {
      try {
        const actor = await this.operatorAuth().authorize(request.headers, "admin:projects");
        diagnostics = actor.role === "admin" && actor.credential !== "Local";
      } catch {
        diagnostics = false;
      }
    }
    if (!diagnostics) return json({ ok: true }, 200, headers);
    const active = this.syncs.activeReleases();
    const failed = Number(this.sql.value("SELECT count(*) FROM gonvex_cf_releases r WHERE state='failed' AND NOT EXISTS (SELECT 1 FROM gonvex_cf_active_releases a WHERE a.project_id=r.project_id)") ?? 0);
    return json(
      {
        ok: true,
        version: this.operatorConfig.runtimeVersion,
        time: new Date().toISOString(),
        databaseSet: true,
        database: { kind: "durable-object-sqlite" },
        valkeySet: false,
        rowsCache: false,
        runtimeManifests: { ready: failed === 0, failedProjects: failed },
        moduleHost: { ready: true, activeProjects: active.length, kind: "worker-version" },
        s3Set: !!(this.env as OperatorObjectEnv).RELEASES,
        sandbox: { enabled: false, ready: true, error: "" },
      },
      200,
      headers,
    );
  }

  // ------------------------------------------------------------------- RPC

  /** `/dev/sync` behind the router. The router deploys `deploy` after a success. */
  async devSync(request: SyncRequest): Promise<SyncOutcome> {
    const outcome = await this.syncs.sync(request);
    // A sync is a /dev write: dashboards re-read whatever happened.
    this.ctx.waitUntil(this.sockets.refresh({ reason: "write" }));
    return outcome;
  }

  /** The queue consumer's per-tenant report. */
  async rolloutReport(report: RolloutReport): Promise<{ accepted: boolean }> {
    return this.syncs.report(report);
  }

  /** Router pre-check: avoids storing an unauthenticated body (`project_accepts_sync_key`). */
  async syncKeyAccepted(project: string, key: string): Promise<boolean> {
    return this.operatorAuth().projectAcceptsSyncKey(project, key, this.operatorConfig.devSyncKey);
  }

  /** A tenant object committed a change: re-read dashboard data subscriptions on it. */
  async operatorDataChanged(projectId: string, tenantId: string): Promise<void> {
    await this.sockets.refresh({ reason: "data", projectId, tenantId });
  }

  /** The active release of a project (tests, the builder's version build). */
  async activeRelease(projectId: string): Promise<{ hash: string; generation: number; planId: string; summary: ReleaseSummary } | null> {
    return this.syncs.activeRelease(projectId);
  }

  // ------------------------------------------------------------ sockets

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.sockets.message(ws, message);
  }

  override async webSocketClose(ws: WebSocket, code: number, _reason: string, _wasClean: boolean): Promise<void> {
    try {
      ws.close(code === 1005 || code === 1006 ? 1000 : code);
    } catch {
      // already closed
    }
  }

  override async webSocketError(): Promise<void> {}

  // -------------------------------------------------------------- alarm

  /**
   * One alarm for K06's hourly prune and the operator sockets' ticks. K06's
   * `alarm()` prunes and re-arms itself one hour out; the due time is kept
   * here so a socket tick never prunes early and a prune is never skipped.
   */
  override async alarm(): Promise<void> {
    const now = Date.now();
    const pruneDue = this.ctx.storage.kv.get<number>(PRUNE_DUE_KEY) ?? now;
    if (now >= pruneDue) {
      await super.alarm();
      this.ctx.storage.kv.put(PRUNE_DUE_KEY, now + PRUNE_EVERY_MS);
    }
    await this.sockets.tick(now);
    await this.armAlarm();
  }

  async armAlarm(): Promise<void> {
    const pruneDue = this.ctx.storage.kv.get<number>(PRUNE_DUE_KEY) ?? Date.now() + PRUNE_EVERY_MS;
    const socketDue = this.sockets.nextDue();
    const next = socketDue === undefined ? pruneDue : Math.min(pruneDue, socketDue);
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > next || current < Date.now()) await this.ctx.storage.setAlarm(next);
  }
}

export { DASHBOARD_TAG, LOGS_TAG, METRICS_TAG, bearer, error };
