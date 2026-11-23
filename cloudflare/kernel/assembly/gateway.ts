// Production session ports. No fixture handlers or K05 stand-ins enter this graph.
import { tenantDispatchWithDeadline } from "./deadline";
import { GatewayObjectBase, type GatewayBindings, type GatewayEnv } from "../session/placement/gateway";
import type { Ports } from "../session/ports";
import { rpc, type TenantRpc } from "../session/rpc";
import { publicGuard, type PublicGuardEnv } from "../public/guard";
import { controlTelemetryPort, type TelemetryControlRpc } from "../telemetry/port";
import type { SessionControlObject } from "./control";
import type { ReplicaDelivery } from "../delivery/protocol";
import { telemetrySubject } from "../telemetry/limiter";
import { fixtureSpans } from "../delivery/spans";
import { admittedMember, leasesFor } from "../session/identity-lease";
import { controlReadKey, controlReadsFor, controlWrite } from "../session/control-read-cache";

type Control = Pick<SessionControlObject,
  "sessionProject" | "sessionIdentity" | "sessionAuthenticate" | "sessionImpersonation" |
  "sessionPublicTenant" | "sessionRevalidate" | "sessionDispatch" | "serviceAuthenticate" |
  "serviceControl" | "moduleInfo" | "moduleEpoch" | "connectionState" | "watch" | "watchModule" | "runtimeInstance"> & TelemetryControlRpc;

export interface AssemblyGatewayEnv extends GatewayEnv, PublicGuardEnv {
  GATEWAY: DurableObjectNamespace<any>;
  CONTROL: DurableObjectNamespace<any>;
  TENANT: DurableObjectNamespace<any>;
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
}


export function assemblyPorts(env: AssemblyGatewayEnv): Ports {
  const control = () => rpc<Control>(env.CONTROL, "control");
  const tenant = (id: string) => rpc<Required<TenantRpc>>(env.TENANT, id);
  const leases = leasesFor(env);
  const controlReads = controlReadsFor(env);
  return {
    control: {
      project: (id) => control().sessionProject(id),
      // X17j: identity leases (kernel/session/identity-lease.ts), one store per gateway object.
      loadSessionIdentity: async (token, project) => {
        const leased = leases.identity(token, project);
        if (leased) return { ok: true, value: leased };
        const outcome = await control().sessionIdentity(token, project);
        if (outcome.ok) leases.putIdentity(token, project, outcome.value, (outcome as { expiresAt?: number }).expiresAt);
        return outcome;
      },
      authenticateSession: async (token, project, id) => {
        const leased = id ? leases.session(token, project, id) : undefined;
        if (leased) {
          try {
            const admitted = admittedMember(await tenant(leased.route.tenantId).admit(leased.identity.account.id));
            if (admitted) return { ok: true, value: { ...leased, member: admitted.member, admissionRevision: admitted.admissionRevision } };
          } catch { /* Control answers below */ }
          leases.drop(token, project, id!);
        }
        // #540 review: after the tenant refused a leased member, Control must admit live too (never a recent admission).
        const outcome = leased
          ? await (control() as unknown as { sessionAuthenticate(t: string, p: string | undefined, i: string | undefined, o: { liveOnly: true }): ReturnType<Control["sessionAuthenticate"]> }).sessionAuthenticate(token, project, id, { liveOnly: true })
          : await control().sessionAuthenticate(token, project, id);
        if (outcome.ok && id) leases.putSession(token, project, id, outcome.value, (outcome as { expiresAt?: number }).expiresAt);
        return outcome;
      },
      authenticateImpersonation: (token, project, id, connection) => control().sessionImpersonation(token, project, id, connection),
      resolvePublicTenant: (project, id) => control().sessionPublicTenant(project, id),
      revalidate: (items) => control().sessionRevalidate(items),
      dispatch: async (socket, frame) => {
        // X17l: the Control reads a cold load repeats (kernel/session/control-read-cache.ts).
        const project = socket.connection.projectId;
        const key = controlReadKey(socket, frame);
        let cached = key ? controlReads.get(key) : undefined;
        let watched = false;
        if (cached && "id" in frame) {
          if (frame.type === "query.subscribe" && !controlReads.watchedRecently(project)) {
            await control().watch(project, "GATEWAY", env.GATEWAY.idFromName(socket.host).toString());
            controlReads.noteWatched(project);
            watched = true;
            // #542 re-review: an invalidation during the awaited watch drops the entry; read live instead.
            cached = controlReads.get(key!);
          }
          if (cached) return [JSON.stringify({ type: "query.result", id: frame.id, path: (frame as { path: string }).path, reason: "initial", result: cached.result })];
        }
        if (frame.type === "query.subscribe" && !watched) { await control().watch(project, "GATEWAY", env.GATEWAY.idFromName(socket.host).toString()); controlReads.noteWatched(project); }
        const write = controlWrite(frame);
        if (write) controlReads.invalidate(project);
        const generation = controlReads.generation(project); // #542 review: a read across an invalidation is not stored
        const replies = await control().sessionDispatch(socket, frame);
        // X17j + X17l: a Control write this gateway forwarded (logout, refresh, ...) drops the project's leases and
        // cached reads here at once; other gateways drop theirs on Control's notice (one Control alarm later).
        if (write) { leases.invalidate(project); controlReads.invalidate(project); }
        if (key && replies.length === 1) {
          try {
            const reply = JSON.parse(replies[0]!) as { type?: string; result?: unknown };
            if (reply.type === "query.result") controlReads.put(key, project, reply.result, generation);
          } catch { /* not cached */ }
        }
        return replies;
      },
      authenticateService: (token, project, id) => control().serviceAuthenticate(token, project, id),
      serviceCall: (grant, path, args) => control().serviceControl(grant, path, args),
      connectionState: (id, state) => control().connectionState(id, state),
    },
    modules: {
      module: (project) => control().moduleInfo(project),
      epoch: (project, database) => control().moduleEpoch(project, database),
    },
    tenants: {
      dispatch: async (socket, frame) => {
        const route = socket.tenant!.route;
        return tenantDispatchWithDeadline(route.projectId, frame, () => tenant(route.tenantId).dispatch(socket, frame),
          (id, path) => rpc<{ recordOverrun(id: string, path: string): Promise<"quarantined" | "committed" | "unrelated"> }>(env.TENANT, route.tenantId).recordOverrun(id, path));
      },
      serviceCall: (grant, call) => tenant(grant.route.tenantId).serviceCall(grant, call),
      presenceLeave: (route, id, host, previous) => tenant(route.tenantId).presenceLeave(id, host, previous ?? null),
      connectionReset: (route, id, epoch) => tenant(route.tenantId).connectionReset(id, epoch),
    },
    telemetry: controlTelemetryPort(control),
  };
}

const bindings: GatewayBindings = {
  ports: (env) => assemblyPorts(env as AssemblyGatewayEnv),
  projectAuthenticated: (env, project, holder) => rpc<Control>((env as AssemblyGatewayEnv).CONTROL, "control").watch(project, "GATEWAY", holder),
  // X15: one Control call instead of `watch` then `moduleInfo`. A Control version without it (rollout skew)
  // answers with workerd's missing-method error: then the same two calls, in order.
  projectAuthenticatedModule: async (env, project, holder) => {
    const control = () => rpc<Control>((env as AssemblyGatewayEnv).CONTROL, "control");
    try {
      return await control().watchModule(project, "GATEWAY", holder);
    } catch (error) {
      if (!missingMethod(error, "watchModule")) throw error;
      await control().watch(project, "GATEWAY", holder);
      return control().moduleInfo(project);
    }
  },
  tenantLink: (env, route) => rpc<Pick<TenantRpc, "attachGateway" | "detachGateway">>((env as AssemblyGatewayEnv).TENANT, route.tenantId),
  publicGuard: (env) => publicGuard(env as AssemblyGatewayEnv),
};

/** workerd's error for an RPC method the callee's version does not have. */
export function missingMethod(error: unknown, method: string): boolean {
  const message = String((error as Error | undefined)?.message ?? "");
  return message.includes("does not implement") && message.includes(`"${method}"`);
}

/** X17e: attempts of a failed Control refresh (retries after 250 ms, 1 s, 4 s, 16 s). */
export const REFRESH_ATTEMPTS = 5;
export const REFRESH_RETRY_MS = 250;

interface RefreshedSession { readonly closed: boolean; controlChanged(projectId: string, reason: "control-change" | "presence-change"): Promise<void> }

/**
 * X17e (#529 review): a refresh that fails is retried with backoff instead of leaving the subscription stale until
 * the next notification. A retry's calls go through CallOrigin like any other: if launching then is not safe, they
 * wait for the gateway's alarm. After `REFRESH_ATTEMPTS` the session keeps its last result (as before). A module
 * function, not a method: every gateway method counts as an inbound request (`trackEntries`).
 */
export async function refreshSessions(
  sessions: readonly RefreshedSession[], projectId: string, reason: "control-change" | "presence-change",
  options: { attempts?: number; retryMs?: number; sleep?: (ms: number) => Promise<void> } = {},
): Promise<void> {
  const attempts = options.attempts ?? REFRESH_ATTEMPTS, retryMs = options.retryMs ?? REFRESH_RETRY_MS;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  let pending = sessions;
  for (let attempt = 1; ; attempt++) {
    const results = await Promise.allSettled(pending.map((session) => session.controlChanged(projectId, reason)));
    const failed = pending.filter((session, index) => results[index]!.status === "rejected" && !session.closed);
    if (!failed.length) return;
    const error = String((results.find((result) => result.status === "rejected") as PromiseRejectedResult).reason);
    if (attempt >= attempts) {
      console.error("gateway: Control refresh failed; giving up", { projectId, sessions: failed.length, attempts: attempt, error });
      return;
    }
    console.error("gateway: Control refresh failed; retrying", { projectId, sessions: failed.length, attempt, error });
    await sleep(retryMs * 4 ** (attempt - 1));
    pending = failed;
  }
}

export class AssemblyGateway extends GatewayObjectBase<AssemblyGatewayEnv> {
  private controlInstance: string | undefined;
  private fixtureBootstrapGate: {promise:Promise<void>;release():void;entered:number;phase:'bootstrap'|'challenge'}|undefined;
  protected bindings(): GatewayBindings { return bindings; }
  constructor(ctx: DurableObjectState, env: AssemblyGatewayEnv) {
    super(ctx, env);
    ctx.waitUntil(this.refreshControlWatches());
  }
  private async refreshControlWatches(): Promise<void> {
    const projects = new Set([...this.host.sessions.values()].filter((session) => !session.closed && session.record.connection.projectId).map((session) => session.record.connection.projectId));
    const control = rpc<Control>(this.env.CONTROL, "control");
    if (!projects.size) return;
    const instance = await control.runtimeInstance();
    const restarted = this.controlInstance !== instance;
    this.controlInstance = instance;
    for (const project of projects) {
      await control.watch(project, "GATEWAY", this.holderId());
      // A Control restart loses its in-memory watcher set. Refresh also covers
      // changes in the interval before this gateway registered again.
      // X17j + X17l (#542 re-review): leases and cached reads go first, so the refresh that recovers lost notices reads
      // live and no lease outlives a revocation whose notice the restart lost.
      if (restarted) { leasesFor(this.env).invalidate(project); controlReadsFor(this.env).invalidate(project); }
      if (restarted) this.ctx.waitUntil(Promise.all([...this.host.sessions.values()].map((session) => session.controlChanged(project))));
    }
  }
  override async alarm(): Promise<void> {
    await super.alarm();
    await this.refreshControlWatches();
  }
  /** X17e: a notice from Control's alarm (depth 1); its refreshes launch at once (CallOrigin `notice`). */
  async controlChanged(projectId: string): Promise<void> {
    // X17j: a revocation (session, account, membership) reaches the gateway as this notice: no lease outlives it.
    leasesFor(this.env).invalidate(projectId);
    // X17l: cached Control reads of the project go before the subscription refresh reads live.
    controlReadsFor(this.env).invalidate(projectId);
    this.ctx.waitUntil(refreshSessions([...this.host.sessions.values()], projectId, "control-change"));
  }
  async presenceChanged(projectId: string): Promise<void> {
    this.ctx.waitUntil(refreshSessions([...this.host.sessions.values()], projectId, "presence-change"));
  }
  /** X13i fixture: switch dev spans in this isolate; returns the lines captured since the last call. */
  async fixtureSpans(on: boolean) {
    if (!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY) throw new Error("assembly fixture control disabled");
    return fixtureSpans(on === true);
  }
  /** X03 dev/test fixture: a backend restart for conformance on a deployed Worker (Rust: process restart).
   * Aborts this holder: every socket drops abruptly and nothing in memory survives; storage stays. */
  async fixtureRestart(): Promise<void> {
    if (!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY) throw new Error("assembly fixture control disabled");
    this.ctx.abort("assembly fixture: gateway restart");
  }
  /** Private dev evidence only: actual holders and windows, never auth tokens,
   * Member permissions, recovery metadata or an alternative telemetry handler. */
  async fixtureTelemetry(projectId: string) {
    if (!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY) throw new Error("assembly fixture inspection disabled");
    const sessions = [...this.host.sessions.values()].filter((s) => !s.closed && s.record.connection.projectId === projectId);
    if (sessions.length > 128) throw new Error("fixture inspection page exceeds 128 sockets");
    return { holder: this.gatewayName, at: Date.now(), sockets: sessions.map((s) => {
      const r = s.record, accountId = r.connection.identity?.account.id ?? null;
      const subject = telemetrySubject(projectId, accountId, r.connectionId);
      return { connectionId: r.connectionId, accountId, tenantId: r.connection.tenant?.route.tenantId ?? null,
        window: this.host.telemetry.limiter.inspect(subject.key) };
    }) };
  }
  /** Dev-only fault gate on the actual existing ordered delivery (single or batched). It never
   * supplies a reply or substitutes a handler; release runs the real base. */
  protected override replicaGate(_tenantId:string,payload:ReplicaDelivery):Promise<void>|undefined{
    const gate=this.env.GONVEX_ASSEMBLY_FIXTURE_KEY?this.fixtureBootstrapGate:undefined;
    if(gate&&payload.scopeFence===1&&!payload.recovery&&!payload.initialFailure&&!payload.shared.length
      &&payload.watermark===undefined&&!payload.membership
      &&(gate.phase==='bootstrap'?Object.keys(payload.sockets).length>0:Object.keys(payload.sockets).length===0)
      &&Object.values(payload.sockets).every(work=>!work.ops.length&&work.watermark===undefined&&work.opened===undefined&&work.removed===undefined&&!work.initialScope&&!work.initialCompletions)){
      if(gate.entered>=8)return Promise.reject(new Error("assembly fixture bootstrap gate exhausted"));
      gate.entered++;return gate.promise;
    }
    return undefined;
  }
  async fixtureBootstrapControl(hold?:boolean,phase:'bootstrap'|'challenge'='bootstrap'){
    if(!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY)throw new Error("assembly fixture control disabled");
    if(hold!==undefined&&typeof hold!=="boolean")throw new Error("fixture hold must be boolean");
    if(phase!=='bootstrap'&&phase!=='challenge')throw new Error('invalid fixture gate phase');
    if(hold===true){
      const previous=this.fixtureBootstrapGate;
      if(previous?.phase===phase)throw new Error("assembly fixture bootstrap already held");
      let release!:()=>void;const promise=new Promise<void>(resolve=>{release=resolve;});
      this.fixtureBootstrapGate={promise,release,entered:0,phase};
      // A private phase handoff arms the NEXT genuine ordered RPC before the
      // previous one resumes. It never manufactures a receipt or a retry event.
      previous?.release();
    }else if(hold===false){const old=this.fixtureBootstrapGate;this.fixtureBootstrapGate=undefined;old?.release();}
    return {holding:!!this.fixtureBootstrapGate,entered:this.fixtureBootstrapGate?.entered??0,phase:this.fixtureBootstrapGate?.phase??null,
      pendingInitials:[...this.host.sessions.values()].reduce((n,session)=>n+Object.keys(session.record.initialRequests??{}).length,0)};
  }
  /** Private lifecycle observation. Calling it can wake this holder; use only
   * AFTER an unobserved deadline window when diagnosing idle behavior. */
  async fixtureLifecycle() {
    if (!this.env.GONVEX_ASSEMBLY_FIXTURE_KEY) throw new Error("assembly fixture inspection disabled");
    const native = this.ctx.getWebSockets();
    if (native.length > 128 || this.host.sessions.size > 128) throw new Error("fixture lifecycle page exceeds 128 sockets");
    return { holder: this.gatewayName, at: Date.now(), alarmAt: await this.ctx.storage.getAlarm(), nativeCount: native.length,
      sessions: [...this.host.sessions].map(([ws,session])=>({connectionId:session.record.connectionId,closed:session.closed,
        pendingUntil:session.record.pendingUntil,openedAt:session.record.openedAt,readyState:ws.readyState,native:native.includes(ws),
        authenticated:!!session.record.connection.identity,tenant:!!session.record.tenant})) };
  }
  /** New method fails closed against older holders before same-id replies
   * could be applied without generation authority. */
  async settleActionReply(target: { tenantId: string; connectionId: string; epoch: number; id: string; path: string; generation?: number }, text: string): Promise<{ v: 1; status: "enqueued" | "obsolete" }> {
    if (!Number.isSafeInteger(target.generation) || target.generation! <= 0) throw new Error("Action reply generation unsupported");
    for (const session of this.host.sessions.values()) {
      if (session.record.connectionId === target.connectionId && (session.record.tenant?.route.tenantId ?? session.record.service?.route.tenantId) === target.tenantId) {
        return { v: 1, status: await session.actionFinished(target.id, target.path, target.epoch, text, target.generation) };
      }
    }
    if (this.host.ownsConnection(target.connectionId)) throw new Error("Action reply holder is unavailable with durable ownership retained");
    return { v: 1, status: "obsolete" };
  }
  async actionFinished(target: { tenantId: string; connectionId: string; epoch: number; id: string; path: string }, text: string): Promise<void> {
    for (const session of this.host.sessions.values()) {
      if (session.record.connectionId === target.connectionId && (session.record.tenant?.route.tenantId ?? session.record.service?.route.tenantId) === target.tenantId) {
        await session.actionFinished(target.id, target.path, target.epoch, text);
        return;
      }
    }
  }
}
