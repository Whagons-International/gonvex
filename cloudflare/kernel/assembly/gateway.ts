// Production session ports. No fixture handlers or K05 stand-ins enter this graph.
import { tenantDispatchWithDeadline } from "./deadline";
import { GatewayObjectBase, type GatewayBindings, type GatewayEnv } from "../session/placement/gateway";
import type { Ports } from "../session/ports";
import { rpc, type TenantRpc } from "../session/rpc";
import { publicGuard, type PublicGuardEnv } from "../public/guard";
import { controlTelemetryPort, type TelemetryControlRpc } from "../telemetry/port";
import type { SessionControlObject } from "./control";
import type { ReplicaDelivery, ReplicaReceipt } from "../delivery/protocol";
import { telemetrySubject } from "../telemetry/limiter";

type Control = Pick<SessionControlObject,
  "sessionProject" | "sessionIdentity" | "sessionAuthenticate" | "sessionImpersonation" |
  "sessionPublicTenant" | "sessionRevalidate" | "sessionDispatch" | "serviceAuthenticate" |
  "serviceControl" | "moduleInfo" | "moduleEpoch" | "connectionState" | "watch" | "runtimeInstance"> & TelemetryControlRpc;

export interface AssemblyGatewayEnv extends GatewayEnv, PublicGuardEnv {
  GATEWAY: DurableObjectNamespace<any>;
  CONTROL: DurableObjectNamespace<any>;
  TENANT: DurableObjectNamespace<any>;
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
}

export function assemblyPorts(env: AssemblyGatewayEnv): Ports {
  const control = () => rpc<Control>(env.CONTROL, "control");
  const tenant = (id: string) => rpc<Required<TenantRpc>>(env.TENANT, id);
  return {
    control: {
      project: (id) => control().sessionProject(id),
      loadSessionIdentity: (token, project) => control().sessionIdentity(token, project),
      authenticateSession: (token, project, id) => control().sessionAuthenticate(token, project, id),
      authenticateImpersonation: (token, project, id, connection) => control().sessionImpersonation(token, project, id, connection),
      resolvePublicTenant: (project, id) => control().sessionPublicTenant(project, id),
      revalidate: (items) => control().sessionRevalidate(items),
      dispatch: async (socket, frame) => {
        if (frame.type === "query.subscribe") await control().watch(socket.connection.projectId, "GATEWAY", env.GATEWAY.idFromName(socket.host).toString());
        return control().sessionDispatch(socket, frame);
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
  tenantLink: (env, route) => rpc<Pick<TenantRpc, "attachGateway" | "detachGateway">>((env as AssemblyGatewayEnv).TENANT, route.tenantId),
  publicGuard: (env) => publicGuard(env as AssemblyGatewayEnv),
};

export class AssemblyGateway extends GatewayObjectBase<AssemblyGatewayEnv> {
  private controlInstance: string | undefined;
  private fixtureBootstrapGate: {promise:Promise<void>;release():void;entered:number;phase:'bootstrap'|'challenge'}|undefined;
  protected bindings(): GatewayBindings { return bindings; }
  constructor(ctx: DurableObjectState, env: AssemblyGatewayEnv) {
    super(ctx, env);
    ctx.waitUntil(this.refreshControlWatches());
  }
  private async refreshControlWatches(): Promise<void> {
    const projects = new Set([...this.host.sessions.values()].filter((session) => !session.closed && Object.keys(session.record.controlQueries ?? {}).length).map((session) => session.record.connection.projectId));
    const control = rpc<Control>(this.env.CONTROL, "control");
    if (!projects.size) return;
    const instance = await control.runtimeInstance();
    const restarted = this.controlInstance !== instance;
    this.controlInstance = instance;
    for (const project of projects) {
      await control.watch(project, "GATEWAY", this.ctx.id.toString());
      // A Control restart loses its in-memory watcher set. Refresh also covers
      // changes in the interval before this gateway registered again.
      if (restarted) this.ctx.waitUntil(Promise.all([...this.host.sessions.values()].map((session) => session.controlChanged(project))));
    }
  }
  override async alarm(): Promise<void> {
    await super.alarm();
    await this.refreshControlWatches();
  }
  async controlChanged(projectId: string): Promise<void> {
    this.ctx.waitUntil(Promise.all([...this.host.sessions.values()].map((session) => session.controlChanged(projectId))));
  }
  async presenceChanged(projectId: string): Promise<void> {
    this.ctx.waitUntil(Promise.all([...this.host.sessions.values()].map((session) => session.controlChanged(projectId, "presence-change"))));
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
  /** Dev-only fault gate on the actual existing ordered RPC. It never
   * supplies a reply or substitutes a handler; release runs the real base. */
  override async deliverReplica(tenantId:string,payload:ReplicaDelivery):Promise<ReplicaReceipt>{
    const gate=this.env.GONVEX_ASSEMBLY_FIXTURE_KEY?this.fixtureBootstrapGate:undefined;
    if(gate&&payload.scopeFence===1&&!payload.recovery&&!payload.initialFailure&&!payload.shared.length
      &&payload.watermark===undefined&&!payload.membership
      &&(gate.phase==='bootstrap'?Object.keys(payload.sockets).length>0:Object.keys(payload.sockets).length===0)
      &&Object.values(payload.sockets).every(work=>!work.ops.length&&work.watermark===undefined&&work.opened===undefined&&work.removed===undefined&&!work.initialScope&&!work.initialCompletions)){
      if(gate.entered>=8)throw new Error("assembly fixture bootstrap gate exhausted");
      gate.entered++;await gate.promise;
    }
    return super.deliverReplica(tenantId,payload);
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
