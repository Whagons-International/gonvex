// Production session ports. No fixture handlers or K05 stand-ins enter this graph.
import { tenantDispatchWithDeadline } from "./deadline";
import { GatewayObjectBase, type GatewayBindings, type GatewayEnv } from "../session/placement/gateway";
import type { Ports } from "../session/ports";
import { rpc, type TenantRpc } from "../session/rpc";
import { publicGuard, type PublicGuardEnv } from "../public/guard";
import { controlTelemetryPort, type TelemetryControlRpc } from "../telemetry/port";
import type { SessionControlObject } from "./control";
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
  async actionFinished(target: { tenantId: string; connectionId: string; epoch: number; id: string; path: string }, text: string): Promise<void> {
    for (const session of this.host.sessions.values()) {
      if (session.record.connectionId === target.connectionId && (session.record.tenant?.route.tenantId ?? session.record.service?.route.tenantId) === target.tenantId) {
        await session.actionFinished(target.id, target.path, target.epoch, text);
        return;
      }
    }
  }
}
