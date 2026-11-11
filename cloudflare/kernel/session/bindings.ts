/**
 * Resolves the session layer's ports from Worker bindings. Today every port
 * is a stub object (`stub/`); K06 and the tenant object replace the bindings
 * here, and nothing else in the session layer changes.
 */
import type { ClientFrame } from "./decode";
import type { Ports, SocketView, TenantRoute } from "./ports";
import type { GatewayBindings, TenantLink } from "./placement/gateway";
import { type ControlRpc, type TenantRpc, rpc } from "./rpc";

export interface SessionBindingsEnv {
  CONTROL_STUB: DurableObjectNamespace<any>;
  TENANT_STUB: DurableObjectNamespace<any>;
}

/** The single control object (K06 shards it later, if ever: 0012). */
export const CONTROL_OBJECT = "control";

/** Object name of a tenant (tenant ids are unique per runtime, README "Reference behaviours" 3). */
export const tenantObjectName = (route: Pick<TenantRoute, "tenantId">) => route.tenantId;

export function sessionPorts(env: SessionBindingsEnv): Ports {
  const control = () => rpc<ControlRpc>(env.CONTROL_STUB, CONTROL_OBJECT);
  const tenant = (socket: SocketView) => rpc<TenantRpc>(env.TENANT_STUB, tenantObjectName(socket.tenant!.route));
  return {
    control: {
      project: (projectId) => control().project(projectId),
      loadSessionIdentity: (token, projectId) => control().loadSessionIdentity(token, projectId),
      authenticateSession: (token, projectId, tenant) => control().authenticateSession(token, projectId, tenant),
      authenticateImpersonation: (token, projectId, tenantId, connectionId) => control().authenticateImpersonation(token, projectId, tenantId, connectionId),
      resolvePublicTenant: (projectId, tenant) => control().resolvePublicTenant(projectId, tenant),
      revalidate: (items) => control().revalidate(items),
      dispatch: (socket: SocketView, frame: ClientFrame) => control().dispatch(socket, frame),
    },
    modules: {
      module: (projectId) => control().module(projectId),
      epoch: (projectId, databaseUrl) => control().epoch(projectId, databaseUrl),
    },
    tenants: {
      dispatch: (socket: SocketView, frame: ClientFrame) => tenant(socket).dispatch(socket, frame),
      presenceLeave: (route, connectionId, host, previous) => rpc<TenantRpc>(env.TENANT_STUB, tenantObjectName(route)).presenceLeave(connectionId, host, previous ?? null),
      // K17: reached only after a service grant, which the control stand-in never gives.
      serviceCall: (grant, call) => rpc<Required<TenantRpc>>(env.TENANT_STUB, tenantObjectName(grant.route)).serviceCall(grant, call),
    },
  };
}

export const gatewayBindings: GatewayBindings = {
  ports: (env) => sessionPorts(env as SessionBindingsEnv),
  tenantLink: (env, route): TenantLink => {
    const tenant = rpc<TenantRpc>((env as SessionBindingsEnv).TENANT_STUB, tenantObjectName(route));
    return { attachGateway: (name) => tenant.attachGateway(name), detachGateway: (name) => tenant.detachGateway(name) };
  },
};
