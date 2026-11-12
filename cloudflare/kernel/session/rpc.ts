/**
 * Plain RPC shapes of the objects the session layer talks to. Workers RPC
 * stubs are typed through these (not through the classes) because the
 * runtime's stub type mapping does not terminate on recursive JSON types.
 */
import type { ClientFrame, Json } from "./decode";
import type { DeliveryBatch, DeliveryReceipt } from "./placement/gateway";
import type { ConnectionReport } from "../presence/hub";
import type { PresenceDelivery, PresenceReceipt } from "../presence/tenant";
import type { ServiceTenantCall, ServiceTenantOutcome } from "../principals/frames";
import type { ServiceGrant } from "../principals/grant";
import type {
  Impersonation, Member, ModuleInfo, Outcome, RevalidationItem, SessionIdentity, SocketView, TenantRoute, TenantSession,
} from "./ports";

export interface GatewayRpc {
  deliver(batch: DeliveryBatch): Promise<DeliveryReceipt>;
  moduleReloaded(projectId: string, module: ModuleInfo | null): Promise<number>;
  ping(): Promise<number>;
  hopToTenant(tenant: string, n: number): Promise<number[]>;
  presence(delivery: PresenceDelivery): Promise<PresenceReceipt>;
  presenceConnections(tenantId: string): Promise<ConnectionReport[]>;
  liveConnections(connectionIds: string[]): Promise<string[]>;
}

export interface TenantRpc {
  addMember(input: { id: string; accountId: string; displayName: string }): Promise<Member>;
  admit(accountId: string): Promise<(Member & { admissionRevision: number }) | null>;
  setRole(memberId: string, role: string): Promise<{ gateways: number; sockets: number }>;
  commit(batch: Omit<DeliveryBatch, "v" | "revision" | "tenantId">): Promise<{ revision: number; gateways: number; sockets: number }>;
  attachGateway(gateway: string): Promise<void>;
  detachGateway(gateway: string): Promise<void>;
  registeredGateways(): Promise<string[]>;
  dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]>;
  ping(): Promise<number>;
  hopToGateway(gateway: string, n: number): Promise<number[]>;
  presenceLeave(connectionId: string, host: string, previous?: ConnectionReport | null): Promise<void>;
  /** K17: implemented by the K07 tenant object, reached only after a service grant. */
  serviceCall?(grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome>;
}

export interface ControlRpc {
  createProject(name: string): Promise<{ projectId: string; projectKey: string }>;
  deploy(projectId: string, manifest: Record<string, unknown>): Promise<{ ok: boolean; functionCount: number }>;
  createTenant(projectId: string, tenantId: string, name: string): Promise<{ id: string }>;
  createMember(projectId: string, tenantId: string, input: { email: string; password: string; name: string }): Promise<{ accountId: string; memberId: string }>;
  module(projectId: string): Promise<ModuleInfo | null>;
  epoch(projectId: string, databaseUrl: string): Promise<string>;
  project(projectId: string): Promise<Outcome<null>>;
  loadSessionIdentity(token: string, projectId: string | undefined): Promise<Outcome<SessionIdentity>>;
  authenticateSession(token: string, projectId: string | undefined, tenant: string | undefined): Promise<Outcome<TenantSession>>;
  authenticateImpersonation(token: string, projectId: string | undefined, tenant: string | undefined, connectionId: string): Promise<Outcome<Impersonation>>;
  resolvePublicTenant(projectId: string, tenant: string): Promise<Outcome<TenantRoute>>;
  revalidate(items: RevalidationItem[]): Promise<boolean[]>;
  dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]>;
}

/** A typed RPC stub for the object `name` of `namespace`. */
export function rpc<T>(namespace: DurableObjectNamespace<any>, name: string): T {
  return (namespace as unknown as { getByName(name: string): unknown }).getByName(name) as T;
}

export type { Json };
