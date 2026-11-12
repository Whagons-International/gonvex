/**
 * What the session layer needs from the rest of the kernel. The session layer
 * owns the socket, the handshake, auth and the frame boundary; everything
 * behind these interfaces belongs to other packages:
 *
 *  - `ControlPort`: the control object (K06). Sessions, identities, tenant
 *    resolution, admission against the tenant's `members` row, revalidation,
 *    and every `scope:"control"` frame.
 *  - `ModulePort`: the active module generation of a project (K19 deploys it):
 *    artifact hash, client contract, replica epoch.
 *  - `TenantPort`: tenant-scoped frames of an authenticated session (K07
 *    Reducers, K08 Queries, K10 replicas, K14 presence, K15 telemetry).
 *
 * Until those packages land, `stub/` implements the three ports with the
 * observable behaviour of the Rust runtime for the paths the session layer
 * exercises (contract-changes 0020 lists what the stubs cover).
 */
import type { ClientFrame, Json } from "./decode";
import type { Negotiated } from "./capabilities";
import type { ConnectionReport } from "../presence/hub";
import type { ServiceControlOutcome, ServiceTenantCall, ServiceTenantOutcome } from "../principals/frames";
import type { ServiceGrant } from "../principals/grant";

export interface Account {
  id: string;
  email: string;
  emailVerified: boolean;
  name: string;
  avatarUrl: string;
  provider: string;
}

export interface Member {
  id: string;
  accountId: string;
  status: string;
  displayName: string;
  avatarUrl: string;
  role: string;
  permissions: Json;
  membershipRevision: number;
}

export interface TenantRoute {
  projectId: string;
  tenantId: string;
  /** Reproduces Rust's `database_url` (0008); input to the replica epoch. */
  databaseUrl: string;
}

export interface SessionIdentity {
  projectId: string;
  account: Account;
}

/** `TenantSession` (store/src/lib.rs:170). `anonymous` replaces Rust's empty-identity convention. */
export interface TenantSession {
  identity: SessionIdentity;
  route: TenantRoute;
  member: Member;
  /** Tenant revision covered by the admission read. */
  admissionRevision: number;
  delegation: Json | null;
  developerSession: Json | null;
  anonymous: boolean;
}

/** `control::ControlConnection`, the per-socket auth state. */
export interface Connection {
  projectId: string;
  identity: SessionIdentity | null;
  tenant: TenantSession | null;
  impersonationId: string;
  /** The credential revalidated every 30 s (empty: not revalidated). */
  authToken: string;
}

export const NO_CONNECTION: Connection = Object.freeze({ projectId: "", identity: null, tenant: null, impersonationId: "", authToken: "" });

/**
 * `gonvex_store::DatabaseError` classes that pick `auth.error.code`
 * (`lib.rs:4453-4462`). `message` is the error's Display text, sent verbatim.
 */
export type FailureKind = "InvalidSession" | "SessionProjectMismatch" | "MemberNotFound" | "NoTenantMembership" | "Other";
export interface Failure { ok: false; kind: FailureKind; message: string }
export type Outcome<T> = { ok: true; value: T } | Failure;

export function authErrorCode(kind: FailureKind): "credential_rejected" | "member_removed" | "transient" {
  if (kind === "InvalidSession" || kind === "SessionProjectMismatch") return "credential_rejected";
  if (kind === "MemberNotFound" || kind === "NoTenantMembership") return "member_removed";
  return "transient";
}

export interface Impersonation {
  session: TenantSession;
  grantId: string;
  actorAccountId: string;
  reconnectToken: string;
}

/** One socket's credential in a revalidation round. */
export interface RevalidationItem {
  connectionId: string;
  projectId: string;
  token: string;
  impersonationId: string;
  accountId: string;
  /** Present for tenant sessions: the round also re-admits the Member (C.4 item 5c). */
  tenantId?: string;
  memberId?: string;
}

/** What a port sees of a socket when it handles a frame. */
export interface SocketView {
  connectionId: string;
  connection: Connection;
  /** The tenant data session (null for control-only sockets). */
  tenant: TenantSession | null;
  negotiated: Negotiated;
  publicTransport: boolean;
  /** The gateway or object holding the socket, for replies outside a frame (delivery). */
  host: string;
  /** K14: the socket's presence before a presence event (rebuild of a tenant object that lost its memory). */
  presencePrevious?: ConnectionReport | null;
}

/** K17: a `gvx_svc_` credential's admission (`authenticate_service_principal`). */
export type ServiceAuthOutcome = { ok: true; grant: ServiceGrant } | { ok: false; error: string };

export interface ControlPort {
  /** `ControlPlane::project`: Ok when the project exists and is active. */
  project(projectId: string): Promise<Outcome<null>>;
  /** `load_session_identity`: a `gvx_session_` token for a control-only socket. */
  loadSessionIdentity(token: string, projectId: string | undefined): Promise<Outcome<SessionIdentity>>;
  /** `authenticate_session`: identity, tenant (requested, else first admitted), admission. */
  authenticateSession(token: string, projectId: string | undefined, tenant: string | undefined): Promise<Outcome<TenantSession>>;
  /** `authenticate_impersonation`: `gvx_imp_` / `gvx_dev_` grants. */
  authenticateImpersonation(token: string, projectId: string | undefined, tenant: string | undefined, connectionId: string): Promise<Outcome<Impersonation>>;
  /** `resolve_public_tenant`. */
  resolvePublicTenant(projectId: string, tenant: string): Promise<Outcome<TenantRoute>>;
  /** One batched revalidation round: `true` for each item still valid. */
  revalidate(items: RevalidationItem[]): Promise<boolean[]>;
  /** Every `scope:"control"` frame (and `query.unsubscribe` of a control subscription). */
  dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]>;
  /**
   * K17 (optional): admits a `gvx_svc_` credential for one project and tenant.
   * Without it every service credential is refused with Rust's text.
   */
  authenticateService?(token: string, projectId: string | undefined, tenant: string | undefined): Promise<ServiceAuthOutcome>;
  /** K17 (optional): `control.servicePrincipals.delegate` / `revokeDelegation` from a service socket. */
  serviceCall?(grant: ServiceGrant, path: string, args: Json): Promise<ServiceControlOutcome>;
  /** K06 `connectionState` (optional): live support sessions and the close-time last-seen write (K14). */
  connectionState?(connectionId: string, state: { projectId: string; tenantId: string; authenticated: boolean; supportSessionId?: string } | null): Promise<void>;
}

export interface ModuleInfo {
  artifactHash: string;
  /** `client-contract.json` version, or null when the module declares none. */
  clientContract: number | null;
}

export interface ModulePort {
  /** The active generation of a project, or null when it has none. */
  module(projectId: string): Promise<ModuleInfo | null>;
  /** The replica epoch for one tenant database (0008). */
  epoch(projectId: string, databaseUrl: string): Promise<string>;
}

export interface TenantPort {
  /** Tenant-scoped frames of a socket with a tenant session. Returns the frames to send, in order. */
  dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]>;
  /** K17 (optional): a service principal's allowlisted call, run as its system identity. */
  serviceCall?(grant: ServiceGrant, call: ServiceTenantCall): Promise<ServiceTenantOutcome>;
  /** K14: the socket closed or changed session; the tenant removes its presence with grace. */
  presenceLeave?(route: TenantRoute, connectionId: string, host: string, previous?: ConnectionReport | null): Promise<void>;
}

export interface Ports {
  control: ControlPort;
  modules: ModulePort;
  tenants: TenantPort;
}
