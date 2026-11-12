/**
 * Session and connection shapes shared by the control object and the object
 * that owns a socket (K05). They cross Durable Object RPC as plain JSON.
 * Field meanings follow gonvex_store (store/src/lib.rs:120-230).
 */

export interface Account {
  id: string;
  email: string;
  emailVerified: boolean;
  name: string;
  avatarUrl: string;
  provider: string;
}

export interface SessionIdentity {
  projectId: string;
  account: Account;
}

export interface Member {
  id: string;
  accountId: string;
  status: string;
  displayName: string;
  avatarUrl: string;
  role: string;
  permissions: unknown;
  membershipRevision: number;
}

export interface TenantRoute {
  projectId: string;
  tenantId: string;
  /** For a tenant object: `do://tenant/<tenantId>` (the object is `idFromName(tenantId)`). */
  databaseUrl: string;
}

export interface DelegationActor {
  kind: string;
  name: string;
  reference?: string;
}

export interface SessionDelegation {
  principal: string;
  actor: DelegationActor | null;
}

export interface TenantSession {
  identity: SessionIdentity;
  route: TenantRoute;
  member: Member;
  admissionRevision: number;
  delegation: SessionDelegation | null;
  developerSession: unknown | null;
}

/** `ControlConnection` (control.rs:84-91): what a socket presents to every Control call. */
export interface ControlConnection {
  connectionId: string;
  projectId: string;
  identity: SessionIdentity | null;
  tenant: TenantSession | null;
  impersonationId: string;
  authToken: string;
}

export const emptyConnection = (): ControlConnection => ({
  connectionId: "",
  projectId: "",
  identity: null,
  tenant: null,
  impersonationId: "",
  authToken: "",
});

export const isAnonymous = (session: TenantSession): boolean => session.identity.account.id === "";

export type ControlKind = "query" | "reducer" | "action";

/** A Control call's outcome across RPC (thrown errors lose their class over RPC). */
export type Outcome<T = unknown> =
  | { ok: true; value: T }
  | { ok: false; error: string; errorClass: "rejected" | "transient" | "update_required" | "unauthenticated" };

/** Rows read and written by one Control call, for the PR's numbers and the tests. */
export interface Cost {
  rowsRead: number;
  rowsWritten: number;
  tenantCalls: number;
}

/** A configured service principal (`GONVEX_SERVICE_PRINCIPALS`, config.rs:795-893). */
export interface ServicePrincipalConfig {
  id: string;
  tokenSha256: string;
  projects: string[];
  functions: string[];
  maxDelegationSeconds: number;
  manifest: boolean;
  /**
   * The invocation channel of the principal's own calls: `api` (Rust's, the
   * default) or `system` for a host component such as the Ingress Worker
   * (0045, K18 §3). Set only by configuration, never by a request.
   */
  channel: "api" | "system";
}

/** The authenticated state of one service socket (service_principal.rs `ServiceGrant`). */
export interface ServiceGrant {
  principalId: string;
  route: TenantRoute;
  functions: string[];
  maxDelegationSeconds: number;
  /** Present only for a `system` principal (0045); absent means `api`. */
  channel?: "system";
}

/** The deployed module facts the control object needs (from `gonvex_runtime_manifests`). */
export interface ProjectModule {
  artifactHash: string;
  invitationAcceptanceReducer: string;
  /** function path -> { kind, internal } */
  functions: Record<string, { kind: string; internal: boolean }>;
}
