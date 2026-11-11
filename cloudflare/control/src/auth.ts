/**
 * Socket authentication: the `auth` frame branches of lib.rs `authenticate`
 * (lib.rs:4498-4790) and service principals (service_principal.rs:241-357).
 *
 * The object that owns the socket (K05's gateway) parses the frame and calls
 * `ControlObject.authenticate`; the reply frame and the resulting
 * `ControlConnection` come back. Replica directives and the artifact hash are
 * included here so the owner can send `auth.result` unchanged.
 */
import type { Ctx } from "./context";
import {
  authenticateImpersonation,
  authenticateSession,
  loadSessionIdentity,
  project as loadProject,
  projectModule,
  resolvePublicTenant,
  resolveTenant,
} from "./directory";
import { asControlError } from "./errors";
import type { ControlConnection, ServiceGrant, ServicePrincipalConfig, TenantSession } from "./types";
import { emptyConnection } from "./types";
import { constantTimeEqual, sha256Hex } from "./util";

export interface AuthRequest {
  id: string;
  token?: string | null;
  project?: string | null;
  tenant?: string | null;
  controlOnly: boolean;
  public: boolean;
  connectionId: string;
}

export type AuthMessage =
  | { type: "auth.result"; id: string; result: Record<string, unknown> }
  | { type: "auth.error"; id: string; code: string; error: string };

export interface AuthOutcome {
  message: AuthMessage;
  connection: ControlConnection;
  /** Set for a `gvx_svc_` credential. */
  service?: ServiceGrant;
}

const SERVICE_PREFIX = "gvx_svc_";
const SERVICE_FAILURE = "service credential is invalid or not permitted for this project";

const authError = (id: string, code: string, error: string): AuthOutcome => ({
  message: { type: "auth.error", id, code, error },
  connection: emptyConnection(),
});

function hashJson(value: unknown): string {
  return sha256Hex(JSON.stringify(value));
}

/**
 * `ProjectModule::replica_directive` (modules.rs:507-567). The epoch hashes
 * the deployed artifact and the tenant's database; scope and visibility
 * scope add the account and its permissions. Values are opaque to clients;
 * equal inputs give equal outputs, which is what clients rely on.
 */
export function replicaDirective(projectId: string, artifactHash: string, tenantId: string, databaseUrl: string, accountId: string, permissions: unknown): Record<string, unknown> {
  const epoch = hashJson({ protocolVersion: 1, project: projectId, database: databaseUrl, moduleHash: artifactHash });
  const permissionsHash = hashJson(permissions);
  const account = accountId.trim() ? accountId : "anonymous";
  return {
    protocolVersion: 1,
    scope: hashJson({ protocolVersion: 1, projectId, tenantId, accountId: account, permissionsHash, epoch }),
    visibilityScope: hashJson({ protocolVersion: 1, kind: "replica-visibility", projectId, tenantId, accountId: account, permissionsHash }),
    epoch,
  };
}

function localIdentity(session: TenantSession, developer: boolean): Record<string, unknown> {
  const account = session.identity.account;
  return {
    auth: {
      account: { id: account.id, email: account.email, name: account.name, avatarUrl: account.avatarUrl },
      ...(developer ? { developerSession: session.developerSession } : {}),
    },
    tenant: { id: session.route.tenantId },
    member: {
      id: session.member.id,
      accountId: session.member.accountId,
      status: session.member.status,
      role: session.member.role,
      displayName: session.member.displayName,
      permissions: session.member.permissions,
    },
  };
}

/** `authenticate` (lib.rs:4498). */
export async function authenticate(ctx: Ctx, request: AuthRequest): Promise<AuthOutcome> {
  const { id, connectionId } = request;
  const requestedProject = request.project?.trim() || null;
  const token = request.token?.trim() || null;
  const tenant = request.tenant ?? null;
  const module = (projectId: string) => projectModule(ctx, projectId);
  try {
    if (token?.startsWith(SERVICE_PREFIX)) return authenticateServicePrincipal(ctx, id, token, request.project ?? null, tenant);
    if (request.public) {
      let route = null;
      if (!token && !request.controlOnly && requestedProject && tenant && tenant.trim()) {
        try {
          route = resolvePublicTenant(ctx, requestedProject, tenant.trim());
        } catch {
          route = null;
        }
      }
      if (!route) return authError(id, "transient", "public tenant session is unavailable");
      const artifactHash = module(route.projectId)?.artifactHash ?? "";
      return {
        message: {
          type: "auth.result",
          id,
          result: { projectId: route.projectId, tenantId: route.tenantId, accountId: "", public: true, artifactHash },
        },
        connection: {
          ...emptyConnection(),
          connectionId,
          projectId: route.projectId,
          tenant: {
            identity: { projectId: route.projectId, account: { id: "", email: "", emailVerified: false, name: "", avatarUrl: "", provider: "" } },
            route,
            member: { id: "", accountId: "", status: "", displayName: "", avatarUrl: "", role: "", permissions: {}, membershipRevision: 0 },
            admissionRevision: 0,
            delegation: null,
            developerSession: null,
          },
        },
      };
    }
    if (request.controlOnly && !token) {
      if (!requestedProject) return authError(id, "transient", "project is required for public Control Plane calls");
      loadProject(ctx, requestedProject);
      return {
        message: { type: "auth.result", id, result: { projectId: requestedProject, accountId: "", tenantId: "" } },
        connection: { ...emptyConnection(), connectionId, projectId: requestedProject },
      };
    }
    if (!token) return authError(id, "transient", "a Gonvex app session is required");
    if (token.startsWith("gvx_imp_") || token.startsWith("gvx_dev_")) {
      if (request.controlOnly && tenant === null) return authError(id, "transient", "developer grants require an active tenant");
      let impersonation;
      try {
        impersonation = await authenticateImpersonation(ctx, token, requestedProject, tenant, connectionId);
      } catch (error) {
        const failure = asControlError(error);
        return authError(id, failure.authCode, `impersonation grant is invalid, expired, revoked, or already used: ${failure.message}`);
      }
      const session = impersonation.tenant;
      const deployed = module(session.identity.projectId);
      return {
        message: {
          type: "auth.result",
          id,
          result: {
            projectId: session.identity.projectId,
            accountId: session.identity.account.id,
            tenantId: session.route.tenantId,
            impersonationId: impersonation.grantId,
            impersonatorId: impersonation.actorAccountId,
            developerSessionToken: impersonation.reconnectToken,
            localIdentity: localIdentity(session, true),
            artifactHash: deployed?.artifactHash ?? "",
            replica:
              deployed && !request.controlOnly
                ? replicaDirective(session.identity.projectId, deployed.artifactHash, session.route.tenantId, session.route.databaseUrl, session.identity.account.id, session.member.permissions)
                : null,
          },
        },
        connection: {
          connectionId,
          projectId: session.identity.projectId,
          identity: session.identity,
          tenant: session,
          impersonationId: impersonation.grantId,
          authToken: impersonation.reconnectToken,
        },
      };
    }
    if (request.controlOnly && tenant === null) {
      const identity = loadSessionIdentity(ctx, token, requestedProject);
      return {
        message: {
          type: "auth.result",
          id,
          result: {
            projectId: identity.projectId,
            accountId: identity.account.id,
            tenantId: "",
            artifactHash: module(identity.projectId)?.artifactHash ?? "",
          },
        },
        connection: { connectionId, projectId: identity.projectId, identity, tenant: null, impersonationId: "", authToken: token },
      };
    }
    const session = await authenticateSession(ctx, token, requestedProject, tenant);
    const deployed = module(session.identity.projectId);
    return {
      message: {
        type: "auth.result",
        id,
        result: {
          projectId: session.identity.projectId,
          accountId: session.identity.account.id,
          tenantId: session.route.tenantId,
          artifactHash: deployed?.artifactHash ?? "",
          replica:
            deployed && !request.controlOnly
              ? replicaDirective(session.identity.projectId, deployed.artifactHash, session.route.tenantId, session.route.databaseUrl, session.identity.account.id, session.member.permissions)
              : null,
          localIdentity: localIdentity(session, false),
        },
      },
      connection: { connectionId, projectId: session.identity.projectId, identity: session.identity, tenant: session, impersonationId: "", authToken: token },
    };
  } catch (error) {
    const failure = asControlError(error);
    return authError(id, failure.authCode, failure.message);
  }
}

/** `find_principal`: every digest compared in constant time. */
export function findPrincipal(principals: ServicePrincipalConfig[], token: string): ServicePrincipalConfig | null {
  const trimmed = token.trim();
  if (!trimmed.startsWith(SERVICE_PREFIX)) return null;
  const digest = sha256Hex(trimmed);
  let found: ServicePrincipalConfig | null = null;
  for (const principal of principals) if (constantTimeEqual(digest, principal.tokenSha256)) found = principal;
  return found;
}

/** `authenticate_service_principal` (service_principal.rs:241). */
function authenticateServicePrincipal(ctx: Ctx, id: string, token: string, project: string | null, tenant: string | null): AuthOutcome {
  const principal = findPrincipal(ctx.config.servicePrincipals, token);
  if (!principal) return authError(id, "transient", SERVICE_FAILURE);
  const projectId = (project ?? "").trim();
  const tenantId = (tenant ?? "").trim();
  if (!projectId || !tenantId || !principal.projects.includes(projectId)) return authError(id, "transient", SERVICE_FAILURE);
  let route;
  try {
    route = resolveTenant(ctx, projectId, tenantId);
  } catch {
    return authError(id, "transient", "tenant is not available");
  }
  const grant: ServiceGrant = { principalId: principal.id, route, functions: principal.functions, maxDelegationSeconds: principal.maxDelegationSeconds };
  return {
    message: {
      type: "auth.result",
      id,
      result: {
        projectId: route.projectId,
        tenantId: route.tenantId,
        accountId: "",
        servicePrincipal: principal.id,
        functions: principal.functions,
        maxDelegationSeconds: principal.maxDelegationSeconds,
      },
    },
    connection: { ...emptyConnection(), projectId: route.projectId },
    service: grant,
  };
}

/** Parses `GONVEX_SERVICE_PRINCIPALS` (config.rs:795-893). Invalid entries fail loudly. */
export function parseServicePrincipals(value: string | undefined): ServicePrincipalConfig[] {
  if (!value || !value.trim()) return [];
  const fail = (message: string): never => {
    throw new Error(`GONVEX_SERVICE_PRINCIPALS is invalid: ${message}`);
  };
  let entries: unknown;
  try {
    entries = JSON.parse(value);
  } catch (error) {
    return fail(String(error));
  }
  if (!Array.isArray(entries)) return fail("expected an array");
  const out: ServicePrincipalConfig[] = [];
  for (const raw of entries as Array<Record<string, unknown>>) {
    const known = ["id", "tokenSha256", "projects", "functions", "maxDelegationSeconds", "manifest"];
    for (const key of Object.keys(raw ?? {})) if (!known.includes(key)) fail(`unknown field \`${key}\``);
    const id = String(raw.id ?? "").trim();
    if (!id || id.length > 64 || !/^[A-Za-z0-9_-]+$/.test(id)) fail(`principal id ${JSON.stringify(id)} must be 1-64 of [A-Za-z0-9_-]`);
    if (out.some((p) => p.id === id)) fail(`principal id ${JSON.stringify(id)} is duplicated`);
    const tokenSha256 = String(raw.tokenSha256 ?? "").trim().toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(tokenSha256)) fail(`principal ${JSON.stringify(id)} tokenSha256 must be 64 hex characters`);
    if (out.some((p) => p.tokenSha256 === tokenSha256)) fail(`principal ${JSON.stringify(id)} reuses another principal's credential`);
    const projects = (Array.isArray(raw.projects) ? raw.projects : []).map((p) => String(p).trim()).filter(Boolean);
    if (!projects.length) fail(`principal ${JSON.stringify(id)} must list at least one project`);
    const functions = (Array.isArray(raw.functions) ? raw.functions : []).map((f) => String(f).trim()).filter(Boolean);
    if (functions.some((f) => f.startsWith("control.") || f.includes("*"))) fail(`principal ${JSON.stringify(id)} functions must be exact tenant module paths`);
    const seconds = raw.maxDelegationSeconds === undefined || raw.maxDelegationSeconds === null ? 900 : Number(raw.maxDelegationSeconds);
    if (!Number.isInteger(seconds) || seconds < 60 || seconds > 3600) fail(`principal ${JSON.stringify(id)} maxDelegationSeconds must be between 60 and 3600`);
    out.push({ id, tokenSha256, projects, functions, maxDelegationSeconds: seconds, manifest: raw.manifest === true });
  }
  return out;
}
