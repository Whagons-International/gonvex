/**
 * Tenant routing, admission and session lookup: `ControlPlane` in
 * rust/crates/store/src/lib.rs:197-1080, on the control object's SQLite and
 * tenant objects.
 */
import type { Ctx } from "./context";
import { FANOUT, mapLimit } from "./context";
import { ControlError, db, isKind } from "./errors";
import { NOW_MS, bool, json, num, str, type Row } from "./sql";
import type { Admission } from "./tenant-port";
import type { Account, Member, ProjectModule, SessionDelegation, SessionIdentity, TenantRoute, TenantSession } from "./types";
import { randomId, reconnectToken, sha256Hex } from "./util";

export const SERVICE_PRINCIPAL_ACTOR_PREFIX = "service:";

function required(value: string | undefined | null, error: () => ControlError): string {
  const trimmed = (value ?? "").trim();
  if (!trimmed) throw error();
  return trimmed;
}

/** Joins the Account's most recently updated identity as `identity` (store lib.rs:188-195). */
const LATEST_IDENTITY_JOIN = `LEFT JOIN account_identities identity ON identity.rowid = (
  SELECT rowid FROM account_identities
  WHERE account_id = account.id ORDER BY updated_at DESC, rowid DESC LIMIT 1)`;

export function accountFromRow(row: Row): Account {
  return {
    id: str(row.id),
    email: str(row.email),
    emailVerified: num(row.email_verified) !== 0,
    name: str(row.name),
    avatarUrl: str(row.avatar_url),
    provider: str(row.provider),
  };
}

/** Where a tenant object lives. The directory's database_url column holds it. */
export const tenantDatabaseUrl = (tenantId: string): string => `do://tenant/${tenantId}`;

/** `ControlPlane::project`. */
export function project(ctx: Ctx, projectId: string): { id: string; name: string; environment: string; databaseMode: string; status: string; authMode: string } {
  const id = required(projectId, db.missingProject);
  const row = ctx.sql.get(
    `SELECT id, name, environment, database_mode, status, auth_mode
     FROM gonvex_runtime_projects
     WHERE id = $1 AND status NOT IN ('deleted', 'disabled')`,
    id,
  );
  if (!row) throw db.projectNotFound(id);
  return {
    id: str(row.id),
    name: str(row.name),
    environment: str(row.environment),
    databaseMode: str(row.database_mode),
    status: str(row.status),
    authMode: str(row.auth_mode),
  };
}

/** `ControlPlane::resolve_tenant`: id beats domain; disabled and deleted tenants fail. */
export function resolveTenant(ctx: Ctx, projectId: string, tenantId: string): TenantRoute {
  const projectKey = required(projectId, db.missingProject);
  const tenantKey = required(tenantId, db.missingTenant);
  const row = ctx.sql.get(
    `SELECT tenant_id,NULLIF(database_url, '') AS database_url
     FROM gonvex_runtime_tenants
     WHERE project_id = $1
       AND (tenant_id = $2 OR lower(domain) = lower($2))
       AND deleted_at IS NULL
       AND status NOT IN ('deleted', 'disabled')
     ORDER BY (tenant_id = $2) DESC LIMIT 1`,
    projectKey,
    tenantKey,
  );
  if (!row) throw db.tenantDatabaseMissing(tenantKey);
  const canonical = str(row.tenant_id);
  if (row.database_url === null || row.database_url === undefined) throw db.tenantDatabaseMissing(canonical);
  return { projectId: projectKey, tenantId: canonical, databaseUrl: str(row.database_url) };
}

/** `ControlPlane::resolve_public_tenant`: needs an active directory row. */
export function resolvePublicTenant(ctx: Ctx, projectId: string, tenant: string): TenantRoute {
  project(ctx, projectId);
  const row = ctx.sql.get(
    `SELECT tenant_id FROM gonvex_runtime_tenants WHERE project_id=$1 AND (tenant_id=$2 OR lower(domain)=lower($2))
     AND status='active' AND deleted_at IS NULL ORDER BY (tenant_id=$2) DESC LIMIT 1`,
    projectId,
    tenant,
  );
  if (!row) throw db.tenantDatabaseMissing(tenant);
  return resolveTenant(ctx, projectId, str(row.tenant_id));
}

async function tenantCall<T>(ctx: Ctx, route: TenantRoute, call: (port: ReturnType<Ctx["tenant"]>) => Promise<T>): Promise<T> {
  ctx.tenantCalls++;
  try {
    return await call(ctx.tenant(route));
  } catch (error) {
    if (error instanceof ControlError) throw error;
    throw db.unavailable(error instanceof Error ? error.message : String(error));
  }
}

/** `ControlPlane::admit_member`: the only admission gate (C.4 item 4). */
export async function admitMember(ctx: Ctx, projectId: string, tenantId: string, accountId: string): Promise<{ route: TenantRoute; member: Member; revision: number }> {
  const account = required(accountId, db.missingAccount);
  const route = resolveTenant(ctx, projectId, tenantId);
  const admission: Admission | null = await tenantCall(ctx, route, (port) => port.admit(account));
  if (!admission) throw db.memberNotFound(account);
  return { route, member: admission.member, revision: admission.revision };
}

/** `ControlPlane::admit_developer` (store lib.rs:846-889). */
export async function admitDeveloper(ctx: Ctx, projectId: string, tenantId: string, accountId: string): Promise<{ route: TenantRoute; member: Member; revision: number }> {
  const row = ctx.sql.get(
    `SELECT account.name,account.avatar_url FROM accounts account
     WHERE account.id=$2 AND account.auth_realm_id=$1 AND account.disabled_at IS NULL
       AND (EXISTS(SELECT 1 FROM gonvex_runtime_projects project
                  WHERE project.id=$1 AND project.owner_email<>''
                    AND lower(project.owner_email)=lower(account.email))
         OR EXISTS(SELECT 1 FROM gonvex_project_members member
                   WHERE member.project_id=$1 AND lower(member.email)=lower(account.email)
                     AND member.role IN ('owner','admin','dev')))`,
    projectId,
    accountId,
  );
  if (!row) throw db.invalidSession();
  const route = resolvePublicTenant(ctx, projectId, tenantId);
  try {
    return await admitMember(ctx, projectId, route.tenantId, accountId);
  } catch (error) {
    if (!isKind(error, "MemberNotFound")) throw error;
    return {
      route,
      member: {
        id: `developer:${accountId}`,
        accountId,
        status: "active",
        displayName: str(row.name),
        avatarUrl: str(row.avatar_url),
        role: "admin",
        permissions: { developer: true },
        membershipRevision: 0,
      },
      revision: 0,
    };
  }
}

/** `ControlPlane::load_session_identity`. `last_seen_at` is not written (C.3.8 item 10: nothing reads it). */
export function loadSessionIdentity(ctx: Ctx, token: string, requestedProject: string | null): SessionIdentity {
  const trimmed = token.trim();
  if (!trimmed.startsWith("gvx_session_")) throw db.invalidSession();
  const row = ctx.sql.get(
    `SELECT session.project_id, account.id, account.email,
            COALESCE(identity.verified_email, 0) AS email_verified,
            account.name, account.avatar_url,
            COALESCE(identity.provider, '') AS provider
     FROM gonvex_auth_sessions session
     JOIN accounts account
       ON account.id = session.account_id
      AND account.auth_realm_id = session.project_id
     ${LATEST_IDENTITY_JOIN}
     WHERE session.token_hash = $1
       AND session.revoked_at IS NULL
       AND session.expires_at > ${NOW_MS}
       AND account.disabled_at IS NULL`,
    sha256Hex(trimmed),
  );
  if (!row) throw db.invalidSession();
  const identity = { projectId: str(row.project_id), account: accountFromRow(row) };
  const requested = (requestedProject ?? "").trim();
  if (requested && requested !== identity.projectId) throw db.sessionProjectMismatch();
  return identity;
}

/**
 * `ControlPlane::first_admitted_tenant`, adapted by 0012 R15: candidates come
 * from the membership projection (`account_tenant_index`), directory order
 * kept, and are re-admitted in batches of at most 32 concurrent tenant RPCs.
 */
export async function firstAdmittedTenant(ctx: Ctx, projectId: string, accountId: string): Promise<string> {
  const rows = ctx.sql.all(
    `SELECT tenant.tenant_id
     FROM gonvex_runtime_tenants tenant
     JOIN account_tenant_index directory
       ON directory.tenant_id = tenant.tenant_id
      AND directory.account_id = $2
      AND directory.status = 'active'
     WHERE tenant.project_id = $1
       AND tenant.deleted_at IS NULL
       AND tenant.status NOT IN ('deleted', 'disabled')
     ORDER BY lower(tenant.name), tenant.tenant_id`,
    projectId,
    accountId,
  );
  const ids = rows.map((row) => str(row.tenant_id));
  for (let start = 0; start < ids.length; start += FANOUT) {
    const batch = ids.slice(start, start + FANOUT);
    const results = await mapLimit(batch, FANOUT, async (tenantId) => {
      try {
        await admitMember(ctx, projectId, tenantId, accountId);
        return { ok: true as const };
      } catch (error) {
        return { ok: false as const, error };
      }
    });
    for (const result of results) {
      if (result.ok) return batch[results.indexOf(result)]!;
      if (!isKind(result.error, "MemberNotFound")) throw result.error;
    }
  }
  throw db.noTenantMembership();
}

/** `ControlPlane::authenticate_session`. */
export async function authenticateSession(ctx: Ctx, token: string, requestedProject: string | null, requestedTenant: string | null): Promise<TenantSession> {
  const identity = loadSessionIdentity(ctx, token, requestedProject);
  const tenant = (requestedTenant ?? "").trim() || (await firstAdmittedTenant(ctx, identity.projectId, identity.account.id));
  const { route, member, revision } = await admitMember(ctx, identity.projectId, tenant, identity.account.id);
  return { identity, route, member, admissionRevision: revision, delegation: null, developerSession: null };
}

/** `developer_session_metadata` (store lib.rs:175-186). */
function developerSessionMetadata(grantId: string, reason: string, actor: string, account: string, tenant: string, expiresAt: number): unknown | null {
  return grantId.startsWith("devmode_") && reason === "developer mode" && actor !== "" && actor === account && tenant !== "" && expiresAt > Date.now()
    ? { grantId, accountId: account, tenantId: tenant, expiresAt }
    : null;
}

/** `session_delegation` (store lib.rs:1957-1978). */
function sessionDelegation(actorAccountId: string, actor: unknown): SessionDelegation | null {
  if (!actorAccountId.startsWith(SERVICE_PRINCIPAL_ACTOR_PREFIX)) return null;
  const principal = actorAccountId.slice(SERVICE_PRINCIPAL_ACTOR_PREFIX.length);
  if (!principal) throw db.invalidSession();
  if (actor === null || actor === undefined) return { principal, actor: null };
  if (typeof actor !== "object" || Array.isArray(actor)) throw db.invalidSession();
  const value = actor as Record<string, unknown>;
  if (typeof value.kind !== "string" || typeof value.name !== "string") throw db.invalidSession();
  if (value.reference !== undefined && value.reference !== null && typeof value.reference !== "string") throw db.invalidSession();
  return {
    principal,
    actor: { kind: value.kind, name: value.name, ...(typeof value.reference === "string" ? { reference: value.reference } : {}) },
  };
}

export interface ImpersonationSession {
  tenant: TenantSession;
  grantId: string;
  actorAccountId: string;
  reconnectToken: string;
}

/**
 * `ControlPlane::authenticate_impersonation` (store lib.rs:543-660): consumes
 * a `gvx_imp_` once or rotates a `gvx_dev_`, binds the grant to the
 * connection, then admits. A project or tenant mismatch rolls the
 * consumption back.
 */
export async function authenticateImpersonation(
  ctx: Ctx,
  token: string,
  requestedProject: string | null,
  requestedTenant: string | null,
  connectionId: string,
): Promise<ImpersonationSession> {
  const trimmed = token.trim();
  if (!(trimmed.startsWith("gvx_imp_") || trimmed.startsWith("gvx_dev_"))) throw db.invalidSession();
  const next = reconnectToken();
  const consumed = ctx.sql.tx(() => {
    const row = trimmed.startsWith("gvx_imp_")
      ? ctx.sql.get(
          `UPDATE gonvex_impersonation_grants SET used_at=${NOW_MS},used_connection_id=$2,reconnect_token_hash=$3
           WHERE token_hash=$1 AND used_at IS NULL AND revoked_at IS NULL AND expires_at>${NOW_MS}
           RETURNING id,project_id,actor_account_id,target_account_id,tenant_id,delegation_actor,reason,expires_at`,
          sha256Hex(trimmed),
          connectionId,
          sha256Hex(next),
        )
      : ctx.sql.get(
          `UPDATE gonvex_impersonation_grants SET used_connection_id=$2,reconnect_token_hash=$3
           WHERE reconnect_token_hash=$1 AND used_at IS NOT NULL AND revoked_at IS NULL AND expires_at>${NOW_MS}
           RETURNING id,project_id,actor_account_id,target_account_id,tenant_id,delegation_actor,reason,expires_at`,
          sha256Hex(trimmed),
          connectionId,
          sha256Hex(next),
        );
    if (!row) throw db.invalidSession();
    const grantId = str(row.id);
    const projectId = str(row.project_id);
    const actorAccountId = str(row.actor_account_id);
    const accountId = str(row.target_account_id);
    const tenantId = str(row.tenant_id);
    const developerSession = developerSessionMetadata(grantId, str(row.reason), actorAccountId, accountId, tenantId, num(row.expires_at));
    const delegation = sessionDelegation(actorAccountId, json(row.delegation_actor));
    const project = (requestedProject ?? "").trim();
    const tenant = (requestedTenant ?? "").trim();
    if ((project && project !== projectId) || (tenant && tenant !== tenantId)) throw db.invalidSession();
    const accountRow = ctx.sql.get(
      `SELECT account.id,account.email,account.name,account.avatar_url,
              COALESCE(identity.verified_email,0) AS email_verified,
              COALESCE(identity.provider,'') AS provider
       FROM accounts account
       ${LATEST_IDENTITY_JOIN}
       WHERE account.id=$1 AND account.auth_realm_id=$2 AND account.disabled_at IS NULL`,
      accountId,
      projectId,
    );
    if (!accountRow) throw db.invalidSession();
    return { grantId, projectId, actorAccountId, accountId, tenantId, developerSession, delegation, account: accountFromRow(accountRow) };
  });
  const admitted = consumed.developerSession
    ? await admitDeveloper(ctx, consumed.projectId, consumed.tenantId, consumed.accountId)
    : await admitMember(ctx, consumed.projectId, consumed.tenantId, consumed.accountId);
  return {
    tenant: {
      identity: { projectId: consumed.projectId, account: consumed.account },
      route: admitted.route,
      member: admitted.member,
      admissionRevision: admitted.revision,
      delegation: consumed.delegation,
      developerSession: consumed.developerSession,
    },
    grantId: consumed.grantId,
    actorAccountId: consumed.actorAccountId,
    reconnectToken: next,
  };
}

/** `ControlPlane::validate_impersonation_session`: revalidates without rotating. */
export async function validateImpersonationSession(ctx: Ctx, token: string, projectId: string, tenantId: string, connectionId: string): Promise<void> {
  if (!token.trim().startsWith("gvx_dev_")) throw db.invalidSession();
  const row = ctx.sql.get(
    `SELECT id,project_id,actor_account_id,target_account_id,tenant_id,reason,expires_at
     FROM gonvex_impersonation_grants
     WHERE reconnect_token_hash=$1 AND used_at IS NOT NULL
       AND used_connection_id=$2 AND revoked_at IS NULL AND expires_at>${NOW_MS}`,
    sha256Hex(token),
    connectionId,
  );
  if (!row) throw db.invalidSession();
  const account = str(row.target_account_id);
  const tenant = str(row.tenant_id);
  if (str(row.project_id) !== projectId || tenant !== tenantId) throw db.invalidSession();
  const developer = developerSessionMetadata(str(row.id), str(row.reason), str(row.actor_account_id), account, tenant, num(row.expires_at)) !== null;
  if (developer) await admitDeveloper(ctx, projectId, tenant, account);
  else await admitMember(ctx, projectId, tenant, account);
}

/** `ControlPlane::tenant_session_for_account`. */
export async function tenantSessionForAccount(ctx: Ctx, projectId: string, tenantId: string, accountId: string): Promise<TenantSession> {
  const row = ctx.sql.get(
    `SELECT account.id, account.email,
            COALESCE(identity.verified_email, 0) AS email_verified,
            account.name, account.avatar_url,
            COALESCE(identity.provider, '') AS provider
     FROM accounts account
     ${LATEST_IDENTITY_JOIN}
     WHERE account.id = $1 AND account.auth_realm_id = $2 AND account.disabled_at IS NULL`,
    accountId,
    projectId,
  );
  if (!row) throw db.invalidSession();
  const account = accountFromRow(row);
  const admitted = await admitMember(ctx, projectId, tenantId, account.id);
  return {
    identity: { projectId, account },
    route: admitted.route,
    member: admitted.member,
    admissionRevision: admitted.revision,
    delegation: null,
    developerSession: null,
  };
}

/** `ControlPlane::valid_session_hashes` (revalidation round, C.3.8 item 10). */
export function validSessionHashes(ctx: Ctx, projectId: string, tokenHashes: string[]): string[] {
  const valid: string[] = [];
  // 500 per statement in the reference; one json_each binding here.
  for (let start = 0; start < tokenHashes.length; start += 500) {
    const rows = ctx.sql.all(
      `SELECT session.token_hash
       FROM gonvex_auth_sessions session
       JOIN accounts account
         ON account.id = session.account_id
        AND account.auth_realm_id = session.project_id
       WHERE session.token_hash IN (SELECT value FROM json_each($1))
         AND ($2 = '' OR session.project_id = $2)
         AND session.revoked_at IS NULL
         AND session.expires_at > ${NOW_MS}
         AND account.disabled_at IS NULL`,
      JSON.stringify(tokenHashes.slice(start, start + 500)),
      projectId.trim(),
    );
    for (const row of rows) valid.push(str(row.token_hash));
  }
  return valid;
}

/** `ControlPlane::active_member_accounts`. */
export async function activeMemberAccounts(ctx: Ctx, projectId: string, tenantId: string, accountIds: string[]): Promise<string[]> {
  const route = resolveTenant(ctx, projectId, tenantId);
  if (accountIds.length === 0) return [];
  return tenantCall(ctx, route, (port) => port.activeMemberAccounts(accountIds));
}

/** `ControlPlane::member_by_id`. */
export async function memberById(ctx: Ctx, route: TenantRoute, memberId: string): Promise<Member | null> {
  return tenantCall(ctx, route, (port) => port.memberById(memberId));
}

export { tenantCall };

/** The deployed module facts, from `gonvex_runtime_manifests` (written by `/dev/sync`, K19). */
export function projectModule(ctx: Ctx, projectId: string): ProjectModule | null {
  const row = ctx.sql.get("SELECT manifest,module_hash FROM gonvex_runtime_manifests WHERE project_id=$1 AND module_hash<>''", projectId);
  if (!row) return null;
  const manifest = (json(row.manifest) ?? {}) as Record<string, unknown>;
  const module = (manifest.module ?? {}) as Record<string, unknown>;
  const functions: ProjectModule["functions"] = {};
  const list = manifest.functions;
  if (Array.isArray(list)) {
    for (const entry of list as Array<Record<string, unknown>>) {
      if (typeof entry?.path === "string") functions[entry.path] = { kind: String(entry.kind ?? ""), internal: entry.internal === true };
    }
  } else if (list && typeof list === "object") {
    for (const [path, entry] of Object.entries(list as Record<string, Record<string, unknown>>)) {
      functions[path] = { kind: String(entry?.kind ?? ""), internal: entry?.internal === true };
    }
  }
  return {
    artifactHash: str(row.module_hash),
    invitationAcceptanceReducer: typeof module.invitationAcceptanceReducer === "string" ? module.invitationAcceptanceReducer.trim() : "",
    functions,
  };
}

export { randomId, bool };
