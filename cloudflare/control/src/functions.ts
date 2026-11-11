/**
 * Control Plane Query, Reducer and Action dispatch
 * (rust/crates/runtime/src/control.rs; contract C.0-C.1).
 *
 * Each reference transaction is one synchronous `sql.tx` here; each reference
 * tenant transaction is one tenant-object RPC (`TenantPort`). Sagas keep the
 * reference's order: Control reservation, tenant write, Control finish.
 */
import type { Ctx } from "./context";
import { FANOUT, mapLimit } from "./context";
import {
  type Args,
  type Authorization,
  byteCompare,
  emptyArgs,
  exactObject,
  invitationProviders,
  isLiveControlQuery,
  membershipRole,
  normalizeEmail,
  optionalString,
  requireAuthMode,
  requireDefinition,
  requiredString,
  uniqueStringArray,
} from "./definitions";
import {
  admitMember,
  projectModule,
  resolvePublicTenant,
  resolveTenant,
  tenantCall,
  tenantDatabaseUrl,
  tenantSessionForAccount,
} from "./directory";
import {
  ControlError,
  administrationDuringImpersonation,
  authenticationRequired,
  databaseInvariant,
  debugStr,
  developerRequired,
  invalid,
  notImplemented,
  projectAdminRequired,
  tenantAdminRequired,
  unknownFunction,
} from "./errors";
import { type ExternalAuthConfiguration, verifyExternalToken } from "./external-auth";
import { hashPassword, verifyPassword } from "./password";
import { decryptControlSecret, encryptControlSecret } from "./secrets";
import {
  type AuthAccount,
  type SessionGrant,
  type VerifiedExternalIdentity,
  REFRESH_TOKEN_MS,
  issueSession,
  newFamily,
  refreshSession,
  resolveExternalAccount,
  sessionResult,
  sessionResultFromDirectory,
} from "./sessions";
import { NOW_MS, blob, bool, json, num, optNum, str, type Row } from "./sql";
import type { ControlConnection, ControlKind, SessionIdentity, TenantSession } from "./types";
import { isAnonymous } from "./types";
import { byteLength, randomId, rfc3339, secureToken, sha256Hex, slug, uuidSimple } from "./util";

const ROW_NOT_FOUND = "no rows returned by a query that expected to return at least one row";

/** Live connection facts the socket owners report (Rust's process-local presence and metrics). */
export interface LiveConnections {
  tenantCounts(projectId: string): Map<string, number>;
  supportLiveIds(projectId: string): Set<string>;
}

// ---------------------------------------------------------------- helpers

function account(connection: ControlConnection): SessionIdentity {
  if (!connection.identity) throw authenticationRequired();
  return connection.identity;
}

function tenant(connection: ControlConnection): TenantSession {
  if (!connection.tenant || isAnonymous(connection.tenant)) throw tenantAdminRequired();
  return connection.tenant;
}

function authMode(ctx: Ctx, projectId: string): string {
  const mode = ctx.sql.value<string>("SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1", projectId);
  if (mode === undefined) throw new ControlError(ROW_NOT_FOUND, "rejected");
  return mode;
}

const nativePasswords = (mode: string): boolean => mode === "gonvex-native" || mode === "hybrid";

function affectedResult(affected: number): { updated: true } {
  if (affected === 0) throw invalid("record not found");
  return { updated: true };
}

/** `control_subject` (control.rs:4181). */
function controlSubject(connection: ControlConnection, authorization: Authorization): string {
  let subject = connection.identity?.account.id ?? "public";
  if (authorization === "tenantAdmin") subject += `|tenant:${connection.tenant?.route.tenantId ?? ""}`;
  return subject;
}

/** `auth_subject` (control.rs:4097). */
function authSubject(path: string, args: unknown): string {
  if (!args || typeof args !== "object" || Array.isArray(args)) throw invalid("expected an object");
  const object = args as Args;
  let material: string;
  if (path === "control.auth.passwordLogin") material = normalizeEmail(requiredString(object, "email"));
  else if (path === "control.auth.exchangeExternalToken") material = `${requiredString(object, "provider")}\0${requiredString(object, "token")}`;
  else if (path === "control.auth.refreshSession") material = requiredString(object, "refreshToken");
  else material = "public";
  return `auth:${sha256Hex(material)}`;
}

// ------------------------------------------------------------ idempotency

/** `claim_control_idempotency` (control.rs:3451). Returns the stored result on replay. */
function claimIdempotency(ctx: Ctx, project: string, subject: string, key: string, kind: ControlKind, path: string): { replay: unknown } | null {
  const inserted = ctx.sql.affected(
    `INSERT INTO gonvex_control_idempotency(project_id,subject_id,idempotency_key,kind,path)
     VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
    project,
    subject,
    key,
    kind,
    path,
  );
  if (inserted === 1) return null;
  return storedOutcome(ctx, project, subject, key, kind, path, false);
}

/** `claim_control_saga` (control.rs:3517): a pending row means "resume". */
function claimSaga(ctx: Ctx, project: string, subject: string, key: string, kind: ControlKind, path: string): { replay: unknown } | null {
  ctx.sql.run(
    `INSERT INTO gonvex_control_idempotency(project_id,subject_id,idempotency_key,kind,path)
     VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
    project,
    subject,
    key,
    kind,
    path,
  );
  return storedOutcome(ctx, project, subject, key, kind, path, true);
}

function storedOutcome(ctx: Ctx, project: string, subject: string, key: string, kind: ControlKind, path: string, saga: boolean): { replay: unknown } | null {
  const row = ctx.sql.get(
    "SELECT state,kind,path,result,error FROM gonvex_control_idempotency WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3",
    project,
    subject,
    key,
  );
  if (!row) {
    throw databaseInvariant(
      saga ? "Control Plane saga idempotency claim" : "Control Plane idempotency claim",
      `record for ${debugStr(path)} disappeared after ${saga ? "insert" : "a conflicting insert"}`,
    );
  }
  if (str(row.kind) !== kind || str(row.path) !== path) throw invalid("idempotency key was already used for another operation");
  if (str(row.state) === "pending") {
    if (saga) return null;
    throw invalid("Control Plane operation is still in progress");
  }
  const error = str(row.error);
  if (error) throw invalid(error);
  return { replay: row.result === null || row.result === undefined ? null : json(row.result) };
}

function completeIdempotency(ctx: Ctx, project: string, subject: string, key: string, result: unknown): void {
  ctx.sql.run(
    `UPDATE gonvex_control_idempotency SET state='completed',result=$4,error='',updated_at=${NOW_MS}
     WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3`,
    project,
    subject,
    key,
    JSON.stringify(result),
  );
}

/** Kernel: idempotency rows expire after 24 h (0012 R9). Called from the alarm, in pages. */
export const IDEMPOTENCY_TTL_MS = 24 * 60 * 60 * 1000;
export function pruneIdempotency(ctx: Ctx, limit = 500): number {
  return ctx.sql.affected(
    `DELETE FROM gonvex_control_idempotency WHERE rowid IN (
       SELECT rowid FROM gonvex_control_idempotency WHERE created_at < ${NOW_MS} - ${IDEMPOTENCY_TTL_MS} LIMIT ${limit})`,
  );
}

/** Kernel: a claim older than 24 h no longer replays (expired, not yet pruned). */
function dropExpiredClaim(ctx: Ctx, project: string, subject: string, key: string): void {
  ctx.sql.run(
    `DELETE FROM gonvex_control_idempotency WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3
     AND created_at < ${NOW_MS} - ${IDEMPOTENCY_TTL_MS}`,
    project,
    subject,
    key,
  );
}

// ------------------------------------------------------------ authorization

/** `authorize_control` (control.rs:2013). */
async function authorize(ctx: Ctx, connection: ControlConnection, authorization: Authorization): Promise<void> {
  if (authorization === "public") return;
  const identity = connection.identity;
  if (!identity) throw authenticationRequired();
  if (authorization === "account") return;
  if (authorization === "tenantAdmin") {
    if (!connection.tenant) throw tenantAdminRequired();
    let role: string;
    try {
      role = (await admitMember(ctx, identity.projectId, connection.tenant.route.tenantId, identity.account.id)).member.role;
    } catch {
      throw tenantAdminRequired();
    }
    if (role !== "owner" && role !== "admin") throw tenantAdminRequired();
    return;
  }
  if (connection.impersonationId) throw administrationDuringImpersonation();
  const roles = authorization === "projectAdmin" ? ["owner", "admin"] : ["owner", "admin", "dev"];
  const allowed = num(
    ctx.sql.value(
      `SELECT EXISTS(
         SELECT 1 FROM gonvex_runtime_projects
         WHERE id=$1 AND lower(owner_email)=lower($2) AND owner_email<>''
         UNION ALL
         SELECT 1 FROM gonvex_project_members
         WHERE project_id=$1 AND lower(email)=lower($2) AND role IN (SELECT value FROM json_each($3))
       )`,
      connection.projectId,
      identity.account.email,
      JSON.stringify(roles),
    ),
  );
  if (!allowed) throw authorization === "projectAdmin" ? projectAdminRequired() : developerRequired();
}

// ------------------------------------------------------------------ queries

/** `execute_control_query` (control.rs:138). */
export async function executeControlQuery(ctx: Ctx, live: LiveConnections, connection: ControlConnection, path: string, args: unknown): Promise<unknown> {
  const definition = requireDefinition(path, "query");
  await authorize(ctx, connection, definition.authorization);
  if (path === "control.tenants.mine" || path === "users.myTenants") {
    emptyArgs(args);
    return listAccountTenants(ctx, connection, path === "users.myTenants");
  }
  if (path === "control.auth.memberProviders") return memberAuthProviders(ctx, connection, args);
  if (path === "control.support.listTenants") {
    emptyArgs(args);
    return listSupportTenants(ctx, live, connection);
  }
  return controlQuery(ctx, live, connection, path, args);
}

/** `open_control_query` checks (control.rs:1947). */
export function checkSubscribable(connection: ControlConnection, path: string): void {
  if (connection.tenant && isAnonymous(connection.tenant)) throw authenticationRequired();
  if (!isLiveControlQuery(path)) throw invalid(`Control Plane query ${debugStr(path)} is not subscribable`);
}

function controlQuery(ctx: Ctx, live: LiveConnections, connection: ControlConnection, path: string, args: unknown): unknown {
  const project = connection.projectId;
  switch (path) {
    case "control.accounts.me": {
      emptyArgs(args);
      const { account: a } = account(connection);
      return { id: a.id, email: a.email, name: a.name, avatarUrl: a.avatarUrl };
    }
    case "control.auth.publicSettings": {
      emptyArgs(args);
      const mode = authMode(ctx, project);
      requireAuthMode(mode);
      const rows = ctx.sql.all("SELECT provider FROM gonvex_auth_providers WHERE project_id=$1 AND enabled=TRUE ORDER BY provider", project);
      const providers: string[] = nativePasswords(mode) ? ["password"] : [];
      for (const row of rows) {
        const provider = str(row.provider);
        const allowed =
          provider !== "password" &&
          (mode === "hybrid" || mode === provider || (mode === "gonvex-native" && provider !== "firebase" && provider !== "external-oidc"));
        if (allowed) providers.push(provider);
      }
      return { mode, providers };
    }
    case "control.auth.realms.list": {
      emptyArgs(args);
      const mode = authMode(ctx, project);
      const rows = ctx.sql.all(
        `SELECT provider, enabled, signup_mode, azure_tenant_id, client_id,
                client_secret_encrypted IS NOT NULL AS has_client_secret,
                issuer,audience,jwks_url,firebase_project_id,firebase_tenant_id,
                admin_credentials_encrypted IS NOT NULL AS has_admin_credentials
         FROM gonvex_auth_providers WHERE project_id=$1 ORDER BY provider`,
        project,
      );
      return [
        { provider: "password", enabled: nativePasswords(mode), signupMode: "inviteOnly", hasClientSecret: false, hasAdminCredentials: false, authMode: mode },
        ...rows.map((row) => ({
          provider: str(row.provider),
          enabled: bool(row.enabled),
          signupMode: str(row.signup_mode),
          azureTenantId: str(row.azure_tenant_id),
          clientId: str(row.client_id),
          hasClientSecret: bool(row.has_client_secret),
          issuer: str(row.issuer),
          audience: str(row.audience),
          jwksUrl: str(row.jwks_url),
          firebaseProjectId: str(row.firebase_project_id),
          firebaseTenantId: str(row.firebase_tenant_id),
          hasAdminCredentials: bool(row.has_admin_credentials),
          authMode: mode,
        })),
      ];
    }
    case "control.tenants.getByDomain": {
      const object = exactObject(args, ["domain"]);
      const domain = requiredString(object, "domain");
      const row = ctx.sql.get(
        `SELECT tenant_id,name,domain FROM gonvex_runtime_tenants
         WHERE project_id=$1 AND lower(domain)=lower($2) AND deleted_at IS NULL AND status <> 'deleted'`,
        project,
        domain,
      );
      if (!row) throw invalid("tenant not found");
      return { id: str(row.tenant_id), name: str(row.name), domain: str(row.domain) };
    }
    case "control.invitations.lookup":
    case "tenants.getInvitationByToken": {
      const object = exactObject(args, ["token", "invitationToken"]);
      const token = nonEmpty(optionalString(object, "token")) ?? nonEmpty(optionalString(object, "invitationToken"));
      if (!token) throw invalid("token is required");
      const row = ctx.sql.get(
        `SELECT invitation.tenant_id,tenant.name,invitation.role,invitation.email,
                invitation.team_ids,invitation.allowed_auth_providers,invitation.expires_at
         FROM gonvex_auth_membership_invitations invitation
         JOIN gonvex_runtime_tenants tenant
           ON tenant.project_id=invitation.project_id AND tenant.tenant_id=invitation.tenant_id
         WHERE invitation.project_id=$1 AND invitation.token_hash=$2
           AND invitation.revoked_at IS NULL AND invitation.accepted_at IS NULL
           AND invitation.expires_at>${NOW_MS}`,
        project,
        sha256Hex(token),
      );
      if (!row) throw invalid("invitation is invalid or expired");
      if (path === "tenants.getInvitationByToken") {
        return {
          tenantId: str(row.tenant_id),
          invitationToken: token,
          userEmail: str(row.email),
          teamIds: json(row.team_ids),
          allowedAuthProviders: json(row.allowed_auth_providers),
        };
      }
      return {
        tenantId: str(row.tenant_id),
        tenantName: str(row.name),
        email: str(row.email),
        role: str(row.role),
        teamIds: json(row.team_ids),
        allowedAuthProviders: json(row.allowed_auth_providers),
        expiresAt: rfc3339(num(row.expires_at)),
      };
    }
    case "control.invitations.list": {
      emptyArgs(args);
      const session = tenant(connection);
      return ctx.sql
        .all(
          `SELECT id,email,role,permissions,team_ids,allowed_auth_providers,
                  expires_at,revoked_at,accepted_at,created_at,updated_at,handoff_state
           FROM gonvex_auth_membership_invitations
           WHERE project_id=$1 AND tenant_id=$2 ORDER BY created_at DESC`,
          project,
          session.route.tenantId,
        )
        .map((row) => ({
          id: str(row.id),
          email: str(row.email),
          role: str(row.role),
          permissions: json(row.permissions),
          teamIds: json(row.team_ids),
          allowedAuthProviders: json(row.allowed_auth_providers),
          expiresAt: rfc3339(num(row.expires_at)),
          revoked: optNum(row.revoked_at) !== null,
          accepted: optNum(row.accepted_at) !== null,
          state: str(row.handoff_state),
          createdAt: rfc3339(num(row.created_at)),
          updatedAt: rfc3339(num(row.updated_at)),
        }));
    }
    case "control.project.developers.list": {
      emptyArgs(args);
      return ctx.sql
        .all("SELECT email,name,role FROM gonvex_project_members WHERE project_id=$1 ORDER BY lower(email)", project)
        .map((row) => ({ email: str(row.email), name: str(row.name), role: str(row.role) }));
    }
    case "control.developer.status": {
      emptyArgs(args);
      const identity = account(connection);
      const developer = num(
        ctx.sql.value(
          `SELECT EXISTS(
             SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND lower(owner_email)=lower($2)
             UNION ALL
             SELECT 1 FROM gonvex_project_members WHERE project_id=$1 AND lower(email)=lower($2)
           )`,
          project,
          identity.account.email,
        ),
      );
      return {
        developer: developer !== 0,
        mode: connection.impersonationId !== "",
        tenantId: connection.tenant?.route.tenantId ?? "",
        grantId: connection.impersonationId,
      };
    }
    case "control.assistant.getDefaults": {
      emptyArgs(args);
      const value = ctx.sql.value("SELECT value FROM gonvex_control_settings WHERE project_id=$1 AND kind=$2 AND scope_id=$3", project, "assistant.defaults", "");
      return value === undefined ? {} : json(value);
    }
    case "control.voice.getConfiguration": {
      emptyArgs(args);
      return ctx.sql
        .all("SELECT kind,scope_id,value FROM gonvex_control_settings WHERE project_id=$1 AND kind LIKE $2 ORDER BY kind,scope_id", project, "voice.%")
        .map((row) => ({ kind: str(row.kind), scopeId: str(row.scope_id), value: json(row.value) }));
    }
    case "control.support.listSessions": {
      emptyArgs(args);
      const rows = ctx.sql.all(
        `SELECT id,tenant_id,account_id,release,environment,last_seen_at
         FROM gonvex_support_sessions WHERE project_id=$1 ORDER BY last_seen_at DESC LIMIT 1000`,
        project,
      );
      const liveIds = live.supportLiveIds(project);
      const now = Date.now();
      return rows
        .map((row) => {
          const seen = liveIds.has(str(row.id)) ? now : num(row.last_seen_at);
          return {
            seen,
            value: {
              id: str(row.id),
              tenantId: str(row.tenant_id),
              accountId: str(row.account_id),
              release: str(row.release),
              environment: str(row.environment),
              lastSeenAt: rfc3339(seen),
            },
          };
        })
        .sort((a, b) => b.seen - a.seen)
        .map((entry) => entry.value);
    }
    case "control.support.getSession": {
      const object = exactObject(args, ["id"]);
      const id = requiredString(object, "id");
      const row = ctx.sql.get(
        `SELECT id,tenant_id,account_id,connection_id,release,environment,last_seen_at,created_at
         FROM gonvex_support_sessions WHERE project_id=$1 AND id=$2`,
        project,
        id,
      );
      if (!row) throw invalid("support session not found");
      const seen = live.supportLiveIds(project).has(id) ? Date.now() : num(row.last_seen_at);
      return {
        id: str(row.id),
        tenantId: str(row.tenant_id),
        accountId: str(row.account_id),
        connectionId: str(row.connection_id),
        release: str(row.release),
        environment: str(row.environment),
        lastSeenAt: rfc3339(seen),
        createdAt: rfc3339(num(row.created_at)),
      };
    }
    case "control.support.getTenant": {
      const object = exactObject(args, ["tenantId"]);
      const tenantId = requiredString(object, "tenantId");
      const row = ctx.sql.get(
        `SELECT tenant_id,name,domain,status,timezone,description,profile,seat_limit,created_at,updated_at
         FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`,
        project,
        tenantId,
      );
      if (!row) throw invalid("tenant not found");
      return {
        id: str(row.tenant_id),
        name: str(row.name),
        domain: str(row.domain),
        status: str(row.status),
        timezone: str(row.timezone),
        description: str(row.description),
        profile: json(row.profile),
        seatLimit: optNum(row.seat_limit),
        createdAt: rfc3339(num(row.created_at)),
        updatedAt: rfc3339(num(row.updated_at)),
      };
    }
    case "control.support.listErrors": {
      emptyArgs(args);
      const groups = ctx.sql.all(
        `SELECT fingerprint,project_id,title,culprit,level,status,priority,
                assignee,first_seen,last_seen,event_count,tenants,releases,
                environments,accounts,devices,latest_event,regression
         FROM gonvex_error_groups WHERE project_id=$1 ORDER BY last_seen DESC LIMIT 500`,
        project,
      );
      const releases = ctx.sql.all(
        `SELECT release FROM gonvex_error_events WHERE project_id=$1 AND release<>'' GROUP BY release
         ORDER BY max(occurred_at) DESC,release DESC`,
        project,
      );
      return { groups: groups.map(errorGroupJson), releases: releases.map((row) => str(row.release)) };
    }
    case "control.support.getError": {
      const object = exactObject(args, ["fingerprint"]);
      const fingerprint = requiredString(object, "fingerprint");
      const row = ctx.sql.get(
        `SELECT fingerprint,project_id,title,culprit,level,status,priority,
                assignee,first_seen,last_seen,event_count,tenants,releases,
                environments,accounts,devices,latest_event,regression
         FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2`,
        project,
        fingerprint,
      );
      if (!row) throw invalid("error group not found");
      return errorGroupJson(row);
    }
    default:
      throw unknownFunction(path);
  }
}

const nonEmpty = (value: string | undefined): string | undefined => (value ? value : undefined);

function errorGroupJson(row: Row): Record<string, unknown> {
  return {
    fingerprint: str(row.fingerprint),
    project: str(row.project_id),
    title: str(row.title),
    culprit: str(row.culprit),
    level: str(row.level),
    status: str(row.status),
    priority: str(row.priority),
    assignee: str(row.assignee),
    firstSeen: rfc3339(num(row.first_seen)),
    lastSeen: rfc3339(num(row.last_seen)),
    count: num(row.event_count),
    tenants: json(row.tenants),
    releases: json(row.releases),
    environments: json(row.environments),
    accounts: json(row.accounts),
    devices: json(row.devices),
    latest: json(row.latest_event),
    regression: bool(row.regression),
  };
}

/**
 * `list_account_tenants` (control.rs:3308), adapted by 0012 R15: candidates
 * come from `account_tenant_index`, re-admitted by batched tenant RPCs.
 */
async function listAccountTenants(ctx: Ctx, connection: ControlConnection, idsOnly: boolean): Promise<unknown[]> {
  const identity = account(connection);
  const developerTenant = connection.tenant?.developerSession ? connection.tenant : null;
  const rows = ctx.sql.all(
    `SELECT tenant.tenant_id,tenant.name,tenant.domain,tenant.timezone,tenant.description,tenant.profile
     FROM gonvex_runtime_tenants tenant
     WHERE tenant.project_id=$1 AND tenant.deleted_at IS NULL
       AND tenant.status NOT IN ('deleted','disabled')
       AND (EXISTS(SELECT 1 FROM account_tenant_index directory
                   WHERE directory.tenant_id=tenant.tenant_id AND directory.account_id=$2 AND directory.status='active')
            OR tenant.tenant_id=$3)
     ORDER BY lower(tenant.name),tenant.tenant_id`,
    connection.projectId,
    identity.account.id,
    developerTenant?.route.tenantId ?? "",
  );
  const members = await mapLimit(rows, FANOUT, async (row) => {
    const id = str(row.tenant_id);
    // A redeemed self developer grant exposes its selected tenant on this connection only.
    if (developerTenant && developerTenant.route.tenantId === id) return developerTenant.member;
    try {
      return (await admitMember(ctx, connection.projectId, id, identity.account.id)).member;
    } catch {
      return null;
    }
  });
  const items: unknown[] = [];
  rows.forEach((row, index) => {
    const member = members[index];
    if (!member) return;
    if (idsOnly) items.push(str(row.tenant_id));
    else {
      items.push({
        id: str(row.tenant_id),
        name: str(row.name),
        role: member.role,
        permissions: member.permissions,
        domain: str(row.domain),
        timezone: str(row.timezone),
        description: str(row.description),
        profile: json(row.profile),
      });
    }
  });
  return items;
}

/** `member_auth_providers` (control.rs:3370). */
async function memberAuthProviders(ctx: Ctx, connection: ControlConnection, args: unknown): Promise<unknown[]> {
  const object = exactObject(args, ["memberIds"]);
  const memberIds = uniqueStringArray(object.memberIds, "memberIds");
  if (memberIds.length > 1000) throw invalid("memberIds cannot contain more than 1000 entries");
  const caller = tenant(connection);
  const route = resolveTenant(ctx, connection.projectId, caller.route.tenantId);
  const rows = await tenantCall(ctx, route, (port) => port.memberAccounts(memberIds));
  const accountByMember = new Map(rows.map((row) => [row.id, row.accountId]));
  const identities = ctx.sql.all(
    `SELECT account_id,provider,sign_in_provider FROM account_identities
     WHERE project_id=$1 AND account_id IN (SELECT value FROM json_each($2))
     ORDER BY account_id,provider`,
    connection.projectId,
    JSON.stringify([...accountByMember.values()].sort(byteCompare)),
  );
  const providers = new Map<string, string[]>();
  for (const row of identities) {
    const signIn = str(row.sign_in_provider).trim();
    const list = providers.get(str(row.account_id)) ?? [];
    list.push(signIn || str(row.provider));
    providers.set(str(row.account_id), list);
  }
  for (const [key, list] of providers) providers.set(key, [...new Set(list.sort(byteCompare))]);
  return memberIds.flatMap((memberId) => {
    const accountId = accountByMember.get(memberId);
    return accountId === undefined ? [] : [{ memberId, providers: providers.get(accountId) ?? [] }];
  });
}

/** `list_support_tenants` (control.rs:2082). Member counts fan out to tenant objects (0012 R15). */
async function listSupportTenants(ctx: Ctx, live: LiveConnections, connection: ControlConnection): Promise<unknown[]> {
  const rows = ctx.sql.all(
    `SELECT tenant_id,name,domain,status,timezone,seat_limit,created_at
     FROM gonvex_runtime_tenants WHERE project_id=$1 AND deleted_at IS NULL ORDER BY name,tenant_id`,
    connection.projectId,
  );
  const counts = live.tenantCounts(connection.projectId);
  return mapLimit(rows, FANOUT, async (row) => {
    const tenantId = str(row.tenant_id);
    let userCount: number | null = null;
    try {
      const route = resolveTenant(ctx, connection.projectId, tenantId);
      userCount = await tenantCall(ctx, route, (port) => port.activeMemberCount());
    } catch {
      userCount = null;
    }
    return {
      id: tenantId,
      name: str(row.name),
      domain: str(row.domain),
      status: str(row.status),
      timezone: str(row.timezone),
      seatLimit: optNum(row.seat_limit),
      createdAt: rfc3339(num(row.created_at)),
      userCount,
      activeSessions: counts.get(tenantId) ?? 0,
    };
  });
}

// ------------------------------------------------------------------- writes

/** `execute_control_write` (control.rs:251). */
export async function executeControlWrite(
  ctx: Ctx,
  live: LiveConnections,
  connection: ControlConnection,
  path: string,
  args: unknown,
  idempotencyKey: string,
  kind: "reducer" | "action",
): Promise<unknown> {
  const definition = requireDefinition(path, kind);
  await authorize(ctx, connection, definition.authorization);
  const key = idempotencyKey.trim();
  if (!key) throw invalid("Control Plane write calls require an idempotency key");
  if (kind === "action" && ["control.auth.passwordLogin", "control.auth.exchangeExternalToken", "control.auth.refreshSession"].includes(path)) {
    return executeAuthAction(ctx, connection, path, args, key);
  }
  if (kind === "reducer") {
    switch (path) {
      case "control.invitations.accept":
      case "tenants.acceptInvitation":
        return acceptInvitation(ctx, connection, args, key);
      case "control.accounts.resetMemberPassword":
        return resetMemberPassword(ctx, connection, args, key);
      case "control.accounts.provisionMemberLogin":
        return provisionMemberLogin(ctx, connection, args, key);
      case "control.developer.provisionSelf":
      case "control.developer.removeSelf":
        return changeDeveloperMembership(ctx, connection, path, args, key);
      case "control.support.createImpersonation":
        return createSupportImpersonation(ctx, connection, args, key);
      case "control.tenants.create":
        return createTenant(ctx, connection, args, key);
      case "control.demos.create":
        return createDemoAccount(ctx, connection, args, key);
      case "control.demos.delete":
        return deleteDemoAccount(ctx, connection, args, key);
    }
  }
  if (path === "control.developer.enter") {
    const object = exactObject(args, ["tenantId"]);
    resolvePublicTenant(ctx, connection.projectId, requiredString(object, "tenantId"));
  }
  const subject = controlSubject(connection, definition.authorization);
  return ctx.sql.tx(() => {
    dropExpiredClaim(ctx, connection.projectId, subject, key);
    const claim = claimIdempotency(ctx, connection.projectId, subject, key, kind, path);
    if (claim) return claim.replay;
    const result = controlWriteBody(ctx, live, connection, path, args);
    completeIdempotency(ctx, connection.projectId, subject, key, result);
    return result;
  });
}

/** `control_write_body` (control.rs:2515). Runs inside the writer transaction. */
function controlWriteBody(ctx: Ctx, live: LiveConnections, connection: ControlConnection, path: string, args: unknown): unknown {
  const project = connection.projectId;
  const accountId = connection.identity?.account.id ?? "public";
  switch (path) {
    case "control.accounts.updatePassword": {
      const object = exactObject(args, ["currentPassword", "newPassword"]);
      const current = requiredString(object, "currentPassword");
      const next = requiredString(object, "newPassword");
      if (byteLength(next) < 12) throw invalid("newPassword must contain at least 12 characters");
      if (!nativePasswords(authMode(ctx, project))) throw invalid("password management is owned by the configured external identity provider");
      const stored = ctx.sql.value<string>("SELECT password_hash FROM gonvex_account_passwords WHERE project_id=$1 AND account_id=$2", project, accountId);
      if (stored !== undefined && !verifyPassword(current, stored)) throw invalid("current password is incorrect");
      ctx.sql.run(
        `INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)
         ON CONFLICT(project_id,account_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,updated_at=${NOW_MS}`,
        project,
        accountId,
        hashPassword(next),
      );
      return { updated: true };
    }
    case "control.auth.logout": {
      const object = exactObject(args, ["refreshToken", "all"]);
      const refresh = requiredString(object, "refreshToken");
      if (object.all === true) {
        ctx.sql.run(`UPDATE gonvex_auth_sessions SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE project_id=$1 AND account_id=$2`, project, accountId);
        ctx.sql.run(`UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE project_id=$1 AND account_id=$2`, project, accountId);
      } else {
        const family = ctx.sql.value<string>(
          "SELECT family_id FROM gonvex_auth_refresh_tokens WHERE project_id=$1 AND account_id=$2 AND token_hash=$3",
          project,
          accountId,
          sha256Hex(refresh),
        );
        if (family !== undefined) {
          for (const table of ["gonvex_auth_sessions", "gonvex_auth_refresh_tokens"]) {
            ctx.sql.run(`UPDATE ${table} SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE family_id=$1`, family);
          }
        }
      }
      return { updated: true };
    }
    case "control.auth.realms.configure":
      return configureRealm(ctx, connection, args);
    case "control.tenants.updateProfile": {
      const object = exactObject(args, ["name", "domain", "description"]);
      const session = tenant(connection);
      const name = requiredString(object, "name");
      const domain = requiredString(object, "domain").toLowerCase();
      const description = typeof object.description === "string" ? object.description.trim() : "";
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_runtime_tenants SET name=$3,domain=$4,description=$5,updated_at=${NOW_MS}
           WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`,
          project,
          session.route.tenantId,
          name,
          domain,
          description,
        ),
      );
    }
    case "control.tenants.updateTimezone": {
      const object = exactObject(args, ["timezone"]);
      const timezone = requiredString(object, "timezone");
      if (byteLength(timezone) > 128 || (!timezone.includes("/") && timezone !== "UTC")) throw invalid("timezone must be an IANA timezone or UTC");
      const session = tenant(connection);
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_runtime_tenants SET timezone=$3,updated_at=${NOW_MS} WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`,
          project,
          session.route.tenantId,
          timezone,
        ),
      );
    }
    case "control.tenants.delete": {
      emptyArgs(args);
      const session = tenant(connection);
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_runtime_tenants SET status='deleted',deleted_at=COALESCE(deleted_at,${NOW_MS}),updated_at=${NOW_MS}
           WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`,
          project,
          session.route.tenantId,
        ),
      );
    }
    case "control.tenants.setException": {
      const object = exactObject(args, ["tenantId", "value"]);
      const tenantId = requiredString(object, "tenantId");
      return upsertSetting(ctx, project, "tenant.exception", tenantId, object.value ?? null, accountId);
    }
    case "control.tenants.setSeatLimit": {
      const object = exactObject(args, ["tenantId", "seatLimit"]);
      const tenantId = requiredString(object, "tenantId");
      const raw = object.seatLimit;
      let seat: number | null = null;
      if (raw !== undefined && raw !== null) {
        if (typeof raw === "number" && Number.isInteger(raw) && raw >= 0 && raw <= 2147483647) seat = raw;
        else throw invalid("seatLimit must be a nonnegative integer or null");
      }
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_runtime_tenants SET seat_limit=$3,updated_at=${NOW_MS} WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL`,
          project,
          tenantId,
          seat,
        ),
      );
    }
    case "control.invitations.create": {
      const object = exactObject(args, ["email", "role", "permissions", "teamIds", "allowedAuthProviders", "payload"]);
      const session = tenant(connection);
      const email = normalizeEmail(requiredString(object, "email"));
      const role = membershipRole(requiredString(object, "role"));
      if (session.member.role === "admin" && (role === "owner" || role === "admin")) throw tenantAdminRequired();
      const permissions = object.permissions === undefined ? {} : object.permissions;
      if (!permissions || typeof permissions !== "object" || Array.isArray(permissions)) throw invalid("permissions must be an object");
      const teams = uniqueStringArray(object.teamIds, "teamIds");
      const providers = invitationProviders(object.allowedAuthProviders);
      const payload = object.payload === undefined ? {} : object.payload;
      const id = randomId("invite");
      const token = secureToken("invite");
      ctx.sql.run(
        `INSERT INTO gonvex_auth_membership_invitations
         (project_id,tenant_id,email,role,permissions,team_ids,allowed_auth_providers,
          application_payload,invited_by,expires_at,id,token_hash,revoked_at,accepted_at,
          accepted_account_id,accepted_idempotency_key,handoff_state,handoff_command_id,
          completed_at,updated_at)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,${NOW_MS}+604800000,$10,$11,
                NULL,NULL,NULL,NULL,'pending','',NULL,${NOW_MS})
         ON CONFLICT(project_id,tenant_id,email) DO UPDATE SET
           role=EXCLUDED.role,permissions=EXCLUDED.permissions,team_ids=EXCLUDED.team_ids,
           allowed_auth_providers=EXCLUDED.allowed_auth_providers,
           application_payload=EXCLUDED.application_payload,invited_by=EXCLUDED.invited_by,
           expires_at=EXCLUDED.expires_at,id=EXCLUDED.id,token_hash=EXCLUDED.token_hash,
           revoked_at=NULL,accepted_at=NULL,accepted_account_id=NULL,
           accepted_idempotency_key=NULL,handoff_state='pending',handoff_command_id='',
           completed_at=NULL,updated_at=${NOW_MS}`,
        project,
        session.route.tenantId,
        email,
        role,
        JSON.stringify(permissions),
        JSON.stringify(teams),
        JSON.stringify(providers),
        JSON.stringify(payload),
        accountId,
        id,
        sha256Hex(token),
      );
      return { id, token };
    }
    case "control.invitations.update": {
      const object = exactObject(args, ["id", "role", "permissions", "teamIds", "allowedAuthProviders", "payload"]);
      const session = tenant(connection);
      const id = requiredString(object, "id");
      const role = membershipRole(requiredString(object, "role"));
      if (session.member.role === "admin" && (role === "owner" || role === "admin")) throw tenantAdminRequired();
      const permissions = object.permissions === undefined ? {} : object.permissions;
      if (!permissions || typeof permissions !== "object" || Array.isArray(permissions)) throw invalid("permissions must be an object");
      const teams = uniqueStringArray(object.teamIds, "teamIds");
      const providers = invitationProviders(object.allowedAuthProviders);
      const payload = object.payload === undefined ? {} : object.payload;
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_auth_membership_invitations
           SET role=$4,permissions=$5,team_ids=$6,allowed_auth_providers=$7,
               application_payload=$8,invited_by=$9,updated_at=${NOW_MS}
           WHERE project_id=$1 AND tenant_id=$2 AND id=$3 AND accepted_at IS NULL AND revoked_at IS NULL`,
          project,
          session.route.tenantId,
          id,
          role,
          JSON.stringify(permissions),
          JSON.stringify(teams),
          JSON.stringify(providers),
          JSON.stringify(payload),
          accountId,
        ),
      );
    }
    case "control.invitations.revoke": {
      const object = exactObject(args, ["id", "email"]);
      const session = tenant(connection);
      const id = optionalString(object, "id") ?? "";
      const email = optionalString(object, "email") ?? "";
      if (!id && !email) throw invalid("id or email is required");
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_auth_membership_invitations SET revoked_at=${NOW_MS},updated_at=${NOW_MS}
           WHERE project_id=$1 AND tenant_id=$2 AND (id=$3 OR lower(email)=lower($4)) AND accepted_at IS NULL`,
          project,
          session.route.tenantId,
          id,
          email,
        ),
      );
    }
    case "control.agentAuth.issue": {
      const object = exactObject(args, ["permissions", "expiresInSeconds"]);
      const permissions = uniqueStringArray(object.permissions, "permissions");
      const expires = object.expiresInSeconds;
      if (!(typeof expires === "number" && Number.isInteger(expires) && expires >= 1 && expires <= 86400)) {
        throw invalid("expiresInSeconds must be between 1 and 86400");
      }
      const id = randomId("agent");
      const token = secureToken("agent_claim");
      ctx.sql.run(
        `INSERT INTO gonvex_agent_claim_tokens(id,project_id,token_hash,permissions,expires_at,created_by)
         VALUES($1,$2,$3,$4,${NOW_MS}+($5*1000),$6)`,
        id,
        project,
        sha256Hex(token),
        JSON.stringify(permissions),
        expires,
        accountId,
      );
      return { id, token };
    }
    case "control.agentAuth.claim": {
      const object = exactObject(args, ["token"]);
      const token = requiredString(object, "token");
      const row = ctx.sql.get(
        `UPDATE gonvex_agent_claim_tokens SET claimed_at=${NOW_MS},claimed_account_id=$3
         WHERE project_id=$1 AND token_hash=$2 AND claimed_at IS NULL
           AND revoked_at IS NULL AND expires_at>${NOW_MS} RETURNING id,permissions`,
        project,
        sha256Hex(token),
        accountId,
      );
      if (!row) throw invalid("agent token is invalid, expired, revoked, or already claimed");
      return { id: str(row.id), permissions: json(row.permissions) };
    }
    case "control.agentAuth.revoke": {
      const object = exactObject(args, ["id"]);
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_agent_claim_tokens SET revoked_at=${NOW_MS} WHERE project_id=$1 AND id=$2 AND revoked_at IS NULL`,
          project,
          requiredString(object, "id"),
        ),
      );
    }
    case "control.project.developers.invite": {
      const object = exactObject(args, ["email", "name", "role"]);
      const email = normalizeEmail(requiredString(object, "email"));
      const name = requiredString(object, "name");
      const role = requiredString(object, "role");
      if (!["owner", "admin", "dev"].includes(role)) throw invalid("role is invalid");
      ctx.sql.run(
        `INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,$2,$3,$4)
         ON CONFLICT(project_id,email) DO UPDATE SET name=EXCLUDED.name,role=EXCLUDED.role`,
        project,
        email,
        name,
        role,
      );
      return { updated: true };
    }
    case "control.project.developers.remove": {
      const object = exactObject(args, ["email"]);
      return affectedResult(
        ctx.sql.affected("DELETE FROM gonvex_project_members WHERE project_id=$1 AND email=$2", project, normalizeEmail(requiredString(object, "email"))),
      );
    }
    case "control.developer.enter": {
      const object = exactObject(args, ["tenantId"]);
      const tenantId = requiredString(object, "tenantId");
      const id = randomId("devmode");
      const token = secureToken("imp");
      const expires = Date.now() + 30 * 60 * 1000;
      ctx.sql.run(
        `INSERT INTO gonvex_impersonation_grants(id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at)
         VALUES($1,$2,$3,$4,$4,$5,'developer mode',$6)`,
        id,
        project,
        sha256Hex(token),
        accountId,
        tenantId,
        expires,
      );
      return { id, token, expiresAt: rfc3339(expires) };
    }
    case "control.developer.exit": {
      const object = exactObject(args, ["grantId"]);
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_impersonation_grants SET revoked_at=${NOW_MS} WHERE project_id=$1 AND id=$2 AND actor_account_id=$3 AND revoked_at IS NULL`,
          project,
          requiredString(object, "grantId"),
          accountId,
        ),
      );
    }
    case "control.assistant.setDefaults":
    case "control.voice.setRateCard":
    case "control.voice.setTenantEntitlement":
    case "control.voice.setUserOverride": {
      const object = exactObject(args, ["scopeId", "value"]);
      const scope = typeof object.scopeId === "string" ? object.scopeId.trim() : "";
      const kind =
        path === "control.assistant.setDefaults"
          ? "assistant.defaults"
          : path === "control.voice.setRateCard"
            ? "voice.rateCard"
            : path === "control.voice.setTenantEntitlement"
              ? "voice.tenantEntitlement"
              : "voice.userOverride";
      return upsertSetting(ctx, project, kind, scope, object.value ?? null, accountId);
    }
    case "control.support.heartbeat": {
      const object = exactObject(args, ["release", "environment"]);
      const release = typeof object.release === "string" ? object.release.trim() : "";
      const environment = typeof object.environment === "string" ? object.environment.trim() : "";
      const sessionId = `support_${connection.connectionId}`;
      ctx.sql.run(
        `INSERT INTO gonvex_support_sessions
         (id,project_id,tenant_id,account_id,connection_id,release,environment,last_seen_at)
         VALUES($1,$2,$3,$4,$5,$6,$7,${NOW_MS})
         ON CONFLICT(id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,
           account_id=EXCLUDED.account_id,connection_id=EXCLUDED.connection_id,
           release=EXCLUDED.release,environment=EXCLUDED.environment,last_seen_at=${NOW_MS}`,
        sessionId,
        project,
        connection.tenant?.route.tenantId ?? "",
        accountId,
        connection.connectionId,
        release,
        environment,
      );
      return { sessionId };
    }
    case "control.support.sendCommand": {
      const object = exactObject(args, ["sessionId", "kind", "payload"]);
      const sessionId = requiredString(object, "sessionId");
      const kind = requiredString(object, "kind");
      const payload = object.payload === undefined ? {} : object.payload;
      const exists = num(ctx.sql.value("SELECT EXISTS(SELECT 1 FROM gonvex_support_sessions WHERE project_id=$1 AND id=$2)", project, sessionId));
      if (!exists) throw invalid("support session not found");
      const id = randomId("command");
      ctx.sql.run(
        "INSERT INTO gonvex_support_commands(id,project_id,session_id,kind,payload,created_by) VALUES($1,$2,$3,$4,$5,$6)",
        id,
        project,
        sessionId,
        kind,
        JSON.stringify(payload),
        accountId,
      );
      // 0012 R8: no `support.command` push; Rust only broadcasts on the Query path.
      return { id };
    }
    case "control.support.ackCommand": {
      const object = exactObject(args, ["id"]);
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_support_commands SET acknowledged_at=${NOW_MS}
           WHERE id=$1 AND project_id=$2 AND acknowledged_at IS NULL
             AND EXISTS(SELECT 1 FROM gonvex_support_sessions session
                        WHERE session.id=gonvex_support_commands.session_id AND session.account_id=$3)`,
          requiredString(object, "id"),
          project,
          accountId,
        ),
      );
    }
    case "control.support.pruneSessions": {
      const object = exactObject(args, ["olderThanSeconds"]);
      const seconds = object.olderThanSeconds;
      if (!(typeof seconds === "number" && Number.isInteger(seconds) && seconds >= 60 && seconds <= 31_536_000)) {
        throw invalid("olderThanSeconds must be between 60 and 31536000");
      }
      const liveIds = [...live.supportLiveIds(project)];
      const deleted = ctx.sql.affected(
        `DELETE FROM gonvex_support_sessions WHERE project_id=$1 AND last_seen_at < ${NOW_MS}-($2*1000)
         AND id NOT IN (SELECT value FROM json_each($3))`,
        project,
        seconds,
        JSON.stringify(liveIds),
      );
      return { deleted };
    }
    case "control.demos.resetPassword": {
      const object = exactObject(args, ["accountId", "password"]);
      const target = requiredString(object, "accountId");
      const password = requiredString(object, "password");
      if (byteLength(password) < 12) throw invalid("password must contain at least 12 characters");
      return affectedResult(
        ctx.sql.affected(
          `UPDATE gonvex_account_passwords SET password_hash=$3,updated_at=${NOW_MS}
           WHERE project_id=$1 AND account_id=$2 AND EXISTS(SELECT 1 FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2)`,
          project,
          target,
          hashPassword(password),
        ),
      );
    }
    default:
      throw notImplemented(path);
  }
}

function upsertSetting(ctx: Ctx, project: string, kind: string, scope: string, value: unknown, actor: string): { updated: true } {
  ctx.sql.run(
    `INSERT INTO gonvex_control_settings(project_id,kind,scope_id,value,updated_by) VALUES($1,$2,$3,$4,$5)
     ON CONFLICT(project_id,kind,scope_id) DO UPDATE SET value=EXCLUDED.value,updated_by=EXCLUDED.updated_by,updated_at=${NOW_MS}`,
    project,
    kind,
    scope,
    JSON.stringify(value),
    actor,
  );
  return { updated: true };
}

/** `validate_external_url` (control.rs:4646). */
function validateExternalUrl(value: string): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw invalid("jwksUrl is invalid");
  }
  if (url.protocol !== "https:" || url.username || url.password || !url.hostname) throw invalid("jwksUrl must be an HTTPS URL without credentials");
}

/** control.auth.realms.configure (control.rs:2618). */
function configureRealm(ctx: Ctx, connection: ControlConnection, args: unknown): { updated: true } {
  const project = connection.projectId;
  const object = exactObject(args, [
    "provider",
    "authMode",
    "enabled",
    "signupMode",
    "azureTenantId",
    "clientId",
    "clientSecret",
    "issuer",
    "audience",
    "jwksUrl",
    "firebaseProjectId",
    "firebaseTenantId",
    "adminCredentials",
  ]);
  const provider = requiredString(object, "provider");
  if (!["google", "microsoft", "apple", "firebase", "external-oidc"].includes(provider)) throw invalid("provider is unsupported");
  const signupMode = requiredString(object, "signupMode");
  if (signupMode !== "personal" && signupMode !== "inviteOnly") throw invalid("signupMode must be personal or inviteOnly");
  if (typeof object.enabled !== "boolean") throw invalid("enabled must be boolean");
  const enabled = object.enabled;
  const mode = nonEmpty(optionalString(object, "authMode")) ?? authMode(ctx, project);
  requireAuthMode(mode);
  const external = provider === "firebase" || provider === "external-oidc";
  if (enabled && ((external && mode !== "hybrid" && mode !== provider) || (!external && !nativePasswords(mode)))) {
    throw invalid(`authMode ${debugStr(mode)} does not enable provider ${debugStr(provider)}`);
  }
  const azureTenant = optionalString(object, "azureTenantId") ?? "";
  const clientId = optionalString(object, "clientId") ?? "";
  const clientSecret = optionalString(object, "clientSecret") ?? "";
  let issuer = optionalString(object, "issuer") ?? "";
  let audience = optionalString(object, "audience") ?? "";
  let jwksUrl = optionalString(object, "jwksUrl") ?? "";
  const firebaseProject = optionalString(object, "firebaseProjectId") ?? "";
  const firebaseTenant = optionalString(object, "firebaseTenantId") ?? "";
  const adminCredentials = optionalString(object, "adminCredentials") ?? "";
  if (provider === "firebase" && enabled) {
    if (!firebaseProject) throw invalid("firebaseProjectId is required when Firebase auth is enabled");
    if (!issuer) issuer = `https://securetoken.google.com/${firebaseProject}`;
    if (!audience) audience = firebaseProject;
    if (!jwksUrl) jwksUrl = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com";
  }
  if (provider === "external-oidc" && enabled && (!issuer || !audience || !jwksUrl)) {
    throw invalid("issuer, audience, and jwksUrl are required when external OIDC is enabled");
  }
  if (enabled && (provider === "microsoft" || provider === "apple") && !clientId) throw invalid("clientId is required when this provider is enabled");
  if (provider === "microsoft" && enabled && !azureTenant) throw invalid("azureTenantId is required when Microsoft auth is enabled");
  if (jwksUrl) validateExternalUrl(jwksUrl);
  if (adminCredentials) {
    if (provider !== "firebase") throw invalid("adminCredentials is supported only for Firebase");
    let credential: unknown;
    try {
      credential = JSON.parse(adminCredentials);
    } catch {
      throw invalid("Firebase adminCredentials must be valid JSON");
    }
    if ((credential as { project_id?: unknown } | null)?.project_id !== firebaseProject) {
      throw invalid("Firebase adminCredentials project_id does not match firebaseProjectId");
    }
  }
  const existingSecret = num(
    ctx.sql.value("SELECT EXISTS(SELECT 1 FROM gonvex_auth_providers WHERE project_id=$1 AND provider=$2 AND client_secret_encrypted IS NOT NULL)", project, provider),
  );
  if (enabled && (provider === "microsoft" || provider === "apple") && !clientSecret && !existingSecret) {
    throw invalid("clientSecret is required when this provider is enabled");
  }
  const encryptedSecret = clientSecret ? encryptControlSecret(ctx.config.controlSecret, clientSecret) : null;
  const encryptedAdmin = adminCredentials ? encryptControlSecret(ctx.config.controlSecret, adminCredentials) : null;
  ctx.sql.run(
    `INSERT INTO gonvex_auth_providers
     (project_id,provider,enabled,signup_mode,azure_tenant_id,client_id,
      client_secret_encrypted,issuer,audience,jwks_url,firebase_project_id,
      firebase_tenant_id,admin_credentials_encrypted)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT(project_id,provider) DO UPDATE SET
       enabled=EXCLUDED.enabled,signup_mode=EXCLUDED.signup_mode,
       azure_tenant_id=EXCLUDED.azure_tenant_id,client_id=EXCLUDED.client_id,
       client_secret_encrypted=COALESCE(EXCLUDED.client_secret_encrypted,gonvex_auth_providers.client_secret_encrypted),
       issuer=EXCLUDED.issuer,audience=EXCLUDED.audience,jwks_url=EXCLUDED.jwks_url,
       firebase_project_id=EXCLUDED.firebase_project_id,
       firebase_tenant_id=EXCLUDED.firebase_tenant_id,
       admin_credentials_encrypted=COALESCE(EXCLUDED.admin_credentials_encrypted,gonvex_auth_providers.admin_credentials_encrypted),
       updated_at=${NOW_MS}`,
    project,
    provider,
    enabled,
    signupMode,
    azureTenant,
    clientId,
    encryptedSecret,
    issuer,
    audience,
    jwksUrl,
    firebaseProject,
    firebaseTenant,
    encryptedAdmin,
  );
  ctx.sql.run(`UPDATE gonvex_runtime_projects SET auth_mode=$2,updated_at=${NOW_MS} WHERE id=$1`, project, mode);
  return { updated: true };
}

// ------------------------------------------------------------ auth Actions

/** `load_external_configuration` (control.rs:3613). */
function loadExternalConfiguration(ctx: Ctx, project: string, provider: string): ExternalAuthConfiguration {
  const mode = authMode(ctx, project);
  if (mode !== "hybrid" && mode !== provider) throw invalid(`project auth mode ${debugStr(mode)} does not allow provider ${debugStr(provider)}`);
  const row = ctx.sql.get(
    `SELECT provider,issuer,audience,jwks_url,firebase_project_id,firebase_tenant_id,signup_mode,admin_credentials_encrypted
     FROM gonvex_auth_providers WHERE project_id=$1 AND provider=$2 AND enabled=TRUE`,
    project,
    provider,
  );
  if (!row) throw invalid("external identity provider is disabled");
  let issuer = str(row.issuer);
  let audience = str(row.audience);
  let jwksUrl = str(row.jwks_url);
  const firebaseProject = str(row.firebase_project_id);
  if (provider === "firebase") {
    if (!issuer) issuer = `https://securetoken.google.com/${firebaseProject}`;
    if (!audience) audience = firebaseProject;
    if (!jwksUrl) jwksUrl = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com";
  }
  if (!issuer || !audience || !jwksUrl) throw invalid("external identity provider configuration is incomplete");
  const encrypted = blob(row.admin_credentials_encrypted);
  return {
    provider: str(row.provider),
    issuer,
    audience,
    jwksUrl,
    firebaseTenantId: str(row.firebase_tenant_id),
    signupMode: str(row.signup_mode),
    firebaseProjectId: firebaseProject,
    firebaseAdminCredentials: encrypted ? decryptControlSecret(ctx.config.controlSecret, encrypted) : null,
  };
}

/** `execute_auth_action` (control.rs:373). */
async function executeAuthAction(ctx: Ctx, connection: ControlConnection, path: string, args: unknown, key: string): Promise<unknown> {
  const project = connection.projectId;
  let externalIdentity: VerifiedExternalIdentity | null = null;
  if (path === "control.auth.exchangeExternalToken") {
    const object = exactObject(args, ["provider", "token", "tenantId", "previousRefreshToken"]);
    const provider = requiredString(object, "provider");
    if (provider !== "firebase" && provider !== "external-oidc") throw invalid("provider must be firebase or external-oidc");
    const configuration = loadExternalConfiguration(ctx, project, provider);
    // Token verification (network) happens before the writer transaction.
    externalIdentity = await verifyExternalToken(ctx.config, ctx.fetch, configuration, requiredString(object, "token"));
  }
  const subject = authSubject(path, args);
  const outcome = ctx.sql.tx((): { replay: unknown } | { result: Record<string, unknown> } | { reuse: string } => {
    dropExpiredClaim(ctx, project, subject, key);
    const claim = claimIdempotency(ctx, project, subject, key, "action", path);
    if (claim) return claim;
    let grant: SessionGrant;
    let authAccount: AuthAccount;
    let requestedTenant = "";
    if (path === "control.auth.passwordLogin") {
      const object = exactObject(args, ["email", "password"]);
      if (!nativePasswords(authMode(ctx, project))) throw invalid("password authentication is disabled; use the project's external identity provider");
      const email = normalizeEmail(requiredString(object, "email"));
      const row = ctx.sql.get(
        `SELECT account.id,password.password_hash,account.email,account.name,account.avatar_url
         FROM accounts account
         JOIN gonvex_account_passwords password ON password.project_id=account.auth_realm_id AND password.account_id=account.id
         WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2) AND account.disabled_at IS NULL`,
        project,
        email,
      );
      if (!row) throw invalid("invalid email or password");
      if (!verifyPassword(requiredString(object, "password"), str(row.password_hash))) throw invalid("invalid email or password");
      authAccount = { id: str(row.id), email: str(row.email), emailVerified: true, name: str(row.name), picture: str(row.avatar_url), provider: "password" };
      grant = issueSession(ctx, project, authAccount.id, newFamily(), Date.now() + REFRESH_TOKEN_MS);
    } else if (path === "control.auth.exchangeExternalToken") {
      const object = exactObject(args, ["provider", "token", "tenantId", "previousRefreshToken"]);
      authAccount = resolveExternalAccount(ctx, project, externalIdentity!);
      grant = issueSession(ctx, project, authAccount.id, newFamily(), Date.now() + REFRESH_TOKEN_MS);
      const previous = optionalString(object, "previousRefreshToken");
      if (previous) {
        ctx.sql.run(
          `UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,${NOW_MS})
           WHERE project_id=$1 AND account_id=$2 AND family_id=(
             SELECT family_id FROM gonvex_auth_refresh_tokens WHERE project_id=$1 AND account_id=$2 AND token_hash=$3 LIMIT 1)`,
          project,
          authAccount.id,
          sha256Hex(previous),
        );
      }
      requestedTenant = optionalString(object, "tenantId") ?? "";
    } else {
      const object = exactObject(args, ["refreshToken"]);
      const refreshed = refreshSession(ctx, project, requiredString(object, "refreshToken"));
      if (refreshed.kind === "reuseRevoked") {
        const message = "refresh token reuse detected; this login was revoked";
        ctx.sql.run(
          `UPDATE gonvex_control_idempotency SET state='failed',error=$4,updated_at=${NOW_MS}
           WHERE project_id=$1 AND subject_id=$2 AND idempotency_key=$3`,
          project,
          subject,
          key,
          message,
        );
        // The revocation and the failed receipt commit; the caller gets the error.
        return { reuse: message };
      }
      grant = refreshed.grant;
      authAccount = refreshed.account;
    }
    const result = sessionResult(grant, authAccount, requestedTenant);
    completeIdempotency(ctx, project, subject, key, result);
    return { result };
  });
  if ("reuse" in outcome) throw invalid(outcome.reuse);
  const result = "replay" in outcome ? (outcome.replay as Record<string, unknown>) : outcome.result;
  return sessionResultFromDirectory(ctx, project, result ?? {});
}

// --------------------------------------------------------------------- sagas

/** control.accounts.resetMemberPassword (control.rs:568). */
async function resetMemberPassword(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const object = exactObject(args, ["memberId", "newPassword"]);
  const memberId = requiredString(object, "memberId");
  const newPassword = requiredString(object, "newPassword");
  if (byteLength(newPassword) < 12) throw invalid("newPassword must contain at least 12 characters");
  const caller = tenant(connection);
  if (!nativePasswords(authMode(ctx, connection.projectId))) throw invalid("password management is owned by the configured external identity provider");
  const route = resolveTenant(ctx, connection.projectId, caller.route.tenantId);
  const target = await tenantCall(ctx, route, (port) => port.activeMember(memberId));
  if (!target) throw invalid("active tenant member was not found");
  if (caller.member.role === "admin" && (target.role === "owner" || target.role === "admin")) throw tenantAdminRequired();
  const subject = controlSubject(connection, "tenantAdmin");
  const hash = hashPassword(newPassword);
  return ctx.sql.tx(() => {
    dropExpiredClaim(ctx, connection.projectId, subject, key);
    const claim = claimIdempotency(ctx, connection.projectId, subject, key, "reducer", "control.accounts.resetMemberPassword");
    if (claim) return claim.replay;
    ctx.sql.run(
      `INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash,updated_at) VALUES($1,$2,$3,${NOW_MS})
       ON CONFLICT(project_id,account_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,updated_at=${NOW_MS}`,
      connection.projectId,
      target.accountId,
      hash,
    );
    for (const table of ["gonvex_auth_sessions", "gonvex_auth_refresh_tokens"]) {
      ctx.sql.run(`UPDATE ${table} SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE project_id=$1 AND account_id=$2`, connection.projectId, target.accountId);
    }
    const result = { updated: true };
    completeIdempotency(ctx, connection.projectId, subject, key, result);
    return result;
  });
}

const upsertIndexSql = `INSERT INTO account_tenant_index
  (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
  VALUES($1,$2,$3,'active',$4,${NOW_MS})
  ON CONFLICT(account_id,tenant_id) DO UPDATE SET member_id=EXCLUDED.member_id,
    status='active',tenant_membership_revision=max(account_tenant_index.tenant_membership_revision,
      EXCLUDED.tenant_membership_revision),updated_at=${NOW_MS}`;

/** control.accounts.provisionMemberLogin (control.rs:667). */
async function provisionMemberLogin(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const object = exactObject(args, ["email", "name", "password", "role", "permissions"]);
  const email = normalizeEmail(requiredString(object, "email"));
  const name = requiredString(object, "name");
  const password = requiredString(object, "password");
  const role = requiredString(object, "role");
  const permissions = object.permissions;
  if (!permissions || typeof permissions !== "object" || Array.isArray(permissions)) throw invalid("permissions must be an object");
  if (byteLength(password) < 12) throw invalid("password must contain at least 12 characters");
  if (!["owner", "admin", "member"].includes(role)) throw invalid("role is invalid");
  const caller = tenant(connection);
  if (caller.member.role === "admin" && (role === "owner" || role === "admin")) throw tenantAdminRequired();
  const project = connection.projectId;
  const tenantId = caller.route.tenantId;
  const accountId = ctx.sql.tx(() => {
    if (!nativePasswords(authMode(ctx, project))) {
      throw invalid("login provisioning is owned by the configured external identity provider; invite the account instead");
    }
    const existing = ctx.sql.get(
      `SELECT account.id,
              EXISTS(SELECT 1 FROM gonvex_member_login_provisioning provisioning
                     WHERE provisioning.project_id=$1 AND provisioning.tenant_id=$3
                       AND provisioning.email=$2 AND provisioning.account_id=account.id) AS resumable
       FROM accounts account WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2)
       ORDER BY account.id LIMIT 1`,
      project,
      email,
      tenantId,
    );
    if (existing) {
      if (!bool(existing.resumable)) throw invalid("this account already exists; invite it instead of provisioning its login");
      return str(existing.id);
    }
    const id = randomId("acct");
    ctx.sql.run(`INSERT INTO accounts(id,auth_realm_id,email,name,updated_at) VALUES($1,$2,$3,$4,${NOW_MS})`, id, project, email, name);
    ctx.sql.run(
      `INSERT INTO account_identities(project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
       VALUES($1,$2,'password',$1,$3,$3,TRUE,${NOW_MS})`,
      project,
      id,
      email,
    );
    ctx.sql.run("INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)", project, id, hashPassword(password));
    ctx.sql.run(
      "INSERT INTO gonvex_member_login_provisioning(project_id,tenant_id,email,account_id,created_by) VALUES($1,$2,$3,$4,$5)",
      project,
      tenantId,
      email,
      id,
      caller.identity.account.id,
    );
    return id;
  });
  const route = resolveTenant(ctx, project, tenantId);
  const written = await tenantCall(ctx, route, (port) =>
    port.writeMembership({ kind: "provision", commandId: `member-provision:${accountId}`, accountId, displayName: name, role, permissions }),
  );
  const subject = controlSubject(connection, "tenantAdmin");
  return ctx.sql.tx(() => {
    dropExpiredClaim(ctx, project, subject, key);
    const claim = claimIdempotency(ctx, project, subject, key, "reducer", "control.accounts.provisionMemberLogin");
    if (claim) return claim.replay;
    ctx.sql.run(upsertIndexSql, accountId, tenantId, written.memberId, written.revision);
    const result = { updated: true, accountId, memberId: written.memberId };
    completeIdempotency(ctx, project, subject, key, result);
    return result;
  });
}

/** control.developer.provisionSelf / removeSelf (control.rs:858). */
async function changeDeveloperMembership(ctx: Ctx, connection: ControlConnection, path: string, args: unknown, key: string): Promise<unknown> {
  const identity = account(connection);
  const object = exactObject(args, ["tenantId"]);
  const tenantId = requiredString(object, "tenantId");
  const project = connection.projectId;
  const route = resolveTenant(ctx, project, tenantId);
  const provision = path === "control.developer.provisionSelf";
  const commandId = `developer-membership:${identity.account.id}:${key}`;
  const written = await tenantCall(ctx, route, (port) =>
    port.writeMembership(
      provision
        ? { kind: "developerProvision", commandId, accountId: identity.account.id, displayName: identity.account.name, avatarUrl: identity.account.avatarUrl }
        : { kind: "developerRemove", commandId, accountId: identity.account.id },
    ),
  );
  const subject = controlSubject(connection, "developer");
  return ctx.sql.tx(() => {
    dropExpiredClaim(ctx, project, subject, key);
    const claim = claimIdempotency(ctx, project, subject, key, "reducer", path);
    if (claim) return claim.replay;
    if (provision) {
      ctx.sql.run(upsertIndexSql, identity.account.id, tenantId, written.memberId, written.revision);
    } else {
      ctx.sql.run(
        `UPDATE account_tenant_index SET status='revoked',tenant_membership_revision=max(tenant_membership_revision,$3),updated_at=${NOW_MS}
         WHERE account_id=$1 AND tenant_id=$2`,
        identity.account.id,
        tenantId,
        written.revision,
      );
    }
    const result = provision ? { updated: true, tenantId, memberId: written.memberId } : { updated: written.updated };
    completeIdempotency(ctx, project, subject, key, result);
    return result;
  });
}

/** control.support.createImpersonation (control.rs:987). */
async function createSupportImpersonation(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const actor = account(connection);
  const object = exactObject(args, ["accountId", "tenantId", "reason"]);
  const targetAccount = requiredString(object, "accountId");
  const tenantId = requiredString(object, "tenantId");
  const reason = requiredString(object, "reason");
  try {
    await admitMember(ctx, connection.projectId, tenantId, targetAccount);
  } catch {
    throw invalid("target account is not an active tenant member");
  }
  const subject = controlSubject(connection, "projectAdmin");
  return ctx.sql.tx(() => {
    dropExpiredClaim(ctx, connection.projectId, subject, key);
    const claim = claimIdempotency(ctx, connection.projectId, subject, key, "reducer", "control.support.createImpersonation");
    if (claim) return claim.replay;
    const id = randomId("imp");
    const token = secureToken("imp");
    const expires = Date.now() + 5 * 60 * 1000;
    ctx.sql.run(
      `INSERT INTO gonvex_impersonation_grants(id,project_id,token_hash,actor_account_id,target_account_id,tenant_id,reason,expires_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
      id,
      connection.projectId,
      sha256Hex(token),
      actor.account.id,
      targetAccount,
      tenantId,
      reason,
      expires,
    );
    const result = { id, token, expiresAt: rfc3339(expires) };
    completeIdempotency(ctx, connection.projectId, subject, key, result);
    return result;
  });
}

const DIRECTORY_INSERT_SQL = `INSERT INTO gonvex_runtime_tenants
  (relationship_id,project_id,tenant_id,name,database_alias,database_name,
   database_url,domain,status,description,provisioned,runtime_created)
  VALUES($1,$2,$1,$3,$4,$5,$6,$7,'active','Account-created tenant database.',FALSE,TRUE)
  ON CONFLICT(project_id,tenant_id) DO UPDATE SET name=EXCLUDED.name,
    domain=EXCLUDED.domain,database_url=EXCLUDED.database_url,updated_at=${NOW_MS}`;

/**
 * control.tenants.create (control.rs:1055). Replace (C.1 row 14): the tenant
 * is a tenant object (`idFromName(tenantId)`) provisioned by RPC; the
 * directory row lives here.
 */
async function createTenant(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const identity = account(connection);
  const object = exactObject(args, ["name", "domain"]);
  const requestedName = requiredString(object, "name");
  const rawDomain = typeof object.domain === "string" ? object.domain.trim() : "";
  const requestedDomain = rawDomain ? rawDomain.toLowerCase() : slug(requestedName);
  const domainBytes = Buffer.from(requestedDomain);
  const validDomain =
    domainBytes.length > 0 &&
    domainBytes.length <= 63 &&
    [...domainBytes].every((byte, index) => (byte >= 0x61 && byte <= 0x7a) || (byte >= 0x30 && byte <= 0x39) || (byte === 0x2d && index > 0 && index + 1 < domainBytes.length));
  if (!validDomain) throw invalid("tenant domain must be a lowercase DNS label");
  const project = connection.projectId;
  const reserved = ctx.sql.tx(() => {
    const row = ctx.sql.get(
      `SELECT COALESCE(NULLIF(database_mode,''),'single') AS database_mode,
              EXISTS(SELECT 1 FROM gonvex_auth_providers WHERE project_id=$1 AND enabled=TRUE AND signup_mode='inviteOnly') AS invite_only
       FROM gonvex_runtime_projects WHERE id=$1`,
      project,
    );
    if (!row) throw new ControlError(ROW_NOT_FOUND, "rejected");
    if (str(row.database_mode) !== "multiTenant") throw invalid("project is not configured for tenant databases");
    if (bool(row.invite_only)) throw invalid("this project allows tenant creation only through its Control Plane");
    const checkpoint = ctx.sql.get(
      "SELECT tenant_id,database_name,database_alias,name,domain,account_id FROM gonvex_tenant_provisioning WHERE project_id=$1 AND idempotency_key=$2",
      project,
      key,
    );
    if (checkpoint) {
      return {
        tenantId: str(checkpoint.tenant_id),
        databaseName: str(checkpoint.database_name),
        databaseAlias: str(checkpoint.database_alias),
        name: str(checkpoint.name),
        domain: str(checkpoint.domain),
        owner: str(checkpoint.account_id),
      };
    }
    const tenantId = randomId("tenant");
    const databaseName = `gonvex_tenant_${uuidSimple()}`;
    const databaseAlias = `${slug(requestedName) || "workspace"}-${tenantId.replaceAll("-", "").slice(0, 8)}`;
    const inserted = ctx.sql.affected(
      `INSERT INTO gonvex_tenant_provisioning(project_id,idempotency_key,tenant_id,database_name,database_alias,name,domain,account_id)
       SELECT $1,$2,$3,$4,$5,$6,$7,$8
       WHERE NOT EXISTS (SELECT 1 FROM gonvex_runtime_tenants WHERE project_id=$1 AND lower(domain)=lower($7)
                         AND deleted_at IS NULL AND status NOT IN ('deleted','disabled'))`,
      project,
      key,
      tenantId,
      databaseName,
      databaseAlias,
      requestedName,
      requestedDomain,
      identity.account.id,
    );
    if (inserted === 0) throw invalid("tenant domain is already in use");
    return { tenantId, databaseName, databaseAlias, name: requestedName, domain: requestedDomain, owner: identity.account.id };
  });
  if (reserved.owner !== identity.account.id) throw invalid("tenant provisioning command belongs to another account");
  const databaseUrl = tenantDatabaseUrl(reserved.tenantId);
  ctx.sql.tx(() =>
    ctx.sql.run(DIRECTORY_INSERT_SQL, reserved.tenantId, project, reserved.name, reserved.databaseAlias, reserved.databaseName, databaseUrl, reserved.domain),
  );
  const route = { projectId: project, tenantId: reserved.tenantId, databaseUrl };
  if (!projectModule(ctx, project)) throw invalid("project module is unavailable");
  await tenantCall(ctx, route, (port) => port.provision(project, reserved.tenantId));
  const written = await tenantCall(ctx, route, (port) =>
    port.writeMembership({
      kind: "tenantOwner",
      commandId: `tenant-create:${reserved.tenantId}`,
      accountId: identity.account.id,
      displayName: identity.account.name,
      avatarUrl: identity.account.avatarUrl,
    }),
  );
  const subject = controlSubject(connection, "account");
  return ctx.sql.tx(() => {
    dropExpiredClaim(ctx, project, subject, key);
    const claim = claimIdempotency(ctx, project, subject, key, "reducer", "control.tenants.create");
    if (claim) return claim.replay;
    ctx.sql.run(upsertIndexSql, identity.account.id, reserved.tenantId, written.memberId, written.revision);
    ctx.sql.run(`UPDATE gonvex_runtime_tenants SET provisioned=TRUE,updated_at=${NOW_MS} WHERE project_id=$1 AND tenant_id=$2`, project, reserved.tenantId);
    const result = {
      id: reserved.tenantId,
      name: reserved.name,
      role: "owner",
      permissions: {},
      domain: reserved.domain,
      timezone: "UTC",
      description: "Account-created tenant database.",
      profile: {},
    };
    completeIdempotency(ctx, project, subject, key, result);
    return result;
  });
}

/** control.demos.create (control.rs:1304). */
async function createDemoAccount(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const object = exactObject(args, ["tenantId", "email", "name", "password", "label"]);
  const tenantId = requiredString(object, "tenantId");
  const email = normalizeEmail(requiredString(object, "email"));
  const name = requiredString(object, "name");
  const password = requiredString(object, "password");
  const label = requiredString(object, "label");
  if (byteLength(password) < 12) throw invalid("password must contain at least 12 characters");
  const project = connection.projectId;
  const route = resolveTenant(ctx, project, tenantId);
  const subject = controlSubject(connection, "projectAdmin");
  const hash = hashPassword(password);
  const reserved = ctx.sql.tx((): { replay: unknown } | { accountId: string } => {
    dropExpiredClaim(ctx, project, subject, key);
    const claim = claimSaga(ctx, project, subject, key, "reducer", "control.demos.create");
    if (claim) return claim;
    const existing = ctx.sql.get(
      `SELECT account.id,
              EXISTS(SELECT 1 FROM gonvex_demo_accounts demo WHERE demo.project_id=$1 AND demo.account_id=account.id AND demo.tenant_id=$3) AS is_demo
       FROM accounts account WHERE account.auth_realm_id=$1 AND lower(account.email)=lower($2) ORDER BY account.id LIMIT 1`,
      project,
      email,
      tenantId,
    );
    if (existing && !bool(existing.is_demo)) throw invalid("email already belongs to a non-demo account");
    const accountId = existing ? str(existing.id) : randomId("acct");
    ctx.sql.run(
      `INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at) VALUES($1,$2,$3,$4,NULL,${NOW_MS})
       ON CONFLICT(id) DO UPDATE SET email=EXCLUDED.email,name=EXCLUDED.name,disabled_at=NULL,updated_at=${NOW_MS}`,
      accountId,
      project,
      email,
      name,
    );
    ctx.sql.run(
      `INSERT INTO account_identities(project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
       VALUES($1,$2,'password',$1,$3,$3,TRUE,${NOW_MS})
       ON CONFLICT(project_id,provider,issuer,subject) DO UPDATE SET account_id=EXCLUDED.account_id,email=EXCLUDED.email,
         verified_email=TRUE,updated_at=${NOW_MS}`,
      project,
      accountId,
      email,
    );
    ctx.sql.run(
      `INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)
       ON CONFLICT(project_id,account_id) DO UPDATE SET password_hash=EXCLUDED.password_hash,updated_at=${NOW_MS}`,
      project,
      accountId,
      hash,
    );
    ctx.sql.run(
      `INSERT INTO gonvex_demo_accounts(project_id,account_id,tenant_id,label) VALUES($1,$2,$3,$4)
       ON CONFLICT(project_id,account_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id,label=EXCLUDED.label`,
      project,
      accountId,
      tenantId,
      label,
    );
    return { accountId };
  });
  if ("replay" in reserved) return reserved.replay;
  const { accountId } = reserved;
  const written = await tenantCall(ctx, route, (port) =>
    port.writeMembership({ kind: "demoCreate", commandId: `demo-create:${accountId}`, accountId, displayName: name }),
  );
  return ctx.sql.tx(() => {
    ctx.sql.run(upsertIndexSql, accountId, tenantId, written.memberId, written.revision);
    const result = { accountId, memberId: written.memberId };
    completeIdempotency(ctx, project, subject, key, result);
    return result;
  });
}

/** control.demos.delete (control.rs:1467). */
async function deleteDemoAccount(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const object = exactObject(args, ["accountId"]);
  const accountId = requiredString(object, "accountId");
  const project = connection.projectId;
  const subject = controlSubject(connection, "projectAdmin");
  const lookup = ctx.sql.tx((): { replay: unknown } | { tenantId: string } => {
    dropExpiredClaim(ctx, project, subject, key);
    const claim = claimSaga(ctx, project, subject, key, "reducer", "control.demos.delete");
    if (claim) return claim;
    const tenantId = ctx.sql.value<string>("SELECT tenant_id FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2", project, accountId);
    if (tenantId === undefined) throw invalid("demo account not found");
    return { tenantId };
  });
  if ("replay" in lookup) return lookup.replay;
  const route = resolveTenant(ctx, project, lookup.tenantId);
  const written = await tenantCall(ctx, route, (port) => port.writeMembership({ kind: "demoDelete", commandId: `demo-delete:${accountId}`, accountId }));
  return ctx.sql.tx(() => {
    ctx.sql.run("DELETE FROM gonvex_demo_accounts WHERE project_id=$1 AND account_id=$2", project, accountId);
    ctx.sql.run(`UPDATE accounts SET disabled_at=${NOW_MS},updated_at=${NOW_MS} WHERE id=$1 AND auth_realm_id=$2`, accountId, project);
    ctx.sql.run(
      `UPDATE account_tenant_index SET status='revoked',tenant_membership_revision=max(tenant_membership_revision,$3),updated_at=${NOW_MS}
       WHERE account_id=$1 AND tenant_id=$2`,
      accountId,
      lookup.tenantId,
      written.revision,
    );
    const result = { updated: true };
    completeIdempotency(ctx, project, subject, key, result);
    return result;
  });
}

const invalidInvitation = () => invalid("invitation is invalid, expired, revoked, or already accepted");

/** control.invitations.accept (control.rs:1556). */
async function acceptInvitation(ctx: Ctx, connection: ControlConnection, args: unknown, key: string): Promise<unknown> {
  const identity = account(connection);
  const object = exactObject(args, ["token", "invitationToken"]);
  const token = nonEmpty(optionalString(object, "token")) ?? nonEmpty(optionalString(object, "invitationToken"));
  if (!token) throw invalid("token is required");
  const project = connection.projectId;
  type Claimed = { done: { tenantId: string } } | { invitation: Row; tenantId: string };
  const claimed: Claimed = ctx.sql.tx((): Claimed => {
    const row = ctx.sql.get(
      `SELECT id,tenant_id,email,role,permissions,team_ids,invited_by,allowed_auth_providers,application_payload,expires_at,
              revoked_at,accepted_at,accepted_account_id,accepted_idempotency_key,handoff_state
       FROM gonvex_auth_membership_invitations WHERE project_id=$1 AND token_hash=$2`,
      project,
      sha256Hex(token),
    );
    if (!row) throw invalidInvitation();
    const tenantId = str(row.tenant_id);
    if (optNum(row.revoked_at) !== null || num(row.expires_at) <= Date.now()) throw invalidInvitation();
    const acceptedAccount = row.accepted_account_id === null ? null : str(row.accepted_account_id);
    const acceptedKey = row.accepted_idempotency_key === null ? null : str(row.accepted_idempotency_key);
    if (optNum(row.accepted_at) !== null) {
      if (acceptedAccount !== identity.account.id || acceptedKey !== key) throw invalidInvitation();
      return { done: { tenantId } };
    }
    if (normalizeEmail(str(row.email)) !== normalizeEmail(identity.account.email)) throw invalid("invitation does not belong to this account");
    const allowed = json(row.allowed_auth_providers);
    if (!Array.isArray(allowed)) throw invalid("invitation provider policy is invalid");
    if (allowed.length > 0) {
      const linked = new Set(ctx.sql.all("SELECT DISTINCT provider FROM account_identities WHERE account_id=$1", identity.account.id).map((r) => str(r.provider)));
      if (!allowed.some((provider) => typeof provider === "string" && linked.has(provider))) {
        throw invalid("invitation requires a linked authentication provider");
      }
    }
    const permissions = json(row.permissions);
    if (!permissions || typeof permissions !== "object" || Array.isArray(permissions) || !Array.isArray(json(row.team_ids))) {
      throw invalid("invitation application data is invalid");
    }
    if (str(row.handoff_state) === "claimed" && (acceptedAccount !== identity.account.id || acceptedKey !== key)) {
      throw invalid("invitation acceptance is already in progress");
    }
    const affected = ctx.sql.affected(
      `UPDATE gonvex_auth_membership_invitations
       SET handoff_state='claimed',handoff_command_id=$3,accepted_account_id=$4,accepted_idempotency_key=$5,updated_at=${NOW_MS}
       WHERE project_id=$1 AND id=$2 AND accepted_at IS NULL AND revoked_at IS NULL
         AND (handoff_state='pending' OR (handoff_state='claimed' AND accepted_account_id=$4 AND accepted_idempotency_key=$5))`,
      project,
      str(row.id),
      `invitation:${str(row.id)}`,
      identity.account.id,
      key,
    );
    if (affected !== 1) throw invalid("invitation acceptance is already in progress");
    return { invitation: row, tenantId };
  });
  if ("done" in claimed) {
    const session = await tenantSessionForAccount(ctx, project, claimed.done.tenantId, identity.account.id);
    return { tenantId: claimed.done.tenantId, memberId: session.member.id };
  }
  const row = claimed.invitation;
  const invitationId = str(row.id);
  const tenantId = claimed.tenantId;
  const route = resolveTenant(ctx, project, tenantId);
  const inviterAccountId = str(row.invited_by);
  let inviterMemberId: string | null = null;
  try {
    inviterMemberId = (await tenantSessionForAccount(ctx, project, tenantId, inviterAccountId)).member.id;
  } catch {
    inviterMemberId = null;
  }
  const module = projectModule(ctx, project);
  const reducerPath = module?.invitationAcceptanceReducer ?? "";
  const outcome = await tenantCall(ctx, route, (port) =>
    port.acceptInvitation({
      commandId: `invitation:${invitationId}`,
      invitationId,
      accountId: identity.account.id,
      accountEmail: identity.account.email,
      accountName: identity.account.name,
      accountAvatarUrl: identity.account.avatarUrl,
      idempotencyKey: key,
      role: str(row.role),
      permissions: json(row.permissions),
      teamIds: json(row.team_ids),
      payload: json(row.application_payload),
      inviterAccountId,
      inviterMemberId,
      moduleAvailable: module !== null,
      reducerPath,
      reducerDefinition: reducerPath && module ? (module.functions[reducerPath] ?? null) : null,
      artifactHash: module?.artifactHash ?? "",
    }),
  );
  if (!outcome.ok) {
    if (outcome.release) {
      ctx.sql.tx(() =>
        ctx.sql.run(
          `UPDATE gonvex_auth_membership_invitations
           SET handoff_state='pending',handoff_command_id='',accepted_account_id=NULL,accepted_idempotency_key=NULL,updated_at=${NOW_MS}
           WHERE project_id=$1 AND id=$2 AND accepted_at IS NULL AND handoff_state='claimed' AND accepted_account_id=$3 AND accepted_idempotency_key=$4`,
          project,
          invitationId,
          identity.account.id,
          key,
        ),
      );
    }
    throw new ControlError(outcome.error.startsWith("invalid arguments: ") ? outcome.error : `invalid arguments: ${outcome.error}`, "rejected");
  }
  const memberId = outcome.memberId;
  return ctx.sql.tx(() => {
    const affected = ctx.sql.affected(
      `UPDATE gonvex_auth_membership_invitations
       SET accepted_at=${NOW_MS},completed_at=${NOW_MS},handoff_state='completed',accepted_account_id=$3,accepted_idempotency_key=$4,updated_at=${NOW_MS}
       WHERE project_id=$1 AND id=$2 AND accepted_at IS NULL AND revoked_at IS NULL
         AND handoff_state='claimed' AND accepted_account_id=$3 AND accepted_idempotency_key=$4`,
      project,
      invitationId,
      identity.account.id,
      key,
    );
    if (affected !== 1) throw invalid("invitation was already accepted");
    ctx.sql.run(
      `INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
       VALUES($1,$2,$3,'active',1,${NOW_MS})
       ON CONFLICT(account_id,tenant_id) DO UPDATE SET member_id=EXCLUDED.member_id,
         status='active',tenant_membership_revision=max(account_tenant_index.tenant_membership_revision,EXCLUDED.tenant_membership_revision),updated_at=${NOW_MS}`,
      identity.account.id,
      tenantId,
      memberId,
    );
    return { tenantId, memberId };
  });
}

/** Membership projection (membership_projector.rs:119-185): revision-guarded upsert. */
export function projectMemberships(ctx: Ctx, tenantId: string, rows: Array<{ accountId: string; memberId: string; status: string; membershipRevision: number }>): number {
  return ctx.sql.tx(() => {
    for (const row of rows) {
      ctx.sql.run(
        `INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
         VALUES($1,$2,$3,$4,$5,${NOW_MS})
         ON CONFLICT(account_id,tenant_id) DO UPDATE SET
           member_id=excluded.member_id,status=excluded.status,
           tenant_membership_revision=excluded.tenant_membership_revision,updated_at=${NOW_MS}
         WHERE excluded.tenant_membership_revision >= account_tenant_index.tenant_membership_revision`,
        row.accountId,
        tenantId,
        row.memberId,
        row.status,
        row.membershipRevision,
      );
    }
    return rows.length;
  });
}

export { hashPassword, isLiveControlQuery };
