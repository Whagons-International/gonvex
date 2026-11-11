/**
 * Sessions, refresh rotation and external account resolution
 * (control.rs:3680-4095; contract C.3.1, C.3.4, C.3.6, C.4 item 1).
 */
import type { Ctx } from "./context";
import { FANOUT, mapLimit } from "./context";
import { admitMember } from "./directory";
import { invalid } from "./errors";
import { NOW_MS, json, num, optNum, str } from "./sql";
import { randomId, secureToken, sha256Hex } from "./util";

export const ACCESS_TOKEN_MS = 15 * 60 * 1000;
export const REFRESH_TOKEN_MS = 30 * 24 * 60 * 60 * 1000;
const REUSE_GRACE_MS = 5000;

export interface SessionGrant {
  accessToken: string;
  accessExpiresAt: number;
  refreshToken: string;
  refreshExpiresAt: number;
}

export interface AuthAccount {
  id: string;
  email: string;
  emailVerified: boolean;
  name: string;
  picture: string;
  provider: string;
}

export interface VerifiedExternalIdentity {
  provider: string;
  issuer: string;
  subject: string;
  email: string;
  emailVerified: boolean;
  name: string;
  picture: string;
  signInProvider: string;
  authTime: number;
}

/** `issue_session` (control.rs:3860). Must run inside a writer transaction. */
export function issueSession(ctx: Ctx, projectId: string, accountId: string, familyId: string, refreshExpiresAt: number): SessionGrant {
  const accessToken = secureToken("session");
  const refreshToken = secureToken("refresh");
  const accessExpiresAt = Date.now() + ACCESS_TOKEN_MS;
  ctx.sql.run(
    "INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,family_id,expires_at) VALUES($1,$2,$3,$4,$5)",
    sha256Hex(accessToken),
    projectId,
    accountId,
    familyId,
    accessExpiresAt,
  );
  ctx.sql.run(
    "INSERT INTO gonvex_auth_refresh_tokens(token_hash,family_id,project_id,account_id,expires_at) VALUES($1,$2,$3,$4,$5)",
    sha256Hex(refreshToken),
    familyId,
    projectId,
    accountId,
    refreshExpiresAt,
  );
  return { accessToken, accessExpiresAt, refreshToken, refreshExpiresAt };
}

export const newFamily = (): string => randomId("family");

/** `load_auth_account` (control.rs:3829). */
export function loadAuthAccount(ctx: Ctx, projectId: string, accountId: string): AuthAccount {
  const row = ctx.sql.get(
    `SELECT account.id,account.email,account.name,account.avatar_url,
            COALESCE(identity.verified_email,FALSE) AS email_verified,
            COALESCE(identity.provider,'') AS provider
     FROM accounts account
     LEFT JOIN account_identities identity ON identity.rowid=(
       SELECT rowid FROM account_identities
       WHERE account_id=account.id ORDER BY updated_at DESC LIMIT 1
     )
     WHERE account.id=$1 AND account.auth_realm_id=$2 AND account.disabled_at IS NULL`,
    accountId,
    projectId,
  );
  if (!row) throw invalid("account is unavailable");
  return {
    id: str(row.id),
    email: str(row.email),
    emailVerified: num(row.email_verified) !== 0,
    name: str(row.name),
    picture: str(row.avatar_url),
    provider: str(row.provider),
  };
}

export function revokeFamily(ctx: Ctx, familyId: string): void {
  for (const table of ["gonvex_auth_sessions", "gonvex_auth_refresh_tokens"]) {
    ctx.sql.run(`UPDATE ${table} SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE family_id=$1`, familyId);
  }
}

export type RefreshResult = { kind: "refreshed"; grant: SessionGrant; account: AuthAccount } | { kind: "reuseRevoked" };

/** `refresh_session` (control.rs:3938). Must run inside a writer transaction. */
export function refreshSession(ctx: Ctx, projectId: string, refreshToken: string): RefreshResult {
  const hash = sha256Hex(refreshToken);
  const row = ctx.sql.get(
    "SELECT project_id,account_id,family_id,expires_at,used_at,revoked_at FROM gonvex_auth_refresh_tokens WHERE token_hash=$1",
    hash,
  );
  if (!row) throw invalid("invalid or expired refresh token");
  const familyId = str(row.family_id);
  const expiresAt = num(row.expires_at);
  const usedAt = optNum(row.used_at);
  if (str(row.project_id) !== projectId || optNum(row.revoked_at) !== null || expiresAt <= Date.now()) {
    throw invalid("invalid or expired refresh token");
  }
  if (usedAt !== null) {
    if (Date.now() - usedAt > REUSE_GRACE_MS) {
      revokeFamily(ctx, familyId);
      return { kind: "reuseRevoked" };
    }
    throw invalid("refresh token was already rotated; use the latest session");
  }
  ctx.sql.run(`UPDATE gonvex_auth_refresh_tokens SET used_at=${NOW_MS} WHERE token_hash=$1 AND used_at IS NULL`, hash);
  const account = loadAuthAccount(ctx, projectId, str(row.account_id));
  const grant = issueSession(ctx, projectId, str(row.account_id), familyId, expiresAt);
  return { kind: "refreshed", grant, account };
}

/** `session_result` (control.rs:4076). */
export function sessionResult(grant: SessionGrant, account: AuthAccount, requestedTenant: string): Record<string, unknown> {
  return {
    accessToken: grant.accessToken,
    tokenType: "Bearer",
    expiresIn: 900,
    expiresAt: grant.accessExpiresAt,
    refreshToken: grant.refreshToken,
    refreshExpiresAt: grant.refreshExpiresAt,
    account: {
      id: account.id,
      email: account.email,
      emailVerified: account.emailVerified,
      name: account.name,
      picture: account.picture,
      provider: account.provider,
    },
    tenants: [],
    activeTenantId: requestedTenant,
  };
}

/**
 * `session_result_from_directory` (control.rs:4001). The directory is only a
 * routing projection: every candidate is re-admitted against its tenant
 * object, at most 32 RPCs in flight (0012 R15), order kept.
 */
export async function sessionResultFromDirectory(ctx: Ctx, projectId: string, result: Record<string, unknown>): Promise<Record<string, unknown>> {
  const account = (result.account ?? {}) as Record<string, unknown>;
  const accountId = typeof account.id === "string" ? account.id : "";
  const requested = typeof result.activeTenantId === "string" ? result.activeTenantId : "";
  const rows = ctx.sql.all(
    `SELECT tenant.tenant_id,tenant.name,tenant.domain,tenant.timezone,
            tenant.description,tenant.profile,directory.member_id
     FROM account_tenant_index directory
     JOIN gonvex_runtime_tenants tenant ON tenant.tenant_id=directory.tenant_id
     WHERE tenant.project_id=$1 AND directory.account_id=$2
       AND directory.status='active' AND tenant.deleted_at IS NULL
       AND tenant.status NOT IN ('deleted','disabled')
     ORDER BY lower(tenant.name),tenant.tenant_id`,
    projectId,
    accountId,
  );
  const admitted = await mapLimit(rows, FANOUT, async (row) => {
    try {
      return (await admitMember(ctx, projectId, str(row.tenant_id), accountId)).member;
    } catch {
      return null;
    }
  });
  const tenants: Array<Record<string, unknown>> = [];
  rows.forEach((row, index) => {
    const member = admitted[index];
    if (!member) return;
    tenants.push({
      id: str(row.tenant_id),
      name: str(row.name),
      role: member.role,
      permissions: member.permissions,
      memberId: member.id,
      domain: str(row.domain),
      timezone: str(row.timezone),
      description: str(row.description),
      profile: json(row.profile),
    });
  });
  const match = tenants.find((tenant) => tenant.id === requested || String(tenant.domain).toLowerCase() === requested.toLowerCase());
  return { ...result, tenants, activeTenantId: String((match ?? tenants[0])?.id ?? "") };
}

/** `require_external_signup` (control.rs:3898). */
function requireExternalSignup(ctx: Ctx, projectId: string, identity: VerifiedExternalIdentity): void {
  const mode = ctx.sql.value<string>("SELECT signup_mode FROM gonvex_auth_providers WHERE project_id=$1 AND provider=$2 AND enabled=TRUE", projectId, identity.provider);
  if (mode === undefined) throw new Error("no rows returned by a query that expected to return at least one row");
  if (mode !== "inviteOnly") return;
  if (!identity.emailVerified || !identity.email) throw invalid("a verified invited email is required");
  const invited = num(
    ctx.sql.value(
      `SELECT EXISTS(
         SELECT 1 FROM gonvex_auth_membership_invitations
         WHERE project_id=$1 AND lower(email)=lower($2)
           AND revoked_at IS NULL AND accepted_at IS NULL AND expires_at>${NOW_MS}
       )`,
      projectId,
      identity.email,
    ),
  );
  if (!invited) throw invalid("account signup requires an active invitation");
}

function insertAccount(ctx: Ctx, projectId: string, identity: VerifiedExternalIdentity): string {
  const accountId = randomId("acct");
  ctx.sql.run(
    `INSERT INTO accounts(id,auth_realm_id,email,name,avatar_url,updated_at) VALUES($1,$2,$3,$4,$5,${NOW_MS})`,
    accountId,
    projectId,
    identity.email,
    identity.name,
    identity.picture,
  );
  return accountId;
}

/** `resolve_external_account` (control.rs:3680). Must run inside a writer transaction. */
export function resolveExternalAccount(ctx: Ctx, projectId: string, identity: VerifiedExternalIdentity): AuthAccount {
  const existing = ctx.sql.value<string>(
    "SELECT account_id FROM account_identities WHERE project_id=$1 AND provider=$2 AND issuer=$3 AND subject=$4",
    projectId,
    identity.provider,
    identity.issuer,
    identity.subject,
  );
  if (existing !== undefined) {
    ctx.sql.run(
      `UPDATE account_identities SET email=$5,verified_email=$6,sign_in_provider=$7,updated_at=${NOW_MS}
       WHERE project_id=$1 AND provider=$2 AND issuer=$3 AND subject=$4`,
      projectId,
      identity.provider,
      identity.issuer,
      identity.subject,
      identity.email,
      identity.emailVerified,
      identity.signInProvider,
    );
    ctx.sql.run(
      `UPDATE accounts SET name=COALESCE(NULLIF($2,''),name),avatar_url=COALESCE(NULLIF($3,''),avatar_url),updated_at=${NOW_MS} WHERE id=$1`,
      existing,
      identity.name,
      identity.picture,
    );
    return loadAuthAccount(ctx, projectId, existing);
  }
  let resolution = "new";
  let accountId: string;
  if (identity.emailVerified && identity.email) {
    const matches = ctx.sql.all(
      `SELECT DISTINCT account_id FROM account_identities
       WHERE project_id=$1 AND verified_email=TRUE AND lower(email)=lower($2)
       ORDER BY account_id LIMIT 2`,
      projectId,
      identity.email,
    );
    if (matches.length > 1) throw invalid("verified email matches more than one account");
    if (matches[0]) {
      resolution = "verified_email";
      accountId = str(matches[0].account_id);
    } else {
      requireExternalSignup(ctx, projectId, identity);
      if (!identity.email) throw invalid("external identity email is required for first login");
      accountId = insertAccount(ctx, projectId, identity);
    }
  } else {
    requireExternalSignup(ctx, projectId, identity);
    accountId = insertAccount(ctx, projectId, identity);
  }
  if (!identity.email && resolution === "new") throw invalid("external identity email is required for first login");
  const inserted = ctx.sql.affected(
    `INSERT INTO account_identities
     (project_id,account_id,provider,issuer,subject,email,verified_email,sign_in_provider,updated_at)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,${NOW_MS})
     ON CONFLICT(project_id,provider,issuer,subject) DO NOTHING`,
    projectId,
    accountId,
    identity.provider,
    identity.issuer,
    identity.subject,
    identity.email,
    identity.emailVerified,
    identity.signInProvider,
  );
  const resolved =
    inserted === 1
      ? accountId
      : str(
          ctx.sql.value(
            "SELECT account_id FROM account_identities WHERE project_id=$1 AND provider=$2 AND issuer=$3 AND subject=$4",
            projectId,
            identity.provider,
            identity.issuer,
            identity.subject,
          ),
        );
  ctx.sql.run(
    "INSERT INTO gonvex_auth_identity_events(project_id,account_id,provider,issuer,subject,resolution) VALUES($1,$2,$3,$4,$5,$6)",
    projectId,
    resolved,
    identity.provider,
    identity.issuer,
    identity.subject,
    resolution,
  );
  return loadAuthAccount(ctx, projectId, resolved);
}
