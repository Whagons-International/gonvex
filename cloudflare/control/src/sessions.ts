/**
 * Sessions, refresh rotation and external account resolution
 * (control.rs:3680-4095; contract C.3.1, C.3.4, C.3.6, C.4 item 1).
 */
import type { Ctx } from "./context";
import { FANOUT, mapLimit } from "./context";
import { admitMember } from "./directory";
import type { Member } from "./types";
import { invalid } from "./errors";
import { NOW_MS, json, num, optNum, str, type Row } from "./sql";
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
/**
 * X17f: last admissions per (project, tenant, account). An entry is used only while the directory row it was admitted
 * under is unchanged: same member id, membership revision and projection time (`updated_at`). The revision alone is
 * not complete (#530 review): application Reducers change `role`/`permissions`/`status` without bumping it. The members
 * trigger queues every update in the tenant's projection outbox, whose push only acks rows no write touched while it
 * was in flight (kernel/assembly/tenant.ts `projectMemberships`, #530 re-review), and every projection rewrites
 * `updated_at`. Entries also expire after `ADMISSION_CACHE_TTL_MS`.
 *
 * `at` is when the admission RPC started (#540 review), not when the entry was written: the tenant's read happened
 * at or after it, so every age bound (the TTL here, `AUTH_REUSE_MS` in `recentAdmission`) bounds the tenant read.
 */
interface CachedAdmission { readonly member: Member; readonly memberId: string; readonly revision: number; readonly projectedAt: number; readonly at: number; readonly admissionRevision?: number }
const admissionCache = new Map<string, CachedAdmission>();
const ADMISSION_CACHE_MAX = 4096;
export const ADMISSION_CACHE_TTL_MS = 30 * 1000;
/** Test seam. */
export function clearAdmissionCache(): void { admissionCache.clear(); }

/**
 * `session_result_from_directory` (control.rs:4001). The directory is only a
 * routing projection: every candidate is re-admitted against its tenant
 * object, at most 32 RPCs in flight (0012 R15), order kept.
 *
 * X17f: the tenant the session will use (`activeTenantId`: the requested one, else the first candidate) is always
 * admitted live, falling through to the next candidate when that admission fails, as before. The other entries of
 * the list may come from `admissionCache`, so an idle tenant is not woken for the sign-in's tenant list (a cold tenant
 * cost 0.3-1.8 s of the exchange on dev). Residual staleness of those entries: a membership change the tenant has
 * not projected to Control yet (its outbox push from the tenant alarm, retried every second). These cached entries
 * never grant entry to a tenant: `auth` admits the tenant it enters live, except that it may reuse an admission
 * this sign-in (or an earlier one) made live at most `AUTH_REUSE_MS` before, measured from the admission RPC's start,
 * under an unchanged directory row and with the tenant's admission revision (X17k, `recentAdmission`). A caller that
 * has just seen the tenant refuse the member passes `liveOnly` and is always admitted live (#540 x #539 review).
 */
/** The account's active directory rows, in the session result's order. */
function directoryRows(ctx: Ctx, projectId: string, accountId: string): Row[] {
  return ctx.sql.all(
    `SELECT tenant.tenant_id,tenant.name,tenant.domain,tenant.timezone,
            tenant.description,tenant.profile,directory.member_id,directory.tenant_membership_revision,
            directory.updated_at AS projected_at
     FROM account_tenant_index directory
     JOIN gonvex_runtime_tenants tenant ON tenant.tenant_id=directory.tenant_id
     WHERE tenant.project_id=$1 AND directory.account_id=$2
       AND directory.status='active' AND tenant.deleted_at IS NULL
       AND tenant.status NOT IN ('deleted','disabled')
     ORDER BY lower(tenant.name),tenant.tenant_id`,
    projectId,
    accountId,
  );
}

/** The cache entry for `row`, if it is still valid for the row's member id, revision and projection time. */
function cachedAdmission(projectId: string, accountId: string, row: Row): Member | undefined {
  const cached = admissionCache.get(`${projectId}\u0000${str(row.tenant_id)}\u0000${accountId}`);
  if (!cached || Date.now() - cached.at > ADMISSION_CACHE_TTL_MS) return undefined;
  const revision = Number(row.tenant_membership_revision ?? -1), projectedAt = Number(row.projected_at ?? -1);
  if (revision < 0 || projectedAt < 0 || cached.revision !== revision || cached.projectedAt !== projectedAt) return undefined;
  if (cached.memberId !== str(row.member_id) || cached.member.id !== str(row.member_id) || cached.member.status !== "active") return undefined;
  return cached.member;
}

/**
 * X17k: the admission this Control made live for (tenant, account) at most `maxAgeMs` ago, if the tenant's directory
 * row is unchanged since (member id, membership revision, projection time): a tenant auth right after the sign-in
 * reuses the sign-in's admission instead of a second tenant round trip. Undefined otherwise (admit live). The age is
 * measured from the admission RPC's start (#540 review), so the tenant read it reuses is at most `maxAgeMs` old.
 */
export const AUTH_REUSE_MS = 5_000;
export function recentAdmission(ctx: Ctx, projectId: string, tenantId: string, accountId: string, maxAgeMs = AUTH_REUSE_MS): { member: Member; admissionRevision: number } | undefined {
  const cached = admissionCache.get(`${projectId}\u0000${tenantId}\u0000${accountId}`);
  if (!cached || cached.admissionRevision === undefined || Date.now() - cached.at > maxAgeMs) return undefined;
  const row = ctx.sql.get(
    `SELECT directory.tenant_id,directory.member_id,directory.tenant_membership_revision,directory.updated_at AS projected_at
     FROM account_tenant_index directory
     JOIN gonvex_runtime_tenants tenant ON tenant.tenant_id=directory.tenant_id
     WHERE tenant.project_id=$1 AND directory.account_id=$2 AND directory.tenant_id=$3
       AND directory.status='active' AND tenant.deleted_at IS NULL AND tenant.status NOT IN ('deleted','disabled')`,
    projectId, accountId, tenantId,
  );
  if (!row || !cachedAdmission(projectId, accountId, row)) return undefined;
  return { member: cached.member, admissionRevision: cached.admissionRevision };
}

/** #540 re-review: drop the cached admission for (project, tenant, account), e.g. after a live admission refused it. */
export function forgetAdmission(projectId: string, tenantId: string, accountId: string): void {
  admissionCache.delete(`${projectId}\u0000${tenantId}\u0000${accountId}`);
}
/** Whether the cached admission for (project, tenant, account), if any, has exactly `member` (id, role, permissions...). */
export function sameAdmittedMember(projectId: string, tenantId: string, accountId: string, member: Member): boolean {
  const cached = admissionCache.get(`${projectId}\u0000${tenantId}\u0000${accountId}`);
  return !cached || JSON.stringify(cached.member) === JSON.stringify(member);
}

/** Candidates for the active tenant, in the order the session chooses them: the requested one (id or domain) first. */
function preferredOrder(rows: Row[], requested: string): Row[] {
  const matches = (row: Row) => str(row.tenant_id) === requested || str(row.domain).toLowerCase() === requested.toLowerCase();
  return [...rows.filter(matches), ...rows.filter((row) => !matches(row))];
}

/** The directory row an admission was made under: its cache key parts. */
const rowStamp = (row: Row) => `${str(row.member_id)}\u0000${Number(row.tenant_membership_revision ?? -1)}\u0000${Number(row.projected_at ?? -1)}`;

/**
 * X17h: tenant admissions started before the session exists, while the external provider's account check
 * (`accounts:lookup`) is in flight. Only the admissions the session result would make live are started: the first
 * active-tenant candidate and the rows the admission cache cannot serve, at most `FANOUT` (#537 review). Each is the
 * same read-only tenant admission; it is used only for this account, only after the account check passed and the
 * session committed, and only if the tenant's directory row is unchanged since it started (member id, membership
 * revision, projection time), so it is cached under the key it was read for. A failed one counts as failed.
 * A disabled or revoked account whose ID token is still valid can therefore wake at most this bounded set, read-only
 * (accepted, #537).
 */
interface EarlyAdmission { readonly stamp: string; readonly startedAt: number; readonly member: Promise<Member | null>; readonly admissionRevision?: Promise<number | undefined> }
/**
 * #537 re-review: one FANOUT bound per sign-in. Prefetched admissions hold a slot until they settle; every admission
 * RPC the post-commit path starts takes one, so both phases together stay within FANOUT in flight (0012 R15).
 */
class AdmissionSlots {
  private used = 0;
  private readonly waiters: Array<() => void> = [];
  constructor(private readonly max: number) {}
  take(): void { this.used++; }
  async acquire(): Promise<void> {
    if (this.used < this.max) { this.used++; return; }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
  }
  release(): void { const next = this.waiters.shift(); if (next) next(); else this.used--; }
}
export interface PrefetchedAdmissions { readonly accountId: string; readonly admissions: ReadonlyMap<string, EarlyAdmission>; readonly slots: AdmissionSlots }
export function prefetchAdmissions(ctx: Ctx, projectId: string, accountId: string, requested = ""): PrefetchedAdmissions {
  const rows = directoryRows(ctx, projectId, accountId);
  const first = preferredOrder(rows, requested)[0];
  const wanted = rows.filter((row) => row === first || !cachedAdmission(projectId, accountId, row));
  if (first && wanted[0] !== first) { wanted.splice(wanted.indexOf(first), 1); wanted.unshift(first); }
  const admissions = new Map<string, EarlyAdmission>();
  const slots = new AdmissionSlots(FANOUT);
  for (const row of wanted.slice(0, FANOUT)) {
    const tenantId = str(row.tenant_id);
    slots.take();
    const startedAt = Date.now();
    const admitting = admitMember(ctx, projectId, tenantId, accountId);
    const member = admitting.then((admitted) => admitted.member, () => null);
    const admissionRevision = admitting.then((admitted) => admitted.revision, () => undefined);
    void member.finally(() => slots.release());
    admissions.set(tenantId, { stamp: rowStamp(row), startedAt, member, admissionRevision });
  }
  return { accountId, admissions, slots };
}

export async function sessionResultFromDirectory(ctx: Ctx, projectId: string, result: Record<string, unknown>, prefetched?: PrefetchedAdmissions): Promise<Record<string, unknown>> {
  const account = (result.account ?? {}) as Record<string, unknown>;
  const accountId = typeof account.id === "string" ? account.id : "";
  const requested = typeof result.activeTenantId === "string" ? result.activeTenantId : "";
  const rows = directoryRows(ctx, projectId, accountId);
  const early = prefetched && accountId && prefetched.accountId === accountId ? prefetched.admissions : undefined;
  const slots = prefetched?.slots ?? new AdmissionSlots(FANOUT);
  const admitNow = async (tenantId: string): Promise<{ member: Member; revision: number; startedAt: number }> => {
    await slots.acquire();
    try {
      const startedAt = Date.now();
      const admitted = await admitMember(ctx, projectId, tenantId, accountId);
      return { member: admitted.member, revision: admitted.revision, startedAt };
    } finally { slots.release(); }
  };
  const keyOf = (row: Row) => `${projectId}\u0000${str(row.tenant_id)}\u0000${accountId}`;
  const admitLive = async (row: Row): Promise<Member | null> => {
    const key = keyOf(row);
    try {
      // An admission started during the account check, if the directory row it was read for is still this one.
      const started = early?.get(str(row.tenant_id));
      let member: Member | null, admissionRevision: number | undefined, startedAt: number;
      if (started && started.stamp === rowStamp(row)) { member = await started.member; admissionRevision = await started.admissionRevision; startedAt = started.startedAt; }
      else ({ member, revision: admissionRevision, startedAt } = await admitNow(str(row.tenant_id)));
      if (!member) throw new Error("admission failed");
      admissionCache.delete(key);
      admissionCache.set(key, { member, memberId: str(row.member_id), revision: Number(row.tenant_membership_revision ?? -1), projectedAt: Number(row.projected_at ?? -1), at: startedAt,
        ...(admissionRevision !== undefined ? { admissionRevision } : {}) });
      if (admissionCache.size > ADMISSION_CACHE_MAX) admissionCache.delete(admissionCache.keys().next().value!);
      return member;
    } catch {
      admissionCache.delete(key);
      return null;
    }
  };
  const fromCache = (row: Row): Member | undefined => cachedAdmission(projectId, accountId, row);
  const preferred = preferredOrder(rows, requested);
  // The first candidate is admitted live, alongside the rest of the list (cached or live).
  const live = new Set<Row>();
  const admitted = new Map<Row, Member | null>();
  const results = await mapLimit(rows, FANOUT, async (row) => {
    if (row === preferred[0]) { live.add(row); return admitLive(row); }
    const cached = fromCache(row);
    if (cached) return cached;
    live.add(row);
    return admitLive(row);
  });
  rows.forEach((row, index) => admitted.set(row, results[index]!));
  let active: Row | undefined;
  for (const row of preferred) {
    // Fall through, as before; a candidate served from the cache is admitted live before it becomes the active tenant.
    if (!live.has(row)) { live.add(row); admitted.set(row, await admitLive(row)); }
    if (admitted.get(row)) { active = row; break; }
  }
  const tenants: Array<Record<string, unknown>> = [];
  for (const row of rows) {
    const member = admitted.get(row);
    if (!member) continue;
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
  }
  return { ...result, tenants, activeTenantId: active ? str(active.tenant_id) : "" };
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
