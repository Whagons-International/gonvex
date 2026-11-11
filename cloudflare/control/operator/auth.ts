/**
 * Operator authentication and authorization (CONTRACT D.3), as
 * rust/crates/runtime/src/operations.rs `authenticate`, `authorize`,
 * `can_access_project`, `can_manage_project`, `authorize_project_resource`
 * and `project_key_matches` do it, against the control object's own SQLite.
 */
import { createHmac } from "node:crypto";
import type { Ctx } from "../src/context";
import { loadSessionIdentity } from "../src/directory";
import { Sql, bool, str } from "../src/sql";
import { base64url, constantTimeEqual, fromBase64url, sha256Hex } from "../src/util";
import type { OperatorConfig } from "./config";
import { HttpError, displayName, normalizeEmail } from "./http";

export type Credential = "Session" | "NativeSession" | "PersonalAccessToken" | "AdminKey" | "Local";

export interface OperatorActor {
  email: string;
  name: string;
  role: string;
  credential: Credential;
  permissions: string[];
}

export const credentialName: Record<Credential, string> = {
  Session: "session",
  NativeSession: "nativeSession",
  PersonalAccessToken: "personalAccessToken",
  AdminKey: "adminKey",
  Local: "local",
};

/** `OperatorActor::has`: only personal access tokens are restricted. */
export function has(actor: OperatorActor, permission: string): boolean {
  return (
    actor.credential !== "PersonalAccessToken" ||
    actor.permissions.some((granted) => granted === "*" || granted === permission || (granted.endsWith("*") && permission.startsWith(granted.slice(0, -1))))
  );
}

/** `OperatorActor::global_admin`: a session-based admin is not a global admin. */
export function globalAdmin(actor: OperatorActor): boolean {
  return (
    actor.role === "admin" &&
    (actor.credential === "AdminKey" ||
      actor.credential === "Local" ||
      (actor.credential === "PersonalAccessToken" && actor.permissions.some((value) => value === "*" || value === "admin:projects")))
  );
}

/** operations.rs `bearer`: the `Authorization` value with an optional `Bearer ` prefix, trimmed. */
export function bearer(headers: Headers): string {
  const raw = (headers.get("authorization") ?? "").trim();
  return raw.startsWith("Bearer ") ? raw.slice("Bearer ".length) : raw;
}

export const timingEqual = (left: string, right: string): boolean => constantTimeEqual(left, right);

export class OperatorAuth {
  constructor(private readonly ctx: Ctx, private readonly config: OperatorConfig) {}

  private get sql(): Sql {
    return this.ctx.sql;
  }

  /** `dashboard_secret`: the session secret, else the admin key. */
  secret(): string | undefined {
    return this.config.controlSecret ?? this.config.adminKey;
  }

  /** `dashboard_auth_optional`, plus the kernel's explicit local opt-in (CONTRACT D.3: never in deployed Workers). */
  authOptional(): boolean {
    return (
      this.config.localAllowed &&
      !this.config.requireAuth &&
      this.config.dashboardAuthProjectId === undefined &&
      this.secret() === undefined &&
      this.config.dashboardAccount === undefined
    );
  }

  adminKeyMatches(headers: Headers): boolean {
    const key = this.config.adminKey;
    return key !== undefined && timingEqual(bearer(headers), key);
  }

  async authenticate(headers: Headers): Promise<OperatorActor> {
    const token = bearer(headers);
    const session = this.verifySignedSession(token);
    if (session) return session;
    if (token.startsWith("gvx_session_") && this.config.dashboardAuthProjectId !== undefined) {
      try {
        const identity = loadSessionIdentity(this.ctx, token, this.config.dashboardAuthProjectId);
        if (identity.account.emailVerified) {
          const actor = this.actorForEmail(identity.account.email, "NativeSession");
          if (actor) return actor;
        }
      } catch {
        // An invalid session falls through to the next credential kind.
      }
    }
    if (token.startsWith("gvx_pat_")) {
      const actor = this.verifyPersonalToken(token);
      if (actor) return actor;
    }
    if (this.config.adminKey !== undefined && timingEqual(token, this.config.adminKey)) {
      return { email: "admin@gonvex.local", name: "Gonvex Admin", role: "admin", credential: "AdminKey", permissions: ["*"] };
    }
    if (this.authOptional()) {
      return { email: "local@gonvex.dev", name: "Local Developer", role: "admin", credential: "Local", permissions: ["*"] };
    }
    throw new HttpError(401, "dashboard sign-in or personal access token is required");
  }

  async authorize(headers: Headers, permission: string): Promise<OperatorActor> {
    const actor = await this.authenticate(headers);
    if (permission !== "" && !has(actor, permission)) {
      throw new HttpError(403, "personal access token does not grant the required permission", { permission });
    }
    return actor;
  }

  canAccessProject(actor: OperatorActor, project: string): boolean {
    if (this.authOptional() || globalAdmin(actor)) return true;
    return bool(
      this.sql.value(
        `SELECT EXISTS(
           SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND (lower(owner_email)=lower($2) OR (owner_email='' AND $3='admin'))
           UNION ALL SELECT 1 FROM gonvex_project_members WHERE project_id=$1 AND lower(email)=lower($2)
         ) AS allowed`,
        project,
        actor.email,
        actor.role,
      ),
    );
  }

  canManageProject(actor: OperatorActor, project: string): boolean {
    if (this.authOptional() || globalAdmin(actor)) return true;
    return bool(
      this.sql.value(
        `SELECT EXISTS(
           SELECT 1 FROM gonvex_runtime_projects WHERE id=$1 AND lower(owner_email)=lower($2)
           UNION ALL SELECT 1 FROM gonvex_project_members
           WHERE project_id=$1 AND lower(email)=lower($2) AND role IN('owner','admin')
         ) AS allowed`,
        project,
        actor.email,
      ),
    );
  }

  /** `project_accepts_sync_key` (store/src/lib.rs:824-845). */
  projectAcceptsSyncKey(project: string, key: string, fallback: string | undefined): boolean {
    const stored = this.sql.value<string>("SELECT project_key FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN ('deleted','disabled')", project);
    const projectKey = stored === undefined ? undefined : str(stored);
    if (projectKey !== undefined && projectKey !== "" && timingEqual(projectKey, key)) return true;
    return (projectKey ?? "") === "" && fallback !== undefined && fallback !== "" && timingEqual(fallback, key);
  }

  projectKeyMatches(headers: Headers, project: string): boolean {
    return this.projectAcceptsSyncKey(project, bearer(headers), undefined);
  }

  /** `authorize_project_resource`: the project's own key as Bearer, else an operator with access (or manage). */
  async authorizeProjectResource(headers: Headers, project: string, permission: string, manage: boolean): Promise<void> {
    if (this.projectKeyMatches(headers, project)) return;
    const actor = await this.authorize(headers, permission);
    const allowed = manage ? this.canManageProject(actor, project) : this.canAccessProject(actor, project);
    if (!allowed) throw new HttpError(403, "project access is required");
  }

  /** `authorize` then `can_access_project` / `can_manage_project` with the handler's own 403 text. */
  async authorizeProject(headers: Headers, permission: string, project: string, manage: boolean, denied: string): Promise<OperatorActor> {
    const actor = await this.authorize(headers, permission);
    const allowed = manage ? this.canManageProject(actor, project) : this.canAccessProject(actor, project);
    if (!allowed) throw new HttpError(403, denied);
    return actor;
  }

  // ------------------------------------------------------------ credentials

  actorForEmail(email: string, credential: Credential): OperatorActor | undefined {
    const normalized = normalizeEmail(email);
    const row = this.sql.get("SELECT email,name,role FROM gonvex_dashboard_accounts WHERE email=$1", normalized);
    if (row) return { email: str(row.email), name: str(row.name), role: str(row.role), credential, permissions: ["*"] };
    if (this.config.dashboardAccount !== undefined && this.config.dashboardAccount === normalized) {
      return { email: normalized, name: displayName(normalized), role: "admin", credential, permissions: ["*"] };
    }
    return undefined;
  }

  private verifyPersonalToken(token: string): OperatorActor | undefined {
    const dot = token.indexOf(".");
    if (dot < 0) return undefined;
    const prefixed = token.slice(0, dot);
    if (!prefixed.startsWith("gvx_")) return undefined;
    const id = prefixed.slice(4);
    if (!id.startsWith("pat_")) return undefined;
    return this.sql.tx(() => {
      const row = this.sql.get(
        `SELECT owner_email,permissions FROM gonvex_account_access_tokens
         WHERE id=$1 AND token_hash=$2 AND revoked_at IS NULL
           AND (expires_at IS NULL OR expires_at>CAST(unixepoch('subsec')*1000 AS INTEGER))`,
        id,
        sha256Hex(token),
      );
      if (!row) return undefined;
      this.sql.run("UPDATE gonvex_account_access_tokens SET last_used_at=CAST(unixepoch('subsec')*1000 AS INTEGER) WHERE id=$1", id);
      let permissions: unknown;
      try {
        permissions = JSON.parse(str(row.permissions));
      } catch {
        return undefined;
      }
      if (!Array.isArray(permissions) || permissions.some((value) => typeof value !== "string")) return undefined;
      const owner = this.sql.get("SELECT email,name,role FROM gonvex_dashboard_accounts WHERE email=$1", str(row.owner_email));
      if (!owner) return undefined;
      return { email: str(owner.email), name: str(owner.name), role: str(owner.role), credential: "PersonalAccessToken" as const, permissions: permissions as string[] };
    });
  }

  private verifySignedSession(token: string): OperatorActor | undefined {
    const secret = this.secret();
    if (secret === undefined) return undefined;
    const dot = token.indexOf(".");
    if (dot < 0) return undefined;
    const payload = token.slice(0, dot);
    const signature = token.slice(dot + 1);
    const expected = base64url(new Uint8Array(createHmac("sha256", secret).update(payload).digest()));
    if (!timingEqual(signature, expected)) return undefined;
    const bytes = fromBase64url(payload);
    if (!bytes) return undefined;
    let session: Record<string, unknown>;
    try {
      session = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes)) as Record<string, unknown>;
    } catch {
      return undefined;
    }
    // serde: email, name, role, provider and expiresAt are required.
    for (const field of ["email", "name", "role", "provider"]) if (typeof session[field] !== "string") return undefined;
    if (typeof session.expiresAt !== "number" || !Number.isInteger(session.expiresAt)) return undefined;
    const email = String(session.email);
    if ((session.expiresAt as number) <= Date.now() || normalizeEmail(email) === "") return undefined;
    const revision = typeof session.sessionRevision === "string" ? session.sessionRevision : "";
    if (revision === "") return undefined;
    const authority = typeof session.authority === "string" ? session.authority : "";
    let name: string;
    let role: string;
    if (authority === "configured") {
      const configured = this.configuredSessionRevision();
      if (this.config.dashboardAccount !== email || configured === undefined || !timingEqual(revision, configured)) return undefined;
      name = displayName(email);
      role = "admin";
    } else if (authority === "database") {
      const row = this.sql.get("SELECT name,role,session_revision,password_hash FROM gonvex_dashboard_accounts WHERE email=$1", email);
      if (!row || !timingEqual(revision, str(row.session_revision)) || str(row.password_hash).startsWith("!")) return undefined;
      name = str(row.name);
      role = str(row.role);
    } else {
      return undefined;
    }
    return { email: normalizeEmail(email), name, role, credential: "Session", permissions: ["*"] };
  }

  /** `configured_session_revision`: HMAC over the configured account and password. */
  configuredSessionRevision(): string | undefined {
    const account = this.config.dashboardAccount;
    const password = this.config.dashboardPassword;
    const secret = this.secret();
    if (account === undefined || password === undefined || secret === undefined) return undefined;
    const mac = createHmac("sha256", secret);
    mac.update("gonvex-dashboard-configured-session-v1\0");
    // serde_json::to_vec(&(account, password)) is a JSON array of two strings.
    mac.update(JSON.stringify([account, password]));
    return base64url(new Uint8Array(mac.digest()));
  }

  /** `sign_session`: the payload without `accessToken`, HMAC-signed. */
  signSession(session: SignedSession): string {
    const secret = this.secret();
    if (secret === undefined) throw new HttpError(503, "dashboard session secret is not configured");
    const { accessToken: _drop, ...unsigned } = session;
    const payload = base64url(new TextEncoder().encode(JSON.stringify(serializeSession(unsigned))));
    return `${payload}.${base64url(new Uint8Array(createHmac("sha256", secret).update(payload).digest()))}`;
  }
}

export interface SignedSession {
  email: string;
  name: string;
  role: string;
  provider: string;
  expiresAt: number;
  accessToken?: string;
  sessionRevision: string;
  authority: string;
}

/** serde field order of `SignedSession`; `accessToken` is skipped when empty. */
export function serializeSession(session: Omit<SignedSession, "accessToken"> & { accessToken?: string }): Record<string, unknown> {
  const out: Record<string, unknown> = {
    email: session.email,
    name: session.name,
    role: session.role,
    provider: session.provider,
    expiresAt: session.expiresAt,
  };
  if (session.accessToken) out.accessToken = session.accessToken;
  out.sessionRevision = session.sessionRevision;
  out.authority = session.authority;
  return out;
}
