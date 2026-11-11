/**
 * Stand-in for the control object (K06) and the module registry (K19), only
 * as far as the session layer needs them: projects, a deployed module's hash
 * and client contract, password accounts, `gvx_session_` tokens, tenants with
 * their directory, and `control.auth.passwordLogin`. Results and error texts
 * are the Rust runtime's (`store/src/lib.rs`, `control.rs`), so conformance
 * compares the session layer, not the stub. K06 replaces this object behind
 * the same `ControlPort`.
 *
 * Not a security boundary: password hashes are plain SHA-256, tokens never
 * rotate, and every other `control.*` function answers "unknown".
 */
import { DurableObject } from "cloudflare:workers";
import type { ClientFrame, Json } from "../decode";
import { replicaEpoch, sha256Hex } from "../directive";
import type {
  Account, Failure, Impersonation, ModuleInfo, Outcome, RevalidationItem, SessionIdentity, SocketView, TenantRoute, TenantSession,
} from "../ports";
import { type GatewayRpc, type TenantRpc, rpc } from "../rpc";

export interface ControlStubEnv {
  TENANT_STUB: DurableObjectNamespace<any>;
  GATEWAY: DurableObjectNamespace<any>;
  GONVEX_GATEWAY_SHARDS?: string;
}

const ACCESS_TTL_MS = 900_000;
const REFRESH_TTL_MS = 30 * 24 * 3600_000;

/** Rust's `{:?}` for a plain string. */
const debug = (s: string) => JSON.stringify(s);
const fail = (kind: Failure["kind"], message: string): Failure => ({ ok: false, kind, message });
const ok = <T>(value: T): Outcome<T> => ({ ok: true, value });
const randomToken = (kind: string) => `gvx_${kind}_${[...crypto.getRandomValues(new Uint8Array(24))].map(b => b.toString(16).padStart(2, "0")).join("")}`;

type Row = Record<string, string | number | null>;

export class ControlStubObject extends DurableObject<ControlStubEnv> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: ControlStubEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    for (const ddl of [
      "CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT NOT NULL, key TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active')",
      "CREATE TABLE IF NOT EXISTS modules (project_id TEXT PRIMARY KEY, artifact_hash TEXT NOT NULL, client_contract INTEGER, functions TEXT NOT NULL, schema TEXT NOT NULL)",
      "CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, email TEXT NOT NULL, name TEXT NOT NULL, password_hash TEXT NOT NULL, UNIQUE (project_id, email))",
      "CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, kind TEXT NOT NULL, project_id TEXT NOT NULL, account_id TEXT NOT NULL, expires_at INTEGER NOT NULL, revoked INTEGER NOT NULL DEFAULT 0)",
      "CREATE TABLE IF NOT EXISTS tenants (tenant_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, name TEXT NOT NULL, domain TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active')",
      "CREATE TABLE IF NOT EXISTS directory (account_id TEXT NOT NULL, tenant_id TEXT NOT NULL, member_id TEXT NOT NULL, PRIMARY KEY (account_id, tenant_id))",
    ]) this.sql.exec(ddl);
  }

  private rows(query: string, ...args: unknown[]): Row[] { return this.sql.exec<Row>(query, ...args).toArray(); }
  private tenantStub(tenantId: string) { return rpc<TenantRpc>(this.env.TENANT_STUB, tenantId); }

  // ---- seeding (stub routes) ----------------------------------------------------------

  async createProject(name: string): Promise<{ projectId: string; projectKey: string }> {
    const projectId = crypto.randomUUID();
    const projectKey = randomToken("project");
    this.sql.exec("INSERT INTO projects (id, name, key) VALUES (?, ?, ?)", projectId, name, projectKey);
    return { projectId, projectKey };
  }

  projectKey(projectId: string): string | null {
    return (this.rows("SELECT key FROM projects WHERE id = ?", projectId)[0]?.key as string | undefined) ?? null;
  }

  /** `/dev/sync` as far as sessions see it: the active module, then `ModuleReloaded` to every gateway. */
  async deploy(projectId: string, manifest: { module?: { hash?: string; files?: Record<string, string>; functions?: Json }; schema?: Json; functions?: Json }): Promise<{ ok: boolean; functionCount: number }> {
    const module = manifest.module ?? {};
    const contractFile = module.files?.["client-contract.json"];
    const contract = contractFile ? (JSON.parse(atob(contractFile)) as { version: number }).version : null;
    const functions = (manifest.functions ?? module.functions ?? {}) as Json;
    const previous = this.rows("SELECT 1 FROM modules WHERE project_id = ?", projectId).length > 0;
    this.sql.exec(
      "INSERT INTO modules (project_id, artifact_hash, client_contract, functions, schema) VALUES (?, ?, ?, ?, ?) ON CONFLICT(project_id) DO UPDATE SET artifact_hash = excluded.artifact_hash, client_contract = excluded.client_contract, functions = excluded.functions, schema = excluded.schema",
      projectId, String(module.hash ?? ""), contract, JSON.stringify(functions), JSON.stringify(manifest.schema ?? null),
    );
    if (previous) {
      // K19 fans out through a project registry; the stub asks every gateway shard.
      const info = await this.module(projectId);
      const shards = Number(this.env.GONVEX_GATEWAY_SHARDS ?? 8) || 8;
      await Promise.all(Array.from({ length: shards }, (_, i) => rpc<GatewayRpc>(this.env.GATEWAY, `gw-${i}`).moduleReloaded(projectId, info)));
    }
    return { ok: true, functionCount: functions && typeof functions === "object" ? Object.keys(functions).length : 0 };
  }

  async createTenant(projectId: string, tenantId: string, name: string): Promise<{ id: string }> {
    this.sql.exec("INSERT INTO tenants (tenant_id, project_id, name) VALUES (?, ?, ?)", tenantId, projectId, name);
    return { id: tenantId };
  }

  async createMember(projectId: string, tenantId: string, input: { email: string; password: string; name: string }): Promise<{ accountId: string; memberId: string }> {
    const email = input.email.trim().toLowerCase();
    let account = this.rows("SELECT id FROM accounts WHERE project_id = ? AND email = ?", projectId, email)[0];
    if (!account) {
      const id = crypto.randomUUID();
      this.sql.exec("INSERT INTO accounts (id, project_id, email, name, password_hash) VALUES (?, ?, ?, ?, ?)", id, projectId, email, input.name, await sha256Hex(`${email}\u0000${input.password}`));
      account = { id };
    }
    const accountId = String(account.id);
    const memberId = crypto.randomUUID();
    const member = await this.tenantStub(tenantId).addMember({ id: memberId, accountId, displayName: input.name });
    this.sql.exec("INSERT OR REPLACE INTO directory (account_id, tenant_id, member_id) VALUES (?, ?, ?)", accountId, tenantId, member.id);
    return { accountId, memberId: member.id };
  }

  // ---- ModulePort --------------------------------------------------------------------------

  async module(projectId: string): Promise<ModuleInfo | null> {
    const row = this.rows("SELECT artifact_hash, client_contract FROM modules WHERE project_id = ?", projectId)[0];
    return row ? { artifactHash: String(row.artifact_hash), clientContract: row.client_contract === null ? null : Number(row.client_contract) } : null;
  }

  async epoch(projectId: string, databaseUrl: string): Promise<string> {
    const row = this.rows("SELECT artifact_hash, functions, schema FROM modules WHERE project_id = ?", projectId)[0];
    return replicaEpoch({
      projectId, databaseUrl, functions: JSON.parse(String(row?.functions ?? "null")) as Json,
      schema: JSON.parse(String(row?.schema ?? "null")) as Json, moduleHash: String(row?.artifact_hash ?? ""),
    });
  }

  // ---- ControlPort ---------------------------------------------------------------------------

  async project(projectId: string): Promise<Outcome<null>> {
    return this.rows("SELECT 1 FROM projects WHERE id = ? AND status = 'active'", projectId).length ? ok(null) : fail("Other", `project ${debug(projectId)} is not active`);
  }

  async loadSessionIdentity(token: string, projectId: string | undefined): Promise<Outcome<SessionIdentity>> {
    const row = this.rows("SELECT project_id, account_id FROM sessions WHERE token_hash = ? AND kind = 'session' AND revoked = 0 AND expires_at > ?", await sha256Hex(token.trim()), Date.now())[0];
    if (!row) return fail("InvalidSession", "invalid or expired app session");
    if (projectId !== undefined && row.project_id !== projectId) return fail("SessionProjectMismatch", "app session was issued for a different project");
    const account = this.account(String(row.account_id));
    return account ? ok({ projectId: String(row.project_id), account }) : fail("InvalidSession", "invalid or expired app session");
  }

  async authenticateSession(token: string, projectId: string | undefined, tenant: string | undefined): Promise<Outcome<TenantSession>> {
    const identity = await this.loadSessionIdentity(token, projectId);
    if (!identity.ok) return identity;
    const project = identity.value.projectId, accountId = identity.value.account.id;
    const requested = tenant?.trim();
    let tenantId: string;
    if (requested) tenantId = requested;
    else {
      const first = await this.firstAdmittedTenant(project, accountId);
      if (!first) return fail("NoTenantMembership", "the account has no active tenant membership");
      tenantId = first;
    }
    const route = this.resolveTenant(project, tenantId);
    if (!route.ok) return route;
    const member = await this.tenantStub(route.value.tenantId).admit(accountId);
    if (!member) return fail("MemberNotFound", `active tenant member for account ${debug(accountId)} not found`);
    const { admissionRevision, ...rest } = member;
    return ok({ identity: identity.value, route: route.value, member: rest, admissionRevision, delegation: null, developerSession: null, anonymous: false });
  }

  async authenticateImpersonation(_token: string, _projectId?: string, _tenant?: string, _connectionId?: string): Promise<Outcome<Impersonation>> {
    // No grants exist in the stub: Rust's answer for an unknown grant.
    return fail("InvalidSession", "invalid or expired app session");
  }

  async resolvePublicTenant(projectId: string, tenant: string): Promise<Outcome<TenantRoute>> {
    const project = await this.project(projectId);
    if (!project.ok) return project;
    const row = this.rows("SELECT tenant_id FROM tenants WHERE project_id = ? AND (tenant_id = ? OR lower(domain) = lower(?)) AND status = 'active' ORDER BY (tenant_id = ?) DESC LIMIT 1", projectId, tenant, tenant, tenant)[0];
    if (!row) return fail("Other", `tenant ${debug(tenant)} has no configured physical database`);
    return this.resolveTenant(projectId, String(row.tenant_id));
  }

  async revalidate(items: RevalidationItem[]): Promise<boolean[]> {
    return Promise.all(items.map(async (item) => {
      const identity = await this.loadSessionIdentity(item.token, item.projectId);
      if (!identity.ok) return false;
      if (!item.tenantId) return true;
      const member = await this.tenantStub(item.tenantId).admit(identity.value.account.id);
      return member?.id === item.memberId;
    }));
  }

  async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    if (frame.type === "action.call" && frame.path === "control.auth.passwordLogin") return this.passwordLogin(socket, frame.id, frame.path, frame.args);
    if (frame.type === "query.unsubscribe") return [];
    const unknown = (path: string) => `unknown Control Plane function ${debug(path)}`;
    switch (frame.type) {
      case "query.call":
      case "query.subscribe": return [JSON.stringify({ type: "query.error", id: frame.id, path: frame.path, error: unknown(frame.path) })];
      case "reducer.call": return [JSON.stringify({ type: "reducer.error", id: frame.id, path: frame.path, error: unknown(frame.path), class: "rejected", retryable: false })];
      case "action.call": return [JSON.stringify({ type: "action.error", id: frame.id, path: frame.path, error: unknown(frame.path) })];
      default: return [];
    }
  }

  // ---- internals ------------------------------------------------------------------------------

  private account(accountId: string): Account | null {
    const row = this.rows("SELECT id, email, name FROM accounts WHERE id = ?", accountId)[0];
    return row ? { id: String(row.id), email: String(row.email), emailVerified: true, name: String(row.name), avatarUrl: "", provider: "password" } : null;
  }

  private resolveTenant(projectId: string, tenant: string): Outcome<TenantRoute> {
    const row = this.rows("SELECT tenant_id FROM tenants WHERE project_id = ? AND (tenant_id = ? OR lower(domain) = lower(?)) AND status NOT IN ('deleted', 'disabled') ORDER BY (tenant_id = ?) DESC LIMIT 1", projectId, tenant, tenant, tenant)[0];
    if (!row) return fail("Other", `tenant ${debug(tenant)} has no configured physical database`);
    const tenantId = String(row.tenant_id);
    return ok({ projectId, tenantId, databaseUrl: `sqlite://tenants/${tenantId}.db` });
  }

  /** `first_admitted_tenant`: every project tenant, by `lower(name)`, the first one that admits the account. */
  private async firstAdmittedTenant(projectId: string, accountId: string): Promise<string | null> {
    for (const row of this.rows("SELECT tenant_id FROM tenants WHERE project_id = ? AND status NOT IN ('deleted', 'disabled') ORDER BY lower(name), tenant_id", projectId)) {
      if (await this.tenantStub(String(row.tenant_id)).admit(accountId)) return String(row.tenant_id);
    }
    return null;
  }

  /** `control.auth.passwordLogin` with Rust's session result (`control.rs:4076`, `session_result_from_directory`). */
  private async passwordLogin(socket: SocketView, id: string, path: string, args: Json): Promise<string[]> {
    const a = (args ?? {}) as Record<string, Json>;
    const email = String(a.email ?? "").trim().toLowerCase();
    const projectId = socket.connection.projectId;
    const row = this.rows("SELECT id, password_hash FROM accounts WHERE project_id = ? AND email = ?", projectId, email)[0];
    if (!row || row.password_hash !== await sha256Hex(`${email}\u0000${String(a.password ?? "")}`)) {
      return [JSON.stringify({ type: "action.error", id, path, error: "invalid email or password" })];
    }
    const account = this.account(String(row.id))!;
    const now = Date.now();
    const accessToken = randomToken("session"), refreshToken = randomToken("refresh");
    this.sql.exec("INSERT INTO sessions (token_hash, kind, project_id, account_id, expires_at) VALUES (?, 'session', ?, ?, ?)", await sha256Hex(accessToken), projectId, account.id, now + ACCESS_TTL_MS);
    this.sql.exec("INSERT INTO sessions (token_hash, kind, project_id, account_id, expires_at) VALUES (?, 'refresh', ?, ?, ?)", await sha256Hex(refreshToken), projectId, account.id, now + REFRESH_TTL_MS);
    const tenants: Json[] = [];
    for (const t of this.rows("SELECT t.tenant_id, t.name, t.domain FROM directory d JOIN tenants t ON t.tenant_id = d.tenant_id WHERE t.project_id = ? AND d.account_id = ? AND t.status NOT IN ('deleted', 'disabled') ORDER BY lower(t.name), t.tenant_id", projectId, account.id)) {
      const member = await this.tenantStub(String(t.tenant_id)).admit(account.id);
      if (!member) continue;
      tenants.push({
        id: String(t.tenant_id), name: String(t.name), role: member.role, permissions: member.permissions, memberId: member.id,
        domain: String(t.domain), timezone: "UTC", description: "Runtime-created tenant shard.", profile: {},
      });
    }
    const requested = String(a.tenantId ?? "");
    const active = (tenants as Array<Record<string, string>>).find(t => t.id === requested || (t.domain !== "" && t.domain?.toLowerCase() === requested.toLowerCase()))?.id
      ?? (tenants[0] as Record<string, string> | undefined)?.id ?? "";
    const result = {
      accessToken, tokenType: "Bearer", expiresIn: 900, expiresAt: now + ACCESS_TTL_MS, refreshToken, refreshExpiresAt: now + REFRESH_TTL_MS,
      account: { id: account.id, email: account.email, emailVerified: true, name: account.name, picture: "", provider: "password" },
      tenants, activeTenantId: active,
    };
    return [JSON.stringify({ type: "action.result", id, path, result }), JSON.stringify({ type: "control.watermark", id })];
  }
}
