/**
 * The operator HTTP routes (CONTRACT D.1), served by the control object:
 * rust/crates/runtime/src/operations.rs, operator_data.rs and the dev routes
 * of lib.rs. Status codes and `{error}` texts are the reference's.
 *
 * `/dev/sync`, `/dev/dashboard/ws`, `/dev/metrics/stream` and
 * `/dev/logs/stream` are not here: the router and `object.ts` own them.
 */
import { randomBytes, randomUUID } from "node:crypto";
import type { Ctx } from "../src/context";
import { encryptControlSecret } from "../src/secrets";
import { hashPassword, verifyPassword } from "../src/password";
import { normalizeRedirect } from "../src/native-auth";
import { bool, num, optNum, str, type Row } from "../src/sql";
import { base64url, sha256Hex } from "../src/util";
import { OperatorAuth, credentialName, globalAdmin, has, type OperatorActor, type SignedSession, serializeSession } from "./auth";
import type { OperatorConfig } from "./config";
import { Body, HttpError, PlainError, byteLength, displayName, error, isoTime, isoTimeOrNull, json, normalizeEmail, plainResponse, projectHeaderOrQuery } from "./http";
import { releaseKey, type ReleaseSummary } from "./release";
import type { SyncCoordinator } from "./sync";
import type { OperatorTenantPort, PortResult, RowsQuery } from "./tenant/port";
import { internalTable, validIdent } from "./tenant/rows";

export interface Op {
  ctx: Ctx;
  auth: OperatorAuth;
  config: OperatorConfig;
  sync: SyncCoordinator;
  releases: R2Bucket | undefined;
  tenant(tenantId: string): OperatorTenantPort;
  /** `RuntimeEvent::ControlChanged`. */
  controlChanged(projectId: string): void;
  /** Live authenticated sockets per tenant of a project, as socket owners report them (K06 `connectionState`). */
  tenantCounts(projectId: string): Map<string, number>;
  /** Applies the active release to a new tenant object and waits for it. */
  provisionTenant(projectId: string, tenantId: string, planId: string): Promise<void>;
  /** Deletes a tenant object's storage. */
  deleteTenantStorage(tenantId: string): Promise<void>;
}

type Handler = (op: Op, request: Request, url: URL, params: string[]) => Promise<Response>;

interface Route {
  pattern: RegExp;
  methods: Partial<Record<string, Handler>>;
}

const NOW = "CAST(unixepoch('subsec')*1000 AS INTEGER)";

// -------------------------------------------------------------------- helpers

function projectJson(row: Row): Record<string, unknown> {
  return {
    id: str(row.id),
    name: str(row.name),
    environment: str(row.environment),
    database: str(row.database_name),
    databaseMode: str(row.database_mode),
    storageBucket: str(row.storage_bucket),
    status: str(row.status),
    description: str(row.description),
    provisioned: bool(row.provisioned),
    runtimeCreated: bool(row.runtime_created),
    testTab: bool(row.test_tab),
    errorTrackingEnabled: bool(row.error_tracking_enabled),
    ownerEmail: str(row.owner_email),
  };
}

const PROJECT_COLUMNS = "id,name,environment,database_name,database_mode,storage_bucket,status,description,provisioned,runtime_created,test_tab,error_tracking_enabled,owner_email";

/** operations.rs `project_key`: `gvx_<b64url(projectId)>.<b64url(32 random bytes)>`. */
export const newProjectKey = (projectId: string): string => `gvx_${base64url(new TextEncoder().encode(projectId.trim()))}.${base64url(new Uint8Array(randomBytes(32)))}`;
const randomSecret = (): string => base64url(new Uint8Array(randomBytes(32)));

/** operations.rs `valid_env_name`. */
const validEnvName = (value: string): boolean => value !== "" && byteLength(value) <= 256 && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);

/** operations.rs `parse_dotenv`, into a BTreeMap (sorted by name). */
export function parseDotenv(content: string): Map<string, string> {
  const result = new Map<string, string>();
  for (const raw of content.split("\n")) {
    let line = raw.replace(/\r$/, "").trim();
    if (line === "" || line.startsWith("#")) continue;
    if (line.startsWith("export ")) line = line.slice("export ".length).trim();
    const eq = line.indexOf("=");
    if (eq < 0) continue;
    const name = line.slice(0, eq).trim();
    if (!validEnvName(name)) continue;
    let value = line.slice(eq + 1).trim();
    if (value.length >= 2 && ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'")))) value = value.slice(1, -1);
    result.set(name, value);
  }
  return result;
}

const sortedEntries = (map: Map<string, string>) => [...map.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

function secureExternalUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === "https:" && url.hostname !== "" && url.username === "" && url.password === "";
  } catch {
    return false;
  }
}

const SUPPORTED_PROVIDERS = new Set(["google", "microsoft", "apple", "firebase", "external-oidc"]);

function errorGroupJson(row: Row): Record<string, unknown> {
  const parse = (value: unknown) => {
    try {
      return JSON.parse(str(value as SqlStorageValue));
    } catch {
      return null;
    }
  };
  return {
    fingerprint: str(row.fingerprint),
    project: str(row.project_id),
    title: str(row.title),
    culprit: str(row.culprit),
    level: str(row.level),
    status: str(row.status),
    priority: str(row.priority),
    assignee: str(row.assignee),
    firstSeen: isoTime(num(row.first_seen)),
    lastSeen: isoTime(num(row.last_seen)),
    count: num(row.event_count),
    tenants: parse(row.tenants),
    releases: parse(row.releases),
    environments: parse(row.environments),
    accounts: parse(row.accounts),
    devices: parse(row.devices),
    latest: parse(row.latest_event),
    regression: bool(row.regression),
  };
}

function normalizeErrorLevel(value: string): string {
  const lower = value.trim().toLowerCase();
  if (lower === "warn" || lower === "warning") return "warning";
  if (lower === "error") return "error";
  return "";
}

/** gonvex-store `DatabaseError` Display texts the operator routes answer with. */
const dbError = {
  /** `protocol(...)`: sqlx `Error::Protocol`. */
  protocol: (message: string) => `encountered unexpected or invalid data: ${message}`,
  projectNotFound: (project: string) => `project ${JSON.stringify(project)} is not active`,
  tenantNotFound: (project: string, tenant: string) => `tenant ${JSON.stringify(tenant)} does not belong to project ${JSON.stringify(project)}`,
  tenantDatabaseMissing: (tenant: string) => `tenant ${JSON.stringify(tenant)} has no configured physical database`,
};

function unwrap<T>(result: PortResult<T>): T {
  if (!result.ok) throw new HttpError(result.status, result.error);
  return result.value;
}

/** Rust's `resolve_tenant` (exact id or domain), returning the canonical tenant id. */
function resolveTenant(op: Op, project: string, tenant: string): string | undefined {
  const row = op.ctx.sql.get(
    `SELECT tenant_id FROM gonvex_runtime_tenants
     WHERE project_id=$1 AND (tenant_id=$2 OR lower(domain)=lower($2)) AND deleted_at IS NULL
       AND status NOT IN ('deleted','disabled') AND database_url<>''
     ORDER BY (tenant_id=$2) DESC LIMIT 1`,
    project,
    tenant,
  );
  return row ? str(row.tenant_id) : undefined;
}

// -------------------------------------------------------------- account, tokens

const login: Handler = async (op, request) => {
  const body = await Body.read(request);
  const email = normalizeEmail(body.string("email"));
  const password = body.string("password");
  if (email === "" || password === "") return error(400, "email and password are required");
  const config = op.config;
  let actor: OperatorActor;
  let sessionRevision: string;
  let authority: string;
  if (config.dashboardAccount !== undefined && config.dashboardAccount === email && config.dashboardPassword !== undefined && config.dashboardPassword === password) {
    actor = { email, name: displayName(email), role: "admin", credential: "Session", permissions: ["*"] };
    sessionRevision = op.auth.configuredSessionRevision() ?? "";
    authority = "configured";
  } else {
    const row = op.ctx.sql.get("SELECT email,name,role,password_hash,session_revision FROM gonvex_dashboard_accounts WHERE email=$1", email);
    if (!row || !verifyPassword(password, str(row.password_hash))) return error(401, "invalid email or password");
    actor = { email: str(row.email), name: str(row.name), role: str(row.role), credential: "Session", permissions: ["*"] };
    sessionRevision = str(row.session_revision);
    authority = "database";
  }
  const session: SignedSession = {
    email: actor.email,
    name: actor.name,
    role: actor.role,
    provider: "gonvex",
    expiresAt: Date.now() + 7 * 24 * 60 * 60 * 1000,
    sessionRevision,
    authority,
  };
  session.accessToken = op.auth.signSession(session);
  return json({ session: serializeSession(session) });
};

const me: Handler = async (op, request) => {
  const actor = await op.auth.authenticate(request.headers);
  return json({
    account: { email: actor.email, name: actor.name, role: actor.role },
    authentication: credentialName[actor.credential],
    permissions: actor.credential === "PersonalAccessToken" ? actor.permissions : ["*"],
  });
};

async function adminActor(op: Op, request: Request): Promise<OperatorActor> {
  const actor = await op.auth.authorize(request.headers, "admin:projects");
  if (actor.role !== "admin") throw new HttpError(403, "admin access is required");
  return actor;
}

const listAccounts: Handler = async (op, request) => {
  await adminActor(op, request);
  const accounts = op.ctx.sql.all("SELECT email,name,role FROM gonvex_dashboard_accounts ORDER BY email").map((row) => ({ email: str(row.email), name: str(row.name), role: str(row.role) }));
  return json({ accounts });
};

const createAccount: Handler = async (op, request) => {
  await adminActor(op, request);
  const body = await Body.read(request);
  const email = normalizeEmail(body.string("email"));
  const requestedName = body.string("name");
  const password = body.string("password");
  const requestedRole = body.string("role").trim();
  const role = requestedRole === "admin" ? "admin" : requestedRole === "" || requestedRole === "standard" ? "standard" : undefined;
  if (!role) return error(400, "role must be admin or standard");
  if (email === "" || byteLength(password) < 12) return error(400, "email and a password of at least 12 characters are required");
  const name = requestedName.trim() === "" ? displayName(email) : requestedName.trim();
  op.ctx.sql.tx(() =>
    op.ctx.sql.run(
      `INSERT INTO gonvex_dashboard_accounts(email,name,role,password_hash,updated_at)
       VALUES($1,$2,$3,$4,${NOW}) ON CONFLICT(email) DO UPDATE SET
       name=EXCLUDED.name,role=EXCLUDED.role,password_hash=EXCLUDED.password_hash,updated_at=${NOW}`,
      email,
      name,
      role,
      hashPassword(password),
    ),
  );
  return json({ account: { email, name, role } }, 201);
};

function tokenJson(row: Row): Record<string, unknown> {
  let permissions: unknown = [];
  try {
    permissions = JSON.parse(str(row.permissions));
  } catch {
    permissions = [];
  }
  return {
    id: str(row.id),
    name: str(row.name),
    prefix: str(row.token_prefix),
    permissions,
    createdAt: isoTime(num(row.created_at)),
    expiresAt: isoTimeOrNull(row.expires_at),
    lastUsedAt: isoTimeOrNull(row.last_used_at),
    revokedAt: isoTimeOrNull(row.revoked_at),
  };
}

const listTokens: Handler = async (op, request) => {
  const actor = await op.auth.authorize(request.headers, "tokens:read");
  const rows = op.ctx.sql.all(
    `SELECT id,name,token_prefix,permissions,created_at,expires_at,last_used_at,revoked_at
     FROM gonvex_account_access_tokens WHERE owner_email=$1 ORDER BY created_at DESC`,
    actor.email,
  );
  return json({ tokens: rows.map(tokenJson) });
};

const ALLOWED_PERMISSIONS = new Set([
  "*", "projects:*", "projects:read", "projects:create", "projects:update", "projects:delete", "projects:keys:read", "projects:keys:write",
  "projects:members:read", "projects:members:write", "projects:env:read", "projects:env:write", "admin:projects", "tokens:*", "tokens:read",
  "tokens:create", "tokens:revoke",
]);

/** operations.rs `normalize_permissions`. */
function normalizePermissions(actor: OperatorActor, requested: string[]): string[] {
  let values = requested.length === 0 ? ["projects:read", "projects:create", "projects:keys:read"] : requested.map((value) => value.trim().toLowerCase()).filter((value) => value !== "");
  values = [...new Set(values)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  if (values.length === 0 || values.some((value) => !ALLOWED_PERMISSIONS.has(value))) throw new HttpError(400, "token permissions are invalid");
  if (values.some((value) => (value === "*" || value === "admin:projects") && !globalAdmin(actor))) throw new HttpError(400, "current credential cannot grant global administration");
  if (actor.credential === "PersonalAccessToken" && values.some((value) => !has(actor, value))) throw new HttpError(400, "current credential cannot grant the requested permission");
  return values;
}

/** chrono `DateTime::parse_from_rfc3339`, close enough: RFC 3339 with an offset. */
function parseRfc3339(value: string): number | undefined {
  if (!/^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/.test(value)) return undefined;
  const ms = Date.parse(value.replace(" ", "T"));
  return Number.isFinite(ms) ? ms : undefined;
}

const createToken: Handler = async (op, request) => {
  const actor = await op.auth.authorize(request.headers, "tokens:create");
  const body = await Body.read(request);
  const name = body.string("name").trim();
  if (name === "" || byteLength(name) > 120) return error(400, "token name must contain 1 to 120 characters");
  const permissions = normalizePermissions(actor, body.strings("permissions"));
  const rawExpiry = body.optionalString("expiresAt").trim();
  let expiresAt: number | null = null;
  if (rawExpiry !== "") {
    const parsed = parseRfc3339(rawExpiry);
    if (parsed === undefined || parsed <= Date.now()) return error(400, "expiresAt must be a future RFC3339 timestamp");
    expiresAt = parsed;
  }
  const id = `pat_${randomUUID()}`;
  const accessToken = `gvx_${id}.${randomSecret()}`;
  const prefix = `gvx_${id}`;
  const ownerName = actor.name === "" ? displayName(actor.email) : actor.name;
  op.ctx.sql.tx(() => {
    op.ctx.sql.run("INSERT INTO gonvex_dashboard_accounts(email,name,role,password_hash) VALUES($1,$2,$3,'!external-provider') ON CONFLICT(email) DO NOTHING", actor.email, ownerName, actor.role);
    op.ctx.sql.run(
      "INSERT INTO gonvex_account_access_tokens(id,owner_email,name,token_prefix,token_hash,permissions,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7)",
      id,
      actor.email,
      name,
      prefix,
      sha256Hex(accessToken),
      JSON.stringify(permissions),
      expiresAt,
    );
  });
  return json({ token: { id, name, prefix, permissions, createdAt: new Date().toISOString(), expiresAt: expiresAt === null ? null : isoTime(expiresAt) }, accessToken }, 201);
};

const revokeToken: Handler = async (op, request, _url, [token]) => {
  const actor = await op.auth.authorize(request.headers, "tokens:revoke");
  const changed = op.ctx.sql.tx(() =>
    op.ctx.sql.affected(`UPDATE gonvex_account_access_tokens SET revoked_at=${NOW} WHERE id=$1 AND owner_email=$2 AND revoked_at IS NULL`, token!.trim(), actor.email),
  );
  return changed === 1 ? json({ ok: true }) : error(404, "active account token not found");
};

const listNotifications: Handler = async (op, request) => {
  const actor = await op.auth.authenticate(request.headers);
  const notifications = op.ctx.sql
    .all(
      `SELECT id,kind,title,body,project_id,metadata,read_at,created_at
       FROM gonvex_dashboard_notifications WHERE email=$1 ORDER BY created_at DESC LIMIT 100`,
      actor.email,
    )
    .map((row) => {
      let metadata: unknown = null;
      try {
        metadata = JSON.parse(str(row.metadata));
      } catch {
        metadata = null;
      }
      return {
        id: str(row.id),
        kind: str(row.kind),
        title: str(row.title),
        body: str(row.body),
        projectId: str(row.project_id),
        metadata,
        read: row.read_at !== null && row.read_at !== undefined,
        createdAt: isoTime(num(row.created_at)),
      };
    });
  return json({ notifications, unread: notifications.filter((item) => !item.read).length });
};

const readNotifications: Handler = async (op, request) => {
  const actor = await op.auth.authenticate(request.headers);
  const body = await Body.read(request);
  const ids = body.strings("ids");
  op.ctx.sql.tx(() => {
    if (ids.length === 0) op.ctx.sql.run(`UPDATE gonvex_dashboard_notifications SET read_at=${NOW} WHERE email=$1 AND read_at IS NULL`, actor.email);
    else op.ctx.sql.run(`UPDATE gonvex_dashboard_notifications SET read_at=${NOW} WHERE email=$1 AND id IN (SELECT value FROM json_each($2)) AND read_at IS NULL`, actor.email, JSON.stringify(ids));
  });
  return json({ ok: true });
};

// --------------------------------------------------------------------- projects

const listProjects: Handler = async (op, request) => {
  const actor = await op.auth.authorize(request.headers, "projects:read");
  const rows = op.ctx.sql.all(`SELECT ${PROJECT_COLUMNS} FROM gonvex_runtime_projects WHERE status NOT IN('deleted','disabled') ORDER BY name,id`);
  return json({ projects: rows.map(projectJson).filter((project) => op.auth.canAccessProject(actor, String(project.id))) });
};

const createProject: Handler = async (op, request) => {
  const actor = await op.auth.authorize(request.headers, "projects:create");
  const body = await Body.read(request);
  const payload = body.object();
  const name = (typeof payload.name === "string" ? payload.name : "").trim();
  const mode = typeof payload.databaseMode === "string" ? payload.databaseMode : "single";
  if (mode !== "single" && mode !== "multiTenant") return error(422, dbError.protocol("databaseMode must be single or multiTenant"));
  if (name === "" || byteLength(name) > 120) return error(422, dbError.protocol("project name is required"));
  const projectId = randomUUID();
  const databaseName = `projects/${projectId}/project.db`;
  const databaseUrl = `do://tenant/${projectId}`;
  const projectKey = newProjectKey(projectId);
  const owner = actor.email.trim().toLowerCase();
  op.ctx.sql.tx(() => {
    op.ctx.sql.run(
      `INSERT INTO gonvex_runtime_projects
       (id,name,environment,database_name,database_mode,database_url,storage_bucket,status,description,project_key,provisioned,runtime_created,owner_email)
       VALUES($1,$2,'development',$3,$4,$5,$6,'active','Runtime-created project database.',$7,1,1,$8)`,
      projectId,
      name,
      databaseName,
      mode,
      databaseUrl,
      `${projectId}-dev`,
      projectKey,
      owner,
    );
    if (actor.email.trim() !== "") {
      op.ctx.sql.run(
        `INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,lower($2),$3,'owner')
         ON CONFLICT(project_id,email) DO UPDATE SET role='owner'`,
        projectId,
        actor.email.trim(),
        actor.email.split("@")[0] ?? actor.email,
      );
    }
    if (mode === "single") {
      op.ctx.sql.run(
        `INSERT INTO gonvex_runtime_tenants
         (relationship_id,project_id,tenant_id,name,database_alias,database_name,database_url,status,description,provisioned,runtime_created)
         VALUES($1,$1,$1,$2,'default',$3,$4,'active','Single-database tenant.',1,1)`,
        projectId,
        name,
        databaseName,
        databaseUrl,
      );
    }
  });
  return json(
    {
      project: { id: projectId, name, environment: "development", database: databaseName, databaseMode: mode, storageBucket: `${projectId}-dev`, status: "active", description: "Runtime-created project database." },
      projectKey,
    },
    201,
  );
};

const updateProject: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProject(request.headers, "projects:update", project!, true, "project owner or admin access is required");
  const body = await Body.read(request);
  const name = body.maybeString("name");
  const mode = body.maybeString("databaseMode");
  const tracking = body.maybeBool("errorTrackingEnabled");
  const status = body.maybeString("status");
  if (name === undefined && mode === undefined && tracking === undefined && status === undefined) return error(400, "no project fields provided");
  if (name !== undefined && (name.trim() === "" || byteLength(name.trim()) > 120)) return error(400, "project name must contain 1 to 120 characters");
  if (mode !== undefined && mode !== "single" && mode !== "multiTenant") return error(400, "databaseMode must be single or multiTenant");
  if (status !== undefined && status !== "active") return error(400, "status may only promote a project to active");
  const row = op.ctx.sql.tx(() =>
    op.ctx.sql.get(
      `UPDATE gonvex_runtime_projects SET
         name=COALESCE($2,name),database_mode=COALESCE($3,database_mode),
         error_tracking_enabled=COALESCE($4,error_tracking_enabled),
         status=COALESCE($5,status),updated_at=${NOW}
       WHERE id=$1 AND status NOT IN('deleted','disabled')
       RETURNING ${PROJECT_COLUMNS}`,
      project,
      name?.trim() ?? null,
      mode ?? null,
      tracking === undefined ? null : tracking,
      status ?? null,
    ),
  );
  if (!row) return error(404, "project not found");
  return json({ project: projectJson(row) });
};

const deleteProject: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProject(request.headers, "projects:delete", project!, true, "project owner or admin access is required");
  const row = op.ctx.sql.get("SELECT runtime_created FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN('deleted','disabled')", project);
  if (!row) return error(422, dbError.projectNotFound(project!));
  if (!bool(row.runtime_created)) return error(422, dbError.protocol("only runtime-created projects may be deleted through this operation"));
  const tenants = op.ctx.sql.all("SELECT tenant_id FROM gonvex_runtime_tenants WHERE project_id=$1 AND runtime_created=1 AND database_url<>''", project).map((t) => str(t.tenant_id));
  op.ctx.sql.tx(() => op.ctx.sql.run(`UPDATE gonvex_runtime_projects SET status='deleting',updated_at=${NOW} WHERE id=$1`, project));
  for (const tenant of tenants) await op.deleteTenantStorage(tenant);
  op.ctx.sql.tx(() => op.ctx.sql.run("DELETE FROM gonvex_runtime_projects WHERE id=$1 AND status='deleting'", project));
  return json({ ok: true });
};

const projectKey: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProject(request.headers, "projects:keys:read", project!, false, "project access is required");
  const key = op.ctx.sql.value("SELECT project_key FROM gonvex_runtime_projects WHERE id=$1 AND status NOT IN('deleted','disabled')", project);
  if (key === undefined) return error(404, dbError.projectNotFound(project!));
  return json({ projectKey: str(key) });
};

const rotateProjectKey: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProject(request.headers, "projects:keys:write", project!, true, "project owner or admin access is required");
  const key = newProjectKey(project!);
  const changed = op.ctx.sql.tx(() => op.ctx.sql.affected(`UPDATE gonvex_runtime_projects SET project_key=$2,updated_at=${NOW} WHERE id=$1`, project, key));
  return changed === 1 ? json({ projectKey: key }) : error(404, "project not found");
};

const projectMembers: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:members:read", false);
  const members = op.ctx.sql.all("SELECT email,name,role FROM gonvex_project_members WHERE project_id=$1 ORDER BY role,email", project).map((row) => ({ email: str(row.email), name: str(row.name), role: str(row.role) }));
  const invitations = op.ctx.sql
    .all(
      `SELECT id,project_id,email,role,expires_at,accepted_at IS NOT NULL AS accepted
       FROM gonvex_project_invitations WHERE project_id=$1 ORDER BY created_at DESC`,
      project,
    )
    .map((row) => ({ id: str(row.id), projectId: str(row.project_id), email: str(row.email), role: str(row.role), expiresAt: isoTime(num(row.expires_at)), accepted: bool(row.accepted) }));
  return json({ members, invitations });
};

function memberRole(raw: string): string {
  const role = raw.trim();
  if (role === "" || role === "dev") return "dev";
  if (role === "owner" || role === "admin") return role;
  throw new HttpError(400, "role must be owner, admin, or dev");
}

const upsertProjectMember: Handler = async (op, request, _url, [project]) => {
  const actor = await op.auth.authorizeProject(request.headers, "projects:members:write", project!, true, "project owner or admin access is required");
  const body = await Body.read(request);
  const email = normalizeEmail(body.string("email"));
  const name = body.optionalString("name").trim();
  const role = memberRole(body.optionalString("role"));
  if (email === "") return error(400, "email is required");
  op.ctx.sql.tx(() =>
    op.ctx.sql.run(
      `INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,$2,$3,$4)
       ON CONFLICT(project_id,email) DO UPDATE SET name=EXCLUDED.name,role=EXCLUDED.role`,
      project,
      email,
      name,
      role,
    ),
  );
  op.controlChanged(project!);
  return json({ member: { email, name, role, updatedBy: actor.email } });
};

const createProjectInvitation: Handler = async (op, request, _url, [project]) => {
  const actor = await op.auth.authorizeProject(request.headers, "projects:members:write", project!, true, "project owner or admin access is required");
  const body = await Body.read(request);
  const email = normalizeEmail(body.string("email"));
  const role = memberRole(body.optionalString("role"));
  if (email === "") return error(400, "email is required");
  const id = `pinv_${randomUUID()}`;
  const token = `invite_${randomSecret()}`;
  const expiresAt = Date.now() + 14 * 24 * 60 * 60 * 1000;
  op.ctx.sql.tx(() =>
    op.ctx.sql.run(
      `INSERT INTO gonvex_project_invitations
       (id,project_id,email,role,token_hash,invited_by,expires_at,accepted_at,created_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,NULL,${NOW})
       ON CONFLICT(project_id,email) DO UPDATE SET
         id=EXCLUDED.id,role=EXCLUDED.role,token_hash=EXCLUDED.token_hash,
         invited_by=EXCLUDED.invited_by,expires_at=EXCLUDED.expires_at,
         accepted_at=NULL,created_at=${NOW}`,
      id,
      project,
      email,
      role,
      sha256Hex(token),
      actor.email,
      expiresAt,
    ),
  );
  return json({ invitation: { id, projectId: project, email, role, expiresAt: isoTime(expiresAt), accepted: false, token } }, 201);
};

// -------------------------------------------------------------------------- env

const getEnv: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:env:read", false);
  const variables = op.ctx.sql
    .all("SELECT name,value,updated_at FROM gonvex_project_env WHERE project_id=$1 ORDER BY name", project)
    .map((row) => ({ name: str(row.name), value: str(row.value), source: "project", updatedAt: isoTime(num(row.updated_at)) }));
  return json({ variables });
};

const setEnv: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:env:write", true);
  const body = await Body.read(request);
  const name = body.string("name").trim();
  const value = body.string("value");
  if (!validEnvName(name)) return error(400, "variable name is invalid");
  op.ctx.sql.tx(() =>
    op.ctx.sql.run(
      `INSERT INTO gonvex_project_env(project_id,name,value,updated_at) VALUES($1,$2,$3,${NOW})
       ON CONFLICT(project_id,name) DO UPDATE SET value=EXCLUDED.value,updated_at=${NOW}`,
      project,
      name,
      value,
    ),
  );
  return json({ ok: true });
};

const replaceEnv: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:env:write", true);
  const body = await Body.read(request);
  const pairs = parseDotenv(body.optionalString("content"));
  const variables = body.object().variables;
  if (variables !== undefined && variables !== null) {
    if (!Array.isArray(variables)) throw new PlainError(422, "Failed to deserialize the JSON body into the target type: variables: invalid type, expected a sequence");
    for (const item of variables as Array<Record<string, unknown>>) {
      if (typeof item?.name !== "string" || typeof item?.value !== "string") throw new PlainError(422, "Failed to deserialize the JSON body into the target type: variables: missing field `name` or `value`");
      if (validEnvName(item.name.trim())) pairs.set(item.name.trim(), item.value);
    }
  }
  op.ctx.sql.tx(() => {
    op.ctx.sql.run("DELETE FROM gonvex_project_env WHERE project_id=$1", project);
    for (const [name, value] of sortedEntries(pairs)) op.ctx.sql.run("INSERT INTO gonvex_project_env(project_id,name,value) VALUES($1,$2,$3)", project, name, value);
  });
  return json({ ok: true, count: pairs.size });
};

const deleteEnv: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:env:write", true);
  const body = await Body.read(request);
  const name = body.string("name").trim();
  if (!validEnvName(name)) return error(400, "variable name is invalid");
  op.ctx.sql.tx(() => op.ctx.sql.run("DELETE FROM gonvex_project_env WHERE project_id=$1 AND name=$2", project, name));
  return json({ ok: true });
};

// ------------------------------------------------------------- auth providers

function providerJson(provider: string, row: Row): Record<string, unknown> {
  return {
    provider,
    authMode: str(row.auth_mode),
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
  };
}

const getAuthProvider: Handler = async (op, request, _url, [project, provider]) => {
  if (!SUPPORTED_PROVIDERS.has(provider!)) return error(400, "provider is unsupported");
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:read", false);
  const row = op.ctx.sql.get(
    `SELECT COALESCE(NULLIF(project.auth_mode,''),'gonvex-native') AS auth_mode,
            provider.enabled,provider.signup_mode,provider.azure_tenant_id,provider.client_id,
            provider.client_secret_encrypted IS NOT NULL AS has_client_secret,
            provider.issuer,provider.audience,provider.jwks_url,provider.firebase_project_id,
            provider.firebase_tenant_id,provider.admin_credentials_encrypted IS NOT NULL AS has_admin_credentials
     FROM gonvex_runtime_projects project JOIN gonvex_auth_providers provider
       ON provider.project_id=project.id AND provider.provider=$2 WHERE project.id=$1`,
    project,
    provider,
  );
  return row ? json(providerJson(provider!, row)) : error(404, "provider is not configured");
};

const putAuthProvider: Handler = async (op, request, _url, [project, provider]) => {
  if (!SUPPORTED_PROVIDERS.has(provider!)) return error(400, "provider is unsupported");
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:update", true);
  const body = await Body.read(request);
  const object = body.value;
  if (!object || typeof object !== "object" || Array.isArray(object)) return error(400, "provider configuration must be an object");
  const o = object as Record<string, unknown>;
  const text = (key: string) => (typeof o[key] === "string" ? (o[key] as string).trim() : "");
  const authMode = typeof o.authMode === "string" ? o.authMode : provider === "google" || provider === "microsoft" || provider === "apple" ? "gonvex-native" : provider!;
  if (!["gonvex-native", "firebase", "external-oidc", "hybrid"].includes(authMode)) return error(400, "authMode is unsupported");
  const signupMode = typeof o.signupMode === "string" ? o.signupMode : "personal";
  if (signupMode !== "personal" && signupMode !== "inviteOnly") return error(400, "signupMode must be personal or inviteOnly");
  const enabled = typeof o.enabled === "boolean" ? o.enabled : true;
  const issuer = text("issuer");
  const audience = text("audience");
  const jwksUrl = text("jwksUrl");
  const firebaseProject = text("firebaseProjectId");
  if (provider === "firebase" && firebaseProject === "") return error(400, "firebaseProjectId is required");
  if (provider === "external-oidc" && (issuer === "" || audience === "" || jwksUrl === "")) return error(400, "external-oidc requires issuer, audience, and jwksUrl");
  for (const value of [issuer, jwksUrl]) if (value !== "" && !secureExternalUrl(value)) return error(400, "issuer and jwksUrl must use https without credentials");
  let clientSecret: Uint8Array | null = null;
  let adminCredentials: Uint8Array | null = null;
  try {
    if (typeof o.clientSecret === "string" && o.clientSecret !== "") clientSecret = encryptControlSecret(op.ctx.config.controlSecret, o.clientSecret);
    if (typeof o.adminCredentials === "string" && o.adminCredentials !== "") adminCredentials = encryptControlSecret(op.ctx.config.controlSecret, o.adminCredentials);
  } catch {
    return error(503, "provider secrets cannot be encrypted");
  }
  op.ctx.sql.tx(() => {
    op.ctx.sql.run(
      `INSERT INTO gonvex_auth_providers
       (project_id,provider,enabled,signup_mode,azure_tenant_id,client_id,client_secret_encrypted,
        issuer,audience,jwks_url,firebase_project_id,firebase_tenant_id,admin_credentials_encrypted,updated_at)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,${NOW})
       ON CONFLICT(project_id,provider) DO UPDATE SET enabled=EXCLUDED.enabled,signup_mode=EXCLUDED.signup_mode,
         azure_tenant_id=EXCLUDED.azure_tenant_id,client_id=EXCLUDED.client_id,
         client_secret_encrypted=COALESCE(EXCLUDED.client_secret_encrypted,gonvex_auth_providers.client_secret_encrypted),
         issuer=EXCLUDED.issuer,audience=EXCLUDED.audience,jwks_url=EXCLUDED.jwks_url,
         firebase_project_id=EXCLUDED.firebase_project_id,firebase_tenant_id=EXCLUDED.firebase_tenant_id,
         admin_credentials_encrypted=COALESCE(EXCLUDED.admin_credentials_encrypted,gonvex_auth_providers.admin_credentials_encrypted),updated_at=${NOW}`,
      project,
      provider,
      enabled,
      signupMode,
      text("azureTenantId"),
      text("clientId"),
      clientSecret,
      issuer,
      audience,
      jwksUrl,
      firebaseProject,
      text("firebaseTenantId"),
      adminCredentials,
    );
    op.ctx.sql.run(`UPDATE gonvex_runtime_projects SET auth_mode=$2,updated_at=${NOW} WHERE id=$1`, project, authMode);
  });
  op.controlChanged(project!);
  return json({ provider, authMode, enabled, signupMode, updated: true });
};

function googleView(op: Op, project: string): Record<string, unknown> {
  const provider = op.ctx.sql.get("SELECT enabled,signup_mode,client_id,client_secret_encrypted IS NOT NULL AS has_secret FROM gonvex_auth_providers WHERE project_id=$1 AND provider='google'", project);
  const redirects = op.ctx.sql.all("SELECT redirect_uri FROM gonvex_auth_redirect_uris WHERE project_id=$1 AND provider='google' ORDER BY redirect_uri", project).map((row) => str(row.redirect_uri));
  const enabled = provider ? bool(provider.enabled) : false;
  const signupMode = provider ? str(provider.signup_mode) : "personal";
  const configuredClient = provider ? str(provider.client_id) !== "" && bool(provider.has_secret) : false;
  const callback = op.config.authPublicUrl ? `${op.config.authPublicUrl.replace(/\/+$/, "")}/auth/google/callback` : "";
  const issues: string[] = [];
  if (!configuredClient && !op.config.googleClient) issues.push("Google client ID and secret are not configured");
  if (callback === "") issues.push("GONVEX_AUTH_URL is required");
  if (redirects.length === 0) issues.push("no app callback is registered");
  return { enabled, signupMode, redirectUris: redirects, brokerCallbackUrl: callback, ready: enabled && issues.length === 0, issues };
}

const getGoogle: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:read", false);
  return json(googleView(op, project!));
};

const putGoogle: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:update", true);
  const body = await Body.read(request);
  let redirect: string;
  try {
    redirect = normalizeRedirect(body.string("redirectUri"));
  } catch (cause) {
    if (cause instanceof PlainError) throw cause;
    return error(400, (cause as Error).message);
  }
  const signup = body.optionalString("signupMode") || "personal";
  if (signup !== "personal" && signup !== "inviteOnly") return error(400, "signupMode must be personal or inviteOnly");
  op.ctx.sql.tx(() => {
    op.ctx.sql.run(
      `INSERT INTO gonvex_auth_providers(project_id,provider,enabled,signup_mode) VALUES($1,'google',TRUE,$2)
       ON CONFLICT(project_id,provider) DO UPDATE SET enabled=TRUE,signup_mode=EXCLUDED.signup_mode,updated_at=${NOW}`,
      project,
      signup,
    );
    op.ctx.sql.run("INSERT INTO gonvex_auth_redirect_uris(project_id,provider,redirect_uri) VALUES($1,'google',$2) ON CONFLICT DO NOTHING", project, redirect);
    op.ctx.sql.run(`UPDATE gonvex_runtime_projects SET auth_mode=CASE WHEN auth_mode IN('firebase','external-oidc') THEN 'hybrid' ELSE 'gonvex-native' END,updated_at=${NOW} WHERE id=$1`, project);
  });
  return json(googleView(op, project!));
};

const deleteGoogle: Handler = async (op, request, url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:update", true);
  const raw = (url.searchParams.get("redirect_uri") ?? "").trim();
  if (raw === "") {
    op.ctx.sql.tx(() => op.ctx.sql.run(`UPDATE gonvex_auth_providers SET enabled=FALSE,updated_at=${NOW} WHERE project_id=$1 AND provider='google'`, project));
  } else {
    let redirect: string;
    try {
      redirect = normalizeRedirect(raw);
    } catch (cause) {
      return error(400, (cause as Error).message);
    }
    op.ctx.sql.tx(() => op.ctx.sql.run("DELETE FROM gonvex_auth_redirect_uris WHERE project_id=$1 AND provider='google' AND redirect_uri=$2", project, redirect));
  }
  return json({ ok: true });
};

// --------------------------------------------------------- project auth realm

const projectAuthAccounts: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:read", false);
  const accounts = op.ctx.sql
    .all(
      `SELECT account.id,account.email,account.name,account.avatar_url,
              account.disabled_at IS NOT NULL AS disabled,account.created_at,account.updated_at,
              COALESCE((SELECT verified_email FROM account_identities
                WHERE account_id=account.id ORDER BY updated_at DESC LIMIT 1),0) AS email_verified,
              COALESCE((SELECT provider FROM account_identities
                WHERE account_id=account.id ORDER BY updated_at DESC LIMIT 1),'') AS provider
       FROM accounts account
       WHERE account.auth_realm_id=$1 ORDER BY account.created_at DESC`,
      project,
    )
    .map((row) => ({
      id: str(row.id),
      accountId: str(row.id),
      email: str(row.email),
      emailVerified: bool(row.email_verified),
      name: str(row.name),
      picture: str(row.avatar_url),
      provider: str(row.provider),
      disabled: bool(row.disabled),
      createdAt: isoTime(num(row.created_at)),
      lastSignedInAt: isoTime(num(row.updated_at)),
    }));
  return json({ accounts });
};

async function setAccountDisabled(op: Op, request: Request, project: string, account: string, disabled: boolean): Promise<Response> {
  await op.auth.authorizeProjectResource(request.headers, project, "projects:update", true);
  const changed = op.ctx.sql.tx(() => {
    const updated = op.ctx.sql.affected(
      `UPDATE accounts SET disabled_at=CASE WHEN $3 THEN COALESCE(disabled_at,${NOW}) ELSE NULL END,updated_at=${NOW} WHERE auth_realm_id=$1 AND id=$2`,
      project,
      account,
      disabled,
    );
    if (updated === 0) return 0;
    if (disabled) {
      op.ctx.sql.run(`UPDATE gonvex_auth_sessions SET revoked_at=COALESCE(revoked_at,${NOW}) WHERE project_id=$1 AND account_id=$2`, project, account);
      op.ctx.sql.run(`UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,${NOW}) WHERE project_id=$1 AND account_id=$2`, project, account);
    }
    return updated;
  });
  // Live sockets of a disabled account fail their next revalidation (C.3.8 item 10).
  return changed === 0 ? error(404, "account not found") : json({ ok: true, disabled });
}

const updateProjectAuthAccount: Handler = async (op, request, _url, [project, account]) => {
  const body = await Body.read(request);
  return setAccountDisabled(op, request, project!, account!, body.bool("disabled"));
};

const deleteProjectAuthAccount: Handler = (op, request, _url, [project, account]) => setAccountDisabled(op, request, project!, account!, true);

const projectAuthTenants: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:read", false);
  const tenants = op.ctx.sql
    .all(
      `SELECT tenant.tenant_id,tenant.name,count(directory.account_id) FILTER (WHERE directory.status='active') AS member_count
       FROM gonvex_runtime_tenants tenant
       LEFT JOIN account_tenant_index directory ON directory.tenant_id=tenant.tenant_id
       WHERE tenant.project_id=$1 AND tenant.deleted_at IS NULL
       GROUP BY tenant.tenant_id,tenant.name ORDER BY lower(tenant.name),tenant.tenant_id`,
      project,
    )
    .map((row) => ({ id: str(row.tenant_id), name: str(row.name), memberCount: num(row.member_count) }));
  return json({ tenants });
};

/** `store_operator_membership_invitation`. */
function storeMembershipInvitation(op: Op, project: string, tenant: string, emailRaw: string, role: string, permissions: unknown): void {
  const email = normalizeEmail(emailRaw);
  if (email === "") throw new HttpError(400, "email is required");
  const entry = op.ctx.sql.get(
    "SELECT tenant_id FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL AND status NOT IN ('deleted','disabled')",
    project,
    tenant,
  );
  if (project.trim() === "") throw new HttpError(400, "project id is required");
  if (tenant.trim() === "") throw new HttpError(400, "tenant id is required");
  if (!entry) throw new HttpError(400, dbError.tenantNotFound(project, tenant));
  op.ctx.sql.tx(() =>
    op.ctx.sql.run(
      `INSERT INTO gonvex_auth_membership_invitations
       (project_id,tenant_id,email,role,permissions,invited_by,expires_at,id,token_hash,
        revoked_at,accepted_at,accepted_account_id,accepted_idempotency_key,handoff_state,
        handoff_command_id,completed_at,updated_at)
       VALUES($1,$2,$3,$4,$5,'project-admin',${NOW}+604800000,$6,$7,
              NULL,NULL,NULL,NULL,'pending','',NULL,${NOW})
       ON CONFLICT(project_id,tenant_id,email) DO UPDATE SET role=EXCLUDED.role,
         permissions=EXCLUDED.permissions,invited_by=EXCLUDED.invited_by,
         expires_at=EXCLUDED.expires_at,id=EXCLUDED.id,token_hash=EXCLUDED.token_hash,
         revoked_at=NULL,accepted_at=NULL,accepted_account_id=NULL,
         accepted_idempotency_key=NULL,handoff_state='pending',handoff_command_id='',
         completed_at=NULL,updated_at=${NOW}`,
      project,
      tenant,
      email,
      role,
      JSON.stringify(permissions),
      `invite_${randomUUID()}`,
      sha256Hex(`invite_${randomSecret()}`),
    ),
  );
}

const createProjectAuthTenant: Handler = async (op, request, _url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:update", true);
  const body = await Body.read(request);
  const name = body.string("name").trim();
  const ownerEmail = body.optionalString("ownerEmail");
  if (name === "") return error(400, "tenant name is required");
  let tenant: Record<string, unknown>;
  try {
    tenant = await provisionRuntimeTenant(op, project!, undefined, name);
  } catch (cause) {
    return error(422, cause instanceof Error ? cause.message : String(cause));
  }
  if (ownerEmail.trim() !== "") {
    try {
      storeMembershipInvitation(op, project!, String(tenant.id), ownerEmail, "owner", {});
    } catch (cause) {
      return json({ error: (cause as Error).message, tenant }, 500);
    }
  }
  op.controlChanged(project!);
  return json({ tenant }, 201);
};

const projectAuthMemberships: Handler = async (op, request, url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:read", false);
  const tenant = (url.searchParams.get("tenant") ?? "").trim();
  if (tenant === "") return error(400, "tenant is required");
  const resolved = resolveTenant(op, project!, tenant);
  if (!resolved) return error(404, dbError.tenantDatabaseMissing(tenant));
  const rows = unwrap(await op.tenant(resolved).operatorActiveMembers());
  const members = rows.map((row) => {
    const account = op.ctx.sql.get("SELECT email,name FROM accounts WHERE auth_realm_id=$1 AND id=$2", project, row.accountId);
    const accountName = account ? str(account.name) : "";
    return { memberId: row.id, email: account ? str(account.email) : "", name: accountName !== "" ? accountName : row.displayName, role: row.role, permissions: row.permissions };
  });
  const invitations = op.ctx.sql
    .all(
      `SELECT email,role,permissions,expires_at FROM gonvex_auth_membership_invitations
       WHERE project_id=$1 AND tenant_id=$2 AND expires_at>${NOW}
         AND revoked_at IS NULL AND accepted_at IS NULL ORDER BY lower(email)`,
      project,
      tenant,
    )
    .map((row) => {
      let permissions: unknown = {};
      try {
        permissions = JSON.parse(str(row.permissions));
      } catch {
        permissions = {};
      }
      return { email: str(row.email), role: str(row.role), permissions, expiresAt: isoTime(num(row.expires_at)) };
    });
  return json({ members, invitations });
};

const putProjectAuthMembership: Handler = async (op, request, url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:update", true);
  const tenant = (url.searchParams.get("tenant") ?? "").trim();
  const body = await Body.read(request);
  const email = body.string("email");
  if (tenant === "") return error(400, "tenant is required");
  const rawRole = body.optionalString("role").trim();
  const role = rawRole === "" ? "member" : ["owner", "admin", "member"].includes(rawRole) ? rawRole : undefined;
  if (!role) return error(400, "invalid member role");
  const raw = body.object().permissions;
  let permissions: unknown;
  if (raw === undefined || raw === null) permissions = {};
  else if (typeof raw === "object" && !Array.isArray(raw)) permissions = raw;
  else return error(400, "permissions must be an object");
  storeMembershipInvitation(op, project!, tenant, email, role, permissions);
  op.controlChanged(project!);
  return json({ ok: true });
};

const deleteProjectAuthMembership: Handler = async (op, request, url, [project]) => {
  await op.auth.authorizeProjectResource(request.headers, project!, "projects:update", true);
  const tenant = (url.searchParams.get("tenant") ?? "").trim();
  const member = (url.searchParams.get("member") ?? "").trim();
  const email = (url.searchParams.get("email") ?? "").trim();
  if (tenant === "" || (member === "" && email === "")) return error(400, "tenant and member or invitation email are required");
  if (member !== "") {
    const resolved = resolveTenant(op, project!, tenant);
    if (!resolved) return error(404, dbError.tenantDatabaseMissing(tenant));
    const revoked = await op.tenant(resolved).operatorRevokeMember(member);
    if (!revoked.ok) return error(400, revoked.error);
    if (!revoked.value) return error(400, "member was not found");
  } else {
    op.ctx.sql.tx(() =>
      op.ctx.sql.run(
        `UPDATE gonvex_auth_membership_invitations SET revoked_at=${NOW},updated_at=${NOW} WHERE project_id=$1 AND tenant_id=$2 AND lower(email)=lower($3) AND accepted_at IS NULL`,
        project,
        tenant,
        email,
      ),
    );
  }
  op.controlChanged(project!);
  return json({ ok: true });
};

// ---------------------------------------------------------------------- tenants

function tenantJson(row: Row): Record<string, unknown> {
  let profile: unknown = {};
  try {
    profile = JSON.parse(str(row.profile));
  } catch {
    profile = {};
  }
  return {
    id: str(row.tenant_id),
    projectId: str(row.project_id),
    name: str(row.name),
    databaseAlias: str(row.database_alias),
    database: str(row.database_name),
    status: str(row.status),
    description: str(row.description),
    domain: str(row.domain),
    timezone: str(row.timezone),
    profile,
    provisioned: bool(row.provisioned),
  };
}

const listTenants: Handler = async (op, request, url) => {
  const actor = await op.auth.authorize(request.headers, "projects:read");
  const project = url.searchParams.get("project");
  if (project !== null) {
    if (!op.auth.canAccessProject(actor, project)) return error(403, "project access is required");
  } else if (!globalAdmin(actor)) {
    return error(400, "project is required for non-global credentials");
  }
  const rows = op.ctx.sql.all(
    `SELECT tenant_id,project_id,name,database_alias,database_name,status,description,domain,timezone,profile,provisioned
     FROM gonvex_runtime_tenants
     WHERE ($1='' OR project_id=$1) AND deleted_at IS NULL
       AND status NOT IN('deleted','disabled') ORDER BY project_id,name,tenant_id`,
    (project ?? "").trim(),
  );
  return json({ tenants: rows.map(tenantJson) });
};

/** lib.rs `provision_runtime_tenant` with store `create_runtime_tenant`. */
async function provisionRuntimeTenant(op: Op, projectRaw: string, requested: string | undefined, nameRaw: string): Promise<Record<string, unknown>> {
  const release = op.sync.activeRelease(projectRaw);
  if (!release) throw new Error("install the TypeScript module before creating the first tenant shard");
  const projectId = projectRaw.trim();
  const name = nameRaw.trim();
  if (projectId === "" || name === "" || byteLength(name) > 120) throw new Error(dbError.protocol("projectId and tenant name are required"));
  const tenantId = requested?.trim() || randomUUID();
  if (byteLength(tenantId) > 120 || !/^[A-Za-z0-9_-]+$/.test(tenantId)) throw new Error(dbError.protocol("tenantId is invalid"));
  const mode = op.ctx.sql.value("SELECT database_mode FROM gonvex_runtime_projects WHERE id=$1 AND status='active'", projectId);
  if (mode === undefined) throw new Error(dbError.projectNotFound(projectId));
  if (str(mode) !== "multiTenant") throw new Error(dbError.protocol("additional tenant shards require a multiTenant project"));
  const existing = op.ctx.sql.get(
    "SELECT database_name,database_alias FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND deleted_at IS NULL",
    projectId,
    tenantId,
  );
  let tenant: Record<string, unknown>;
  if (existing) {
    tenant = { id: tenantId, projectId, name, database: str(existing.database_name), databaseAlias: str(existing.database_alias), status: "active" };
  } else {
    const databaseName = `projects/${projectId}/tenants/${tenantId}.db`;
    const databaseAlias = `tenant_${randomUUID().replaceAll("-", "").slice(0, 12)}`;
    op.ctx.sql.tx(() =>
      op.ctx.sql.run(
        `INSERT INTO gonvex_runtime_tenants
         (relationship_id,project_id,tenant_id,name,database_alias,database_name,database_url,status,description,provisioned,runtime_created)
         VALUES($1,$2,$1,$3,$4,$5,$6,'active','Runtime-created tenant shard.',0,1)
         ON CONFLICT(project_id,tenant_id) DO NOTHING`,
        tenantId,
        projectId,
        name,
        databaseAlias,
        databaseName,
        `do://tenant/${tenantId}`,
      ),
    );
    tenant = { id: tenantId, projectId, name, database: databaseName, databaseAlias, status: "active" };
  }
  await op.provisionTenant(projectId, tenantId, release.planId);
  op.ctx.sql.tx(() => op.ctx.sql.run(`UPDATE gonvex_runtime_tenants SET provisioned=1,updated_at=${NOW} WHERE project_id=$1 AND tenant_id=$2`, projectId, tenantId));
  return tenant;
}

const createTenantShard: Handler = async (op, request) => {
  const body = await Body.read(request);
  const payload = body.object();
  const projectId = (typeof payload.projectId === "string" ? payload.projectId : "").trim();
  const requestedRaw = typeof payload.tenantId === "string" ? payload.tenantId : typeof payload.id === "string" ? payload.id : undefined;
  const requested = requestedRaw?.trim() === "" ? undefined : requestedRaw?.trim();
  const name = (typeof payload.name === "string" ? payload.name : "").trim();
  await op.auth.authorizeProjectResource(request.headers, projectId, "projects:update", true);
  try {
    return json(await provisionRuntimeTenant(op, projectId, requested, name), 201);
  } catch (cause) {
    return error(422, cause instanceof Error ? cause.message : String(cause));
  }
};

const deleteTenantShard: Handler = async (op, request, url, [tenantRaw]) => {
  const project = (url.searchParams.get("project") ?? "").trim();
  const actor = await op.auth.authorize(request.headers, "projects:update");
  if (project === "" || !op.auth.canManageProject(actor, project)) return error(403, "project owner or admin access is required");
  const tenant = tenantRaw!.trim();
  const row = op.ctx.sql.get(
    `SELECT tenant.runtime_created,project.database_mode FROM gonvex_runtime_tenants tenant
     JOIN gonvex_runtime_projects project ON project.id=tenant.project_id
     WHERE tenant.project_id=$1 AND tenant.tenant_id=$2 AND tenant.deleted_at IS NULL AND tenant.status NOT IN('deleted','disabled')`,
    project,
    tenant,
  );
  if (!row) return error(422, dbError.tenantNotFound(project, tenant));
  if (!bool(row.runtime_created) || str(row.database_mode) !== "multiTenant") return error(422, dbError.protocol("only runtime-created multi-tenant shards may be deleted through this operation"));
  op.ctx.sql.tx(() => op.ctx.sql.run(`UPDATE gonvex_runtime_tenants SET status='deleting',updated_at=${NOW} WHERE project_id=$1 AND tenant_id=$2`, project, tenant));
  await op.deleteTenantStorage(tenant);
  op.ctx.sql.tx(() => op.ctx.sql.run("DELETE FROM gonvex_runtime_tenants WHERE project_id=$1 AND tenant_id=$2 AND status='deleting'", project, tenant));
  return json({ ok: true });
};

const e2eMember: Handler = async (op, request) => {
  if (!op.auth.adminKeyMatches(request.headers)) return error(403, "runtime admin key is required");
  const { e2eMember: provision } = await import("../src/dev");
  const body = await Body.read(request);
  const p = body.object();
  const projectId = (typeof p.projectId === "string" ? p.projectId : "").trim();
  const tenantId = (typeof p.tenantId === "string" ? p.tenantId : "").trim();
  const email = (typeof p.email === "string" ? p.email : "").trim().toLowerCase();
  const accountOnly = p.accountOnly === true;
  if (projectId === "" || (!accountOnly && tenantId === "") || email === "" || byteLength(email) > 320) {
    return error(400, "projectId and email are required; tenantId is required unless accountOnly is true");
  }
  try {
    return json(
      await provision(op.ctx, {
        projectId,
        email,
        password: typeof p.password === "string" ? p.password : "",
        ...(accountOnly ? { accountOnly } : { tenantId }),
        ...(typeof p.name === "string" ? { name: p.name } : {}),
      }),
    );
  } catch (cause) {
    const message = (cause as Error).message;
    return message === "test actor account not found" ? error(404, message) : error(422, message);
  }
};

// --------------------------------------------------------------------- manifest

const manifest: Handler = async (op, request, url) => {
  const project = projectHeaderOrQuery(request, url);
  if (project === "") return error(400, "project is required");
  const service = op.ctx.config.servicePrincipals.length > 0 ? await servicePrincipalAccess(op, request, project) : undefined;
  if (service === "denied") return error(403, "service principal is not allowed to read this project's manifest");
  if (service === undefined && !op.auth.projectKeyMatches(request.headers, project)) {
    await op.auth.authorizeProject(request.headers, "projects:read", project, false, "project access is required");
  }
  const release = op.sync.activeRelease(project);
  if (!release) return error(404, "project module is not installed");
  const view = await readView(op, project, release.hash);
  const module = { hash: release.hash, generation: release.generation };
  if (service === "allowed") return json({ project, functions: view.functions, module });
  return json({ project, functions: view.functions, schema: view.schema, visibility: view.visibility, module });
};

async function readView(op: Op, project: string, hash: string): Promise<{ functions: unknown; schema: unknown; visibility: unknown }> {
  const object = await op.releases?.get(releaseKey(project, hash, "view.json"));
  if (!object) throw new HttpError(503, "release view is unavailable");
  return (await object.json()) as { functions: unknown; schema: unknown; visibility: unknown };
}

/** service_principal.rs `manifest_access`. */
async function servicePrincipalAccess(op: Op, request: Request, project: string): Promise<"allowed" | "denied" | undefined> {
  const token = (request.headers.get("authorization") ?? "").trim().replace(/^Bearer /, "");
  if (!token.startsWith("gvx_svc_")) return undefined;
  const hash = sha256Hex(token);
  const principal = op.ctx.config.servicePrincipals.find((p) => p.tokenSha256 === hash);
  if (!principal) return undefined;
  return principal.manifest && principal.projects.includes(project) ? "allowed" : "denied";
}

// ------------------------------------------------------------- metrics, logs, errors

/** operations.rs `metrics_snapshot`, with kernel values where the process ones have no equivalent. */
export function metricsSnapshot(op: Op, project: string): Record<string, unknown> {
  const sql = op.ctx.sql;
  const functions: Record<string, Record<string, unknown>> = {};
  const release = op.sync.activeRelease(project);
  const stored = release?.summary.storedManifest as { functions?: Record<string, { kind?: string }> } | undefined;
  for (const [path, definition] of Object.entries(stored?.functions ?? {})) {
    functions[path] = { kind: definition?.kind ?? "", calls: 0, errors: 0, averageDurationMs: 0, lastDurationMs: 0, series: [] };
  }
  const rows = sql.all(
    `SELECT events.kind,events.path,count(*) AS calls,
            count(*) FILTER(WHERE events.outcome='error') AS errors,
            CAST(COALESCE(avg(events.client_duration_ms),0) AS REAL) AS average_duration_ms,
            CAST(COALESCE((SELECT latest.client_duration_ms FROM gonvex_performance_events latest
               WHERE latest.project_id=events.project_id AND latest.kind=events.kind
                 AND latest.path=events.path AND latest.client_duration_ms IS NOT NULL
                 AND latest.created_at>${NOW}-86400000
               ORDER BY latest.created_at DESC LIMIT 1),0) AS REAL) AS last_duration_ms,
            max(events.created_at) AS last_called_at
     FROM gonvex_performance_events events
     WHERE events.project_id=$1 AND events.created_at>${NOW}-86400000
     GROUP BY events.project_id,events.kind,events.path`,
    project,
  );
  for (const row of rows) {
    const client = { calls: num(row.calls), errors: num(row.errors), averageDurationMs: num(row.average_duration_ms), lastDurationMs: num(row.last_duration_ms), lastCalledAt: isoTimeOrNull(row.last_called_at) };
    functions[str(row.path)] = { ...client, kind: str(row.kind), series: [], client };
  }
  const logs = sql
    .all("SELECT kind,path,outcome,error,client_duration_ms,reason,tenant_id,account_id,created_at,event_id FROM gonvex_performance_events WHERE project_id=$1 ORDER BY created_at DESC LIMIT 200", project)
    .map((row) => logJson(project, row));
  // Scheduled jobs live in tenant objects in the kernel (0012 R6); K11 reports them.
  const counts = op.tenantCounts(project);
  let connections = 0;
  for (const count of counts.values()) connections += count;
  const zeroLane = { active: 0, queueDepth: 0, admitted: 0, waited: 0, cancelled: 0, waitMs: 0, maxWaitMs: 0, tenantsQueued: 0, largestTenantQueue: 0 };
  return {
    generatedAt: new Date().toISOString(),
    functions,
    cache: { hits: 0, misses: 0, bypasses: 0, requests: 0, hitRate: 0, series: [] },
    running: { current: {}, total: 0, series: [] },
    websocket: { connections, tenants: Object.fromEntries(counts), series: [] },
    database: { pools: 0, openConnections: 0, listenerConnections: 0, inUse: 0, idle: 0, maxOpenConnections: 0, waitCount: 0, waitDurationMs: 0, maxWaitDurationMs: 0, series: [] },
    queryAdmission: { enabled: false, totalPermits: 0, active: 0, bootstrapPermits: 0, bootstrapActive: 0, reactive: zeroLane, foreground: zeroLane, bootstrap: zeroLane, reactiveDelayedByBootstrap: 0 },
    scheduler: { running: 0, queued: 0, scheduled: 0, completed: 0, failed: 0, lagMs: 0, crons: [], recent: [], series: [] },
    logs,
    profiler: {},
    memory: {},
    kernel: { activeRelease: release ? { hash: release.hash, generation: release.generation } : null },
  };
}

export function logJson(project: string, row: Row): Record<string, unknown> {
  return {
    time: isoTime(num(row.created_at)),
    operationId: str(row.event_id),
    project,
    tenant: str(row.tenant_id),
    accountId: str(row.account_id),
    path: str(row.path),
    kind: str(row.kind),
    outcome: str(row.outcome),
    durationMs: optNum(row.client_duration_ms) ?? 0,
    error: str(row.error),
    reason: str(row.reason),
    source: "client",
  };
}

const metrics: Handler = async (op, request, url) => {
  if (url.searchParams.get("format") === "profile") return profile(op, request, url, []);
  const project = projectHeaderOrQuery(request, url);
  if (project === "") return error(400, "project is required");
  await op.auth.authorizeProject(request.headers, "projects:read", project, false, "project access is required");
  return json(metricsSnapshot(op, project));
};

/** `/dev/profile`: the Rust process profiler has no equivalent (CONTRACT D.1, defer). */
const profile: Handler = async (op, request) => {
  await op.auth.authorize(request.headers, "projects:read");
  return json({ available: false, reason: "the Rust process profiler has no equivalent in the kernel" });
};

const clearLogs: Handler = async (op, request, url) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:update", true);
  const cleared = op.ctx.sql.tx(() => op.ctx.sql.affected("DELETE FROM gonvex_performance_events WHERE project_id=$1", project));
  return json({ cleared });
};

/** `/dev/cache`: the kernel has no shared live-query cache (CONTRACT D.1, replace: no-op). */
const clearCache: Handler = async (op, request, url) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:update", true);
  return json({ cleared: true, project });
};

/** `/dev/storage/files`: R2 files belong to K13; until it lands the store reports itself unconfigured. */
const storageFiles: Handler = async (op, request, url) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:read", false);
  return json({ configured: false, files: [] });
};

const errorStatus: Handler = async (op, request, url) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProject(request.headers, "projects:read", project, false, "project access is required");
  const enabled = bool(op.ctx.sql.value("SELECT error_tracking_enabled FROM gonvex_runtime_projects WHERE id=$1", project));
  return json({ enabled, project });
};

const errorGroups: Handler = async (op, request, url) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProject(request.headers, "projects:read", project, false, "project access is required");
  const status = (url.searchParams.get("status") ?? "").trim();
  const level = normalizeErrorLevel(url.searchParams.get("level") ?? "");
  const release = (url.searchParams.get("release") ?? "").trim();
  const groups = op.ctx.sql
    .all(
      "SELECT * FROM gonvex_error_groups WHERE project_id=$1 AND ($2='' OR status=$2) AND ($3='' OR level=$3) AND ($4='' OR EXISTS(SELECT 1 FROM json_each(releases) WHERE key=$4)) ORDER BY last_seen DESC LIMIT 500",
      project,
      status,
      level,
      release,
    )
    .map(errorGroupJson);
  const releases = op.ctx.sql
    .all("SELECT release FROM gonvex_error_events WHERE project_id=$1 AND release<>'' GROUP BY release ORDER BY max(occurred_at) DESC,release DESC", project)
    .map((row) => str(row.release));
  return json({ groups, releases });
};

function loadErrorGroup(op: Op, project: string, fingerprint: string): Record<string, unknown> | undefined {
  const row = op.ctx.sql.get("SELECT * FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2", project, fingerprint);
  return row ? errorGroupJson(row) : undefined;
}

const errorGroup: Handler = async (op, request, url, [fingerprint]) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProject(request.headers, "projects:read", project, false, "project access is required");
  const group = loadErrorGroup(op, project, fingerprint!);
  return group ? json(group) : error(404, "error group not found");
};

const updateErrorGroup: Handler = async (op, request, url, [fingerprint]) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProject(request.headers, "projects:update", project, true, "project owner or admin access is required");
  const body = await Body.read(request);
  const status = body.optionalString("status");
  const priority = body.optionalString("priority");
  const assignee = body.maybeString("assignee");
  if (status !== "" && !["unresolved", "resolved", "ignored"].includes(status)) return error(400, "status is invalid");
  if (priority !== "" && !["low", "medium", "high", "critical"].includes(priority)) return error(400, "priority is invalid");
  const row = op.ctx.sql.tx(() =>
    op.ctx.sql.get(
      "UPDATE gonvex_error_groups SET status=CASE WHEN $3='' THEN status ELSE $3 END,priority=CASE WHEN $4='' THEN priority ELSE $4 END,assignee=COALESCE($5,assignee) WHERE project_id=$1 AND fingerprint=$2 RETURNING *",
      project,
      fingerprint,
      status,
      priority,
      assignee === undefined ? null : assignee.trim(),
    ),
  );
  return row ? json(errorGroupJson(row)) : error(404, "error group not found");
};

const errorBugReport: Handler = async (op, request, url, [fingerprint]) => {
  const project = projectHeaderOrQuery(request, url);
  await op.auth.authorizeProject(request.headers, "projects:read", project, false, "project access is required");
  const group = loadErrorGroup(op, project, fingerprint!);
  if (!group) return error(404, "error group not found");
  const title = typeof group.title === "string" ? group.title : "Captured error";
  const count = typeof group.count === "number" ? group.count : 0;
  const latest = (group.latest ?? null) as Record<string, unknown> | null;
  const culprit = typeof group.culprit === "string" ? group.culprit : "";
  const stack = typeof latest?.stack === "string" ? latest.stack : "";
  const markdown = `## ${title}\n\n**Fingerprint:** \`${fingerprint}\`\n**Impact:** ${count} events\n**Likely source:** \`${culprit}\`\n\n### Error\n\`\`\`\n${stack}\n\`\`\`\n\n### Acceptance criteria\n- Reproduce or verify the failing path.\n- Add a regression test that fails before the fix.\n- Fix the root cause without suppressing unrelated errors.\n- Verify the fix against the affected release and tenant context.\n`;
  return json({
    title,
    markdown,
    agentContext: {
      fingerprint,
      project,
      tenantImpact: group.tenants ?? null,
      accountImpact: group.accounts ?? null,
      deviceImpact: group.devices ?? null,
      release: latest?.release ?? null,
      culprit,
      stack,
      breadcrumbs: latest?.breadcrumbs ?? null,
      context: latest?.context ?? null,
    },
  });
};

// ---------------------------------------------------------- data (operator_data.rs)

function dataProject(request: Request, url: URL): string {
  return (request.headers.get("x-gonvex-project-id") ?? "").trim() || (url.searchParams.get("project") ?? "").trim();
}

function dataTenantParam(request: Request, url: URL): string {
  return (request.headers.get("x-gonvex-tenant-id") ?? "").trim() || (url.searchParams.get("tenant") ?? "").trim();
}

/** `data_database`: the project's own database for an empty tenant, else the tenant's. */
function dataTenant(op: Op, project: string, tenant: string): string {
  if (project === "") throw new HttpError(400, "project is required");
  const resolved = resolveTenant(op, project, tenant === "" ? project : tenant);
  if (!resolved) throw new HttpError(404, "tenant database was not found");
  return resolved;
}

/** serde `i64` query fields: absent is 0, anything unparsable is axum's 400. */
function intParam(url: URL, name: string): number {
  const raw = url.searchParams.get(name);
  if (raw === null || raw === "") return 0;
  if (!/^[+-]?\d+$/.test(raw)) throw new PlainError(400, `Failed to deserialize query string: ${name}: invalid digit found in string`);
  return Number(raw);
}

const listTables: Handler = async (op, request, url) => {
  const project = dataProject(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:read", false);
  const tenant = dataTenant(op, project, dataTenantParam(request, url));
  return json({ tables: unwrap(await op.tenant(tenant).operatorListTables()) });
};

const readRows: Handler = async (op, request, url, [table]) => {
  const project = dataProject(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:read", false);
  if (internalTable(table!)) return error(404, "table not found");
  if (!validIdent(table!)) return error(400, "invalid table name");
  const query: RowsQuery = {
    limit: intParam(url, "limit"),
    offset: intParam(url, "offset"),
    search: url.searchParams.get("search") ?? "",
    sort: url.searchParams.get("sort") ?? "",
    direction: url.searchParams.get("direction") ?? "",
    filters: url.searchParams.get("filters") ?? "",
    columns: url.searchParams.get("columns") ?? "",
    count: url.searchParams.get("count") ?? "",
  };
  const tenant = dataTenant(op, project, dataTenantParam(request, url));
  return json(unwrap(await op.tenant(tenant).operatorReadRows(table!, query)));
};

const insertRow: Handler = async (op, request, url, [table]) => {
  const project = dataProject(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:update", true);
  if (internalTable(table!)) return error(404, "table not found");
  if (!validIdent(table!)) return error(400, "invalid table name");
  const body = await Body.read(request);
  const payload = body.value;
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return error(400, "row must be an object");
  if (Object.keys(payload).length === 0) return error(400, "no values provided");
  const tenant = dataTenant(op, project, dataTenantParam(request, url));
  const row = unwrap(await op.tenant(tenant).operatorInsertRow(table!, payload as Record<string, unknown>));
  return json({ table, row }, 201);
};

const updateRow: Handler = async (op, request, url, [table, rowId]) => {
  const project = dataProject(request, url);
  await op.auth.authorizeProjectResource(request.headers, project, "projects:update", true);
  if (internalTable(table!)) return error(404, "table not found");
  if (!validIdent(table!)) return error(400, "invalid table name");
  const body = await Body.read(request);
  const payload = body.value;
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return error(400, "row patch must be an object");
  const values = Object.keys(payload);
  if (values.length === 0) return error(400, "at least one value is required");
  if (values.some((column) => ["_id", "id", "tenantId", "tenant_id"].includes(column))) return error(400, "identity and tenant columns cannot be changed");
  const tenant = dataTenant(op, project, dataTenantParam(request, url));
  const row = unwrap(await op.tenant(tenant).operatorUpdateRow(table!, rowId!, payload as Record<string, unknown>));
  return json({ table, row });
};

const deleteRow: Handler = async (op, request, url, [table, rowId]) => {
  const project = dataProject(request, url);
  if (!op.auth.adminKeyMatches(request.headers)) return error(403, "runtime admin key is required");
  if (project === "" || internalTable(table!)) return error(404, "table not found");
  if (!validIdent(table!)) return error(400, "invalid table name");
  const tenant = dataTenant(op, project, dataTenantParam(request, url));
  const row = unwrap(await op.tenant(tenant).operatorDeleteRow(table!, rowId!));
  return json({ table, row });
};

const replaceReferences: Handler = async (op, request, url) => {
  if (!op.auth.adminKeyMatches(request.headers)) return error(403, "runtime admin key is required");
  const body = await Body.read(request);
  const raw = body.object().replacements;
  if (!raw || typeof raw !== "object" || Array.isArray(raw) || Object.values(raw).some((v) => typeof v !== "string")) {
    throw new PlainError(422, "Failed to deserialize the JSON body into the target type: replacements: invalid type, expected a map of strings");
  }
  const replacements = raw as Record<string, string>;
  const dryRun = body.maybeBool("dryRun") ?? false;
  const entries = Object.entries(replacements);
  if (entries.length === 0) return error(400, "at least one replacement is required");
  if (entries.length > 10_000) return error(400, "too many replacements");
  if (entries.some(([from, to]) => from.trim() === "" || to.trim() === "")) return error(400, "replacement ids cannot be empty");
  const effective = Object.fromEntries(entries.filter(([from, to]) => from.trim() !== to.trim()));
  if (Object.keys(effective).length === 0) return error(400, "at least one replacement is required");
  const project = dataProject(request, url);
  const tenant = dataTenant(op, project, dataTenantParam(request, url));
  return json(unwrap(await op.tenant(tenant).operatorReplaceReferences(effective, dryRun)));
};

// ---------------------------------------------------------------------- table

const SEG = "([^/]+)";
export const ROUTES: Route[] = [
  { pattern: /^\/dev\/auth\/login$/, methods: { POST: login } },
  { pattern: /^\/dev\/auth\/me$/, methods: { GET: me } },
  { pattern: /^\/dev\/auth\/tokens$/, methods: { GET: listTokens, POST: createToken } },
  { pattern: new RegExp(`^/dev/auth/tokens/${SEG}$`), methods: { DELETE: revokeToken } },
  { pattern: /^\/dev\/auth\/accounts$/, methods: { GET: listAccounts, POST: createAccount } },
  { pattern: /^\/dev\/auth\/notifications$/, methods: { GET: listNotifications } },
  { pattern: /^\/dev\/auth\/notifications\/read$/, methods: { POST: readNotifications } },
  { pattern: /^\/dev\/manifest$/, methods: { GET: manifest } },
  { pattern: /^\/dev\/metrics$/, methods: { GET: metrics } },
  { pattern: /^\/dev\/profile$/, methods: { GET: profile } },
  { pattern: /^\/dev\/logs$/, methods: { DELETE: clearLogs } },
  { pattern: /^\/dev\/cache$/, methods: { DELETE: clearCache } },
  { pattern: /^\/dev\/storage\/files$/, methods: { GET: storageFiles } },
  { pattern: /^\/dev\/projects$/, methods: { GET: listProjects, POST: createProject } },
  { pattern: new RegExp(`^/dev/projects/${SEG}$`), methods: { PATCH: updateProject, DELETE: deleteProject } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/key$`), methods: { POST: projectKey } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/key/rotate$`), methods: { POST: rotateProjectKey } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/members$`), methods: { GET: projectMembers, POST: upsertProjectMember } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/invitations$`), methods: { POST: createProjectInvitation } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/env$`), methods: { GET: getEnv, POST: setEnv, PUT: replaceEnv, DELETE: deleteEnv } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/auth/providers/${SEG}$`), methods: { GET: getAuthProvider, PUT: putAuthProvider } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/auth/google$`), methods: { GET: getGoogle, PUT: putGoogle, DELETE: deleteGoogle } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/auth/accounts$`), methods: { GET: projectAuthAccounts } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/auth/accounts/${SEG}$`), methods: { PATCH: updateProjectAuthAccount, DELETE: deleteProjectAuthAccount } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/auth/memberships$`), methods: { GET: projectAuthMemberships, PUT: putProjectAuthMembership, DELETE: deleteProjectAuthMembership } },
  { pattern: new RegExp(`^/dev/projects/${SEG}/auth/tenants$`), methods: { GET: projectAuthTenants, POST: createProjectAuthTenant } },
  { pattern: /^\/dev\/tenants$/, methods: { GET: listTenants, POST: createTenantShard } },
  { pattern: new RegExp(`^/dev/tenants/${SEG}$`), methods: { DELETE: deleteTenantShard } },
  { pattern: /^\/dev\/internal\/e2e\/members$/, methods: { POST: e2eMember } },
  { pattern: /^\/dev\/errors\/status$/, methods: { GET: errorStatus } },
  { pattern: /^\/dev\/errors\/groups$/, methods: { GET: errorGroups } },
  { pattern: new RegExp(`^/dev/errors/groups/${SEG}$`), methods: { GET: errorGroup, PATCH: updateErrorGroup } },
  { pattern: new RegExp(`^/dev/errors/groups/${SEG}/bug-report$`), methods: { GET: errorBugReport } },
  { pattern: /^\/dev\/data\/tables$/, methods: { GET: listTables } },
  { pattern: new RegExp(`^/dev/data/tables/${SEG}/rows$`), methods: { GET: readRows, POST: insertRow } },
  { pattern: new RegExp(`^/dev/data/tables/${SEG}/rows/${SEG}$`), methods: { PATCH: updateRow, DELETE: deleteRow } },
  { pattern: /^\/dev\/data\/references\/replace$/, methods: { POST: replaceReferences } },
];

/**
 * Dispatches one operator request. Unknown paths are 404 and a wrong method
 * is 405 with `allow`, both with an empty body (axum's router has no fallback).
 */
export async function handleOperatorRoute(op: Op, request: Request): Promise<Response> {
  const url = new URL(request.url);
  for (const route of ROUTES) {
    const match = route.pattern.exec(url.pathname);
    if (!match) continue;
    const method = request.method === "HEAD" ? "GET" : request.method;
    const handler = route.methods[method];
    if (!handler) return new Response(null, { status: 405, headers: { allow: Object.keys(route.methods).join(",") } });
    const params = match.slice(1).map((segment) => decodeURIComponent(segment));
    try {
      return await handler(op, request, url, params);
    } catch (cause) {
      if (cause instanceof PlainError) return plainResponse(cause);
      if (cause instanceof HttpError) return error(cause.status, cause.message, cause.extra);
      return error(500, cause instanceof Error ? cause.message : String(cause));
    }
  }
  return new Response(null, { status: 404 });
}
