/**
 * Seeding for tests and the conformance adapter, equivalent to the
 * reference's development routes (lib.rs:579-586, 935-1360; operations.rs
 * upsert_project_member, put_google_auth). They are not `control.*` contract;
 * the operator routes themselves belong to K19/K20. The test worker exposes
 * them behind an admin key.
 */
import type { Ctx } from "./context";
import { resolveTenant, tenantCall, tenantDatabaseUrl } from "./directory";
import { normalizeRedirect } from "./native-auth";
import { hashPassword } from "./password";
import { NOW_MS, str } from "./sql";
import { randomBytes } from "node:crypto";
import { uuidSimple } from "./util";

/**
 * `POST /dev/projects` (lib.rs:935, provision.rs:434). The reference records
 * the operator as owner; with the admin key that is `admin@gonvex.local`.
 */
export function createProject(ctx: Ctx, input: { name: string; databaseMode?: string; ownerEmail?: string; authMode?: string; id?: string }): { projectId: string; projectKey: string } {
  const projectId = input.id ?? crypto.randomUUID();
  const projectKey = `gvx_project_${Buffer.from(randomBytes(24)).toString("base64url")}`;
  const owner = (input.ownerEmail ?? "admin@gonvex.local").trim().toLowerCase();
  ctx.sql.tx(() => {
    ctx.sql.run(
      `INSERT INTO gonvex_runtime_projects(id,name,database_mode,project_key,owner_email,auth_mode,description) VALUES($1,$2,$3,$4,$5,$6,'Runtime-created project database.')`,
      projectId,
      input.name,
      input.databaseMode ?? "multiTenant",
      projectKey,
      owner,
      input.authMode ?? "gonvex-native",
    );
    if (owner) {
      ctx.sql.run(
        `INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,lower($2),$3,'owner')
         ON CONFLICT(project_id,email) DO UPDATE SET role='owner'`,
        projectId,
        owner,
        owner.split("@")[0],
      );
    }
  });
  return { projectId, projectKey };
}

/** Records a deployed manifest (`/dev/sync` stores it in `gonvex_runtime_manifests`; K19 owns the rest). */
export function saveManifest(ctx: Ctx, projectId: string, manifest: Record<string, unknown>): { ok: true; functionCount: number } {
  const module = (manifest.module ?? {}) as Record<string, unknown>;
  const hash = typeof module.hash === "string" ? module.hash : "";
  ctx.sql.tx(() =>
    ctx.sql.run(
      `INSERT INTO gonvex_runtime_manifests(project_id,manifest,module_hash,updated_at) VALUES($1,$2,$3,${NOW_MS})
       ON CONFLICT(project_id) DO UPDATE SET manifest=excluded.manifest,module_hash=excluded.module_hash,updated_at=excluded.updated_at`,
      projectId,
      JSON.stringify(manifest),
      hash,
    ),
  );
  const functions = manifest.functions;
  return { ok: true, functionCount: Array.isArray(functions) ? functions.length : functions && typeof functions === "object" ? Object.keys(functions).length : 0 };
}

/** `/dev/tenants` POST: a runtime-created tenant shard, provisioned. */
export async function createTenant(ctx: Ctx, projectId: string, tenantId: string, name: string): Promise<{ tenantId: string }> {
  const url = tenantDatabaseUrl(tenantId);
  ctx.sql.tx(() =>
    ctx.sql.run(
      `INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_name,database_url,status,description,provisioned,runtime_created)
       VALUES($1,$2,$1,$3,$4,$5,'active','Runtime-created tenant shard.',TRUE,TRUE)`,
      tenantId,
      projectId,
      name,
      `gonvex_tenant_${tenantId}`,
      url,
    ),
  );
  await tenantCall(ctx, { projectId, tenantId, databaseUrl: url }, (port) => port.provision(projectId, tenantId));
  return { tenantId };
}

/** `/dev/internal/e2e/members` (lib.rs:1156-1360). */
export async function e2eMember(
  ctx: Ctx,
  input: { projectId: string; tenantId?: string; email: string; password: string; name?: string; accountOnly?: boolean },
): Promise<Record<string, string>> {
  const email = input.email.trim().toLowerCase();
  const projectId = input.projectId.trim();
  const account = ctx.sql.tx(() => {
    const mode = ctx.sql.value<string>("SELECT COALESCE(NULLIF(auth_mode,''),'gonvex-native') FROM gonvex_runtime_projects WHERE id=$1 AND status='active'", projectId);
    if (mode === undefined) throw new Error("test actor account not found");
    let row = ctx.sql.get("SELECT id,name,avatar_url FROM accounts WHERE auth_realm_id=$1 AND lower(email)=lower($2) AND disabled_at IS NULL", projectId, email);
    if (!row) {
      const passwordIdentity = mode === "gonvex-native" || mode === "hybrid";
      const id = `acct_${uuidSimple()}`;
      const name = input.name?.trim() || email.split("@")[0]!;
      ctx.sql.run(`INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at) VALUES($1,$2,$3,$4,NULL,${NOW_MS})`, id, projectId, email, name);
      if (passwordIdentity) {
        ctx.sql.run(
          `INSERT INTO account_identities(project_id,account_id,provider,issuer,subject,email,verified_email,updated_at)
           VALUES($1,$2,'password',$1,$3,$3,1,${NOW_MS})`,
          projectId,
          id,
          email,
        );
        ctx.sql.run("INSERT INTO gonvex_account_passwords(project_id,account_id,password_hash) VALUES($1,$2,$3)", projectId, id, hashPassword(input.password));
      }
      row = ctx.sql.get("SELECT id,name,avatar_url FROM accounts WHERE id=$1", id)!;
    }
    return { id: str(row.id), name: str(row.name), avatarUrl: str(row.avatar_url) };
  });
  if (input.accountOnly) return { projectId, accountId: account.id };
  const tenantId = (input.tenantId ?? "").trim();
  const route = resolveTenant(ctx, projectId, tenantId);
  const written = await tenantCall(ctx, route, (port) =>
    port.writeMembership({ kind: "e2eMember", commandId: `e2e-member:${account.id}`, accountId: account.id, displayName: account.name, avatarUrl: account.avatarUrl }),
  );
  ctx.sql.tx(() =>
    ctx.sql.run(
      `INSERT INTO account_tenant_index(account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
       VALUES($1,$2,$3,'active',$4,${NOW_MS})
       ON CONFLICT(account_id,tenant_id) DO UPDATE SET member_id=EXCLUDED.member_id,status='active',
         tenant_membership_revision=max(account_tenant_index.tenant_membership_revision,EXCLUDED.tenant_membership_revision),updated_at=${NOW_MS}`,
      account.id,
      tenantId,
      written.memberId,
      written.revision,
    ),
  );
  return { projectId, tenantId, accountId: account.id, memberId: written.memberId };
}

/** `POST /dev/projects/{p}/members` (operations.rs:474). */
export function upsertProjectMember(ctx: Ctx, projectId: string, input: { email: string; name?: string; role?: string }): Record<string, string> {
  const email = input.email.trim().toLowerCase();
  const role = !input.role || input.role.trim() === "dev" ? "dev" : input.role.trim();
  if (!["owner", "admin", "dev"].includes(role)) throw new Error("role must be owner, admin, or dev");
  ctx.sql.tx(() =>
    ctx.sql.run(
      `INSERT INTO gonvex_project_members(project_id,email,name,role) VALUES($1,$2,$3,$4)
       ON CONFLICT(project_id,email) DO UPDATE SET name=EXCLUDED.name,role=EXCLUDED.role`,
      projectId,
      email,
      (input.name ?? "").trim(),
      role,
    ),
  );
  return { email, name: (input.name ?? "").trim(), role };
}

/** `PUT /dev/projects/{p}/auth/google` (operations.rs:2677). */
export function putGoogleAuth(ctx: Ctx, projectId: string, input: { redirectUri: string; signupMode?: string }): void {
  const redirect = normalizeRedirect(input.redirectUri);
  const signup = input.signupMode || "personal";
  if (signup !== "personal" && signup !== "inviteOnly") throw new Error("signupMode must be personal or inviteOnly");
  ctx.sql.tx(() => {
    ctx.sql.run(
      `INSERT INTO gonvex_auth_providers(project_id,provider,enabled,signup_mode) VALUES($1,'google',TRUE,$2)
       ON CONFLICT(project_id,provider) DO UPDATE SET enabled=TRUE,signup_mode=EXCLUDED.signup_mode,updated_at=${NOW_MS}`,
      projectId,
      signup,
    );
    ctx.sql.run("INSERT INTO gonvex_auth_redirect_uris(project_id,provider,redirect_uri) VALUES($1,'google',$2) ON CONFLICT DO NOTHING", projectId, redirect);
    ctx.sql.run(
      `UPDATE gonvex_runtime_projects SET auth_mode=CASE WHEN auth_mode IN('firebase','external-oidc') THEN 'hybrid' ELSE 'gonvex-native' END,updated_at=${NOW_MS} WHERE id=$1`,
      projectId,
    );
  });
}
