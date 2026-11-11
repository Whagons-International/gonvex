// Operator routes against the reference's statuses and texts (operations.rs, lib.rs, D.3).
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { OperatorControlObject } from "../../../control/operator/object.ts";
import { ADMIN, call, control, createProject, manifest, sync, useBuilder } from "./helpers.ts";

const CONFIGURED = { email: "operator@example.test", password: "configured-password-123" };

async function login(email: string, password: string) {
  return call("POST", "/dev/auth/login", { token: null, body: { email, password } });
}

async function sql(query: string, ...params: unknown[]) {
  return runInDurableObject(control(), async (instance: OperatorControlObject) => instance.sql.all(query, ...params));
}

describe("operator authentication (D.3)", () => {
  it("refuses anonymous callers: Local is never on unless GONVEX_LOCAL_OPERATOR=true", async () => {
    const reply = await call("GET", "/dev/projects", { token: null });
    expect([reply.status, reply.body]).toEqual([401, { error: "dashboard sign-in or personal access token is required" }]);
  });

  it("signs in the configured account and the database accounts, with HMAC sessions", async () => {
    const configured = await login("  Operator@Example.test ", CONFIGURED.password);
    expect(configured.status).toBe(200);
    const session = configured.body.session;
    expect(Object.keys(session)).toEqual(["email", "name", "role", "provider", "expiresAt", "accessToken", "sessionRevision", "authority"]);
    expect(session).toMatchObject({ email: CONFIGURED.email, name: "operator", role: "admin", provider: "gonvex", authority: "configured" });
    const me = await call("GET", "/dev/auth/me", { token: session.accessToken });
    expect(me.body).toEqual({ account: { email: CONFIGURED.email, name: "operator", role: "admin" }, authentication: "session", permissions: ["*"] });
    expect((await login(CONFIGURED.email, "wrong")).status).toBe(401);
    expect((await login("", "")).body).toEqual({ error: "email and password are required" });

    // A session-based admin is not a global admin: listing accounts needs admin:projects AND role admin (passes), tenants without project needs global.
    const tenants = await call("GET", "/dev/tenants", { token: session.accessToken });
    expect([tenants.status, tenants.body]).toEqual([400, { error: "project is required for non-global credentials" }]);

    const created = await call("POST", "/dev/auth/accounts", { body: { email: "Dev@Example.test", name: "", password: "short", role: "standard" } });
    expect([created.status, created.body]).toEqual([400, { error: "email and a password of at least 12 characters are required" }]);
    const ok = await call("POST", "/dev/auth/accounts", { body: { email: "Dev@Example.test", name: "", password: "a-long-password-1", role: "" } });
    expect([ok.status, ok.body]).toEqual([201, { account: { email: "dev@example.test", name: "dev", role: "standard" } }]);
    const dev = await login("dev@example.test", "a-long-password-1");
    expect(dev.body.session).toMatchObject({ authority: "database", role: "standard" });
    const denied = await call("GET", "/dev/auth/accounts", { token: dev.body.session.accessToken });
    expect([denied.status, denied.body]).toEqual([403, { error: "admin access is required" }]);
    const accounts = await call("GET", "/dev/auth/accounts");
    expect(accounts.body.accounts).toEqual([{ email: "dev@example.test", name: "dev", role: "standard" }]);
    // A changed revision (password reset) invalidates the session.
    await sql("UPDATE gonvex_dashboard_accounts SET session_revision='rotated' WHERE email='dev@example.test'");
    expect((await call("GET", "/dev/auth/me", { token: dev.body.session.accessToken })).status).toBe(401);
  });

  it("personal access tokens: own list, bounded grants, permission checks, revocation", async () => {
    const created = await call("POST", "/dev/auth/tokens", { body: { name: "ci", permissions: ["projects:read", "PROJECTS:READ", "tokens:*"] } });
    expect(created.status).toBe(201);
    expect(created.body.token).toMatchObject({ name: "ci", permissions: ["projects:read", "tokens:*"], expiresAt: null });
    expect(created.body.accessToken).toMatch(/^gvx_pat_[0-9a-f-]{36}\.[A-Za-z0-9_-]{43}$/);
    const token = created.body.accessToken;
    const me = await call("GET", "/dev/auth/me", { token });
    expect(me.body).toEqual({ account: { email: "admin@gonvex.local", name: "Gonvex Admin", role: "admin" }, authentication: "personalAccessToken", permissions: ["projects:read", "tokens:*"] });
    const forbidden = await call("POST", "/dev/projects", { token, body: { name: "nope" } });
    expect([forbidden.status, forbidden.body]).toEqual([403, { error: "personal access token does not grant the required permission", permission: "projects:create" }]);
    const escalate = await call("POST", "/dev/auth/tokens", { token, body: { name: "x", permissions: ["projects:delete"] } });
    expect([escalate.status, escalate.body]).toEqual([400, { error: "current credential cannot grant the requested permission" }]);
    const global = await call("POST", "/dev/auth/tokens", { token, body: { name: "x", permissions: ["*"] } });
    expect(global.body).toEqual({ error: "current credential cannot grant global administration" });
    expect((await call("POST", "/dev/auth/tokens", { body: { name: "x", permissions: ["nope"] } })).body).toEqual({ error: "token permissions are invalid" });
    expect((await call("POST", "/dev/auth/tokens", { body: { name: "x", expiresAt: "2001-01-01T00:00:00Z" } })).body).toEqual({ error: "expiresAt must be a future RFC3339 timestamp" });
    const list = await call("GET", "/dev/auth/tokens", { token });
    expect(list.body.tokens.map((t: { name: string }) => t.name)).toEqual(["ci"]);
    expect(list.body.tokens[0].lastUsedAt).not.toBeNull();
    expect((await call("DELETE", `/dev/auth/tokens/${created.body.token.id}`)).body).toEqual({ ok: true });
    expect((await call("DELETE", `/dev/auth/tokens/${created.body.token.id}`)).status).toBe(404);
    expect((await call("GET", "/dev/auth/me", { token })).status).toBe(401);
  });

  it("notifications are per operator and can be marked read", async () => {
    await sql("INSERT INTO gonvex_dashboard_notifications(id,email,kind,title,body,project_id,metadata,created_at) VALUES('n1','admin@gonvex.local','deploy','Deployed','body','p1','{\"a\":1}',1000),('n2','admin@gonvex.local','deploy','Second','','p1','{}',2000),('n3','other@x.test','deploy','Other','','p1','{}',3000)");
    const list = await call("GET", "/dev/auth/notifications");
    expect(list.body).toEqual({
      notifications: [
        { id: "n2", kind: "deploy", title: "Second", body: "", projectId: "p1", metadata: {}, read: false, createdAt: "1970-01-01T00:00:02Z" },
        { id: "n1", kind: "deploy", title: "Deployed", body: "body", projectId: "p1", metadata: { a: 1 }, read: false, createdAt: "1970-01-01T00:00:01Z" },
      ],
      unread: 2,
    });
    expect((await call("POST", "/dev/auth/notifications/read", { body: { ids: ["n1"] } })).body).toEqual({ ok: true });
    expect((await call("GET", "/dev/auth/notifications")).body.unread).toBe(1);
  });
});

describe("projects, keys, members, env", () => {
  it("creates, lists by access, updates, rotates keys and deletes", async () => {
    const created = await call("POST", "/dev/projects", { body: { name: "Alpha", databaseMode: "multiTenant" } });
    expect(created.status).toBe(201);
    const id = created.body.project.id;
    expect(created.body).toEqual({
      project: { id, name: "Alpha", environment: "development", database: `projects/${id}/project.db`, databaseMode: "multiTenant", storageBucket: `${id}-dev`, status: "active", description: "Runtime-created project database." },
      projectKey: created.body.projectKey,
    });
    expect(atob(created.body.projectKey.slice(4).split(".")[0].replace(/-/g, "+").replace(/_/g, "/"))).toBe(id);
    expect((await call("POST", "/dev/projects", { body: { name: "x", databaseMode: "bad" } })).body).toEqual({ error: "encountered unexpected or invalid data: databaseMode must be single or multiTenant" });

    // A database operator sees only projects it owns or is a member of.
    await call("POST", "/dev/auth/accounts", { body: { email: "member@example.test", name: "Member", password: "member-password-1", role: "standard" } });
    const member = (await login("member@example.test", "member-password-1")).body.session.accessToken;
    expect((await call("GET", "/dev/projects", { token: member })).body.projects.some((p: { id: string }) => p.id === id)).toBe(false);
    expect((await call("POST", `/dev/projects/${id}/members`, { token: member, body: { email: "x@y.z" } })).body).toEqual({ error: "project owner or admin access is required" });
    const added = await call("POST", `/dev/projects/${id}/members`, { body: { email: " Member@Example.test ", name: "Mem", role: "" } });
    expect(added.body).toEqual({ member: { email: "member@example.test", name: "Mem", role: "dev", updatedBy: "admin@gonvex.local" } });
    expect((await call("POST", `/dev/projects/${id}/members`, { body: { email: "a@b.c", role: "boss" } })).body).toEqual({ error: "role must be owner, admin, or dev" });
    const visible = await call("GET", "/dev/projects", { token: member });
    expect(visible.body.projects.map((p: { id: string }) => p.id)).toContain(id);
    expect(visible.body.projects.find((p: { id: string }) => p.id === id)).toMatchObject({ provisioned: true, runtimeCreated: true, testTab: false, errorTrackingEnabled: false, ownerEmail: "admin@gonvex.local" });
    // A dev member can read but not manage.
    expect((await call("PATCH", `/dev/projects/${id}`, { token: member, body: { name: "x" } })).status).toBe(403);
    const members = await call("GET", `/dev/projects/${id}/members`, { token: member });
    expect(members.body.members).toEqual([
      { email: "member@example.test", name: "Mem", role: "dev" },
      { email: "admin@gonvex.local", name: "admin", role: "owner" },
    ]);
    const invitation = await call("POST", `/dev/projects/${id}/invitations`, { body: { email: "Invitee@x.test", role: "admin" } });
    expect(invitation.status).toBe(201);
    expect(invitation.body.invitation).toMatchObject({ projectId: id, email: "invitee@x.test", role: "admin", accepted: false });
    expect(invitation.body.invitation.token).toMatch(/^invite_/);

    expect((await call("PATCH", `/dev/projects/${id}`, { body: {} })).body).toEqual({ error: "no project fields provided" });
    expect((await call("PATCH", `/dev/projects/${id}`, { body: { status: "disabled" } })).body).toEqual({ error: "status may only promote a project to active" });
    const renamed = await call("PATCH", `/dev/projects/${id}`, { body: { name: " Beta ", errorTrackingEnabled: true } });
    expect(renamed.body.project).toMatchObject({ name: "Beta", errorTrackingEnabled: true });

    const key = await call("POST", `/dev/projects/${id}/key`, { token: member });
    expect(key.body).toEqual({ projectKey: created.body.projectKey });
    const rotated = await call("POST", `/dev/projects/${id}/key/rotate`);
    expect(rotated.body.projectKey).not.toBe(created.body.projectKey);
    expect((await call("GET", `/dev/projects/${id}/env`, { token: created.body.projectKey })).status).toBe(401);
    expect((await call("GET", `/dev/projects/${id}/env`, { token: rotated.body.projectKey })).status).toBe(200);

    expect((await call("DELETE", `/dev/projects/${id}`)).body).toEqual({ ok: true });
    expect((await call("GET", "/dev/projects")).body.projects.some((p: { id: string }) => p.id === id)).toBe(false);
  });

  it("env: the project key reads and writes; dotenv replace; names validated", async () => {
    const project = await createProject("env-project");
    const asKey = { token: project.key };
    expect((await call("POST", `/dev/projects/${project.id}/env`, { ...asKey, body: { name: "1BAD", value: "x" } })).body).toEqual({ error: "variable name is invalid" });
    expect((await call("POST", `/dev/projects/${project.id}/env`, { ...asKey, body: { name: "API_URL", value: "https://a" } })).body).toEqual({ ok: true });
    const replaced = await call("PUT", `/dev/projects/${project.id}/env`, {
      ...asKey,
      body: { content: "# comment\nexport TOKEN='abc'\nQUOTED=\"x y\"\nbad-name=1\n\nEMPTY=\n", variables: [{ name: "EXTRA", value: "1" }, { name: "9no", value: "x" }] },
    });
    expect(replaced.body).toEqual({ ok: true, count: 4 });
    const env = await call("GET", `/dev/projects/${project.id}/env`, asKey);
    expect(env.body.variables.map((v: { name: string; value: string; source: string }) => [v.name, v.value, v.source])).toEqual([
      ["EMPTY", "", "project"],
      ["EXTRA", "1", "project"],
      ["QUOTED", "x y", "project"],
      ["TOKEN", "abc", "project"],
    ]);
    expect((await call("DELETE", `/dev/projects/${project.id}/env`, { ...asKey, body: { name: "TOKEN" } })).body).toEqual({ ok: true });
    expect((await call("GET", `/dev/projects/${project.id}/env`, asKey)).body.variables).toHaveLength(3);
    // The project key does not manage members or keys.
    expect((await call("POST", `/dev/projects/${project.id}/key/rotate`, asKey)).status).toBe(401);
  });

  it("auth configuration routes: providers and Google", async () => {
    const project = await createProject("auth-config");
    expect((await call("GET", `/dev/projects/${project.id}/auth/providers/myspace`)).body).toEqual({ error: "provider is unsupported" });
    expect((await call("GET", `/dev/projects/${project.id}/auth/providers/firebase`)).body).toEqual({ error: "provider is not configured" });
    expect((await call("PUT", `/dev/projects/${project.id}/auth/providers/firebase`, { body: {} })).body).toEqual({ error: "firebaseProjectId is required" });
    expect((await call("PUT", `/dev/projects/${project.id}/auth/providers/external-oidc`, { body: { issuer: "http://x", audience: "a", jwksUrl: "https://j" } })).body).toEqual({
      error: "issuer and jwksUrl must use https without credentials",
    });
    const put = await call("PUT", `/dev/projects/${project.id}/auth/providers/firebase`, { body: { firebaseProjectId: "fb-1", adminCredentials: "{\"secret\":1}" } });
    expect(put.body).toEqual({ provider: "firebase", authMode: "firebase", enabled: true, signupMode: "personal", updated: true });
    const got = await call("GET", `/dev/projects/${project.id}/auth/providers/firebase`);
    expect(got.body).toMatchObject({ provider: "firebase", authMode: "firebase", firebaseProjectId: "fb-1", hasAdminCredentials: true, hasClientSecret: false });
    const google = await call("PUT", `/dev/projects/${project.id}/auth/google`, { body: { redirectUri: "https://app.example.test/auth/callback" } });
    expect(google.body).toEqual({
      enabled: true,
      signupMode: "personal",
      redirectUris: ["https://app.example.test/auth/callback"],
      brokerCallbackUrl: "",
      ready: false,
      issues: ["Google client ID and secret are not configured", "GONVEX_AUTH_URL is required"],
    });
    expect((await call("GET", `/dev/projects/${project.id}/auth/providers/firebase`)).body.authMode).toBe("hybrid");
    expect((await call("DELETE", `/dev/projects/${project.id}/auth/google?redirect_uri=${encodeURIComponent("https://app.example.test/auth/callback")}`)).body).toEqual({ ok: true });
    expect((await call("GET", `/dev/projects/${project.id}/auth/google`)).body.redirectUris).toEqual([]);
  });
});

describe("tenants and memberships", () => {
  it("lists, creates with provisioning, invites, revokes and deletes tenant shards", async () => {
    await useBuilder();
    const project = await createProject("tenants-project");
    expect((await sync(project.id, await manifest(project.id), project.key)).status).toBe(200);
    const created = await call("POST", `/dev/projects/${project.id}/auth/tenants`, { body: { name: "Acme", ownerEmail: "Owner@Acme.test" } });
    expect(created.status).toBe(201);
    const tenantId = created.body.tenant.id;
    const list = await call("GET", `/dev/tenants?project=${project.id}`);
    expect(list.body.tenants).toEqual([
      { id: tenantId, projectId: project.id, name: "Acme", databaseAlias: created.body.tenant.databaseAlias, database: `projects/${project.id}/tenants/${tenantId}.db`, status: "active", description: "Runtime-created tenant shard.", domain: "", timezone: "UTC", profile: {}, provisioned: true },
    ]);
    const realm = await call("GET", `/dev/projects/${project.id}/auth/tenants`, { token: project.key });
    expect(realm.body).toEqual({ tenants: [{ id: tenantId, name: "Acme", memberCount: 0 }] });
    const memberships = await call("GET", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}`);
    expect(memberships.body.members).toEqual([]);
    expect(memberships.body.invitations.map((i: { email: string; role: string }) => [i.email, i.role])).toEqual([["owner@acme.test", "owner"]]);
    expect((await call("PUT", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}`, { body: { email: "m@acme.test", role: "boss" } })).body).toEqual({ error: "invalid member role" });
    expect((await call("PUT", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}`, { body: { email: "m@acme.test", permissions: [] } })).body).toEqual({ error: "permissions must be an object" });
    expect((await call("PUT", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}`, { body: { email: "M@acme.test", permissions: { tasks: true } } })).body).toEqual({ ok: true });
    expect((await call("DELETE", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}&email=m@acme.test`)).body).toEqual({ ok: true });
    expect((await call("DELETE", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}&member=nobody`)).body).toEqual({ error: "member was not found" });
    expect((await call("DELETE", `/dev/projects/${project.id}/auth/memberships?tenant=${tenantId}`)).body).toEqual({ error: "tenant and member or invitation email are required" });

    expect((await call("DELETE", `/dev/tenants/${tenantId}`)).body).toEqual({ error: "project owner or admin access is required" });
    expect((await call("DELETE", `/dev/tenants/${tenantId}?project=${project.id}`)).body).toEqual({ ok: true });
    expect((await call("GET", `/dev/tenants?project=${project.id}`)).body.tenants).toEqual([]);
  });

  it("a single-database project gets its tenant row at creation", async () => {
    const project = await createProject("single-db", "single");
    const list = await call("GET", `/dev/tenants?project=${project.id}`);
    expect(list.body.tenants).toEqual([
      { id: project.id, projectId: project.id, name: "single-db", databaseAlias: "default", database: `projects/${project.id}/project.db`, status: "active", description: "Single-database tenant.", domain: "", timezone: "UTC", profile: {}, provisioned: true },
    ]);
    await useBuilder();
    expect((await call("POST", "/dev/tenants", { body: { projectId: project.id, name: "x" } })).status).toBe(422);
  });
});

describe("errors, logs, metrics, cache, storage", () => {
  it("serves error groups, filters, updates and bug reports", async () => {
    const project = await createProject("errors-project");
    await sql(
      `INSERT INTO gonvex_error_groups(project_id,fingerprint,title,culprit,level,status,priority,assignee,first_seen,last_seen,event_count,tenants,releases,environments,accounts,devices,latest_event,regression)
       VALUES($1,'fp1','TypeError: x','app.ts','error','unresolved','medium','',1000,5000,3,'{"t1":3}','{"r1":3}','{}','{}','{}','{"stack":"at x","release":"r1"}',0),
             ($1,'fp2','Warn','w.ts','warning','resolved','low','',1000,2000,1,'{}','{"r2":1}','{}','{}','{}','{}',0)`,
      project.id,
    );
    await sql("INSERT INTO gonvex_error_events(project_id,fingerprint,event_id,release,occurred_at,payload) VALUES($1,'fp1','e1','r1',5000,'{}'),($1,'fp2','e2','r2',2000,'{}')", project.id);
    const groups = await call("GET", `/dev/errors/groups?project=${project.id}`);
    expect(groups.body.releases).toEqual(["r1", "r2"]);
    expect(groups.body.groups.map((g: { fingerprint: string }) => g.fingerprint)).toEqual(["fp1", "fp2"]);
    expect(groups.body.groups[0]).toMatchObject({ project: project.id, title: "TypeError: x", count: 3, firstSeen: "1970-01-01T00:00:01Z", lastSeen: "1970-01-01T00:00:05Z", tenants: { t1: 3 }, regression: false });
    expect((await call("GET", `/dev/errors/groups?project=${project.id}&level=warn`)).body.groups.map((g: { fingerprint: string }) => g.fingerprint)).toEqual(["fp2"]);
    expect((await call("GET", `/dev/errors/groups?project=${project.id}&release=r1`)).body.groups.map((g: { fingerprint: string }) => g.fingerprint)).toEqual(["fp1"]);
    expect((await call("PATCH", `/dev/errors/groups/fp1?project=${project.id}`, { body: { status: "maybe" } })).body).toEqual({ error: "status is invalid" });
    expect((await call("PATCH", `/dev/errors/groups/fp1?project=${project.id}`, { body: { status: "resolved", assignee: " me " } })).body).toMatchObject({ status: "resolved", assignee: "me", priority: "medium" });
    const report = await call("GET", `/dev/errors/groups/fp1/bug-report?project=${project.id}`);
    expect(report.body.title).toBe("TypeError: x");
    expect(report.body.markdown).toContain("**Impact:** 3 events");
    expect(report.body.agentContext).toMatchObject({ fingerprint: "fp1", release: "r1", stack: "at x" });
    expect((await call("GET", `/dev/errors/groups/nope?project=${project.id}`)).status).toBe(404);
    expect((await call("GET", `/dev/errors/status?project=${project.id}`)).body).toEqual({ enabled: false, project: project.id });
  });

  it("metrics keep the dashboard's shape; logs clear; cache and storage answer", async () => {
    await useBuilder();
    const project = await createProject("metrics-project");
    await sync(project.id, await manifest(project.id), project.key);
    await sql(
      `INSERT INTO gonvex_performance_events(event_id,project_id,tenant_id,account_id,kind,path,outcome,error,client_duration_ms,client_received_at_ms,reason,created_at)
       VALUES('ev1',$1,'t','a','query','notes.list','ok','',12.5,1,'',CAST(unixepoch('subsec')*1000 AS INTEGER)),
             ('ev2',$1,'t','a','query','notes.list','error','boom',7.5,1,'',CAST(unixepoch('subsec')*1000 AS INTEGER))`,
      project.id,
    );
    const metrics = await call("GET", `/dev/metrics?project=${project.id}`);
    expect(Object.keys(metrics.body)).toEqual(expect.arrayContaining(["generatedAt", "functions", "cache", "running", "websocket", "database", "queryAdmission", "scheduler", "logs", "profiler", "memory"]));
    expect(metrics.body.functions["notes.list"]).toMatchObject({ kind: "query", calls: 2, errors: 1, averageDurationMs: 10 });
    expect(metrics.body.functions["notes.add"]).toMatchObject({ kind: "reducer", calls: 0 });
    expect(metrics.body.logs).toHaveLength(2);
    expect(metrics.body.logs[0]).toMatchObject({ project: project.id, path: "notes.list", source: "client" });
    expect((await call("GET", "/dev/metrics")).body).toEqual({ error: "project is required" });
    expect((await call("DELETE", `/dev/logs?project=${project.id}`, { token: project.key })).body).toEqual({ cleared: 2 });
    expect((await call("DELETE", `/dev/cache?project=${project.id}`, { token: project.key })).body).toEqual({ cleared: true, project: project.id });
    expect((await call("GET", `/dev/storage/files?project=${project.id}`, { token: project.key })).body).toEqual({ configured: false, files: [] });
  });

  it("CORS on /dev, 404 and 405 like axum", async () => {
    const preflight = await call("OPTIONS", "/dev/anything/at/all", { token: null });
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("access-control-allow-headers")).toBe("Authorization, Content-Type, X-Gonvex-Key, X-Gonvex-Project-Key, X-Gonvex-Project-Id, X-Gonvex-Tenant-Id");
    const missing = await call("GET", "/dev/nope");
    expect([missing.status, missing.text, missing.headers.get("access-control-allow-origin")]).toEqual([404, "", "*"]);
    const wrong = await call("PUT", "/dev/projects");
    expect(wrong.status).toBe(405);
    void ADMIN;
  });
});
