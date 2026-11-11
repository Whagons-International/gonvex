/**
 * Test and development Worker for the control object: the control object,
 * the stub tenant object and the minimal control socket owner, behind a
 * router with the reference's seeding routes (admin key required).
 *
 * Deployed only as `gonvex-cf-dev-*` or run locally (`wrangler dev`, vitest).
 */
import { ControlObject, type ControlEnv } from "../src/control-object";
import { ControlSocketObject } from "./control-socket";
import { StubTenantObject } from "./stub-tenant";

export { ControlObject, ControlSocketObject, StubTenantObject };

interface Env extends ControlEnv {
  CONTROL: DurableObjectNamespace<ControlObject>;
  CONTROL_SOCKETS: DurableObjectNamespace<ControlSocketObject>;
  TENANT: DurableObjectNamespace<StubTenantObject>;
  GONVEX_ADMIN_KEY?: string;
}

const json = (body: unknown, status = 200) => Response.json(body, { status });

async function body(request: Request): Promise<Record<string, unknown>> {
  try {
    return (await request.json()) as Record<string, unknown>;
  } catch {
    return {};
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const control = env.CONTROL.getByName("control");
    if (url.pathname === "/healthz") return json({ ok: true });
    if (url.pathname === "/ws") return env.CONTROL_SOCKETS.get(env.CONTROL_SOCKETS.newUniqueId()).fetch(request);
    if (url.pathname.startsWith("/auth/")) return control.fetch(request);
    if (!url.pathname.startsWith("/dev/")) return json({ error: "not found" }, 404);

    const bearer = (request.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "");
    const admin = !!env.GONVEX_ADMIN_KEY && bearer === env.GONVEX_ADMIN_KEY;
    if (request.method === "POST" && url.pathname === "/dev/sync") {
      const manifest = await body(request);
      const project = String(manifest.project ?? "");
      const key = await control.devProjectKey(project);
      if (!admin && (!key || key !== bearer)) return json({ error: "project key is required" }, 403);
      return json(await control.devSaveManifest(project, manifest));
    }
    if (!admin) return json({ error: "runtime admin key is required" }, 403);
    try {
      if (request.method === "POST" && url.pathname === "/dev/projects") {
        const input = await body(request);
        const created = await control.devCreateProject({
          name: String(input.name ?? "project"),
          databaseMode: String(input.databaseMode ?? "multiTenant"),
          ...(typeof input.ownerEmail === "string" ? { ownerEmail: input.ownerEmail } : {}),
          ...(typeof input.authMode === "string" ? { authMode: input.authMode } : {}),
          ...(typeof input.id === "string" ? { id: input.id } : {}),
        });
        return json({ project: { id: created.projectId }, projectKey: created.projectKey });
      }
      if (request.method === "POST" && url.pathname === "/dev/tenants") {
        const input = await body(request);
        const created = await control.devCreateTenant(String(input.projectId), String(input.id), String(input.name ?? input.id));
        return json({ id: created.tenantId });
      }
      if (request.method === "POST" && url.pathname === "/dev/internal/e2e/members") {
        const input = await body(request);
        return json(
          await control.devE2eMember({
            projectId: String(input.projectId ?? ""),
            email: String(input.email ?? ""),
            password: String(input.password ?? ""),
            ...(typeof input.tenantId === "string" ? { tenantId: input.tenantId } : {}),
            ...(typeof input.name === "string" ? { name: input.name } : {}),
            ...(input.accountOnly === true ? { accountOnly: true } : {}),
          }),
        );
      }
      const members = /^\/dev\/projects\/([^/]+)\/members$/.exec(url.pathname);
      if (members && request.method === "POST") {
        const input = await body(request);
        return json({ member: await control.devProjectMember(decodeURIComponent(members[1]!), { email: String(input.email ?? ""), name: String(input.name ?? ""), role: String(input.role ?? "") }) });
      }
      const google = /^\/dev\/projects\/([^/]+)\/auth\/google$/.exec(url.pathname);
      if (google && request.method === "PUT") {
        const input = await body(request);
        const outcome = await control.devGoogleAuth(decodeURIComponent(google[1]!), { redirectUri: String(input.redirectUri ?? ""), signupMode: String(input.signupMode ?? "") });
        return outcome.ok ? json({ ok: true }) : json({ error: outcome.error }, 400);
      }
      const rows = /^\/dev\/data\/tables\/([^/]+)\/rows$/.exec(url.pathname);
      if (rows && request.method === "GET") {
        const tenant = url.searchParams.get("tenant") ?? "";
        const table = decodeURIComponent(rows[1]!);
        const all = (await env.TENANT.getByName(tenant).readTable(table)) as unknown as unknown[];
        return json({ rows: all, total: all.length });
      }
    } catch (error) {
      return json({ error: error instanceof Error ? error.message : String(error) }, 400);
    }
    return json({ error: "not found" }, 404);
  },
} satisfies ExportedHandler<Env>;
