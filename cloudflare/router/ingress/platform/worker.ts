/**
 * The K18 test composition (../test/worker.ts) for a platform run on the
 * development account, plus admin-key routes to seed it and read what the
 * Action port recorded. Deployed only by `run.mjs`, under a gonvex-cf-dev-
 * name, and deleted at the end of the run.
 */
import base, { IngressObject, Recorder, StubTenantObject, TestActions, TestControlObject } from "../test/worker.ts";

export { IngressObject, Recorder, StubTenantObject, TestActions, TestControlObject };

interface PlatformEnv {
  CONTROL: DurableObjectNamespace<TestControlObject>;
  RECORDER: DurableObjectNamespace<Recorder>;
  K18_ADMIN_KEY?: string;
}

export default {
  async fetch(request: Request, env: PlatformEnv & Parameters<typeof base.fetch>[1]): Promise<Response> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith("/_k18/")) return base.fetch(request, env);
    if (!env.K18_ADMIN_KEY || request.headers.get("authorization") !== `Bearer ${env.K18_ADMIN_KEY}`) return new Response(null, { status: 404 });
    const control = env.CONTROL.getByName("control");
    if (url.pathname === "/_k18/seed" && request.method === "POST") {
      const input = (await request.json()) as { project: string; tenants: { id: string; domain?: string; status?: string }[]; secrets: Record<string, string> };
      await control.devCreateProject({ name: input.project, id: input.project });
      for (const tenant of input.tenants) {
        await control.devCreateTenant(input.project, tenant.id, tenant.id);
        await control.testTenant(input.project, tenant.id, { ...(tenant.domain ? { domain: tenant.domain } : {}), ...(tenant.status ? { status: tenant.status } : {}) });
      }
      await env.RECORDER.getByName(input.project).setSecrets(input.project, input.secrets);
      return Response.json({ ok: true });
    }
    const project = url.searchParams.get("project") ?? "";
    if (url.pathname === "/_k18/calls") return Response.json(await env.RECORDER.getByName(project).list());
    if (url.pathname === "/_k18/clear") {
      await env.RECORDER.getByName(project).clear();
      return Response.json({ ok: true });
    }
    return new Response(null, { status: 404 });
  },
};
