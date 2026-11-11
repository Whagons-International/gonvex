import type { GreetResult } from "./hello";

export { HelloObject } from "./hello";

const TENANT_ROUTE = /^\/t\/([a-z0-9][a-z0-9_-]{0,63})\/(greet|ws)$/;

function json(body: unknown, status = 200): Response {
  return Response.json(body, { status });
}

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return json({ ok: true, service: "gonvex-cloudflare" });
    }

    const match = TENANT_ROUTE.exec(url.pathname);
    if (!match) return json({ error: "not found" }, 404);
    const [, tenant, action] = match as unknown as [string, string, "greet" | "ws"];
    const stub = env.HELLO.getByName(tenant);

    if (action === "ws") {
      return stub.fetch(request);
    }
    if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
    const name = url.searchParams.get("name") ?? "world";
    const result: GreetResult = await stub.greet(name);
    return json({ tenant, ...result });
  },
} satisfies ExportedHandler<Env>;
