import type { LegacyWorkerBindings } from "./legacy-bindings";
import type { GreetResult } from "../src/hello";
import { handleStubRoute, handleWebSocketUpgrade, STUB_PREFIX } from "../kernel/session";

export { HelloObject } from "../src/hello";
export { ControlStubObject, GatewayObject, TenantStubObject } from "../kernel/session";
export { PublicLimiterObject } from "../kernel/public";

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
    // K05: the client socket. The tenant arrives in the first `auth` frame (0004).
    if (url.pathname === "/ws") return handleWebSocketUpgrade(request, env);
    if (url.pathname.startsWith(STUB_PREFIX)) return (await handleStubRoute(request, env))!;

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
} satisfies ExportedHandler<LegacyWorkerBindings>;
