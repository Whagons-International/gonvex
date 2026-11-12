/**
 * The Ingress Worker (K18): one Worker for every webhook and the share pages
 * (CONTRACT "Ingress Worker"; IB4). It holds no tenant data: the control
 * object resolves tenants, the ingress objects (one per tenant) rate-limit,
 * deduplicate and run the Action through the service-call port.
 */
import { IngressObject } from "./object.ts";
import { handleIngressRequest, type IngressEnv } from "./routes.ts";

export { IngressObject };

export default {
  async fetch(request: Request, env: IngressEnv): Promise<Response> {
    const response = await handleIngressRequest(request, env);
    if (response) return response;
    if (new URL(request.url).pathname === "/healthz") return Response.json({ ok: true, service: "gonvex-ingress" });
    return new Response(null, { status: 404 });
  },
} satisfies ExportedHandler<IngressEnv>;
