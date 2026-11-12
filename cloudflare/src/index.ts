// K22: the serving kernel. Fixture routes and the K05 stand-ins are test-only.
import { fixtureControl } from "../kernel/assembly/fixture-control";
import type { RolloutMessage } from "../control/operator/release";
import { handleWebSocketUpgrade } from "../kernel/session/router";
import { handleDevRequest } from "../router/dev/routes";
import { handleRolloutBatch } from "../router/dev/rollout";
import { handleStorageRequest, STORAGE_PREFIX } from "../router/storage/index";
import { filesConfigFromEnv } from "../kernel/files/config";
import type { FilesTenantRpc } from "../kernel/files/rpc";
import { handleIngressRequest } from "../router/ingress/routes";

export { SessionControlObject } from "../kernel/assembly/control";
export { AssemblyTenant } from "../kernel/assembly/tenant";
export { AssemblyGateway } from "../kernel/assembly/gateway";
export { PublicLimiterObject } from "../kernel/public";
export { IngressObject } from "../router/ingress/object";

interface ServingEnv extends Env { GONVEX_ASSEMBLY_FIXTURE_KEY?: string }

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/") return Response.json({ ok: true, service: "gonvex-cloudflare" });
    if (url.pathname === "/ws") return handleWebSocketUpgrade(request, env);
    const fixture = await fixtureControl(request, env);
    if (fixture) return fixture;
    const dev = await handleDevRequest(request, env, ctx);
    if (dev) return dev;
    if (url.pathname.startsWith("/auth/")) return env.CONTROL.getByName("control").fetch(request);
    if (url.pathname.startsWith(STORAGE_PREFIX)) return handleStorageRequest(request, {
      bucket: env.FILES, config: filesConfigFromEnv(env as unknown as Record<string, unknown>), tenant: (id) => env.TENANT.getByName(id) as unknown as FilesTenantRpc,
    });
    const ingress = await handleIngressRequest(request, env);
    return ingress ?? Response.json({ error: "not found" }, { status: 404 });
  },
  queue: (batch, env) => handleRolloutBatch(batch as MessageBatch<RolloutMessage>, env),
} satisfies ExportedHandler<ServingEnv>;
