// K22: the serving kernel. Fixture routes and the K05 stand-ins are test-only.
import { handleTenantTransfer, type TransferEnv } from "../kernel/assembly/tenant-transfer";
import { fixtureControl } from "../kernel/assembly/fixture-control";
import type { RolloutMessage } from "../control/operator/release";
import { handleWebSocketUpgrade } from "../kernel/session/router";
import { UPGRADE_HEADERS } from "../kernel/session/placement/gateway";
import { clientAddress } from "../kernel/public/address";
import { handleDevRequest } from "../router/dev/routes";
import { handleRolloutBatch } from "../router/dev/rollout";
import { handleStorageRequest, STORAGE_PREFIX } from "../router/storage/index";
import { filesConfigFromEnv } from "../kernel/files/config";
import type { FilesTenantRpc } from "../kernel/files/rpc";
import { handleIngressRequest } from "../router/ingress/routes";

export { SessionControlObject } from "../kernel/assembly/control";

export { AssemblyGateway } from "../kernel/assembly/gateway";
export { GatewayControllerObject } from "../kernel/gateway/controller";
export { PublicLimiterObject } from "../kernel/public";
export { IngressObject } from "../router/ingress/object";

/** A0 SPIKE: the tenant id from `Origin: <scheme>://<tenant>.<base>`, only for listed tenants. */
function a0Upgrade(request: Request, url: URL, env: Record<string, unknown>): Promise<Response> | undefined {
  if (env.GONVEX_A0_TENANT_SOCKETS !== "1" || url.searchParams.get("a0") !== "1" || url.searchParams.get("public") === "1") return undefined;
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return undefined;
  let tenant: string;
  try { tenant = new URL(request.headers.get("origin") ?? "").hostname.split(".")[0] ?? ""; } catch { return undefined; }
  const allowed = String(env.GONVEX_A0_TENANTS ?? "").split(",").map((t) => t.trim()).filter(Boolean);
  if (!tenant || !allowed.includes(tenant)) return undefined;
  const headers = new Headers(request.headers);
  for (const name of [...headers.keys()]) if (name.startsWith("x-gonvex-")) headers.delete(name);
  headers.set(UPGRADE_HEADERS.connectionId, crypto.randomUUID());
  headers.set(UPGRADE_HEADERS.publicTransport, "0");
  headers.set(UPGRADE_HEADERS.address, clientAddress(request.headers, undefined));
  headers.set(UPGRADE_HEADERS.slots, "");
  const tenants = env.TENANT as DurableObjectNamespace;
  return tenants.getByName(tenant).fetch(new Request(request.url, { method: "GET", headers }));
}

interface ServingEnv extends Env, TransferEnv { GONVEX_ASSEMBLY_FIXTURE_KEY?: string }

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/") return Response.json({ ok: true, service: "gonvex-cloudflare" });
    if (url.pathname === "/ws") {
      // A0 SPIKE (option A, M0): `?a0=1` from the probe sends the upgrade to the tenant object of the page's subdomain,
      // which holds the socket itself (kernel/assembly/a0-local-holder.ts). Dev composition with the var only.
      const a0 = a0Upgrade(request, url, env as unknown as Record<string, unknown>);
      if (a0) return a0;
      return handleWebSocketUpgrade(request, env, ctx);
    }
    const transfer = await handleTenantTransfer(request, env);
    if (transfer) return transfer;
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
