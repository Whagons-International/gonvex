/**
 * Development routes for the stubs, used by the conformance adapter
 * (`cloudflare/conformance/src/backends/cloudflare.mjs`), the workerd tests
 * and the hop benchmark. They mirror the Rust seeding routes the harness uses
 * (`/dev/projects`, `/dev/sync`, `/dev/tenants`, `/dev/internal/e2e/members`,
 * `/dev/data/tables/...`), under `/_stub/session/` so K19's real `/dev/*`
 * routes never collide with them.
 *
 * Disabled unless `GONVEX_STUB_ADMIN_KEY` is set (a `--var` locally; never set
 * on a shared deploy). Every request needs `authorization: Bearer <key>`.
 */
import { CONTROL_OBJECT, type SessionBindingsEnv } from "../bindings";
import type { DeliveryBatch } from "../placement/gateway";
import { type ControlRpc, type GatewayRpc, type TenantRpc, rpc } from "../rpc";

export interface StubRouteEnv extends SessionBindingsEnv {
  GONVEX_STUB_ADMIN_KEY?: string;
  GATEWAY: DurableObjectNamespace<any>;
}

export const STUB_PREFIX = "/_stub/session/";

const json = (body: unknown, status = 200) => Response.json(body, { status });

export async function handleStubRoute(request: Request, env: StubRouteEnv): Promise<Response | null> {
  const url = new URL(request.url);
  if (!url.pathname.startsWith(STUB_PREFIX)) return null;
  const key = env.GONVEX_STUB_ADMIN_KEY;
  if (!key) return json({ error: "not found" }, 404);
  if (request.headers.get("authorization") !== `Bearer ${key}`) return json({ error: "unauthorized" }, 401);
  const route = url.pathname.slice(STUB_PREFIX.length);
  const control = rpc<ControlRpc>(env.CONTROL_STUB, CONTROL_OBJECT);
  const tenantStub = (name: string) => rpc<TenantRpc>(env.TENANT_STUB, name);
  const body = request.method === "POST" ? await request.json<Record<string, unknown>>() : {};
  const str = (v: unknown) => String(v ?? "");
  switch (`${request.method} ${route}`) {
    case "POST projects": return json(await control.createProject(str(body.name)));
    case "POST sync": {
      const { project, ...manifest } = body;
      return json(await control.deploy(str(project), manifest));
    }
    case "POST tenants": return json(await control.createTenant(str(body.projectId), str(body.id), str(body.name)));
    case "POST members": return json(await control.createMember(str(body.projectId), str(body.tenantId), { email: str(body.email), password: str(body.password), name: str(body.name) }));
    case "POST commit": {
      const { tenantId, ...batch } = body as { tenantId: string } & Omit<DeliveryBatch, "v" | "revision" | "tenantId">;
      return json(await tenantStub(tenantId).commit(batch));
    }
    case "POST role": return json(await tenantStub(str(body.tenantId)).setRole(str(body.memberId), str(body.role)));
    case "GET gateways": return json(await tenantStub(str(url.searchParams.get("tenant"))).registeredGateways());
    case "GET hop": {
      // One gateway hop each way, timed inside the objects: gateway -> tenant RPC and tenant -> gateway RPC.
      const n = Math.min(Number(url.searchParams.get("n") ?? 200) || 200, 2000);
      const gateway = str(url.searchParams.get("gateway") || "gw-0");
      const tenant = str(url.searchParams.get("tenant") || "hop-bench");
      const toTenant = await rpc<GatewayRpc>(env.GATEWAY, gateway).hopToTenant(tenant, n);
      const toGateway = await tenantStub(tenant).hopToGateway(gateway, n);
      return json({ n, gateway, tenant, gatewayToTenant: summary(toTenant), tenantToGateway: summary(toGateway), colo: request.cf?.colo ?? null });
    }
  }
  if (request.method === "GET" && route.startsWith("tables/")) return json({ rows: [], total: 0 });
  return json({ error: "not found" }, 404);
}

function summary(samples: number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  const q = (p: number) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] ?? 0;
  const round = (v: number) => Math.round(v * 100) / 100;
  return { min: round(sorted[0] ?? 0), p50: round(q(0.5)), p90: round(q(0.9)), p99: round(q(0.99)), max: round(sorted.at(-1) ?? 0), mean: round(sorted.reduce((a, b) => a + b, 0) / (sorted.length || 1)) };
}
