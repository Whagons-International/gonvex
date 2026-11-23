// Tenant prewarm on the page's first socket (kernel/session/prewarm.ts): the Origin selects which tenant object to
// wake, the upgrade never waits for it, and it is throttled, skippable and fail-safe.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clearPrewarmThrottle, prewarmTenantForUpgrade, PREWARM_ISOLATE_INTERVAL_MS, tenantSelectorFromOrigin } from "../prewarm";
import { handleWebSocketUpgrade, resetWarmCell, type WebSocketRouteEnv } from "../router";

const PROJECT = "691d7a9d-948a-4ac2-894a-f7ff250e7ad8";
const upgrade = (origin: string | null, query = `project=${PROJECT}`) =>
  new Request(`https://gonvex.test/ws?${query}`, { headers: { upgrade: "websocket", ...(origin ? { origin } : {}) } });
function control(prewarmTenant = vi.fn(async () => "warming")) {
  const getByName = vi.fn(() => ({ prewarmTenant }));
  return { namespace: { getByName } as unknown as DurableObjectNamespace, prewarmTenant, getByName };
}
const run = (request: Request, env: Record<string, unknown>, now?: number) => {
  const pending: Promise<unknown>[] = [];
  const started = prewarmTenantForUpgrade(request, new URL(request.url), env, (work) => pending.push(work), now);
  return { started, settled: Promise.all(pending) };
};

beforeEach(() => { clearPrewarmThrottle(); resetWarmCell(); });
afterEach(() => { vi.restoreAllMocks(); });

describe("tenantSelectorFromOrigin", () => {
  it("is the first label of a host with a subdomain", () => {
    expect(tenantSelectorFromOrigin("https://el-rey2.whagons.com")).toBe("el-rey2");
    expect(tenantSelectorFromOrigin("http://devcopy-stg1117-12.localhost:41794")).toBe("devcopy-stg1117-12");
    expect(tenantSelectorFromOrigin("https://Tecal.Whagons.com")).toBe("tecal");
  });
  it("names no tenant for bare hosts, www, IP addresses, opaque or malformed origins", () => {
    for (const origin of [null, "", "null", "https://localhost:3000", "https://whagons", "https://www.whagons.com",
      "http://127.0.0.1:8080", "not a url", "https://-bad.whagons.com", "https://under_score.whagons.com"]) {
      expect(tenantSelectorFromOrigin(origin), String(origin)).toBeUndefined();
    }
  });
});

describe("prewarmTenantForUpgrade", () => {
  it("asks Control to wake the page's tenant, in the background", async () => {
    const c = control();
    const { started, settled } = run(upgrade("https://el-rey2.whagons.com"), { CONTROL: c.namespace });
    expect(started).toBe(true);
    await settled;
    expect(c.getByName).toHaveBeenCalledWith("control");
    expect(c.prewarmTenant).toHaveBeenCalledExactlyOnceWith(PROJECT, "el-rey2");
  });

  it("asks once per (project, selector) per interval in an isolate", async () => {
    const c = control();
    const env = { CONTROL: c.namespace };
    expect(run(upgrade("https://el-rey2.whagons.com"), env, 1_000).started).toBe(true);
    expect(run(upgrade("https://el-rey2.whagons.com"), env, 1_000 + PREWARM_ISOLATE_INTERVAL_MS - 1).started).toBe(false);
    expect(run(upgrade("https://tecal.whagons.com"), env, 1_500).started).toBe(true);
    expect(run(upgrade("https://el-rey2.whagons.com"), env, 1_000 + PREWARM_ISOLATE_INTERVAL_MS).started).toBe(true);
    await Promise.resolve();
    expect(c.prewarmTenant).toHaveBeenCalledTimes(3);
  });

  it("skips public sockets, missing or invalid projects, origins without a tenant, and the kill switch", () => {
    const c = control();
    expect(run(upgrade("https://el-rey2.whagons.com", `project=${PROJECT}&public=1`), { CONTROL: c.namespace }).started).toBe(false);
    expect(run(upgrade("https://el-rey2.whagons.com", ""), { CONTROL: c.namespace }).started).toBe(false);
    expect(run(upgrade("https://el-rey2.whagons.com", "project=has%20space"), { CONTROL: c.namespace }).started).toBe(false);
    expect(run(upgrade(null), { CONTROL: c.namespace }).started).toBe(false);
    expect(run(upgrade("https://el-rey2.whagons.com"), { CONTROL: c.namespace, GONVEX_TENANT_PREWARM: "0" }).started).toBe(false);
    expect(run(upgrade("https://el-rey2.whagons.com"), {}).started).toBe(false);
    expect(c.prewarmTenant).not.toHaveBeenCalled();
  });

  it("honours ?prewarm=0 only on a dev composition (fixture key set)", () => {
    const c = control();
    expect(run(upgrade("https://a.whagons.com", `project=${PROJECT}&prewarm=0`), { CONTROL: c.namespace, GONVEX_ASSEMBLY_FIXTURE_KEY: "k" }).started).toBe(false);
    expect(run(upgrade("https://a.whagons.com", `project=${PROJECT}&prewarm=0`), { CONTROL: c.namespace }).started).toBe(true);
  });

  it("a failing Control never surfaces", async () => {
    const c = control(vi.fn(async () => { throw new Error("Control unavailable"); }));
    const { started, settled } = run(upgrade("https://el-rey2.whagons.com"), { CONTROL: c.namespace });
    expect(started).toBe(true);
    await expect(settled).resolves.toBeDefined();
  });
});

describe("router wiring", () => {
  it("starts the prewarm on the upgrade and does not wait for it", async () => {
    let release!: () => void;
    const prewarmTenant = vi.fn(() => new Promise<string>((resolve) => { release = () => resolve("warming"); }));
    const pending: Promise<unknown>[] = [];
    const gateway = { fetch: vi.fn(async () => new Response(null, { status: 200 })) };
    const env = {
      GATEWAY: { getByName: () => gateway, idFromName: () => ({}), get: () => gateway },
      CONTROL: { getByName: () => ({ prewarmTenant }) },
    } as unknown as WebSocketRouteEnv;
    const response = await handleWebSocketUpgrade(upgrade("https://el-rey2.whagons.com"), env, { waitUntil: (work) => { pending.push(work); } });
    expect(response.status).toBe(200); // answered while the prewarm is still pending
    expect(gateway.fetch).toHaveBeenCalledOnce();
    await Promise.resolve();
    expect(prewarmTenant).toHaveBeenCalledExactlyOnceWith(PROJECT, "el-rey2");
    release();
    await Promise.all(pending);
  });
});
