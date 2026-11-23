// Tenant prewarm on the page's first socket (kernel/session/prewarm.ts): the Origin selects which tenant object to
// wake, the upgrade never waits for it, and it is throttled, skippable and fail-safe.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clearPrewarmThrottle, prewarmThrottleSize, prewarmTenantForUpgrade, rememberBounded, PREWARM_ISOLATE_INTERVAL_MS, PREWARM_ISOLATE_MAX, PREWARM_ISOLATE_PER_SECOND, tenantSelectorFromOrigin } from "../prewarm";
import { handleWebSocketUpgrade, resetWarmCell, WarmCell, type WebSocketRouteEnv } from "../router";

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
      "http://127.0.0.1:8080", "http://[::1]:8080", "not a url", "https://-bad.whagons.com", "https://under_score.whagons.com"]) {
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

describe("caps that no client-chosen value can spread", () => {
  it(`asks at most ${PREWARM_ISOLATE_PER_SECOND} times per second per isolate, whatever the selectors`, async () => {
    const c = control();
    const env = { CONTROL: c.namespace };
    const started = Array.from({ length: PREWARM_ISOLATE_PER_SECOND + 5 }, (_, i) => run(upgrade(`https://r${i}.whagons.com`), env, 5_000 + i).started);
    expect(started.filter(Boolean)).toHaveLength(PREWARM_ISOLATE_PER_SECOND);
    expect(started.slice(PREWARM_ISOLATE_PER_SECOND).every((s) => !s)).toBe(true);
    expect(run(upgrade("https://next-second.whagons.com"), env, 6_000).started).toBe(true); // a new second
    await Promise.resolve();
    expect(c.prewarmTenant).toHaveBeenCalledTimes(PREWARM_ISOLATE_PER_SECOND + 1);
  });

  it(`remembers at most ${PREWARM_ISOLATE_MAX} (project, selector) pairs`, () => {
    const env = { CONTROL: control().namespace };
    for (let i = 0; i < PREWARM_ISOLATE_MAX + 50; i++) run(upgrade(`https://t${i}.whagons.com`), env, i * 1_000);
    expect(prewarmThrottleSize()).toBe(PREWARM_ISOLATE_MAX);
  });

  it("rememberBounded keeps the newest entries within the bound", () => {
    const map = new Map<string, number>();
    for (let i = 0; i < 6; i++) rememberBounded(map, `k${i}`, i, 4);
    rememberBounded(map, "k3", 9, 4); // a refresh moves the key to the newest position
    expect([...map]).toEqual([["k2", 2], ["k4", 4], ["k5", 5], ["k3", 9]]);
  });
});

describe("router wiring", () => {
  it("never prewarms for an upgrade the address limiter refuses (429)", async () => {
    const c = control();
    const admit = vi.fn(async () => ({ ok: false, error: "too many connections from this address" }));
    const gateway = { fetch: vi.fn(async () => new Response(null, { status: 200 })) };
    const env = {
      GATEWAY: { getByName: () => gateway, idFromName: () => ({}), get: () => gateway },
      PUBLIC_LIMITER: { getByName: () => ({ admit, release: vi.fn(async () => undefined) }) },
      CONTROL: c.namespace,
    } as unknown as WebSocketRouteEnv;
    const pending: Promise<unknown>[] = [];
    const response = await handleWebSocketUpgrade(upgrade("https://el-rey2.whagons.com"), env, { waitUntil: (work) => { pending.push(work); } });
    expect(response.status).toBe(429);
    expect(admit).toHaveBeenCalledOnce();
    await Promise.all(pending);
    expect(c.getByName).not.toHaveBeenCalled();
    expect(gateway.fetch).not.toHaveBeenCalled();
  });

  it("prewarms only after the limiter admitted the upgrade", async () => {
    const order: string[] = [];
    const c = control(vi.fn(async () => { order.push("prewarm"); return "warming"; }));
    const admit = vi.fn(async () => { order.push("admit"); return { ok: true }; });
    const gateway = { fetch: vi.fn(async () => new Response(null, { status: 200 })) };
    const env = {
      GATEWAY: { getByName: () => gateway, idFromName: () => ({}), get: () => gateway },
      PUBLIC_LIMITER: { getByName: () => ({ admit, release: vi.fn(async () => undefined) }) },
      CONTROL: c.namespace,
    } as unknown as WebSocketRouteEnv;
    const pending: Promise<unknown>[] = [];
    await handleWebSocketUpgrade(upgrade("https://el-rey2.whagons.com"), env, { waitUntil: (work) => { pending.push(work); } });
    await Promise.all(pending);
    expect(order).toEqual(["admit", "prewarm"]);
  });

  it.each([[101, true], [429, false], [503, false]] as const)("direct path: a %i from the remembered holder prewarms: %s", async (status, expected) => {
    class RememberedHolder extends WarmCell {
      override get hasHolder() { return true; }
      override direct() { return { v: 1 as const, cell: 0, gateway: "gw-direct", epoch: 1, connectionId: crypto.randomUUID() }; }
    }
    resetWarmCell(new RememberedHolder());
    const c = control();
    const holder = { fetch: vi.fn(async () => ({ status, headers: new Headers(status === 503 ? { "x-gonvex-admission-reason": "limiter-unavailable" } : {}) }) as unknown as Response) };
    const env = {
      GATEWAY: { getByName: () => holder, idFromName: () => ({}), get: () => holder },
      GATEWAY_CONTROLLER: { getByName: () => ({ acquire: vi.fn(async () => { throw new Error("not reached"); }), release: vi.fn(async () => undefined) }) },
      CONTROL: c.namespace,
    } as unknown as WebSocketRouteEnv;
    const pending: Promise<unknown>[] = [];
    const response = await handleWebSocketUpgrade(upgrade("https://el-rey2.whagons.com"), env, { waitUntil: (work) => { pending.push(work); } });
    expect(response.status).toBe(status);
    await Promise.allSettled(pending);
    expect(c.prewarmTenant).toHaveBeenCalledTimes(expected ? 1 : 0);
  });

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
