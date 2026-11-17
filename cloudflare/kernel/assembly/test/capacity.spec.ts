import { env, SELF, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { fixtureControl } from "../fixture-control";
import { capacityPage, type CapacityEnv } from "../capacity";
import { CAPACITY_PATH, CAPACITY_LIMITS, type CapacityPage } from "../capacity-contract";
import { controllerCell, ASSIGNMENT_HEADER, WINDOW_FIELDS, type CapacitySample } from "../../gateway/placement";
import type { GatewayControllerObject } from "../../gateway/controller";
import type { AssemblyGateway } from "../gateway";

const key = "capacity-local-only";
const request = (query = "cell=0", options: RequestInit = {}) => new Request(`https://local${CAPACITY_PATH}?${query}`, { headers: { authorization: `Bearer ${key}` }, ...options });
const catalog = { version: "local-catalog", releases: { local: { project: "local", hash: "a".repeat(64), generation: 1 } } };
const measured = (epoch = 1, at = Date.now()): CapacitySample => ({ v: 1, epoch, at, sockets: 3, framesPerSecond: 4, bytesPerSecond: 8, deliveryMs: 2, queueAgeMs: 1, rpcMs: 1, currentPressure: 0 });
const slot = (index: number, cell = 0) => ({ name: `auto-gw-${cell}-${index}`, epoch: index + 1, mode: "active", changedAt: 10, pressuredSince: null, replacement: null, sample: measured(index + 1) });
const state = (count = 1) => ({ v: 1, epoch: count, pending: 0, events: Array.from({ length: count }, (_, i) => ({ at: i + 1, action: "allocate", gateway: `auto-gw-0-${i}`, epoch: i + 1 })), slots: Array.from({ length: count }, (_, i) => slot(i)) });
/** Explicit LOCAL DATA adapters for refusal/fault inputs, never a serving binding. */
function data(input: unknown = state(), holder: (name: string) => Record<string, unknown> = () => ({ capacity: async (epoch: number) => measured(epoch), drainStatus: async () => ({ closed: 0, remaining: 3, blocked: {} }) })) {
  const calls: string[] = [];
  const binding = (kind: string, get: (name: string) => Record<string, unknown>) => ({ getByName(name: string) {
    calls.push(`${kind}:${name}`);
    return new Proxy(get(name), { get(target, method: string) {
      if (!(method in target)) throw new Error("unexpected mutation command");
      const f = target[method]; return typeof f === "function" ? (...args: unknown[]) => { calls.push(method); return f(...args); } : f;
    } });
  } });
  return { calls, bindings: { GATEWAY_CONTROLLER: binding("controller", () => ({ status: async () => structuredClone(input) })), GATEWAY: binding("holder", holder) } as unknown as CapacityEnv };
}
const page = async (r: Response) => { expect(r.status).toBe(200); return r.json<CapacityPage>(); };

describe("K22 bounded capacity HTTP facade, local data refusals", () => {
  it("hides the route without the existing gate/key, including wrong methods, before any RPC", async () => {
    const d = data();
    for (const [gate, authorization, method] of [[undefined, `Bearer ${key}`, "GET"], [key, "", "GET"], [key, "Bearer wrong", "POST"]] as const) {
      const r = await fixtureControl(new Request(`https://local${CAPACITY_PATH}?cell=0`, { method, headers: { authorization } }), { ...d.bindings, TENANT: env.TENANT, ...(gate ? { GONVEX_ASSEMBLY_FIXTURE_KEY: gate } : {}) });
      expect(r!.status).toBe(404); expect(await r!.text()).toBe("");
    }
    expect(d.calls).toEqual([]);
  });
  it.each(["", "cell=32", "cell=-1", "cell=01", "cell=1.5", "cell=9007199254740993", "cell=0&cell=1", "cell=0&holder=foreign", "cell=0&providerUrl=x", "cell=0&limit=0", "cell=0&limit=5", "cell=0&cursor=65", "cell=0&eventLimit=0", "cell=0&eventLimit=17", "cell=0&fresh=true", "cell=0&runId=bad%20label", `cell=0&runId=${"a".repeat(513)}`])("strict query refuses %s before RPC", async query => {
    const d = data(); const r = await capacityPage(request(query), d.bindings, catalog);
    expect(r.status).toBe(400); expect(d.calls).toEqual([]);
  });
  it("requires GET and propagates pre-abort without RPC", async () => {
    const d = data(); expect((await capacityPage(request("cell=0", { method: "POST" }), d.bindings, catalog)).status).toBe(405);
    const abort = new AbortController(); abort.abort();
    expect((await capacityPage(request("cell=0", { signal: abort.signal }), d.bindings, catalog)).status).toBe(499);
    expect(d.calls).toEqual([]);
  });
  it("returns bounded pages, immutable cached times/identity, unknown event loss, and no fresh RPC", async () => {
    const input = state(6); input.slots[0]!.sample.at = Date.now() - 31_000;
    const d = data(input), a = await page(await capacityPage(request("cell=0&limit=4&eventLimit=2&runId=run_1"), d.bindings, catalog));
    const b = await page(await capacityPage(request("cell=0&cursor=4&eventCursor=2&eventLimit=4"), d.bindings, catalog));
    expect(a.coverage!.slots).toMatchObject({ returned: 4, total: 6, nextCursor: 4, hasMore: true });
    expect(b.coverage!.slots).toMatchObject({ returned: 2, nextCursor: null, hasMore: false });
    expect(a.slots[0]!.cached).toMatchObject({ state: "STALE", sample: { at: input.slots[0]!.sample.at }, windowState: "UNKNOWN" });
    expect(a.controller.status).toBe("OK"); expect(b.controller).toEqual(a.controller.status === "OK" && b.controller.status === "OK" ? { ...a.controller, value: { ...a.controller.value, readAt: b.controller.value.readAt } } : a.controller);
    expect(a.coverage!.events.loss).toBe("UNKNOWN"); expect(a.coverage!.fleet).toBe("ONE_CELL_PAGE_ONLY");
    expect(a.unavailable.every(v => v.status === "UNAVAILABLE")).toBe(true);
    expect(a.identity.catalog.status).toBe("OK"); expect(a.identity.objectCodeVersions).toContain("UNAVAILABLE");
    expect(a.provenance.capacityCalls).toBe(0); expect(d.calls).toEqual(["controller:gateway-cell-0", "status", "controller:gateway-cell-0", "status"]);
    const changed = data({ ...input, pending: 1 });
    const c = await page(await capacityPage(request(), changed.bindings, catalog));
    if (a.controller.status === "OK" && c.controller.status === "OK") expect(c.controller.value.snapshotSha256).not.toBe(a.controller.value.snapshotSha256);
    expect(new TextEncoder().encode(JSON.stringify(a)).length).toBeLessThan(CAPACITY_LIMITS.responseBytes);
  });
  it("refuses out-of-range cursors; missing controller stays unknown, not zero", async () => {
    const d = data(); expect((await capacityPage(request("cell=0&cursor=2"), d.bindings, catalog)).status).toBe(400);
    const p = await page(await capacityPage(request(), { GATEWAY: env.GATEWAY }, catalog));
    expect(p.controller).toEqual({ status: "NOT_BOUND" }); expect(p.coverage).toBeNull(); expect(p.pageStatus).toBe("PARTIAL");
  });
  it("projects actual HTTP/version/catalog identity without returning module project IDs", async () => {
    const d = data();
    const releases = { PRIVATE_MODULE_B: { project: "PRIVATE_MODULE_B", hash: "b".repeat(64), generation: 2 }, PRIVATE_MODULE_A: { project: "PRIVATE_MODULE_A", hash: "a".repeat(64), generation: 1 } };
    const config = { ...d.bindings, WORKER_VERSION: { id: "local-worker-version", tag: "local-tag", timestamp: "2026-11-17T00:00:00Z" }, GONVEX_RUNTIME_VERSION: "local-runtime-label" };
    const a = await page(await capacityPage(request(), config, { version: "exact-catalog", releases }));
    const b = await page(await capacityPage(request(), config, { version: "exact-catalog", releases: Object.fromEntries(Object.entries(releases).reverse()) }));
    expect(a.identity.worker).toEqual(config.WORKER_VERSION); expect(a.identity.runtimeLabel).toBe("local-runtime-label");
    expect(a.identity.catalog).toEqual(b.identity.catalog); expect(JSON.stringify(a)).not.toContain("PRIVATE_MODULE_");
    expect(a.identity.catalog).toMatchObject({ status: "OK", value: { version: "exact-catalog", projects: 2 } });
  });
  it("bounds the largest source cardinality, event page and UTF8 response", async () => {
    const input = state(64), now = Date.now();
    for (const s of input.slots) {
      const w = Object.fromEntries(WINDOW_FIELDS.map(k => [k, Number.MAX_SAFE_INTEGER]));
      Object.assign(s.sample, w, { windowStart: now - 5000, windowEnd: now, windowSequence: 3, at: now });
      Object.assign(s, { pressureEvidence: { epoch: s.epoch, since: 1, window: WINDOW_FIELDS.map(k => (s.sample as any)[k]) } });
    }
    const d = data(input); const r = await capacityPage(request("cell=0&cursor=60&eventCursor=48"), d.bindings, catalog);
    const text = await r.text(); expect(r.status).toBe(200); expect(new TextEncoder().encode(text).length).toBeLessThanOrEqual(CAPACITY_LIMITS.responseBytes);
    const p = JSON.parse(text) as CapacityPage;
    expect(p.slots).toHaveLength(4); expect(p.events).toHaveLength(16); expect(p.coverage!.slots.total).toBe(64);
    expect(p.coverage!.events).toMatchObject({ total: 64, sourceLimit: 64, loss: "UNKNOWN", hasMore: false });
  });
  it("controller RPC failure/timeout stays unknown with no holder fanout", async () => {
    for (const timeout of [false, true]) {
      const d = data(); let resolve!: (v: unknown) => void;
      const held = new Promise(r => { resolve = r; });
      const bindings = { ...d.bindings, GATEWAY_CONTROLLER: { getByName: () => ({ status: () => timeout ? held : Promise.reject(new Error("DO_NOT_DISCLOSE")) }) } } as unknown as CapacityEnv;
      try {
        const p = await page(await capacityPage(request("cell=0&fresh=1"), bindings, catalog));
        expect(p.controller.status).toBe(timeout ? "TIMEOUT" : "RPC_FAILED"); expect(p.coverage).toBeNull(); expect(p.slots).toEqual([]);
        expect(p.provenance.capacityCalls).toBe(0); expect(JSON.stringify(p)).not.toContain("DO_NOT_DISCLOSE");
      } finally { resolve(null); }
    }
  });
  it("does not wake retired holders or turn missing samples/future clocks into zero freshness", async () => {
    const input: any = state(3);
    input.slots[0].mode = "retired"; input.slots[0].sample = null;
    input.slots[1].sample.at = Date.now() + 10_000;
    const d = data(input);
    const p = await page(await capacityPage(request("cell=0&fresh=1"), d.bindings, catalog));
    expect(p.slots[0]!.cached).toMatchObject({ sample: null, state: "UNKNOWN", ageMs: null });
    expect(p.slots[0]!.fresh.capacity.status).toBe("RETIRED"); expect(p.slots[1]!.cached.state).toBe("CLOCK_SKEW");
    expect(d.calls.some(c => c === "holder:auto-gw-0-0")).toBe(false);
  });
  it.each([
    (s: any) => { s.extra = "DO_NOT_DISCLOSE"; },
    (s: any) => { s.slots.push(slot(0)); },
    (s: any) => { s.slots[0].name = "auto-gw-1-0"; },
    (s: any) => { s.events[0].gateway = "foreign"; },
    (s: any) => { s.slots[0].sample.unknown = 1; },
    (s: any) => { s.slots[0].sample = { ...measured(), epoch: 9 }; },
    (s: any) => { s.slots = Array.from({ length: 65 }, (_, i) => slot(i)); },
    (s: any) => { s.slots[0].pressureEvidence = { epoch: 1, since: 1, window: ["DO_NOT_DISCLOSE"] }; },
  ])("refuses malformed, oversized-cardinality or foreign controller facts before holder lookup", async mutate => {
    const input = state(); mutate(input); const d = data(input);
    const p = await page(await capacityPage(request("cell=0&fresh=1"), d.bindings, catalog));
    expect(p.controller.status).toBe("MALFORMED"); expect(p.slots).toEqual([]); expect(d.calls.some(c => c.startsWith("holder:"))).toBe(false);
    expect(JSON.stringify(p)).not.toContain("DO_NOT_DISCLOSE");
  });
  it("fresh mode uses only four authoritative page names and at most two RPCs in flight", async () => {
    let inflight = 0, peak = 0;
    const run = async (value: unknown) => { peak = Math.max(peak, ++inflight); await new Promise(r => setTimeout(r, 2)); inflight--; return value; };
    const d = data(state(6), name => ({ capacity: async (epoch: number) => run(measured(epoch)), drainStatus: async () => run({ closed: 0, remaining: 3, blocked: { queued: 1 } }) }));
    const p = await page(await capacityPage(request("cell=0&cursor=2&fresh=1"), d.bindings, catalog));
    expect(p.pageStatus).toBe("COMPLETE"); expect(peak).toBe(2);
    expect(p.provenance).toMatchObject({ capacityCalls: 4, drainStatusCalls: 4, metricsSnapshotMayAdvance: true, socketSampleMayBeRecorded: true });
    expect(d.calls.filter(c => c.startsWith("holder:"))).toEqual(expect.arrayContaining(["holder:auto-gw-0-2", "holder:auto-gw-0-5"]));
    expect(d.calls.some(c => /auto-gw-0-[01]$/.test(c))).toBe(false);
  });
  it.each(["epoch", "unknown", "huge", "stale", "drain"])("fresh %s failure is explicit and leaks no reply", async fault => {
    const d = data(state(), () => ({ capacity: async () => fault === "epoch" ? measured(9) : fault === "unknown" ? { ...measured(), secret: "DO_NOT_DISCLOSE" } : fault === "huge" ? { payload: "DO_NOT_DISCLOSE".repeat(10_000) } : measured(1, fault === "stale" ? Date.now() - 31_000 : Date.now()),
      drainStatus: async () => ({ closed: 0, remaining: 3, blocked: { privateReason: "DO_NOT_DISCLOSE" } }) }));
    const p = await page(await capacityPage(request("cell=0&fresh=1"), d.bindings, catalog));
    expect(p.pageStatus).toBe("PARTIAL");
    if (fault === "epoch") expect(p.slots[0]!.fresh.capacity.status).toBe("EPOCH_MISMATCH");
    if (fault === "stale") expect(p.slots[0]!.fresh.capacity).toMatchObject({ status: "OK", value: { state: "STALE" } });
    if (fault === "unknown" || fault === "huge") expect(p.slots[0]!.fresh.capacity.status).toBe("MALFORMED");
    if (fault === "drain") expect(p.slots[0]!.fresh.drain.status).toBe("MALFORMED");
    expect(JSON.stringify(p)).not.toContain("DO_NOT_DISCLOSE");
  });
  it("timeout stops scheduling more work, including drain; outstanding RPCs are not falsely cancelled", async () => {
    let resolve!: () => void; const held = new Promise<void>(r => { resolve = r; });
    const d = data(state(4), () => ({ capacity: async (epoch: number) => { await held; return measured(epoch); }, drainStatus: async () => { throw new Error("must not run"); } }));
    try {
      const p = await page(await capacityPage(request("cell=0&fresh=1"), d.bindings, catalog));
      expect(p.provenance.capacityCalls).toBe(2); expect(p.provenance.drainStatusCalls).toBe(0);
      expect(p.slots.every(s => s.fresh.capacity.status === "TIMEOUT")).toBe(true);
    } finally { resolve(); }
  });
  it("abort stops new calls and propagates as typed refusal, without swallowing raw error messages", async () => {
    const abort = new AbortController();
    const d = data(state(4), () => ({ capacity: async () => { abort.abort(); throw new Error("DO_NOT_DISCLOSE"); } }));
    const p = await page(await capacityPage(request("cell=0&fresh=1", { signal: abort.signal }), d.bindings, catalog));
    expect(p.pageStatus).toBe("PARTIAL"); expect(p.provenance.capacityCalls).toBeLessThanOrEqual(2); expect(p.provenance.drainStatusCalls).toBe(0);
    expect(JSON.stringify(p)).not.toContain("DO_NOT_DISCLOSE");
  });
});

describe("K22 real composed local Controller/AssemblyGateway HTTP observations", () => {
  it("serves the authenticated route and keeps cached reads read-only across controller restart", async () => {
    const bindings = env as typeof env & { CAPACITY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject>; CAPACITY_GATEWAY: DurableObjectNamespace<AssemblyGateway> };
    const controller = bindings.CAPACITY_CONTROLLER.getByName("gateway-cell-30");
    const connection = Array.from({ length: 1024 }, (_, i) => `capacity-native-${i}`).find(id => controllerCell(id) === 30)!;
    expect(connection).toBeTypeOf("string");
    const assignment = await controller.acquire(connection);
    const holder = bindings.CAPACITY_GATEWAY.getByName(assignment.gateway);
    const before = await controller.status();
    const samples = () => runInDurableObject(holder, object => (object as unknown as { metrics: { socketSamples: number } }).metrics.socketSamples);
    const n = await samples();
    const stored = () => runInDurableObject(controller, async (_object, ctx) => ({ state: await ctx.storage.get("placement"), alarm: await ctx.storage.getAlarm() }));
    const persisted = await stored();
    const cached = await page(await SELF.fetch(request("cell=30")));
    expect(cached.slots[0]!.name).toBe(assignment.gateway); expect(cached.provenance.capacityCalls).toBe(0);
    expect(await samples()).toBe(n); expect(await stored()).toEqual(persisted);
    await evictDurableObject(controller);
    const restarted = await page(await SELF.fetch(request("cell=30")));
    expect(restarted.slots).toEqual(cached.slots); expect(await controller.status()).toEqual(before);
    expect(restarted.controller.status === "OK" && cached.controller.status === "OK" && restarted.controller.value.snapshotSha256).toBe(cached.controller.status === "OK" && cached.controller.value.snapshotSha256);
    const upgraded = await holder.fetch(new Request("https://local/ws", { headers: { upgrade: "websocket", "x-gonvex-connection-id": connection, [ASSIGNMENT_HEADER]: JSON.stringify(assignment) } }));
    expect(upgraded.status).toBe(101); upgraded.webSocket!.accept();
    try {
      const beforeFresh = await samples(), prior = await stored();
      const fresh = await page(await SELF.fetch(request("cell=30&fresh=1")));
      expect(fresh.slots[0]!.fresh.capacity).toMatchObject({ status: "OK", value: { sample: { sockets: 1, epoch: assignment.epoch } } });
      expect(fresh.slots[0]!.fresh.drain).toMatchObject({ status: "OK", value: { closed: 0, remaining: 0 } });
      expect(await samples()).toBe(beforeFresh + 1); expect(await stored()).toEqual(prior);
    } finally { upgraded.webSocket!.close(1000, "operator cleanup"); await controller.release(assignment); }
  });
});
