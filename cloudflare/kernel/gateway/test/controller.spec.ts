import { env, evictDurableObject, runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { GatewayControllerObject } from "../controller";
import { ASSIGNMENT_HEADER, controllerCell, MAX_PENDING, LEASE_MS, SCALE_IN_MS, type GatewayAssignment } from "../placement";
import { handleWebSocketUpgrade } from "../../session/router";

type Bindings = Env & { GATEWAY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject> };
const bindings = env as Bindings;
let serial = 0;
function id(cell = 0): string { for (;;) { const s = `x01-${serial++}`; if (controllerCell(s) === cell) return s; } }
const cell = (n = 0) => bindings.GATEWAY_CONTROLLER.getByName(`gateway-cell-${n}`);
const upgrade = (a: GatewayAssignment) => bindings.GATEWAY.getByName(a.gateway).fetch(new Request("https://x/ws", { headers: {
  upgrade: "websocket", "x-gonvex-connection-id": a.connectionId, [ASSIGNMENT_HEADER]: JSON.stringify(a),
} }));
async function expire(c = cell()) {
  await runInDurableObject(c, async (_object, state) => {
    const s = (await state.storage.get<any>("placement"))!;
    for (const l of s.leases) l.assignment.expiresAt = Date.now() - 1;
    await state.storage.put("placement", s);
  });
  await runDurableObjectAlarm(c);
}

describe("X01 durable placement in workerd (package harness)", () => {
  it("claims exactly once, fences forgery, survives controller eviction, and keeps socket ownership beyond admission TTL", async () => {
    const c = cell(1), a = await c.acquire(id(1));
    expect((await upgrade({ ...a, token: "forged" })).status).toBe(503);
    const r = await upgrade(a);
    expect(r.status).toBe(101);
    r.webSocket!.accept();
    expect((await upgrade(a)).status).toBe(503);
    await evictDurableObject(c);
    expect((await c.status()).slots[0]!.name).toBe(a.gateway);
    await expire(c);
    const holder = bindings.GATEWAY.getByName(a.gateway) as unknown as { capacity(n: number): Promise<{ sockets: number }> };
    expect((await holder.capacity(a.epoch)).sockets).toBe(1);
    r.webSocket!.close();
  });
  it("grows a cell at the reservation threshold without environment changes", async () => {
    const c = cell(2);
    const assignments: GatewayAssignment[] = [];
    for (let i = 0; i < 126; i++) assignments.push(await c.acquire(id(2)));
    expect(new Set(assignments.map(a => a.gateway)).size).toBe(2);
    const original = assignments[0]!;
    expect(await c.acquire(original.connectionId)).toEqual(original);
    for (const a of assignments) await c.release(a);
    expect((await c.status()).pending).toBe(0);
  });
  it("bounds pending reservations and recovers lost responses by TTL", async () => {
    const c = cell(3);
    for (let i = 0; i < MAX_PENDING; i++) await c.acquire(id(3));
    await expect((async () => await c.acquire(id(3)))()).rejects.toThrow("admissions exhausted");
    await expire(c);
    expect((await c.status()).pending).toBe(0);
    const a = await c.acquire(id(3));
    expect(a.expiresAt - Date.now()).toBeLessThanOrEqual(LEASE_MS);
  });
  it("rejects wrong-cell keys and owner-mismatched release without changing a lease", async () => {
    const c = cell(4), a = await c.acquire(id(4));
    await expect((async () => await c.acquire(id(5)))()).rejects.toThrow("invalid placement key");
    await c.release({ ...a, connectionId: "another" });
    expect((await c.status()).pending).toBe(1);
    await c.release(a); await c.release(a);
    expect((await c.status()).pending).toBe(0);
  });
  it("shrinks empty capacity only after hysteresis and has no idle alarm loop", async () => {
    const c = cell(5);
    const list = [];
    for (let i = 0; i < 126; i++) list.push(await c.acquire(id(5)));
    for (const a of list) await c.release(a);
    expect((await c.status()).slots.filter(s => s.mode === "active")).toHaveLength(2);
    await runInDurableObject(c, async (_object, state) => {
      const s = await state.storage.get<any>("placement");
      for (const slot of s.slots) { slot.quietSince = Date.now() - SCALE_IN_MS - 1; slot.changedAt = slot.quietSince; }
      await state.storage.put("placement", s);
    });
    await runDurableObjectAlarm(c);
    expect((await c.status()).slots.filter(s => s.mode === "active")).toHaveLength(1);
    expect((await c.status()).slots.filter(s => s.mode === "retired")).toHaveLength(1);
    await runInDurableObject(c, async (_object, state) => expect(await state.storage.getAlarm()).toBeNull());
  });
  it("strips forged router assignment headers and fails closed on controller outage", async () => {
    const r = await handleWebSocketUpgrade(new Request("https://x/ws", { headers: { upgrade: "websocket", [ASSIGNMENT_HEADER]: "forged" } }), bindings);
    expect(r.status).toBe(101); r.webSocket!.accept(); r.webSocket!.close();
    const broken = { ...bindings, GATEWAY_CONTROLLER: { getByName() { throw new Error("down"); } } } as unknown as Bindings;
    const fail = await handleWebSocketUpgrade(new Request("https://x/ws", { headers: { upgrade: "websocket" } }), broken);
    expect(fail.status).toBe(503);
  });
  it("grows a replacement for sustained existing-holder outbound pressure and never retires its live socket", async () => {
    const c = cell(6), a = await c.acquire(id(6));
    const accepted = await upgrade(a);
    accepted.webSocket!.accept();
    await expire(c);
    await runInDurableObject(c, async (_object, state) => {
      const s = await state.storage.get<any>("placement");
      s.slots[0].changedAt = Date.now() - 61_000;
      s.slots[0].pressuredSince = Date.now() - 11_000;
      await state.storage.put("placement", s);
    });
    await c.observe(a.gateway, { v: 1, epoch: a.epoch, at: Date.now(), sockets: 1,
      framesPerSecond: 0, bytesPerSecond: 5 * 1024 * 1024, deliveryMs: 0, queueAgeMs: 0, rpcMs: 0 });
    const status = await c.status();
    expect(status.slots).toHaveLength(2);
    expect(status.slots.find(s => s.name === a.gateway)!.mode).toBe("draining");
    expect(status.slots.filter(s => s.mode === "active")).toHaveLength(1);
    expect((await (bindings.GATEWAY.getByName(a.gateway) as any).capacity(a.epoch)).sockets).toBe(1);
    accepted.webSocket!.close();
  });
  it("fences stale reports and delayed admission after a retired slot is reused", async () => {
    const c = cell(7), a = await c.acquire(id(7));
    await c.release(a);
    await runInDurableObject(c, async (_object, state) => {
      const s = await state.storage.get<any>("placement");
      s.slots[0].mode = "retired";
      await state.storage.put("placement", s);
    });
    const b = await c.acquire(id(7));
    expect(b.gateway).toBe(a.gateway); expect(b.epoch).toBeGreaterThan(a.epoch);
    expect(await c.claim(a)).toBe(false);
    await c.observe(a.gateway, { v: 1, epoch: a.epoch, at: Date.now(), sockets: 0,
      framesPerSecond: 0, bytesPerSecond: 0, deliveryMs: 0, queueAgeMs: 0, rpcMs: 0 });
    expect((await c.status()).slots[0]!.epoch).toBe(b.epoch);
  });
  it("releases placement and K16 after a failed forward", async () => {
    const fake = { ...bindings, GATEWAY: { getByName() { return { async fetch() { throw new Error("forward failed"); } }; } } } as unknown as Bindings;
    const request = new Request("https://x/ws", { headers: { upgrade: "websocket", "CF-Connecting-IP": "192.0.2.222" } });
    expect((await handleWebSocketUpgrade(request, fake)).status).toBe(503);
    const { limiterFor } = await import("../../public/limiter");
    expect(await limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.222").counts()).toEqual({ pending: 0, public: 0 });
  });

  it("does not leak K16 slots when its admission audit outlives the router timeout", async () => {
    const { AddressBudget, MemorySlotStore, ADMISSION_EXPIRED_ERROR } = await import("../../public/limiter");
    const store = new MemorySlotStore();
    store.put("old", { gateway: "old", at: Date.now() - 60_000, kinds: ["pending", "public"] });
    let release!: () => void;
    const audit = new Promise<void>(resolve => { release = resolve; });
    const budget = new AddressBudget(() => ({ liveConnections: async () => { await audit; return []; } }), Date.now, store);
    let admission!: ReturnType<typeof budget.admit>;
    const limiter = {
      admit(request: Parameters<typeof budget.admit>[0]) {
        expect(request.expiresAt).toBeGreaterThan(Date.now());
        return admission = budget.admit({ ...request, pendingLimit: 1, publicLimit: 1 });
      },
      async release(request: Parameters<typeof budget.release>[0]) { budget.release(request); },
    };
    const fake = { ...bindings, PUBLIC_LIMITER: { getByName() { return limiter; } } } as unknown as Bindings;
    try {
      const response = await handleWebSocketUpgrade(new Request("https://x/ws?public=1", {
        headers: { upgrade: "websocket", "CF-Connecting-IP": "192.0.2.223" },
      }), fake);
      expect(response.status).toBe(503);
      release();
      expect(await admission).toEqual({ ok: false, error: ADMISSION_EXPIRED_ERROR });
      expect(store.entries.size).toBe(0);
      expect(await budget.admit({ v: 1, connectionId: "next", gateway: "gw", publicTransport: true,
        pendingLimit: 1, publicLimit: 1, expiresAt: Date.now() + 2_000 })).toEqual({ ok: true });
    } finally { release(); await admission; }
  });

});

it("awaits every recovery receiver and refuses an old-session acknowledgment", async () => {
  const a = await cell(8).acquire(id(8));
  const response = await upgrade(a); response.webSocket!.accept();
  await runInDurableObject(bindings.GATEWAY.getByName(a.gateway), async (object) => {
    const gateway = object as any;
    const socket = [...gateway.host.sessions.keys()][0];
    const session = gateway.host.sessions.get(socket);
    const tenant = "recovery-fixture";
    gateway.byTenant.set(tenant, new Set([socket]));
    const previous = session.record.tenant;
    session.record.tenant = { anonymous: false, route: { tenantId: tenant } };
    const payload = { v: 1, instance: "test", shared: [], sockets: {}, recovery: { reason: "change-feed-lagged" } };
    const original = session.recoverReplica;
    try {
      session.recoverReplica = undefined;
      expect((await gateway.deliverReplica(tenant, payload)).recoveryAck).toBeUndefined();
      let release!: () => void;
      const held = new Promise<void>(resolve => { release = resolve; });
      session.recoverReplica = async () => { await held; };
      let acknowledged = false;
      const recovery = gateway.deliverReplica(tenant, payload).then((r: any) => { acknowledged = r.recoveryAck === 1; });
      await Promise.resolve(); expect(acknowledged).toBe(false);
      release(); await recovery; expect(acknowledged).toBe(true);
      session.recoverReplica = async () => { throw new Error("query rebuild failed"); };
      await expect(gateway.deliverReplica(tenant, payload)).rejects.toThrow("query rebuild failed");
    } finally {
      session.recoverReplica = original;
      session.record.tenant = previous;
      gateway.byTenant.delete(tenant);
    }
  });
  response.webSocket!.close();
});

it("interleaves allocation probes and claims without a controller/holder admission deadlock", async () => {
  const c = cell(9);
  const responses = await Promise.all(Array.from({ length: 8 }, async () => upgrade(await c.acquire(id(9)))));
  try {
    for (const response of responses) { expect(response.status).toBe(101); response.webSocket!.accept(); }
    expect((await c.status()).slots).toHaveLength(1);
  } finally { for (const response of responses) response.webSocket?.close(); }
});


it("allocates more than eight global holders across cells without an environment change", async () => {
  const assignments = await Promise.all(Array.from({ length: 16 }, (_, n) => cell(n).acquire(id(n))));
  expect(new Set(assignments.map(a => a.gateway)).size).toBe(16);
  await Promise.all(assignments.map(a => cell(a.cell).release(a)));
});
