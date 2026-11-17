import { env, evictDurableObject, runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { ASSIGNMENT_HEADER, controllerCell, type GatewayAssignment } from "../placement";
import { ADMISSION_REASON_HEADER, UPGRADE_HEADERS } from "../../session/placement/gateway";
import { GatewayRoutingIdentity, ROUTING_IDENTITY_KEY } from "../../session/placement/routing-identity";
import { handleWebSocketUpgrade } from "../../session/router";
import type { GatewayControllerObject } from "../controller";
import type { GatewayObject } from "../../session";

const bindings = env as unknown as Omit<Env, "GATEWAY" | "GATEWAY_CONTROLLER"> & {
  GATEWAY: DurableObjectNamespace<GatewayObject>;
  GATEWAY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject>;
  ID_ONLY_GATEWAY: DurableObjectNamespace<GatewayObject>;
  ID_ONLY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject>;
};
let serial = 0;
function connection(cell: number): string { for (;;) { const id = `admission-${serial++}`; if (controllerCell(id) === cell) return id; } }
const control = (cell: number, idOnly = false) => (idOnly ? bindings.ID_ONLY_CONTROLLER : bindings.GATEWAY_CONTROLLER).getByName(`gateway-cell-${cell}`);
const holder = (name: string, idOnly = false) => (idOnly ? bindings.ID_ONLY_GATEWAY : bindings.GATEWAY).getByName(name);
const request = (a: GatewayAssignment, connectionId = a.connectionId) => new Request("https://x/ws", { headers: {
  upgrade: "websocket", [UPGRADE_HEADERS.connectionId]: connectionId, [ASSIGNMENT_HEADER]: JSON.stringify(a),
} });
const sockets: WebSocket[] = [];
afterEach(() => { for (const ws of sockets.splice(0)) ws.close(1000); });
function accepted(response: Response) { expect(response.status).toBe(101); response.webSocket!.accept(); sockets.push(response.webSocket!); }
async function failure(response: Response, reason: string, body = "gateway assignment required") {
  expect(response.status).toBe(503);
  expect(await response.text()).toBe(body);
  expect(response.headers.get(ADMISSION_REASON_HEADER)).toBe(reason);
  // No reflected routing names, IDs, lease tokens or caller headers.
  expect([...response.headers.keys()].sort()).toEqual(["content-type", ADMISSION_REASON_HEADER].sort());
}
async function identity(name: string, idOnly = false) {
  return runInDurableObject(holder(name, idOnly), (object, state) => ({
    stored: state.storage.kv.get(ROUTING_IDENTITY_KEY), name: object.gatewayName,
  }));
}

describe("authenticated gateway routing identity and content-free admission failures", () => {
  it("distinguishes the three guard branches without consuming the valid claim or poisoning identity", async () => {
    const c = control(17), a = await c.acquire(connection(17)), h = holder(a.gateway);
    for (const raw of [null, "broken", "null", "{}", "x".repeat(1025)]) {
      const headers = new Headers({ upgrade: "websocket", [UPGRADE_HEADERS.connectionId]: a.connectionId });
      if (raw !== null) headers.set(ASSIGNMENT_HEADER, raw);
      await failure(await h.fetch(new Request("https://x/ws", { headers })), "parse-invalid");
    }
    await failure(await holder("another-holder").fetch(request(a)), "holder-identity");
    await failure(await h.fetch(request(a, "wrong-connection")), "connection-mismatch");
    expect((await identity(a.gateway)).stored).toBeUndefined();
    expect((await identity("another-holder")).stored).toBeUndefined();
    expect((await c.status()).pending).toBe(1);
    accepted(await h.fetch(request(a)));
    expect((await identity(a.gateway)).stored).toEqual({ v: 1, name: a.gateway });
  });

  it("refuses forged, replayed and expired leases without learning an unclaimed name", async () => {
    const c = control(18, true), a = await c.acquire(connection(18)), h = holder(a.gateway, true);
    await failure(await h.fetch(request({ ...a, token: "forged" })), "claim-refused", "gateway assignment expired");
    expect((await identity(a.gateway, true)).stored).toBeUndefined();
    accepted(await h.fetch(request(a)));
    await failure(await h.fetch(request(a)), "claim-refused", "gateway assignment expired");
    const expired = await c.acquire(connection(18));
    await runInDurableObject(c, async (_object, state) => {
      const s = await state.storage.get<any>("placement");
      s.leases.find((l: any) => l.assignment.token === expired.token).assignment.expiresAt = Date.now() - 1;
      await state.storage.put("placement", s);
    });
    await failure(await h.fetch(request(expired)), "claim-refused", "gateway assignment expired");
    expect((await identity(a.gateway, true)).stored).toEqual({ v: 1, name: a.gateway });
  });

  it("uses native DO-ID equality on first admission, then restores the claimed name before sessions on ID-only reconstruction", async () => {
    const c = control(19, true), a = await c.acquire(connection(19)), h = holder(a.gateway, true);
    await runInDurableObject(h, (instance, state) => {
      const object = instance as any;
      expect(object.ctx.id.name).toBeUndefined(); // Explicit seam; not a claim about production ctx.id.name.
      expect(bindings.ID_ONLY_GATEWAY.idFromName(a.gateway).equals(object.ctx.id)).toBe(true);
      expect(state.storage.kv.get(ROUTING_IDENTITY_KEY)).toBeUndefined();
    });
    accepted(await h.fetch(request(a)));
    await evictDurableObject(h);
    const byId = bindings.ID_ONLY_GATEWAY.get(bindings.ID_ONLY_GATEWAY.idFromString(h.id.toString()));
    await runInDurableObject(byId, (instance, state) => {
      const object = instance as any;
      expect(object.ctx.id.name).toBeUndefined();
      expect(object.gatewayName).toBe(a.gateway);
      expect([...object.host.sessions.values()].map((s: any) => s.view().host)).toEqual([a.gateway]);
      expect(state.storage.kv.get(ROUTING_IDENTITY_KEY)).toEqual({ v: 1, name: a.gateway });
    });
    await runDurableObjectAlarm(byId);
    const next = await c.acquire(connection(19));
    expect(next.gateway).toBe(a.gateway);
    accepted(await byId.fetch(request(next)));
    expect((await byId.capacity(a.epoch)).sockets).toBe(2);
  });

  it("preserves normal named reconstruction without asserting the remote name hypothesis", async () => {
    const c = control(20), a = await c.acquire(connection(20)), h = holder(a.gateway);
    accepted(await h.fetch(request(a)));
    await evictDurableObject(h);
    const byId = bindings.GATEWAY.get(bindings.GATEWAY.idFromString(h.id.toString()));
    expect((await identity(a.gateway)).name).toBe(a.gateway);
    const next = await c.acquire(connection(20));
    accepted(await byId.fetch(request(next)));
    await runInDurableObject(byId, (instance) => {
      const object = instance as any;
      expect([...object.host.sessions.values()].map((s: any) => s.view().host)).toEqual([a.gateway, a.gateway]);
    });
  });

  it("rejects a cross-holder stored identity on wake without replacing it or consuming a valid lease", async () => {
    const c = control(21), a = await c.acquire(connection(21)), h = holder(a.gateway);
    await runInDurableObject(h, (_object, state) => state.storage.kv.put(ROUTING_IDENTITY_KEY, { v: 1, name: "wrong-holder" }));
    await evictDurableObject(h);
    await failure(await h.fetch(request(a)), "holder-identity");
    expect((await identity(a.gateway)).stored).toEqual({ v: 1, name: "wrong-holder" });
    expect((await c.status()).pending).toBe(1);
  });

  it("rejects an assignment for the other real holder without poisoning a fresh ID-only destination", async () => {
    const c = control(22, true), a = await c.acquire(connection(22));
    await failure(await holder("unrelated", true).fetch(request(a)), "holder-identity");
    expect((await identity("unrelated", true)).stored).toBeUndefined();
    await failure(await holder(a.gateway, true).fetch(request({ ...a, gateway: "unrelated" })), "holder-identity");
    expect((await identity(a.gateway, true)).stored).toBeUndefined();
    accepted(await holder(a.gateway, true).fetch(request(a)));
  });

  it("keeps wrong-cell, wrong-epoch and wrong-connection forgeries outside durable identity", async () => {
    const c = control(24, true), a = await c.acquire(connection(24)), h = holder(a.gateway, true);
    for (const forged of [{ ...a, cell: 25 }, { ...a, epoch: a.epoch + 1 }, { ...a, connectionId: "forged" }]) {
      await failure(await h.fetch(request(forged)), "claim-refused", "gateway assignment expired");
      expect((await identity(a.gateway, true)).stored).toBeUndefined();
    }
    accepted(await h.fetch(request(a)));
  });

  it("reads one identity key on construction and writes it once after claim, never per admission or frame", async () => {
    const c = control(25, true), a = await c.acquire(connection(25)), h = holder(a.gateway, true);
    await runInDurableObject(h, async (object, state) => {
      let reads = 0, writes = 0;
      const ctx = { id: bindings.ID_ONLY_GATEWAY.idFromString(state.id.toString()), storage: { kv: {
        get: (key: string) => { reads++; return state.storage.kv.get(key); },
        put: (key: string, value: unknown) => { writes++; state.storage.kv.put(key, value); },
      } } } as unknown as DurableObjectState;
      const routing = new GatewayRoutingIdentity(ctx, bindings.ID_ONLY_GATEWAY);
      expect([reads, writes]).toEqual([1, 0]);
      expect(routing.matches(a.gateway)).toBe(true);
      expect(routing.matches("another-holder")).toBe(false);
      expect([reads, writes]).toEqual([1, 0]);
      // Construct the RPC stub in the holder's I/O context.
      const owner = (object as any).env.GATEWAY_CONTROLLER.getByName("gateway-cell-25");
      expect(await owner.claim(a)).toBe(true); // Actual controller authority, not an invented ACK.
      routing.claimed(a.gateway);
      routing.claimed(a.gateway);
      expect([reads, writes]).toEqual([1, 1]);
      const restored = new GatewayRoutingIdentity(ctx, bindings.ID_ONLY_GATEWAY);
      restored.claimed(a.gateway);
      expect([reads, writes]).toEqual([2, 1]);
      expect(restored.name).toBe(a.gateway);
    });
  });

  it("does not relabel restored legacy sessions from an incoming assignment when no established identity is recoverable", async () => {
    const c = control(23, true), a = await c.acquire(connection(23)), h = holder(a.gateway, true);
    accepted(await h.fetch(request(a)));
    await runInDurableObject(h, (_object, state) => state.storage.kv.delete(ROUTING_IDENTITY_KEY));
    await evictDurableObject(h);
    const next = await c.acquire(connection(23));
    await failure(await h.fetch(request(next)), "identity-unavailable");
    expect((await identity(a.gateway, true)).stored).toBeUndefined();
  });

  it("roundtrips the authoritative assignment, overwrites all client placement headers and forwards only to its derived destination", async () => {
    const seen: string[] = [];
    let authoritative!: GatewayAssignment;
    const wrapped = { ...bindings,
      GATEWAY_CONTROLLER: { getByName(name: string) {
        const actual = bindings.GATEWAY_CONTROLLER.getByName(name);
        return { acquire: async (id: string) => authoritative = await actual.acquire(id), release: (a: GatewayAssignment) => actual.release(a) };
      } },
      GATEWAY: { getByName(name: string) {
        seen.push(name);
        return { fetch: async (r: Request) => {
          expect(r.headers.get(ASSIGNMENT_HEADER)).toBe(JSON.stringify(authoritative));
          expect(r.headers.get(UPGRADE_HEADERS.connectionId)).toBe(authoritative.connectionId);
          expect(r.headers.get(UPGRADE_HEADERS.publicTransport)).toBe("0");
          expect(r.headers.get(UPGRADE_HEADERS.address)).toBe("192.0.2.44");
          expect(r.headers.get(UPGRADE_HEADERS.slots)).toBe("pending");
          expect(name).toBe(authoritative.gateway);
          expect(bindings.GATEWAY.idFromName(name).equals(holder(name).id)).toBe(true);
          return holder(name).fetch(r);
        } };
      } },
    } as unknown as typeof bindings;
    accepted(await handleWebSocketUpgrade(new Request("https://x/ws", { headers: { upgrade: "websocket", "cf-connecting-ip": "192.0.2.44",
      [ASSIGNMENT_HEADER]: "client-forgery", ...Object.fromEntries(Object.values(UPGRADE_HEADERS).map(h => [h, "client-forgery"])),
    } }), wrapped));
    expect(seen).toEqual([authoritative.gateway]);
  });

  it("preserves a holder refusal reason through the router and independently releases placement and address slots", async () => {
    let authoritative!: GatewayAssignment;
    const wrapped = { ...bindings,
      GATEWAY_CONTROLLER: { getByName(name: string) {
        const actual = bindings.GATEWAY_CONTROLLER.getByName(name);
        return { acquire: async (id: string) => authoritative = await actual.acquire(id), release: (a: GatewayAssignment) => actual.release(a) };
      } },
      GATEWAY: { getByName(name: string) { return { fetch: (r: Request) => {
        const headers = new Headers(r.headers);
        headers.delete(ASSIGNMENT_HEADER); // Controlled transport fault, not attributed to live routing.
        return holder(name).fetch(new Request(r, { headers }));
      } }; } },
    } as unknown as typeof bindings;
    await failure(await handleWebSocketUpgrade(new Request("https://x/ws", { headers: {
      upgrade: "websocket", "cf-connecting-ip": "192.0.2.45",
    } }), wrapped), "parse-invalid");
    expect((await control(authoritative.cell).status()).pending).toBe(0);
    const { limiterFor } = await import("../../public/limiter");
    expect(await limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.45").counts()).toEqual({ pending: 0, public: 0 });
  });
});
