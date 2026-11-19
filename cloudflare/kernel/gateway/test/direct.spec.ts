// X17g: connect in one hop. The router sends an upgrade straight to the holder its isolate admitted the last upgrade
// to; that holder admits it with one controller call (`admitDirect`) and the address limiter in parallel. Anything the
// holder does not take falls back to the controller path (`acquire` -> limiter -> holder `claim`).
import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ASSIGNMENT_HEADER, DIRECT_HEADER, MAX_SOCKETS, controllerCell, type DirectAdmission, type GatewayAssignment } from "../placement";
import { ADMISSION_REASON_HEADER, UPGRADE_HEADERS } from "../../session/placement/gateway";
import { handleWebSocketUpgrade, resetWarmCell, WarmCell } from "../../session/router";
import type { GatewayControllerObject } from "../controller";
import type { GatewayObject } from "../../session";

const bindings = env as unknown as Omit<Env, "GATEWAY" | "GATEWAY_CONTROLLER"> & {
  GATEWAY: DurableObjectNamespace<GatewayObject>;
  GATEWAY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject>;
};
let serial = 0;
function connection(cell: number): string { for (;;) { const id = `x17g-${serial++}`; if (controllerCell(id) === cell) return id; } }
const control = (cell: number) => bindings.GATEWAY_CONTROLLER.getByName(`gateway-cell-${cell}`);
const holder = (name: string) => bindings.GATEWAY.getByName(name);
const sockets: WebSocket[] = [];
beforeEach(() => resetWarmCell());
afterEach(() => { for (const ws of sockets.splice(0)) ws.close(1000); });
function accepted(response: Response) { expect(response.status).toBe(101); response.webSocket!.accept(); sockets.push(response.webSocket!); }

/** A holder serving `cell`: one ordinary admission (acquire + claim) through it. */
async function serving(cell: number): Promise<GatewayAssignment> {
  const a = await control(cell).acquire(connection(cell));
  accepted(await holder(a.gateway).fetch(new Request("https://x/ws", { headers: {
    upgrade: "websocket", [UPGRADE_HEADERS.connectionId]: a.connectionId, [ASSIGNMENT_HEADER]: JSON.stringify(a),
  } })));
  return a;
}
function direct(d: DirectAdmission, extra: Record<string, string> = {}, connectionId = d.connectionId) {
  return new Request("https://x/ws", { headers: {
    upgrade: "websocket", [UPGRADE_HEADERS.connectionId]: connectionId, [UPGRADE_HEADERS.address]: "192.0.2.90",
    [UPGRADE_HEADERS.publicTransport]: "0", [DIRECT_HEADER]: JSON.stringify(d), ...extra,
  } });
}
async function refusal(response: Response, reason: string) {
  expect(response.status).toBe(503);
  expect(response.headers.get(ADMISSION_REASON_HEADER)).toBe(reason);
}

describe("X17g controller admitDirect", () => {
  it("grants a claimed lease on the serving slot and nothing else", async () => {
    const a = await serving(3), c = control(3);
    const d: DirectAdmission = { v: 1, cell: 3, gateway: a.gateway, epoch: a.epoch, connectionId: connection(3) };
    const granted = (await c.admitDirect(d, 1))!;
    expect(granted).toMatchObject({ v: 1, cell: 3, gateway: a.gateway, epoch: a.epoch, connectionId: d.connectionId });
    // Already claimed: the token cannot be claimed again, and the same connection is not admitted twice.
    expect(await c.claim(granted)).toBe(false);
    expect(await c.admitDirect(d, 1)).toBeNull();
    // Wrong epoch, holder, cell or a full holder: refused (the router falls back to acquire).
    expect(await c.admitDirect({ ...d, connectionId: connection(3), epoch: a.epoch + 1 }, 1)).toBeNull();
    expect(await c.admitDirect({ ...d, connectionId: connection(3), gateway: "auto-gw-3-99" }, 1)).toBeNull();
    expect(await c.admitDirect({ ...d, connectionId: connection(4) }, 1)).toBeNull();
    expect(await c.admitDirect({ ...d, connectionId: connection(3) }, MAX_SOCKETS)).toBeNull();
  });
});

describe("X17g gateway direct admission", () => {
  it("admits a direct upgrade with one controller call and reserves the address slot", async () => {
    const a = await serving(5);
    const before = (await control(5).status()).pending;
    const d: DirectAdmission = { v: 1, cell: 5, gateway: a.gateway, epoch: a.epoch, connectionId: connection(5) };
    accepted(await holder(a.gateway).fetch(direct(d)));
    expect((await control(5).status()).pending).toBe(before + 1);
    const { limiterFor } = await import("../../public/limiter");
    expect((await limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.90").counts()).pending).toBeGreaterThanOrEqual(1);
  });

  it("refuses what it must not take with a direct-* reason, without a lease or a slot", async () => {
    const a = await serving(6);
    const before = (await control(6).status()).pending;
    const d: DirectAdmission = { v: 1, cell: 6, gateway: a.gateway, epoch: a.epoch, connectionId: connection(6) };
    await refusal(await holder(a.gateway).fetch(direct(d, { [DIRECT_HEADER]: "broken" })), "direct-invalid");
    await refusal(await holder(a.gateway).fetch(direct(d, {}, "another-connection")), "direct-invalid");
    await refusal(await holder("x17g-other-holder").fetch(direct(d)), "direct-identity");
    await refusal(await holder(a.gateway).fetch(direct({ ...d, epoch: a.epoch + 1 })), "direct-unavailable");
    expect((await control(6).status()).pending).toBe(before);
    const { limiterFor } = await import("../../public/limiter");
    expect(await limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.91").counts()).toEqual({ pending: 0, public: 0 });
  });

  it("answers a limiter refusal itself (429, as the router does) and holds no slot for it", async () => {
    const a = await serving(7);
    const { limiterFor } = await import("../../public/limiter");
    const limiter = limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.92");
    for (let i = 0; i < 20; i++) expect((await limiter.admit({ v: 1, connectionId: `x17g-fill-${i}`, gateway: a.gateway, publicTransport: true, pendingLimit: 100, publicLimit: 20 })).ok).toBe(true);
    const d: DirectAdmission = { v: 1, cell: 7, gateway: a.gateway, epoch: a.epoch, connectionId: connection(7) };
    const response = await holder(a.gateway).fetch(direct(d, { [UPGRADE_HEADERS.address]: "192.0.2.92", [UPGRADE_HEADERS.publicTransport]: "1" }));
    expect(response.status).toBe(429);
    expect((await limiter.counts()).public).toBe(20);
  });
});

describe("X17g router: one hop after the first admission", () => {
  function counting() {
    const calls = { acquire: 0, fetches: [] as Array<{ name: string; direct: string | null; assignment: string | null }> };
    const wrapped = { ...bindings,
      GATEWAY_CONTROLLER: { getByName(name: string) {
        const actual = bindings.GATEWAY_CONTROLLER.getByName(name);
        return { acquire: (id: string) => { calls.acquire++; return actual.acquire(id); }, release: (a: GatewayAssignment) => actual.release(a),
          admitDirect: (d: DirectAdmission, n: number) => actual.admitDirect(d, n), claim: (a: GatewayAssignment) => actual.claim(a) };
      } },
      GATEWAY: { getByName(name: string) { return { fetch: (r: Request) => {
        calls.fetches.push({ name, direct: r.headers.get(DIRECT_HEADER), assignment: r.headers.get(ASSIGNMENT_HEADER) });
        return holder(name).fetch(r);
      } }; } },
    } as unknown as typeof bindings;
    return { calls, wrapped };
  }
  const upgrade = (extra: Record<string, string> = {}) => new Request("https://x/ws", { headers: { upgrade: "websocket", "cf-connecting-ip": "192.0.2.93", ...extra } });

  it("the second upgrade goes straight to the first one's holder, with no acquire", async () => {
    const { calls, wrapped } = counting();
    accepted(await handleWebSocketUpgrade(upgrade(), wrapped));
    expect(calls.acquire).toBe(1);
    const first = calls.fetches[0]!;
    accepted(await handleWebSocketUpgrade(upgrade(), wrapped));
    expect(calls.acquire).toBe(1);
    expect(calls.fetches[1]).toMatchObject({ name: first.name, assignment: null });
    expect(JSON.parse(calls.fetches[1]!.direct!)).toMatchObject({ v: 1, gateway: first.name });
  });

  it("falls back to the controller when the holder refuses the direct upgrade", async () => {
    const { calls, wrapped } = counting();
    accepted(await handleWebSocketUpgrade(upgrade(), wrapped));
    const first = JSON.parse(calls.fetches[0]!.assignment!) as GatewayAssignment;
    // A stale memory: the holder's epoch moved on. The holder refuses (direct-unavailable); the router acquires.
    const stale = new WarmCell();
    stale.admitted(first.cell, { gateway: first.gateway, epoch: first.epoch + 1 });
    resetWarmCell(stale);
    accepted(await handleWebSocketUpgrade(upgrade(), wrapped));
    expect(calls.acquire).toBe(2);
    expect(calls.fetches.slice(1).map((f) => [f.direct !== null, f.assignment !== null])).toEqual([[true, false], [false, true]]);
  });

  it("a limiter refusal on the direct path is the client's answer and forgets the holder", async () => {
    const { calls, wrapped } = counting();
    accepted(await handleWebSocketUpgrade(upgrade(), wrapped));
    const first = JSON.parse(calls.fetches[0]!.assignment!) as GatewayAssignment;
    const { limiterFor } = await import("../../public/limiter");
    const limiter = limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.95");
    for (let i = 0; i < 20; i++) expect((await limiter.admit({ v: 1, connectionId: `x17g-cap-${i}`, gateway: first.gateway, publicTransport: true, pendingLimit: 100, publicLimit: 20 })).ok).toBe(true);
    const refused = await handleWebSocketUpgrade(new Request("https://x/ws?public=1", { headers: { upgrade: "websocket", "cf-connecting-ip": "192.0.2.95" } }), wrapped);
    expect(refused.status).toBe(429);
    expect(calls.acquire).toBe(1);
    expect(calls.fetches[1]!.direct).not.toBeNull();
    // The next upgrade goes through the controller again.
    accepted(await handleWebSocketUpgrade(upgrade(), wrapped));
    expect(calls.acquire).toBe(2);
    expect(calls.fetches[2]!.direct).toBeNull();
  });

  it("removes a client's direct header on the controller path", async () => {
    const { calls, wrapped } = counting();
    const forged: DirectAdmission = { v: 1, cell: 0, gateway: "auto-gw-0-0", epoch: 1, connectionId: connection(0) };
    accepted(await handleWebSocketUpgrade(upgrade({ [DIRECT_HEADER]: JSON.stringify(forged) }), wrapped));
    expect(calls.fetches).toHaveLength(1);
    expect(calls.fetches[0]!.direct).toBeNull();
    expect(calls.fetches[0]!.assignment).not.toBeNull();
  });
});

describe("review #533: a limiter refusal on the direct path holds no lease", () => {
  it("direct upgrades the limiter refuses (429) leave no claimed lease, as the controller path releases its lease", async () => {
    const a = await serving(9);
    const { limiterFor } = await import("../../public/limiter");
    const limiter = limiterFor(bindings.PUBLIC_LIMITER!, "192.0.2.94");
    for (let i = 0; i < 20; i++) expect((await limiter.admit({ v: 1, connectionId: `r533-fill-${i}`, gateway: a.gateway, publicTransport: true, pendingLimit: 100, publicLimit: 20 })).ok).toBe(true);
    const before = (await control(9).status()).pending;
    // An address over its connection cap keeps retrying; the router's isolate still remembers this holder.
    for (let i = 0; i < 10; i++) {
      const d: DirectAdmission = { v: 1, cell: 9, gateway: a.gateway, epoch: a.epoch, connectionId: connection(9) };
      expect((await holder(a.gateway).fetch(direct(d, { [UPGRADE_HEADERS.address]: "192.0.2.94", [UPGRADE_HEADERS.publicTransport]: "1" }))).status).toBe(429);
    }
    // Each refused attempt left a claimed lease for LEASE_MS (30 s): MAX_SOCKETS (125) of them fill the holder for
    // `acquire` and `admitDirect`; they also block its scale-in. The controller path releases its unclaimed lease.
    expect((await control(9).status()).pending).toBe(before);
  });
});
