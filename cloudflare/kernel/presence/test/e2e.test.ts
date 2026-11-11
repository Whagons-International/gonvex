/**
 * Presence end to end inside workerd: router -> gateway object -> session ->
 * tenant object (K05's stub, which embeds `TenantPresence` as the real tenant
 * object will). Frame parity with Rust is the conformance harness's job
 * (`presence`, `presence-edges`); this covers what it cannot reach: an evicted
 * tenant object rebuilding from the gateways' attachments, an evicted gateway,
 * and the grace end on the tenant's alarm.
 */
import { env, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";

const BASE = "https://gonvex.test";
const KEY = "test-key";
type Frame = Record<string, any>;

async function stub(method: string, route: string, body?: unknown): Promise<any> {
  const r = await exports.default.fetch(new Request(`${BASE}/_stub/session/${route}`, {
    method, headers: { authorization: `Bearer ${KEY}`, ...(body ? { "content-type": "application/json" } : {}) }, body: body ? JSON.stringify(body) : null,
  }));
  expect(r.status, await r.clone().text()).toBe(200);
  return r.json();
}

class Client {
  readonly frames: Frame[] = [];
  private waiters: Array<() => void> = [];
  constructor(readonly ws: WebSocket) {
    ws.accept();
    ws.addEventListener("message", (e) => {
      const f = JSON.parse(String(e.data));
      // Presence patches ride in `replica.batch` for batch-frame sockets: unwrap them.
      if (f.type === "replica.batch") this.frames.push(...f.items); else this.frames.push(f);
      this.wake();
    });
  }
  static async open(): Promise<Client> {
    const r = await exports.default.fetch(new Request(`${BASE}/ws`, { headers: { upgrade: "websocket" } }));
    expect(r.status).toBe(101);
    return new Client(r.webSocket!);
  }
  private wake() { for (const w of this.waiters.splice(0)) w(); }
  send(frame: unknown) { this.ws.send(JSON.stringify(frame)); }
  async next(match: (f: Frame) => boolean, label = "frame"): Promise<Frame> {
    for (let i = 0; i < 200; i++) {
      const at = this.frames.findIndex(match);
      if (at >= 0) return this.frames.splice(at, 1)[0]!;
      await Promise.race([new Promise<void>(r => this.waiters.push(r)), new Promise(r => setTimeout(r, 50))]);
    }
    throw new Error(`timed out waiting for ${label}`);
  }
  presence(): string[] { return this.frames.filter(f => f.type?.startsWith("presence.")).map(f => f.type); }
}

let seq = 0;
async function world(names: string[]) {
  const n = ++seq;
  const { projectId } = await stub("POST", "projects", { name: `k14-${n}` });
  await stub("POST", "sync", { project: projectId, module: { hash: "artifact-1" } });
  const tenant = `k14-${n}`;
  await stub("POST", "tenants", { projectId, id: tenant, name: "Tenant" });
  const ids: Record<string, string> = {}, tokens: Record<string, string> = {};
  for (const name of names) {
    ids[name] = (await stub("POST", "members", { projectId, tenantId: tenant, email: `${name}@k14.invalid`, password: "pw-123456", name })).memberId;
    const c = await Client.open();
    c.send({ type: "auth", id: "c", controlOnly: true, project: projectId, clientContract: 1 });
    await c.next(f => f.id === "c");
    c.send({ type: "action.call", id: "login", path: "control.auth.passwordLogin", scope: "control", args: { email: `${name}@k14.invalid`, password: "pw-123456" }, idempotencyKey: "login" });
    tokens[name] = (await c.next(f => f.id === "login" && f.type === "action.result")).result.accessToken;
    c.ws.close(1000);
  }
  const signedIn = async (name: string) => {
    const c = await Client.open();
    c.send({ type: "auth", id: "auth", project: projectId, tenant, token: tokens[name], clientContract: 1, capabilities: { presence: 1, replicaBatchFrames: 1 } });
    expect((await c.next(f => f.id === "auth")).type).toBe("auth.result");
    return c;
  };
  return { projectId, tenant, ids, signedIn };
}

const tenantStub = (name: string) => (env as unknown as { TENANT_STUB: { getByName(name: string): any } }).TENANT_STUB.getByName(name);
const gatewayStub = (name: string) => (env as unknown as { GATEWAY: { getByName(name: string): any } }).GATEWAY.getByName(name);
const patchOf = (f: Frame) => f.changes.map((c: Frame) => `${c.memberId}:${c.state}`);

describe("presence end to end", () => {
  it("publishes through the gateways and survives an evicted tenant object without a resync", async () => {
    const w = await world(["ana", "ben", "cy"]);
    const ana = await w.signedIn("ana");
    ana.send({ type: "presence.subscribe", id: "a1" });
    expect((await ana.next(f => f.type === "presence.snapshot")).members).toEqual([]);
    const ben = await w.signedIn("ben");
    ben.send({ type: "presence.set", state: "active" });
    expect(patchOf(await ana.next(f => f.type === "presence.patch"))).toEqual([`${w.ids.ben}:active`]);

    // The tenant object loses its memory; the next event rebuilds it from the gateways' attachments.
    await evictDurableObject(tenantStub(w.tenant));
    const cy = await w.signedIn("cy");
    cy.send({ type: "presence.subscribe", id: "c1" });
    const snapshot = await cy.next(f => f.type === "presence.snapshot");
    expect(snapshot.members.map((m: Frame) => `${m.memberId}:${m.state}`)).toEqual([`${w.ids.ben}:active`]);
    ben.send({ type: "presence.set", state: "idle" });
    expect(patchOf(await ana.next(f => f.type === "presence.patch"))).toEqual([`${w.ids.ben}:idle`]);
    expect(patchOf(await cy.next(f => f.type === "presence.patch"))).toEqual([`${w.ids.ben}:idle`]);
    // Nothing was pending when the object was evicted, so ana got no resync snapshot.
    expect(ana.presence()).toEqual([]);
  });

  it("a closed socket leaves with grace, and the grace end is on the tenant's alarm", async () => {
    const w = await world(["ana", "ben"]);
    const ana = await w.signedIn("ana");
    ana.send({ type: "presence.subscribe", id: "a1" });
    await ana.next(f => f.type === "presence.snapshot");
    const ben = await w.signedIn("ben");
    ben.send({ type: "presence.set", state: "active" });
    await ana.next(f => f.type === "presence.patch");
    // An evicted (hibernated) gateway still reports the close: its index comes back from attachments.
    for (const name of await stub("GET", `gateways?tenant=${w.tenant}`) as string[]) await evictDurableObject(gatewayStub(name));
    const closedAt = Date.now();
    ben.ws.close(1000);
    let alarm: number | null = null;
    for (let i = 0; i < 100 && alarm === null; i++) {
      alarm = await (runInDurableObject as any)(tenantStub(w.tenant), (_: unknown, state: DurableObjectState) => state.storage.getAlarm());
      if (alarm === null) await new Promise(r => setTimeout(r, 20));
    }
    expect(alarm! - closedAt).toBeGreaterThanOrEqual(9_000);
    expect(alarm! - closedAt).toBeLessThanOrEqual(11_000);
    // Inside the grace ben is still published.
    expect((await tenantStub(w.tenant).presencePublished(w.projectId)).map((m: Frame) => m.memberId)).toEqual([w.ids.ben]);
    expect(ana.presence()).toEqual([]);
  });
});
