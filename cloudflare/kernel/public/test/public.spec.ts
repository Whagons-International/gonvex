/**
 * K16 end to end inside workerd, through the kernel Worker's `/ws` path
 * (router -> address limiter -> gateway -> session) with the limits of
 * test/wrangler.jsonc: 4 calls per connection, 6 per address, 2 public sockets
 * and 3 pending sockets per address, 512-byte public payloads. Each test uses
 * its own client address (`CF-Connecting-IP`), so budgets never leak between tests.
 */
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { AddressBudget } from "../limiter";
import type { SocketHost } from "../../session/host";

const BASE = "https://gonvex.test";
const KEY = "k16-test-key";
type Frame = Record<string, any>;

async function stub(method: string, route: string, body?: unknown): Promise<any> {
  const r = await exports.default.fetch(new Request(`${BASE}/_stub/session/${route}`, {
    method, headers: { authorization: `Bearer ${KEY}`, ...(body ? { "content-type": "application/json" } : {}) }, body: body ? JSON.stringify(body) : null,
  }));
  expect(r.status, await r.clone().text()).toBe(200);
  return r.json();
}

function upgrade(address: string | null, query = ""): Promise<Response> {
  const headers: Record<string, string> = { upgrade: "websocket" };
  if (address !== null) headers["cf-connecting-ip"] = address;
  return exports.default.fetch(new Request(`${BASE}/ws${query}`, { headers }));
}

class Client {
  readonly frames: Frame[] = [];
  private waiters: Array<() => void> = [];
  close?: { code: number; reason: string };
  constructor(readonly ws: WebSocket) {
    ws.accept();
    ws.addEventListener("message", (e) => { this.frames.push(JSON.parse(String(e.data))); this.wake(); });
    ws.addEventListener("close", (e) => { this.close = { code: e.code, reason: e.reason }; this.wake(); });
  }
  static async open(address: string | null, query = ""): Promise<Client> {
    const r = await upgrade(address, query);
    if (r.status !== 101) throw new Error(`upgrade refused: ${r.status} ${await r.text()}`);
    const client = new Client(r.webSocket!);
    await client.next(f => f.type === "session.ready", "session.ready");
    return client;
  }
  private wake() { for (const w of this.waiters.splice(0)) w(); }
  send(frame: unknown) { this.ws.send(typeof frame === "string" ? frame : JSON.stringify(frame)); }
  async next(match: (f: Frame) => boolean, label = "frame"): Promise<Frame> {
    for (let i = 0; i < 200; i++) {
      const at = this.frames.findIndex(match);
      if (at >= 0) return this.frames.splice(at, 1)[0]!;
      if (this.close) throw new Error(`closed (${this.close.code}) while waiting for ${label}`);
      await Promise.race([new Promise<void>(r => this.waiters.push(r)), new Promise(r => setTimeout(r, 50))]);
    }
    throw new Error(`timed out waiting for ${label}`);
  }
  /** Every frame received after `count` frames arrived (or the close). */
  async take(count: number): Promise<Frame[]> {
    for (let i = 0; i < 200 && this.frames.length < count && !this.close; i++) {
      await Promise.race([new Promise<void>(r => this.waiters.push(r)), new Promise(r => setTimeout(r, 50))]);
    }
    return this.frames.splice(0, count);
  }
  async closed(): Promise<{ code: number; reason: string }> {
    for (let i = 0; i < 200 && !this.close; i++) await new Promise(r => setTimeout(r, 25));
    if (!this.close) throw new Error("socket did not close");
    return this.close;
  }
  /** Public auth; resolves to the reply. */
  async publicAuth(project: string, tenant: string, id = crypto.randomUUID()): Promise<Frame> {
    this.send({ type: "auth", id, project, tenant, public: true, clientContract: 1 });
    return this.next(f => f.id === id, `${id} reply`);
  }
}

let seq = 0;
async function world(): Promise<{ project: string; tenant: string; domain: string }> {
  const n = ++seq;
  const { projectId } = await stub("POST", "projects", { name: `k16-${n}` });
  await stub("POST", "sync", { project: projectId, module: { hash: "artifact-1" } });
  const tenant = `k16-t${n}`;
  await stub("POST", "tenants", { projectId, id: tenant, name: `Tenant ${n}` });
  return { project: projectId, tenant, domain: tenant };
}

let addresses = 0;
const freshAddress = () => `198.51.100.${++addresses}`;

const limiter = (address: string) => env.PUBLIC_LIMITER.getByName(`addr:${address}`);
const slots = (address: string) =>
  runInDurableObject(limiter(address), (o: unknown) => {
    const budget = (o as { budget: AddressBudget }).budget;
    return { pending: [...budget.slots.pending.entries()], public: [...budget.slots.public.entries()] };
  });
const counts = async (address: string) => { const s = await slots(address); return { pending: s.pending.length, public: s.public.length }; };

async function setTenantStatus(tenant: string, status: string): Promise<void> {
  await runInDurableObject(env.CONTROL_STUB.getByName("control"), (o: unknown) => {
    (o as { ctx: DurableObjectState }).ctx.storage.sql.exec("UPDATE tenants SET status = ? WHERE tenant_id = ?", status, tenant);
  });
}

describe("connection budgets at the upgrade", () => {
  it("refuses the 4th pending socket of an address with 429, across both transports, and frees a slot on close", async () => {
    const address = freshAddress();
    const open = [await Client.open(address), await Client.open(address, "?public=1"), await Client.open(address)];
    expect(await counts(address)).toEqual({ pending: 3, public: 1 });
    const refused = await upgrade(address);
    expect(refused.status).toBe(429);
    expect(await refused.text()).toBe("pending connection limit exceeded");
    expect(refused.headers.get("content-type")).toBe("text/plain; charset=utf-8");
    // Another address is unaffected.
    (await Client.open(freshAddress())).ws.close();
    open[0]!.ws.close();
    await expect.poll(() => counts(address)).toEqual({ pending: 2, public: 1 });
    open.push(await Client.open(address));
    for (const c of open) c.ws.close();
  });

  it("refuses the 3rd public socket of an address with 429 and leaves no pending slot behind", async () => {
    const address = freshAddress();
    const a = await Client.open(address, "?public=1");
    const b = await Client.open(address, "?public=1");
    const refused = await upgrade(address, "?public=1");
    expect(refused.status).toBe(429);
    expect(await refused.text()).toBe("public connection limit exceeded");
    expect(await counts(address)).toEqual({ pending: 2, public: 2 });
    // A member-transport socket still fits the pending budget.
    const member = await Client.open(address);
    b.ws.close();
    await expect.poll(() => counts(address)).toEqual({ pending: 2, public: 1 });
    const c = await Client.open(address, "?public=1");
    for (const s of [a, c, member]) s.ws.close();
  });

  it("frees the pending slot when a socket authenticates, and keeps the public slot until it closes", async () => {
    const { project, tenant } = await world();
    const address = freshAddress();
    const c = await Client.open(address, "?public=1");
    expect((await c.publicAuth(project, tenant)).type).toBe("auth.result");
    await expect.poll(() => counts(address)).toEqual({ pending: 0, public: 1 });
    // A failed auth keeps the pending slot (lib.rs:2877-2882: only an identity or tenant takes it).
    const failed = await Client.open(address, "?public=1");
    expect((await failed.publicAuth(project, "no-such-tenant")).error).toBe("public tenant session is unavailable");
    expect(await counts(address)).toEqual({ pending: 1, public: 2 });
    c.ws.close();
    failed.ws.close();
    await expect.poll(() => counts(address)).toEqual({ pending: 0, public: 0 });
  });

  it("shares one bucket for callers without a usable CF-Connecting-IP", async () => {
    const c = await Client.open(null, "?public=1");
    const s = await slots("0.0.0.0");
    expect(s.public.length).toBeGreaterThanOrEqual(1);
    c.ws.close();
  });
});

describe("call budgets", () => {
  it("trips the connection budget at the 5th call; auth frames count; every refused call gets its correlated error", async () => {
    const { project, tenant } = await world();
    const c = await Client.open(freshAddress(), "?public=1");
    for (let i = 0; i < 4; i++) expect((await c.publicAuth(project, tenant, `a${i}`)).type).toBe("auth.result");
    expect(await c.publicAuth(project, tenant, "a4")).toEqual({ type: "auth.error", id: "a4", error: "public call rate limit exceeded", code: "transient" });
    c.send({ type: "query.unsubscribe", id: "q" });
    c.send({ type: "reducer.callMany", calls: [{ id: "r1", path: "x.y", args: {} }, { id: "r2", path: "x.y", args: {} }] });
    c.send({ type: "action.call", id: "act", path: "x.a", args: {} });
    expect(await c.take(4)).toEqual([
      { type: "auth.error", id: "public-rate-limit", error: "public call rate limit exceeded", code: "transient" },
      { type: "reducer.error", id: "r1", path: "x.y", error: "public call rate limit exceeded", class: "rejected", retryable: false },
      { type: "reducer.error", id: "r2", path: "x.y", error: "public call rate limit exceeded", class: "rejected", retryable: false },
      { type: "action.error", id: "act", path: "x.a", error: "public call rate limit exceeded" },
    ]);
    expect(c.close).toBeUndefined();
    c.ws.close();
  });

  it("trips the address budget at the 7th call across sockets on different gateways", async () => {
    const { project, tenant } = await world();
    const address = freshAddress();
    const first = await Client.open(address, "?public=1");
    let second = await Client.open(address, "?public=1");
    // Reopen until the two sockets sit on different gateway objects (8 shards).
    for (let i = 0; i < 30; i++) {
      const held = (await slots(address)).public;
      if (new Set(held.map(([, s]) => s.gateway)).size === 2) break;
      second.ws.close();
      await expect.poll(() => counts(address).then(n => n.public)).toBe(1);
      second = await Client.open(address, "?public=1");
    }
    expect(new Set((await slots(address)).public.map(([, s]) => s.gateway)).size).toBe(2);
    for (let i = 0; i < 4; i++) expect((await first.publicAuth(project, tenant, `f${i}`)).type).toBe("auth.result");
    expect((await second.publicAuth(project, tenant, "s0")).type).toBe("auth.result");
    expect((await second.publicAuth(project, tenant, "s1")).type).toBe("auth.result");
    // 6 calls from this address: the 7th is refused although `second` has used 2 of its 4.
    expect((await second.publicAuth(project, tenant, "s2")).error).toBe("public call rate limit exceeded");
    // A batch that does not fit is refused whole (and is charged to the connection window only).
    second.send({ type: "reducer.callMany", calls: [{ id: "b1", path: "x.y", args: {} }] });
    expect((await second.next(f => f.id === "b1")).error).toBe("public call rate limit exceeded");
    first.ws.close();
    second.ws.close();
  });

  it("does not limit calls on the member transport", async () => {
    const address = freshAddress();
    const c = await Client.open(address);
    for (let i = 0; i < 8; i++) {
      c.send({ type: "query.call", id: `q${i}`, path: "x.y", args: {} });
      expect((await c.next(f => f.id === `q${i}`)).error).toBe("authenticate with an active tenant before calling a Query");
    }
    c.ws.close();
  });
});

describe("payload caps", () => {
  it("applies GONVEX_PUBLIC_MAX_PAYLOAD_BYTES to the public transport (1002) and to unscoped sockets (1009)", async () => {
    const pad = (bytes: number) => { const base = JSON.stringify({ type: "query.unsubscribe", id: "x", pad: "" }); return JSON.stringify({ type: "query.unsubscribe", id: "x", pad: "x".repeat(bytes - base.length) }); };
    const exact = await Client.open(freshAddress(), "?public=1");
    exact.send(pad(512));
    exact.send({ type: "query.call", id: "probe", path: "x.y", args: {} });
    expect((await exact.next(f => f.id === "probe")).type).toBe("query.error");
    exact.ws.close();
    const over = await Client.open(freshAddress(), "?public=1");
    over.send(pad(513));
    expect((await over.closed()).code).toBe(1002);
    const unscoped = await Client.open(freshAddress());
    unscoped.send(pad(513));
    expect(await unscoped.closed()).toEqual({ code: 1009, reason: "public payload too large" });
  });
});

describe("public admission", () => {
  it("re-reads admission before a call: a disabled tenant refuses it and ends the socket", async () => {
    const { project, tenant } = await world();
    const c = await Client.open(freshAddress(), "?public=1");
    expect((await c.publicAuth(project, tenant)).result).toMatchObject({ projectId: project, tenantId: tenant, accountId: "", public: true });
    c.send({ type: "query.call", id: "before", path: "x.y", args: {} });
    expect((await c.next(f => f.id === "before")).error).toBe("not implemented by the K05 tenant stub");
    await setTenantStatus(tenant, "disabled");
    // A frame that runs no tenant code accepts the check made at auth (under 30 s old): no reply, still open.
    c.send({ type: "query.unsubscribe", id: "u" });
    c.send({ type: "query.call", id: "after", path: "x.y", args: {} });
    expect(await c.next(f => f.id === "after")).toEqual({ type: "query.error", id: "after", path: "x.y", error: "public tenant session is unavailable" });
    expect(await c.closed()).toEqual({ code: 1000, reason: "" });
  });

  it("re-reads admission before each call of a batch, then ends the socket with session-expired", async () => {
    const { project, tenant } = await world();
    const c = await Client.open(freshAddress(), "?public=1");
    await c.publicAuth(project, tenant);
    await setTenantStatus(tenant, "disabled");
    c.send({ type: "reducer.callMany", calls: [{ id: "m1", path: "x.y", args: {} }, { id: "m2", path: "x.y", args: {} }] });
    expect(await c.take(3)).toEqual([
      { type: "reducer.error", id: "m1", path: "x.y", error: "public tenant session is unavailable", class: "rejected", retryable: false },
      { type: "reducer.error", id: "m2", path: "x.y", error: "public tenant session is unavailable", class: "rejected", retryable: false },
      { type: "auth.error", id: "session-expired", error: "public tenant session is unavailable", code: "transient" },
    ]);
    expect((await c.closed()).code).toBe(1000);
  });

  it("the monitor re-checks a quiet socket after 30 s, once per scope, and ends revoked sockets", async () => {
    const { project, tenant, domain } = await world();
    const address = freshAddress();
    const a = await Client.open(address, "?public=1");
    const b = await Client.open(address, "?public=1");
    await a.publicAuth(project, tenant);
    // The scope is canonical: a domain-addressed auth pins the same scope (the stub's domain is the id).
    await b.publicAuth(project, domain);
    const gateways = new Set((await slots(address)).public.map(([, s]) => s.gateway));
    const age = async (gateway: string) => runInDurableObject(env.GATEWAY.getByName(gateway), (o: unknown) => {
      for (const s of (o as { host: SocketHost }).host.sessions.values()) if (s.record.admission) s.record.admission.checkedAt -= 31_000;
    });
    // Still admitted: the monitor refreshes the check and leaves the sockets open.
    for (const g of gateways) { await age(g); await runDurableObjectAlarm(env.GATEWAY.getByName(g)); }
    expect(a.close).toBeUndefined();
    expect(b.close).toBeUndefined();
    await setTenantStatus(tenant, "disabled");
    for (const g of gateways) { await age(g); await runDurableObjectAlarm(env.GATEWAY.getByName(g)); }
    for (const c of [a, b]) {
      expect(await c.next(f => f.type === "auth.error")).toEqual({ type: "auth.error", id: "session-expired", error: "public tenant session is unavailable", code: "transient" });
      expect((await c.closed()).code).toBe(1000);
    }
    await expect.poll(() => counts(address)).toEqual({ pending: 0, public: 0 });
  });

  it("pins the first public scope; another tenant closes 1008", async () => {
    const one = await world();
    const two = await world();
    const c = await Client.open(freshAddress(), "?public=1");
    await c.publicAuth(one.project, one.tenant);
    expect(await c.publicAuth(two.project, two.tenant, "other")).toEqual({ type: "auth.error", id: "other", error: "public connection scope is fixed", code: "transient" });
    expect(await c.closed()).toEqual({ code: 1008, reason: "public connection scope is fixed" });
  });
});
