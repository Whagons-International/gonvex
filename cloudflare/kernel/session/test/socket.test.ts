/**
 * The `/ws` path end to end inside workerd: router -> gateway object ->
 * session -> stub control and tenant objects. Frame-for-frame parity with Rust
 * is the conformance harness's job (`session-handshake`, `session-frames`);
 * these tests cover what the harness cannot reach: the gateway placement
 * (registration, one delivery per gateway, membership changes), alarms
 * (deadline, liveness, revalidation), hibernation restore, the ping
 * auto-response and the platform's 32 MiB caps.
 */
import { env, evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { SocketHost } from "../host";
import { handleWebSocketUpgrade, validExtensionsField } from "../router";
import { HolderRecoveryStore, attachmentRecord } from "../recovery-store";

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
  readonly raw: string[] = [];
  private waiters: Array<() => void> = [];
  close?: { code: number; reason: string };
  constructor(readonly ws: WebSocket) {
    ws.accept();
    ws.addEventListener("message", (e) => { const t = String(e.data); this.raw.push(t); this.frames.push(JSON.parse(t)); this.wake(); });
    ws.addEventListener("close", (e) => { this.close = { code: e.code, reason: e.reason }; this.wake(); });
  }
  static async open(query = ""): Promise<Client> {
    const r = await exports.default.fetch(new Request(`${BASE}/ws${query}`, { headers: { upgrade: "websocket" } }));
    expect(r.status).toBe(101);
    return new Client(r.webSocket!);
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
  async closed(): Promise<{ code: number; reason: string }> {
    for (let i = 0; i < 200 && !this.close; i++) await new Promise(r => setTimeout(r, 25));
    if (!this.close) throw new Error("socket did not close");
    return this.close;
  }
  async auth(frame: Frame): Promise<Frame> {
    this.send({ type: "auth", clientContract: 1, ...frame });
    return this.next(f => f.id === frame.id || f.type === "client.updateRequired", `${frame.id} reply`);
  }
}

let seq = 0;
/** A project with a module, tenants and Members, and a logged-in session per Member. */
async function world(members: Record<string, string[]>, module: Record<string, unknown> = { hash: "artifact-1" }) {
  const n = ++seq;
  const { projectId } = await stub("POST", "projects", { name: `p${n}` });
  await stub("POST", "sync", { project: projectId, module });
  const tenants: Record<string, string> = {}, ids: Record<string, Record<string, string>> = {};
  for (const [tenant, names] of Object.entries(members)) {
    tenants[tenant] = `t${n}-${tenant}`;
    await stub("POST", "tenants", { projectId, id: tenants[tenant], name: `Tenant ${tenant}` });
    for (const name of names) {
      const m = await stub("POST", "members", { projectId, tenantId: tenants[tenant], email: `${name}@k05.invalid`, password: "pw-123456", name });
      (ids[name] ??= {})[tenant] = m.memberId;
    }
  }
  const tokens: Record<string, string> = {};
  for (const name of Object.keys(ids)) {
    const c = await Client.open();
    await c.auth({ id: "c", controlOnly: true, project: projectId });
    c.send({ type: "action.call", id: "login", path: "control.auth.passwordLogin", scope: "control", args: { email: `${name}@k05.invalid`, password: "pw-123456" }, idempotencyKey: "login" });
    tokens[name] = (await c.next(f => f.id === "login" && f.type === "action.result")).result.accessToken;
    c.ws.close(1000);
  }
  return { projectId, tenants, ids, tokens };
}

async function signedIn(w: Awaited<ReturnType<typeof world>>, name: string, tenant: string, extra: Frame = {}) {
  const c = await Client.open();
  await c.next(f => f.type === "session.ready");
  const r = await c.auth({ id: "auth", project: w.projectId, tenant: w.tenants[tenant], token: w.tokens[name], ...extra });
  expect(r.type).toBe("auth.result");
  return c;
}

/** A gateway stub without the RPC type mapping (it does not terminate on the session's JSON types). */
const gateway = (name: string) => (env as unknown as { GATEWAY: { getByName(name: string): any } }).GATEWAY.getByName(name);
/** `runInDurableObject` on a gateway, typed loosely for the same reason. */
const inGateway = (name: string, fn: (instance: any) => unknown): Promise<unknown> =>
  (runInDurableObject as unknown as (stub: unknown, fn: (instance: any) => unknown) => Promise<unknown>)(gateway(name), fn);
const inGatewayState = runInDurableObject as unknown as (stub: unknown, fn: (instance: any, state: DurableObjectState) => any) => Promise<any>;

/** Runs `fn` against the host of every gateway shard that holds a socket of `tenant`. */
async function onGateways(tenant: string, fn: (host: SocketHost) => void | Promise<void>) {
  for (const name of await stub("GET", `gateways?tenant=${tenant}`) as string[]) {
    await inGateway(name, (instance) => fn(instance.host as SocketHost));
  }
}

describe("handshake", () => {
  it("sends session.ready first, without the capabilities the kernel does not implement", async () => {
    const c = await Client.open();
    const ready = await c.next(f => f.type === "session.ready");
    expect(c.raw.length).toBe(1);
    expect(Object.keys(ready)).toEqual(["type", "capabilities"]);
    expect(ready.capabilities).toMatchObject({ protocolVersion: 2, runtimeVersion: "development", replicaBatchFrames: 1, presence: 1 });
    for (const k of ["replicaNativeRows", "replicaSetDigest", "replicaFieldPatches"]) expect(ready.capabilities).not.toHaveProperty(k);
  });

  it("answers {\"type\":\"ping\"} by auto-response (G0-6), where Rust would close 1003", async () => {
    const c = await Client.open();
    c.send('{"type":"ping"}');
    expect(await c.next(f => f.type === "pong")).toEqual({ type: "pong" });
    expect(c.close).toBeUndefined();
  });

  it("refuses bad upgrades in the router", async () => {
    const plain = await exports.default.fetch(new Request(`${BASE}/ws`));
    expect(plain.status).toBe(400);
    const badExtension = await exports.default.fetch(new Request(`${BASE}/ws`, { headers: { upgrade: "websocket", "sec-websocket-extensions": "permessage-deflate; =" } }));
    expect(badExtension.status).toBe(400);
    // Called directly: the test's self-binding turns every upgrade request into a GET.
    const post = await handleWebSocketUpgrade(new Request(`${BASE}/ws`, { method: "POST", headers: { upgrade: "websocket" }, body: "x" }), env);
    expect(post.status).toBe(405);
    expect(validExtensionsField("permessage-deflate; client_max_window_bits, x-other; a=\"q\\\"uoted\"")).toBe(true);
    for (const bad of ["", " , ", "permessage-deflate;", "permessage-deflate; a=", "a b", "a; b=\"open"]) expect(validExtensionsField(bad), bad).toBe(false);
  });

  it("authenticates a session and returns Rust's auth.result shape", async () => {
    const w = await world({ a: ["ana"] });
    const c = await Client.open();
    const r = await c.auth({ id: "x", project: w.projectId, tenant: w.tenants.a, token: w.tokens.ana });
    expect(Object.keys(r.result)).toEqual(["projectId", "accountId", "tenantId", "artifactHash", "replica", "localIdentity"]);
    expect(r.result.artifactHash).toBe("artifact-1");
    expect(r.result.localIdentity.member).toMatchObject({ id: w.ids.ana!.a, role: "admin", status: "active", permissions: { e2e: true } });
  });
});

describe("gateway placement", () => {
  it("0063 targets only A on a shared A/B/C holder, including an auth arrival during empty bootstrap", async () => {
    const w = await world({ a: ["ana", "ben"] });
    const clients = await Promise.all(Array.from({ length: 17 }, () => signedIn(w, "ana", "a")));
    try {
      const names = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
      let selected: { name: string; sockets: Array<{ id: string; epoch: number }> } | undefined;
      for (const name of names) {
        const sockets = await inGateway(name, (instance) => [...instance.host.sessions.values()].filter((s: any) => s.record.tenant?.route.tenantId === w.tenants.a).map((s: any) => ({ id: s.record.connectionId, epoch: s.record.subscriptionEpoch }))) as Array<{ id: string; epoch: number }>;
        if (sockets.length >= 3) { selected = { name, sockets }; break; }
      }
      expect(selected).toBeDefined();
      const { name, sockets: [a, b, c] } = selected!;
      const before = await inGateway(name, (instance) => [...instance.host.sessions.values()].filter((s: any) => [b!.id, c!.id].includes(s.record.connectionId)).map((s: any) => JSON.stringify(s.record)));
      const targeted = { v: 1, scopeFence: 1, instance: "targeted-instance", shared: [], sockets: { [a!.id]: { subscriptionEpoch: a!.epoch, ops: [["f", '{"type":"targeted-probe","owner":"A"}']] } } };
      const receipt = await gateway(name).deliverReplica(w.tenants.a, targeted);
      expect(receipt.scopeRequired).toBeUndefined();
      expect(receipt.scopeAck).toBeUndefined();
      expect(await inGateway(name, (instance) => [...instance.host.sessions.values()].filter((s: any) => [b!.id, c!.id].includes(s.record.connectionId)).map((s: any) => JSON.stringify(s.record)))).toEqual(before);
      // This is a deterministic source-boundary stall around the ACTUAL hook,
      // not evidence of a platform/network fault or assembled deployment.
      await inGateway(name, (instance) => {
        const session = [...instance.host.sessions.values()].find((s: any) => s.record.connectionId === a!.id) as any;
        const actual = session.bootstrapReplica.bind(session);
        const held = new Promise<void>(resolve => { instance.__releaseBootstrap = resolve; });
        instance.__bootstrapEntered = false;
        session.bootstrapReplica = async (...args: any[]) => { instance.__bootstrapEntered = true; await held; return actual(...args); };
      });
      const bootstrap = gateway(name).deliverReplica(w.tenants.a, { ...targeted, sockets: { [a!.id]: { subscriptionEpoch: a!.epoch, ops: [] } } });
      for (let i = 0; i < 50 && !await inGateway(name, (instance) => instance.__bootstrapEntered); i++) await new Promise(resolve => setTimeout(resolve, 1));
      expect(await inGateway(name, (instance) => instance.__bootstrapEntered)).toBe(true);
      await inGateway(name, async (instance) => {
        const session = [...instance.host.sessions.values()].find((s: any) => s.record.connectionId === c!.id) as any;
        await session.receive(JSON.stringify({ type: "auth", id: "during-fence", clientContract: 1, project: w.projectId, tenant: w.tenants.a, token: w.tokens.ben }));
      });
      await inGateway(name, (instance) => instance.__releaseBootstrap());
      const ack = await bootstrap;
      expect(ack.scopeAck).toEqual({ [a!.id]: a!.epoch });
      expect(ack.scopeRequired).toBeUndefined();
      const state = await inGateway(name, (instance) => [...instance.host.sessions.values()].filter((s: any) => [b!.id, c!.id].includes(s.record.connectionId)).map((s: any) => ({ id: s.record.connectionId, epoch: s.record.subscriptionEpoch, instance: s.record.replicaInstance, watermark: s.record.watermark }))) as any[];
      expect(state.every(s => s.instance !== "targeted-instance")).toBe(true);
      // A global watermark is applicable even when A's instance already matches.
      const global = await gateway(name).deliverReplica(w.tenants.a, { ...targeted, sockets: {}, watermark: 50 });
      expect(global.scopeAck).toBeUndefined();
      expect(global.scopeRequired[a!.id]).toBe(a!.epoch);
      for (const s of state) expect(global.scopeRequired[s.id]).toBe(s.epoch);
    } finally {
      // Close server endpoints in their owning Durable Object I/O context.
      for (const name of await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[]) await inGateway(name, (instance) => {
        instance.__releaseBootstrap?.();
        for (const session of instance.host.sessions.values()) if (session.record.tenant?.route.tenantId === w.tenants.a) session.close({ code: 1000, reason: "test complete" });
      });
    }
  });

  it("0067 native sink drops a held initial page after same-epoch close/idreuse on the same ordered outlet",async()=>{
    const w=await world({a:["ana"]});const c=await signedIn(w,"ana","a");
    const [name]=await stub("GET",`gateways?tenant=${w.tenants.a}`) as string[];
    let release=false,releaseInitial!:()=>void;
    const initialGate=new Promise<void>(resolve=>{releaseInitial=resolve;});
    let entered=false,held:Promise<unknown>|undefined;
    try{
      await inGateway(name!,instance=>{
        const session=[...instance.host.sessions.values()].find((s:any)=>s.record.tenant?.route.tenantId===w.tenants.a) as any;
        const actual=session.ports.tenants.dispatch;
        session.ports.tenants.dispatch=(view:any,frame:any)=>frame.type==="replica.open"?Promise.resolve([]):actual(view,frame);
      });
      c.send({type:"replica.open",id:"r",path:"items.all",args:{version:"A"}});
      let origin:any;
      for(let i=0;i<50&&!origin;i++){
        origin=await inGateway(name!,instance=>{const session=[...instance.host.sessions.values()].find((s:any)=>s.record.tenant?.route.tenantId===w.tenants.a) as any;
          const ref=Object.values(session.record.initialRequests??{})[0];return ref?{id:session.record.connectionId,ref}:null;});
        if(!origin)await new Promise(r=>setTimeout(r,1));
      }
      expect(origin).toBeTruthy();
      const {GatewayOutlet}=await import('../../delivery/outlet');
      // Synthetic delay at the transport boundary; after release the actual
      // additive DO method and native Session/sink perform all application.
      const outlet=new GatewayOutlet(w.tenants.a!,()=>({
        deliverReplica:(tenant,payload)=>gateway(name!).deliverReplica(tenant,payload),
        deliverReplicaInitial:async(tenant,payload)=>{entered=true;await initialGate;return gateway(name!).deliverReplicaInitial(tenant,payload);},
      }),async()=>[name!]);
      const page=JSON.stringify({type:"replica.snapshotPage",snapshotId:"held",sequence:0,last:false,id:"r",result:[{id:"stale-row",path:"application value"}],cursor:{epoch:"e",revision:1},key:"id",maxBytes:262144,maxRows:512,mode:"eager",orderBy:"id",orderDirection:"asc",path:"items.all",scope:"s",truncated:false,visibilityScope:"v"});
      held=outlet.send(name!,{v:1,instance:"must-not-install",shared:[],sockets:{[origin.id]:{subscriptionEpoch:origin.ref.epoch,initialScope:origin.ref,ops:[["f",page]]}}});
      void held.catch(()=>{});
      for(let i=0;i<50&&!entered;i++)await new Promise(r=>setTimeout(r,1));
      expect(entered).toBe(true);
      c.send({type:"replica.close",id:"r"});c.send({type:"replica.open",id:"r",path:"items.all",args:{version:"B"}});
      let changed=false;
      for(let i=0;i<50&&!changed;i++){
        changed=await inGateway(name!,instance=>{const session=[...instance.host.sessions.values()].find((s:any)=>s.record.connectionId===origin.id) as any;
          return Object.values(session.record.initialRequests??{}).some((ref:any)=>ref.generation!==origin.ref.generation);}) as boolean;
        if(!changed)await new Promise(r=>setTimeout(r,1));
      }
      expect(changed).toBe(true);
      const before=await inGateway(name!,instance=>{const s=[...instance.host.sessions.values()].find((s:any)=>s.record.connectionId===origin.id) as any;return JSON.stringify(s.record);});
      releaseInitial();release=true;await held;
      const after=await inGateway(name!,instance=>{const s=[...instance.host.sessions.values()].find((s:any)=>s.record.connectionId===origin.id) as any;return JSON.stringify(s.record);});
      expect(after).toBe(before);expect(c.raw.some(text=>text.includes('stale-row'))).toBe(false);
    }finally{
      if(!release)releaseInitial();
      if(held)await Promise.allSettled([held]);
      c.ws.close(1000);await c.closed();
    }
  });

  it("0061 challenges a restarted sender without mutation, rejects a stale Member fence, then acknowledges only real empty bootstrap", async () => {
    const w = await world({ a: ["ana", "ben"] });
    const c = await signedIn(w, "ana", "a");
    const [name] = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
    const before = await inGateway(name!, (instance) => {
      const session = [...instance.host.sessions.values()].find((s: any) => s.record.tenant?.route.tenantId === w.tenants.a) as any;
      session.record.replicaInstance = "before";
      session.delivery.opening("pending-open", "items.all");
      return { id: session.record.connectionId, epoch: session.record.subscriptionEpoch, watermark: session.record.watermark ?? 0 };
    }) as { id: string; epoch: number; watermark: number };
    const payload = { v: 1, scopeFence: 1, instance: "after", shared: [], sockets: {}, watermark: 90 };
    const challenge = await gateway(name!).deliverReplica(w.tenants.a, payload);
    expect(challenge.scopeRequired).toEqual({ [before.id]: before.epoch });
    await inGateway(name!, (instance) => {
      const session = [...instance.host.sessions.values()].find((s: any) => s.record.connectionId === before.id) as any;
      expect(session.record.replicaInstance).toBe("before");
      expect(session.record.watermark ?? 0).toBe(before.watermark);
    });
    await c.auth({ id: "switch", project: w.projectId, tenant: w.tenants.a, token: w.tokens.ben });
    const fence = (epoch: number) => ({ v: 1, scopeFence: 1, instance: "after", shared: [], sockets: { [before.id]: { ops: [], subscriptionEpoch: epoch } } });
    const stale = await gateway(name!).deliverReplica(w.tenants.a, fence(before.epoch));
    expect(stale.scopeAck).toBeUndefined();
    expect(stale.scopeRequired[before.id]).toBeGreaterThan(before.epoch);
    const current = await gateway(name!).deliverReplica(w.tenants.a, payload);
    const epoch = current.scopeRequired[before.id];
    expect(epoch).toBeGreaterThan(before.epoch);
    const ack = await gateway(name!).deliverReplica(w.tenants.a, fence(epoch));
    expect(ack.scopeAck).toEqual({ [before.id]: epoch });
    await gateway(name!).deliverReplica(w.tenants.a, { ...payload, watermark: 100, sockets: { [before.id]: { ops: [["f", '{"type":"private-old-scope"}']], subscriptionEpoch: before.epoch } } });
    await inGateway(name!, (instance) => {
      const session = [...instance.host.sessions.values()].find((s: any) => s.record.connectionId === before.id) as any;
      expect(session.record.replicaInstance).toBe("after");
      expect(session.record.watermark ?? 0).toBe(0);
    });
    expect(c.frames.some((f) => f.type === "private-old-scope")).toBe(false);
    c.ws.close(1000);
  });

  it("registers each gateway once with the tenant and delivers each commit once per gateway", async () => {
    const w = await world({ a: ["ana", "ben"] });
    const clients = await Promise.all(Array.from({ length: 12 }, (_, i) => signedIn(w, i % 2 ? "ben" : "ana", "a")));
    const gateways = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
    expect(gateways.length).toBeGreaterThan(1); // 12 sockets over 8 shards
    expect(new Set(gateways).size).toBe(gateways.length);
    const commit = await stub("POST", "commit", { tenantId: w.tenants.a, all: ['{"type":"probe","n":1}'] });
    expect(commit).toMatchObject({ gateways: gateways.length, sockets: 12 });
    for (const c of clients) {
      expect(await c.next(f => f.type === "probe")).toEqual({ type: "probe", n: 1 });
      expect(c.frames.filter(f => f.type === "probe")).toHaveLength(0);
    }
  });

  it("sends class frames only to sockets of that delivery class", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    const ok = await stub("POST", "commit", { tenantId: w.tenants.a, all: ['{"type":"all"}'], byClass: { "0000000000000000": ['{"type":"other"}'] } });
    expect(ok.sockets).toBe(1);
    await c.next(f => f.type === "all");
    await new Promise(r => setTimeout(r, 50));
    expect(c.frames.some(f => f.type === "other")).toBe(false);
  });

  it("drops a gateway from the tenant once its last socket of the tenant is gone", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    expect(await stub("GET", `gateways?tenant=${w.tenants.a}`)).toHaveLength(1);
    c.ws.close(1000);
    for (let i = 0; i < 40 && (await stub("GET", `gateways?tenant=${w.tenants.a}`)).length; i++) await new Promise(r => setTimeout(r, 25));
    expect(await stub("GET", `gateways?tenant=${w.tenants.a}`)).toEqual([]);
  });

  it("moves a socket's registration on a tenant switch", async () => {
    const w = await world({ a: ["ana"], b: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    const r = await c.auth({ id: "switch", project: w.projectId, tenant: w.tenants.b, token: w.tokens.ana, capabilities: { preserveSubscriptions: 1 } });
    expect(r.result.subscriptionsPreserved).toBeUndefined();
    for (let i = 0; i < 40 && (await stub("GET", `gateways?tenant=${w.tenants.a}`)).length; i++) await new Promise(r => setTimeout(r, 25));
    expect(await stub("GET", `gateways?tenant=${w.tenants.a}`)).toEqual([]);
    expect(await stub("GET", `gateways?tenant=${w.tenants.b}`)).toHaveLength(1);
  });

  it("discards the session when its own Member changes, then sends the commit's watermark (C.4 5a)", async () => {
    const w = await world({ a: ["ana", "ben"] });
    const ana = await signedIn(w, "ana", "a"), ben = await signedIn(w, "ben", "a");
    await stub("POST", "role", { tenantId: w.tenants.a, memberId: w.ids.ana!.a, role: "member" });
    expect(await ana.next(f => f.type === "auth.error")).toEqual({ type: "auth.error", id: "membership-changed", error: "tenant membership changed; authenticate again", code: "transient" });
    const watermark = await ana.next(f => f.type === "replica.watermark");
    expect(watermark.revision).toBeGreaterThan(0);
    ana.send({ type: "query.call", id: "q", path: "x", args: {} });
    expect((await ana.next(f => f.id === "q")).error).toBe("authenticate with an active tenant before calling a Query");
    await new Promise(r => setTimeout(r, 50));
    expect(ben.frames.filter(f => f.type === "auth.error")).toEqual([]);
  });
});

describe("module reload (A.1.5 c)", () => {
  it("sends client.updateRequired to tenant sockets on a stale contract and system.reload to the rest", async () => {
    const w = await world({ a: ["ana"] });
    const tenantSocket = await signedIn(w, "ana", "a");
    const controlSocket = await Client.open();
    await controlSocket.auth({ id: "c", project: w.projectId, controlOnly: true, token: w.tokens.ana });
    const contract = btoa(JSON.stringify({ version: 2, offlineMaxAgeMs: null }));
    await stub("POST", "sync", { project: w.projectId, module: { hash: "artifact-2", files: { "client-contract.json": contract } } });
    expect(await tenantSocket.next(f => f.type === "client.updateRequired")).toEqual({ type: "client.updateRequired", reason: "CLIENT_UPDATE_REQUIRED", contract: 2 });
    expect((await tenantSocket.closed()).code).toBe(1000);
    expect(await controlSocket.next(f => f.type === "system.reload")).toEqual({ type: "system.reload", reason: "module generation changed", artifactHash: "artifact-2" });
    expect(controlSocket.close).toBeUndefined();
  });
});

describe("alarms", () => {
  it("closes a socket still pending at the deadline with 1008", async () => {
    const w = await world({ a: ["ana"] });
    const pending = await Client.open();
    pending.send({ type: "auth", id: "c", project: w.projectId, controlOnly: true });
    await pending.next(f => f.id === "c");
    for (let i = 0; i < 8; i++) {
      await inGateway(`gw-${i}`, (instance) => {
        for (const s of (instance.host as SocketHost).sessions.values()) if (s.record.pendingUntil !== null) s.record.pendingUntil = 1;
      });
      await runDurableObjectAlarm(gateway(`gw-${i}`));
    }
    expect(await pending.closed()).toEqual({ code: 1008, reason: "authentication deadline exceeded" });
  });

  it("sends a liveness watermark to a tenant socket silent for 20 s (G0-6), in the batch envelope it negotiated", async () => {
    const w = await world({ a: ["ana"] });
    const plain = await signedIn(w, "ana", "a");
    const batched = await signedIn(w, "ana", "a", { capabilities: { replicaBatchFrames: 1 } });
    await stub("POST", "commit", { tenantId: w.tenants.a, all: [] });
    await onGateways(w.tenants.a!, (host) => { for (const s of host.sessions.values()) s.lastOutboundAt = 0; });
    for (const g of await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[]) await runDurableObjectAlarm(gateway(g));
    const a = await plain.next(f => f.type === "replica.watermark");
    const b = await batched.next(f => f.type === "replica.batch");
    expect(b).toMatchObject({ items: [], watermark: a.revision, from: a.revision, tenant: w.tenants.a });
    expect(b.class).toMatch(/^[0-9a-f]{16}$/);
  });

  it("revalidates sessions every 30 s and expires a revoked one in place", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    await runInDurableObject(env.CONTROL_STUB.getByName("control"), (_i, state) => { state.storage.sql.exec("UPDATE sessions SET revoked = 1"); });
    await onGateways(w.tenants.a!, (host) => { for (const s of host.sessions.values()) if (s.record.revalidateAt) s.record.revalidateAt = 1; });
    for (const g of await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[]) await runDurableObjectAlarm(gateway(g));
    expect(await c.next(f => f.type === "auth.error")).toEqual({ type: "auth.error", id: "session-expired", error: "authentication expired or was revoked", code: "transient" });
    expect(c.close).toBeUndefined();
  });
});

describe("hibernation", () => {
  for (const publish of [false, true]) it(`recovers the ${publish ? "new" : "old"} semantic generation after eviction ${publish ? "after" : "before"} attachment publication`, async () => {
    const w = await world({ a: ["ana"] }), c = await signedIn(w, "ana", "a");
    const [name] = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
    const transition = await inGatewayState(gateway(name!), (instance, state) => {
      const [ws, session] = [...instance.host.sessions].find(([, s]: any) => s.record.tenant?.route.tenantId === w.tenants.a)! as [WebSocket, any];
      const old = ws.deserializeAttachment() as any;
      const nextRecord = structuredClone(session.record);
      nextRecord.pendingActions = { correlation: "agent.waiting" };
      const next = new HolderRecoveryStore(state.storage).write(nextRecord, old.recovery);
      if (publish) ws.serializeAttachment({ session: attachmentRecord(nextRecord), recovery: next });
      // Intentionally no commit. Real hibernation discards the host's memory;
      // the verified native attachment selects which transition was accepted.
      return { old: old.recovery.generation, next: next.generation, owner: next.socketOwner };
    });
    await evictDurableObject(gateway(name!));
    await inGatewayState(gateway(name!), (instance, state) => {
      const session = [...instance.host.sessions.values()].find((s: any) => s.record.tenant?.route.tenantId === w.tenants.a) as any;
      expect(session.record.pendingActions ?? {}).toEqual(publish ? { correlation: "agent.waiting" } : {});
      expect(state.storage.kv.get<any>(`session-recovery-owner/${transition.owner}`)?.current).toBe(publish ? transition.next : transition.old);
      expect([...state.storage.kv.list({ prefix: `session-recovery/${publish ? transition.old : transition.next}/` })]).toEqual([]);
    });
    c.ws.close(1000); await c.closed();
    await runInDurableObject(gateway(name!), (_instance, state) => {
      expect(state.storage.kv.get(`session-recovery-owner/${transition.owner}`)).toBeUndefined();
    });
  });

  it("serializes large recovery requests and private identity, restores them after eviction and cleans on close", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    const [name] = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
    const generation = await inGateway(name!, (instance) => {
      const host = instance.host;
      const [ws, session] = [...host.sessions].find(([, s]: any) => s.record.tenant?.route.tenantId === w.tenants.a)!;
      session.record.tenant.member.permissions = { legalLargeFixture: "x".repeat(100_000) };
      session.record.connection.tenant = session.record.tenant;
      session.record.liveQueries = { q: { type: "query.subscribe", id: "q", path: "items.live", args: { text: "漢".repeat(100_000) } } };
      session.record.pendingActions = { pending: "agent.run" };
      host.recoveryDirty.add(ws);
      host.persist(ws, session);
      const attachment = ws.deserializeAttachment();
      expect(attachment.session.privateIdentity).toBe(1);
      expect(attachment.session.tenant).toBeUndefined();
      expect(attachment.session.liveQueries).toBeUndefined();
      expect(JSON.stringify(attachment).length).toBeLessThan(16_384);
      return attachment.recovery.generation;
    }) as string;
    await evictDurableObject(gateway(name!));
    await inGateway(name!, (instance) => {
      const session = [...instance.host.sessions.values()].find((s: any) => s.record.tenant?.route.tenantId === w.tenants.a) as any;
      expect(session.record.tenant.member.permissions.legalLargeFixture.length).toBe(100_000);
      expect(session.record.liveQueries.q.args.text.length).toBe(100_000);
      expect(session.record.pendingActions).toEqual({ pending: "agent.run" });
    });
    c.ws.close(1000); await c.closed();
    await runInDurableObject(gateway(name!), (_instance, state) => {
      expect([...state.storage.kv.list({ prefix: `session-recovery/${generation}/` })]).toEqual([]);
    });
  });

  it("fails closed and abandons the unpublished generation when attachment publication fails", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    const [name] = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
    await inGateway(name!, (instance) => {
      const host = instance.host;
      const [ws, session] = [...host.sessions].find(([, s]: any) => s.record.tenant?.route.tenantId === w.tenants.a)!;
      const original = ws.serializeAttachment;
      try {
        ws.serializeAttachment = () => { throw new Error("injected attachment publication failure"); };
        session.record.pendingActions = { pending: "agent.run" };
        host.recoveryDirty.add(ws);
        host.persist(ws, session);
        expect(session.closed).toBe(true);
      } finally { ws.serializeAttachment = original; }
    });
    expect((await c.closed()).code).toBe(1012);
  });

  it("restores sessions from attachments after the gateway is evicted", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    const [name] = await stub("GET", `gateways?tenant=${w.tenants.a}`) as string[];
    await evictDurableObject(gateway(name!));
    // Delivery still finds the socket: the index was rebuilt from attachments, the tenant kept the registration.
    expect((await stub("POST", "commit", { tenantId: w.tenants.a, all: ['{"type":"after-wake"}'] })).sockets).toBe(1);
    await c.next(f => f.type === "after-wake");
    c.send({ type: "query.call", id: "q", path: "x", args: {} });
    expect((await c.next(f => f.id === "q")).error).toBe("not implemented by the K05 tenant stub");
  });
});

describe("size limits", () => {
  // The refusal above 32 MiB is the platform's (1009, measured over TCP in S3 and in this PR);
  // the in-process WebSocket pipe of the test pool does not enforce it.
  it("accepts 32 MiB - 1 KiB on an authenticated socket (0018)", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    const sized = (bytes: number) => { const base = JSON.stringify({ type: "telemetry.batch", events: [], pad: "" }); return JSON.stringify({ type: "telemetry.batch", events: [], pad: "x".repeat(bytes - base.length) }); };
    c.send(sized(32 * 1024 * 1024 - 1024));
    c.send({ type: "query.call", id: "after", path: "x", args: {} });
    expect((await c.next(f => f.id === "after")).error).toBe("not implemented by the K05 tenant stub");
    expect(c.close).toBeUndefined();
  }, 60_000);

  it("closes unscoped sockets at 64 KiB and the public transport with 1002", async () => {
    const unscoped = await Client.open();
    unscoped.send(JSON.stringify({ type: "telemetry.batch", events: [], pad: "x".repeat(64 * 1024) }));
    expect(await unscoped.closed()).toEqual({ code: 1009, reason: "public payload too large" });
    const pub = await Client.open("?public=1");
    pub.send(JSON.stringify({ type: "query.unsubscribe", id: "x", pad: "x".repeat(64 * 1024) }));
    expect((await pub.closed()).code).toBe(1002);
  });

  it("never sends a frame of 32 MiB - 1 KiB or more (0018): it closes 1011 instead", async () => {
    const w = await world({ a: ["ana"] });
    const c = await signedIn(w, "ana", "a");
    // Just over the session's limit, still under the 32 MiB Workers RPC cap that bounds every delivery batch.
    await stub("POST", "commit", { tenantId: w.tenants.a, all: [JSON.stringify({ type: "big", pad: "x".repeat(32 * 1024 * 1024 - 1024) })] });
    expect(await c.closed()).toEqual({ code: 1011, reason: "frame exceeds the outbound limit" });
    expect(c.frames.some(f => f.type === "big")).toBe(false);
  }, 60_000);
});
