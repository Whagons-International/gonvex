// X17 minimal repro: Durable Object RPC latency from a Worker, with and without a gated (confirmed) storage write.
import { DurableObject } from "cloudflare:workers";
export class Probe extends DurableObject {
  read() { return this.ctx.storage.kv.get("k") ?? 0; }
  writeKv() { this.ctx.storage.kv.put("k", Date.now()); return 1; }                   // sync KV write: reply held by the output gate
  async writeUnconfirmed() { await this.ctx.storage.put("k", Date.now(), { allowUnconfirmed: true }); return 1; }
  /** X17: an allowUnconfirmed put inside transactionSync (how HolderRecoveryStore writes since #523). */
  txUnconfirmed() { this.ctx.storage.transactionSync(() => { this.ctx.storage.put("k", Date.now(), { allowUnconfirmed: true }).catch(() => {}); this.ctx.storage.put("k2", Date.now(), { allowUnconfirmed: true }).catch(() => {}); }); return 1; }
  /** The same two puts without the transaction. */
  plainUnconfirmed() { this.ctx.storage.put("k", Date.now(), { allowUnconfirmed: true }).catch(() => {}); this.ctx.storage.put("k2", Date.now(), { allowUnconfirmed: true }).catch(() => {}); return 1; }
  txKv() { this.ctx.storage.transactionSync(() => { this.ctx.storage.kv.put("k", Date.now()); }); return 1; }
  /** X17: CPU of n read-only "invocations" as TenantStorage.invoke does them (async transaction, rolled back by a throw). */
  async txReads(n) { this.seed(); const rb = Symbol("rb"); for (let i = 0; i < n; i++) { try { await this.ctx.storage.transaction(async () => { this.ctx.storage.sql.exec("SELECT count(*) FROM t").toArray(); throw rb; }); } catch (e) { if (e !== rb) throw e; } } return n; }
  async plainReads(n) { this.seed(); for (let i = 0; i < n; i++) this.ctx.storage.sql.exec("SELECT count(*) FROM t").toArray(); return n; }
  async syncTxReads(n) { this.seed(); const rb = Symbol("rb"); for (let i = 0; i < n; i++) { try { this.ctx.storage.transactionSync(() => { this.ctx.storage.sql.exec("SELECT count(*) FROM t").toArray(); throw rb; }); } catch (e) { if (e !== rb) throw e; } } return n; }
  seed() { if (this.seeded) return; this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS t(v)"); this.seeded = true; }
  sql() { this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS t(v)"); this.ctx.storage.sql.exec("INSERT INTO t VALUES(?)", Date.now()); return 1; }
  // X17 step repro: this object holds a client WebSocket (as the gateway does); per message it answers directly,
  // after one RPC to another object (gateway -> tenant), or only when that object calls it back (tenant -> gateway
  // delivery), the shape of one Gonvex client step.
  async fetch(req) {
    const pair = new WebSocketPair(); this.ctx.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
  async webSocketMessage(ws, msg) {
    const m = JSON.parse(msg); const peer = this.env.PROBE.getByName(m.peer || "step-tenant");
    if (m.mode === "direct") return ws.send(msg);
    if (m.mode === "hop") { await peer.read(); return ws.send(msg); }
    if (m.mode === "deliver") { this.pending = (text) => ws.send(text); await peer.callBack(this.ctx.id.name ?? m.self, msg); return; }
  }
  async callBack(name, text) { await this.env.PROBE.getByName(name).deliver(text); return 1; }
  async deliver(text) { for (const ws of this.ctx.getWebSockets()) ws.send(text); return 1; }
  // X17 backpressure repro: queue n frames of `size` bytes to every socket of this object, return at once.
  async blast(n, size, rand) { let text = "x".repeat(size); if (rand) { const b = new Uint8Array(Math.ceil(size / 2)); for (let o = 0; o < b.length; o += 65536) crypto.getRandomValues(b.subarray(o, Math.min(b.length, o + 65536))); text = [...b].map((x) => x.toString(16).padStart(2, "0")).join("").slice(0, size); } let k = 0; for (const ws of this.ctx.getWebSockets()) for (let i = 0; i < n; i++) { ws.send(text); k++; } return { sent: k, sockets: this.ctx.getWebSockets().length }; }
  async ping() { return Date.now(); }
  // Delivery-shaped RPC: an object payload (rows), forwarded to every socket as one JSON frame.
  async deliverRows(payload) { const text = JSON.stringify(payload); for (const ws of this.ctx.getWebSockets()) ws.send(text); return text.length; }
  async seqRun(target, n, rows) {
    const t = this.env.PROBE.getByName(target);
    const mk = (k) => ({ type: "replica.snapshotPage", sequence: k, result: Array.from({ length: rows }, (_, i) => ({ id: "row-" + k + "-" + i, title: "Task " + i + " " + Math.random().toString(36).slice(2), state: i % 3 ? "open" : "closed", n: i, tags: ["a", "b", String(i % 7)] })) });
    const lat = []; let bytes = 0;
    for (let k = 0; k < n; k++) { const p = mk(k); const a = Date.now(); bytes += await t.deliverRows(p); lat.push(Date.now() - a); }
    return { lat, bytes };
  }
  // Does the gateway's per-persist work hold the reply? attach: serializeAttachment on every socket; putU: unconfirmed puts.
  async attach(size) { const v = { s: "x".repeat(size), at: Date.now() }; for (const ws of this.ctx.getWebSockets()) ws.serializeAttachment(v); return 1; }
  async putU(size, k) { const v = "y".repeat(size); for (let i = 0; i < (k || 1); i++) this.ctx.storage.put("u" + i, v + Date.now(), { allowUnconfirmed: true }).catch(() => {}); return 1; }
  async putC(size) { this.ctx.storage.kv.put("c", "z".repeat(size) + Date.now()); return 1; }
  // Caller side (another object, as a tenant calling its gateway): ping latency before, right after, and after a blast.
  async bpRun(target, n, size, rand) {
    const t = this.env.PROBE.getByName(target); const lat = async () => { const a = Date.now(); await t.ping(); return Date.now() - a; };
    const before = []; for (let i = 0; i < 3; i++) before.push(await lat());
    const b0 = Date.now(); const blasted = await t.blast(n, size, rand); const blastMs = Date.now() - b0;
    const after = []; const at = [];
    const s0 = Date.now(); for (let i = 0; i < 12; i++) { after.push(await lat()); at.push(Date.now() - s0); }
    return { before, blastMs, blasted, after, at };
  }
  /** X17: how long setAlarm(now) takes to deliver alarm(): the gateway's CallOrigin waits for exactly this. */
  async armNow(unconfirmed) { this.armed = Date.now(); this.fired = new Promise(r => { this.resolveFired = r; }); await this.ctx.storage.setAlarm(Date.now(), unconfirmed ? { allowUnconfirmed: true } : undefined); return 1; }
  async alarm() { this.resolveFired?.(Date.now() - this.armed); }
  async waitFired() { return await this.fired; }
  async nested(name) { const s = this.env.PROBE.getByName(name); const t = Date.now(); await s.read(); return Date.now() - t; }
  async nestedWrite(name) { const s = this.env.PROBE.getByName(name); const t = Date.now(); await s.writeKv(); return Date.now() - t; }
}
const med = (a) => { const s = [...a].sort((x, y) => x - y); return s[s.length >> 1]; };
export default {
  async fetch(req, env, ctx) {
    const u = new URL(req.url); const n = +(u.searchParams.get("n") || 15); const name = u.searchParams.get("o") || "a";
    const hint = u.searchParams.get("hint") || undefined;
    const time = async (f, k = 5) => { const d = []; for (let i = 0; i < k; i++) { const t = Date.now(); await f(); d.push(Date.now() - t); } return med(d); };
    if (u.pathname === "/cells") {
      // Read-only: drainReady() on a name that never matches returns false without writing.
      const out = { colo: req.cf?.colo };
      for (let c = 0; c < 32; c++) { const s = env.CTRL.getByName(`gateway-cell-${c}`); out[c] = await time(() => s.drainReady("x17-none", 0, "x17-none")); }
      return Response.json(out);
    }
    if (u.pathname === "/txcost") { const s = env.PROBE.getByName("txcost"); const m = u.searchParams.get("m"); const t = Date.now(); await s[m](n); return Response.json({ m, n, wallMs: Date.now() - t }); }
    if (u.pathname === "/cache") {
      const key = new Request("https://gonvex-warm.internal/holder");
      if (u.searchParams.get("put")) { const t = Date.now(); await caches.default.put(key, new Response(JSON.stringify({ v: u.searchParams.get("put"), at: Date.now() }), { headers: { "cache-control": "max-age=120" } })); return Response.json({ put: true, ms: Date.now() - t, colo: req.cf?.colo }); }
      const t = Date.now(); const hit = await caches.default.match(key); return Response.json({ hit: hit ? await hit.json() : null, ms: Date.now() - t, colo: req.cf?.colo });
    }
    if (u.pathname === "/seq") { return Response.json(await env.PROBE.getByName("seq-caller").seqRun(u.searchParams.get("o"), Number(u.searchParams.get("n") || 20), Number(u.searchParams.get("rows") || 2000))); }
    if (u.pathname === "/persist") {
      const t = env.PROBE.getByName(u.searchParams.get("o") || "bp-target"); const size = Number(u.searchParams.get("size") || 4096); const k = Number(u.searchParams.get("k") || 1);
      const lat = async (f) => { const d = []; for (let i = 0; i < 8; i++) { const a = Date.now(); await f(); d.push(Date.now() - a); } return d; };
      return Response.json({ ping: await lat(() => t.ping()), attach: await lat(() => t.attach(size)), putU: await lat(() => t.putU(size, k)), putC: await lat(() => t.putC(size)), pingAfter: await lat(() => t.ping()) });
    }
    if (u.pathname === "/bp") { const r = await env.PROBE.getByName("bp-caller").bpRun(u.searchParams.get("o") || "bp-target", Number(u.searchParams.get("n") || 20), Number(u.searchParams.get("size") || 262144), u.searchParams.get("rand") === "1"); return Response.json(r); }
    if (u.pathname === "/ws") return env.PROBE.getByName(u.searchParams.get("o") || "step-gw").fetch(req);
    if (u.pathname === "/alarm") {
      const s = env.PROBE.getByName("alarm-" + (u.searchParams.get("o") || "a")); const c = [], un = [];
      for (let i = 0; i < n; i++) { await s.armNow(false); c.push(await s.waitFired()); await s.armNow(true); un.push(await s.waitFired()); }
      return Response.json({ colo: req.cf?.colo, confirmed: { med: med(c), all: c }, unconfirmed: { med: med(un), all: un } });
    }
    if (u.pathname === "/txgate") {
      // Reply latency of each write style itself, and a concurrent reader's latency while that style runs in a loop.
      const s = env.PROBE.getByName("txgate-" + (u.searchParams.get("o") || "a")); const out = { colo: req.cf?.colo };
      for (const m of ["read", "plainUnconfirmed", "txUnconfirmed", "txKv", "writeKv"]) {
        const self = []; for (let i = 0; i < n; i++) { const t = Date.now(); await s[m](); self.push(Date.now() - t); }
        let stop = false; const loop = (async () => { while (!stop) await s[m](); })();
        const reads = []; for (let i = 0; i < n; i++) { const t = Date.now(); await s.read(); reads.push(Date.now() - t); }
        stop = true; await loop;
        out[m] = { self: med(self), concurrentRead: med(reads) };
      }
      return Response.json(out);
    }
    if (u.pathname === "/gate") {
      // Output gate scope: read() latency alone, then while another request writes (confirmed) in a loop on the same object.
      const s = env.PROBE.getByName("gate-" + (u.searchParams.get("o") || "a"));
      const alone = []; for (let i = 0; i < n; i++) { const t = Date.now(); await s.read(); alone.push(Date.now() - t); }
      let stop = false; const writer = (async () => { let w = 0; while (!stop) { await s.writeKv(); w++; } return w; })();
      const busy = []; for (let i = 0; i < n; i++) { const t = Date.now(); await s.read(); busy.push(Date.now() - t); }
      stop = true; const writes = await writer;
      let stop2 = false; const writer2 = (async () => { while (!stop2) await s.writeUnconfirmed(); })();
      const busyU = []; for (let i = 0; i < n; i++) { const t = Date.now(); await s.read(); busyU.push(Date.now() - t); }
      stop2 = true; await writer2;
      return Response.json({ colo: req.cf?.colo, readAlone: med(alone), readWhileConfirmedWrites: med(busy), readWhileUnconfirmedWrites: med(busyU), writes, alone, busy });
    }
    if (u.pathname === "/acquire") {
      // acquire + release of an unclaimed lease (no socket): the connect path's first controller step, timed.
      const cell = +(u.searchParams.get("cell") || 5); const c = env.CTRL.getByName(`gateway-cell-${cell}`);
      const fnv = (id) => { let h = 0x811c9dc5; for (let i = 0; i < id.length; i++) h = Math.imul(h ^ id.charCodeAt(i), 0x01000193) >>> 0; return h % 32; };
      const a = [], r = [];
      for (let i = 0; i < n; i++) {
        let id; do id = crypto.randomUUID(); while (fnv(id) !== cell);
        let t = Date.now(); const lease = await c.acquire(id); a.push(Date.now() - t);
        t = Date.now(); await c.release(lease); r.push(Date.now() - t);
      }
      return Response.json({ colo: req.cf?.colo, cell, acquire: { med: med(a), all: a }, release: { med: med(r), all: r } });
    }
    if (u.pathname === "/tenant") {
      const c = env.CONTROL.getByName("control");
      const route = await c.sessionPublicTenant(u.searchParams.get("project"), u.searchParams.get("domain"));
      if (!route?.ok) return Response.json({ route: route?.ok ?? null, error: String(route?.error ?? "").slice(0, 80) });
      const t = env.TENANT.getByName(route.value.tenantId);
      return Response.json({ colo: req.cf?.colo, ping: await time(() => t.ping(), 15), control: await time(() => c.moduleInfo("x17-none"), 15) });
    }
    if (u.pathname === "/opencost") {
      const c = env.CONTROL.getByName("control");
      const route = await c.sessionPublicTenant(u.searchParams.get("project"), u.searchParams.get("domain"));
      if (!route?.ok) return Response.json({ error: "route" });
      const t = env.TENANT.getByName(route.value.tenantId); const at = new Date().toISOString(); const t0 = Date.now();
      const only = u.searchParams.get("only") ? u.searchParams.get("only").split(",") : undefined;
      const out = await t.devOpenCost(u.searchParams.get("phase"), +(u.searchParams.get("rounds") || 1), only);
      return Response.json({ at, wallMs: Date.now() - t0, ...out });
    }
    if (u.pathname === "/control") {
      const s = env.CONTROL.getByName("control");
      return Response.json({ colo: req.cf?.colo, moduleInfo: await time(() => s.moduleInfo("x17-none"), 15) });
    }
    const stub = env.PROBE.getByName(name, hint ? { locationHint: hint } : undefined);
    const out = { colo: req.cf?.colo, n };
    for (const m of ["read", "writeKv", "writeUnconfirmed", "sql", "read"]) {
      const ts = [];
      for (let i = 0; i < n; i++) { const t = performance.now(); await stub[m](); ts.push(Date.now()); }
      // Date.now advances on I/O in Workers: time each call by the clock around the await.
      const d = []; let prev = null;
      for (let i = 0; i < n; i++) { const t0 = Date.now(); await stub[m](); d.push(Date.now() - t0); }
      out[m] = { med: med(d), min: Math.min(...d), max: Math.max(...d) };
    }
    if (u.searchParams.get("o2")) { const d = [], w = []; for (let i = 0; i < n; i++) { d.push(await stub.nested(u.searchParams.get("o2"))); w.push(await stub.nestedWrite(u.searchParams.get("o2"))); } out.doToDo = { read: med(d), write: med(w) }; }
    return Response.json(out);
  },
};
