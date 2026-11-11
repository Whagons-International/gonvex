// K04 / 0012 R25 / 0016 G0-11 probe: what a CPU-runaway Reducer does to a tenant Durable Object.
// Measurement code, not kernel code. Deployed only as gonvex-cf-dev-k04-r25* on the development
// account and deleted by run.mjs when the run ends.
//
//   TenantDO   one per tenant: SQLite rows, hibernatable sockets, Reducers in ctx.storage.transaction(async)
//   GatewayDO  G0-1 placement: holds sockets, forwards calls to the tenant by RPC
//   LOADER     Worker Loader binding: runs "module code" in a separate dynamic isolate (guard candidate 9b)
//
// Every route needs ?k=<ADMIN_KEY>.
//   GET  /ws/t/<tenant>                    socket held by the tenant object (ctx.acceptWebSocket)
//   GET  /ws/g/<gateway>?t=<tenant>        socket held by a gateway object
//   POST /rpc/t/<tenant>/<method>          body = JSON argument; Worker -> tenant RPC
//   POST /rpc/g/<gateway>/<method>         body = JSON argument; Worker -> gateway RPC
//   GET  /health
import { DurableObject, RpcTarget, WorkerEntrypoint } from "cloudflare:workers";

// ---------------------------------------------------------------------------------------------
// Isolate identity. Random values are not allowed at global scope, so the id is made lazily.
let ISO = null;
let ISO_BORN = null;
const iso = () => {
  if (!ISO) {
    ISO = crypto.randomUUID().slice(0, 8);
    ISO_BORN = Date.now();
  }
  return ISO;
};
const LIVE = new Map(); // object name -> { cls, inst } for every object constructed in this isolate

const errInfo = (e) => ({
  name: e?.name ?? null,
  message: e?.message ?? String(e),
  string: String(e),
  retryable: e?.retryable ?? null,
  overloaded: e?.overloaded ?? null,
  remote: e?.remote ?? null,
  durableObjectReset: e?.durableObjectReset ?? null,
});

const withTimeout = (p, ms, label) => {
  let t;
  return Promise.race([
    p.finally(() => clearTimeout(t)),
    new Promise((_, rej) => {
      t = setTimeout(() => rej(new Error(`${label} wall timeout after ${ms} ms`)), ms);
    }),
  ]);
};

// The busy work every "module" path runs. No host calls, no awaits: CPU only.
function spin(iters) {
  let x = 0;
  for (let i = 0; i < iters; i++) x = (x * 31 + i) | 0;
  return x;
}
function forever() {
  let x = 0;
  for (;;) x = (x * 31 + 1) | 0;
}

// ---------------------------------------------------------------------------------------------
// Module code for the dynamic isolate (Worker Loader). It gets a host RpcTarget that runs SQL in
// the tenant object, inside the tenant's open transaction.
const EXECUTOR_SOURCE = `
import { WorkerEntrypoint } from "cloudflare:workers";
let ISO = null;
const iso = () => (ISO ??= crypto.randomUUID().slice(0, 8));
export default class Executor extends WorkerEntrypoint {
  async run(kind, iters, host, hostCalls) {
    const t0 = Date.now();
    let x = 0;
    if (kind === "forever") { for (;;) x = (x * 31 + 1) | 0; }
    if (kind === "busy") { for (let i = 0; i < iters; i++) x = (x * 31 + i) | 0; }
    let hostRows = 0;
    for (let i = 0; i < (hostCalls ?? 0); i++) {
      const r = await host.exec("SELECT count(*) AS n FROM rows");
      hostRows = r[0].n;
    }
    if (kind === "write") await host.exec("INSERT INTO rows(tag) VALUES ('loader-write')");
    return { iso: iso(), x, hostRows, dt: Date.now() - t0 };
  }
}
`;

class Host extends RpcTarget {
  #sql;
  constructor(sql) {
    super();
    this.#sql = sql;
    this.calls = 0;
  }
  exec(query) {
    this.calls++;
    return this.#sql.exec(query).toArray();
  }
}

// ---------------------------------------------------------------------------------------------
export class TenantDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.inst = crypto.randomUUID().slice(0, 8);
    this.born = Date.now();
    this.name = ctx.id.name ?? ctx.id.toString().slice(0, 12);
    this.sql.exec("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v INTEGER)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS rows(id INTEGER PRIMARY KEY, tag TEXT)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS poison(cmd TEXT PRIMARY KEY, at INTEGER, why TEXT)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS inflight(cmd TEXT PRIMARY KEY, at INTEGER)");
    // Spike-only: constructor counter in SQLite, to count resets across isolates.
    this.sql.exec("INSERT INTO meta VALUES ('ctor', 1) ON CONFLICT(k) DO UPDATE SET v = v + 1");
    LIVE.set(this.name, { cls: "tenant", inst: this.inst });
  }

  state() {
    const meta = Object.fromEntries(this.sql.exec("SELECT k, v FROM meta").toArray().map((r) => [r.k, r.v]));
    const counts = this.sql
      .exec(
        "SELECT (SELECT count(*) FROM rows WHERE tag LIKE 'seed%') AS seed, (SELECT count(*) FROM rows WHERE tag LIKE 'partial%') AS partial, (SELECT count(*) FROM rows) AS total, (SELECT count(*) FROM poison) AS poison, (SELECT count(*) FROM inflight) AS inflight",
      )
      .one();
    return {
      name: this.name,
      inst: this.inst,
      iso: iso(),
      isoBorn: ISO_BORN,
      born: this.born,
      ctor: meta.ctor ?? 0,
      ...counts,
      sockets: this.ctx.getWebSockets().length,
      colocated: [...LIVE.entries()].map(([n, v]) => `${v.cls}:${n}:${v.inst}`),
      now: Date.now(),
    };
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") return Response.json(this.state());
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    // "p:<seq>" -> pong with the instance and isolate ids.
    const seq = String(message).slice(2);
    ws.send(JSON.stringify({ pong: seq, inst: this.inst, iso: iso() }));
  }

  async webSocketClose(ws, code, reason) {
    try {
      ws.close(code, reason);
    } catch {}
  }

  info() {
    return this.state();
  }

  seed(n) {
    for (let i = 0; i < n; i++) this.sql.exec("INSERT INTO rows(tag) VALUES (?)", `seed-${i}`);
    return this.state();
  }

  markPoison({ cmd, why }) {
    this.sql.exec("INSERT OR REPLACE INTO poison VALUES (?, ?, ?)", cmd, Date.now(), why ?? "caller");
    return { marked: cmd };
  }

  // One Reducer invocation: one ctx.storage.transaction(async), as K04 runs it.
  //   kind: ok | busy (iters) | forever | loader-* (module code in the dynamic isolate)
  //   poison: none | check (refuse cmd ids in the poison table) | inflight (durable start marker)
  async reducer({ cmd, kind, iters = 0, poison = "none", cpuMs, wallMs = 5000, hostCalls = 0 }) {
    const t0 = Date.now();
    if (poison !== "none") {
      const hit = this.sql.exec("SELECT why FROM poison WHERE cmd = ?", cmd).toArray()[0];
      if (hit) throw new Error(`budget_exceeded: command ${cmd} is quarantined (${hit.why})`);
    }
    if (poison === "inflight") {
      const stale = this.sql.exec("SELECT at FROM inflight WHERE cmd = ?", cmd).toArray()[0];
      if (stale) {
        // A previous attempt started and never finished: the object was reset under it.
        this.sql.exec("INSERT OR REPLACE INTO poison VALUES (?, ?, 'inflight')", cmd, Date.now());
        this.sql.exec("DELETE FROM inflight WHERE cmd = ?", cmd);
        throw new Error(`budget_exceeded: command ${cmd} is quarantined (inflight marker found)`);
      }
      this.sql.exec("INSERT INTO inflight VALUES (?, ?)", cmd, Date.now());
      await this.ctx.storage.sync(); // make the marker durable before running module code
    }
    const tSynced = Date.now();
    let out;
    try {
      out = await this.ctx.storage.transaction(async () => {
        this.sql.exec("INSERT INTO rows(tag) VALUES (?)", `partial-${cmd}`);
        if (kind === "forever") forever();
        if (kind === "busy") spin(iters);
        if (kind.startsWith("loader-")) {
          const host = new Host(this.sql);
          const exec = this.executor(cpuMs);
          try {
            const r = await withTimeout(exec.run(kind.slice(7), iters, host, hostCalls), wallMs, "loader");
            this.sql.exec("UPDATE rows SET tag = ? WHERE tag = ?", `done-${cmd}`, `partial-${cmd}`);
            return { loader: r, hostCalls: host.calls };
          } catch (e) {
            const err = new Error(`budget_exceeded: ${errInfo(e).string}`);
            err.inner = errInfo(e);
            throw err;
          }
        }
        // ok: a completed Reducer keeps its row, retagged as committed.
        this.sql.exec("UPDATE rows SET tag = ? WHERE tag = ?", `done-${cmd}`, `partial-${cmd}`);
        return { kind };
      });
    } finally {
      if (poison === "inflight") this.sql.exec("DELETE FROM inflight WHERE cmd = ?", cmd);
    }
    return { ...out, inst: this.inst, iso: iso(), syncMs: tSynced - t0, ms: Date.now() - t0 };
  }

  executor(cpuMs) {
    const key = `exec-${cpuMs ?? "none"}`;
    return this.env.LOADER.get(key, () => ({
      compatibilityDate: "2026-10-23",
      mainModule: "executor.js",
      modules: { "executor.js": EXECUTOR_SOURCE },
      globalOutbound: null,
      ...(cpuMs ? { limits: { cpuMs } } : {}),
    })).getEntrypoint();
  }

  // Q9b cost: round trips to the dynamic isolate, and host calls back into this object while the
  // tenant transaction is open.
  async loaderLatency({ n = 20, hostCalls = 20 }) {
    const exec = this.executor(undefined);
    const empty = [];
    for (let i = 0; i < n; i++) {
      const t = performance.now();
      await exec.run("busy", 0, null, 0);
      empty.push(performance.now() - t);
    }
    const inTx = await this.ctx.storage.transaction(async () => {
      const host = new Host(this.sql);
      const t = performance.now();
      const r = await exec.run("busy", 0, host, hostCalls);
      return { ms: performance.now() - t, hostCalls: host.calls, loaderIso: r.iso };
    });
    const local = [];
    for (let i = 0; i < n; i++) {
      const t = performance.now();
      await this.ctx.storage.transaction(async () => spin(0));
      local.push(performance.now() - t);
    }
    return { empty, inTx, localTx: local, iso: iso() };
  }

  // Q9b: host callbacks from the dynamic isolate into this object, with and without the input
  // gate held (mode: none | tx = ctx.storage.transaction | bcw = blockConcurrencyWhile).
  async loaderHostCalls({ hostCalls = 5, mode = "none", wallMs = 3000 }) {
    const exec = this.executor(undefined);
    const host = new Host(this.sql);
    const t = performance.now();
    const run = () => withTimeout(exec.run("busy", 0, host, hostCalls), wallMs, "loader");
    try {
      if (mode === "tx") await this.ctx.storage.transaction(run);
      else if (mode === "bcw") await this.ctx.blockConcurrencyWhile(run);
      else await run();
      return { ok: true, mode, hostCalls: host.calls, ms: performance.now() - t };
    } catch (e) {
      return { ok: false, mode, hostCalls: host.calls, ms: performance.now() - t, error: errInfo(e) };
    }
  }

  // Q8: does the clock move during synchronous work?
  async clock({ iters = 3e8, cte = 3e6 }) {
    const out = {};
    let d0 = Date.now(), p0 = performance.now();
    const x = spin(iters);
    out.busy = { iters, dDate: Date.now() - d0, dPerf: performance.now() - p0, x };
    d0 = Date.now(); p0 = performance.now();
    const n = this.sql.exec(`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < ${cte}) SELECT count(*) AS n FROM c`).one().n;
    out.sqlCte = { rows: n, dDate: Date.now() - d0, dPerf: performance.now() - p0 };
    d0 = Date.now(); p0 = performance.now();
    for (let i = 0; i < 2000; i++) this.sql.exec("INSERT INTO rows(tag) VALUES ('clock')");
    this.sql.exec("DELETE FROM rows WHERE tag = 'clock'");
    out.sqlInserts = { calls: 2001, dDate: Date.now() - d0, dPerf: performance.now() - p0 };
    out.inTx = await this.ctx.storage.transaction(async () => {
      const d = Date.now(), p = performance.now();
      spin(iters);
      for (let i = 0; i < 500; i++) this.sql.exec("SELECT count(*) FROM rows").one();
      const a = { dDate: Date.now() - d, dPerf: performance.now() - p };
      await Promise.resolve();
      a.afterMicrotask = { dDate: Date.now() - d, dPerf: performance.now() - p };
      await this.ctx.storage.transaction(async () => {}); // nested: goes through blockConcurrencyWhile
      a.afterNestedTx = { dDate: Date.now() - d, dPerf: performance.now() - p };
      await new Promise((r) => setTimeout(r, 0));
      a.afterSetTimeout0 = { dDate: Date.now() - d, dPerf: performance.now() - p };
      return a;
    });
    return { ...out, iso: iso() };
  }

  // Q9a: guard checks at host-call boundaries. Module code spins and makes a host call every
  // `every` iterations; the host checks a wall budget (clock) and a host-call cap (counter).
  async guarded({ cmd, budgetMs = 1000, maxCalls = 1e9, every = 1e5, maxIters = 2e9 }) {
    const t0 = Date.now(), p0 = performance.now();
    let calls = 0, iters = 0, why = null;
    const host = () => {
      calls++;
      this.sql.exec("SELECT 1").one();
      if (Date.now() - t0 > budgetMs) throw new Error(`budget_exceeded: clock ${Date.now() - t0} ms`);
      if (calls > maxCalls) throw new Error(`budget_exceeded: ${calls} host calls`);
    };
    try {
      await this.ctx.storage.transaction(async () => {
        this.sql.exec("INSERT INTO rows(tag) VALUES (?)", `partial-${cmd}`);
        let x = 0;
        while (iters < maxIters) {
          for (let i = 0; i < every; i++) x = (x * 31 + i) | 0;
          iters += every;
          host();
        }
      });
      why = "ran to maxIters";
    } catch (e) {
      why = errInfo(e).message;
    }
    return { why, calls, iters, dDate: Date.now() - t0, dPerf: performance.now() - p0, ...this.state() };
  }
}

// ---------------------------------------------------------------------------------------------
export class GatewayDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.inst = crypto.randomUUID().slice(0, 8);
    this.name = ctx.id.name ?? ctx.id.toString().slice(0, 12);
    LIVE.set(this.name, { cls: "gateway", inst: this.inst });
  }

  info() {
    return {
      name: this.name,
      inst: this.inst,
      iso: iso(),
      sockets: this.ctx.getWebSockets().length,
      colocated: [...LIVE.entries()].map(([n, v]) => `${v.cls}:${n}:${v.inst}`),
    };
  }

  async fetch(request) {
    const tenant = new URL(request.url).searchParams.get("t");
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ tenant });
    return new Response(null, { status: 101, webSocket: client });
  }

  // "p:<seq>" answered by the gateway alone; "c:<seq>" forwarded to the tenant (info RPC).
  async webSocketMessage(ws, message) {
    const text = String(message);
    const seq = text.slice(2);
    if (text.startsWith("p:")) {
      ws.send(JSON.stringify({ pong: seq, inst: this.inst, iso: iso() }));
      return;
    }
    const { tenant } = ws.deserializeAttachment();
    const t = Date.now();
    try {
      const r = await this.env.TENANT.getByName(tenant).info();
      ws.send(JSON.stringify({ call: seq, ok: true, ms: Date.now() - t, tenantInst: r.inst }));
    } catch (e) {
      ws.send(JSON.stringify({ call: seq, ok: false, ms: Date.now() - t, error: errInfo(e) }));
    }
  }

  async webSocketClose(ws, code, reason) {
    try {
      ws.close(code, reason);
    } catch {}
  }

  // Gateway -> tenant RPC, measured from the gateway.
  async callTenant({ tenant, method, arg }) {
    const t = Date.now();
    try {
      const r = await this.env.TENANT.getByName(tenant)[method](arg);
      return { ok: true, ms: Date.now() - t, result: r, gw: this.info() };
    } catch (e) {
      return { ok: false, ms: Date.now() - t, error: errInfo(e), gw: this.info() };
    }
  }
}

// ---------------------------------------------------------------------------------------------
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return Response.json({ ok: true, iso: iso() });
    if (url.searchParams.get("k") !== env.ADMIN_KEY) return new Response("forbidden", { status: 403 });
    const [kind, cls, name, method] = url.pathname.split("/").filter(Boolean);
    const ns = cls === "t" ? env.TENANT : cls === "g" ? env.GATEWAY : null;
    if (!ns || !name) return new Response("not found", { status: 404 });
    const stub = ns.getByName(name);
    if (kind === "ws") return stub.fetch(request);
    if (kind !== "rpc") return new Response("not found", { status: 404 });
    const arg = request.method === "POST" ? await request.json() : {};
    const t = Date.now();
    try {
      const result = await stub[method](arg);
      return Response.json({ ok: true, ms: Date.now() - t, workerIso: iso(), result });
    } catch (e) {
      const error = errInfo(e);
      // Q9c, caller-side quarantine: the caller saw the overrun, so it marks the command id on a
      // fresh stub (the old one is broken after a reset).
      let marked = null;
      if (arg.poison === "caller" && /exceeded|reset/i.test(error.message)) {
        try {
          marked = await ns.getByName(name).markPoison({ cmd: arg.cmd, why: "caller saw overrun" });
        } catch (e2) {
          marked = { error: errInfo(e2) };
        }
      }
      return Response.json({ ok: false, ms: Date.now() - t, workerIso: iso(), error, marked });
    }
  },
};
