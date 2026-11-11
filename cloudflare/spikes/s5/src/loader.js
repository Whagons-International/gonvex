// S5 deployed Worker. Two builds of this one file (scripts/prepare.mjs):
//
//   loader: app.js is empty. The tenant object loads each module generation at
//           runtime through the Worker Loader (env.LOADER) from the Bundles
//           object, and runs it either as a Runner (host calls over RPC back
//           into this object) or as a Durable Object Facet (own SQLite).
//   baked:  app.js is the Whagons Query/Reducer bundle, as in S2 and 0016 G0-2.
//           Used as the platform baseline ("inline" mode).
//
// Spike instrumentation that a kernel must not copy: the ballast probe, the
// admin key in a query string, per-call timing in responses.
import { installed } from "./runtime-globals.js";
import { DurableObject, RpcTarget } from "cloudflare:workers";
import kernel from "./kernel-modules.json";
import build from "./build.json";
import * as app from "./app.js";
import * as savepointModule from "./savepoint-module.js";
import functions from "./functions.json";
import { dispatch } from "./bootstrap.js";
import { DurableObjectEngine } from "./engines.js";
import { SqlHost } from "./host.js";
import { invokeTx, requestOf, storageTransaction, provisionSql, tableCounts } from "./invoke-tx.js";

const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
const COMPATIBILITY_DATE = "2026-11-01";
const now = () => installed.Date.now();
// Random values are refused at global scope, so the isolate id is drawn on first use.
let isolateId;
const isolateOf = () => (isolateId ??= installed.crypto.randomUUID().slice(0, 8));

/** Bindings for the baked build (inline mode): S2's resolve plus the synthetic s5.* functions. */
const s5Inline = {
  whoami: async () => ({ generation: "baked", isolate: isolateOf() }),
  noop: async () => null,
  slow: async (ctx, { n }) => { for (let i = 0; i < n; i++) await ctx.db.query("SELECT 1 AS one"); return { generation: "baked", isolate: isolateOf(), n }; },
  slowThenThrow: async (ctx, { n }) => { for (let i = 0; i < n; i++) await ctx.db.query("SELECT 1 AS one"); throw new Error(`planned failure after ${n} host calls`); },
};
function resolveInline(path) {
  if (path.startsWith("s5.")) return { binding: s5Inline[path.slice(3)], argsSchema: { kind: "any" } };
  if (path.startsWith("s2.")) { const binding = savepointModule[path.slice(3)]; return { binding, argsSchema: binding?.options?.args }; }
  return { binding: app[path], argsSchema: functions[path]?.args };
}

/** The tenant object's host, handed to module code in the Dynamic Worker for one invocation. */
class HostTarget extends RpcTarget {
  #host;
  constructor(host) { super(); this.#host = host; }
  handle(payload) { return this.#host.handle(payload); }
  noop() { this.#host.stats.calls++; return 1; }
}

/** Module bundles by code key, in chunks under the 2 MB row limit. A kernel would use R2 or the control object. */
export class Bundles extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS chunk (code TEXT, name TEXT, part INTEGER, data TEXT, PRIMARY KEY (code, name, part))");
  }
  put(code, name, part, data) { this.ctx.storage.sql.exec("INSERT OR REPLACE INTO chunk VALUES (?1, ?2, ?3, ?4)", code, name, part, data); return { bytes: data.length }; }
  modules(code, names) {
    const out = {};
    for (const { name, data } of this.ctx.storage.sql.exec("SELECT name, data FROM chunk WHERE code = ?1 ORDER BY name, part", code).toArray()) {
      if (names && !names.includes(name)) continue;
      out[name] = (out[name] ?? "") + data;
    }
    return out;
  }
  list() { return this.ctx.storage.sql.exec("SELECT code, name, count(*) AS parts, sum(length(data)) AS bytes FROM chunk GROUP BY code, name").toArray(); }
}

export class Tenant extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.constructedAt = now();
    this.inst = installed.crypto.randomUUID().slice(0, 8);
    this.active = ctx.storage.kv.get("active") ?? null;
    this.kinds = new Map();
    this.loads = [];
    this.ballastMb = 0;
    this.ballast = [];
    this.inflight = new Set();
  }

  bundles() { return this.env.BUNDLES.get(this.env.BUNDLES.idFromName("bundles")); }

  /** The WorkerCode for one generation; the callback runs only when no warm isolate has this id. */
  worker(gen) {
    return this.env.LOADER.get(gen.id, async () => {
      const started = now();
      const modules = await this.bundles().modules(gen.code, ["app.js", "functions.json"]);
      const fetched = now();
      const code = {
        compatibilityDate: COMPATIBILITY_DATE,
        compatibilityFlags: ["nodejs_als"],
        mainModule: "gen.js",
        modules: {
          ...kernel,
          "app.js": modules["app.js"],
          "functions.json": { json: JSON.parse(modules["functions.json"]) },
          "meta.json": { json: { generation: gen.label, code: gen.code, nonce: gen.nonce ?? null } },
        },
        globalOutbound: null,
        env: {},
        // Per-invocation CPU limit for the generation (Runner calls and facet events).
        ...(gen.cpuMs ? { limits: { cpuMs: gen.cpuMs } } : {}),
      };
      this.loads.push({ id: gen.id, at: started, fetchMs: fetched - started, appBytes: modules["app.js"].length });
      return code;
    });
  }

  runner(gen = this.active) { return this.worker(gen).getEntrypoint("Runner"); }
  facet() {
    return this.ctx.facets.get("app", async () => ({ class: this.worker(this.active).getDurableObjectClass("TenantFacet") }));
  }

  async functionKinds(gen) {
    if (!this.kinds.has(gen.code)) this.kinds.set(gen.code, JSON.parse((await this.bundles().modules(gen.code, ["kinds.json"]))["kinds.json"] ?? "{}"));
    return this.kinds.get(gen.code);
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.headers.get("upgrade") === "websocket") {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    const op = url.pathname.split("/").pop();
    const body = request.method === "POST" ? await request.json() : {};
    try {
      return json(await this.op(op, body));
    } catch (error) {
      return json({ error: `${error?.name ?? "Error"}: ${error?.message ?? error}`, stack: String(error?.stack ?? ""), remote: error?.remote ?? null, inst: this.inst }, 500);
    }
  }

  async op(op, body) {
    switch (op) {
      case "stats": return { inst: this.inst, isolate: isolateOf(), build: build.variant, constructedAt: this.constructedAt, active: this.active, sockets: this.ctx.getWebSockets().length, loads: this.loads, ballastMb: this.ballastMb };
      case "provision": {
        if (body.target === "facet") return { ...(await this.facet().provision(body)), target: "facet" };
        return { ...provisionSql(this.ctx.storage.sql, body), target: "self" };
      }
      case "counts": return body.target === "facet" ? this.facet().counts() : tableCounts(this.ctx.storage.sql);
      case "set-active": { this.active = body.gen; this.ctx.storage.kv.put("active", body.gen); return { active: this.active }; }
      case "load": return this.load(body.gen ?? this.active);
      case "invoke": return this.invokeAll(body);
      case "hostbench": return this.hostbench(body);
      case "activate": return this.activate(body);
      case "isolation": return this.runner(body.gen ?? this.active).isolation();
      case "ballast-dyn": return this.runner(body.gen ?? this.active).ballast(body.mb);
      case "ballast-facet": return this.facet().ballast(body.mb);
      case "ballast-do": {
        for (let i = 0; i < body.mb; i++) { this.ballast.push(Array.from({ length: 131072 }, (_, j) => j + 0.5)); this.ballastMb++; }
        return { inst: this.inst, ballastMb: this.ballastMb };
      }
      case "facet-info": return this.facet().info();
      // An unrelated event's write, outside any transaction: does it land inside an open invocation transaction?
      case "mark": {
        this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS s5_marks (id TEXT PRIMARY KEY, at INTEGER)");
        this.ctx.storage.sql.exec("INSERT INTO s5_marks VALUES (?1, ?2)", body.id, now());
        return { id: body.id, at: now() };
      }
      case "marks": {
        this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS s5_marks (id TEXT PRIMARY KEY, at INTEGER)");
        return this.ctx.storage.sql.exec("SELECT id FROM s5_marks ORDER BY id").toArray().map((r) => r.id);
      }
      default: throw new Error(`unknown op ${op}`);
    }
  }

  /** Time to a first answer from a generation's Runner, and whether the code callback ran. */
  async load(gen) {
    const before = this.loads.length;
    const started = now();
    const info = await this.runner(gen).info();
    const ms = now() - started;
    const load = this.loads.slice(before).find((entry) => entry.id === gen.id);
    return { ms, callbackRan: !!load, fetchMs: load?.fetchMs ?? null, info, inst: this.inst };
  }

  /** Runs calls in order, one storage transaction each, in the given mode. */
  async invokeAll({ calls, mode }) {
    const results = [];
    const kinds = mode.startsWith("rpc") ? await this.functionKinds(this.active) : functions;
    for (const call of calls) {
      const started = now();
      let outcome;
      try {
        if (mode === "facet") {
          const pending = this.facet().invoke(call);
          this.inflight.add(pending);
          try { outcome = await pending; } finally { this.inflight.delete(pending); }
        } else {
          const role = call.identity?.member?.role;
          const options = {
            engine: new DurableObjectEngine(this.ctx.storage), call,
            // rpc-notx: no storage transaction (S2's "implicit" mode). Only to measure RPC cost:
            // it has no rollback, and other events interleave at every host call.
            transaction: mode === "rpc-notx" ? (run) => run(() => {}) : storageTransaction(this.ctx.storage),
            permissions: () => role === "owner" || role === "admin",
            schedulable: (path) => !!kinds[path] && kinds[path].kind !== "query",
            wallNow: now,
          };
          if (mode === "rpc" || mode === "rpc-notx") {
            const runner = this.runner();
            outcome = await invokeTx(options, (host) => runner.run(call, new HostTarget(host)));
          } else {
            const { binding, argsSchema } = resolveInline(call.path);
            outcome = await invokeTx(options, (host) => dispatch(binding, requestOf(call), call.args, (payload) => host.handle(payload), argsSchema));
          }
        }
      } catch (error) {
        outcome = { envelope: { status: "error", kind: "platform", message: String(error?.message ?? error) }, committed: false };
      }
      results.push({ path: call.path, ms: now() - started, startedAt: started, ...outcome });
    }
    return { mode, inst: this.inst, active: this.active?.label ?? null, results };
  }

  /** n RPC round trips from the Runner back into this object, outside or inside a storage transaction. */
  async hostbench({ n, kind = "noop", inTx = false, cpuMs = null, wallMs = null }) {
    // cpuMs: the Worker Loader's per-call CPU limit (getEntrypoint limits), as K04's R25 guard uses it.
    const runner = cpuMs ? this.worker(this.active).getEntrypoint("Runner", { limits: { cpuMs } }) : this.runner();
    const host = new SqlHost(new DurableObjectEngine(this.ctx.storage), { capability: "query", permissions: () => false, schedulable: () => false, wallNow: now });
    // wallMs: give up after this long (a timer race, as K04's probe does) and report how many calls completed.
    const run = () => {
      const loop = runner.hostLoop(new HostTarget(host), n, kind);
      if (!wallMs) return loop;
      return Promise.race([loop, new Promise((_, reject) => setTimeout(() => reject(new Error(`wall timeout after ${wallMs} ms with ${host.stats.calls} of ${n} host calls completed`)), wallMs))]);
    };
    const started = now();
    let result;
    try {
      result = inTx ? await this.ctx.storage.transaction(() => run()) : await run();
    } catch (error) {
      return { n, kind, inTx, cpuMs, wallMs, failed: String(error?.message ?? error), completed: host.stats.calls, ms: now() - started, inst: this.inst };
    }
    const ms = now() - started;
    return { ...result, inTx, cpuMs, ms, perCallMs: ms / n, completed: host.stats.calls, inst: this.inst };
  }

  /**
   * Generation switch: optionally warm the new generation first, swap the
   * reference, abort the facet if asked, then send system.reload to every socket.
   */
  async activate({ gen, preload = true, abortFacet = false, drain = false }) {
    const started = now();
    if (preload) await this.runner(gen).info();
    const warmed = now();
    // drain: let facet invocations already running finish on the old generation before the abort.
    const draining = this.inflight.size;
    if (drain) await Promise.allSettled([...this.inflight]);
    const drained = now();
    const previous = this.active;
    this.active = gen;
    this.ctx.storage.kv.put("active", gen);
    if (abortFacet) this.ctx.facets.abort("app", new Error("module generation changed"));
    const frame = JSON.stringify({ type: "system.reload", reason: "module generation changed", artifactHash: gen.id });
    let sent = 0;
    for (const socket of this.ctx.getWebSockets()) { try { socket.send(frame); sent++; } catch { /* closing */ } }
    return { previous: previous?.label ?? null, active: gen.label, preloadMs: warmed - started, draining, drainMs: drained - warmed, sent, at: started, inst: this.inst };
  }

  webSocketMessage(socket, message) {
    if (message === "echo") { socket.send(JSON.stringify({ type: "echo", generation: this.active?.label ?? null, inst: this.inst, t: now() })); return; }
    if (typeof message === "string" && message.startsWith("write")) {
      const frame = JSON.stringify({ type: "w", t: now(), pad: "x".repeat(256) });
      for (const peer of this.ctx.getWebSockets()) { try { peer.send(frame); } catch { /* closing */ } }
    }
  }
  webSocketClose(socket, code, reason) { try { socket.close(code, reason); } catch { /* already closed */ } }
}

/**
 * G0-1 placement: a gateway object holds sockets and calls the tenant object by RPC. When an answer
 * shows a new generation, it sends system.reload to its own sockets (as the kernel would on the
 * tenant's activation fan-out).
 */
export class Gateway extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.inst = installed.crypto.randomUUID().slice(0, 8); this.generation = null; }
  async fetch(request) {
    const url = new URL(request.url);
    if (request.headers.get("upgrade") === "websocket") {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    return json({ inst: this.inst, sockets: this.ctx.getWebSockets().length, generation: this.generation });
  }
  async webSocketMessage(socket, message) {
    if (typeof message !== "string" || !message.startsWith("invoke:")) return;
    const tenant = this.env.TENANT.get(this.env.TENANT.idFromName(message.slice(7)));
    try {
      const result = await tenant.op("invoke", { mode: "facet", calls: [{ path: "s5.whoami", args: {}, now: 1792324800123, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: "gw" } }] });
      const envelope = result.results?.[0]?.envelope;
      const generation = envelope?.status === "ok" ? JSON.parse(envelope.value).generation : null;
      if (generation && this.generation && generation !== this.generation) {
        const frame = JSON.stringify({ type: "system.reload", reason: "module generation changed", artifactHash: generation });
        for (const peer of this.ctx.getWebSockets()) { try { peer.send(frame); } catch { /* closing */ } }
      }
      if (generation) this.generation = generation;
      socket.send(JSON.stringify({ type: "result", ok: envelope?.status === "ok", generation, message: envelope?.message ?? null }));
    } catch (error) {
      socket.send(JSON.stringify({ type: "result", ok: false, message: String(error?.message ?? error).slice(0, 200) }));
    }
  }
  webSocketClose(socket, code, reason) { try { socket.close(code, reason); } catch { /* closed */ } }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ping") return json({ ok: true, build: build.variant, appExports: Object.keys(app).length, isolate: isolateOf() });
    if (!env.ADMIN_KEY || url.searchParams.get("k") !== env.ADMIN_KEY) return json({ error: "forbidden" }, 403);
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts[0] === "bundle") {
      const store = env.BUNDLES.get(env.BUNDLES.idFromName("bundles"));
      if (parts[1] === "list") return json(await store.list());
      const q = (k) => url.searchParams.get(k);
      return json(await store.put(q("code"), q("name"), Number(q("part")), await request.text()));
    }
    if (parts[0] === "timed" && parts[1] === "t" && parts[2]) {
      // The front Worker times the call into the tenant object: its clock moves across that I/O, so this
      // is the object's wall time (including the output gate), without the client's network.
      const stub = env.TENANT.get(env.TENANT.idFromName(parts[2]));
      const body = await request.text();
      const started = Date.now();
      const response = await stub.fetch(new Request(`https://do/t/${parts[2]}/${parts[3]}`, { method: "POST", body }));
      const value = await response.json();
      return json({ workerMs: Date.now() - started, status: response.status, value });
    }
    if (parts[0] === "t" && parts[1]) return env.TENANT.get(env.TENANT.idFromName(parts[1])).fetch(request);
    if (parts[0] === "g" && parts[1]) return env.GATEWAY.get(env.GATEWAY.idFromName(parts[1])).fetch(request);
    return json({ error: "not found" }, 404);
  },
};
