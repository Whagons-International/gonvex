// Main module of one module generation, loaded at runtime through the Worker
// Loader. The deployed Worker (loader.js) supplies the kernel modules (S2's
// bootstrap, host, SQL and globals) and the per-generation modules: app.js
// (the Whagons Query/Reducer split bundle), functions.json and meta.json.
//
// Two ways in:
//   Runner (WorkerEntrypoint): dispatch here, host calls go back to the tenant
//     object over RPC (`host` is an RpcTarget stub owned by the tenant object).
//   TenantFacet (DurableObject): run as a Durable Object Facet of the tenant
//     object. The facet owns its own SQLite, so host calls are local sync SQL.
//
// The import order matters: runtime-globals installs the pinned Date, the
// seeded Math.random and the entropy gate before the app bundle evaluates.
import { installed } from "./runtime-globals.js";
import { WorkerEntrypoint, DurableObject } from "cloudflare:workers";
import * as app from "./app.js";
import * as savepointModule from "./savepoint-module.js";
import functions from "./functions.json";
import meta from "./meta.json";
import { dispatch } from "./bootstrap.js";
import { DurableObjectEngine } from "./engines.js";
import { invokeTx, requestOf, storageTransaction, provisionSql, tableCounts } from "./invoke-tx.js";

const evaluatedAt = installed.Date.now();
// Random values are refused at global scope, so the isolate id is drawn on first use.
let isolateId;
const isolateOf = () => (isolateId ??= installed.crypto.randomUUID().slice(0, 8));
const appExports = Object.keys(app).length;

/** Synthetic functions for the swap, overhead and in-flight probes. */
const s5 = {
  whoami: async () => ({ generation: meta.generation, isolate: isolateOf() }),
  noop: async () => null,
  slow: async (ctx, { n }) => {
    for (let i = 0; i < n; i++) await ctx.db.query("SELECT 1 AS one");
    return { generation: meta.generation, isolate: isolateOf(), n };
  },
  // Yields to the event loop mid-invocation (as savepoint Reducers do), with a write before and after.
  sleepy: async (ctx, { ms, id }) => {
    await ctx.db.query("CREATE TABLE IF NOT EXISTS s5_sleepy (id TEXT PRIMARY KEY, phase TEXT)");
    await ctx.db.query("INSERT INTO s5_sleepy VALUES ($1, 'before')", [`${id}-before`]);
    await new Promise((resolve) => setTimeout(resolve, ms));
    await ctx.db.query("INSERT INTO s5_sleepy VALUES ($1, 'after')", [`${id}-after`]);
    return { generation: meta.generation, isolate: isolateOf() };
  },
  sleepyRows: async (ctx) => {
    await ctx.db.query("CREATE TABLE IF NOT EXISTS s5_sleepy (id TEXT PRIMARY KEY, phase TEXT)");
    return (await ctx.db.query("SELECT id FROM s5_sleepy ORDER BY id")).map((row) => row.id);
  },
  // Runaway probe (R25 under a facet): seed rows, then write one row and spin forever.
  seed: async (ctx, { n }) => {
    await ctx.db.query("CREATE TABLE IF NOT EXISTS s5_rows (id INTEGER PRIMARY KEY, tag TEXT)");
    for (let i = 0; i < n; i++) await ctx.db.query("INSERT INTO s5_rows (tag) VALUES ($1)", [`seed-${i}`]);
    return n;
  },
  countRows: async (ctx) => {
    await ctx.db.query("CREATE TABLE IF NOT EXISTS s5_rows (id INTEGER PRIMARY KEY, tag TEXT)");
    return (await ctx.db.query("SELECT (SELECT count(*) FROM s5_rows WHERE tag LIKE 'seed-%') AS seed, (SELECT count(*) FROM s5_rows WHERE tag = 'partial') AS partial"))[0];
  },
  spinAfterWrite: async (ctx) => {
    await ctx.db.query("INSERT INTO s5_rows (tag) VALUES ('partial')");
    let x = 0;
    for (;;) x = (x * 31 + 1) | 0;
  },
  slowThenThrow: async (ctx, { n }) => {
    for (let i = 0; i < n; i++) await ctx.db.query("SELECT 1 AS one");
    throw new Error(`planned failure after ${n} host calls`);
  },
};

function resolve(path) {
  if (path.startsWith("s5.")) return { binding: s5[path.slice(3)], argsSchema: { kind: "any" } };
  if (path.startsWith("s2.")) {
    const binding = savepointModule[path.slice(3)];
    return { binding, argsSchema: binding?.options?.args };
  }
  return { binding: app[path], argsSchema: functions[path]?.args };
}

const info = () => ({ generation: meta.generation, code: meta.code, nonce: meta.nonce, isolate: isolateOf(), evaluatedAt, appExports });
const ballast = [];
let ballastMb = 0;

export class Runner extends WorkerEntrypoint {
  info() { return info(); }

  /** One invocation; host calls cross back into the tenant object. */
  run(call, host) {
    const { binding, argsSchema } = resolve(call.path);
    return dispatch(binding, requestOf(call), call.args, (payload) => host.handle(payload), argsSchema);
  }

  /** n sequential round trips to the tenant object: `noop` or a real dbQuery. */
  async hostLoop(host, n, kind) {
    const started = installed.Date.now();
    for (let i = 0; i < n; i++) {
      if (kind === "noop") await host.noop();
      else await host.handle({ kind: "dbQuery", statement: "SELECT 1 AS one", parameters: [] });
    }
    return { n, kind, insideMs: installed.Date.now() - started };
  }

  /** What module code can reach from here. */
  async isolation() {
    const attempt = async (label, run) => {
      try { const value = await run(); return { label, ok: true, value: String(value).slice(0, 120) }; } catch (error) { return { label, ok: false, error: `${error?.name}: ${String(error?.message).slice(0, 160)}` }; }
    };
    const { connect } = await import("cloudflare:sockets");
    return {
      envKeys: Object.keys(this.env ?? {}),
      checks: [
        await attempt("fetch https://example.com", async () => (await fetch("https://example.com")).status),
        await attempt("fetch https://api.cloudflare.com", async () => (await fetch("https://api.cloudflare.com/client/v4")).status),
        await attempt("connect example.com:80", async () => { const socket = connect("example.com:80"); await socket.opened; return "opened"; }),
        await attempt("caches.default.match", async () => (await caches.default.match("https://example.com")) ?? "miss"),
        await attempt("eval", () => eval("1 + 1")),
        await attempt("new Function", () => new Function("return 1")()),
        await attempt("crypto.randomUUID outside an invocation", () => crypto.randomUUID().length),
      ],
    };
  }

  /** Memory probe: adds `mb` of heap that stays reachable in this isolate. */
  ballast(mb) {
    for (let i = 0; i < mb; i++) { ballast.push(Array.from({ length: 131072 }, (_, j) => j + 0.5)); ballastMb++; }
    return { ...info(), ballastMb };
  }
}

export class TenantFacet extends DurableObject {
  info() { return { ...info(), databaseSize: this.ctx.storage.sql.databaseSize, facetId: String(this.ctx.id) }; }
  provision(body) { return provisionSql(this.ctx.storage.sql, body); }
  counts() { return tableCounts(this.ctx.storage.sql); }

  /** S2's model unchanged: one storage transaction, local sync host calls. */
  async invoke(call) {
    const { binding, argsSchema } = resolve(call.path);
    const role = call.identity?.member?.role;
    const engine = new DurableObjectEngine(this.ctx.storage);
    const outcome = await invokeTx({
      engine, transaction: storageTransaction(this.ctx.storage), call,
      permissions: () => role === "owner" || role === "admin",
      schedulable: (path) => !!functions[path] && functions[path].kind !== "query",
      wallNow: () => installed.Date.now(),
    }, (host) => dispatch(binding, requestOf(call), call.args, (payload) => host.handle(payload), argsSchema));
    return { ...outcome, generation: meta.generation, isolate: isolateOf() };
  }

  /** Memory probe inside the facet's isolate. */
  ballast(mb) {
    for (let i = 0; i < mb; i++) { ballast.push(Array.from({ length: 131072 }, (_, j) => j + 0.5)); ballastMb++; }
    return { ...info(), ballastMb };
  }
}

export default {
  fetch() { return Response.json(info()); },
};
