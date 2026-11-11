// S2 Durable Object: one tenant's SQLite, running real Whagons Reducers from
// the app's Query/Reducer bundle with the Gonvex module contract.
//
// The import order matters: runtime-globals installs the pinned Date, the
// seeded Math.random and the entropy gate before the app bundle evaluates.
import { installed } from "./runtime-globals.js";
import { DurableObject } from "cloudflare:workers";
import * as app from "./app.js";
import * as savepointModule from "./savepoint-module.js";
import functions from "./functions.json";
import { invoke } from "./invoke.js";
import { DurableObjectEngine } from "./engines.js";
import { dispatch } from "./bootstrap.js";
import * as contractRunner from "./contract/runner.js";
import vectors from "./contract/vectors.json";

const appExports = Object.keys(app).length;
const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

/** Function bindings: app paths, plus the spike's savepoint module under `s2.*`. */
function resolve(path) {
  if (path.startsWith("s2.")) {
    const binding = savepointModule[path.slice(3)];
    return { binding, argsSchema: binding?.options?.args };
  }
  return { binding: app[path], argsSchema: functions[path]?.args };
}

export class Tenant extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    globalThis.__s2Constructed = (globalThis.__s2Constructed ?? 0) + 1;
    this.instance = globalThis.__s2Constructed;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const body = request.method === "POST" ? await request.json() : undefined;
    try {
      switch (url.pathname) {
        case "/provision": return json(this.provision(body));
        case "/invoke": return json(await this.invoke(body));
        case "/snapshot": return json(this.snapshot());
        case "/contract": return json(await this.contract());
        case "/meta": return json({ instance: this.instance, appExports, databaseSize: this.ctx.storage.sql.databaseSize });
        default: return json({ error: "not found" }, 404);
      }
    } catch (error) {
      return json({ error: String(error?.stack ?? error) }, 500);
    }
  }

  /** DDL statements one at a time, then provisioned rows. */
  provision({ statements, rows, extra = [] }) {
    const failures = [];
    let executed = 0;
    for (const statement of [...statements, ...extra.map((sql) => ({ type: "extra", name: "extra", sql }))]) {
      try { this.sql.exec(statement.sql); executed++; } catch (error) { failures.push({ type: statement.type, name: statement.name, bytes: statement.sql.length, error: String(error.message).slice(0, 300) }); }
    }
    let inserted = 0;
    for (const { table, rows: data } of rows ?? []) {
      for (const row of data) {
        const keys = Object.keys(row);
        try {
          this.sql.exec(`INSERT INTO "${table}" (${keys.map((k) => `"${k}"`).join(",")}) VALUES (${keys.map((_, i) => `?${i + 1}`).join(",")})`, ...keys.map((k) => row[k]));
          inserted++;
        } catch (error) { failures.push({ type: "row", name: table, error: String(error.message).slice(0, 300) }); }
      }
    }
    return { executed, inserted, failures, databaseSize: this.sql.databaseSize };
  }

  /**
   * One invocation. mode "transaction" (the design): ctx.storage.transaction
   * around the whole Reducer. mode "implicit": no transaction API at all, to
   * show what the platform does without it.
   */
  async invoke({ call, mode = "transaction", measureTurns = true }) {
    const engine = new DurableObjectEngine(this.ctx.storage);
    const { binding, argsSchema } = resolve(call.path);
    const transaction = mode === "transaction"
      ? (run) => this.ctx.storage.transaction((txn) => run(() => txn.rollback()))
      : (run) => run(() => {});
    const role = call.identity?.member?.role;
    const started = installed.Date.now();
    const outcome = await invoke({
      ...call, engine, binding, argsSchema, transaction, measureTurns,
      permissions: () => role === "owner" || role === "admin",
      schedulable: (path) => !!functions[path] && functions[path].kind !== "query",
      wallNow: () => installed.Date.now(),
    });
    return { ...outcome, serverMs: installed.Date.now() - started, instance: this.instance };
  }

  /** Every non-empty table, raw values in rowid/primary-key order. */
  snapshot() {
    const tables = this.sql.exec("SELECT name, sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name").toArray();
    const out = {};
    for (const { name, sql } of tables) {
      const order = /WITHOUT\s+ROWID/i.test(sql) ? "1" : "rowid";
      const cursor = this.sql.exec(`SELECT * FROM "${name.replaceAll('"', '""')}" ORDER BY ${order}`);
      const rows = cursor.toArray();
      if (rows.length) out[name] = rows.map((row) => Object.fromEntries(Object.entries(row).map(([k, v]) => [k, v instanceof ArrayBuffer ? `blob:${new Uint8Array(v).length}` : v])));
    }
    return out;
  }

  /** The 34 published runtime-contract vectors, twice in the same isolate (as the Rust suite does). */
  async contract() {
    const results = [];
    for (let round = 0; round < 2; round++) {
      for (const vector of vectors.cases) {
        const allowed = vector.capabilities?.networkOrigins ?? [];
        const host = async (payload) => {
          if (payload.kind === "fetch") {
            const origin = new URL(payload.request.url).origin;
            if (!allowed.includes(origin)) return { status: "denied", message: `fetch to ${origin} is outside the declared network origins` };
            return { status: "failed", message: "contract case reached an external service" };
          }
          return { status: "failed", message: "contract case reached an external service" };
        };
        const start = installed.Date.now();
        const envelope = await dispatch(contractRunner.contract, {
          kind: vector.kind, function: vector.id, now: vector.now ?? 1792324800123, intentEntropy: vector.intentEntropy,
          identity: {}, invocation: {}, capabilities: { network: true }, maxResultBytes: 8 * 1024 * 1024,
        }, vector.input ?? vector, host, vector.argsSchema ?? { kind: "any" });
        const end = installed.Date.now();
        results.push({ round, id: vector.id, kind: vector.kind, ...judge(vector, envelope, start, end) });
      }
    }
    return results;
  }
}

function judge(vector, envelope, start, end) {
  const error = envelope.status === "ok" ? undefined : envelope.message;
  if (vector.expectedError) return error?.includes(vector.expectedError) ? { pass: true, detail: error } : { pass: false, detail: error ?? `ok: ${envelope.value}` };
  if (error) return { pass: false, detail: error };
  const value = JSON.parse(envelope.value);
  if (vector.expectActionGlobals) {
    const ok = value.now >= start && value.now <= end && value.constructed >= start && value.constructed <= end && value.random >= 0 && value.random < 1 && value.bytes === 32 && value.uuid === true;
    return { pass: ok, detail: JSON.stringify({ ...value, start, end }) };
  }
  const pass = JSON.stringify(sorted(value)) === JSON.stringify(sorted(vector.expected));
  return { pass, detail: pass ? undefined : JSON.stringify({ got: value, expected: vector.expected }) };
}
const sorted = (value) => (Array.isArray(value) ? value.map(sorted) : value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((k) => [k, sorted(value[k])])) : value);

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ping") return json({ ok: true, appExports });
    const name = url.searchParams.get("tenant") ?? "tenant-s2";
    return env.TENANT.get(env.TENANT.idFromName(name)).fetch(request);
  },
};
