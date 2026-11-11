// K02 verification Worker: a tenant object that runs TenantMigrator with K03's
// capture hooks, and a plan-holder object that serves release plans to it (the
// stand-in for K19's plan storage). Driven by verify/provision.mjs.
//
// Plain JS on purpose: it imports K03's kernel/capture, which the typecheck of
// this package must not depend on before K03 merges.
import { DurableObject } from "cloudflare:workers";
import { applyInstallUnit, directSql, finalize, planChangeFeedInstall } from "../../../capture/index.ts";
import { TenantMigrator } from "../../migrator.ts";

const capture = { directSql, finalize, planChangeFeedInstall, applyInstallUnit };

/** One object per plan id: `manifest` and `<unit>/<index>` -> JSON. */
export class K02Plans extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS plan (key TEXT PRIMARY KEY, json TEXT NOT NULL)");
  }
  put(entries) {
    this.ctx.storage.transactionSync(() => {
      for (const [key, json] of entries) this.ctx.storage.sql.exec("INSERT OR REPLACE INTO plan VALUES (?, ?)", key, json);
    });
    return entries.length;
  }
  get(key) {
    return this.ctx.storage.sql.exec("SELECT json FROM plan WHERE key = ?", key).toArray()[0]?.json ?? null;
  }
}

class HolderStore {
  constructor(env) {
    this.env = env;
  }
  async manifest(id) {
    const json = await this.env.PLANS.getByName(id).get("manifest");
    return json ? JSON.parse(json) : null;
  }
  async chunk(id, unit, index) {
    const json = await this.env.PLANS.getByName(id).get(`${unit}/${index}`);
    if (!json) throw new Error(`plan ${id}: no chunk ${index} of ${unit}`);
    return JSON.parse(json);
  }
}

export class K02Tenant extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.events = [];
    this.failures = [];
    this.wokeAt = Date.now();
    const budget = env.BUDGET ? JSON.parse(env.BUDGET) : undefined;
    this.migrator = new TenantMigrator({
      storage: ctx.storage,
      plans: new HolderStore(env),
      capture,
      abort: (reason) => ctx.abort(reason),
      budget,
      observe: (e) => this.events.push(e),
      reportFailure: (f) => void this.failures.push(f),
    });
  }
  async alarm() {
    await this.migrator.alarm();
  }
  async apply(planId) {
    return this.migrator.applyRelease(planId);
  }
  /** Status plus the transaction events seen since the last call (lost if the object restarted). */
  poll() {
    const events = this.events;
    this.events = [];
    return { status: this.migrator.status(), events, failures: this.failures, wokeAt: this.wokeAt, databaseSize: this.ctx.storage.sql.databaseSize };
  }
  query(sql, params = []) {
    return this.ctx.storage.sql.exec(sql, ...params).toArray();
  }
  kill() {
    this.ctx.abort("k02 verify: kill");
  }
  async wipe() {
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
    this.ctx.abort("k02 verify: wiped");
  }
}

const json = (body, status = 200) => Response.json(body, { status });

export default {
  async fetch(request, env) {
    if (env.TOKEN && request.headers.get("x-k02-token") !== env.TOKEN) return json({ error: "forbidden" }, 403);
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);
    try {
      const body = request.method === "POST" ? await request.json() : {};
      if (parts[0] === "plan" && parts[2] === "put") return json({ ok: true, n: await env.PLANS.getByName(parts[1]).put(body.entries) });
      if (parts[0] === "t" && parts[1]) {
        const tenant = env.TENANT.getByName(parts[1]);
        switch (parts[2]) {
          case "apply":
            return json(await tenant.apply(body.planId));
          case "poll":
            return json(await tenant.poll());
          case "query":
            return json({ rows: await tenant.query(body.sql, body.params ?? []) });
          case "kill":
            await tenant.kill().catch(() => undefined);
            return json({ ok: true });
          case "wipe":
            await tenant.wipe().catch(() => undefined);
            return json({ ok: true });
        }
      }
      return json({ error: "not found" }, 404);
    } catch (error) {
      return json({ error: String(error?.message ?? error) }, 500);
    }
  },
};
