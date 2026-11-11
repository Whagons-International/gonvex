// Platform probe for S2: what Durable Object SQLite allows. Each path is one
// probe (see docs/cloudflare/spikes/S2-execution.md, "Platform facts"):
//   /all          exec of BEGIN/SAVEPOINT/..., parameter binding, value types,
//                 transactionSync / transaction() rollback and nesting,
//                 imperative savepoints over nested transaction(), implicit mode
//   /slow + /poke an async transaction held across a real 300 ms timer while a
//                 second request arrives (input gate)
//   /throw-txn    a rejected transaction() does not reset the object
//   /fk-setup, /fk-async, /fk-sync, /fk-implicit, /fk-check, /fk-guarded, /fk-ok
//                 deferred foreign keys at the implicit commit (object reset) and
//                 the in-transaction pragma_foreign_key_check guard
//   /fk-scale     cost of pragma_foreign_key_check at 100k and 1M child rows
//   /limits       compound SELECT terms, expression depth, statement length
//   /instance     constructor count (detects object resets)
// Run: workerd serve --experimental probe.capnp (from this directory).
import { DurableObject } from "cloudflare:workers";

const rows = (sql) => sql.exec("SELECT v FROM t ORDER BY v").toArray().map((r) => r.v);
const attempt = (fn) => { try { const v = fn(); return { ok: true, v }; } catch (e) { return { ok: false, error: `${e.name}: ${e.message}` }; } };
const attemptAsync = async (fn) => { try { const v = await fn(); return { ok: true, v }; } catch (e) { return { ok: false, error: `${e.name}: ${e.message}` }; } };

export class Probe extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.sql = ctx.storage.sql; globalThis.__constructed = (globalThis.__constructed ?? 0) + 1; this.instance = globalThis.__constructed; }
  reset() { this.sql.exec("DROP TABLE IF EXISTS t"); this.sql.exec("CREATE TABLE t(v TEXT)"); }
  async fetch(request) {
    const url = new URL(request.url);
    const name = url.pathname.slice(1);
    if (name === "slow") {
      // Holds an async transaction open across a real timer for 300 ms.
      this.reset();
      const order = [];
      const result = await attemptAsync(() => this.ctx.storage.transaction(async () => {
        this.sql.exec("INSERT INTO t VALUES ('slow-before')");
        order.push("slow-before-wait");
        await new Promise((resolve) => setTimeout(resolve, 300));
        order.push("slow-after-wait");
        this.sql.exec("INSERT INTO t VALUES ('slow-after')");
        throw new Error("boom after real I/O");
      }));
      return Response.json({ result, order, rows: rows(this.sql), seen: this.seen ?? null });
    }
    if (name === "instance") return Response.json({ instance: this.instance, constructed: globalThis.__constructed });
    if (name === "clock") {
      const t0 = Date.now();
      await crypto.subtle.digest("SHA-256", new Uint8Array(1 << 20));
      const t1 = Date.now();
      await this.ctx.storage.transaction(async () => { this.sql.exec("SELECT 1"); await Promise.resolve(); });
      const t2 = Date.now();
      await new Promise((r) => setTimeout(r, 50));
      const t3 = Date.now();
      return Response.json({ digestAdvance: t1 - t0, txnAdvance: t2 - t1, timerAdvance: t3 - t2 });
    }
    if (name === "throw-txn") {
      const r = await attemptAsync(() => this.ctx.storage.transaction(async () => { this.sql.exec("SELECT 1"); await Promise.resolve(); throw new Error("reducer rejected"); }));
      return Response.json({ r, instance: this.instance });
    }
    if (name === "fk-setup") {
      this.sql.exec("DROP TABLE IF EXISTS child; DROP TABLE IF EXISTS parent; CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(id TEXT PRIMARY KEY, parent TEXT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
      return Response.json({ ok: true, instance: this.instance });
    }
    if (name === "fk-async") {
      const r = await attemptAsync(() => this.ctx.storage.transaction(async () => { this.sql.exec("INSERT INTO child VALUES ('c1','missing')"); await Promise.resolve(); return "callback returned"; }));
      return Response.json({ r, instance: this.instance, children: attempt(() => this.sql.exec("SELECT * FROM child").toArray()) });
    }
    if (name === "fk-sync") {
      const r = attempt(() => this.ctx.storage.transactionSync(() => { this.sql.exec("INSERT INTO child VALUES ('c2','missing')"); return "callback returned"; }));
      return Response.json({ r, instance: this.instance, children: attempt(() => this.sql.exec("SELECT * FROM child").toArray()) });
    }
    if (name === "fk-guarded") {
      const r = await attemptAsync(() => this.ctx.storage.transaction(async () => {
        this.sql.exec("INSERT INTO child VALUES ('c4','missing')");
        await Promise.resolve();
        const violations = this.sql.exec("SELECT \"table\", parent FROM pragma_foreign_key_check('child') LIMIT 1").toArray();
        if (violations.length) throw new Error(`FOREIGN KEY constraint failed: ${JSON.stringify(violations[0])}`);
        return "committed";
      }));
      return Response.json({ r, instance: this.instance, children: attempt(() => this.sql.exec("SELECT * FROM child").toArray()) });
    }
    if (name === "fk-pragma-off") {
      return Response.json({ off: attempt(() => this.sql.exec("PRAGMA foreign_keys=OFF").toArray()), after: attempt(() => this.sql.exec("PRAGMA foreign_keys").toArray()), on: attempt(() => this.sql.exec("PRAGMA foreign_keys=ON").toArray()) });
    }
    if (name === "fk-ok") {
      const r = await attemptAsync(() => this.ctx.storage.transaction(async () => { this.sql.exec("INSERT INTO child VALUES ('c5','p1')"); await Promise.resolve(); this.sql.exec("INSERT INTO parent VALUES ('p1')"); return "child before parent committed"; }));
      return Response.json({ r, instance: this.instance, children: attempt(() => this.sql.exec("SELECT * FROM child").toArray()) });
    }
    if (name === "fk-check") {
      return Response.json({ instance: this.instance, children: attempt(() => this.sql.exec("SELECT * FROM child").toArray()), check: attempt(() => this.sql.exec("SELECT * FROM pragma_foreign_key_check").toArray()) });
    }
    if (name === "fk-implicit") {
      this.sql.exec("INSERT INTO child VALUES ('c3','missing')");
      return Response.json({ wrote: true, instance: this.instance });
    }
    if (name === "fk-scale") {
      const out = {};
      this.sql.exec("DROP TABLE IF EXISTS kid; DROP TABLE IF EXISTS par; CREATE TABLE par(id INTEGER PRIMARY KEY); CREATE TABLE kid(id INTEGER PRIMARY KEY, p INTEGER REFERENCES par(id) DEFERRABLE INITIALLY DEFERRED)");
      for (const n of [100000, 1000000]) {
        this.ctx.storage.transactionSync(() => {
          this.sql.exec("DELETE FROM kid"); this.sql.exec("DELETE FROM par");
          this.sql.exec("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < ?) INSERT INTO par SELECT x FROM c", n);
          this.sql.exec("INSERT INTO kid SELECT id, id FROM par");
        });
        await new Promise((r) => setTimeout(r, 10));
        const samples = [];
        for (let i = 0; i < 5; i++) {
          const t0 = performance.now();
          const c = this.sql.exec("SELECT * FROM pragma_foreign_key_check LIMIT 1"); c.toArray();
          await new Promise((r) => setTimeout(r, 1));
          samples.push(performance.now() - t0);
          out[`rowsRead${n}`] = c.rowsRead;
        }
        out[`ms${n}`] = samples.map((v) => Math.round(v));
      }
      return Response.json(out);
    }
    if (name === "limits") {
      const compound = (n) => attempt(() => this.sql.exec(Array.from({ length: n }, (_, i) => `SELECT ${i}`).join(" UNION ALL ")).toArray().length);
      const depth = (n) => attempt(() => this.sql.exec(`SELECT ${"(".repeat(n)}1${")".repeat(n)} + ${Array.from({ length: n }, () => "1").join(" + ")}`).toArray().length);
      const length = (n) => attempt(() => this.sql.exec(`SELECT '${"x".repeat(n)}'`).toArray().length);
      const out = {};
      for (const n of [5, 6, 7, 8, 9]) out[`compound${n}`] = compound(n);
      for (const n of [50, 99, 100, 101, 999]) out[`depth${n}`] = depth(n);
      for (const n of [99000, 100100]) out[`length${n}`] = length(n);
      return Response.json(out);
    }
    if (name === "poke") {
      // Arrives while "slow" waits; records what it can see.
      this.seen = attempt(() => rows(this.sql));
      return Response.json({ seen: this.seen });
    }
    const out = {};
    const s = this.sql;
    out.version = attempt(() => s.exec("SELECT sqlite_version() AS v").one().v);
    out.foreignKeys = attempt(() => s.exec("PRAGMA foreign_keys").toArray());
    out.jsonFunctions = attempt(() => s.exec("SELECT json_extract('{\"a\":1}', '$.a') AS a, unixepoch('subsec') AS t").toArray());
    out.regexp = attempt(() => s.exec("SELECT 'abc' REGEXP 'b' AS m").toArray());
    out.pragmaTableInfo = attempt(() => { s.exec("CREATE TABLE IF NOT EXISTS pti(a INTEGER PRIMARY KEY, b JSON, c BOOLEAN DEFAULT 0)"); return s.exec("SELECT name, type, pk, dflt_value IS NOT NULL AS has_default FROM pragma_table_info('pti')").toArray(); });
    out.sqliteSchema = attempt(() => s.exec("SELECT count(*) AS n FROM sqlite_schema").one());
    out.explain = attempt(() => s.exec("EXPLAIN SELECT 1").toArray().length);
    out.deferFk = attempt(() => s.exec("PRAGMA defer_foreign_keys=ON").toArray());
    for (const stmt of ["BEGIN", "SAVEPOINT a", "RELEASE a", "ROLLBACK TO a", "COMMIT", "ROLLBACK"]) out[`exec ${stmt}`] = attempt(() => s.exec(stmt).toArray());

    this.reset();
    out.positional = attempt(() => s.exec("SELECT ?2 AS b, ?1 AS a, ?2 AS b2", "one", "two").toArray());
    out.dollarNamed = attempt(() => s.exec("SELECT $2 AS b, $1 AS a", "one", "two").toArray());
    out.cursorCounters = attempt(() => { const c = s.exec("INSERT INTO t VALUES ('x'),('y') RETURNING v"); const r = c.toArray(); return { r, rowsRead: c.rowsRead, rowsWritten: c.rowsWritten, columnNames: c.columnNames }; });
    out.rawTypes = attempt(() => s.exec("SELECT 1 AS i, 1.5 AS f, 'a' AS s, x'00ff' AS b, NULL AS n, 9007199254740993 AS big").raw().toArray().map((r) => r.map((v) => [typeof v, v instanceof ArrayBuffer ? "ArrayBuffer" : String(v)])));

    this.reset();
    out.syncRollback = attempt(() => this.ctx.storage.transactionSync(() => { s.exec("INSERT INTO t VALUES ('a')"); throw new Error("x"); }));
    out.syncRollbackRows = rows(s);

    this.reset();
    out.syncNested = attempt(() => this.ctx.storage.transactionSync(() => {
      s.exec("INSERT INTO t VALUES ('outer')");
      const inner = attempt(() => this.ctx.storage.transactionSync(() => { s.exec("INSERT INTO t VALUES ('inner')"); throw new Error("inner"); }));
      s.exec("INSERT INTO t VALUES ('outer2')");
      return inner;
    }));
    out.syncNestedRows = rows(s);

    this.reset();
    let crossedMacrotask = false;
    setTimeout(() => { crossedMacrotask = true; }, 0);
    out.asyncRollback = await attemptAsync(() => this.ctx.storage.transaction(async () => {
      s.exec("INSERT INTO t VALUES ('a')");
      await Promise.resolve();
      const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("x"));
      s.exec("INSERT INTO t VALUES ('b')");
      throw new Error(`fail after digest(${digest.byteLength}); crossedMacrotask=${crossedMacrotask}`);
    }));
    out.asyncRollbackRows = rows(s);

    this.reset();
    out.asyncNested = await attemptAsync(() => this.ctx.storage.transaction(async () => {
      s.exec("INSERT INTO t VALUES ('outer')");
      const inner = await attemptAsync(() => this.ctx.storage.transaction(async () => { s.exec("INSERT INTO t VALUES ('inner')"); await Promise.resolve(); throw new Error("inner"); }));
      s.exec("INSERT INTO t VALUES ('outer2')");
      return inner;
    }));
    out.asyncNestedRows = rows(s);

    this.reset();
    out.syncInsideAsync = await attemptAsync(() => this.ctx.storage.transaction(async () => {
      s.exec("INSERT INTO t VALUES ('outer')");
      await Promise.resolve();
      const inner = attempt(() => this.ctx.storage.transactionSync(() => { s.exec("INSERT INTO t VALUES ('inner')"); throw new Error("inner"); }));
      s.exec("INSERT INTO t VALUES ('outer2')");
      return inner;
    }));
    out.syncInsideAsyncRows = rows(s);

    // Imperative savepoint emulation: SAVEPOINT = open a nested async
    // transaction whose callback waits for RELEASE / ROLLBACK TO.
    this.reset();
    out.imperativeSavepoint = await attemptAsync(() => this.ctx.storage.transaction(async () => {
      s.exec("INSERT INTO t VALUES ('before')");
      let settle;
      const gate = new Promise((resolve, reject) => { settle = { resolve, reject }; });
      let opened;
      const openedP = new Promise((r) => { opened = r; });
      const nested = this.ctx.storage.transaction(async () => { opened(); await gate; });
      nested.catch(() => {});
      await openedP;
      s.exec("INSERT INTO t VALUES ('inside-savepoint')");
      settle.reject(new Error("ROLLBACK TO"));
      const nestedResult = await attemptAsync(() => nested);
      s.exec("INSERT INTO t VALUES ('after')");
      return nestedResult;
    }));
    out.imperativeSavepointRows = rows(s);

    // Same, but with txn.rollback() instead of throwing.
    this.reset();
    out.txnRollbackApi = await attemptAsync(() => this.ctx.storage.transaction(async (txn) => {
      s.exec("INSERT INTO t VALUES ('a')");
      txn.rollback();
      return "returned after rollback()";
    }));
    out.txnRollbackApiRows = rows(s);

    // Failed statement inside a transaction: is the transaction still usable?
    this.reset();
    out.errorThenContinue = await attemptAsync(() => this.ctx.storage.transaction(async () => {
      s.exec("INSERT INTO t VALUES ('a')");
      const failed = attempt(() => s.exec("INSERT INTO missing VALUES (1)"));
      s.exec("INSERT INTO t VALUES ('b')");
      return failed;
    }));
    out.errorThenContinueRows = rows(s);

    // Implicit transaction: no transaction API, throw after writes.
    this.reset();
    out.implicit = await attemptAsync(async () => { s.exec("INSERT INTO t VALUES ('implicit')"); await Promise.resolve(); throw new Error("thrown without transaction API"); });
    out.implicitRows = rows(s);
    out.crossedMacrotaskAtEnd = crossedMacrotask;
    return Response.json(out, { headers: { "content-type": "application/json" } });
  }
}

export default {
  async fetch(request, env) {
    const id = env.PROBE.idFromName("probe");
    return env.PROBE.get(id).fetch(request);
  },
};
