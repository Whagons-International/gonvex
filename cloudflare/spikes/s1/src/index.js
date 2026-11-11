// S1 schema spike. One Durable Object per object name (`?o=`), SQLite-backed.
// Every route is a POST with a JSON body and answers JSON. Timings are taken by
// the caller: inside a Worker `Date.now()` does not advance during CPU work.
import { DurableObject } from "cloudflare:workers";

const json = (value, status = 200) =>
  new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

const describe = (error) => String(error && error.message ? error.message : error);

export class TenantSpike extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.requests = 0;
  }

  async fetch(request) {
    const route = new URL(request.url).pathname;
    const body = request.method === "POST" ? await request.json().catch(() => ({})) : {};
    this.requests += 1;
    try {
      const handler = ROUTES[route];
      if (!handler) return json({ error: `no route ${route}` }, 404);
      return json(await handler(this, body));
    } catch (error) {
      return json({ error: describe(error) }, 500);
    }
  }
}

const ROUTES = {
  // Is this the first request since the object (isolate) started?
  "/wake": (self, body) => {
    const fresh = self.requests === 1;
    const out = { fresh };
    // The first statement makes SQLite read and parse the whole schema.
    out.schema = self.sql.exec("SELECT type, count(*) AS n FROM sqlite_schema GROUP BY type").toArray();
    if (body.sql) {
      const cursor = self.sql.exec(body.sql, ...(body.params || []));
      out.rows = cursor.toArray().length;
      out.rowsRead = cursor.rowsRead;
    }
    return out;
  },

  // Restart the object: the next request runs a fresh constructor and reopens SQLite.
  "/abort": (self) => {
    self.ctx.abort("s1 cold-wake measurement");
    return { aborted: true };
  },

  // Holds {mb} more MiB of JS memory (touched, so it is resident), to find the
  // isolate's memory headroom with and without the tenant schema loaded.
  "/alloc": (self, body) => {
    self.held ||= [];
    for (let i = 0; i < body.mb; i += 1) self.held.push(new Uint8Array(1 << 20).fill(i & 255));
    return { heldMiB: self.held.length };
  },

  "/reset": async (self) => {
    await self.ctx.storage.deleteAll();
    return { reset: true };
  },

  "/stats": (self) => ({
    databaseSize: self.sql.databaseSize,
    schema: self.sql.exec("SELECT type, count(*) AS n FROM sqlite_schema GROUP BY type").toArray(),
  }),

  // {statements: string[], transaction?: bool, continueOnError?: bool}
  // Runs each string with one exec() call. A string may hold several statements.
  "/exec": (self, body) => {
    for (const pragma of body.pragmas || []) self.sql.exec(pragma);
    const errors = [];
    let rowsWritten = 0;
    const run = () => {
      body.statements.forEach((statement, index) => {
        try {
          const cursor = self.sql.exec(statement);
          for (const _ of cursor) { /* drain */ }
          rowsWritten += cursor.rowsWritten;
        } catch (error) {
          errors.push({ index, bytes: statement.length, error: describe(error) });
          if (!body.continueOnError) throw error;
        }
      });
    };
    if (body.transaction) {
      try {
        self.ctx.storage.transactionSync(run);
      } catch (error) {
        return { ok: false, errors, rowsWritten };
      }
    } else {
      try { run(); } catch (error) { return { ok: false, errors, rowsWritten }; }
    }
    return { ok: errors.length === 0, errors, rowsWritten, statements: body.statements.length };
  },

  // Row count of every table, and the integrity checks the platform allows.
  "/verify": (self) => {
    const counts = {};
    const tables = self.sql.exec("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' AND sql NOT LIKE 'CREATE VIRTUAL%'").toArray();
    for (const { name } of tables) counts[name] = self.sql.exec(`SELECT count(*) AS n FROM "${name.replace(/"/g, '""')}"`).one().n;
    const foreignKeyViolations = self.sql.exec("SELECT count(*) AS n FROM pragma_foreign_key_check").one().n;
    const quickCheck = self.sql.exec("PRAGMA quick_check").toArray().map((r) => Object.values(r)[0]);
    return { counts, foreignKeyViolations, quickCheck, databaseSize: self.sql.databaseSize };
  },

  // {sql, params?}
  "/query": (self, body) => {
    const cursor = self.sql.exec(body.sql, ...(body.params || []));
    const rows = cursor.toArray();
    return { rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
  },

  // {sql, rows: any[][]} inserts every row with one prepared shape, in one transaction.
  "/insert": (self, body) => {
    for (const pragma of body.pragmas || []) self.sql.exec(pragma);
    let rowsWritten = 0;
    self.ctx.storage.transactionSync(() => {
      for (const row of body.rows) {
        const cursor = self.sql.exec(body.sql, ...row.map(decodeValue));
        for (const _ of cursor) { /* drain */ }
        rowsWritten += cursor.rowsWritten;
      }
    });
    return { rows: body.rows.length, rowsWritten };
  },
};

// Blobs travel as {"$b64": "..."}.
function decodeValue(value) {
  if (value && typeof value === "object" && typeof value.$b64 === "string") {
    const bytes = atob(value.$b64);
    const out = new Uint8Array(bytes.length);
    for (let i = 0; i < bytes.length; i += 1) out[i] = bytes.charCodeAt(i);
    return out.buffer;
  }
  return value;
}

export default {
  async fetch(request, env) {
    // The deployed spike holds a copy of tenant data: every request must carry
    // the secret set with `wrangler secret put S1_TOKEN`.
    if (env.S1_TOKEN && request.headers.get("x-s1-token") !== env.S1_TOKEN) return new Response("forbidden", { status: 403 });
    const url = new URL(request.url);
    const name = url.searchParams.get("o") || "default";
    try {
      return await env.TENANT.get(env.TENANT.idFromName(name)).fetch(request);
    } catch (error) {
      // Platform-side failures (CPU or memory limit, object reset) surface here.
      return json({ error: describe(error), platform: true }, 502);
    }
  },
};
