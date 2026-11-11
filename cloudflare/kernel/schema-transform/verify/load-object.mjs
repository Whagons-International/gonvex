// Loads transformed DDL into a Durable Object and checks it there, through the
// S1 spike Worker's routes (cloudflare/spikes/s1: /reset, /exec, /query).
//
//   node verify/load-object.mjs migrations <base> <object> <migrations-dir> <prelude.sql> <tenant.db> [--out f]
//     Fresh object: the runtime prelude and every migration, transformed, one
//     storage transaction per T7 chunk; then the objects the runtime
//     generates (the tenant file's objects that migrations do not create),
//     through transform.runtime with the file's record catalog.
//   node verify/load-object.mjs compile <base> <object> [--out f]
//     Prepares every table's INSERT, UPDATE and DELETE (WHERE 0: no row is
//     touched, but every trigger program they could fire compiles) and selects
//     from every view. Reports failures by error.
//
// Requests carry `x-s1-token: $S1_TOKEN` for the deployed Worker. Prints
// counts, names and timings only.
import { DatabaseSync } from "node:sqlite";
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SchemaTransform, ADDED_PREFIX } from "../index.ts";

const [mode, base, object, ...rest] = process.argv.slice(2);
const option = (name) => (rest.includes(name) ? rest[rest.indexOf(name) + 1] : null);
const positional = rest.filter((a, i) => !a.startsWith("--") && !(i > 0 && rest[i - 1].startsWith("--")));
const report = { mode, object, steps: [] };
const q = (name) => `"${String(name).replace(/"/g, '""')}"`;

async function call(route, body = {}) {
  const started = performance.now();
  for (let attempt = 0; ; attempt += 1) {
    const response = await fetch(`${base}${route}?o=${encodeURIComponent(object)}`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" },
      body: JSON.stringify(body),
    });
    const text = await response.text();
    let value;
    try {
      value = JSON.parse(text);
    } catch {
      value = { error: text.slice(0, 300) };
    }
    // A reset object (platform) answers 500 once; retry only idempotent reads.
    if (value.error && /reset|Network connection lost|internal error/i.test(value.error) && attempt < 2 && route === "/query") continue;
    return { ...value, status: response.status, ms: performance.now() - started };
  }
}
const step = (name, fields) => {
  report.steps.push({ step: name, ...fields });
  console.log(JSON.stringify({ step: name, ...fields }));
};

if (mode === "migrations") {
  const [dir, preludePath, tenantPath] = positional;
  await call("/reset");
  const transform = new SchemaTransform();
  const inputs = [{ name: "runtime-prelude", sql: readFileSync(preludePath, "utf8") }].concat(
    readdirSync(dir).filter((f) => /^\d+_.*\.sql$/.test(f)).sort().map((f) => ({ name: f, sql: readFileSync(join(dir, f), "utf8") })),
  );
  let total = 0;
  let failures = 0;
  for (const input of inputs) {
    const out = transform.migration(input.name, input.sql);
    let ms = 0;
    let maxMs = 0;
    const errors = [];
    for (const chunk of out.chunks) {
      const r = await call("/exec", { statements: chunk, transaction: true, pragmas: out.foreignKeyRebuild ? ["PRAGMA foreign_keys=OFF"] : [] });
      ms += r.ms;
      maxMs = Math.max(maxMs, r.ms);
      if (r.ok !== true) {
        errors.push(...(r.errors ?? [{ error: r.error }]).map((e) => ({ error: String(e.error).slice(0, 200), head: e.index === undefined ? null : chunk[e.index]?.replace(/\s+/g, " ").slice(0, 100) })));
        break;
      }
    }
    if (out.foreignKeyRebuild) {
      await call("/exec", { statements: ["PRAGMA foreign_keys=ON"] });
      const fk = await call("/query", { sql: "SELECT count(*) AS n FROM pragma_foreign_key_check" });
      if (fk.rows?.[0]?.n) errors.push({ error: `${fk.rows[0].n} foreign key violations` });
    }
    total += ms;
    failures += errors.length;
    step(`migration ${input.name}`, { statements: out.statements.length, chunks: out.chunks.length, ms: Math.round(ms), maxChunkMs: Math.round(maxMs), ok: errors.length === 0, errors: errors.slice(0, 3) });
    if (errors.length) break;
  }
  report.migrationsMs = Math.round(total);
  // Runtime-generated objects: those of the tenant file that migrations did not create.
  const db = new DatabaseSync(tenantPath, { readOnly: true });
  const records = db.prepare("SELECT name, storage_table, key_column, schema FROM _gonvex_record_views").all();
  const present = new Set((await call("/query", { sql: "SELECT name FROM sqlite_schema" })).rows.map((r) => r.name.toLowerCase()));
  const order = ["table", "index", "view", "trigger"];
  const missing = db.prepare("SELECT type, name, sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY rowid").all()
    .filter((o) => !present.has(o.name.toLowerCase()) && !o.name.startsWith("sqlite_") && !/_fts_(data|idx|content|docsize|config)$/.test(o.name))
    .sort((a, b) => order.indexOf(a.type) - order.indexOf(b.type));
  db.close();
  const runtime = transform.runtime("runtime-generated", missing.map((o) => o.sql).join(";\n") + ";", { records });
  let ms = 0;
  const errors = [];
  for (const chunk of runtime.chunks) {
    const r = await call("/exec", { statements: chunk, transaction: true });
    ms += r.ms;
    if (r.ok !== true) errors.push(...(r.errors ?? [{ error: r.error }]).map((e) => String(e.error).slice(0, 200)));
  }
  failures += errors.length;
  step("runtime-generated objects", { objects: missing.length, byType: Object.fromEntries(order.map((t) => [t, missing.filter((o) => o.type === t).length])), statements: runtime.statements.length, ms: Math.round(ms), failed: errors.length, errors: errors.slice(0, 3) });
  const stats = await call("/stats");
  step("final", { totalMs: Math.round(total + ms), failures, schema: stats.schema, databaseSize: stats.databaseSize, added: transform.addedObjects().length });
} else if (mode === "compile") {
  const tables = (await call("/query", { sql: "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite%' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT GLOB '*_fts_*' ORDER BY name" })).rows.map((r) => r.name);
  const failures = {};
  let compiled = 0;
  const started = performance.now();
  for (const table of tables) {
    const columns = (await call("/query", { sql: "SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0", params: [table] })).rows.map((r) => q(r.name));
    const t = q(table);
    const statements = {
      insert: `INSERT INTO ${t} (${columns.join(", ")}) SELECT ${columns.join(", ")} FROM ${t} WHERE 0`,
      update: `UPDATE ${t} SET ${columns.map((c) => `${c} = ${c}`).join(", ")} WHERE 0`,
      delete: `DELETE FROM ${t} WHERE 0`,
    };
    for (const [op, sql] of Object.entries(statements)) {
      const r = await call("/exec", { statements: [sql], transaction: true });
      compiled += 1;
      if (r.ok !== true) {
        const error = String(r.errors?.[0]?.error ?? r.error).replace(/: \S+$/, "").slice(0, 100);
        (failures[error] ||= []).push(`${op} ${table}`);
      }
    }
  }
  const writeFailures = Object.values(failures).reduce((s, l) => s + l.length, 0);
  step("write statements", { tables: tables.length, statements: compiled, failing: writeFailures, ms: Math.round(performance.now() - started), byError: Object.fromEntries(Object.entries(failures).map(([k, v]) => [k, { count: v.length, first: v.slice(0, 6) }])) });
  const views = (await call("/query", { sql: "SELECT name FROM sqlite_schema WHERE type='view' ORDER BY name" })).rows.map((r) => r.name);
  const viewFailures = {};
  for (const view of views) {
    // `expected` views read scratch rows only during a fill; selecting them is still a compile check.
    const r = await call("/query", { sql: `SELECT count(*) AS n FROM (SELECT * FROM ${q(view)} LIMIT 1)` });
    if (r.error) {
      const error = String(r.error).replace(/: \S+$/, "").slice(0, 100);
      (viewFailures[error] ||= []).push(view);
    }
  }
  const failedViews = Object.values(viewFailures).flat();
  step("views", {
    views: views.length, failing: failedViews.length, addedFailing: failedViews.filter((v) => v.startsWith(ADDED_PREFIX)).length,
    byError: Object.fromEntries(Object.entries(viewFailures).map(([k, v]) => [k, { count: v.length, first: v.slice(0, 6) }])),
  });
} else {
  console.error("usage: load-object.mjs migrations|compile ...");
  process.exit(2);
}
if (option("--out")) writeFileSync(option("--out"), JSON.stringify(report, null, 2) + "\n");
