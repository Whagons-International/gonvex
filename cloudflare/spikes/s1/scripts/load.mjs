// Loads the Whagons tenant schema (and optionally its data) into one Durable
// Object and times every step from the client.
//
//   node scripts/load.mjs migrations <base> <object> <prelude.sql> <migrations-dir> <tenant.db> [--raw] [--out f]
//     Runtime prelude, then each migration in one atomic request, then the
//     objects the runtime generates (taken from the tenant file).
//   node scripts/load.mjs snapshot <base> <object> <tenant.db> [--raw] [--schema-only] [--out f]
//     The tenant file's final schema and every row: tables, rows, FTS
//     rebuild, indexes, views, triggers, ANALYZE.
//
// --raw sends statements unchanged (to count what fails); the default applies
// the workarounds in sql.mjs. Never prints row values.
import { DatabaseSync } from "node:sqlite";
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { splitStatements, applyWorkarounds, quoteIdent } from "./sql.mjs";

const [mode, base, object, ...rest] = process.argv.slice(2);
const flag = (name) => rest.includes(name);
const option = (name) => (rest.includes(name) ? rest[rest.indexOf(name) + 1] : null);
const positional = rest.filter((a, i) => !a.startsWith("--") && !(i > 0 && rest[i - 1] === "--out"));
const raw = flag("--raw");
const keepGoing = raw || flag("--continue");
const MAX_REQUEST_BYTES = 8_000_000;
// DDL is slow on the platform (seconds per MB with a large schema); smaller
// requests stay under the storage-operation timeout.
const MAX_DDL_REQUEST_BYTES = Number(option("--ddl-bytes")) || 1_000_000;
const report = { mode, object, workarounds: !raw, steps: [] };

async function call(route, body = {}) {
  const started = performance.now();
  const response = await fetch(`${base}${route}?o=${encodeURIComponent(object)}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  const ms = performance.now() - started;
  let value;
  try { value = JSON.parse(text); } catch { value = { error: text.slice(0, 500) }; }
  return { ...value, status: response.status, ms };
}

function step(name, fields) {
  const entry = { step: name, ...fields };
  report.steps.push(entry);
  const { errors, ...shown } = entry;
  console.log(JSON.stringify(shown));
}

// Runs statements in requests of at most MAX_REQUEST_BYTES, not atomically.
async function execBatches(name, statements, { continueOnError = true, pragmas = [] } = {}) {
  let batch = [];
  let bytes = 0;
  let ms = 0;
  let requests = 0;
  const errors = [];
  let offset = 0;
  const send = async () => {
    if (!batch.length) return;
    const result = await call("/exec", { statements: batch, continueOnError, pragmas });
    ms += result.ms;
    requests += 1;
    if (result.error) errors.push({ index: offset, error: result.error });
    for (const e of result.errors || []) errors.push({ ...e, index: offset + e.index, head: batch[e.index].replace(/\s+/g, " ").slice(0, 120) });
    offset += batch.length;
    batch = [];
    bytes = 0;
  };
  for (const s of statements) {
    if (bytes + s.length > MAX_DDL_REQUEST_BYTES) await send();
    batch.push(s);
    bytes += s.length;
  }
  await send();
  step(name, { statements: statements.length, requests, ms: Math.round(ms), failed: errors.length, errorKinds: kinds(errors), firstErrors: errors.slice(0, 5), errors });
  return errors;
}

function kinds(errors) {
  const out = {};
  for (const e of errors) {
    const k = String(e.error).replace(/"[^"]*"|'[^']*'|\b\d+\b/g, "_").replace(/at offset _/, "").slice(0, 90);
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

const hoisted = new Map();
const transform = (statements) => (raw ? statements : statements.flatMap((s) => applyWorkarounds(s, hoisted)));

function tenantObjects(path) {
  const db = new DatabaseSync(path, { readOnly: true });
  const objects = db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY rowid").all();
  const virtual = objects.filter((o) => o.type === "table" && /^CREATE VIRTUAL/i.test(o.sql)).map((o) => o.name);
  const shadow = new Set(virtual.flatMap((v) => ["_data", "_idx", "_content", "_docsize", "_config"].map((s) => v + s)));
  const keep = objects.filter((o) => !o.name.startsWith("sqlite_") && !shadow.has(o.name));
  return { db, objects: keep, virtual };
}

await call("/reset");

if (mode === "migrations") {
  const [preludePath, dir, tenantPath] = positional;
  const prelude = splitStatements(readFileSync(preludePath, "utf8"));
  await execBatches("runtime prelude", transform(prelude), { continueOnError: false });
  const files = readdirSync(dir).filter((f) => /^\d+_.*\.sql$/.test(f)).sort();
  let total = 0;
  for (const file of files) {
    const text = readFileSync(join(dir, file), "utf8");
    const statements = transform(splitStatements(text));
    const rebuild = /^-- gonvex:foreign-key-rebuild\s*$/m.test(text);
    const bytes = statements.reduce((s, x) => s + x.length, 0);
    // One request, one transaction: a migration applies whole or not at all.
    // --chunk N splits it into transactions of N statements (not atomic; for
    // measuring what the platform's storage-operation timeout allows).
    const chunk = Number(option("--chunk")) || statements.length;
    const result = { ok: true, errors: [], ms: 0, requests: 0, maxRequestMs: 0 };
    for (let k = 0; k < statements.length; k += chunk) {
      const part = await call("/exec", {
        statements: statements.slice(k, k + chunk),
        transaction: true,
        continueOnError: keepGoing,
        pragmas: rebuild ? ["PRAGMA foreign_keys=OFF"] : [],
      });
      result.ms += part.ms;
      result.requests += 1;
      result.maxRequestMs = Math.max(result.maxRequestMs, part.ms);
      result.errors.push(...(part.errors || []).map((e) => ({ ...e, index: e.index + k })));
      if (part.error) result.error = part.error.slice(0, 300);
      if (part.ok !== true) { result.ok = false; if (!keepGoing) break; }
    }
    if (rebuild) await call("/exec", { statements: ["PRAGMA foreign_keys=ON"] });
    total += result.ms;
    const errors = (result.errors || []).map((e) => ({ ...e, head: statements[e.index]?.replace(/\s+/g, " ").slice(0, 120) }));
    step(`migration ${file}`, {
      statements: statements.length, requestBytes: bytes, ms: Math.round(result.ms), requests: result.requests, maxRequestMs: Math.round(result.maxRequestMs), ok: result.ok === true,
      foreignKeyRebuild: rebuild, error: result.error, failed: errors.length, errorKinds: kinds(errors), firstErrors: errors.slice(0, 3), errors,
    });
    if (!keepGoing && result.ok !== true) break;
  }
  report.migrationsMs = Math.round(total);
  // Objects the runtime generates after migrations (capture, record
  // dependencies, reference staging): whatever the tenant file has that the
  // migrated object lacks.
  const { objects } = tenantObjects(tenantPath);
  const present = new Map((await call("/query", { sql: "SELECT name, sql FROM sqlite_schema" })).rows.map((r) => [r.name, r.sql]));
  const missing = objects.filter((o) => !present.has(o.name) && !o.name.includes("__part"));
  const differing = objects.filter((o) => present.has(o.name) && present.get(o.name) !== o.sql && o.type !== "table");
  const order = ["table", "index", "view", "trigger"];
  for (const type of order) {
    const list = missing.filter((o) => o.type === type);
    if (list.length) await execBatches(`runtime-generated ${type}s`, transform(list.map((o) => o.sql)));
  }
  step("schema diff vs tenant file", {
    missingCreated: Object.fromEntries(order.map((t) => [t, missing.filter((o) => o.type === t).length])),
    differingSql: differing.length,
    differingNames: differing.slice(0, 10).map((o) => `${o.type}:${o.name}`),
  });
} else if (mode === "snapshot") {
  const [tenantPath] = positional;
  const { db, objects, virtual } = tenantObjects(tenantPath);
  const of = (type) => objects.filter((o) => o.type === type);
  await execBatches("create tables", transform(of("table").map((o) => o.sql)));
  if (!flag("--schema-only")) {
    // Rows, table by table, with foreign keys off (sources are consistent; the
    // check runs at the end). Rowid tables keep their rowids.
    const listed = new Map(db.prepare("SELECT name, wr FROM pragma_table_list WHERE schema='main'").all().map((r) => [r.name, r.wr]));
    let rows = 0;
    let ms = 0;
    let requests = 0;
    let bigints = 0;
    const failures = [];
    const started = performance.now();
    for (const table of of("table").filter((o) => !virtual.includes(o.name))) {
      const columns = db.prepare("SELECT name FROM pragma_table_xinfo(?) WHERE hidden IN (0)").all(table.name).map((c) => c.name);
      const rowid = listed.get(table.name) === 0;
      const names = (rowid ? ["rowid"] : []).concat(columns);
      const insert = `INSERT INTO ${quoteIdent(table.name)} (${names.map(quoteIdent).join(", ")}) VALUES (${names.map(() => "?").join(", ")})`;
      const select = db.prepare(`SELECT ${names.map((n) => (n === "rowid" ? "rowid" : quoteIdent(n))).join(", ")} FROM ${quoteIdent(table.name)}`);
      select.setReadBigInts(true);
      let batch = [];
      let bytes = 0;
      const send = async () => {
        if (!batch.length) return;
        const result = await call("/insert", { sql: insert, rows: batch, pragmas: ["PRAGMA foreign_keys=OFF"] });
        ms += result.ms;
        requests += 1;
        if (result.error) failures.push({ table: table.name, error: result.error });
        else rows += result.rows;
        batch = [];
        bytes = 0;
      };
      for (const row of select.iterate()) {
        const values = names.map((n) => {
          const v = row[n];
          if (typeof v === "bigint") {
            if (v > BigInt(Number.MAX_SAFE_INTEGER) || v < BigInt(Number.MIN_SAFE_INTEGER)) bigints += 1;
            return Number(v);
          }
          if (v instanceof Uint8Array) return { $b64: Buffer.from(v).toString("base64") };
          return v;
        });
        const size = JSON.stringify(values).length;
        if (bytes + size > MAX_REQUEST_BYTES || batch.length >= 20_000) await send();
        batch.push(values);
        bytes += size;
      }
      await send();
    }
    step("import rows", { rows, requests, requestMs: Math.round(ms), wallMs: Math.round(performance.now() - started), unsafeIntegers: bigints, failed: failures.length, failures: failures.slice(0, 5) });
    await execBatches("fts rebuild", virtual.map((v) => `INSERT INTO ${quoteIdent(v)}(${quoteIdent(v)}) VALUES ('rebuild')`));
    await execBatches("sqlite_sequence", db.prepare("SELECT name, seq FROM sqlite_sequence").all().map((r) => `INSERT INTO sqlite_sequence(name, seq) VALUES ('${r.name.replace(/'/g, "''")}', ${r.seq})`));
  }
  await execBatches("create indexes", transform(of("index").map((o) => o.sql)));
  const views = transform(of("view").map((o) => o.sql));
  const viewErrors = await execBatches("create views", views);
  // A view that read a later view fails once; run the failures again.
  if (viewErrors.length) await execBatches("create views (retry)", viewErrors.map((e) => views[e.index]).filter(Boolean));
  await execBatches("create triggers", transform(of("trigger").map((o) => o.sql)));
  await execBatches("analyze", ["ANALYZE"]);
  const verify = await call("/verify");
  if (!verify.error) {
    let mismatched = [];
    for (const table of of("table").filter((o) => !virtual.includes(o.name))) {
      const expected = db.prepare(`SELECT count(*) AS n FROM ${quoteIdent(table.name)}`).get().n;
      if (verify.counts[table.name] !== expected) mismatched.push({ table: table.name, expected, actual: verify.counts[table.name] });
    }
    step("verify", { ms: Math.round(verify.ms), tables: Object.keys(verify.counts).length, rowCountMismatches: mismatched.length, mismatched: mismatched.slice(0, 5), foreignKeyViolations: verify.foreignKeyViolations, quickCheck: verify.quickCheck, databaseSize: verify.databaseSize });
  } else step("verify", { error: verify.error });
}

const stats = await call("/stats");
step("final", { databaseSize: stats.databaseSize, schema: stats.schema, error: stats.error });
report.totalMs = Math.round(report.steps.reduce((s, x) => s + (x.ms || x.requestMs || 0), 0));
if (option("--out")) writeFileSync(option("--out"), JSON.stringify(report, null, 2) + "\n");
