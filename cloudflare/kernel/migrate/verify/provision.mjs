// Provisions a fresh tenant object with an app's migrations through K02 and
// compares it with a tenant the Rust runtime provisioned from the same files
// (verify/rust-reference). Prints counts, names and timings only.
//
//   node --experimental-strip-types kernel/migrate/verify/provision.mjs <base-url> <migrations-dir> <rust-tenant.db> <object> \
//        [--kill-after-ms N] [--compare-only] [--no-wipe] [--out result.json]
//
// <base-url> runs verify/worker (wrangler dev, or the deployed gonvex-cf-dev-k02 with K02_TOKEN set).
// --kill-after-ms: aborts the object N ms after the apply starts and sends nothing else; the
// object's alarm must resume and finish the release on its own.
import { DatabaseSync } from "node:sqlite";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildReleasePlan } from "../build.ts";
import { parseMigration } from "../migrations.ts";
import { unitKey } from "../plan.ts";

const [base, dir, rustPath, object, ...rest] = process.argv.slice(2);
const option = (name) => (rest.includes(name) ? rest[rest.indexOf(name) + 1] : null);
const killAfter = option("--kill-after-ms") ? Number(option("--kill-after-ms")) : null;
const report = { object, base: base.replace(/^https?:\/\/([^.]+).*/, "$1"), steps: {} };
const log = (k, v) => {
  report.steps[k] = v;
  console.log(JSON.stringify({ [k]: v }));
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function call(path, body) {
  for (let attempt = 0; ; attempt += 1) {
    try {
      const response = await fetch(`${base}${path}`, {
        method: body === undefined ? "GET" : "POST",
        headers: { "content-type": "application/json", "x-k02-token": process.env.K02_TOKEN ?? "" },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const value = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
      if (value.error && attempt < 4 && /reset|Network connection lost|internal error|HTTP 5|restarted|aborted/i.test(value.error) && !path.endsWith("/apply")) {
        await sleep(500);
        continue;
      }
      return value;
    } catch (error) {
      if (attempt >= 4) throw error;
      await sleep(500);
    }
  }
}

// 1. Plan (outside the object).
const files = readdirSync(dir).filter((f) => /^\d{4}_.*\.sql$/.test(f)).sort();
const migrations = await Promise.all(files.map((f) => parseMigration(f, readFileSync(join(dir, f), "utf8"))));
const heap0 = process.memoryUsage().heapUsed;
const t0 = performance.now();
const plan = await buildReleasePlan(migrations);
log("plan", {
  id: plan.id,
  ms: Math.round(performance.now() - t0),
  heapMB: Math.round((process.memoryUsage().heapUsed - heap0) / 1e6),
  migrations: plan.migrations.length,
  chunks: plan.prelude.reduce((s, u) => s + u.chunks, 0) + plan.migrations.reduce((s, m) => s + m.chunks, 0),
  statements: plan.migrations.reduce((s, m) => s + m.statements, 0),
  addedObjects: plan.addedObjects.length,
  bytes: JSON.stringify(plan.chunks).length,
});

const compareOnly = rest.includes("--compare-only");
let status = { state: "done" };
if (!compareOnly) {
// 2. Upload the plan to the plan holder.
const entries = [["manifest", JSON.stringify({ ...plan, chunks: undefined })]];
for (const unit of [...plan.prelude, ...plan.migrations]) plan.chunks[unitKey(unit)].forEach((c, i) => entries.push([`${unitKey(unit)}/${i}`, JSON.stringify(c)]));
let batch = [];
let size = 0;
const t1 = performance.now();
for (const entry of entries) {
  batch.push(entry);
  size += entry[1].length;
  if (size > 4_000_000) {
    const r = await call(`/plan/${plan.id}/put`, { entries: batch });
    if (!r.ok) throw new Error(`plan upload: ${r.error}`);
    batch = [];
    size = 0;
  }
}
if (batch.length) await call(`/plan/${plan.id}/put`, { entries: batch });
log("upload", { entries: entries.length, ms: Math.round(performance.now() - t1) });

// 3. Apply and wait.
if (!rest.includes("--no-wipe")) await call(`/t/${object}/wipe`, {});
const started = Date.now();
const events = [];
let killed = false;
status = await call(`/t/${object}/apply`, { planId: plan.id });
if (status.error) throw new Error(`apply: ${status.error}`);
let restarts = 0;
let lastWoke = null;
for (;;) {
  if (killAfter !== null && !killed && Date.now() - started >= killAfter) {
    killed = true;
    await call(`/t/${object}/kill`, {});
    log("kill", { atMs: Date.now() - started });
  }
  await sleep(killed || killAfter === null ? 1000 : 200);
  const polled = await call(`/t/${object}/poll`, {});
  if (polled.error) continue;
  if (lastWoke !== null && polled.wokeAt !== lastWoke) restarts += 1;
  lastWoke = polled.wokeAt;
  events.push(...polled.events);
  status = polled.status;
  if (status.state !== "running") {
    report.databaseSize = polled.databaseSize;
    report.failures = polled.failures;
    break;
  }
  if (Date.now() - started > 30 * 60_000) throw new Error("timed out");
}
const ms = events.map((e) => e.ms).sort((a, b) => a - b);
const pct = (q) => ms[Math.min(ms.length - 1, Math.floor(q * ms.length))];
const slowest = [...events].sort((a, b) => b.ms - a.ms).slice(0, 5).map((e) => `${e.unit}#${e.chunk}:${e.statements}st:${e.ms}ms`);
log("provision", {
  state: status.state,
  error: status.error,
  wallMs: Date.now() - started,
  activeMs: status.activeMs,
  events: status.events,
  objectRestarts: restarts,
  transactionsSeen: events.length,
  transactionMs: { p50: pct(0.5), p90: pct(0.9), max: ms[ms.length - 1] },
  slowest,
  databaseSize: report.databaseSize,
});
if (status.state !== "done") {
  writeFileSync(option("--out") ?? "/dev/null", JSON.stringify(report, null, 2));
  process.exit(1);
}
}

// 4. Compare with the Rust-provisioned tenant.
const rust = new DatabaseSync(rustPath, { readOnly: true });
const q = async (sql, params) => {
  const r = await call(`/t/${object}/query`, { sql, params });
  if (r.error) throw new Error(`${sql}: ${r.error}`);
  return r.rows;
};
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const rustRows = (sql) => rust.prepare(sql).all().map((r) => ({ ...r }));

const ledgerSql = "SELECT name, checksum FROM gonvex_migrations ORDER BY name";
const kernelLedger = await q(ledgerSql);
const rustLedger = rustRows(ledgerSql);
const durations = await q("SELECT name, duration_ms FROM gonvex_migrations ORDER BY name");
log("ledger", { equal: same(kernelLedger, rustLedger), rows: kernelLedger.length, durationMs: Object.fromEntries(durations.map((r) => [r.name, r.duration_ms])) });

const tables = {
  _gonvex_sync_bootstrap: "SELECT name, digest FROM _gonvex_sync_bootstrap ORDER BY name",
  _gonvex_schema_capabilities: "SELECT * FROM _gonvex_schema_capabilities ORDER BY name",
  _gonvex_table_options: "SELECT * FROM _gonvex_table_options ORDER BY name",
  _gonvex_record_views: "SELECT * FROM _gonvex_record_views ORDER BY name",
  _gonvex_record_dependencies: "SELECT * FROM _gonvex_record_dependencies ORDER BY 1,2,3,4",
  _gonvex_id_domains: "SELECT * FROM _gonvex_id_domains ORDER BY _id",
  _gonvex_reference_catalog: "SELECT * FROM _gonvex_reference_catalog",
  members: "SELECT * FROM members ORDER BY id",
  _gonvex_sync_clock: "SELECT singleton, revision, retained_revision FROM _gonvex_sync_clock",
};
const runtimeTables = {};
for (const [name, sql] of Object.entries(tables)) {
  const k = await q(sql);
  const r = rustRows(sql);
  runtimeTables[name] = { rows: k.length, equal: same(k, r) };
  if (!same(k, r) && name === "_gonvex_sync_bootstrap") {
    const rm = new Map(r.map((x) => [x.name, x.digest]));
    const km = new Map(k.map((x) => [x.name, x.digest]));
    runtimeTables[name].differ = [...new Set([...rm.keys(), ...km.keys()])].filter((n) => rm.get(n) !== km.get(n)).slice(0, 10);
  }
}
log("runtimeTables", runtimeTables);

// sqlite_schema: names, and the SQL of every object the transform did not rewrite.
const added = new Set(plan.addedObjects.filter((o) => o.type !== "column").map((o) => o.name.toLowerCase()));
const kernelSchema = [];
for (let after = 0; ; ) {
  const page = await q("SELECT rowid AS r, type, name, tbl_name, sql FROM sqlite_schema WHERE rowid > ? ORDER BY rowid LIMIT 1500", [after]);
  if (!page.length) break;
  kernelSchema.push(...page);
  after = page[page.length - 1].r;
}
const rustSchema = rustRows("SELECT type, name, tbl_name, sql FROM sqlite_schema");
const internal = (n) => /^sqlite_/.test(n) || /^_cf_/.test(n);
const kernelByName = new Map(kernelSchema.filter((o) => !internal(o.name)).map((o) => [o.name.toLowerCase(), o]));
const rustByName = new Map(rustSchema.filter((o) => !internal(o.name)).map((o) => [o.name.toLowerCase(), o]));
const missing = [...rustByName.keys()].filter((n) => !kernelByName.has(n));
const extra = [...kernelByName.keys()].filter((n) => !rustByName.has(n) && !added.has(n));
// The transform re-emits every trigger it parses as `BEGIN\n  stmt;\nEND`, so compare with
// whitespace outside string literals collapsed as well.
const squash = (sql) => (sql ?? "").replace(/'(?:[^']|'')*'|\s+/g, (m) => (m.startsWith("'") ? m : " ")).replace(/ ?([;(),]) ?/g, "$1").trim();
let identical = 0;
let whitespaceOnly = 0;
const rewritten = {};
const rewrittenSample = [];
for (const [name, r] of rustByName) {
  const k = kernelByName.get(name);
  if (!k) continue;
  if (k.type !== r.type || k.tbl_name !== r.tbl_name) (rewritten.typeOrTable ??= []).push(name);
  else if (k.sql === r.sql) identical += 1;
  else if (squash(k.sql) === squash(r.sql)) whitespaceOnly += 1;
  else {
    const why = /gonvex_capture|gonvex_record_value/.test(r.sql ?? "") ? "T5 capture" : /reference(Bindings|Expected)_/.test(r.sql ?? "") ? `${r.type} reading a reference view (T6)` : `${r.type} (T1-T4)`;
    rewritten[why] = (rewritten[why] ?? 0) + 1;
    if (!why.startsWith("T5") && !why.includes("T6") && rewrittenSample.length < 12) rewrittenSample.push(name);
  }
}
const byType = (list) => list.reduce((m, o) => ((m[o.type] = (m[o.type] ?? 0) + 1), m), {});
log("schema", {
  kernel: byType([...kernelByName.values()]),
  rust: byType([...rustByName.values()]),
  added: added.size,
  missingFromKernel: missing.length,
  missingSample: missing.slice(0, 10),
  extraNotAdded: extra.length,
  extraSample: extra.slice(0, 10),
  identicalSql: identical,
  whitespaceOnly,
  rewritten,
  rewrittenSample,
});
const fk = await q("PRAGMA foreign_keys");
log("state", { foreignKeys: fk[0]?.foreign_keys, progress: status });
if (option("--out")) writeFileSync(option("--out"), JSON.stringify(report, null, 2) + "\n");
