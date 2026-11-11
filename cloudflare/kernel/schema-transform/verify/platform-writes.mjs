// Write cost on an object: real record writes with T5's flush, one storage
// transaction each, timed from the client (minus a measured round trip).
//
//   S1_TOKEN=... node verify/platform-writes.mjs <base> <object> <tenant.db> [--records tasks,templates] [--n 5] [--out f]
//
// Per record: `touch` updates a column no reference trigger watches;
// `reference` sets the watched columns of row A to row B's values and back.
import { DatabaseSync } from "node:sqlite";
import { writeFileSync } from "node:fs";
import { recordFlushSql, recordView, RECORD_PENDING_CLEANUP_SQL } from "../index.ts";

const [base, object, tenant, ...rest] = process.argv.slice(2);
const option = (name, d) => (rest.includes(name) ? rest[rest.indexOf(name) + 1] : d);
const names = option("--records", "tasks,templates,workplanItems,approvals,workspaces,goals").split(",");
const n = Number(option("--n", "5"));
const q = (x) => `"${String(x).replace(/"/g, '""')}"`;
const db = new DatabaseSync(tenant, { readOnly: true });
const records = new Map(db.prepare("SELECT name, storage_table, key_column, schema FROM _gonvex_record_views").all().map((r) => [r.name, recordView(r)]));
const schema = db.prepare("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name LIKE 'referenceBindings_%_source_update'").all();
async function call(route, body) {
  const t = performance.now();
  const r = await fetch(`${base}${route}?o=${encodeURIComponent(object)}`, { method: "POST", headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) });
  const v = await r.json().catch(() => ({ error: "bad json" }));
  return { ...v, ms: performance.now() - t };
}
const rtt = [];
for (let k = 0; k < 5; k += 1) rtt.push((await call("/query", { sql: "SELECT 1" })).ms);
const base0 = rtt.sort((a, b) => a - b)[2];
const out = { object, roundTripMs: Math.round(base0), records: [] };
for (const name of names) {
  const rec = records.get(name);
  if (!rec) continue;
  const table = rec.storageTable;
  const key = rec.keyColumn;
  const keys = db.prepare(`SELECT ${q(key)} k FROM ${q(table)} ORDER BY ${q(key)} LIMIT ${n + 1}`).all().map((r) => r.k);
  if (keys.length < 2) continue;
  const watchedSql = schema.find((t) => t.sql.includes(`ON ${q(table)}`));
  const watched = watchedSql ? [...watchedSql.sql.match(/AFTER UPDATE OF (.+?) ON/)[1].matchAll(/"([^"]+)"/g)].map((m) => m[1]) : [];
  const cols = db.prepare(`SELECT name FROM pragma_table_info(?)`).all(table).map((r) => r.name);
  const touchCol = cols.find((c) => c !== key && !watched.includes(c) && /updated/i.test(c)) ?? cols.find((c) => c !== key && !watched.includes(c));
  const flush = [recordFlushSql(rec), RECORD_PENDING_CLEANUP_SQL];
  const run = async (statements) => {
    const r = await call("/exec", { statements: [...statements, ...flush], transaction: true });
    return { ms: r.ms - base0, rowsWritten: r.rowsWritten, ok: r.ok === true, error: r.ok ? undefined : String(r.errors?.[0]?.error ?? r.error).slice(0, 120) };
  };
  const touch = [];
  const reference = [];
  const other = keys[keys.length - 1];
  for (let i = 0; i < keys.length - 1; i += 1) {
    const k = keys[i];
    touch.push(await run([`UPDATE ${q(table)} SET ${q(touchCol)} = ${q(touchCol)} WHERE ${q(key)} = ${k}`]));
    if (watched.length) {
      const from = db.prepare(`SELECT ${watched.map((c) => `quote(${q(c)}) AS ${q(c)}`).join(", ")} FROM ${q(table)} WHERE ${q(key)} = ?`).get(other);
      const back = db.prepare(`SELECT ${watched.map((c) => `quote(${q(c)}) AS ${q(c)}`).join(", ")} FROM ${q(table)} WHERE ${q(key)} = ?`).get(k);
      reference.push(await run([`UPDATE ${q(table)} SET ${watched.map((c) => `${q(c)} = ${from[c]}`).join(", ")} WHERE ${q(key)} = ${k}`]));
      reference.push(await run([`UPDATE ${q(table)} SET ${watched.map((c) => `${q(c)} = ${back[c]}`).join(", ")} WHERE ${q(key)} = ${k}`]));
    }
  }
  const median = (l) => {
    const v = l.filter((x) => x.ok).map((x) => x.ms).sort((a, b) => a - b);
    return v.length ? Math.round(v[Math.floor(v.length / 2)]) : null;
  };
  const entry = {
    record: name, rows: keys.length - 1, watched: watched.length,
    touch: { medianMs: median(touch), rowsWritten: touch[0]?.rowsWritten, failed: touch.filter((x) => !x.ok).length },
    reference: { medianMs: median(reference), rowsWritten: Math.max(0, ...reference.map((x) => x.rowsWritten ?? 0)), failed: reference.filter((x) => !x.ok).length, error: reference.find((x) => !x.ok)?.error },
  };
  out.records.push(entry);
  console.log(JSON.stringify(entry));
}
if (option("--out")) writeFileSync(option("--out"), JSON.stringify(out, null, 2) + "\n");
