// Change capture without custom functions, on a loaded tenant object.
//
// The Rust runtime registers gonvex_record_value(record, key) and uses the
// pre-update/commit hooks. Durable Objects allow neither, so this replaces
// every gonvex_record_value('<record>', <key>) call in the stored triggers
// with a json_object subquery over the record's public view, typed exactly as
// record_views::image does (JSON parsed, BOOLEAN as true/false). It then
// compiles every table's write path and runs real writes with a flush.
//
// Usage: node scripts/capture.mjs <base> <object> [out.json]
// Prints keys, counts and timings only, never row values.
import { writeFileSync } from "node:fs";
import { quoteIdent } from "./sql.mjs";

const [base, object, outPath] = process.argv.slice(2);
const report = { object };

async function call(route, body = {}) {
  const started = performance.now();
  const response = await fetch(`${base}${route}?o=${encodeURIComponent(object)}`, { method: "POST",
    headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) });
  const value = await response.json().catch(async () => ({ error: (await response.text()).slice(0, 300) }));
  return { ...value, ms: performance.now() - started };
}

const kind = (declared) => {
  const t = declared.toUpperCase();
  if (t.includes("JSON")) return "json";
  if (t.includes("BOOL")) return "bool";
  return "plain";
};
// The SQL twin of record_views::image (JSON text parsed, booleans true/false).
const imageExpression = (schema) => {
  const columns = Object.entries(schema.columns);
  const pairs = columns.map(([name, def]) => {
    const c = quoteIdent(name);
    const k = kind(def.type);
    const value = k === "json" ? `json(${c})` : k === "bool" ? `CASE WHEN ${c} IS NULL THEN NULL WHEN typeof(${c}) IN ('integer','real') THEN json(iif(${c}, 'true', 'false')) ELSE ${c} END` : c;
    return `'${name.replace(/'/g, "''")}', ${value}`;
  });
  if (pairs.length * 2 > 126) throw new Error("record wider than 63 columns needs json_patch chunks");
  return `json_object(${pairs.join(", ")})`;
};
const snapshot = (record, keyExpression) =>
  `(SELECT ${imageExpression(record.schema)} FROM ${quoteIdent(record.name)} WHERE ${quoteIdent(record.schema.key)} = ${keyExpression})`;

// 1. Records and the triggers that call the custom function.
const records = new Map((await call("/query", { sql: "SELECT name, storage_table, key_column, schema FROM _gonvex_record_views" })).rows
  .map((r) => [r.name, { ...r, schema: JSON.parse(r.schema) }]));
const triggers = (await call("/query", { sql: "SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND sql LIKE '%gonvex_record_value(%'" })).rows;
const CALL = /gonvex_record_value\('((?:[^']|'')*)',((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)/g;
const rewritten = triggers.map((t) => ({
  name: t.name,
  sql: t.sql.replace(CALL, (_, name, key) => snapshot(records.get(name.replace(/''/g, "'")), key.trim())),
}));
const left = rewritten.filter((t) => t.sql.includes("gonvex_record_value(")).length;
const install = await call("/exec", {
  transaction: true,
  statements: rewritten.flatMap((t) => [`DROP TRIGGER ${quoteIdent(t.name)}`, t.sql]),
});
report.rewrite = {
  records: records.size, triggers: triggers.length, unresolvedCalls: left, ok: install.ok, ms: Math.round(install.ms),
  maxTriggerBytes: Math.max(...rewritten.map((t) => t.sql.length)), errors: (install.errors || []).slice(0, 3),
};
console.log(JSON.stringify({ rewrite: report.rewrite }));
if (process.argv.includes("--rewrite-only")) process.exit(0);

// 2. Compile every table's write path. `WHERE 0` touches no row, but SQLite
// still compiles every trigger program the statement could fire.
const tables = (await call("/query", { sql: "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite%' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT LIKE '%\\_fts\\_%' ESCAPE '\\'" })).rows.map((r) => r.name);
const failures = {};
let compiled = 0;
for (const table of tables) {
  const columns = (await call("/query", { sql: "SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0", params: [table] })).rows.map((r) => quoteIdent(r.name));
  const t = quoteIdent(table);
  const statements = {
    insert: `INSERT INTO ${t} (${columns.join(", ")}) SELECT ${columns.join(", ")} FROM ${t} WHERE 0`,
    update: `UPDATE ${t} SET ${columns.map((c) => `${c} = ${c}`).join(", ")} WHERE 0`,
    delete: `DELETE FROM ${t} WHERE 0`,
  };
  for (const [op, sql] of Object.entries(statements)) {
    const result = await call("/exec", { statements: [sql], transaction: true });
    compiled += 1;
    if (!result.ok) {
      const error = (result.errors && result.errors[0] && result.errors[0].error) || result.error;
      const key = String(error).replace(/: \S+$/, "").slice(0, 80);
      (failures[key] ||= []).push(`${op} ${table}`);
    }
  }
}
report.writePaths = { tables: tables.length, statements: compiled, failing: Object.values(failures).reduce((s, l) => s + l.length, 0),
  byError: Object.fromEntries(Object.entries(failures).map(([k, v]) => [k, { count: v.length, statements: v }])) };
console.log(JSON.stringify({ writePaths: { ...report.writePaths, byError: Object.fromEntries(Object.entries(report.writePaths.byError).map(([k, v]) => [k, { count: v.count, first: v.statements.slice(0, 8) }])) } }));

// 3. Real writes, then the per-record flush (FLUSH_SQL with the snapshot
// subquery per record name instead of the custom function), in one transaction.
const flushFor = (name) => {
  const record = records.get(name);
  return `INSERT INTO _gonvex_sync_changes(table_name,row_id,operation,new_value)
WITH resolved AS MATERIALIZED (SELECT table_name,row_id,old_value,${snapshot(record, "p.row_id")} new_value FROM _gonvex_record_pending p WHERE table_name='${name}')
SELECT table_name,row_id,CASE WHEN old_value IS NULL THEN 'insert' WHEN new_value IS NULL THEN 'delete' ELSE 'update' END,
 json_array(json(old_value),json(new_value),json(CASE WHEN old_value IS NULL OR new_value IS NULL THEN '[]' ELSE
 (SELECT json_group_array(k.key) FROM json_each(new_value) k
  WHERE json_type(old_value,'$.'||json_quote(k.key)) IS NOT json_type(new_value,'$.'||json_quote(k.key))
     OR json_extract(old_value,'$.'||json_quote(k.key)) IS NOT json_extract(new_value,'$.'||json_quote(k.key))) END))
FROM resolved WHERE old_value IS NOT new_value`;
};
report.writes = [];
for (const [name, set] of [["tasks", `"updatedAt" = "updatedAt" + 1`], ["statuses", `"updatedAt" = "updatedAt" + 1`], ["members", `"updated_at" = "updated_at" + 1`], ["notifications", `"updatedAt" = coalesce("updatedAt", 0) + 1`], ["workplanItems", `"updatedAt" = coalesce("updatedAt", 0) + 1`]]) {
  const record = records.get(name);
  if (!record) continue;
  const storage = quoteIdent(record.storage_table);
  const before = (await call("/query", { sql: "SELECT coalesce(max(event_id), 0) AS s FROM _gonvex_sync_changes" })).rows?.[0]?.s ?? 0;
  const result = await call("/exec", {
    transaction: true,
    statements: [
      `UPDATE ${storage} SET ${set} WHERE ${quoteIdent(record.key_column)} = (SELECT min(${quoteIdent(record.key_column)}) FROM ${storage})`,
      flushFor(name),
      "DELETE FROM _gonvex_record_pending",
    ],
  });
  const change = (await call("/query", { sql: "SELECT table_name, operation, json_array_length(new_value, '$[0]') IS NULL AS hasOld, json_extract(new_value, '$[2]') AS changed, length(new_value) AS bytes FROM _gonvex_sync_changes WHERE event_id > ? ORDER BY event_id", params: [before] })).rows;
  const entry = { record: name, ok: result.ok, ms: Math.round(result.ms), rowsWritten: result.rowsWritten, error: result.errors?.[0]?.error || result.error, changes: change };
  report.writes.push(entry);
  console.log(JSON.stringify(entry));
}
if (outPath) writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
