// Cuts a closed schema subset from the provisioned tenant schema
// (.artifact/tenant-schema.sqlite) for the objects the S2 Reducers touch
// (.artifact/reference-results.json statement logs). Closure rules:
// - every table/view a statement or host call names;
// - triggers on included objects, and every object their SQL names;
// - views' and tables' referenced objects (view bodies, FK parents);
// - indexes on included tables; all runtime tables (_gonvex_*, gonvex_*).
// Rows that provisioning inserted (record view schemas, catalogs, defaults)
// are copied for included tables.
// Output: .artifact/schema-subset.json = [{ sql }...] in a valid order, plus stats.
import { DatabaseSync } from "node:sqlite";
import { readFileSync, writeFileSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const db = new DatabaseSync(join(spike, ".artifact/tenant-schema.sqlite"), { readOnly: true });
const objects = db.prepare("SELECT type, name, tbl_name, sql, rowid FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY rowid").all();
const byName = new Map(objects.map((o) => [o.name.toLowerCase(), o]));
const relations = new Set(objects.filter((o) => o.type === "table" || o.type === "view").map((o) => o.name.toLowerCase()));
const identifiers = (sql) => {
  const names = new Set();
  for (const match of sql.matchAll(/"((?:[^"]|"")+)"|`([^`]+)`|\[([^\]]+)\]|\b([A-Za-z_][A-Za-z0-9_$]*)\b/g)) {
    const name = (match[1]?.replaceAll('""', '"') ?? match[2] ?? match[3] ?? match[4]).toLowerCase();
    if (relations.has(name)) names.add(name);
  }
  return names;
};

const results = JSON.parse(readFileSync(join(spike, ".artifact/reference-results.json"), "utf8"));
const seeds = new Set();
for (const result of results) for (const statement of result.stats.statementsLog) {
  const marker = /^\[(?:insert|update|delete|deleteMany) (.+)\]$/.exec(statement);
  if (marker) seeds.add(marker[1].toLowerCase());
  else for (const name of identifiers(statement)) seeds.add(name);
}
for (const o of objects) if (/^_?gonvex_/i.test(o.name) && (o.type === "table" || o.type === "view")) seeds.add(o.name.toLowerCase());
seeds.add("members");

const included = new Set();
const queue = [...seeds];
while (queue.length) {
  const name = queue.pop();
  if (included.has(name) || !byName.has(name)) continue;
  included.add(name);
  const object = byName.get(name);
  const next = new Set(identifiers(object.sql));
  if (object.type === "table") for (const fk of db.prepare("SELECT DISTINCT \"table\" FROM pragma_foreign_key_list(?)").all(object.name)) next.add(fk.table.toLowerCase());
  for (const other of objects) {
    if ((other.type === "trigger" || other.type === "index") && other.tbl_name.toLowerCase() === name) next.add(other.name.toLowerCase());
  }
  for (const n of next) if (!included.has(n)) queue.push(n);
}
const rank = { table: 0, index: 1, view: 2, trigger: 3 };
const chosen = objects.filter((o) => included.has(o.name.toLowerCase())).sort((a, b) => rank[a.type] - rank[b.type] || a.rowid - b.rowid);
const statements = chosen.map((o) => ({ type: o.type, name: o.name, sql: o.sql }));
const rows = [];
for (const o of chosen.filter((o) => o.type === "table")) {
  const data = db.prepare(`SELECT * FROM "${o.name.replaceAll('"', '""')}"`).all();
  if (data.length) rows.push({ table: o.name, rows: data.map((r) => ({ ...r })) });
}
const count = (type) => chosen.filter((o) => o.type === type).length;
const total = (type) => objects.filter((o) => o.type === type).length;
const stats = {
  seeds: seeds.size,
  tables: `${count("table")} of ${total("table")}`, views: `${count("view")} of ${total("view")}`,
  triggers: `${count("trigger")} of ${total("trigger")}`, indexes: `${count("index")} of ${total("index")}`,
  ddlBytes: statements.reduce((n, s) => n + s.sql.length, 0), largestStatementBytes: Math.max(...statements.map((s) => s.sql.length)),
  seededTables: rows.length, seededRows: rows.reduce((n, r) => n + r.rows.length, 0),
};
writeFileSync(join(spike, ".artifact/schema-subset.json"), JSON.stringify({ statements, rows, stats }));
console.log(JSON.stringify(stats, null, 2));
