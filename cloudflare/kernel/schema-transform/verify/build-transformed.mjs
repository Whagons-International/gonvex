// Turns a copy of a tenant file into the kernel's schema in place: the
// transform's `image` of the file's own sqlite_schema, over the same rows.
// Triggers and views are dropped and recreated; tables keep their rows;
// indexes the transform changed are rebuilt; added objects are created.
import { DatabaseSync } from "node:sqlite";
import { recordView, SchemaTransform } from "../index.ts";

const q = (name) => `"${String(name).replace(/"/g, '""')}"`;
const nameOf = (m) => m.replace(/^"|"$/g, "").replace(/""/g, '"').toLowerCase();
const TABLE = /^CREATE\s+(?:VIRTUAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("(?:[^"]|"")*"|[\w$]+)/i;
const INDEX = /^CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?("(?:[^"]|"")*"|[\w$]+)/i;

/** Reads the catalog the transform needs from a tenant file: schema rows, record views, gonvex_capture table shapes. */
export function readCatalog(db) {
  const schema = db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY rowid").all();
  const hasRecords = db.prepare("SELECT 1 FROM sqlite_schema WHERE name = '_gonvex_record_views'").get();
  const records = hasRecords ? db.prepare("SELECT name, storage_table, key_column, schema FROM _gonvex_record_views ORDER BY name").all() : [];
  const capturedTables = [...new Set(schema.filter((o) => o.type === "trigger" && /gonvex_capture\s*\(/i.test(o.sql ?? "")).map((o) => o.tbl_name))].sort();
  const tables = capturedTables.map((name) => ({
    name,
    columns: db.prepare("SELECT name, type AS declared, pk FROM pragma_table_info(?) ORDER BY cid").all(name).map((c) => ({ name: c.name, declared: c.declared, pk: c.pk })),
  }));
  return { schema, records, recordsByName: new Map(records.map((r) => [r.name, recordView(r)])), capturedTables, tables };
}

/**
 * Applies the transform to the file at `path` (a scratch copy). Returns the
 * transform, its image and the catalog it read.
 * `noT6`: measurement only, the layout without T6 (S1's lax-host layout).
 */
export function buildTransformed(path, { name = "tenant", noT6 = false } = {}) {
  const db = new DatabaseSync(path);
  const catalog = readCatalog(db);
  const transform = noT6 ? new SchemaTransform({ referenceMinBranches: Infinity, limits: { unionBranchesRead: Infinity } }) : new SchemaTransform();
  const t0 = performance.now();
  const image = transform.image(name, catalog.schema, { records: catalog.records, tables: catalog.tables });
  const transformMs = Math.round(performance.now() - t0);
  const existing = new Map(catalog.schema.map((o) => [o.name.toLowerCase(), o]));
  const t1 = performance.now();
  db.exec("PRAGMA foreign_keys=OFF");
  db.exec("BEGIN");
  for (const o of catalog.schema) if (o.type === "trigger") db.exec(`DROP TRIGGER ${q(o.name)}`);
  for (const o of catalog.schema) if (o.type === "view") db.exec(`DROP VIEW ${q(o.name)}`);
  for (const s of image.statements) {
    const m = INDEX.exec(s);
    const before = m && existing.get(nameOf(m[1]));
    if (before && before.sql !== s) db.exec(`DROP INDEX ${q(before.name)}`);
  }
  for (const s of image.statements) {
    const table = TABLE.exec(s);
    if (table && existing.has(nameOf(table[1]))) continue;
    const index = INDEX.exec(s);
    if (index && existing.get(nameOf(index[1]))?.sql === s) continue;
    db.exec(s);
  }
  db.exec("COMMIT");
  db.exec("PRAGMA foreign_keys=ON");
  const applyMs = Math.round(performance.now() - t1);
  const schemaStats = db.prepare("SELECT type, count(*) n, sum(length(sql)) bytes FROM sqlite_schema GROUP BY type ORDER BY type").all();
  db.close();
  return { transform, image, catalog, transformMs, applyMs, schemaStats };
}
