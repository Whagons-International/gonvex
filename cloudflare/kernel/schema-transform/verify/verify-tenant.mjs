// A01 "same results" check on one tenant file (0014 rule 3).
//
//   node verify/verify-tenant.mjs <tenant.db> --work <dir> --rust-bin <gonvex-a01-replay> [--rows 2] [--json out.json] [--keep]
//   node verify/verify-tenant.mjs <tenant.db> --work <dir> --rust-bin x --build-only   (only writes <dir>/xf.db)
//
// 1. Copies the file twice (reflink): `orig.db` keeps the original schema;
//    `xf.db` gets the transformed schema (the transform's `image` of the
//    file's own sqlite_schema) over the same rows.
// 2. Views the transform rewrote: identical rows (multiset, with storage
//    classes) on both, with every owner in scope for reference views; for T6
//    views also the `expected` view after a fill.
// 3. Index keys T3 rewrote: equal on every row and on a 10,000-string
//    Unicode fuzz corpus. A duplicate live key (a replay write, below) is
//    rejected with the same error by both.
// 4. Writes generated from the file replay on `orig.db` through the Rust
//    runtime's own capture (gonvex-a01-replay) and on `xf.db` through the
//    transformed schema plus T5's flush: same outcome and error per write,
//    same change rows, and then the same content in every table.
//
// Prints counts, names and keys only, never row values. Only native SQLite
// is used here; DO limits are checked by loading the result into an object.
import { DatabaseSync } from "node:sqlite";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { recordFlushSql, RECORD_PENDING_NAMES_SQL, RECORD_PENDING_CLEANUP_SQL, ADDED_PREFIX } from "../index.ts";
import { buildTransformed } from "./build-transformed.mjs";
import { foldChainsInIndex } from "../fold.ts";

const args = process.argv.slice(2);
const option = (name, fallback = null) => (args.includes(name) ? args[args.indexOf(name) + 1] : fallback);
const tenant = args[0];
const work = option("--work");
const rustBin = option("--rust-bin");
const rowsPerRecord = Number(option("--rows", "2"));
if (!tenant || !work || !rustBin) {
  console.error("usage: verify-tenant.mjs <tenant.db> --work <dir> --rust-bin <bin> [--rows 2] [--json out.json]");
  process.exit(2);
}
rmSync(work, { recursive: true, force: true });
mkdirSync(work, { recursive: true });
const origPath = join(work, "orig.db");
const xfPath = join(work, "xf.db");
execFileSync("cp", ["--reflink=auto", tenant, origPath]);
execFileSync("cp", ["--reflink=auto", tenant, xfPath]);
execFileSync("chmod", ["600", origPath, xfPath]); // the source may be read-only; the copies are scratch

const startedAt = Date.now();
// Values a trigger or default took from the clock (unixepoch, 'now') differ
// between the two runs. Integers within this run's window (ms or s) and ISO
// timestamps of the same window are masked on both sides; nothing else is.
const inWindow = (n) => (n >= startedAt - 5_000 && n <= Date.now() + 5_000) || (n >= startedAt / 1000 - 5 && n <= Date.now() / 1000 + 5);
const isoInWindow = (s) => {
  if (!/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(s)) return false;
  const t = Date.parse(s.includes("T") || s.endsWith("Z") ? s : s.replace(" ", "T") + "Z");
  return Number.isFinite(t) && inWindow(t);
};
const maskValue = (v) => {
  if (typeof v === "number") return Number.isInteger(v) && inWindow(v) ? "<clock>" : v;
  if (typeof v === "string") return isoInWindow(v) ? "<clock>" : v;
  if (Array.isArray(v)) return v.map(maskValue);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, maskValue(x)]));
  return v;
};
const report = { tenant: tenant.split("/").pop().replace(/\.db$/, ""), steps: {} };
const log = (step, value) => {
  report.steps[step] = value;
  console.log(JSON.stringify({ step, ...value }));
};
const q = (name) => `"${String(name).replace(/"/g, '""')}"`;
const time = (fn) => {
  const t = performance.now();
  const value = fn();
  return [value, Math.round(performance.now() - t)];
};

// --- 1. transformed copy --------------------------------------------------------
const built = buildTransformed(xfPath, { name: report.tenant, noT6: args.includes("--no-t6") });
const { transform, image, transformMs, applyMs } = built;
const { schema, records, recordsByName, capturedTables, tables } = built.catalog;
const rules = {};
for (const r of image.rewrites) rules[r.rule] = (rules[r.rule] ?? 0) + 1;
const orig = new DatabaseSync(origPath, { readOnly: true });
const xf = new DatabaseSync(xfPath);
const existing = new Map(schema.map((o) => [o.name.toLowerCase(), o]));
const xfSchema = built.schemaStats;
log("transform", {
  objects: schema.length, capturedTables: capturedTables.length, records: records.length, outputStatements: image.statements.length, rewrites: rules, added: transform.addedObjects().length,
  maxStatementBytes: Math.max(...image.statements.map((s) => Buffer.byteLength(s))), transformMs, applyMs,
  schema: Object.fromEntries(xfSchema.map((r) => [r.type, { n: r.n, bytes: r.bytes }])),
});

if (args.includes("--build-only")) {
  // Leaves work/xf.db: the tenant's rows under the transformed schema, for loading into an object.
  xf.close();
  orig.close();
  rmSync(origPath, { force: true });
  process.exit(0);
}

// --- 2. views ------------------------------------------------------------------------
const columnsOf = (db, rel) => db.prepare(`SELECT name FROM pragma_table_info(${`'${rel.replace(/'/g, "''")}'`})`).all().map((r) => r.name);
const canonicalRows = (db, rel) => {
  const cols = columnsOf(db, rel);
  const expr = `json_array(${cols.map((c) => `typeof(${q(c)}), quote(${q(c)})`).join(", ")})`;
  return db.prepare(`SELECT ${expr} AS r FROM ${q(rel)}`).all().map((r) => maskRow(r.r)).sort();
};
// [type, literal, type, literal, ...]: clock-valued integers and ISO text literals are masked.
function maskRow(text) {
  const cells = JSON.parse(text);
  for (let k = 0; k < cells.length; k += 2) {
    const type = cells[k];
    const lit = cells[k + 1];
    if (type === "integer" && inWindow(Number(lit))) cells[k + 1] = "<clock>";
    else if (type === "text" && lit.length < 40 && isoInWindow(lit.slice(1, -1))) cells[k + 1] = "<clock>";
  }
  return JSON.stringify(cells);
}
const digest = (list) => createHash("sha256").update(list.join("\n")).digest("hex").slice(0, 16);
const rewrittenViews = [...new Set(image.rewrites.filter((r) => ["T1", "T2", "T3", "T6"].includes(r.rule)).map((r) => r.object))]
  .filter((name) => existing.get(name.toLowerCase())?.type === "view");
const split = new Set(image.rewrites.filter((r) => r.rule === "T6").map((r) => r.object));
const viewResults = [];
const origRw = new DatabaseSync(origPath); // writable only inside rolled-back transactions here
for (const view of rewrittenViews.sort()) {
  const reference = view.startsWith("referenceExpected_");
  const source = view.slice("referenceExpected_".length);
  const scope = (db) => {
    if (!reference) return 0;
    const rec = recordsByName.get(source);
    if (!rec) throw new Error(`${view}: no record ${source}`);
    db.exec(`INSERT INTO schemaRelationshipOwners(sourceName, ownerId) SELECT '${source}', ${q(rec.keyColumn)} FROM ${q(rec.storageTable)} WHERE 1 ON CONFLICT DO NOTHING`);
    return db.prepare("SELECT count(*) n FROM schemaRelationshipOwners WHERE sourceName = ?").get(source).n;
  };
  origRw.exec("BEGIN");
  const owners = scope(origRw);
  const [a, origMs] = time(() => canonicalRows(origRw, view));
  origRw.exec("ROLLBACK");
  xf.exec("BEGIN");
  scope(xf);
  const [b, xfMs] = time(() => canonicalRows(xf, view));
  let expected = null;
  if (split.has(view)) {
    const base = `${ADDED_PREFIX}${view}`;
    const [rows, ms] = time(() => {
      xf.exec(`DELETE FROM ${q(base + "__rows")}`);
      xf.exec(`INSERT INTO ${q(base + "__queue")}(n) VALUES (NULL)`);
      xf.exec(`DELETE FROM ${q(base + "__queue")}`);
      return canonicalRows(xf, base + "__expected");
    });
    expected = { same: digest(rows) === digest(a), ms };
  }
  xf.exec("ROLLBACK");
  const entry = { view, owners, rows: a.length, same: a.length === b.length && digest(a) === digest(b), origMs, xfMs, expected };
  viewResults.push(entry);
}
log("views", {
  checked: viewResults.length, allSame: viewResults.every((v) => v.same && (!v.expected || v.expected.same)),
  rows: viewResults.reduce((s, v) => s + v.rows, 0), different: viewResults.filter((v) => !v.same || (v.expected && !v.expected.same)).map((v) => v.view),
  slowest: viewResults.slice().sort((x, y) => y.xfMs - x.xfMs).slice(0, 3).map((v) => ({ view: v.view, origMs: v.origMs, xfMs: v.xfMs, expectedMs: v.expected?.ms })),
});
origRw.close();

// --- 3. index keys --------------------------------------------------------------------
const folds = image.rewrites.filter((r) => r.rule === "T3" && existing.get(r.object.toLowerCase())?.type === "index").map((r) => existing.get(r.object.toLowerCase()));
const indexResults = [];
const exprOf = (sql) => {
  const open = sql.indexOf("(");
  const where = sql.lastIndexOf(") WHERE");
  return sql.slice(open + 1, where < 0 ? sql.lastIndexOf(")") : where);
};
function fuzzCorpus(n) {
  // Deterministic: a fixed xorshift seed. Mixes the code points the fold maps
  // (Latin-1, Latin Extended, Greek, Cyrillic, Armenian, Georgian), Unicode
  // spaces and dashes, combining marks, astral characters and ASCII.
  let x = 0x9e3779b9;
  const next = () => ((x ^= x << 13), (x ^= x >>> 17), (x ^= x << 5), (x >>> 0));
  const ranges = [[0x20, 0x7e], [0xa0, 0x24f], [0x370, 0x3ff], [0x400, 0x52f], [0x531, 0x58f], [0x10a0, 0x10ff], [0x1e00, 0x1fff], [0x2000, 0x206f], [0x2c00, 0x2c7f], [0xff00, 0xff5e], [0x300, 0x36f], [0x1f600, 0x1f64f]];
  const specials = [9, 10, 11, 12, 13, 133, 0x3000, 0x2028, 0x2029, 0x205f, 0x130, 0x131, 0x1e9e, 0xdf, 0x3a3, 0x3c2, 0x2d];
  const out = [];
  for (let k = 0; k < n; k += 1) {
    const len = 1 + (next() % 24);
    let s = "";
    for (let j = 0; j < len; j += 1) {
      if (next() % 5 === 0) s += String.fromCodePoint(specials[next() % specials.length]);
      else {
        const [lo, hi] = ranges[next() % ranges.length];
        s += String.fromCodePoint(lo + (next() % (hi - lo + 1)));
      }
    }
    out.push(s);
  }
  return out;
}
const corpus = fuzzCorpus(10_000);
for (const index of folds) {
  const transformed = image.statements.find((s) => new RegExp(`^CREATE\\s+UNIQUE\\s+INDEX\\s+"?${index.name}"?\\s`, "i").test(s) || new RegExp(`^CREATE\\s+INDEX\\s+"?${index.name}"?\\s`, "i").test(s));
  const differ = xf.prepare(`SELECT count(*) n FROM ${q(index.tbl_name)} WHERE (${exprOf(index.sql)}) IS NOT (${exprOf(transformed)})`).get().n;
  const rows = xf.prepare(`SELECT count(*) n FROM ${q(index.tbl_name)}`).get().n;
  // Fuzz: a scratch table with the indexed table's columns and the same fold.
  const cols = columnsOf(orig, index.tbl_name);
  const fz = new DatabaseSync(":memory:");
  fz.exec(`CREATE TABLE fz(${cols.map(q).join(", ")})`);
  const fold = foldChainsInIndex(index.sql.replace(new RegExp(`ON\\s+"?${index.tbl_name}"?`), "ON fz"), index.name, "fz");
  for (const c of fold.columns) fz.exec(c.sql);
  const nameCol = cols.includes("name") ? "name" : cols[1];
  const insert = fz.prepare(`INSERT INTO fz(${q(nameCol)}) VALUES (?)`);
  fz.exec("BEGIN");
  for (const s of corpus) insert.run(s);
  fz.exec("COMMIT");
  const fuzzDiffer = fz.prepare(`SELECT count(*) n FROM fz WHERE (${exprOf(index.sql)}) IS NOT (${exprOf(fold.index)})`).get().n;
  fz.close();
  indexResults.push({ index: index.name, rows, differ, fuzz: corpus.length, fuzzDiffer, generatedColumns: fold.columns.length });
}
log("indexes", { checked: indexResults.length, allSame: indexResults.every((i) => i.differ === 0 && i.fuzzDiffer === 0), results: indexResults });

// --- 4. replay ----------------------------------------------------------------------
const literal = (db, table, key, keyValue, cols) =>
  db.prepare(`SELECT ${cols.map((c) => `quote(${q(c)})`).join(", ")} FROM ${q(table)} WHERE ${q(key)} = ?`).all(keyValue)[0];
const writes = [];
const sourceUpdateTriggers = new Map();
for (const o of schema) {
  const m = /^CREATE TRIGGER "referenceBindings_[^"]+_source_update" AFTER UPDATE OF (.+?) ON "([^"]+)"/.exec(o.sql ?? "");
  if (m) sourceUpdateTriggers.set(m[2], [...m[1].matchAll(/"([^"]+)"/g)].map((x) => x[1]));
}
for (const rec of [...recordsByName.values()]) {
  const table = rec.storageTable;
  const cols = columnsOf(orig, table);
  const key = rec.keyColumn;
  const keys = orig.prepare(`SELECT ${q(key)} k FROM ${q(table)} ORDER BY ${q(key)} LIMIT ${rowsPerRecord}`).all().map((r) => r.k);
  const lastKeys = orig.prepare(`SELECT ${q(key)} k FROM ${q(table)} ORDER BY ${q(key)} DESC LIMIT 1`).all().map((r) => r.k);
  const watched = sourceUpdateTriggers.get(table);
  for (const [i, k] of keys.entries()) {
    const row = Object.values(literal(orig, table, key, k, cols));
    const keyLit = row[cols.indexOf(key)];
    const other = cols.find((c) => c !== key);
    if (other) writes.push({ id: `${table}#${i}:noop`, statements: [`UPDATE ${q(table)} SET ${q(other)} = ${q(other)} WHERE ${q(key)} = ${keyLit}`] });
    if (watched && lastKeys.length && lastKeys[0] !== k) {
      const from = literal(orig, table, key, lastKeys[0], watched);
      const back = literal(orig, table, key, k, watched);
      writes.push({ id: `${table}#${i}:swap`, statements: [`UPDATE ${q(table)} SET ${watched.map((c) => `${q(c)} = ${from[`quote(${q(c)})`]}`).join(", ")} WHERE ${q(key)} = ${keyLit}`] });
      writes.push({ id: `${table}#${i}:restore`, statements: [`UPDATE ${q(table)} SET ${watched.map((c) => `${q(c)} = ${back[`quote(${q(c)})`]}`).join(", ")} WHERE ${q(key)} = ${keyLit}`] });
    }
    writes.push({
      id: `${table}#${i}:reinsert`,
      statements: [`DELETE FROM ${q(table)} WHERE ${q(key)} = ${keyLit}`, `INSERT INTO ${q(table)}(${cols.map(q).join(", ")}) VALUES (${row.join(", ")})`],
    });
  }
}
// Captured plain tables (gonvex_capture): a no-op update, a swap of every
// non-key column with the last row's values and back, and a reinsert.
for (const shape of tables) {
  const table = shape.name;
  const cols = shape.columns.map((c) => c.name);
  const keys = shape.columns.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name);
  if (!keys.length) continue;
  const rest = cols.filter((c) => !keys.includes(c));
  const order = keys.map(q).join(", ");
  const literals = (where) => orig.prepare(`SELECT ${cols.map((c) => `quote(${q(c)}) AS ${q(c)}`).join(", ")} FROM ${q(table)} ${where}`).all();
  const rows = literals(`ORDER BY ${order} LIMIT ${rowsPerRecord}`);
  const last = literals(`ORDER BY ${keys.map((k) => `${q(k)} DESC`).join(", ")} LIMIT 1`)[0];
  for (const [i, row] of rows.entries()) {
    const where = keys.map((k) => `${q(k)} = ${row[k]}`).join(" AND ");
    if (rest.length) {
      writes.push({ id: `${table}#${i}:noop`, statements: [`UPDATE ${q(table)} SET ${q(rest[0])} = ${q(rest[0])} WHERE ${where}`] });
      if (last && keys.some((k) => last[k] !== row[k])) {
        writes.push({ id: `${table}#${i}:swap`, statements: [`UPDATE ${q(table)} SET ${rest.map((c) => `${q(c)} = ${last[c]}`).join(", ")} WHERE ${where}`] });
        writes.push({ id: `${table}#${i}:restore`, statements: [`UPDATE ${q(table)} SET ${rest.map((c) => `${q(c)} = ${row[c]}`).join(", ")} WHERE ${where}`] });
      }
    }
    writes.push({ id: `${table}#${i}:reinsert`, statements: [`DELETE FROM ${q(table)} WHERE ${where}`, `INSERT INTO ${q(table)}(${cols.map(q).join(", ")}) VALUES (${cols.map((c) => row[c]).join(", ")})`] });
  }
}

// A duplicate live key for every unique index T3 folded, through the full write path (identity key, record capture, triggers).
for (const index of folds.filter((i) => /UNIQUE/i.test(i.sql))) {
  const rec = [...recordsByName.values()].find((r) => r.storageTable.toLowerCase() === index.tbl_name.toLowerCase());
  if (!rec) continue;
  const where = /\)\s*WHERE\s+(.*)$/is.exec(index.sql)?.[1] ?? "1";
  const live = orig.prepare(`SELECT ${q(rec.keyColumn)} k FROM ${q(index.tbl_name)} WHERE ${where} ORDER BY ${q(rec.keyColumn)} LIMIT 1`).get();
  if (!live) continue;
  const domain = orig.prepare("SELECT domain FROM _gonvex_identity_keys WHERE _id = ?").get(live.k).domain;
  const cols = columnsOf(orig, index.tbl_name).filter((c) => c !== rec.keyColumn);
  writes.push({
    id: `${index.name}:duplicate-live-key`,
    statements: [
      `INSERT INTO _gonvex_identity_keys(domain, value) VALUES (${domain}, 'a01-duplicate-key-probe')`,
      `INSERT INTO ${q(index.tbl_name)}(${q(rec.keyColumn)}, ${cols.map(q).join(", ")}) SELECT last_insert_rowid(), ${cols.map(q).join(", ")} FROM ${q(index.tbl_name)} WHERE ${q(rec.keyColumn)} = ${live.k}`,
    ],
  });
}
orig.close();
writeFileSync(join(work, "writes.json"), JSON.stringify(writes));
const [, rustMs] = time(() => execFileSync(rustBin, [origPath, join(work, "writes.json"), join(work, "rust.json")], { stdio: "inherit" }));
const rust = JSON.parse(readFileSync(join(work, "rust.json"), "utf8"));

const normalizeError = (e) => (e === null || e === undefined ? null : String(e).replace(/^error returned from database: \(code: \d+\) /, "").replace(/: SQLITE_\w+$/, "").trim());
const jsOut = [];
const [, jsMs] = time(() => {
  for (const w of writes) {
    let error = null;
    let changes = [];
    xf.exec("BEGIN IMMEDIATE");
    try {
      for (const s of w.statements) xf.exec(s);
      for (const { table_name } of xf.prepare(RECORD_PENDING_NAMES_SQL).all()) xf.exec(recordFlushSql(recordsByName.get(table_name)));
      xf.exec(RECORD_PENDING_CLEANUP_SQL);
      xf.exec("DELETE FROM _gonvex_sync_changes WHERE revision IS NULL AND operation = 'update' AND new_value IS NULL");
      xf.exec(`UPDATE _gonvex_sync_changes SET old_value = nullif(new_value -> '$[0]', 'null'), changed_columns = coalesce(new_value -> '$[2]', '[]'), new_value = nullif(new_value -> '$[1]', 'null') WHERE revision IS NULL AND json_type(new_value) = 'array'`);
      changes = xf.prepare("SELECT table_name, row_id, operation, old_value, new_value, changed_columns FROM _gonvex_sync_changes WHERE revision IS NULL ORDER BY event_id").all();
      if (changes.length) {
        const revision = xf.prepare("UPDATE _gonvex_sync_clock SET revision = revision + 1 WHERE singleton = 1 RETURNING revision").get().revision;
        xf.prepare("UPDATE _gonvex_sync_changes SET revision = ?, transaction_id = ? WHERE revision IS NULL").run(revision, revision);
      }
      xf.exec("COMMIT");
    } catch (e) {
      error = e.message;
      changes = [];
      try {
        xf.exec("ROLLBACK");
      } catch {
        // already rolled back
      }
    }
    jsOut.push({ id: w.id, ok: error === null, error, changes });
  }
});
if (args.includes("--keep")) writeFileSync(join(work, "transformed.json"), JSON.stringify(jsOut));
const sameJson = (a, b) => {
  if (a === b) return true;
  if (a === null || b === null) return false;
  try {
    return JSON.stringify(maskValue(JSON.parse(a))) === JSON.stringify(maskValue(JSON.parse(b)));
  } catch {
    return false;
  }
};
let outcomeDiffs = 0;
let errorDiffs = 0;
let changeRows = 0;
let exactRows = 0;
let jsonEqualRows = 0;
let exactMaskedRows = 0;
// Raw JSON text with clock-valued integers masked: byte identity apart from the clock.
const maskText = (t) => (t === null ? null : t.replace(/-?\d{9,13}(?![\d.eE])/g, (m) => (inWindow(Number(m)) ? "<clock>" : m)));
const mismatches = [];
for (const [i, w] of writes.entries()) {
  const r = rust[i];
  const j = jsOut[i];
  if (r.ok !== j.ok) {
    outcomeDiffs += 1;
    if (mismatches.length < 20) mismatches.push({ id: w.id, kind: "outcome", rust: normalizeError(r.error)?.slice(0, 160), transformed: normalizeError(j.error)?.slice(0, 160) });
    continue;
  }
  if (!r.ok && normalizeError(r.error) !== normalizeError(j.error)) {
    errorDiffs += 1;
    if (mismatches.length < 20) mismatches.push({ id: w.id, kind: "error", rust: normalizeError(r.error)?.slice(0, 160), transformed: normalizeError(j.error)?.slice(0, 160) });
  }
  if (r.changes.length !== j.changes.length) {
    if (mismatches.length < 20) mismatches.push({ id: w.id, kind: "change-count", rust: r.changes.length, transformed: j.changes.length });
    outcomeDiffs += 1;
    continue;
  }
  for (const [k, rc] of r.changes.entries()) {
    const jc = j.changes[k];
    changeRows += 1;
    const head = rc.table_name === jc.table_name && rc.row_id === jc.row_id && rc.operation === jc.operation;
    const exact = head && rc.old_value === jc.old_value && rc.new_value === jc.new_value && rc.changed_columns === jc.changed_columns;
    const json = head && sameJson(rc.old_value, jc.old_value) && sameJson(rc.new_value, jc.new_value) && sameJson(rc.changed_columns, jc.changed_columns);
    if (exact) exactRows += 1;
    if (head && maskText(rc.old_value) === maskText(jc.old_value) && maskText(rc.new_value) === maskText(jc.new_value) && rc.changed_columns === jc.changed_columns) exactMaskedRows += 1;
    if (json) jsonEqualRows += 1;
    else if (mismatches.length < 20) {
      const fields = ["table_name", "row_id", "operation", "old_value", "new_value", "changed_columns"].filter((f) => (f.endsWith("value") || f === "changed_columns" ? !sameJson(rc[f], jc[f]) : rc[f] !== jc[f]));
      mismatches.push({ id: w.id, kind: "change", index: k, table: rc.table_name, fields });
    }
  }
}
const dupes = writes.map((w, i) => [w, i]).filter(([w]) => w.id.endsWith(":duplicate-live-key")).map(([w, i]) => ({ id: w.id, rust: normalizeError(rust[i].error), transformed: normalizeError(jsOut[i].error) }));
log("replay", {
  duplicateLiveKey: dupes,
  writes: writes.length, ok: rust.filter((r) => r.ok).length, failedOnBoth: rust.filter((r, i) => !r.ok && !jsOut[i].ok).length,
  outcomeDiffs, errorDiffs, changeRows, exactRows, exactMaskedRows, jsonEqualRows, rustMs, transformedMs: jsMs, mismatches,
});

// --- 5. final content of every table -------------------------------------------------
const after = new DatabaseSync(origPath, { readOnly: true });
const skip = new Set(["_gonvex_sync_changes", "_gonvex_sync_transactions", "_gonvex_sync_clock", "sqlite_stat1", "sqlite_stat4", "sqlite_sequence"]);
const allTables = after.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite%' AND sql NOT LIKE 'CREATE VIRTUAL%' ORDER BY name").all().map((r) => r.name)
  .filter((t) => !skip.has(t) && !/_fts_(data|idx|content|docsize|config)$/.test(t));
const tableDiffs = [];
let tableRows = 0;
for (const t of allTables) {
  const a = canonicalRows(after, t);
  const b = canonicalRows(xf, t);
  tableRows += a.length;
  if (a.length !== b.length || digest(a) !== digest(b)) tableDiffs.push({ table: t, rows: [a.length, b.length] });
}
const leftover = transform.addedObjects().filter((o) => o.type === "table").map((o) => ({ table: o.name, rows: xf.prepare(`SELECT count(*) n FROM ${q(o.name)}`).get().n })).filter((x) => x.rows);
log("tables", { compared: allTables.length, rows: tableRows, different: tableDiffs, scratchRowsLeft: leftover });
after.close();
xf.close();

report.pass =
  report.steps.views.allSame && report.steps.indexes.allSame && report.steps.replay.outcomeDiffs === 0 && report.steps.replay.errorDiffs === 0 &&
  report.steps.replay.jsonEqualRows === report.steps.replay.changeRows &&
  report.steps.replay.duplicateLiveKey.every((d) => d.rust === d.transformed && /UNIQUE constraint failed: index/.test(d.rust ?? "")) && report.steps.tables.different.length === 0 && report.steps.tables.scratchRowsLeft.length === 0;
console.log(JSON.stringify({ tenant: report.tenant, pass: report.pass }));
if (option("--json")) writeFileSync(option("--json"), JSON.stringify(report, null, 2) + "\n");
if (!args.includes("--keep")) rmSync(work, { recursive: true, force: true });
process.exit(report.pass ? 0 : 1);
