// Static analysis of the Whagons tenant schema against Durable Object SQLite
// limits, plus a native replay of the migrations for reference timings.
// Usage: node scripts/analyze.mjs <tenant.db> <migrations-dir> <prelude.sql> [out.json]
// Writes names and numbers only, never row values.
import { DatabaseSync } from "node:sqlite";
import { readFileSync, readdirSync, writeFileSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { splitStatements, compoundChains, functionCalls, likeLiterals, ddlBytes } from "./sql.mjs";

const [tenantPath, migrationsDir, preludePath, outPath] = process.argv.slice(2);
const LIMITS = { statementBytes: 100_000, columns: 100, functionArguments: 127, compoundTerms: 5, likePatternBytes: 50, rowBytes: 2_000_000 };
const bytes = (s) => Buffer.byteLength(s);
const report = { sqliteNative: null, limits: LIMITS, migrations: {}, schema: {}, data: {} };

// --- 1. Native migration replay, one statement at a time -----------------------
{
  const dir = mkdtempSync(join(tmpdir(), "s1-native-"));
  const db = new DatabaseSync(join(dir, "m.db"));
  report.sqliteNative = db.prepare("SELECT sqlite_version() AS v").get().v;
  const files = [["prelude", readFileSync(preludePath, "utf8")]].concat(
    readdirSync(migrationsDir).filter((f) => /^\d+_.*\.sql$/.test(f)).sort().map((f) => [f, readFileSync(join(migrationsDir, f), "utf8")]));
  const all = [];
  for (const [name, text] of files) {
    const statements = splitStatements(text);
    const started = performance.now();
    const kinds = {};
    const slow = [];
    // The runtime turns foreign keys off around `-- gonvex:foreign-key-rebuild` migrations.
    const rebuild = /^-- gonvex:foreign-key-rebuild\s*$/m.test(text);
    if (rebuild) db.exec("PRAGMA foreign_keys=OFF");
    db.exec("BEGIN");
    for (const [index, statement] of statements.entries()) {
      const t0 = performance.now();
      db.exec(statement);
      const ms = performance.now() - t0;
      const kind = statement.replace(/^(\s|--[^\n]*\n)+/, "").split(/\s+/).slice(0, 2).join(" ").toUpperCase();
      kinds[kind] = (kinds[kind] || 0) + 1;
      slow.push({ index, ms: +ms.toFixed(1), kind, head: statement.replace(/\s+/g, " ").slice(0, 90) });
      all.push({ file: name, index, bytes: bytes(statement), statement });
    }
    if (rebuild && db.prepare("SELECT count(*) AS n FROM pragma_foreign_key_check").get().n) throw new Error(`${name}: foreign_key_check failed`);
    db.exec("COMMIT");
    if (rebuild) db.exec("PRAGMA foreign_keys=ON");
    slow.sort((a, b) => b.ms - a.ms);
    report.migrations[name] = {
      fileBytes: bytes(text),
      statements: statements.length,
      maxStatementBytes: Math.max(0, ...statements.map(bytes)),
      nativeMs: +(performance.now() - started).toFixed(0),
      foreignKeyRebuild: rebuild,
      kinds,
      slowest: slow.slice(0, 5),
    };
  }
  report.migrationTotals = {
    statements: all.length,
    nativeMs: Object.values(report.migrations).reduce((s, m) => s + m.nativeMs, 0),
    schema: Object.fromEntries(db.prepare("SELECT type, count(*) AS n FROM sqlite_schema GROUP BY type").all().map((r) => [r.type, r.n])),
    ddlOverLimitAfterQuoteDoubling: all.filter((s) => /^\s*CREATE/i.test(s.statement) && ddlBytes(s.statement) > LIMITS.statementBytes).length,
    overStatementLimit: all.filter((s) => s.bytes > LIMITS.statementBytes).map((s) => ({ file: s.file, index: s.index, bytes: s.bytes, head: s.statement.replace(/\s+/g, " ").slice(0, 100) })),
    overCompoundLimit: all.filter((s) => Math.max(0, ...compoundChains(s.statement)) > LIMITS.compoundTerms).length,
    overFunctionArgs: all.filter((s) => functionCalls(s.statement).some((c) => c.args > LIMITS.functionArguments)).length,
  };
  db.close();
  rmSync(dir, { recursive: true, force: true });
}

// --- 2. Final schema of the real tenant file -----------------------------------
const db = new DatabaseSync(tenantPath, { readOnly: true });
const objects = db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY rowid").all();
const virtualTables = objects.filter((o) => o.type === "table" && /^CREATE VIRTUAL TABLE/i.test(o.sql));
const shadow = new Set();
for (const v of virtualTables) for (const suffix of ["_data", "_idx", "_content", "_docsize", "_config"]) shadow.add(v.name + suffix);
const counts = {};
for (const o of objects) counts[o.type] = (counts[o.type] || 0) + 1;
report.schema.counts = counts;
report.schema.ddlBytes = objects.reduce((s, o) => s + bytes(o.sql), 0);
report.schema.virtualTables = virtualTables.map((v) => ({ name: v.name, sql: v.sql }));
report.schema.shadowTables = [...shadow].filter((n) => objects.some((o) => o.name === n)).length;
report.schema.internalTables = objects.filter((o) => o.type === "table" && o.name.startsWith("sqlite_")).map((o) => o.name);

const statementOver = objects.filter((o) => bytes(o.sql) > LIMITS.statementBytes);
report.schema.statementsOver100KB = statementOver.map((o) => ({ type: o.type, name: o.name, bytes: bytes(o.sql), compoundTerms: Math.max(0, ...compoundChains(o.sql)) }));
// What SQLite actually checks for DDL: the text with single quotes doubled.
report.schema.ddlOverLimitAfterQuoteDoubling = objects.filter((o) => ddlBytes(o.sql) > LIMITS.statementBytes).map((o) => ({ type: o.type, name: o.name, bytes: bytes(o.sql), effectiveBytes: ddlBytes(o.sql) }));

const compound = [];
const args = [];
const likes = [];
const regexpUses = [];
for (const o of objects) {
  const chains = compoundChains(o.sql);
  const worst = Math.max(0, ...chains);
  if (worst > LIMITS.compoundTerms) compound.push({ type: o.type, name: o.name, maxTerms: worst, chainsOverLimit: chains.filter((c) => c > LIMITS.compoundTerms).length });
  for (const call of functionCalls(o.sql)) if (call.args > LIMITS.functionArguments) args.push({ type: o.type, name: o.name, function: call.name, args: call.args });
  for (const like of likeLiterals(o.sql)) if (like.bytes > LIMITS.likePatternBytes) likes.push({ type: o.type, name: o.name, bytes: like.bytes });
  if (/\bREGEXP\b|\bregexp\s*\(/i.test(o.sql.replace(/'(?:[^']|'')*'/g, "''"))) regexpUses.push({ type: o.type, name: o.name });
}
const byType = (list) => list.reduce((acc, x) => ((acc[x.type] = (acc[x.type] || 0) + 1), acc), {});
report.schema.compoundOverLimit = { count: compound.length, byType: byType(compound), maxTerms: Math.max(0, ...compound.map((c) => c.maxTerms)), objects: compound };
report.schema.functionArgsOverLimit = { count: args.length, objects: args };
report.schema.likeLiteralsOverLimit = { count: likes.length, objects: likes };
report.schema.regexpUses = regexpUses;

// Columns: tables, and views (a view's result set counts against the same limit).
const wide = [];
const columnHistogram = {};
for (const o of objects.filter((x) => x.type === "table" || x.type === "view")) {
  if (shadow.has(o.name)) continue;
  let n;
  try { n = db.prepare("SELECT count(*) AS n FROM pragma_table_xinfo(?)").get(o.name).n; } catch { continue; }
  const bucket = n > 100 ? ">100" : n > 50 ? "51-100" : n > 25 ? "26-50" : "1-25";
  columnHistogram[`${o.type}:${bucket}`] = (columnHistogram[`${o.type}:${bucket}`] || 0) + 1;
  if (n > LIMITS.columns) wide.push({ type: o.type, name: o.name, columns: n });
}
report.schema.columnsOverLimit = wide;
report.schema.columnHistogram = columnHistogram;
report.schema.maxColumns = Math.max(...objects.filter((x) => x.type === "table" && !shadow.has(x.name)).map((x) => {
  try { return db.prepare("SELECT count(*) AS n FROM pragma_table_xinfo(?)").get(x.name).n; } catch { return 0; }
}));

// --- 3. Data: rows per table, widest row and cell -----------------------------
const quote = (name) => `"${name.replace(/"/g, '""')}"`;
const tables = objects.filter((o) => o.type === "table" && !shadow.has(o.name) && !o.name.startsWith("sqlite_") && !/^CREATE VIRTUAL/i.test(o.sql));
let totalRows = 0;
let totalBytes = 0;
const perTable = [];
for (const t of tables) {
  const cols = db.prepare("SELECT name FROM pragma_table_info(?)").all(t.name).map((c) => c.name);
  const rowExpr = cols.map((c) => `coalesce(length(CAST(${quote(c)} AS BLOB)),0)`).join("+");
  const cellExpr = cols.length > 1 ? `max(${cols.map((c) => `coalesce(length(CAST(${quote(c)} AS BLOB)),0)`).join(",")})` : `coalesce(length(CAST(${quote(cols[0])} AS BLOB)),0)`;
  const r = db.prepare(`SELECT count(*) AS rows, coalesce(max(${rowExpr}),0) AS maxRow, coalesce(sum(${rowExpr}),0) AS total, coalesce(max(${cellExpr}),0) AS maxCell FROM ${quote(t.name)}`).get();
  totalRows += r.rows;
  totalBytes += r.total;
  perTable.push({ name: t.name, columns: cols.length, rows: r.rows, bytes: r.total, maxRowBytes: r.maxRow, maxCellBytes: r.maxCell });
}
perTable.sort((a, b) => b.bytes - a.bytes);
report.data = {
  tables: tables.length,
  nonEmptyTables: perTable.filter((t) => t.rows > 0).length,
  rows: totalRows,
  valueBytes: totalBytes,
  fileBytes: db.prepare("SELECT page_count * page_size AS n FROM pragma_page_count, pragma_page_size").get().n,
  rowsOver2MB: perTable.filter((t) => t.maxRowBytes > LIMITS.rowBytes).map((t) => ({ name: t.name, maxRowBytes: t.maxRowBytes })),
  widestRow: perTable.reduce((a, b) => (b.maxRowBytes > a.maxRowBytes ? b : a)),
  largestTables: perTable.slice(0, 15),
};
db.close();

const text = JSON.stringify(report, null, 2);
if (outPath) writeFileSync(outPath, text + "\n");
console.log(JSON.stringify({
  sqliteNative: report.sqliteNative,
  migrationTotals: report.migrationTotals,
  perMigration: Object.fromEntries(Object.entries(report.migrations).map(([k, v]) => [k, { statements: v.statements, ms: v.nativeMs, maxBytes: v.maxStatementBytes }])),
  schema: { ...report.schema, compoundOverLimit: { ...report.schema.compoundOverLimit, objects: report.schema.compoundOverLimit.objects.slice(0, 5) }, virtualTables: report.schema.virtualTables.map((v) => v.name) },
  data: { ...report.data, largestTables: report.data.largestTables.slice(0, 5) },
}, null, 1));
