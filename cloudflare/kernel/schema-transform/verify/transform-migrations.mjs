// Runs the transform over a migrations directory (in name order) and prints a
// summary. Exits 1 with the TransformError (migration, object, limit) on an
// unhandled construct. Used by the CI drift job and by hand.
//
//   node verify/transform-migrations.mjs <migrations-dir> [--prelude file.sql] [--out dir] [--json report.json]
import { readFileSync, readdirSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { SchemaTransform, TransformError, TRANSFORM_VERSION } from "../index.ts";

const args = process.argv.slice(2);
const option = (name) => (args.includes(name) ? args[args.indexOf(name) + 1] : null);
const dir = args.find((a, i) => !a.startsWith("--") && !(i > 0 && args[i - 1].startsWith("--")));
if (!dir) {
  console.error("usage: transform-migrations.mjs <migrations-dir> [--prelude file.sql] [--out dir] [--json report.json]");
  process.exit(2);
}
const files = readdirSync(dir).filter((f) => /^\d+_.*\.sql$/.test(f)).sort();
const inputs = [];
if (option("--prelude")) inputs.push({ name: "runtime-prelude", sql: readFileSync(option("--prelude"), "utf8") });
for (const f of files) inputs.push({ name: f, sql: readFileSync(join(dir, f), "utf8") });

const transform = new SchemaTransform();
const report = { version: TRANSFORM_VERSION, migrations: [] };
const started = performance.now();
try {
  for (const input of inputs) {
    const t0 = performance.now();
    const m = transform.migration(input.name, input.sql);
    const rules = {};
    for (const r of m.rewrites) rules[r.rule] = (rules[r.rule] ?? 0) + 1;
    const bytes = m.statements.reduce((s, x) => s + Buffer.byteLength(x), 0);
    const entry = { name: m.name, source: m.sourceStatements, output: m.statements.length, chunks: m.chunks.length, bytes,
      maxStatementBytes: Math.max(0, ...m.statements.map((x) => Buffer.byteLength(x))), foreignKeyRebuild: m.foreignKeyRebuild, rewrites: rules, ms: Math.round(performance.now() - t0) };
    report.migrations.push(entry);
    console.log(JSON.stringify(entry));
    if (option("--out")) {
      mkdirSync(option("--out"), { recursive: true });
      writeFileSync(join(option("--out"), input.name.replace(/\.sql$/, "") + ".sql"), m.statements.map((s) => s + ";").join("\n"));
    }
  }
} catch (error) {
  if (error instanceof TransformError) {
    console.error(JSON.stringify({ failed: true, migration: error.migration, object: error.object, limit: error.limit, detail: error.detail }));
    process.exit(1);
  }
  throw error;
}
const added = transform.addedObjects();
report.added = { count: added.length, byType: added.reduce((m, a) => ((m[a.type] = (m[a.type] ?? 0) + 1), m), {}) };
report.ms = Math.round(performance.now() - started);
console.log(JSON.stringify({ version: report.version, added: report.added, ms: report.ms }));
if (option("--json")) writeFileSync(option("--json"), JSON.stringify({ ...report, addedObjects: added }, null, 2) + "\n");
