// Runs the S2 steps on the node:sqlite reference.
// Usage: node test/run-reference.mjs <db-path> [--savepoints] [--subset <skip.json>] [--out <results.json>]
//   default schema: the full provisioned tenant schema;
//   --subset: exactly the schema-subset DDL the Durable Object accepted
//   (objects named in skip.json are left out, as DO SQLite refused them).
import { openReference, runOnNode } from "./reference-lib.mjs";
import { DatabaseSync } from "node:sqlite";
import { rmSync, readFileSync } from "node:fs";
import { fixture, steps, callOf, T0 } from "./scenarios.mjs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { writeFileSync } from "node:fs";

const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dbPath = process.argv[2] ?? join(spike, ".artifact/reference-run.sqlite");
const flag = (name) => { const index = process.argv.indexOf(`--${name}`); return index >= 0 ? process.argv[index + 1] : undefined; };
let db;
if (flag("subset")) {
  const skip = new Set(JSON.parse(readFileSync(flag("subset"), "utf8")));
  const subset = JSON.parse(readFileSync(join(spike, ".artifact/schema-subset.json"), "utf8"));
  rmSync(dbPath, { force: true });
  db = new DatabaseSync(dbPath);
  db.function("regexp", { deterministic: true }, (pattern, value) => (value == null ? null : new RegExp(String(pattern)).test(String(value)) ? 1 : 0));
  db.exec("PRAGMA foreign_keys=ON");
  for (const statement of subset.statements) if (!skip.has(statement.name)) db.exec(statement.sql);
  for (const { table, rows } of subset.rows) for (const row of rows) {
    const keys = Object.keys(row);
    db.prepare(`INSERT INTO "${table}" (${keys.map((k) => `"${k}"`).join(",")}) VALUES (${keys.map(() => "?").join(",")})`).run(...keys.map((k) => row[k]));
  }
} else db = openReference(join(spike, ".artifact/tenant-schema.sqlite"), dbPath);
for (const { table, rows } of fixture) for (const row of rows) {
  const keys = Object.keys(row);
  db.prepare(`INSERT INTO "${table}" (${keys.map((k) => `"${k}"`).join(",")}) VALUES (${keys.map(() => "?").join(",")})`).run(...keys.map((k) => row[k]));
}
db.exec('CREATE TABLE IF NOT EXISTS "s2SavepointRows" ("_id" TEXT PRIMARY KEY, "label" TEXT NOT NULL)');
if (process.argv.includes("--provision-only")) { db.close(); process.exit(0); }
const savepointModule = await import(join(spike, ".build/savepoint-module.js"));
const savepointSteps = process.argv.includes("--savepoints") ? [
  ["S1 caught mid-batch failure", "caughtBatchFailure", "s1", "ok"], ["S2 uncaught mid-batch failure", "uncaughtBatchFailure", "s2", "error"],
  ["S3 nested savepoints, ROLLBACK TO, continue", "nestedSavepoints", "s3", "ok"], ["S4 caught SQL error without ROLLBACK TO", "caughtSqlErrorWithoutRecovery", "s4", "error"],
].map(([name, exported, prefix, expect], index) => ({ name, path: `s2.${exported}`, binding: savepointModule[exported], args: { prefix }, as: "owner", expect, now: T0 + 3_600_000 + index * 1000, intentEntropy: "5a".repeat(32), commandId: `cmd-s2-sp-${index + 1}` })) : [];
const results = [];
for (const step of [...steps, ...savepointSteps]) {
  const call = callOf(step);
  const outcome = await runOnNode(db, { ...call, permissions: () => step.as === "owner", ...(step.binding ? { binding: step.binding, argsSchema: step.binding.options.args } : {}) });
  const ok = outcome.envelope.status === "ok";
  results.push({ name: step.name, path: step.path, expect: step.expect, ok, committed: outcome.committed, envelope: outcome.envelope, stats: outcome.stats });
  const mark = (ok ? "ok" : "error") === step.expect ? "PASS" : "FAIL";
  console.log(`${mark} ${step.name} -> ${ok ? outcome.envelope.value.slice(0, 120) : outcome.envelope.message.slice(0, 300)}  [stmts ${outcome.stats.statements}, savepoints ${outcome.stats.savepoints}, decltype-dependent ${outcome.stats.decltypeDependentColumns}]`);
}
writeFileSync(flag("out") ?? join(spike, ".artifact/reference-results.json"), JSON.stringify(results, null, 2));
db.close();
