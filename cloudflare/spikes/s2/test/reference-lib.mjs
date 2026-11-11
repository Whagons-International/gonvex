// node:sqlite reference: the same .build host and bootstrap, on a full
// Whagons tenant schema (scripts/provision-reference.mjs), with real
// BEGIN/SAVEPOINT and decltype fallback (as the Rust runtime has).
import { DatabaseSync } from "node:sqlite";
import { copyFileSync, rmSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const build = join(spike, ".build");
await import(join(build, "runtime-globals.js"));
const app = await import(join(build, "app.js"));
const { invoke } = await import(join(build, "invoke.js"));
const { NodeEngine } = await import(join(build, "engines.js"));
const { readFileSync } = await import("node:fs");
export const functions = JSON.parse(readFileSync(join(build, "functions.json"), "utf8"));
export { app };

export function openReference(schemaFile, copyTo) {
  rmSync(copyTo, { force: true });
  copyFileSync(schemaFile, copyTo);
  const db = new DatabaseSync(copyTo);
  db.function("regexp", { deterministic: true }, (pattern, value) => (value == null ? null : new RegExp(String(pattern)).test(String(value)) ? 1 : 0));
  db.exec("PRAGMA foreign_keys=ON");
  return db;
}

export function runOnNode(db, call) {
  const engine = new NodeEngine(db);
  return invoke({
    binding: app[call.path], argsSchema: functions[call.path]?.args, ...call, engine,
    transaction: (run) => engine.transaction(run), decltypeFallback: true,
    schedulable: (path) => !!functions[path] && functions[path].kind !== "query",
  });
}
