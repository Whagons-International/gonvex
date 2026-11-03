#!/usr/bin/env node
// Builds a project's module artifact with this checkout's CLI bundler and
// writes `{ "module": <artifact> }` for driver.py.
//
//   node bench/v8-memory/build-artifact.mjs <project-root> <out.json> [--split]
//
// The project's gonvex.json supplies the entrypoint and bundle path; --split
// forces `module.split` on regardless of gonvex.json.
import { readdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";

import { buildModuleArtifact, moduleSourceFiles } from "../../packages/gonvex/dist/module-artifact.js";

const [root, out, ...flags] = process.argv.slice(2);
if (!root || !out) throw new Error("usage: build-artifact.mjs <project-root> <out.json> [--split]");
const config = JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));
const backendDir = join(root, "gonvex");
const migrationsDir = join(root, "migrations");
const migrations = existsSync(migrationsDir)
  ? (await readdir(migrationsDir)).filter((name) => name.endsWith(".sql")).map((name) => join(migrationsDir, name))
  : [];
const started = performance.now();
const artifact = await buildModuleArtifact({
  root,
  backendDir,
  files: await moduleSourceFiles(backendDir),
  migrations,
  entrypoint: config.module?.entrypoint,
  bundle: config.module?.bundle,
  split: flags.includes("--split") || config.module?.split === true,
});
await writeFile(out, JSON.stringify({ module: artifact }));
const size = (bundle) => bundle ? Buffer.from(bundle.code, "base64").length : 0;
console.log(JSON.stringify({
  ms: Math.round(performance.now() - started),
  hash: artifact.hash,
  completeBytes: size(artifact.javascript),
  reducerBytes: size(artifact.reducerBundle),
  reducerFunctions: artifact.reducerBundle?.functions.length ?? 0,
  functions: Object.keys(artifact.functions).length,
}));
