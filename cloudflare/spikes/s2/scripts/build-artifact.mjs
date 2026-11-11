// Builds the Whagons module artifact exactly as `gonvex dev` does
// (packages/gonvex/src/index.ts collectBackendSources + buildManifest), then
// writes the complete bundle, the Query/Reducer split bundle and the function
// manifest into ../.artifact/ (git-ignored).
//
// Usage: node scripts/build-artifact.mjs /path/to/whagons5-client
import { readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "../../../..");
const { buildModuleArtifact, moduleSourceFiles } = await import(join(repo, "packages/gonvex/dist/module-artifact.js"));

const root = resolve(process.argv[2] ?? "");
if (!existsSync(join(root, "gonvex.json"))) throw new Error(`no gonvex.json in ${root}`);
const config = JSON.parse(await readFile(join(root, "gonvex.json"), "utf8"));

async function migrationFiles(dir) {
  if (!existsSync(dir)) return [];
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...await migrationFiles(path));
    else if (entry.isFile() && entry.name.endsWith(".sql")) files.push(path);
  }
  return files.sort();
}

const started = performance.now();
const backendDir = join(root, "gonvex");
const artifact = await buildModuleArtifact({
  root,
  backendDir,
  files: await moduleSourceFiles(backendDir),
  migrations: await migrationFiles(join(root, "migrations")),
  entrypoint: config.module?.entrypoint,
  bundle: config.module?.bundle,
  split: config.module?.split === true,
});
const buildMs = Math.round(performance.now() - started);

const out = resolve(here, "../.artifact");
await mkdir(out, { recursive: true });
const complete = Buffer.from(artifact.javascript.code, "base64");
await writeFile(join(out, "module.js"), complete);
let reducers;
if (artifact.reducerBundle) {
  reducers = Buffer.from(artifact.reducerBundle.code, "base64");
  await writeFile(join(out, "module.reducers.js"), reducers);
}
const functions = Object.fromEntries(Object.entries(artifact.functions).map(([path, entry]) => [path, {
  kind: entry.kind, export: entry.export, file: entry.file, internal: entry.internal ?? false,
  args: entry.args, result: entry.result, actionCapabilities: entry.actionCapabilities,
}]));
await writeFile(join(out, "functions.json"), JSON.stringify(functions));
const counts = {};
for (const entry of Object.values(artifact.functions)) counts[entry.kind] = (counts[entry.kind] ?? 0) + 1;
const summary = {
  artifactHash: artifact.hash, generation: artifact.generation, buildMs,
  completeBytes: complete.length, reducerBundleBytes: reducers?.length ?? null,
  reducerBundleFunctions: artifact.reducerBundle?.functions.length ?? null,
  functionCounts: counts, migrations: Object.keys(artifact.files).filter((f) => f.endsWith(".sql")).length,
};
await writeFile(join(out, "summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify(summary, null, 2));
