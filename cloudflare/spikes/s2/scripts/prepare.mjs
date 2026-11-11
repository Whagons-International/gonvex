// Assembles .build/: the spike sources, the published runtime-contract files,
// local-runtime's validator, the app bundle and its function manifest, plus a
// workerd config. Node (reference) and workerd load the same files.
//
// Usage: node scripts/prepare.mjs [--bundle reducers|complete|none] [--port N]
import { cpSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const spike = resolve(here, "..");
const repo = resolve(spike, "../../..");
const args = process.argv.slice(2);
const option = (name, fallback) => { const index = args.indexOf(`--${name}`); return index >= 0 ? args[index + 1] : fallback; };
const bundle = option("bundle", "reducers");
const port = Number(option("port", "18790"));
const out = join(spike, ".build");
const artifact = join(spike, ".artifact");

rmSync(out, { recursive: true, force: true });
mkdirSync(join(out, "contract"), { recursive: true });
mkdirSync(join(out, "local-runtime"), { recursive: true });
cpSync(join(spike, "src"), out, { recursive: true });
for (const file of ["globals.js", "runner.js", "vectors.json"]) cpSync(join(repo, "packages/module-sdk/runtime-contract", file), join(out, "contract", file));
cpSync(join(repo, "packages/local-runtime/dist/validation.js"), join(out, "local-runtime/validation.js"));
const bundleFile = { reducers: "module.reducers.js", complete: "module.js" }[bundle];
if (bundleFile) cpSync(join(artifact, bundleFile), join(out, "app.js"));
else writeFileSync(join(out, "app.js"), "export {};\n");
cpSync(join(artifact, "functions.json"), join(out, "functions.json"));
// The synthetic savepoint module, bundled with the real module SDK.
const { rolldown } = await import(join(repo, "packages/gonvex/node_modules/rolldown/dist/index.mjs"));
const savepointBundle = await rolldown({ input: join(spike, "test/savepoint-module.js"), platform: "neutral", resolve: { alias: { "@gonvex/module-sdk": join(repo, "packages/module-sdk/dist/index.js") } } });
await savepointBundle.write({ file: join(out, "savepoint-module.js"), format: "esm" });
await savepointBundle.close();

const modules = [
  ["worker.js", "esModule"], ["runtime-globals.js", "esModule"], ["contract/globals.js", "esModule"], ["contract/runner.js", "esModule"],
  ["contract/vectors.json", "json"], ["functions.json", "json"],
  ["local-runtime/validation.js", "esModule"], ["bootstrap.js", "esModule"], ["host.js", "esModule"], ["sql.js", "esModule"],
  ["engines.js", "esModule"], ["invoke.js", "esModule"], ["app.js", "esModule"], ["savepoint-module.js", "esModule"],
];
const storage = option("storage", join(spike, ".storage"));
mkdirSync(storage, { recursive: true });
writeFileSync(join(out, "config.capnp"), `using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [ (name = "main", worker = .tenant), (name = "disk", disk = (path = ${JSON.stringify(storage)}, writable = true)) ],
  sockets = [ (name = "http", address = "127.0.0.1:${port}", http = (), service = "main") ],
);
const tenant :Workerd.Worker = (
  modules = [
${modules.map(([name, type]) => `    (name = ${JSON.stringify(name)}, ${type} = embed ${JSON.stringify(name)}),`).join("\n")}
  ],
  compatibilityDate = "2026-10-01",
  compatibilityFlags = ["nodejs_als"],
  durableObjectNamespaces = [ (className = "Tenant", uniqueKey = "gonvex-cf-dev-s2-tenant", enableSql = true) ],
  durableObjectStorage = (localDisk = "disk"),
  bindings = [ (name = "TENANT", durableObjectNamespace = "Tenant") ],
);
`);
console.log(JSON.stringify({ out, bundle, port, bytes: readFileSync(join(out, "app.js")).length }));
