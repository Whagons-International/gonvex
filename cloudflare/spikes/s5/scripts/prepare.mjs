// Assembles the two deployable Workers and the generation uploads.
//
//   .build/loader/  gonvex-cf-dev-s5        app.js empty; generations load through env.LOADER
//   .build/baked/   gonvex-cf-dev-s5-baked  app.js = the Whagons Query/Reducer bundle (S2 / 0016 G0-2)
//   .artifact/upload/<code>/{app.js,functions.json,kinds.json}  generations for the Bundles object
//
// Kernel modules come from S2 (cloudflare/spikes/s2/src) unchanged, plus this spike's
// invoke-tx.js and gen.js. The app bundle and function manifest come from S2's
// scripts/build-artifact.mjs (whagons5-client 2457bb65f), copied into .artifact/.
//
// Usage: node scripts/prepare.mjs [--built <gonvex checkout with built packages>]
//   --built defaults to this checkout; it needs packages/local-runtime/dist,
//   packages/module-sdk/dist and packages/gonvex/node_modules/rolldown
//   (pnpm --filter @gonvex/cli... build).
import { cpSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const spike = resolve(here, "..");
const repo = resolve(spike, "../../..");
const s2 = resolve(spike, "../s2");
const args = process.argv.slice(2);
const option = (name, fallback) => { const index = args.indexOf(`--${name}`); return index >= 0 ? args[index + 1] : fallback; };
const built = resolve(option("built", repo));
const artifact = join(spike, ".artifact");

/** Kernel module sources, by the module name the generation imports them under. */
const kernelSources = {
  "gen.js": join(spike, "src/gen.js"),
  "invoke-tx.js": join(spike, "src/invoke-tx.js"),
  "runtime-globals.js": join(s2, "src/runtime-globals.js"),
  "bootstrap.js": join(s2, "src/bootstrap.js"),
  "host.js": join(s2, "src/host.js"),
  "sql.js": join(s2, "src/sql.js"),
  "engines.js": join(s2, "src/engines.js"),
  "contract/globals.js": join(repo, "packages/module-sdk/runtime-contract/globals.js"),
  "local-runtime/validation.js": join(built, "packages/local-runtime/dist/validation.js"),
};

// The synthetic savepoint module from S2, bundled with the real module SDK.
const { rolldown } = await import(join(built, "packages/gonvex/node_modules/rolldown/dist/index.mjs"));
const savepointBundle = await rolldown({ input: join(s2, "test/savepoint-module.js"), platform: "neutral", resolve: { alias: { "@gonvex/module-sdk": join(built, "packages/module-sdk/dist/index.js") } } });
const { output } = await savepointBundle.generate({ format: "esm" });
await savepointBundle.close();
const savepointSource = output[0].code;

const kernel = Object.fromEntries(Object.entries(kernelSources).map(([name, path]) => [name, readFileSync(path, "utf8")]));
kernel["savepoint-module.js"] = savepointSource;

const appSource = readFileSync(join(artifact, "module.reducers.js"), "utf8");
const functionsSource = readFileSync(join(artifact, "functions.json"), "utf8");
const kinds = Object.fromEntries(Object.entries(JSON.parse(functionsSource)).map(([path, entry]) => [path, { kind: entry.kind }]));

for (const variant of ["loader", "baked"]) {
  const out = join(spike, ".build", variant);
  rmSync(out, { recursive: true, force: true });
  mkdirSync(join(out, "contract"), { recursive: true });
  mkdirSync(join(out, "local-runtime"), { recursive: true });
  for (const [name, source] of Object.entries(kernel)) writeFileSync(join(out, name), source);
  cpSync(join(spike, "src/loader.js"), join(out, "loader.js"));
  writeFileSync(join(out, "kernel-modules.json"), JSON.stringify(kernel));
  writeFileSync(join(out, "build.json"), JSON.stringify({ variant }));
  writeFileSync(join(out, "app.js"), variant === "baked" ? appSource : "export {};\n");
  writeFileSync(join(out, "functions.json"), variant === "baked" ? functionsSource : "{}");
  writeFileSync(join(out, "wrangler.jsonc"), JSON.stringify({
    name: variant === "baked" ? "gonvex-cf-dev-s5-baked" : "gonvex-cf-dev-s5",
    main: "loader.js",
    compatibility_date: "2026-11-01",
    compatibility_flags: ["nodejs_als"],
    workers_dev: true,
    preview_urls: false,
    observability: { enabled: true, head_sampling_rate: 1 },
    durable_objects: { bindings: [{ name: "TENANT", class_name: "Tenant" }, { name: "BUNDLES", class_name: "Bundles" }, { name: "GATEWAY", class_name: "Gateway" }] },
    migrations: [{ tag: "v1", new_sqlite_classes: ["Tenant", "Bundles"] }, { tag: "v2", new_sqlite_classes: ["Gateway"] }],
    worker_loaders: [{ binding: "LOADER" }],
    vars: { BUILD: "a" },
  }, null, 2));
}

// Generations for the Bundles object: the real app, and an empty app for the probes.
const uploads = join(artifact, "upload");
rmSync(uploads, { recursive: true, force: true });
for (const [code, files] of Object.entries({
  whagons: { "app.js": appSource, "functions.json": functionsSource, "kinds.json": JSON.stringify(kinds) },
  tiny: { "app.js": "export {};\n", "functions.json": "{}", "kinds.json": "{}" },
})) {
  mkdirSync(join(uploads, code), { recursive: true });
  for (const [name, text] of Object.entries(files)) writeFileSync(join(uploads, code, name), text);
}

const kernelBytes = Object.values(kernel).reduce((sum, text) => sum + Buffer.byteLength(text), 0);
console.log(JSON.stringify({ kernelModules: Object.keys(kernel).length, kernelBytes, appBytes: Buffer.byteLength(appSource), functionsBytes: Buffer.byteLength(functionsSource) }));
