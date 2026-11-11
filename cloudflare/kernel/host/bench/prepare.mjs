// Copies the Whagons Query/Reducer bundle and function manifest into .build/,
// with the kernel prelude (src/bundle.ts) the Worker build gives every app
// bundle. Input: a directory with S2's artifacts (scripts/build-artifact.mjs
// of cloudflare/spikes/s2): module.reducers.js, functions.json, summary.json.
//
// Usage: node bench/prepare.mjs <artifact dir>
import { mkdirSync, readFileSync, writeFileSync, copyFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { wrapBundle } from "../src/bundle.ts";

const here = dirname(fileURLToPath(import.meta.url));
const artifact = resolve(process.argv[2] ?? "");
const out = join(here, ".build");
mkdirSync(out, { recursive: true });
const source = readFileSync(join(artifact, "module.reducers.js"), "utf8");
writeFileSync(join(out, "app.js"), wrapBundle(source, "../../src/module-globals.ts"));
copyFileSync(join(artifact, "functions.json"), join(out, "functions.json"));
copyFileSync(join(artifact, "summary.json"), join(out, "summary.json"));
console.log(JSON.stringify({ out, bundleBytes: source.length }));
