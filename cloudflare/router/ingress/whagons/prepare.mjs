// Copies the four Whagons functions the ingress calls, and the complete module
// bundle that holds them, out of an S2 artifact into ../.build/ (git-ignored:
// the app is private and this repository is public). test/whagons.test.ts then
// runs the real Actions behind the ingress.
//
//   node cloudflare/spikes/s2/scripts/build-artifact.mjs <whagons5-client checkout>
//   node cloudflare/router/ingress/whagons/prepare.mjs cloudflare/spikes/s2/.artifact
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const PATHS = ["calling.ingestWebhook", "relay.ingestTelnyxWebhook", "github.ingestWebhook", "taskPublicShares.getPublicPreviewByToken"];

const here = dirname(fileURLToPath(import.meta.url));
const artifact = resolve(process.argv[2] ?? "");
const out = join(here, "..", ".build");
mkdirSync(out, { recursive: true });
const functions = JSON.parse(readFileSync(join(artifact, "functions.json"), "utf8"));
const summary = JSON.parse(readFileSync(join(artifact, "summary.json"), "utf8"));
const picked = {};
for (const path of PATHS) {
  if (!functions[path]) throw new Error(`the artifact has no ${path}`);
  picked[path] = functions[path];
}
// `.module.js`: the test config gives it the kernel prelude, as the Worker build does.
copyFileSync(join(artifact, "module.js"), join(out, "whagons.module.js"));
writeFileSync(join(out, "whagons.functions.json"), JSON.stringify(picked, null, 2));
writeFileSync(join(out, "whagons.summary.json"), JSON.stringify(summary, null, 2));
console.log(JSON.stringify({ out, artifactHash: summary.artifactHash, functions: PATHS.length }));
