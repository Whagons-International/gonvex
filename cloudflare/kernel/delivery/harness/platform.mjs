#!/usr/bin/env node
// Deploys the K10 harness Worker (kernel/delivery/harness) to the development
// account for a platform run of the replica scenarios, and deletes it again.
// Only names starting with `gonvex-cf-dev-` are ever touched (AGENT_BRIEF).
//
//   whagons-dev credentials exec full-cloudflare-access -- node kernel/delivery/harness/platform.mjs deploy <name>
//     → prints {"url": ..., "key": ...} (the stub admin key is a fresh random value, not a credential)
//   GONVEX_CONFORMANCE_CF_URL=<url> GONVEX_CONFORMANCE_CF_KEY=<key> node conformance/src/cli.mjs \
//     --backend rust=rust --backend cf=cloudflare-delivery --scenario reducer-second-client …
//   whagons-dev credentials exec full-cloudflare-access -- node kernel/delivery/harness/platform.mjs delete <name>
//
// Credentials come only from the file `whagons-dev` names; nothing here prints them.
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const CLOUDFLARE = join(HERE, "../../..");
const CONFORMANCE = join(CLOUDFLARE, "conformance");

function credentials() {
  const file = process.env.WHAGONS_CREDENTIAL_FILE ?? process.env.FULL_CLOUDFLARE_ACCESS_FILE;
  if (!file) throw new Error("run under `whagons-dev credentials exec full-cloudflare-access --`");
  let parsed;
  try { parsed = JSON.parse(readFileSync(file, "utf8")); } catch { throw new Error("credential file is not valid JSON (contents not shown)"); }
  if (typeof parsed?.accountId !== "string" || typeof parsed?.apiToken !== "string") throw new Error("credential is missing accountId or apiToken");
  return { CLOUDFLARE_ACCOUNT_ID: parsed.accountId, CLOUDFLARE_API_TOKEN: parsed.apiToken };
}

function wrangler(args, env, input) {
  const result = spawnSync(join(CLOUDFLARE, "node_modules/.bin/wrangler"), args, {
    cwd: CLOUDFLARE, env: { ...process.env, ...env, WRANGLER_SEND_METRICS: "false", CI: "1" }, encoding: "utf8", input,
  });
  if (result.status !== 0) throw new Error(`wrangler ${args[0]} failed:\n${(result.stderr || result.stdout).slice(-2000)}`);
  return result.stdout;
}

const [command, name] = process.argv.slice(2);
if (!name || !/^gonvex-cf-dev-[a-z0-9-]+$/.test(name)) throw new Error("usage: platform.mjs deploy|delete gonvex-cf-dev-<name>");

if (command === "deploy") {
  // The fixture bundles of a conformance run, built the way the local adapter builds them.
  const work = mkdtempSync(join(tmpdir(), "k10-platform-"));
  const { buildFixture } = await import(join(CONFORMANCE, "src/fixture.mjs"));
  const manifests = [];
  for (const generation of [1, 2, 3]) {
    const { manifest } = await buildFixture(work, generation);
    const path = join(work, `manifest-v${generation}.json`);
    writeFileSync(path, JSON.stringify(manifest));
    manifests.push(path);
  }
  const { generateBundles } = await import(join(CLOUDFLARE, "kernel/reducer/harness/generate-bundles.mjs"));
  await generateBundles(manifests);
  const key = `platform-stub-${randomBytes(16).toString("hex")}`;
  const output = wrangler(["deploy", "--config", join(HERE, "wrangler.jsonc"), "--name", name, "--var", `GONVEX_STUB_ADMIN_KEY:${key}`], credentials());
  const url = /https:\/\/[^\s]+\.workers\.dev/.exec(output)?.[0];
  if (!url) throw new Error("deployed, but no workers.dev URL in wrangler's output");
  console.log(JSON.stringify({ url, key }));
} else if (command === "delete") {
  wrangler(["delete", "--name", name, "--force"], credentials());
  console.log(JSON.stringify({ deleted: name }));
} else {
  throw new Error("usage: platform.mjs deploy|delete gonvex-cf-dev-<name>");
}

