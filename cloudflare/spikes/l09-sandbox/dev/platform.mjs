// Deploys dev/ as gonvex-cf-dev-l09-sandbox, runs every probe, writes
// results/platform.json, and deletes the Worker. Run from cloudflare/spikes/l09-sandbox:
//   whagons-dev credentials exec full-cloudflare-access -- node dev/platform.mjs
// Credentials are read from $WHAGONS_CREDENTIAL_FILE and never printed.
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const dev = fileURLToPath(new URL(".", import.meta.url));
const credentials = JSON.parse(readFileSync(process.env.WHAGONS_CREDENTIAL_FILE, "utf8"));
const childEnv = { ...process.env, CLOUDFLARE_ACCOUNT_ID: credentials.accountId, CLOUDFLARE_API_TOKEN: credentials.apiToken };
const wrangler = (args, input) => execFileSync("npx", ["wrangler", ...args], { cwd: dev, env: childEnv, input, encoding: "utf8", stdio: [input === undefined ? "ignore" : "pipe", "pipe", "pipe"] });
const key = randomBytes(24).toString("hex");
const cases = ["network", "isolation", "deadline", "cpuSpin", "microtaskSpin", "hostCallSpin", "afterSpin", "memory64", "memory", "analysis", "largeImport"];
const results = { worker: "gonvex-cf-dev-l09-sandbox", runAt: new Date().toISOString(), cases: {} };
try {
  const deployed = wrangler(["deploy"]);
  const url = /https:\/\/gonvex-cf-dev-l09-sandbox\.[\w.-]+\.workers\.dev/.exec(deployed)?.[0];
  if (!url) throw new Error("no workers.dev URL in the deploy output");
  wrangler(["secret", "put", "K20_KEY"], key);
  await new Promise((resolve) => setTimeout(resolve, 5_000));
  for (const name of cases) {
    const started = Date.now();
    const response = await fetch(`${url}/probe/${name}`, { method: "POST", headers: { "x-k20-key": key } });
    const body = await response.text();
    let parsed;
    try { parsed = JSON.parse(body); } catch { parsed = { status: response.status, body: body.slice(0, 500) }; }
    results.cases[name] = { clientMs: Date.now() - started, ...parsed };
    console.log(name, response.status, `${Date.now() - started} ms`);
  }
} finally {
  try { wrangler(["delete", "--name", "gonvex-cf-dev-l09-sandbox", "--force"]); console.log("deleted gonvex-cf-dev-l09-sandbox"); } catch (error) { console.error("delete failed; delete gonvex-cf-dev-l09-sandbox by hand"); }
  writeFileSync(new URL("../results/platform.json", import.meta.url), `${JSON.stringify(results, null, 2)}\n`);
}
