// Deploys dev/ as gonvex-cf-dev-k20-sandbox (the Worker and its Container image),
// runs every probe, writes dev/results/platform.json, and deletes the Worker and
// its Container application. Needs docker. Run from cloudflare/:
//   whagons-dev credentials exec full-cloudflare-access -- node sandbox/dev/platform.mjs
// Credentials are read from $WHAGONS_CREDENTIAL_FILE and never printed.
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const dev = fileURLToPath(new URL(".", import.meta.url));
const credentials = JSON.parse(readFileSync(process.env.WHAGONS_CREDENTIAL_FILE, "utf8"));
const childEnv = { ...process.env, CLOUDFLARE_ACCOUNT_ID: credentials.accountId, CLOUDFLARE_API_TOKEN: credentials.apiToken };
const wrangler = (args, input) => execFileSync("npx", ["wrangler", ...args], { cwd: dev, env: childEnv, input, encoding: "utf8", maxBuffer: 64 << 20, stdio: [input === undefined ? "ignore" : "pipe", "pipe", "pipe"] });
const key = randomBytes(24).toString("hex");
const keep = process.argv.includes("--keep");
const results = { worker: "gonvex-cf-dev-k20-sandbox", instanceType: "basic", runAt: new Date().toISOString(), cases: {} };
const probe = async (url, name, tenant) => {
  const started = Date.now();
  const response = await fetch(`${url}/probe/${name}${tenant ? `?tenant=${tenant}` : ""}`, { method: "POST", headers: { "x-k20-key": key } });
  const body = await response.text();
  let parsed;
  try { parsed = JSON.parse(body); } catch { parsed = { status: response.status, body: body.slice(0, 500) }; }
  console.log(name, response.status, `${Date.now() - started} ms`);
  return { clientMs: Date.now() - started, ...parsed };
};
try {
  const deployed = wrangler(["deploy"]);
  const url = /https:\/\/gonvex-cf-dev-k20-sandbox\.[\w.-]+\.workers\.dev/.exec(deployed)?.[0];
  if (!url) throw new Error("no workers.dev URL in the deploy output");
  wrangler(["secret", "put", "K20_KEY"], key);
  await new Promise((resolve) => setTimeout(resolve, 20_000));
  // Cold starts: three new tenants, each starting its own Container.
  for (let i = 1; i <= 3; i++) results.cases[`coldStart${i}`] = await probe(url, "coldStart");
  // Every other probe on one warm tenant, then the same tenant's stats.
  const tenant = `tenant-warm-${randomBytes(4).toString("hex")}`;
  for (const name of ["network", "isolation", "deadline", "cpuSpin", "heap", "analysis", "largeImport", "stats"]) {
    results.cases[name] = await probe(url, name, tenant);
  }
} finally {
  if (!keep) {
    try { wrangler(["delete", "--name", "gonvex-cf-dev-k20-sandbox", "--force"]); console.log("deleted gonvex-cf-dev-k20-sandbox"); } catch (error) { console.error("worker delete failed; delete gonvex-cf-dev-k20-sandbox by hand"); }
    try {
      const listed = JSON.parse(wrangler(["containers", "list", "--json"]));
      for (const app of listed.filter((entry) => String(entry.name ?? "").startsWith("gonvex-cf-dev-k20-sandbox"))) {
        wrangler(["containers", "delete", app.id], "y\n");
        console.log(`deleted container application ${app.name}`);
      }
    } catch (error) { console.error(`container application cleanup: ${error.message.split("\n")[0]}`); }
  }
  writeFileSync(new URL("./results/platform.json", import.meta.url), `${JSON.stringify(results, null, 2)}\n`);
}
