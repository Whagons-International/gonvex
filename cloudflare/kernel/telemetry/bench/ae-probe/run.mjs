/**
 * K15 platform probe: does an Analytics Engine dataset named with the
 * `gonvex-cf-dev-` prefix accept writes from the kernel's sink, and how soon
 * are they queryable?
 *
 *   whagons-dev credentials exec full-cloudflare-access -- node kernel/telemetry/bench/ae-probe/run.mjs
 *
 * Deploys gonvex-cf-dev-k15-ae-probe, writes 3 × 100 rows, polls the
 * Analytics Engine SQL API until they are counted (at most 5 minutes), then
 * deletes the Worker. The dataset keeps the rows for the platform's three
 * months; nothing else is created. Credentials are never printed.
 */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const PACKAGE = join(HERE, "../../../..");
const DATASET = "gonvex-cf-dev-k15-telemetry";
const { accountId, apiToken } = JSON.parse(readFileSync(process.env.WHAGONS_CREDENTIAL_FILE, "utf8"));
const env = { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId, CLOUDFLARE_API_TOKEN: apiToken, WRANGLER_SEND_METRICS: "false" };
delete env.WHAGONS_CREDENTIAL_FILE;
const wrangler = (...args) => {
  const r = spawnSync("pnpm", ["exec", "wrangler", ...args, "--config", join(HERE, "wrangler.jsonc")], { cwd: PACKAGE, env, encoding: "utf8" });
  return { code: r.status, out: `${r.stdout}\n${r.stderr}` };
};
const sql = async (query) => {
  const r = await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/analytics_engine/sql`, { method: "POST", headers: { authorization: `Bearer ${apiToken}` }, body: query });
  return { status: r.status, text: await r.text() };
};

const run = `r${Date.now()}`;
const report = { dataset: DATASET, run };
try {
  const deployed = wrangler("deploy");
  report.deploy = deployed.code === 0 ? "ok" : deployed.out.split("\n").filter((l) => /error|✘/i.test(l)).slice(0, 5);
  const url = deployed.out.match(/https:\/\/gonvex-cf-dev-k15-ae-probe\.[^\s]+/)?.[0];
  if (deployed.code !== 0 || !url) throw new Error("deploy failed");
  report.writes = [];
  for (let i = 0; i < 3; i++) {
    for (let attempt = 0; attempt < 30; attempt++) {
      const r = await fetch(`${url}/write?n=100&run=${run}`, { method: "POST" }).catch(() => null);
      if (r?.ok) { report.writes.push(await r.json()); break; }
      await new Promise((resolve) => setTimeout(resolve, 2_000));
    }
  }
  const started = Date.now();
  while (Date.now() - started < 300_000) {
    const r = await sql(`SELECT count() AS n, sum(_sample_interval) AS weighted FROM "${DATASET}" WHERE index1 = 'k15-probe-${run}' FORMAT JSON`);
    report.query = { status: r.status, body: r.text.slice(0, 300) };
    if (r.status === 200 && /"n":\s*"?([1-9]\d*)/.test(r.text)) { report.visibleAfterMs = Date.now() - started; break; }
    if (r.status !== 200 && !/does not exist|unknown table/i.test(r.text)) break;
    await new Promise((resolve) => setTimeout(resolve, 10_000));
  }
} catch (error) {
  report.error = String(error?.message ?? error);
} finally {
  const deleted = wrangler("delete", "--force");
  report.deleted = deleted.code === 0;
  console.log(JSON.stringify(report, null, 1));
}
