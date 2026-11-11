// Runs real Whagons Queries and Reducers through the K04 module host in local
// workerd (wrangler dev --local) and measures rows read with the catalog cache
// off (Rust's per-invocation catalogs, as S2 measured) and on.
//
// Inputs: S2's artifacts (cloudflare/spikes/s2/scripts/build-artifact.mjs on
// whagons5-client 2457bb65f, then scripts/provision-reference.mjs and
// scripts/subset-schema.mjs): module.reducers.js, functions.json,
// summary.json and schema-subset.json. The workload is S2's
// (cloudflare/spikes/s2/test/scenarios.mjs).
//
// Usage: node bench/run.mjs <artifact dir>   -> bench/.results/whagons.json
import { spawn, execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { fixture, steps, callOf, T0 } from "../../../spikes/s2/test/scenarios.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const artifact = resolve(process.argv[2] ?? "");
execFileSync("node", [join(here, "prepare.mjs"), artifact], { stdio: "inherit" });
const functions = JSON.parse(readFileSync(join(artifact, "functions.json"), "utf8"));
const subset = JSON.parse(readFileSync(join(artifact, "schema-subset.json"), "utf8"));

rmSync(join(here, ".build/state"), { recursive: true, force: true });
const port = await new Promise((done) => { const server = createServer().listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => done(port)); }); });
const wrangler = resolve(here, "../../../node_modules/.bin/wrangler");
const child = spawn(wrangler, ["dev", "--local", "--port", String(port), "--ip", "127.0.0.1", "-c", join(here, "wrangler.jsonc"), "--persist-to", join(here, ".build/state"), "--show-interactive-dev-session=false"], { cwd: here, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, WRANGLER_SEND_METRICS: "false" } });
let log = "";
child.stdout.on("data", (chunk) => { log += chunk; });
child.stderr.on("data", (chunk) => { log += chunk; });
const base = `http://127.0.0.1:${port}`;
const started = performance.now();
for (;;) {
  try { if ((await fetch(`${base}/ping`)).ok) break; } catch { /* starting */ }
  if (child.exitCode !== null) throw new Error(`wrangler exited:\n${log}`);
  if (performance.now() - started > 120_000) throw new Error(`wrangler did not start:\n${log}`);
  await new Promise((r) => setTimeout(r, 100));
}
const readyMs = Math.round(performance.now() - started);
const post = async (path, body, tenant) => {
  const response = await fetch(`${base}${path}?tenant=${tenant}`, { method: body === undefined ? "GET" : "POST", body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await response.text();
  try { return JSON.parse(text); } catch { return { error: text, status: response.status }; }
};

// Whagons Queries have no JavaScript handler: every one is a replica, live or
// one-shot plan that the runtime executes itself (RT/execution.rs:300-330).
// Dispatching one to the module host gives Rust's InvalidArtifact.
const queries = [["boards.listMessages", { boardId: "board-1" }], ["spots.list", {}]];

async function workload(tenant, cache) {
  const meta = await post("/meta", undefined, tenant);
  const provision = await post("/provision", { statements: subset.statements, rows: [...subset.rows, ...fixture] }, tenant);
  const results = [];
  const call = async (name, path, args, extra = {}) => {
    const kind = functions[path].kind;
    const body = { cache, call: { function: path, kind, args, ...extra } };
    const t = performance.now();
    const report = await post("/invoke", body, tenant);
    results.push({
      name, path, kind, ok: report.ok, committed: report.committed, error: report.error?.message?.split("\n")[0],
      rowsRead: report.stats?.rowsRead, catalogRowsRead: report.stats?.catalogRowsRead, catalogStatements: report.stats?.catalogStatements,
      catalogHits: report.stats?.catalogHits, rowsWritten: report.stats?.rowsWritten, statements: report.stats?.statements,
      commitCheckRowsRead: report.stats?.commitCheckRowsRead, serverMs: report.serverMs, clientMs: Math.round(performance.now() - t),
    });
  };
  for (const step of steps) {
    const c = callOf(step, "whagons");
    await call(step.name, step.path, step.args, { identity: c.identity, invocation: c.invocation, now: c.now, intentEntropy: c.intentEntropy });
  }
  const owner = callOf(steps[0], "whagons");
  for (const [path, args] of queries) {
    await call(`Q ${path}`, path, args, { identity: owner.identity, invocation: { ...owner.invocation, commandId: null }, now: T0 + 10_000_000 });
  }
  const snapshot = await post("/snapshot", undefined, tenant);
  return { snapshot, meta, provision: { executed: provision.executed, failures: provision.failures?.length, refused: provision.failures?.filter((f) => f.type !== "row").map((f) => f.name) }, results };
}

let report;
try {
  const off = await workload("bench-cache-off", false);
  const on = await workload("bench-cache-on", true);
  report = { generatedAt: new Date().toISOString(), readyMs, artifact: JSON.parse(readFileSync(join(artifact, "summary.json"), "utf8")), off, on };
} finally {
  child.kill("SIGINT");
}
mkdirSync(join(here, ".results"), { recursive: true });
writeFileSync(join(here, ".results/whagons.json"), JSON.stringify(report, null, 2));

// Same database contents with and without the cache.
const tablesOff = Object.keys(report.off.snapshot), tablesOn = Object.keys(report.on.snapshot);
const differing = [...new Set([...tablesOff, ...tablesOn])].filter((table) => JSON.stringify(report.off.snapshot[table]) !== JSON.stringify(report.on.snapshot[table]));
report.snapshotTables = tablesOn.length;
report.snapshotDiffering = differing;
writeFileSync(join(here, ".results/whagons.json"), JSON.stringify(report, null, 2));

// Summary table.
const rows = report.off.results.map((a, i) => ({ a, b: report.on.results[i] }));
const same = rows.every(({ a, b }) => a.ok === b.ok && a.error === b.error && a.rowsWritten === b.rowsWritten);
const sum = (list, key) => list.reduce((total, r) => total + (r[key] ?? 0), 0);
const passing = rows.filter(({ a }) => a.ok);
console.log(`ready ${readyMs} ms; provisioned ${report.on.provision.executed} statements, refused: ${report.on.provision.refused?.length}`);
console.log("| Step | Function | Outcome | Rows read, cache off (catalog) | Rows read, cache on (catalog) | Change | Rows written |");
console.log("|---|---|---|---|---|---|---|");
for (const { a, b } of rows) {
  const change = a.rowsRead ? `${Math.round((1 - b.rowsRead / a.rowsRead) * 100)} %` : "";
  console.log(`| ${a.name.split(" ")[0]} | \`${a.path}\` | ${a.ok ? "ok" : `error: ${String(a.error).slice(0, 80)}`} | ${a.rowsRead} (${a.catalogRowsRead}) | ${b.rowsRead} (${b.catalogRowsRead}) | ${change} | ${a.rowsWritten} |`);
}
const offOk = sum(passing.map((r) => r.a), "rowsRead"), onOk = sum(passing.map((r) => r.b), "rowsRead");
const offAll = sum(rows.map((r) => r.a), "rowsRead"), onAll = sum(rows.map((r) => r.b), "rowsRead");
console.log(`\noutcomes identical with and without cache: ${same}; tables compared: ${report.snapshotTables}, differing: ${JSON.stringify(differing)}`);
console.log(`loaded ${report.on.meta.functions} functions (${report.on.meta.exports} exports, ${report.on.meta.unresolved} without a handler) in ${report.on.meta.loadMs} ms`);
console.log(`passing calls: ${passing.length}; rows read ${offOk} -> ${onOk} (${Math.round((1 - onOk / offOk) * 100)} % fewer)`);
console.log(`all calls: rows read ${offAll} -> ${onAll} (${Math.round((1 - onAll / offAll) * 100)} % fewer)`);
