// Runs real Whagons Reducers through the K07 Reducer path (ReducerPath.execute
// → ReducerStorage → K04 host, K03 capture) in local workerd (wrangler dev
// --local), with an idempotency key per step, then replays every committed
// step with its key and checks the receipts.
//
// Inputs: S2's artifacts (cloudflare/spikes/s2/scripts/build-artifact.mjs on
// whagons5-client 2457bb65f, then scripts/provision-reference.mjs and
// scripts/subset-schema.mjs): module.reducers.js, functions.json,
// summary.json and schema-subset.json. The workload is S2's
// (cloudflare/spikes/s2/test/scenarios.mjs), as in kernel/host/bench.
//
// Usage: node bench/run.mjs <artifact dir> [K04 bench results json]
//   -> bench/.results/whagons.json and a markdown table on stdout.
// The K04 comparison uses the given kernel/host/bench/.results/whagons.json,
// else the committed steps of the K04 README table.
import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { fixture, members, steps, tenant } from "../../../spikes/s2/test/scenarios.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const artifact = resolve(process.argv[2] ?? "");
const k04File = process.argv[3] ? resolve(process.argv[3]) : undefined;
execFileSync("node", [join(here, "prepare.mjs"), artifact], { stdio: "inherit" });
const subset = JSON.parse(readFileSync(join(artifact, "schema-subset.json"), "utf8"));
const summary = JSON.parse(readFileSync(join(artifact, "summary.json"), "utf8"));

// S2's subset keeps 165 of the 530 tables but seeds the full capture metadata:
// `_gonvex_table_options` rows of absent derived tables and record
// dependencies between absent tables. K03's install refuses those, as Rust's
// does ("derived table … must declare its owner foreign key", "record
// dependency … needs a native SQL relationship"), so the bench drops the
// metadata rows whose tables the subset does not create.
const subsetTables = new Set(subset.statements.filter((s) => s.type === "table").map((s) => s.name));
const orphanFilters = {
  _gonvex_table_options: (row) => subsetTables.has(row.name),
  _gonvex_record_dependencies: (row) => subsetTables.has(row.source_table) && subsetTables.has(row.owner_storage),
};
const droppedMetadata = {};
const seedRows = subset.rows.map(({ table, rows }) => {
  const keep = orphanFilters[table];
  if (!keep) return { table, rows };
  const kept = rows.filter(keep);
  droppedMetadata[table] = rows.length - kept.length;
  return { table, rows: kept };
});

// ---- wrangler dev (one, in its own process group, always killed) ---------------------------
rmSync(join(here, ".build/state"), { recursive: true, force: true });
const port = await new Promise((done) => { const server = createServer().listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => done(port)); }); });
const wrangler = resolve(here, "../../../node_modules/.bin/wrangler");
const child = spawn(wrangler, ["dev", "--local", "--port", String(port), "--ip", "127.0.0.1", "-c", join(here, "wrangler.jsonc"), "--persist-to", join(here, ".build/state"), "--show-interactive-dev-session=false"], {
  cwd: here, stdio: ["ignore", "pipe", "pipe"], detached: true, env: { ...process.env, WRANGLER_SEND_METRICS: "false" },
});
let log = "";
child.stdout.on("data", (chunk) => { log += chunk; });
child.stderr.on("data", (chunk) => { log += chunk; });
const exited = new Promise((done) => child.once("exit", done));
async function stopWrangler() {
  if (child.exitCode !== null || child.signalCode !== null) return;
  try { process.kill(-child.pid, "SIGINT"); } catch { /* gone */ }
  const timeout = new Promise((done) => setTimeout(() => done("timeout"), 5000));
  if (await Promise.race([exited, timeout]) === "timeout") { try { process.kill(-child.pid, "SIGKILL"); } catch { /* gone */ } await exited; }
}
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => { stopWrangler().finally(() => process.exit(130)); });

const base = `http://127.0.0.1:${port}`;
const TENANT = "bench-k07";
const request = async (path, body) => {
  const response = await fetch(`${base}${path}?tenant=${TENANT}`, { method: body === undefined ? "GET" : "POST", body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await response.text();
  try { return JSON.parse(text); } catch { throw new Error(`${path}: HTTP ${response.status}: ${text.slice(0, 2000)}\n--- wrangler log ---\n${log.slice(-4000)}`); }
};

/** A tenant session for one of S2's members (session/ports.ts `TenantSession`). */
function sessionOf(key) {
  const m = members[key];
  return {
    identity: { projectId: "whagons", account: { id: m.accountId, email: m.email, emailVerified: true, name: m.name, avatarUrl: "", provider: "bench" } },
    route: { projectId: "whagons", tenantId: tenant.id, databaseUrl: "" },
    member: { id: m.id, accountId: m.accountId, status: "active", displayName: m.name, avatarUrl: "", role: m.role, permissions: {}, membershipRevision: 1 },
    admissionRevision: 0, delegation: null, developerSession: null, anonymous: false,
  };
}
const keyOf = (step) => `bench-${step.commandId}`;
const requestOf = (step) => ({ id: step.commandId, path: step.path, args: step.args, idempotencyKey: keyOf(step), intentEntropy: step.intentEntropy });

let report;
try {
  const started = performance.now();
  for (;;) {
    try { if ((await fetch(`${base}/ping`)).ok) break; } catch { /* starting */ }
    if (child.exitCode !== null) throw new Error(`wrangler exited:\n${log}`);
    if (performance.now() - started > 120_000) throw new Error(`wrangler did not start:\n${log}`);
    await new Promise((r) => setTimeout(r, 100));
  }
  const readyMs = Math.round(performance.now() - started);
  const meta = await request("/meta");
  const provision = await request("/provision", { statements: subset.statements, rows: [...seedRows, ...fixture] });
  const install = await request("/install", {});
  const stateBefore = await request("/state");

  // 1. The workload, one keyed reducer.call per step.
  const calls = [];
  for (const step of steps) {
    const t = performance.now();
    const r = await request("/call", { session: sessionOf(step.as), request: requestOf(step) });
    calls.push({ step: step.name.split(" ")[0], path: step.path, key: keyOf(step), expect: step.expect, clientMs: Math.round(performance.now() - t), ...r });
  }
  const committed = calls.filter((c) => c.ok);

  // 2. Replay every committed step with the same key: stored result, the clock's revision, no row changes.
  const before = await request("/snapshot");
  const replays = [];
  for (const call of committed) {
    const step = steps.find((s) => s.name.startsWith(`${call.step} `));
    const r = await request("/call", { session: sessionOf(step.as), request: requestOf(step) });
    const checks = {
      ok: r.ok === true,
      sameResult: r.result === call.result,
      clockRevision: r.committedRevision === r.clock?.revision,
      moduleNotRun: r.numbers === null,
      noDirectWrites: r.direct?.rowsWritten === 0,
    };
    replays.push({ step: call.step, path: call.path, committedRevision: r.committedRevision, clock: r.clock, direct: r.direct, error: r.error, checks, pass: Object.values(checks).every(Boolean) });
  }
  const after = await request("/snapshot");
  const names = [...new Set([...Object.keys(before.tables), ...Object.keys(after.tables)])].sort();
  const changed = names.filter((n) => JSON.stringify(before.tables[n]) !== JSON.stringify(after.tables[n]));

  // 3. The receipt table: one row per keyed committed step, with its result.
  const receipts = await request("/receipts");
  const byKey = new Map(receipts.map((r) => [r.idempotency_key, r]));
  const receiptChecks = {
    rows: receipts.length,
    committedKeyed: committed.length,
    onePerCommitted: receipts.length === committed.length && committed.every((c) => byKey.has(c.key)),
    resultsMatch: committed.every((c) => byKey.get(c.key)?.result === c.result && byKey.get(c.key)?.path === c.path && byKey.get(c.key)?.subject === members[steps.find((s) => s.name.startsWith(`${c.step} `)).as].accountId),
    noneForFailed: calls.filter((c) => !c.ok).every((c) => !byKey.has(c.key)),
  };
  const stateAfter = await request("/state");
  report = { generatedAt: new Date().toISOString(), readyMs, artifact: summary, meta, droppedMetadata, provision: { executed: provision.executed, refused: provision.failures.filter((f) => f.type !== "row"), rowFailures: provision.failures.filter((f) => f.type === "row") }, install, stateBefore, stateAfter, calls, replays, snapshot: { tables: names.length, changed, unreadable: before.unreadable }, receipts, receiptChecks };
} finally {
  await stopWrangler();
}
mkdirSync(join(here, ".results"), { recursive: true });
writeFileSync(join(here, ".results", "whagons.json"), JSON.stringify(report, null, 2));

// ---- K04 comparison -----------------------------------------------------------------------
// The K04 README table: the 10 committed Reducer calls of S2's workload.
let k04Committed = new Set(["01", "08", "19", "22", "24", "26", "27", "28", "29", "30"]);
let k04Source = "kernel/host/README.md table";
if (k04File && existsSync(k04File)) {
  const k04 = JSON.parse(readFileSync(k04File, "utf8"));
  k04Committed = new Set(k04.off.results.filter((r) => r.ok && !r.name.startsWith("Q ")).map((r) => r.name.split(" ")[0]));
  k04Source = k04File;
}
const k07Committed = new Set(report.calls.filter((c) => c.ok).map((c) => c.step));
const onlyK07 = [...k07Committed].filter((s) => !k04Committed.has(s));
const onlyK04 = [...k04Committed].filter((s) => !k07Committed.has(s));

// ---- table ----------------------------------------------------------------------------------
const replayOf = new Map(report.replays.map((r) => [r.step, r]));
const sum = (list, f) => list.reduce((t, x) => t + (f(x) ?? 0), 0);
console.log(`ready ${report.readyMs} ms; provisioned ${report.provision.executed} statements, refused ${report.provision.refused.length}, row failures ${report.provision.rowFailures.length}`);
console.log(`dropped orphan capture metadata rows: ${JSON.stringify(report.droppedMetadata)}`);
console.log(`change feed: ${report.install.units} units (${JSON.stringify(report.install.byKind)}), ${report.install.statements} statements, ${report.install.captureTriggers} capture triggers, failures ${report.install.failures.length}, warm plan ${report.install.warmPlanUnits} units, ${report.install.ms} ms`);
console.log(`module: ${report.meta.functions} functions (${report.meta.unresolved} without a handler), permissionGrants: ${JSON.stringify(report.meta.permissionGrants)}\n`);
console.log("| Step | Function | Outcome | committedRevision | Change rows | Rows read (of which finalize) | Rows written (of which finalize) | Replay ok |");
console.log("|---|---|---|---|---|---|---|---|");
for (const c of report.calls) {
  const outcome = c.ok ? "ok" : `error: ${String(c.error?.message ?? "").split("\n")[0].slice(0, 90)}`;
  const replay = replayOf.get(c.step);
  const n = c.numbers;
  console.log(`| ${c.step} | \`${c.path}\` | ${outcome} | ${c.committedRevision ?? ""} | ${c.ok ? c.changeRows : ""} | ${n ? `${n.rowsRead} (${c.finalize?.rowsRead ?? 0})` : "–"} | ${n ? `${n.rowsWritten} (${n.captureRowsWritten})` : "–"} | ${replay ? (replay.pass ? "yes" : `NO ${JSON.stringify(replay.checks)}`) : ""} |`);
}
const ok = report.calls.filter((c) => c.ok);
console.log(`\ncommitted: ${ok.length} of ${report.calls.length} calls (${new Set(ok.map((c) => c.path)).size} distinct Reducers): ${ok.map((c) => `${c.step} ${c.path}`).join(", ")}`);
console.log(`vs K04 (${k04Source}): same set: ${onlyK07.length === 0 && onlyK04.length === 0}; only K07: ${JSON.stringify(onlyK07)}; only K04: ${JSON.stringify(onlyK04)}`);
console.log(`rows read / written, committed calls: ${sum(ok, (c) => c.numbers?.rowsRead)} / ${sum(ok, (c) => c.numbers?.rowsWritten)} (capture ${sum(ok, (c) => c.numbers?.captureRowsWritten)}); all calls: ${sum(report.calls, (c) => c.numbers?.rowsRead)} / ${sum(report.calls, (c) => c.numbers?.rowsWritten)}`);
console.log(`of which K03 finalize: ${sum(report.calls, (c) => c.finalize?.rowsRead)} read / ${sum(report.calls, (c) => c.finalize?.rowsWritten)} written; K01 FK-model rows: ${sum(report.calls, (c) => c.numbers?.schemaRowsRead)}, catalog rows: ${sum(report.calls, (c) => c.numbers?.catalogRowsRead)}`);
console.log(`outside K01's counters: the path's direct reads (receipt table check and lookup, clock) ${sum(report.calls, (c) => c.direct?.rowsRead)} read / ${sum(report.calls, (c) => c.direct?.rowsWritten)} written; mixed-mode rowReported scan ${sum(report.calls, (c) => c.rowReportedRowsRead)} read (sqlite_schema: ${report.stateBefore.schemaObjects} objects)`);
console.log(`replays: ${report.replays.filter((r) => r.pass).length}/${report.replays.length} pass; rows read ${sum(report.replays, (r) => r.direct?.rowsRead)}, written ${sum(report.replays, (r) => r.direct?.rowsWritten)}; tables compared ${report.snapshot.tables}, changed by the replays: ${JSON.stringify(report.snapshot.changed)}, unreadable: ${JSON.stringify(Object.keys(report.snapshot.unreadable))}`);
console.log(`receipts: ${JSON.stringify(report.receiptChecks)}`);
console.log(`clock ${JSON.stringify(report.stateBefore.clock)} -> ${JSON.stringify(report.stateAfter.clock)}; change rows ${report.stateAfter.changes}, provenance rows ${report.stateAfter.transactions}, outbox rows ${report.stateAfter.outbox}`);
