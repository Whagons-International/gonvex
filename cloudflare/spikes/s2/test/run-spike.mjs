// S2 driver. Starts local workerd with the Query/Reducer bundle, provisions
// the schema subset in a Durable Object, runs every step in
// test/scenarios.mjs (one storage transaction each), checks rollback for
// expected failures, runs the savepoint module and the runtime-contract
// vectors, then compares every table with the node:sqlite reference run.
//
// Prerequisites: node scripts/build-artifact.mjs <whagons5-client>,
// node scripts/provision-reference.mjs <whagons5-client>, then this file.
// Usage: node test/run-spike.mjs
import { spawn, execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, rmSync, mkdirSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:net";
import { fixture, steps, callOf, T0 } from "./scenarios.mjs";

const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const artifact = join(spike, ".artifact");
const freePort = () => new Promise((done) => { const server = createServer().listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => done(port)); }); });

/** Savepoint cases on the real SDK (test/savepoint-module.js). */
const savepointSteps = [
  { name: "S1 caught mid-batch failure", path: "s2.caughtBatchFailure", args: { prefix: "s1" }, expect: "ok", keep: ["s1-after", "s1-before"] },
  { name: "S2 uncaught mid-batch failure", path: "s2.uncaughtBatchFailure", args: { prefix: "s2" }, expect: "error", keep: [] },
  { name: "S3 nested savepoints, ROLLBACK TO, continue", path: "s2.nestedSavepoints", args: { prefix: "s3" }, expect: "ok", keep: ["s3-kept"], returns: (value) => value.clockPinned === true && value.random.length === 2 },
  { name: "S4 caught SQL error without ROLLBACK TO", path: "s2.caughtSqlErrorWithoutRecovery", args: { prefix: "s4" }, expect: "error", keep: [] },
].map((step, index) => ({ ...step, as: "owner", now: T0 + 3_600_000 + index * 1000, intentEntropy: "5a".repeat(32), commandId: `cmd-s2-sp-${index + 1}` }));
const s2Table = 'CREATE TABLE IF NOT EXISTS "s2SavepointRows" ("_id" TEXT PRIMARY KEY, "label" TEXT NOT NULL)';

async function startWorkerd(bundle) {
  const port = await freePort();
  const storage = join(spike, ".storage", bundle);
  rmSync(storage, { recursive: true, force: true });
  mkdirSync(storage, { recursive: true });
  execFileSync("node", [join(spike, "scripts/prepare.mjs"), "--bundle", bundle, "--port", String(port), "--storage", storage], { stdio: "ignore" });
  const started = performance.now();
  const child = spawn(join(spike, "node_modules/.bin/workerd"), ["serve", "--experimental", join(spike, ".build/config.capnp")], { stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const base = `http://127.0.0.1:${port}`;
  for (;;) {
    try { const response = await fetch(`${base}/ping`); if (response.ok) break; } catch { /* not listening yet */ }
    if (child.exitCode !== null) throw new Error(`workerd exited: ${stderr}`);
    await new Promise((r) => setTimeout(r, 20));
  }
  return { child, base, readyMs: Math.round(performance.now() - started), stderr: () => stderr };
}

const post = async (base, path, body, tenant = "tenant-s2") => {
  const request = () => fetch(`${base}${path}?tenant=${tenant}`, { method: body === undefined ? "GET" : "POST", body: body === undefined ? undefined : JSON.stringify(body) });
  // A pooled keep-alive socket can be closed by workerd while the driver
  // runs a long synchronous step; retry once on a fresh connection.
  // A request that failed to send never reached the object.
  let started = performance.now();
  let response;
  try { response = await request(); } catch (error) { if (error?.cause?.code !== "EPIPE" && error?.cause?.code !== "ECONNRESET") throw error; started = performance.now(); response = await request(); }
  const text = await response.text();
  let value;
  try { value = JSON.parse(text); } catch { value = { error: text }; }
  return { status: response.status, ms: performance.now() - started, value };
};

/** Normalizes a snapshot for comparison: drops wall-clock and random-UUID columns. */
const volatile = {
  _gonvex_action_outbox: (row) => { const { created_at, available_at, ...rest } = row; return rest.action_path === "_gonvex.scheduler.enqueue" ? { ...rest, id: "<random uuid>" } : rest; },
  members: ({ created_at, updated_at, ...rest }) => rest,
  _gonvex_control_plane_membership_outbox: ({ updated_at, ...rest }) => rest,
};
const normalize = (table, rows) => rows.map((row) => (volatile[table] ? volatile[table](row) : row)).map((row) => JSON.stringify(row)).sort();

function nodeSnapshot(dbPath, tables) {
  const script = `
    const { DatabaseSync } = require("node:sqlite");
    const db = new DatabaseSync(${JSON.stringify(dbPath)}, { readOnly: true });
    const out = {};
    for (const name of ${JSON.stringify(tables)}) {
      const exists = db.prepare("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?").get(name);
      if (!exists) continue;
      const order = /WITHOUT\\s+ROWID/i.test(exists.sql) ? "1" : "rowid";
      const rows = db.prepare('SELECT * FROM "' + name.replaceAll('"', '""') + '" ORDER BY ' + order).all();
      if (rows.length) out[name] = rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, v instanceof Uint8Array ? "blob:" + v.length : typeof v === "bigint" ? Number(v) : v])));
    }
    process.stdout.write(JSON.stringify(out));`;
  return JSON.parse(execFileSync("node", ["-e", script], { maxBuffer: 1 << 28, stdio: ["ignore", "pipe", "ignore"] }).toString());
}

let lastStderr;
const median = (values) => { const s = [...values].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };

async function main() {
  const subset = JSON.parse(readFileSync(join(artifact, "schema-subset.json"), "utf8"));
  const report = { generatedAt: new Date().toISOString(), subset: subset.stats };


  const worker = await startWorkerd("reducers");
  lastStderr = worker.stderr;
  // (after startWorkerd prepared .build with the Query/Reducer bundle)
  // Full-schema reference: what the steps do when the whole schema loads.
  execFileSync("node", [join(spike, "test/run-reference.mjs"), join(artifact, "reference-run.sqlite"), "--savepoints"], { stdio: "ignore" });
  const fullReference = JSON.parse(readFileSync(join(artifact, "reference-results.json"), "utf8"));

  report.workerdReadyMs = worker.readyMs;
  try {
    const meta = await post(worker.base, "/meta");
    report.firstObjectRequestMs = Math.round(meta.ms);
    report.appExports = meta.value.appExports;

    const provision = await post(worker.base, "/provision", { statements: subset.statements, rows: [...subset.rows, ...fixture], extra: [s2Table] });
    report.provision = { ms: Math.round(provision.ms), ...provision.value, failures: provision.value.failures };
    // Like-for-like reference: node:sqlite with exactly the DDL DO accepted.
    const refused = provision.value.failures.filter((f) => f.type !== "row").map((f) => f.name);
    writeFileSync(join(artifact, "do-refused-objects.json"), JSON.stringify(refused));
    execFileSync("node", [join(spike, "test/run-reference.mjs"), join(artifact, "reference-subset.sqlite"), "--savepoints", "--subset", join(artifact, "do-refused-objects.json"), "--out", join(artifact, "reference-subset-results.json")], { stdio: "ignore" });
    const reference = JSON.parse(readFileSync(join(artifact, "reference-subset-results.json"), "utf8"));

    // Provisioning parity: the DO and the like-for-like reference right after
    // provisioning, before any Reducer ran.
    execFileSync("node", [join(spike, "test/run-reference.mjs"), join(artifact, "reference-provisioned.sqlite"), "--subset", join(artifact, "do-refused-objects.json"), "--provision-only"], { stdio: "ignore" });
    const provisionedDo = (await post(worker.base, "/snapshot")).value;
    const provisionedRef = nodeSnapshot(join(artifact, "reference-provisioned.sqlite"), subset.statements.filter((s) => s.type === "table").map((s) => s.name).concat(["s2SavepointRows", "members"]));
    report.provisionParity = [...new Set([...Object.keys(provisionedDo), ...Object.keys(provisionedRef)])].sort()
      .filter((t) => JSON.stringify(normalize(t, provisionedDo[t] ?? [])) !== JSON.stringify(normalize(t, provisionedRef[t] ?? [])))
      .map((t) => ({ table: t, doRows: (provisionedDo[t] ?? []).length, referenceRows: (provisionedRef[t] ?? []).length }));

    const stepResults = [];
    for (const step of [...steps, ...savepointSteps]) {
      const call = callOf(step);
      const before = step.expect === "error" ? (await post(worker.base, "/snapshot")).value : undefined;
      const response = await post(worker.base, "/invoke", { call });
      const outcome = response.value;
      if (outcome.error) { stepResults.push({ name: step.name, path: step.path, expect: step.expect, pass: false, detail: outcome.error }); continue; }
      const ok = outcome.envelope.status === "ok";
      const result = {
        name: step.name, path: step.path, why: step.why, expect: step.expect, ok,
        detail: ok ? outcome.envelope.value.slice(0, 160) : outcome.envelope.message.slice(0, 240),
        ms: Math.round(response.ms * 10) / 10, serverMs: outcome.serverMs,
        statements: outcome.stats.statements, hostCalls: outcome.stats.calls, savepoints: outcome.stats.savepoints,
        rowsRead: outcome.stats.rowsRead, rowsWritten: outcome.stats.rowsWritten, fkGuardRowsRead: outcome.stats.fkGuardRowsRead ?? 0, fkGuardMs: outcome.stats.fkGuardMs,
        catalogStatements: outcome.stats.catalogStatements, catalogRowsRead: outcome.stats.catalogRowsRead,
        withinOneTurn: outcome.turn?.withinOneTurn, microtaskHops: outcome.turn?.hops, instance: outcome.instance,
      };
      result.pass = (ok ? "ok" : "error") === step.expect;
      if (step.expect === "error") {
        const after = (await post(worker.base, "/snapshot")).value;
        const changed = Object.keys({ ...before, ...after }).filter((table) => JSON.stringify(normalize(table, before[table] ?? [])) !== JSON.stringify(normalize(table, after[table] ?? [])));
        result.rolledBack = changed.length === 0;
        result.changedTables = changed;
        result.pass &&= result.rolledBack;
      }
      if (step.keep) {
        const rows = ((await post(worker.base, "/snapshot")).value.s2SavepointRows ?? []).map((row) => row._id).filter((id) => id.startsWith(`${step.args.prefix}-`)).sort();
        result.savepointRows = rows;
        result.pass &&= JSON.stringify(rows) === JSON.stringify([...step.keep].sort());
      }
      if (step.returns) result.pass &&= ok && step.returns(JSON.parse(outcome.envelope.value));
      const ref = reference.find((r) => r.name === step.name);
      const full = fullReference.find((r) => r.name === step.name);
      result.fullSchemaReference = full ? { ok: full.ok, pass: (full.ok ? "ok" : "error") === step.expect, detail: full.ok ? full.envelope.value.slice(0, 120) : full.envelope.message.slice(0, 200) } : undefined;
      const missing = refused.find((name) => result.detail.includes(name));
      if (missing) result.blockedBy = missing;
      // A step that fails in the DO for a different reason than on the full
      // schema (a refused object, a fixture row that could not be created) is
      // not evidence for what it was meant to test.
      const errorKind = (message) => String(message).replace(/^\w+Error: /, "").replace(/^error returned from database: \(code: \d+\) /, "").replace(/ \([^)]*\)$/, "");
      if (full && full.ok === ok && !ok && errorKind(full.envelope.message) !== errorKind(outcome.envelope.message)) result.vacuous = true;
      if (full && full.ok !== ok) result.blockedByFixture = !missing;
      result.reference = ref ? { ok: ref.ok, detail: ref.ok ? ref.envelope.value.slice(0, 160) : ref.envelope.message.slice(0, 240), decltypeDependentColumns: ref.stats.decltypeDependentColumns } : undefined;
      result.sameOutcomeAsReference = ref ? ref.ok === ok && (ok ? ref.envelope.value === outcome.envelope.value : true) : undefined;
      stepResults.push(result);
      console.log(`${result.pass ? "PASS" : result.blockedBy ? "BLOCKED" : "FAIL"} ${step.name} ${result.ok ? "ok" : "error"} ${result.detail.slice(0, 100)} | ${result.statements} stmts, ${result.rowsRead} read, ${result.rowsWritten} written, sp ${result.savepoints}, turn ${result.withinOneTurn} | ref ${result.sameOutcomeAsReference ? "same" : "DIFF"}`);
    }
    report.steps = stepResults;

    // Row-level comparison with the reference.
    const finalDo = (await post(worker.base, "/snapshot")).value;
    const tables = [...new Set([...Object.keys(finalDo)])];
    const finalRef = nodeSnapshot(join(artifact, "reference-subset.sqlite"), subset.statements.filter((s) => s.type === "table").map((s) => s.name).concat(["s2SavepointRows"]));
    const allTables = [...new Set([...Object.keys(finalDo), ...Object.keys(finalRef)])].sort();
    const comparison = allTables.map((table) => {
      const a = normalize(table, finalDo[table] ?? []), b = normalize(table, finalRef[table] ?? []);
      const same = JSON.stringify(a) === JSON.stringify(b);
      return { table, doRows: a.length, referenceRows: b.length, same, ...(same ? {} : { onlyDo: a.filter((x) => !b.includes(x)).slice(0, 3), onlyReference: b.filter((x) => !a.includes(x)).slice(0, 3) }) };
    });
    report.comparison = { tables: comparison.length, identical: comparison.filter((c) => c.same).length, rows: comparison.reduce((n, c) => n + c.doRows, 0), differences: comparison.filter((c) => !c.same) };

    // Implicit mode: no transaction API. A: a Reducer that writes and then
    // throws leaks its writes. B: a deferred foreign-key violation surfaces
    // at the platform's implicit commit, after the event.
    const implicitTenant = "tenant-implicit";
    await post(worker.base, "/provision", { statements: subset.statements, rows: [...subset.rows, ...fixture], extra: [s2Table] }, implicitTenant);
    await post(worker.base, "/invoke", { call: callOf(steps.find((s) => s.path === "cleaningStatuses.create")) }, implicitTenant);
    const implicitCase = async (step) => {
      const before = (await post(worker.base, "/snapshot", undefined, implicitTenant)).value;
      const instanceBefore = (await post(worker.base, "/meta", undefined, implicitTenant)).value.instance;
      const response = await post(worker.base, "/invoke", { call: callOf(step), mode: "implicit" }, implicitTenant);
      const after = (await post(worker.base, "/snapshot", undefined, implicitTenant)).value;
      const instanceAfter = (await post(worker.base, "/meta", undefined, implicitTenant)).value.instance;
      return {
        step: step.name, httpStatus: response.status, status: response.value.envelope?.status, message: (response.value.envelope?.message ?? response.value.error ?? "").slice(0, 200),
        leakedTables: Object.keys({ ...before, ...after }).filter((t) => JSON.stringify(normalize(t, before[t] ?? [])) !== JSON.stringify(normalize(t, after[t] ?? []))),
        objectReset: instanceAfter !== instanceBefore, instanceBefore, instanceAfter,
      };
    };
    report.implicitMode = [
      await implicitCase({ ...savepointSteps.find((s) => s.path === "s2.uncaughtBatchFailure"), args: { prefix: "implicit" } }),
      await implicitCase(steps.find((s) => s.path === "cleaningAutomation.create" && s.expect === "error")),
    ];

    // Runtime-contract vectors in workerd.
    const contract = (await post(worker.base, "/contract", {})).value;
    report.contract = { cases: contract.length / 2, passedBothRounds: [...new Set(contract.map((c) => c.id))].filter((id) => contract.filter((c) => c.id === id).every((c) => c.pass)).length, results: contract };

    // Warm invocation cost: repeat one cheap Reducer and one heavy one.
    const timings = {};
    const benches = [
      ["spots.create", (i) => ({ entity: { _id: `spot-bench-${i}`, name: `Bench ${i}` } })],
      ["boards.createMessage", (i) => ({ entity: { _id: `msg-bench-${i}`, _creationTime: T0, boardId: "board-1", createdBy: "mem-owner", title: `Bench ${i}`, content: "x" } })],
      ["centers.askJev", () => ({})],
    ];
    for (const [label, argsOf] of benches) {
      const step = steps.find((s) => s.path === label);
      const samples = [], server = [];
      for (let i = 0; i < 15; i++) {
        const args = argsOf(i);
        const call = callOf({ ...step, args, commandId: `bench-${label}-${i}`, now: T0 + 7_200_000 + i * 3_600_000 });
        const response = await post(worker.base, "/invoke", { call, measureTurns: false });
        if (response.value.envelope?.status !== "ok") throw new Error(`${label} bench failed: ${JSON.stringify(response.value).slice(0, 300)}`);
        samples.push(response.ms);
        server.push(response.value.serverMs);
      }
      timings[label] = { medianMs: Math.round(median(samples.slice(3)) * 10) / 10, maxMs: Math.round(Math.max(...samples.slice(3)) * 10) / 10, serverMedianMs: median(server.slice(3)), samples: samples.length - 3 };
    }
    const ping = []; for (let i = 0; i < 20; i++) ping.push((await post(worker.base, "/meta")).ms);
    timings.objectRoundTrip = { medianMs: Math.round(median(ping.slice(3)) * 10) / 10 };
    report.timings = timings;
    report.finalMeta = (await post(worker.base, "/meta")).value;
    report.workerdRssKb = Number(execFileSync("ps", ["-o", "rss=", "-p", String(worker.child.pid)]).toString().trim());
  } finally {
    worker.child.kill();
  }
  writeFileSync(join(artifact, "spike-results.json"), JSON.stringify(report, null, 2));
  const failed = report.steps.filter((s) => !s.pass);
  console.log(JSON.stringify({ steps: report.steps.length, failed: failed.map((s) => s.name), comparison: { tables: report.comparison.tables, identical: report.comparison.identical, rows: report.comparison.rows }, contract: `${report.contract.passedBothRounds}/${report.contract.cases}`, implicit: report.implicitMode, provision: { ms: report.provision.ms, executed: report.provision.executed, failures: report.provision.failures.length }, timings: report.timings }, null, 2));
}

main().catch((error) => { console.error(error); console.error(lastStderr?.()); process.exit(1); });
