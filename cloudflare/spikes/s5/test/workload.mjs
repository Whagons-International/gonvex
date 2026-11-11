// S2's workload (real Whagons Reducers + savepoint cases) in each execution mode, on fresh tenants.
//   rpc    loader Worker; app in a Dynamic Worker; host calls over RPC into the tenant object
//   facet  loader Worker; app + host in a Durable Object Facet with its own SQLite
//   inline baked Worker; app in the deployed Worker (S2 / 0016 G0-2 baseline)
// Then repeated Reducers, 20 per request, for per-invocation cost without network noise.
//   node test/workload.mjs [--modes rpc,facet,inline] [--tenants 3] [--repeat 5]
import { op, gen, stats, save, log, run, subset, BASE, BAKED_BASE, round } from "./lib.mjs";
import { fixture, steps, callOf, T0 } from "../../s2/test/scenarios.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const modes = opt("modes", "rpc,facet,inline").split(",");
const tenants = Number(opt("tenants", "3"));
const repeat = Number(opt("repeat", "5"));

const savepointSteps = [
  { name: "S1 caught mid-batch failure", path: "s2.caughtBatchFailure", args: { prefix: "s1" } },
  { name: "S2 uncaught mid-batch failure", path: "s2.uncaughtBatchFailure", args: { prefix: "s2" } },
  { name: "S3 nested savepoints, ROLLBACK TO, continue", path: "s2.nestedSavepoints", args: { prefix: "s3" } },
  { name: "S4 caught SQL error without ROLLBACK TO", path: "s2.caughtSqlErrorWithoutRecovery", args: { prefix: "s4" } },
].map((step, index) => ({ ...step, as: "owner", now: T0 + 3_600_000 + index * 1000, intentEntropy: "5a".repeat(32), commandId: `cmd-s2-sp-${index + 1}` }));
const s2Table = 'CREATE TABLE IF NOT EXISTS "s2SavepointRows" ("_id" TEXT PRIMARY KEY, "label" TEXT NOT NULL)';
const G = gen("whagons", "A");
const schema = subset();
const signature = (e) => (e.status === "ok" ? `ok:${e.value?.slice(0, 120)}` : `${e.kind}:${String(e.message).slice(0, 160)}`);

/** Repeatable invocations, distinct ids each time. */
const repeatable = (i, step) => [
  { path: "spots.create", as: "owner", args: { entity: { _id: `spot-r${step}-${i}`, name: `Room r${i}` } } },
  { path: "boards.createMessage", as: "owner", args: { entity: { _id: `msg-r${step}-${i}`, _creationTime: T0, boardId: "board-1", createdBy: "mem-owner", title: `T${i}`, content: "Hello" } } },
  { path: "s5.noop", as: "owner", args: {} },
  { path: "s5.slow", as: "owner", args: { n: 30 } },
].map((s, j) => ({ ...s, now: T0 + 7_200_000 + i * 1000 + j, intentEntropy: "7b".repeat(32), commandId: `cmd-r-${step}-${i}-${j}` }));

const out = { run, startedAt: new Date().toISOString(), modes: {} };
for (const mode of modes) {
  const base = mode === "inline" ? BAKED_BASE : BASE;
  const m = { tenants: [], steps: {}, repeated: {} };
  out.modes[mode] = m;
  for (let t = 0; t < tenants; t++) {
    const tenant = `wl-${mode}-${run}-${t}`;
    const target = mode === "facet" ? "facet" : "self";
    const rtt = [];
    for (let i = 0; i < 3; i++) rtt.push((await op(tenant, "stats", undefined, base)).ms);
    if (mode !== "inline") await op(tenant, "set-active", { gen: G }, base);
    const firstLoad = mode !== "inline" ? await op(tenant, "load", { gen: G }, base) : null;
    const provision = await op(tenant, "provision", { target, statements: schema.statements, rows: [...schema.rows, ...fixture], extra: [s2Table] }, base);
    log(mode, tenant, "provision", provision.status, Math.round(provision.ms), "ms", JSON.stringify({ executed: provision.value.executed, refused: provision.value.refused?.length }));
    const rttMs = Math.min(...rtt);
    const record = { tenant, rttMs: round(rttMs), firstLoad: firstLoad?.value, provisionMs: round(provision.ms), provision: { ...provision.value, refused: provision.value.refused?.length } };
    for (const step of [...steps, ...savepointSteps]) {
      const r = await op(tenant, "invoke", { mode, calls: [callOf(step)] }, base);
      const res = r.value.results?.[0];
      const entry = (m.steps[step.name] ??= { path: step.path, outcomes: [], clientMs: [], doMs: [], hostCalls: null, statements: null });
      if (!res) { entry.outcomes.push(`http ${r.status}: ${JSON.stringify(r.value).slice(0, 200)}`); continue; }
      entry.outcomes.push(`${res.committed ? "committed" : "rolled back"} ${signature(res.envelope)}`);
      entry.clientMs.push(r.ms - rttMs);
      entry.doMs.push(res.ms);
      entry.hostCalls = res.stats?.calls ?? null;
      entry.statements = res.stats?.statements ?? null;
      entry.rowsRead = res.stats?.rowsRead ?? null;
      entry.catalogRowsRead = res.stats?.catalogRowsRead ?? null;
      entry.rowsWritten = res.stats?.rowsWritten ?? null;
    }
    record.counts = (await op(tenant, "counts", { target }, base)).value;
    // Repeated invocations, 20 per request.
    for (let r = 0; r < repeat; r++) {
      const calls = [];
      for (let i = 0; i < 5; i++) for (const s of repeatable(i, `${t}-${r}`)) calls.push(callOf(s));
      const resp = await op(tenant, "invoke", { mode, calls }, base);
      const results = resp.value.results ?? [];
      const perPath = {};
      for (const res of results) (perPath[res.path] ??= []).push(res);
      for (const [path, list] of Object.entries(perPath)) {
        const e = (m.repeated[path] ??= { ok: 0, failed: [], doMs: [] });
        for (const res of list) { if (res.envelope.status === "ok") e.ok++; else e.failed.push(signature(res.envelope)); e.doMs.push(res.ms); }
      }
      (m.repeated._batch ??= { clientMsPerCall: [] }).clientMsPerCall.push((resp.ms - rttMs) / Math.max(1, results.length));
    }
    m.tenants.push(record);
  }
  for (const entry of Object.values(m.steps)) { entry.clientMs = stats(entry.clientMs); entry.doMs = stats(entry.doMs); entry.consistent = new Set(entry.outcomes).size === 1; }
  for (const [path, entry] of Object.entries(m.repeated)) {
    if (path === "_batch") entry.clientMsPerCall = stats(entry.clientMsPerCall);
    else { entry.doMs = stats(entry.doMs); entry.failed = [...new Set(entry.failed)]; }
  }
}

// Cross-mode comparison: each step's outcome must match across modes.
const names = Object.keys(out.modes[modes[0]].steps);
out.comparison = names.map((name) => {
  const per = Object.fromEntries(modes.map((mode) => [mode, out.modes[mode].steps[name]?.outcomes[0]]));
  return { name, same: new Set(Object.values(per)).size === 1, ...per };
});
out.allSame = out.comparison.every((c) => c.same);
out.countsSame = modes.every((mode) => out.modes[mode].tenants.every((t) => JSON.stringify(t.counts) === JSON.stringify(out.modes[modes[0]].tenants[0].counts)));
save("workload", out);
console.log(JSON.stringify({ allSame: out.allSame, countsSame: out.countsSame, differing: out.comparison.filter((c) => !c.same) }, null, 1));
for (const mode of modes) console.log(mode, JSON.stringify(out.modes[mode].repeated));
