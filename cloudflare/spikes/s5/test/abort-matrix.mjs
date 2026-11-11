// What a facet abort leaves behind. Per trial: a fresh tenant, one facet call in flight (or none),
// a generation switch with facets.abort, then five facet calls and the object instance before/after.
//   node test/abort-matrix.mjs [--trials 6]
import { op, gen, run, sleep, save, log } from "./lib.mjs";

const args = process.argv.slice(2);
const trials = Number(args[args.indexOf("--trials") + 1] || 6);
const call = (path, a = {}, i = 0) => ({ path, args: a, now: 1792324800123 + i, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: `c-${i}` } });
const cases = {
  idle: null,
  "sync-inflight": call("s5.slow", { n: 40000 }, 1),
  "yielding-inflight": call("s5.sleepy", { ms: 1500, id: "y" }, 1),
};
const out = { trials, cases: {} };
for (const [name, inflight] of Object.entries(cases)) {
  const rows = [];
  for (let i = 0; i < trials; i++) {
    const t = `am-${name}-${run}-${i}`;
    await op(t, "set-active", { gen: gen("tiny", "A") });
    await op(t, "invoke", { mode: "facet", calls: [call("s5.noop")] });
    const before = (await op(t, "stats")).value.inst;
    const running = inflight ? op(t, "invoke", { mode: "facet", calls: [inflight] }) : null;
    if (running) await sleep(300);
    const a = await op(t, "activate", { gen: gen("tiny", `B${i}`), preload: true, abortFacet: true });
    const r = running ? await running : null;
    const after = [];
    for (let j = 0; j < 5; j++) {
      const x = await op(t, "invoke", { mode: "facet", calls: [call("s5.whoami", {}, 10 + j)] });
      const e = x.value.results?.[0]?.envelope;
      after.push(e?.status === "ok" ? JSON.parse(e.value).generation : `${x.status} ${e?.message ?? JSON.stringify(x.value).slice(0, 80)}`.slice(0, 90));
    }
    const instAfter = (await op(t, "stats")).value.inst;
    const row = {
      activate: a.status === 200 ? "ok" : `${a.status} ${JSON.stringify(a.value).slice(0, 80)}`,
      inflight: r ? (r.value.results?.[0]?.envelope?.status === "ok" ? "ok" : String(r.value.results?.[0]?.envelope?.message ?? r.status).slice(0, 80)) : null,
      after, objectReset: before !== instAfter,
    };
    rows.push(row);
    log(name, i, JSON.stringify(row));
  }
  out.cases[name] = {
    rows,
    switchedCleanly: rows.filter((r) => r.activate === "ok" && r.after.every((g) => g.startsWith("B"))).length,
    facetBrokenAfter: rows.filter((r) => r.after.some((g) => g.includes("internal error"))).length,
    activateFailed: rows.filter((r) => r.activate !== "ok").length,
    objectReset: rows.filter((r) => r.objectReset).length,
  };
}
save("abort-matrix", out);
for (const [k, v] of Object.entries(out.cases)) console.log(k, JSON.stringify({ switchedCleanly: v.switchedCleanly, facetBrokenAfter: v.facetBrokenAfter, activateFailed: v.activateFailed, objectReset: v.objectReset }));
