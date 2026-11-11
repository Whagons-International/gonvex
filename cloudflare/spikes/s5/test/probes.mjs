// Platform probes for S5. One subcommand per question; each writes results/<name>-<env>.json.
//   node test/probes.mjs load      [--reps 5]   cold/warm generation load, Runner and facet, real app and empty app
//   node test/probes.mjs hostbench [--reps 5]   RPC round trip per host call, outside a transaction
//   node test/probes.mjs deadlock               host-call RPC inside ctx.storage.transaction (expect a reset)
//   node test/probes.mjs isolation              globalOutbound: null and what module code can reach
//   node test/probes.mjs inflight               generation switch with an invocation running (facet abort, drain, Runner)
//   node test/probes.mjs ballast  [--what dyn-whagons,dyn-tiny,facet,do-loader,do-baked]
import { op, gen, stats, save, log, run, sleep, BASE, BAKED_BASE, round } from "./lib.mjs";

const args = process.argv.slice(2);
const cmd = args[0];
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const reps = Number(opt("reps", "5"));
const identity = { member: { role: "owner", id: "mem-owner" }, account: { id: "acct-owner" }, tenant: { id: "t" } };
const call = (path, args = {}, i = 0) => ({ path, args, now: 1792324800123 + i, intentEntropy: "aa".repeat(32), identity, invocation: { commandId: `c-${i}` } });

async function rtt(tenant, base = BASE) {
  const values = [];
  for (let i = 0; i < 3; i++) values.push((await op(tenant, "stats", undefined, base)).ms);
  return Math.min(...values);
}

const probes = {
  async load() {
    const out = { reps, runner: { whagons: [], tiny: [] }, facet: { whagons: [] } };
    for (let i = 0; i < reps; i++) {
      for (const code of ["whagons", "tiny"]) {
        const g = gen(code, "cold", `${run}-${i}`);
        const a = `load-${code}-${run}-${i}-a`;
        const b = `load-${code}-${run}-${i}-b`;
        const rttA = await rtt(a);
        const cold = await op(a, "load", { gen: g });
        const warm = await op(a, "load", { gen: g });
        const rttB = await rtt(b);
        const other = await op(b, "load", { gen: g });
        const row = {
          cold: { doMs: cold.value.ms, clientMs: round(cold.ms - rttA), callbackRan: cold.value.callbackRan, fetchMs: cold.value.fetchMs, isolate: cold.value.info?.isolate, error: cold.value.error?.slice(0, 300) },
          warm: { doMs: warm.value.ms, clientMs: round(warm.ms - rttA), callbackRan: warm.value.callbackRan, isolate: warm.value.info?.isolate },
          otherObject: { doMs: other.value.ms, clientMs: round(other.ms - rttB), callbackRan: other.value.callbackRan, isolate: other.value.info?.isolate },
        };
        out.runner[code].push(row);
        log("load", code, i, JSON.stringify(row));
      }
      // Facet: a fresh generation and object, so the facet start includes the code load.
      const g = gen("whagons", "facet", `${run}-${i}`);
      const t = `load-facet-${run}-${i}`;
      const rttT = await rtt(t);
      await op(t, "set-active", { gen: g });
      const cold = await op(t, "facet-info", {});
      const warm = await op(t, "facet-info", {});
      const invoke = await op(t, "invoke", { mode: "facet", calls: [call("s5.noop")] });
      const row = { coldClientMs: round(cold.ms - rttT), warmClientMs: round(warm.ms - rttT), firstInvokeDoMs: invoke.value.results?.[0]?.ms, isolate: cold.value.isolate, error: cold.value.error?.slice(0, 300) };
      out.facet.whagons.push(row);
      log("facet", i, JSON.stringify(row));
    }
    const summarize = (rows, path) => stats(rows.map((r) => path(r)));
    out.summary = {
      runnerWhagons: { coldDoMs: summarize(out.runner.whagons, (r) => r.cold.doMs), coldFetchMs: summarize(out.runner.whagons, (r) => r.cold.fetchMs), warmDoMs: summarize(out.runner.whagons, (r) => r.warm.doMs), otherObjectDoMs: summarize(out.runner.whagons, (r) => r.otherObject.doMs), otherObjectCallbackRan: out.runner.whagons.filter((r) => r.otherObject.callbackRan).length },
      runnerTiny: { coldDoMs: summarize(out.runner.tiny, (r) => r.cold.doMs), warmDoMs: summarize(out.runner.tiny, (r) => r.warm.doMs), otherObjectDoMs: summarize(out.runner.tiny, (r) => r.otherObject.doMs) },
      facetWhagons: { coldClientMs: summarize(out.facet.whagons, (r) => r.coldClientMs), warmClientMs: summarize(out.facet.whagons, (r) => r.warmClientMs) },
    };
    save("load", out);
    console.log(JSON.stringify(out.summary, null, 1));
  },

  async hostbench() {
    // Client-side time: on the platform Date.now() in the object does not move across these calls.
    const n = Number(opt("n", "500"));
    const out = { reps, n, rows: [] };
    for (const code of ["tiny", "whagons"]) {
      const t = `hb-${code}-${run}`;
      await op(t, "set-active", { gen: gen(code, "hb") });
      await op(t, "load", {});
      for (let i = 0; i < reps; i++) {
        const base = await rtt(t);
        for (const kind of ["noop", "query"]) for (const inTx of [false]) {   // inTx deadlocks from the second call (probe deadlock)
          const r = await op(t, "hostbench", { n, kind, inTx });
          out.rows.push({ code, kind, inTx, status: r.status, clientPerCallMs: (r.ms - base) / n, doPerCallMs: r.value.perCallMs, error: r.value.error?.slice(0, 200) });
        }
      }
    }
    out.summary = {};
    for (const code of ["tiny", "whagons"]) for (const kind of ["noop", "query"]) for (const inTx of [false]) {   // inTx deadlocks from the second call (probe deadlock)
      const rows = out.rows.filter((r) => r.code === code && r.kind === kind && r.inTx === inTx);
      out.summary[`${code}/${kind}/${inTx ? "tx" : "noTx"}`] = { clientPerCallMs: stats(rows.map((r) => r.clientPerCallMs)), errors: rows.filter((r) => r.status !== 200).length };
    }
    save("hostbench", out);
    console.log(JSON.stringify(out.summary, null, 1));
  },

  async deadlock() {
    const t = `dl-${run}`;
    await op(t, "set-active", { gen: gen("tiny", "dl") });
    const before = (await op(t, "stats")).value;
    const noTx = await op(t, "hostbench", { n: 1, kind: "noop", inTx: false });
    const inTx = await op(t, "hostbench", { n: 1, kind: "noop", inTx: true });
    const after = (await op(t, "stats")).value;
    const reducer = await op(t, "invoke", { mode: "rpc", calls: [call("s5.slow", { n: 1 })] });
    const after2 = (await op(t, "stats")).value;
    const out = {
      noTx: { status: noTx.status, ms: round(noTx.ms), value: noTx.value },
      inTx: { status: inTx.status, ms: round(inTx.ms), body: JSON.stringify(inTx.value).slice(0, 400) },
      reducerInTx: { status: reducer.status, ms: round(reducer.ms), body: JSON.stringify(reducer.value).slice(0, 400) },
      instances: [before.inst, after.inst, after2.inst],
    };
    save("deadlock", out);
    console.log(JSON.stringify(out, null, 1));
  },

  async gate() {
    // A long failing Reducer in one storage transaction; meanwhile another request writes a row
    // outside any transaction. Does the second request wait (input gate), and does its write survive?
    const out = {};
    const n = Number(opt("n", "3000"));
    for (const mode of opt("modes", "rpc,facet,inline").split(",")) {
      const base = mode === "inline" ? BAKED_BASE : BASE;
      const t = `gate-${mode}-${run}`;
      if (mode !== "inline") await op(t, "set-active", { gen: gen("tiny", "gate") }, base);
      await op(t, "invoke", { mode, calls: [call("s5.slow", { n: 10 })] }, base);
      await op(t, "marks", {}, base);
      const t0 = Date.now();
      const slow = op(t, "invoke", { mode, calls: [call("s5.slowThenThrow", { n }, 1)] }, base).then((r) => ({ ...r, doneAt: Date.now() - t0 }));
      await sleep(Number(opt("delay", "150")));
      const mark = await op(t, "mark", { id: `m-${mode}` }, base).then((r) => ({ ...r, doneAt: Date.now() - t0 }));
      const s = await slow;
      const marks = (await op(t, "marks", {}, base)).value;
      const stat = (await op(t, "stats", undefined, base)).value;
      out[mode] = {
        slowDoneAtMs: s.doneAt, slowStatus: s.value.results?.[0]?.envelope?.message?.slice(0, 120) ?? JSON.stringify(s.value).slice(0, 200),
        markDoneAtMs: mark.doneAt, markWaitedForSlow: mark.doneAt >= s.doneAt, markSurvived: Array.isArray(marks) && marks.includes(`m-${mode}`),
        instanceAfter: stat.inst,
      };
      log("gate", mode, JSON.stringify(out[mode]));
    }
    save("gate", out);
  },

  async isolation() {
    const t = `iso-${run}`;
    await op(t, "set-active", { gen: gen("whagons", "iso") });
    const r = await op(t, "isolation", {});
    save("isolation", r.value);
    console.log(JSON.stringify(r.value, null, 1));
  },

  async inflight() {
    const out = {};
    const slowN = Number(opt("n", "20000"));
    // Facet: abort while an invocation runs, then the same with drain.
    for (const variant of ["abort", "drain"]) {
      const t = `if-${variant}-${run}`;
      await op(t, "set-active", { gen: gen("tiny", "A", run) });
      const warm = await op(t, "invoke", { mode: "facet", calls: [call("s5.slow", { n: 10 })] });
      const t0 = Date.now();
      const running = op(t, "invoke", { mode: "facet", calls: [call("s5.slow", { n: slowN }, 1)] });
      await sleep(Number(opt("delay", "300")));
      const activate = await op(t, "activate", { gen: gen("tiny", "B", run), preload: true, abortFacet: true, drain: variant === "drain" });
      const after = await op(t, "invoke", { mode: "facet", calls: [call("s5.whoami", {}, 2)] });
      const slow = await running;
      const res = slow.value.results?.[0];
      out[`facet-${variant}`] = {
        warm: warm.value.results?.[0]?.envelope?.status,
        inflight: { clientMs: round(Date.now() - t0), status: res?.envelope?.status, message: res?.envelope?.message?.slice(0, 300), value: res?.envelope?.value, committed: res?.committed },
        activate: activate.value,
        next: after.value.results?.[0]?.envelope,
      };
      log("inflight", variant, JSON.stringify(out[`facet-${variant}`]));
    }
    // Facet with a Reducer that yields on a timer inside its transaction: can the switch land mid-invocation?
    for (const variant of ["abort", "drain"]) {
      const t = `if-sleepy-${variant}-${run}`;
      await op(t, "set-active", { gen: gen("tiny", "A", run) });
      await op(t, "invoke", { mode: "facet", calls: [call("s5.noop")] });
      const t0 = Date.now();
      const running = op(t, "invoke", { mode: "facet", calls: [call("s5.sleepy", { ms: 1500, id: variant }, 1)] }).then((r) => ({ ...r, doneAt: Date.now() - t0 }));
      await sleep(400);
      const activate = await op(t, "activate", { gen: gen("tiny", "B", run), preload: true, abortFacet: true, drain: variant === "drain" }).then((r) => ({ ...r, doneAt: Date.now() - t0 }));
      const slow = await running;
      const rows = await op(t, "invoke", { mode: "facet", calls: [call("s5.sleepyRows", {}, 2)] });
      const res = slow.value.results?.[0];
      out[`facet-sleepy-${variant}`] = {
        inflight: { doneAtMs: slow.doneAt, status: res?.envelope?.status, message: res?.envelope?.message?.slice(0, 300), value: res?.envelope?.value, committed: res?.committed },
        activate: { doneAtMs: activate.doneAt, ...activate.value },
        rowsAfter: rows.value.results?.[0]?.envelope,
      };
      log("inflight sleepy", variant, JSON.stringify(out[`facet-sleepy-${variant}`]));
    }
    // Runner (stateless, no transaction): the old isolate keeps serving the call that started on it.
    {
      const t = `if-runner-${run}`;
      await op(t, "set-active", { gen: gen("tiny", "A", run) });
      await op(t, "load", {});
      const running = op(t, "invoke", { mode: "rpc-notx", calls: [call("s5.slow", { n: Math.round(slowN / 10) }, 1)] });
      await sleep(Number(opt("delay", "300")));
      const activate = await op(t, "activate", { gen: gen("tiny", "B", run), preload: true });
      const after = await op(t, "invoke", { mode: "rpc-notx", calls: [call("s5.whoami", {}, 2)] });
      const slow = await running;
      out.runner = { inflight: slow.value.results?.[0]?.envelope, inflightMs: slow.value.results?.[0]?.ms, activate: activate.value, next: after.value.results?.[0]?.envelope };
      log("inflight runner", JSON.stringify(out.runner));
    }
    save("inflight", out);
  },

  async ballast() {
    const what = opt("what", "dyn-whagons,dyn-tiny,facet,do-loader,do-baked").split(",");
    const step = Number(opt("step", "4"));
    const out = { step };
    for (const w of what) {
      const t = `bal-${w}-${run}`;
      const base = w === "do-baked" ? BAKED_BASE : BASE;
      const code = w === "dyn-tiny" ? "tiny" : "whagons";
      if (w !== "do-baked") await op(t, "set-active", { gen: gen(code, "bal", `${w}-${run}`) }, base);
      const opName = w.startsWith("dyn") ? "ballast-dyn" : w === "facet" ? "ballast-facet" : "ballast-do";
      if (w === "facet") await op(t, "facet-info", {});
      if (w.startsWith("dyn")) await op(t, "load", {});
      let first, last = 0, why = null;
      for (let i = 0; i < 64; i++) {
        const r = await op(t, opName, { mb: step }, base);
        const id = r.value.isolate ?? r.value.inst;
        if (r.status !== 200 || r.value.error) { why = `status ${r.status}: ${JSON.stringify(r.value).slice(0, 200)}`; break; }
        first ??= id;
        if (id !== first) { why = `new isolate/instance ${id} with ${r.value.ballastMb} MB`; break; }
        last = r.value.ballastMb;
      }
      out[w] = { maxBallastMb: last, stoppedBecause: why };
      log("ballast", w, JSON.stringify(out[w]));
    }
    save("ballast", out);
  },
};

if (!probes[cmd]) { console.error(`usage: node test/probes.mjs ${Object.keys(probes).join("|")}`); process.exit(2); }
await probes[cmd]();
