// facets.abort while the facet is still starting (its first call is loading the generation).
//   node test/abort-startup.mjs [--trials 5] [--code whagons] [--delay 100]
import { op, gen, run, sleep, save, log } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1] ?? true; };
const trials = Number(opt("trials", "5"));
const code = opt("code", "whagons");
const delay = Number(opt("delay", "100"));
const call = (path, a = {}, i = 0) => ({ path, args: a, now: 1792324800123 + i, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: `c-${i}` } });
const rows = [];
for (let i = 0; i < trials; i++) {
  const t = `ast-${code}-${run}-${i}`;
  await op(t, "set-active", { gen: gen(code, `A${i}`) });
  const first = op(t, "invoke", { mode: "facet", calls: [call("s5.slow", { n: 10 })] });
  await sleep(delay);
  const a = await op(t, "activate", { gen: gen(code, `B${i}`), preload: false, abortFacet: true });
  const f = await first;
  const after = [];
  for (let j = 0; j < 4; j++) {
    const x = await op(t, "invoke", { mode: "facet", calls: [call("s5.whoami", {}, 10 + j)] });
    const e = x.value.results?.[0]?.envelope;
    after.push(e?.status === "ok" ? JSON.parse(e.value).generation : String(e?.message ?? x.status).slice(0, 70));
    await sleep(200);
  }
  const row = { activate: a.status, first: f.value.results?.[0]?.envelope?.status === "ok" ? JSON.parse(f.value.results[0].envelope.value).generation : String(f.value.results?.[0]?.envelope?.message ?? f.status).slice(0, 70), after };
  rows.push(row);
  log(i, JSON.stringify(row));
}
const out = { code, delay, rows, broken: rows.filter((r) => r.after.some((x) => x.includes("internal error"))).length };
save(`abort-startup-${code}`, out);
console.log(JSON.stringify({ code, delay, broken: out.broken, of: trials }));
