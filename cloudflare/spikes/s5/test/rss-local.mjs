// Local proxy for memory per generation: workerd RSS while one object loads successive generations
// (Runner) and then runs each as a facet. Local only (S5_LOCAL=1, wrangler dev on :8798).
//   node test/rss-local.mjs <workerd pid> [--gens 4] [--code whagons]
import { readFileSync } from "node:fs";
import { op, gen, run, upload, save, sleep, log } from "./lib.mjs";

const pid = process.argv[2];
const args = process.argv.slice(3);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const gens = Number(opt("gens", "4"));
const code = opt("code", "whagons");
const rssMb = () => Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "utf8"))[1]) / 1024;
const call = (path) => ({ path, args: {}, now: 1, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: "x" } });
await upload("tiny"); await upload(code);
const t = `rss-${run}`;
await op(t, "stats");
await sleep(1000);
const rows = [{ step: "object, no generation", rssMb: rssMb() }];
for (let i = 0; i < gens; i++) {
  const g = gen(code, `R${i}`);
  await op(t, "load", { gen: g });
  await sleep(500);
  rows.push({ step: `Runner generation ${i + 1} loaded`, rssMb: rssMb() });
}
for (let i = 0; i < gens; i++) {
  await op(t, "activate", { gen: gen(code, `F${i}`), preload: false, abortFacet: true });
  await op(t, "invoke", { mode: "facet", calls: [call("s5.whoami")] });
  await sleep(500);
  rows.push({ step: `facet on generation F${i + 1}`, rssMb: rssMb() });
}
for (const r of rows) r.rssMb = Math.round(r.rssMb * 10) / 10;
const deltas = rows.slice(1).map((r, i) => Math.round((r.rssMb - rows[i].rssMb) * 10) / 10);
save(`rss-${code}`, { code, rows, deltas });
rows.forEach((r, i) => log(r.step, r.rssMb, i ? `+${deltas[i - 1]}` : ""));
