// Host-call round trips from Worker Loader module code into a tenant object that holds an open
// ctx.storage.transaction(async) (ruling 0022, K04 R25 follow-up). Per case: a fresh tenant, the
// generation warmed outside the transaction, then n host calls inside it, with and without limits.cpuMs.
//   node test/txcalls.mjs [--ns 0,1,2,5] [--cpu none,1000] [--kinds noop,query]
import { op, gen, run, save, log, round } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const ns = opt("ns", "0,1,2,5").split(",").map(Number);
const cpus = opt("cpu", "none,1000").split(",").map((c) => (c === "none" ? null : Number(c)));
const kinds = opt("kinds", "noop,query").split(",");
const wallMs = opt("wall", null) ? Number(opt("wall", null)) : null;
const rows = [];
for (const cpuMs of cpus) for (const kind of kinds) for (const n of ns) for (const inTx of [false, true]) {
  const t = `txc-${run}-${cpuMs ?? "none"}-${kind}-${n}-${inTx ? "tx" : "notx"}`;
  await op(t, "set-active", { gen: gen("tiny", "txc") });
  const warm = await op(t, "load", {});
  const rtt = Math.min(...(await Promise.all([0, 1, 2].map(() => op(t, "stats")))).map((r) => r.ms));
  const r = await op(t, "hostbench", { n, kind, inTx, cpuMs, wallMs });
  const after = (await op(t, "stats")).value;
  const row = {
    wallMs, cpuMs, kind, n, inTx, status: r.status, clientMs: round(r.ms), clientPerCallMs: n ? round((r.ms - rtt) / n) : null,
    completed: r.value.completed ?? null, failed: r.value.failed ?? null, error: r.value.error ? String(r.value.error).slice(0, 160) : null,
    objectReset: warm.value.inst !== after.inst,
  };
  rows.push(row);
  log(JSON.stringify(row));
}
save(`txcalls${wallMs ? `-wall${wallMs}` : ""}`, { rows });
