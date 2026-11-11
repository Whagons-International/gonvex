// Repeated generation switches on one object with facets.abort. After each switch: one facet call.
//   node test/abort-sequence.mjs [--switches 10] [--inflight none|sync|yielding] [--drain]
import { op, gen, run, sleep, save, log } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1] ?? true; };
const switches = Number(opt("switches", "10"));
const inflightKind = opt("inflight", "none");
const drain = args.includes("--drain");
const call = (path, a = {}, i = 0) => ({ path, args: a, now: 1792324800123 + i, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: `c-${i}` } });
const inflightCall = { sync: call("s5.slow", { n: 40000 }, 1), yielding: call("s5.sleepy", { ms: 1500, id: "y" }, 1) }[inflightKind];
const t = `as-${inflightKind}${drain ? "-drain" : ""}-${run}`;
await op(t, "set-active", { gen: gen("tiny", "S0") });
await op(t, "invoke", { mode: "facet", calls: [call("s5.noop")] });
const inst0 = (await op(t, "stats")).value.inst;
const rows = [];
for (let k = 1; k <= switches; k++) {
  const running = inflightCall ? op(t, "invoke", { mode: "facet", calls: [{ ...inflightCall, args: { ...inflightCall.args, id: `y${k}` }, invocation: { commandId: `if-${k}` } }] }) : null;
  if (running) await sleep(300);
  const a = await op(t, "activate", { gen: gen("tiny", `S${k}`), preload: true, abortFacet: true, drain });
  const r = running ? await running : null;
  const x = await op(t, "invoke", { mode: "facet", calls: [call("s5.whoami", {}, 100 + k)] });
  const e = x.value.results?.[0]?.envelope;
  const inst = (await op(t, "stats")).value.inst;
  const row = { k, activate: a.status === 200 ? "ok" : `${a.status}`, inflight: r ? (r.value.results?.[0]?.envelope?.status === "ok" ? "ok" : String(r.value.results?.[0]?.envelope?.message ?? r.status).slice(0, 60)) : null, next: e?.status === "ok" ? JSON.parse(e.value).generation : String(e?.message ?? x.status).slice(0, 60), objectReset: inst !== inst0 };
  rows.push(row);
  log(t, JSON.stringify(row));
}
const out = { tenant: t, inflightKind, drain, rows, firstBroken: rows.find((r) => !r.next.startsWith("S"))?.k ?? null, broken: rows.filter((r) => !r.next.startsWith("S")).length };
save(`abort-sequence-${inflightKind}${drain ? "-drain" : ""}`, out);
console.log(JSON.stringify({ inflightKind, drain, firstBroken: out.firstBroken, broken: out.broken, of: switches }));
