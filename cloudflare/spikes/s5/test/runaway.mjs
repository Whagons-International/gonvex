// R25 in the facet layout: a Reducer that writes one row and spins forever, in a facet whose
// generation has limits.cpuMs (or none). Meanwhile: tenant-held sockets echo every 500 ms, and
// other calls are queued at +300 ms (facet), +400 ms (supervisor) and +500 ms (facet).
//   node test/runaway.mjs [--cpu 1000,none] [--sockets 20]
import WebSocket from "ws";
import { op, gen, run, sleep, save, log, round, BASE, KEY } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const cpus = opt("cpu", "1000,none").split(",").map((c) => (c === "none" ? null : Number(c)));
const nSockets = Number(opt("sockets", "20"));
const call = (path, a = {}, i = 0) => ({ path, args: a, now: 1792324800123 + i, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: `c-${i}` } });
const envelopeOf = (r) => r.value.results?.[0]?.envelope ?? { status: "http", message: `${r.status} ${JSON.stringify(r.value).slice(0, 160)}` };
const out = {};
for (const cpuMs of cpus) {
  const t = `ra-${run}-${cpuMs ?? "none"}`;
  await op(t, "set-active", { gen: gen("tiny", "ra", run, cpuMs) });
  await op(t, "invoke", { mode: "facet", calls: [call("s5.seed", { n: 50 })] });
  const inst0 = (await op(t, "stats")).value.inst;
  const sockets = [];
  const echoes = [];
  const closes = [];
  for (let i = 0; i < nSockets; i++) {
    const ws = new WebSocket(`${BASE.replace(/^http/, "ws")}/t/${t}/ws?k=${KEY}`);
    await new Promise((r) => ws.on("open", r));
    ws.on("message", () => echoes.push(Date.now()));
    ws.on("close", (code) => closes.push({ code, at: Date.now() }));
    sockets.push(ws);
  }
  const pinging = setInterval(() => sockets.forEach((ws) => ws.readyState === 1 && ws.send("echo")), 500);
  await sleep(1500);
  const t0 = Date.now();
  const at = (p) => p.then((r) => ({ r, doneAt: Date.now() - t0 }));
  const runaway = at(op(t, "invoke", { mode: "facet", calls: [call("s5.spinAfterWrite", {}, 1)] }));
  await sleep(300);
  const facetQueued = at(op(t, "invoke", { mode: "facet", calls: [call("s5.whoami", {}, 2)] }));
  await sleep(100);
  const supervisor = at(op(t, "stats"));
  await sleep(100);
  const facetQueued2 = at(op(t, "invoke", { mode: "facet", calls: [call("s5.countRows", {}, 3)] }));
  const [a, b, c, d] = await Promise.all([runaway, facetQueued, supervisor, facetQueued2]);
  await sleep(1500);
  clearInterval(pinging);
  const after = await op(t, "invoke", { mode: "facet", calls: [call("s5.countRows", {}, 4)] });
  const inst1 = (await op(t, "stats")).value.inst;
  const gaps = echoes.filter((e) => e > t0 - 1000).map((e, i, all) => (i ? e - all[i - 1] : 0));
  const row = {
    cpuMs,
    runaway: { doneAtMs: a.doneAt, envelope: envelopeOf(a.r) },
    queuedFacetCall: { doneAtMs: b.doneAt, envelope: envelopeOf(b.r) },
    queuedSupervisorCall: { doneAtMs: c.doneAt, status: c.r.status, inst: c.r.value.inst },
    queuedFacetCall2: { doneAtMs: d.doneAt, envelope: envelopeOf(d.r) },
    rowsAfter: envelopeOf(after),
    supervisorReset: inst0 !== inst1,
    socketsClosed: closes.length, closeCodes: [...new Set(closes.map((x) => x.code))], maxEchoGapMs: Math.max(0, ...gaps),
  };
  out[cpuMs ?? "none"] = row;
  log(JSON.stringify(row));
  sockets.forEach((ws) => ws.close());
}
save("runaway", out);
process.exit(0);
