// Generation switches against live sockets on one tenant object, then (with --deploy) a new
// Worker version for comparison. The object is kept active by a writer (2 broadcasts/s).
//   node test/sockets.mjs [--count 500] [--swaps 5] [--code whagons] [--deploy]
// With --deploy, run under bin/cf-env.sh: it calls `wrangler deploy` for .build/loader.
import { spawn } from "node:child_process";
import { join } from "node:path";
import WebSocket from "ws";
import { op, gen, stats, save, log, run, sleep, BASE, KEY, spike, round } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1] ?? true; };
const count = Number(opt("count", "500"));
const swaps = Number(opt("swaps", "5"));
const code = opt("code", "whagons");
const tenant = `ws-${run}`;
const wsUrl = `${BASE.replace(/^http/, "ws")}/t/${tenant}/ws?k=${KEY}`;

class Client {
  constructor(index) { this.index = index; this.reloads = []; this.closes = []; this.frames = 0; this.opens = 0; this.reconnect = false; }
  open() {
    return new Promise((resolve) => {
      const ws = new WebSocket(wsUrl, { perMessageDeflate: false });
      this.ws = ws;
      ws.on("open", () => { this.opens++; resolve(true); });
      ws.on("error", () => resolve(false));
      ws.on("message", (data) => {
        const text = data.toString();
        if (text.includes('"system.reload"')) this.reloads.push({ at: Date.now(), hash: JSON.parse(text).artifactHash });
        else this.frames++;
      });
      ws.on("close", (closeCode) => {
        this.closes.push({ at: Date.now(), code: closeCode });
        if (this.reconnect) setTimeout(() => this.open(), 1000 * Math.random() + 500);
      });
    });
  }
}

const clients = Array.from({ length: count }, (_, i) => new Client(i));
await op(tenant, "set-active", { gen: gen(code, "S0", run) });
const firstLoad = await op(tenant, "load", {});
log("first load", JSON.stringify(firstLoad.value).slice(0, 200));
for (let i = 0; i < count; i += 50) await Promise.all(clients.slice(i, i + 50).map((c) => c.open()));
const opened = clients.filter((c) => c.opens === 1).length;
log("opened", opened, "of", count);
const writer = new Client(-1);
await writer.open();
const writing = setInterval(() => writer.ws.readyState === 1 && writer.ws.send("write"), 500);
await sleep(3000);

const out = { tenant, count, opened, code, swaps: [] };
for (let k = 1; k <= swaps; k++) {
  const g = gen(code, `S${k}`, run);
  const before = clients.map((c) => c.reloads.length);
  // --with-load: a ~1 s facet Reducer is running when the switch arrives (the case that can reset the object).
  let load = null;
  if (opt("with-load", false)) {
    load = op(tenant, "invoke", { mode: "facet", calls: [{ path: "s5.slow", args: { n: 40000 }, now: 1792324800123 + k, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: `load-${k}` } }] });
    await sleep(300);
  }
  const t0 = Date.now();
  const activate = await op(tenant, "activate", { gen: g, preload: true, abortFacet: true, drain: opt("drain", false) === true });
  const loadResult = load ? await load : null;
  const t1 = Date.now();
  await sleep(3000);
  const got = clients.filter((c, i) => c.reloads.length === before[i] + 1 && c.reloads.at(-1).hash === g.id);
  const lag = got.map((c) => c.reloads.at(-1).at - t1);
  const row = {
    k, gen: g.id, activateClientMs: t1 - t0, activateStatus: activate.status, activate: activate.value, reloadReceived: got.length,
    load: loadResult ? { status: loadResult.status, envelope: loadResult.value.results?.[0]?.envelope?.status ?? JSON.stringify(loadResult.value).slice(0, 120), message: loadResult.value.results?.[0]?.envelope?.message?.slice(0, 120) } : null,
    reloadAfterResponseMs: stats(lag), closesSoFar: clients.reduce((n, c) => n + c.closes.length, 0) + writer.closes.length,
  };
  out.swaps.push(row);
  log("swap", JSON.stringify(row));
}
out.closesDuringSwaps = clients.reduce((n, c) => n + c.closes.length, 0);
out.framesDuringSwaps = clients.reduce((n, c) => n + c.frames, 0);
out.statsAfterSwaps = (await op(tenant, "stats")).value;

if (opt("deploy", false)) {
  // Same sockets, now a new Worker version (S3 deploy B/C: active object). Clients reconnect after a close.
  for (const c of clients) c.reconnect = true;
  writer.reconnect = true;
  const closesBefore = clients.reduce((n, c) => n + c.closes.length, 0);
  const d = await new Promise((resolve) => {
    const t0 = Date.now();
    const p = spawn(join(spike, "node_modules/.bin/wrangler"), ["deploy", "--var", `BUILD:${run}`], { cwd: join(spike, ".build/loader"), stdio: ["ignore", "pipe", "pipe"] });
    let text = "";
    p.stdout.on("data", (x) => (text += x));
    p.stderr.on("data", (x) => (text += x));
    p.on("close", (c) => resolve({ code: c, t0, t1: Date.now(), ms: Date.now() - t0, version: /Current Version ID: (\S+)/.exec(text)?.[1] ?? null, tail: c ? text.slice(-400) : undefined }));
  });
  log("deploy", JSON.stringify(d));
  const samples = [];
  for (let s = 0; s < 20; s++) {
    await sleep(2000);
    samples.push({ t: Math.round((Date.now() - d.t1) / 1000), open: clients.filter((c) => c.ws.readyState === 1).length, closes: clients.reduce((n, c) => n + c.closes.length, 0) - closesBefore });
  }
  const closes = clients.flatMap((c) => c.closes.filter((x) => x.at >= d.t0));
  const codes = {};
  for (const c of closes) codes[c.code] = (codes[c.code] ?? 0) + 1;
  const afterDeployLoad = await op(tenant, "load", {});
  out.deploy = {
    ...d, closed: closes.length, codes, closeAfterDeployMs: stats(closes.map((c) => c.at - d.t1)), samples,
    openAtEnd: clients.filter((c) => c.ws.readyState === 1).length,
    generationStillWarm: afterDeployLoad.value.callbackRan === false, afterDeployLoad: afterDeployLoad.value,
  };
  log("deploy result", JSON.stringify({ closed: out.deploy.closed, codes, openAtEnd: out.deploy.openAtEnd }));
}

clearInterval(writing);
for (const c of [...clients, writer]) { c.reconnect = false; c.ws.close(); }
save(`sockets${opt("deploy", false) ? "-deploy" : ""}${opt("with-load", false) ? "-load" : ""}`, out);
console.log(JSON.stringify({ opened, closesDuringSwaps: out.closesDuringSwaps, swaps: out.swaps.map((s) => [s.k, s.activateStatus, s.reloadReceived, s.activate?.preloadMs, s.activateClientMs, s.closesSoFar]) }));
process.exit(0);
