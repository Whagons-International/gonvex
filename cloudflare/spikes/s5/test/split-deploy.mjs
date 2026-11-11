// Split-deploy probe: sockets on a gateway object in one script, tenant objects in another.
// Deploys a new version of the tenant script only (immediate code update, the worst case) while
// 500 sockets are open and one socket invokes through the gateway every 200 ms.
// Run under bin/cf-env.sh and bin/cf-target.sh:  node test/split-deploy.mjs [--count 500] [--deploys 2]
import { spawn } from "node:child_process";
import { join } from "node:path";
import WebSocket from "ws";
import { save, log, sleep, stats, spike, KEY, run } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1] ?? true; };
const count = Number(opt("count", "500"));
const deploys = Number(opt("deploys", "2"));
const url = `${(process.env.S5_GW_BASE ?? "https://gonvex-cf-dev-s5-gw.whagons-international.workers.dev").replace(/^http/, "ws")}/ws?k=${KEY}&g=g-${run}`;

const sockets = [];
const closes = [];
const reloads = new Map();
const open = (i) => new Promise((resolve) => {
  const ws = new WebSocket(url, { perMessageDeflate: false });
  ws.on("open", () => resolve(ws));
  ws.on("error", () => resolve(null));
  ws.on("close", (code) => closes.push({ i, code, at: Date.now() }));
  ws.on("message", (d) => { const m = JSON.parse(d.toString()); if (m.type === "system.reload") reloads.set(m.artifactHash, (reloads.get(m.artifactHash) ?? 0) + 1); });
  sockets.push(ws);
});
for (let i = 0; i < count; i += 50) await Promise.all(Array.from({ length: Math.min(50, count - i) }, (_, j) => open(i + j)));
const opened = sockets.filter((s) => s.readyState === 1).length;
log("opened", opened);

const results = [];
const invoker = await open(-1);
invoker.on("message", (d) => { const m = JSON.parse(d.toString()); if (m.type === "result") results.push({ ...m, at: Date.now() }); });
const ticking = setInterval(() => invoker.readyState === 1 && invoker.send("invoke"), 200);
await sleep(3000);

const out = { count, opened, deploys: [] };
for (let k = 0; k < deploys; k++) {
  const build = `${run}-${k}`;
  const d = await new Promise((resolve) => {
    const t0 = Date.now();
    const p = spawn(join(spike, "node_modules/.bin/wrangler"), ["deploy", "--var", `BUILD:${build}`, "--durable-objects-code-update-mode", "immediate"], { cwd: join(spike, "split/tenant"), stdio: ["ignore", "pipe", "pipe"] });
    let text = "";
    p.stdout.on("data", (x) => (text += x));
    p.stderr.on("data", (x) => (text += x));
    p.on("close", (c) => resolve({ code: c, t0, t1: Date.now(), ms: Date.now() - t0, tail: c ? text.slice(-300) : undefined }));
  });
  await sleep(20000);
  const window = results.filter((r) => r.at >= d.t0);
  const failed = window.filter((r) => !r.ok);
  const firstNew = window.find((r) => r.ok && r.build === build);
  const row = {
    build, deploy: d, invocations: window.length, failed: failed.length,
    failedWindowMs: failed.length ? failed.at(-1).at - failed[0].at : 0,
    errors: [...new Set(failed.map((f) => f.error))].slice(0, 3),
    newBuildAfterDeployMs: firstNew ? firstNew.at - d.t1 : null,
    reloadsReceived: reloads.get(build) ?? 0,
    socketCloses: closes.filter((c) => c.at >= d.t0).length,
    rpcMs: stats(window.filter((r) => r.ok).map((r) => r.ms)),
  };
  out.deploys.push(row);
  log("deploy", JSON.stringify(row));
}
clearInterval(ticking);
out.closesTotal = closes.length;
for (const s of sockets) s?.close();
save("split-deploy", out);
console.log(JSON.stringify({ opened, closesTotal: out.closesTotal, deploys: out.deploys.map((d) => [d.build, d.failed, d.failedWindowMs, d.newBuildAfterDeployMs, d.reloadsReceived, d.socketCloses]) }));
process.exit(0);
