// Generation switches with sockets on gateway objects (G0-1) and on the tenant object at once.
// Each gateway forwards an "invoke" from one of its sockets to the tenant facet every 300 ms; when the
// answer shows a new generation, the gateway sends system.reload to its sockets.
//   node test/gateway-sockets.mjs [--gateways 4] [--per 125] [--tenant-sockets 200] [--swaps 5] [--code whagons]
import WebSocket from "ws";
import { op, gen, run, sleep, save, log, stats, BASE, KEY } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const gateways = Number(opt("gateways", "4"));
const per = Number(opt("per", "125"));
const tenantSockets = Number(opt("tenant-sockets", "200"));
const swaps = Number(opt("swaps", "5"));
const code = opt("code", "whagons");
const tenant = `gws-${run}`;
const wsBase = BASE.replace(/^http/, "ws");
const all = [];
const openSocket = (path, kind, index) => new Promise((resolve) => {
  const s = { kind, index, reloads: [], closes: [], results: [] };
  const ws = new WebSocket(`${wsBase}${path}?k=${KEY}`, { perMessageDeflate: false });
  s.ws = ws;
  ws.on("open", () => resolve(s));
  ws.on("error", () => resolve(s));
  ws.on("close", (c) => s.closes.push({ code: c, at: Date.now() }));
  ws.on("message", (d) => { const m = JSON.parse(d.toString()); if (m.type === "system.reload") s.reloads.push({ hash: m.artifactHash, at: Date.now() }); else if (m.type === "result") s.results.push({ ...m, at: Date.now() }); });
  all.push(s);
});
await op(tenant, "set-active", { gen: gen(code, "G0") });
await op(tenant, "invoke", { mode: "facet", calls: [{ path: "s5.whoami", args: {}, now: 1, intentEntropy: "aa".repeat(32), identity: { member: { role: "owner" } }, invocation: { commandId: "w" } }] });
for (let g = 0; g < gateways; g++) for (let i = 0; i < per; i += 25) await Promise.all(Array.from({ length: Math.min(25, per - i) }, (_, j) => openSocket(`/g/gw-${run}-${g}`, `gateway${g}`, i + j)));
for (let i = 0; i < tenantSockets; i += 25) await Promise.all(Array.from({ length: Math.min(25, tenantSockets - i) }, (_, j) => openSocket(`/t/${tenant}/ws`, "tenant", i + j)));
const opened = all.filter((s) => s.ws.readyState === 1).length;
log("opened", opened, "of", gateways * per + tenantSockets);
const invokers = all.filter((s) => s.kind.startsWith("gateway") && s.index === 0);
const ticking = setInterval(() => invokers.forEach((s) => s.ws.readyState === 1 && s.ws.send(`invoke:${tenant}`)), 300);
await sleep(3000);
const out = { gateways, per, tenantSockets, opened, code, swaps: [] };
for (let k = 1; k <= swaps; k++) {
  const g = gen(code, `G${k}`);
  const t0 = Date.now();
  const a = await op(tenant, "activate", { gen: g, preload: true, abortFacet: true, drain: true });
  await sleep(3000);
  const tenantGot = all.filter((s) => s.kind === "tenant" && s.reloads.some((r) => r.hash === g.id)).length;
  const gatewayGot = all.filter((s) => s.kind.startsWith("gateway") && s.reloads.some((r) => r.hash === `G${k}`)).length;
  const gwLag = all.filter((s) => s.kind.startsWith("gateway")).map((s) => s.reloads.find((r) => r.hash === `G${k}`)?.at - t0).filter((x) => x >= 0);
  const failed = invokers.flatMap((s) => s.results.filter((r) => r.at >= t0 && !r.ok));
  const row = { k, activateStatus: a.status, preloadMs: a.value.preloadMs, drainMs: a.value.drainMs, tenantReloads: tenantGot, gatewayReloads: gatewayGot, gatewayReloadAfterActivateStartMs: stats(gwLag), failedGatewayCalls: failed.length, failMessages: [...new Set(failed.map((f) => f.message))].slice(0, 2), closesSoFar: all.reduce((n, s) => n + s.closes.length, 0) };
  out.swaps.push(row);
  log("swap", JSON.stringify(row));
}
clearInterval(ticking);
out.closes = all.reduce((n, s) => n + s.closes.length, 0);
all.forEach((s) => s.ws.close());
save("gateway-sockets", out);
console.log(JSON.stringify({ opened, closes: out.closes, swaps: out.swaps.map((s) => [s.k, s.activateStatus, s.tenantReloads, s.gatewayReloads, s.failedGatewayCalls]) }));
process.exit(0);
