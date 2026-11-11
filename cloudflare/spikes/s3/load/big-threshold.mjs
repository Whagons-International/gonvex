// Narrow the platform message caps found by big.mjs.
import { writeFileSync, mkdirSync } from "node:fs";
import WebSocket from "ws";
import { env, admin } from "./lib.mjs";
const { base, key } = env();
const tenant = `bigt-${Date.now().toString(36)}`;
const MiB = 1024 * 1024;
const out = { inbound: [], outbound: [] };
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
function connect() {
  return new Promise((resolve, reject) => {
    const u = new URL(base.replace(/^http/, "ws") + "/ws");
    for (const [k, v] of Object.entries({ k: key, t: tenant, m: "big", c: "-1" })) u.searchParams.set(k, v);
    const ws = new WebSocket(u, { perMessageDeflate: false, maxPayload: 0 });
    ws.on("open", () => resolve(ws));
    ws.on("error", reject);
  });
}
function wait(ws, ms, fn) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const done = (r) => (clearTimeout(t), ws.removeAllListeners("message"), ws.removeAllListeners("close"), resolve({ ...r, ms: Date.now() - t0 }));
    const t = setTimeout(() => done({ result: "timeout" }), ms);
    ws.on("message", (d) => done({ result: "message", len: d.length }));
    ws.on("close", (code, reason) => done({ result: "closed", code, reason: reason.toString() }));
    fn();
  });
}
let ws = await connect();
for (const sub of [768, 512, 256, 128, 64, 32]) {
  const bytes = 32 * MiB - sub;
  if (ws.readyState !== 1) ws = await connect();
  const r = await wait(ws, 20000, () => ws.send(Buffer.alloc(bytes, 97)));
  out.inbound.push({ bytes, sub, ...r });
  log("inbound 32MiB -", sub, JSON.stringify(r));
}
for (const bytes of [4 * MiB, 8 * MiB, 16 * MiB, 24 * MiB, 31 * MiB, 32 * MiB - 1024]) {
  if (ws.readyState !== 1) ws = await connect();
  let adminR;
  const r = await wait(ws, 30000, () => admin(`t/${tenant}/sendbig`, { bytes, kind: "binary" }).then((x) => (adminR = x.body)));
  out.outbound.push({ bytes, admin: adminR, ...r });
  log("outbound", bytes, JSON.stringify(adminR), JSON.stringify(r));
}
ws.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/big-threshold-cf.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
