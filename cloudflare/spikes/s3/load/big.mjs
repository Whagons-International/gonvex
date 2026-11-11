// Message size caps: inbound (client -> object) around 32 MiB, outbound (object -> client) above it.
import { writeFileSync, mkdirSync } from "node:fs";
import WebSocket from "ws";
import { env, admin, sleep } from "./lib.mjs";

const { base, key } = env();
const tenant = `big-${Date.now().toString(36)}`;
const MiB = 1024 * 1024;
const out = { inbound: [], outbound: [] };
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);

function connect() {
  return new Promise((resolve, reject) => {
    const u = new URL(base.replace(/^http/, "ws") + "/ws");
    u.searchParams.set("k", key);
    u.searchParams.set("t", tenant);
    u.searchParams.set("m", "big");
    u.searchParams.set("c", "-1");
    const ws = new WebSocket(u, { perMessageDeflate: false, maxPayload: 0 });
    ws.on("open", () => resolve(ws));
    ws.on("error", reject);
  });
}

function sendAndWait(ws, payload, timeoutMs = 60000) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const done = (r) => {
      clearTimeout(t);
      ws.removeAllListeners("message");
      ws.removeAllListeners("close");
      resolve({ ...r, ms: Date.now() - t0 });
    };
    const t = setTimeout(() => done({ result: "timeout" }), timeoutMs);
    ws.on("message", (d) => done({ result: "reply", reply: JSON.parse(d.toString()) }));
    ws.on("close", (code, reason) => done({ result: "closed", code, reason: reason.toString() }));
    ws.send(payload);
  });
}

let ws = await connect();
for (const [label, bytes] of [["1 MiB", MiB], ["16 MiB", 16 * MiB], ["32 MiB - 1 KiB", 32 * MiB - 1024], ["32 MiB - 16", 32 * MiB - 16], ["32 MiB", 32 * MiB], ["32 MiB + 1", 32 * MiB + 1], ["33 MiB", 33 * MiB]]) {
  for (const kind of ["text", "binary"]) {
    if (ws.readyState !== WebSocket.OPEN) ws = await connect();
    const payload = kind === "text" ? "a".repeat(bytes) : Buffer.alloc(bytes, 97);
    const r = await sendAndWait(ws, payload);
    out.inbound.push({ label, bytes, kind, ...r });
    log("inbound", label, kind, JSON.stringify(r));
  }
}

// Outbound: the object sends a big frame to the "big" socket.
for (const [label, bytes] of [["1 MiB", MiB], ["32 MiB", 32 * MiB], ["33 MiB", 33 * MiB], ["64 MiB", 64 * MiB], ["100 MiB", 100 * MiB]]) {
  if (ws.readyState !== WebSocket.OPEN) ws = await connect();
  const got = new Promise((resolve) => {
    const t0 = Date.now();
    const t = setTimeout(() => resolve({ result: "timeout" }), 90000);
    ws.once("message", (d) => (clearTimeout(t), resolve({ result: "received", len: d.length, ms: Date.now() - t0 })));
    ws.once("close", (code, reason) => (clearTimeout(t), resolve({ result: "closed", code, reason: reason.toString() })));
  });
  const r = await admin(`t/${tenant}/sendbig`, { bytes, kind: "text" });
  const g = await got;
  out.outbound.push({ label, bytes, admin: { status: r.status, body: r.body }, client: g });
  log("outbound", label, r.status, JSON.stringify(r.body).slice(0, 200), JSON.stringify(g));
  ws.removeAllListeners("message");
  ws.removeAllListeners("close");
}
ws.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/big-${process.env.S3_LOCAL ? "local" : "cf"}.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
