// Does anything close or ping an idle socket? Three sockets on one tenant, held for --minutes:
//   silent: sends nothing at all (today's SDK on an idle tenant with presence v2)
//   pinger: sends the ping frame every 20 s (proposed SDK change)
//   proto:  sends a WebSocket protocol Ping every 20 s (what a native client could do; invisible to the object)
import { writeFileSync, mkdirSync } from "node:fs";
import WebSocket from "ws";
import { env, admin, PING } from "./lib.mjs";

const minutes = Number(process.argv[2] ?? 20);
const { base, key } = env();
const tenant = `idle-${Date.now().toString(36)}`;
const out = { minutes, tenant, sockets: {} };
const t0 = Date.now();
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);

function open(name) {
  const u = new URL(base.replace(/^http/, "ws") + "/ws");
  u.searchParams.set("k", key);
  u.searchParams.set("t", tenant);
  u.searchParams.set("m", name);
  u.searchParams.set("c", "-1");
  u.searchParams.set("live", "1");
  const ws = new WebSocket(u, { perMessageDeflate: false });
  const st = (out.sockets[name] = { serverPings: 0, pongs: 0, messages: 0, closed: null });
  ws.on("ping", () => st.serverPings++);
  ws.on("pong", () => st.pongs++);
  ws.on("message", () => st.messages++);
  ws.on("close", (code, reason) => {
    st.closed = { code, reason: reason.toString(), afterSec: Math.round((Date.now() - t0) / 1000) };
    log(name, "closed", st.closed);
  });
  return ws;
}
const silent = open("silent");
const pinger = open("pinger");
const proto = open("proto");
const timer = setInterval(() => {
  if (pinger.readyState === 1) pinger.send(PING);
  if (proto.readyState === 1) proto.ping();
}, 20000);
await new Promise((r) => setTimeout(r, minutes * 60000));
clearInterval(timer);
out.stats = (await admin(`t/${tenant}/stats`)).body;
for (const ws of [silent, pinger, proto]) ws.close(1000);
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/idle-${process.env.S3_LOCAL ? "local" : "cf"}.json`, import.meta.url), JSON.stringify(out, null, 2));
log(JSON.stringify(out));
process.exit(0);
