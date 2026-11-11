#!/usr/bin/env node
/**
 * Dead-peer detection for an idle presence socket (0016 G0-6).
 *
 *   node dead-peer.mjs <base-url> <stub-admin-key>
 *
 * Seeds a project, a tenant and two Members through the K05 stub routes,
 * opens a watcher (subscribed) and a victim (`presence.set active`, then
 * silent: the shipped SDK sends nothing periodic under presence v2). It then
 * prints `READY <victim local port>` and waits for a line on stdin: the
 * caller makes the victim's peer vanish (packets dropped both ways, no FIN or
 * RST) and writes `DROPPED`. The script reports when the watcher sees the
 * victim's Member go offline, as one JSON line:
 *
 *   {"droppedAt":…,"offlineAt":…,"detectionMs":…,"frames":…}
 *
 * `detectionMs` = offline patch time - drop time; it includes the 10 s
 * offline grace. `run-dead-peer.sh` drives it inside a network namespace;
 * against a deploy, drop the victim's port with the host firewall instead.
 */
import { createConnection, createServer } from "node:net";
import { createInterface } from "node:readline";

const [base, key] = process.argv.slice(2);
if (!base || !key) { console.error("usage: dead-peer.mjs <base-url> <stub-admin-key>"); process.exit(2); }
const wsUrl = base.replace(/^http/, "ws") + "/ws";
const limitMs = Number(process.env.DEAD_PEER_LIMIT_MS ?? 30 * 60_000);

async function stub(method, route, body) {
  const r = await fetch(`${base}/_stub/session/${route}`, { method, headers: { authorization: `Bearer ${key}`, "content-type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  if (!r.ok) throw new Error(`${route}: ${r.status} ${await r.text()}`);
  return r.json();
}

/**
 * The victim connects through a local relay so its server-side TCP connection
 * has a known local port (`relayPort`): dropping that 4-tuple makes the peer
 * vanish for the server exactly as a dead client network would.
 */
let relayPort = null;
const relay = await new Promise((resolve) => {
  const target = new URL(base);
  const server = createServer((client) => {
    const upstream = createConnection({ host: target.hostname, port: Number(target.port || 80) }, () => { relayPort = upstream.localPort; });
    client.pipe(upstream).pipe(client);
    const end = () => { client.destroy(); upstream.destroy(); };
    client.on("error", end); upstream.on("error", end);
  });
  server.listen(0, "127.0.0.1", () => resolve(server));
});
const relayUrl = `ws://127.0.0.1:${relay.address().port}/ws`;

function open(url = wsUrl) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const frames = [], waiters = [];
    ws.addEventListener("message", (e) => {
      const f = JSON.parse(String(e.data));
      for (const x of f.type === "replica.batch" ? [...f.items, ...(f.watermark === undefined ? [] : [{ type: "replica.watermark", revision: f.watermark }])] : [f]) frames.push({ at: Date.now(), ...x });
      for (const w of waiters.splice(0)) w();
    });
    ws.addEventListener("open", () => resolve({
      ws, frames,
      send: (f) => ws.send(JSON.stringify(f)),
      async next(match, ms = 15_000) {
        const end = Date.now() + ms;
        for (;;) {
          const i = frames.findIndex(match);
          if (i >= 0) return frames.splice(i, 1)[0];
          if (Date.now() > end) throw new Error("timed out");
          await new Promise(r => { waiters.push(r); setTimeout(r, Math.max(1, end - Date.now())); });
        }
      },
    }), { once: true });
    ws.addEventListener("error", reject, { once: true });
  });
}

const run = Date.now().toString(36);
const { projectId } = await stub("POST", "projects", { name: `dead-peer-${run}` });
await stub("POST", "sync", { project: projectId, module: { hash: "dead-peer" } });
const tenant = `dead-peer-${run}`;
await stub("POST", "tenants", { projectId, id: tenant, name: "dead peer" });
const members = {};
for (const name of ["watcher", "victim"]) {
  members[name] = (await stub("POST", "members", { projectId, tenantId: tenant, email: `${name}-${run}@k14.invalid`, password: "pw-123456", name })).memberId;
}
async function signedIn(name, url) {
  const c = await open(url);
  c.send({ type: "auth", id: "c", controlOnly: true, project: projectId, clientContract: 1 });
  await c.next(f => f.id === "c");
  c.send({ type: "action.call", id: "login", path: "control.auth.passwordLogin", scope: "control", args: { email: `${name}-${run}@k14.invalid`, password: "pw-123456" }, idempotencyKey: "login" });
  const token = (await c.next(f => f.id === "login")).result.accessToken;
  // The SDK's capabilities for these two: presence v2 and batch frames.
  c.send({ type: "auth", id: "a", project: projectId, tenant, token, clientContract: 1, capabilities: { presence: 1, replicaBatchFrames: 1 } });
  if ((await c.next(f => f.id === "a")).type !== "auth.result") throw new Error("auth failed");
  return c;
}

const watcher = await signedIn("watcher");
watcher.send({ type: "presence.subscribe", id: "w" });
await watcher.next(f => f.type === "presence.snapshot");
const victim = await signedIn("victim", relayUrl);
victim.send({ type: "presence.set", state: "active" });
await watcher.next(f => f.type === "presence.patch" && f.changes.some(c => c.memberId === members.victim && c.state === "active"));

// The victim's local port: the address the firewall rule matches.
console.log(`READY ${relayPort}`);
const lines = createInterface({ input: process.stdin });
for await (const line of lines) if (line.trim() === "DROPPED") break;
const droppedAt = Date.now();
console.error(`dropped at ${new Date(droppedAt).toISOString()}; waiting up to ${limitMs} ms`);
const victimFrames = () => victim.frames.filter(f => f.at >= droppedAt).length;
try {
  // Offline arrives as a patch, or as a resync snapshot without the victim (a tenant object reset mid-grace).
  const gone = await watcher.next(f => (f.type === "presence.patch" && f.changes.some(c => c.memberId === members.victim && c.state === "offline"))
    || (f.type === "presence.snapshot" && !f.members.some(m => m.memberId === members.victim)), limitMs);
  console.log(JSON.stringify({ droppedAt, offlineAt: gone.at, detectionMs: gone.at - droppedAt, via: gone.type, victimFramesAfterDrop: victimFrames() }));
} catch {
  console.log(JSON.stringify({ droppedAt, offlineAt: null, detectionMs: null, limitMs, victimFramesAfterDrop: victimFrames() }));
}
process.exit(0);
