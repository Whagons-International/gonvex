// K16 platform probe: do the public limits trip at Rust's default counts on a
// deployed kernel Worker, where the edge sets CF-Connecting-IP? Needs no seeded
// tenant: a public auth to an unknown tenant is still a call the budgets count.
//
//   node kernel/public/probe/limits.mjs https://gonvex-cf-dev-preview-<slug>.<account>.workers.dev [out.json]
//
// Every check runs from this machine's one address, in this order (each needs
// the budgets the previous ones left): calls per connection, calls per address,
// public sockets, pending sockets. Run it once per fresh deploy (or wait 60 s
// between runs so the address window restarts).
import { writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";

const base = process.argv[2];
if (!base) throw new Error("usage: limits.mjs <worker url> [out.json]");
const ws = base.replace(/^http/, "ws") + "/ws";

function open(publicTransport) {
  return new Promise((resolve, reject) => {
    // One address family: IPv4 and IPv6 are different client addresses, so a dual-stack client mixing them has two budgets.
    const socket = new WebSocket(publicTransport ? `${ws}?public=1` : ws, { family: 4 });
    const inbox = [];
    const s = { socket, inbox, waiters: [] };
    socket.on("message", (d) => { inbox.push(JSON.parse(String(d))); for (const w of s.waiters.splice(0)) w(); });
    socket.on("unexpected-response", (_q, res) => { let body = ""; res.on("data", c => { body += c; }); res.on("end", () => resolve({ refused: { status: res.statusCode, body } })); });
    socket.on("error", (e) => { if (socket.readyState !== WebSocket.OPEN) reject(e); });
    socket.on("open", () => resolve(s));
  });
}
async function next(s, id) {
  const end = Date.now() + 15_000;
  for (;;) {
    const i = s.inbox.findIndex(m => m.id === id || (id === "session.ready" && m.type === id));
    if (i >= 0) return s.inbox.splice(i, 1)[0];
    if (Date.now() > end) throw new Error(`timed out waiting for ${id}`);
    await Promise.race([new Promise(r => s.waiters.push(r)), delay(100)]);
  }
}
const auth = async (s, id) => { s.socket.send(JSON.stringify({ type: "auth", id, project: "k16-probe", tenant: "none", public: true })); return next(s, id); };
const ready = async (p) => { const s = await open(p); if (s.refused) throw new Error(`refused ${s.refused.status} ${s.refused.body}`); await next(s, "session.ready"); return s; };
const errors = (replies) => [...new Set(replies.map(r => r.error))];

const result = { url: base, at: new Date().toISOString() };
const t0 = Date.now();

// 30 calls per connection: the 31st is refused.
const one = await ready(true);
const first = [];
for (let i = 0; i < 31; i++) first.push(await auth(one, `c${i}`));
result.connection = { first30: errors(first.slice(0, 30)), call31: first[30].error };

// 120 per address: this connection has used 31; three more connections of 30 reach 121.
const sockets = [one];
const perSocket = [];
for (let n = 0; n < 3; n++) {
  const s = await ready(true);
  sockets.push(s);
  const replies = [];
  for (let i = 0; i < 30; i++) replies.push(await auth(s, `s${n}-${i}`));
  perSocket.push(replies.filter(r => r.error !== "public call rate limit exceeded").length);
}
// The first socket's 31st call was refused by its connection window but still charged to the address window
// (Rust charges both independently), so 120 - 31 = 89 are admitted over the next three.
result.address = { admittedPerSocket: perSocket, expected: [30, 30, 29] };

for (const s of sockets) s.socket.close();
await delay(2_000);

// Socket caps. The preview has no tenant to authenticate in, so every socket here stays pending and the 10 s auth
// deadline closes it (and frees its slots, as in Rust): each cap is filled in parallel and checked at once.
const parallel = (n, publicTransport) => Promise.all(Array.from({ length: n }, () => open(publicTransport)));
const filled = async (n, publicTransport) => {
  const t = Date.now();
  const opened = await parallel(n, publicTransport);
  const extra = await open(publicTransport);
  const refusedEarly = opened.filter(s => s.refused).length;
  return { opened, extra, refusedEarly, ms: Date.now() - t };
};

// 20 public sockets per address: the 21st is refused; a close frees a slot.
{
  const { opened, extra, refusedEarly, ms } = await filled(20, true);
  result.publicSockets = { refusedOf20: refusedEarly, socket21: extra.refused ?? "admitted", fillMs: ms };
  const live = opened.filter(s => !s.refused);
  if (!extra.refused) live.push(extra);
  live.pop().socket.close();
  let again;
  for (let i = 0; i < 30 && !again; i++) { const s = await open(true); if (s.refused) await delay(100); else again = s; }
  result.publicSockets.afterClose = again ? "admitted" : "refused";
  if (again) live.push(again);
  for (const s of live) s.socket.close();
}
await delay(2_000);

// 100 pending sockets per address (99 member-transport + 1 public): the 101st, on either transport, is refused.
{
  const members = await parallel(99, false);
  const one = await open(true);
  const extra = await open(false);
  const extraPublic = await open(true);
  const all = [...members, one, extra, extraPublic];
  result.pending = { refusedOf100: all.slice(0, 100).filter(s => s.refused).length, socket101: extra.refused ?? "admitted", socket101Public: extraPublic.refused ?? "admitted" };
  for (const s of all) if (!s.refused) s.socket.close();
}
result.ms = Date.now() - t0;

console.log(JSON.stringify(result, null, 2));
if (process.argv[3]) await writeFile(process.argv[3], JSON.stringify(result, null, 2) + "\n");
process.exit(0);
