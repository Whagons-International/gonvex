// Socket fleet: a worker thread that holds a share of the client sockets and
// records delivery latency of fan-out frames. Controlled by load/lib.mjs.
import { parentPort, workerData } from "node:worker_threads";
import WebSocket from "ws";

const PING = '{"type":"ping"}';
const cfg = workerData;
const HIST = 30001; // 1 ms buckets, 0..30 s, last bucket = overflow
let hist = new Uint32Array(HIST);
let frames = 0;
let other = 0;
let pongs = 0;
let protoPings = 0;
let closes = [];
const perSeq = new Map(); // seq -> [count, minLat, maxLat]
const sockets = new Map(); // index -> state

function record(seq, lat) {
  hist[Math.max(0, Math.min(HIST - 1, Math.round(lat)))]++;
  const s = perSeq.get(seq);
  if (s) {
    s[0]++;
    if (lat < s[1]) s[1] = lat;
    if (lat > s[2]) s[2] = lat;
  } else perSeq.set(seq, [1, lat, lat]);
}

function url(i) {
  const u = new URL(cfg.base.replace(/^http/, "ws") + "/ws");
  u.searchParams.set("k", cfg.key);
  u.searchParams.set("t", cfg.tenant);
  u.searchParams.set("m", `m${i}`);
  u.searchParams.set("c", String(i % cfg.classes));
  if (cfg.live) u.searchParams.set("live", "1");
  if (cfg.gateways > 0) u.searchParams.set("gw", String(i % cfg.gateways));
  return u.toString();
}

function open(i) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const ws = new WebSocket(url(i), { handshakeTimeout: 15000, perMessageDeflate: cfg.deflate ? { clientNoContextTakeover: false, serverNoContextTakeover: false } : false });
    const st = sockets.get(i) ?? { i, silent: false };
    st.ws = ws;
    st.open = false;
    sockets.set(i, st);
    let done = false;
    const finish = (ok, err) => {
      if (done) return;
      done = true;
      resolve({ i, ok, ms: Date.now() - t0, err, ext: ws.extensions || "" });
    };
    ws.on("open", () => {
      st.open = true;
      st.openedAt = Date.now();
      finish(true);
    });
    ws.on("ping", () => protoPings++);
    ws.on("message", (data, isBinary) => {
      const now = Date.now();
      if (isBinary) {
        other++;
        return;
      }
      const s = data.length > 64 ? data.toString("latin1", 0, 64) : data.toString();
      if (s.startsWith('{"type":"replica.transaction","s":')) {
        const m = /"s":(\d+),"w":(\d+)/.exec(s);
        frames++;
        if (m && Number(m[2]) > 0) record(Number(m[1]), now - Number(m[2]));
      } else if (s === '{"type":"pong"}') pongs++;
      else {
        other++;
        if (cfg.forward) parentPort.postMessage({ type: "msg", i, data: data.toString(), at: now });
      }
    });
    ws.on("close", (code, reason) => {
      const at = Date.now();
      if (st.ws === ws) st.open = false;
      closes.push({ i, code, reason: reason.toString(), at, openFor: st.openedAt ? at - st.openedAt : null });
      finish(false, `close ${code}`);
      if (cfg.reconnect && !st.stopped && st.ws === ws) {
        setTimeout(() => open(i).then((r) => parentPort.postMessage({ type: "reconnected", ...r, at: Date.now() })), Math.random() * (cfg.reconnectDelay ?? 0));
      }
    });
    ws.on("error", (e) => finish(false, String(e.message)));
  });
}

async function openAll() {
  const results = [];
  const ids = [];
  for (let i = cfg.start; i < cfg.end; i++) ids.push(i);
  let next = 0;
  async function lane() {
    while (next < ids.length) {
      const i = ids[next++];
      let r = await open(i);
      for (let attempt = 1; !r.ok && attempt < 4; attempt++) {
        // A stuck upgrade is retried; the attempts are reported.
        r = { ...(await open(i)), retries: attempt, firstErr: r.err };
      }
      results.push(r);
    }
  }
  await Promise.all(Array.from({ length: cfg.concurrency }, lane));
  return results;
}

let pingTimer;
function startPings() {
  if (!cfg.live || !cfg.pingMs) return;
  // Spread pings evenly over the interval.
  const list = [...sockets.values()];
  let k = 0;
  const step = Math.max(1, Math.floor(cfg.pingMs / Math.max(1, list.length)));
  pingTimer = setInterval(() => {
    const st = list[k++ % list.length];
    if (st && st.open && !st.silent) st.ws.send(PING);
  }, step);
}

function bytesRead() {
  let n = 0;
  for (const st of sockets.values()) n += st.ws?._socket?.bytesRead ?? 0;
  return n;
}

parentPort.on("message", async (cmd) => {
  if (cmd.type === "open") {
    const r = await openAll();
    startPings();
    parentPort.postMessage({ type: "opened", results: r });
  } else if (cmd.type === "stats") {
    parentPort.postMessage({
      type: "stats",
      hist: cmd.reset ? hist : hist.slice(),
      frames,
      other,
      pongs,
      protoPings,
      closes,
      open: [...sockets.values()].filter((s) => s.open).length,
      bytesRead: bytesRead(),
      perSeq: [...perSeq.entries()],
    });
    if (cmd.reset) {
      hist = new Uint32Array(HIST);
      frames = 0;
      other = 0;
      pongs = 0;
      closes = [];
      perSeq.clear();
    }
  } else if (cmd.type === "silence") {
    for (const i of cmd.ids) {
      const st = sockets.get(i);
      if (st) st.silent = true;
    }
    parentPort.postMessage({ type: "ok" });
  } else if (cmd.type === "send") {
    const st = sockets.get(cmd.i);
    st.ws.send(cmd.data);
    parentPort.postMessage({ type: "ok" });
  } else if (cmd.type === "setReconnect") {
    cfg.reconnect = cmd.on;
    parentPort.postMessage({ type: "ok" });
  } else if (cmd.type === "close") {
    clearInterval(pingTimer);
    for (const st of sockets.values()) {
      st.stopped = true;
      try {
        st.ws.close(1000);
      } catch {}
    }
    setTimeout(() => parentPort.postMessage({ type: "closed" }), 500);
  }
});
