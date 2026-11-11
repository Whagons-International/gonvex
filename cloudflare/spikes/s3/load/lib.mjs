// Shared helpers for the S3 load scenarios.
import { Worker } from "node:worker_threads";
import WebSocket from "ws";

export const PING = '{"type":"ping"}';
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function env() {
  const base = process.env.S3_BASE ?? "http://127.0.0.1:8797";
  const key = process.env.S3_KEY ?? "local-spike-key";
  return { base, key };
}

export async function admin(path, params = {}) {
  const { base, key } = env();
  const u = new URL(base + "/admin/" + path);
  u.searchParams.set("k", key);
  for (const [k, v] of Object.entries(params)) u.searchParams.set(k, String(v));
  const t0 = Date.now();
  const res = await fetch(u, { method: "POST" });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  return { status: res.status, ms: Date.now() - t0, body };
}

export function pct(sorted, p) {
  if (!sorted.length) return null;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

export function histStats(hist) {
  let n = 0;
  for (const c of hist) n += c;
  const at = (p) => {
    const target = Math.ceil((p / 100) * n);
    let acc = 0;
    for (let i = 0; i < hist.length; i++) {
      acc += hist[i];
      if (acc >= target) return i;
    }
    return null;
  };
  let max = null;
  for (let i = hist.length - 1; i >= 0; i--)
    if (hist[i]) {
      max = i;
      break;
    }
  return { n, p50: n ? at(50) : null, p90: n ? at(90) : null, p99: n ? at(99) : null, max };
}

/** A fleet of sockets spread over worker threads. */
export class Fleet {
  constructor({ tenant = "t1", count = 1000, classes = 8, threads = 4, live = false, pingMs = 20000, deflate = false, gateways = 0, concurrency = 25, reconnect = false, reconnectDelay = 0 }) {
    const { base, key } = env();
    this.count = count;
    this.events = [];
    this.workers = [];
    const per = Math.ceil(count / threads);
    for (let t = 0; t < threads; t++) {
      const start = t * per;
      const end = Math.min(count, start + per);
      if (start >= end) break;
      const w = new Worker(new URL("./fleet.mjs", import.meta.url), {
        workerData: { base, key, tenant, start, end, classes, live, pingMs, deflate, gateways, concurrency, reconnect, reconnectDelay },
      });
      w.pending = [];
      w.on("message", (m) => {
        if (m.type === "reconnected" || m.type === "msg") this.events.push(m);
        else w.pending.shift()?.(m);
      });
      w.on("error", (e) => console.error("fleet worker error", e));
      this.workers.push(w);
    }
  }
  call(w, cmd) {
    return new Promise((resolve) => {
      w.pending.push(resolve);
      w.postMessage(cmd);
    });
  }
  all(cmd) {
    return Promise.all(this.workers.map((w) => this.call(w, cmd)));
  }
  async open() {
    const t0 = Date.now();
    const r = await this.all({ type: "open" });
    const results = r.flatMap((x) => x.results);
    const ms = results.map((x) => x.ms).sort((a, b) => a - b);
    return {
      wallMs: Date.now() - t0,
      ok: results.filter((x) => x.ok).length,
      failed: results.filter((x) => !x.ok).map((x) => x.err).slice(0, 5),
      retried: results.filter((x) => x.retries).length,
      retryErrors: [...new Set(results.filter((x) => x.retries).map((x) => x.firstErr))].slice(0, 5),
      connectP50: pct(ms, 50),
      connectP99: pct(ms, 99),
      extensions: [...new Set(results.map((x) => x.ext))],
    };
  }
  async stats(reset = false) {
    const r = await this.all({ type: "stats", reset });
    const hist = new Uint32Array(r[0].hist.length);
    const perSeq = new Map();
    for (const x of r) {
      for (let i = 0; i < hist.length; i++) hist[i] += x.hist[i];
      for (const [seq, [c, mn, mx]] of x.perSeq) {
        const s = perSeq.get(seq);
        if (s) {
          s[0] += c;
          s[1] = Math.min(s[1], mn);
          s[2] = Math.max(s[2], mx);
        } else perSeq.set(seq, [c, mn, mx]);
      }
    }
    return {
      latency: histStats(hist),
      frames: r.reduce((s, x) => s + x.frames, 0),
      other: r.reduce((s, x) => s + x.other, 0),
      pongs: r.reduce((s, x) => s + x.pongs, 0),
      protoPings: r.reduce((s, x) => s + x.protoPings, 0),
      open: r.reduce((s, x) => s + x.open, 0),
      bytesRead: r.reduce((s, x) => s + x.bytesRead, 0),
      closes: r.flatMap((x) => x.closes),
      perSeq,
    };
  }
  async silence(ids) {
    await this.all({ type: "silence", ids });
  }
  async setReconnect(on) {
    await this.all({ type: "setReconnect", on });
  }
  async close() {
    await this.all({ type: "close" });
    for (const w of this.workers) await w.terminate();
  }
}

/** One control socket in the main thread (writer, echo probes). */
export class Control {
  constructor({ tenant = "t1", m = "writer", c = -1, deflate = false, gw = null } = {}) {
    const { base, key } = env();
    const u = new URL(base.replace(/^http/, "ws") + "/ws");
    u.searchParams.set("k", key);
    u.searchParams.set("t", tenant);
    u.searchParams.set("m", m);
    u.searchParams.set("c", String(c));
    if (gw !== null) u.searchParams.set("gw", String(gw));
    this.url = u.toString();
    this.deflate = deflate;
    this.waiters = [];
    this.log = [];
  }
  open() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url, { perMessageDeflate: this.deflate });
      this.ws.on("open", () => resolve(this));
      this.ws.on("error", reject);
      this.ws.on("close", (code, reason) => {
        this.closed = { code, reason: reason.toString(), at: Date.now() };
      });
      this.ws.on("message", (data) => {
        const s = data.toString();
        if (s.startsWith('{"type":"replica')) return;
        let msg;
        try {
          msg = JSON.parse(s);
        } catch {
          msg = { raw: s.slice(0, 80) };
        }
        msg.recvAt = Date.now();
        const w = this.waiters.findIndex((x) => x.match(msg));
        if (w >= 0) this.waiters.splice(w, 1)[0].resolve(msg);
        else this.log.push(msg);
      });
    });
  }
  wait(match, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("timeout")), timeoutMs);
      this.waiters.push({ match, resolve: (m) => (clearTimeout(t), resolve(m)) });
    });
  }
  async echo() {
    const id = Math.random();
    const w = Date.now();
    this.ws.send(JSON.stringify({ op: "echo", id, w }));
    const m = await this.wait((x) => x.type === "echo" && x.id === id);
    return { ...m, rtt: m.recvAt - w };
  }
  write(size = 2048) {
    const w = Date.now();
    this.ws.send(JSON.stringify({ op: "write", w, size }));
    return w;
  }
  close() {
    this.ws?.close(1000);
  }
}

/** Local only: V8 heap of the local workerd isolate via its inspector (S3_INSPECTOR=ws://...). */
export async function inspectorUrl() {
  const { execSync } = await import("node:child_process");
  const { readFileSync } = await import("node:fs");
  const lines = execSync("ss -ltnpH", { encoding: "utf8" }).split("\n");
  for (const line of lines) {
    const pid = /pid=(\d+)/.exec(line)?.[1];
    const port = /127\.0\.0\.1:(\d+)/.exec(line)?.[1];
    if (!pid || !port) continue;
    let cmd = "";
    try {
      cmd = readFileSync(`/proc/${pid}/cmdline`, "latin1");
    } catch {}
    if (!cmd.includes("cf-S3-sockets") || !cmd.includes("--inspector-addr=")) continue;
    try {
      const list = await (await fetch(`http://127.0.0.1:${port}/json/list`, { signal: AbortSignal.timeout(1000) })).json();
      const t = list.find((x) => String(x.id).startsWith("core:user:"));
      if (t) return t.webSocketDebuggerUrl;
    } catch {}
  }
  return null;
}

export async function localHeap({ gc = true } = {}) {
  if (!process.env.S3_LOCAL) return null;
  const url = await inspectorUrl();
  if (!url) return null;
  return new Promise((resolve) => {
    setTimeout(() => resolve(null), 10000);
    const ws = new WebSocket(url);
    let id = 0;
    const pending = new Map();
    const call = (method, params = {}) =>
      new Promise((r) => {
        const i = ++id;
        pending.set(i, r);
        ws.send(JSON.stringify({ id: i, method, params }));
      });
    ws.on("message", (d) => {
      const m = JSON.parse(d.toString());
      if (m.id && pending.has(m.id)) pending.get(m.id)(m);
    });
    ws.on("error", () => resolve(null));
    ws.on("close", () => resolve(null));
    ws.on("open", async () => {
      await call("Runtime.enable");
      if (gc) void call("HeapProfiler.collectGarbage"); // workerd never answers this one; the next call runs after it
      const r = await call("Runtime.getHeapUsage");
      ws.close();
      const x = r.result ?? {};
      resolve({ usedMB: +(x.usedSize / 1048576).toFixed(2), totalMB: +(x.totalSize / 1048576).toFixed(2), embedderMB: +((x.embedderHeapUsedSize ?? 0) / 1048576).toFixed(2), backingMB: +((x.backingStorageSize ?? 0) / 1048576).toFixed(2) });
    });
  });
}
