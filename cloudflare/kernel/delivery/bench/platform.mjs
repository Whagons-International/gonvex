#!/usr/bin/env node
// Platform measurements of replica delivery against a deployed K10 harness
// (`kernel/delivery/harness/platform.mjs deploy`):
//
//  1. G1 latency: one writer commits, one reader receives the delta through its
//     Replica Collection; per commit, call → result and call → reader's delta.
//  2. A large window: `rows` items of `pad` characters, opened cold by a raw
//     reader. The snapshot streams in pages through a 128 MB isolate (two
//     passes over 4 MiB); the bench reports bytes, pages and time, and checks
//     the tenant object still serves afterwards.
//
//   node kernel/delivery/bench/platform.mjs <url> <stub key> [rows=2500] [pad=20000] [commits=40]
import { randomUUID } from "node:crypto";

const [url, key, rowsArg = "2500", padArg = "20000", commitsArg = "40"] = process.argv.slice(2);
if (!url || !key) throw new Error("usage: platform.mjs <url> <stub key> [rows] [pad] [commits]");
const http = url.replace(/\/$/, "");
const ws = http.replace(/^http/, "ws") + "/ws";
const ROWS = Number(rowsArg), PAD = Number(padArg), COMMITS = Number(commitsArg);
const CAPS = { replicaNoOldValues: 1, replicaSnapshotPages: 1, replicaWatermark: 1, replicaReadyMany: 1 };

async function stub(method, route, body) {
  const r = await fetch(`${http}/_stub/session/${route}`, { method, headers: { authorization: `Bearer ${key}`, "content-type": "application/json" }, body: body && JSON.stringify(body) });
  if (!r.ok) throw new Error(`${route}: ${r.status} ${await r.text()}`);
  return r.json();
}

class Socket {
  constructor() { this.inbox = []; this.waiters = []; this.bytes = 0; }
  async open() {
    this.ws = new WebSocket(ws);
    this.ws.addEventListener("message", (e) => { const t = String(e.data); this.bytes += t.length; this.inbox.push(JSON.parse(t)); this.waiters.splice(0).forEach((f) => f()); });
    await new Promise((resolve, reject) => { this.ws.addEventListener("open", resolve, { once: true }); this.ws.addEventListener("error", reject, { once: true }); });
    return this;
  }
  send(frame) { this.ws.send(JSON.stringify(frame)); }
  async next(test, ms = 120_000) {
    const deadline = Date.now() + ms;
    for (;;) {
      const i = this.inbox.findIndex(test);
      if (i >= 0) return this.inbox.splice(i, 1)[0];
      if (Date.now() > deadline) throw new Error("timed out");
      await new Promise((r) => { this.waiters.push(r); setTimeout(r, 1000); });
    }
  }
}

async function login(socket, project, email) {
  const id = randomUUID();
  socket.send({ type: "auth", id, project, controlOnly: true, clientContract: 1, capabilities: {} });
  await socket.next((m) => m.id === id);
  const call = randomUUID();
  socket.send({ type: "action.call", id: call, path: "control.auth.passwordLogin", scope: "control", args: { email, password: "conformance-password-1" } });
  const r = await socket.next((m) => m.id === call && (m.type === "action.result" || m.type === "action.error"));
  if (r.type !== "action.result") throw new Error(r.error);
  return r.result.accessToken;
}

async function member(project, tenant, name) {
  await stub("POST", "members", { projectId: project, tenantId: tenant, email: `${name}@bench.invalid`, password: "conformance-password-1", name });
  const s = await new Socket().open();
  const token = await login(s, project, `${name}@bench.invalid`);
  const id = randomUUID();
  s.send({ type: "auth", id, project, token, tenant, clientContract: 1, capabilities: CAPS });
  await s.next((m) => m.id === id && m.type === "auth.result");
  return s;
}

async function call(socket, path, args) {
  const id = randomUUID();
  const at = performance.now();
  socket.send({ type: "reducer.call", id, path, args });
  const r = await socket.next((m) => m.id === id && (m.type === "reducer.result" || m.type === "reducer.error"));
  if (r.type !== "reducer.result") throw new Error(`${path}: ${r.error}`);
  return { at, ms: performance.now() - at, revision: r.committedRevision };
}

const pct = (values, p) => { const s = [...values].sort((a, b) => a - b); return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] * 10) / 10; };

// The conformance fixture, built as the harness built the bundles it was deployed with.
const { buildFixture } = await import(new URL("../../../conformance/src/fixture.mjs", import.meta.url).href);
const { mkdtemp } = await import("node:fs/promises");
const { tmpdir } = await import("node:os");
const { manifest } = await buildFixture(await mkdtemp(`${tmpdir()}/k10-bench-`), 1);
const { projectId } = await stub("POST", "projects", { name: `k10-bench-${Date.now()}` });
await stub("POST", "sync", { ...manifest, project: projectId });
const tenant = `k10-bench-${randomUUID().slice(0, 8)}`;
await stub("POST", "tenants", { projectId, id: tenant, name: "K10 bench" });
const writer = await member(projectId, tenant, "writer");
const reader = await member(projectId, tenant, "reader");

// 1. G1 latency.
reader.send({ type: "replica.open", id: "g1", path: "board.all", args: {} });
await reader.next((m) => m.type === "replica.ready" && m.id === "g1");
const results = [], deltas = [];
for (let i = 0; i < COMMITS; i++) {
  const done = await call(writer, "board.add", { id: `g1-${i}`, title: `row ${i}`, position: i });
  results.push(done.ms);
  await reader.next((m) => m.type === "replica.delta" && m.id === "g1" && m.upserts.some((r) => r.id === `g1-${i}`));
  deltas.push(performance.now() - done.at);
}
console.log(JSON.stringify({ g1: { commits: COMMITS, resultMs: { p50: pct(results, 50), p95: pct(results, 95) }, readerDeltaMs: { p50: pct(deltas, 50), p95: pct(deltas, 95) } } }));

// 2. A large window.
let filled = 0;
const perCall = Math.max(1, Math.floor((2 << 20) / (PAD + 64)));
const fillAt = performance.now();
while (filled < ROWS) {
  const count = Math.min(perCall, ROWS - filled);
  await call(writer, "windows.winFillLarge", { prefix: "big", grp: "g", from: filled, count, pad: PAD });
  filled += count;
}
const fillMs = performance.now() - fillAt;
const before = reader.bytes;
const openAt = performance.now();
reader.send({ type: "replica.open", id: "big", path: "windows.winAll", args: {} });
let pages = 0, rows = 0;
for (;;) {
  const m = await reader.next((x) => x.id === "big" && (x.type === "replica.snapshotPage" || x.type === "replica.ready" || x.type === "replica.error"), 600_000);
  if (m.type === "replica.error") throw new Error(m.error);
  if (m.type === "replica.ready") break;
  pages += 1;
  rows += m.result.length;
}
const openMs = performance.now() - openAt;
const after = await call(writer, "board.add", { id: "after-big", title: "the object still serves", position: 0 });
const stats = await (await fetch(`${http}/_stub/delivery/stats?tenant=${tenant}`, { headers: { authorization: `Bearer ${key}` } })).json();
console.log(JSON.stringify({ window: { rows, pages, megabytes: Math.round(((reader.bytes - before) / 2 ** 20) * 10) / 10, fillMs: Math.round(fillMs), openMs: Math.round(openMs), callAfterMs: Math.round(after.ms) }, stats }));
process.exit(0);
