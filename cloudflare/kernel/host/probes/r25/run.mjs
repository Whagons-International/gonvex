#!/usr/bin/env node
// K04 R25 driver. Runs every experiment against local workerd or the development account and
// writes raw results to .results/ (git-ignored).
//
//   node run.mjs local                     starts `wrangler dev`, runs the local subset, stops it
//   whagons-dev credentials exec full-cloudflare-access -- node run.mjs platform [--variants default,10000,2000] [--keep]
//
// Platform mode deploys gonvex-cf-dev-k04-r25 once per CPU variant (a generated copy of
// wrangler.jsonc with `limits.cpu_ms`), runs the experiments, then deletes the Worker and checks
// through the API that it is gone. Credentials are read from $WHAGONS_CREDENTIAL_FILE and passed only
// to the wrangler child process and the one deletion-check request; they are never printed.
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const DIR = dirname(fileURLToPath(import.meta.url));
const RESULTS = join(DIR, ".results");
const WRANGLER = join(DIR, "../../../../node_modules/.bin/wrangler");
const NAME = "gonvex-cf-dev-k04-r25";
if (!/^gonvex-cf-dev-/.test(NAME)) throw new Error("unsafe worker name");
mkdirSync(RESULTS, { recursive: true });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => performance.now();
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
const RUN = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

// ---------------------------------------------------------------------------------------------
// HTTP + sockets
let BASE = "";
let KEY = "";
const wsBase = () => BASE.replace(/^http/, "ws");

async function rpc(cls, name, method, arg = {}, timeoutMs = 400_000) {
  const t = now();
  try {
    const res = await fetch(`${BASE}/rpc/${cls}/${encodeURIComponent(name)}/${method}?k=${KEY}`, {
      method: "POST",
      body: JSON.stringify(arg),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text.slice(0, 500) };
    }
    return { status: res.status, clientMs: Math.round(now() - t), ...body };
  } catch (e) {
    return { status: null, clientMs: Math.round(now() - t), fetchError: String(e) };
  }
}

class Sock {
  constructor(label, path, t0ref) {
    this.label = label;
    this.path = path;
    this.ref = t0ref; // { t0 } shared clock origin
    this.events = [];
    this.pending = new Map();
    this.seq = 0;
    this.closed = null;
    this.reconnect = null;
  }
  rel() {
    return Math.round(now() - this.ref.t0);
  }
  open() {
    return new Promise((resolve) => {
      const ws = new WebSocket(`${wsBase()}${this.path}${this.path.includes("?") ? "&" : "?"}k=${KEY}`);
      this.ws = ws;
      const timer = setTimeout(() => resolve(false), 15_000);
      ws.onopen = () => {
        clearTimeout(timer);
        this.events.push({ t: this.rel(), e: "open" });
        resolve(true);
      };
      ws.onmessage = (m) => {
        const msg = JSON.parse(m.data);
        const id = msg.pong ?? msg.call;
        const sent = this.pending.get(String(id));
        this.pending.delete(String(id));
        this.events.push({ t: this.rel(), e: msg.call ? "call" : "pong", rtt: sent ? Math.round(now() - sent) : null, ...msg });
      };
      ws.onclose = (ev) => {
        clearTimeout(timer);
        if (!this.closed) this.closed = { t: this.rel(), code: ev.code, reason: ev.reason, wasClean: ev.wasClean };
        this.events.push({ t: this.rel(), e: "close", code: ev.code, reason: ev.reason });
        resolve(false);
      };
      ws.onerror = () => this.events.push({ t: this.rel(), e: "error" });
    });
  }
  send(kind) {
    if (this.ws?.readyState !== 1) return;
    const id = `${this.seq++}`;
    this.pending.set(id, now());
    this.ws.send(`${kind}:${id}`);
    this.events.push({ t: this.rel(), e: "send", kind, id });
  }
  close() {
    try {
      this.ws?.close(1000, "done");
    } catch {}
  }
  // Time from the close event until a new socket opens and answers a ping.
  async reconnectUntilPong(maxMs = 90_000) {
    const start = now();
    let attempts = 0;
    while (now() - start < maxMs) {
      attempts++;
      const probe = new Sock(this.label + ":re", this.path, this.ref);
      if (await probe.open()) {
        probe.send("p");
        const t = now();
        while (now() - t < 5000 && !probe.events.some((e) => e.e === "pong")) await sleep(20);
        const pong = probe.events.find((e) => e.e === "pong");
        probe.close();
        if (pong) return { ms: Math.round(now() - start), attempts, inst: pong.inst, iso: pong.iso };
      }
      await sleep(250);
    }
    return { ms: null, attempts, gaveUp: true };
  }
}

// ---------------------------------------------------------------------------------------------
// The runaway experiment (questions 1-7). kind: forever | busy | loader-forever
async function runaway(tag, { kind = "forever", iters, cpuMs, wallMs, direct = 10, viaGw = 10, siblings = 8, observeAfterMs = 6000, maxMs = 400_000, reconnectDuring = false } = {}) {
  const T = `r25-${RUN}-${tag}`;
  const ref = { t0: now() };
  const out = { tag, tenant: T, kind, cpuMs: cpuMs ?? null, wallMs: wallMs ?? null };
  log(`[${tag}] setup tenant ${T}`);
  out.seed = (await rpc("t", T, "seed", 50)).result;
  const sibNames = Array.from({ length: siblings }, (_, i) => `${T}-sib${i}`);
  const gwNames = [`${T}-gw0`, `${T}-gw1`];
  out.before = { tenant: out.seed, siblings: {}, gateways: {} };
  await Promise.all(sibNames.map(async (s) => (out.before.siblings[s] = (await rpc("t", s, "seed", 5)).result)));

  const socks = [];
  for (let i = 0; i < direct; i++) socks.push(Object.assign(new Sock(`direct${i}`, `/ws/t/${T}`, ref), { group: "direct" }));
  for (let i = 0; i < viaGw; i++) socks.push(Object.assign(new Sock(`gw${i}`, `/ws/g/${gwNames[i % 2]}?t=${encodeURIComponent(T)}`, ref), { group: "gateway" }));
  for (const s of sibNames) socks.push(Object.assign(new Sock(`sib:${s}`, `/ws/t/${s}`, ref), { group: "sibling" }));
  const opened = await Promise.all(socks.map((s) => s.open()));
  out.socketsOpened = opened.filter(Boolean).length;
  await Promise.all(gwNames.map(async (g) => (out.before.gateways[g] = (await rpc("g", g, "info")).result)));
  out.before.tenant = (await rpc("t", T, "info")).result;

  // Ping loop: every socket every 500 ms; gateway sockets alternate a gateway-local ping and a
  // tenant call through the gateway.
  let tick = 0;
  const pinger = setInterval(() => {
    tick++;
    for (const s of socks) s.send(s.group === "gateway" && tick % 2 === 0 ? "c" : "p");
  }, 500);
  await sleep(1500);

  // Fire the runaway, then queue other calls behind it.
  ref.t0 = now();
  log(`[${tag}] runaway ${kind} start`);
  const runP = rpc("t", T, "reducer", { cmd: `${tag}-run`, kind, iters, cpuMs, wallMs }, maxMs).then((r) => ({ ...r, endRel: Math.round(now() - ref.t0) }));
  const queued = [];
  const queue = (label, at, p) =>
    queued.push(
      sleep(at).then(async () => {
        const startRel = Math.round(now() - ref.t0);
        const r = await p();
        return { label, startRel, endRel: Math.round(now() - ref.t0), ...r };
      }),
    );
  queue("tenant.info", 300, () => rpc("t", T, "info"));
  queue("tenant.reducer(ok)", 400, () => rpc("t", T, "reducer", { cmd: `${tag}-ok1`, kind: "ok" }));
  queue("gateway.callTenant(info)", 500, () => rpc("g", gwNames[0], "callTenant", { tenant: T, method: "info" }));
  queue("gateway.info (local)", 600, () => rpc("g", gwNames[1], "info"));
  queue("sibling.info", 700, () => rpc("t", sibNames[0], "info"));
  queue("tenant.info@5s", 5000, () => rpc("t", T, "info"));

  // Optional: a new client tries to open a tenant socket every 2 s while the overrun lasts.
  let runDone = false;
  runP.then(() => (runDone = true));
  const during = [];
  const duringLoop = reconnectDuring
    ? (async () => {
        await sleep(1000);
        while (!runDone) {
          const startRel = Math.round(now() - ref.t0);
          const probe = new Sock("during", `/ws/t/${T}`, ref);
          const opened = await probe.open();
          let pong = null;
          if (opened) {
            probe.send("p");
            const t = now();
            while (now() - t < 3000 && !probe.events.some((e) => e.e === "pong" || e.e === "close")) await sleep(20);
            pong = probe.events.find((e) => e.e === "pong") ?? null;
          }
          during.push({ startRel, opened, pongAt: pong?.t ?? null, newInst: pong?.inst ?? null, closed: probe.closed, endRel: Math.round(now() - ref.t0) });
          probe.close();
          await sleep(2000);
        }
      })()
    : Promise.resolve();
  out.runaway = await runP;
  await duringLoop;
  out.reconnectDuring = during;
  log(`[${tag}] runaway done in ${out.runaway.endRel} ms: ${out.runaway.error?.message ?? (out.runaway.ok ? "ok" : out.runaway.fetchError ?? out.runaway.raw)}`);
  out.queued = await Promise.all(queued);
  const runEnd = out.runaway.endRel;
  await sleep(observeAfterMs);
  clearInterval(pinger);

  // Reconnect every closed tenant-held socket (in parallel) and time it.
  const reconnects = await Promise.all(
    socks.filter((s) => s.closed).map(async (s) => ({ label: s.label, closedAt: s.closed.t, ...(await s.reconnectUntilPong()) })),
  );

  out.after = { tenant: (await rpc("t", T, "info")).result, siblings: {}, gateways: {} };
  await Promise.all(sibNames.map(async (s) => (out.after.siblings[s] = (await rpc("t", s, "info")).result)));
  await Promise.all(gwNames.map(async (g) => (out.after.gateways[g] = (await rpc("g", g, "info")).result)));
  for (const s of socks) s.close();

  // Per-group socket summary.
  const groups = {};
  for (const s of socks) {
    const g = (groups[s.group] ??= { n: 0, closed: 0, closes: {}, pongsDuring: 0, pongsAfter: 0, callsDuring: [], maxPongGapMs: 0, lastPongBeforeEnd: null });
    g.n++;
    if (s.closed) {
      g.closed++;
      const k = `${s.closed.code} ${JSON.stringify(s.closed.reason)}`;
      (g.closes[k] ??= []).push(s.closed.t);
    }
    let last = null;
    for (const e of s.events) {
      if (e.e === "pong") {
        if (e.t >= 0 && e.t <= runEnd) g.pongsDuring++;
        if (e.t > runEnd) g.pongsAfter++;
        if (last !== null && e.t >= 0) g.maxPongGapMs = Math.max(g.maxPongGapMs, e.t - last);
        last = e.t;
      }
      if (e.e === "call" && e.t >= 0 && e.t <= runEnd + 3000) g.callsDuring.push({ t: e.t, ok: e.ok, ms: e.ms, err: e.error?.message });
    }
  }
  for (const g of Object.values(groups)) {
    // Compress the call list: distinct outcomes with counts and first/last time.
    const by = {};
    for (const c of g.callsDuring) {
      const k = c.ok ? "ok" : c.err;
      const b = (by[k] ??= { count: 0, first: c.t, last: c.t, maxMs: 0 });
      b.count++;
      b.last = c.t;
      b.maxMs = Math.max(b.maxMs, c.ms ?? 0);
    }
    g.callsDuring = by;
  }
  out.groups = groups;
  out.reconnects = reconnects;
  const isoA = out.before.tenant?.iso;
  out.colocation = {
    tenantIso: isoA,
    workerIsoOfRunawayRequest: out.runaway.workerIso ?? null,
    siblingsSameIso: Object.values(out.before.siblings).filter((x) => x?.iso === isoA).map((x) => x.name),
    gatewaysSameIso: Object.values(out.before.gateways).filter((x) => x?.iso === isoA).map((x) => x.name),
    siblingsReset: sibNames.filter((s) => out.before.siblings[s]?.inst !== out.after.siblings[s]?.inst),
    gatewaysReset: gwNames.filter((g) => out.before.gateways[g]?.inst !== out.after.gateways[g]?.inst),
    gatewayIsoAfter: gwNames.map((g) => out.after.gateways[g]?.iso),
    siblingIsosBefore: [...new Set(Object.values(out.before.siblings).map((x) => x?.iso))],
  };
  out.state = {
    ctorBefore: out.before.tenant?.ctor,
    ctorAfter: out.after.tenant?.ctor,
    instChanged: out.before.tenant?.inst !== out.after.tenant?.inst,
    seedRows: out.after.tenant?.seed,
    partialRows: out.after.tenant?.partial,
  };
  out.socketEvents = Object.fromEntries(socks.map((s) => [s.label, s.events]));
  return out;
}

// Q9c: retry storm with and without a quarantine marker.
async function retryStorm(tag, attempts = 3) {
  const out = {};
  for (const poison of ["none", "caller", "inflight"]) {
    const T = `r25-${RUN}-${tag}-${poison}`;
    await rpc("t", T, "seed", 10);
    const before = (await rpc("t", T, "info")).result;
    const tries = [];
    for (let i = 0; i < (poison === "none" ? 2 : attempts); i++) {
      const r = await rpc("t", T, "reducer", { cmd: "poisoned-1", kind: "forever", poison });
      tries.push({ ms: r.clientMs, ok: r.ok, error: r.error?.message ?? r.fetchError ?? r.raw, marked: r.marked ?? null });
      log(`[${tag}] retry ${poison} #${i + 1}: ${r.clientMs} ms ${r.error?.message ?? ""}`);
      await sleep(500);
    }
    const after = (await rpc("t", T, "info")).result;
    out[poison] = { tries, ctorBefore: before?.ctor, ctorAfter: after?.ctor, resets: (after?.ctor ?? 0) - (before?.ctor ?? 0), poison: after?.poison, inflight: after?.inflight };
  }
  // Cost of the durable inflight marker (write + storage.sync()) on a healthy Reducer.
  const T = `r25-${RUN}-${tag}-cost`;
  await rpc("t", T, "seed", 1);
  for (const poison of ["none", "check", "inflight"]) {
    const ms = [], sync = [];
    for (let i = 0; i < 20; i++) {
      const r = await rpc("t", T, "reducer", { cmd: `c-${poison}-${i}`, kind: "ok", poison });
      ms.push(r.result?.ms);
      sync.push(r.result?.syncMs);
    }
    out[`cost_${poison}`] = { insideObjectMs: stats(ms), syncMs: stats(sync) };
  }
  return out;
}

async function loaderSuite(tag, { runaway = true } = {}) {
  const out = {};
  const T = `r25-${RUN}-${tag}-loader`;
  await rpc("t", T, "seed", 10);
  out.latency = await rpc("t", T, "loaderLatency", { n: 20, hostCalls: 1 }, 120_000);
  if (out.latency.result) {
    out.latencyStats = { emptyRoundTripMs: stats(out.latency.result.empty.slice(1)), firstMs: out.latency.result.empty[0], localTxMs: stats(out.latency.result.localTx) };
  }
  for (const mode of ["none", "tx"]) {
    for (const hostCalls of [1, 2, 5]) out[`hostCalls_${mode}_${hostCalls}`] = await rpc("t", `${T}-hc-${mode}-${hostCalls}`, "loaderHostCalls", { hostCalls, mode, wallMs: 3000 });
  }
  // Runaway in the dynamic isolate without a loader CPU limit: the tenant's wall timeout fires,
  // then check whether the next loader call to the same (still spinning) isolate works.
  if (!runaway) return out;
  out.noLimitRunaway = await rpc("t", `${T}-nolimit`, "reducer", { cmd: "nl", kind: "loader-forever", wallMs: 5000 });
  out.afterNoLimit = await rpc("t", `${T}-nolimit`, "reducer", { cmd: "nl2", kind: "loader-busy", iters: 1000, wallMs: 20_000 });
  out.afterNoLimitState = (await rpc("t", `${T}-nolimit`, "info")).result;
  return out;
}

// Client-visible cost of running module code in a dynamic isolate, measured in the stateless
// Worker around the tenant RPC (Date.now advances across I/O there).
async function latencySuite(tag, n = 30) {
  const T = `r25-${RUN}-${tag}-lat`;
  await rpc("t", T, "seed", 1);
  const out = {};
  for (const [label, arg] of [
    ["inObject", { kind: "ok" }],
    ["loader", { kind: "loader-busy", iters: 0, wallMs: 5000 }],
    ["loaderCpuLimit", { kind: "loader-busy", iters: 0, cpuMs: 1000, wallMs: 5000 }],
    ["inObject2", { kind: "ok" }],
  ]) {
    const w = [], c = [], errs = [];
    for (let i = 0; i < n; i++) {
      const r = await rpc("t", T, "reducer", { cmd: `${label}-${i}`, ...arg });
      if (!r.ok) errs.push(r.error?.message ?? r.fetchError);
      w.push(r.ms);
      c.push(r.clientMs);
    }
    out[label] = { workerMs: stats(w), clientMs: stats(c), firstWorkerMs: w[0], errors: errs.slice(0, 3) };
  }
  return out;
}

function stats(xs) {
  const v = xs.filter((x) => typeof x === "number").sort((a, b) => a - b);
  if (!v.length) return null;
  const q = (p) => v[Math.min(v.length - 1, Math.floor(p * v.length))];
  return { n: v.length, min: v[0], p50: q(0.5), p90: q(0.9), max: v[v.length - 1] };
}

// ---------------------------------------------------------------------------------------------
// Experiment sets
async function clockAndGuard(tag) {
  const T = `r25-${RUN}-${tag}-clock`;
  return {
    clock: await rpc("t", T, "clock", { iters: 3e8, cte: 3e6 }),
    guardClock: await rpc("t", `${T}-g1`, "guarded", { cmd: "g1", budgetMs: 1000, maxIters: 2e9 }),
    guardCounter: await rpc("t", `${T}-g2`, "guarded", { cmd: "g2", budgetMs: 1e9, maxCalls: 1000, maxIters: 2e9 }),
  };
}

function save(name, data) {
  const file = join(RESULTS, `${RUN}-${name}.json`);
  writeFileSync(file, JSON.stringify(data, null, 2));
  log(`wrote ${file}`);
}

// ---------------------------------------------------------------------------------------------
// Local
async function local() {
  const port = 8826;
  BASE = `http://127.0.0.1:${port}`;
  KEY = "local";
  const dev = spawn(WRANGLER, ["dev", "--port", String(port), "--var", "ADMIN_KEY:local"], { cwd: DIR, env: { ...process.env, WRANGLER_SEND_METRICS: "false" }, stdio: ["ignore", "pipe", "pipe"], detached: true });
  let devLog = "";
  dev.stdout.on("data", (d) => (devLog += d));
  dev.stderr.on("data", (d) => (devLog += d));
  try {
    await waitHealthy();
    const out = { mode: "local" };
    out.clockAndGuard = await clockAndGuard("local");
    out.loader = await loaderSuite("local", { runaway: false });
    // A finite busy Reducer (~5-10 s CPU) to see the input gate locally without hanging workerd.
    out.busy = await runaway("local-busy", { kind: "busy", iters: 2e9, siblings: 2, maxMs: 120_000 });
    save("local", out);
    // An endless loop: local workerd enforces no CPU limit, so observe 45 s and stop workerd.
    out.forever = await runaway("local-forever", { kind: "forever", siblings: 2, maxMs: 45_000, observeAfterMs: 1000 });
    out.forever.note = "client gave up after 45 s; workerd was killed afterwards";
    save("local", out);
  } finally {
    try {
      process.kill(-dev.pid, "SIGKILL");
    } catch {}
    writeFileSync(join(RESULTS, `${RUN}-local-wrangler.log`), devLog.replace(/\x1b\[[0-9;]*m/g, ""));
  }
}

async function waitHealthy() {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(5000) });
      if (r.ok) return;
    } catch {}
    await sleep(1000);
  }
  throw new Error("worker did not become healthy");
}

// ---------------------------------------------------------------------------------------------
// Platform
function credentials() {
  const file = process.env.WHAGONS_CREDENTIAL_FILE;
  if (!file) throw new Error("run under: whagons-dev credentials exec full-cloudflare-access -- node run.mjs platform");
  const c = JSON.parse(readFileSync(file, "utf8"));
  if (!c.accountId || !c.apiToken) throw new Error("credential is missing accountId or apiToken (values not shown)");
  return { accountId: c.accountId, apiToken: c.apiToken };
}

function wrangler(args, creds) {
  const env = { ...process.env, WRANGLER_SEND_METRICS: "false", CLOUDFLARE_ACCOUNT_ID: creds.accountId, CLOUDFLARE_API_TOKEN: creds.apiToken };
  delete env.WHAGONS_CREDENTIAL_FILE;
  return new Promise((resolve) => {
    const child = spawn(WRANGLER, args, { cwd: DIR, env, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (out += d));
    child.on("close", (code) => resolve({ code, out: out.replace(/\x1b\[[0-9;]*m/g, "") }));
  });
}

function variantConfig(variant, { loader = true } = {}) {
  const base = JSON.parse(readFileSync(join(DIR, "wrangler.jsonc"), "utf8").replace(/^\s*\/\/.*$/gm, ""));
  delete base.$schema;
  base.main = "../worker.js";
  if (variant !== "default") base.limits = { cpu_ms: Number(variant) };
  if (!loader) delete base.worker_loaders;
  const file = join(RESULTS, `wrangler.${variant}.json`);
  writeFileSync(file, JSON.stringify(base, null, 2));
  return file;
}

async function deploy(variant, creds, meta) {
  let cfg = variantConfig(variant);
  let r = await wrangler(["deploy", "--config", cfg, "--var", `ADMIN_KEY:${KEY}`], creds);
  meta.deploys.push({ variant, code: r.code, tail: r.out.split("\n").filter((l) => /error|warn|limit|loader|Uploaded|Deployed|cpu/i.test(l)).slice(0, 20) });
  if (r.code !== 0 && /loader/i.test(r.out)) {
    meta.loaderAvailable = false;
    cfg = variantConfig(variant, { loader: false });
    r = await wrangler(["deploy", "--config", cfg, "--var", `ADMIN_KEY:${KEY}`], creds);
    meta.deploys.push({ variant, retryWithoutLoader: true, code: r.code });
  }
  if (r.code !== 0) throw new Error(`deploy ${variant} failed:\n${r.out.slice(-2000)}`);
  const url = r.out.match(/https:\/\/[a-z0-9.-]+\.workers\.dev/)?.[0];
  if (!url) throw new Error("no workers.dev URL in deploy output");
  BASE = url;
  await waitHealthy();
  await sleep(3000);
}

async function followup(creds, meta, results) {
  log("=== followup: deploy default");
  await deploy("default", creds, meta);
  const v = (results.followup = {});
  v.latency = await latencySuite("f");
  save("platform-followup", results);
  v.runaway = await runaway("f-default", { kind: "forever", siblings: 4, reconnectDuring: true });
  save("platform-followup", results);
  // Loader runaway without a loader CPU limit, with sockets on the tenant: does the tenant's 5 s
  // wall timer fire while the dynamic isolate spins?
  v.loaderNoLimit = await runaway("f-loader-nolimit", { kind: "loader-forever", wallMs: 5000, siblings: 4, reconnectDuring: true });
  save("platform-followup", results);
}

async function platform(variants, keep) {
  const creds = credentials();
  KEY = randomBytes(18).toString("hex");
  const meta = { mode: "platform", run: RUN, variants, deploys: [], loaderAvailable: true };
  const results = { meta };
  try {
    if (variants[0] === "followup") {
      await followup(creds, meta, results);
      variants = [];
    }
    for (const variant of variants) {
      log(`=== variant ${variant}: deploy`);
      await deploy(variant, creds, meta);
      const v = (results[variant] = { workerIso: (await (await fetch(`${BASE}/health`)).json()).iso });
      if (variant === "default") {
        v.clockAndGuard = await clockAndGuard(`d`);
        save("platform", results);
        if (meta.loaderAvailable) {
          v.loader = await loaderSuite("d");
          save("platform", results);
          v.loaderRunaway = await runaway("d-loader", { kind: "loader-forever", cpuMs: 1000, wallMs: 5000, siblings: 2 });
          save("platform", results);
        }
      }
      v.runaway = await runaway(`v${variant}`, { kind: "forever" });
      save("platform", results);
      if (variant === variants[variants.length - 1]) {
        v.retryStorm = await retryStorm(`v${variant}-retry`);
        save("platform", results);
      }
    }
  } finally {
    if (!keep) {
      log(`deleting ${NAME}`);
      const d = await wrangler(["delete", "--name", NAME, "--force"], creds);
      const check = await fetch(`https://api.cloudflare.com/client/v4/accounts/${creds.accountId}/workers/scripts/${NAME}`, {
        headers: { authorization: `Bearer ${creds.apiToken}` },
      });
      meta.deleted = { wranglerExit: d.code, apiStatusAfterDelete: check.status, gone: check.status === 404 };
      log(`delete: wrangler exit ${d.code}, API GET script -> HTTP ${check.status}`);
    }
    save(variants.length ? "platform" : "platform-followup", results);
  }
}

const [mode, ...rest] = process.argv.slice(2);
const vi = rest.indexOf("--variants");
const variants = vi >= 0 ? rest[vi + 1].split(",") : ["default", "10000", "2000"];
if (mode === "local") await local();
else if (mode === "platform") await platform(variants, rest.includes("--keep"));
else console.log("usage: node run.mjs local | platform [--variants default,10000,2000] [--keep]");
process.exit(0);
