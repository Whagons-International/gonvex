#!/usr/bin/env node
// Drives one S4 scenario against a runner Worker and summarises what the
// tenant object recorded.
//
//   node scripts/scenario.mjs --base http://127.0.0.1:8787 --runner workflow --preset sse5
//   S4_TOKEN=... node scripts/scenario.mjs --base https://gonvex-cf-dev-s4-runner.<sub>.workers.dev ...
//
// Options: --runner queue|workflow|alarm|alarm-await, --preset NAME or --spec JSON,
// --tenant NAME, --timeout SECONDS, --kill-runner-at SECONDS (alarm runners),
// --crash-tenant-at SECONDS, --replay (re-send the applied result afterwards),
// --redispatch-at SECONDS (duplicate hand-off), --skip-handoff, --out FILE,
// --resume ID (re-attach to a running Action; pass its --tenant too).
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const at = argv.indexOf(`--${name}`);
  if (at < 0) return fallback;
  const next = argv[at + 1];
  return next === undefined || next.startsWith("--") ? true : next;
};

const PRESETS = {
  agent30: { steps: [{ kind: "agent", seconds: 1800, turnSeconds: 20, pauseSeconds: 10 }] },
  agent20: { steps: [{ kind: "agent", seconds: 1200, turnSeconds: 20, pauseSeconds: 10 }] },
  agent3: { steps: [{ kind: "agent", seconds: 180, turnSeconds: 20, pauseSeconds: 10 }] },
  sse5: { steps: [{ kind: "sse", seconds: 300, intervalMs: 200 }] },
  sse30s: { steps: [{ kind: "sse", seconds: 30, intervalMs: 200 }] },
  dl64: { steps: [{ kind: "download", mb: 64, mode: "stream" }] },
  dl512: { steps: [{ kind: "download", mb: 512, mode: "stream" }] },
  dl512hash: { steps: [{ kind: "download", mb: 512, mode: "hash" }] },
  dl1024hash: { steps: [{ kind: "download", mb: 1024, mode: "hash" }] },
  dl64buf: { steps: [{ kind: "download", mb: 64, mode: "buffer" }] },
  dl32buf: { steps: [{ kind: "download", mb: 32, mode: "buffer" }] },
  slow800: { steps: [{ kind: "slow", seconds: 800 }] },
  slow120: { steps: [{ kind: "slow", seconds: 120 }] },
  poll80: { steps: [{ kind: "poll", seconds: 80, everyMs: 2000 }] },
  // Kill attempt 1 (isolate OOM on the platform) 90 s in.
  crash: {
    steps: [{ kind: "agent", seconds: 240, turnSeconds: 20, pauseSeconds: 10 }],
    crash: { onAttempt: 1, afterSeconds: 90, how: "oom" },
  },
  // Attempt 1 stops renewing 45 s in but keeps running (a zombie). The lease
  // expires, the sweep re-dispatches, attempt 2 wins, attempt 1's result is refused.
  stall: {
    steps: [{ kind: "agent", seconds: 600, turnSeconds: 20, pauseSeconds: 10 }],
    stall: { onAttempt: 1, afterSeconds: 45 },
  },
  stallShort: {
    steps: [{ kind: "agent", seconds: 150, turnSeconds: 20, pauseSeconds: 10 }],
    stall: { onAttempt: 1, afterSeconds: 20 },
  },
  quick: { steps: [{ kind: "sse", seconds: 5, intervalMs: 200 }, { kind: "poll", seconds: 6, everyMs: 2000 }] },
};

const base = opt("base", "http://127.0.0.1:8787");
const runner = opt("runner", "workflow");
const tenant = opt("tenant", `tenant-s4-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
const presetName = opt("preset", "quick");
const spec = opt("spec") ? JSON.parse(opt("spec")) : PRESETS[presetName];
const timeoutS = Number(opt("timeout", 3600));
const pollMs = Number(opt("poll", 5000));
const killRunnerAt = Number(opt("kill-runner-at", 0));
const crashTenantAt = Number(opt("crash-tenant-at", 0));
const redispatchAt = Number(opt("redispatch-at", 0));
const token = process.env.S4_TOKEN;
if (!spec) throw new Error(`unknown preset ${presetName}`);

const headers = { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) };
const call = async (method, path, body) => {
  // The driver's own network can blip; retry so a long run keeps its summary.
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await callOnce(method, path, body);
    } catch (error) {
      if (attempt >= 20) throw error;
      console.error(`driver ${method} ${path} failed (${error.cause?.code ?? error.message}); retrying`);
      await new Promise((resolve) => setTimeout(resolve, 3000 * attempt));
    }
  }
};
const callOnce = async (method, path, body) => {
  const sep = path.includes("?") ? "&" : "?";
  const response = await fetch(`${base}${path}${sep}tenant=${encodeURIComponent(tenant)}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    return { status: response.status, text };
  }
};

/** SHA-256 of the upstream's deterministic N MB body (see src/bytes.ts). */
function expectedSha(mb) {
  const BLOCK = 64 * 1024;
  const block = Buffer.alloc(BLOCK);
  for (let i = 0; i < BLOCK; i += 1) block[i] = (i * 31 + 7) & 0xff;
  const hash = createHash("sha256");
  for (let index = 0; index < (mb * 1024 * 1024) / BLOCK; index += 1) {
    block.writeUInt32LE(index, 0);
    hash.update(block);
  }
  return hash.digest("hex");
}

const log = (...parts) => console.log(new Date().toISOString().slice(11, 19), ...parts);
const t0 = Date.now();
// --resume ID re-attaches to a running Action (pass the same --tenant).
const resume = opt("resume");
const { id } = resume
  ? { id: resume }
  : await callOnce("POST", "/actions", { runner, spec, skipHandoff: Boolean(opt("skip-handoff", false)) });
if (!id) throw new Error("enqueue failed");
log(`${resume ? "resumed" : "enqueued"} ${id} runner=${runner} preset=${presetName} tenant=${tenant}`);

let state;
let killed = false;
let crashedTenant = false;
let redispatched = false;
let seen = 0;
for (;;) {
  await new Promise((resolve) => setTimeout(resolve, pollMs));
  const elapsed = (Date.now() - t0) / 1000;
  if (killRunnerAt && !killed && elapsed >= killRunnerAt) {
    killed = true;
    log("killing runner object", JSON.stringify(await call("POST", `/actions/${id}/runner/kill`)));
  }
  if (crashTenantAt && !crashedTenant && elapsed >= crashTenantAt) {
    crashedTenant = true;
    log("crashing tenant object", JSON.stringify(await call("POST", "/tenant/crash")));
  }
  if (redispatchAt && !redispatched && elapsed >= redispatchAt) {
    redispatched = true;
    log("duplicate hand-off", JSON.stringify(await call("POST", `/actions/${id}/redispatch`)));
  }
  state = await call("GET", `/actions/${id}`);
  for (const event of (state.events ?? []).slice(seen)) {
    if (event.kind !== "renewed") log(`  ${event.kind} attempt=${event.attempt ?? "-"} ${event.detail ?? ""}`);
  }
  seen = state.events?.length ?? seen;
  const status = state.row?.status;
  if (status === "done" || status === "dead" || elapsed > timeoutS) break;
}

// Give a zombie attempt time to try its (refused) completion.
const settle = Number(opt("settle", 0));
if (settle) {
  log(`settling ${settle}s for late completions`);
  await new Promise((resolve) => setTimeout(resolve, settle * 1000));
  state = await call("GET", `/actions/${id}`);
}

let replay = null;
if (opt("replay", false) && state.row?.status === "done") {
  replay = await call("POST", `/actions/${id}/complete`, { attempt: state.row.attempts, result: { replay: true } });
  const stale = await call("POST", `/actions/${id}/complete`, { attempt: state.row.attempts - 1, result: { stale: true } });
  replay = { sameAttempt: replay, staleAttempt: stale };
  state = await call("GET", `/actions/${id}`);
}

const events = state.events ?? [];
const counts = {};
for (const event of events) counts[event.kind] = (counts[event.kind] ?? 0) + 1;
const renewAt = events.filter((event) => event.kind === "renewed").map((event) => event.at);
const gaps = renewAt.slice(1).map((at, i) => at - renewAt[i]);
const claimed = events.filter((event) => event.kind === "claimed");
const summary = {
  id,
  tenant,
  runner,
  preset: presetName,
  status: state.row?.status,
  attempts: state.row?.attempts,
  totalS: events.length ? Math.round((Date.now() - events[0].at) / 100) / 10 : null,
  enqueueToClaimMs: claimed[0] ? claimed[0].at - events[0].at : null,
  claims: claimed.map((event) => ({ attempt: event.attempt, atS: Math.round((event.at - events[0].at) / 100) / 10, detail: JSON.parse(event.detail) })),
  events: counts,
  renewGapMs: gaps.length
    ? { n: gaps.length, min: Math.min(...gaps), max: Math.max(...gaps), mean: Math.round(gaps.reduce((a, b) => a + b, 0) / gaps.length) }
    : null,
  effects: state.effects?.length,
  appliedTotalOnTenant: state.appliedTotal,
  replay,
  result: state.row?.result,
  lastError: state.row?.last_error,
};
for (const step of spec.steps) {
  if (step.kind === "download") summary.expectedSha256 = expectedSha(step.mb);
}
if (runner.startsWith("alarm")) summary.runnerState = await call("GET", `/actions/${id}/runner`);
console.log(JSON.stringify(summary, null, 2));
const out = opt("out");
if (out) {
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, JSON.stringify({ summary, events }, null, 2));
}
