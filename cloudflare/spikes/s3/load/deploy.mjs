// What a deploy does to open sockets. Run under bin/cf-env.sh (it calls wrangler deploy).
// Three deploys against one tenant with N sockets that reconnect with 0-1 s jitter:
//   A. default strategy (deferred, 300 s) while the object is hibernated (no traffic)
//   B. deferred 30 s while the object is active (2 writes/s)
//   C. immediate while the object is active
//   node load/deploy.mjs [--count 1000] [--steps A,B,C]
import { spawn } from "node:child_process";
import { writeFileSync, mkdirSync } from "node:fs";
import { Fleet, Control, admin, sleep, pct } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => {
  const i = args.indexOf(`--${k}`);
  return i < 0 ? d : args[i + 1] ?? true;
};
const count = Number(opt("count", 1000));
const steps = String(opt("steps", "A,B,C")).split(",");
const tenant = `dep-${Date.now().toString(36)}`;
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
const out = { tenant, count, steps: [] };

function deploy(build, mode) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const a = ["wrangler", "deploy", "--var", `BUILD:${build}`];
    if (mode) a.push("--durable-objects-code-update-mode", mode);
    const p = spawn("npx", a, { stdio: ["ignore", "pipe", "pipe"] });
    let text = "";
    p.stdout.on("data", (d) => (text += d));
    p.stderr.on("data", (d) => (text += d));
    p.on("close", (code) => resolve({ code, t0, t1: Date.now(), ms: Date.now() - t0, version: /Current Version ID: (\S+)/.exec(text)?.[1] ?? null, tail: code ? text.slice(-400) : undefined }));
  });
}

const fleet = new Fleet({ tenant, count, threads: 4, reconnect: true, reconnectDelay: 1000 });
out.open = await fleet.open();
log("opened", out.open.ok);

async function probe() {
  const c = await new Control({ tenant, m: `probe${Date.now()}` }).open();
  const e = await c.echo();
  c.close();
  return { build: e.build, inst: e.inst, sockets: e.sockets, wakes: e.wakes, rtt: e.rtt };
}

async function step(name, build, mode, active, watchSec) {
  let writer = null;
  let writing = null;
  if (active) {
    writer = await new Control({ tenant }).open();
    writing = setInterval(() => writer.ws.readyState === 1 && writer.write(512), 500);
    await sleep(5000);
  } else {
    await sleep(30000); // let it hibernate
  }
  const before = await probe();
  if (!active) await sleep(30000); // the probe woke it; let it hibernate again
  await fleet.stats(true);
  fleet.events.length = 0;
  const d = await deploy(build, mode);
  log(name, "deploy", JSON.stringify(d));
  // Watch closes and reconnects.
  const watch0 = Date.now();
  const samples = [];
  while (Date.now() - watch0 < watchSec * 1000) {
    await sleep(2000);
    const st = await fleet.stats(false);
    samples.push({ t: Math.round((Date.now() - d.t1) / 1000), open: st.open, closes: st.closes.length });
  }
  if (writing) clearInterval(writing);
  writer?.close();
  const st = await fleet.stats(true);
  const closeAt = st.closes.map((c) => c.at - d.t1).sort((a, b) => a - b);
  const reconnects = fleet.events.filter((e) => e.type === "reconnected" && e.ok);
  const lastReconnect = reconnects.length ? Math.max(...reconnects.map((e) => e.at)) : null;
  const after = await probe();
  const codes = {};
  for (const c of st.closes) codes[`${c.code} ${c.reason}`] = (codes[`${c.code} ${c.reason}`] ?? 0) + 1;
  const r = {
    name,
    mode: mode ?? "default (deferred 300 s)",
    active,
    deploy: d,
    before,
    after,
    closes: st.closes.length,
    closeCodes: codes,
    firstCloseAfterDeployMs: closeAt[0] ?? null,
    lastCloseAfterDeployMs: closeAt[closeAt.length - 1] ?? null,
    reconnected: reconnects.length,
    reconnectFailures: fleet.events.filter((e) => e.type === "reconnected" && !e.ok).length,
    allBackAfterFirstCloseMs: lastReconnect && closeAt.length ? lastReconnect - (d.t1 + closeAt[0]) : null,
    reconnectMsP50: pct(reconnects.map((e) => e.ms).sort((a, b) => a - b), 50),
    reconnectMsP99: pct(reconnects.map((e) => e.ms).sort((a, b) => a - b), 99),
    openAtEnd: st.open,
    samples,
  };
  log(name, JSON.stringify({ ...r, samples: undefined }));
  out.steps.push(r);
}

if (steps.includes("A")) await step("A", "b", null, false, 60);
if (steps.includes("B")) await step("B", "c", "deferred 30s", true, 75);
if (steps.includes("C")) await step("C", "d", "immediate", true, 45);
await fleet.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/deploy-cf.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
