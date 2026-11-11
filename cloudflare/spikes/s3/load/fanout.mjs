// Fan-out scenario: 1,000 sockets in 8 classes, single commits, admin fan-out
// CPU probes, then a burst of 30 writes/s for 60 s.
//   node load/fanout.mjs [--count 1000] [--deflate] [--gateways 4] [--burst-rate 30] [--burst-seconds 60] [--skip-probes]
import { readFileSync, readdirSync, writeFileSync, mkdirSync } from "node:fs";
import { Fleet, Control, admin, sleep, histStats, pct } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => {
  const i = args.indexOf(`--${k}`);
  return i < 0 ? d : args[i + 1] === undefined || args[i + 1].startsWith("--") ? true : args[i + 1];
};
const count = Number(opt("count", 1000));
const deflate = !!opt("deflate", false);
const gateways = Number(opt("gateways", 0));
const rate = Number(opt("burst-rate", 30));
const seconds = Number(opt("burst-seconds", 60));
const tenant = opt("tenant", `fan-${Date.now().toString(36)}`);
const label = opt("label", `fanout${deflate ? "-deflate" : ""}${gateways ? `-gw${gateways}` : ""}`);

// Local only: workerd CPU and RSS from /proc.
function workerdPids() {
  const out = [];
  for (const d of readdirSync("/proc")) {
    if (!/^\d+$/.test(d)) continue;
    try {
      const cmd = readFileSync(`/proc/${d}/cmdline`, "latin1");
      if (cmd.includes("workerd") && cmd.includes("serve") && cmd.includes("cf-S3-sockets")) out.push(Number(d));
    } catch {}
  }
  return out;
}
function local() {
  if (!process.env.S3_LOCAL) return null;
  let cpu = 0;
  let rss = 0;
  for (const pid of workerdPids()) {
    const f = readFileSync(`/proc/${pid}/stat`, "latin1").split(") ")[1].split(" ");
    cpu += (Number(f[11]) + Number(f[12])) * 10; // ms (CLK_TCK 100)
    rss += Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "latin1"))[1]) / 1024;
  }
  return { cpuMs: cpu, rssMB: Math.round(rss * 10) / 10 };
}

const result = { label, tenant, count, deflate, gateways, startedAt: new Date().toISOString() };
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);

result.localBefore = local();
const fleet = new Fleet({ tenant, count, deflate, gateways, threads: Number(opt("threads", 4)) });
result.open = await fleet.open();
log("opened", result.open);
const writer = await new Control({ tenant, deflate }).open();
await sleep(1000);
result.localAfterOpen = local();
result.stats = (await admin(`t/${tenant}/stats`)).body;
result.colos = { tenant: (await admin(`t/${tenant}/where`)).body.colo, gateways: [] };
for (let g = 0; g < gateways; g++) result.colos.gateways.push((await admin(`g/${tenant}/${g}/where`)).body.colo);
log("colos", result.colos);
log("tenant stats", { sockets: result.stats.sockets, classes: result.stats.classes });

// Single commits, 1 s apart.
await fleet.stats(true);
const singles = [];
for (let i = 0; i < Number(opt("singles", 20)); i++) {
  const w = writer.write(2048);
  const ack = await writer.wait((m) => m.type === "ack" && m.w === w);
  singles.push({ seq: ack.seq, ackMs: ack.recvAt - w, sent: ack.sent, gwSent: ack.gwSent, hop: ack.hop, bytes: ack.bytes });
  await sleep(1000);
}
await sleep(2000);
let st = await fleet.stats(true);
for (const s of singles) {
  const p = st.perSeq.get(s.seq);
  s.delivered = p?.[0] ?? 0;
  s.firstMs = p?.[1];
  s.lastMs = p?.[2];
}
result.singles = {
  perCommit: singles,
  latency: st.latency,
  ackMs: pct(singles.map((s) => s.ackMs).sort((a, b) => a - b), 50),
  lastDeliveryP50: pct(singles.map((s) => s.lastMs).sort((a, b) => a - b), 50),
  lastDeliveryMax: Math.max(...singles.map((s) => s.lastMs)),
  hopMsAll: singles.flatMap((s) => s.hop).sort((a, b) => a - b),
};
log("singles", { latency: st.latency, lastP50: result.singles.lastDeliveryP50, lastMax: result.singles.lastDeliveryMax, ackP50: result.singles.ackMs });

// Echo RTT baseline (network round trip to the object, no fan-out).
const rtts = [];
for (let i = 0; i < 20; i++) rtts.push((await writer.echo()).rtt);
result.echoRtt = { p50: pct(rtts.sort((a, b) => a - b), 50), min: rtts[0], max: rtts[rtts.length - 1] };
log("echo rtt", result.echoRtt);

// Admin fan-out probes: n fan-outs in one event, frames carry w=0 so receivers only count them.
if (!opt("skip-probes", false) && !gateways) {
  result.probes = [];
  for (const [mode, encodeOnly] of [["cache", 0], ["tags", 0], ["attach", 0], ["cache", 1]]) {
    for (let rep = 0; rep < 3; rep++) {
      const before = local();
      const r = await admin(`t/${tenant}/fanout`, { n: 10, mode, encodeOnly });
      const after = local();
      result.probes.push({ mode, encodeOnly, ms: r.ms, ...r.body, localCpuMs: before && after ? after.cpuMs - before.cpuMs : null });
      await sleep(1500);
    }
  }
  for (const reps of [1, 10]) {
    const before = local();
    const r = await admin(`t/${tenant}/touch`, { reps });
    const after = local();
    result.probes.push({ op: "touch", reps, ms: r.ms, ...r.body, localCpuMs: before && after ? after.cpuMs - before.cpuMs : null });
    const b2 = local();
    const r2 = await admin(`t/${tenant}/rebuild`, { reps: reps * 10 });
    const a2 = local();
    result.probes.push({ op: "rebuild", reps: reps * 10, ms: r2.ms, ...r2.body, localCpuMs: b2 && a2 ? a2.cpuMs - b2.cpuMs : null });
  }
  log("probes", result.probes.map((p) => `${p.op ?? p.mode}${p.encodeOnly ? "/enc" : ""} ${p.ms}ms cpu=${p.localCpuMs}`).join(" | "));
  await sleep(3000);
}

// Bursts: "RATExSECONDS[@SIZE]" steps, e.g. --bursts 10x20,20x20,30x60.
async function burst(rate, seconds, size) {
  await sleep(3000);
  await fleet.stats(true);
  const bytes0 = (await fleet.stats(false)).bytesRead;
  const acks = [];
  const pending = new Set();
  const sink = {
    match: (m) => {
      if (m.type === "ack" && pending.has(m.w)) {
        acks.push({ ms: m.recvAt - m.w, hop: m.hop, sent: m.sent, gwSent: m.gwSent });
        pending.delete(m.w);
      }
      return false;
    },
    resolve: () => {},
  };
  writer.waiters.push(sink);
  const cpu0 = local();
  const total = Math.round(rate * seconds);
  const t0 = Date.now();
  let sent = 0;
  let lastW = 0;
  while (sent < total) {
    const due = t0 + (sent * 1000) / rate;
    const wait = due - Date.now();
    if (wait > 0) await sleep(wait);
    let w = Date.now();
    if (w <= lastW) w = lastW + 1; // unique per write
    lastW = w;
    writer.ws.send(JSON.stringify({ op: "write", w, size }));
    pending.add(w);
    sent++;
  }
  const sendMs = Date.now() - t0;
  // Drain: wait until every ack arrived or 30 s passed.
  const drain0 = Date.now();
  while (pending.size && Date.now() - drain0 < 30000) await sleep(250);
  const drainMs = Date.now() - drain0;
  await sleep(2000);
  writer.waiters.splice(writer.waiters.indexOf(sink), 1);
  const cpu1 = local();
  const st = await fleet.stats(true);
  const ackMs = acks.map((a) => a.ms).sort((a, b) => a - b);
  const hops = acks.flatMap((a) => a.hop).sort((a, b) => a - b);
  const r = {
    rate,
    seconds,
    size,
    writesSent: total,
    sendMs,
    acks: acks.length,
    drainMs,
    achievedWritesPerSec: +(acks.length / ((sendMs + drainMs) / 1000)).toFixed(1),
    ackP50: pct(ackMs, 50),
    ackP99: pct(ackMs, 99),
    ackMax: ackMs[ackMs.length - 1],
    hopP50: pct(hops, 50),
    hopP99: pct(hops, 99),
    framesReceived: st.frames,
    framesExpected: total * count,
    latency: st.latency,
    bytesReadDelta: st.bytesRead - bytes0,
    bytesPerFrame: Math.round((st.bytesRead - bytes0) / Math.max(1, st.frames)),
    closes: st.closes.length,
    localCpuMs: cpu0 && cpu1 ? cpu1.cpuMs - cpu0.cpuMs : null,
    at: new Date(t0).toISOString(),
    writerClosed: writer.closed ?? null,
    objectAfter: (await admin(`t/${tenant}/stats`)).body,
  };
  log("burst", JSON.stringify(r));
  return r;
}
result.bursts = [];
for (const step of String(opt("bursts", `${rate}x${seconds}`)).split(",")) {
  if (step === "0" || step === "none") break;
  const [rs, rest] = step.split("x");
  const [secs, size] = rest.split("@");
  result.bursts.push(await burst(Number(rs), Number(secs), Number(size ?? opt("size", 2048))));
  if (writer.closed) {
    log("writer socket closed, stopping", writer.closed);
    break;
  }
}

result.final = (await admin(`t/${tenant}/stats`)).body;
writer.close();
await fleet.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
const file = new URL(`../results/${label}-${process.env.S3_LOCAL ? "local" : "cf"}.json`, import.meta.url);
writeFileSync(file, JSON.stringify(result, (k, v) => (v instanceof Map ? undefined : v), 2));
log("wrote", file.pathname);
process.exit(0);
