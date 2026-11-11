// Hibernation with sockets open: when does the object hibernate, and what does a wake cost?
// Two tenants run side by side: one with N fleet sockets (attachments to rebuild), one with only the probe socket.
//   node load/hibernate.mjs [--count 1000] [--idles 5,10,15,20,30,60] [--cycles 5] [--rebuild eager|lazy]
import { writeFileSync, mkdirSync } from "node:fs";
import { Fleet, Control, admin, sleep, pct } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => {
  const i = args.indexOf(`--${k}`);
  return i < 0 ? d : args[i + 1] ?? true;
};
const count = Number(opt("count", 1000));
const idles = String(opt("idles", "5,10,15,20,30,60")).split(",").map(Number);
const cycles = Number(opt("cycles", 5));
const rebuild = opt("rebuild", "eager");
const stamp = Date.now().toString(36);
const big = `hib-${stamp}`;
const small = `hib0-${stamp}`;
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
const out = { count, rebuild, idles: [], cycles: [] };

await admin(`t/${big}/set`, { name: "rebuild", value: rebuild });
await admin(`t/${small}/set`, { name: "rebuild", value: rebuild });
const fleet = new Fleet({ tenant: big, count, threads: 4 });
out.open = await fleet.open();
log("opened", out.open.ok);
const pb = await new Control({ tenant: big, m: "probe" }).open();
const ps = await new Control({ tenant: small, m: "probe" }).open();

async function warm(p) {
  const r = [];
  for (let i = 0; i < 10; i++) r.push((await p.echo()).rtt);
  r.sort((a, b) => a - b);
  return { p50: pct(r, 50), min: r[0], max: r[r.length - 1] };
}
out.warm = { big: await warm(pb), small: await warm(ps) };
log("warm", out.warm);

// When does it hibernate? Idle T seconds, then one echo: did the instance change?
for (const idle of idles) {
  const a = await Promise.all([pb.echo(), ps.echo()]);
  await sleep(idle * 1000);
  const b = await Promise.all([pb.echo(), ps.echo()]);
  const row = {
    idle,
    big: { hibernated: a[0].inst !== b[0].inst, rtt: b[0].rtt, rebuilt: b[0].rebuiltSockets },
    small: { hibernated: a[1].inst !== b[1].inst, rtt: b[1].rtt },
  };
  out.idles.push(row);
  log("idle", JSON.stringify(row));
}

// Wake latency: idle 60 s (well past the threshold), then echo; compare to the warm echo right after.
for (let c = 0; c < cycles; c++) {
  await sleep(60000);
  const [b, s] = await Promise.all([pb.echo(), ps.echo()]);
  const [b2, s2] = await Promise.all([pb.echo(), ps.echo()]);
  const row = { big: { wakeRtt: b.rtt, warmRtt: b2.rtt, rebuilt: b.rebuiltSockets, wakes: b.wakes, sockets: b.sockets }, small: { wakeRtt: s.rtt, warmRtt: s2.rtt, wakes: s.wakes } };
  out.cycles.push(row);
  log("cycle", JSON.stringify(row));
}
const st = await fleet.stats(false);
out.fleetOpenAtEnd = st.open;
out.fleetCloses = st.closes.length;
out.fleetCloseCodes = st.closes.map((c) => ({ code: c.code, reason: c.reason, openFor: c.openFor }));
pb.close();
ps.close();
await fleet.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/hibernate-${opt("label", rebuild)}-${process.env.S3_LOCAL ? "local" : "cf"}.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
