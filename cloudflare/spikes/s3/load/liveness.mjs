// Liveness without server pings.
// Phase 1: N live sockets send the ping frame every 20 s (answered by setWebSocketAutoResponse);
//          nothing else happens. Does the object still hibernate? Are timestamps recorded?
// Phase 2: the alarm sweep (interval 20 s, timeout 60 s) is on; `silent` sockets stop pinging
//          but keep their TCP connection open. When are they closed?
//   node load/liveness.mjs [--count 100] [--silent 5] [--phase1 90] [--phase2 150]
import { writeFileSync, mkdirSync } from "node:fs";
import { Fleet, admin, sleep } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => {
  const i = args.indexOf(`--${k}`);
  return i < 0 ? d : args[i + 1] ?? true;
};
const count = Number(opt("count", 100));
const silent = Number(opt("silent", 5));
const tenant = `live-${Date.now().toString(36)}`;
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
const out = { tenant, count, silent };

const fleet = new Fleet({ tenant, count, live: true, pingMs: 20000, threads: 1 });
out.open = await fleet.open();
const t0 = Date.now();
log("opened", out.open.ok);
let s = (await admin(`t/${tenant}/stats`)).body;
out.start = { inst: s.inst, wakes: s.wakes };
log("start", out.start);

// Phase 1: pings only.
await sleep(Number(opt("phase1", 90)) * 1000);
s = (await admin(`t/${tenant}/stats`)).body;
const st1 = await fleet.stats(true);
out.phase1 = { inst: s.inst, wakes: s.wakes, hibernatedDuringPings: s.inst !== out.start.inst, auto: s.auto, pongs: st1.pongs, protoPings: st1.protoPings, closes: st1.closes.length };
log("phase1", out.phase1);

// Phase 2: silence some sockets, turn the sweep on.
const silentIds = Array.from({ length: silent }, (_, i) => i);
await fleet.silence(silentIds);
const silencedAt = Date.now();
await admin(`t/${tenant}/liveness`, { on: 1, interval: 20000, timeout: 60000 });
const wakes0 = (await admin(`t/${tenant}/stats`)).body.wakes;
await sleep(Number(opt("phase2", 150)) * 1000);
const st2 = await fleet.stats(true);
s = (await admin(`t/${tenant}/stats`)).body;
out.phase2 = {
  silencedAfterOpenMs: silencedAt - t0,
  closes: st2.closes.map((c) => ({ i: c.i, code: c.code, reason: c.reason, msAfterSilence: c.at - silencedAt, openFor: c.openFor })),
  open: st2.open,
  wakesDuringPhase2: s.wakes - wakes0,
  inst: s.inst,
  auto: s.auto,
  lastSweep: (await admin(`t/${tenant}/get`, { name: "lastSweep" })).body.value,
};
log("phase2", JSON.stringify(out.phase2));
await admin(`t/${tenant}/liveness`, { on: 0 });
await fleet.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/liveness-${process.env.S3_LOCAL ? "local" : "cf"}.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
