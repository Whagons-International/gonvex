// CPU per fan-out, read later from GraphQL analytics (durableObjectsPeriodicGroups.cpuTime) by tenant name.
// One tenant per variant, 1,000 sockets each, 300 fan-outs (30 events x 10) of one ~2 KB change in 8 classes.
//   node load/cpu.mjs   -> results/cpu-cf.json lists tenant names and time windows
import { writeFileSync, mkdirSync } from "node:fs";
import { Fleet, admin, sleep } from "./lib.mjs";
const stamp = Date.now().toString(36);
const variants = [
  { name: "cache", mode: "cache", encodeOnly: 0, deflate: false },
  { name: "tags", mode: "tags", encodeOnly: 0, deflate: false },
  { name: "attach", mode: "attach", encodeOnly: 0, deflate: false },
  { name: "encode", mode: "cache", encodeOnly: 1, deflate: false },
  { name: "deflate", mode: "cache", encodeOnly: 0, deflate: true },
  { name: "idle", mode: "cache", encodeOnly: 0, deflate: false, idle: true },
];
const out = { stamp, variants: [] };
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
for (const v of variants) {
  const tenant = `cpu-${v.name}-${stamp}`;
  const fleet = new Fleet({ tenant, count: 1000, threads: 4, deflate: v.deflate });
  const o = await fleet.open();
  const t0 = new Date().toISOString();
  let walls = [];
  if (!v.idle) {
    for (let i = 0; i < 30; i++) {
      const r = await admin(`t/${tenant}/fanout`, { n: 10, mode: v.mode, encodeOnly: v.encodeOnly });
      walls.push(r.ms);
      await sleep(1500);
    }
  } else await sleep(60000);
  const t1 = new Date().toISOString();
  const st = await fleet.stats(true);
  out.variants.push({ ...v, tenant, opened: o.ok, extensions: o.extensions, from: t0, to: t1, fanouts: v.idle ? 0 : 300, framesReceived: st.frames, bytesRead: st.bytesRead, wallMsP50: walls.sort((a, b) => a - b)[15] ?? null });
  log(v.name, tenant, o.ok, st.frames, walls[15]);
  await fleet.close();
  await sleep(3000);
}
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/cpu-cf.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
