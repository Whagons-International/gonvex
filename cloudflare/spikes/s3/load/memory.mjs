// Memory with N sockets. Local: V8 heap via the inspector at each step.
// Platform: ballast probe (allocate 4 MB steps until the isolate is reset), with 0 and with N sockets.
//   node load/memory.mjs [--steps 0,250,500,1000] [--probe]
import { writeFileSync, mkdirSync } from "node:fs";
import { Fleet, Control, admin, sleep, localHeap } from "./lib.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => {
  const i = args.indexOf(`--${k}`);
  return i < 0 ? d : args[i + 1] ?? true;
};
const steps = String(opt("steps", "0,250,500,1000")).split(",").map(Number);
const tenant = `mem-${Date.now().toString(36)}`;
const out = { tenant, steps: [] };
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);

async function probe(label) {
  // Allocate 4 MB of heap at a time until the isolate is reset: a failed request,
  // or a reply from a new instance (the reset can also be silent).
  let last = null;
  let inst = null;
  for (let mb = 4; mb <= 256; mb += 4) {
    const r = await admin(`t/${tenant}/ballast`, { mb: 4, kind: opt("kind", "heap") });
    if (r.status !== 200 || (inst && r.body.inst !== inst)) {
      const why = r.status !== 200 ? `status ${r.status}` : `new instance ${r.body.inst} (ballast ${r.body.ballastMB} MB)`;
      log(label, "reset at", mb, "MB:", why);
      return { label, resetAtMB: mb, lastOkMB: last, why };
    }
    inst = r.body.inst;
    last = r.body.ballastMB;
  }
  return { label, resetAtMB: null, lastOkMB: last };
}

let fleet = null;
let opened = 0;
for (const n of steps) {
  if (n > opened) {
    const more = new Fleet({ tenant, count: n - opened, threads: 2 });
    // Member ids must not collide: offset by reusing tenant with distinct ranges is not needed for memory.
    const r = await more.open();
    fleet = fleet ? [...fleet, more] : [more];
    opened = n;
    log("opened", n, r.ok, r.extensions);
  }
  await sleep(2000);
  const stats = (await admin(`t/${tenant}/stats`)).body;
  const heap = await localHeap();
  out.steps.push({ sockets: stats.sockets, heap, inst: stats.inst });
  log("step", n, stats.sockets, heap);
}
if (opt("probe", false)) {
  out.probeWithSockets = await probe(`with ${opened} sockets`);
  await sleep(5000);
  const fs = [];
  for (const f of fleet ?? []) fs.push(await f.stats(false));
  out.socketsAfterReset = { open: fs.reduce((a, x) => a + x.open, 0), closes: fs.flatMap((x) => x.closes).length, codes: [...new Set(fs.flatMap((x) => x.closes).map((c) => `${c.code} ${c.reason}`))] };
  out.objectAfterReset = (await admin(`t/${tenant}/stats`)).body;
  log("after reset", out.socketsAfterReset, out.objectAfterReset.sockets);
  for (const f of fleet ?? []) await f.close();
  await admin(`t/${tenant}/closeall`);
  await sleep(3000);
  out.probeEmpty = await probe("0 sockets");
} else for (const f of fleet ?? []) await f.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/memory-${process.env.S3_LOCAL ? "local" : "cf"}.json`, import.meta.url), JSON.stringify(out, null, 2));
log(JSON.stringify(out));
process.exit(0);
