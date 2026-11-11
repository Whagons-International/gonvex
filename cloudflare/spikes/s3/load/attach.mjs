// Attachment size limit and wake cost with large attachments (1,000 sockets, eager rebuild).
import { writeFileSync, mkdirSync } from "node:fs";
import { Fleet, Control, admin, sleep } from "./lib.mjs";
const tenant = `att-${Date.now().toString(36)}`;
const out = { tenant, limit: [], wake: [] };
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
const fleet = new Fleet({ tenant, count: 1000, threads: 4 });
log("opened", (await fleet.open()).ok);
for (const bytes of [2048, 16000, 16250, 16300, 16310, 16320, 16330, 16384]) {
  const r = (await admin(`t/${tenant}/pad`, { bytes })).body;
  out.limit.push(r);
  log("pad", bytes, r.ok, r.err?.slice(0, 160));
}
const probe = await new Control({ tenant, m: "probe" }).open();
for (const bytes of [0, 15000]) {
  await admin(`t/${tenant}/pad`, { bytes });
  for (let c = 0; c < 4; c++) {
    await sleep(20000);
    const a = await probe.echo();
    const b = await probe.echo();
    const row = { padBytes: bytes, hibernated: true, wakeRtt: a.rtt, warmRtt: b.rtt, rebuilt: a.rebuiltSockets, wakes: a.wakes };
    out.wake.push(row);
    log("wake", JSON.stringify(row));
  }
}
const st = await fleet.stats(false);
out.fleetOpen = st.open;
probe.close();
await fleet.close();
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/attach-cf.json`, import.meta.url), JSON.stringify(out, null, 2));
process.exit(0);
