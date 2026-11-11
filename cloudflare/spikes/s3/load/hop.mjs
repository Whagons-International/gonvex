// Gateway hop: RPC round trip tenant -> gateway (empty delivery), for gateways the tenant
// created itself and for gateways first reached from the edge (as clients would create them).
import { writeFileSync, mkdirSync } from "node:fs";
import { admin } from "./lib.mjs";
const t = `hop-${Date.now().toString(36)}`;
const n = Number(process.argv[2] ?? 4);
const out = {};
out.tenantColo = (await admin(`t/${t}/where`)).body.colo;
out.tenantCreated = (await admin(`t/${t}/hop`, { names: Array.from({ length: n }, (_, i) => `${t}/gw${i}`).join(","), reps: 20 })).body;
const t2 = `${t}b`;
for (let i = 0; i < n; i++) await admin(`g/${t2}/${i}/where`);
out.edgeCreated = (await admin(`t/${t2}/hop`, { names: Array.from({ length: n }, (_, i) => `${t2}/gw${i}`).join(","), reps: 20 })).body;
console.log(JSON.stringify(out, null, 1));
mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
writeFileSync(new URL(`../results/hop-cf.json`, import.meta.url), JSON.stringify(out, null, 2));
