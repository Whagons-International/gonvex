// Reducer round trip, facet vs baked, timed by the front Worker (no client network in the number).
// Per mode: fresh tenants, S2's setup steps, then repeated committed Reducers with fresh ids, one per request.
//   node test/roundtrip.mjs [--tenants 3] [--reps 15]
import { gen, run, save, log, stats, subset, BASE, BAKED_BASE, KEY, op } from "./lib.mjs";
import { fixture, steps, callOf, T0 } from "../../s2/test/scenarios.mjs";

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i < 0 ? d : args[i + 1]; };
const tenants = Number(opt("tenants", "3"));
const reps = Number(opt("reps", "15"));
const schema = subset();
const s2Table = 'CREATE TABLE IF NOT EXISTS "s2SavepointRows" ("_id" TEXT PRIMARY KEY, "label" TEXT NOT NULL)';
const timed = async (base, tenant, name, body) => {
  const r = await fetch(`${base}/timed/t/${tenant}/${name}?k=${KEY}`, { method: "POST", body: JSON.stringify(body) });
  return r.json();
};
const repeated = (i, tag) => [
  { path: "spots.create", args: { entity: { _id: `spot-${tag}-${i}`, name: `Room ${i}` } } },
  { path: "boards.createMessage", args: { entity: { _id: `msg-${tag}-${i}`, _creationTime: T0, boardId: "board-1", createdBy: "mem-owner", title: `T${i}`, content: "Hello" } } },
  { path: "boards.createComment", args: { commentId: `cmt-${tag}-${i}`, boardId: "board-1", createdBy: "mem-owner", createdAt: T0, boardMessageId: "msg-1", content: "Thanks!" } },
  { path: "settings.togglePlugin", args: { slug: "boards", isEnabled: i % 2 === 0 } },
  { path: "s5.noop", args: {} },
].map((s, j) => ({ ...s, as: "owner", now: T0 + 9_000_000 + i * 1000 + j, intentEntropy: "3c".repeat(32), commandId: `rt-${tag}-${i}-${j}` }));

const out = { tenants, reps, modes: {} };
for (const mode of ["facet", "inline"]) {
  const base = mode === "inline" ? BAKED_BASE : BASE;
  const per = {};
  for (let t = 0; t < tenants; t++) {
    const tenant = `rt-${mode}-${run}-${t}`;
    if (mode === "facet") await op(tenant, "set-active", { gen: gen("whagons", "RT") }, base);
    await op(tenant, "provision", { target: mode === "facet" ? "facet" : "self", statements: schema.statements, rows: [...schema.rows, ...fixture], extra: [s2Table] }, base);
    for (const step of steps) await op(tenant, "invoke", { mode, calls: [callOf(step)] }, base);
    for (let i = 0; i < reps; i++) {
      for (const s of repeated(i, `${t}`)) {
        const r = await timed(base, tenant, "invoke", { mode, calls: [callOf(s)] });
        const res = r.value?.results?.[0];
        const e = (per[s.path] ??= { workerMs: [], ok: 0, failed: [], rowsRead: null, rowsWritten: null });
        e.workerMs.push(r.workerMs);
        if (res?.envelope?.status === "ok") e.ok++; else e.failed.push(String(res?.envelope?.message ?? r.status).slice(0, 100));
        e.rowsRead = res?.stats?.rowsRead ?? e.rowsRead;
        e.rowsWritten = res?.stats?.rowsWritten ?? e.rowsWritten;
      }
    }
    log(mode, tenant, "done");
  }
  for (const e of Object.values(per)) { e.workerMs = stats(e.workerMs); e.failed = [...new Set(e.failed)]; }
  out.modes[mode] = per;
}
save("roundtrip", out);
for (const path of Object.keys(out.modes.facet)) console.log(path, "facet", JSON.stringify(out.modes.facet[path].workerMs), "baked", JSON.stringify(out.modes.inline[path].workerMs), "rows", out.modes.facet[path].rowsRead, out.modes.facet[path].rowsWritten);
