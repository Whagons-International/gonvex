// Restore points on the platform (point-in-time recovery does not exist in local workerd).
// A small app whose second migration fails in its third chunk, after two chunks committed.
// Expected: the failure is reported, the object restarts onto the bookmark taken before that
// migration, the two committed chunks are gone, the first migration stays, and the object
// reads "failed and rolled back" with the gate open. A fixed second release then applies.
//
//   K02_TOKEN=... node --experimental-strip-types kernel/migrate/verify/restore.mjs <base-url> <object>
import { buildReleasePlan } from "../build.ts";
import { migrationsFromFiles } from "../migrations.ts";
import { unitKey } from "../plan.ts";

const [base, object] = process.argv.slice(2);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function call(path, body) {
  for (let attempt = 0; ; attempt += 1) {
    try {
      const response = await fetch(`${base}${path}`, { method: "POST", headers: { "content-type": "application/json", "x-k02-token": process.env.K02_TOKEN ?? "" }, body: JSON.stringify(body ?? {}) });
      const value = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
      if (value.error && attempt < 6 && !path.endsWith("/apply") && /reset|lost|internal|HTTP 5|abort|restor/i.test(value.error)) {
        await sleep(1000);
        continue;
      }
      return value;
    } catch (error) {
      if (attempt >= 6) throw error;
      await sleep(1000);
    }
  }
}
async function upload(plan) {
  const entries = [["manifest", JSON.stringify({ ...plan, chunks: undefined })]];
  for (const unit of [...plan.prelude, ...plan.migrations]) plan.chunks[unitKey(unit)].forEach((c, i) => entries.push([`${unitKey(unit)}/${i}`, JSON.stringify(c)]));
  const r = await call(`/plan/${plan.id}/put`, { entries });
  if (!r.ok) throw new Error(r.error);
}
const seen = new Set();
async function settle(planId) {
  const s = await call(`/t/${object}/apply`, { planId });
  if (s.error) throw new Error(s.error);
  if (s.error === undefined && s.state === "failed") seen.add(s.error);
  for (let i = 0; i < 1200; i += 1) {
    await sleep(250);
    const p = await call(`/t/${object}/poll`);
    if (p.error) continue;
    for (const f of p.failures ?? []) seen.add(`${f.unit} chunk ${f.chunk}: ${f.error} (restore ${f.restored})`);
    if (p.status.error) seen.add(`status: ${p.status.error}`);
    if (p.status.state !== "running" && !(p.status.state === "failed" && p.status.gateClosed)) return p;
  }
  throw new Error("timed out");
}
const tables = async () => (await call(`/t/${object}/query`, { sql: "SELECT name FROM sqlite_schema WHERE type='table' AND name IN ('a','b1','b2','b3','c') ORDER BY name" })).rows.map((r) => r.name);

const base1 = {
  "migrations/0001_a.sql": "-- gonvex:scope tenant\nCREATE TABLE a (id INTEGER PRIMARY KEY);\nINSERT INTO a VALUES (1);\n",
  "migrations/0002_b.sql": "-- gonvex:scope tenant\nCREATE TABLE b1 (id INTEGER PRIMARY KEY);\nCREATE TABLE b2 (id INTEGER PRIMARY KEY);\nINSERT INTO missing_table VALUES (1);\n",
};
const failing = await buildReleasePlan(await migrationsFromFiles(base1), { chunk: { maxStatements: 1 } });
await upload(failing);
await call(`/t/${object}/wipe`);
const t0 = Date.now();
const after = await settle(failing.id);
const out = {
  seen: [...seen],
  status: { state: after.status.state, gateClosed: after.status.gateClosed, error: after.status.error },
  tablesAfterRestore: await tables(),
  ledger: (await call(`/t/${object}/query`, { sql: "SELECT name FROM gonvex_migrations ORDER BY name" })).rows.map((r) => r.name),
  ms: Date.now() - t0,
};
console.log(JSON.stringify({ restore: out }));

const fixed = await buildReleasePlan(await migrationsFromFiles({ ...base1, "migrations/0002_b.sql": "-- gonvex:scope tenant\nCREATE TABLE b1 (id INTEGER PRIMARY KEY);\nCREATE TABLE b3 (id INTEGER PRIMARY KEY);\n" }), { chunk: { maxStatements: 1 } });
await upload(fixed);
const done = await settle(fixed.id);
console.log(JSON.stringify({ fixed: { state: done.status.state, applied: done.status.applied, tables: await tables() } }));
const pass = out.status.state === "failed" && !out.status.gateClosed && JSON.stringify(out.tablesAfterRestore) === '["a"]' && JSON.stringify(out.ledger) === '["0001_a.sql"]' && done.status.state === "done";
console.log(JSON.stringify({ pass }));
process.exit(pass ? 0 : 1);
