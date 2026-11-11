// Finds strict hosts and shows T6 on them (S1 "Blocker"): on fresh objects,
// a 12-branch reference layer with only T1 applied, then the same layer with
// T6. A strict host refuses `SELECT ... FROM referenceExpected_src` and the
// writes whose triggers read it ("nested too deep"); with T6 every write must
// compile there and the bindings must equal the T1-only layer's on a lax host.
//
//   S1_TOKEN=... node verify/strict-hosts.mjs <base> <objects> [--prefix p] [--out f]
import { writeFileSync } from "node:fs";
import { SchemaTransform } from "../index.ts";
import { referenceSchema } from "../test/fixtures.ts";

const [base, countArg, ...rest] = process.argv.slice(2);
const option = (name) => (rest.includes(name) ? rest[rest.indexOf(name) + 1] : null);
const prefix = option("--prefix") ?? `a01-strict-${Date.now()}`;
const schema = referenceSchema(12);
const t1Only = new SchemaTransform({ referenceMinBranches: 1_000, limits: { unionBranchesRead: Infinity } }).migration("t1-only", schema).statements;
const t6 = new SchemaTransform().migration("t6", schema).statements;
const writes = [
  `INSERT INTO src VALUES (1, '{"f0":"a","f1":["b","c"],"f2":"d","f9":["e"]}')`,
  `INSERT INTO src VALUES (2, '{"f0":"x","f3":["y"],"f4":"z","f11":["q","r"]}')`,
  `UPDATE src SET data = '{"f0":"a2","f1":["c"],"f5":["e"],"f6":"g"}' WHERE _id = 1`,
  `DELETE FROM src WHERE _id = 2`,
];
async function call(object, route, body = {}) {
  const r = await fetch(`${base}${route}?o=${encodeURIComponent(object)}`, { method: "POST", headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) });
  const text = await r.text();
  try { return JSON.parse(text); } catch { return { error: text.slice(0, 200) }; }
}
const results = [];
for (let k = 0; k < Number(countArg); k += 1) {
  const object = `${prefix}-${k}`;
  await call(object, "/reset");
  const load = await call(object, "/exec", { statements: t1Only, transaction: true });
  const read = await call(object, "/query", { sql: `SELECT count(*) AS n FROM "referenceExpected_src"` });
  const strict = /nested too deep/i.test(read.error ?? "");
  const t1Writes = [];
  for (const w of writes) {
    const r = await call(object, "/exec", { statements: [w], transaction: true });
    t1Writes.push(r.ok ? null : String(r.errors?.[0]?.error ?? r.error).slice(0, 80));
  }
  const t1Bindings = (await call(object, "/query", { sql: `SELECT sourceId, path, "target" FROM bindings ORDER BY 1, 2` })).rows ?? null;
  await call(object, "/reset");
  const loadT6 = await call(object, "/exec", { statements: t6, transaction: true });
  const t6Writes = [];
  for (const w of writes) {
    const r = await call(object, "/exec", { statements: [w], transaction: true });
    t6Writes.push(r.ok ? null : String(r.errors?.[0]?.error ?? r.error).slice(0, 80));
  }
  const t6Bindings = (await call(object, "/query", { sql: `SELECT sourceId, path, "target" FROM bindings ORDER BY 1, 2` })).rows ?? null;
  await call(object, "/reset");
  const entry = { object, strict, t1Load: load.ok === true, readError: read.error ? String(read.error).slice(0, 80) : null, t1Writes, t6Load: loadT6.ok === true, t6Writes, t6Bindings: t6Bindings?.length ?? null, t1Bindings: t1Bindings?.length ?? null, t6BindingsJson: JSON.stringify(t6Bindings) };
  results.push(entry);
  const { t6BindingsJson, ...shown } = entry;
  console.log(JSON.stringify(shown));
}
const lax = results.find((r) => !r.strict && r.t1Writes.every((w) => w === null));
const summary = {
  objects: results.length, strict: results.filter((r) => r.strict).length,
  t6WritesFailing: results.reduce((s, r) => s + r.t6Writes.filter(Boolean).length, 0),
  t6BindingsEqualLaxT1: lax ? results.every((r) => r.t6BindingsJson === JSON.stringify(JSON.parse(r.t6BindingsJson)) && r.t6BindingsJson === results.find((x) => x === lax).t6BindingsJson) : null,
};
console.log(JSON.stringify(summary));
if (option("--out")) writeFileSync(option("--out"), JSON.stringify({ summary, results: results.map(({ t6BindingsJson, ...r }) => r) }, null, 2) + "\n");
