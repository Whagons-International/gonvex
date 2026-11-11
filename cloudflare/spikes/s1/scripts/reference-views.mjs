// Queries every referenceExpected_* view on a loaded object; reports which the
// platform refuses. Usage: node scripts/reference-views.mjs <base> <object> [out.json]
import { writeFileSync } from "node:fs";
const [base, object, outPath] = process.argv.slice(2);
const call = async (sql) => (await fetch(`${base}/query?o=${object}`, { method: "POST", headers: { "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify({ sql }) })).json();
const views = (await call("SELECT name FROM sqlite_schema WHERE type='view' AND name LIKE 'referenceExpected\\_%' ESCAPE '\\' AND name NOT LIKE '%\\_\\_part%' ESCAPE '\\' ORDER BY name")).rows.map((r) => r.name);
const failing = {};
for (const v of views) {
  const r = await call(`SELECT count(*) AS n FROM "${v}"`);
  if (r.error) (failing[r.error.replace(/: SQLITE_\w+$/, "")] ||= []).push(v);
}
const out = { object, views: views.length, failing: Object.fromEntries(Object.entries(failing).map(([k, v]) => [k, { count: v.length, views: v }])) };
console.log(JSON.stringify(out));
if (outPath) writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
