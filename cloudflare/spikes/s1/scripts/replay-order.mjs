// Copies one object's schema into another, statement for statement, in the
// source's sqlite_schema rowid order (or grouped by type with --grouped), to
// test whether object order changes what the platform accepts.
// Usage: node scripts/replay-order.mjs <base> <from-object> <to-object> [--grouped]
const [base, from, to] = process.argv.slice(2);
const grouped = process.argv.includes("--grouped");
const call = async (o, route, body) => (await fetch(`${base}${route}?o=${o}`, { method: "POST", headers: { "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) })).json();
let rows = (await call(from, "/query", { sql: "SELECT rowid AS r, type, name, sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY rowid" })).rows;
const virtual = rows.filter((r) => /^CREATE VIRTUAL/i.test(r.sql)).map((r) => r.name);
rows = rows.filter((r) => !virtual.some((v) => r.name !== v && r.name.startsWith(v + "_") && r.type === "table"));
if (grouped) rows = ["table", "index", "view", "trigger"].flatMap((t) => rows.filter((r) => r.type === t));
await call(to, "/reset", {});
let failed = 0;
for (let k = 0; k < rows.length; k += 400) {
  const r = await call(to, "/exec", { statements: rows.slice(k, k + 400).map((x) => x.sql), continueOnError: true, transaction: true });
  failed += (r.errors || []).length;
}
console.log(JSON.stringify({ from, to, grouped, statements: rows.length, failed }));
