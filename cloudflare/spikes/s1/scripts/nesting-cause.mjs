// Which triggers make a write statement exceed the platform's nesting limit?
// For each table, compile its INSERT (WHERE 0) with a set of triggers dropped,
// inside a transaction that always rolls back (the last statement fails).
// Usage: node scripts/nesting-cause.mjs <base> <object> <table>...
import { quoteIdent } from "./sql.mjs";
const [base, object, ...tables] = process.argv.slice(2);
const call = async (route, body) => (await fetch(`${base}${route}?o=${object}`, { method: "POST", headers: { "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) })).json();
for (const table of tables) {
  const cols = (await call("/query", { sql: "SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0", params: [table] })).rows.map((r) => quoteIdent(r.name)).join(", ");
  const insert = `INSERT INTO ${quoteIdent(table)} (${cols}) SELECT ${cols} FROM ${quoteIdent(table)} WHERE 0`;
  const triggers = (await call("/query", { sql: "SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name = ?", params: [table] })).rows.map((r) => r.name);
  const groups = {
    none: [],
    capture: triggers.filter((n) => n.startsWith("_gonvex_sync_")),
    reference: triggers.filter((n) => n.startsWith("referenceBindings_")),
    dependency: triggers.filter((n) => n.startsWith("gonvex_record_dependency_")),
  };
  const out = { table, triggers: triggers.length };
  for (const [name, drop] of Object.entries(groups)) {
    const r = await call("/exec", { transaction: true, statements: [...drop.map((d) => `DROP TRIGGER ${quoteIdent(d)}`), insert, "SELECT * FROM s1_no_such_table_rollback"] });
    const e = (r.errors || [])[0];
    out[`drop_${name}(${drop.length})`] = e && e.index === drop.length + 1 ? "compiles" : e ? e.error.slice(0, 40) : r.error;
  }
  console.log(JSON.stringify(out));
}
