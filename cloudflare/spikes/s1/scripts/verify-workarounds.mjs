// Checks that the rewritten views return the same rows as the originals, on
// native SQLite over a scratch copy of the tenant file. Every reference view is
// scoped to the owners in schemaRelationshipOwners, so the check puts every
// owner that has bindings there (inside a transaction that is rolled back).
// Usage: node scripts/verify-workarounds.mjs <scratch-copy.db> [out.json]
import { DatabaseSync } from "node:sqlite";
import { writeFileSync } from "node:fs";
import { applyWorkarounds, rewriteDeepReplace } from "./sql.mjs";

const db = new DatabaseSync(process.argv[2]);
// Every view the workarounds change.
const views = db.prepare("SELECT name, sql FROM sqlite_schema WHERE type='view' ORDER BY name").all()
  .filter((v) => { const out = applyWorkarounds(v.sql, new Map()); return out.length !== 1 || out[0] !== v.sql; });

const results = [];
db.exec("BEGIN");
for (const view of views) {
  const scoped = view.name.startsWith("referenceExpected_");
  const source = view.name.slice("referenceExpected_".length);
  const alias = `s1x_${view.name}`;
  const renamed = view.sql.replace(`"${view.name}"`, `"${alias}"`);
  const statements = applyWorkarounds(renamed, new Map());
  const t0 = performance.now();
  for (const s of statements) db.exec(s);
  const createMs = performance.now() - t0;
  if (scoped) db.exec(`INSERT INTO schemaRelationshipOwners(sourceName, ownerId) SELECT DISTINCT '${source}', sourceId FROM "referenceBindings_${source}" WHERE 1 ON CONFLICT DO NOTHING`);
  const owners = scoped ? db.prepare("SELECT count(*) AS n FROM schemaRelationshipOwners WHERE sourceName = ?").get(source).n : null;
  const time = (sql) => {
    const s = performance.now();
    const rows = db.prepare(sql).all();
    return { rows, ms: performance.now() - s };
  };
  const canon = (rows) => rows.map((r) => JSON.stringify(r)).sort();
  const before = time(`SELECT * FROM "${view.name}"`);
  const after = time(`SELECT * FROM "${alias}"`);
  const a = canon(before.rows);
  const b = canon(after.rows);
  const same = a.length === b.length && a.every((x, k) => x === b[k]);
  // One owner, as a write trigger sees it: only that owner is in scope.
  const one = scoped ? db.prepare(`SELECT ownerId FROM "${view.name}" LIMIT 1`).get() : null;
  let single = null;
  if (one) {
    db.exec(`DELETE FROM schemaRelationshipOwners WHERE sourceName = '${source}'`);
    db.prepare("INSERT INTO schemaRelationshipOwners(sourceName, ownerId) VALUES (?, ?)").run(source, String(one.ownerId));
    const o1 = time(`SELECT * FROM "${view.name}" WHERE ownerId = ${JSON.stringify(one.ownerId)}`);
    const o2 = time(`SELECT * FROM "${alias}" WHERE ownerId = ${JSON.stringify(one.ownerId)}`);
    single = { originalMs: +o1.ms.toFixed(2), rewrittenMs: +o2.ms.toFixed(2), rows: o1.rows.length, same: JSON.stringify(canon(o1.rows)) === JSON.stringify(canon(o2.rows)) };
  }
  results.push({
    view: view.name, originalBytes: Buffer.byteLength(view.sql), statements: statements.length,
    maxStatementBytes: Math.max(...statements.map((s) => Buffer.byteLength(s))), owners, rows: a.length, same,
    allOwnersMs: { original: +before.ms.toFixed(1), rewritten: +after.ms.toFixed(1) }, oneOwner: single, createMs: +createMs.toFixed(1),
  });
  console.log(JSON.stringify(results[results.length - 1]));
}
// Indexes: the rewritten expression must equal the original on every row.
const indexes = [];
for (const index of db.prepare("SELECT name, tbl_name, sql FROM sqlite_schema WHERE type='index' AND sql IS NOT NULL").all()) {
  const out = rewriteDeepReplace(index.sql);
  if (out.length === 1) continue;
  for (const s of out.slice(0, -1)) db.exec(s);
  const expr = (sql) => { const open = sql.indexOf("("); const where = sql.lastIndexOf(") WHERE"); return sql.slice(open + 1, where < 0 ? sql.lastIndexOf(")") : where); };
  const differ = db.prepare(`SELECT count(*) AS n FROM "${index.tbl_name}" WHERE (${expr(index.sql)}) IS NOT (${expr(out[out.length - 1])})`).get().n;
  const rows = db.prepare(`SELECT count(*) AS n FROM "${index.tbl_name}"`).get().n;
  indexes.push({ index: index.name, generatedColumns: out.length - 1, rows, differ, same: differ === 0 });
  console.log(JSON.stringify(indexes[indexes.length - 1]));
}
db.exec("ROLLBACK");
const summary = { views: results.length, indexes, allSame: results.every((r) => r.same && (!r.oneOwner || r.oneOwner.same)) && indexes.every((i) => i.same), results };
if (process.argv[3]) writeFileSync(process.argv[3], JSON.stringify(summary, null, 2) + "\n");
console.log(JSON.stringify({ views: summary.views, allSame: summary.allSame }));
