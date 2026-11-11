// Probes platform behavior the tenant schema depends on: SQL limits, the
// function allow-list, FTS5 trigram, json_object change-capture triggers and
// regexp(). Usage: node scripts/probes.mjs <base-url> [out.json]
import { writeFileSync } from "node:fs";

const base = process.argv[2] || "http://localhost:8787";
const object = `probe-${Date.now()}`;

async function call(route, body = {}) {
  const response = await fetch(`${base}${route}?o=${object}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" },
    body: JSON.stringify(body),
  });
  return response.json();
}
const query = (sql, params) => call("/query", { sql, params });
const exec = (statements) => call("/exec", { statements, continueOnError: true });
const ok = (result) => !result.error && result.ok !== false;
const error = (result) => result.error || (result.errors || []).map((e) => e.error).join("; ");

// Largest n in [lo, hi] for which test(n) passes.
async function largest(lo, hi, test) {
  if (!(await test(lo))) return lo - 1;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (await test(mid)) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}

const report = { object, limits: {}, functions: {}, features: {} };

await call("/reset");

// --- limits ---------------------------------------------------------------
report.limits.statementBytes = await largest(1000, 2_000_000, async (n) => {
  const head = "SELECT 1 /*";
  const sql = head + "x".repeat(n - head.length - 2) + "*/";
  return ok(await query(sql));
});
report.limits.boundParameters = await largest(1, 2000, async (n) =>
  ok(await query(`SELECT ${Array.from({ length: n }, (_, i) => `?${i + 1}`).join(",")}`, Array(n).fill(1))));
report.limits.tableColumns = await largest(1, 2000, async (n) => {
  const name = `cols_${n}`;
  const result = await exec([`CREATE TABLE ${name} (${Array.from({ length: n }, (_, i) => `c${i}`).join(",")})`]);
  await exec([`DROP TABLE IF EXISTS ${name}`]);
  return ok(result);
});
report.limits.resultColumns = await largest(1, 2000, async (n) =>
  ok(await query(`SELECT ${Array.from({ length: n }, (_, i) => `${i} AS c${i}`).join(",")}`)));
report.limits.functionArguments = await largest(1, 2000, async (n) =>
  ok(await query(`SELECT json_array(${Array(n).fill("1").join(",")}) AS j`)));
report.limits.compoundSelectTerms = await largest(1, 5000, async (n) =>
  ok(await query(Array.from({ length: n }, (_, i) => `SELECT ${i}`).join(" UNION ALL "))));
// Nested calls (parentheses alone do not deepen the tree).
report.limits.expressionDepth = await largest(1, 5000, async (n) =>
  ok(await query(`SELECT ${"abs(".repeat(n)}1${")".repeat(n)} AS x`)));
// Opcodes per program: a VALUES list compiles to ~2 opcodes per row (21,505
// opcodes for 10,745 rows on SQLite 3.54) and is exempt from the compound limit.
report.limits.valuesRowsBeforeNomem = await largest(10, 40_000, async (n) =>
  ok(await query(`SELECT count(*) AS n FROM (VALUES ${Array.from({ length: n }, (_, i) => `(${i})`).join(",")})`)));
{
  const temp = await exec(["CREATE TEMP TABLE t_guard (ok INTEGER CHECK (ok = 1))"]);
  report.limits.tempTables = ok(temp) ? "ok" : error(temp);
}
report.limits.likePatternBytes = await largest(1, 100_000, async (n) =>
  ok(await query("SELECT 'a' LIKE ? AS x", ["%".repeat(n)])));
report.limits.valueBytes = await largest(1000, 50_000_000, async (n) =>
  ok(await query("SELECT length(zeroblob(?)) AS x", [n])));
await exec(["CREATE TABLE rowsize (id INTEGER PRIMARY KEY, a TEXT, b TEXT)"]);
report.limits.rowBytes = await largest(1000, 50_000_000, async (n) => {
  const half = Math.floor(n / 2);
  return ok(await query("INSERT OR REPLACE INTO rowsize VALUES (1, ?, ?)", ["a".repeat(half), "b".repeat(n - half)]));
});

// --- functions the tenant schema and runtime call ---------------------------
const FUNCTIONS = {
  json_extract: "json_extract('{\"a\":1}','$.a')", json_type: "json_type('{}')", json_quote: "json_quote('x')",
  json_array: "json_array(1,2)", json_object: "json_object('a',1)", json_each: "(SELECT count(*) FROM json_each('[1,2]'))",
  json_tree: "(SELECT count(*) FROM json_tree('{\"a\":[1]}'))", json: "json('{}')", json_valid: "json_valid('{}')",
  json_array_length: "json_array_length('[1]')", json_remove: "json_remove('{\"a\":1}','$.a')", json_set: "json_set('{}','$.a',1)",
  json_patch: "json_patch('{}','{\"a\":1}')", json_insert: "json_insert('{}','$.a',1)", jsonb: "length(jsonb('{}'))",
  json_group_array: "(SELECT json_group_array(value) FROM json_each('[1]'))",
  json_group_object: "(SELECT json_group_object(key,value) FROM json_each('{\"a\":1}'))",
  arrow: "'{\"a\":1}' ->> '$.a'", typeof: "typeof(1)", cast: "CAST('1' AS INTEGER)", replace: "replace('a','a','b')",
  count: "(SELECT count(*) FROM json_each('[1]'))", min: "min(1,2)", max: "max(1,2)", coalesce: "coalesce(NULL,1)",
  hex: "hex('a')", randomblob: "length(randomblob(16))", substr: "substr('abc',2)", instr: "instr('abc','b')",
  lower: "lower('A')", upper: "upper('a')", length: "length('a')", unixepoch: "unixepoch('subsec')", abs: "abs(-1)",
  random: "typeof(random())", char: "char(65)", group_concat: "(SELECT group_concat(value) FROM json_each('[1,2]'))",
  trim: "trim(' a ')", rtrim: "rtrim('a ')", nullif: "nullif(1,2)", printf: "printf('%d',1)", format: "format('%d',1)",
  strftime: "strftime('%Y','now')", concat_ws: "concat_ws(',','a','b')", concat: "concat('a','b')", iif: "iif(1,2,3)",
  round: "round(1.5)", ifnull: "ifnull(NULL,1)", quote: "quote('a')", unhex: "unhex('41')", likelihood: "likelihood(1,0.5)",
  datetime: "datetime('now')", julianday: "julianday('now')", total_changes: "total_changes()", changes: "changes()",
  last_insert_rowid: "last_insert_rowid()", sqlite_version: "sqlite_version()", sqlite_source_id: "sqlite_source_id()",
  regexp_function: "regexp('a','a')", regexp_operator: "'a' REGEXP 'a'", glob: "'a' GLOB 'a'", like: "'a' LIKE 'a'",
  row_number_window: "(SELECT max(r) FROM (SELECT row_number() OVER (ORDER BY value) r FROM json_each('[1,2]')))",
  pragma_table_info: "(SELECT count(*) FROM pragma_table_info('rowsize'))",
  pragma_foreign_key_list: "(SELECT count(*) FROM pragma_foreign_key_list('rowsize'))",
  pragma_index_list: "(SELECT count(*) FROM pragma_index_list('rowsize'))",
  pragma_foreign_key_check: "(SELECT count(*) FROM pragma_foreign_key_check)",
  sqlite_schema: "(SELECT count(*) FROM sqlite_schema)",
};
for (const [name, expression] of Object.entries(FUNCTIONS)) {
  const result = await query(`SELECT ${expression} AS v`);
  report.functions[name] = ok(result) ? "ok" : error(result);
}
const PRAGMAS = ["PRAGMA foreign_keys", "PRAGMA foreign_keys=ON", "PRAGMA defer_foreign_keys=ON", "PRAGMA quick_check",
  "PRAGMA integrity_check", "PRAGMA foreign_key_check", "PRAGMA table_info('rowsize')", "PRAGMA optimize",
  "PRAGMA journal_mode", "PRAGMA cache_size", "PRAGMA recursive_triggers", "PRAGMA legacy_alter_table=ON", "ANALYZE",
  "SELECT count(*) FROM sqlite_stat1", "VACUUM", "SAVEPOINT s", "BEGIN", "ATTACH ':memory:' AS x"];
for (const sql of PRAGMAS) {
  const result = await query(sql);
  report.functions[sql] = ok(result) ? "ok" : error(result);
}

// --- FTS5 trigram ------------------------------------------------------------
{
  const created = await exec([
    "CREATE VIRTUAL TABLE fts_tri USING fts5(title, body, tokenize='trigram')",
    "INSERT INTO fts_tri(title, body) VALUES ('Fix the boiler in room 204', 'Leaking valve'), ('Clean lobby', 'Weekly')",
  ]);
  const match = await query("SELECT title FROM fts_tri WHERE fts_tri MATCH ? ORDER BY rank", ["oile"]);
  const like = await query("SELECT title FROM fts_tri WHERE title LIKE ?", ["%room 20%"]);
  const caseless = await exec(["CREATE VIRTUAL TABLE fts_tri_ci USING fts5(t, tokenize='trigram case_sensitive 0 remove_diacritics 1')"]);
  const external = await exec([
    "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)",
    "CREATE VIRTUAL TABLE docs_fts USING fts5(title, content='docs', content_rowid='id', tokenize='trigram')",
    "CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN INSERT INTO docs_fts(rowid, title) VALUES (NEW.id, NEW.title); END",
    "INSERT INTO docs(title) VALUES ('Replace air filter')",
    "INSERT INTO docs_fts(docs_fts) VALUES ('rebuild')",
  ]);
  const externalMatch = await query("SELECT rowid FROM docs_fts WHERE docs_fts MATCH ?", ["filt"]);
  report.features.fts5Trigram = {
    create: ok(created) ? "ok" : error(created),
    match: ok(match) ? match.rows : error(match),
    likeOnTrigram: ok(like) ? like.rows : error(like),
    caseInsensitiveRemoveDiacritics: ok(caseless) ? "ok" : error(caseless),
    externalContentWithRebuild: ok(external) ? externalMatch.rows : error(external),
  };
}

// --- json_object change capture (shape from rust/crates/store/src/feed.rs) ---
{
  const columns = Array.from({ length: 70 }, (_, i) => `c${i}`);
  const wide = (prefix) => columns.map((c) => `'${c}', ${prefix}.${c}`).join(", ");
  // A 70-column row image needs 140 function arguments in one json_object call.
  const flat = await exec([
    `CREATE TABLE wide (id TEXT PRIMARY KEY, ${columns.map((c) => `${c} TEXT`).join(", ")})`,
    "CREATE TABLE changes (seq INTEGER PRIMARY KEY, revision INTEGER, tbl TEXT, op TEXT, doc JSON)",
    "CREATE INDEX changes_pending ON changes(seq) WHERE revision IS NULL",
    `CREATE TRIGGER wide_ai AFTER INSERT ON wide BEGIN INSERT INTO changes(tbl, op, doc) VALUES ('wide','insert', json_object('id', NEW.id, ${wide("NEW")})); END`,
  ]);
  // Workaround shape: chunks of at most 16 pairs merged with json_patch.
  const chunks = [];
  for (let i = 0; i < columns.length; i += 15) chunks.push(columns.slice(i, i + 15));
  const patched = chunks.map((chunk) => `json_object(${chunk.map((c) => `'${c}', NEW.${c}`).join(", ")})`)
    .reduce((acc, part) => `json_patch(${acc}, ${part})`, "json_object('id', NEW.id)");
  const chunked = await exec([
    "DROP TRIGGER IF EXISTS wide_ai",
    `CREATE TRIGGER wide_ai2 AFTER INSERT ON wide BEGIN INSERT INTO changes(tbl, op, doc) VALUES ('wide','insert', ${patched}); END`,
  ]);
  const insert = await query(`INSERT INTO wide (id, c0, c69) VALUES ('r1', 'zero', 'sixty-nine')`);
  const captured = await query("SELECT op, json_extract(doc, '$.c69') AS c69, json_extract(doc, '$.id') AS id FROM changes WHERE revision IS NULL");
  // Narrow table: the common case.
  const narrow = await exec([
    "CREATE TABLE narrow (id TEXT PRIMARY KEY, name TEXT, done BOOLEAN, meta JSON)",
    "CREATE TRIGGER narrow_au AFTER UPDATE ON narrow BEGIN INSERT INTO changes(tbl, op, doc) VALUES ('narrow','update', json_object('id', NEW.id, 'name', NEW.name, 'done', CASE WHEN NEW.done THEN json('true') ELSE json('false') END, 'meta', json(NEW.meta))); END",
    "INSERT INTO narrow VALUES ('n1', 'a', 0, '{\"k\":1}')",
    "UPDATE narrow SET name = 'b', done = 1 WHERE id = 'n1'",
  ]);
  const narrowCaptured = await query("SELECT doc FROM changes WHERE tbl = 'narrow'");
  // Revision stamping with a window function (feed::finalize shape).
  const stamp = await query("UPDATE changes SET revision = 7 FROM (SELECT seq AS s, row_number() OVER (ORDER BY seq) AS o FROM changes WHERE revision IS NULL) WHERE seq = s RETURNING seq");
  report.features.jsonObjectCapture = {
    flat140Arguments: ok(flat) ? "ok" : error(flat),
    chunkedJsonPatch: ok(chunked) ? "ok" : error(chunked),
    insertThroughTrigger: ok(insert) ? "ok" : error(insert),
    captured: ok(captured) ? captured.rows : error(captured),
    narrowTrigger: ok(narrow) ? narrowCaptured.rows : error(narrow),
    revisionStamp: ok(stamp) ? stamp.rows.length : error(stamp),
  };
}

// --- regexp ------------------------------------------------------------------
{
  const regexp = await query("SELECT 'abc' REGEXP '^a' AS x");
  report.features.regexp = ok(regexp) ? regexp.rows : error(regexp);
}

await call("/reset");
const text = JSON.stringify(report, null, 2);
if (process.argv[3]) writeFileSync(process.argv[3], text + "\n");
console.log(text);
