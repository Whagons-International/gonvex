import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { updateDataRows } from '../dist/index.js';

// Runs the batch UPDATE the SDK emits against real SQLite, with the runtime's
// `$1` placeholders, and compares it with the previous `UPDATE ... FROM
// json_each(array)` statement on the same rows.
function database() {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE members (id TEXT PRIMARY KEY, permissions JSON NOT NULL DEFAULT '{}', role TEXT, rank INTEGER);
           CREATE TABLE counters (n INTEGER PRIMARY KEY, label TEXT);`);
  return db;
}
const writer = (db, statements) => ({
  query: async (sql, parameters) => {
    statements.push(sql);
    return db.prepare(sql.replace(/\$(\d+)/g, '?$1')).all(...parameters);
  },
});
const legacy = (db, table, key, updates) => {
  const columns = Object.keys(updates[0].fields);
  const value = (name) => `CASE WHEN (SELECT upper(type) FROM pragma_table_info('${table}') WHERE name = '${name}') LIKE '%JSON%' THEN nullif(source.value -> '$.${name}', 'null') ELSE source.value ->> '$.${name}' END`;
  db.prepare(`UPDATE "${table}" SET ${columns.map((name) => `"${name}" = ${value(name)}`).join(', ')} FROM json_each(?) AS source WHERE "${table}"."${key}" = source.value ->> '$.${key}'`)
    .run(JSON.stringify(updates.map((update) => ({ ...update.fields, [key]: update.id }))));
};
const dump = (db, table) => db.prepare(`SELECT * FROM ${table} ORDER BY 1`).all().map((row) => ({ ...row }));

for (const [name, ids] of [
  ['label-safe ids', ['a', 'b.c', '123e4567-e89b-12d3-a456-426614174000', 'x:y', '9', '__proto__']],
  ['ids that need the json_each lookup', ['$x', 'a"b', 'a[0]', 'sp ace', 'é', "o'k", 'plain']],
]) {
  test(`batch update by primary key matches the json_each join (${name})`, async () => {
    const before = (db) => {
      for (const [index, id] of [...ids, 'untouched'].entries())
        db.prepare('INSERT INTO members (id, permissions, role, rank) VALUES (?, ?, ?, ?)').run(id, JSON.stringify({ old: index }), 'member', index);
    };
    const updates = ids.map((id, index) => ({ id, fields: { permissions: { new: index, list: [id] }, role: index % 2 ? null : `r${index}`, rank: index * 10 } }));
    const expected = database(); before(expected); legacy(expected, 'members', 'id', updates);
    const actual = database(); before(actual);
    const statements = [];
    await updateDataRows(writer(actual, statements), 'members', updates, 'id');
    assert.equal(statements.length, 1);
    assert.doesNotMatch(statements[0], /FROM json_each\(\$1\) AS source WHERE "members"/);
    assert.deepEqual(dump(actual, 'members'), dump(expected, 'members'));
  });
}

test('batch update finds integer primary keys and skips missing rows', async () => {
  const seed = (db) => { for (const n of [1, 2, 3]) db.prepare('INSERT INTO counters VALUES (?, ?)').run(n, `n${n}`); };
  const updates = [{ id: '2', fields: { label: 'two' } }, { id: '7', fields: { label: 'seven' } }];
  const expected = database(); seed(expected); legacy(expected, 'counters', 'n', updates);
  const actual = database(); seed(actual);
  await updateDataRows(writer(actual, []), 'counters', updates, 'n');
  assert.deepEqual(dump(actual, 'counters'), dump(expected, 'counters'));
  assert.equal(dump(actual, 'counters')[1].label, 'two');
});

test('batch update looks rows up by key instead of scanning the table per batch row', async () => {
  const db = database();
  const insert = db.prepare('INSERT INTO members (id) VALUES (?)');
  for (let index = 0; index < 2000; index++) insert.run(`m${index}`);
  const statements = [];
  const updates = Array.from({ length: 500 }, (_, index) => ({ id: `m${index * 4}`, fields: { role: 'admin' } }));
  await updateDataRows(writer(db, statements), 'members', updates, 'id');
  const plan = db.prepare(`EXPLAIN QUERY PLAN ${statements[0].replace(/\$(\d+)/g, '?$1')}`).all('{}').map((row) => row.detail).join(' | ');
  assert.doesNotMatch(plan, /SCAN source/, plan);
  assert.equal(db.prepare("SELECT count(*) AS n FROM members WHERE role = 'admin'").get().n, 500);
});
