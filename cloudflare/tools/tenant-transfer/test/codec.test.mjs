// node --test test/  (no account, no network, no workerd)

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { test } from 'node:test';
import { RowHasher, fromWire, keyIndexes, selectSql, tablePlan, toWire } from '../src/codec.mjs';
import { openFile } from '../src/file.mjs';
import { MAX_PARAMS, MAX_STATEMENT_BYTES, buildInserts, readPlans, readSchema, readSequences, schemaPhases } from '../src/sql.mjs';

const EDGE_ROWS = [
  // a: INTEGER PRIMARY KEY, b: no affinity, c: TEXT, d: REAL, e: BLOB, f: INTEGER, g: NUMERIC
  [1, 5, 'plain', 1.5, new Uint8Array([0, 1, 2]), 9007199254740993n, 3],
  [2, 5.0, '', 3.0, new Uint8Array([]), -9223372036854775808n, 2.5],
  [3, 'text', 'ünïcødé ✓ \u0000 nul', Infinity, null, 9223372036854775807n, '7'],
  [4, null, null, -Infinity, new Uint8Array([255]), 0, null],
  [5, new Uint8Array([9]), '5', -0.0, null, -1, 1e300],
];

function source() {
  const file = openFile(':memory:');
  file.db.exec('CREATE TABLE t (a INTEGER PRIMARY KEY, b, c TEXT, d REAL, e BLOB, f INTEGER, g NUMERIC)');
  const insert = file.db.prepare('INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)');
  for (const row of EDGE_ROWS) insert.run(...row);
  // Invalid UTF-8 stored as TEXT.
  file.db.exec("INSERT INTO t (a, c) VALUES (6, CAST(X'FF00FE' AS TEXT))");
  return file;
}

function checksum(file, plan) {
  const hasher = new RowHasher(createHash);
  for (const raw of file.sql.exec(selectSql(plan).sql).raw()) hasher.addTyped(raw);
  return hasher.digest();
}

function typed(file) {
  return file.db.prepare('SELECT a, typeof(b), quote(b), typeof(c), hex(c), typeof(d), quote(d), typeof(e), hex(e), CAST(f AS TEXT), typeof(g), quote(g) FROM t ORDER BY a').all();
}

test('every storage class survives wire encoding and re-insertion exactly', () => {
  const from = source();
  const [plan] = readPlans(from.sql);
  const rows = [...from.sql.exec(selectSql(plan).sql).raw()].map((raw) => {
    const row = [];
    for (let i = 0; i < raw.length; i += 2) row.push(toWire(raw[i], raw[i + 1]));
    return row;
  });
  // JSON round trip: what actually travels.
  const wire = JSON.parse(JSON.stringify(rows));
  const to = openFile(':memory:');
  to.db.exec('CREATE TABLE t (a INTEGER PRIMARY KEY, b, c TEXT, d REAL, e BLOB, f INTEGER, g NUMERIC)');
  for (const { sql, params } of buildInserts('t', plan.copyColumns, wire)) to.sql.run(sql, ...params);
  assert.deepEqual(typed(to), typed(from));
  assert.deepEqual(checksum(to, plan), checksum(from, plan));
});

test('checksum notices a storage-class change that JavaScript would hide', () => {
  const a = openFile(':memory:');
  const b = openFile(':memory:');
  a.db.exec('CREATE TABLE t (x); INSERT INTO t VALUES (3)');
  b.db.exec('CREATE TABLE t (x); INSERT INTO t VALUES (3.0)');
  const [plan] = readPlans(a.sql);
  assert.notEqual(checksum(a, plan).sha256, checksum(b, plan).sha256);
});

test('wire forms', () => {
  assert.equal(toWire('integer', '42'), 42);
  assert.deepEqual(toWire('integer', '9007199254740993'), { i: '9007199254740993' });
  assert.deepEqual(toWire('real', 2), { r: 2 });
  assert.equal(toWire('real', 2.5), 2.5);
  assert.deepEqual(toWire('real', -Infinity), { r: '-Infinity' });
  assert.deepEqual(toWire('text', new Uint8Array([0xff])), { t: '/w==' });
  assert.deepEqual(fromWire(null), { sql: 'NULL' });
  assert.deepEqual(fromWire({ r: 'Infinity' }), { sql: '9e999' });
});

test('inserts respect 100 bound parameters and 100 KB of statement text', () => {
  const columns = Array.from({ length: 43 }, (_, i) => `c${i}`);
  const rows = Array.from({ length: 500 }, (_, r) => columns.map((_, i) => (i % 3 ? `v${r}` : { i: '9007199254740993' })));
  const statements = buildInserts('wide', columns, rows);
  assert.equal(statements.reduce((n, s) => n + (s.sql.match(/\(/g).length - 1), 0) >= 500, true);
  for (const { sql, params } of statements) {
    assert.ok(params.length <= MAX_PARAMS, `${params.length} params`);
    assert.ok(sql.length < MAX_STATEMENT_BYTES, `${sql.length} bytes`);
  }
  // Big values travel as parameters, not text.
  const [big] = buildInserts('t', ['a', 'b'], [[1, 'x'.repeat(1_500_000)]]);
  assert.ok(big.sql.length < 200);
  assert.throws(() => buildInserts('t', Array.from({ length: 101 }, (_, i) => `c${i}`), [[]]), /100 bound parameters/);
});

test('table plans: rowid alias, hidden rowid, WITHOUT ROWID composite keys', () => {
  const file = openFile(':memory:');
  file.db.exec(`
    CREATE TABLE aliased (id INTEGER PRIMARY KEY, v TEXT);
    CREATE TABLE textkey (id TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE composite (a TEXT, b INTEGER, v TEXT, PRIMARY KEY (b, a)) WITHOUT ROWID;
    CREATE VIRTUAL TABLE f USING fts5(v, content='textkey', content_rowid='rowid');`);
  const plans = Object.fromEntries(readPlans(file.sql).map((p) => [p.name, p]));
  assert.deepEqual(Object.keys(plans).sort(), ['aliased', 'composite', 'textkey']);
  assert.deepEqual(plans.aliased.copyColumns, ['id', 'v']);
  assert.deepEqual(plans.textkey.copyColumns, ['rowid', 'id', 'v']);
  assert.deepEqual(plans.composite.key, ['b', 'a']);
  assert.deepEqual(keyIndexes(plans.composite), [1, 0]);
  const phases = schemaPhases(readSchema(file.sql));
  assert.deepEqual(phases.fts, ['f']);
  assert.ok(!phases.before.some((o) => o.name.startsWith('f_')), 'FTS shadow tables are created by SQLite');
});

test('keyset paging over a composite key visits every row once', () => {
  const file = openFile(':memory:');
  file.db.exec('CREATE TABLE c (a TEXT, b INTEGER, PRIMARY KEY (a, b)) WITHOUT ROWID');
  const insert = file.db.prepare('INSERT INTO c VALUES (?, ?)');
  for (let i = 0; i < 250; i++) insert.run(`k${i % 7}`, i);
  const [plan] = readPlans(file.sql);
  const keys = keyIndexes(plan);
  const seen = [];
  let after = null;
  do {
    const { sql, params } = selectSql(plan, { after, limit: 33 });
    const page = [...file.sql.exec(sql, ...params).raw()].map((raw) => [toWire(raw[0], raw[1]), toWire(raw[2], raw[3])]);
    seen.push(...page);
    after = page.length === 33 ? keys.map((i) => page.at(-1)[i]) : null;
  } while (after);
  assert.equal(seen.length, 250);
  assert.equal(new Set(seen.map((r) => JSON.stringify(r))).size, 250);
});

test('sqlite_sequence values are read exactly', () => {
  const file = openFile(':memory:');
  file.db.exec("CREATE TABLE s (id INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO s VALUES (9007199254740993); DELETE FROM s");
  assert.deepEqual(readSequences(file.sql), { s: { i: '9007199254740993' } });
});

test('tablePlan ignores generated and hidden columns', () => {
  const plan = tablePlan({
    name: 'g',
    sql: 'CREATE TABLE g (id INTEGER PRIMARY KEY, a TEXT, b TEXT GENERATED ALWAYS AS (upper(a)))',
    columns: [
      { name: 'id', type: 'INTEGER', pk: 1, hidden: 0 },
      { name: 'a', type: 'TEXT', pk: 0, hidden: 0 },
      { name: 'b', type: 'TEXT', pk: 0, hidden: 2 },
    ],
    primaryKey: ['id'],
  });
  assert.deepEqual(plan.copyColumns, ['id', 'a']);
});
