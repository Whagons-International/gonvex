import assert from 'node:assert/strict';
import test from 'node:test';
import { compileLocalSchema } from '../dist/local-schema.js';

test('local schema compiles SQLite migrations and reads declared columns back', async () => {
  const schema = await compileLocalSchema([{ name: '0001.sql', sql: `-- gonvex:scope tenant
CREATE TABLE tasks (
  "_id" TEXT PRIMARY KEY, title TEXT NOT NULL, tags JSON NOT NULL DEFAULT '[]', done BOOLEAN NOT NULL DEFAULT 0,
  estimate REAL, created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE TABLE _gonvex_internal (id TEXT PRIMARY KEY);
CREATE INDEX tasks_by_title ON tasks (title);` }]);
  assert.deepEqual(Object.keys(schema).sort(), ['members', 'tasks']);
  assert.deepEqual(schema.tasks, { key: '_id', columns: {
    _id: { type: 'text', nullable: false },
    title: { type: 'text', nullable: false },
    tags: { type: 'json', nullable: false, default: "'[]'" },
    done: { type: 'boolean', nullable: false, default: '0' },
    estimate: { type: 'real', nullable: true },
    created_at: { type: 'integer', nullable: false, default: "CAST(unixepoch('subsec') * 1000 AS INTEGER)" },
  } });
  assert.equal(schema.members.key, 'id');
  assert.equal(schema.members.columns.permissions.type, 'json');
});

test('PostgreSQL-only migration syntax fails with the migration name', async () => {
  await assert.rejects(
    compileLocalSchema([{ name: '0002_pg.sql', sql: 'CREATE EXTENSION IF NOT EXISTS pg_trgm;' }]),
    /Local schema compilation failed in 0002_pg\.sql/,
  );
});

test('authoring retains reference requests without claiming partial-replica enforcement', async () => {
  const declaration = {version:2,existing:'reject',tables:[{name:'notes',key:'_id',live:'true'}],references:[]};
  let observed;
  const schema=await compileLocalSchema([{name:'0001_refs.sql',sql:`CREATE TABLE notes(_id TEXT PRIMARY KEY,body TEXT);
    INSERT INTO _gonvex_reference_requests(declaration) VALUES('${JSON.stringify(declaration)}');`}], db=>{
    observed=JSON.parse(db.prepare('SELECT declaration FROM _gonvex_reference_requests').get().declaration);
    assert.equal(db.prepare("SELECT count(*) n FROM sqlite_schema WHERE name='_gonvex_reference_edges'").get().n,0);
  });
  assert.deepEqual(observed,declaration);
  assert.deepEqual(Object.keys(schema).sort(),['members','notes']);
});
