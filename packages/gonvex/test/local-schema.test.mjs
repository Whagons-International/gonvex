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

test('explicit PostgreSQL executes original extension/ALTER/default/type semantics', async () => {
  const pg = await compileLocalSchema([{name:'pg.sql',sql:`
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE records(id uuid PRIMARY KEY DEFAULT gen_random_uuid(), data jsonb NOT NULL DEFAULT '{}'::jsonb, tags text[], amount numeric(12,2), at timestamptz DEFAULT now(), optional text);
ALTER TABLE records ADD COLUMN IF NOT EXISTS optional text;
CREATE TABLE proof(value text DEFAULT encode(digest('schema','sha256'),'hex'));
`}], 'postgres');
  assert.equal(pg.records.key,'id'); assert.equal(pg.records.columns.id.type,'uuid');
  assert.equal(pg.records.columns.id.nullable,false); assert.equal(pg.records.columns.data.type,'jsonb');
  assert.equal(pg.records.columns.tags.type,'text[]'); assert.equal(pg.records.columns.amount.type,'numeric(12,2)');
  assert.equal(pg.records.columns.at.type,'timestamp with time zone'); assert.equal(pg.records.columns.optional.nullable,true);
  assert.equal(pg.records.columns.data.default,"'{}'::jsonb"); assert.match(pg.proof.columns.value.default,/digest/);
});

test('explicit dialect rejects mismatched SQL and preserves composite PG constraint identity', async () => {
  await assert.rejects(compileLocalSchema([{name:'bad.sql',sql:'CREATE TABLE t(x int); ALTER TABLE t ADD COLUMN IF NOT EXISTS y text;'}]), /bad.sql/);
  const composite=await compileLocalSchema([{name:'composite.sql',sql:'CREATE TABLE t(a text,b text,PRIMARY KEY(b,a));'}],'postgres');
  assert.deepEqual(composite.t.primaryKey,['b','a']);assert.equal(composite.t.localKeyUnsupported,'composite');
  assert.equal(composite.t.key,'_id');assert.equal(composite.t.columns.a.nullable,false);
});

test('actual Whagons PG baseline compiles offline without rewriting', {skip: !process.env.GONVEX_PG_BASELINE}, async () => {
  const {readFile}=await import('node:fs/promises');
  const sql=await readFile(process.env.GONVEX_PG_BASELINE,'utf8');
  const schema=await compileLocalSchema([{name:'0000_tenant_schema_v2_baseline.sql',sql}],'postgres');
  const tables=[...new Set([...sql.matchAll(/CREATE TABLE IF NOT EXISTS "([^"]+)"/g)].map(x=>x[1]).concat('members'))].filter(x=>!x.startsWith('_gonvex')).sort();
  assert.deepEqual(Object.keys(schema).sort(),tables);assert.equal(schema.tasks.key,'_id');
  assert.equal(schema.tasks.columns._id.type,'text'); assert.equal(schema.tasks.columns._id.nullable,false);
});


test('project dialect is explicit and schema caches cannot cross dialects', async (t) => {
  const {mkdtemp,mkdir,writeFile,rm}=await import('node:fs/promises');
  const {tmpdir}=await import('node:os');const {join}=await import('node:path');
  const {projectLocalSchema}=await import('../dist/local-bindings.js');
  const root=await mkdtemp(join(tmpdir(),'dialect-'));t.after(()=>rm(root,{recursive:true,force:true}));
  await mkdir(join(root,'migrations'));
  await writeFile(join(root,'migrations/1.sql'),'CREATE TABLE sample(id text PRIMARY KEY); ALTER TABLE sample ADD COLUMN IF NOT EXISTS extra jsonb;');
  await writeFile(join(root,'gonvex.json'),JSON.stringify({module:{schemaDialect:'postgres'}}));
  assert.equal((await projectLocalSchema(root)).sample.columns.extra.type,'jsonb');
  await writeFile(join(root,'gonvex.json'),JSON.stringify({module:{schemaDialect:'sqlite'}}));
  await assert.rejects(projectLocalSchema(root),/1.sql/);
  await writeFile(join(root,'gonvex.json'),JSON.stringify({module:{schemaDialect:'auto'}}));
  await assert.rejects(projectLocalSchema(root),/schemaDialect/);
});
