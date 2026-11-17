import assert from 'node:assert/strict';
import test from 'node:test';
import {compileLocalSchema} from '../dist/local-schema.js';

test('schema excludes actual FTS virtual/shadow storage while retaining ordinary names containing fts',async()=>{
  const schema=await compileLocalSchema([{name:'search.sql',sql:`
    CREATE TABLE voiceTaskDrafts(_id TEXT PRIMARY KEY,body TEXT);
    CREATE VIRTUAL TABLE task_search USING fts5(body);
    CREATE TABLE ordinary_fts_config(_id TEXT PRIMARY KEY,body TEXT);
  `}]);
  assert.deepEqual(Object.keys(schema).sort(),['members','ordinary_fts_config','voiceTaskDrafts']);
});

test('schema records composite SQL foreign keys and excludes registered derived storage',async()=>{
  const schema=await compileLocalSchema([{name:'derived.sql',sql:`
    CREATE TABLE parents(id TEXT PRIMARY KEY,version INTEGER,UNIQUE(id,version));
    CREATE TABLE edges(id TEXT PRIMARY KEY,owner TEXT,version INTEGER,FOREIGN KEY(owner,version) REFERENCES parents(id,version) ON DELETE CASCADE);
    CREATE TABLE bindings(owner TEXT REFERENCES parents(id),path TEXT,PRIMARY KEY(owner,path)) WITHOUT ROWID;
    INSERT INTO _gonvex_table_options(name,capture,owner_table,owner_column) VALUES('bindings','derived','parents','owner');
  `}]);
  assert.equal(schema.bindings,undefined);
  assert.deepEqual(schema.edges.foreignKeys,[{columns:['owner','version'],targetTable:'parents',targetColumns:['id','version'],onDelete:'CASCADE',onUpdate:'NO ACTION'}]);
});
