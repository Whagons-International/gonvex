import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync, execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { compileLocalSchema } from '../dist/local-schema.js';
import { inspectSchemaDesign, schemaFindings } from '../dist/schema-inspection.js';

async function inspect(sql){
  const migrations=[{name:'0001_fixture.sql',sql}];let result;
  await compileLocalSchema(migrations,(db,schema)=>{result=inspectSchemaDesign(db,schema,migrations);});
  return result;
}
test('diagnostics report changed concerns and locations, not the unchanged backlog',async()=>{
  const base=`CREATE TABLE tasks(_id TEXT PRIMARY KEY, legacyId TEXT, teamId TEXT, note TEXT);
CREATE INDEX tasks_team ON tasks(teamId);`;
  const before=await inspect(base);
  const after=await inspect(base+`
ALTER TABLE tasks ADD COLUMN teamIds JSON;
ALTER TABLE tasks ADD COLUMN ownerId TEXT;
CREATE INDEX tasks_team_note ON tasks(teamId,note);
CREATE TRIGGER costly AFTER UPDATE ON tasks BEGIN SELECT count(*) FROM tasks; END;
CREATE VIEW task_titles AS SELECT note FROM tasks;`);
  const findings=schemaFindings(before,after);
  assert.deepEqual(new Set(findings.map(f=>f.code)),new Set(['relationship-declaration','duplicate-representation','overlapping-indexes','generated-machinery','broad-validation']));
  assert.ok(findings.every(f=>f.source?.file==='migrations/0001_fixture.sql'&&f.source.line>=3));
  assert.ok(findings.every(f=>f.change&&f.suggestion&&f.evidence));
  assert.ok(!findings.some(f=>f.concern.includes('legacyId')));
  assert.deepEqual(schemaFindings(after,after),[]);
});
test('native FKs and explicit reference requests declare relationships without inventing targets',async()=>{
  const before=await inspect('CREATE TABLE targets(_id TEXT PRIMARY KEY);');
  const after=await inspect(`CREATE TABLE targets(_id TEXT PRIMARY KEY);
CREATE TABLE links(_id TEXT PRIMARY KEY,targetId TEXT REFERENCES targets(_id),targets JSON);
CREATE TABLE _gonvex_reference_requests(declaration JSON NOT NULL);
INSERT INTO _gonvex_reference_requests(declaration) VALUES('{"version":1,"references":[{"source":"links","path":"$.targets[*]","target":"targets"}]}');`);
  assert.equal(after.tables.links.columns.targetId.declared,true);
  assert.equal(after.tables.links.columns.targets.declared,true);
  assert.equal(schemaFindings(before,after).filter(f=>f.code==='relationship-declaration').length,0);
});
test('index comparisons preserve uniqueness, predicates, expressions, collation and direction',async()=>{
  const base='CREATE TABLE samples(_id TEXT PRIMARY KEY,a TEXT,b TEXT);';
  const before=await inspect(base);
  const after=await inspect(base+`
CREATE INDEX regular ON samples(a);
CREATE UNIQUE INDEX unique_a ON samples(a);
CREATE UNIQUE INDEX unique_ab ON samples(a,b);
CREATE INDEX partial_a ON samples(a) WHERE b IS NOT NULL;
CREATE INDEX other_partial ON samples(a) WHERE b IS NULL;
CREATE INDEX nocase_a ON samples(a COLLATE NOCASE);
CREATE INDEX descending_a ON samples(a DESC);
CREATE INDEX expression_a ON samples(lower(a));`);
  assert.deepEqual(schemaFindings(before,after).filter(f=>f.code==='overlapping-indexes'),[]);
});

test('real CLI/codegen reports advisory deltas with exit zero and rejects invalid SQL',()=>{
  const root=mkdtempSync(join(process.env.GONVEX_TEST_DATA_DIR??tmpdir(),'schema-advisory-'));
  const cli=fileURLToPath(new URL('../dist/index.js',import.meta.url));
  function command(args){return spawnSync(process.execPath,[cli,...args,'--project',root],{encoding:'utf8',timeout:60_000});}
  try{
    mkdirSync(join(root,'migrations'));mkdirSync(join(root,'gonvex'));
    writeFileSync(join(root,'gonvex.json'),JSON.stringify({project:'schema-test',module:{entrypoint:'gonvex/index.ts'}}));
    writeFileSync(join(root,'gonvex/index.ts'),'export const fixture = 1;');
    writeFileSync(join(root,'migrations/0001.sql'),'CREATE TABLE tasks(_id TEXT PRIMARY KEY, oldOwnerId TEXT);');
    execFileSync('git',['init','-q'],{cwd:root});
    const first=command(['codegen']);assert.equal(first.status,0,first.stderr);
    execFileSync('git',['add','gonvex/_generated/schema-inspection.json'],{cwd:root});
    execFileSync('git',['-c','user.name=Schema Test','-c','user.email=schema@example.invalid','commit','-qm','base inspection'],{cwd:root});
    writeFileSync(join(root,'migrations/0002.sql'),'ALTER TABLE tasks ADD COLUMN newOwnerId TEXT;');
    const generated=command(['codegen']);assert.equal(generated.status,0,generated.stderr);assert.match(generated.stderr,/advisory relationship-declaration/);
    const checked=command(['schema','check','--base','HEAD','--json']);assert.equal(checked.status,0,checked.stderr);
    const report=JSON.parse(checked.stdout);
    assert.equal(report.findings.length,1);assert.match(report.findings[0].concern,/newOwnerId/);assert.doesNotMatch(report.findings[0].concern,/oldOwnerId/);
    assert.match(report.coverage,/SQLite inspection compared/);
    if(process.env.GONVEX_SCHEMA_REPORT_DIR){
      mkdirSync(process.env.GONVEX_SCHEMA_REPORT_DIR,{recursive:true});
      writeFileSync(join(process.env.GONVEX_SCHEMA_REPORT_DIR,'cli-advisory.json'),checked.stdout);
    }
    const missing=command(['schema','check','--base','nonexistent-base']);assert.equal(missing.status,0);assert.match(JSON.parse(missing.stdout).coverage,/No compatible inspection/);
    writeFileSync(join(root,'migrations/0003.sql'),'CREATE TABLE broken(id TEXT, parent TEXT REFERENCES nonexistent(id)); INSERT INTO broken VALUES(\'1\',\'x\');');
    const invalid=command(['schema','check']);assert.notEqual(invalid.status,0);assert.match(invalid.stderr,/0003.sql/);
  }finally{rmSync(root,{recursive:true,force:true});}
});
