import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { auditTaskBinding, proposedTaskBinding, whagonsTaskPreparation } from '../bindings/whagons-task.mjs';
import { sdkApplication } from '../checks/sdk.mjs';

const cli=fileURLToPath(new URL('../cli.mjs',import.meta.url));
const changed = fn => { const c=proposedTaskBinding(); fn(c); return auditTaskBinding(c); };
const blocked = r => {
  assert.equal(r.status,'BLOCKED'); assert.equal(r.executionAllowed,false);
  assert.ok(r.blockers.includes('offline-only'));
  assert.equal(Object.hasOwn(r,'availability'),false);
  assert.equal(Object.hasOwn(r,'operations'),false);
};
test('source-consistent task plan remains blocked, has no application execution interface',()=>{
  const report=auditTaskBinding();blocked(report);
  assert.equal(report.integrity,'valid');assert.equal(report.sourceAudit,'consistent');
  for (const prerequisite of ['durableCommitPort','freshSqliteReadProof','secondClientApplyAndEpoch','sideEffectIsolation','boundedRetentionCleanup'])
    assert.ok(report.blockers.includes(`unverified:${prerequisite}`));
  const plan=proposedTaskBinding();assert.equal(plan.enabled,false);
  assert.ok(Object.values(plan.refs).every(ref=>!('localExecution' in ref)));
  assert.equal(Object.values(plan).some(v=>typeof v==='function'),false);
  assert.throws(()=>{whagonsTaskPreparation.selection.read='tasks.list';},TypeError);
});
for (const [role,kind] of [['create','action'],['update','query'],['remove','query'],['read','reducer'],['collection','reducer']])
  test(`wrong function kind cannot bind ${role}`,()=>{
    const r=changed(c=>{c.refs[role].kind=kind;});blocked(r);assert.ok(r.issues.includes(`function-kind:${role}`));
  });
test('live grid with query kind cannot substitute for bounded second-client replica',()=>{
  const r=changed(c=>{c.refs.collection={path:'tasks.grid',kind:'query',delivery:'live'};});blocked(r);
  assert.ok(r.issues.includes('delivery-mismatch:collection'));assert.ok(r.issues.includes('function-mismatch:collection'));
});
test('discarding delivery metadata or using a query result as second-client apply is refused',()=>{
  for(const role of ['read','collection']) {
    const r=changed(c=>{delete c.refs[role].delivery;});blocked(r);assert.ok(r.issues.includes(`delivery-mismatch:${role}`));
  }
  assert.ok(changed(c=>{c.methods.collection='query';}).issues.includes('sdk-method:collection'));
});
test('existing sdkApplication refuses actual Whagons replica read before auth or network',()=>{
  let calls=0;
  const bindings={refs:proposedTaskBinding().refs,connect:()=>{calls++;throw Error('must not connect');},
    argumentsFor:()=>{calls++;},matches:()=>true,durableEvidence:()=>{calls++;},readEvidence:()=>{calls++;}};
  for(const delivery of ['replica','live','unknown']) {
    bindings.refs.read.delivery=delivery;
    assert.throws(()=>sdkApplication(bindings),error=>error.code==='sdk-read-delivery' && error.status==='blocked');
  }
  bindings.refs.read={path:'tasks.create',kind:'reducer',delivery:null};
  assert.throws(()=>sdkApplication(bindings),error=>error.code==='sdk-read-delivery');assert.equal(calls,0);
});
test('broad list/numeric read and broad legacy update cannot replace exact proposed operations',()=>{
  for(const [role,path] of [['read','tasks.list'],['read','tasks.getByNumericId'],['update','tasks.update'],['remove','tasks.removeByNumericId']]) {
    const r=changed(c=>{c.refs[role].path=path;});blocked(r);assert.ok(r.issues.includes(`function-mismatch:${role}`));
  }
});
test('fresh entity id, exact row lookup and archive _id scope are mandatory declarations',()=>{
  for(const [role,fields] of [['create',['name','workspaceId']],['read',[]],['collection',['numericId']],['remove',['taskId']]]) {
    const r=changed(c=>{c.argumentFields[role]=fields;});blocked(r);assert.ok(r.issues.includes(`argument-scope:${role}`));
  }
});
test('provenance suppression, recipients and extra mutable fields are outside synthetic argument scope',()=>{
  for(const field of ['createdVia','createdByWorkflowId','assigneeUserIds','pendingReminders','attachments','updates']) {
    const r=changed(c=>{c.argumentFields.create.push(field);});blocked(r);assert.ok(r.issues.includes('argument-scope:create'));
  }
  assert.ok(changed(c=>{c.argumentFields.update.push('statusId');}).issues.includes('argument-scope:update'));
});
test('local reducers, default offline queue, optimism, reused clients or telemetry fail policy',()=>{
  for(const [key,value] of [['offline','queue'],['localExecution','enabled'],['optimistic','enabled'],['freshClients',false],['telemetry','enabled']]) {
    const r=changed(c=>{c.sdkPolicy[key]=value;});blocked(r);assert.ok(r.issues.includes(`sdk-policy:${key}`));
  }
});
test('auth, scope, side-effect, revision, apply and cleanup assertions never become proof',()=>{
  for(const key of whagonsTaskPreparation.missingPrerequisites) {
    for(const state of [true,'pass','proposed','configured-unverified']) {
      const r=changed(c=>{c.prerequisites[key]=state;});blocked(r);assert.ok(r.blockers.includes(`unverified:${key}`));
      if(state===true || state==='pass') assert.ok(r.issues.includes(`prerequisite-declaration:${key}`));
    }
    assert.ok(changed(c=>{delete c.prerequisites[key];}).issues.includes(`prerequisite-declaration:${key}`));
  }
});
test('source changes and activation switches require new preparation, never an execution verdict',()=>{
  assert.ok(changed(c=>{c.applicationCommit='other';}).issues.includes('source-pin-mismatch'));
  for(const value of [true,null,'false']) assert.ok(changed(c=>{c.enabled=value;}).issues.includes('disabled-preparation-required'));
});
test('malformed and private nested input cannot leak through audit output',()=>{
  const marker='FAKE_BINDING_PRIVATE_CONTENT';
  const inputs=[null,[],marker,{},...['refs','sdkPolicy','prerequisites'].map(key=>{
    const c=proposedTaskBinding();c[key].headers={authorization:marker};return c;
  })];
  const c=proposedTaskBinding();c.prerequisites.durableCommitPort={result:{credentials:marker,committedRevision:42}};inputs.push(c);
  for(const input of inputs) {const r=auditTaskBinding(input);blocked(r);assert.ok(!JSON.stringify(r).includes(marker));assert.notEqual(r.sourceAudit,'consistent');}
  const hostile=new Proxy({}, {getPrototypeOf(){throw Error(marker);}});
  const r=auditTaskBinding(hostile);blocked(r);assert.equal(r.integrity,'invalid');assert.ok(!JSON.stringify(r).includes(marker));
});
test('CLI is offline, nonzero for blocked declarations, private errors and oversized input',async()=>{
  const hosted=process.env.GITHUB_ACTIONS==='true' && process.env.RUNNER_TEMP ? path.join(process.env.RUNNER_TEMP,'m05-foundation') : null;
  const root=process.env.M05_TEST_ROOT ?? hosted;
  assert.ok(root && (root.startsWith('/mnt/data/work/gonvex-m05-foundation/') || root===hosted));
  await mkdir(root,{recursive:true});const dir=await mkdtemp(path.join(root,'binding-'));
  const marker='FAKE_BINDING_PRIVATE_CONTENT';
  try {
    const file=path.join(dir,'input.json');
    for(const body of [JSON.stringify(proposedTaskBinding()),JSON.stringify({credentials:marker}),`{"${marker}":`,marker.repeat(6000)]) {
      await writeFile(file,body);const run=spawnSync(process.execPath,[cli,'binding-audit',file],{encoding:'utf8',timeout:10000});
      assert.ok([1,2].includes(run.status));assert.ok(!(run.stdout+run.stderr).includes(marker));
      if(run.stdout) blocked(JSON.parse(run.stdout)); else assert.equal(run.stderr,'monitor-input-integrity\n');
    }
    const run=spawnSync(process.execPath,[cli,'binding-audit'],{encoding:'utf8',timeout:10000});
    assert.equal(run.status,2);blocked(JSON.parse(run.stdout));assert.equal(run.stderr,'');
  } finally {await rm(dir,{recursive:true,force:true});}
});
