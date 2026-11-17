import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { ordinaryCommitEvidenceSupport } from '../bindings/commit-evidence.mjs';
import { auditTaskBinding, proposedTaskBinding } from '../bindings/whagons-task.mjs';

const refuse = report => {
  assert.equal(report.status,'BLOCKED'); assert.equal(report.executionAllowed,false);
  assert.equal(report.commitEvidenceSupport.status,'unsupported');
  assert.ok(report.blockers.includes(report.commitEvidenceSupport.reason));
  assert.ok(report.blockers.includes('unverified:freshSqliteReadProof'));
  assert.ok(report.blockers.includes('unverified:secondClientApplyAndEpoch'));
  for(const key of ['operations','availability','confirmation','cursor']) assert.equal(Object.hasOwn(report,key),false);
};
test('actual native read-fence witness cannot become a command commit proof',async()=>{
  const witness=JSON.parse(await readFile(new URL('../../../docs/cloudflare/verification/M05-commit-evidence-contract/native-witness.json',import.meta.url),'utf8'));
  assert.equal(witness.domain,'controlled-auth-synthetic-native-workerd-sqlite-do-socket');
  assert.equal(witness.receiptBefore.revision,witness.command.revision);
  assert.ok(witness.unrelated.revision>witness.command.revision);
  assert.equal(witness.receiptAfter.revision,witness.unrelated.revision);
  assert.equal(witness.receiptAfter.found,true);assert.equal(witness.command.hasEpoch,false);
  const candidate=proposedTaskBinding();
  for(const k of Object.keys(candidate.prerequisites))candidate.prerequisites[k]='configured-unverified';
  refuse(auditTaskBinding(candidate));
  // This is the actual native witness, not a fabricated app/SDK receipt.
  const report=auditTaskBinding({...candidate,durableEvidence:witness});refuse(report);
  assert.equal(report.integrity,'invalid');
  assert.ok(!JSON.stringify(report).includes('bumps'));
});
test('forged auth, session, capability and epoch assertions cannot upgrade the planner',()=>{
  const marker='FAKE_PRIVATE_COMMIT_EVIDENCE';
  for(const proof of [
    {authenticated:true,subject:marker,tenant:marker,sessionGeneration:1},
    {reducerReceiptLookup:1,replicaWatermark:1},
    {source:'server-durable',cursor:{epoch:marker,revision:7}},
    {result:{committedRevision:7},lookup:{found:true,cursor:{epoch:marker,revision:19}}},
    {resultRevision:7,lookupEpoch:marker,moduleDirectiveEpoch:marker,sameEpoch:true},
  ]) {
    const plan=proposedTaskBinding();plan.prerequisites.durableCommitPort=proof;
    const report=auditTaskBinding(plan);refuse(report);
    assert.ok(report.issues.includes('prerequisite-declaration:durableCommitPort'));
    assert.ok(!JSON.stringify(report).includes(marker));
  }
});
test('all asserted prerequisites and claimed support still leave two independent observations missing',()=>{
  const plan=proposedTaskBinding();
  for(const k of Object.keys(plan.prerequisites))plan.prerequisites[k]=true;
  refuse(auditTaskBinding(plan));
  for(const key of ['supported','commitEvidenceSupport','confirmation','credentials','connect','argumentsFor']) {
    const report=auditTaskBinding({...proposedTaskBinding(),[key]:true});refuse(report);assert.equal(report.integrity,'invalid');
  }
});
test('support assessment does not inspect callbacks, private fields or hostile observations',()=>{
  let accessed=0;
  const hostile=new Proxy({}, {get(){accessed++;throw Error('private');},ownKeys(){accessed++;throw Error('private');},getPrototypeOf(){accessed++;throw Error('private');}});
  const before=ordinaryCommitEvidenceSupport();
  assert.equal(ordinaryCommitEvidenceSupport(hostile),before);
  assert.equal(ordinaryCommitEvidenceSupport(()=>{accessed++;}),before);
  assert.equal(accessed,0);assert.throws(()=>{before.status='supported';},TypeError);
});
test('audit rejects executable observation hooks without invoking network or mutation callbacks',()=>{
  let calls=0;
  const plan=proposedTaskBinding();
  Object.defineProperty(plan,'durableEvidence',{enumerable:true,get(){calls++;throw Error('must not inspect');}});
  refuse(auditTaskBinding(plan));
  for(const key of ['connect','fetch','mutate','durableEvidence']) {
    refuse(auditTaskBinding({...proposedTaskBinding(),[key]:()=>{calls++;throw Error('must not call');}}));
  }
  assert.equal(calls,0);
});
