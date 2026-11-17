import test, { after } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { writeFile } from 'node:fs/promises';
import { ids, runCheck, WriteGuard } from '../checks/contract.mjs';
import { taskScenario, jobScenario } from '../checks/scenarios.mjs';
import { replay, validateRaw } from '../reports/replay.mjs';

const ctx = extra => ({ environment:'local-synthetic',syntheticOnly:true,sideEffectsAudited:true,coldFixture:false,
  tenantAlias:'synthetic-monitor',identities:{writer:'synthetic-writer',reader:'synthetic-reader',unauthorized:'synthetic-outsider'},
  operatorCredentials:false,sourceSha:process.env.M05_SOURCE_SHA ?? 'revision-regression-fixture',deploy:'review-local',region:'local',executor:'node-local',
  configSha:'review-config',domain:'local-handler-fixture',deadlineMs:10,scheduledAt:Date.now(),...extra });
const guard = () => new WriteGuard({paused:false,active:null,cleanupHealthy:true,remainingWrites:10,fixtures:[]},async()=>{});
const block = ms => { const until=performance.now()+ms; while(performance.now()<until) {} };
const evidence = { domain:'author-local-builtins-and-adapter-fixtures-NOT-SDK-or-hosted-proof',reviewedHead:'445695ee9f4002a571a7aef71a6bda91183490f8',sourceCommit:process.env.M05_SOURCE_SHA ?? 'uncommitted-fixture',results:{} };
after(async()=>{
  const file=process.env.M05_REVISION_RESULTS;
  if(file) {
    assert.ok(file.startsWith('/mnt/data/work/gonvex-m05-foundation/revisions/r1/'));
    await writeFile(file,JSON.stringify(evidence,null,2)+'\n');
  }
});

for (const [name,adapter] of [
  ['sync-body',()=>block(40)],
  ['async-microtask',async()=>{await Promise.resolve();block(40);}],
  ['async-turn-boundary',async()=>{await nextTurn();block(40);}],
]) test(`F1: ${name} cannot PASS a 10ms deadline after 40ms work`,async t=>{
  // Hold only the deadline timer for this case so the real next-turn body
  // must settle. Otherwise a loaded executor may correctly time out before
  // that body starts, which does not prove a 40ms settlement observation.
  if (name==='async-turn-boundary') t.mock.timers.enable({apis:['setTimeout']});
  const g=guard(),r=await runCheck(ctx(),'sqlite-read',adapter,g);
  assert.equal(r.status,'fail');assert.equal(r.code,'deadline');assert.equal(r.attempt,0);
  assert.ok(r.durationMs>=40);assert.equal(r.deadlineExceeded,true);
  if (name==='async-turn-boundary') {
    assert.equal(r.settlement,'fulfilled');assert.ok(r.settledElapsedMs>=40);
    assert.equal(r.observedStatus,'pass');
  }
  // Separate real-timer tests below retain unsettled deadline observations.
  assert.ok(['fulfilled','unsettled'].includes(r.settlement));
  assert.equal(g.state.paused,true);validateRaw(r);
  evidence.results[name]=r;
});
test('F1: late rejection retains failure evidence and does not certify cleanup',async()=>{
  const g=guard();await g.begin(Date.now(),'inflight');
  const r=await runCheck(ctx(),'api',()=>{block(40);throw new Error('private credential content');},g);
  assert.equal(r.status,'fail');assert.equal(r.code,'deadline');assert.equal(r.settlement,'rejected');
  assert.equal(r.observedCode,'adapter-error');assert.equal(r.cleanupState,'retained');
  assert.equal(g.state.active,'inflight');assert.ok(!JSON.stringify(r).includes('private credential'));
});
test('F1: timeout observation never rewrites itself when unresolved work later settles',async()=>{
  const g=guard();await g.begin(Date.now(),'inflight');let finish;
  const r=await runCheck(ctx(),'sdk-task',()=>new Promise(resolve=>{finish=resolve;}),g);
  assert.equal(r.settlement,'unsettled');assert.equal(r.cleanupState,'unknown');assert.equal(r.status,'fail');
  const snapshot=JSON.stringify(r);finish();await nextTurn();
  assert.equal(JSON.stringify(r),snapshot);assert.equal(g.state.active,'inflight');assert.equal(g.state.paused,true);
  evidence.results.unsettledTimeout=r;
});
test('F1: synchronous setup overrun blocks the first write; cleanup is still attempted',async()=>{
  let writes=0,cleanup=0;const g=guard();
  const a={openReader:()=>{block(40);return {principal:'synthetic-reader',waitApplied:async()=>{},close:async()=>{}};},
    mutate:async()=>{writes++;},freshRead:async()=>{},cleanup:async()=>{cleanup++;return true;}};
  const r=await runCheck(ctx(),'sdk-task',c=>taskScenario(c,a),g);
  assert.equal(r.status,'fail');assert.equal(r.code,'deadline');assert.equal(writes,0);assert.equal(cleanup,1);
  assert.equal(r.operations.length,0);assert.equal(g.state.paused,true);
});
test('F1: on-time completion remains a PASS with actual settlement recorded',async()=>{
  const r=await runCheck(ctx({deadlineMs:1000}),'api',async()=>{await nextTurn();});
  assert.equal(r.status,'pass');assert.equal(r.deadlineExceeded,false);assert.equal(r.settlement,'fulfilled');
  assert.ok(r.settledElapsedMs>=0 && r.settledElapsedMs<=r.durationMs);validateRaw(r);
});

const raw=(at,status,extra={})=>({schema:'gonvex.monitor.v1',sourceSha:'source',deploy:'review-local',region:'local',executor:'node-local',
  configSha:'review-config',domain:'local-handler-fixture',environment:'local-synthetic',check:'sdk-task',scheduledAt:at,startedAt:at,
  finishedAt:at+5,deadlineMs:10,clockUncertaintyMs:0,attempt:0,...ids(),status,code:'fixture',durationMs:5,operations:[],...extra});
const op=extra=>({operationId:ids().operationId,kind:'create',attempted:true,deadlineMs:10,status:'fail',...extra});
const schedule={start:0,end:100,intervalMs:100,deadlineMs:10,lagMs:0,clockUncertaintyMs:0,check:'sdk-task',region:'local',deploy:'review-local',component:'Sync'};
const heartbeat=[{at:0,region:'local',executor:'node-local',witness:'separate',independentWitness:true,ok:true}];
const report=(records,extra={})=>replay({schedule,records,heartbeat,asOf:200,...extra});
test('F2: direct review counterexample retains off-grid failed create and removes perfect availability',()=>{
  const failed=op(),r=report([raw(0,'pass'),raw(1,'fail',{operations:[failed]})]);
  assert.deepEqual(r.operations,{attempted:1,success:0,failed:1});assert.equal(r.recordAccounting.offGrid,1);
  assert.equal(r.recordAccounting.firstFail,1);assert.equal(r.observations.length,2);
  assert.equal(r.inclusive.healthyMs,0);assert.equal(r.inclusive.unknownMs,100);assert.equal(r.inclusive.lower,0);
  assert.ok(r.slots[0].coverageIssues.includes('off-grid'));assert.equal(r.slots[0].firstStatus,'pass');
  evidence.results.offGridCounterexample=r;
});
test('F2: completed-after-cutoff and unsettled attempts remain in the denominator without future success',()=>{
  const r=report([raw(0,'pass'),raw(1,'fail',{finishedAt:120,durationMs:119,operations:[op()]})],{asOf:100});
  assert.equal(r.operations.attempted,1);assert.equal(r.operations.failed,1);assert.equal(r.recordAccounting.unobservedAtAsOf,1);
  assert.equal(r.inclusive.unknownMs,100);
  const futurePass=report([raw(0,'pass'),raw(1,'pass',{finishedAt:120,durationMs:119,operations:[op({status:'pass',durableMs:1,appliedMs:2})]})],{asOf:100});
  assert.equal(futurePass.operations.success,0);assert.equal(futurePass.operations.failed,1);
  evidence.results.unobservedAtCutoff=r;
});
test('F2: eligible off-grid retries are distinct attempts and cannot erase a failed first operation',()=>{
  const r=report([raw(0,'pass'),raw(1,'fail',{operations:[op()]}),raw(1,'pass',{attempt:1,operations:[op({status:'pass',durableMs:1,appliedMs:2})]})]);
  assert.deepEqual(r.operations,{attempted:2,success:1,failed:1});assert.equal(r.recordAccounting.retries,1);
  assert.equal(r.inclusive.lower,0);assert.equal(r.observations[1].status,'fail');
});
test('F2: schedule outside window cannot hide an actual in-window attempt; actual outside window is explicit',()=>{
  const r=report([raw(0,'pass'),raw(1000,'fail',{startedAt:20,finishedAt:25,operations:[op({attemptedAt:21})]})]);
  assert.equal(r.operations.failed,1);assert.equal(r.recordAccounting.offGrid,1);assert.equal(r.inclusive.lower,0);
  const outside=report([raw(0,'pass'),raw(1000,'fail',{operations:[op()]})]);
  assert.equal(outside.recordAccounting.outsideWindow,1);assert.equal(outside.operations.attempted,0);
});
test('F2: per-operation timestamps account window-straddling checks and flag legacy timing uncertainty',()=>{
  const exact=report([raw(0,'pass'),raw(1,'fail',{startedAt:90,finishedAt:120,durationMs:30,operations:[op({attemptedAt:95}),op({attemptedAt:110})]})]);
  assert.equal(exact.operations.attempted,1);assert.equal(exact.operations.failed,1);assert.equal(exact.recordAccounting.operationsOutsideWindow,1);
  const legacy=report([raw(0,'pass'),raw(1,'pass',{startedAt:90,finishedAt:120,durationMs:30,operations:[op({status:'pass',durableMs:1,appliedMs:2})]})]);
  assert.equal(legacy.operations.success,0);assert.equal(legacy.recordAccounting.operationsUnknown,1);assert.equal(legacy.recordAccounting.operationWindowUncertain,1);
});
test('F2: duplicate off-grid samples/operations and malformed records fail input integrity explicitly',()=>{
  assert.throws(()=>report([raw(1,'fail'),raw(1,'pass')]),/duplicate-attempt/);
  const duplicate=op();assert.throws(()=>report([raw(1,'fail',{operations:[duplicate]}),raw(2,'pass',{operations:[duplicate]})]),/duplicate-operation/);
  assert.throws(()=>report([raw(0,'pass'),raw(1,'fail',{operations:[{...op(),attempted:'yes'}]})]),/operation-schema/);
  assert.throws(()=>report([raw(0,'pass'),raw(1,'fail',{scheduledAt:NaN})]),/raw-timing/);
});
test('F2: a late observation removes actual affected-slot coverage as well as leaving its expected slot unknown',()=>{
  const r=report([raw(0,'fail',{startedAt:150,finishedAt:155,operations:[op({attemptedAt:150})]}),raw(100,'pass'),raw(200,'pass')],
    {schedule:{...schedule,end:300},heartbeat:[0,100,200].map(at=>({...heartbeat[0],at})),asOf:300});
  assert.equal(r.operations.failed,1);assert.equal(r.slots[0].state,'unknown');assert.equal(r.slots[1].state,'unknown');assert.equal(r.slots[2].state,'healthy');
  assert.equal(r.recordAccounting.late,1);assert.equal(r.recordAccounting.firstFail,1);
});
test('F2: off-grid anomalies remain scoped to the actual component/region/deployment',()=>{
  const r=report([raw(0,'pass'),raw(1,'fail',{region:'us-west-1',operations:[op()]})]);
  assert.equal(r.inclusive.lower,1);assert.equal(r.recordAccounting.offGrid,0);assert.equal(r.operations.attempted,0);
});

const invalidElapsed=[undefined,null,'', '0','100',false,true,NaN,Infinity,-Infinity,-1,{},[]];
for (const elapsed of invalidElapsed) test(`F3: malformed/unknown elapsed ${String(elapsed)} never proves the job deadline`,async()=>{
  const r=await runCheck(ctx({deadlineMs:1000,jobDeadlineMs:100,jobDeadlineSource:'job-v1'}),'job',c=>jobScenario(c,
    {jobTerminal:async()=>({durable:true,terminal:'completed',elapsedMs:elapsed,deadlineSource:'job-v1'})}));
  assert.equal(r.status,'unknown');assert.equal(r.code,'job-elapsed-unknown');validateRaw(r);
  if(elapsed===null) evidence.results.nullJobElapsed=r;
});
test('F3: numeric zero/exact deadline pass, overdue fails, and durable/source/terminal proof remains mandatory',async()=>{
  const run=fields=>runCheck(ctx({deadlineMs:1000,jobDeadlineMs:100,jobDeadlineSource:'job-v1'}),'job',c=>jobScenario(c,
    {jobTerminal:async()=>({durable:true,terminal:'completed',elapsedMs:100,deadlineSource:'job-v1',...fields})}));
  assert.equal((await run({elapsedMs:0})).status,'pass');assert.equal((await run({elapsedMs:100})).status,'pass');
  assert.equal((await run({elapsedMs:100.001})).status,'fail');
  for(const invalid of [{durable:false},{terminal:'pending'},{deadlineSource:'different-job'}]) assert.equal((await run(invalid)).status,'fail');
});

test('F2: a not-yet-due attempt stays in denominator as pending rather than an invented service failure',()=>{
  const r=report([raw(0,'pass'),raw(1,'pass',{startedAt:95,finishedAt:120,durationMs:25,operations:[op({attemptedAt:95,status:'pass',durableMs:1,appliedMs:2})]})],{asOf:100});
  assert.deepEqual(r.operations,{attempted:1,success:0,failed:0});assert.equal(r.recordAccounting.operationsPending,1);assert.equal(r.inclusive.lower,0);
});
test('input integrity: operation records cannot widen the declared deadline or contain impossible elapsed evidence',()=>{
  assert.throws(()=>report([raw(0,'pass',{operations:[op({status:'pass',deadlineMs:1000,durableMs:500,appliedMs:900})]})]),/operation-success-proof/);
  assert.throws(()=>report([raw(0,'pass',{operations:[op({status:'pass',deadlineMs:1000,durableMs:1,appliedMs:2})]})]),/operation-deadline-contract/);
});
