import test, { after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,writeFile,rm } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ids,runCheck,WriteGuard,CheckError } from '../checks/contract.mjs';
import { RawIntegrityError } from '../checks/raw-result.mjs';
import { replay,validateRaw } from '../reports/replay.mjs';

const raw=(at,status='pass',extra={})=>({schema:'gonvex.monitor.v1',sourceSha:'source',deploy:'review-local',region:'local',executor:'node-local',configSha:'cfg',domain:'local-handler-fixture',environment:'local-synthetic',check:'sdk-task',scheduledAt:at,startedAt:at,finishedAt:at+5,deadlineMs:10,clockUncertaintyMs:0,attempt:0,...ids(),status,code:'fixture',durationMs:5,operations:[],...extra});
const op=x=>({operationId:ids().operationId,kind:'create',attempted:true,deadlineMs:10,status:'fail',...x});
const schedule={start:0,end:100,intervalMs:100,deadlineMs:10,lagMs:0,clockUncertaintyMs:0,check:'sdk-task',region:'local',deploy:'review-local',component:'Sync'};
const heartbeat=[{at:0,region:'local',executor:'node-local',witness:'independent',independentWitness:true,ok:true}];
const report=(records,x={})=>replay({schedule,records,heartbeat,asOf:200,...x});
const evidence={domain:'author-local-input-and-privacy-fixtures-NOT-hosted-proof',sourceCommit:process.env.M05_SOURCE_SHA ?? 'uncommitted-fixture',reviewedHead:'49399492911e2655d7295f7e9668d2fb911d0475',results:{}};
after(async()=>{if(process.env.M05_R2_RESULTS){assert.ok(process.env.M05_R2_RESULTS.startsWith('/mnt/data/work/gonvex-m05-foundation/revisions/r2/'));await writeFile(process.env.M05_R2_RESULTS,JSON.stringify(evidence,null,2)+'\n');}});

test('R1: exact recheck edge remains valid, retains failed attempt and withholds healthy coverage',()=>{
  const edge=raw(100,'fail',{clockUncertaintyMs:1,operations:[op({attemptedAt:99})]});validateRaw(edge);
  const r=report([raw(0),edge]);assert.deepEqual(r.operations,{attempted:1,success:0,failed:1});
  assert.equal(r.recordAccounting.eligible,2);assert.equal(r.recordAccounting.operationWindowUncertain,1);
  assert.equal(r.inclusive.unknownMs,100);assert.equal(r.inclusive.lower,0);assert.equal(r.inclusive.upper,1);
  assert.equal(r.inclusive.failedMs,0);evidence.results.actualAttemptRetained=r;
});
test('R1: clock/window/status matrix counts actual eligible attempts before enclosing-record filtering',()=>{
  let cases=0;
  for(const kind of ['create','update']) for(const status of ['pass','fail']) for(const [at,checkStart,error] of [[99,100,1],[99,101,2],[0,1,1],[100,100,1]]) {
    const o=op({kind,status,attemptedAt:at,...(status==='pass'?{durableMs:1,appliedMs:2}:{})});
    const edge=raw(checkStart,status,{clockUncertaintyMs:error,operations:[o]});validateRaw(edge);
    const r=report([raw(0),edge]);assert.equal(r.operations.attempted,1);assert.equal(r.operations.success,0);
    assert.equal(r.operations.failed,status==='fail'?1:0);assert.equal(r.inclusive.lower,0);cases++;
  }
  evidence.results.windowClockStatusCases=cases;
});
test('R1: exact exclusive boundary outside the window remains explicit, without inventing an outage',()=>{
  const r=report([raw(0),raw(100,'fail',{operations:[op({attemptedAt:100})]})]);
  assert.equal(r.operations.attempted,0);assert.equal(r.recordAccounting.operationsOutsideWindow,1);assert.equal(r.inclusive.lower,1);
});
test('R1: coarse-clock uncertainty at the opening boundary remains an attempted UNKNOWN, not a waived record',()=>{
  const s={...schedule,start:50};const r=report([raw(50),raw(50,'pass',{attempt:1,clockUncertaintyMs:2,operations:[op({attemptedAt:49,status:'pass',durableMs:1,appliedMs:2})]})],{schedule:s,heartbeat:[{...heartbeat[0],at:50}]});
  assert.equal(r.operations.attempted,1);assert.equal(r.operations.success,0);assert.equal(r.recordAccounting.operationsUnknown,1);assert.equal(r.inclusive.lower,0);
});
test('R1: duplicate attempts retained at an uncertain edge still fail integrity rather than silently deduplicating',()=>{
  const o=op({attemptedAt:99});assert.throws(()=>report([raw(0),raw(100,'fail',{clockUncertaintyMs:1,operations:[o]}),raw(101,'fail',{clockUncertaintyMs:2,operations:[o]})]),/duplicate-operation/);
});

test('R2: direct future application counterexample fails integrity before any availability object',()=>{
  const impossible=raw(0,'pass',{finishedAt:100,durationMs:100,operations:[op({attemptedAt:95,status:'pass',durableMs:2,appliedMs:10})]});
  assert.throws(()=>validateRaw(impossible),/operation-observation/);
  assert.throws(()=>report([impossible],{asOf:100}),/operation-observation/);evidence.results.futureApplication='rejected-operation-observation';
});
test('R2: exact/uncertain application/cutoff matrix preserves real boundaries and refuses early success',()=>{
  const make=(appliedMs,uncertainty,extra={})=>raw(0,'pass',{finishedAt:100,durationMs:100,clockUncertaintyMs:uncertainty,operations:[op({attemptedAt:95,status:'pass',durableMs:2,appliedMs,...extra})]});
  assert.equal(report([make(5,0)],{asOf:100}).operations.success,1);
  for(const applied of [5.01,6,10]) assert.throws(()=>report([make(applied,0)],{asOf:100}),/operation-observation/);
  const coarse=make(6,1);validateRaw(coarse);
  const before=report([coarse],{asOf:100});assert.equal(before.operations.success,0);assert.equal(before.recordAccounting.operationsUnknown,1);
  assert.equal(report([coarse],{asOf:102}).operations.success,1);
  assert.throws(()=>validateRaw(make(6,1,{attemptedAfterMs:95})),/operation-observation/);
  assert.equal(report([make(6,1,{attemptedAfterMs:94})],{asOf:102}).operations.success,1);
  evidence.results.cutoffUnknown=before;
});
test('R2: monotonic and declared wall evidence must agree; old v1 fallback stays usable',()=>{
  assert.throws(()=>validateRaw(raw(0,'pass',{finishedAt:100,durationMs:100,operations:[op({attemptedAt:95,attemptedAfterMs:90,status:'pass',durableMs:1,appliedMs:2})]})),/operation-clock/);
  const legacy=raw(20,'pass',{finishedAt:30,durationMs:10,operations:[op({status:'pass',durableMs:1,appliedMs:4})]});
  const r=report([raw(0),legacy]);assert.equal(r.operations.success,1);assert.equal(r.recordAccounting.operationTimeFallback,1);
});
test('R2: valid failed late elapsed evidence can exceed its deadline but not its observed timeline',()=>{
  const late=raw(0,'fail',{finishedAt:100,durationMs:100,operations:[op({attemptedAt:10,durableMs:20,appliedMs:30})]});
  const safe=validateRaw(late);assert.equal(safe.operations[0].durableMs,20);assert.equal(report([late]).operations.failed,1);
  assert.throws(()=>validateRaw(raw(0,'fail',{finishedAt:100,durationMs:100,operations:[op({attemptedAt:95,appliedMs:10})]})),/operation-observation/);
});

const invalid=[['undefined',undefined],['null',null],['empty',''],['string','1'],['false',false],['true',true],['nan',NaN],['infinity',Infinity],['negative-infinity',-Infinity],['negative',-1],['huge',Number.MAX_VALUE],['object',{authorization:'FAKE_PRIVATE_SENTINEL'}],['array',['FAKE_CONTENT_SENTINEL']],['function',()=>{}]];
for(const status of ['pass','fail']) for(const field of ['durableMs','appliedMs','attemptedAfterMs','attemptedAt']) test(`R3: all PRESENT ${field} values are scalar bounded timing on ${status}`,()=>{
  for(const [name,value] of invalid) {
    const o=op({status,...(status==='pass'?{durableMs:1,appliedMs:2}:{}),[field]:value});
    const bad=raw(0,status,{operations:[o]});
    assert.throws(()=>validateRaw(bad),error=>error.status==='unknown' && !String(error).includes('SENTINEL'),name);
    assert.throws(()=>report([raw(0),{...bad,attempt:1}]),error=>!String(error).includes('SENTINEL'),name);
  }
});
test('R3: strict validation returns an independent plain snapshot; failed absent timing remains a failed attempt',()=>{
  const input=raw(0,'fail',{operations:[op()]});const safe=validateRaw(input);
  assert.notEqual(safe,input);assert.notEqual(safe.operations,input.operations);assert.notEqual(safe.operations[0],input.operations[0]);
  input.operations[0].durableMs={authorization:'FAKE_PRIVATE_SENTINEL'};
  assert.ok(!JSON.stringify(safe).includes('SENTINEL'));assert.equal(report([safe]).operations.failed,1);
  evidence.results.safeFailedSnapshot=safe;
});
test('R3: accessors, custom serializers, arrays and non-string trace identities cannot smuggle data',()=>{
  let reads=0;const accessor=op();Object.defineProperty(accessor,'durableMs',{enumerable:true,get(){reads++;return {private:'FAKE_PRIVATE_SENTINEL'};}});
  assert.throws(()=>validateRaw(raw(0,'fail',{operations:[accessor]})));assert.equal(reads,0);
  const serializer=op();Object.defineProperty(serializer,'toJSON',{value:()=>({private:'FAKE_PRIVATE_SENTINEL'})});
  assert.throws(()=>validateRaw(raw(0,'fail',{operations:[serializer]})));
  const array=[op()];Object.setPrototypeOf(array,{toJSON:()=>({private:'FAKE_PRIVATE_SENTINEL'})});assert.throws(()=>validateRaw(raw(0,'fail',{operations:array})));
  assert.throws(()=>validateRaw(raw(0,'fail',{traceId:{toString:()=> 'a'.repeat(32),private:'FAKE_PRIVATE_SENTINEL'}})));
  const privateError=Object.assign(new RawIntegrityError('operation-schema'),{headers:{authorization:'FAKE_PRIVATE_SENTINEL'}});
  const hostile=new Proxy(op(),{ownKeys(){throw privateError;}});
  assert.throws(()=>validateRaw(raw(0,'fail',{operations:[hostile]})),error=>error!==privateError && !JSON.stringify(error).includes('SENTINEL'));
  const proxy=new Proxy(op(),{ownKeys(){throw new Error('FAKE_PRIVATE_SENTINEL');}});
  assert.throws(()=>validateRaw(raw(0,'fail',{operations:[proxy]})),error=>error.code==='raw-integrity' && !String(error).includes('SENTINEL'));
});
const context=()=>({environment:'local-synthetic',syntheticOnly:true,sideEffectsAudited:true,coldFixture:false,tenantAlias:'synthetic-monitor',identities:{writer:'synthetic-writer',reader:'synthetic-reader',unauthorized:'synthetic-outsider'},operatorCredentials:false,sourceSha:process.env.M05_SOURCE_SHA ?? 'fixture',deploy:'local',region:'local',executor:'node-local',configSha:'fixture',domain:'local-handler-fixture',deadlineMs:1000,scheduledAt:Date.now()});
test('R3: producer rejects unsafe adapter output before export and pauses new writes',async()=>{
  const g=new WriteGuard({paused:false,active:null,cleanupHealthy:true,remainingWrites:2,fixtures:[]},async()=>{});
  await assert.rejects(runCheck(context(),'sdk-task',c=>{c.operations.push(op({durableMs:{authorization:'FAKE_PRIVATE_SENTINEL'}}));},g),error=>error.status==='unknown' && !String(error).includes('SENTINEL'));
  assert.equal(g.state.paused,true);
});
test('R3: producer returns safe failed attempts and declares Date.now alignment/quantization',async()=>{
  let operation;const r=await runCheck(context(),'sdk-task',c=>{operation=op({attemptedAt:Date.now(),attemptedAfterMs:c.elapsedMs()});c.operations.push(operation);throw new CheckError('write-rejected');});
  assert.equal(r.status,'fail');assert.equal(r.operations.length,1);assert.ok(r.clockUncertaintyMs>=1);validateRaw(r);
  operation.appliedMs={private:'FAKE_PRIVATE_SENTINEL'};assert.ok(!JSON.stringify(r).includes('SENTINEL'));
});
test('R3: CLI integrity rejection has no stdout, raw body, parser context or private error text',async()=>{
  const root=process.env.M05_TEST_ROOT ?? (process.env.GITHUB_ACTIONS==='true'?process.env.RUNNER_TEMP:null);assert.ok(root);
  const dir=await mkdtemp(root+'/privacy-');
  try {
    const cli=fileURLToPath(new URL('../cli.mjs',import.meta.url));const file=dir+'/input.json';
    for(const content of [JSON.stringify({schedule,records:[raw(0,'fail',{operations:[op({appliedMs:['FAKE_CONTENT_SENTINEL']})]})],heartbeat,asOf:200}),'{"FAKE_PRIVATE_SENTINEL":']) {
      await writeFile(file,content);
      assert.throws(()=>execFileSync(process.execPath,[cli,'replay',file],{stdio:['ignore','pipe','pipe']}),error=>error.status===1 && error.stdout.length===0 && error.stderr.toString()==='monitor-input-integrity\n');
    }
    assert.throws(()=>report([raw(0)],{schedule:{...schedule,component:{headers:'FAKE_PRIVATE_SENTINEL'}}}),/schedule-labels/);
  } finally {await rm(dir,{recursive:true,force:true});}
});
