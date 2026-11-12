import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, writeFile, readFile } from 'node:fs/promises';
import path from 'node:path';
import { ids, WriteGuard, runCheck } from '../checks/contract.mjs';
import { taskScenario, readScenario, fileScenario, jobScenario } from '../checks/scenarios.mjs';
import { dispatch } from '../providers/dispatch.mjs';
import { sdkApplication } from '../checks/sdk.mjs';
import { networkAdapter } from '../checks/network.mjs';
import { envelope, TraceBudget, limits, metricLabels, crossClockBounds, alias } from '../correlation/envelope.mjs';
import { replay, validateRaw } from '../reports/replay.mjs';
import { config, regionalDefinitions, estimate } from '../providers/config.mjs';
import { defineCheckly } from '../providers/checkly.mjs';
import { originalPaging, alertPayload } from '../providers/operations.mjs';
import { sqliteApplication } from '../fixtures/sqlite-application.mjs';
const c = extra => ({ environment:'local-synthetic',syntheticOnly:true,sideEffectsAudited:true,coldFixture:false,tenantAlias:'synthetic-monitor',identities:{writer:'synthetic-writer',reader:'synthetic-reader',unauthorized:'synthetic-outsider'},operatorCredentials:false,sourceSha:'6273dddf6af1890f039fc14f210753593add5be6',deploy:'local-handler-fixture',region:'local',executor:'node-local',configSha:'test-fixture',domain:'local-handler-fixture',deadlineMs:1000,scheduledAt:Date.now(),...extra });
const guard = (save=async()=>{})=>new WriteGuard({paused:false,active:null,cleanupHealthy:true,remainingWrites:10,fixtures:[]},save);
async function withSqlite(fault, fn) {
  const hostedRoot=process.env.GITHUB_ACTIONS==='true' && process.env.RUNNER_TEMP ? path.join(process.env.RUNNER_TEMP,'m05-foundation') : null;
  const root=process.env.M05_TEST_ROOT ?? hostedRoot;
  assert.ok(root && (root.startsWith('/mnt/data/work/gonvex-m05-foundation/') || root===hostedRoot),'M05_TEST_ROOT must be an owned /mnt path (or hosted RUNNER_TEMP)');
  await mkdir(root,{recursive:true});
  const dir=await mkdtemp(path.join(root,'sqlite-'));
  const a=sqliteApplication(path.join(dir,'app.sqlite'),fault);
  try { await fn(a,dir); } finally { a.close(); await rm(dir,{recursive:true,force:true}); }
}

test('actual isolated SQLite handler: durable create/update, independent applied state, fresh connection reads and cleanup',async()=>withSqlite(null,async(a,dir)=>{
  const ledger=path.join(dir,'ledger.json'), g=guard(s=>writeFile(ledger,JSON.stringify(s)));
  const r=await runCheck(c(),'sdk-task',x=>taskScenario(x,a),g);
  assert.equal(r.status,'pass');assert.equal(r.operations.length,2);assert.equal(a.rowCount(),0);
  assert.equal(JSON.parse(await readFile(ledger,'utf8')).fixtures.length,0);
  assert.equal(r.domain,'local-handler-fixture'); validateRaw(r);
  assert.equal(new Set([r.traceId,r.operationId,r.idempotencyKey,...r.operations.map(o=>o.operationId)]).size,5);
  assert.equal((await runCheck(c(),'sqlite-read',x=>readScenario(x,a))).status,'pass');
}));
for(const fault of ['write','lost-reply','stalled-sync','cleanup']) test(`SQLite adverse ${fault}: never green; all attempted writes retained`,async()=>withSqlite(fault,async a=>{
  const g=guard(),r=await runCheck(c(),'sdk-task',x=>taskScenario(x,a),g);
  assert.notEqual(r.status,'pass');assert.ok(r.operations.length>=1);
  if(fault!=='cleanup') assert.equal(r.operations[0].status,'fail');
  if(fault==='cleanup') {
    assert.equal(r.status,'unknown');assert.equal(g.state.paused,true);
    assert.equal((await runCheck(c(),'sdk-task',x=>taskScenario(x,a),g)).code,'writes-paused');
  }
}));
test('missing real Whagons binding is BLOCKED, never fixture fallback',async()=>{
  const r=await dispatch(c({check:'sdk-task',domain:'real-sdk'}),{},guard());assert.equal(r.status,'blocked');assert.equal(r.code,'application-config-missing');
});
test('synthetic preflight refuses production, operator credentials, shared identities and cold fixture',async()=>{
  for(const extra of [{environment:'production'},{operatorCredentials:true},{coldFixture:true},{sideEffectsAudited:false},{identities:{writer:'synthetic-a',reader:'synthetic-a',unauthorized:'synthetic-c'}}]) await assert.rejects(runCheck(c(extra),'task',()=>{}));
});
test('cleanup ledger refuses exhausted, old, busy and undurable budgets BEFORE side effects',async()=>{
  for(const change of [{remainingWrites:1},{fixtures:[{id:'old',createdAt:0}]},{active:'another'},{cleanupHealthy:false}]) {const g=guard();Object.assign(g.state,change);await assert.rejects(g.begin(Date.now(),'new'));}
  const g=guard(async()=>{throw new Error('disk');});await assert.rejects(g.begin(Date.now(),'new'));assert.equal(g.state.paused,true);
});
test('hung adapter deadline returns failure, preserves active charge and pauses writes',async()=>{
  const g=guard();await g.begin(Date.now(),'inflight');
  const r=await runCheck(c({deadlineMs:10}),'sdk-task',()=>new Promise(()=>{}),g);
  assert.equal(r.code,'deadline');assert.equal(g.state.active,'inflight');assert.equal(g.state.paused,true);
});
test('error messages containing credentials never leave raw records',async()=>{
  const r=await runCheck(c(),'api',()=>{throw new Error('Authorization: Bearer private-secret; user@example.com');});assert.equal(r.code,'adapter-error');assert.ok(!JSON.stringify(r).includes('private-secret'));validateRaw(r);
});
test('cached application reads cannot prove SQLite health',async()=>{
  const r=await runCheck(c(),'sqlite-read',x=>readScenario(x,{applicationRead:()=>({authenticated:true,storage:'local-cache',fresh:true,expected:true})}));assert.equal(r.status,'fail');
});
test('file checks bound bytes and require explicit unauthorized denial, cleanup failure pauses',async()=>{
  const a={privateFileRoundtrip:async(_,bytes)=>({bytes,privateHandle:{}}),unauthorizedFileRead:async()=>({denied:false,bytesReturned:1}),cleanupFile:async()=>false};
  const g=guard(),r=await runCheck(c({fileBytes:new Uint8Array([1]),fileIntervalMs:900000,fileSideEffectsApproved:true}),'files',x=>fileScenario(x,a),g);
  assert.equal(r.status,'unknown');assert.equal(g.state.paused,true);
  const oversize=await runCheck(c({fileBytes:new Uint8Array(1025)}),'files',x=>fileScenario(x,a),guard());assert.equal(oversize.status,'blocked');
});
test('job-specific deadline and durable terminal evidence required; generic five-minute success rejected',async()=>{
  assert.equal((await runCheck(c(),'job',x=>jobScenario(x,{}))).status,'blocked');
  const ctx=c({jobDeadlineMs:1500,jobDeadlineSource:'fixture-contract-v1'});
  assert.equal((await runCheck(ctx,'job',x=>jobScenario(x,{jobTerminal:async()=>({durable:true,terminal:'completed',elapsedMs:1600,deadlineSource:'fixture-contract-v1'})}))).status,'fail');
});
test('network target allowlist has no ambient production URL and refuses credentials/redirect target ambiguity',()=>{
  for(const origin of ['http://localhost','https://user:secret@example.com','https://example.com?key=secret','https://other.example']) assert.throws(()=>networkAdapter({origin,allowedOrigins:['https://example.com'],bootstrapMarker:'app'}));
});

const span = (attributes={}) => ({...ids(),spanId:'abcdef0123456789',stage:'durable-commit',attempt:0,attributes});
const budget = patch => new TraceBudget({spans:0,bytes:0,failed:false,counters:{outcomes:0,sampledOut:0,dropped:0,exportFailed:0,exported:0},...patch},async()=>{});
test('redaction rejects credentials/content/raw identities/headers/queryargs and arbitrary nesting',()=>{
  for(const key of ['authorization','cookies','headers','signedUrl','content','email','tenantId','queryargs','unexpected']) assert.throws(()=>envelope(span({[key]:'private-secret'})));
  assert.throws(()=>envelope(span({durationMs:{secret:'x'}})));
  assert.equal(envelope(span({tenantAlias:alias('s'.repeat(32),'raw-tenant')})).attributes.tenantAlias.length,32);
});
test('async links/versioned envelope and bounded metrics cannot promote trace IDs to idempotency or labels',()=>{
  assert.throws(()=>envelope({...span(),traceId:'bad'}));
  assert.throws(()=>envelope({...span(),links:Array(9).fill({})}));
  for(const key of ['operationId','tenantId','gatewayId','traceId']) assert.throws(()=>metricLabels({[key]:'abc'}));
  assert.deepEqual(metricLabels({region:'local',component:'Sync'}),{region:'local',component:'Sync'});
});
test('export budget reserves failures, never sends over caps, counts dropped/export-failed, rejects 65 spans',async()=>{
  let sends=0;const b=budget({spans:19999}); const x=span();
  await b.export([x],'failure',async()=>{sends++;throw new Error('private sink credential');});
  await b.export([x],'failure',async()=>{sends++;});
  assert.equal(sends,1);assert.equal(b.state.counters.exportFailed,1);assert.equal(b.state.counters.dropped,1);
  await assert.rejects(b.export(Array(65).fill(x),'failure',async()=>{}));
  const bytes=budget({bytes:limits.bytesPerDeployDay});assert.equal(await bytes.export([x],'slow',async()=>{throw new Error('must not send');}),false);
});
test('failed ledger stops exports; outcome counters remain; cross-clock uncertainty is explicit',async()=>{
  const b=budget();b.persist=async()=>{throw new Error('disk');};assert.equal(await b.export([span()],'failure',async()=>assert.fail()),false);assert.equal(b.state.failed,true);
  assert.deepEqual(crossClockBounds({wallMs:100,uncertaintyMs:20},{wallMs:110,uncertaintyMs:30}),{lowerMs:-40,upperMs:60,synchronized:false});
});

const raw=(at,status='pass',extra={})=>({schema:'gonvex.monitor.v1',sourceSha:'source',deploy:'fixture',region:'local',executor:'local-node',configSha:'config',domain:'local-handler-fixture',environment:'local-synthetic',check:'sdk-task',scheduledAt:at,startedAt:at,finishedAt:at+5,deadlineMs:10,clockUncertaintyMs:1,attempt:0,...ids(),status,code:'fixture',durationMs:5,operations:[],...extra});
const sched={start:0,end:400,intervalMs:100,deadlineMs:10,lagMs:5,clockUncertaintyMs:1,check:'sdk-task',region:'local',deploy:'fixture',component:'Sync'};
const hearts=[0,100,200,300].map(at=>({at,region:'local',executor:'local-node',witness:'separate-runner',independentWitness:true,ok:true}));
const report=(records,extra={})=>replay({schedule:sched,records,heartbeat:hearts,asOf:500,...extra});
test('regional first failure survives passing retry, missing/late samples remain unknown',()=>{
  const r=report([raw(0),raw(100,'fail'),raw(100,'pass',{attempt:1}),raw(200,'pass',{startedAt:250,finishedAt:255})]);
  assert.equal(r.inclusive.firstFail,1);assert.equal(r.inclusive.failedMs,100);assert.equal(r.inclusive.unknownMs,200);assert.equal(r.inclusive.lower,.25);assert.equal(r.inclusive.upper,.75);assert.equal(r.inclusive.missing,1);assert.equal(r.inclusive.late,1);
});
test('provider heartbeat, export failure and paused monitors are UNKNOWN, healthy other region cannot hide it',()=>{
  for(const status of ['paused','export-failed','blocked','unknown']) assert.equal(report([raw(0,status)]).slots[0].state,'unknown');
  assert.equal(report([raw(0)],{heartbeat:[]}).inclusive.unknownMs,400);
  assert.equal(report([raw(0,'pass',{region:'us-east-1'})]).inclusive.firstPass,0);
});
test('inclusive maintenance preserved; retrospective maintenance rejected; duplicate attempt rejected',()=>{
  const records=[raw(0,'fail'),raw(100),raw(200),raw(300)];
  const r=report(records,{maintenance:[{start:0,end:100,declaredAt:0}]});assert.equal(r.inclusive.failedMs,100);assert.equal(r.maintenanceAdjusted.failedMs,0);
  assert.throws(()=>report(records,{maintenance:[{start:0,end:100,declaredAt:1}]}));
  assert.throws(()=>report([raw(0),raw(0)]));
});
test('ambiguous create belongs in denominator; late reconciliation cannot rewrite initial operation',()=>{
  const op={operationId:ids().operationId,kind:'create',attempted:true,deadlineMs:10,status:'fail'};
  const r=report([raw(0,'fail',{operations:[op]})]);assert.deepEqual(r.operations,{attempted:1,success:0,failed:1});
  assert.throws(()=>report([raw(0,'fail',{operations:[op]}),raw(100,'pass',{operations:[{...op,status:'pass',durableMs:1,appliedMs:2}]})]));
  assert.throws(()=>validateRaw(raw(0,'pass',{operations:[{...op,status:'pass',durableMs:2,appliedMs:11}]})));
});
test('recovery requires two healthy regional observations; unknown breaks streak; raw privacy enforced',()=>{
  const r=report([raw(0,'fail'),raw(100),raw(200,'unknown'),raw(300)]);assert.equal(r.incidents[0].recoveredAt,null);
  const recovered=report([raw(0,'fail'),raw(100),raw(200),raw(300)]);assert.ok(recovered.incidents[0].recoveredAt);
  assert.throws(()=>validateRaw({...raw(0),headers:{authorization:'secret'}}));
});
test('configuration has independent single-region schedules, no activation; original P1-P5 unresolved',()=>{
  assert.equal(config.activated,false);const defs=regionalDefinitions();assert.equal(defs.length,36);
  assert.ok(defs.every(x=>x.locations.length===1 && !x.activated && x.alertChannels.length===0));
  assert.equal(new Set(defs.map(x=>x.locations[0])).size,3);assert.equal(originalPaging.filter(x=>x.state==='unresolved').length,5);
  const e=estimate();assert.equal(e.cost,null);assert.equal(e.rows.filter(x=>x.check==='sdk-task').reduce((s,x)=>s+x.scheduled,0),25920);
  assert.throws(()=>estimate(32));
});
test('Checkly adapter builds only disabled constructs, missing code bindings block rather than GET 200 substitution',()=>{
  class Construct { constructor(id,c) {this.id=id;this.config=c;} }
  assert.throws(()=>defineCheckly({ApiCheck:Construct,BrowserCheck:Construct}));
  assert.throws(()=>defineCheckly({ApiCheck:Construct,BrowserCheck:Construct,origin:'https://example.invalid',runtimeId:'unverified',entrypoints:{}}));
  const entrypoints=Object.fromEntries(config.checks.map(x=>[x.id,'explicit-local-spec.mjs']));
  assert.equal(defineCheckly({ApiCheck:Construct,BrowserCheck:Construct,origin:'https://example.invalid',runtimeId:'unverified',entrypoints,Frequency:{EVERY_1M:1,EVERY_5M:5,EVERY_15M:15},RetryStrategyBuilder:{singleRetry:options=>options}}).every(x=>x.config.activated===false),true);
});
test('alert sink prepares bounded local payload only, no send or destination, stable dedupe',()=>{
  const input={environment:'dev-synthetic',component:'Sync',region:'us-east-1',failureClass:'http200-stalled-sync',phase:'symptom',incidentId:'a'.repeat(32)};
  const p=alertPayload(input);assert.equal(p.destination,null);assert.equal(p.receivedAt,null);assert.equal(p.dedupeKey,alertPayload({...input,phase:'recovery'}).dedupeKey);assert.throws(()=>alertPayload({...input,component:'secret customer content'}));
});

test('real SDK adapter requires application and durable evidence bindings; cannot infer revision from app result',()=>{
  assert.throws(()=>sdkApplication({}));
  assert.throws(()=>sdkApplication({connect:()=>{},argumentsFor:()=>{},matches:()=>true,readEvidence:()=>({})}));
});
test('actual SQLite handlers enforce writer/reader separation at handler entry',async()=>withSqlite(null,async a=>{
  await assert.rejects(a.applicationRead('synthetic-writer',{}));
  await assert.rejects(a.mutate('synthetic-reader',{kind:'create',fixtureId:'synthetic-denied',operationId:ids().operationId}));
  assert.equal(a.rowCount(),0);
}));
test('file roundtrip still fails when unauthorized access returns any bytes, even if cleanup succeeds',async()=>{
  const a={privateFileRoundtrip:async(_,bytes)=>({bytes,privateHandle:{}}),unauthorizedFileRead:async()=>({denied:false,bytesReturned:1}),cleanupFile:async()=>true};
  const r=await runCheck(c({fileBytes:new Uint8Array([1]),fileIntervalMs:900000,fileSideEffectsApproved:true}),'files',x=>fileScenario(x,a),guard());
  assert.equal(r.status,'fail');assert.equal(r.code,'private-file-owner');
});
test('partial-slot maintenance has exact eligible time and never disappears from inclusive view',()=>{
  const r=report([raw(0,'fail'),raw(100),raw(200),raw(300)],{maintenance:[{start:25,end:75,declaredAt:0}]});
  assert.equal(r.inclusive.eligibleMs,400);assert.equal(r.maintenanceAdjusted.eligibleMs,350);assert.equal(r.maintenanceAdjusted.failedMs,50);
  assert.throws(()=>report([raw(0),raw(100,'pass',{configSha:'other-config'})]));
});

test('deadline abort prevents a second write even if first adapter ignores cancellation',async()=>{
  let mutations=0;const g=guard();
  const a={openReader:async()=>({principal:'synthetic-reader',waitApplied:async()=>({principal:'synthetic-reader',revision:1,matches:true,applied:true}),close:async()=>{}}),
    mutate:async(_,o)=>{mutations++;await new Promise(r=>setTimeout(r,30));return {durable:true,operationId:o.operationId,revision:1};},
    freshRead:async()=>({storage:'tenant-sqlite',fresh:true,matches:true,revision:1}),cleanup:async()=>true};
  const r=await runCheck(c({deadlineMs:10}),'sdk-task',x=>taskScenario(x,a),g);
  assert.equal(r.status,'fail');assert.equal(r.operations[0].status,'fail');
  await new Promise(r=>setTimeout(r,40));assert.equal(mutations,1);assert.equal(g.state.paused,true);
});
