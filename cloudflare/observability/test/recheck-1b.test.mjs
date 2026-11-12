import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { CheckError, WriteGuard, ids } from '../checks/contract.mjs';
import { dispatch } from '../providers/dispatch.mjs';
import { validateRaw, replay } from '../reports/replay.mjs';

const context=()=>({environment:'local-synthetic',syntheticOnly:true,sideEffectsAudited:true,coldFixture:false,
  tenantAlias:'synthetic-review',identities:{writer:'synthetic-writer',reader:'synthetic-reader',unauthorized:'synthetic-other'},
  operatorCredentials:false,sourceSha:'local-review-fix',deploy:'isolated',region:'local',executor:'node',
  configSha:'cfg',domain:'local-handler-fixture',deadlineMs:1000,scheduledAt:Date.now(),check:'sqlite-read'});
const guard=()=>new WriteGuard({paused:false,active:null,cleanupHealthy:true,remainingWrites:2,fixtures:[]},async()=>{});
const marker='FAKE_PRIVATE_RECHECK_MARKER';
for(const kind of ['proxy','revoked-proxy','code-getter','status-getter','code-object']) test(`N1: dispatch contains ${kind} without invoking private error behavior`,async()=>{
  const secret=Object.assign(Error(marker),{headers:{authorization:marker}});let reads=0,error;
  if(kind==='proxy') error=new Proxy({},{getPrototypeOf(){reads++;throw secret;},get(){reads++;throw secret;}});
  else if(kind==='revoked-proxy') {const p=Proxy.revocable({},{});p.revoke();error=p.proxy;}
  else {
    error=new CheckError('read-rejected');
    if(kind==='code-object')error.code={toString(){reads++;throw secret;}};
    else Object.defineProperty(error,kind==='code-getter'?'code':'status',{get(){reads++;throw secret;}});
  }
  const g=guard();
  const result=await dispatch(context(),{application:{applicationRead:async()=>{throw error;}}},g);
  assert.notEqual(result.status,'pass');assert.equal(result.code,'adapter-error');assert.equal(g.state.paused,true);
  assert.equal(reads,0);assert(!JSON.stringify(result).includes(marker));
});
test('N1: ordinary unclassified rejection is safe and pauses, while intact declared failure retains its code',async()=>{
  const g=guard();const ordinary=await dispatch(context(),{application:{applicationRead:async()=>{throw Error(marker);}}},g);
  assert.equal(ordinary.status,'fail');assert.equal(ordinary.code,'adapter-error');assert.equal(g.state.paused,true);
  assert(!JSON.stringify(ordinary).includes(marker));
  const knownGuard=guard();const known=await dispatch(context(),{application:{applicationRead:async()=>{throw new CheckError('read-rejected','unknown');}}},knownGuard);
  assert.equal(known.status,'unknown');assert.equal(known.code,'read-rejected');assert.equal(knownGuard.state.paused,false);
});
test('N1: constructing a declared failure cannot coerce arbitrary private error-code input',async()=>{
  let reads=0;const g=guard();
  const r=await dispatch(context(),{application:{applicationRead:async()=>{throw new CheckError({toString(){reads++;throw Error(marker);}});}}},g);
  assert.equal(reads,0);assert.equal(r.status,'unknown');assert.equal(r.code,'adapter-error');assert.equal(g.state.paused,true);
  assert(!JSON.stringify(r).includes(marker));
});

const raw=(extra={})=>({schema:'gonvex.monitor.v1',sourceSha:'local-review-fix',deploy:'isolated',region:'local',executor:'node',
  configSha:'cfg',domain:'local-handler-fixture',environment:'local-synthetic',check:'sdk-task',scheduledAt:0,startedAt:0,
  finishedAt:5,deadlineMs:100,clockUncertaintyMs:0,attempt:0,...ids(),status:'pass',code:'ok',durationMs:100,
  operations:[{operationId:ids().operationId,kind:'create',attempted:true,deadlineMs:100,status:'pass',durableMs:80,appliedMs:90}],...extra});
const input=r=>({records:[r],schedule:{start:0,end:10,intervalMs:10,deadlineMs:100,lagMs:0,clockUncertaintyMs:0,
  check:'sdk-task',region:'local',deploy:'isolated',component:'Sync'},asOf:10,
  heartbeat:[{at:0,region:'local',executor:'node',witness:'independent',independentWitness:true,ok:true}]});
for(const status of ['pass','fail']) for(const timing of ['legacy','offset-only','timestamp']) test(`N2: ${status}/${timing} elapsed proof must fit the wall observation`,()=>{
  const r=raw();r.status=status;r.operations[0].status=status;
  if(timing==='offset-only')r.operations[0].attemptedAfterMs=0;
  if(timing==='timestamp')r.operations[0].attemptedAt=0;
  assert.throws(()=>validateRaw(r),/operation-observation/);assert.throws(()=>replay(input(r)),/operation-observation/);
});
test('N2: offset-only proof includes time before the attempt, not just elapsed time alone',()=>{
  const r=raw({durationMs:5});Object.assign(r.operations[0],{attemptedAfterMs:3,durableMs:1,appliedMs:3});
  assert.throws(()=>validateRaw(r),/operation-observation/);
  r.operations[0].attemptedAfterMs=2;validateRaw(r);assert.equal(replay(input(r)).operations.success,1);
});
test('N2: valid legacy evidence and failed late evidence remain usable; declared uncertainty cannot certify early application',()=>{
  const consistent=raw({finishedAt:100});validateRaw(consistent);
  const later=input(consistent);Object.assign(later.schedule,{end:110,intervalMs:110});later.asOf=110;
  assert.equal(replay(later).operations.success,1);
  const failed=structuredClone(consistent);failed.status='fail';failed.operations[0].status='fail';failed.operations[0].deadlineMs=10;
  assert.equal(replay({...later,records:[failed]}).operations.failed,1);
  const uncertain=raw({clockUncertaintyMs:85});validateRaw(uncertain);
  const r=replay(input(uncertain));assert.equal(r.operations.attempted,1);assert.equal(r.operations.success,0);
  assert.equal(r.recordAccounting.operationsUnknown,1);
});
test('N2: actual CLI rejects contradictory legacy evidence without output or private/path leakage',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'gonvex-monitor-recheck-'));
  try {
    const file=join(directory,marker+'.json');await writeFile(file,JSON.stringify(input(raw())));
    const r=spawnSync(process.execPath,[fileURLToPath(new URL('../cli.mjs',import.meta.url)),'replay',file],{encoding:'utf8',timeout:5000});
    assert.equal(r.status,1);assert.equal(r.stdout,'');assert.equal(r.stderr,'monitor-input-integrity\n');
  } finally {await rm(directory,{recursive:true,force:true});}
});
