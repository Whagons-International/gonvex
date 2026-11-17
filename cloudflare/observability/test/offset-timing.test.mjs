import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,writeFile,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ids } from '../checks/contract.mjs';
import { validateRaw,replay } from '../reports/replay.mjs';

const raw=(offset=80)=>({schema:'gonvex.monitor.v1',sourceSha:'offset-review',deploy:'isolated',region:'local',executor:'node',configSha:'cfg',
  domain:'local-handler-fixture',environment:'local-synthetic',check:'sdk-task',scheduledAt:0,startedAt:0,finishedAt:100,durationMs:100,
  deadlineMs:100,clockUncertaintyMs:0,attempt:0,...ids(),status:'fail',code:'fixture',
  operations:[{operationId:ids().operationId,kind:'create',attempted:true,attemptedAfterMs:offset,deadlineMs:10,status:'fail'}]});
const input=(r,cutoff,start=0)=>({records:[r],schedule:{start,end:cutoff,intervalMs:cutoff-start,deadlineMs:100,lagMs:0,
  clockUncertaintyMs:0,check:'sdk-task',region:'local',deploy:'isolated',component:'Sync'},asOf:cutoff,heartbeat:[]});
const timestamp=r=>{const copy=structuredClone(r);copy.operations[0].attemptedAt=copy.startedAt+copy.operations[0].attemptedAfterMs;delete copy.operations[0].attemptedAfterMs;return copy;};
for(const kind of ['create','update']) for(const attempt of [0,1]) test(`R1: failed ${kind}/attempt${attempt} offset is checked without completion timings`,()=>{
  const r=raw();Object.assign(r,{finishedAt:5,attempt});r.operations[0].kind=kind;
  assert.throws(()=>validateRaw(r),/operation-observation/);assert.throws(()=>replay(input(r,10)),/operation-observation/);
  r.operations[0].attemptedAfterMs=5;validateRaw(r);
  r.clockUncertaintyMs=1;r.operations[0].attemptedAfterMs=6;validateRaw(r);
  r.operations[0].attemptedAfterMs=6.01;assert.throws(()=>validateRaw(r),/operation-observation/);
});
for(const kind of ['create','update']) for(const attempt of [0,1]) for(const offset of [20,80]) test(`R2: ${kind}/attempt${attempt}/offset${offset} matches equivalent timestamp at cutoffs`,()=>{
  const r=raw(offset);r.attempt=attempt;r.operations[0].kind=kind;validateRaw(r);
  for(const cutoff of [10,offset-1,offset+5,offset+10,100]) {
    const projected=replay(input(r,cutoff)),actual=replay(input(timestamp(r),cutoff));
    assert.deepEqual(projected.operations,actual.operations);
    for(const key of ['operationsPending','operationsUnknown','operationsOutsideWindow','operationWindowUncertain'])assert.equal(projected.recordAccounting[key],actual.recordAccounting[key]);
    if(cutoff<offset)assert.equal(projected.operations.attempted,0);
    else if(cutoff<offset+10){assert.equal(projected.operations.failed,0);assert.equal(projected.recordAccounting.operationsPending,1);}
    else assert.equal(projected.operations.failed,1);
  }
});
test('R2: declared clock bounds withhold expiry and preserve uncertain-window accounting',()=>{
  const r=raw();r.clockUncertaintyMs=2;
  for(const cutoff of [10,81,85,90,91,92,100]) for(const start of [0,80]) {
    if(start>=cutoff)continue;
    const projected=replay(input(r,cutoff,start)),actual=replay(input(timestamp(r),cutoff,start));
    assert.deepEqual(projected.operations,actual.operations);assert.equal(projected.recordAccounting.operationsPending,actual.recordAccounting.operationsPending);
    assert.equal(projected.recordAccounting.operationWindowUncertain,actual.recordAccounting.operationWindowUncertain);
    if(cutoff===90 || cutoff===91){assert.equal(projected.operations.failed,0);assert.equal(projected.recordAccounting.operationsPending,1);}
    if(cutoff===92)assert.equal(projected.operations.failed,1);
  }
});
test('R2: observed success, late failure, zero offsets and shifted time origins retain timestamp-equivalent semantics',()=>{
  for(const shift of [0,1000]) for(const offset of [0,20,80]) for(const status of ['pass','fail']) {
    const r=raw(offset);r.scheduledAt=shift;r.startedAt=shift;r.finishedAt=shift+100;r.status=status;
    Object.assign(r.operations[0],{status,durableMs:1,appliedMs:status==='fail'?15:2});
    const config=input(r,shift+110,shift);config.schedule.intervalMs=110;
    const projected=replay(config),actual=replay({...config,records:[timestamp(r)]});assert.deepEqual(projected.operations,actual.operations);
    assert.equal(projected.operations[status==='pass'?'success':'failed'],1);
  }
  const legacy=raw();delete legacy.operations[0].attemptedAfterMs;
  const observed=replay(input(legacy,100));assert.equal(observed.operations.failed,1);assert.equal(observed.recordAccounting.operationTimeFallback,1);
});
test('R1/R2: actual CLI refuses impossible offset and reports pending/future offsets without invented failure',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'gonvex-monitor-offset-'));
  const run=async data=>{const file=join(directory,'input.json');await writeFile(file,JSON.stringify(data));return spawnSync(process.execPath,[fileURLToPath(new URL('../cli.mjs',import.meta.url)),'replay',file],{encoding:'utf8',timeout:5000});};
  try {
    const impossible=raw();impossible.finishedAt=5;const rejected=await run(input(impossible,10));assert.equal(rejected.status,1);assert.equal(rejected.stdout,'');assert.equal(rejected.stderr,'monitor-input-integrity\n');
    for(const [cutoff,attempted,failed,pending] of [[10,0,0,0],[85,1,0,1],[90,1,1,0],[100,1,1,0]]) {
      const response=await run(input(raw(),cutoff));assert.equal(response.status,0);
      const report=JSON.parse(response.stdout);assert.deepEqual(report.operations,{attempted,success:0,failed});assert.equal(report.recordAccounting.operationsPending,pending);
    }
  }finally{await rm(directory,{recursive:true,force:true});}
});
