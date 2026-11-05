import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';

test('merged common plateau excludes ramp errors and samples but retains them separately', () => {
  const dir = mkdtempSync(join(tmpdir(), 'gv-merge-'));
  try {
    const out = join(dir, 'result');
    for (let k = 0; k < 2; k++) writeFileSync(`${out}.shard-${k}`, JSON.stringify({
      targetUsers: 1, connectedUsers: 1, steadyStartMs: 1000 + k * 100, steadyEndMs: 2000,
      errors: { refused: 1 }, errorEvents: [{ at: 1050, key: 'refused' }],
      rawSamples: { reducer: [{ at: 1200, ms: 100 }], save: [{ at: 1050, ms: 9000 }, { at: 1200, ms: 100 }] },
      collectionReadiness: { readyAtTeardown: 1, notReadyAtTeardown: 0 },
      visibilityChecks: { checked: 1, mismatched: 0 }, client: { kind: 'thin' },
    }));
    execFileSync(process.execPath, [new URL('./merge-shards.mjs', import.meta.url).pathname, out, '2']);
    let r = JSON.parse(readFileSync(out));
    assert.equal(r.steadyStartMs, 1100); assert.equal(r.steadyLatencyMs.save.p99, 100);
    assert.equal(r.rampLatencyMs.save.p99, 9000); assert.deepEqual(r.steadyErrors, {});
    assert.deepEqual(r.rampErrors, { refused: 2 }); assert.deepEqual(r.errors, { refused: 2 });
    assert.equal(r.collectionReadiness.readyAtTeardown, 2); assert.equal(r.errorRate, 0);
    const p = JSON.parse(readFileSync(`${out}.shard-1`));
    p.collectionReadiness = { readyAtTeardown: 0, notReadyAtTeardown: 1 };
    writeFileSync(`${out}.shard-1`, JSON.stringify(p));
    execFileSync(process.execPath, [new URL('./merge-shards.mjs', import.meta.url).pathname, out, '2']);
    r = JSON.parse(readFileSync(out)); assert.equal(r.clientSloPassed, false);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('merged result rejects artifact drift and ramp-origin delivery loss even with fast steady samples',()=>{
 const dir=mkdtempSync(join(tmpdir(),'gv-merge-guards-'));
 try{
  const out=join(dir,'result');
  const base={targetUsers:1,connectedUsers:1,steadyStartMs:1000,steadyEndMs:2000,errorEvents:[],errors:{},artifactPins:{sdk:'same'},collectionReadiness:{readyAtTeardown:1},visibilityChecks:{mismatched:0},client:{kind:'thin'},rawSamples:Object.fromEntries(['save','heartbeat','reducer','delivery','taskDelivery'].map(k=>[k,[{at:1200,ms:20}]]))};
  for(const fault of ['none','artifact','missing','pending','visibility']){
   for(let k=0;k<2;k++){
    const p=structuredClone(base);p.visibilityComparisons=[{principal:{identity:`synthetic-${k}`},verdict:{qualified:fault!=='visibility',passed:fault!=='visibility'}}];
    if(k===1&&fault==='artifact')p.artifactPins.sdk='other';
    if(k===1&&fault==='missing')p.taskExpectations={undeliveredOps:1};
    if(k===1&&fault==='pending')p.operationAttempts=[{at:10,done:false}];
    if(k===1&&fault==='visibility')p.visibilityChecks.mismatched=1;
    writeFileSync(`${out}.shard-${k}`,JSON.stringify(p));
   }
   execFileSync(process.execPath,[new URL('./merge-shards.mjs',import.meta.url).pathname,out,'2']);
   const result=JSON.parse(readFileSync(out));assert.equal(result.clientSloPassed,fault==='none',fault);assert.deepEqual(result.visibilityComparisons.map(x=>x.principal.identity),['synthetic-0','synthetic-1']);
  }
 }finally{rmSync(dir,{recursive:true,force:true});}
});
