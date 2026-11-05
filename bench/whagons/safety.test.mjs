import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loopback, quantiles, passRule, GATED_KINDS } from './safety.mjs';
test('reject external destinations, DNS names, disguised credentials and alternate IP spellings',()=>{
  for(const u of ['https://example.com','http://65.109.93.58','http://app.whagons.com','http://localhost','http://127.0.0.1.example.com','http://user@127.0.0.1','file:///tmp/test','http://127.1','http://2130706433']) assert.throws(()=>loopback(u));
  assert.equal(loopback('http://127.0.0.1:8080').port,'8080');
});
test('nearest-rank quantiles and absent samples',()=>{
 assert.deepEqual(quantiles([]),{p50:null,p95:null,p99:null});
 assert.deepEqual(quantiles([4,1,3,2]),{p50:2,p95:4,p99:4});
});
test('traffic aggregation emits counts and rates without account or tenant identifiers',async()=>{
 const {mkdtemp,writeFile,rm}=await import('node:fs/promises');
 const {tmpdir}=await import('node:os');
 const {join}=await import('node:path');
 const {execFileSync}=await import('node:child_process');
 const dir=await mkdtemp(join(tmpdir(),'gonvex-load-aggregate-'));
 try {
  const file=join(dir,'synthetic.csv');
  await writeFile(file,'created_at,account_id,tenant_id,path\n'+[
   '2026-01-01T00:00:00Z,private-account-marker,private-tenant-marker,presence.beat',
   '2026-01-01T00:00:05Z,private-account-marker,private-tenant-marker,tasks.create',
   '2026-01-01T00:00:15Z,private-account-marker,private-tenant-marker,tasks.create',
   '2026-01-01T00:00:30Z,private-account-marker,private-tenant-marker,presence.beat',
  ].join('\n'));
  const output=execFileSync('python3',[new URL('./aggregate.py',import.meta.url).pathname,file],{encoding:'utf8'});
  const result=JSON.parse(output);assert.equal(result.traces,4);assert.equal(result.accounts,1);
  assert.equal(result.path_counts['tasks.create'],2);
  assert.doesNotMatch(output,/private-account-marker|private-tenant-marker|synthetic\.csv/);
 } finally {await rm(dir,{recursive:true,force:true});}
});
test('challenge pass rule: p95/p99 bounds, error rate and plateau-optional kinds',()=>{
 const fast=Object.fromEntries(GATED_KINDS.filter(k=>!['query','login'].includes(k)).map(k=>[k,{samples:100,p95:100,p99:200}]));
 const base={latencyMs:fast,errors:{},connectedUsers:5,targetUsers:5,aborted:false};
 assert.equal(passRule(base,{}).passed,true,'query/login may be absent from the plateau');
 assert.deepEqual(passRule({...base,latencyMs:{...fast,save:{samples:9,p95:499,p99:1000}}},{}).failingKinds,['save']);
 assert.deepEqual(passRule({...base,latencyMs:{...fast,delivery:{samples:0}}},{}).failingKinds,['delivery']);
 assert.equal(passRule({...base,errors:{x:2}},{}).passed,false,'2 failures in 102 operations is above 1%');
 assert.equal(passRule({...base,errors:{x:1}},{}).passed,true,'1 failure in 101 operations is below 1%');
 assert.equal(passRule({...base,connectedUsers:4},{}).passed,false);
});


test('heartbeat samples are optional only when all target users negotiated presence v2', () => {
 const latencyMs=Object.fromEntries(GATED_KINDS.filter(k=>!['heartbeat','query','login'].includes(k)).map(k=>[k,{samples:100,p95:100,p99:200}]));
 const base={latencyMs,errors:{},connectedUsers:10,targetUsers:10,aborted:false};
 assert.ok(passRule(base).failingKinds.includes('heartbeat'));
 assert.ok(passRule({...base,presenceV2Users:9}).failingKinds.includes('heartbeat'));
 assert.equal(passRule({...base,presenceV2Users:10}).passed,true);
 const noDelivery={...latencyMs,delivery:{samples:0}};
 assert.equal(passRule({...base,presenceV2Users:10,latencyMs:noDelivery}).passed,false);
});
