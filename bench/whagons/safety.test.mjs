import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loopback, quantiles } from './safety.mjs';
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
