import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shardEpoch } from './shard-epoch.mjs';
test('all shards share an atomically published epoch after the last participant arrives',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gv-epoch-'));
 try{
  const root=join(dir,'case');const floor=Date.now()+1000;
  const epochs=await Promise.all(Array.from({length:8},(_,k)=>shardEpoch(root,'start',k,8,floor)));
  assert.equal(new Set(epochs).size,1);assert.ok(epochs[0]>=floor);
  const plateau=await Promise.all(Array.from({length:8},(_,k)=>shardEpoch(root,'plateau',k,8,floor+900000)));
  assert.equal(new Set(plateau).size,1);assert.ok(plateau[0]>=floor+900000);
  await assert.rejects(()=>shardEpoch(root,'start',0,8,floor));
  await assert.rejects(()=>shardEpoch(root,'missing',0,2,0,1),/timed out/);
 }finally{await rm(dir,{recursive:true,force:true});}
});
