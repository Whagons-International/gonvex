import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readFile,rm}from'node:fs/promises';
import {tmpdir}from'node:os';import{join}from'node:path';import{spawn}from'node:child_process';import{once}from'node:events';
import{stopWorkers}from'./stop-workers.mjs';
test('coordinated SIGTERM preserves all eight real Node worker receipts and error/pending state',async()=>{
 const d=await mkdtemp(join(tmpdir(),'worker-stop-')),children=[];
 try{
  const fixture=join(d,'worker.mjs');await writeFile(fixture,`import{workerReceipt}from ${JSON.stringify(new URL('./worker-receipt.mjs',import.meta.url).href)};const path=process.argv[2];process.on('SIGTERM',()=>{workerReceipt(path,{workerPid:process.pid,stoppedAt:Date.now(),aborted:true,errors:{observed:1},operationAttempts:[{done:false}]},{durable:true});});process.send('ready');setInterval(()=>{},1000);`);
  const workers=[];for(let i=0;i<8;i++){const path=join(d,'partial-'+i);const c=spawn(process.execPath,[fixture,path],{stdio:['ignore','ignore','pipe','ipc']});children.push(c);await once(c,'message');workers.push({pid:c.pid,partial:path});}
  const result=await stopWorkers(workers);assert.equal(result.complete,true);assert.equal(result.acknowledged.length,8);
  for(const w of workers){const r=JSON.parse(await readFile(w.partial));assert.equal(r.errors.observed,1);assert.equal(r.operationAttempts[0].done,false);}
 }finally{for(const c of children)c.kill('SIGKILL');await rm(d,{recursive:true,force:true});}
});
