import {test} from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,rm,access} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {runBounded} from './deploy.mjs';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
test('at the deadline the whole process group is stopped, SIGTERM-ignoring members are SIGKILLed, and nothing writes after the result',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gonvex-bounded-'));
 try{
  const marker=join(dir,'late-write');
  // The "upload": a grandchild in the same group, also ignoring SIGTERM, that writes well after the deadline.
  const grandchild=`process.on('SIGTERM',()=>{});setTimeout(()=>require('fs').writeFileSync(${JSON.stringify(marker)},'x'),1500);setInterval(()=>{},1000);`;
  const parent=`process.on('SIGTERM',()=>{console.log('term ignored')});require('child_process').spawn(process.execPath,['-e',${JSON.stringify(grandchild)}],{stdio:'inherit'});setInterval(()=>{},1000);`;
  const started=Date.now();
  const result=await runBounded(process.execPath,['-e',parent],{deadline:Date.now()+300,graceMs:400});
  assert.equal(result.killed,true);
  assert.equal(result.signal,'SIGKILL');
  assert.match(result.output,/term ignored/);
  assert.match(result.output,/SIGTERM to process group[\s\S]*SIGKILL to process group/);
  assert.ok(Date.now()-started>=700,'settles only after the grace period');
  await sleep(1800);
  await assert.rejects(access(marker),{code:'ENOENT'},'no write may happen after the result');
 }finally{await rm(dir,{recursive:true,force:true});}
});
test('a command that finishes before its deadline is not signalled',async()=>{
 const result=await runBounded(process.execPath,['-e','console.log("done")'],{deadline:Date.now()+10_000,graceMs:100});
 assert.deepEqual([result.code,result.signal,result.killed],[0,null,false]);assert.match(result.output,/done/);
});
