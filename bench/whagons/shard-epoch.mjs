import { writeFile, readFile, rename, access } from 'node:fs/promises';
const pause = ms => new Promise(r=>setTimeout(r,ms));
async function exists(path) { try { await access(path); return true; } catch { return false; } }
// Publish via rename so no shard can read a partially written epoch.
export async function shardEpoch(root, phase, shard, shards, notBefore = 0, timeoutMs = 120000) {
  const base = `${root}.${phase}`, until = Date.now()+timeoutMs;
  await writeFile(`${base}.ready-${shard}`,String(Date.now()),{flag:'wx'});
  while (!(await Promise.all(Array.from({length:shards},(_,k)=>exists(`${base}.ready-${k}`)))).every(Boolean)) {
    if(Date.now()>until)throw Error(`${phase} shard barrier timed out`);await pause(20);
  }
  if(shard===0){
    const epoch = Math.max(Date.now(),notBefore);
    await writeFile(base+'.epoch.tmp',String(epoch));await rename(base+'.epoch.tmp',base+'.epoch');
  }
  while(!await exists(base+'.epoch')){if(Date.now()>until)throw Error(`${phase} epoch missing`);await pause(20);}
  const epoch=Number(await readFile(base+'.epoch','utf8'));
  if(!Number.isSafeInteger(epoch)||epoch<=0)throw Error('Invalid shard epoch');return epoch;
}
