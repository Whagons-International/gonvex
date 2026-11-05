import {readFile,writeFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
export async function stopWorkers(workers,{graceMs=10000}={}){
 const requestedAt=Date.now(),pending=new Set(workers.map((_,i)=>i)),acks=[];
 for(const w of workers)try{process.kill(w.pid,'SIGTERM');}catch(e){if(e.code!=='ESRCH')throw e;}
 const deadline=requestedAt+graceMs;
 while(pending.size&&Date.now()<deadline){
  for(const i of pending){const w=workers[i];try{
   const receipt=JSON.parse(await readFile(w.partial,'utf8'));
   if(receipt.aborted&&receipt.stoppedAt>=requestedAt&&receipt.workerPid===w.pid){acks.push({worker:i,pid:w.pid,partial:w.partial,stoppedAt:receipt.stoppedAt});pending.delete(i);}
  }catch(e){if(!['ENOENT'].includes(e.code)&&!(e instanceof SyntaxError))throw e;}}
  if(pending.size)await new Promise(r=>setTimeout(r,25));
 }
 // Kill only these owned Node PIDs after durable acknowledgements or timeout.
 for(const w of workers)try{process.kill(w.pid,'SIGKILL');}catch(e){if(e.code!=='ESRCH')throw e;}
 return {requestedAt,acknowledged:acks,missing:[...pending],complete:!pending.size};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 const workers=JSON.parse(await readFile(process.argv[2],'utf8'));
 const result=await stopWorkers(workers);await writeFile(process.argv[3],JSON.stringify(result,null,2));if(!result.complete)process.exitCode=2;
}
