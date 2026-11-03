/** Durable inactive receipts for Node SDK load clients, outside the server
 * budget. Pages in one event-loop batch share a sync; no page resolves early. */
import { mkdir, open } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';

export function fileSnapshotStaging(directory, {openFile=open}={}) {
  const root=resolve(directory);
  const totals={pages:0,bytes:0,fsyncs:0,errors:0,clients:0,clientsWithPages:0,
    batches:0,largestBatchPages:0,activeSnapshots:0,peakOpenSnapshots:0,
    stageMsTotal:0,stageMsMax:0,syncMsTotal:0,syncMsMax:0};
  const pending=[];
  let file,initialized,flushing,failure,closing=false,directorySynced=false;
  const sync=async handle=>{
    const at=performance.now();await handle.sync();const ms=performance.now()-at;
    totals.fsyncs++;totals.syncMsTotal+=ms;totals.syncMsMax=Math.max(totals.syncMsMax,ms);
  };
  const initialize=()=>initialized??=(async()=>{
    await mkdir(root,{recursive:true,mode:0o700});
    const parent=await openFile(dirname(root),'r');
    try{await sync(parent);}finally{await parent.close();}
    file=await openFile(join(root,'pages.jsonl'),'wx',0o600);
  })();
  const schedule=()=>{
    if(flushing)return;
    let done;flushing=new Promise(resolve=>{done=resolve;});
    setImmediate(async()=>{
      try{
        while(pending.length){
          const batch=pending.splice(0);
          try{
            await initialize();
            await file.writeFile(Buffer.concat(batch.map(entry=>entry.payload)));
            await sync(file);
            if(!directorySynced){
              const folder=await openFile(root,'r');
              try{await sync(folder);directorySynced=true;}finally{await folder.close();}
            }
            totals.batches++;totals.largestBatchPages=Math.max(totals.largestBatchPages,batch.length);
            for(const entry of batch)entry.commit();
          }catch(error){
            failure=error;
            for(const entry of [...batch,...pending.splice(0)])entry.reject(error);
          }
        }
      }finally{flushing=undefined;done();}
    });
  };
  return {
    stats:()=>({...totals}),
    async close(){closing=true;while(flushing)await flushing;await file?.close();file=undefined;},
    client(){
      const client=totals.clients++;
      const stats={pages:0,bytes:0},streams=new Map();
      const keyFor=(scope,id)=>JSON.stringify([scope,id]);
      const retire=stream=>{if(stream.active){stream.active=false;totals.activeSnapshots--;}};
      return {stats,staging:{
        stage(scope,page){
          const at=performance.now(),key=keyFor(scope,page.snapshotId);
          let stream=streams.get(key);
          if(!stream){stream={next:0,closed:false,active:false};streams.set(key,stream);}
          if(failure||closing||stream.closed||page.sequence!==stream.next){
            totals.errors++;return Promise.reject(failure||Error('snapshot-staging-sequence-or-closed'));
          }
          stream.next++;stream.closed=page.last===true;
          if(!stream.active){stream.active=true;totals.activeSnapshots++;totals.peakOpenSnapshots=Math.max(totals.peakOpenSnapshots,totals.activeSnapshots);}
          const payload=Buffer.from(JSON.stringify({client,scope,page})+'\n');
          const result=new Promise((resolve,reject)=>{
            pending.push({payload,
              commit(){
                if(stats.pages===0)totals.clientsWithPages++;
                stats.pages++;stats.bytes+=payload.byteLength;
                totals.pages++;totals.bytes+=payload.byteLength;
                const ms=performance.now()-at;totals.stageMsTotal+=ms;totals.stageMsMax=Math.max(totals.stageMsMax,ms);
                if(page.last)retire(stream);
                resolve();
              },
              reject(error){totals.errors++;stream.closed=true;retire(stream);reject(error);},
            });
          });
          stream.pending=result.catch(()=>{});schedule();return result;
        },
        async discard(scope,id){
          const stream=streams.get(keyFor(scope,id));if(!stream)return;
          stream.closed=true;await stream.pending;retire(stream);
          // Retain the receipt and closed identity until the run ends. Neither
          // staged pages nor this log ever supplies LocalReplica/query reads.
        },
      }};
    },
  };
}

/** In-memory receipts for SDK oracle sessions: the real SDK still stages,
 * verifies digests and activates snapshots, but nothing waits on disk sync.
 * Oracle sessions check correctness; their fsync time is generator cost, not
 * server cost, and made audits overrun the hold at 200 users. */
export function memorySnapshotStaging() {
  const totals = { pages: 0, bytes: 0, clients: 0 };
  return {
    stats: () => ({ ...totals }),
    async close() {},
    client() {
      totals.clients++;
      const stats = { pages: 0, bytes: 0 }, next = new Map();
      return { stats, staging: {
        stage(scope, page) {
          const key = JSON.stringify([scope, page.snapshotId]), expected = next.get(key) ?? 0;
          if (page.sequence !== expected) return Promise.reject(Error('snapshot-staging-sequence'));
          next.set(key, page.last ? Infinity : expected + 1);
          stats.pages++; totals.pages++;
          return Promise.resolve();
        },
        async discard(scope, id) { next.delete(JSON.stringify([scope, id])); },
      } };
    },
  };
}
