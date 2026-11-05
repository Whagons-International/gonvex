/** Identical protocol histories through the installed SDK's public transport and
 * the harness driver. SDK artifact is supplied explicitly by the bounded runner. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { ThinClient, RowPool } from './thin-client.mjs';
import { ThinCheckpointStore, integrity } from './thin-checkpoint.mjs';
const entry=process.env.LOAD_TEST_SDK;
const turn=()=>new Promise(r=>setTimeout(r,5));
async function until(fn){for(let i=0;i<200;i++){if(fn())return;await turn();}throw Error('SDK fixture did not settle');}
class Socket {
 static OPEN=1;static CONNECTING=0;static instances=[];
 constructor(){this.readyState=0;this.events=new Map();this.sent=[];Socket.instances.push(this);}
 addEventListener(t,fn){const xs=this.events.get(t)||[];xs.push(fn);this.events.set(t,xs);}
 removeEventListener(t,fn){this.events.set(t,(this.events.get(t)||[]).filter(x=>x!==fn));}
 emit(t,event={}){for(const fn of this.events.get(t)||[])fn(event);}
 send(text){this.sent.push(JSON.parse(text));}
 open(){this.readyState=1;this.emit('open');}
 receive(m){this.emit('message',{data:JSON.stringify(m)});}
 close(){this.readyState=3;this.emit('close');}
}
const directive={protocolVersion:1,scope:'tenant',visibilityScope:'visible-p',epoch:'module-v1'};
const ref={kind:'query',path:'tasks.all',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','value'],maxRows:20000}};
const cursor=n=>({epoch:'data-v1',revision:n});
const digest=rows=>{const p=new RowPool();return integrity(new Map(rows.map(r=>[r.id,p.acquireValue(r)])),ref.replica.columns).digest;};
const opens=s=>s.sent.flatMap(m=>m.type==='replica.openMany'?m.opens:m.type==='replica.open'?[m]:[]);
const normalized=m=>{const {id,...rest}=m;return rest;};

test('installed SDK and cached thin fresh instances agree on rows, cursor/digest opens and integrity repair', {skip:!entry},async()=>{
 const Native=globalThis.WebSocket;globalThis.WebSocket=Socket;
 const {GonvexClient,MemoryLocalReplicaStorage}=await import(pathToFileURL(entry));
 const stores=[new MemoryLocalReplicaStorage(),new ThinCheckpointStore()],clients=[];
 const start=async thin=>{
  const c=thin?new ThinClient('ws://127.0.0.1:1/ws',{project:'p',checkpointStore:stores[1],checkpointIdentity:'u',modulePin:'m'}):new GonvexClient('ws://127.0.0.1:1/ws',{project:'p',token:'synthetic',tenant:'t',outbox:{enabled:false},localReplica:{storage:stores[0]}});
  clients.push(c);if(thin)c.auth={token:'synthetic',tenant:'t'};
  c.connect();const socket=Socket.instances.at(-1);socket.open();
  socket.receive({type:'session.ready',replica:directive,capabilities:{replicaResume:1,replicaBatch:1,replicaSnapshotPages:1,replicaBatchFrames:1,replicaWatermark:1}});
  await until(()=>socket.sent.some(m=>m.type==='auth'));const auth=socket.sent.find(m=>m.type==='auth');
  socket.receive({type:'auth.result',id:auth.id,result:{replica:directive}});await turn();
  const w=c.watchReplica(ref,{b:2,a:1});w.onUpdate(()=>{try{w.localReplicaResult();}catch{}});
  await until(()=>opens(socket).length>0);return {c,socket,w,id:opens(socket)[0].id};
 };
 try{
  const a=await start(false),b=await start(true);
  assert.deepEqual(normalized(opens(a.socket)[0]),normalized(opens(b.socket)[0]));
  const rows=[{id:'a',value:'A'},{id:'old',value:'revoke'}];
  for(const x of [a,b]){
   x.socket.receive({type:'replica.snapshot',id:x.id,key:'id',result:rows,cursor:cursor(1)});
   x.socket.receive({type:'replica.ready',id:x.id,cursor:cursor(1),digest:digest(rows)});
  }
  await until(()=>a.w.status().isUpToDate&&b.w.status().isUpToDate);
  for(const x of [a,b]){
   x.socket.receive({type:'replica.delta',id:x.id,cursor:cursor(2),upserts:[{id:'a',value:'A2'},{id:'new',value:'N'}],deleted:['old']});
   x.socket.receive({type:'replica.ready',id:x.id,cursor:cursor(2),digest:digest([{id:'a',value:'A2'},{id:'new',value:'N'}])});
  }
  await until(()=>a.w.status().isUpToDate&&a.w.localReplicaResult()?.[0]?.value==='A2');
  assert.deepEqual(a.w.localReplicaResult(),b.w.localReplicaResult());a.c.close();b.c.close();
  const warmSdk=await start(false),warmThin=await start(true);
  assert.deepEqual(normalized(opens(warmSdk.socket)[0]),normalized(opens(warmThin.socket)[0]));
  assert.deepEqual(opens(warmThin.socket)[0].cursor,cursor(2));
  assert.deepEqual(warmSdk.w.localReplicaResult(),warmThin.w.localReplicaResult());
  for(const x of [warmSdk,warmThin])x.socket.receive({type:'replica.needHashes',id:x.id});
  await until(()=>opens(warmSdk.socket).length===2&&opens(warmThin.socket).length===2);
  assert.deepEqual(normalized(opens(warmSdk.socket)[1]),normalized(opens(warmThin.socket)[1]));
  // Server pruned the cursor: both must stop advertising rows/cursor on retry.
  for(const x of [warmSdk,warmThin])x.socket.receive({type:'replica.reset',id:x.id,reason:'retention-expired'});
  await until(()=>opens(warmSdk.socket).length>=3&&opens(warmThin.socket).length>=3);
  assert.deepEqual(normalized(opens(warmSdk.socket).at(-1)),normalized(opens(warmThin.socket).at(-1)));
  const pageRows=[{id:'a',value:'paged-A'},{id:'b',value:'paged-B'}];
  for(const x of [warmSdk,warmThin]){
   const id=opens(x.socket).at(-1).id;
   for(let sequence=0;sequence<2;sequence++)x.socket.receive({type:'replica.snapshotPage',id,path:ref.path,snapshotId:'paged',key:'id',scope:directive.scope,visibilityScope:directive.visibilityScope,cursor:cursor(4),sequence,last:sequence===1,result:[pageRows[sequence]],...(sequence===1?{digest:digest(pageRows)}:{})});
   x.socket.receive({type:'replica.ready',id,cursor:cursor(4),digest:digest(pageRows)});
  }
  await until(()=>warmSdk.w.status().isUpToDate&&warmSdk.w.localReplicaResult()?.[0]?.value==='paged-A');
  assert.deepEqual(warmSdk.w.localReplicaResult(),warmThin.w.localReplicaResult());
  const finalRows=[{id:'a',value:'transaction-A'},{id:'c',value:'added-C'}];
  for(const x of [warmSdk,warmThin]){
   const id=opens(x.socket).at(-1).id;
   x.socket.receive({type:'replica.batch',tenant:'t',class:'0',from:4,items:[
    {type:'replica.transaction',cursor:cursor(5),changes:[{entity:'tasks',id:'a',operation:'update',newValue:finalRows[0]}]},
    {type:'replica.stream',id,stream:'class-stream'},
    {type:'replica.delta',id:'class-stream',cursor:cursor(5),upserts:finalRows,deleted:['b']},
    {type:'replica.ready',id:'class-stream',cursor:cursor(5),digest:digest(finalRows)},
   ],watermark:5});
  }
  try{await until(()=>warmSdk.w.status().isUpToDate&&warmSdk.w.localReplicaResult()?.[0]?.value==='transaction-A');}catch(e){throw Error(JSON.stringify({sdk:warmSdk.w.status(),sdkRows:warmSdk.w.localReplicaResult(),thin:warmThin.w.status(),thinRows:warmThin.w.localReplicaResult(),opens:opens(warmSdk.socket),cacheFailures:ThinClient.stats.checkpointFailures}));}
  assert.deepEqual(warmSdk.w.localReplicaResult(),warmThin.w.localReplicaResult());
  warmSdk.c.close();warmThin.c.close();
  const lastSdk=await start(false),lastThin=await start(true);
  assert.deepEqual(lastSdk.w.localReplicaResult(),lastThin.w.localReplicaResult());
  assert.deepEqual(normalized(opens(lastSdk.socket)[0]),normalized(opens(lastThin.socket)[0]));

 }finally{for(const c of clients)c.close();stores[1].clear();globalThis.WebSocket=Native;}
});
