// Bundled against the exact candidate artifact by the lane harness.
import {GonvexClient,IndexedDBEngineReplicaStorage} from '../../dist/index.js';
import {createBrowserStarterInflater} from '../../dist/starter-browser.js';
export async function run(url,token,mode,emit,line) {
 const assert=(ok,msg)=>{if(!ok)throw new Error(msg);};
 const sent=[],received=[],waiters=[];const Native=globalThis.WebSocket;
 class Socket extends Native {
  constructor(url){super(url);this.addEventListener('message',e=>{try{received.push(JSON.parse(String(e.data)));}catch{}});}
  send(data){const m=JSON.parse(String(data));sent.push(m);if(mode==='revoke-before-piece'&&m.type==='replica.starterPiece'){void(async()=>{await emit('OFFERED');assert(await line()==='CONTINUE','revoke handshake');super.send(data);})();return;}super.send(data);for(const done of waiters.splice(0))done(m);}
 }
 globalThis.WebSocket=Socket;
 const databaseName='starter-browser-'+crypto.randomUUID();
 const storage=new IndexedDBEngineReplicaStorage(databaseName);
 const inflate=await createBrowserStarterInflater();assert(inflate,'native bounded decoder available');
 let client,stop,staged=0,previousReady,resolveReady,rejectReady;
 let ready;const reset=()=>{ready=new Promise((r,j)=>{resolveReady=r;rejectReady=j;});};reset();
 const observer=m=>{if(m.type==='replica.ready')resolveReady(m);if(m.type==='replica.error')rejectReady(new Error(m.error));};
 const ref={kind:'query',path:'tasks.all',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','body','rank'],maxRows:10000}};
 const waitReady=()=>Promise.race([ready,new Promise((_,j)=>setTimeout(()=>j(new Error('ready timeout '+JSON.stringify({sent:sent.map(m=>m.type),received:received.map(m=>m.type)}))),20000))]);
 try {
  client=new GonvexClient(url,{project:'starter-wire',tenant:'tenant',token,replicaStarterFiles:mode!=='old-client',localReplica:{storage},replicaSubscriptionRetentionMs:0,replicaOpenBatchMs:0,starterInflater:async(bytes,limits)=>{
   const raw=await inflate(bytes,limits);staged++;
   const offer=received.filter(m=>m.type==='replica.starterOffer').at(-1);
   const active=Object.values(client.localReplica.snapshot().liveQueries),durable=Object.values((await storage.load(offer.visibilityScope))?.liveQueries??{});
   if(previousReady){assert(active[0]?.cursor.revision===previousReady.cursor.revision,'active stays prior until ready');assert(durable[0]?.cursor.revision===previousReady.cursor.revision,'IDB stays prior until ready');}
   else {assert(active.length===0,'no active staged window');assert(durable.length===0,'no durable staged checkpoint');}
   await emit('STAGED');assert(await line()==='CONTINUE','staged handshake');return raw;
  }});
  stop=client.subscribeReplica(ref,{},observer);const first=await waitReady();
  await new Promise(r=>setTimeout(r,0));
  const rows=client.localReplica.entityRows('tasks');assert(JSON.stringify(rows.map(r=>r.id).sort())===JSON.stringify(mode==='revoke-before-piece'?['c']:['a','c']),'exact authorized set');assert(!JSON.stringify(rows).includes('private-secret'),'no private columns');
  if(mode==='bootstrap') {
   assert(staged===1,'actual Rust compressed piece decoded');assert(!received.some(m=>m.type==='replica.snapshot'||m.type==='replica.snapshotPage'),'bootstrap has no normal snapshot');
   const offer=received.find(m=>m.type==='replica.starterOffer');assert(first.cursor.revision>offer.cursor.revision,'final ready catches up');
   const durable=await storage.load(offer.visibilityScope);assert(Object.values(durable.liveQueries)[0].cursor.revision===first.cursor.revision,'durable final cursor');
   // A second storage instance reads the committed IndexedDB transaction.
   const reopened=new IndexedDBEngineReplicaStorage(databaseName);
   try {const persisted=await reopened.load(offer.visibilityScope);assert(Object.values(persisted.liveQueries)[0].cursor.revision===first.cursor.revision,"independent durable IDB read");}finally{reopened.close();}
   previousReady=first;await emit('READY');assert(await line()==='RESUME','resume handshake');
   const closing=new Promise(r=>waiters.push(m=>{if(m.type==='replica.close')r();}));stop();await closing;
   reset();stop=client.subscribeReplica(ref,{},observer);previousReady=await waitReady();
   const lastOpen=()=>sent.filter(m=>m.type==='replica.open').at(-1)??sent.filter(m=>m.type==='replica.openMany').at(-1)?.opens.at(-1);
   assert(lastOpen().cursor.revision===first.cursor.revision && !lastOpen().starter && staged===1,'normal cursor resume');await emit('RESUMED');
   await emit('RETENTION');assert(await line()==='OPEN_OLD','retention handshake');
   const closed=new Promise(r=>waiters.push(m=>{if(m.type==='replica.close')r();}));stop();await closed;
   reset();stop=client.subscribeReplica(ref,{},observer);const retained=await waitReady();
   assert(!lastOpen().starter && lastOpen().cursor.revision===previousReady.cursor.revision,'normal stale cursor open');assert(retained.cursor.revision>previousReady.cursor.revision && staged===2,'retention bootstrap');await emit('RETENTION_BOOTSTRAPPED');
  } else if(mode==='revoke-before-piece') {
   assert(staged===0 && !received.some(m=>m.type==='replica.starterPiece'),'revocation before bytes');assert(received.some(m=>m.type==='replica.starterFallback'),'single normal fallback');
  } else {assert(staged===0 && !received.some(m=>m.type==='replica.starterOffer'),'old peer fallback');await emit('FALLBACK');}
  await emit('RESULT '+JSON.stringify({mode,sentTypes:sent.map(m=>m.type),receivedTypes:received.map(m=>m.type),rows:rows.length,cursor:first.cursor,starterStats:client.starterBootstrapStats(),storage:'native IndexedDB'}));
 }finally{stop?.();client?.close();storage.close();globalThis.WebSocket=Native;}
}
