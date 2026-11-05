import assert from 'node:assert/strict';
import {createInterface} from 'node:readline';
import {pathToFileURL} from 'node:url';
const artifact=process.env.STARTER_SDK_CLIENT_ARTIFACT;
if(!artifact)throw new Error('STARTER_SDK_CLIENT_ARTIFACT must name the compiled candidate client dist directory');
const {GonvexClient,MemoryLocalReplicaStorage}=await import(pathToFileURL(artifact+'/index.js').href);
const {nodeStarterInflater}=await import(pathToFileURL(artifact+'/node-starter-inflater.js').href);
const [url,token,mode='bootstrap']=process.argv.slice(2);
const sent=[],received=[],waiters=[];
const Native=globalThis.WebSocket;
class Socket extends Native {
  constructor(url){super(url);this.addEventListener('message',e=>{try{received.push(JSON.parse(String(e.data)));}catch{}});}
  send(data){const m=JSON.parse(String(data));sent.push(m);if(mode==='revoke-before-piece'&&m.type==='replica.starterPiece'){console.log('OFFERED');void line().then(l=>{assert.equal(l,'CONTINUE');super.send(data);});return;}super.send(data);for(const done of waiters.splice(0))done(m);}
}
globalThis.WebSocket=Socket;
const lines=[],lineWait=[];const rl=createInterface({input:process.stdin});rl.on('line',l=>{if(lineWait.length)lineWait.shift()(l);else lines.push(l);});
const line=()=>lines.length?Promise.resolve(lines.shift()):new Promise(r=>lineWait.push(r));
const benchmarking=mode.startsWith('benchmark-');let promotionNs=0,codecNs=0,cryptoCalls=0,cryptoInputBytes=0;
// Count actual native WebCrypto calls in BOTH frozen A-only and candidate SDKs.
// No row contents retained; the same wrapper overhead applies to both cases.
const subtle=globalThis.crypto.subtle,nativeDigest=subtle.digest.bind(subtle);
if(benchmarking)subtle.digest=(algorithm,data)=>{cryptoCalls++;cryptoInputBytes+=data.byteLength;return nativeDigest(algorithm,data);};
class Storage extends MemoryLocalReplicaStorage {async promoteStarterWindow(...args){const t=process.hrtime.bigint();try{return await super.promoteStarterWindow(...args);}finally{promotionNs+=Number(process.hrtime.bigint()-t);}}}
const storage=new Storage();let staged=0,client,previousReady;const start=process.hrtime.bigint(),beforeUsage=process.resourceUsage(),beforeMemory=process.memoryUsage();
const ref={kind:'query',path:'tasks.all',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','body','rank'],maxRows:10000}};
let readyResolve,readyReject;let ready=new Promise((r,j)=>{readyResolve=r;readyReject=j;});
const observer=m=>{if(m.type==='replica.ready')readyResolve(m);if(m.type==='replica.error')readyReject(new Error(m.error));};
let stop;
try {
 client=new GonvexClient(url,{project:'starter-wire',tenant:'tenant',token,replicaStarterFiles:mode!=='old-client'&&mode!=='benchmark-normal',starterInflater:async(bytes,limits)=>{
   const codecStart=process.hrtime.bigint();const raw=await nodeStarterInflater(bytes,limits);codecNs+=Number(process.hrtime.bigint()-codecStart);staged++;
   const offer=received.filter(x=>x.type==='replica.starterOffer').at(-1);
   const active=Object.values(client.localReplica.snapshot().liveQueries), durable=Object.values((await storage.load(offer.visibilityScope))?.liveQueries??{});
   if(previousReady){assert.equal(active[0]?.cursor.revision,previousReady.cursor.revision);assert.equal(durable[0]?.cursor.revision,previousReady.cursor.revision);assert(offer.cursor.revision>previousReady.cursor.revision);}
   else {assert.equal(active.length,0,'staging leaked a usable window');assert.equal(durable.length,0,'staging persisted a usable checkpoint');}
   if(!benchmarking){console.log('STAGED');assert.equal(await line(),'CONTINUE');}return raw;
 },localReplica:{storage},replicaSubscriptionRetentionMs:0,replicaOpenBatchMs:0});
 stop=client.subscribeReplica(ref,{},observer);
 const first=await Promise.race([ready,new Promise((_,j)=>setTimeout(()=>j(new Error('first ready timeout: '+JSON.stringify({sent:sent.map(x=>x.type),received:received.map(x=>x.type)}))),benchmarking?180000:20000).unref())]);
 const readyNs=Number(process.hrtime.bigint()-start);
 await new Promise(resolve=>setImmediate(resolve));
 const rows=client.localReplica.entityRows('tasks');if(benchmarking){assert.equal(rows.length,9002);assert(!rows.some(r=>r.id==='b'));}else assert.deepEqual(rows.map(r=>r.id).sort(),mode==='revoke-before-piece'?['c']:['a','c']);assert(!JSON.stringify(rows).includes('private-secret'));
 if(benchmarking){const after=process.resourceUsage();console.log('BENCH '+JSON.stringify({mode,totalNs:readyNs,codecNs,promotionNs,cryptoCalls,cryptoInputBytes,rows:rows.length,staged,clientCpuUs:after.userCPUTime+after.systemCPUTime-beforeUsage.userCPUTime-beforeUsage.systemCPUTime,heapBefore:beforeMemory.heapUsed,heapAfter:process.memoryUsage().heapUsed,maxRSSKiB:after.maxRSS,stats:client.starterBootstrapStats(),wireJsonBytes:received.reduce((n,m)=>n+Buffer.byteLength(JSON.stringify(m)),0)}));assert.equal(staged>0,mode==='benchmark-starter');
 } else if(mode==='bootstrap') {
   assert.equal(staged,1);assert.equal(received.filter(x=>x.type==='replica.starterOffer').length,1);assert.equal(received.filter(x=>x.type==='replica.snapshot'||x.type==='replica.snapshotPage').length,0);
   assert(first.cursor.revision>received.find(x=>x.type==='replica.starterOffer').cursor.revision,'unrelated revision not caught up');
   previousReady=first;console.log('READY');assert.equal(await line(),'RESUME');
   const closes=new Promise(resolve=>{waiters.push(m=>{if(m.type==='replica.close')resolve();});});stop();await closes;
   ready=new Promise((r,j)=>{readyResolve=r;readyReject=j;});stop=client.subscribeReplica(ref,{},observer);previousReady=await ready;
   const open=sent.filter(m=>m.type==='replica.open').at(-1) ?? sent.filter(m=>m.type==='replica.openMany').at(-1)?.opens?.at(-1);
   assert.equal(open.cursor.revision,first.cursor.revision);assert(open.digest.startsWith('xor1:'));assert.equal(open.starter,undefined);assert.equal(staged,1);
   console.log('RESUMED');
   console.log('RETENTION');assert.equal(await line(),'OPEN_OLD');
   const closed=new Promise(resolve=>{waiters.push(m=>{if(m.type==='replica.close')resolve();});});stop();await closed;
   ready=new Promise((r,j)=>{readyResolve=r;readyReject=j;});stop=client.subscribeReplica(ref,{},observer);const retained=await ready;
   const oldOpen=sent.filter(m=>m.type==='replica.open').at(-1)??sent.filter(m=>m.type==='replica.openMany').at(-1)?.opens.at(-1);
   assert.equal(oldOpen.starter,undefined);assert.equal(oldOpen.cursor.revision,previousReady.cursor.revision);assert(retained.cursor.revision>previousReady.cursor.revision);assert.equal(received.filter(x=>x.type==='replica.starterOffer').length,2);assert.equal(staged,2);assert.equal(received.filter(x=>x.type==='replica.snapshot'||x.type==='replica.snapshotPage').length,0);
   console.log('RETENTION_BOOTSTRAPPED');
 } else if(mode==='revoke-before-piece') {
   assert.equal(staged,0);assert.equal(received.filter(x=>x.type==='replica.starterPiece').length,0);assert(received.some(x=>x.type==='replica.starterFallback'));assert.equal(sent.reduce((n,x)=>n+(x.type==='replica.open'?1:x.type==='replica.openMany'?x.opens.length:0),0),1);
 } else {
   assert.equal(staged,0);assert.equal(received.filter(x=>x.type==='replica.starterOffer').length,0);assert(received.some(x=>x.type==='replica.snapshot'||x.type==='replica.snapshotPage'));
   console.log('FALLBACK');
 }
 await new Promise(resolve=>setImmediate(resolve));
 console.log('RESULT '+JSON.stringify({mode,sentTypes:sent.map(m=>m.type),receivedTypes:received.map(m=>m.type),rows:rows.length,cursor:first.cursor,starterStats:client.starterBootstrapStats()}));
} finally {stop?.();client?.close();rl.close();}
