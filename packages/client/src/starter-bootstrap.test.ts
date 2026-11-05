import { afterEach, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { constants, deflateRawSync } from "node:zlib";
import type { StarterOffer, JsonValue } from "@gonvex/protocol";
import { StarterStage, StarterBudget } from "./starter-bootstrap";
import { nodeStarterInflater } from "./node-starter-inflater";
import { replicaRowsHashes,replicaAggregateDigest } from "./replica-integrity";
import { LocalReplica,MemoryLocalReplicaStorage,type StarterPromotionGuard,type ReplicaWindow,type ReplicaSnapshot,type ReplicaScope,type ReplicaRow } from "./local-replica";
const stages:StarterStage[]=[];
afterEach(()=>stages.splice(0).forEach(s=>s.dispose()));
function hash(b:Uint8Array){return createHash("sha256").update(b).digest("hex");}
async function fixture(rows:JsonValue[]=[{id:"a",body:"first"}],budget=new StarterBudget()) {
 const raw=new TextEncoder().encode(rows.map(r=>JSON.stringify(r)).join("\n")+"\n");
 const full=deflateRawSync(raw,{level:1,windowBits:12,finishFlush:constants.Z_SYNC_FLUSH});const bytes=full.subarray(0,-4);
 const offer:StarterOffer={type:"replica.starterOffer",id:"open",offerId:"opaque",version:1,expiresAt:Date.now()+60000,cursor:{epoch:"e",revision:2},key:"id",path:"tasks.all",scope:"tenant",visibilityScope:"visible",orderBy:"id",orderDirection:"asc",mode:"eager",maxRows:100000,maxBytes:0,truncated:false,digest:replicaAggregateDigest(await replicaRowsHashes(rows,"id")),rows:rows.length,args:{},serializedDefinition:JSON.stringify({table:"tasks",key:"id",columns:["id","body"],orderBy:"id",orderDirection:"asc",mode:"eager",maxRows:100000,maxBytes:0}),format:1,encoding:"json-lines-pmd-level1-window12-no-context-v1",columnTypes:{id:"text",body:"text"},pieces:[{index:0,hash:hash(bytes),rawHash:hash(raw),bytes:bytes.length,rawBytes:raw.length,compressed:true}]};
 const stage=new StarterStage(offer,nodeStarterInflater,budget);stages.push(stage);
 const piece={type:"replica.starterPiece" as const,id:offer.id,offerId:offer.offerId,index:0,data:Buffer.from(bytes).toString("base64")};
 return {stage,offer,piece,budget};
}
it("keeps rows and intermediate cursors outside ALL active storage until final ready promotion",async()=>{
 const f=await fixture();const storage=new MemoryLocalReplicaStorage();const replica=new LocalReplica(storage);await replica.activateScope("visible");
 await f.stage.piece(f.piece);await f.stage.seal();
 expect(replica.windowResumeState("tasks")).toBeUndefined();expect(await storage.load("visible")).toBeUndefined();
 const reload=new LocalReplica(storage);await reload.activateScope("visible");expect(reload.windowResumeState("tasks")).toBeUndefined();reload.dispose();
 const next={id:"a",body:"after catchup"};f.stage.delta({type:"replica.delta",id:"open",cursor:{epoch:"e",revision:3},upserts:[next]});
 expect(replica.entity("tasks","a")).toBeUndefined();
 const digest=replicaAggregateDigest(await replicaRowsHashes([next],"id"));const snapshot=await f.stage.promote({type:"replica.ready",id:"open",cursor:{epoch:"e",revision:4},digest,truncated:false});
 const controller=new AbortController();await replica.promoteStarterWindow({signature:"tasks",kind:"replica",entity:"tasks",key:"id",rows:snapshot.result as ReplicaRow[],completeness:"complete",source:"server",scope:"visible",cursor:snapshot.cursor,hashes:snapshot.hashes},controller.signal,()=>true);
 expect(replica.windowResumeState("tasks")?.cursor).toEqual({epoch:"e",revision:4});expect(replica.entity("tasks","a")?.body).toBe("after catchup");replica.dispose();
});
it("bounds the decoder DURING inflation and rejects unexpected private fields",async()=>{
 const f=await fixture([{id:"a",body:"x".repeat(200000)}]);const forged={...f.offer,pieces:[{...f.offer.pieces[0]!,rawBytes:1}]};const stage=new StarterStage(forged,nodeStarterInflater,new StarterBudget());stages.push(stage);
 await expect(stage.piece(f.piece)).rejects.toThrow();
 const g=await fixture([{id:"a",body:"value",private:"secret"}]);await expect(g.stage.piece(g.piece)).rejects.toThrow("projection");
});
it("enforces one aggregate SDK budget, including cancelled in-flight operations",async()=>{
 const budget=new StarterBudget();const first=await fixture([{id:"a",body:"x".repeat(2_000_000)}],budget).catch(()=>undefined);
 // A single piece is independently bounded before invoking the codec.
 expect(first).toBeUndefined();expect(budget.usage().chargedBytes).toBe(0);
 const loan=budget.acquire(40*1024*1024);expect(()=>budget.acquire(30*1024*1024)).toThrow("budget");loan.release();expect(budget.usage().chargedBytes).toBe(0);
 const f=await fixture(undefined,budget);f.stage.dispose();expect(budget.usage().chargedBytes).toBe(0);
});
it("retires promotion before storage commit and never changes the new scope",async()=>{
 let entered!:()=>void,release!:()=>void;const entering=new Promise<void>(r=>entered=r),gate=new Promise<void>(r=>release=r);
 class Controlled extends MemoryLocalReplicaStorage {override async promoteStarterWindow(w:ReplicaWindow,s:ReplicaSnapshot,scope:ReplicaScope,p:readonly ReplicaRow[],guard:StarterPromotionGuard){entered();await gate;return super.promoteStarterWindow(w,s,scope,p,guard);}}
 const storage=new Controlled(),replica=new LocalReplica(storage);await replica.activateScope("old");const abort=new AbortController();let owned=true;
 const promotion=replica.promoteStarterWindow({signature:"tasks",kind:"replica",entity:"tasks",key:"id",rows:[{id:"a",body:"old-stage"}],completeness:"complete",source:"server",scope:"old",cursor:{epoch:"e",revision:4}},abort.signal,()=>owned);
 await entering;owned=false;abort.abort();const activation=replica.activateScope("new");release();await expect(promotion).rejects.toThrow();await activation;
 expect(await storage.load("old")).toBeUndefined();expect(await storage.load("new")).toBeUndefined();expect(replica.entity("tasks","a")).toBeUndefined();replica.dispose();
});
it("keeps pending codec buffers charged until a cancelled inflate actually settles",async()=>{
 const f=await fixture(),budget=new StarterBudget();let entered!:()=>void,release!:()=>void;const entering=new Promise<void>(r=>entered=r),gate=new Promise<void>(r=>release=r);
 const stage=new StarterStage(f.offer,async(bytes,limits)=>{entered();await gate;return nodeStarterInflater(bytes,limits);},budget);stages.push(stage);
 const pending=stage.piece(f.piece);await entering;stage.dispose();expect(budget.usage().stages).toBe(1);expect(()=>budget.acquire(budget.maxBytes)).toThrow();release();await expect(pending).rejects.toThrow("retired");expect(budget.usage().chargedBytes).toBe(0);
});
it("aborts real IndexedDB writes before commit and interleaved readers retain the old active checkpoint",async()=>{
 const {IDBFactory,IDBKeyRange}=await import('fake-indexeddb');const {Dexie}=await import('dexie');const {IndexedDBEngineReplicaStorage}=await import('./indexeddb-engine');
 const priorIDB=globalThis.indexedDB,priorRange=globalThis.IDBKeyRange;Object.assign(globalThis,{indexedDB:new IDBFactory(),IDBKeyRange});Dexie.dependencies.indexedDB=globalThis.indexedDB;Dexie.dependencies.IDBKeyRange=IDBKeyRange;
 const name='starter-atomic-'+Math.random(),storage=new IndexedDBEngineReplicaStorage(name),peer=new IndexedDBEngineReplicaStorage(name),replica=new LocalReplica(storage);
 try {
  await replica.activateScope('scope');const old={signature:'tasks',kind:'replica' as const,entity:'tasks',key:'id',rows:[{id:'a',body:'old'}],completeness:'complete' as const,source:'server' as const,scope:'scope',cursor:{epoch:'e',revision:1}};
  await replica.replaceWindow(old);
  const controller=new AbortController();let read:Promise<ReplicaSnapshot|undefined>|undefined;
  const target=storage as any,write=target.writeWindows.bind(storage);
  target.writeWindows=async(...args:any[])=>{
    await write(...args);
    expect(replica.entity('tasks','a')?.body).toBe('old');
    // Start a genuinely independent ordinary reader after physical writes but
    // before their transaction commits. It must observe old state after abort.
    read=Dexie.ignoreTransaction(()=>peer.load('scope'));
    controller.abort();
  };
  const promotion=replica.promoteStarterWindow({...old,rows:[{id:'a',body:'UNCOMMITTED'}],cursor:{epoch:'e',revision:4}},controller.signal,()=>true);
  await expect(promotion).rejects.toThrow();expect(read).toBeDefined();const snapshot=await read;
  expect(snapshot?.entities.tasks?.a?.body).toBe('old');expect(snapshot?.liveQueries.tasks?.cursor?.revision).toBe(1);expect(replica.entity('tasks','a')?.body).toBe('old');

 } finally {replica.dispose();storage.close();peer.close();Object.assign(globalThis,{indexedDB:priorIDB,IDBKeyRange:priorRange});Dexie.dependencies.indexedDB=priorIDB;Dexie.dependencies.IDBKeyRange=priorRange;}
});
it("does not publish or persist a staged working set after storage failure",async()=>{
 class Failing extends MemoryLocalReplicaStorage {override async promoteStarterWindow(){throw new Error('disk failed');}}
 const storage=new Failing(),replica=new LocalReplica(storage);await replica.activateScope('s');const controller=new AbortController();await expect(replica.promoteStarterWindow({signature:'w',kind:'replica',entity:'tasks',key:'id',rows:[{id:'a'}],completeness:'complete',source:'server',scope:'s',cursor:{epoch:'e',revision:3}},controller.signal,()=>true)).rejects.toThrow('disk failed');expect(replica.entity('tasks','a')).toBeUndefined();expect(replica.windowResumeState('w')).toBeUndefined();expect(await storage.load('s')).toBeUndefined();replica.dispose();
});

class StarterSocket {
 static instances:StarterSocket[]=[]; static OPEN=1; static CONNECTING=0;
 readyState=0;sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
 constructor(readonly url:string){StarterSocket.instances.push(this);}
 addEventListener(type:string,handler:(event:any)=>void){this.listeners.set(type,[...(this.listeners.get(type)??[]),handler]);}
 emit(type:string,event={}){for(const f of this.listeners.get(type)??[])f(event);}
 send(data:string){this.sent.push(JSON.parse(data));}
 open(){this.readyState=1;this.emit('open');}close(){this.readyState=3;this.emit('close');}
 receive(data:unknown){this.emit('message',{data:JSON.stringify(data)});}
}
for(const retirement of ['auth','managed','disconnect','unsubscribe','failure','metadata'] as const) it(`SDK ${retirement} preserves ownership rules at final physical promotion`,async()=>{
 const {GonvexClient}=await import('./index');
 let enter!:()=>void,release!:()=>void;const entered=new Promise<void>(r=>enter=r),gate=new Promise<void>(r=>release=r);
 class Controlled extends MemoryLocalReplicaStorage {override async promoteStarterWindow(w:ReplicaWindow,s:ReplicaSnapshot,scope:ReplicaScope,p:readonly ReplicaRow[],guard:StarterPromotionGuard){enter();await gate;if(retirement==='failure')throw new Error('controlled disk failure');return super.promoteStarterWindow(w,s,scope,p,guard);}}
 const storage=new Controlled();vi.stubGlobal('WebSocket',StarterSocket);StarterSocket.instances=[];
 const client=new GonvexClient('ws://synthetic.invalid/ws',{token:'native',tenant:'tenant',project:'synthetic',replicaStarterFiles:true,starterInflater:nodeStarterInflater,localReplica:{storage},replicaSubscriptionRetentionMs:0,replicaOpenBatchMs:0});
 let stop=client.subscribeReplica({kind:'query',path:'tasks.all',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','body']}},{},()=>{});
 try {
  const socket=StarterSocket.instances.at(-1)!;socket.open();const directive={protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'};
  socket.receive({type:'session.ready',replica:directive,capabilities:{replicaStarterFiles:1}});
  await vi.waitFor(()=>expect(socket.sent.some(x=>x.type==='auth')).toBe(true));const auth=socket.sent.find(x=>x.type==='auth');socket.receive({type:'auth.result',id:auth.id,result:{replica:directive,replicaStarterFiles:true}});
  await vi.waitFor(()=>expect(socket.sent.some(x=>x.type==='replica.open'||x.type==='replica.openMany')).toBe(true));
  const open=socket.sent.find(x=>x.type==='replica.open')??socket.sent.find(x=>x.type==='replica.openMany').opens[0];expect(open.starter).toEqual({version:1});
  const f=await fixture();socket.receive({...f.offer,id:open.id});await (client as any).replicaFrames;socket.receive({...f.piece,id:open.id});await (client as any).replicaFrames;
  expect(socket.sent.some(x=>x.type==='replica.starterCommit')).toBe(true);
  socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:4},digest:f.offer.digest,truncated:false});await entered;
  expect(client.localReplica.entity('tasks','a')).toBeUndefined();const reload=new LocalReplica(storage);await reload.activateScope('visible');expect(reload.windowResumeState('tasks')).toBeUndefined();expect(reload.entityRows('tasks')).toEqual([]);reload.dispose();
  if(retirement==='managed')void client.authenticate({token:'new-owner'}).catch(()=>{});else if(retirement==='metadata')client.setAuth({fetchToken:async()=> 'native'});else if(retirement==='auth')client.setAuth({token:'new-owner'});else if(retirement==='disconnect')socket.close();else if(retirement==='unsubscribe'){stop();await new Promise(r=>setTimeout(r,10));}
  release();await (client as any).replicaFrames;
  if(retirement==='metadata'){expect(client.localReplica.entity('tasks','a')?.body).toBe('first');expect(Object.keys((await storage.load('visible'))?.liveQueries??{})).toHaveLength(1);expect(socket.sent.filter(x=>x.type==='auth')).toHaveLength(1);}else{expect(client.localReplica.entity('tasks','a')).toBeUndefined();expect((await storage.load('visible'))?.liveQueries??{}).toEqual({});}expect(client.starterBootstrapStats().chargedBytes).toBe(0);
  if(retirement==='failure')expect(socket.sent.filter(x=>x.type==='replica.starterCancel')).toHaveLength(1);
 } finally {release();stop();client.close();vi.unstubAllGlobals();}
});

for(const boundary of ['offer','piece','sealed','catchup','final-ready'] as const) it(`ordinary reload at ${boundary} ignores UNCOMMITTED RAM rows and cursor`,async()=>{
 const f=await fixture(),storage=new MemoryLocalReplicaStorage(),active=new LocalReplica(storage);await active.activateScope('visible');
 await active.replaceWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'id',rows:[{id:'old',body:'active'}],completeness:'complete',source:'server',scope:'visible',cursor:{epoch:'e',revision:1},hashes:await replicaRowsHashes([{id:'old',body:'active'}],'id')});
 if(boundary!=='offer')await f.stage.piece(f.piece);
 if(['sealed','catchup','final-ready'].includes(boundary))await f.stage.seal();
 if(['catchup','final-ready'].includes(boundary))f.stage.delta({type:'replica.delta',id:'open',cursor:{epoch:'e',revision:3}});
 if(boundary==='final-ready')await f.stage.promote({type:'replica.ready',id:'open',cursor:{epoch:'e',revision:4},digest:f.offer.digest,truncated:false});
 f.stage.dispose();const reloaded=new LocalReplica(storage);await reloaded.activateScope('visible');expect(reloaded.entityRows('tasks')).toEqual([{id:'old',body:'active'}]);expect(reloaded.windowResumeState('tasks')?.cursor).toEqual({epoch:'e',revision:1});reloaded.dispose();active.dispose();
});
it('rejects oversized catchup rows and delete-key buffers before serializing copies',async()=>{
 const f=await fixture();await f.stage.piece(f.piece);await f.stage.seal();
 expect(()=>f.stage.delta({type:'replica.delta',id:'open',cursor:{epoch:'e',revision:3},upserts:[{id:'a',body:'x'.repeat(300000)}]})).toThrow();
 expect(()=>f.stage.delta({type:'replica.delta',id:'open',cursor:{epoch:'e',revision:3},deleted:['x'.repeat(6*1024*1024)]})).toThrow();
 f.stage.dispose();expect(f.budget.usage().chargedBytes).toBe(0);
});
