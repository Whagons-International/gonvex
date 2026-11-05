import test from'node:test';import assert from'node:assert/strict';import{readFileSync}from'node:fs';import{gunzipSync}from'node:zlib';import{pathToFileURL}from'node:url';
import{installWireRecorder,transportReplicaAuthority}from'./wire-record.mjs';
import{ThinClient,RowPool}from'./thin-client.mjs';import{ThinCheckpointStore}from'./thin-checkpoint.mjs';import{captureVisibility,compareVisibility}from'./visibility-oracle.mjs';
const fixture=JSON.parse(gunzipSync(readFileSync(new URL('./fixtures/visibility-team-revoke-799-800.json.gz',import.meta.url))));
const turn=()=>new Promise(r=>setTimeout(r,5));async function until(fn){for(let i=0;i<2000;i++){if(fn())return;await turn();}throw Error('real frame fixture did not settle');}
class Socket{static OPEN=1;static CONNECTING=0;static instances=[];constructor(){this.readyState=0;this.events=new Map();this.sent=[];Socket.instances.push(this);}addEventListener(k,f){this.events.set(k,[...(this.events.get(k)||[]),f]);}removeEventListener(k,f){this.events.set(k,(this.events.get(k)||[]).filter(x=>x!==f));}emit(k,e={}){for(const fn of this.events.get(k)||[])fn(e);}send(raw){this.sent.push(JSON.parse(raw));}open(){this.readyState=1;this.emit('open');}receive(m){this.emit('message',{data:JSON.stringify(m)});}close(){this.readyState=3;this.emit('close');}}
const opens=s=>s.sent.flatMap(m=>m.type==='replica.openMany'?m.opens:m.type==='replica.open'?[m]:[]);
function rewrite(m,old,id){const r={...m};if(r.id===old)r.id=id;if(r.items)r.items=r.items.map(x=>rewrite(x,old,id));if(r.ready)r.ready=r.ready.map(x=>rewrite(x,old,id));return r;}
async function start(thin,SDK){const c=thin?new ThinClient('ws://127.0.0.1:1/ws',{project:'fixture',pool:new RowPool(),checkpointStore:new ThinCheckpointStore(),checkpointIdentity:fixture.principal.identity,modulePin:fixture.moduleHash}):new SDK.GonvexClient('ws://127.0.0.1:1/ws',{project:'fixture',token:'synthetic-only',tenant:fixture.principal.tenant,outbox:{enabled:false},localReplica:{storage:new SDK.MemoryLocalReplicaStorage()}});if(thin)c.auth={token:'synthetic-only',tenant:fixture.principal.tenant};c.connect();const s=Socket.instances.at(-1);s.open();s.receive({type:'session.ready',capabilities:{replicaResume:1,replicaSnapshotPages:1,replicaBatch:1,replicaWatermark:1}});await until(()=>s.sent.some(m=>m.type==='auth'));s.receive({type:'auth.result',id:s.sent.find(m=>m.type==='auth').id,result:{replica:fixture.authority}});await turn();const w=c.watchReplica(fixture.ref,fixture.args);w.onUpdate(()=>{try{w.localReplicaResult();}catch{}});await until(()=>opens(s).length);return{c,s,w};}
const capture=x=>captureVisibility({client:x.c,watch:x.w,ref:fixture.ref,args:fixture.args,principal:fixture.principal,authority:transportReplicaAuthority(x.c)});
function feed(x,frames,old=fixture.originalId){for(const m of frames)x.s.receive(rewrite(m,old,opens(x.s).at(-1).id));}
const cold=fixture.frames.filter(f=>f.type==='replica.snapshotPage'||f.type==='replica.ready'&&f.cursor.revision===799),tail=fixture.frames.slice(cold.length);
test('actual captured member-team revoke keeps 1071 stale rows detectable and compares only certified revision 800', {skip:!process.env.LOAD_TEST_SDK},async()=>{
 const Native=globalThis.WebSocket;globalThis.WebSocket=Socket;installWireRecorder(undefined,{observe:()=>{}});const SDK=await import(pathToFileURL(process.env.LOAD_TEST_SDK));const clients=[];
 try{
  const thin=await start(true,SDK),sdk=await start(false,SDK),fresh=await start(false,SDK);clients.push(thin,sdk,fresh);feed(thin,cold);feed(sdk,cold);feed(fresh,fixture.freshFrames,fixture.freshId);
  await until(()=>capture(thin).qualified&&capture(sdk).qualified&&capture(fresh).qualified);
  assert.equal(capture(thin).rowCount,1489);assert.equal(capture(fresh).rowCount,418);
  // This is the old oracle's exact reported excess, from genuine module frames.
  const a=new Set(thin.w.localReplicaResult().map(r=>r._id)),b=new Set(fresh.w.localReplicaResult().map(r=>r._id));assert.equal([...a].filter(id=>!b.has(id)).length,1071);
  assert.equal(compareVisibility(capture(thin),capture(fresh)).reason,'revision-mismatch');assert.equal(compareVisibility(capture(thin),capture(fresh)).passed,false);
  assert.equal(compareVisibility(capture(thin),capture(sdk)).passed,true);
  // Lost invalidation/delta: a complete client falsely stamped at 800 must FAIL,
  // not gain a scope/hash/timing exemption merely because cursors agree.
  const stale=await start(true,SDK);clients.push(stale);feed(stale,cold);await until(()=>capture(stale).qualified);feed(stale,tail.filter(m=>m.type==='replica.batch'));
  const bad=compareVisibility(capture(stale),capture(fresh));assert.equal(bad.qualified,true);assert.equal(bad.kind,'mismatch');assert.equal(bad.extra.length,1071);assert.equal(bad.missing.length,0);
  // Full normal needHashes/batch/delta/ready histories on both real models.
  feed(thin,tail);feed(sdk,tail);await until(()=>capture(thin).qualified&&capture(sdk).qualified&&capture(thin).cursor.revision===800&&capture(sdk).cursor.revision===800);
  for(const x of[thin,sdk]){const v=compareVisibility(capture(x),capture(fresh));assert.equal(v.passed,true);assert.deepEqual(v.extra,[]);assert.deepEqual(v.missing,[]);assert.deepEqual(v.changed,[]);}
  assert.equal(compareVisibility(capture(thin),capture(sdk)).passed,true);
  const corrupt=captureVisibility({client:sdk.c,watch:{status:()=>sdk.w.status(),localReplicaState:()=>sdk.w.localReplicaState(),localReplicaResult:()=>sdk.w.localReplicaResult().map((r,i)=>i? r:{...r,name:'corrupt'})},ref:fixture.ref,args:fixture.args,principal:fixture.principal,authority:transportReplicaAuthority(sdk.c)});assert.equal(corrupt.qualified,false);assert.equal(corrupt.reason,'checkpoint-row-digest-mismatch');
  // Reproduce the old "0 extra, 0 missing" diagnostic with non-current state.
  feed(thin,[tail.find(m=>m.type==='replica.needHashes')]);assert.deepEqual(thin.w.localReplicaResult(),fresh.w.localReplicaResult());const pending=compareVisibility(capture(thin),capture(fresh));assert.equal(pending.qualified,false);assert.equal(pending.passed,false);
  const other=captureVisibility({client:fresh.c,watch:fresh.w,ref:fixture.ref,args:fixture.args,principal:{...fixture.principal,identity:'different@example.invalid'},authority:fixture.authority});assert.equal(compareVisibility(capture(sdk),other).reason,'authority-or-window-mismatch');
 }finally{for(const x of clients){x.c.close();x.c.checkpointStore?.clear();}globalThis.WebSocket=Native;}
});

test('transport authority follows normal auth, reauthentication, error and close without retaining credentials',()=>{
 const Native=globalThis.WebSocket;globalThis.WebSocket=Socket;installWireRecorder(undefined,{observe:()=>{}});
 try{const socket=new globalThis.WebSocket('ws://127.0.0.1:1'),client={socket};socket.open();socket.send(JSON.stringify({type:'auth',token:'synthetic-secret'}));assert.equal(transportReplicaAuthority(client),undefined);socket.receive({type:'auth.result',result:{replica:fixture.authority,token:'synthetic-secret'}});assert.deepEqual(transportReplicaAuthority(client),fixture.authority);assert.equal(Object.isFrozen(transportReplicaAuthority(client)),true);
 socket.send(JSON.stringify({type:'auth',tenant:'other-synthetic-tenant'}));assert.equal(transportReplicaAuthority(client),undefined);socket.receive({type:'auth.result',result:{replica:fixture.authority}});socket.receive({type:'auth.error'});assert.equal(transportReplicaAuthority(client),undefined);socket.receive({type:'auth.result',result:{replica:fixture.authority}});socket.close();assert.equal(transportReplicaAuthority(client),undefined);
 }finally{globalThis.WebSocket=Native;}
});
