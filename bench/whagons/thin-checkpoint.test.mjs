import test from 'node:test';
import assert from 'node:assert/strict';
import { ThinClient, RowPool } from './thin-client.mjs';
import { ThinCheckpointStore, integrity } from './thin-checkpoint.mjs';
const ref={path:'tasks.list',delivery:'replica',replica:{table:'tasks',key:'_id',columns:['_id','name']}};
const cursor=revision=>({epoch:'db-e',revision});
const directive={protocolVersion:1,scope:'wire-s',visibilityScope:'principal-s',epoch:'module-e'};
const tick=()=>new Promise(r=>setTimeout(r,5));
const digest=rows=>{const p=new RowPool();return integrity(new Map(rows.map(r=>[r._id,p.acquireValue(r)])),ref.replica.columns).digest;};
async function client(store,options={}){
 const sent=[],c=new ThinClient(options.url||'ws://127.0.0.1:1/ws',{project:'p',pool:new RowPool(),checkpointStore:store,checkpointIdentity:'user-a',modulePin:'module-sha',...options});
 c.socket={readyState:1,send:s=>sent.push(JSON.parse(s)),close(){}};
 c.serverCapabilities={replicaResume:1,replicaWatermark:1};
 const auth=c.authenticate({project:'p',token:'synthetic-token',tenant:options.tenant||'t'});
 c.receive(JSON.stringify({type:'auth.result',id:sent.at(-1).id,result:{replica:options.directive||directive}}));await auth;
 const w=c.watchReplica(ref,options.args||{b:2,a:1});w.onUpdate(()=>{});await tick();
 const id=sent.find(m=>m.type==='replica.openMany').opens[0].id;
 return {c,sent,w,id};
}
function snapshot(x,rows=[{_id:'a',name:'A'}],revision=1){
 x.c.receive(JSON.stringify({type:'replica.snapshot',id:x.id,cursor:cursor(revision),key:'_id',result:rows}));
 x.c.receive(JSON.stringify({type:'replica.ready',id:x.id,cursor:cursor(revision),digest:digest(rows)}));
}
test('fresh normal client restores complete own rows and cursor, negotiates before current, and answers needHashes',async()=>{
 const store=new ThinCheckpointStore();const a=await client(store);snapshot(a);
 assert.equal(a.w.status().isUpToDate,true);assert.equal(store.stats().windows,1);
 a.c.close();const b=await client(store);
 const open=b.sent.find(m=>m.type==='replica.openMany').opens[0];
 assert.deepEqual(open.cursor,cursor(1));assert.equal(open.digest,digest([{_id:'a',name:'A'}]));
 assert.deepEqual(b.w.localReplicaResult(),[{_id:'a',name:'A'}]);assert.equal(b.w.status().isUpToDate,false);
 b.c.receive(JSON.stringify({type:'replica.needHashes',id:b.id}));
 assert.deepEqual(b.sent.at(-1).opens[0].keys,['a']);assert.equal(b.sent.at(-1).opens[0].fullIntegrity,true);
 b.c.receive(JSON.stringify({type:'replica.ready',id:b.id,cursor:cursor(1),digest:open.digest}));
 assert.equal(b.w.status().isUpToDate,true);
 assert.equal(b.c.cacheEvidence([b.c.replicaSignature(ref,{a:1,b:2})]).cachedFullSetVerified,true);
 b.c.close();store.clear();assert.equal(a.c.pool.size,0);assert.equal(b.c.pool.size,0);
});
test('transactions, adds, revokes and batch watermark atomically checkpoint real window rows',async()=>{
 const store=new ThinCheckpointStore(),a=await client(store);snapshot(a,[{_id:'a',name:'A'},{_id:'old',name:'revoke'}]);
 a.c.receive(JSON.stringify({type:'replica.batch',items:[
  {type:'replica.transaction',cursor:cursor(2),changes:[{entity:'tasks',id:'a',operation:'update',newValue:{_id:'a',name:'A2'}}]},
  {type:'replica.stream',id:a.id,stream:'s1'},
  {type:'replica.delta',id:'s1',cursor:cursor(2),upserts:[{_id:'b',name:'B'}],deleted:['old']},
  {type:'replica.ready',id:'s1',cursor:cursor(2),digest:digest([{_id:'a',name:'A2'},{_id:'b',name:'B'}])},
 ],watermark:2}));
 assert.equal(a.w.status().isUpToDate,true);a.c.close();
 const b=await client(store);assert.deepEqual(b.w.localReplicaResult(),[{_id:'a',name:'A2'},{_id:'b',name:'B'}]);
 assert.deepEqual(b.sent.at(-1).opens[0].cursor,cursor(2));b.c.close();store.clear();
});
test('incomplete pages never replace persisted rows; failed opens, cancel, bad digest and reset fail closed',async()=>{
 for(const fault of ['partial','bad-digest','order','failed','reset']){
  const store=new ThinCheckpointStore(),a=await client(store);snapshot(a);a.c.close();const b=await client(store);
  if(fault==='failed')b.c.receive(JSON.stringify({type:'replica.error',id:b.id,error:'budget'}));
  else if(fault==='reset')b.c.receive(JSON.stringify({type:'replica.reset',id:b.id,reason:'cursor-pruned'}));
  else{
   b.c.receive(JSON.stringify({type:'replica.snapshotPage',scope:directive.scope,visibilityScope:directive.visibilityScope,id:b.id,snapshotId:'new',sequence:0,last:false,key:'_id',cursor:cursor(3),scope:directive.scope,visibilityScope:directive.visibilityScope,result:[{_id:'z',name:'Z'}]}));
   if(fault==='bad-digest')b.c.receive(JSON.stringify({type:'replica.ready',id:b.id,cursor:cursor(3),digest:'wrong'}));
   if(fault==='order')b.c.receive(JSON.stringify({type:'replica.snapshotPage',scope:directive.scope,visibilityScope:directive.visibilityScope,id:b.id,snapshotId:'new',sequence:2,last:true,key:'_id',cursor:cursor(3),result:[]}));
  }
  assert.equal(b.w.status().isUpToDate,false,fault);assert.equal(store.stats().windows,0,fault);
  b.c.close();const fresh=await client(store);assert.equal(fresh.sent.at(-1).opens[0].cursor,undefined,fault);fresh.c.close();store.clear();
 }
});
test('identity, tenant, visibility and module epoch rotations never restore another authority',async()=>{
 for(const options of [{checkpointIdentity:'user-b'},{tenant:'other-tenant'},{url:'ws://127.0.0.1:2/ws'},{modulePin:'module-other'},{directive:{...directive,visibilityScope:'new-principal'}},{directive:{...directive,epoch:'module-new'}}]){
  const store=new ThinCheckpointStore(),a=await client(store);snapshot(a);a.c.close();const b=await client(store,options);
  assert.equal(b.sent.at(-1).opens[0].cursor,undefined);assert.equal(b.w.localReplicaResult(),undefined);b.c.close();store.clear();
 }
});
test('legacy paged and native snapshot activation validates all pages and their digest before reuse',async()=>{
 for(const native of [false,true]){
  const store=new ThinCheckpointStore(),x=await client(store,{nativeRows:native});
  const rows=[{_id:'a',name:'A'},{_id:'b',name:'B'}];
  if(native)x.c.receive(JSON.stringify({type:'replica.snapshotStart',scope:directive.scope,visibilityScope:directive.visibilityScope,id:x.id,snapshotId:'pg',cursor:cursor(1),key:'_id'}));
  for(let i=0;i<2;i++)x.c.receive(JSON.stringify({type:native?'replica.snapshotEncodedPage':'replica.snapshotPage',id:x.id,snapshotId:'pg',cursor:cursor(1),key:'_id',sequence:i,last:i===1,scope:directive.scope,visibilityScope:directive.visibilityScope,...(native?{canonicalRows:[JSON.stringify(rows[i])]}:{result:[rows[i]]}),...(i===1?{digest:digest(rows)}:{})}));
  assert.equal(store.stats().windows,0);
  x.c.receive(JSON.stringify({type:'replica.ready',id:x.id,cursor:cursor(1),digest:digest(rows)}));
  assert.equal(x.w.status().isUpToDate,true);assert.equal(store.stats().windows,1);
  assert.equal(x.sent.filter(m=>m.type==='replica.snapshotAck').length,native?2:0);x.c.close();store.clear();
 }
});

test('scope revocation clears displayed and persisted rows immediately; failed-open retry can recover safely',async t=>{
 const store=new ThinCheckpointStore(),a=await client(store);snapshot(a);a.c.close();const b=await client(store);
 b.c.receive(JSON.stringify({type:'replica.error',id:b.id,error:'admission refused'}));
 assert.equal(store.stats().windows,0);
 // Observe the normal retry scheduler rather than manually opening the subscription.
 await new Promise(r=>setTimeout(r,300));
 assert.ok(b.sent.filter(m=>m.type==='replica.openMany').length>=2);
 b.c.receive(JSON.stringify({type:'replica.ready',id:b.id,cursor:cursor(1),digest:digest([{_id:'a',name:'A'}])}));
 assert.equal(b.w.status().isUpToDate,true);assert.equal(store.stats().windows,1);
 b.c.receive(JSON.stringify({type:'auth.error',id:'membership-changed',code:'revoked',error:'removed'}));
 assert.equal(store.stats().windows,0);assert.deepEqual(b.w.localReplicaResult(),[]);assert.equal(b.w.status().isUpToDate,false);
 b.c.close();store.clear();
});
test('canceling a staged snapshot and cursor-only SHA digest checkpoints preserve no partial membership',async()=>{
 const store=new ThinCheckpointStore(),x=await client(store);snapshot(x);
 x.c.receive(JSON.stringify({type:'replica.needHashes',id:x.id}));
 x.c.receive(JSON.stringify({type:'replica.snapshotPage',scope:directive.scope,visibilityScope:directive.visibilityScope,id:x.id,snapshotId:'pending',sequence:0,last:false,key:'_id',cursor:cursor(2),result:[{_id:'z',name:'Z'}]}));
 x.c.close();assert.equal(store.stats().windows,0);assert.equal(x.c.pool.size,0);
 const fresh=await client(store);snapshot(fresh);
 // Legacy digest mode is computed from UTF-8 sorted key/hash pairs, not copied from ready.
 const actual=integrity([...fresh.c.subscriptions.values()][0].rows,ref.replica.columns,'sha256');
 fresh.c.receive(JSON.stringify({type:'replica.ready',id:fresh.id,cursor:cursor(2),digest:actual.digest}));
 assert.equal(fresh.w.status().isUpToDate,true);
 fresh.c.receive(JSON.stringify({type:'replica.watermark',revision:3}));fresh.c.close();
 const reload=await client(store);assert.deepEqual(reload.sent.at(-1).opens[0].cursor,cursor(3));reload.c.close();store.clear();
});
test('changed collection contract, empty/missing cache, and stale epoch cannot qualify a cached wave',async()=>{
 const store=new ThinCheckpointStore(),x=await client(store);snapshot(x);x.c.close();const next=await client(store);
 assert.equal(next.c.cacheEvidence([next.c.replicaSignature({...ref,replica:{...ref.replica,maxRows:1}},{a:1,b:2})]).cachedFullSetVerified,false);
 next.c.receive(JSON.stringify({type:'replica.delta',id:next.id,cursor:{epoch:'unnegotiated-db-epoch',revision:9},upserts:[],deleted:[]}));
 assert.equal(next.w.status().isUpToDate,false);assert.equal(store.stats().windows,0);next.c.close();store.clear();
 const empty=await client(store);snapshot(empty,[]);
 assert.equal(empty.c.cacheEvidence([empty.c.replicaSignature(ref,{a:1,b:2})]).cachedFullSetVerified,false);empty.c.close();store.clear();
});

test('transaction during snapshot cannot checkpoint new rows with the old snapshot cursor',async()=>{
 const store=new ThinCheckpointStore(),x=await client(store);
 x.c.receive(JSON.stringify({type:'replica.snapshotPage',scope:directive.scope,visibilityScope:directive.visibilityScope,id:x.id,snapshotId:'pages',sequence:0,last:false,key:'_id',cursor:cursor(1),result:[{_id:'a',name:'A'}]}));
 x.c.receive(JSON.stringify({type:'replica.transaction',cursor:cursor(2),changes:[{entity:'tasks',id:'a',operation:'update',newValue:{_id:'a',name:'A2'}}]}));
 x.c.receive(JSON.stringify({type:'replica.snapshotPage',scope:directive.scope,visibilityScope:directive.visibilityScope,id:x.id,snapshotId:'pages',sequence:1,last:true,key:'_id',cursor:cursor(1),result:[],digest:digest([{_id:'a',name:'A'}])}));
 x.c.receive(JSON.stringify({type:'replica.ready',id:x.id,cursor:cursor(1),digest:digest([{_id:'a',name:'A'}])}));
 assert.equal(store.stats().windows,0);assert.deepEqual(x.w.localReplicaResult(),[{_id:'a',name:'A2'}]);
 x.c.receive(JSON.stringify({type:'replica.watermark',revision:2}));assert.equal(store.stats().windows,1);x.c.close();
 const fresh=await client(store);assert.deepEqual(fresh.sent.at(-1).opens[0].cursor,cursor(2));assert.deepEqual(fresh.w.localReplicaResult(),[{_id:'a',name:'A2'}]);fresh.c.close();store.clear();
});

test('native canonical JSON lexical numbers and shared immutable values produce SDK-model hashes',async()=>{
 const store=new ThinCheckpointStore(),x=await client(store,{nativeRows:true});
 x.c.receive(JSON.stringify({type:'replica.snapshotStart',scope:directive.scope,visibilityScope:directive.visibilityScope,id:x.id,snapshotId:'native',key:'_id',cursor:cursor(1)}));
 x.c.receive(JSON.stringify({type:'replica.snapshotEncodedPage',scope:directive.scope,visibilityScope:directive.visibilityScope,key:'_id',cursor:cursor(1),digest:digest([{_id:'a',name:1}]),id:x.id,snapshotId:'native',sequence:0,last:true,canonicalRows:['{ "_id": "a", "name": 1.0 }']}));
 x.c.receive(JSON.stringify({type:'replica.ready',id:x.id,cursor:cursor(1),digest:digest([{_id:'a',name:1}])}));
 assert.equal(x.w.status().isUpToDate,true);assert.equal(store.stats().windows,1);
 assert.throws(()=>{x.w.localReplicaResult()[0].name='mutated';},TypeError);
 x.c.close();const fresh=await client(store);assert.deepEqual(fresh.w.localReplicaResult(),[{_id:'a',name:1}]);fresh.c.close();store.clear();
});

test('canonical arguments are pinned before caller mutation and contract changes cannot alias a saved window',async()=>{
 const store=new ThinCheckpointStore(),args={b:{z:2,y:3},a:1},x=await client(store,{args});
 args.b.z=99;snapshot(x);x.c.close();
 const warm=await client(store,{args:{a:1,b:{y:3,z:2}}});
 assert.deepEqual(warm.sent.at(-1).opens[0].cursor,cursor(1));
 assert.throws(()=>warm.c.watchReplica({...ref,replica:{...ref.replica,maxRows:1}},{a:1,b:{z:2,y:3}}),/contract-changed/);
 warm.c.close();const other=await client(store,{args});assert.equal(other.sent.at(-1).opens[0].cursor,undefined);other.c.close();store.clear();
});
