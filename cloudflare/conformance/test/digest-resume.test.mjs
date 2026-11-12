import {test} from 'node:test';
import assert from 'node:assert/strict';
import {checkDigests,expectedDigest,rowHash} from '../src/digest-check.mjs';
const rows=[{id:'a',value:'ordinary digest/cursor app string'}];
const digest=expectedDigest(new Map(rows.map(r=>[r.id,rowHash(r)])),'xor1');
const cursor={epoch:'E',revision:4};
const rec=(dir,frame,socket=1)=>({dir,frame,socket});
const auth=rec('in',{type:'auth.result',result:{projectId:'p',tenantId:'t',accountId:'a',replica:{scope:'scope',visibilityScope:'visibility'}}});
const open=rec('out',{type:'replica.open',id:'s',path:'board.all',args:{tag:'ordinary',digest:'app-value'}});
const snapshot=rec('in',{type:'replica.snapshot',id:'s',result:rows,key:'id',cursor});
const ready=rec('in',{type:'replica.ready',id:'s',digest,cursor,truncated:false});
const baseline=[auth,open,snapshot,ready];
const resume=(extra={},socket=1)=>rec('out',{...open.frame,cursor,digest,...extra},socket);
const check=extra=>checkDigests([...baseline,...extra]);
test('0063 same physical socket and scope reuses only recomputed complete rows; every subsequent digest remains checked',()=>{
 assert.deepEqual(check([rec('in',{type:'system.reload'}),resume(),ready]),{checked:2,problems:[]});
 const wrong={...ready.frame,digest:'xor1:'+'f'.repeat(64)};
 const r=check([resume(),rec('in',wrong)]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/digest mismatch/);
});
test('0063 watermark can advance a known epoch cursor but cannot invent an unseen revision',()=>{
 const watermark=rec('in',{type:'replica.watermark',revision:9});
 assert.deepEqual(check([watermark,resume({cursor:{epoch:'E',revision:8}}),rec('in',{...ready.frame,cursor:{epoch:'E',revision:9}})]),{checked:2,problems:[]});
 for(const c of [{epoch:'other',revision:4},{epoch:'E',revision:3},{epoch:'E',revision:10}]){
  const r=check([watermark,resume({cursor:c}),ready]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/unproven resume baseline/);
 }
});
for(const [label,change]of Object.entries({id:{id:'new'},path:{path:'other'},args:{args:{tag:'different'}},descriptor:{budget:1},digest:{digest:'xor1:'+'f'.repeat(64)}}))test(`0063 ${label} changes cannot borrow another request's rows`,()=>{
 const r=check([resume(change),rec('in',{...ready.frame,id:change.id??'s'})]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/unproven resume baseline/);
});
test('0063 new physical socket cannot borrow prior rows',()=>{
 const r=check([resume({},2),rec('in',ready.frame,2)]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/unproven resume baseline/);
});
for(const frame of [{type:'auth.result',result:{...auth.frame.result,accountId:'b'}},{type:'auth.error',id:'membership-changed',error:'revoked'},{type:'replica.reset',id:'s'},{type:'replica.closed',id:'s'}])test(`0063 ${frame.type} invalidates prior cache authority`,()=>{
 const r=check([rec('in',frame),resume(),ready]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/unproven resume baseline/);
});
test('0063 a preserved auth only retains the exact same authorization descriptor',()=>{
 const same=rec('in',{type:'auth.result',result:{...auth.frame.result,subscriptionsPreserved:true}});
 assert.deepEqual(check([same,resume(),ready]),{checked:2,problems:[]});
 const changed=rec('in',{type:'auth.result',result:{...auth.frame.result,accountId:'changed',subscriptionsPreserved:true}});
 assert.match(check([changed,resume(),ready]).problems.join('\n'),/unproven resume baseline/);
});
test('0063 partial or truncated windows cannot become verified resume caches',()=>{
 const partial=rec('in',{type:'replica.snapshotPage',id:'s',sequence:0,last:false,result:rows,cursor});
 assert.match(check([partial,resume(),ready]).problems.join('\n'),/unproven resume baseline/);
 const truncated=rec('in',{...ready.frame,truncated:true});
 assert.match(check([truncated,resume(),ready]).problems.join('\n'),/unproven resume baseline/);
});
test('0063 explicit keys and hashes take precedence, including an empty claimed set',()=>{
 const emptyDigest=expectedDigest(new Map(),'xor1');
 const emptyReady=rec('in',{...ready.frame,digest:emptyDigest});
 assert.deepEqual(check([resume({keys:[],hashes:{},digest:emptyDigest}),emptyReady]),{checked:2,problems:[]});
 const r=check([resume({keys:[],hashes:{},digest}),ready]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/digest mismatch/);
});
test('0063 a fresh full snapshot can establish rows after an unverifiable resume without skipping verification',()=>{
 assert.deepEqual(check([resume({args:{changed:true}}),snapshot,ready]),{checked:2,problems:[]});
});
test('0063 metadata without authorization scope cannot certify cache reuse',()=>{
 const missing=rec('in',{type:'auth.result',result:{projectId:'p',tenantId:'t',accountId:'a'}});
 const r=checkDigests([missing,open,snapshot,ready,resume(),ready]);assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/unproven resume baseline/);
});
test('0063 a drifted delta after verified resume still fails and client close invalidates its cache',()=>{
 const r=check([resume(),rec('in',{type:'replica.delta',id:'s',upserts:[{id:'a',value:'changed'}],deleted:[],cursor:{epoch:'E',revision:5},digest})]);
 assert.equal(r.checked,2);assert.match(r.problems.join('\n'),/digest mismatch/);
 const closed=check([rec('out',{type:'replica.close',id:'s'}),resume(),ready]);assert.equal(closed.checked,2);assert.match(closed.problems.join('\n'),/unproven resume baseline/);
});
