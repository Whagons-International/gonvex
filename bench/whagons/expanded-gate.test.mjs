import test from 'node:test';
import assert from 'node:assert/strict';
import { replicaLifecycle, qualifyExpanded, visibilityDelta, cachedFullSet } from './expanded-gate.mjs';

test('SDK refused opens retain their origin through retries, hashes and late recovery', () => {
 const l=replicaLifecycle();
 l.frame(1,'send',{type:'replica.openMany',opens:[{id:'a',path:'tasks.all'}]},0);
 l.frame(1,'recv',{type:'replica.error',id:'a'},10);
 l.frame(1,'send',{type:'replica.open',id:'a'},260);
 l.frame(1,'recv',{type:'replica.needHashes',id:'a'},300);
 assert.deepEqual(l.events.map(e=>e.type),['refused','retry']);
 l.frame(1,'recv',{type:'replica.error',id:'a'},400);
 l.frame(1,'send',{type:'replica.openMany',opens:[{id:'a'}]},900);
 l.frame(1,'recv',{type:'replica.ready',id:'a'},1500);
 assert.equal(l.events.at(-1).failedAt,10);
 assert.equal(l.events.at(-1).type,'recovered');
 l.frame(2,'recv',{type:'replica.error',id:'a'},1600);
 assert.equal(l.events.length,5);
});
const good=()=>({reloadWindow:[100,105],samples:{reconnect:[{at:100,ms:50}]},targetUsers:2,reloads:[0,1].map(user=>({user,plannedAt:100,readyAt:200,cacheSupported:true,cachedWindows:132,cachedFullSetVerified:true})),permissions:[{granted:true,revoked:true,oldRowRemoved:true,additions:1,removals:1,grantMs:50,revokeMs:60}],capabilities:Object.fromEntries(['currentSQLitePair','currentAppOpens','fullPermissionCoverage','starterNegotiatedPipeline'].map(k=>[k,true]))});
test('expanded gate fails closed for missing capabilities and fake cached reloads',()=>{
 assert.equal(qualifyExpanded(good()).passed,true);
 for(const fault of ['capability','missing','cold','slow','oldRow','noop','error']){
  const x=good();
  if(fault==='capability')delete x.capabilities.starterNegotiatedPipeline;
  if(fault==='missing')x.reloads.pop();
  if(fault==='cold')x.reloads[0].cachedWindows=0;
  if(fault==='slow')x.reloads[0].readyAt=2000;
  if(fault==='oldRow')x.permissions[0].oldRowRemoved=false;
  if(fault==='noop')x.permissions[0].additions=0;
  if(fault==='error')x.reloads[0].error='refused';
  assert.equal(qualifyExpanded(x).passed,false,fault);
 }
});
test('permission deltas count additions and removals independently',()=>{
 assert.deepEqual(visibilityDelta(new Set(['old','shared']),new Set(['new','shared'])),{additions:1,removals:1});
});

test('deploy refresh retains late completions and unfinished/errors in its offered window',()=>{
 for(const fault of ['late','pending','error']){
  const x=good();
  if(fault==='late')x.samples.save=[{at:104,ms:3000}];
  if(fault==='pending')x.attempts=[{at:104,done:false}];
  if(fault==='error')x.errors=[{at:104}];
  assert.equal(qualifyExpanded(x).passed,false,fault);
 }
});

test('cached readiness requires every expected stored cursor and every visible row',()=>{
 const snapshot={liveQueries:{x:{entity:'tasks',key:'_id',completeness:'complete',cursor:{epoch:'e',revision:1},ids:['a']}},entities:{tasks:{a:{_id:'a'}}}};
 assert.equal(cachedFullSet(snapshot,['x']),true);
 assert.equal(cachedFullSet(snapshot,['x','missing']),false);
 delete snapshot.entities.tasks.a;
 assert.equal(cachedFullSet(snapshot,['x']),false);
 snapshot.entities.tasks.a={_id:'a'};delete snapshot.liveQueries.x.cursor;
 assert.equal(cachedFullSet(snapshot,['x']),false);
});
