import test from 'node:test';
import assert from 'node:assert/strict';
import { sdkApplication } from '../checks/sdk.mjs';

// Adapter unit boundary only. These callbacks are tripwires, not real app proof.
const references = () => ({create:{kind:'reducer',path:'tasks.create'},update:{kind:'reducer',path:'tasks.rename'},remove:{kind:'reducer',path:'tasks.remove'},read:{kind:'query',path:'eligible.oneShot'},collection:{kind:'query',path:'tasks.get',delivery:'replica'}});
const blocked = code => e => e.code===code && e.status==='blocked';
const config = (refs, connect = () => { throw Error('connection must not run'); }) => ({refs,connect,argumentsFor:()=>({}),matches:()=>false,durableEvidence:()=>({}),readEvidence:()=>({})});

for (const role of ['create','update','remove']) for (const metadata of [{localExecution:1,offline:{mode:'allowed'}},{optimistic:{transaction:{}}}]) {
  test(`generated ${role} execution metadata refuses before authentication or effects: ${Object.keys(metadata)[0]}`,()=>{
    const refs=references();Object.assign(refs[role],metadata);let callbacks=0;
    const tripwire=()=>{callbacks++;throw Error('PRIVATE callback error');};
    assert.throws(()=>sdkApplication({...config(refs,tripwire),argumentsFor:tripwire,matches:tripwire,durableEvidence:tripwire,readEvidence:tripwire}),blocked('sdk-server-execution-unavailable'));
    assert.equal(callbacks,0);assert.deepEqual(Object.fromEntries(Object.keys(metadata).map(k=>[k,refs[role][k]])),metadata);
  });
}
for(const kind of ['query','action']) test(`wrong write function kind ${kind} refuses before connect`,()=>{
  const refs=references();refs.create.kind=kind;assert.throws(()=>sdkApplication(config(refs)),blocked('sdk-write-kind'));
});
test('real replica-delivery read mismatch stays the first fail-closed prerequisite',()=>{
  const refs=references();refs.read=refs.collection;refs.create.localExecution=1;
  assert.throws(()=>sdkApplication(config(refs)),blocked('sdk-read-delivery'));
});
for (const phase of ['before-connect','during-auth','arguments']) for (const operation of ['mutate','cleanup']) test(`${operation} rechecks metadata changed ${phase}; no SDK mutation`,async()=>{
  const refs=references();let connections=0,writes=0,closes=0;
  const selected=operation==='mutate'?refs.create:refs.remove;
  const app=sdkApplication({...config(refs,async()=>{
    connections++;if(phase==='during-auth')selected.localExecution=1;
    return {localSessionState:()=> 'authenticated',reducer:()=>{writes++;throw Error('PRIVATE reducer must not run');},close:()=>{closes++;}};
  }),argumentsFor:()=>{if(phase==='arguments')selected.optimistic={transaction:{}};return {};}});
  if(phase==='before-connect')selected.localExecution=1;
  await assert.rejects(async()=>operation==='mutate'?app.mutate('writer',{kind:'create'}):app.cleanup('writer',{timeoutMs:100}),blocked('sdk-server-execution-unavailable'));
  assert.equal(writes,0);assert.equal(connections,phase==='before-connect'?0:1);assert.equal(closes,connections);
});
test('supported adapter refs reach ordinary SDK unchanged; result and cleanup semantics stay ordinary',async()=>{
  const refs=references(),calls=[],args={synthetic:'unit-only'};let closes=0;
  const app=sdkApplication({...config(refs,async()=>({localSessionState:()=> 'authenticated',reducer:(ref,actual,options)=>{calls.push({ref,actual,options});return {applicationResult:'not-commit-proof'};},close:()=>{closes++;}})),argumentsFor:()=>args,durableEvidence:()=>({durable:true,revision:7})});
  assert.deepEqual(await app.mutate('writer',{kind:'create',operationId:'unit-op'}),{durable:true,revision:7,operationId:'unit-op'});
  assert.equal(await app.cleanup('writer',{timeoutMs:321}),true);
  assert.strictEqual(calls[0].ref,refs.create);assert.strictEqual(calls[1].ref,refs.remove);
  assert.strictEqual(calls[0].actual,args);assert.strictEqual(calls[1].actual,args);
  assert.deepEqual(calls[0].options,{offline:'reject',optimistic:[],timeoutMs:10000});
  assert.deepEqual(calls[1].options,{offline:'reject',optimistic:[],timeoutMs:321});assert.equal(closes,2);
});
