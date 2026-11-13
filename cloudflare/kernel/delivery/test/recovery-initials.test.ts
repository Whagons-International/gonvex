// Pure bounded-offer/receipt validation; native sink/RPC and SDK behavior have separate tests.
import {expect,it} from 'vitest';
import {captureRecoveryInitials,recoveryInitialCleanup,RECOVERY_INITIAL_METADATA_BYTES} from '../recovery-initials';
import {validRecoveryInitialDelivery,type InitialRequestRef,type ReplicaDelivery} from '../protocol';
const ref=(generation:number):InitialRequestRef=>({kind:'query',id:'q'+generation,path:'items.live',epoch:2,generation});
function fixture(id='physical'){
 const owner={},refs=new Map([1,2,3].map(generation=>[generation,ref(generation)]));
 const entry={id,owner,epoch:2,refs};return {entry,refs,capture:captureRecoveryInitials([entry])};
}
it('captures immutable exact original identities and preserves explicit omitted/partial facts',()=>{
 const f=fixture('__proto__');
 expect(Object.hasOwn(f.capture.requests,'__proto__')).toBe(true);
 expect(Object.isFrozen(f.capture.requests.__proto__![0])).toBe(true);
 const facts=Object.fromEntries([['__proto__',[{generation:2,status:'enqueued'}]]]);
 expect(recoveryInitialCleanup(f.capture,facts,()=>f.entry)).toEqual([{id:'__proto__',request:ref(2)}]);
 expect(f.refs.size).toBe(3); // validation never mutates an owner book
 expect(recoveryInitialCleanup(f.capture,undefined,()=>f.entry)).toEqual([]);
 expect(recoveryInitialCleanup(f.capture,{},()=>f.entry)).toEqual([]);
});
it('validates the entire subset before permitting any cleanup, including foreign/duplicate/reverse facts',()=>{
 const f=fixture(),g=fixture('constructor'),capture=captureRecoveryInitials([f.entry,g.entry]);
 const current=(id:string)=>id==='physical'?f.entry:g.entry;
 const valid={generation:1,status:'enqueued'};
 expect(recoveryInitialCleanup(capture,{physical:[valid],constructor:[{generation:2,status:'obsolete'}]},current)).toHaveLength(2);
 for(const invalid of [
  {physical:[valid],constructor:[valid,valid]},
  {physical:[valid],constructor:[{generation:999,status:'enqueued'}]},
  {physical:[valid],foreign:[valid]},
  {physical:[valid],constructor:[{generation:2,status:'applied'}]},
  {physical:[valid],constructor:[{generation:NaN,status:'enqueued'}]},
 ])expect(recoveryInitialCleanup(capture,invalid,current)).toBeUndefined();
 expect(recoveryInitialCleanup(capture,{physical:[{generation:3,status:'enqueued'},{generation:1,status:'obsolete'}]},current)).toHaveLength(2);
 expect(f.refs.size).toBe(3);expect(g.refs.size).toBe(3);
});
it('refuses all cleanup after an owner/epoch/original-book change',()=>{
 const f=fixture(),facts={physical:[{generation:1,status:'enqueued'}]};
 expect(recoveryInitialCleanup(f.capture,facts,()=>({...f.entry,owner:{}}))).toBeUndefined();
 expect(recoveryInitialCleanup(f.capture,facts,()=>({...f.entry,epoch:3}))).toBeUndefined();
 f.refs.set(1,{...ref(1),id:'reused',path:'other'});
 expect(recoveryInitialCleanup(f.capture,facts,()=>f.entry)).toBeUndefined();
});
it('offers bounded metadata without truncating legal oversized references or enlarging reset credits',()=>{
 const huge={...ref(1),path:'p'.repeat(2_000_000)},owner={};
 const refs=new Map([ [1,huge],...Array.from({length:100},(_,i)=>[i+2,ref(i+2)] as const) ]);
 const capture=captureRecoveryInitials([{id:'c',owner,epoch:2,refs}]);
 expect(capture.bytes).toBeLessThanOrEqual(RECOVERY_INITIAL_METADATA_BYTES);
 expect(capture.offered).toBeGreaterThan(0);expect(capture.omitted).toBeGreaterThan(0);
 expect(capture.offered+capture.omitted).toBe(refs.size);
 expect(capture.requests.c!.some(r=>r.generation===1)).toBe(false);
 expect(refs.get(1)!.path).toBe(huge.path);
});
it('permits offers only on genuine empty whole recovery or scoped bootstrap, never normal data or mixed metadata',()=>{
 const whole:ReplicaDelivery={v:1,instance:'i',shared:[],sockets:{},recovery:{reason:'change-feed-lagged'},recoveryInitials:{c:[ref(1)]}};
 const scope:ReplicaDelivery={v:1,scopeFence:1,instance:'i',shared:[],sockets:{c:{ops:[],subscriptionEpoch:2,recoveryInitials:[ref(1)]}}};
 expect(validRecoveryInitialDelivery(whole)).toBe(true);expect(validRecoveryInitialDelivery(scope)).toBe(true);
 const {recovery:_recovery,...normal}=whole;
 expect(validRecoveryInitialDelivery(normal)).toBe(false);
 expect(validRecoveryInitialDelivery({...whole,watermark:5})).toBe(false);
 expect(validRecoveryInitialDelivery({...scope,sockets:{c:{...scope.sockets.c!,ops:[['f','{}']]}}})).toBe(false);
 expect(validRecoveryInitialDelivery({...scope,sockets:{c:{...scope.sockets.c!,initialCompletions:[]}}})).toBe(false);
 expect(validRecoveryInitialDelivery({...whole,recoveryInitials:{c:[ref(1),ref(1)]}})).toBe(false);
});
