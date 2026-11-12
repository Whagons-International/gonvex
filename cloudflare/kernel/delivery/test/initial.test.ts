import {expect,it} from 'vitest';
import {splitPayload} from '../engine';
import {initialDeliveryOnly,initialFailureOnly,initialFrameFields,validInitialCompletions,type InitialRequestRef,type ReplicaDelivery} from '../protocol';
const ref:InitialRequestRef={kind:'replica',id:'r',path:'items.all',epoch:4,generation:8};
const ready='{"type":"replica.ready","id":"r","cursor":{"epoch":"e","revision":7},"digest":"x","mode":"eager","path":"items.all","truncated":false}';
const scopePayload=(ops:import('../protocol').SocketOp[]):ReplicaDelivery=>({v:1,instance:'new',shared:[],sockets:{c:{subscriptionEpoch:4,initialScope:ref,ops}}});
it('parses only top-level metadata and preserves ordinary application id/path/reason values',()=>{
 const text=JSON.stringify({type:'replica.snapshotPage',snapshotId:'s',sequence:0,last:false,id:'r',result:[{id:'not-r',path:'wrong',reason:'initial',value:'\\"path\\":\\"evil\\"'}],path:'items.all'});
 expect(initialFrameFields(text)).toEqual({type:'replica.snapshotPage',id:'r',path:'items.all'});
 expect(initialDeliveryOnly(scopePayload([['f',text]]))).toBe(true);
 expect(initialDeliveryOnly(scopePayload([['i',0]]))).toBe(false);
});
it('remaps completion indices and retains the exact origin fence on every split',()=>{
 const payload=scopePayload([['f',JSON.stringify({type:'replica.snapshot',id:'r',path:'items.all',result:['x'.repeat(500)]})],['f',ready],['f','{"type":"replica.watermark","revision":7}']]);
 const work={...payload.sockets.c!,watermark:7,initialCompletions:[{request:ref,opIndex:1,outcome:'accepted' as const}]};
 const parts=splitPayload({...payload,sockets:{c:work}},200);
 expect(parts.length).toBeGreaterThan(1);
 const completed=parts.flatMap(p=>p.sockets.c?.initialCompletions??[]);
 expect(completed).toHaveLength(1);
 for(const part of parts){
  expect(part.sockets.c?.initialScope).toEqual(ref);expect(part.sockets.c?.subscriptionEpoch).toBe(4);
  expect(validInitialCompletions(part.sockets.c!)).toBe(true);
  expect(initialDeliveryOnly(part)).toBe(true);
  expect(part.sockets.c?.watermark).toBeUndefined();
  expect(part.sockets.c?.ops.some(op=>op[0]==='f'&&op[1].includes('replica.watermark'))).toBe(false);
 }
});
it('rejects duplicates, wrong id/path, unsafe refs and mixed normal commit work before application',()=>{
 const payload=scopePayload([['f',ready]]),completion={request:ref,opIndex:0,outcome:'accepted' as const};
 expect(initialDeliveryOnly({...payload,sockets:{c:{...payload.sockets.c!,initialCompletions:[completion]}}})).toBe(true);
 expect(initialDeliveryOnly({...payload,sockets:{c:{...payload.sockets.c!,initialCompletions:[completion,completion]}}})).toBe(false);
 expect(initialDeliveryOnly({...payload,sockets:{c:{...payload.sockets.c!,initialScope:{...ref,generation:NaN}}}})).toBe(false);
 expect(initialDeliveryOnly({...payload,membership:{revision:5,members:[],accounts:[]}})).toBe(false);
 expect(initialDeliveryOnly({...payload,watermark:5})).toBe(false);
 expect(initialDeliveryOnly(scopePayload([['f','{"type":"transaction","changes":[]}']]))).toBe(false);
 expect(initialDeliveryOnly({...payload,sockets:{c:{...payload.sockets.c!,initialCompletions:[{...completion,request:{...ref,path:'other'}}]}}})).toBe(false);
});
it('terminal-only multi-ref refusals require per-op binding and cannot carry progress',()=>{
 const request={kind:'query' as const,id:'q',path:'items.live',epoch:4,generation:9};
 const payload:ReplicaDelivery={v:1,instance:'not-proof',shared:[],sockets:{c:{subscriptionEpoch:4,ops:[['f','{"type":"query.error","id":"q","path":"items.live","error":"refused"}']],initialCompletions:[{request,opIndex:0,outcome:'refused'}]}}};
 expect(initialDeliveryOnly(payload)).toBe(true);
 expect(initialDeliveryOnly({...payload,sockets:{c:{...payload.sockets.c!,watermark:999}}})).toBe(false);
});

it('failure-only processing rejects duplicate generations, malformed refs and any ordinary envelope authority',()=>{
 const payload:ReplicaDelivery={v:1,instance:'no-proof',shared:[],sockets:{},initialFailure:{v:1,reason:'delivery-recovery-failed',requests:{c:[ref]}}};
 expect(initialFailureOnly(payload)).toBe(true);
 expect(initialFailureOnly({...payload,v:2})).toBe(false);
 expect(initialFailureOnly({...payload,scopeFence:1})).toBe(false);
 expect(initialFailureOnly({...payload,watermark:0})).toBe(false);
 expect(initialFailureOnly({...payload,initialFailure:{...payload.initialFailure!,requests:{c:[ref,{...ref,id:'new-id'}]}}})).toBe(false);
 expect(initialFailureOnly({...payload,initialFailure:{...payload.initialFailure!,requests:{c:[{...ref,epoch:-1}]}}})).toBe(false);
});
