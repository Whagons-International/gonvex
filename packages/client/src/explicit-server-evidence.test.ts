import {afterEach,expect,it,vi} from 'vitest';
import {GonvexClient,MemoryLocalReplicaStorage,type FunctionReference} from './index.js';
import {replicaRowsHashes,replicaHashesDigest} from './replica-integrity.js';
// SDK race tests only. Native generated-binding proof is separate.
class Socket {
 static OPEN=1;static CONNECTING=0;static CLOSING=2;static CLOSED=3;static instances:Socket[]=[];
 readyState=0;sent:any[]=[];listeners=new Map<string,Array<(e:any)=>void>>();
 constructor(readonly url:string){Socket.instances.push(this);}
 addEventListener(t:string,f:(e:any)=>void){this.listeners.set(t,[...this.listeners.get(t)??[],f]);}
 emit(t:string,e:any={}){for(const f of this.listeners.get(t)??[])f(e);}
 open(){this.readyState=1;this.emit('open');}close(){this.readyState=3;this.emit('close');}
 send(s:string){this.sent.push(JSON.parse(s));}receive(frame:any){this.emit('message',{data:JSON.stringify(frame)});}
}
const clients:GonvexClient[]=[];
afterEach(()=>{for(const c of clients.splice(0))c.close();Socket.instances=[];vi.unstubAllGlobals();});
const collection:FunctionReference={kind:'query',path:'board.all',delivery:'replica',replica:{table:'board',key:'id',columns:['id','title'],maxRows:10}};
const localRef:FunctionReference={kind:'reducer',path:'board.rename',localExecution:1,offline:{mode:'allowed'}};
const options={offline:'reject' as const,optimistic:[] as [],timeoutMs:1000,execution:'server' as const};
const directive={protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'module-definition'};
const identity={auth:{account:{id:'a'}},tenant:{id:'t'},member:{id:'m'}};
const tick=()=>new Promise<void>(resolve=>setTimeout(resolve,0));
const writes=(s:Socket)=>s.sent.filter(x=>x.type==='reducer.call');
const reads=(s:Socket)=>s.sent.filter(x=>x.type==='query.call'&&x.path==='__gonvex.reducerReceipt');
const result=(id:string)=>({type:'reducer.result',id,path:localRef.path,result:{renamed:true},originCommandId:id,committedRevision:4});
const transaction=(id:string,revision=4)=>({type:'replica.transaction',originCommandId:id,cursor:{epoch:'db',revision},provenance:{rootCommandId:id,channel:'ui',rootChannel:'ui',actorAccountId:'a',actorMemberId:'m'},changes:[{entity:'board',id:'row',operation:'update',newValue:{id:'row',title:'synthetic'}}]});
async function setup(config:{presence?:boolean;hash?:any;bindingHash?:string;binding?:boolean;storage?:MemoryLocalReplicaStorage;admissionObserver?:(c:GonvexClient)=>void;localObserver?:(c:GonvexClient)=>void}={}){
 const {bindingHash='artifact-one',binding=true,storage=new MemoryLocalReplicaStorage()}=config;const hash=Object.hasOwn(config,'hash')?config.hash:'artifact-one';
 vi.stubGlobal('WebSocket',Socket);const local={execute:0,replay:0};
 const runtime={mode:'portable' as const,artifactHash:bindingHash,tables:[],create:()=>({ready:Promise.resolve(),execute:async()=>{local.execute++;throw Error('PRIVATE local execution');},replay:async()=>{local.replay++;throw Error('PRIVATE local replay');},close:()=>{}})};
 const c=new GonvexClient('ws://controlled.invalid/ws',{project:'p',tenant:'t',token:'one',identity:{iss:'fixture',sub:'a'},errorReporting:false,localDependencyResidency:'off',clientContract:{version:3,offlineMaxAgeMs:null},localReplica:{storage},...(binding?{localRuntime:runtime}:{})});clients.push(c);
 if(config.admissionObserver)c.onTenantAdmissionChange(()=>config.admissionObserver!(c));if(config.localObserver)c.onLocalSessionChange(()=>config.localObserver!(c));
 const w=c.watchReplica(collection,{}),s=Socket.instances.at(-1)!;s.open();s.receive({type:'session.ready',capabilities:{replicaWatermark:1,reducerReceiptLookup:1,...(config.presence?{presence:1}:{})},replica:directive});
 const auth=s.sent.find(x=>x.type==='auth');s.receive({type:'auth.result',id:auth.id,result:{replica:directive,localIdentity:identity,...(hash===undefined?{}:{artifactHash:hash})}});
 await vi.waitFor(()=>expect(s.sent.some(x=>x.type==='replica.open')).toBe(true));
 const open=s.sent.filter(x=>x.type==='replica.open').at(-1),hashes=await replicaRowsHashes([],'id'),digest=await replicaHashesDigest(hashes);
 s.receive({type:'replica.snapshot',id:open.id,path:collection.path,result:[],cursor:{epoch:'db',revision:3},key:'id',hashes,digest,truncated:false});s.receive({type:'replica.ready',id:open.id,path:collection.path,cursor:{epoch:'db',revision:3},digest,truncated:false});s.receive({type:'replica.watermark',revision:3});
 await vi.waitFor(()=>{expect(c.localSessionState()).toBe('authenticated');expect(w.localReplicaState()?.isUpToDate).toBe(true);});await tick();return {c,s,w,open,local,runtime,auth};
}
const begin=(c:GonvexClient,ref=localRef,args:any={id:'row',title:'synthetic'},opts:any=options)=>c.reducerWithTransactionEvidence(ref,args,opts).then(value=>({value,error:undefined as any}),error=>({value:undefined,error}));
async function finish(s:Socket,id:string){s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});await vi.waitFor(()=>expect(reads(s)).toHaveLength(1));const q=reads(s)[0];s.receive({type:'query.result',id:q.id,path:q.path,result:{found:true,cursor:{epoch:'db',revision:5}}});}

function initialNotification(w: Awaited<ReturnType<typeof setup>>['w'], handler: () => void) {
 const callbacks: Array<() => void> = [];
 const queue = vi.spyOn(globalThis, 'queueMicrotask').mockImplementation(callback => callbacks.push(callback));
 let stop: () => void;
 try { stop = w.onUpdate(handler); } finally { queue.mockRestore(); }
 expect(callbacks).toHaveLength(1);
 return { run: callbacks[0], stop: stop! };
}

it('queued initial replica observer failure fences a sent write without an uncaught exception or receipt', async () => {
 const {c,s,w} = await setup();
 const notification = initialNotification(w, () => { throw Error('PRIVATE initial observer'); });
 const pending = begin(c), id = writes(s)[0].id;
 try {
 expect(() => notification.run()).not.toThrow();
 const outcome = await pending;
 expect(outcome.error).toMatchObject({outcome:'unknown',possibleWriteApplied:true});
 expect(JSON.stringify(outcome)).not.toContain('PRIVATE');
 expect(c.connectionState().isWebSocketConnected).toBe(false);
 expect(s.readyState).toBe(Socket.CLOSED);
 s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
 await tick();
 expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(0);
 expect((await begin(c)).error).toMatchObject({possibleWriteApplied:false});
 expect(writes(s)).toHaveLength(1);
 } finally { notification.stop(); }
});

it('queued initial replica observer failure cannot close a connection replaced by that observer', async () => {
 const {c,s,w} = await setup();let acted = false;
 const notification = initialNotification(w, () => {
  if (acted) return;
  acted = true;s.close();c.connect();Socket.instances.at(-1)!.open();
  throw Error('PRIVATE old observer');
 });
 expect(() => notification.run()).not.toThrow();
 const replacement = Socket.instances.at(-1)!;
 expect(replacement).not.toBe(s);
 expect(replacement.readyState).toBe(Socket.OPEN);
 expect(c.connectionState().isWebSocketConnected).toBe(true);
 expect(writes(replacement)).toHaveLength(0);expect(reads(replacement)).toHaveLength(0);
 notification.stop();
});

it('two failing initial replica observers cannot escape through synchronous close recovery', async () => {
 const {c,s,w} = await setup();
 const first = initialNotification(w, () => { throw Error('PRIVATE first observer'); });
 const second = initialNotification(w, () => { throw Error('PRIVATE second observer'); });
 const pending = begin(c);
 try {
  expect(() => first.run()).not.toThrow();expect(() => second.run()).not.toThrow();
  expect((await pending).error).toMatchObject({outcome:'unknown',possibleWriteApplied:true});
  expect(c.connectionState().isWebSocketConnected).toBe(false);
  expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(0);
 } finally { first.stop();second.stop(); }
});

it('unsubscribing before the queued initial notification prevents the observer from running', async () => {
 const {c,w} = await setup();const handler = vi.fn();
 const notification = initialNotification(w, handler);notification.stop();notification.run();
 expect(handler).not.toHaveBeenCalled();expect(c.connectionState().isWebSocketConnected).toBe(true);
});

it('a synchronous replica observer failure still refuses actual transaction confirmation', async () => {
 const {c,s,w} = await setup();let fail = false;
 const stop = w.onUpdate(() => { if (fail) throw Error('PRIVATE transaction observer'); });
 await tick();const pending = begin(c), id = writes(s)[0].id;fail = true;
 s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
 try {
 expect((await pending).error).toMatchObject({outcome:'unknown',possibleWriteApplied:true});
 expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(0);
 } finally { fail = false;stop(); }
});
for(const order of ['result-first','transaction-first'])it(`explicit ${order}: one keyed direct command, fresh entropy, exact auth hash, no command contract`,async()=>{
 const {c,s,local}=await setup();const before={...local};const p=begin(c);const call=writes(s)[0];expect(call).toBeDefined();
 expect(call.idempotencyKey).toBe(call.id);expect(call.receiptPath).toBe(localRef.path);expect(call.artifactHash).toBe('artifact-one');expect(call.intentEntropy).toMatch(/^[0-9a-f]{64}$/);expect(Object.hasOwn(call,'clientContract')).toBe(false);expect(s.sent.find(x=>x.type==='auth').clientContract).toBe(3);
 if(order==='result-first'){s.receive(result(call.id));s.receive(transaction(call.id));}else{s.receive(transaction(call.id));s.receive(result(call.id));}
 s.receive({type:'replica.watermark',revision:4});await vi.waitFor(()=>expect(reads(s)).toHaveLength(1));const q=reads(s)[0];s.receive({type:'query.result',id:q.id,path:q.path,result:{found:true,cursor:{epoch:'db',revision:5}}});
 expect((await p).value?.confirmation.cursor).toEqual({epoch:'db',revision:4});expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(1);expect(local).toEqual(before);expect(localRef.localExecution).toBe(1);
});
for(const hash of [undefined,null,0,true,'',' ','a\nb','x'.repeat(1025)])it(`invalid auth artifact ${typeof hash}/${String(hash).length} never authorizes explicit execution`,async()=>{
 const x=await setup({hash});expect((await begin(x.c)).error).toMatchObject({reason:'precondition',possibleWriteApplied:false});expect(writes(x.s)).toHaveLength(0);
});
for(const extra of [{execution:'local'},{execution:null},{artifactHash:'artifact-one'},{identity:identity},{entropy:'00'.repeat(32)},{idempotencyKey:'caller'},{proof:true},{callback:()=>{throw Error('PRIVATE callback');}}])it(`rejects unknown option ${Object.keys(extra)[0]}`,async()=>{
 const {c,s}=await setup();expect((await begin(c,localRef,{}, {...options,...extra})).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
for(const ref of [{...localRef,localExecution:2},{...localRef,localExecution:null},{...localRef,optimistic:{}},{...localRef,intentEntropy:'00'.repeat(32)},{...localRef,artifactHash:'other'},{...localRef,receiptPath:'other'}])it(`refuses incompatible ref ${JSON.stringify(ref)}`,async()=>{
 const {c,s}=await setup();expect((await begin(c,ref as any)).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
for(const state of [{binding:false},{bindingHash:'other'},{bindingHash:''}])it(`missing or mismatched generated binding ${JSON.stringify(state)}`,async()=>{
 const {c,s}=await setup(state);expect((await begin(c)).error).toMatchObject({reason:'precondition',possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
it('option absence still rejects local refs; ordinary direct payload keeps its contract and caller entropy behavior',async()=>{
 const {c,s}=await setup();const old={offline:'reject' as const,optimistic:[] as [],timeoutMs:1000};expect((await begin(c,localRef,{},old)).error).toMatchObject({possibleWriteApplied:false});
 const ref={kind:'reducer',path:localRef.path,artifactHash:'ordinary-hash',intentEntropy:'ab'.repeat(32)};const p=begin(c,ref,{},old);const call=writes(s)[0];expect(call.clientContract).toBe(3);expect(call.artifactHash).toBe('ordinary-hash');expect(call.intentEntropy).toBe('ab'.repeat(32));await finish(s,call.id);expect((await p).value).toBeDefined();
});
it('entropy failure is private and sends nothing',async()=>{
 const {c,s}=await setup();const real=globalThis.crypto;vi.stubGlobal('crypto',{randomUUID:()=>real.randomUUID(),getRandomValues:()=>{throw Error('PRIVATE entropy');}});const r=await begin(c);expect(r.error).toMatchObject({possibleWriteApplied:false});expect(JSON.stringify(r)).not.toContain('PRIVATE');expect(writes(s)).toHaveLength(0);
});
for(const change of ['reload','auth','ref','binding'])it(`serialization ${change} refuses before send`,async()=>{
 const {c,s,runtime}=await setup();const ref={...localRef};const args={toJSON(){if(change==='reload')s.receive({type:'system.reload',artifactHash:'artifact-one'});if(change==='auth')c.setAuth({token:'other'});if(change==='ref')ref.localExecution=2 as any;if(change==='binding')runtime.artifactHash='other';return {};}};
 const r=await begin(c,ref,args);expect(r.error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
for(const event of ['reload','same-hash-reload','auth-start','socket-error','close','epoch','reset','reset-local','tenant','account','project','unsubscribe'])it(`explicit pending ${event} stays UNKNOWN and never retries after late evidence`,async()=>{
 const {c,s,open,w}=await setup();const p=begin(c),id=writes(s)[0].id;
 if(event.includes('reload'))s.receive({type:'system.reload',artifactHash:event==='same-hash-reload'?'artifact-one':'other'});
 if(event==='auth-start')c.setAuth({token:'other'});if(event==='tenant')c.setAuth({tenant:'other'});if(event==='project')c.setAuth({project:'other'});if(event==='account')c.setAuth({identity:{iss:'fixture',sub:'other'}});if(event==='reset-local')void c.resetLocalReplica().catch(()=>undefined);if(event==='socket-error')s.emit('error');if(event==='close')s.close();if(event==='epoch')s.receive({type:'replica.ready',id:open.id,path:collection.path,cursor:{epoch:'other',revision:4}});if(event==='reset')s.receive({type:'replica.reset',id:open.id,path:collection.path,reason:'test'});if(event==='unsubscribe'){const stop=w.onUpdate(()=>{});stop();await tick();}
 s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});const r=await p;expect(r.error).toMatchObject({outcome:'unknown',possibleWriteApplied:true});expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(0);
});
it('a reload advertisement cannot restore auth authority before a new accepted auth result',async()=>{
 const {c,s}=await setup();s.receive({type:'system.reload',artifactHash:'artifact-one'});await tick();expect(c.activeArtifactHash()).toBe('artifact-one');expect((await begin(c)).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
for(const hash of [undefined,null,'other'])it(`new accepted auth ${hash} cannot inherit the old artifact`,async()=>{
 const {c,s}=await setup();const ready=c.authenticate({token:'second'});const auth=s.sent.filter(x=>x.type==='auth').at(-1);s.receive({type:'auth.result',id:auth.id,result:{replica:directive,localIdentity:identity,...(hash===undefined?{}:{artifactHash:hash}),subscriptionsPreserved:true}});await ready;await tick();expect((await begin(c)).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
it('stale auth cannot grant a new artifact; only accepted latest matching auth can restore proof',async()=>{
 const {c,s,auth}=await setup();const ready=c.authenticate({token:'second'});const latest=s.sent.filter(x=>x.type==='auth').at(-1);s.receive({type:'auth.result',id:auth.id,result:{artifactHash:'artifact-one',replica:directive,localIdentity:identity}});expect((await begin(c)).error).toMatchObject({possibleWriteApplied:false});
 s.receive({type:'auth.result',id:latest.id,result:{artifactHash:'artifact-one',replica:directive,localIdentity:identity,subscriptionsPreserved:true}});await ready;await tick();const p=begin(c);expect(writes(s)).toHaveLength(1);await finish(s,writes(s)[0].id);expect((await p).value).toBeDefined();
});
class HeldStorage extends MemoryLocalReplicaStorage{hold=false;entered=false;release=()=>{};override async applyTransaction(...args:Parameters<MemoryLocalReplicaStorage['applyTransaction']>){if(this.hold){this.entered=true;await new Promise<void>(r=>{this.release=r;});}return super.applyTransaction(...args);}}
for(const invalidation of ['reload','timeout'])it(`held storage ${invalidation} cannot become success or cancellation`,async()=>{
 const storage=new HeldStorage(),{c,s}=await setup({storage});storage.hold=true;const p=begin(c,localRef,{}, {...options,timeoutMs:invalidation==='timeout'?40:1000});const id=writes(s)[0].id;s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});await vi.waitFor(()=>expect(storage.entered).toBe(true));if(invalidation==='reload')s.receive({type:'system.reload',artifactHash:'artifact-one'});const r=await p;expect(r.error).toMatchObject({possibleWriteApplied:true,outcome:'unknown'});storage.release();await tick();expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(0);
});
for(const prior of [4,5])it(`explicit matching R4 skipped after R${prior} cannot be acknowledged as applied`,async()=>{
 const {c,s}=await setup();const p=begin(c),id=writes(s)[0].id;s.receive(transaction('unrelated',prior));s.receive({type:'replica.watermark',revision:prior});await tick();s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:prior});expect((await p).error).toMatchObject({outcome:'unknown'});expect(writes(s)).toHaveLength(1);expect(reads(s)).toHaveLength(0);
});

for(const observer of ['admission','local'])it(`observer at ${observer} publication cannot send before artifact capture`,async()=>{
 const attempts:Array<ReturnType<typeof begin>>=[];const collect=(c:GonvexClient)=>{if(c.tenantAdmission())attempts.push(begin(c));};
 const {c,s}=await setup(observer==='admission'?{admissionObserver:collect}:{localObserver:collect});
 expect(attempts.length).toBeGreaterThan(0);for(const p of attempts)expect((await p).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
 const p=begin(c);expect(writes(s)).toHaveLength(1);await finish(s,writes(s)[0].id);expect((await p).value).toBeDefined();
});
it('observer auth replacement after actual storage application cannot cross the receipt barrier',async()=>{
 class ObservedStorage extends MemoryLocalReplicaStorage {after=()=>{};applied:number[]=[];override async applyTransaction(...args:Parameters<MemoryLocalReplicaStorage['applyTransaction']>){const result=await super.applyTransaction(...args);this.applied.push(args[0].cursor.revision);this.after();return result;}}
 const storage=new ObservedStorage(),{c,s}=await setup({storage});let replaced=false;storage.after=()=>{replaced=true;c.setAuth({token:'observer-replacement'});};
 const p=begin(c),id=writes(s)[0].id;s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});expect((await p).error).toMatchObject({possibleWriteApplied:true,outcome:'unknown'});expect(replaced).toBe(true);expect(storage.applied).toEqual([4]);expect(reads(s)).toHaveLength(0);expect(writes(s)).toHaveLength(1);
});
it('new auth missing admission identity cannot reuse an earlier admitted principal',async()=>{
 const {c,s}=await setup();const ready=c.authenticate({token:'second'}).catch(()=>undefined),auth=s.sent.filter(x=>x.type==='auth').at(-1);s.receive({type:'auth.result',id:auth.id,result:{artifactHash:'artifact-one',replica:directive,subscriptionsPreserved:true}});await ready;await tick();expect((await begin(c)).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
for(const fault of ['duplicate-result','duplicate-transaction','missing-transaction','actor','root'])it(`explicit ${fault} never obtains a receipt or a retry`,async()=>{
 const {c,s}=await setup();const p=begin(c,localRef,{}, {...options,timeoutMs:50}),id=writes(s)[0].id;s.receive(result(id));
 if(fault==='duplicate-result')s.receive(result(id));
 if(fault==='duplicate-transaction'){s.receive(transaction(id));s.receive(transaction(id));}
 if(fault==='actor')s.receive({...transaction(id),provenance:{...transaction(id).provenance,actorMemberId:'foreign'}});
 if(fault==='root')s.receive({...transaction(id),provenance:{...transaction(id).provenance,rootCommandId:'foreign'}});
 s.receive({type:'replica.watermark',revision:4});expect((await p).error).toMatchObject({outcome:'unknown',possibleWriteApplied:true});expect(reads(s)).toHaveLength(0);expect(writes(s)).toHaveLength(1);
});

for(const extra of ['hidden','symbol'])it(`explicit unknown ${extra} option keys refuse`,async()=>{
 const {c,s}=await setup();const opts={...options};Object.defineProperty(opts,extra==='symbol'?Symbol('private'): 'privateUnknown',{value:()=>{throw Error('PRIVATE callback');},enumerable:false});expect((await begin(c,localRef,{},opts)).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
for(const metadata of [{intentEntropy:undefined},{artifactHash:undefined}])it(`present undefined ref metadata ${Object.keys(metadata)[0]} is not authority`,async()=>{
 const {c,s}=await setup();expect((await begin(c,{...localRef,...metadata} as any)).error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
it('option-absent direct evidence does not start reading a local binding hash',async()=>{
 const {c,s,runtime}=await setup();Object.defineProperty(runtime,'artifactHash',{get(){throw Error('PRIVATE binding getter');}});const p=begin(c,{kind:'reducer',path:localRef.path},{},{offline:'reject',optimistic:[],timeoutMs:1000});expect(writes(s)).toHaveLength(1);await finish(s,writes(s)[0].id);expect((await p).value).toBeDefined();
});
it('explicit proof never enters generic send/call/local/outbox/reconnect helpers',async()=>{
 const {c,s}=await setup();const names=['send','call','reducer','runLocalReducer','connect','requestReauthentication','drainOutbox'];const spies=names.map(name=>vi.spyOn(c as any,name));
 try {const p=begin(c);expect(writes(s)).toHaveLength(1);await finish(s,writes(s)[0].id);expect((await p).value).toBeDefined();for(const spy of spies)expect(spy).not.toHaveBeenCalled();}
 finally {for(const spy of spies)spy.mockRestore();}
});

it('auth change during receipt serialization refuses before the second send',async()=>{
 const {c,s}=await setup();const p=begin(c),id=writes(s)[0].id;const original=JSON.stringify;let serialized=0;
 const spy=vi.spyOn(JSON,'stringify').mockImplementation(((value:any,...args:any[])=>{const payload=Reflect.apply(original,JSON,[value,...args]);if(value?.type==='query.call'&&value.path==='__gonvex.reducerReceipt'){serialized++;c.setAuth({token:'receipt-serialization-replacement'});}return payload;}) as typeof JSON.stringify);
 try {s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});expect((await p).error).toMatchObject({outcome:'unknown',possibleWriteApplied:true});expect(serialized).toBe(1);expect(reads(s)).toHaveLength(0);expect(writes(s)).toHaveLength(1);}
 finally {spy.mockRestore();}
});

it('synchronous close recovery rejects ordinary sent reducers before observer notifications',async()=>{
 const {c,s,w}=await setup();const ordinaryRef:FunctionReference={kind:'reducer',path:'board.ordinary'};
 let observed:any;const p=c.reducer(ordinaryRef,{},{timeoutMs:1000}).then(v=>observed={value:v},e=>observed={error:e});
 const a=initialNotification(w,()=>{throw Error('initial');}),b=initialNotification(w,()=>{throw Error('recovery');});
 try{
  expect(()=>a.run()).not.toThrow();await tick();expect(s.readyState).toBe(Socket.CLOSED);
  expect(observed?.error?.code).toBe('disconnected');
 }finally{a.stop();b.stop();c.close();await p;}
});

for (const throwing of [false, true]) it(`presence disconnect: clears admission before ${throwing ? 'throwing' : 'reconnecting'} presence observers`, async () => {
 const {c,s,w} = await setup({presence:true});let armed=false,handled=false;let observed:unknown='not called';let replacement:Socket|undefined;
 const pending=c.reducer({kind:'reducer',path:'board.ordinary'},{},{timeoutMs:1000}).catch(error=>error);
 const stop=c.subscribePresence(()=>{
  if(!armed||handled||c.presenceSnapshot().ready)return;
  handled=true;observed=c.tenantAdmission();
  if(throwing)throw Error('PRIVATE presence observer');
  c.connect();replacement=Socket.instances.at(-1)!;replacement.open();
 });
 const subscribe=s.sent.find(x=>x.type==='presence.subscribe');expect(subscribe).toBeDefined();
 s.receive({type:'presence.snapshot',id:subscribe.id,members:[]});expect(c.presenceSnapshot().ready).toBe(true);
 expect(c.tenantAdmission()?.memberId).toBe('m');armed=true;
 const notification=throwing?initialNotification(w,()=>{throw Error('PRIVATE initial observer');}):undefined;
 try{
  if(notification)expect(()=>notification.run()).not.toThrow();else s.close();
  expect(handled).toBe(true);expect(observed).toBeUndefined();expect(c.tenantAdmission()).toBeUndefined();
  expect((await pending).code).toBe('disconnected');
  if(replacement){expect(replacement).not.toBe(s);expect(replacement.readyState).toBe(Socket.OPEN);expect(c.connectionState().isWebSocketConnected).toBe(true);expect(writes(replacement)).toHaveLength(0);}
 }finally{notification?.stop();stop();}
});

it('presence disconnect: admission observer replacement keeps its newly accepted identity', async () => {
 const {c,s}=await setup({presence:true});let armed=false,handled=false;let replacement:Socket|undefined;
 const stop=c.onTenantAdmissionChange(()=>{
  if(!armed||handled||c.tenantAdmission())return;
  handled=true;c.connect();replacement=Socket.instances.at(-1)!;replacement.open();
  replacement.receive({type:'session.ready',capabilities:{replicaWatermark:1,reducerReceiptLookup:1,presence:1},replica:directive});
  const auth=replacement.sent.find(x=>x.type==='auth');
  replacement.receive({type:'auth.result',id:auth.id,result:{replica:directive,artifactHash:'artifact-one',localIdentity:{...identity,member:{id:'replacement-member'}}}});
 });
 armed=true;
 try{s.close();await vi.waitFor(()=>expect(c.tenantAdmission()?.memberId).toBe('replacement-member'));expect(handled).toBe(true);expect(replacement?.readyState).toBe(Socket.OPEN);expect(c.connectionState().isWebSocketConnected).toBe(true);}
 finally{stop();}
});
