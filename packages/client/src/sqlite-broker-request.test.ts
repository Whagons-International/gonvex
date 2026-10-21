import { expect, it, vi } from 'vitest';
import { SQLiteReplicaBroker } from './sqlite-broker.js';

// Exercise public admission without a browser election. Transport dispatch is
// replaced at its boundary; real multi-tab tests cover the actual transports.
function client() {
  const broker = Object.assign(Object.create(SQLiteReplicaBroker.prototype), {
    closed: false, failed: false,cooldown:0, changeListeners:new Set(),lastChanges:new Map(),lastChangeBytes:0, pending: new Map(), inflight: new Map(), inflightBytes: 0, pendingBytes: 0, serial: 0, tabID: 'test',
    send: vi.fn((call: any) => {
      clearTimeout(call.timer); broker.pending.delete(call.id); broker.pendingBytes -= call.bytes;
      call.resolve({ accepted: call.op, args: call.args });
    }),
  }) as SQLiteReplicaBroker;
  return broker;
}
it('dispatches native view pages through the public broker admission path', async () => {
  const broker = client();
  await expect(broker.request('viewPage', { scope: 's', signature: 'grid', offset: 1000, plan: { table: 'tasks', limit: 32 } })).resolves.toMatchObject({ accepted: 'viewPage', args: { offset: 1000 } });
  expect(broker.stats()).toMatchObject({ pending: 0, pendingBytes: 0 });
});
it('admits staged membership operations and rejects internal owner operations', async () => {
  const broker = client();
  for (const op of ['windowBegin', 'windowAppend', 'windowPublish', 'windowHeader', 'windowPage', 'windowHashes', 'windowCollect']) {
    await expect(broker.request(op, { scope: 's' })).resolves.toMatchObject({ accepted: op });
  }
  await expect(broker.request('open', { owner: 'intruder' })).rejects.toMatchObject({ code: 'OPERATION' });
});

it('admits restartable bootstrap operations through the same bounded replay channel', async () => {
  const broker = client();
  for (const op of ['bootstrapBegin', 'bootstrapAppend', 'bootstrapFinish'])
    await expect(broker.request(op, { scope: 's', token: 'first-sync' })).resolves.toMatchObject({ accepted: op });
  await expect(broker.request('bootstrapAppend', { scope: 's', encoded: 'x'.repeat(131072) })).rejects.toMatchObject({ code: 'BUDGET' });
  expect(broker.stats()).toMatchObject({ pending: 0, pendingBytes: 0 });
});

it('accepts actual progress only from the current ready worker without settling requests', () => {
  const broker = client() as any;
  const generation = { ready: true, lastProgress: 1000 };
  broker.generation = generation; broker.announce = vi.fn();
  const now = vi.spyOn(Date, 'now').mockReturnValue(3000);
  try {
    broker.progress(generation, 2500); expect(generation.lastProgress).toBe(2500);
    broker.progress(generation, 2400); broker.progress(generation, 5000);
    broker.progress({ ...generation }, 2700); broker.progress(generation, NaN);
    generation.ready = false; broker.progress(generation, 2700);
    expect(generation.lastProgress).toBe(2500); expect(broker.announce).toHaveBeenCalledOnce();
    generation.ready = true; broker.progress(generation, 2800);
    expect(generation.lastProgress).toBe(2800); expect(broker.announce).toHaveBeenCalledTimes(2);
    expect(broker.pending.size).toBe(0);
  } finally { now.mockRestore(); }
});

it('bounds change subscriptions and releases their retained listeners',()=>{
  const broker=client() as any;broker.options={};const calls:number[]=[];
  const release=broker.subscribeChanges((_scope:string,sequence:number)=>calls.push(sequence));
  broker.subscribeChanges(()=>{throw Error('observer');});
  broker.receiveControl({protocol:2,type:'change',scope:'s',sequence:3});expect(calls).toEqual([3]);
  release();broker.receiveControl({protocol:2,type:'change',scope:'s',sequence:4});expect(calls).toEqual([3]);
  const releases=Array.from({length:63},()=>broker.subscribeChanges(()=>{}));
  expect(()=>broker.subscribeChanges(()=>{})).toThrow('listener budget');
  for(const close of releases)close();expect(broker.stats().listeners).toBe(1);
});

it('admits binary bootstrap frames and snapshots bytes before retaining replay requests', async()=>{
  const broker=client(); const input=new TextEncoder().encode('[{"row":"東京"}]');
  const result=await broker.request<any>('bootstrapAppend',{scope:'s',token:'t',page:0,encoded:input});
  const expected=input.slice();input.fill(0);
  expect(result.args.encoded).toEqual(expected);
  await expect(broker.request('apply',{scope:'s',encoded:expected})).rejects.toMatchObject({code:'INVALID_ROW'});
  await expect(broker.request('bootstrapAppend',{scope:'s',encoded:new Uint8Array(131072)})).rejects.toMatchObject({code:'BUDGET'});
  let read=false;
  await expect(broker.request('bootstrapAppend',{encoded:expected,get scope(){read=true;return 's';}})).rejects.toMatchObject({code:'INVALID_ROW'});
  expect(read).toBe(false);expect(broker.stats()).toMatchObject({pending:0,pendingBytes:0});
});

it('admits encoded native snapshot bytes under the same immutable transport budget',async()=>{
 const broker=client(),encoded=new TextEncoder().encode('{"type":"replica.snapshotEncodedPage"}');
 const result=await broker.request<any>('snapshotAppendEncoded',{scope:'s',encoded});
 const expected=encoded.slice();encoded.fill(0);expect(result.args.encoded).toEqual(expected);
 await expect(broker.request('snapshotAppendEncoded',{scope:'s',encoded:new Uint8Array(131072)})).rejects.toMatchObject({code:'BUDGET'});
});

it('preserves a caller request ID through owner forwarding independently of transport correlation IDs',async()=>{
 const broker=client() as any,forwarded:any[]=[],generation={ready:true,token:'owner',worker:{postMessage(message:any){forwarded.push(message);queueMicrotask(()=>{const pending=broker.workerCalls.get(message.id);broker.workerCalls.delete(message.id);pending.resolve({sequence:7});});}}};
 broker.generation=generation;broker.workerCalls=new Map();broker.reply=vi.fn();
 const message={type:'request',protocol:2,token:'owner',from:'follower',id:'follower:r:1',op:'canonicalCall',args:{scope:'s',method:'clear',args:['s']}};
 await broker.serve(message);await broker.serve(message);
 expect(forwarded.map(value=>value.requestID)).toEqual(['follower:r:1','follower:r:1']);expect(forwarded[0].id).not.toBe(forwarded[1].id);expect(broker.reply).toHaveBeenCalledTimes(2);
});
it('deduplicates port/channel changes under a bounded scope ledger and rejects older owner epochs',()=>{
 const broker=client() as any,calls:number[]=[];broker.options={onChange:(_scope:string,sequence:number)=>calls.push(sequence)};broker.owner={epoch:2};
 const message={protocol:2,type:'change',scope:'s',sequence:4,epoch:2};broker.receiveControl(message);broker.receiveControl(message);broker.receiveControl({...message,sequence:3});broker.receiveControl({...message,epoch:1,sequence:100});expect(calls).toEqual([4]);
 broker.receiveControl({...message,epoch:3,sequence:1});expect(calls).toEqual([4,1]);
 for(let n=0;n<1000;n++)broker.receiveControl({...message,scope:'scope-'+n});expect(broker.stats().changeScopes).toBe(128);expect(broker.stats().changeBytes).toBeLessThanOrEqual(262144);
});

it('sends only one outstanding heartbeat and announces an unchanged owner only on demand',()=>{
 const broker=client() as any;broker.options={};broker.owner={id:'owner',token:'owner-token',epoch:1,seen:Date.now()};broker.broadcast=vi.fn();
 for(let n=0;n<10000;n++)broker.sendHeartbeat();expect(broker.broadcast).toHaveBeenCalledOnce();
 broker.receiveReply({type:'tab-pong',protocol:2,to:'test',token:'owner-token',sentAt:Date.now()});broker.sendHeartbeat();expect(broker.broadcast).toHaveBeenCalledTimes(2);
 const generation={ready:true,token:'owned',epoch:2,lastProgress:Date.now()};broker.generation=generation;broker.broadcast.mockClear();
 for(let n=0;n<10000;n++)broker.announce(generation);expect(broker.broadcast).toHaveBeenCalledOnce();broker.announce(generation,true);expect(broker.broadcast).toHaveBeenCalledTimes(2);
});

it('distinguishes a global reset from a literal star scope and acknowledges duplicate deliveries',()=>{
 const broker=client() as any,calls:boolean[]=[];broker.options={onChange:(_scope:string,_sequence:number,reset:boolean)=>calls.push(reset)};broker.owner={epoch:1};broker.acknowledgeChange=vi.fn();
 const message={protocol:2,type:'change',scope:'*',sequence:4,epoch:1,notification:1};
 broker.receiveControl(message);broker.receiveControl({...message,reset:true,notification:2});broker.receiveControl({...message,reset:true,notification:2});
 expect(calls).toEqual([false,true]);expect(broker.acknowledgeChange).toHaveBeenCalledTimes(3);
});

it('probes for an existing owner when a newly opened tab missed its original announcement',()=>{
 const broker=client() as any;broker.broadcast=vi.fn();broker.sendHeartbeat=vi.fn();broker.tick();expect(broker.broadcast).toHaveBeenCalledWith({type:'probe'});
 broker.broadcast.mockClear();broker.owner={id:'other',token:'known',epoch:1,seen:Date.now()};broker.tick();expect(broker.broadcast).not.toHaveBeenCalled();
});
it('never resends an Action or a correlated token reply after dispatch, including across owner replacement',async()=>{
 for(const method of ['action','token']){
  const broker=client() as any;broker.owner={id:'test',token:'old-owner',epoch:1,seen:Date.now()};broker.send=SQLiteReplicaBroker.prototype['send'];broker.serve=vi.fn(async()=>{});
  const pending=broker.request('clientCall',{method,path:'external/run'});void pending.catch(()=>{});const call=broker.pending.values().next().value;expect(broker.serve).toHaveBeenCalledOnce();broker.send(call);broker.send(call);expect(broker.serve).toHaveBeenCalledOnce();broker.owner.token='new-owner';broker.send(call);await expect(pending).rejects.toMatchObject({code:'FENCED',outcome:'unknown'});expect(broker.serve).toHaveBeenCalledOnce();expect(broker.pendingBytes).toBe(0);expect(broker.pending.size).toBe(0);
 }
});
it('keeps durable Reducer replay but reports a single-shot refusal as an unknown outcome',async()=>{
 const broker=client() as any;broker.owner={id:'test',token:'old',epoch:1,seen:Date.now()};broker.send=SQLiteReplicaBroker.prototype['send'];broker.serve=vi.fn(async()=>{});
 const durable=broker.request('clientCall',{method:'reducer',path:'tasks/change'}),call=broker.pending.values().next().value;broker.send(call);broker.owner.token='new';broker.send(call);expect(broker.serve).toHaveBeenCalledTimes(3);broker.receiveReply({protocol:2,to:'test',token:'new',id:call.id,value:'original'});expect(await durable).toBe('original');
 const single=broker.request('clientCall',{method:'action',path:'external/run'});void single.catch(()=>{});const sent=broker.pending.values().next().value;broker.receiveReply({protocol:2,to:'test',token:'new',id:sent.id,error:{code:'BUSY',message:'delivery interrupted'}});await expect(single).rejects.toMatchObject({code:'FENCED',outcome:'unknown'});expect(broker.pending.size).toBe(0);
});

it('joins an identical stable reducer request even at capacity, rejects changed content and keeps its command ID in replay',async()=>{
 const broker=client() as any;broker.owner={id:'test',token:'owner',epoch:1,seen:Date.now()};broker.send=vi.fn();const input={method:'reducer',path:'tasks/change',args:{id:'a'},now:100};
 const pending=broker.request('clientCall',input,90000,'stable-command'),others=Array.from({length:255},(_,n)=>broker.request('clientCall',{...input,now:101+n}));expect(broker.pending.size).toBe(256);expect(broker.request('clientCall',input,90000,'stable-command')).toBe(pending);
 await expect(broker.request('clientCall',{...input,args:{id:'b'}},90000,'stable-command')).rejects.toMatchObject({code:'INTENT_CONFLICT'});await expect(broker.request('clientCall',input)).rejects.toMatchObject({code:'BUDGET'});expect(broker.pending.get('stable-command').args).toEqual(input);
 for(const call of [...broker.pending.values()])broker.receiveReply({protocol:2,to:'test',token:'owner',id:call.id,value:call.id});expect(await pending).toBe('stable-command');await Promise.all(others);expect(broker.pendingBytes).toBe(0);
 await expect(broker.request('clientCall',{method:'action',path:'external'},90000,'stable-command')).rejects.toMatchObject({code:'OPERATION'});
});

it('delivers fresh credited control hints at the same SQL generation while rejecting duplicate notifications',()=>{
 const broker=client() as any,calls:string[]=[];broker.options={onChange:(scope:string)=>calls.push(scope)};broker.owner={epoch:1};broker.acknowledgeChange=vi.fn();
 const message={protocol:2,type:'change',scope:'@control',sequence:4,epoch:1,notification:1};broker.receiveControl(message);broker.receiveControl({...message,notification:2});broker.receiveControl({...message,notification:2});broker.receiveControl(message);expect(calls).toEqual(['@control','@control']);
 const reset={...message,scope:'*',reset:true,notification:3};broker.receiveControl(reset);broker.receiveControl({...reset,notification:4});broker.receiveControl({...reset,notification:4});expect(calls).toEqual(['@control','@control','*','*']);expect(broker.acknowledgeChange).toHaveBeenCalledTimes(7);
});

it('fails unsupported startup without selecting a different storage engine',async()=>{
 const originalDocument=globalThis.document,originalNavigator=globalThis.navigator;
 const channels:any[]=[];class Channel{onmessage?:Function;constructor(_name:string){channels.push(this);}postMessage(_value:unknown){}close(){}}
 const workers:{kind:string;terminated:boolean}[]=[];
 const makeWorker=(kind:string)=>{const state={kind,terminated:false};workers.push(state);return {onmessage:null as null|((event:{data:any})=>void),onerror:null as null|(()=>void),postMessage(this:any,message:any){if(message.op==='open')queueMicrotask(()=>this.onmessage?.({data:{id:message.id,sentAt:Date.now(),...(kind==='sqlite'?{error:{code:'UNSUPPORTED',message:'OPFS is unavailable'}}:{value:{epoch:2,adapter:'indexeddb'}})}}));else if(message.op==='ping')queueMicrotask(()=>this.onmessage?.({data:{id:message.id,sentAt:Date.now(),value:{ready:true}}}));},terminate(){state.terminated=true;}} as unknown as Worker;};
 vi.stubGlobal('BroadcastChannel',Channel);vi.stubGlobal('document',{addEventListener(){},removeEventListener(){}});vi.stubGlobal('addEventListener',()=>{});vi.stubGlobal('removeEventListener',()=>{});
 vi.stubGlobal('navigator',{locks:{request:async(_name:string,_options:unknown,run:()=>Promise<void>)=>run()}});
 const broker=new SQLiteReplicaBroker({schemaVersion:'a'.repeat(64),createWorker:()=>makeWorker('sqlite')});
 try{await vi.waitFor(()=>expect(broker.stats()).toMatchObject({state:'unavailable',adapter:'sqlite'}),{timeout:3000});expect(workers.map(worker=>worker.kind)).toEqual(['sqlite']);expect(workers[0]?.terminated).toBe(true);}
 finally{broker.close();vi.unstubAllGlobals();if(originalDocument)vi.stubGlobal('document',originalDocument);if(originalNavigator)vi.stubGlobal('navigator',originalNavigator);for(const channel of channels)channel.close();}
});

it('retries one capacity-blocked request on completion instead of waiting for the heartbeat',async()=>{
 const broker=client() as any;broker.owner={id:'test',token:'owner',epoch:1,seen:Date.now()};broker.send=vi.fn();
 const blocked=broker.request('clientCall',{method:'replicaPage',signature:'grid'}),first=broker.send.mock.calls[0][0];
 const active=broker.request('clientCall',{method:'controls'}),second=broker.send.mock.calls[1][0];
 broker.receiveReply({protocol:2,to:'test',token:'owner',id:first.id,error:{code:'BUSY',message:'full'}});
 await Promise.resolve();expect(broker.send).toHaveBeenCalledTimes(2);
 broker.receiveReply({protocol:2,to:'test',token:'owner',id:second.id,value:'ready'});await expect(active).resolves.toBe('ready');
 expect(broker.send).toHaveBeenCalledTimes(3);expect(broker.send.mock.calls[2][0]).toBe(first);
 broker.receiveReply({protocol:2,to:'test',token:'owner',id:first.id,value:'cached rows'});await expect(blocked).resolves.toBe('cached rows');
 expect(broker.stats()).toMatchObject({pending:0,pendingBytes:0});
});

it('uses each fresh owner progress hint for at most one blocked retry without spinning on BUSY or duplicates',async()=>{
 const broker=client() as any;broker.owner={id:'test',token:'owner',epoch:1,seen:Date.now()};broker.options={};broker.acknowledgeChange=vi.fn();broker.send=vi.fn();
 const pending=broker.request('clientCall',{method:'replicaPage',signature:'grid'}),call=broker.send.mock.calls[0][0];
 const busy=()=>broker.receiveReply({protocol:2,to:'test',token:'owner',id:call.id,error:{code:'BUSY',message:'full'}});
 busy();await Promise.resolve();expect(broker.send).toHaveBeenCalledTimes(1);
 const hint={protocol:2,type:'change',scope:'s',sequence:1,epoch:1};broker.receiveControl(hint);await Promise.resolve();expect(broker.send).toHaveBeenCalledTimes(2);
 busy();broker.receiveControl(hint);await Promise.resolve();expect(broker.send).toHaveBeenCalledTimes(2);
 broker.receiveControl({...hint,sequence:2});await Promise.resolve();expect(broker.send).toHaveBeenCalledTimes(3);
 busy();await Promise.resolve();await Promise.resolve();expect(broker.send).toHaveBeenCalledTimes(3);
 broker.receiveReply({protocol:2,to:'test',token:'owner',id:call.id,value:'rows'});await expect(pending).resolves.toBe('rows');
});


it('holds requests above owner capacity locally and dispatches the oldest after a reply',async()=>{
 const broker=client() as any;broker.owner={id:'test',token:'owner',epoch:1,seen:Date.now()};broker.send=SQLiteReplicaBroker.prototype['send'];broker.serve=vi.fn(async()=>{});
 const pending=Array.from({length:66},(_,n)=>broker.request('clientCall',{method:'replicaPage',signature:String(n)}));
 const settled=Promise.allSettled(pending);
 try{
  expect(broker.serve).toHaveBeenCalledTimes(64);
  const calls=[...broker.pending.values()];expect(calls[64].sentToken).toBeUndefined();expect(calls[65].sentToken).toBeUndefined();
  broker.receiveReply({protocol:2,to:'test',token:'owner',id:calls[0].id,value:'rows'});await Promise.resolve();
  expect(broker.serve).toHaveBeenCalledTimes(65);expect(calls[64].sentToken).toBe('owner');expect(calls[65].sentToken).toBeUndefined();
  broker.receiveReply({protocol:2,to:'test',token:'owner',id:calls[1].id,value:'rows'});await Promise.resolve();
  expect(broker.serve).toHaveBeenCalledTimes(66);expect(calls[65].sentToken).toBe('owner');
 }finally{for(const call of [...broker.pending.values()]){clearTimeout(call.timer);call.reject(Error('test cleanup'));}broker.pending.clear();await settled;}
});


it('reapplies owner capacity after failover and preserves queued single-shot dispatch',async()=>{
 const broker=client() as any;broker.owner={id:'test',token:'old',epoch:1,seen:Date.now()};broker.send=SQLiteReplicaBroker.prototype['send'];broker.serve=vi.fn(async()=>{});
 const pending=Array.from({length:64},(_,n)=>broker.request('clientCall',{method:'replicaPage',signature:String(n)}));
 const action=broker.request('clientCall',{method:'action',path:'external/run'});const settled=Promise.allSettled([...pending,action]);
 try{
  const calls=[...broker.pending.values()];expect(broker.serve).toHaveBeenCalledTimes(64);expect(calls[64].sentToken).toBeUndefined();
  broker.owner.token='new';for(const call of broker.pending.values())broker.send(call);expect(broker.serve).toHaveBeenCalledTimes(128);expect(calls[64].sentToken).toBeUndefined();
  broker.receiveReply({protocol:2,to:'test',token:'old',id:calls[0].id,value:'stale'});await Promise.resolve();expect(broker.serve).toHaveBeenCalledTimes(128);
  broker.receiveReply({protocol:2,to:'test',token:'new',id:calls[0].id,value:'rows'});await Promise.resolve();expect(broker.serve).toHaveBeenCalledTimes(129);expect(calls[64].sentToken).toBe('new');
  broker.send(calls[64]);expect(broker.serve).toHaveBeenCalledTimes(129);
 }finally{for(const call of [...broker.pending.values()]){clearTimeout(call.timer);call.reject(Error('test cleanup'));}broker.pending.clear();await settled;}
});

it('starts the elected worker without an unused document WASM preload',async()=>{
 const previousNavigator=globalThis.navigator,previousDocument=globalThis.document,links:any[]=[];
 let acquired:(()=>Promise<void>)|undefined;
 vi.stubGlobal('navigator',{locks:{request:vi.fn((_name,_options,run)=>{acquired=run;return Promise.resolve();})}});
 vi.stubGlobal('document',{head:{appendChild:(link:unknown)=>links.push(link)},createElement:()=>({})});
 const broker=client() as any;broker.options={createWorker:()=>{expect(links).toHaveLength(0);throw Error('stop before worker starts');}};
 try{
  broker.elect();expect(links).toEqual([]);expect(acquired).toBeTypeOf('function');
  await expect(acquired!()).rejects.toThrow('stop before worker starts');
  expect(links).toEqual([]);
  await expect(acquired!()).rejects.toThrow('stop before worker starts');expect(links).toHaveLength(0);
 }finally{broker.closed=true;broker.election?.abort();vi.stubGlobal('navigator',previousNavigator);vi.stubGlobal('document',previousDocument);}
});

it('classifies a read deadline without claiming an unknown commit or creating an intent',async()=>{
 vi.useFakeTimers();const broker=client() as any;broker.send=vi.fn();broker.setState=vi.fn();broker.wakeBlocked=vi.fn();
 try{
  const pending=broker.request('clientCall',{method:'livePage',signature:'grid'},10);
  const failure=expect(pending).rejects.toMatchObject({code:'TIMEOUT',outcome:'rejected',operation:'livePage',message:'Storage read timed out; retry the read'});
  await vi.advanceTimersByTimeAsync(10);await failure;expect(broker.pending.size).toBe(0);expect(broker.pendingBytes).toBe(0);
 }finally{vi.useRealTimers();}
});
it('retries a lost durable reply with the original public intent ID and returns its existing receipt once',async()=>{
 vi.useFakeTimers();const broker=client() as any;broker.setState=vi.fn();broker.wakeBlocked=vi.fn();
 const receipts=new Map<string,unknown>();let commits=0,drop=true;
 broker.send=vi.fn((call:any)=>{
  if(!receipts.has(call.id)){commits++;receipts.set(call.id,{commandId:call.id,result:'saved'});}
  if(!drop){clearTimeout(call.timer);broker.pending.delete(call.id);broker.pendingBytes-=call.bytes;call.resolve(receipts.get(call.id));}
 });
 try{
  const input={method:'reducer',path:'tasks/updateDetails',args:{id:'task',name:'offline'},now:100};
  const first=broker.request('clientCall',input,10,'same-intent');
  const failure=expect(first).rejects.toMatchObject({code:'TIMEOUT',outcome:'unknown',operation:'reducer',intentId:'same-intent'});
  await vi.advanceTimersByTimeAsync(10);await failure;drop=false;
  expect(await broker.request('clientCall',input,10,'same-intent')).toEqual({commandId:'same-intent',result:'saved'});
  expect(commits).toBe(1);expect(receipts.size).toBe(1);expect(broker.pendingBytes).toBe(0);
 }finally{vi.useRealTimers();}
});

it('delivers repeated credited preview hints at one committed generation without accepting duplicates',()=>{
 const broker=client() as any,calls:string[]=[];broker.options={onChange:(scope:string)=>calls.push(scope)};broker.owner={epoch:1};broker.acknowledgeChange=vi.fn();
 const hint={protocol:2,type:'change',scope:'@preview',sequence:4,epoch:1,notification:1};
 broker.receiveControl(hint);broker.receiveControl({...hint,notification:2});broker.receiveControl({...hint,notification:2});broker.receiveControl(hint);
 expect(calls).toEqual(['@preview','@preview']);
 broker.receiveControl({...hint,scope:'tasks',sequence:5,notification:3});expect(calls).toEqual(['@preview','@preview','tasks']);
});

it('forwards follower entity reads with their tab ownership and returns the complete row',async()=>{
 const broker=client() as any,forwarded:any[]=[],row={id:'task',name:'Follower task'};
 broker.generation={ready:true,token:'owner',worker:{postMessage(message:any){forwarded.push(message);queueMicrotask(()=>{const pending=broker.workerCalls.get(message.id);broker.workerCalls.delete(message.id);pending.resolve({generation:{epoch:1,sequence:3},rows:[{id:'task',value:row}]});});}}};
 broker.workerCalls=new Map();broker.reply=vi.fn();
 for(let n=0;n<5;n++)await broker.serve({type:'request',protocol:2,token:'owner',from:'follower',id:'follower:r:'+n,op:'clientCall',args:{method:'entities',entity:'tasks',ids:['task']}});
 expect(forwarded).toHaveLength(5);expect(forwarded.every(message=>message.tabID==='follower')).toBe(true);
 expect(new Set(forwarded.map(message=>message.requestID)).size).toBe(5);
 expect(broker.reply).toHaveBeenCalledTimes(5);
 for(const call of broker.reply.mock.calls)expect(call[1]).toEqual({generation:{epoch:1,sequence:3},rows:[{id:'task',value:row}]});
});

it('reports whether a follower reaches the dedicated owner directly or through its page',()=>{
 const broker=client() as any;expect(broker.stats().route).toBe('broadcast-page');broker.port={};expect(broker.stats().route).toBe('shared-router-page');broker.direct={ready:false};expect(broker.stats().directPending).toBe(true);broker.direct.ready=true;expect(broker.stats().route).toBe('direct-worker');broker.generation={};expect(broker.stats().route).toBe('owner-local');
});
