import {afterEach,expect,it,vi} from 'vitest';
import {GonvexClient,MemoryLocalReplicaStorage,type FunctionReference} from './index.js';
import {replicaRowsHashes,replicaHashesDigest} from './replica-integrity.js';
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
const collection:FunctionReference={kind:'query',path:'board.one',delivery:'replica',replica:{table:'board',key:'id',columns:['id','title'],maxRows:1}};
const reducer:FunctionReference={kind:'reducer',path:'board.rename'};
const options={offline:'reject' as const,optimistic:[] as [],timeoutMs:2000};
async function setup(storage=new MemoryLocalReplicaStorage()){
  vi.stubGlobal('WebSocket',Socket);
  const c=new GonvexClient('ws://controlled.invalid/ws',{project:'p',tenant:'t',token:'fixture',identity:{iss:'fixture',sub:'a'},errorReporting:false,localReplica:{storage}});clients.push(c);
  const watch=c.watchReplica(collection,{}),s=Socket.instances.at(-1)!;s.open();
  const directive={protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'module-definition'};
  s.receive({type:'session.ready',capabilities:{replicaWatermark:1,reducerReceiptLookup:1},replica:directive});
  const auth=s.sent.find(x=>x.type==='auth');
  s.receive({type:'auth.result',id:auth.id,result:{replica:directive,localIdentity:{auth:{account:{id:'a'}},tenant:{id:'t'},member:{id:'m'}}}});
  await vi.waitFor(()=>expect(s.sent.some(x=>x.type==='replica.open')).toBe(true));
  const open=s.sent.filter(x=>x.type==='replica.open').at(-1),hashes=await replicaRowsHashes([],'id'),digest=await replicaHashesDigest(hashes);
  s.receive({type:'replica.snapshot',id:open.id,path:collection.path,result:[],cursor:{epoch:'db',revision:3},key:'id',hashes,digest,truncated:false});
  s.receive({type:'replica.ready',id:open.id,path:collection.path,cursor:{epoch:'db',revision:3},digest,truncated:false});
  s.receive({type:'replica.watermark',revision:3});
  await vi.waitFor(()=>{expect(c.localSessionState()).toBe('authenticated');expect(watch.localReplicaState()?.isUpToDate).toBe(true);});
  return {c,s,watch,open};
}
const writes=(s:Socket)=>s.sent.filter(x=>x.type==='reducer.call');
const begin=(c:GonvexClient)=>c.reducerWithTransactionEvidence(reducer,{id:'row',title:'synthetic'},options).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
function result(id:string,extra:any={}){return {type:'reducer.result',id,path:reducer.path,result:{renamed:true},originCommandId:id,committedRevision:4,...extra};}
function transaction(id:string,extra:any={}){return {type:'replica.transaction',originCommandId:id,cursor:{epoch:'db',revision:4},provenance:{rootCommandId:id,channel:'ui',rootChannel:'ui',actorAccountId:'a',actorMemberId:'m'},changes:[{entity:'board',id:'row',operation:'update',newValue:{id:'row',title:'synthetic'}}],...extra};}
async function receipt(s:Socket,fence=5){await vi.waitFor(()=>expect(s.sent.some(x=>x.type==='query.call' && x.path==='__gonvex.reducerReceipt')).toBe(true));const q=s.sent.find(x=>x.type==='query.call'&&x.path==='__gonvex.reducerReceipt');s.receive({type:'query.result',id:q.id,path:q.path,reason:'initial',result:{found:true,cursor:{epoch:'db',revision:fence}}});return q;}
for(const order of ['result-first','transaction-first'])it(`joins ${order}; returns transaction R, not later receipt fence`,async()=>{
  const {c,s}=await setup(),p=begin(c),call=writes(s)[0];expect(call.idempotencyKey).toBe(call.id);expect(call.receiptPath).toBe(reducer.path);
  if(order==='result-first'){s.receive(result(call.id));s.receive(transaction(call.id));}else{s.receive(transaction(call.id));s.receive(result(call.id));}
  expect(s.sent.filter(x=>x.type==='query.call')).toHaveLength(0);s.receive({type:'replica.watermark',revision:4});
  const q=await receipt(s);expect(q.args).toEqual({idempotencyKey:call.id,receiptPath:reducer.path});
  expect((await p).value).toEqual({result:{renamed:true},confirmation:{source:'server-durable',basis:'matching-replica-transaction',cursor:{epoch:'db',revision:4}}});
  expect(writes(s)).toHaveLength(1);expect(s.sent.filter(x=>x.type==='query.call')).toHaveLength(1);
});
for(const fault of ['origin','path','revision','actor','root','epoch','duplicate-result','duplicate-transaction'])it(`refuses forged/conflicting ${fault} without another write`,async()=>{
  const {c,s}=await setup(),p=begin(c),id=writes(s)[0].id;
  if(fault==='origin')s.receive(result(id,{originCommandId:'other'}));
  if(fault==='path')s.receive(result(id,{path:'other'}));
  if(fault==='revision')s.receive(result(id,{committedRevision:null}));
  if(fault==='actor')s.receive(transaction(id,{provenance:{rootCommandId:id,channel:'ui',rootChannel:'ui',actorAccountId:'other',actorMemberId:'m'}}));
  if(fault==='root')s.receive(transaction(id,{provenance:{rootCommandId:'other',channel:'ui',actorAccountId:'a',actorMemberId:'m'}}));
  if(fault==='epoch')s.receive(transaction(id,{cursor:{epoch:'replacement',revision:4}}));
  if(fault==='duplicate-result'){s.receive(result(id));s.receive(result(id,{result:'conflicting-private-result'}));}
  if(fault==='duplicate-transaction'){s.receive(transaction(id));s.receive(transaction(id));}
  const r=await p;expect(r.error?.outcome).toBe('unknown');expect(r.value).toBeUndefined();expect(JSON.stringify(r.error)).not.toContain('private-result');expect(writes(s)).toHaveLength(1);
});
for(const event of ['auth-start','close','error','reset','epoch','unsubscribe'])it(`owner invalidation ${event} cannot be resurrected by late evidence`,async()=>{
  const {c,s,watch,open}=await setup(),p=begin(c),id=writes(s)[0].id;
  if(event==='auth-start')c.setAuth({token:'replacement'});
  if(event==='close')s.close();
  if(event==='error')s.emit('error');
  if(event==='reset')s.receive({type:'replica.reset',id:open.id,path:collection.path,reason:'test-reset'});
  if(event==='epoch')s.receive({type:'replica.ready',id:open.id,path:collection.path,cursor:{epoch:'changed',revision:4}});
  if(event==='unsubscribe'){const stop=watch.onUpdate(()=>{});stop();}
  s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
  const r=await p;expect(r.error?.outcome).toBe('unknown');expect(writes(s)).toHaveLength(1);
});
it('missing/no-op/filtered/unsubscribed transaction evidence times out UNKNOWN and never reacquires',async()=>{
  const {c,s}=await setup();const p=c.reducerWithTransactionEvidence(reducer,{}, {...options,timeoutMs:40}).catch(error=>error);const id=writes(s)[0].id;
  s.receive(result(id));s.receive({type:'replica.watermark',revision:4});
  const error=await p;expect(error.reason).toBe('deadline');expect(error.possibleWriteApplied).toBe(true);
  s.receive(transaction(id));expect(writes(s)).toHaveLength(1);expect(s.sent.filter(x=>x.type==='query.call')).toHaveLength(0);
});
it('a later or different-epoch receipt cannot supply a missing original cursor',async()=>{
  const {c,s}=await setup(),p=begin(c),id=writes(s)[0].id;s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
  await vi.waitFor(()=>expect(s.sent.some(x=>x.type==='query.call')).toBe(true));const q=s.sent.find(x=>x.type==='query.call');
  s.receive({type:'query.result',id:q.id,path:q.path,result:{found:true,cursor:{epoch:'other',revision:99}}});expect((await p).error.reason).toBe('conflicting-evidence');
});
it('refuses unsafe generated local reducers before any send or connect',async()=>{
  const {c,s}=await setup();for(const ref of [{...reducer,localExecution:1 as const},{...reducer,scope:'control' as const},{...reducer,kind:'action'},{...reducer,authorization:'public' as const}])await expect(c.reducerWithTransactionEvidence(ref,{},options)).rejects.toMatchObject({possibleWriteApplied:false});
  expect(writes(s)).toHaveLength(0);expect(Socket.instances).toHaveLength(1);
});
class HeldStorage extends MemoryLocalReplicaStorage {
  hold=false;entered=false;release=()=>{};
  override async applyTransaction(...args:Parameters<MemoryLocalReplicaStorage['applyTransaction']>){
    if(this.hold){this.entered=true;await new Promise<void>(resolve=>{this.release=resolve;});}
    return super.applyTransaction(...args);
  }
}
it('held actual LocalReplica storage processing cannot be bypassed by a result or watermark',async()=>{
  const storage=new HeldStorage(),{c,s}=await setup(storage);storage.hold=true;
  const p=begin(c),id=writes(s)[0].id;s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
  await vi.waitFor(()=>expect(storage.entered).toBe(true));expect(s.sent.filter(x=>x.type==='query.call')).toHaveLength(0);
  storage.release();await receipt(s);expect((await p).value?.confirmation.cursor).toEqual({epoch:'db',revision:4});
});
it('deadline while storage is held is not cancellation; late apply cannot resolve or reacquire',async()=>{
  const storage=new HeldStorage(),{c,s}=await setup(storage);storage.hold=true;
  const p=c.reducerWithTransactionEvidence(reducer,{}, {...options,timeoutMs:40}).catch(e=>e),id=writes(s)[0].id;
  s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
  const error=await p;expect(error).toMatchObject({reason:'deadline',possibleWriteApplied:true,outcome:'unknown'});
  storage.release();expect(s.sent.filter(x=>x.type==='query.call')).toHaveLength(0);expect(writes(s)).toHaveLength(1);
});
it('a synchronous argument serializer cannot send after its total deadline or leak its error',async()=>{
  const {c,s}=await setup();
  const args={toJSON(){const end=performance.now()+30;while(performance.now()<end){}return {};}} as any;
  await expect(c.reducerWithTransactionEvidence(reducer,args,{...options,timeoutMs:10})).rejects.toMatchObject({reason:'deadline',possibleWriteApplied:false});
  const error=await c.reducerWithTransactionEvidence(reducer,{toJSON(){throw Error('FAKE_PRIVATE_SERIALIZER');}} as any,options).catch(e=>e);
  expect(JSON.stringify(error)).not.toContain('FAKE_PRIVATE_SERIALIZER');expect(writes(s)).toHaveLength(0);
});
for(const invalid of ['missing','false','low','epoch'])it(`receipt ${invalid} cannot be upgraded or read again`,async()=>{
  const {c,s}=await setup(),p=begin(c),id=writes(s)[0].id;s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});
  await vi.waitFor(()=>expect(s.sent.some(x=>x.type==='query.call')).toBe(true));const q=s.sent.find(x=>x.type==='query.call');
  const value=invalid==='missing'?null:invalid==='false'?{found:false}:{found:true,cursor:{epoch:invalid==='epoch'?'other':'db',revision:invalid==='low'?3:4}};
  s.receive({type:'query.result',id:q.id,path:q.path,result:value});expect((await p).error.outcome).toBe('unknown');
  expect(s.sent.filter(x=>x.type==='query.call')).toHaveLength(1);expect(writes(s)).toHaveLength(1);
});

it('multiple active observation windows are refused before allocating call proof state or writing',async()=>{
  const {c,s}=await setup();c.watchReplica({...collection,path:'board.second'},{});
  await expect(c.reducerWithTransactionEvidence(reducer,{},options)).rejects.toMatchObject({possibleWriteApplied:false,reason:'precondition'});
  expect(writes(s)).toHaveLength(0);
});
