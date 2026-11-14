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
async function setup(config:{hash?:any;bindingHash?:string;binding?:boolean;storage?:MemoryLocalReplicaStorage;admissionObserver?:(c:GonvexClient)=>void;localObserver?:(c:GonvexClient)=>void}={}){
 const {bindingHash='artifact-one',binding=true,storage=new MemoryLocalReplicaStorage()}=config;const hash=Object.hasOwn(config,'hash')?config.hash:'artifact-one';
 vi.stubGlobal('WebSocket',Socket);const local={execute:0,replay:0};
 const runtime={mode:'portable' as const,artifactHash:bindingHash,tables:[],create:()=>({ready:Promise.resolve(),execute:async()=>{local.execute++;throw Error('PRIVATE local execution');},replay:async()=>{local.replay++;throw Error('PRIVATE local replay');},close:()=>{}})};
 const c=new GonvexClient('ws://controlled.invalid/ws',{project:'p',tenant:'t',token:'one',identity:{iss:'fixture',sub:'a'},errorReporting:false,localDependencyResidency:'off',clientContract:{version:3,offlineMaxAgeMs:null},localReplica:{storage},...(binding?{localRuntime:runtime}:{})});clients.push(c);
 if(config.admissionObserver)c.onTenantAdmissionChange(()=>config.admissionObserver!(c));if(config.localObserver)c.onLocalSessionChange(()=>config.localObserver!(c));
 const w=c.watchReplica(collection,{}),s=Socket.instances.at(-1)!;s.open();s.receive({type:'session.ready',capabilities:{replicaWatermark:1,reducerReceiptLookup:1},replica:directive});
 const auth=s.sent.find(x=>x.type==='auth');s.receive({type:'auth.result',id:auth.id,result:{replica:directive,localIdentity:identity,...(hash===undefined?{}:{artifactHash:hash})}});
 await vi.waitFor(()=>expect(s.sent.some(x=>x.type==='replica.open')).toBe(true));
 const open=s.sent.filter(x=>x.type==='replica.open').at(-1),hashes=await replicaRowsHashes([],'id'),digest=await replicaHashesDigest(hashes);
 s.receive({type:'replica.snapshot',id:open.id,path:collection.path,result:[],cursor:{epoch:'db',revision:3},key:'id',hashes,digest,truncated:false});s.receive({type:'replica.ready',id:open.id,path:collection.path,cursor:{epoch:'db',revision:3},digest,truncated:false});s.receive({type:'replica.watermark',revision:3});
 await vi.waitFor(()=>{expect(c.localSessionState()).toBe('authenticated');expect(w.localReplicaState()?.isUpToDate).toBe(true);});await tick();return {c,s,w,open,local,runtime,auth};
}
const begin=(c:GonvexClient,ref=localRef,args:any={id:'row',title:'synthetic'},opts:any=options)=>c.reducerWithTransactionEvidence(ref,args,opts).then(value=>({value,error:undefined as any}),error=>({value:undefined,error}));
async function finish(s:Socket,id:string){s.receive(result(id));s.receive(transaction(id));s.receive({type:'replica.watermark',revision:4});await vi.waitFor(()=>expect(reads(s)).toHaveLength(1));const q=reads(s)[0];s.receive({type:'query.result',id:q.id,path:q.path,result:{found:true,cursor:{epoch:'db',revision:5}}});}

it('reviewer: changing an obsolete installed binding hash cannot fabricate current binding provenance',async()=>{
 const {c,s,runtime}=await setup({bindingHash:'obsolete-artifact'});
 runtime.artifactHash='artifact-one';
 const p=begin(c); if(writes(s).length)await finish(s,writes(s)[0].id);
 const outcome=await p;console.log('REVIEW_BINDING_MUTATION',JSON.stringify({writes:writes(s).length,reads:reads(s).length,outcome}));
 expect(outcome.error).toMatchObject({reason:'precondition',possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
it('reviewer: invalidation in the last live binding getter must prevent a write',async()=>{
 const {c,s,runtime}=await setup();let probes=0;
 const args={toJSON(){Object.defineProperty(runtime,'artifactHash',{configurable:true,get(){probes++;if(probes===2)s.emit('error');return 'artifact-one';}});return {};}};
 const outcome=await begin(c,localRef,args);console.log('REVIEW_LAST_GETTER',JSON.stringify({probes,writes:writes(s).length,reads:reads(s).length,error:outcome.error}));
 if(writes(s).length)expect(outcome.error).toMatchObject({possibleWriteApplied:true});
 else expect(outcome.error).toMatchObject({possibleWriteApplied:false});
 expect(writes(s)).toHaveLength(0);
});
it('reviewer: caller-owned saved identity mutation must not retain exact admitted principal proof',async()=>{
 class MutableStorage extends MemoryLocalReplicaStorage {saved:any;override async saveSession(scope:any,saved:any){this.saved=saved;return super.saveSession(scope,saved);}}
 const storage=new MutableStorage(),{c,s}=await setup({storage});expect(storage.saved).toBeDefined();
 storage.saved.identity.member.id='different-member';
 const p=begin(c);if(writes(s).length)await finish(s,writes(s)[0].id);const outcome=await p;console.log('REVIEW_IDENTITY_MUTATION',JSON.stringify({writes:writes(s).length,outcome}));
 expect(outcome.error).toMatchObject({possibleWriteApplied:false});expect(writes(s)).toHaveLength(0);
});
