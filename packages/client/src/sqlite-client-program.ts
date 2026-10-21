import {sqliteOwnerRequestCapacity} from './sqlite-worker-protocol.js';
import {nativeLivePayloadBytes} from './native-live-window-frame.js';
import {lowerSQLiteLivePlan} from './sqlite-plan-lowering.js';
import {GonvexClientError} from './client-error.js';
import {SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
import {waitForSQLiteOwnerAdmission} from './sqlite-owner-transactions.js';
import {SQLiteLiveWindows,liveStageBudgetStats} from './sqlite-live-windows.js';
import type {SQLiteLiveQueryUpdate} from './sqlite-native-transport.js';
import {ByteLedger} from './byte-budget.js';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import {SQLiteOwnerMaintenance} from './sqlite-owner-maintenance.js';
import type {ClientContract} from './client-upgrades.js';
import type {JsonValue} from '@gonvex/protocol';
import type {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import type {ReadCoverage} from '@gonvex/local-runtime/read-view';
import {control} from './control.js';
import type {FunctionReference,GonvexClientAuth,ReducerRejectionEvent,SupportCommand} from './index.js';
import {SQLiteNativeTransport} from './sqlite-native-transport.js';
import type {SQLiteOwnerCaller,SQLiteOwnerProgram,SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteWorkerTokens} from './sqlite-worker-tokens.js';
import {SQLiteNativeFeed} from './sqlite-native-feed.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import {SQLiteReducerOwner} from './sqlite-reducer-owner.js';
import {SQLiteReducerDelivery} from './sqlite-reducer-delivery.js';
import {SQLiteSessionJournal} from './sqlite-session-journal.js';
import {SQLiteLegacyBootstrap} from './sqlite-legacy-bootstrap.js';
import type {LocalReplicaSession} from './local-replica.js';
import {sqliteCollectionPagePlan} from './sqlite-collection-plan.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Auth=Pick<GonvexClientAuth,'project'|'tenant'|'token'|'identity'>;
type NativeFunctionReference=FunctionReference&{localDependencyTables?:string};
type Source={previewInterest?:boolean;forceSnapshot?:boolean;id:string;signature:string;ref:FunctionReference;args:JsonValue;tabs:Set<string>;leases:Map<string,Set<string>>;eager:boolean;bytes:number;closing?:Promise<void>;retireTimer?:ReturnType<typeof setTimeout>};
type ControlSource={signature:string;ref:FunctionReference;args:JsonValue;tabs:Set<string>;value?:JsonValue;error?:string;version:number;bytes:number;resultBytes:number;close():void};
type LiveSource={leases:Map<string,Set<string>>;signature:string;ref:FunctionReference;args:JsonValue;tabs:Set<string>;bytes:number;error?:string;close():void};
type Connection={activeSourceOpens?:Set<Source>;pendingSourceOpens?:Set<Source>;requiredReducers:Set<string>;requiredReducerBytes:number;supportCommands:Array<SupportCommand&{sequence:number}>;supportSequence:number;supportCursors:Map<string,number>;liveStream:string;lives:Map<string,LiveSource>;liveBytes:number;lastIdentity?:string;rejections:Array<ReducerRejectionEvent&{sequence:number}>;rejectionSequence:number;rejectionCursors:Map<string,number>;controlOnly:boolean;public:boolean;authVersion:number;authError?:{id:string;message:string;definitive:boolean;code?:string};developer?:{id:string;token:string;acked:Set<string>};route:Omit<Auth,'token'>;id:string;url:string;identityKey:string;client:SQLiteNativeTransport;feed:SQLiteNativeFeed;delivery:SQLiteReducerDelivery;tabs:Set<string>;sources:Map<string,Source>;sourceBytes:number;controls:Map<string,ControlSource>;controlBytes:number;restoring?:Promise<void>;migrationBlocked?:unknown;restoreScope?:string;restoreGeneration?:number;migrationTimer?:ReturnType<typeof setTimeout>;migrationSession?:LocalReplicaSession;denied?:boolean;admissionKey?:{generation:string;value:Promise<string>};recovered?:boolean;recovering?:Promise<void>;lastScope?:string;reconnectTimer?:ReturnType<typeof setTimeout>;reconnectAttempts?:number;timer?:ReturnType<typeof setTimeout>;failure?:{code:string;message:string;stack?:string}};
type Binding={artifactHash:string;clientContract?:ClientContract;runtime:Pick<PortableReducerRuntime,'prepare'|'execute'|'isPrepared'>;
 /** Generated public refs plus SDK-owned __local collections. No app RPC
  * supplies its own function metadata, authorization, SQL or visibility plan. */
 legacyStorage?:{replicaName:string;outboxName:string};
 references:readonly NativeFunctionReference[];collections:readonly string[];offlineMaxAgeMs:number|null;
};
const stable=(value:JsonValue):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable(value[key]!)).join(',')+'}';
async function digest(value:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');}
function text(value:unknown,max:number):asserts value is string{if(typeof value!=='string'||!value||value.length>max)throw new TypeError('Invalid native client identifier');}
/** Generated worker program. All server rows and reducer execution stay in the
 * elected dedicated worker. Main-thread calls select generated refs and bounded
 * pages; authenticated scope/identity comes exclusively from the session registry.
 * This assembly is not installed by codegen until the browser facade and verified
 * migration/fallback contract are complete. */
export class SQLiteClientProgram implements SQLiteOwnerProgram {
 private readonly liveWindows:SQLiteLiveWindows;
 private readonly safety:SQLiteSessionJournal;private readonly sessions:SQLiteWorkerSessions;private readonly tokens:SQLiteWorkerTokens;
 private readonly reducer:SQLiteReducerOwner;private readonly windows:SQLiteWindowStore;
 private readonly references=new Map<string,NativeFunctionReference>();private readonly connections=new Map<string,Connection>();private readonly tabs=new Map<string,string>();
 private readonly maintenance:SQLiteOwnerMaintenance;private readonly legacy:SQLiteLegacyBootstrap;
 private readonly ledger=new ByteLedger(21495808);private readonly transient=new SQLiteTransientBudget();private readonly transport=new SQLiteTransientBudget(2097152,256);private readonly transportAuth=new SQLiteTransientBudget(524288,8);private readonly catalogBytes:number;
 private readonly entityReadTimings:Array<{id:string;entity:string;ids:string[];count:number;received:number;sqlStart?:number;sqlEnd?:number;reply?:number}>=[];
 private connecting=Promise.resolve();private closed=false;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly binding:Binding){
  text(binding.artifactHash,256);sqlitePayloadBytes(binding.clientContract??{},65536);this.catalogBytes=sqlitePayloadBytes({references:binding.references,collections:binding.collections},2097152)+binding.references.length*64;if(this.catalogBytes>2097152)fail('BUDGET','Generated native function catalog exceeds its metadata allowance');if(binding.references.length>4096||binding.collections.length>4096)fail('BUDGET','Generated native function catalog exceeds budget');
  const builtins:FunctionReference[]=[];const collect=(value:unknown):void=>{if(!value||typeof value!=='object')return;if('kind' in value&&'path' in value){builtins.push(value as FunctionReference);return;}for(const child of Object.values(value))collect(child);};collect(control);
  for(const ref of [...builtins,...binding.references]){if(this.references.size>=4096)fail('BUDGET','Native function catalog entry limit exceeded');text(ref.path,1024);if(this.references.has(ref.path))throw new TypeError('Duplicate generated native reference');this.references.set(ref.path,ref);}
  this.catalogBytes+=sqlitePayloadBytes(builtins,262144)+builtins.length*64;if(this.catalogBytes>2097152)fail('BUDGET','Native function catalog exceeds its metadata allowance');
  for(const path of binding.collections){const ref=this.references.get(path);if(ref?.delivery!=='replica'||!ref.replica)throw new TypeError('Generated native dependency is not a Replica Collection');}
  this.safety=new SQLiteSessionJournal(owner);
  this.sessions=new SQLiteWorkerSessions({artifactHash:binding.artifactHash,offlineMaxAgeMs:binding.offlineMaxAgeMs,database:owner.database,canonical:owner.canonical,safety:this.safety,runOwner:owner.runOwner,fence:()=>owner.engine.verifyOwner()});
  this.windows=new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner());this.liveWindows=new SQLiteLiveWindows(owner,binding.artifactHash);void owner.runOwner(0,()=>this.liveWindows.ready()).catch(()=>{});
  this.tokens=new SQLiteWorkerTokens({recipient:(id,key,exclude)=>[...(this.connections.get(id)?.tabs??[])].find(tab=>tab!==exclude&&this.tabs.get(tab)===id&&this.connections.get(id)?.identityKey===key),eligible:(tab,id,key)=>this.tabs.get(tab)===id&&this.connections.get(id)?.identityKey===key,notifyTab:tab=>owner.notifyTab(tab)});
  this.reducer=new SQLiteReducerOwner(owner,{runtime:binding.runtime,artifactHash:binding.artifactHash,resolveTab:tab=>this.sessions.resolveTab(tab,true),context:(scope,includeCoverage)=>this.context(scope,includeCoverage)});
  this.legacy=new SQLiteLegacyBootstrap({owner,journal:this.reducer.journal,artifactHash:binding.artifactHash,replicaName:binding.legacyStorage?.replicaName??'gonvex-local-replica',outboxName:binding.legacyStorage?.outboxName??'gonvex-outbox'});
  this.ledger.register('native-function-catalog',()=>({name:'native-function-catalog',bytes:this.catalogBytes,entries:this.references.size,maxBytes:2097152,maxEntries:4096,evictions:0,refusals:0}));
  this.ledger.register('native-source-interests',()=>({name:'native-source-interests',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.sourceBytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.sources.size,0),maxBytes:2097152,maxEntries:2048,evictions:0,refusals:0}));
  this.ledger.register('native-live-watches',()=>({name:'native-live-watches',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.liveBytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.lives.size,0),maxBytes:524288,maxEntries:256,evictions:0,refusals:0}));
  this.ledger.register('native-control-watches',()=>({name:'native-control-watches',bytes:[...this.connections.values()].reduce((sum,connection)=>sum+connection.controlBytes,0),entries:[...this.connections.values()].reduce((sum,connection)=>sum+connection.controls.size,0),maxBytes:2097152,maxEntries:256,evictions:0,refusals:0}));
  this.ledger.register('native-source-descriptors',()=>({name:'native-source-descriptors',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.feed.stats().sourceBytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.feed.stats().sources,0),maxBytes:2097152,maxEntries:2048,evictions:0,refusals:0}));
  this.ledger.register('native-transient',()=>this.transient.stats());
  this.ledger.register('native-transport',()=>({...this.transport.stats(),name:'native-transport'}));
  this.ledger.register('native-transport-auth',()=>({...this.transportAuth.stats(),name:'native-transport-auth'}));
  this.ledger.register('native-intent-deliveries',()=>({name:'native-intent-deliveries',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.delivery.stats().bytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.delivery.stats().entries,0),maxBytes:2621440,maxEntries:64,evictions:0,refusals:0}));
  for(const snapshot of this.sessions.budgets())this.ledger.register(snapshot.name,()=>this.sessions.budgets().find(value=>value.name===snapshot.name)!);this.ledger.register('worker-token-requests',()=>this.tokens.stats());
  this.ledger.register('native-connections',()=>({name:'native-connections',bytes:this.connections.size*8192+this.tabs.size*512+[...this.connections.values()].reduce((sum,connection)=>sum+(connection.developer?.token.length??0)*2+connection.rejections.length*2048+connection.rejectionCursors.size*128,0),entries:this.connections.size+this.tabs.size,maxBytes:524288,maxEntries:72,evictions:0,refusals:0}));
  this.maintenance=new SQLiteOwnerMaintenance(owner,error=>{for(const connection of this.connections.values()){connection.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);}});this.maintenance.start();
 }
 private ref(path:unknown,kind?:string){text(path,1024);const ref=this.references.get(path);if(!ref||kind&&ref.kind!==kind||ref.authorization==='internal')fail('INVALID_ROW','Unknown generated native function');return ref;}
 private connection(tab:string){const id=this.tabs.get(tab),connection=id&&this.connections.get(id);if(!connection)fail('FENCED','Tab has no native connection');return connection;}
 private notify(connection:Connection){for(const tab of connection.tabs)void this.owner.notifyTab(tab).catch(()=>{});}
 private admission(connection:Connection,write=false){for(const tab of connection.tabs){try{return this.sessions.resolveTab(tab,write);}catch{/* Another tab may still have a current admission. */}}return undefined;}
 private async admissionKey(connection:Connection){
  const admitted=this.admission(connection);if(!admitted)return null;
  if(connection.admissionKey?.generation!==admitted.generation){const encoded=stable({scope:admitted.scope,identity:admitted.identity as JsonValue,artifact:this.binding.artifactHash}),release=this.transient.reserve(encoded.length*8+1024),value=digest(encoded).finally(release);connection.admissionKey={generation:admitted.generation,value};void value.catch(()=>{if(connection.admissionKey?.value===value)connection.admissionKey=undefined;});}
  const key=await connection.admissionKey!.value;if(this.admission(connection)?.generation!==admitted.generation)fail('FENCED','Native admission changed');return key;
 }
 private async context(scope:string,includeCoverage=true){
  for(const connection of this.connections.values()){
   const admitted=this.admission(connection);if(admitted?.scope!==scope)continue;const saved=await this.owner.canonical.loadSession(admitted.intentScope);if(!saved||saved.directive.visibilityScope!==scope)continue;
   const coverage:Record<string,ReadCoverage[string]>={};
   if(!includeCoverage)return {intentScope:admitted.intentScope,identity:admitted.identity,coverage};
   const rows=await this.owner.database.getAllAsync<{entity:string;key:string;columns:string}>(`SELECT json_extract(g.header,'$.entity') entity,json_extract(g.header,'$.key') key,json_group_array(DISTINCT c.value) columns
    FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation),json_each(g.header,'$.columns') c
    WHERE w.scope=? AND json_extract(g.header,'$.entityCoverage')='complete' AND json_extract(g.header,'$.completeness')='complete'
     AND json_extract(g.header,'$.cursor.epoch')=(SELECT value FROM _gonvex_replica_meta m WHERE m.scope=w.scope AND m.key='currentEpoch') AND json_extract(g.header,'$.artifactHash')=?
     AND json_extract(g.header,'$.cursor.revision')>=coalesce((SELECT applied FROM gv_native_feed_progress p WHERE p.scope=w.scope AND p.epoch=?),0) GROUP BY json_extract(g.header,'$.entity'),json_extract(g.header,'$.key')`,scope,this.binding.artifactHash,saved.directive.epoch);
   for(const row of rows)coverage[row.entity]={key:row.key,complete:true,columns:JSON.parse(row.columns) as string[]};
   const pending=await this.owner.database.getFirstAsync<{unclosed:number;applied:number}>('SELECT unclosed,applied FROM gv_native_feed_progress WHERE scope=? AND epoch=?',scope,saved.directive.epoch);
   const resetting=await this.owner.database.getFirstAsync('SELECT 1 FROM gv_native_feed_resets WHERE scope=?',scope);
   return {intentScope:admitted.intentScope,identity:admitted.identity,coverage,...(resetting||pending&&pending.unclosed>pending.applied?{predictionBlocked:true}:{})};
  }fail('FENCED','Native reducer scope is not admitted');
 }
 private recover(connection:Connection):Promise<void>{
  if(connection.recovered)return Promise.resolve();if(connection.recovering)return connection.recovering;
  const operation=(async()=>{
   const admitted=this.admission(connection);if(!admitted)return;
   for(;;){
    const count=await this.owner.runOwner(262144,async()=>{let count=0;await this.owner.database.withTransactionAsync(async()=>{
     const current=this.admission(connection);if(!current||current.readGeneration!==admitted.readGeneration)fail('FENCED','Recovery admission changed');
     const entries=await this.reducer.journal.unrecovered(admitted.intentScope);
     for(const entry of entries)await this.reducer.core.recoverLegacy({scope:admitted.scope,path:entry.path,commandId:entry.idempotencyKey,args:entry.args as JsonValue,now:entry.localExecution?.now??entry.createdAt,...(entry.localExecution?.intentEntropy===undefined?{}:{intentEntropy:entry.localExecution.intentEntropy})},entry);
     count=entries.length;
    });return count;});if(!count)break;
   }
   await this.reducer.rebase(admitted.scope);connection.recovered=true;
  })();connection.recovering=operation;void operation.then(()=>{connection.recovering=undefined;},()=>{connection.recovering=undefined;});return operation;
 }
 private checkCaller(caller:SQLiteOwnerCaller,connection:Connection,generation?:string){
  if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id)fail('FENCED','Native caller detached');const current=this.sessions.resolveTab(caller.tabID);if(generation!==undefined&&current.generation!==generation)fail('FENCED','Native read admission changed');return current;
 }
 private checkCachedCaller(caller:SQLiteOwnerCaller,connection:Connection,generation:string){const current=this.checkCaller(caller,connection);if(current.readGeneration!==generation)fail('FENCED','Cached read admission changed');return current;}
 private async readCommitted<T>(connection:Connection,admitted:ReturnType<SQLiteWorkerSessions['resolveTab']>,caller:SQLiteOwnerCaller,read:()=>Promise<T>){
  connection.feed.flushControls();
  await this.recover(connection);
  let reconciliationRequired=false;
  const attempt=()=>this.owner.runOwner(131072,async()=>{
   let result!:T;await this.owner.database.withTransactionAsync(async()=>{
    this.checkCachedCaller(caller,connection,admitted.readGeneration);const saved=await this.owner.canonical.loadSession(admitted.intentScope);
    if(!saved||saved.directive.visibilityScope!==admitted.scope)fail('FENCED','Cached read session changed');
    try{await connection.feed.assertCommittedReadable({scope:admitted.scope,epoch:saved.directive.epoch});}catch(error){reconciliationRequired=true;throw error;}
    result=await read();this.checkCachedCaller(caller,connection,admitted.readGeneration);
   });return result;
  },[],'committed-read');
  try{const result=await attempt();if(result!==null&&result!==undefined)return result;}catch(error){if((error as {code?:string})?.code!=='PAGE_CHANGED'||!reconciliationRequired)throw error;}
  // A cold/mixed bootstrap has no committed page yet. Wait outside the owner
  // queue for the existing feed barrier, then recheck in the read transaction.
  const saved=await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(admitted.intentScope));
  if(!saved||saved.directive.visibilityScope!==admitted.scope)fail('FENCED','Cached read session changed');
  await connection.feed.assertReadable({scope:admitted.scope,epoch:saved.directive.epoch});return attempt();
 }
 private feedFailed(connection:Connection,error:unknown){
  connection.activeSourceOpens?.clear();connection.pendingSourceOpens?.clear();
  if((error as {code?:string})?.code==='BUDGET'||(error as {resyncRequired?:boolean})?.resyncRequired){for(const source of connection.sources.values())source.forceSnapshot=true;error=Object.assign(Error('Replica ingress recovery in progress'),{code:'PAGE_CHANGED'});}
  const resetSource=(error as {resetSource?:string})?.resetSource;if(resetSource)for(const source of connection.sources.values())if(source.id===resetSource)source.forceSnapshot=true;
  const code=String((error as any)?.code??'SQLITE');connection.failure={code,message:String((error as any)?.message??error).slice(0,512),stack:String((error as any)?.stack??'').split('\n').slice(0,8).join('\n').slice(0,1536)};connection.client.close();this.notify(connection);
  if(!['PAGE_CHANGED','INVALID_ROW','BUDGET','INTENT_CONFLICT'].includes(code)||this.closed||this.connections.get(connection.id)!==connection)return;
  clearTimeout(connection.reconnectTimer);const attempts=Math.min(6,(connection.reconnectAttempts??0)+1);connection.reconnectAttempts=attempts;
  connection.reconnectTimer=setTimeout(()=>{connection.reconnectTimer=undefined;if(!this.closed&&this.connections.get(connection.id)===connection&&connection.tabs.size)connection.client.connect();},Math.min(60000,1000*2**(attempts-1)));
 }
 private scheduleDelivery(connection:Connection,delay=0){
  if(this.closed||this.connections.get(connection.id)!==connection)return;if(connection.timer)clearTimeout(connection.timer);
  connection.timer=setTimeout(()=>{connection.timer=undefined;void this.recover(connection).then(()=>connection.delivery.drainOnce()).then(result=>{this.notify(connection);if(result.nextDelay!==null)this.scheduleDelivery(connection,result.nextDelay);},error=>{if(this.closed||this.connections.get(connection.id)!==connection)return;const code=String((error as any)?.code??'SQLITE');if(code!=='FENCED')connection.failure??={code,message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);if(this.admission(connection)?.online)this.scheduleDelivery(connection,1000);});},Math.min(60000,Math.max(0,delay)));
 }
 private scheduleMigration(connection:Connection,delay=0){
  clearTimeout(connection.migrationTimer);if(this.closed||!connection.migrationSession||this.connections.get(connection.id)!==connection)return;
  connection.migrationTimer=setTimeout(()=>{connection.migrationTimer=undefined;const admitted=this.admission(connection),session=connection.migrationSession;if(!admitted||!session)return;if(connection.restoring){this.scheduleMigration(connection,100);return;}
   void this.legacy.step(admitted.intentScope,session,()=>this.connections.get(connection.id)===connection&&this.admission(connection)?.readGeneration===admitted.readGeneration).then(async result=>{if(result.imported){connection.recovered=false;await this.recover(connection);this.scheduleDelivery(connection);this.notify(connection);}if(!result.verified||result.watching)this.scheduleMigration(connection,!result.verified||result.moreIntents?100:1000);}).catch(error=>{if(this.closed||this.connections.get(connection.id)!==connection)return;connection.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);});
  },delay);
 }
 private async demandReducerDependencies(connection:Connection,ref:NativeFunctionReference){
  if(ref.localExecution!==1||connection.controlOnly||connection.public)return;
  await this.reducer.core.prepare(ref.path);
  const encoded=ref.localDependencyTables??'[]';sqlitePayloadBytes(encoded,32768);const tables:unknown=JSON.parse(encoded);if(!Array.isArray(tables)||tables.length>128||tables.some(entity=>typeof entity!=='string'||!entity||entity.length>256))fail('INVALID_ROW','Invalid generated reducer dependencies');
  for(const entity of tables as string[]){const dependency=this.references.get('__local.'+entity);if(!dependency?.replica)fail('INVALID_ROW','Generated reducer dependency is unavailable');if(!dependency.replica.maxRows||!dependency.replica.maxBytes)fail('BUDGET','Required reducer dependency must declare finite row and byte budgets');await this.addSource(connection,dependency!,{},undefined,true);}
 }
 private async watchControl(connection:Connection,ref:FunctionReference,args:JsonValue,tab:string){
  if(ref.scope!=='control'||ref.kind!=='query'||ref.delivery==='replica')fail('INVALID_ROW','Expected a generated Control query');
  const encoded=stable(args),signature=await digest(connection.id+'\0'+ref.path+'\0'+encoded);if(this.tabs.get(tab)!==connection.id)fail('FENCED','Control watch caller detached');
  let source=connection.controls.get(signature);if(!source){const bytes=sqlitePayloadBytes({path:ref.path,args},16384)+512;if(connection.controls.size>=32||connection.controlBytes+bytes+64>262144)fail('BUDGET','Control watch allowance exceeded');
   source={signature,ref,args:JSON.parse(encoded),tabs:new Set(),version:0,bytes,resultBytes:0,close:()=>{}};const selected=source;
   source.close=connection.client.watchControl(ref,source.args,value=>{
    if(this.connections.get(connection.id)!==connection||connection.controls.get(signature)!==selected)return;
    let resultBytes:number;try{resultBytes=sqlitePayloadBytes(value,131072);if(connection.controlBytes-selected.resultBytes+resultBytes>262144)fail('BUDGET','Control result allowance exceeded');}catch{selected.error='Control result exceeds its byte budget';selected.version++;this.notify(connection);return;}
    connection.controlBytes+=resultBytes-selected.resultBytes;selected.resultBytes=resultBytes;selected.value='result'in value?value.result:undefined;selected.error='error'in value?value.error:undefined;selected.version++;this.notify(connection);
   });connection.controls.set(signature,source);connection.controlBytes+=bytes;
  }
  if(!source.tabs.has(tab)){if(connection.controlBytes+64>262144)fail('BUDGET','Control interest allowance exceeded');source.tabs.add(tab);connection.controlBytes+=64;}return {signature};
 }
 private unwatchControl(connection:Connection,source:ControlSource,tab:string){
  if(!source.tabs.delete(tab))return;connection.controlBytes-=64;if(source.tabs.size)return;connection.controls.delete(source.signature);connection.controlBytes-=source.bytes+source.resultBytes;source.close();
 }
 private async addSource(connection:Connection,ref:FunctionReference,args:JsonValue,tab?:string,eager=false,lease='default'):Promise<Source>{
  if(tab)text(lease,128);if(ref.delivery!=='replica'||!ref.replica)fail('INVALID_ROW','Expected a generated Replica Collection');const encoded=stable(args),signature=await digest(this.binding.artifactHash+'\0'+connection.id+'\0'+ref.path+'\0'+encoded);let source=connection.sources.get(signature);
  if(source?.closing){await source.closing.catch(()=>{});return this.addSource(connection,ref,args,tab,eager,lease);}
  if(tab&&this.tabs.get(tab)!==connection.id)fail('FENCED','Native source caller detached');
  if(source?.retireTimer){clearTimeout(source.retireTimer);source.retireTimer=undefined;}
  if(!source){
   // Charge the retained source record. The generated ref/path is shared with
   // the catalog; lease/tab entries are charged separately below. The old
   // per-source 1KiB reserve double-counted shared catalog/descriptor state.
   const bytes=sqlitePayloadBytes({id:'source:'+signature,signature,args,tabs:[],leases:[],eager},16384);
   if(connection.sources.size>=256||connection.sourceBytes+bytes+(tab?192:0)>262144)fail('BUDGET',`Native subscription allowance exceeded (sources=${connection.sources.size}, bytes=${connection.sourceBytes}, eager=${[...connection.sources.values()].filter(source=>source.eager).length}); inspect subscriptionBudget for live paths`);
   source={id:'source:'+signature,signature,ref,args:JSON.parse(encoded),tabs:new Set(),leases:new Map(),eager,bytes};
   const completeEntity=!Object.keys(ref.replica.equalFilters??{}).length&&!ref.replica.excludeWhenSet?.length;
   connection.feed.register({id:source.id,signature,entity:ref.replica.table,key:ref.replica.key,path:ref.path,...(completeEntity?{completeEntity:true}:{}),...(ref.replica.columns?{columns:ref.replica.columns}:{})});
   connection.sources.set(signature,source);connection.sourceBytes+=bytes;
   if(connection.client.localSessionState()==='authenticated')await this.openSource(connection,source);
  }
  if(tab){text(lease,128);let leases=source.leases.get(tab);if(!leases?.has(lease)){if((leases?.size??0)>=64||connection.sourceBytes+192>262144)fail('BUDGET','Native source interests exceed budget');if(!leases)source.leases.set(tab,leases=new Set());leases.add(lease);source.tabs.add(tab);connection.sourceBytes+=192;}}source.eager||=eager;return source;
 }
 private releaseSourceInterest(connection:Connection,source:Source,tab:string,lease?:string){const leases=source.leases.get(tab);if(!leases)return;if(lease===undefined){connection.sourceBytes-=leases.size*192;leases.clear();}else if(leases.delete(lease))connection.sourceBytes-=192;if(!leases.size){source.leases.delete(tab);source.tabs.delete(tab);}}
 private deferSourceClose(connection:Connection,source:Source){
  if(source.retireTimer||source.closing)return;
  // Keep a released bounded descriptor for one frame so a provider remount
  // can reuse its existing server subscription instead of close/verify churn.
  source.retireTimer=setTimeout(()=>{
   source.retireTimer=undefined;
   if(this.connections.get(connection.id)===connection&&!source.tabs.size&&!source.eager&&!source.closing)void this.closeSource(connection,source).catch(error=>this.feedFailed(connection,error));
  },16);
 }
 private closeSource(connection:Connection,source:Source):Promise<void>{
  connection.pendingSourceOpens?.delete(source);
  clearTimeout(source.retireTimer);source.retireTimer=undefined;
  if(source.closing)return source.closing;
  source.closing=connection.feed.closeSource(source.id).catch(error=>{if((error as any)?.code!=='FENCED')throw error;}).finally(()=>{if(connection.sources.get(source.signature)===source){connection.sources.delete(source.signature);connection.sourceBytes-=source.bytes;}connection.activeSourceOpens?.delete(source);this.pumpSourceOpens(connection);});return source.closing;
 }
 private async watchLive(connection:Connection,ref:FunctionReference,args:JsonValue,tab:string,lease:string){
  text(lease,128);if(connection.controlOnly||connection.public||ref.delivery!=='live'||ref.scope==='control'||!ref.live)fail('INVALID_ROW','Expected a generated tenant Live Query');
  const bytes=sqlitePayloadBytes({path:ref.path,args},16384)+1024,signature=await digest(stable({path:ref.path,args}));if(this.tabs.get(tab)!==connection.id)fail('FENCED','Live query caller detached');let source=connection.lives.get(signature);
  if(!source){if(connection.lives.size>=32||connection.liveBytes+bytes>65536)fail('BUDGET','Native live query registry exceeded budget');const selected:LiveSource={leases:new Map(),signature,ref,args:structuredClone(args),tabs:new Set(),bytes,close:()=>{}};source=selected;connection.lives.set(signature,selected);connection.liveBytes+=bytes;
   try{selected.close=connection.client.watchLive(ref,selected.args,frame=>this.receiveLive(connection,selected,frame));}catch(error){connection.lives.delete(signature);connection.liveBytes-=bytes;throw error;}
  }let leases=source.leases.get(tab);if(!leases?.has(lease)){if((leases?.size??0)>=64||connection.liveBytes+192>65536)fail('BUDGET','Live query interests exceeded budget');if(!leases)source.leases.set(tab,leases=new Set());leases.add(lease);source.tabs.add(tab);connection.liveBytes+=192;}return {signature};
 }
 private receiveLive(connection:Connection,source:LiveSource,frame:SQLiteLiveQueryUpdate){
  if('error'in frame){source.error=frame.error;this.notify(connection);return;}
  const stream=connection.liveStream,authVersion=connection.authVersion,admitted=this.admission(connection);if(!admitted)return;let payloadBytes:number;try{payloadBytes=nativeLivePayloadBytes(source.ref,frame);}catch(error){source.error=error instanceof Error?error.message:'Invalid live result';this.notify(connection);return;}
  const current=()=>{if(connection.liveStream!==stream||connection.lives.get(source.signature)!==source||this.admission(connection)?.generation!==admitted.generation)fail('FENCED','Live window admission changed');};
  const run=()=>waitForSQLiteOwnerAdmission(()=>this.owner.runOwner(524288,()=>this.owner.database.withTransactionAsync(async()=>{
   if(connection.liveStream!==stream||connection.authVersion!==authVersion||connection.lives.get(source.signature)!==source||this.admission(connection)?.generation!==admitted.generation)fail('FENCED','Live query admission changed');
   const saved=await this.owner.canonical.loadSession(admitted.intentScope);if(!saved||saved.directive.visibilityScope!==admitted.scope)fail('FENCED','Live query session changed');connection.feed.observeCursor({epoch:frame.subscriptionRevision.epoch,revision:frame.subscriptionRevision.sequence});
   await this.liveWindows.stage(stream,admitted.scope,source.signature,source.ref,frame);source.error=undefined;
   const progress=await this.owner.database.getFirstAsync<{applied:number;unclosed:number}>('SELECT applied,unclosed FROM gv_native_feed_progress WHERE scope=? AND epoch=?',admitted.scope,saved.directive.epoch);
   if(progress&&!progress.unclosed&&await this.liveWindows.pending(stream,admitted.scope,frame.subscriptionRevision.epoch,progress.applied))await this.reducer.core.rebase(admitted.scope,async()=>{await this.liveWindows.publish(stream,admitted.scope,frame.subscriptionRevision.epoch,progress.applied);});
  }),[admitted.scope]),current);
  void (async()=>{const release=await this.transient.acquire(payloadBytes+262144);try{const loaded=new Set<string>();for(;;){try{await run();return;}catch(error){if(!(error instanceof SQLiteReducerPreparationRequiredError))throw error;if(loaded.has(error.path)||loaded.size>=128)fail('BUDGET','Live rebase preparation exceeded budget');loaded.add(error.path);await this.reducer.core.prepare(error.path);current();}}}finally{release();}})().then(()=>this.notify(connection),error=>{if(connection.liveStream===stream&&connection.lives.get(source.signature)===source){source.error=error instanceof Error?error.message:'Live source failed';this.notify(connection);}});
 }
 private unwatchLive(connection:Connection,source:LiveSource,tab:string,lease?:string){const leases=source.leases.get(tab);if(!leases)return;if(lease===undefined){connection.liveBytes-=leases.size*192;leases.clear();}else if(leases.delete(lease))connection.liveBytes-=192;if(!leases.size){source.leases.delete(tab);source.tabs.delete(tab);}if(source.tabs.size)return;source.close();connection.lives.delete(source.signature);connection.liveBytes-=source.bytes;const stream=connection.liveStream;void this.owner.runOwner(0,()=>this.owner.database.withTransactionAsync(()=>this.liveWindows.retire(stream,source.signature))).catch(()=>{});}
 private pumpSourceOpens(connection:Connection){
  const active=connection.activeSourceOpens??=new Set(),pending=connection.pendingSourceOpens??=new Set();
  while(active.size<sqliteOwnerRequestCapacity&&pending.size){const source=pending.values().next().value!;pending.delete(source);if(connection.sources.get(source.signature)===source&&!source.closing)void this.openSource(connection,source).catch(error=>this.feedFailed(connection,error));}
 }
 private async openSource(connection:Connection,source:Source){
  const active=connection.activeSourceOpens??=new Set();if(active.has(source))return;
  if(active.size>=sqliteOwnerRequestCapacity){(connection.pendingSourceOpens??=new Set()).add(source);return;}
  active.add(source);try{await this.openSourceNow(connection,source);}catch(error){active.delete(source);throw error;}
 }
 private async openSourceNow(connection:Connection,source:Source){
  const admitted=this.admission(connection),version=connection.authVersion,capabilities=connection.client.serverInfo(),sum=capabilities.replicaSetDigest===1;
  let cached:{cursor:import('@gonvex/protocol').ReplicaCursor;digest:string}|undefined;
  if(admitted&&!source.forceSnapshot&&capabilities.replicaResume===1)await this.owner.runOwner(0,()=>this.owner.database.withTransactionAsync(async()=>{
   const saved=await this.owner.canonical.loadSession(admitted.intentScope);if(!saved||saved.directive.visibilityScope!==admitted.scope)return;
   // This is a resume offer, not a read certificate. An unfinished global
   // cohort must not erase another source's durably verified cursor. The
   // server still validates its epoch, retained history and aggregate digest.
   // Explicit cache resets and generated-contract changes require a snapshot.
   if(await this.owner.database.getFirstAsync('SELECT 1 FROM gv_native_feed_resets WHERE scope=?',admitted.scope))return;
   const window=await this.windows.header(admitted.scope,source.signature),header=window?.header;
   if(header?.kind!=='replica'||header.source!=='server'||header.entity!==source.ref.replica!.table||header.key!==source.ref.replica!.key||header.artifactHash!==this.binding.artifactHash||!header.cursor||!header.aggregateDigest?.startsWith(sum?'sum128-v1:':'xor1:'))return;
   const columns=source.ref.replica!.columns??Object.keys(this.owner.engine.schema[source.ref.replica!.table]!.fields);
   if(header.columns?.length!==columns.length||!header.columns.every((column,index)=>column===columns[index]))return;
   cached={cursor:header.cursor,digest:header.aggregateDigest};
  }));
  if(connection.authVersion!==version||connection.sources.get(source.signature)!==source||source.closing||admitted&&this.admission(connection)?.generation!==admitted.generation)return;
  connection.client.sendOwnerMessage({type:'replica.open',id:source.id,path:source.ref.path,args:source.args,...(cached??{digest:sum?new ReplicaSetDigest().digest():new ReplicaSetDigest().legacyDigest()})});
 }
 private connect(input:any,caller:SQLiteOwnerCaller):Promise<unknown>{
  const work=this.connecting.then(async()=>{
   if(this.closed||caller.signal.aborted)fail('FENCED','Native client is closed');if(input.artifactHash!==undefined&&input.artifactHash!==this.binding.artifactHash)fail('SCHEMA_MISMATCH','Browser and owner module artifacts differ');await this.maintenance.ready();text(input.url,2048);const url=new URL(input.url);if(!['ws:','wss:'].includes(url.protocol)||url.username||url.password||url.hash)throw new TypeError('Invalid native WebSocket URL');
   const controlOnly=input.role==='control',publicClient=input.public===true;if(input.role!==undefined&&!controlOnly||input.public!==undefined&&typeof input.public!=='boolean'||controlOnly&&publicClient)throw new TypeError('Invalid native connection mode');
   const auth=input.auth??{};sqlitePayloadBytes(auth,65536);for(const field of Object.keys(auth))if(!['project','tenant','token','identity'].includes(field))throw new TypeError('Unsupported native auth field');for(const field of ['project','tenant'])if(auth[field]!==undefined&&(typeof auth[field]!=='string'||auth[field].length>512))throw new TypeError('Invalid native tenant routing');if(auth.identity!==undefined){text(auth.identity?.sub,512);if(auth.identity.iss!==undefined)text(auth.identity.iss,512);if(Object.keys(auth.identity).some(field=>!['sub','iss'].includes(field)))throw new TypeError('Invalid external identity hint');}if(auth.token!==undefined&&(typeof auth.token!=='string'||auth.token.length>16384))throw new TypeError('Invalid native token');
   if(publicClient&&(!auth.project||!auth.tenant||auth.token||auth.identity))throw new TypeError('Public clients require fixed anonymous tenant routing');
   const identityKey=await digest(stable({controlOnly,public:publicClient,url:url.href,project:auth.project??null,tenant:auth.tenant??null,identity:auth.identity??null,credential:auth.identity?null:auth.token?await digest(auth.token):null})),id='connection:'+identityKey;
   const old=this.tabs.get(caller.tabID);if(old&&old!==id)this.detachTab(caller.tabID);
   let connection=this.connections.get(id);
   if(!connection){
    if(this.connections.size>=8)fail('BUDGET','Native connection allowance exceeded');this.sessions.open(id);
    let record!:Connection;
    const feed=new SQLiteNativeFeed({owner:this.owner,sessions:this.sessions,connection:id,artifactHash:this.binding.artifactHash,previewStaging:async(source,snapshot)=>{
     if(!this.owner.publishPreviewTab||record.controlOnly||record.public)return;
     const selected=[...record.sources.values()].find(value=>value.id===source.id);
     if(!selected?.previewInterest||!selected.tabs.size)return;
     const admission=this.admission(record);if(!admission?.online)return;
     const authVersion=record.authVersion,key=await this.admissionKey(record);
     if(!key||record.authVersion!==authVersion)return;
     const current=()=>this.connections.get(record.id)===record&&record.authVersion===authVersion&&this.admission(record)?.generation===admission.generation&&record.sources.get(selected.signature)===selected;
     let page;
     try{page=await this.owner.pendingReader.page({scope:admission.scope,entity:source.entity,subscription:source.id,snapshot,limit:24},current);}catch(error){
      if(['BUDGET','PAGE_CHANGED'].includes(String((error as any)?.code)))return;
      throw error;
     }
     page.serverVerified=false;const preview={id:crypto.randomUUID(),signature:selected.signature,admissionKey:key,page};
     try{sqlitePayloadBytes(preview,65536);}catch{return;}
     const tabs=[...selected.tabs].filter(tab=>this.tabs.get(tab)===record.id);
     const sent=tabs.filter(tab=>this.owner.publishPreviewTab!(tab,preview));
     return ()=>{for(const tab of sent)if(current()&&this.tabs.get(tab)===record.id)this.owner.publishPreviewTab!(tab,{...preview,page:null});};
    },freshSnapshot:async id=>{const source=[...record.sources.values()].find(source=>source.id===id);if(!source)fail('FENCED','Reset source detached');source!.forceSnapshot=true;record.activeSourceOpens?.delete(source!);await this.openSource(record,source!);},sourcePrepared:id=>{for(const source of record.sources.values())if(source.id===id)record.activeSourceOpens?.delete(source);this.pumpSourceOpens(record);},sourceReady:id=>{for(const source of record.sources.values())if(source.id===id){source.forceSnapshot=false;record.activeSourceOpens?.delete(source);}this.pumpSourceOpens(record);},windowsPending:(scope,epoch,revision)=>this.liveWindows.pending(record.liveStream,scope,epoch,revision),publishWindows:async(scope,epoch,revision)=>{await this.liveWindows.publish(record.liveStream,scope,epoch,revision);},reserve:bytes=>this.transient.reserve(bytes),reserveSnapshot:bytes=>this.transient.hold(bytes),acquire:bytes=>this.transient.acquire(bytes),prepare:path=>this.reducer.core.prepare(path),authority:(scope,mutate)=>this.reducer.core.rebase(scope,mutate).then(()=>{}),send:message=>record.client.sendOwnerMessage(message),advanceWatermark:revision=>{record.reconnectAttempts=0;record.failure=undefined;record.client.advanceOwnerWatermark(revision);this.notify(record);},failed:error=>this.feedFailed(record,error)});
    const restore=(scope:string,generation:number)=>{
     const retiredStream=record.liveStream;record.liveStream=crypto.randomUUID();void this.owner.runOwner(0,()=>this.owner.database.withTransactionAsync(()=>this.liveWindows.retire(retiredStream))).catch(()=>{});feed.authenticating(scope,generation);record.rejections.length=0;if(controlOnly||publicClient){record.restoring=undefined;return;}record.restoreScope=scope;record.restoreGeneration=generation;
     const current=()=>!this.closed&&this.connections.get(id)===record&&record.restoreScope===scope&&record.restoreGeneration===generation;
     const prior=record.restoring;
     const restoring=(async()=>{await prior?.catch(()=>{});if(!current())return;
      const saved=await this.safety.session(scope)??await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(scope));
      let selected:LocalReplicaSession|undefined;
      try{selected=await this.legacy.prepare(scope,saved?.artifactHash===this.binding.artifactHash?saved:undefined,current);}catch(error){if((error as any)?.code!=='SCHEMA_MISMATCH')throw error;}
      if(!current())return;
      if(selected){record.lastIdentity??=stable(selected.identity as JsonValue);record.migrationSession=selected;record.recovered=false;}
      try{await this.sessions.restore(id,current);}catch(error){if((error as any)?.code!=='SCHEMA_MISMATCH'&&((error as any)?.code!=='FENCED'||current()))throw error;}
     })();record.restoring=restoring;void restoring.then(()=>{if(record.restoring!==restoring)return;record.restoring=undefined;record.migrationBlocked=undefined;this.scheduleMigration(record);this.notify(record);},error=>{if(record.restoring!==restoring)return;record.migrationBlocked=error;record.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(record);});
    };
    const host={opening:restore,authenticating:restore,session:async(session:any,current:()=>boolean)=>{
     await record.restoring?.catch(()=>{});const identity=session.identity?stable(session.identity):undefined;if(record.lastIdentity&&record.lastIdentity!==identity){record.authVersion++;record.supportCommands.length=0;}
     const cutover=(async()=>{await feed.session(session,current);record.lastIdentity=identity;
      if(session.directive&&session.identity){const saved=await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(session.intentScope));if(!saved)fail('FENCED','Native session disappeared before legacy cutover');await this.legacy.prepare(session.intentScope,saved,current);record.migrationSession=saved;}
      record.denied=false;record.recovered=false;await this.recover(record);
      if(record.lastScope&&record.lastScope!==session.directive?.visibilityScope)void this.maintenance.retire(record.lastScope,[...record.sources.values()].map(source=>source.id)).catch(()=>{});record.lastScope=session.directive?.visibilityScope;
      if(!session.directive)record.failure=undefined;
      if(session.directive&&session.identity){record.activeSourceOpens?.clear();record.pendingSourceOpens?.clear();for(const path of record.requiredReducers)await this.demandReducerDependencies(record,this.ref(path,'reducer'));for(const source of [...record.sources.values()].sort((a,b)=>Number(!!b.tabs.size)-Number(!!a.tabs.size)))if(!source.closing)await this.openSource(record,source);this.scheduleDelivery(record);this.scheduleMigration(record);}
      this.notify(record);
     })();record.restoring=cutover;try{await cutover;record.migrationBlocked=undefined;}catch(error){record.migrationBlocked=error;throw error;}finally{if(record.restoring===cutover)record.restoring=undefined;}
    },binary:(frame:ArrayBuffer,generation:number)=>feed.binary(frame,generation),frame:(message:any,generation:number)=>feed.frame(message,generation),quarantined:(generation:number)=>{record.denied=true;record.rejections.length=0;record.supportCommands.length=0;for(const source of record.controls.values())if(source.ref.authorization!=='public'){record.controlBytes-=source.resultBytes;source.resultBytes=0;source.value=undefined;source.error='Session unavailable';source.version++;}feed.quarantined(generation);this.notify(record);},authenticated:()=>this.notify(record),authError:(message:string,details:{definitive:boolean;code?:string})=>{record.authError={id:crypto.randomUUID(),message:message.slice(0,512),...details,...(details.code?{code:details.code.slice(0,128)}:{})};this.notify(record);},supportCommand:(command:SupportCommand)=>{if(record.supportSequence>=Number.MAX_SAFE_INTEGER){record.failure={code:'FENCED',message:'Support notification sequence exhausted'};this.notify(record);return;}record.supportCommands.push({...command,sequence:++record.supportSequence});if(record.supportCommands.length>8)record.supportCommands.shift();this.notify(record);},developerCredential:(token:string)=>{record.developer={id:crypto.randomUUID(),token,acked:new Set()};this.notify(record);},disconnected:()=>{feed.disconnected();this.notify(record);},failed:(error:unknown)=>feed.failed(error)};
    const client=new SQLiteNativeTransport(url.href,{...auth,controlOnly,public:publicClient,reserve:bytes=>this.transport.reserve(bytes),releaseAuth:this.transportAuth.reserve(65536),clientContract:this.binding.clientContract,ownerHost:host,fetchToken:publicClient?undefined:request=>this.tokens.fetch(id,identityKey,request.forceRefreshToken)});
    const delivery=new SQLiteReducerDelivery({owner:this.owner,reducer:this.reducer,admission:()=>{const current=this.admission(record);return current&&{scope:current.scope,intentScope:current.intentScope,generation:current.generation,online:current.online};},rejected:event=>{if(record.rejectionSequence>=Number.MAX_SAFE_INTEGER){record.failure={code:'FENCED',message:'Intent notification sequence exhausted'};return;}record.rejections.push({...event,error:event.error.slice(0,512),sequence:++record.rejectionSequence});if(record.rejections.length>8)record.rejections.shift();this.notify(record);},send:entry=>client.callOwnerReducer({...this.ref(entry.path,'reducer'),artifactHash:entry.localExecution?.artifactHash??this.binding.artifactHash,intentEntropy:entry.localExecution?.intentEntropy},entry.args as JsonValue,entry.idempotencyKey),updateRequired:message=>{record.failure={code:'SCHEMA_MISMATCH',message};this.notify(record);}});
    record=connection={requiredReducers:new Set(),requiredReducerBytes:0,liveStream:crypto.randomUUID(),lives:new Map(),liveBytes:0,rejections:[],rejectionSequence:0,rejectionCursors:new Map(),supportCommands:[],supportSequence:0,supportCursors:new Map(),controlOnly,public:publicClient,authVersion:0,route:{project:auth.project,tenant:auth.tenant,identity:auth.identity},id,url:url.href,identityKey,client,feed,delivery,tabs:new Set(),sources:new Map(),sourceBytes:0,controls:new Map(),controlBytes:0};this.connections.set(id,record);
   }
   if(this.tabs.size>=64&&!this.tabs.has(caller.tabID))fail('BUDGET','Native tab allowance exceeded');this.tabs.set(caller.tabID,id);connection.tabs.add(caller.tabID);this.sessions.attachTab(caller.tabID,id);
   connection.client.connect();return {connection:id,identityKey};
  });this.connecting=work.then(()=>{},()=>{});return work;
 }
 async call(value:unknown,caller:SQLiteOwnerCaller):Promise<unknown>{
  sqlitePayloadBytes(value,131072);if(!value||typeof value!=='object'||Array.isArray(value))throw new TypeError('Invalid native client request');const input=structuredClone(value) as any;if(this.closed||caller.signal.aborted)fail('FENCED','Native client is closed');
  if(input.method==='connect')return this.connect(input,caller);
  const connection=this.connection(caller.tabID),callerAuthVersion=connection.authVersion;
  const cachedReadGeneration=['localLivePage','entities','replicaPage','collectionPage','livePage'].includes(input.method)?this.admission(connection)?.readGeneration:undefined;
  if((connection.controlOnly||connection.public)&&['resetLocalReplica','localLivePage','entities','replicaPage','replicaPendingPage','collectionPage','livePage','livePendingPage'].includes(input.method))fail('INVALID_ROW','This connection has no tenant replica');
  if(connection.controlOnly&&['query','action','reducer','prepareReducer'].includes(input.method)&&this.ref(input.path).scope!=='control')fail('INVALID_ROW','Control connections cannot invoke tenant functions');
  if(['resetLocalReplica','localLivePage','entities','replicaPage','replicaPendingPage','collectionPage','livePage','livePendingPage','reducer'].includes(input.method)){
   await connection.restoring;
   // Only committed reads can cross a same-identity credential refresh. Writes
   // and unverified pending stages retain their strict authentication fence.
   if(connection.authVersion!==callerAuthVersion&&(!cachedReadGeneration||this.sessions.resolveTab(caller.tabID).readGeneration!==cachedReadGeneration))fail('FENCED','Caller authentication changed during restoration');
   if(connection.migrationBlocked)throw connection.migrationBlocked;
  }
  switch(input.method){
   case 'controls':{
    const admitted=this.admission(connection);let key:string|null|undefined;
    if(admitted)key=await this.admissionKey(connection);else if(connection.denied||!connection.restoring)key=null;
    return {tokens:this.tokens.poll(caller.tabID),status:connection.client.connectionState(),serverInfo:connection.client.serverInfo(),authentication:connection.client.authenticationState(),session:admitted?admitted.online?'authenticated':'restored':connection.client.localSessionState(),...(key!==undefined?{admissionKey:key}:{}),failure:connection.failure??null,rejections:connection.rejections.filter(event=>event.sequence>(connection.rejectionCursors.get(caller.tabID)??0)),supportCommands:connection.supportCommands.filter(event=>event.sequence>(connection.supportCursors.get(caller.tabID)??0)),authError:connection.authError??null,...(connection.developer&&!connection.developer.acked.has(caller.tabID)?{developerCredential:{id:connection.developer.id,token:connection.developer.token}}:{})};
   }
   case 'reportError':{const version=connection.authVersion;await connection.client.reportError(input.type,input.payload);if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id||version!==connection.authVersion)fail('FENCED','Error report caller session changed');return null;}
   case 'restoreSession':{
    // Reconnect may replace the restoration while an offline caller awaits it.
    // Follow the current same-identity operation, never an obsolete socket's
    // readiness result. This does not wait for network authentication.
    for(;;){const restoring=connection.restoring;if(!restoring)break;await restoring;if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id||this.connections.get(connection.id)!==connection)fail('FENCED','Restore caller detached');if(connection.restoring===restoring)break;}
    if(connection.migrationBlocked)throw connection.migrationBlocked;return !!this.admission(connection);
   }
   case 'resetLocalReplica':{
    if(input.keepOutbox!==undefined&&typeof input.keepOutbox!=='boolean')throw new TypeError('Invalid cache reset option');
    const admitted=this.checkCaller(caller,connection),version=connection.authVersion;
    if(!admitted.online||!connection.client.connectionState().isWebSocketConnected)throw new GonvexClientError('Cannot reset local data while offline. Reconnect and try again; cached data was kept.',{code:'disconnected'});
    connection.client.close();let discardedIntents=0;
    try{await this.owner.runOwner(0,()=>this.owner.database.withTransactionAsync(async()=>{
     const current=this.checkCaller(caller,connection);if(version!==connection.authVersion||current.intentScope!==admitted.intentScope||current.scope!==admitted.scope)fail('FENCED','Cache reset admission changed');
     if(input.keepOutbox===false)discardedIntents=await this.reducer.journal.discardUnsent(admitted.intentScope);
     await this.owner.database.runAsync('INSERT OR IGNORE INTO gv_native_feed_resets VALUES(?)',admitted.scope);
     await this.owner.canonical.clear(admitted.scope);
     await this.owner.database.runAsync('UPDATE gv_snapshot_stages SET discarded=1 WHERE scope=?',admitted.scope);
     await this.owner.database.runAsync('DELETE FROM gv_native_feed_progress WHERE scope=?',admitted.scope);
     await this.reducer.core.rebase(admitted.scope);
    }),[admitted.scope]);connection.recovered=false;}
    finally{if(!this.closed&&this.connections.get(connection.id)===connection)connection.client.connect();}
    return {discardedIntents,resubscribed:connection.sources.size+connection.lives.size};
   }
   case 'outboxCount':{const admitted=this.admission(connection);return admitted?(await this.reducer.journal.summary(admitted.intentScope)).queued:0;}
   case 'rejectionsAck':if(!Number.isSafeInteger(input.sequence)||input.sequence<0||input.sequence>connection.rejectionSequence)throw new TypeError('Invalid rejection notification ACK');connection.rejectionCursors.set(caller.tabID,Math.max(connection.rejectionCursors.get(caller.tabID)??0,input.sequence));return null;
   case 'supportAck':if(!Number.isSafeInteger(input.sequence)||input.sequence<0||input.sequence>connection.supportSequence)throw new TypeError('Invalid support notification ACK');connection.supportCursors.set(caller.tabID,Math.max(connection.supportCursors.get(caller.tabID)??0,input.sequence));return null;
   case 'credentialAck':if(connection.developer&&input.id===connection.developer.id)connection.developer.acked.add(caller.tabID);return null;
   case 'watchControl':return this.watchControl(connection,this.ref(input.path,'query'),input.args??{},caller.tabID);
   case 'unwatchControl':{const source=connection.controls.get(input.signature);if(source)this.unwatchControl(connection,source,caller.tabID);return null;}
   case 'controlState':{const source=connection.controls.get(input.signature);if(!source?.tabs.has(caller.tabID))fail('FENCED','Tab does not own that Control watch');if(input.afterVersion===source.version)return {version:source.version};return {version:source.version,...(source.value!==undefined?{result:source.value}:{}),...(source.error?{error:source.error}:{})};}
   case 'token':this.tokens.respond(caller.tabID,input.response);return null;
   case 'auth':{
    const controlOnly=input.role==='control',publicClient=input.public===true;if(input.role!==undefined&&!controlOnly||input.public!==undefined&&typeof input.public!=='boolean'||controlOnly&&publicClient)throw new TypeError('Invalid native connection mode');
   const auth=input.auth??{};sqlitePayloadBytes(auth,65536);if(Object.keys(auth).some(field=>!['token'].includes(field)))throw new TypeError('Native identity changes require a new connection');if(auth.token!==undefined&&(typeof auth.token!=='string'||auth.token.length>16384))throw new TypeError('Invalid native token');if(!connection.route.identity)return this.connect({url:connection.url,auth:{...connection.route,...auth},...(connection.controlOnly?{role:'control'}:{})},caller);connection.client.setAuth(auth,()=>{connection.authVersion++;this.tokens.invalidate(connection.id);});return null;
   }
   case 'watchLive':return this.watchLive(connection,this.ref(input.path,'query'),input.args??{},caller.tabID,input.lease??'default');
   case 'unwatchLive':{const source=connection.lives.get(input.signature);if(source)this.unwatchLive(connection,source,caller.tabID,input.lease??'default');return null;}
   case 'livePendingPage':{const source=connection.lives.get(input.signature);if(!source?.tabs.has(caller.tabID))fail('FENCED','Tab does not own that live window');const admitted=this.checkCaller(caller,connection),stream=connection.liveStream;return this.owner.runOwner(262144,async()=>{let result;await this.owner.database.withTransactionAsync(async()=>{this.checkCaller(caller,connection,admitted.generation);if(connection.liveStream!==stream||connection.lives.get(source.signature)!==source||!source.tabs.has(caller.tabID))fail('FENCED','Pending live query detached');const saved=await this.owner.canonical.loadSession(admitted.intentScope);if(!saved)fail('FENCED','Pending live session changed');const databaseEpoch=connection.feed.databaseEpoch();if(!databaseEpoch)return;result=await this.liveWindows.page(stream,admitted.scope,source.signature,databaseEpoch,{offset:input.offset,limit:input.limit,generation:input.generation});this.checkCaller(caller,connection,admitted.generation);});return result;});}
   case 'livePage':{const source=connection.lives.get(input.signature);if(!source?.tabs.has(caller.tabID))fail('FENCED','Tab does not own that live window');if(source.error)throw Error(source.error);const admitted=this.checkCaller(caller,connection);return this.readCommitted(connection,admitted,caller,async()=>{if(connection.lives.get(source.signature)!==source||!source.tabs.has(caller.tabID))fail('FENCED','Live query detached');return await this.owner.reader.window({scope:admitted.scope,signature:source.signature,offset:input.offset,limit:input.limit,generation:input.generation,expected:input.expected})??null;});}
   case 'watchReplica':{if(connection.controlOnly||connection.public)fail('INVALID_ROW','This connection does not own tenant replica collections');const source=await this.addSource(connection,this.ref(input.path,'query'),input.args??{},caller.tabID,false,input.lease??'default');return {id:source.id,signature:source.signature};}
   case 'unwatchReplica':{text(input.signature,128);const source=connection.sources.get(input.signature);if(source){this.releaseSourceInterest(connection,source,caller.tabID,input.lease??'default');if(!source.tabs.size&&!source.eager)this.deferSourceClose(connection,source);}return null;}
   case 'entities':{
    const timing={id:caller.requestID,entity:input.entity,ids:[...input.ids],count:input.ids.length,received:Date.now(),sqlStart:undefined as number|undefined,sqlEnd:undefined as number|undefined,reply:undefined as number|undefined};this.entityReadTimings.push(timing);if(this.entityReadTimings.length>64)this.entityReadTimings.shift();
    const admission=this.checkCaller(caller,connection);try{const result=await this.readCommitted(connection,admission,caller,async()=>{timing.sqlStart=Date.now();this.checkCachedCaller(caller,connection,admission.readGeneration);const result=await this.owner.reader.entities({scope:admission.scope,entity:input.entity,ids:input.ids,offset:input.offset,expected:input.expected});timing.sqlEnd=Date.now();this.checkCachedCaller(caller,connection,admission.readGeneration);return result;});timing.reply=Date.now();if(!input.readDiagnostics)return result;const reply={...result,_sqliteReadTiming:timing};try{sqlitePayloadBytes(reply,131072);return reply;}catch{return result;}}finally{timing.reply??=Date.now();}
   }
   case 'replicaPendingPage':{
    text(input.signature,128);const source=connection.sources.get(input.signature);if(!source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Tab does not own that source');const admitted=this.checkCaller(caller,connection),pending=await connection.feed.pendingSource(source.id);if(!pending)return null;
    return this.owner.runOwner(131072,async()=>{const current=()=>{this.checkCaller(caller,connection,admitted.generation);return connection.sources.get(source.signature)===source&&(source.eager||source.tabs.has(caller.tabID));};if(!current())fail('FENCED','Pending source detached');if(!await this.owner.database.getFirstAsync('SELECT 1 FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=? AND discarded=0',admitted.scope,pending.subscription,pending.snapshot)){if(input.expected)fail('PAGE_CHANGED','Pending source was replaced');return null;}if(await this.owner.database.getFirstAsync('SELECT 1 FROM gv_snapshot_activation_receipts WHERE scope=? AND subscription=? AND snapshot=?',admitted.scope,pending.subscription,pending.snapshot)){if(input.expected)fail('PAGE_CHANGED','Pending source has become canonical');return null;}return this.owner.pendingReader.page({scope:admitted.scope,...pending,after:input.after,limit:input.limit,expected:input.expected},current);});
   }
   case 'replicaPage':{
    text(input.signature,128);const source=connection.sources.get(input.signature);if(!source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Tab does not own that source');const admission=this.checkCaller(caller,connection);source.previewInterest=true;return this.readCommitted(connection,admission,caller,async()=>{this.checkCachedCaller(caller,connection,admission.readGeneration);const result=await this.owner.reader.window({scope:admission.scope,signature:source.signature,offset:input.offset,limit:input.limit,generation:input.generation,expected:input.expected});this.checkCachedCaller(caller,connection,admission.readGeneration);if(connection.sources.get(source.signature)!==source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Native source was detached');return result??null;});
   }
   case 'declareReducerReadSet':{
    const ref=this.ref(input.path,'reducer');if(connection.controlOnly||connection.public)fail('INVALID_ROW','Tenant read-set requires a tenant replica');
    if(ref.localExecution!==1)return null;
    if(!connection.requiredReducers.has(ref.path)){const bytes=ref.path.length*2+64;if(connection.requiredReducers.size>=128||connection.requiredReducerBytes+bytes>32768)fail('BUDGET','Required reducer metadata allowance exceeded');connection.requiredReducers.add(ref.path);connection.requiredReducerBytes+=bytes;}
    await connection.restoring;if(this.admission(connection))await this.demandReducerDependencies(connection,ref);return null;
   }
   case 'subscriptionBudget':return {entries:connection.sources.size,bytes:connection.sourceBytes,maxEntries:256,maxBytes:262144,paths:[...connection.sources.values()].map(source=>({path:source.ref.path,refs:source.tabs.size,eager:source.eager,closing:!!source.closing,bytes:source.bytes}))};
   case 'prepareReducer':{const ref=this.ref(input.path,'reducer');if(ref.localExecution===1)await this.reducer.core.prepare(ref.path);if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id)fail('FENCED','Reducer preparation caller detached');return null;}
   case 'localLivePage':{
    const ref=this.ref(input.path,'query');if(input.signature!==undefined)text(input.signature,128);
    const source=input.signature!==undefined?connection.sources.get(input.signature):[...connection.sources.values()].find(value=>{const replica=value.ref.replica;return !!replica&&(value.eager||value.tabs.has(caller.tabID))&&replica.table===ref.live?.entity&&!Object.keys(replica.equalFilters??{}).length&&!replica.excludeWhenSet?.length;});
    if(!source){if(input.signature===undefined)return null;fail('FENCED','Local query has no admitted collection');}
    if(!(source.eager||source.tabs.has(caller.tabID))||!source.ref.replica||!ref.live?.plan||ref.live.entity!==source.ref.replica.table)fail('FENCED','Local query has no admitted collection');
    const admitted=this.checkCaller(caller,connection);
    // Scoped local reads can demand generated reducer dependencies. These
    // stay on disk and never hydrate into the renderer. Network preparation
    // runs outside the SQL FIFO; only certified complete data answers a join.
    const cached=await this.owner.runOwner(0,()=>this.windows.header(admitted.scope,source.signature));if(!cached)return null;if(cached.header.truncated)return {supported:false,reason:'truncatedReplica',generation:cached.generation};
    const related=new Set<string>();const visit=(expression:any)=>{if(!expression)return;if(expression.operator==='inRelation'){related.add(expression.relation.table);visit(expression.relation.where);}for(const child of expression.children??[])visit(child);};visit(ref.live.plan.where);
    for(const entity of related){const dependency=this.references.get('__local.'+entity);if(dependency?.replica)await this.addSource(connection,dependency,{},undefined,true);}
    return this.readCommitted(connection,admitted,caller,async()=>{
     const header=await this.windows.header(admitted.scope,source.signature);if(!header)return null;
     if(header.header.completeness!=='complete'||header.header.truncated)return {supported:false,reason:'truncatedReplica',generation:header.generation};
     const args=input.args??{};if(!args||typeof args!=='object'||Array.isArray(args))throw new TypeError('Local query arguments must be an object');
     const context=await this.context(admitted.scope),identity=admitted.identity;
     const lowered=lowerSQLiteLivePlan(this.owner.engine.schema,ref.live!.plan!,args,{'member.id':identity.member?.id??null,'auth.account.id':identity.auth.account?.id??null},{textOrder:'binary',offlineSearch:true});
     if(!lowered.supported)return {supported:false,reason:lowered.reason,generation:header.generation};
     if(lowered.offset)return {supported:false,reason:'offsetRequiresKeyset',generation:header.generation};
     if(input.generation!==undefined&&input.generation!==header.generation)fail('PAGE_CHANGED','Local query membership changed');
     const {table:_table,membership:_membership,...options}=lowered.plan;options.limit=Math.min(128,options.limit);if(input.after!==undefined)options.after=input.after;
     const plan=sqliteCollectionPagePlan(source.ref,source.args,options,{signature:source.signature,generation:header.generation},new Set(Object.keys(context.coverage)),{allowStaleRelations:true});
     const staleRelations=[...related].filter(entity=>!context.coverage[entity]);
     const page=await this.reducer.predictions.page(admitted.scope,plan.plan,plan.allowLocalInserts);const total=input.after===undefined?await this.reducer.predictions.count(admitted.scope,plan.plan,plan.allowLocalInserts):undefined;
     this.checkCachedCaller(caller,connection,admitted.readGeneration);
     return {...page,...(total!==undefined?{total}:{}),supported:true,stale:staleRelations.length>0,staleRelations,partialSearch:!!lowered.partialSearch,complete:page.cursor===null,generation:header.generation};
    });
   }
   case 'collectionPage':{
    text(input.signature,128);const source=connection.sources.get(input.signature);if(!source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Tab does not own that collection');const admitted=this.checkCaller(caller,connection);
    return this.readCommitted(connection,admitted,caller,async()=>{const owns=()=>{this.checkCachedCaller(caller,connection,admitted.readGeneration);if(connection.sources.get(source.signature)!==source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Native collection was detached');};owns();const header=await this.windows.header(admitted.scope,source.signature);if(!header) return null;
     if(input.generation!==undefined&&input.generation!==header.generation)fail('PAGE_CHANGED','Collection membership changed');const context=await this.context(admitted.scope),lowered=sqliteCollectionPagePlan(source.ref,source.args,input.plan,{signature:source.signature,generation:header.generation},new Set(Object.keys(context.coverage)));
     if(input.plan.after&&input.generation===undefined)throw new TypeError('Collection keyset continuation requires its window generation');if(input.plan.search&&source.ref.replica?.columns&&this.owner.engine.schema[lowered.plan.table]?.search?.some(field=>!source.ref.replica!.columns!.includes(field)))fail('INVALID_ROW','Search requires undeclared collection columns');
     const page=await this.reducer.predictions.page(admitted.scope,lowered.plan,lowered.allowLocalInserts);owns();return {...page,generation:header.generation};
    });
   }
   case 'reducer':{const ref=this.ref(input.path,'reducer');if(ref.scope==='control'||connection.public){const version=connection.authVersion,result=await connection.client.callOwnerReducer(ref,input.args??{},caller.requestID);if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id||version!==connection.authVersion)fail('FENCED','Control caller session changed');sqlitePayloadBytes(result,131072);return {commandId:caller.requestID,result};}if(input.expectedAdmissionKey!==undefined&&input.expectedAdmissionKey!==await this.admissionKey(connection))fail('FENCED','Reducer was issued under an older admission');await this.recover(connection);if(ref.localExecution!==1)fail('INVALID_ROW','Native reducer has no generated portable body');await this.demandReducerDependencies(connection,ref);let receipt;try{receipt=await this.reducer.invoke({path:ref.path,args:input.args??{},now:input.now,intentEntropy:input.intentEntropy},caller);}catch(error){connection.recovered=false;throw error;}this.scheduleDelivery(connection);return receipt;}
   case 'query':case 'action':{
    const ref=this.ref(input.path,input.method==='query'?'query':'action');if(ref.delivery==='replica'||ref.delivery==='live'&&ref.scope!=='control')fail('INVALID_ROW','Reactive reads require bounded native subscriptions');const version=connection.authVersion;if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id)fail('FENCED','Remote caller detached');const result=await (input.method==='query'?connection.client.query(ref,input.args??{}):connection.client.action(ref,input.args??{}));if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id||version!==connection.authVersion)fail('FENCED','Remote caller session changed');sqlitePayloadBytes(result,131072);return result;
   }
   case 'status':return {connection:connection.client.connectionState(),session:connection.client.localSessionState(),admission:this.admission(connection)??null,failure:connection.failure??null};
   default:fail('INVALID_ROW','Unknown native client operation');
  }
 }
 detachTab(tabID:string){const id=this.tabs.get(tabID);if(!id)return;this.tabs.delete(tabID);this.sessions.detachTab(tabID);const connection=this.connections.get(id);if(!connection)return;connection.tabs.delete(tabID);connection.rejectionCursors.delete(tabID);connection.supportCursors.delete(tabID);connection.developer?.acked.delete(tabID);this.tokens.detachTab(tabID);
  for(const source of [...connection.lives.values()])this.unwatchLive(connection,source,tabID);
  for(const source of [...connection.controls.values()])this.unwatchControl(connection,source,tabID);
  // A surviving follower needs acknowledged per-source cancellation. The
  // final tab instead closes the transport/feed below, which releases all
  // source state at once. Do not allocate hundreds of redundant close waiters
  // immediately before disconnecting their socket.
  if(connection.tabs.size)for(const source of connection.sources.values())if(source.tabs.has(tabID)){this.releaseSourceInterest(connection,source,tabID);if(!source.tabs.size&&!source.eager)void this.closeSource(connection,source).catch(()=>{});}
  if(!connection.tabs.size){for(const source of connection.sources.values())clearTimeout(source.retireTimer);if(connection.lastScope)void this.maintenance.retire(connection.lastScope,[...connection.sources.values()].map(source=>source.id)).catch(()=>{});if(connection.timer)clearTimeout(connection.timer);clearTimeout(connection.reconnectTimer);clearTimeout(connection.migrationTimer);this.tokens.invalidate(id);connection.delivery.close();connection.client.dispose();connection.feed.close();this.sessions.close(id);this.connections.delete(id);}
 }
 async diagnostics(){return {entityReadTimings:this.entityReadTimings.slice(),entitySQLTimings:this.owner.reader.readTimings(),liveDecode:liveStageBudgetStats(),feeds:[...this.connections.values()].map(connection=>({...connection.feed.stats(),...connection.client.diagnostics(),restoring:!!connection.restoring,recovering:!!connection.recovering,recovered:!!connection.recovered})),progress:await this.owner.database.getAllAsync('SELECT observed,applied,unclosed FROM gv_native_feed_progress LIMIT 8')};}
 budgets(){return this.ledger.snapshot();}
 close(){if(this.closed)return;this.closed=true;this.maintenance.close();for(const tab of [...this.tabs.keys()])this.detachTab(tab);this.tokens.close();this.legacy.close();}
}
