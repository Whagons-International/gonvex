import {SQLiteLastVisiblePages} from './sqlite-last-visible-pages.js';
import {SQLiteBrowserMemberships,type SQLiteBrowserMembership} from './sqlite-browser-memberships.js';
import type {SQLitePendingLivePage} from './sqlite-live-windows.js';
import type {JsonValue} from '@gonvex/protocol';
import type {FunctionReference,GonvexClientAuth,ConnectionState,QueuedReducerOutcome,CallOptions,GonvexClientOptions,ReducerRejectionEvent,LocalExecutionFallbackEvent,GonvexTelemetryEvent,SupportCommand,ResetLocalReplicaOptions,ResetLocalReplicaResult} from './index.js';
import type {SQLiteReplicaBroker} from './sqlite-broker.js';
import type {SQLiteNativeWindowResult,SQLiteNativeEntityResult} from './sqlite-native-replica-reader.js';
import type {SQLitePendingSnapshotPage} from './sqlite-pending-snapshot-reader.js';
import type {SQLiteReducerReceipt} from './sqlite-reducer-core.js';
import type {SQLiteTokenRequest} from './sqlite-worker-tokens.js';
import {GonvexErrorReporter} from './error-reporter.js';
import {GonvexClientError} from './client-error.js';
import type {SQLiteCollectionPageOptions} from './sqlite-collection-plan.js';
import type {ReplicaCollectionSubscriptionState,ReplicaRow} from './local-replica.js';
import type {SQLiteTypedPage} from './sqlite-typed-engine.js';
import {SQLiteBrowserEntities} from './sqlite-browser-entities.js';
import {SQLiteBrowserControls} from './sqlite-browser-controls.js';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import {ByteLedger,BoundedByteCache} from './byte-budget.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';

type Auth=Pick<GonvexClientAuth,'project'|'tenant'|'identity'|'token'>;
type Broker=Pick<SQLiteReplicaBroker,'request'|'subscribeChanges'|'close'|'stats'>;
type Controls={supportCommands?:Array<SupportCommand&{sequence:number}>;rejections?:Array<ReducerRejectionEvent&{sequence:number}>;serverInfo?:Readonly<import('@gonvex/protocol').ServerCapabilities>;admissionKey?:string|null;tokens:SQLiteTokenRequest[];status:unknown;authentication?:'unavailable'|'authenticating'|'authenticated'|'denied';session:string;failure:{code:string;message:string}|null;authError?:{id:string;message:string;definitive:boolean;code?:string}|null;developerCredential?:{id:string;token:string}};
type Handle={connection:string;identityKey:string};
export type SQLiteCollectionPageResult=SQLiteTypedPage&{generation:string};
export type SQLiteLocalQueryPage=(SQLiteTypedPage&{supported:true;complete:boolean;partialSearch?:boolean;stale?:boolean;staleRelations?:string[];total?:number;generation:string})|{supported:false;reason:string;generation:string;rows?:never};
type Page=SQLiteLocalQueryPage|SQLitePendingLivePage|SQLiteNativeWindowResult|SQLitePendingSnapshotPage|SQLiteNativeEntityResult|SQLiteCollectionPageResult|null;
type WatchSnapshot={page:Page|undefined;stale:boolean;version:number};
type Watch={previewID?:string;startupPublications?:number;bootPageKey?:string;kind:'replica'|'live';state:WatchSnapshot;retiring?:Promise<void>;key:string;remoteKey:string;lease:string;path:string;args:JsonValue;bytes:number;refs:number;source?:{id?:string;signature:string};bound:number;opening?:Promise<void>;listeners:Set<()=>void>;unpin?:()=>void;stale:boolean;version:number;invalidation:number;pendingDisplay?:boolean;compatibleFirstPage?:boolean};
export type SQLiteBrowserOptions={broker:Broker;artifactHash?:string;onUpdateRequired?:GonvexClientOptions['onUpdateRequired'];onLocalExecutionFallback?:GonvexClientOptions['onLocalExecutionFallback'];errorReporting?:GonvexClientOptions['errorReporting'];role?:'control';public?:{tenant:string};auth?:Auth;fetchToken?:GonvexClientAuth['fetchToken']};
export type SQLiteWatchOptions={view?:JsonValue;deferStart?:boolean};
export type SQLiteReplicaPageOptions={offset?:number;limit?:number;generation?:string;expected?:SQLiteNativeWindowResult['generation']};
export type SQLiteReplicaWatch<T extends JsonValue=ReplicaRow>={
 /** A mounted watch retains one bounded page, never a collection-sized list. */
 page(options?:SQLiteReplicaPageOptions):Promise<SQLiteNativeWindowResult|null>;
 localQueryPage(ref:Pick<FunctionReference,'path'>,args?:JsonValue):Promise<SQLiteLocalQueryPage|null>;
 queryPage(plan:SQLiteCollectionPageOptions,generation?:string):Promise<(SQLiteTypedPage&{generation:string})|null>;
 pendingPage(options?:{after?:number;limit?:number;expected?:SQLitePendingSnapshotPage['generation']}):Promise<SQLitePendingSnapshotPage|null>;
 snapshot():{page:Page|undefined;stale:boolean;version:number};
 subscribe(listener:()=>void):()=>void;
 /** Bounded compatibility surface for existing small-collection React hooks. */
 onUpdate(listener:()=>void):()=>void;
 localReplicaResult():T[]|undefined;
 localReplicaState():ReplicaCollectionSubscriptionState<T & ReplicaRow>|undefined;
 close():void;
};
const stable=(value:JsonValue):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable(value[key]!)).join(',')+'}';
/** Main-thread bounded RPC facade. It imports no SQLite host, portable reducer
 * runtime or application transport. The elected worker owns all durable state.
 * Generated clients install this facade by default. SQLite persistence and
 * one-time legacy migration stay in the elected worker. */
// Compare only JSON values already retained by the bounded page cache. No
// encoded copy or additional row map survives publication.
function sameJSON(left:unknown,right:unknown):boolean{
 if(left===right)return true;
 if(left===null||right===null||typeof left!=='object'||typeof right!=='object')return false;
 if(Array.isArray(left))return Array.isArray(right)&&left.length===right.length&&left.every((value,index)=>sameJSON(value,right[index]));
 if(Array.isArray(right))return false;
 const a=left as Record<string,unknown>,b=right as Record<string,unknown>;
 let count=0;for(const key in a)if(Object.hasOwn(a,key)){count++;if(!Object.hasOwn(b,key)||!sameJSON(a[key],b[key]))return false;}
 for(const key in b)if(Object.hasOwn(b,key))count--;
 return count===0;
}
export class SQLiteBrowserClient {
 readonly replicaQueryBackend='sqlite' as const;
 private readonly lastVisible:SQLiteLastVisiblePages;
 private cachedPaint?:{promise:Promise<void>;release:()=>void};
 /** The rendering caller owns this lifecycle hold. Only sealed watches wait;
  * cache misses, auth, intent durability and the owner's SQL FIFO keep running. */
 deferCachedDisplayUntilCommit(){
  if(this.closed||this.cachedPaint)return;
  let release!:()=>void;const promise=new Promise<void>(resolve=>{release=resolve;});
  this.cachedPaint={promise,release};
 }
 commitCachedDisplay(){
  const pending=this.cachedPaint;this.cachedPaint=undefined;
  if(pending){try{performance.measure('gonvex-startup:cached-display-commit',{start:performance.now(),end:performance.now()});}catch{}pending.release();}
 }

 private closedBudgets?:ReturnType<ByteLedger['snapshot']>;
 private startupPublications=0;private bootRestored=false;private displayTimer?:ReturnType<typeof setTimeout>;
 private readonly displayHide=()=>{void this.persistDisplay().catch(()=>{});};
 private readonly displayVisibility=()=>{if(typeof document!=='undefined'&&document.visibilityState==='hidden')this.displayHide();};
 private async displayFence(){
  if(this.closed||this.publicTenant||this.options.role==='control')return 0;
  const auth=this.auth;
  if(!auth.project||!auth.tenant||!auth.identity||!auth.token||!this.options.artifactHash)return 0;
  return this.lastVisible.open({url:this.url,artifact:this.options.artifactHash,project:auth.project,tenant:auth.tenant,identity:JSON.parse(stable(auth.identity as JsonValue)),credential:auth.token});
 }
 private async persistDisplay(){
  const revision=this.revision;await this.displayFence();if(this.closed||revision!==this.revision||!this.admissionKey)return;
  for(const watch of this.watches.values())if(watch.compatibleFirstPage&&!this.lastVisible.restored(this.pages.get(watch.key)))this.lastVisible.remember(watch.key,this.pages.get(watch.key),this.admissionKey);
  await this.lastVisible.persist();
 }
 private scheduleDisplay(){if(this.closed||this.displayTimer)return;this.displayTimer=setTimeout(()=>{this.displayTimer=undefined;void this.persistDisplay().catch(()=>{});},25);}
 private fetchToken?:GonvexClientAuth['fetchToken'];private readonly controlListeners=new Set<()=>void>();private readonly authListeners=new Set<(message:string,details:{definitive:boolean;code?:string})=>void>();private readonly credentialListeners=new Set<(token:string)=>void>();private readonly reporter?:GonvexErrorReporter;private lastRejection=0;private lastSupport=0;private readonly supportListeners=new Set<(command:SupportCommand)=>void>();private readonly rejectionListeners=new Set<(event:ReducerRejectionEvent)=>void>();private readonly fallbackListeners=new Set<(event:LocalExecutionFallbackEvent)=>void>();private readonly telemetryListeners=new Set<(event:GonvexTelemetryEvent)=>void>();private authSerial=0;private readonly authWaiters=new Set<()=>void>();private lastUpdateRequired?:string;private lastAuthError?:string;private lastCredential?:string;
 private readonly publicTenant?:string;private auth:Auth;private closed=false;private revision=0;private dataRevision=0;private admissionKey?:string|null;private epoch?:number;private handle?:Handle;private connecting?:Promise<Handle>;private connectingRevision=0;
 private readonly watches=new Map<string,Watch>();private watchBytes=0;
 private readonly entityRPCTimings:Array<{entity:string;ids:string[];enqueue:number;admitted?:number;sent?:number;replyReceived?:number;owner?:Record<string,unknown>}>=[];
 entityReadDiagnostics(){return this.entityRPCTimings.slice();}
 private readonly entityReads=new Map<string,Promise<SQLiteNativeEntityResult>>();
 private readonly reads=new Map<string,Promise<Page>>();private readBytes=0;
 private readonly ledger=new ByteLedger(12582912);private readonly entityView:SQLiteBrowserEntities;private readonly controlQueries:SQLiteBrowserControls;
 private readonly memberships=new SQLiteBrowserMemberships();
 private readonly requests=new SQLiteTransientBudget(2097152,128);
 private preparationLane:Promise<void>=Promise.resolve();
 private readonly preparations=new Map<string,Promise<void>>();
 private readonly pages=new BoundedByteCache<string,Page>('browser-row-pages',2097152,192,this.ledger);
 private readonly tokenRequests=new Set<string>();private readonly tokenCancels=new Map<string,()=>void>();private polling?:Promise<void>;private pollAgain=false;
 private readonly releaseChanges:()=>void;private controlsValue?:Pick<Controls,'status'|'session'|'failure'|'authentication'|'serverInfo'>;
 constructor(readonly url:string,private readonly options:SQLiteBrowserOptions){
  const parsed=new URL(url);if(!['ws:','wss:'].includes(parsed.protocol)||parsed.username||parsed.password||parsed.hash)throw new TypeError('Invalid native browser URL');
  sqlitePayloadBytes(options.auth??{},65536);
  // Control/public clients have no tenant display context and must never read
  // or revoke another clients sealed tenant page.
  this.lastVisible=new SQLiteLastVisiblePages(options.role==='control'||options.public?undefined:(()=>{try{return globalThis.sessionStorage;}catch{return undefined;}})(),524288);
  this.auth=structuredClone(options.auth??{});if(options.public){if(options.role==='control'||!options.public.tenant.trim()||!this.auth.project?.trim()||this.auth.token||this.auth.identity||this.auth.tenant||options.fetchToken)throw new TypeError('Public clients require fixed project/tenant routing without account credentials');this.publicTenant=options.public.tenant.trim();this.auth.tenant=this.publicTenant;parsed.searchParams.set('public','1');this.url=parsed.href;}this.fetchToken=options.fetchToken;if(options.onLocalExecutionFallback)this.fallbackListeners.add(options.onLocalExecutionFallback);this.controlQueries=new SQLiteBrowserControls(input=>this.rpc(input),this.ledger);this.entityView=new SQLiteBrowserEntities((entity,ids,expected,offset)=>this.entities(entity,ids,expected,offset),this.ledger,error=>{if(this.closed||['PAGE_CHANGED','FENCED'].includes(String((error as any)?.code)))return;this.controlsValue={...this.controlsValue,status:this.controlsValue?.status,session:this.controlsValue?.session??(this.bootRestored?'restored':'unavailable'),failure:{code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)}};for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}});
  globalThis.addEventListener?.('pagehide',this.displayHide);if(typeof document!=='undefined')document.addEventListener('visibilitychange',this.displayVisibility);
  this.ledger.register('browser-last-visible-pages',()=>this.lastVisible.stats());
  this.ledger.register('browser-live-memberships',()=>this.memberships.stats());
  this.ledger.register('browser-shared-reads',()=>({name:'browser-shared-reads',bytes:this.readBytes,entries:this.reads.size,maxBytes:524288,maxEntries:384,evictions:0,refusals:0}));
  this.ledger.register('browser-watch-metadata',()=>({name:'browser-watch-metadata',bytes:this.watchBytes,entries:this.watches.size,maxBytes:524288,maxEntries:192,evictions:0,refusals:0}));
  this.ledger.register('browser-request-admission',()=>({...this.requests.stats(),name:'browser-request-admission'}));
  this.ledger.register('browser-observers',()=>({name:'browser-observers',bytes:(this.controlListeners.size+this.authListeners.size+this.credentialListeners.size+this.rejectionListeners.size+this.fallbackListeners.size+this.telemetryListeners.size)*128+this.authWaiters.size*256,entries:this.controlListeners.size+this.authListeners.size+this.credentialListeners.size+this.rejectionListeners.size+this.fallbackListeners.size+this.telemetryListeners.size+this.authWaiters.size,maxBytes:26624,maxEntries:200,evictions:0,refusals:0}));
  this.ledger.register('browser-auth',()=>({name:'browser-auth',bytes:sqlitePayloadBytes(this.auth,65536),entries:1,maxBytes:65536,maxEntries:1,evictions:0,refusals:0}));
  this.ledger.register('browser-controls',()=>({name:'browser-controls',bytes:this.controlsValue?sqlitePayloadBytes(this.controlsValue,65536):0,entries:Number(!!this.controlsValue),maxBytes:65536,maxEntries:1,evictions:0,refusals:0}));
  this.ledger.register('browser-token-requests',()=>({name:'browser-token-requests',bytes:this.tokenRequests.size*40960,entries:this.tokenRequests.size,maxBytes:327680,maxEntries:8,evictions:0,refusals:0}));
  this.releaseChanges=options.broker.subscribeChanges((scope,_sequence,_reset,preview)=>{
   const epoch=options.broker.stats().ownerEpoch;if(epoch!==undefined&&this.epoch!==undefined&&epoch!==this.epoch)this.invalidate(false);if(epoch!==undefined)this.epoch=epoch;
   if(scope==='@preview'){
    if(preview&&preview.admissionKey===this.admissionKey){
     for(const watch of this.watches.values())if(watch.kind==='replica'&&watch.source?.signature===preview.signature){
      if(preview.page){const retained=this.pages.get(watch.key);if(retained&&'window'in retained&&retained.window.header.completeness==='complete'&&!watch.previewID)continue;watch.previewID=preview.id;watch.pendingDisplay=true;this.store(watch,preview.page,false,true);}
      else if(watch.previewID===preview.id){watch.previewID=undefined;watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);watch.stale=true;this.notify(watch);}
     }
    }else if(!preview){
     for(const watch of this.watches.values())if(watch.pendingDisplay){watch.invalidation++;watch.stale=true;this.notify(watch);}
    }
    return;
   }
   if(scope==='@pending'){
    // Coalesce staging hints across sources without invalidating complete
    // cached pages. Canonical authority is published at its terminal cohort.
    for(const watch of this.watches.values()){const page=this.pages.get(watch.key);if(watch.pendingDisplay||page&&'serverVerified'in page&&page.serverVerified===false){watch.invalidation++;watch.stale=true;this.notify(watch);}}
    // Staging can invalidate a still-unresolved entity batch's storage lease.
    // Certified cached values only change on a canonical publication.
    this.entityView.refresh(false);void this.pollControls().catch(()=>{});return;
   }
   if(scope!=='@control'){for(const watch of this.watches.values()){watch.invalidation++;watch.stale=true;this.notify(watch);}this.entityView.refresh();}
   this.controlQueries.refresh();void this.pollControls().catch(()=>{});
  });
  if(options.errorReporting&&this.auth.project){this.reporter=new GonvexErrorReporter({...options.errorReporting,project:this.auth.project,tenant:this.auth.tenant,client:this,transport:(kind,payload)=>this.reportError(kind,payload)});this.ledger.register('error-reporting',()=>this.reporter!.memoryBudget());}
 }
 private current(revision:number){if(this.closed||revision!==this.revision)fail('FENCED','Native browser session changed');}
 private notify(watch:Watch){watch.version++;watch.state={page:this.pages.get(watch.key),stale:watch.stale,version:watch.version};for(const listener of watch.listeners)try{listener();}catch{/* Observer only. */}}
 private invalidate(identity:boolean,preserveDisplay=false){if(identity&&!preserveDisplay)this.commitCachedDisplay();this.revision++;this.dataRevision++;this.lastRejection=0;this.lastSupport=0;if(identity){this.bootRestored=false;this.lastVisible.revoke({preserveStored:preserveDisplay});this.admissionKey=undefined;this.lastAuthError=undefined;this.lastCredential=undefined;}this.handle=undefined;this.controlsValue=undefined;this.controlQueries.invalidate(identity);this.memberships.invalidate();this.entityView.invalidate(identity);
  for(const watch of this.watches.values()){watch.bound=-1;watch.source=undefined;watch.opening=undefined;watch.invalidation++;watch.stale=true;if(identity){watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);}this.notify(watch);}
 }
 private admit(key:string|null){
  const accepted=this.lastVisible.admit(key),rejectedBoot=this.bootRestored&&!accepted;if(key===null||!accepted)this.bootRestored=false;
  if(this.admissionKey===key)return;
  const changed=this.admissionKey!==undefined||key===null||rejectedBoot;this.admissionKey=key;if(!changed)return;this.dataRevision++;this.controlQueries.invalidate(true);this.entityView.invalidate(true);
  for(const watch of this.watches.values()){watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);watch.invalidation++;watch.stale=true;this.notify(watch);}
 }
 connect():Promise<Handle>{
  if(this.closed)return Promise.reject(Object.assign(Error('Native browser client is closed'),{code:'FENCED'}));if(this.handle)return Promise.resolve(this.handle);if(this.connecting){const pending=this.connecting,pendingRevision=this.connectingRevision;return pending.then(handle=>this.handle?handle:this.connect(),error=>{if(!this.closed&&pendingRevision!==this.revision)return this.connect();throw error;});}
  const revision=this.revision,operation=this.options.broker.request<Handle>('clientCall',{method:'connect',url:this.url,auth:this.auth,...(this.options.role?{role:this.options.role}:{}),...(this.publicTenant?{public:true}:{}),artifactHash:this.options.artifactHash}).then(handle=>{this.current(revision);this.handle=handle;this.epoch=this.options.broker.stats().ownerEpoch;
   // Credential hints must be serviced while authentication is waiting. Never
   // put a token fetch behind the durable connection/session readiness barrier.
   queueMicrotask(()=>{void this.pollControls().catch(()=>{});for(const watch of this.watches.values())void this.open(watch).then(()=>{if(!this.closed&&this.lastVisible.restored(this.pages.get(watch.key))){watch.stale=true;this.notify(watch);}}).catch(()=>{});});return handle;
  });this.connecting=operation;this.connectingRevision=revision;void operation.then(()=>{if(this.connecting===operation)this.connecting=undefined;},()=>{if(this.connecting===operation)this.connecting=undefined;});return operation;
 }
 async setAuth(input:GonvexClientAuth){
  this.current(this.revision);if(this.publicTenant&&(input.token||input.fetchToken||input.identity||Object.hasOwn(input,'tenant')&&input.tenant!==this.publicTenant||Object.hasOwn(input,'project')&&input.project!==this.auth.project))throw new TypeError('Public client routing is fixed');this.authSerial++;for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}
  if(Object.hasOwn(input,'fetchToken'))this.fetchToken=input.fetchToken;const auth:Auth={};for(const key of ['project','tenant','identity','token'] as const)if(Object.hasOwn(input,key))Object.assign(auth,{[key]:input[key]});
  sqlitePayloadBytes(auth,65536);const identity=stable({project:Object.hasOwn(auth,'project')?auth.project??null:this.auth.project??null,tenant:Object.hasOwn(auth,'tenant')?auth.tenant??null:this.auth.tenant??null,identity:Object.hasOwn(auth,'identity')?auth.identity as JsonValue??null:this.auth.identity as JsonValue??null});
  // Initial credential adoption must leave the sealed disk envelope available
  // for exact-context verification. A previously admitted/credentialed client
  // still revokes it on every account, tenant or credential change.
  const preserveDisplay=!this.auth.token&&!this.bootRestored&&this.admissionKey===undefined&&(!!auth.token||!!auth.identity);
  const old=stable({project:this.auth.project??null,tenant:this.auth.tenant??null,identity:this.auth.identity as JsonValue??null});if(Object.hasOwn(auth,'token')&&auth.token!==this.auth.token){this.bootRestored=false;this.lastVisible.revoke({preserveStored:preserveDisplay});}this.auth={...this.auth,...structuredClone(auth)};if(Object.hasOwn(auth,'tenant'))this.reporter?.setTenant(auth.tenant);if(auth.project!==undefined)this.reporter?.setProject(auth.project);
  if(identity!==old||!this.auth.identity&&Object.hasOwn(auth,'token')){this.invalidate(true,preserveDisplay);await this.connect();return;}
  const revision=this.revision;await this.connect();this.current(revision);await this.options.broker.request('clientCall',{method:'auth',auth:{token:this.auth.token}});this.current(revision);void this.pollControls().catch(()=>{});
 }
 private readonly completedTokenRequests=new Set<string>();
 private async storageCall<T>(input:any,commandID?:string):Promise<T>{
  const revision=this.revision,readOnly=new Set(['controls','status','entities','replicaPage','collectionPage','localLivePage','livePage','replicaPendingPage','livePendingPage','controlState']).has(input?.method);let retries=0;
  for(;;){this.current(revision);try{return await this.options.broker.request<T>('clientCall',input,readOnly?2000:90000,commandID);}catch(error){
   if(!readOnly||(error as any)?.code!=='TIMEOUT'||(error as any)?.outcome!=='rejected'||retries>=2)throw error;
   // A timed-out read has no durable side effects. Keep its mounted snapshot
   // and one request reservation while the owner/router recovers; never apply
   // this policy to an unknown write outcome or an admission failure.
   await new Promise(resolve=>setTimeout(resolve,Math.min(1000,100*2**Math.min(retries++,4))));
  }}
 }
 private async rpc<T>(input:unknown,commandID?:string,reserved=false):Promise<T>{const revision=this.revision,dataRevision=this.dataRevision,tenant=!!input&&typeof input==='object'&&'method'in input&&['resetLocalReplica','localLivePage','entities','replicaPage','replicaPendingPage','collectionPage','livePage','livePendingPage','reducer'].includes(String(input.method));this.current(revision);const timing=(input as any)?.method==='entities'?{entity:(input as any).entity as string,ids:[...(input as any).ids] as string[],enqueue:Date.now(),admitted:undefined as number|undefined,sent:undefined as number|undefined,replyReceived:undefined as number|undefined,owner:undefined as Record<string,unknown>|undefined}:undefined;if(timing){this.entityRPCTimings.push(timing);if(this.entityRPCTimings.length>64)this.entityRPCTimings.shift();}const bytes=sqlitePayloadBytes(input,131072),owned=structuredClone(input) as any,release=reserved?()=>{}:await this.requests.acquire(bytes+512);if(timing)timing.admitted=Date.now();try{this.current(revision);if(tenant&&owned.method==='reducer'&&this.admissionKey!==undefined)owned.expectedAdmissionKey=this.admissionKey;await this.connect();this.current(revision);if(timing)timing.sent=Date.now();const value=await this.storageCall<T>(owned,commandID);if(timing){timing.replyReceived=Date.now();const reply=value as any;if(reply&&typeof reply==='object'&&reply._sqliteReadTiming){timing.owner=reply._sqliteReadTiming;delete reply._sqliteReadTiming;}}this.current(revision);if(tenant&&dataRevision!==this.dataRevision)fail('FENCED','Native page admission changed');return value;}finally{release();}}
 pollControls():Promise<void>{
  if(this.closed)return Promise.resolve();if(this.polling){this.pollAgain=true;return this.polling;}
  const operation=(async()=>{do{this.pollAgain=false;const revision=this.revision;try{const handle=await this.connect(),controls=await this.storageCall<Controls>({method:'controls'});this.current(revision);sqlitePayloadBytes(controls,65536);const admissionKey=this.admissionKey;if(controls.admissionKey!==undefined&&(controls.admissionKey!==null||!this.bootRestored||controls.authentication==='denied'||!['unavailable','authenticating'].includes(controls.authentication??'unavailable')))this.admit(controls.admissionKey);
   const next={status:controls.status,serverInfo:controls.serverInfo,authentication:controls.authentication,session:controls.session,failure:controls.failure};
   // React connection observers use snapshot identity. Source progress does
   // not change connection state, and metadata-only updates retain it too.
   if(this.controlsValue&&JSON.stringify(this.controlsValue.status)===JSON.stringify(next.status))next.status=this.controlsValue.status;
   const controlsChanged=admissionKey!==this.admissionKey||JSON.stringify(this.controlsValue)!==JSON.stringify(next);
   const failureChanged=JSON.stringify(this.controlsValue?.failure??null)!==JSON.stringify(next.failure??null);
   if(controlsChanged)this.controlsValue=next;
   if(failureChanged)for(const watch of this.watches.values()){watch.invalidation++;watch.stale=true;this.notify(watch);}
   if(controls.failure?.code==='SCHEMA_MISMATCH'&&this.lastUpdateRequired!==controls.failure.message){this.lastUpdateRequired=controls.failure.message;try{this.options.onUpdateRequired?.(controls.failure.message);}catch{/* Observer only. */}}if(controlsChanged)for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}this.current(revision);
   if(this.rejectionListeners.size&&controls.rejections?.length){for(const event of controls.rejections){if(event.sequence<=this.lastRejection)continue;this.lastRejection=event.sequence;for(const listener of this.rejectionListeners)try{listener(event);}catch{/* Observer only. */}}this.current(revision);await this.options.broker.request('clientCall',{method:'rejectionsAck',sequence:this.lastRejection});}
   if(this.supportListeners.size&&controls.supportCommands?.length){for(const event of controls.supportCommands){if(event.sequence<=this.lastSupport)continue;this.lastSupport=event.sequence;for(const listener of this.supportListeners)try{listener({id:event.id,kind:event.kind,payload:event.payload});}catch{/* Observer only. */}}this.current(revision);await this.options.broker.request('clientCall',{method:'supportAck',sequence:this.lastSupport});}
   if(controls.authError&&controls.authError.id!==this.lastAuthError&&this.authListeners.size){this.lastAuthError=controls.authError.id;for(const listener of this.authListeners)try{listener(controls.authError.message,{definitive:controls.authError.definitive,code:controls.authError.code});}catch{/* Observer only. */}}
   if(controls.developerCredential&&this.credentialListeners.size){if(controls.developerCredential.id!==this.lastCredential){this.lastCredential=controls.developerCredential.id;for(const listener of this.credentialListeners)try{this.current(revision);listener(controls.developerCredential.token);}catch{/* Observer only. */}}await this.options.broker.request('clientCall',{method:'credentialAck',id:controls.developerCredential.id});}
   for(const request of controls.tokens){const tokenKey=JSON.stringify([revision,request.connection,request.id]);if(this.completedTokenRequests.has(tokenKey))continue;if(request.connection!==handle.connection||request.identityKey!==handle.identityKey||this.tokenRequests.has(request.id))continue;if(this.tokenRequests.size>=8)fail('BUDGET','Native browser token request budget exhausted');this.tokenRequests.add(request.id);
    void (async()=>{try{let timer:ReturnType<typeof setTimeout>|undefined;let token:string|null|undefined;try{token=this.fetchToken?await Promise.race([this.fetchToken({forceRefreshToken:request.forceRefreshToken}),new Promise<never>((_,reject)=>{this.tokenCancels.set(request.id,()=>reject(Error('Native token request canceled')));timer=setTimeout(()=>reject(Error('Native token provider timed out')),30000);})]):this.auth.token??null;}finally{clearTimeout(timer);}this.current(revision);token??=this.auth.token??null;if(token!==null&&(typeof token!=='string'||token.length>16384))throw new TypeError('Invalid native browser token');await this.options.broker.request('clientCall',{method:'token',response:{...request,token}});this.completedTokenRequests.add(tokenKey);if(this.completedTokenRequests.size>128)this.completedTokenRequests.delete(this.completedTokenRequests.values().next().value!);}catch{if(!this.closed&&revision===this.revision)await this.options.broker.request('clientCall',{method:'token',response:{...request,error:'Token provider unavailable'}}).catch(()=>{});}finally{this.tokenRequests.delete(request.id);this.tokenCancels.delete(request.id);}})();
   }
  }catch(error){if(!this.closed&&revision!==this.revision&&(error as {code?:string})?.code==='FENCED'){this.pollAgain=true;continue;}throw error;}}while(this.pollAgain&&!this.closed);})();this.polling=operation;
  // A listener may queue a hint after the loop's last test but before this
  // promise settles. Drain that demand on both success and failure.
  const settled=()=>{this.polling=undefined;if(this.pollAgain&&!this.closed){this.pollAgain=false;queueMicrotask(()=>{void this.pollControls().catch(()=>{});});}};
  void operation.then(settled,settled);return operation;
 }
 get localReplica(){return this.entityView;}
 retainReplicaEntities(entity:string,ids:readonly string[]){return this.entityView.retain(entity,ids);}
 watchControlQuery<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args={} as Args){if(ref.scope!=='control'||ref.kind!=='query'||ref.delivery!=='live')throw new TypeError('Expected a generated live Control query');return this.controlQueries.watch<T>(ref,args);}
 async restoreSession(auth:GonvexClientAuth){
  const authoritative=this.setAuth(auth).then(async()=>{const restored=await this.rpc<boolean>({method:'restoreSession'});await this.pollControls();if(!restored&&this.bootRestored){this.bootRestored=false;this.lastVisible.revoke();this.admit(null);for(const listener of this.controlListeners)try{listener();}catch{}}return restored;});
  const revision=this.revision;
  void authoritative.catch(()=>{if(!this.closed&&revision===this.revision&&this.bootRestored){this.bootRestored=false;this.lastVisible.revoke();this.admit(null);for(const listener of this.controlListeners)try{listener();}catch{}}});
  try{
   const token=this.auth.token??await this.fetchToken?.({forceRefreshToken:false});this.current(revision);
   if(token&&!this.auth.token)this.auth.token=token;
   if(await this.displayFence()){this.current(revision);this.bootRestored=true;for(const listener of this.controlListeners)try{listener();}catch{}return true;}
  }catch{/* Owner restore remains authoritative on a display-cache miss. */}
  return authoritative;
 }
 authenticate(auth:GonvexClientAuth):Promise<void>{
  if(this.closed)return Promise.reject(new GonvexClientError('Native browser client is closed',{code:'closed'}));
  if(this.authWaiters.size>=8)return Promise.reject(Object.assign(Error('Authentication waiter allowance exceeded'),{code:'BUDGET'}));
  const serial=this.authSerial+1;
  return new Promise<void>((resolve,reject)=>{let armed=false,settled=false,timer:ReturnType<typeof setTimeout>|undefined;let stop=()=>{};
   const cancel=()=>finish(new GonvexClientError('Client closed during authentication',{code:'closed'}));
   const finish=(error?:Error)=>{if(settled)return;settled=true;clearTimeout(timer);stop();this.authWaiters.delete(cancel);if(error)reject(error);else resolve();};
   const check=()=>{if(settled)return;if(this.authSerial!==serial){finish(new GonvexClientError('Authentication was superseded',{code:'superseded'}));return;}if(!armed)return;const state=this.controlsValue;if(state?.authentication==='authenticated')finish();else if(state?.authentication==='denied')finish(new GonvexClientError('Authentication rejected',{code:'auth'}));};
   try{stop=this.observe(this.controlListeners,check);}catch(error){reject(error);return;}this.authWaiters.add(cancel);let connectedSince:number|undefined;
   const deadline=()=>{if(settled)return;const connected=this.connectionState().isWebSocketConnected;if(!connected)connectedSince=undefined;else connectedSince??=Date.now();if(connectedSince!==undefined&&Date.now()-connectedSince>=30000){finish(new GonvexClientError('Authentication timed out',{code:'timeout'}));return;}timer=setTimeout(deadline,1000);};timer=setTimeout(deadline,1000);
   void this.setAuth(auth).then(()=>this.pollControls()).then(()=>{armed=true;check();},error=>finish(error instanceof Error?error:Error(String(error))));
  });
 }
 async reportError(type:'register'|'envelope'|'heartbeat',payload:unknown):Promise<void>{sqlitePayloadBytes(payload,65536);return this.rpc({method:'reportError',type,payload});}
 serverInfo(){return this.controlsValue?.serverInfo??{};}
 activeArtifactHash(){return this.options.artifactHash??'';}
 get localSessionReady(){return this.rpc<boolean>({method:'restoreSession'});}
 async resetLocalReplica(options:ResetLocalReplicaOptions={}):Promise<ResetLocalReplicaResult>{
  const result=await this.rpc<ResetLocalReplicaResult>({method:'resetLocalReplica',keepOutbox:options.keepOutbox!==false});
  this.invalidate(true);await this.connect();return result;
 }
 outboxCount(){return this.rpc<number>({method:'outboxCount'});}
 replicaEntities<T extends import('./local-replica.js').ReplicaRow>(entity:string,ids:readonly string[]){if(ids.length>128)throw new RangeError('Entity views are bounded');return ids.map(id=>this.entityView.entity<T>(entity,id));}
 memoryCounters(){const entities=this.entityView.counters();let pageRows=0;for(const watch of this.watches.values())pageRows+=watch.state.page?.rows?.length??0;return {...entities,replicaRows:entities.replicaRows+pageRows,replicaMetadataBytes:entities.replicaMetadataBytes+this.memberships.stats().bytes,replicaBytes:entities.replicaBytes+this.pages.stats().bytes,replicaWindows:this.watches.size,retainedWindows:this.watches.size,pendingCommands:0,queuedStorageOperations:this.requests.stats().entries,querySubscriptions:this.controlQueries.count(),replicaSubscriptions:this.watches.size,pendingCalls:this.requests.stats().entries,outboxHead:0};}
 status(){return this.controlsValue;}


 private observe<T>(set:Set<T>,listener:T){if(this.closed)fail('FENCED','Native browser client is closed');if(set.size>=32)fail('BUDGET','Native observer budget exhausted');set.add(listener);return ()=>{set.delete(listener);};}
 onSupportCommand(listener:(command:SupportCommand)=>void){const release=this.observe(this.supportListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onReducerRejection(listener:(event:ReducerRejectionEvent)=>void){const release=this.observe(this.rejectionListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onLocalExecutionFallback(listener:(event:LocalExecutionFallbackEvent)=>void){return this.observe(this.fallbackListeners,listener);}
 onTelemetry(listener:(event:GonvexTelemetryEvent)=>void){return this.observe(this.telemetryListeners,listener);}
 onAuthError(listener:(message:string,details:{definitive:boolean;code?:string})=>void){const release=this.observe(this.authListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onDeveloperCredential(listener:(token:string)=>void){const release=this.observe(this.credentialListeners,listener);void this.pollControls().catch(()=>{});return release;}
 connectionState():ConnectionState{return this.controlsValue?.status as ConnectionState??{isWebSocketConnected:false,hasEverConnected:false,connectionCount:0,connectionRetries:0,hasInflightRequests:false,inflightReducers:0,inflightActions:0,inflightOneShotQueries:0};}
 subscribeToConnectionState(listener:(state:ConnectionState)=>void){return this.observe(this.controlListeners,()=>listener(this.connectionState()));}
 localSessionState(){return (this.controlsValue?.session??(this.bootRestored?'restored':'unavailable')) as 'unavailable'|'authenticated'|'restored';}
 onLocalSessionChange(listener:()=>void){return this.observe(this.controlListeners,listener);}
 onSessionScopeChange(listener:()=>void){let key=this.admissionKey;return this.observe(this.controlListeners,()=>{if(key!==this.admissionKey){key=this.admissionKey;listener();}});}

 private async open(watch:Watch):Promise<void>{
  if(this.cachedPaint){
   await this.displayFence();
   if(this.lastVisible.hasWatch(watch.key))await this.cachedPaint?.promise;
  }
  if(!watch.refs)fail('FENCED','Native watch is closed');if(watch.retiring)await watch.retiring;await this.connect();if(!watch.refs)fail('FENCED','Native watch is closed');if(watch.bound===this.revision&&watch.source)return;if(watch.opening)return watch.opening;
  // Views retain independent pages, but their identical source owns one lease.
  // Wait for a previous final release before reusing that source.
  for(const other of this.watches.values())if(other!==watch&&other.remoteKey===watch.remoteKey&&other.retiring)await other.retiring;
  if(!watch.refs)fail('FENCED','Native watch closed during source retirement');if(watch.bound===this.revision&&watch.source)return;if(watch.opening)return watch.opening;
  const revision=this.revision;
  const leader=[...this.watches.values()].find(other=>other!==watch&&other.remoteKey===watch.remoteKey&&other.refs&&(other.opening||other.bound===revision&&other.source));
  if(leader)watch.lease=leader.lease;
  const operation=leader?(async()=>{await leader.opening;this.current(revision);if(!leader.source||leader.bound!==revision)fail('FENCED','Shared native source changed');watch.source=leader.source;watch.bound=revision;})():this.rpc<{id?:string;signature:string}>({method:watch.kind==='live'?'watchLive':'watchReplica',path:watch.path,args:watch.args,lease:watch.lease}).then(source=>{this.current(revision);watch.source=source;watch.bound=revision;});
  watch.opening=operation;void operation.then(()=>{if(watch.opening===operation)watch.opening=undefined;},()=>{if(watch.opening===operation)watch.opening=undefined;});return operation;
 }
 private store(watch:Watch,value:Page,compatibleFirstPage=false,stale=false){
  if(!watch.refs)fail('FENCED','Native watch closed during read');
  const previous=this.pages.get(watch.key);
  // A read completion is not a data change. In particular, empty certified
  // pages must not notify their own consumer into another identical read.
  if(previous!==undefined&&watch.compatibleFirstPage===compatibleFirstPage&&watch.stale===stale&&sameJSON(previous,value))return;
  if(value&&'window'in value)watch.previewID=undefined;
  if(value&&'window'in value){const prior=this.pages.get(watch.key);if(prior&&'window'in prior&&prior.window.header.entity===value.window.header.entity&&prior.rows.length===value.rows.length){for(let index=0;index<value.rows.length;index++){const slot=value.rows[index],old=prior.rows[index];if(slot.id===old.id&&sameJSON(slot.value,old.value)){slot.id=old.id;slot.value=old.value;}}}}
  const bytes=sqlitePayloadBytes({key:watch.key,value},262144);watch.unpin?.();watch.unpin=undefined;
  try{this.pages.put(watch.key,value,bytes);}finally{watch.unpin=this.pages.pin(watch.key);}
  if(!this.lastVisible.restored(value))watch.bootPageKey=undefined;watch.compatibleFirstPage=compatibleFirstPage;watch.stale=stale;
  // At most three first-publication marks per admitted watch, capped by the
  // 192-watch registry even across later watch recreation. No row values.
  const boot=this.lastVisible.restored(value),precommit=!!watch.previewID&&!!value&&'source'in value&&value.source==='pending',publication=precommit?4:boot?1:2;
  if(value&&!((watch.startupPublications??0)&publication)&&this.startupPublications<384){watch.startupPublications=(watch.startupPublications??0)|publication;this.startupPublications++;try{performance.measure('gonvex-startup:page-published',{start:performance.now(),end:performance.now(),detail:{path:watch.path,origin:precommit?'precommit':boot?'sealed':'sqlite',rows:value.rows?.length}});}catch{/* Timing never affects publication. */}}
  this.notify(watch);if(compatibleFirstPage&&!this.lastVisible.restored(value))this.scheduleDisplay();
 }
 private deferredBounded(ref:Pick<FunctionReference,'path'>,args:JsonValue,options:SQLiteWatchOptions,kind:'replica'|'live'){
  sqlitePayloadBytes({path:ref.path,args,view:options.view??null},32768);const path=ref.path,owned=structuredClone(args),view=structuredClone(options.view);let selected:ReturnType<SQLiteBrowserClient['watchBounded']>|undefined,closed=false,refs=0,held=false;const empty:WatchSnapshot={page:undefined,stale:true,version:0};
  const ensure=()=>{if(closed||this.closed)fail('FENCED','Deferred native watch is closed');return selected??=this.watchBounded({path},owned,{view},kind);};
  return {read:<T>(run:(watch:ReturnType<SQLiteBrowserClient['watchBounded']>)=>Promise<T>)=>{if(!refs)held=true;return run(ensure());},snapshot:()=>selected?.snapshot()??empty,subscribe:(listener:()=>void)=>{const watch=ensure(),stop=watch.subscribe(listener);refs++;let released=false;return ()=>{if(released)return;released=true;stop();if(!--refs&&!held){selected?.close();selected=undefined;}};},close:()=>{if(closed)return;closed=true;selected?.close();selected=undefined;}};
 }
 watchLiveWindow(ref:FunctionReference,args:JsonValue={},options:SQLiteWatchOptions={}):Pick<SQLiteReplicaWatch,'page'|'snapshot'|'subscribe'|'close'>&{pendingPage(options?:{offset?:number;limit?:number;generation?:string}):Promise<SQLitePendingLivePage|null>}{
  if(ref.kind!=='query'||ref.delivery!=='live'||ref.scope==='control'||!ref.live)throw new TypeError('Expected a generated tenant Live Query');
  if(options.deferStart){const watch=this.deferredBounded(ref,args,options,'live');return {page:options=>watch.read(value=>value.page(options)),pendingPage:options=>watch.read(value=>value.pendingLive(options)),snapshot:watch.snapshot,subscribe:watch.subscribe,close:watch.close};}
  const watch=this.watchBounded(ref,args,options,'live');return {...watch,pendingPage:watch.pendingLive};
 }
 /** Compatibility with structured Live Query hooks. The result is capped at
  * 256 rows and is rebuilt from fenced owner pages after each change. */
 private collectionAuthorityAvailable(){const failure=this.controlsValue?.failure;return !failure||failure.code==='PAGE_CHANGED'&&!this.connectionState().isWebSocketConnected;}
 replicaCollectionState<T extends ReplicaRow=ReplicaRow>(ref:Pick<FunctionReference,'path'>,args:JsonValue={}):ReplicaCollectionSubscriptionState<T>{
  const page=this.pages.get(stable({kind:'replica',path:ref.path,args,view:null}));
  const freshness=this.connectionState().isWebSocketConnected?'current':'offline';
  if(!page||!('window'in page))return {rows:[],ids:[],source:'cache',freshness,completeness:'partial',isUpToDate:false,truncated:false,computedRevision:0};
  const rows=page.rows.filter(slot=>slot.value!==null).map(slot=>slot.value as T),truncated=Boolean(page.window.header.truncated||page.nextOffset!==null),complete=page.window.header.completeness==='complete'&&!truncated&&this.collectionAuthorityAvailable();
  return {rows,ids:page.rows.map(slot=>slot.id),total:page.window.total,source:page.window.header.source,freshness,completeness:complete?'complete':'partial',truncated,isUpToDate:complete&&!this.lastVisible.restored(page),computedRevision:page.generation.sequence};
 }
 replicaSignature(ref:Pick<FunctionReference,'path'>,args:JsonValue={}){return stable({path:ref.path,args});}
 retainReplicaWindow(signature:string){return this.memberships.retain(signature);}
 subscribeReplicaWindow(signature:string,listener:()=>void){return this.memberships.subscribe(signature,listener);}
 replicaWindowMembership(signature:string):SQLiteBrowserMembership|undefined{return this.memberships.read(signature);}
 subscribeLiveQuery(ref:FunctionReference,args:JsonValue,listener:()=>void){const watch=this.watchLiveQuery(ref,args,{deferStart:true}),release=watch.onUpdate(listener);return ()=>{release();watch.close();};}
 watchLiveQuery<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args={} as Args,options:SQLiteWatchOptions={}){
  const signature=this.replicaSignature(ref,args);let releaseMembership:(()=>void)|undefined;
  const watch=this.watchLiveWindow(ref,args,{...options,deferStart:true}),listeners=new Set<()=>void>();let result:T|undefined,closed=false,loading=false,again=false,stop:(()=>void)|undefined;
  const publish=()=>{for(const listener of listeners)try{listener();}catch{/* Observer only. */}};
  const refresh=async()=>{if(this.closed||closed||!listeners.size)return;if(loading){again=true;return;}loading=true;try{do{again=false;
    if(ref.live?.plan&&typeof ref.live.plan.table==='string'&&ref.live.plan.window){
     let after:unknown,generation:string|undefined;const localRows:JsonValue[]=[];let unsupportedReason:string|undefined;let localSupported=false,exhausted=false,partialSearch=false,staleRelations=false,total:number|undefined;
     const requested=Math.min(256,Math.max(1,Number((args as any)?.[(ref.live.plan as any).window?.limitArgument??'limit']??(ref.live.plan as any).window?.defaultLimit??128)));
     for(;;){const local=await this.rpc<SQLiteLocalQueryPage|null>({method:'localLivePage',path:ref.path,args,...(generation?{generation}:{}),...(after?{after}:{})});
      if(!local?.supported){unsupportedReason=local?.reason;break;}localSupported=true;total??=local.total;partialSearch||=!!local.partialSearch;staleRelations||=!!local.stale;generation??=local.generation;
      localRows.push(...local.rows.slice(0,requested-localRows.length) as JsonValue[]);exhausted=local.cursor===null;
      if(exhausted||localRows.length>=requested)break;after=local.cursor;
     }
     if(unsupportedReason&&!this.connectionState().isWebSocketConnected){
      let empty:JsonValue=[];const path=ref.live.resultPath??[];for(let i=path.length-1;i>=0;i--)empty={...(i===0?{total:0,offset:0,limit:requested}:{}),[path[i]!]:empty};result=empty as T;
      this.memberships.publish(signature,{entity:ref.live.entity,ids:[],total:0,source:'cache',completeness:'partial',freshness:'offline',supported:false,unsupportedOperator:unsupportedReason});publish();continue;
     }
     if(localSupported){
      const path=ref.live.resultPath??[],metadata:Record<string,JsonValue>={offset:0,limit:requested,...(total!==undefined?{total}:exhausted?{total:localRows.length}:{})};
      let localValue:JsonValue=localRows;for(let i=path.length-1;i>=0;i--)localValue={...(i===0?metadata:{}),[path[i]!]:localValue};
      sqlitePayloadBytes(localValue,2097152);result=localValue as T;
      this.memberships.publish(signature,{entity:ref.live.entity,ids:localRows.map(row=>String((row as Record<string,JsonValue>)[ref.live!.key])),...(total!==undefined?{total}:exhausted?{total:localRows.length}:{}),offset:0,limit:requested,source:'cache',completeness:partialSearch?'partial':'complete',freshness:this.connectionState().isWebSocketConnected?(staleRelations?'verifying':'current'):'offline'});
      publish();continue;
     }
    }
    let offset=0,generation:string|undefined,header:SQLiteNativeWindowResult['window']['header']|undefined;const rows:JsonValue[]=[];for(;;){const page=await watch.page({offset,limit:128,...(generation?{generation}:{})});if(!page){result=undefined;publish();break;}if(closed||!listeners.size)return;generation??=page.window.generation;header=page.window.header;for(const slot of page.rows)if(slot.value!==null)rows.push(slot.value as JsonValue);if(page.nextOffset===null){
      const skeleton=header.resultSkeleton??[],path=header.resultPath??ref.live?.resultPath??[];const base:JsonValue=structuredClone(skeleton);let value:JsonValue=base;if(path.length){for(const key of path.slice(0,-1)){if(!value||typeof value!=='object'||Array.isArray(value))fail('INVALID_ROW','Live result path is not an object');value=(value as Record<string,JsonValue>)[key]!;}if(!value||typeof value!=='object'||Array.isArray(value)||!Array.isArray((value as Record<string,JsonValue>)[path.at(-1)!]))fail('INVALID_ROW','Live result path is not an array');(value as Record<string,JsonValue>)[path.at(-1)!]=rows;value=base;}else value=rows;
      sqlitePayloadBytes(value,2097152);result=value as T;
      const record=base&&typeof base==='object'&&!Array.isArray(base)?base as Record<string,JsonValue>:undefined;
      this.memberships.publish(signature,{entity:ref.live!.entity,ids:rows.map(row=>String((row as Record<string,JsonValue>)[ref.live!.key])),...(typeof record?.total==='number'?{total:record.total}:{}),...(typeof record?.offset==='number'?{offset:record.offset}:{}),...(typeof record?.limit==='number'?{limit:record.limit}:{}),source:'server',completeness:'complete',freshness:this.lastVisible.restored(page)?(this.connectionState().isWebSocketConnected?'verifying':'offline'):this.connectionState().isWebSocketConnected?'current':'offline'});
      publish();break;
     }if(page.nextOffset<=offset||page.nextOffset>256)fail('BUDGET','Structured live result exceeds its bounded row allowance');offset=page.nextOffset;}
   }while(again&&!closed);}catch(error){if((error as {code?:string})?.code!=='PAGE_CHANGED'){result=undefined;publish();}}finally{loading=false;if(again&&!this.closed&&!closed&&listeners.size)setTimeout(()=>{void refresh();},25);}};
  return {localLiveQueryResult:()=>result,onUpdate:(listener:()=>void)=>{if(closed)fail('FENCED','Native live watch is closed');listeners.add(listener);if(!stop){releaseMembership=this.memberships.retain(signature);stop=watch.subscribe(()=>{if(watch.snapshot().stale)void refresh();});void refresh();}return ()=>{listeners.delete(listener);if(!listeners.size){stop?.();stop=undefined;releaseMembership?.();releaseMembership=undefined;result=undefined;}};},close:()=>{if(closed)return;closed=true;stop?.();releaseMembership?.();releaseMembership=undefined;listeners.clear();result=undefined;watch.close();}};
 }
 watchReplica<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args={} as Args,options:SQLiteWatchOptions={}):SQLiteReplicaWatch<T>{
  if(options.deferStart){const watch=this.deferredBounded(ref,args,options,'replica');return this.compatibleReplica<T>({page:options=>watch.read(value=>value.page(options)),queryPage:(plan,generation)=>watch.read(value=>value.queryPage(plan,generation)),localQueryPage:(ref,args)=>watch.read(value=>value.localQueryPage(ref,args)),pendingPage:options=>watch.read(value=>value.pendingReplica(options)),snapshot:watch.snapshot,subscribe:watch.subscribe,close:watch.close},this.cachedReplicaPage(ref,args,options));}
  const watch=this.watchBounded(ref,args,options,'replica');return this.compatibleReplica<T>({...watch,pendingPage:watch.pendingReplica},this.cachedReplicaPage(ref,args,options));
 }
 // Mounted compatibility consumers share only the exact default first-page read.
 // Explicit page calls still consult the owner, and invalidation fences the cache.
 private cachedReplicaPage(ref:Pick<FunctionReference,'path'>,args:JsonValue,options:SQLiteWatchOptions){
  const key=stable({kind:'replica',path:ref.path,args,view:options.view??null});
  return ()=>{const watch=this.watches.get(key);return watch?.compatibleFirstPage&&(!watch.stale||this.bootRestored&&this.lastVisible.restored(this.pages.get(key)))?this.pages.get(key) as SQLiteNativeWindowResult|null|undefined:undefined;};
 }
 private compatibleReplica<T extends JsonValue>(base:Pick<SQLiteReplicaWatch,'page'|'queryPage'|'localQueryPage'|'pendingPage'|'snapshot'|'subscribe'|'close'>,cached:()=>SQLiteNativeWindowResult|null|undefined):SQLiteReplicaWatch<T>{
  let failure:unknown;let state:ReplicaCollectionSubscriptionState<T & ReplicaRow>|undefined,publishedPage:SQLiteNativeWindowResult|null|undefined,loading=false,again=false,closed=false,stop:(()=>void)|undefined;
  const listeners=new Set<()=>void>();
  const publish=()=>{for(const listener of listeners)try{listener();}catch{/* Observer only. */}};
  const clear=()=>{if(state!==undefined){state=undefined;publishedPage=undefined;publish();}};
  const publishPreview=()=>{
   const page=base.snapshot().page;
   if(!page||!('source'in page)||page.source!=='pending'||!('complete'in page)||page.complete!==false||!('receivedRows'in page))return;
   const pending=page as SQLitePendingSnapshotPage;
   state={rows:pending.rows.filter(slot=>slot.value!==null).map(slot=>slot.value as T & ReplicaRow),ids:pending.rows.map(slot=>slot.id),total:pending.receivedRows,source:'server',completeness:'partial',freshness:'verifying',truncated:true,isUpToDate:false,computedRevision:pending.generation.sequence};
   publishedPage=undefined;publish();
  };
  const refresh=async()=>{if(this.closed||closed||!listeners.size)return;if(loading){again=true;return;}loading=true;try{do{again=false;const retained=cached(),page=retained===undefined?await base.page({limit:128}):retained;if(closed||!listeners.size)return;
    if(page){failure=undefined;publishedPage=page;const previous=state;let rowIndex=0,identical=!!previous;for(const slot of page.rows)if(slot.value!==null){if(!previous||previous.rows[rowIndex]!==slot.value)identical=false;rowIndex++;}if(previous&&rowIndex!==previous.rows.length)identical=false;const rows=identical?previous!.rows:page.rows.filter(slot=>slot.value!==null).map(slot=>slot.value as T & ReplicaRow),ids=previous&&previous.ids.length===page.rows.length&&page.rows.every((slot,index)=>slot.id===previous.ids[index])?previous.ids:page.rows.map(slot=>slot.id),header=page.window.header,truncated=Boolean(header.truncated||page.nextOffset!==null),complete=header.completeness==='complete'&&!truncated&&this.collectionAuthorityAvailable();
     state={rows,ids,total:page.window.total,source:header.source,completeness:complete?'complete':'partial',freshness:this.lastVisible.restored(page)?(this.connectionState().isWebSocketConnected?'verifying':'offline'):this.connectionState().isWebSocketConnected?'current':'offline',truncated,isUpToDate:complete&&!this.lastVisible.restored(page),computedRevision:page.generation.sequence};publish();}
   }while(again&&!closed);}catch(error){const code=(error as {code?:string})?.code,page=base.snapshot().page;const certified=page&&'window'in page&&page.window.header.completeness==='complete'&&!page.window.header.truncated&&page.nextOffset===null;if(code!=='PAGE_CHANGED'&&(code!=='FENCED'||!certified)){clear();if(code==='BUDGET'){failure=error;publish();}}}finally{loading=false;if(again&&!this.closed&&!closed&&listeners.size)setTimeout(()=>{void refresh();},25);}};
  return {...base,onUpdate:(listener:()=>void)=>{if(closed)fail('FENCED','Native watch is closed');listeners.add(listener);if(!stop){stop=base.subscribe(()=>{const snapshot=base.snapshot();if(!snapshot.page)clear();else publishPreview();if(snapshot.stale||snapshot.page!==publishedPage)void refresh();});void refresh();}return ()=>{listeners.delete(listener);if(!listeners.size){stop?.();stop=undefined;state=undefined;publishedPage=undefined;}};},localReplicaResult:()=>{if(failure)throw failure;return state?.rows;},localReplicaState:()=>{if(failure)throw failure;return state;},close:()=>{if(closed)return;closed=true;stop?.();stop=undefined;listeners.clear();state=undefined;publishedPage=undefined;base.close();}};
 }
 private watchBounded(ref:Pick<FunctionReference,'path'>,args:JsonValue,options:{view?:JsonValue},kind:'replica'|'live'){
  sqlitePayloadBytes({path:ref.path,args,view:options.view??null},32768);const key=stable({kind,path:ref.path,args,view:options.view??null}),remoteKey=stable({kind,path:ref.path,args});let watch=this.watches.get(key);
  if(!watch){const bytes=sqlitePayloadBytes({key,remoteKey,args,path:ref.path},65536)+2048;if(this.watches.size>=192||this.watchBytes+bytes>524288)fail('BUDGET','Native browser watch budget exhausted');watch={kind,state:{page:undefined,stale:true,version:0},key,remoteKey,lease:crypto.randomUUID(),path:ref.path,args:JSON.parse(stable(args)),bytes,refs:0,bound:-1,listeners:new Set(),stale:true,version:0,invalidation:0};this.watches.set(key,watch);this.watchBytes+=bytes;}
  const selected=watch;if(selected.refs>=64)fail('BUDGET','Native watch lease budget exhausted');selected.refs++;let closed=false;const listeners=new Set<()=>void>();void this.open(selected).catch(()=>{});
  const read=async<T extends Page>(method:string,options:unknown,display=true):Promise<T>=>{
   options=structuredClone(options);
   const revision=this.revision,dataRevision=this.dataRevision;if(closed)fail('FENCED','Native watch is closed');
   // An identity/reference subscription may mount before the restore effect.
   // Verify the exact credential envelope before choosing display vs owner;
   // otherwise open() waits for a grid commit which this subscription enables.
   if(display&&this.cachedPaint&&!this.bootRestored&&await this.displayFence()){
    this.current(revision);if(dataRevision!==this.dataRevision)fail('FENCED','Display admission changed during verification');
    this.bootRestored=true;
   }

   const displayKey=method==='replicaPage'?selected.key:stable({watch:selected.key,method,options:options as JsonValue});
   if(display&&['replicaPage','localLivePage'].includes(method)&&this.bootRestored){
    const opts=options as SQLiteReplicaPageOptions;
    if((method==='localLivePage'||(opts.limit??128)===128&&(opts.offset??0)===0)&&opts.expected===undefined&&opts.generation===undefined){
     const shared=this.pages.get(selected.key);
     if(shared&&selected.bootPageKey===displayKey&&this.lastVisible.restored(shared))return shared as T;
     const saved=this.lastVisible.read<T>(displayKey);
     if(saved!==undefined){selected.bootPageKey=displayKey;this.store(selected,saved,method==='replicaPage',true);
      // A connected owner is not yet a published SQLite page. Keep stale
      // display available while the ordinary fenced read reconciles it.
      void read<T>(method,options,false).catch(()=>{/* Owner/control failures remain authoritative; source changes retry reads. */});
      return saved;}
    }
   }
   if(method==='replicaPendingPage'||method==='livePendingPage')selected.pendingDisplay=true;
   await this.open(selected);this.current(revision);if(closed||!selected.refs)fail('FENCED','Native watch closed before read');
   const observed=selected.invalidation,queryOptions=options;
   const descriptor={watch:selected.remoteKey,version:observed,revision,dataRevision,method,options:options as JsonValue};const bytes=sqlitePayloadBytes(descriptor,131072)+256,key=stable(descriptor);let pending=this.reads.get(key);
   if(!pending){
    if(this.reads.size>=384||this.readBytes+bytes>524288)fail('BUDGET','Shared native read metadata exhausted');
    pending=(async()=>{const value=await this.rpc<Page>({method,signature:selected.source!.signature,...queryOptions as object});this.current(revision);if(dataRevision!==this.dataRevision)fail('FENCED','Native watch admission changed');return value;})();
    this.reads.set(key,pending);this.readBytes+=bytes;const operation=pending,release=()=>{if(this.reads.get(key)===operation){this.reads.delete(key);this.readBytes-=bytes;}};void operation.then(release,release);
   }
   const value=await pending;if(closed||!selected.refs)fail('FENCED','Native watch closed during read');this.current(revision);
   if(['replicaPage','localLivePage'].includes(method)&&this.admissionKey){const admission=this.admissionKey;void this.displayFence().then(()=>{if(!this.closed&&revision===this.revision&&admission===this.admissionKey){this.lastVisible.remember(displayKey,value,admission);this.scheduleDisplay();}}).catch(()=>{});}
   if(value!==null||this.pages.get(selected.key)===undefined){const pageOptions=queryOptions as SQLiteReplicaPageOptions,compatible=method==='replicaPage'&&pageOptions.limit===128&&(pageOptions.offset??0)===0&&pageOptions.generation===undefined&&pageOptions.expected===undefined;if(this.pages.get(selected.key)!==value||selected.compatibleFirstPage!==compatible||selected.stale!==(selected.invalidation!==observed))this.store(selected,value,compatible,selected.invalidation!==observed);}
   return value as T;
  };
  return {localQueryPage:(ref:Pick<FunctionReference,'path'>,args:JsonValue={})=>read<SQLiteLocalQueryPage|null>('localLivePage',{path:ref.path,args}),queryPage:(plan:SQLiteCollectionPageOptions,generation?:string)=>read<SQLiteCollectionPageResult|null>('collectionPage',{plan,...(generation?{generation}:{})}),page:(options:SQLiteReplicaPageOptions={})=>read<SQLiteNativeWindowResult|null>(kind==='live'?'livePage':'replicaPage',options),pendingReplica:(options:{after?:number;limit?:number;expected?:SQLitePendingSnapshotPage['generation']}={})=>read<SQLitePendingSnapshotPage|null>('replicaPendingPage',options),pendingLive:(options:{offset?:number;limit?:number;generation?:string}={})=>read<SQLitePendingLivePage|null>('livePendingPage',options),snapshot:()=>selected.state,subscribe:(listener:()=>void)=>{if(closed)fail('FENCED','Native watch is closed');if(selected.listeners.size>=32)fail('BUDGET','Native watch listener budget exhausted');const wrapped=()=>listener();listeners.add(wrapped);selected.listeners.add(wrapped);return ()=>{listeners.delete(wrapped);selected.listeners.delete(wrapped);};},close:()=>{if(closed)return;closed=true;for(const listener of listeners)selected.listeners.delete(listener);listeners.clear();if(--selected.refs)return;this.lastVisible.releaseWatch(selected.key);selected.state={page:undefined,stale:true,version:selected.version+1};selected.unpin?.();selected.unpin=undefined;this.pages.delete(selected.key);
   const retirement=(async()=>{try{await selected.opening;if(selected.refs)return;const shared=[...this.watches.values()].some(other=>other!==selected&&other.refs&&other.remoteKey===selected.remoteKey&&other.lease===selected.lease);
    if(!shared){const first=[...this.watches.values()].find(other=>!other.refs&&other.remoteKey===selected.remoteKey&&other.lease===selected.lease&&other.retiring);if(first&&first!==selected){await first.retiring;return;}if(selected.source)await this.rpc({method:selected.kind==='live'?'unwatchLive':'unwatchReplica',signature:selected.source.signature,lease:selected.lease});selected.source=undefined;selected.bound=-1;}}finally{if(this.watches.get(selected.key)===selected&&!selected.refs){this.watches.delete(selected.key);this.watchBytes-=selected.bytes;}}})();selected.retiring=retirement;void retirement.then(()=>{if(selected.retiring===retirement)selected.retiring=undefined;},()=>{if(selected.retiring===retirement)selected.retiring=undefined;});}};
 }
 entities(entity:string,ids:readonly string[],expected?:SQLiteNativeEntityResult['generation'],offset=0):Promise<SQLiteNativeEntityResult>{sqlitePayloadBytes({entity,ids},32768);if(ids.length>128)throw new RangeError('Native entity reads are bounded');if((!this.handle||this.cachedPaint)&&this.bootRestored){const cached=this.lastVisible.entities(entity,ids,expected);if(cached)return Promise.resolve(cached as SQLiteNativeEntityResult);}const key=JSON.stringify([this.revision,this.dataRevision,entity,ids,expected,offset]);const active=this.entityReads.get(key);if(active)return active;const result=this.rpc<SQLiteNativeEntityResult>({method:'entities',readDiagnostics:true,entity,ids,...(offset?{offset}:{}),...(expected?{expected}:{})});if(this.entityReads.size<128){this.entityReads.set(key,result);const done=()=>{if(this.entityReads.get(key)===result)this.entityReads.delete(key);};void result.then(done,done);}return result;}
 invokeReducer(ref:Pick<FunctionReference,'path'>,args:JsonValue={},options:{now?:number;intentEntropy?:string;commandId?:string}={}):Promise<SQLiteReducerReceipt>{if(options.commandId&&options.now===undefined)throw new TypeError('Retryable reducers require their original invocation time');const id=options.commandId??crypto.randomUUID(),sent=Date.now();return this.rpc<SQLiteReducerReceipt>({method:'reducer',path:ref.path,args,now:options.now??sent,intentEntropy:options.intentEntropy},id).then(receipt=>{if(receipt.queuedOnly)for(const listener of this.fallbackListeners)try{listener({path:ref.path,intentId:id,phase:'call',reason:'incompleteReplica',error:receipt.predictionUnavailable?JSON.stringify(receipt.predictionUnavailable):'Local prediction awaits complete admitted data'});}catch{/* Observer only. */}const received=Date.now();for(const listener of this.telemetryListeners)try{listener({type:'reducer',id,path:ref.path,outcome:'ok',clientSentAtMs:sent,clientReceivedAtMs:received,clientDurationMs:received-sent});}catch{/* Observer only. */}return receipt;});}
 reducer<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args,options:CallOptions&{offline:'queue'}):Promise<T|QueuedReducerOutcome>;
 reducer<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args?:Args,options?:CallOptions):Promise<T>;
 async reducer<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue={},_options:CallOptions={}):Promise<T|QueuedReducerOutcome>{if(this.publicTenant&&(_options.offline==='queue'||_options.optimistic?.length))throw new GonvexClientError('Public reducers are online-only',{code:'disconnected',path:ref.path,operation:'reducer'});const receipt=await this.invokeReducer(ref,args);return receipt.queuedOnly?{status:'queued',reducerId:receipt.commandId}:receipt.result as T;}
 async declareReducerReadSet(ref:Pick<FunctionReference,'path'>):Promise<void>{await this.rpc({method:'declareReducerReadSet',path:ref.path});}
 async prepareReducer(ref:Pick<FunctionReference,'path'>):Promise<void>{
  const revision=this.revision,input={method:'prepareReducer',path:ref.path};this.current(revision);
  if(typeof ref.path!=='string'||!ref.path||ref.path.length>1024)throw new TypeError('Invalid reducer path');
  const key=stable({revision,path:ref.path}),pending=this.preparations.get(key);if(pending)return pending;
  if(this.preparations.size>=this.requests.maxWaitingEntries)fail('BUDGET','Native reducer preparation queue exhausted');
  const bytes=sqlitePayloadBytes({key,input},65536)+512,releaseQueued=this.requests.hold(bytes);
  // Only the dispatched warmup uses an RPC slot. Pending work keeps its
  // metadata charged to the same budget and shares one promise per path.
  const operation=this.preparationLane.then(async()=>{this.current(revision);releaseQueued();const release=await this.requests.acquire(bytes);try{this.current(revision);return await this.rpc<void>(input,undefined,true);}finally{release();}}).finally(()=>{releaseQueued();if(this.preparations.get(key)===operation)this.preparations.delete(key);});
  this.preparations.set(key,operation);
  this.preparationLane=operation.then(()=>{},()=>{});return operation;
 }
 query<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args={} as Args,_options:CallOptions={}):Promise<T>{return this.rpc({method:'query',path:ref.path,args});}
 action<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args={} as Args,_options:CallOptions={}):Promise<T>{return this.rpc({method:'action',path:ref.path,args});}
 budgets(){return this.closedBudgets?this.closedBudgets.map(metric=>metric.name==='browser-request-admission'?{...metric,...this.requests.stats(),name:metric.name}:metric):this.ledger.snapshot();}
 close(){if(this.closed)return;this.commitCachedDisplay();this.closedBudgets=this.ledger.snapshot().map(metric=>({...metric,bytes:0,entries:0}));clearTimeout(this.displayTimer);globalThis.removeEventListener?.('pagehide',this.displayHide);if(typeof document!=='undefined')document.removeEventListener('visibilitychange',this.displayVisibility);this.lastVisible.close();this.closed=true;this.revision++;this.requests.close();this.reads.clear();this.readBytes=0;this.memberships.close();this.controlQueries.close();this.entityView.close();this.reporter?.close();for(const cancel of this.authWaiters)cancel();this.authWaiters.clear();this.releaseChanges();for(const watch of this.watches.values()){watch.refs=0;watch.state={page:undefined,stale:true,version:watch.version+1};watch.unpin?.();watch.listeners.clear();}this.watches.clear();this.watchBytes=0;for(const cancel of this.tokenCancels.values())cancel();this.tokenCancels.clear();this.tokenRequests.clear();this.completedTokenRequests.clear();this.auth={};this.fetchToken=undefined;this.controlListeners.clear();this.authListeners.clear();this.credentialListeners.clear();this.rejectionListeners.clear();this.supportListeners.clear();this.fallbackListeners.clear();this.telemetryListeners.clear();this.controlsValue=undefined;this.pages.close();this.ledger.close();this.options.broker.close();}
}
