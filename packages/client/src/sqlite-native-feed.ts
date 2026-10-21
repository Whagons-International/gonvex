import {yieldSQLiteTask} from './sqlite-task-yield.js';
import type {ClientMessage,ReplicaCursor,ServerMessage} from '@gonvex/protocol';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import type {SQLiteTransportHost,SQLiteTransportSession} from './sqlite-transport-host.js';
import type {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteNativeIngress} from './sqlite-native-ingress.js';
import {SQLiteWindowStore,type SQLiteWindowHeader} from './sqlite-window-store.js';
import {SQLiteFeedGroups} from './sqlite-feed-groups.js';
import {SQLiteDeferredReplicaFrames} from './sqlite-deferred-replica-frames.js';
import {decodeSQLiteNativeDeltaBatches,decodeSQLiteNativeTransactionBatches} from './sqlite-native-control.js';
import {waitForSQLiteOwnerAdmission,markSQLiteStagingCommit} from './sqlite-owner-transactions.js';
import {SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Start=Pick<Extract<ServerMessage,{type:'replica.snapshotStart'}>,'snapshotId'|'cursor'|'mode'|'truncated'>;
type Delta=Extract<ServerMessage,{type:'replica.delta'}>;
export type SQLiteFeedSource={id:string;signature:string;entity:string;key:string;path:string;completeEntity?:true;columns?:readonly string[]};
type Source=SQLiteFeedSource&{deferredKey:string;bytes:number;start?:Start;preparedStart?:Start;deferActivation?:boolean;opening:boolean;closing?:{promise:Promise<void>;resolve():void;reject(error:unknown):void;timer:ReturnType<typeof setTimeout>}};
type ControlBatch={state:State;frames:ServerMessage[];admissions:Promise<unknown>[];bytes:number;previous:Promise<void>;preceding:Promise<void>;promise:Promise<void>;resolve():void;reject(error:unknown):void;timer?:ReturnType<typeof setTimeout>};
type State={catchup:boolean;stream:string;generation:number;scope:string;epoch:string;cursorEpoch?:string;logicalScope:string;gate:Promise<void>;ingress:SQLiteNativeIngress;admission?:number;closed:boolean;watermark?:number};
type Options={owner:SQLiteOwnerProgramContext;sessions:SQLiteWorkerSessions;connection:string;artifactHash:string;
 reserve?(bytes:number):()=>void;
 /** Binary pages already own bounded protocol credit and must not consume
  * active control slots needed to release their preceding gates. */
 reserveSnapshot?(bytes:number):()=>void;
 acquire?(bytes:number):Promise<()=>void>;
 sourceReady?(id:string):void;
 /** Durable Ready releases network opening credit, not authority. */
 sourcePrepared?(id:string):void;
 previewStaging?(source:SQLiteFeedSource,snapshot:string):Promise<(()=>void)|undefined>;
 freshSnapshot?(id:string):Promise<void>;
 windowsPending?(scope:string,epoch:string,revision:number):Promise<boolean>;
 publishWindows?(scope:string,epoch:string,revision:number):Promise<void>;
 send(message:ClientMessage):void;advanceWatermark(revision:number):void;failed(error:unknown):void;
 /** Rebase derived predictions in the same transaction as canonical authority. */
 authority?(scope:string,mutate:()=>Promise<void>):Promise<void>;
 /** Portable chunks are fetched outside the SQL FIFO before atomic retry. */
 prepare?(path:string):Promise<void>;
};
const progressSchema=`CREATE TABLE IF NOT EXISTS gv_native_feed_progress(scope TEXT NOT NULL,epoch TEXT NOT NULL,observed INTEGER NOT NULL,applied INTEGER NOT NULL,unclosed INTEGER NOT NULL,PRIMARY KEY(scope,epoch)) WITHOUT ROWID; CREATE TABLE IF NOT EXISTS gv_native_feed_resets(scope TEXT PRIMARY KEY) WITHOUT ROWID;`;
const empty={entities:{},liveQueries:{}};
const watermarkEffect=Symbol('watermark');
/** One authenticated socket's ordered native feed. Its bounded control lane
 * never waits for future snapshot bytes. Membership changes preceding the last
 * base page live on disk; only a verified window can retire those records. */
export class SQLiteNativeFeed implements SQLiteTransportHost {
 private admissions=0;private catchupMaxControlBatchFrames=0;
 private readonly sources=new Map<string,Source>();
 private sourceBytes=0;private startBytes=0;private closingBytes=0;
 private state?:State;
 private pendingControls?:ControlBatch;
 private activeControls?:{state:State;after:Map<string|symbol,()=>void>};
 private readonly controlKinds=new Map<string,number>();private readonly sourceControlCounts=new Map<string,number>();private binaryFrames=0;
 private countControl(message:ServerMessage){const kind=this.controlKinds.has(message.type)||this.controlKinds.size<32?message.type:'other';this.controlKinds.set(kind,(this.controlKinds.get(kind)??0)+1);const id=(message as any).id;if(typeof id==='string'&&this.sources.has(id))this.sourceControlCounts.set(id,(this.sourceControlCounts.get(id)??0)+1);}
 private registryPeakBytes=0;private registryPeakEntries=0;private closePeakBytes=0;
 private observeRegistry(){this.registryPeakBytes=Math.max(this.registryPeakBytes,this.sourceBytes+this.startBytes+this.closingBytes);this.registryPeakEntries=Math.max(this.registryPeakEntries,this.sources.size);this.closePeakBytes=Math.max(this.closePeakBytes,this.closingBytes);}
 private overflowResyncs=0;private coalescedWatermarks=0;
 private queued=0;private queuedBytes=0;private authorityDepth=0;
 private readonly windows:SQLiteWindowStore;private readonly deferred:SQLiteDeferredReplicaFrames;
 private readonly initialized:Promise<void>;private readonly groups:SQLiteFeedGroups;
 constructor(private readonly options:Options){
  const {owner}=options,fence=()=>owner.engine.verifyOwner();this.windows=new SQLiteWindowStore(owner.database,fence);this.deferred=new SQLiteDeferredReplicaFrames(owner.database,options.artifactHash,fence);this.groups=new SQLiteFeedGroups(owner.database,fence);
  this.initialized=owner.runOwner(0,async()=>{await this.windows.ready();await this.deferred.ready();await this.groups.ready();await owner.snapshots.ready();await owner.activation.ready();await owner.database.execAsync(progressSchema);});void this.initialized.catch(()=>{});
 }
 /** Called by the trusted generated program before sending replica.open.
  * Application RPCs cannot supply an entity, visibility scope or SQL plan. */
 register(input:SQLiteFeedSource){
  sqlitePayloadBytes(input,16384);if(input.completeEntity!==undefined&&input.completeEntity!==true||!input.id||input.id.length>256||!input.signature||input.signature.length>4096||!input.path||input.path.length>1024||!Object.hasOwn(this.options.owner.engine.schema,input.entity)||this.options.owner.engine.schema[input.entity]!.key!==input.key)throw new TypeError('Invalid generated native source');
  if(input.columns!==undefined&&(!Array.isArray(input.columns)||input.columns.length>128||new Set(input.columns).size!==input.columns.length||!input.columns.includes(input.key)||input.columns.some(column=>typeof column!=='string'||!Object.hasOwn(this.options.owner.engine.schema[input.entity]!.fields,column))))throw new TypeError('Invalid generated source projection');
  // Column names belong to the immutable generated schema. Retain canonical
  // references, rather than another charged copy of every column string.
  const names=new Map(Object.keys(this.options.owner.engine.schema[input.entity]!.fields).map(name=>[name,name]));
  const columns=input.columns?Object.freeze(input.columns.map(name=>names.get(name)!)):undefined;
  const descriptor={id:input.id,signature:input.signature,entity:input.entity,key:input.key,path:input.path,...(input.completeEntity?{completeEntity:true as const}: {})};
  const owned:SQLiteFeedSource={...descriptor,...(columns?{columns}:{})},bytes=sqlitePayloadBytes(descriptor,16384)+256+(columns?.length??0)*8;
  if(this.sources.has(input.id))throw new TypeError('Native source already registered');if(this.sources.size>=256||this.sourceBytes+this.startBytes+this.closingBytes+bytes>262144)fail('BUDGET','Native source registry exceeded budget');
  this.sources.set(input.id,{...owned,deferredKey:crypto.randomUUID(),bytes,opening:true});this.sourceBytes+=bytes;this.observeRegistry();
 }
 private async retireDeferred(scope:string,key:string){
  // A closed source has no remaining ordered writer. Empty queues need no
  // durable cancellation record or later cancellation-GC transaction.
  if(await this.options.owner.database.getFirstAsync('SELECT 1 FROM gv_deferred_replica_frames WHERE scope=? AND signature=? LIMIT 1',scope,key))await this.deferred.cancel(scope,key);
 }
 private removeSource(id:string){const source=this.sources.get(id);if(!source)return;this.sources.delete(id);this.sourceControlCounts.delete(id);if(source.start)this.startBytes-=sqlitePayloadBytes(source.start,16384);if(source.closing){clearTimeout(source.closing.timer);this.closingBytes-=512;}this.sourceBytes-=source.bytes;const state=this.state;if(state)void this.initialized.then(()=>this.options.owner.runOwner(16384,()=>this.retireDeferred(state.scope,source.deferredKey))).catch(()=>{});}
 unregister(id:string){if(this.state&&!this.state.closed)fail('FENCED','Close an online source through its protocol acknowledgement');this.removeSource(id);}
 closeSource(id:string):Promise<void>{
  const source=this.source(id);if(source.closing)return source.closing.promise;const state=this.state;if(!state||state.closed){this.removeSource(id);return Promise.resolve();}this.check(state);
  if(this.sourceBytes+this.startBytes+this.closingBytes+512>262144){
   // Do not allocate another tombstone. Retiring the connection supersedes
   // every old-session close; canonical rows and durable intents stay intact.
   this.removeSource(id);this.reject(state,Object.assign(Error('Native close cohort requires recertification'),{code:'PAGE_CHANGED',resyncRequired:true}));return Promise.resolve();
  }
  let resolve!:()=>void,reject!:(error:unknown)=>void;const promise=new Promise<void>((yes,no)=>{resolve=yes;reject=no;});source.closing={promise,resolve,reject,timer:setTimeout(()=>this.reject(state,Object.assign(Error('Native source close acknowledgement timed out'),{code:'PAGE_CHANGED'})),30000)};this.closingBytes+=512;this.observeRegistry();
  try{this.options.send({type:'replica.close',id});}catch(error){this.removeSource(id);reject(error);}return promise;
 }
 private source(id:string){const source=this.sources.get(id);if(!source)fail('FENCED','Native frame has no generated subscription');return source;}
 private check(state:State){if(this.state!==state||state.closed||state.admission===undefined)fail('FENCED','Native feed admission changed');this.options.sessions.assertFrameAdmission(this.options.connection,state.generation,state.admission);}
 private stop(){const state=this.state;if(state){state.closed=true;const pending=this.pendingControls;if(pending?.state===state){this.pendingControls=undefined;clearTimeout(pending.timer);pending.reject(Object.assign(Error('Native feed admission changed'),{code:'FENCED'}));}state.ingress.close();const keys=[...this.sources.values()].map(source=>source.deferredKey);void this.initialized.then(()=>this.options.owner.runOwner(16384,async()=>{await this.groups.retire(state.stream);for(const key of keys)await this.retireDeferred(state.scope,key);})).catch(()=>{});}this.state=undefined;for(const source of [...this.sources.values()])if(source.closing){const closing=source.closing;this.removeSource(source.id);closing.reject(Object.assign(Error('Native source connection changed'),{code:'FENCED'}));}}
 authenticating(intentScope:string,generation:number){this.stop();this.startBytes=0;this.options.sessions.authenticating(this.options.connection,intentScope,generation);for(const source of this.sources.values()){source.start=undefined;source.preparedStart=undefined;source.deferActivation=false;source.opening=true;source.deferredKey=crypto.randomUUID();}}
 async session(input:SQLiteTransportSession,isCurrent:()=>boolean){
  this.stop();if(!input.directive){await this.options.sessions.activate(this.options.connection,input,isCurrent);return;}
  if(input.capabilities.replicaNativeRows!==1)throw Object.assign(Error('Server requires the legacy replica adapter'),{code:'UNSUPPORTED'});
  const {owner}=this.options;let state!:State;
  const ingress=new SQLiteNativeIngress({database:owner.database,snapshots:owner.snapshots,scope:input.directive.visibilityScope,reserve:this.options.reserveSnapshot??this.options.reserve,
   runOwner:async run=>{
    let prepared:Array<{source:Source;start:Start}>=[];
    const result=await this.run(state,524288,async()=>{
     await this.initialized;prepared=[];
     // Bind private typed partitions in the credited page's existing FULL TX.
     for(const source of this.sources.values())if(source.start&&source.preparedStart!==source.start){
      const start=source.start;await owner.snapshots.prepareTyped({scope:state.scope,subscription:source.id,snapshot:start.snapshotId,entity:source.entity});prepared.push({source,start});
     }
     return run();
    },['@pending']);
    // A failed append/rebase rolls back its binding. Retry with the same page.
    for(const {source,start} of prepared)if(this.sources.get(source.id)===source&&source.start===start)source.preparedStart=start;
    return result;
   },
   previewStaging:async receipt=>{
    this.check(state);const source=this.source(receipt.ack.id);
    if(source.start?.snapshotId!==receipt.ack.snapshotId)fail('FENCED','Preview snapshot admission changed');
    return this.options.previewStaging?.(source,receipt.ack.snapshotId);
   },
   commitProgress:async()=>{await markSQLiteStagingCommit(owner.database,()=>owner.engine.verifyOwner());},
   sendAck:ack=>{this.check(state);this.options.send(ack);},
   activateVerified:async receipt=>{
    this.check(state);const source=this.source(receipt.ack.id),start=source.start;if(!start||start.snapshotId!==receipt.ack.snapshotId)fail('FENCED','Snapshot has no matching start header');
    // A warm snapshot is durably staged/ACKed here. Publish its authority
    // with Ready and the terminal watermark, preserving the last cohort on
    // disconnect or crash before that watermark.
    if(source.deferActivation)return;
    if(!receipt.progress.digest)fail('INVALID_ROW','Verified snapshot has no digest');
    // Cold activation is still part of an unfinished canonical cohort.
    // Only pending display readers can use it before the terminal watermark.
    const header=this.snapshotHeader(source,start,receipt.progress.digest!);
    await owner.database.runAsync('INSERT INTO gv_native_feed_progress VALUES(?,?,0,0,?) ON CONFLICT(scope,epoch) DO UPDATE SET unclosed=CASE WHEN excluded.unclosed>applied THEN max(unclosed,excluded.unclosed) ELSE unclosed END',state.scope,state.epoch,start.cursor.revision);
    await this.mutate(state.scope,async()=>{await owner.activation.activate({scope:state.scope,subscription:source.id,snapshot:start.snapshotId,header});});
    await this.replay(state,source);
   }});
  state={catchup:this.admissions++>0,stream:crypto.randomUUID(),generation:input.connectionGeneration,scope:input.directive.visibilityScope,epoch:input.directive.epoch,logicalScope:input.directive.scope,gate:Promise.resolve(),ingress,closed:false};this.state=state;
  state.gate=(async()=>{await this.initialized;const admitted=await this.options.sessions.activate(this.options.connection,input,()=>this.state===state&&!state.closed&&isCurrent());if(!admitted)fail('FENCED','Native feed has no tenant admission');const current=this.options.sessions.frameAdmission(this.options.connection,state.generation);state.admission=current.token;this.check(state);})();void state.gate.catch(()=>{});await state.gate;
 }
 binary(frame:ArrayBuffer,generation:number){const state=this.state;if(!state||state.generation!==generation||state.closed)return;try{this.acceptBinary(frame,generation);}catch(error){this.reject(state,error);}}
 private acceptBinary(frame:ArrayBuffer,generation:number){this.binaryFrames++;this.flushControls();const state=this.state;if(!state||state.generation!==generation||state.closed)fail('FENCED','Binary frame has no native session');void state.ingress.accept(frame,state.gate).catch(error=>this.reject(state,error));}
 frame(message:ServerMessage,generation:number){const state=this.state;if(!state||state.generation!==generation||state.closed)return false;try{return this.acceptFrame(message,generation);}catch(error){this.reject(state,error);return false;}}
 private acceptFrame(message:ServerMessage,generation:number){
  if(!message.type.startsWith('replica.'))return false;this.countControl(message);
  const state=this.state;if(!state||state.generation!==generation||state.closed)fail('FENCED','Control frame has no native session');
  if(message.type==='replica.watermark'&&this.pendingControls?.state===state){const prior=this.pendingControls.frames.at(-1);if(prior?.type==='replica.watermark'&&Number.isSafeInteger(message.revision)&&message.revision>=0){prior.revision=Math.max(prior.revision,message.revision);this.coalescedWatermarks++;return true;}}
  const bytes=sqlitePayloadBytes(message,262144);// One certificate per admitted source plus bounded protocol controls.
  if(this.queued>=320||this.queuedBytes+bytes>2097152)fail('BUDGET','Native control queue exceeded budget');
  const batchable=['replica.snapshotStart','replica.delta','replica.transactionEncoded','replica.ready','replica.readyMany','replica.watermark'].includes(message.type);
  if(this.pendingControls&&(this.pendingControls.state!==state||this.pendingControls.bytes+bytes>262144||this.pendingControls.frames.length>=(state.catchup?8:64)))this.flushControls();
  // Charge rollback metadata to the first frame credit before queueing. Never
  // acquire a second active slot while processing a batch that holds slots.
  const credit=bytes+512+(batchable&&!this.pendingControls?this.sources.size*128+64*64:0);
  let release:(()=>void)|undefined;const admitted=this.options.acquire?this.options.acquire(credit).then(value=>{release=value;}):Promise.resolve().then(()=>{release=this.options.reserve?.(credit);});void admitted.catch(()=>{});let owned:ServerMessage;try{owned=structuredClone(message);}catch(error){void admitted.then(()=>release?.(),()=>{});throw error;}// Preparing a different snapshot only installs its private stage. It does
  // not consume preceding binary rows or publish canonical authority. Ordered
  // controls still flush every preceding frame before they enter the gate.
  const preceding=message.type==='replica.snapshotStart'?Promise.resolve():state.ingress.flush();void preceding.catch(()=>{});this.queued++;this.queuedBytes+=bytes;
  let next:Promise<void>;
  if(batchable){
   let batch=this.pendingControls;
   if(batch&&(batch.state!==state||batch.bytes+bytes>262144||batch.frames.length>=(state.catchup?8:64))){this.flushControls();batch=undefined;}
   if(!batch){
    let resolve!:()=>void,reject!:(error:unknown)=>void;const promise=new Promise<void>((yes,no)=>{resolve=yes;reject=no;});
    batch={state,frames:[],admissions:[],bytes:0,previous:state.gate,preceding,promise,resolve,reject};this.pendingControls=batch;state.gate=promise;
    // Global queue count/bytes and this group's existing byte credit bound
    // tiny controls too. A separate sixteen-frame cap fragmented a byte-small
    // startup burst into multiple FULL commits before the coalescing turn.
    // Coalesce controls within the current turn without timer throttling.
    // Binary ingress retains its independent task scheduler.
    queueMicrotask(()=>this.flushControls());
   }
   batch.admissions.push(admitted);batch.frames.push(owned);batch.bytes+=bytes;next=batch.promise;
   if(batch.bytes>=262144)this.flushControls();
  }else{
   this.flushControls();next=state.gate.then(()=>preceding).then(()=>admitted).then(async()=>{this.check(state);await this.process(state,owned);});state.gate=next;
  }
  void next.then(()=>{this.queued--;this.queuedBytes-=bytes;void admitted.then(()=>release?.(),()=>{});},error=>{this.queued--;this.queuedBytes-=bytes;void admitted.then(()=>release?.(),()=>{});this.reject(state,error);});return true;
 }
 /** Release the short group-commit delay when a row reader or binary frame waits.
  * Does not wait for network freshness or drain an unfinished feed cohort. */
 flushControls(){
  const batch=this.pendingControls;if(!batch)return;this.pendingControls=undefined;clearTimeout(batch.timer);
  void batch.previous.then(()=>batch.preceding).then(()=>this.processControls(batch)).then(batch.resolve,batch.reject);
 }
 private committed(key:string|symbol,effect:()=>void){if(this.activeControls)this.activeControls.after.set(key,effect);else effect();}
 private async processControls(batch:ControlBatch){
  await Promise.all(batch.admissions);const state=batch.state;if(state.catchup)this.catchupMaxControlBatchFrames=Math.max(this.catchupMaxControlBatchFrames,batch.frames.length);if(state.catchup)await yieldSQLiteTask();let after=new Map<string|symbol,()=>void>();
  await this.run(state,Math.max(262144,batch.bytes),async()=>{
   const saved=[...this.sources.values()].map(source=>({source,start:source.start,preparedStart:source.preparedStart,opening:source.opening,deferActivation:source.deferActivation})),startBytes=this.startBytes,watermark=state.watermark,cursorEpoch=state.cursorEpoch;
   const active={state,after:new Map<string|symbol,()=>void>()};this.activeControls=active;
   try{for(const frame of batch.frames){this.check(state);await this.process(state,frame);const ready=frame.type==='replica.ready'?[frame]:frame.type==='replica.readyMany'?frame.ready:[];for(const item of ready)this.committed('prepared:'+item.id,()=>this.options.sourcePrepared?.(item.id));}after=active.after;}
   catch(error){for(const entry of saved){entry.source.start=entry.start;entry.source.preparedStart=entry.preparedStart;entry.source.opening=entry.opening;entry.source.deferActivation=entry.deferActivation;}this.startBytes=startBytes;state.watermark=watermark;state.cursorEpoch=cursorEpoch;throw error;}
   finally{this.activeControls=undefined;}
  });this.check(state);for(const effect of after.values())effect();
 }
 private reject(state:State,error:unknown){if(this.state!==state||state.closed)return;const sourceID=String((error as Error)?.message??error).match(/source:[0-9a-f]{64}/)?.[0],source=sourceID?this.sources.get(sourceID):undefined;
  if((error as {code?:string})?.code==='BUDGET'){this.overflowResyncs++;error=Object.assign(new Error('Replica ingress requires resync after bounded admission overflow'),{code:'PAGE_CHANGED',resyncRequired:true});}
  if(source)error=Object.assign(new Error(`${String((error as Error).message)} (${source.path})`),{code:(error as {code?:string}).code,...((error as {resyncRequired?:boolean}).resyncRequired?{resyncRequired:true}:{})});this.stop();this.options.failed(error);}
 /** Live windows share the authenticated database clock with replica frames. */
 observeCursor(cursor:ReplicaCursor){const state=this.state;if(!state)fail('FENCED','Native feed unavailable');this.check(state!);this.cursor(state!,cursor);}
 databaseEpoch(){return this.state?.cursorEpoch;}
 private cursor(state:State,cursor:ReplicaCursor){if(!cursor||typeof cursor.epoch!=='string'||!cursor.epoch||cursor.epoch.length>256||!Number.isSafeInteger(cursor.revision)||cursor.revision<0)fail('INVALID_ROW','Invalid native feed cursor');if(state.cursorEpoch===undefined)state.cursorEpoch=cursor.epoch;else if(cursor.epoch!==state.cursorEpoch)fail('FENCED','Native feed cursor changed epoch');}
 private async run<T>(state:State,bytes:number,run:()=>Promise<T>,scopes:readonly string[]=[state.scope]):Promise<T>{
  if(this.activeControls?.state===state){this.check(state);const value=await run();this.check(state);return value;}
  const loaded=new Set<string>();let preparationBytes=0;
  for(;;){try{return await waitForSQLiteOwnerAdmission(()=>this.options.owner.runOwner(bytes,async()=>{let value!:T;await this.options.owner.database.withTransactionAsync(async()=>{this.check(state);value=await run();this.check(state);});return value;},scopes),()=>this.check(state));}
   catch(error){if(!(error instanceof SQLiteReducerPreparationRequiredError)||!this.options.prepare)throw error;
    if(loaded.has(error.path))fail('FENCED','Prepared native feed reducer remains unavailable');preparationBytes+=error.path.length*2+128;
    if(loaded.size>=128||preparationBytes>131072)fail('BUDGET','Native feed reducer preparation exceeded budget');loaded.add(error.path);await this.options.prepare(error.path);
   }
  }
 }
 private snapshotHeader(source:Source,start:Start,digest:string):SQLiteWindowHeader{return {signature:source.signature,entity:source.entity,key:source.key,kind:'replica',source:'server',cursor:start.cursor,aggregateDigest:digest,completeness:start.truncated?'partial':'complete',...(start.mode?{mode:start.mode}:{}),...(source.completeEntity&&!start.truncated?{entityCoverage:'complete' as const}:{}),columns:source.columns??Object.keys(this.options.owner.engine.schema[source.entity]!.fields),artifactHash:this.options.artifactHash};}
 private async mutate(scope:string,run:()=>Promise<void>){if(this.authorityDepth){await run();return;}this.authorityDepth++;try{if(this.options.authority)await this.options.authority(scope,run);else await run();}finally{this.authorityDepth--;}}
 private async applyDelta(state:State,source:Source,frame:Delta){
  this.cursor(state,frame.cursor);const upserts:Array<{id:string;hash:string}>=[];for await(const rows of decodeSQLiteNativeDeltaBatches(frame,source.key))for(const {id,hash} of rows)upserts.push({id,hash});const current=await this.windows.header(state.scope,source.signature);
  if(!current||current.header.cursor?.epoch!==state.cursorEpoch)fail('PAGE_CHANGED','Native delta needs a verified base window');
  await this.windows.applyDelta({scope:state.scope,signature:source.signature,generation:current.generation,cursor:frame.cursor,upserts,deleted:frame.deleted??[],digest:frame.digest??''},()=>this.mutate(state.scope,async()=>{for await(const rows of decodeSQLiteNativeDeltaBatches(frame,source.key))await this.options.owner.canonical.applyTransaction({cursor:frame.cursor,changes:rows.map(({id,row})=>({entity:source.entity,id,operation:'update',newValue:row}))},empty,state.scope);}));
 }
 private async replay(state:State,source:Source){
  const header=await this.windows.header(state.scope,source.signature);if(!header?.header.cursor)fail('PAGE_CHANGED','Native base publication missing');let after=header.header.cursor.revision;
  for(;;){const page=await this.deferred.read(state.scope,source.deferredKey,state.cursorEpoch??state.epoch,after);if(!page.length)break;for(const entry of page){await this.applyDelta(state,source,entry.frame);await this.options.owner.database.runAsync('INSERT INTO gv_native_feed_progress VALUES(?,?,0,0,?) ON CONFLICT(scope,epoch) DO UPDATE SET unclosed=max(unclosed,excluded.unclosed)',state.scope,state.epoch,entry.revision);after=entry.revision;}}
  // Each iteration owns bounded records; callers already hold the owner FIFO.
  while((await this.deferred.discardThrough(state.scope,source.deferredKey,state.cursorEpoch??state.epoch,after)).remaining){}
 }
 private async publishWatermark(state:State){
  if(state.watermark===undefined||[...this.sources.values()].some(source=>source.opening))return;
  const revision=state.watermark;if([...this.sources.values()].some(source=>source.deferActivation&&source.start&&source.start.cursor.revision>revision))return;
  const readySources=new Set<string>();let applied=false;await this.run(state,0,()=>this.options.owner.database.withTransactionAsync(async()=>{
   // Registration does not wait for this SQL turn. Recheck after admission and
   // keep one source set through the transaction's asynchronous reads. A later
   // source must earn its own Ready before this watermark can certify it.
   const sources=[...this.sources.values()];if(sources.some(source=>source.opening))return;
   const pending=await this.options.owner.database.getFirstAsync<{unclosed:number;applied:number}>('SELECT unclosed,applied FROM gv_native_feed_progress WHERE scope=? AND epoch=?',state.scope,state.epoch);if(pending&&pending.unclosed>revision||await this.groups.after(state.stream,revision))return;
   const stages=sources.some(source=>source.deferActivation&&source.start),groups=await this.groups.pending(state.stream),windows=await this.options.windowsPending?.(state.scope,state.cursorEpoch??state.epoch,revision);
   // Repeated quiet watermarks do not change authority. Keep them read-only;
   // new windows or Ready records at this same revision must still publish.
   if(pending&&pending.applied>=revision&&!pending.unclosed&&!stages&&!groups&&!windows){applied=true;return;}
   const publish=async()=>{
   for(const source of sources)if(source.deferActivation&&source.start){
    const start=source.start,stage=await this.options.owner.database.getFirstAsync<{digest:string;complete:number}>('SELECT digest,complete FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=? AND discarded=0',state.scope,source.id,start.snapshotId);
    if(!stage?.complete)fail('PAGE_CHANGED','Warm snapshot has no verified disk stage');
    await this.options.owner.activation.activate({scope:state.scope,subscription:source.id,snapshot:start.snapshotId,header:this.snapshotHeader(source,start,stage.digest)});
    // Native credits can delay snapshotStart after the subscription is live.
    // Deltas received before that header are buffered, even for a warm source.
    // Replay them against this base before validating its terminal certificate.
    await this.replay(state,source);
   }
   await this.groups.drain(state.stream,revision,async frame=>{
    if(frame.type==='replica.transactionEncoded'){const keys=Object.fromEntries(Object.entries(this.options.owner.engine.schema).map(([entity,table])=>[entity,table.key]));await this.mutate(state.scope,async()=>{for(const changes of decodeSQLiteNativeTransactionBatches(frame,keys))await this.options.owner.canonical.applyTransaction({cursor:frame.cursor,changes},empty,state.scope);});}
    else if(frame.type==='replica.delta')await this.applyDelta(state,this.source(frame.id),frame);
    else {await this.windows.confirmReady(state.scope,this.source(frame.id).signature,frame.cursor,frame.digest,frame.truncated??false);readySources.add(frame.id);}
   });
   await this.options.publishWindows?.(state.scope,state.cursorEpoch??state.epoch,revision);
   // A terminal watermark certifies quiet active sources as well as changed
   // ones. Closed windows keep their last observed cursor and cannot grant
   // reducer completeness after authority has advanced beyond them.
   await this.options.owner.database.runAsync(`UPDATE gv_window_generations AS g SET header=json_set(g.header,'$.cursor.revision',max(json_extract(g.header,'$.cursor.revision'),?))
    WHERE g.scope=? AND json_extract(g.header,'$.cursor.epoch')=? AND (g.signature,g.generation) IN
     (SELECT w.signature,w.generation FROM gv_windows w WHERE w.scope=? AND w.signature IN (SELECT value FROM json_each(?)))`,revision,state.scope,state.cursorEpoch??'',state.scope,JSON.stringify(sources.filter(source=>this.sources.get(source.id)===source&&!source.closing&&!source.opening).map(source=>source.signature)));
   await this.options.owner.database.runAsync('DELETE FROM gv_native_feed_resets WHERE scope=?',state.scope);
   await this.options.owner.database.runAsync('INSERT INTO gv_native_feed_progress VALUES(?,?,?, ?,0) ON CONFLICT(scope,epoch) DO UPDATE SET observed=max(observed,excluded.observed),applied=max(applied,excluded.applied),unclosed=0',state.scope,state.epoch,revision,revision);if(!pending||pending.applied<revision||pending.unclosed)await markSQLiteStagingCommit(this.options.owner.database,()=>this.options.owner.engine.verifyOwner());applied=true;
   };if(stages||pending?.unclosed||groups||windows)await this.mutate(state.scope,publish);else await publish();
  }));this.check(state);if(applied){for(const id of readySources){const source=this.sources.get(id);if(source?.deferActivation&&source.start){this.startBytes-=sqlitePayloadBytes(source.start,16384);source.start=undefined;source.preparedStart=undefined;source.deferActivation=false;}this.committed(id,()=>this.options.sourceReady?.(id));}this.committed(watermarkEffect,()=>this.options.advanceWatermark(revision));}
 }
 private async process(state:State,message:ServerMessage):Promise<void>{
  const {owner}=this.options;
  switch(message.type){
   case 'replica.snapshotStart':{
    const source=this.source(message.id);this.cursor(state,message.cursor);
    if(message.visibilityScope!==state.scope||message.scope!==state.logicalScope||message.key!==source.key||message.path!==undefined&&message.path!==source.path||!message.snapshotId||message.snapshotId.length>128)fail('INVALID_ROW','Native start differs from generated source');
    if(source.start)fail('PAGE_CHANGED','Native source received overlapping snapshots');const retained:Start={snapshotId:message.snapshotId,cursor:{...message.cursor},...(message.mode?{mode:message.mode}:{}),...(message.truncated===undefined?{}:{truncated:message.truncated})};const startBytes=sqlitePayloadBytes(retained,16384);if(this.sourceBytes+this.startBytes+this.closingBytes+startBytes>262144)fail('BUDGET','Native source header registry exceeded budget');
    const priorWindow=()=>owner.database.getFirstAsync<{generation:string}>("SELECT generation FROM gv_window_generations WHERE scope=? AND signature=? AND generation<>coalesce((SELECT generation FROM gv_windows WHERE scope=? AND signature=?),'') LIMIT 1",state.scope,source.signature,state.scope,source.signature);
    const priorStage=()=>owner.database.getFirstAsync<{snapshot:string}>('SELECT snapshot FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot<>? LIMIT 1',state.scope,source.id,message.snapshotId);
    const prepare=async()=>{return {defer:!!await owner.database.getFirstAsync('SELECT 1 FROM gv_native_feed_progress WHERE scope=? AND epoch=? AND applied>0',state.scope,state.epoch)};};
    // Most starts have nothing obsolete to collect. Keep their checks and
    // preparation in one fenced transaction and one owner-queue turn.
    const prepared=await this.run(state,16384,async()=>{if(await priorWindow()||await priorStage())return;return prepare();});
    if(!prepared){
     // Published aliases own their values and membership. Retire stale disk
     // generations in the existing bounded turns before preparing another.
     for(;;){const more=await this.run(state,16384,async()=>{const prior=await priorWindow();if(!prior)return false;await this.windows.collect(state.scope,source.signature,prior.generation);return true;});if(!more)break;}
     for(;;){const more=await this.run(state,16384,async()=>{const prior=await priorStage();if(!prior)return false;await owner.snapshots.discard(state.scope,source.id,prior.snapshot);return true;});if(!more)break;}
    }
    source.deferActivation=(prepared??await this.run(state,16384,prepare)).defer;source.start=retained;this.startBytes+=startBytes;this.observeRegistry();source.opening=true;return;
   }
   case 'replica.closed':{
    const source=this.source(message.id),closing=source.closing;if(!closing)fail('INVALID_ROW','Unexpected native source close acknowledgement');
    while(await this.run(state,16384,()=>this.groups.discardSource(state.stream,source.id))){}
    await this.run(state,16384,async()=>{
     // The last verified alias belongs to the retained replica, not to the
     // network lease. Only discard an unfinished source incarnation here.
     if(source.start)await owner.snapshots.discard(state.scope,source.id,source.start.snapshotId);
    });
    this.removeSource(source.id);closing.resolve();await this.publishWatermark(state);return;
   }
   case 'replica.delta':{
    const source=this.source(message.id);this.cursor(state,message.cursor);
    await this.run(state,262144,async()=>{const header=await this.windows.header(state.scope,source.signature);if(!source.deferActivation&&(source.start||!header||header.header.cursor?.epoch!==state.cursorEpoch))await this.deferred.append(state.scope,source.deferredKey,message);else await this.groups.append(state.stream,message);});return;
   }
   case 'replica.transactionEncoded':{
    this.cursor(state,message.cursor);
    // Validate before taking disk space; canonical authority stays unchanged
    // until the terminal watermark publishes this complete group.
    for(const _changes of decodeSQLiteNativeTransactionBatches(message,Object.fromEntries(Object.entries(owner.engine.schema).map(([entity,table])=>[entity,table.key])))){/* Validate without retaining the decoded frame. */}
    await this.run(state,262144,()=>this.groups.append(state.stream,message));return;
   }
   case 'replica.readyMany':for(const ready of message.ready)await this.process(state,{type:'replica.ready',...ready});return;
   case 'replica.ready':{
    const source=this.source(message.id);this.cursor(state,message.cursor);
    if(!source.start&&!source.deferActivation&&(message.truncated===undefined||typeof message.truncated==='boolean')){
     // A matching digest at a newer cursor changes only its certificate.
     // Pending data/reset cohorts still take the ordered reconciliation path.
     const unchanged=await this.run(state,0,async()=>{
      if(!await owner.database.getFirstAsync(`SELECT 1 FROM gv_native_feed_progress p WHERE p.scope=? AND p.epoch=? AND p.unclosed=0 AND p.applied<=?
       AND NOT EXISTS(SELECT 1 FROM gv_native_feed_resets r WHERE r.scope=p.scope)
       AND EXISTS(SELECT 1 FROM _gonvex_replica_meta m WHERE m.scope=p.scope AND m.key='currentEpoch' AND m.value=?)`,state.scope,state.epoch,message.cursor.revision,message.cursor.epoch)||await this.groups.pending(state.stream))return false;
      const header=(await this.windows.header(state.scope,source.signature))?.header,columns=source.columns??Object.keys(owner.engine.schema[source.entity]!.fields);
      const matches=header?.kind==='replica'&&header.source==='server'&&header.entity===source.entity&&header.key===source.key&&header.artifactHash===this.options.artifactHash
       &&header.cursor?.epoch===message.cursor.epoch&&header.cursor.revision<=message.cursor.revision&&header.aggregateDigest===message.digest
       &&header.completeness===(message.truncated?'partial':'complete')&&header.columns?.length===columns.length&&header.columns.every((column,index)=>column===columns[index]);
      if(matches)await this.windows.confirmReady(state.scope,source.signature,message.cursor,message.digest,message.truncated??false);
      return matches;
     });
     if(unchanged){source.opening=false;this.committed(source.id,()=>this.options.sourceReady?.(source.id));await this.publishWatermark(state);return;}
    }
    if(source.deferActivation&&source.start){
     await this.run(state,16384,()=>this.groups.append(state.stream,message));source.opening=false;await this.publishWatermark(state);return;
    }
    if(!source.opening||!source.start){
     await this.run(state,16384,()=>this.groups.append(state.stream,message));
     // A resumed source has no snapshot stage. Its delta and ready certificate
     // belong to the same terminal group; verify after applying that delta.
     if(source.opening){source.opening=false;if(state.watermark!==undefined&&state.watermark>=message.cursor.revision)await this.publishWatermark(state);}return;
    }
    await this.run(state,16384,async()=>{await this.replay(state,source);await this.windows.confirmReady(state.scope,source.signature,message.cursor,message.digest,message.truncated??false);});if(source.start)this.startBytes-=sqlitePayloadBytes(source.start,16384);source.start=undefined;source.preparedStart=undefined;source.opening=false;this.committed(source.id,()=>this.options.sourceReady?.(source.id));await this.publishWatermark(state);return;
   }
   case 'replica.watermark':if(!Number.isSafeInteger(message.revision)||message.revision<0)fail('INVALID_ROW','Invalid native watermark');state.watermark=Math.max(state.watermark??0,message.revision);await this.publishWatermark(state);return;
   case 'replica.needHashes':{const source=this.source(message.id);if(this.options.freshSnapshot&&!source.start){source.opening=true;await this.options.freshSnapshot(source.id);return;}throw Object.assign(Error(`Native source requires fresh snapshot reconciliation (${source.path}: hashes requested)`),{code:'PAGE_CHANGED',resetSource:source.id});}
   case 'replica.reset':{const source=this.source(message.id);throw Object.assign(Error(`Native source requires fresh snapshot reconciliation (${source.path}: ${message.reason})`),{code:'PAGE_CHANGED',resetSource:source.id});}
   case 'replica.error':fail('PAGE_CHANGED','Native source requires reconciliation');
   default:fail('INVALID_ROW','Unexpected native replica control');
  }
 }
 /** UI reads may use the last committed cohort while a newer feed group is
  * staged. The caller must hold the same owner transaction for this guard and
  * the row read. No network/control/ingress drain belongs on a cached read.
  * A bootstrap that has already changed one canonical source remains blocked
  * until its terminal cohort closes. This does not grant reducer coverage. */
 async assertCommittedReadable(cached:{scope:string;epoch:string}){
  if(!this.options.owner.database.ownerTransactionDepth)throw Error('Committed read guard requires an owner transaction');
  sqlitePayloadBytes(cached,4096);await this.options.owner.engine.verifyOwner();await this.readableEpoch(cached.scope,cached.epoch,this.state?.scope===cached.scope&&this.state.epoch===cached.epoch?this.state.cursorEpoch:undefined);
  const progress=await this.options.owner.database.getFirstAsync<{unclosed:number;applied:number}>('SELECT unclosed,applied FROM gv_native_feed_progress WHERE scope=? AND epoch=?',cached.scope,cached.epoch);
  if(progress?.unclosed&&progress.unclosed>progress.applied)fail('PAGE_CHANGED','Committed feed requires reconciliation of its unfinished cohort');
 }
 /** A verified read cannot expose half a transaction/membership feed group.
  * Pending first-sync pages use their separate incomplete display contract. */
 async assertReadable(cached?:{scope:string;epoch:string}){
  this.flushControls();
  const state=this.state;
  if(!state){
   if(!cached)fail('FENCED','Native feed unavailable');sqlitePayloadBytes(cached,4096);
   await this.options.owner.runOwner(0,async()=>{await this.options.owner.engine.verifyOwner();await this.readableEpoch(cached.scope,cached.epoch);const progress=await this.options.owner.database.getFirstAsync<{unclosed:number}>('SELECT unclosed FROM gv_native_feed_progress WHERE scope=? AND epoch=?',cached.scope,cached.epoch);if(progress?.unclosed)fail('PAGE_CHANGED','Cached feed requires reconciliation of its unfinished group');});return;
  }
  await state.gate;await state.ingress.flush();await this.run(state,0,async()=>{await this.readableEpoch(state.scope,state.epoch,state.cursorEpoch);if(await this.groups.pending(state.stream))fail('PAGE_CHANGED','Native feed awaits its terminal watermark');const progress=await this.options.owner.database.getFirstAsync<{unclosed:number}>('SELECT unclosed FROM gv_native_feed_progress WHERE scope=? AND epoch=?',state.scope,state.epoch);if(progress?.unclosed)fail('PAGE_CHANGED','Native feed awaits its terminal watermark');});
 }
 private async readableEpoch(scope:string,epoch:string,cursorEpoch?:string){if(await this.options.owner.database.getFirstAsync('SELECT 1 FROM gv_native_feed_resets WHERE scope=?',scope))fail('PAGE_CHANGED','Cache reset awaits its terminal watermark');const current=await this.options.owner.database.getFirstAsync<{value:string}>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='currentEpoch'",scope);if(!current)return;if(cursorEpoch&&current.value!==cursorEpoch)fail('PAGE_CHANGED','Canonical rows need the newly admitted database epoch');const progress=await this.options.owner.database.getFirstAsync('SELECT 1 FROM gv_native_feed_progress WHERE scope=? AND epoch=?',scope,epoch);if(!progress&&current.value!==epoch)fail('PAGE_CHANGED','Canonical rows have no committed admission epoch');}
 /** Only an authenticated registered source can select a pending disk stage.
  * This is display metadata, never a complete replica coverage certificate. */
 pendingSource(id:string){
  const state=this.state;if(!state)return;const source=this.source(id);if(!source.start)return;this.check(state);
  // This header is installed only after its fenced SQL preparation commits.
  // Display already committed pages without waiting for later wire controls,
  // a lazy reducer download or the rest of an in-progress import.
  return {snapshot:source.start.snapshotId,subscription:source.id,entity:source.entity};
 }
 quarantined(generation:number){this.stop();void this.options.sessions.quarantine(this.options.connection,generation).catch(error=>this.options.failed(error));}
 disconnected(){this.stop();this.options.sessions.disconnected(this.options.connection);}
 failed(error:unknown){if(this.state){this.reject(this.state,error);return;}this.options.failed((error as {code?:string})?.code==='BUDGET'?Object.assign(Error('Replica ingress requires resync'),{code:'PAGE_CHANGED',resyncRequired:true}):error);}
 close(){this.stop();this.sources.clear();this.sourceBytes=0;this.startBytes=0;this.closingBytes=0;}
 stats(){this.observeRegistry();return {overflowResyncs:this.overflowResyncs,coalescedWatermarks:this.coalescedWatermarks,catchupMaxControlBatchFrames:this.catchupMaxControlBatchFrames,binaryFrames:this.binaryFrames,controlKinds:Object.fromEntries(this.controlKinds),sourceControlCounts:[...this.sourceControlCounts].map(([id,frames])=>({path:this.sources.get(id)?.path,frames})),registryPeakBytes:this.registryPeakBytes,registryPeakEntries:this.registryPeakEntries,closePeakBytes:this.closePeakBytes,sources:this.sources.size,opening:[...this.sources.values()].filter(source=>source.opening).length,watermark:this.state?.watermark,sourceBytes:this.sourceBytes+this.startBytes+this.closingBytes,queued:this.queued,queuedBytes:this.queuedBytes,ingress:this.state?.ingress.stats()};}
}
