import {sqliteCurrentDiagnostics} from './sqlite-current-diagnostics.js';
import type {SQLitePrecommitDisplay} from './sqlite-precommit-display.js';
import {OriginStorageWorkerLease} from './origin-storage-worker-lease.js';
import type {ByteBudgetSnapshot} from './byte-budget.js';
import {SQLitePendingSnapshotReader} from './sqlite-pending-snapshot-reader.js';
import {SQLiteOwnerTabs} from './sqlite-owner-tabs.js';
import {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {sqliteRequestBytes} from './sqlite-request-frame.js';
import {SQLiteCanonicalDispatcher} from './sqlite-canonical-dispatch.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {waitForSQLiteOwnerAdmission,SQLiteOwnerQueueBusyError,sqliteOwnerTransactionStats, sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import { sqliteWorkerOperations as publicOperations, sqliteOperationPublishes,sqliteWorkerProtocol as protocol } from './sqlite-worker-protocol.js';
import { openOfficialSQLiteHost, type OfficialSQLiteHost } from './sqlite-official-host.js';
import { SQLiteTypedReplicaEngine, sqlitePayloadBytes, type SQLiteTypedWrite } from './sqlite-typed-engine.js';
import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';
import type { SQLitePagePlan } from './sqlite-typed-query.js';

import type {SQLiteReplicaDatabase,SQLiteLocalReplicaStorage} from './sqlite-replica-storage.js';
export type SQLiteOwnerCaller={tabID:string;requestID:string;signal:AbortSignal};
export type SQLiteOwnerProgram={
 /** Network waits must stay outside the SQL FIFO. Use context.runOwner for
  * every disk phase; server replies must never wait behind their own call. */
 call(args:unknown,caller:SQLiteOwnerCaller):Promise<unknown>;
 budgets?():ByteBudgetSnapshot[];
 diagnostics?():Promise<unknown>;
 detachTab?(tabID:string):void;
 close?():Promise<void>|void;
};
export type SQLiteOwnerProgramContext={
 database:SQLiteReplicaDatabase;engine:SQLiteTypedReplicaEngine;reader:SQLiteNativeReplicaReader;pendingReader:SQLitePendingSnapshotReader;canonical:SQLiteLocalReplicaStorage;snapshots:SQLiteSnapshotPages;activation:SQLiteSnapshotActivator;
 runOwner<T>(bytes:number,run:()=>Promise<T>,publishScopes?:readonly string[],label?:string):Promise<T>;
 /** Metadata-only hint; receiver polls bounded controls through clientCall. */
 notifyTab(tabID:string):Promise<void>;
 /** No SQL queue or new committed generation. Only SDK-owned stale display. */
 notifyPreviewTab?(tabID:string):void;
 publishPreviewTab?(tabID:string,value:SQLitePrecommitDisplay):boolean;
};
export type SQLiteOwnerBinding={schema?:SQLiteTypedSchema;schemaVersion?:string;createProgram?(context:SQLiteOwnerProgramContext):SQLiteOwnerProgram};
let installed=false;
/** Generated worker entries supply their schema and portable client program.
 * The factory is synchronous. It may start initialization through runOwner,
 * but must return before awaiting that queue. The framework never imports
 * application code statically. */
export function installSQLiteOwner(binding:SQLiteOwnerBinding={}):()=>Promise<void>{
 if(typeof globalThis.postMessage!=='function'||'document'in globalThis||'onconnect'in globalThis)throw Error('SQLite owner actor requires a dedicated worker');
 if(installed)throw Error('Only one SQLite owner actor may run in a worker');installed=true;
// Dedicated worker entry. The broker is the only producer. OPFS initialization
// is lazy and occurs only after the tab has acquired the origin ownership lock.
let host: OfficialSQLiteHost | undefined, engine: SQLiteTypedReplicaEngine | undefined;
const startupMemory:Array<{stage:string;wasmBytes:number;sqlite:ReturnType<NonNullable<OfficialSQLiteHost['memoryStats']>>|null}>=[];
function sampleStartupMemory(stage:string){if(host&&startupMemory.length<3)startupMemory.push({stage,wasmBytes:host.memoryBytes(),sqlite:host.memoryStats?.()??null});}
let canonical:SQLiteCanonicalDispatcher|undefined;
let nativeReader:SQLiteNativeReplicaReader|undefined,pendingReader:SQLitePendingSnapshotReader|undefined;
let snapshots:SQLiteSnapshotPages|undefined,activation:SQLiteSnapshotActivator|undefined;
const queueTimings:Array<{label:string;finishedAt:number;waitMs:number;runMs:number;bytes:number}>=[];let maxQueueWaitMs=0,maxOwnerRunMs=0;
const capacityWaiters:Array<()=>void>=[];
const waitForCapacity=(size:number)=>queuedOperations<64&&queuedBytes+size<=2097152?Promise.resolve():new Promise<void>(resolve=>{capacityWaiters.push(resolve);});
const releaseCapacity=()=>{capacityWaiters.shift()?.();};
let lane = Promise.resolve(), operations = 0, bytes = 0,queuedOperations=0,queuedBytes=0,closed=false;
let program:SQLiteOwnerProgram|undefined,ownerToken='';const workerLease=new OriginStorageWorkerLease();let originEpoch:number|undefined;
const wireGeneration=(generation:{epoch:number;sequence:number})=>({...generation,epoch:originEpoch??generation.epoch});
const ports = new Map<string, { port: MessagePort; seen: number; token: string }>();
const tabs=new SQLiteOwnerTabs((to,change)=>{
 const message={type:'change',protocol,to,token:ownerToken,sentAt:Date.now(),...change};const peer=ports.get(to);
 if(peer)peer.port.postMessage(message);else globalThis.postMessage(message);
},id=>{ports.get(id)?.port.close();ports.delete(id);try{program?.detachTab?.(id);}catch{globalThis.postMessage({type:'storage-fault'});}});


const timer=setInterval(() => {
  tabs.expire();
}, 5000);
function scope(value: unknown): string {
  if (typeof value !== 'string' || !value || value.length > 512) throw new TypeError('Invalid storage scope');
  return value;
}
function publish(scope:string,generation:{epoch:number;sequence:number}){
 tabs.publish(scope,wireGeneration(generation));
}
function schedule<T>(size:number,run:()=>Promise<T>,label='operation'):Promise<T>{
 if(closed)return Promise.reject(Object.assign(Error('Storage owner is closed'),{code:'FENCED'}));
 if(!Number.isSafeInteger(size)||size<0||size>2097152)return Promise.reject(Object.assign(Error('Storage owner operation exceeds queue budget'),{code:'BUSY'}));
 if(queuedOperations>=64||queuedBytes+size>2097152)return Promise.reject(new SQLiteOwnerQueueBusyError());
 // The worker lifetime lock protects the connection through every disk phase.
 // SQLite's durable owner epoch fences reads and writes without a second DB.
 const queuedAt=performance.now();queuedOperations++;queuedBytes+=size;const operation=lane.then(async()=>{const started=performance.now();maxQueueWaitMs=Math.max(maxQueueWaitMs,started-queuedAt);try{if(closed)throw Object.assign(Error('Storage owner is closed'),{code:'FENCED'});if(ownerToken&&!workerLease.stats().held)throw Object.assign(Error('Storage worker lease was released'),{code:'FENCED'});if(engine)await engine.verifyOwner();return await run();}finally{const runMs=performance.now()-started;maxOwnerRunMs=Math.max(maxOwnerRunMs,runMs);queueTimings.push({label,finishedAt:Date.now(),waitMs:started-queuedAt,runMs,bytes:size});if(queueTimings.length>64)queueTimings.shift();}});
 lane=operation.then(()=>undefined,()=>undefined);void operation.then(()=>{queuedOperations--;queuedBytes-=size;releaseCapacity();},()=>{queuedOperations--;queuedBytes-=size;releaseCapacity();});return operation;
}
async function execute(op: string, args: any, requestID:string) {
  if (op === 'open') {
    if (host || args?.protocol !== protocol) throw Object.assign(new Error('Incompatible storage worker protocol'), { code: 'PROTOCOL' });
    try {
      if(typeof args.owner!=='string'||!args.owner||args.owner.length>128)throw new TypeError('Invalid owner token');ownerToken=args.owner;
      if(args.schemaVersion!==undefined&&args.schemaVersion!==binding.schemaVersion)throw Object.assign(Error('Generated owner schema differs from browser binding'),{code:'SCHEMA_MISMATCH'});
      await workerLease.acquire();globalThis.postMessage({type:'progress',stage:'claiming-origin',sentAt:Date.now()});globalThis.postMessage({type:'progress',stage:'claimed-origin',sentAt:Date.now()});
      const initialize=async()=>{
      globalThis.postMessage({type:'progress',stage:'opening-sqlite',sentAt:Date.now()});host = await openOfficialSQLiteHost();globalThis.postMessage({type:'progress',stage:'opened-sqlite',sentAt:Date.now()});sampleStartupMemory('host');
      const database=sqliteOwnerTransactions(host.database);
      engine = new SQLiteTypedReplicaEngine(database, binding.schema??args.schema as SQLiteTypedSchema, undefined, { onProgress: () => globalThis.postMessage({ type: 'progress', sentAt: Date.now() }) });
      const epoch = await engine.open(args.owner);originEpoch=epoch;sampleStartupMemory('engine');globalThis.postMessage({type:'progress',stage:'opened-engine',sentAt:Date.now()});
      const storage=sqliteTypedReplicaStorage(database,engine.schema);
      canonical=new SQLiteCanonicalDispatcher(database,storage,engine);
      snapshots=new SQLiteSnapshotPages(database,()=>engine!.verifyOwner(),engine.schema);
      activation=new SQLiteSnapshotActivator(database,storage,()=>engine!.verifyOwner(),engine.schema);
      await snapshots.ready();await activation.ready();sampleStartupMemory('canonical');globalThis.postMessage({type:'progress',stage:'opened-canonical',sentAt:Date.now()});
      nativeReader=new SQLiteNativeReplicaReader(database,storage,engine);await nativeReader.ready();tabs.setGeneration(wireGeneration(await engine.verifyOwner()));
      pendingReader=new SQLitePendingSnapshotReader({database,canonical:storage,snapshots,schema:engine.schema,fence:()=>engine!.verifyOwner()});await pendingReader.ready();
      program=binding.createProgram?.({database,engine,notifyPreviewTab:tabID=>tabs.hintPending(tabID),publishPreviewTab:(tabID,value)=>tabs.preview(tabID,value),notifyTab:tabID=>schedule(0,async()=>{if(typeof tabID!=='string'||!tabID||tabID.length>128)throw new TypeError('Invalid control recipient');tabs.notify(tabID,wireGeneration(await engine!.verifyOwner()));}),reader:nativeReader,pendingReader,canonical:storage,snapshots,activation,
       runOwner:async(size,run,scopes=[],label='operation')=>{
        if(!Array.isArray(scopes)||scopes.length>8||scopes.some(value=>typeof value!=='string'||!value||value.length>512))throw new TypeError('Invalid owner publication scope');
        return waitForSQLiteOwnerAdmission(()=>schedule(size,async()=>{const before=await engine!.verifyOwner();const value=await run();const after=await engine!.verifyOwner();if(after.sequence!==before.sequence)for(const selected of scopes)publish(selected,after);return value;},label),()=>{if(closed)throw Object.assign(Error('Storage owner is closed'),{code:'FENCED'});},()=>waitForCapacity(size));
       }});
      if(program&&typeof program.call!=='function')throw new TypeError('Owner program factories must return synchronously; queue initialization through runOwner');
      globalThis.postMessage({type:'progress',stage:'created-program',sentAt:Date.now()});return { epoch:originEpoch??epoch, wasmBytes: host.memoryBytes() };
      };return await initialize();
    } catch (e) {
      (host as OfficialSQLiteHost|undefined)?.close(); host = undefined; engine = undefined;
      const message = String((e as Error).message);
      if (/mode invariant/.test(message)) Object.assign(e as object, { code: 'STORAGE_MODE' });
      else if (['SecurityError','NotSupportedError','NotAllowedError'].includes((e as Error).name) || /not supported|unavailable|is not a function/i.test(message)) Object.assign(e as object, { code: 'UNSUPPORTED' });
      throw e;
    }
  }
  if (!host || !engine) throw Object.assign(new Error('Storage owner is not ready'), { code: 'FENCED' });
  // A generated authenticated program is the sole data boundary. Raw broker
  // operations are for standalone storage hosts, never an alternate path by
  // which a tab can select another identity's scope or grant itself authority.
  if (binding.createProgram && !['ping','stats','integrity'].includes(op)) throw Object.assign(new Error('Use the authenticated generated client program'), {code:'OPERATION'});
  switch (op) {
    case 'nativeWindowPage': return (await nativeReader!.window({...args,scope:scope(args.scope)}))??null;
    case 'nativeEntityPage': return nativeReader!.entities({...args,scope:scope(args.scope)});
    case 'snapshotActivate': return activation!.activate({...args,scope:scope(args.scope)});
    case 'snapshotAppendEncoded': return snapshots!.appendEncoded(scope(args.scope),args.encoded);
    case 'snapshotAppend': return snapshots!.append(scope(args.scope),args.page);
    case 'snapshotRead': return snapshots!.read(scope(args.scope),args.subscription,args.snapshot,args.after,args.limit);
    case 'snapshotDiscard': return {removed:await snapshots!.discard(scope(args.scope),args.subscription,args.snapshot)};
    case 'canonicalCall': return canonical!.call(requestID,args);
    case 'canonicalRead': return canonical!.read(args);
    case 'canonicalGeneration': return canonical!.generation(scope(args.scope),args.expected);
    case 'ping': return { ready: true };
    case 'stats': if(args.current)return sqliteCurrentDiagnostics({originEpoch,persistence:host.persistence,wasmBytes:host.memoryBytes(),sqliteMemory:host.memoryStats?.(),statementBytes:host.statementBytes?.(),clients:ports.size,queuedOperations,queuedBytes,requests:operations,requestBytes:bytes,program:program?.budgets?.()??[],pendingPages:pendingReader?.budgets()??[],workerLease:workerLease.stats(),delivery:tabs.stats(),ownerQueue:{maxQueueWaitMs,maxOwnerRunMs},transactions:sqliteOwnerTransactionStats()});return { ...engine.stats(),originEpoch,persistence:host.persistence,workerLease:workerLease.stats(),startupMemory,...snapshots!.stats(),ingressNativeHash:Number(!!host.database.ingressNativeHash), wasmBytes: host.memoryBytes(),sqliteMemory:host.memoryStats?.(),statementBytes:host.statementBytes?.(), clients: ports.size,queuedOperations,queuedBytes,ownerQueue:{maxQueueWaitMs,maxOwnerRunMs,recent:queueTimings.slice()},transactions:sqliteOwnerTransactionStats(),requests:operations,requestBytes:bytes,delivery:tabs.stats(),nativeProfile:host.takeProfile?.(),pendingPages:pendingReader?.budgets()??[],program:program?.budgets?.()??[],programDiagnostics:await program?.diagnostics?.() };
    case 'integrity': return engine.integrity();
    case 'page': return engine.page(scope(args.scope), args.plan as SQLitePagePlan);
    case 'viewPage': return engine.viewPage(scope(args.scope), args.signature, args.plan as SQLitePagePlan, args.offset);
    case 'windowBegin': return engine.windowBegin(scope(args.scope), args.header, args.generation);
    case 'windowAppend': return engine.windowAppend(scope(args.scope), args.signature, args.generation, args.page, args.encoded);
    case 'windowPublish': await engine.windowPublish(scope(args.scope), args.signature, args.progress); return { published: true };
    case 'windowHeader': return (await engine.windowHeader(scope(args.scope), args.signature)) ?? null;
    case 'windowPage': return engine.windowPage(scope(args.scope), args.signature, args.generation, args.offset, args.limit);
    case 'windowHashes': return engine.windowHashes(scope(args.scope), args.signature, args.generation, args.after, args.limit);
    case 'windowCollect': return engine.windowCollect(scope(args.scope), args.signature, args.generation);
    case 'bootstrapBegin': return engine.beginBootstrap(scope(args.scope), args.token);
    case 'bootstrapAppend': return engine.appendBootstrap(scope(args.scope), args.token, args.page, args.encoded);
    case 'bootstrapFinish': return engine.finishBootstrap(scope(args.scope), args.token, args.expected);
    case 'count': return engine.count(scope(args.scope), args.entity, args.group, args.values);
    case 'apply': {
      if (!args.intent || typeof args.intent.id !== 'string') throw new TypeError('Broker writes require an immutable durable intent ID');
      return engine.apply(scope(args.scope), args.writes as SQLiteTypedWrite[], args.intent);
    }
    case 'outbox': return engine.outbox(scope(args.scope), args.afterSequence, args.limit);
    case 'acknowledge': await engine.acknowledge(scope(args.scope), args.ids); return { acknowledged: true };
    default: throw Object.assign(new Error('Unsupported storage worker operation'), { code: 'OPERATION' });
  }
}
function reply(id: string, value?: unknown, failure?: unknown, peer?: { port: MessagePort; token: string }, tab?: string) {
  const e = failure as { code?: string; name?: string; message?: string } | undefined;
  if (!failure) sqlitePayloadBytes(value??null, 262144);
  const output = { id, sentAt: Date.now(), ...(peer ? { protocol, to: tab, token: peer.token } : {}), ...(failure ? { error: { code: e?.code ?? 'SQLITE', name: e?.name ?? 'Error', message: String(e?.message ?? failure).slice(0, 512) } } : { value:value??null }) };
  if (peer) peer.port.postMessage(output); else globalThis.postMessage(output);
}
function reportFailure(id:string,e:unknown,peer?:{port:MessagePort;token:string},tab?:string){
      const failure = e as { resultCode?: number; code?: string | number; message?: string };
      const native = typeof failure.resultCode === 'number' ? failure.resultCode & 255 : typeof failure.code === 'number' ? failure.code & 255 : 0;
      const transient = [5,6,10,13].includes(native) || /SQLITE_(BUSY|LOCKED|IOERR|FULL)|database is locked|database or disk is full|QuotaExceeded|disk I\/O/i.test(String(failure.message));
      if (transient) {
        reply(id, undefined, { code: 'BUSY', message: 'Storage is recovering; retry the same intent ID' }, peer, tab);
        globalThis.postMessage({ type: 'storage-fault' });
      } else reply(id, undefined, e, peer, tab);
}
function admit(data: any, peer?: { port: MessagePort; token: string }, tab?: string) {
  let size: number;
  try {
    if (typeof data?.id !== 'string' || data.id.length > 256 || typeof data.op !== 'string') throw new TypeError('Invalid worker request');
    if(data.requestID!==undefined&&(typeof data.requestID!=='string'||!data.requestID||data.requestID.length>256||peer&&data.requestID!==data.id))throw new TypeError('Invalid durable worker request ID');
    size = sqliteRequestBytes(data.op,data.args,data.op === 'open' ? 1048576 : 262144);
    if (operations >= 64 || bytes + size > 2097152) throw Object.assign(new Error('Storage worker queue is full'), { code: 'BUSY' });
    const sender=peer?tab:data.tabID;if(typeof sender!=='string'||!sender||sender.length>128)throw new TypeError('Worker requests require their caller tab');tabs.register(sender);
  } catch (e) { reply(data?.id, undefined, e, peer, tab); return; }
  if(closed){reply(data.id,undefined,{code:'FENCED',message:'Storage owner is closed'},peer,tab);return;}
  operations++; bytes += size;
  const run = async () => {
    try {
      const writes=sqliteOperationPublishes(data.op,data.args)&&typeof data.args?.scope==='string';
      const before=writes&&engine?await engine.verifyOwner():undefined;
      const value = await execute(data.op, data.args, data.requestID??data.id);

      // Canonical per-scope receipts and the global storage fence have distinct
      // clocks. Peers reconcile against the current global committed clock.
      const after=before?await engine!.verifyOwner():undefined;
      const generation=after&&after.sequence!==before!.sequence?after:undefined;
      reply(data.id, value, undefined, peer, tab);
      if (generation) {
        publish(data.args.scope,generation);
      }
    } catch (e) {
      reportFailure(data.id,e,peer,tab);
    }
  };
  const work=data.op==='clientCall'
    ? Promise.resolve().then(async()=>{
      if(!program)throw Object.assign(Error('This owner has no generated client program'),{code:'UNSUPPORTED'});
      const caller=peer?tab:data.tabID;
      if(typeof caller!=='string'||!caller||caller.length>128)throw new TypeError('Invalid owner client tab');
      if(engine)await engine.verifyOwner();const value=await program.call(data.args,{tabID:caller,requestID:data.requestID??data.id,signal:tabs.register(caller)});if(engine)await engine.verifyOwner();reply(data.id,value,undefined,peer,tab);
    }).catch(error=>reportFailure(data.id,error,peer,tab))
    : schedule(size,run).catch(error=>reportFailure(data.id,error,peer,tab));
  void work.then(()=>{operations--;bytes-=size;},()=>{operations--;bytes-=size;});
};

function heartbeat(tab:string){tabs.register(tab);const message={type:'tab-pong',protocol,to:tab,token:ownerToken,sentAt:Date.now()};const peer=ports.get(tab);if(peer)peer.port.postMessage(message);else globalThis.postMessage(message);}
globalThis.onmessage = ({ data }) => {
  if(['tab-heartbeat','tab-bye','change-ack'].includes(data?.type)){
    if(data.protocol!==protocol||data.token!==ownerToken||typeof data.tab!=='string'||!data.tab||data.tab.length>128)return;
    if(data.type==='tab-bye'){tabs.drop(data.tab);return;}
    if(data.type==='change-ack'){tabs.acknowledge(data.tab,data.notification,{epoch:data.epoch,sequence:data.sequence});return;}
    try{heartbeat(data.tab);}catch{/* A control heartbeat cannot exceed tab admission. */}return;
  }
  if (data?.type !== 'attach') { admit(data); return; }
  const { tab, token, port } = data;
  if (!engine || typeof tab !== 'string' || tab.length > 128 || typeof token !== 'string' ||token!==ownerToken|| !(port instanceof MessagePort) || ports.size >= 64 && !ports.has(tab)) { port?.close(); return; }
  try{tabs.register(tab);}catch{port.close();return;}ports.get(tab)?.port.close();
  const peer = { port, token, seen: Date.now() }; ports.set(tab, peer);
  port.onmessage = ({ data: m }: MessageEvent) => {
    if (ports.get(tab) !== peer) return;
    peer.seen = Date.now();
    if (m?.type === 'bye') { tabs.drop(tab); return; }
    if(m?.protocol!==protocol||m.token!==token||m.from!==tab)return;
    if(m.type==='tab-heartbeat'){heartbeat(tab);return;}
    if(m.type==='change-ack'){tabs.acknowledge(tab,m.notification,{epoch:m.epoch,sequence:m.sequence});return;}
    if (m?.protocol !== protocol || m.token !== token || m.from !== tab || !publicOperations.has(m.op)) return;
    admit(m, peer, tab);
  };
  port.start(); port.postMessage({ type: 'attached' });
};

let disposal:Promise<void>|undefined;
return ()=>disposal??=(async()=>{
 closed=true;for(const resume of capacityWaiters.splice(0))resume();workerLease.cancelPending();clearInterval(timer);globalThis.onmessage=null;
 tabs.close();for(const peer of ports.values())peer.port.close();ports.clear();
 try{await program?.close?.();await lane;}finally{pendingReader?.close();host?.close();await workerLease.close();installed=false;}
})();

}
