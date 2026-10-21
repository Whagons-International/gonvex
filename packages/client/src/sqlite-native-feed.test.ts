import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import {afterEach,expect,it,vi} from 'vitest';
import {GonvexClient} from './index.js';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {SQLiteOwnerQueueBusyError,sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteSessionJournal} from './sqlite-session-journal.js';
import {SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
import {SQLiteNativeFeed} from './sqlite-native-feed.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import {SQLiteDeferredReplicaFrames} from './sqlite-deferred-replica-frames.js';
import {decodeSQLiteNativeDelta,decodeSQLiteNativeTransaction} from './sqlite-native-control.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
const sqlite=await initializePinnedSQLite(),cleanup:Array<()=>void>=[];
afterEach(()=>{vi.restoreAllMocks();for(const close of cleanup.splice(0))close();});
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},n:{kind:'number'}},indexes:[]}} as const;
const cursor=(revision:number)=>({epoch:'e',revision});
const stable=(row:Record<string,unknown>)=>JSON.stringify(Object.fromEntries(Object.keys(row).sort().map(key=>[key,row[key]])));
const rows=[{id:'a',name:'Before café 東京',n:1},{id:'b',name:'Second',n:2}],changed={id:'a',name:'Newer canonical',n:12};
const digest=async(values=rows)=>replicaSumDigest(await replicaRowsHashes(values,'id'));
async function fixture(extra:Partial<ConstructorParameters<typeof SQLiteNativeFeed>[0]>={},completeEntity=false,moduleEpoch='e'){
 const raw=new sqlite.oo1.DB(':memory:'),host=officialSQLiteDatabase(sqlite,raw),db=sqliteOwnerTransactions(host.database),engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('feed');const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();
 const fence=()=>engine.verifyOwner(),snapshots=new SQLiteSnapshotPages(db,fence,schema),activation=new SQLiteSnapshotActivator(db,canonical,fence,schema);
 let lane=Promise.resolve();const publications:string[][]=[];const runOwner=<T>(_bytes:number,run:()=>Promise<T>,scopes:readonly string[]=[])=>{const next=lane.then(async()=>{const before=await engine.verifyOwner(),value=await run(),after=await engine.verifyOwner();if(before.sequence!==after.sequence)publications.push([...scopes]);return value;});lane=next.then(()=>{},()=>{});return next;};
 const owner={database:db,engine,canonical,snapshots,activation,runOwner} as SQLiteOwnerProgramContext;
 const safety=new SQLiteSessionJournal(owner);
 const sessions=new SQLiteWorkerSessions({artifactHash:'artifact',offlineMaxAgeMs:5000,database:db,canonical,safety,runOwner,fence});sessions.open('one');sessions.attachTab('tab','one');
 const sends:any[]=[],watermarks:number[]=[],failures:unknown[]=[];const feed=new SQLiteNativeFeed({owner,sessions,connection:'one',artifactHash:'artifact',send:message=>sends.push(message),advanceWatermark:revision=>watermarks.push(revision),failed:error=>failures.push(error),...extra});
 cleanup.push(()=>{feed.close();raw.close();});
 feed.register({id:'source',signature:'tasks-all',entity:'tasks',key:'id',path:'tasks/all',...(completeEntity?{completeEntity:true as const}:{})});feed.authenticating('identity',1);
 const session={connectionGeneration:1,directive:{protocolVersion:1 as const,scope:'tenant',visibilityScope:'visible',epoch:moduleEpoch},identity:{auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}},artifactHash:'artifact',intentScope:'identity',lastOnlineAtMs:1000,capabilities:{replicaNativeRows:1}};
 await feed.session(session,()=>true);
 const start=async()=>{feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:await digest(),path:'tasks/all'},1);};
 const append=async(part:typeof rows,sequence:number,last:boolean)=>{feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence,last,canonicalRows:part.map(stable),path:'tasks/all',...(last?{digest:await digest()}:{})})).buffer,1);};
 const ready=async(revision=10,values=rows)=>feed.frame({type:'replica.ready',id:'source',cursor:cursor(revision),digest:await digest(values)},1);
 const transaction=(revision=12)=>feed.frame({type:'replica.transactionEncoded',cursor:cursor(revision),originCommandId:'intent',canonicalChanges:[stable({entity:'tasks',id:'a',operation:'update',newValue:{...changed,n:revision}})]},1);
 return {host,owner,db,engine,canonical,feed,sends,watermarks,failures,publications,start,append,ready,transaction,session,sessions,safety,runOwner};
}
it('keeps the module admission epoch distinct from the database replica cursor epoch',async()=>{
 const f=await fixture({},false,'module-epoch');
 await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);
 await f.feed.assertReadable();expect(f.failures).toEqual([]);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).length).toBe(2);
});
it('joins auth, source start, binary staging, newer canonical data, disk deltas and terminal watermark without deadlock',async()=>{
 const f=await fixture({},false,'module-epoch');await f.start();await f.append(rows.slice(0,1),0,false);await vi.waitFor(()=>expect(f.sends).toHaveLength(1));
 f.transaction();f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])},1);
 f.feed.frame({type:'replica.watermark',revision:14},1);await vi.waitFor(async()=>{expect(f.failures).toEqual([]);expect((await f.runOwner(0,()=>new SQLiteDeferredReplicaFrames(f.db,'artifact',()=>f.engine.verifyOwner()).summary('visible'))).frames).toBe(1);});expect(f.watermarks).toEqual([]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await f.append(rows.slice(1),1,true);await f.ready(14,[changed,rows[1]]);await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.watermarks).toEqual([14]);expect(f.sends.map(ack=>ack.sequence)).toEqual([0,1]);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual([changed,rows[1]]);const window=await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all');expect(window?.header.cursor).toEqual(cursor(14));expect(window?.header.aggregateDigest).toBe(await digest([changed,rows[1]]));expect((await new SQLiteDeferredReplicaFrames(f.db,'artifact',()=>f.engine.verifyOwner()).summary('visible')).frames).toBe(0);expect(await f.engine.integrity()).toBe('ok');
});
it('leaves a newer feed group pending when a ready arrives before its terminal watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.transaction(12);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])},1);await f.ready(12,[changed,rows[1]]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});expect(f.failures).toEqual([]);expect(f.watermarks).toEqual([10]);
 f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(f.watermarks).toEqual([10,12]);
});
it.each(['queued','running'] as const)('does not certify a newly registered source during a %s watermark publication',async(phase)=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const windows=new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()),first=(await windows.header('visible','tasks-all'))!;
 await windows.publishSnapshot('visible',{...first.header,signature:'tasks-second'},'source','base');
 let release!:()=>void,entered!:()=>void,armed=true;const barrier=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 if(phase==='queued'){
  const original=f.owner.runOwner.bind(f.owner);vi.spyOn(f.owner,'runOwner').mockImplementation(async(...args)=>{if(armed&&args[0]===262144){armed=false;entered();await barrier;}return original(...args);});
 }else{
  const original=f.db.getFirstAsync.bind(f.db);vi.spyOn(f.db,'getFirstAsync').mockImplementation(async(sql,...args)=>{const value=await original(sql,...args);if(armed&&sql.startsWith('SELECT unclosed,applied FROM gv_native_feed_progress')){armed=false;entered();await barrier;}return value;});
 }
 f.feed.frame({type:'replica.watermark',revision:12},1);await waiting;
 f.feed.register({id:'second',signature:'tasks-second',entity:'tasks',key:'id',path:'tasks/second'});release();
 await f.feed.assertReadable().catch(error=>{expect(error).toMatchObject({code:'PAGE_CHANGED'});});expect(f.failures).toEqual([]);
 expect((await windows.header('visible','tasks-second'))?.header.cursor).toEqual(cursor(10));
 f.feed.frame({type:'replica.ready',id:'second',cursor:cursor(10),digest:await digest()},1);f.feed.frame({type:'replica.watermark',revision:12},1);
 await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.watermarks.at(-1)).toBe(12);
 expect((await windows.header('visible','tasks-second'))?.header.cursor).toEqual(cursor(12));expect(await f.engine.integrity()).toBe('ok');
});
it('rolls back final snapshot activation and returns no final credit when a deferred certificate is invalid',async()=>{
 const f=await fixture();await f.start();await f.append(rows.slice(0,1),0,false);await vi.waitFor(()=>expect(f.sends).toHaveLength(1));f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:'sum128-v1:'+'0'.repeat(32)},1);await f.append(rows.slice(1),1,true);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect(f.sends).toHaveLength(1);expect(f.watermarks).toEqual([]);expect(await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all')).toBeUndefined();expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual([]);expect((await f.db.getFirstAsync<{next_page:number}>('SELECT next_page FROM gv_snapshot_stages'))?.next_page).toBe(1);expect(await f.engine.integrity()).toBe('ok');
});
it('fences an already queued control after same-socket reauthentication without failing the new admission',async()=>{
 const f=await fixture();f.transaction();f.feed.authenticating('identity',1);await f.feed.session(f.session,()=>true);await new Promise(resolve=>setTimeout(resolve,10));expect(f.failures).toEqual([]);expect(await f.canonical.loadEntityRows('visible','tasks',['a'])).toEqual([]);expect(f.feed.stats().queued).toBe(0);
});
it('owns binary credits immediately but waits for durable authentication before preparing or writing a stage',async()=>{
 const f=await fixture();const original=f.safety.saveSession.bind(f.safety);let release!:()=>void,entered!:()=>void;const waiting=new Promise<void>(resolve=>{entered=resolve;}),barrier=new Promise<void>(resolve=>{release=resolve;});
 vi.spyOn(f.safety,'saveSession').mockImplementation(async(...args)=>{entered();await barrier;return original(...args);});
 f.feed.authenticating('identity',1);const activation=f.feed.session(f.session,()=>true);await waiting;await f.start();await f.append(rows,0,true);await f.ready();expect(f.sends).toEqual([]);expect(f.feed.stats().ingress?.entries).toBe(1);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_stages')).toBeNull();
 release();await activation;await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(1);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).length).toBe(2);
});
it('rolls back a cold prediction rebase, loads its chunk outside the SQL queue and retries atomically',async()=>{
 let cold=false,loads=0,release!:()=>void,entered!:()=>void;const loading=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 const f=await fixture({async authority(_scope,mutate){if(cold)throw new SQLiteReducerPreparationRequiredError('tasks/increment');await mutate();},async prepare(path){expect(path).toBe('tasks/increment');loads++;entered();await loading;cold=false;}});
 await f.start();await f.append(rows,0,true);await f.ready();await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();cold=true;f.transaction();f.feed.frame({type:'replica.watermark',revision:12},1);await waiting;
 expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a'])))[0].row).toEqual(rows[0]);expect(await f.runOwner(0,()=>f.engine.integrity())).toBe('ok');
 release();f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(loads).toBe(1);expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(changed);
});
it('processes authenticated GClient socket events through the WASM feed and settles a reducer only after durable terminal progress',async()=>{
 class Socket {
  static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static current:Socket;
  readyState=0;binaryType='blob';sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
  constructor(){Socket.current=this;}
  addEventListener(type:string,listener:(event:any)=>void){const values=this.listeners.get(type)??[];values.push(listener);this.listeners.set(type,values);}
  emit(type:string,event:any){for(const listener of this.listeners.get(type)??[])listener(event);}
  send(text:string){this.sent.push(JSON.parse(text));}
  receive(message:unknown){this.emit('message',{data:message instanceof ArrayBuffer?message:JSON.stringify(message)});}
  close(){this.readyState=3;this.emit('close',{});}
 }
 vi.stubGlobal('WebSocket',Socket);vi.stubGlobal('postMessage',()=>{});let client!:GonvexClient;
 const f=await fixture({send:message=>client.sendOwnerMessage(message),advanceWatermark:revision=>client.advanceOwnerWatermark(revision)});
 client=new GonvexClient('ws://synthetic-private.test/ws',{ownerHost:f.feed,errorReporting:false,project:'project',tenant:'tenant',token:'opaque',identity:{sub:'account'}});
 try{
  client.connect();const socket=Socket.current;socket.readyState=1;socket.emit('open',{});socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1}});const auth=socket.sent.find(message=>message.type==='auth');expect(auth.capabilities.replicaNativeRows).toBe(1);
  socket.receive({type:'auth.result',id:auth.id,result:{localIdentity:f.session.identity,replica:f.session.directive,artifactHash:'artifact'}});
  socket.receive({type:'replica.snapshotStart',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:await digest(),path:'tasks/all'});
  socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),path:'tasks/all',digest:await digest()})).buffer);
  socket.receive({type:'replica.ready',id:'source',cursor:cursor(10),digest:await digest()});socket.receive({type:'replica.watermark',revision:10});await f.feed.assertReadable();expect(client.localSessionState()).toBe('authenticated');expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(1);
  let settled=false;const pending=client.callOwnerReducer({kind:'reducer',path:'tasks/change',artifactHash:'artifact'},{id:'a'},'command',0).then(value=>{settled=true;return value;});void pending.catch(()=>{});const call=socket.sent.find(message=>message.type==='reducer.call');
  socket.receive({type:'reducer.result',id:call.id,result:{ok:true},originCommandId:'command',committedRevision:12});expect(settled).toBe(false);
  socket.receive({type:'replica.transactionEncoded',cursor:cursor(12),canonicalChanges:[stable({entity:'tasks',id:'a',operation:'update',newValue:changed})]});socket.receive({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])});socket.receive({type:'replica.watermark',revision:12});
  expect(await pending).toEqual({ok:true});expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(changed);expect(f.failures).toEqual([]);expect(client.localReplica.snapshot().entities).toEqual({});expect(await f.engine.integrity()).toBe('ok');
 }finally{client.close();vi.unstubAllGlobals();}
});
it('prunes old orphans through real typed native activation only when the generated source declares complete entity coverage',async()=>{
 const f=await fixture({},true);
 await f.canonical.applyTransaction({cursor:cursor(5),changes:[{entity:'tasks',id:'orphan',operation:'insert',newValue:{id:'orphan',name:'removed before source',n:5}}]},{entities:{},liveQueries:{}},'visible');
 await f.canonical.applyTransaction({cursor:cursor(25),changes:[{entity:'tasks',id:'newer',operation:'insert',newValue:{id:'newer',name:'newer authority',n:25}}]},{entities:{},liveQueries:{}},'visible');
 await f.start();await f.append(rows,0,true);await f.ready();await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['orphan','newer','a','b'])).map(entry=>entry.id)).toEqual(['newer','a','b']);expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.entityCoverage).toBe('complete');expect(await f.engine.integrity()).toBe('ok');
});
it('keeps admitted frames and their credits while another bounded owner operation temporarily fills the SQL queue',async()=>{
 const f=await fixture(),original=f.owner.runOwner.bind(f.owner);let refused=0;
 vi.spyOn(f.owner,'runOwner').mockImplementation(async(...args:any[])=>{if(refused++<2)throw new SQLiteOwnerQueueBusyError();return (original as any)(...args);});
 await f.start();await f.append(rows,0,true);await f.ready();await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(1);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).length).toBe(2);expect(refused).toBeGreaterThan(2);
});
it('retires a closing source only after its earlier binary frame commits and the server acknowledges cancellation',async()=>{
 const f=await fixture();await f.start();await f.append(rows.slice(0,1),0,false);let closed=false;const pending=f.feed.closeSource('source').then(()=>{closed=true;});expect(f.sends.at(-1).type).toBe('replica.close');expect(closed).toBe(false);expect(f.feed.stats().sources).toBe(1);f.feed.frame({type:'replica.closed',id:'source'},1);await pending;await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.feed.stats().sources).toBe(0);expect(f.sends.filter(frame=>frame.type==='replica.snapshotAck')).toHaveLength(1);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_stages')).toBeNull();expect(await f.engine.integrity()).toBe('ok');
});
it('releases a pending source close when its connection changes, without leaking a reply waiter',async()=>{
 const f=await fixture();await f.start();await f.feed.assertReadable();const closing=f.feed.closeSource('source');void closing.catch(()=>{});f.feed.disconnected();await expect(closing).rejects.toMatchObject({code:'FENCED'});expect(f.feed.stats().sources).toBe(0);expect(f.failures).toEqual([]);
});
it('rolls back canonical writes when same-socket admission changes during an awaited disk phase',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();const original=f.canonical.applyTransaction.bind(f.canonical);let entered!:()=>void,release!:()=>void;const waiting=new Promise<void>(resolve=>{entered=resolve;}),barrier=new Promise<void>(resolve=>{release=resolve;});
 vi.spyOn(f.canonical,'applyTransaction').mockImplementation(async(...args)=>{const result=await original(...args);entered();await barrier;return result;});f.transaction();f.feed.frame({type:'replica.watermark',revision:12},1);await waiting;f.feed.authenticating('identity',1);release();await vi.waitFor(()=>expect(f.feed.stats().queued).toBe(0));expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(rows[0]);expect((await f.db.getFirstAsync<{applied:number}>('SELECT applied FROM gv_native_feed_progress'))?.applied).toBe(10);
});
it('hashes canonical delta text before number decoding and rejects oversized or malformed controls before parsing',async()=>{
 const text='{"id":"unsafe","n":9007199254740993,"name":"東京"}',frame={type:'replica.delta' as const,id:'source',cursor:cursor(12),canonicalUpserts:[text],deleted:[]};
 const decoded=await decodeSQLiteNativeDelta(frame,'id');const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text))),byte=>byte.toString(16).padStart(2,'0')).join('');expect(decoded[0].hash).toBe(hash);expect(decoded[0].row.n).toBe(9007199254740992);
 const parse=vi.spyOn(JSON,'parse');await expect(decodeSQLiteNativeDelta({...frame,canonicalUpserts:[' '.repeat(200000)]},'id')).rejects.toMatchObject({code:'BUDGET'});expect(parse).not.toHaveBeenCalled();parse.mockRestore();await expect(decodeSQLiteNativeDelta({...frame,canonicalUpserts:[text,text]},'id')).rejects.toMatchObject({code:'INVALID_ROW'});
 expect(()=>decodeSQLiteNativeTransaction({type:'replica.transactionEncoded',cursor:cursor(12),canonicalChanges:[stable({entity:'other',id:'a',operation:'delete'})]},{tasks:'id'})).toThrow('Invalid native transaction');
});

it('serves the last consistent offline page after disconnect between transaction, delta and watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.transaction(12);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:['b'],digest:await digest([changed])},1);await f.ready(12,[changed]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});
 expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a','b']))).map(entry=>entry.row)).toEqual(rows);
 f.feed.disconnected();await f.feed.assertReadable({scope:'visible',epoch:'e'});expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a','b']))).map(entry=>entry.row)).toEqual(rows);
 expect((await f.runOwner(0,()=>new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all')))?.header.cursor).toEqual(cursor(10));expect(f.watermarks).toEqual([10]);expect(f.failures).toEqual([]);
});
it('rolls the whole terminal group back when a later membership digest fails',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();f.transaction(12);
 f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:'sum128-v1:'+'0'.repeat(32)},1);f.feed.frame({type:'replica.watermark',revision:12},1);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a','b']))).map(entry=>entry.row)).toEqual(rows);await f.feed.assertReadable({scope:'visible',epoch:'e'});expect(f.watermarks).toEqual([10]);expect(await f.runOwner(0,()=>f.engine.integrity())).toBe('ok');
});

it('rebases predictions once for an entire completed authority group, including its membership and ready certificate',async()=>{
 let rebases=0;const f=await fixture({async authority(_scope,mutate){rebases++;await mutate();}});await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();rebases=0;
 f.transaction(12);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])},1);await f.ready(12,[changed,rows[1]]);f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(rebases).toBe(1);expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(changed);expect(f.watermarks).toEqual([10,12]);
});

it('fences cached canonical rows after admission moves to a new database epoch, including offline reads',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();f.feed.authenticating('identity',2);await f.feed.session({...f.session,connectionGeneration:2,directive:{...f.session.directive,epoch:'replacement-epoch'}},()=>true);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});f.feed.disconnected();await expect(f.feed.assertReadable({scope:'visible',epoch:'replacement-epoch'})).rejects.toMatchObject({code:'PAGE_CHANGED'});expect(f.failures).toEqual([]);
});

it('displays an earlier durable page while a later import batch waits outside SQL for a lazy reducer',async()=>{
 const {SQLitePendingSnapshotReader}=await import('./sqlite-pending-snapshot-reader.js');let resume!:()=>void,preparing=false,first=true;const held=new Promise<void>(resolve=>{resume=resolve;});
 const f=await fixture({authority:async(_scope,run)=>{if(first){first=false;throw new SQLiteReducerPreparationRequiredError('lazy.reducer');}await run();},prepare:async()=>{preparing=true;await held;}});
 try{
  await f.start();await f.append(rows.slice(0,1),0,false);await vi.waitFor(()=>expect(f.sends).toHaveLength(1));
  await f.append(rows.slice(1),1,true);await vi.waitFor(()=>expect(preparing).toBe(true));
  const pointer=f.feed.pendingSource('source');expect(pointer).toEqual({snapshot:'base',subscription:'source',entity:'tasks'});
  const reader=new SQLitePendingSnapshotReader({database:f.db,canonical:f.canonical,snapshots:f.owner.snapshots,schema,fence:()=>f.engine.verifyOwner()});await reader.ready();
  const page=await f.runOwner(131072,()=>reader.page({scope:'visible',...pointer!,limit:10}));
  expect(page).toMatchObject({complete:false,serverVerified:false,receivedRows:1,rows:[{id:'a',value:rows[0]}]});expect(f.sends).toHaveLength(1);
  resume();await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();expect(f.sends).toHaveLength(2);expect(f.failures).toEqual([]);expect(await f.engine.integrity()).toBe('ok');reader.close();
 }finally{resume();}
});

it('reads the last committed cohort without draining a newer group, under the same SQL transaction',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const read=()=>f.runOwner(131072,async()=>{let result;await f.db.withTransactionAsync(async()=>{await f.feed.assertCommittedReadable({scope:'visible',epoch:'e'});const windows=new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()),header=await windows.header('visible','tasks-all');result={rows:(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row),ids:(await windows.page('visible','tasks-all',header!.generation,0,10)).ids};});return result;});
 await expect(f.feed.assertCommittedReadable({scope:'visible',epoch:'e'})).rejects.toThrow('requires an owner transaction');
 f.transaction(12);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:['b'],digest:await digest([changed])},1);await f.ready(12,[changed]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});expect(await read()).toEqual({rows,ids:['a','b']});
 f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(await read()).toEqual({rows:[changed,rows[1]],ids:['a']});expect(f.failures).toEqual([]);
});
it('blocks committed reads through an activated but unfinished bootstrap cohort and after an epoch replacement',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});
 const guard=(epoch='e')=>f.runOwner(0,()=>f.db.withTransactionAsync(()=>f.feed.assertCommittedReadable({scope:'visible',epoch})));
 await expect(guard()).rejects.toMatchObject({code:'PAGE_CHANGED'});f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();await expect(guard()).resolves.toBeUndefined();await expect(guard('replacement')).rejects.toMatchObject({code:'PAGE_CHANGED'});
});

it('reclaims published staging metadata before repeated snapshots without waiting for idle maintenance',async()=>{
 const f=await fixture();
 for(let n=0;n<4;n++){
  const snapshotId='reload-'+n;
  f.feed.frame({type:'replica.snapshotStart',id:'source',snapshotId,scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:await digest(),path:'tasks/all'},1);
  f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId,scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),digest:await digest(),path:'tasks/all'})).buffer,1);
  await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
  expect(f.failures).toEqual([]);
  expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
  expect((await f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_stages')).n).toBeLessThanOrEqual(1);
  if(n===0)await f.db.runAsync("INSERT INTO gv_snapshot_stages SELECT scope,subscription,'discarded-before-crash',metadata,next_page,rows,bytes,receipt,complete,digest,1 FROM gv_snapshot_stages LIMIT 1");
 }
 expect(await f.engine.integrity()).toBe('ok');
});

it('verifies resumed ready only after its changed membership publishes at the terminal watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.feed.register({id:'resume',signature:'tasks-all',entity:'tasks',key:'id',path:'tasks/all'});
 f.feed.frame({type:'replica.delta',id:'resume',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])},1);
 f.feed.frame({type:'replica.ready',id:'resume',cursor:cursor(12),digest:await digest([changed,rows[1]])},1);
 await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});expect(f.failures).toEqual([]);
 f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual([changed,rows[1]]);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.aggregateDigest).toBe(await digest([changed,rows[1]]));
});

it('accepts an unchanged resumed certificate without rewriting its already committed cohort',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const before=await f.db.getFirstAsync('SELECT total_changes() n');
 f.feed.register({id:'resume-quiet',signature:'tasks-all',entity:'tasks',key:'id',path:'tasks/all'});
 f.feed.frame({type:'replica.ready',id:'resume-quiet',cursor:cursor(10),digest:await digest()},1);await f.feed.assertReadable();
 expect(f.feed.stats().opening).toBe(0);expect(f.failures).toEqual([]);
 expect(await f.db.getFirstAsync('SELECT total_changes() n')).toEqual(before);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(row=>row.row)).toEqual(rows);
});

it('does not treat an incompatible resumed truncation certificate as an unchanged cohort',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.feed.register({id:'resume-bad',signature:'tasks-all',entity:'tasks',key:'id',path:'tasks/all'});
 f.feed.frame({type:'replica.ready',id:'resume-bad',cursor:cursor(10),digest:await digest(),truncated:true},1);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect(f.failures[0]).toMatchObject({code:'PAGE_CHANGED'});
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(row=>row.row)).toEqual(rows);
});

it('accounts shared schema column names as references under the unchanged source registry cap',()=>{
 const fields=Object.fromEntries(['id',...Array.from({length:100},(_,n)=>'shared_generated_column_name_'+n)].map(name=>[name,{kind:'text'}]));
 const owner={database:{},engine:{schema:{tasks:{key:'id',fields,indexes:[]}}},runOwner:async()=>undefined} as unknown as SQLiteOwnerProgramContext;
 const feed=new SQLiteNativeFeed({owner,sessions:{} as SQLiteWorkerSessions,connection:'metadata',artifactHash:'artifact',send:()=>{},advanceWatermark:()=>{},failed:()=>{}});
 try{for(let n=0;n<100;n++)feed.register({id:'source-'+n,signature:'signature-'+n,path:'tasks/ref-'+n,entity:'tasks',key:'id',columns:Object.keys(fields)});
  expect(feed.stats().sources).toBe(100);expect(feed.stats().sourceBytes).toBeLessThan(262144);
 }finally{feed.close();}
});

it('retires closed membership frames without losing the shared transaction or publishing ahead of its watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.transaction(12);const values=[{...changed,n:12},rows[1]!];
 f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(values[0]!)],deleted:[],digest:await digest(values)},1);await f.ready(12,values);
 const closing=f.feed.closeSource('source');f.feed.frame({type:'replica.closed',id:'source'},1);await closing;
 expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0]?.row?.n).toBe(1);
 f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(f.failures).toEqual([]);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0]?.row?.n).toBe(12);
 expect(await f.db.getFirstAsync('SELECT bytes,frames FROM gv_feed_group_usage WHERE id=1')).toEqual({bytes:0,frames:0});expect(await f.engine.integrity()).toBe('ok');
});

it('admits the declared source certificate burst plus a watermark within unchanged byte limits',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const certificate=await digest();
 for(let n=0;n<255;n++){const id='small-'+n;f.feed.register({id,signature:'tasks-all',entity:'tasks',key:'id',path:'tasks/all'});f.feed.frame({type:'replica.ready',id,cursor:cursor(10),digest:certificate},1);}
 await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.feed.stats().opening).toBe(0);expect(await f.engine.integrity()).toBe('ok');
});

it('keeps a committed cohort readable when a verified warm snapshot loses its terminal watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const next=[changed,rows[1]],nextDigest=await digest(next);
 f.feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'warm',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',digest:nextDigest,path:'tasks/all'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'warm',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',sequence:0,last:true,canonicalRows:next.map(stable),digest:nextDigest,path:'tasks/all'})).buffer,1);
 f.feed.frame({type:'replica.ready',id:'source',cursor:cursor(12),digest:nextDigest},1);
 await vi.waitFor(()=>{expect(f.sends).toHaveLength(2);expect(f.feed.stats().opening).toBe(0);});
 await f.runOwner(0,()=>f.db.withTransactionAsync(()=>f.feed.assertCommittedReadable({scope:'visible',epoch:'e'})));
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
 f.feed.disconnected();await f.feed.assertReadable({scope:'visible',epoch:'e'});
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(10));
 expect(f.failures).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
});

it('publishes a warm snapshot, later canonical changes and membership certificates in one terminal cohort',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const next=[changed,rows[1]],nextDigest=await digest(next),later={...changed,n:14},laterDigest=await digest([later,rows[1]]);
 f.feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'warm',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',digest:nextDigest,path:'tasks/all'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'warm',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',sequence:0,last:true,canonicalRows:next.map(stable),digest:nextDigest,path:'tasks/all'})).buffer,1);
 f.transaction(14);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(14),canonicalUpserts:[stable(later)],deleted:[],digest:laterDigest},1);
 f.feed.frame({type:'replica.ready',id:'source',cursor:cursor(14),digest:laterDigest},1);
 await vi.waitFor(()=>expect(f.sends).toHaveLength(2));expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
 f.feed.frame({type:'replica.watermark',revision:14},1);await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual([later,rows[1]]);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(14));expect(f.watermarks).toEqual([10,14]);expect(await f.engine.integrity()).toBe('ok');
});

it('keeps an existing cohort available while a newly mounted empty source waits for its watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.feed.register({id:'new-source',signature:'scoped-tasks',entity:'tasks',key:'id',path:'tasks/scoped'});
 const hash=await digest([]);
 f.feed.frame({type:'replica.snapshotStart',id:'new-source',snapshotId:'empty',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',digest:hash,path:'tasks/scoped'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'new-source',snapshotId:'empty',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',sequence:0,last:true,canonicalRows:[],digest:hash,path:'tasks/scoped'})).buffer,1);
 f.feed.frame({type:'replica.ready',id:'new-source',cursor:cursor(12),digest:hash},1);
 await vi.waitFor(()=>{expect(f.sends).toHaveLength(2);expect(f.feed.stats().opening).toBe(0);});
 await f.runOwner(0,()=>f.db.withTransactionAsync(()=>f.feed.assertCommittedReadable({scope:'visible',epoch:'e'})));
 expect(await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','scoped-tasks')).toBeUndefined();
 f.feed.disconnected();await f.feed.assertReadable({scope:'visible',epoch:'e'});
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);expect(f.failures).toEqual([]);
});

it('rolls warm snapshot activation back with an invalid terminal certificate',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const next=[changed,rows[1]],hash=await digest(next);
 f.feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'warm-bad-ready',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',digest:hash,path:'tasks/all'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'warm-bad-ready',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',sequence:0,last:true,canonicalRows:next.map(stable),digest:hash,path:'tasks/all'})).buffer,1);
 f.feed.frame({type:'replica.ready',id:'source',cursor:cursor(12),digest:'sum128-v1:'+'0'.repeat(32)},1);f.feed.frame({type:'replica.watermark',revision:12},1);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));
 expect(f.sends).toHaveLength(2);expect(f.watermarks).toEqual([10]);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(10));
 await f.feed.assertReadable({scope:'visible',epoch:'e'});expect(await f.engine.integrity()).toBe('ok');
});

it('coalesces staging hints while publishing canonical notifications only when authority changes',async()=>{
 const f=await fixture();await f.start();await f.append(rows.slice(0,1),0,false);await vi.waitFor(()=>expect(f.sends).toHaveLength(1));
 expect(f.publications).toContainEqual(['@pending']);expect(f.publications.some(scopes=>scopes.includes('visible'))).toBe(false);
 await f.append(rows.slice(1),1,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();expect(f.publications).toContainEqual(['visible']);
 f.publications.length=0;const hash=await digest([changed,rows[1]]);
 f.feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'warm-hints',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',digest:hash,path:'tasks/all'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'warm-hints',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',sequence:0,last:true,canonicalRows:[changed,rows[1]].map(stable),digest:hash,path:'tasks/all'})).buffer,1);
 f.feed.frame({type:'replica.ready',id:'source',cursor:cursor(12),digest:hash},1);await vi.waitFor(()=>{expect(f.sends).toHaveLength(3);expect(f.feed.stats().opening).toBe(0);});
 expect(f.publications).toEqual([['@pending']]);
 f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(f.publications).toEqual([['@pending'],['visible']]);expect(f.failures).toEqual([]);
});

it('does not notify canonical readers of a cold snapshot before its terminal cohort',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);
 await vi.waitFor(()=>expect(f.sends).toHaveLength(1));
 expect(f.publications).toContainEqual(['@pending']);
 expect(f.publications.some(scopes=>scopes.includes('visible'))).toBe(false);
 await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 expect(f.publications.filter(scopes=>scopes.includes('visible'))).toHaveLength(1);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(row=>row.row)).toEqual(rows);
});

it('does not rewrite disk certificates for an already published quiet watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const before=await f.db.getFirstAsync('SELECT total_changes() n');
 f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 expect(await f.db.getFirstAsync('SELECT total_changes() n')).toEqual(before);
 expect(f.failures).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
 // A source first admitted at the same database revision is still authority.
 f.feed.register({id:'second',signature:'second-window',entity:'tasks',key:'id',path:'tasks/second'});
 const hash=await digest();f.feed.frame({type:'replica.snapshotStart',id:'second',snapshotId:'second-base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:hash,path:'tasks/second'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'second',snapshotId:'second-base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),digest:hash,path:'tasks/second'})).buffer,1);
 f.feed.frame({type:'replica.ready',id:'second',cursor:cursor(10),digest:hash},1);await f.feed.assertReadable();
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','second-window'))?.header.aggregateDigest).toBe(hash);
 expect(f.failures).toEqual([]);
});


it('accepts a clean snapshot header without a disk write and prepares it with the first credited page',async()=>{
 const f=await fixture();f.host.takeProfile();const before=await f.db.getFirstAsync('SELECT total_changes() n');
 await f.start();await vi.waitFor(()=>expect(f.feed.pendingSource('source')).toMatchObject({snapshot:'base'}));
 expect(await f.db.getFirstAsync('SELECT total_changes() n')).toEqual(before);
 expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_typed')).toBeNull();
 expect(f.sends).toEqual([]);expect(f.watermarks).toEqual([]);
 expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual([]);
 await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(1);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
 expect(await f.engine.integrity()).toBe('ok');
});

it('prepares an independent source while earlier binary ingress waits outside SQL, then commits both before credit',async()=>{
 const {SQLiteTaskYield}=await import('./sqlite-task-yield.js');
 const f=await fixture();await f.start();await vi.waitFor(()=>expect(f.feed.pendingSource('source')).toBeDefined());
 let release!:()=>void;const held=new Promise<void>(resolve=>{release=resolve;});
 vi.spyOn(SQLiteTaskYield.prototype,'yield').mockImplementation(()=>held);
 try{
  await f.append(rows,0,true);
  f.feed.register({id:'second',signature:'second-window',entity:'tasks',key:'id',path:'tasks/second'});
  f.feed.frame({type:'replica.snapshotStart',id:'second',snapshotId:'second-base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:await digest(),path:'tasks/second'},1);
  await vi.waitFor(()=>expect(f.feed.pendingSource('second')).toMatchObject({snapshot:'second-base'}));
  expect(f.sends).toEqual([]);expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual([]);
  f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'second',snapshotId:'second-base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),digest:await digest(),path:'tasks/second'})).buffer,1);
  release();await f.ready();f.feed.frame({type:'replica.ready',id:'second',cursor:cursor(10),digest:await digest()},1);f.feed.frame({type:'replica.watermark',revision:10},1);
  await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(2);
  expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
  expect(await f.engine.integrity()).toBe('ok');
 }finally{release();}
});

it('replays a newly mounted source delta received before its credit-delayed warm snapshot header',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.feed.register({id:'second',signature:'second-window',entity:'tasks',key:'id',path:'tasks/second'});
 // The server installs the subscription before native delivery has a free
 // binary credit. Its later delta can reach the socket before snapshotStart.
 const newer=[changed,rows[1]],hash=await digest();
 f.feed.frame({type:'replica.delta',id:'second',cursor:cursor(14),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest(newer)},1);
 f.feed.frame({type:'replica.snapshotStart',id:'second',snapshotId:'delayed',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',digest:hash,path:'tasks/second'},1);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'second',snapshotId:'delayed',scope:'tenant',visibilityScope:'visible',cursor:cursor(12),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),digest:hash,path:'tasks/second'})).buffer,1);
 f.feed.frame({type:'replica.ready',id:'second',cursor:cursor(14),digest:await digest(newer)},1);
 await vi.waitFor(()=>expect(f.sends).toHaveLength(2));expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(rows);
 f.feed.frame({type:'replica.watermark',revision:14},1);await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual(newer);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','second-window'))?.header.cursor).toEqual(cursor(14));
 expect((await new SQLiteDeferredReplicaFrames(f.db,'artifact',()=>f.engine.verifyOwner()).summary('visible')).frames).toBe(0);
 expect(await f.engine.integrity()).toBe('ok');
});

it('FULL-commits adjacent terminal controls together and flushes immediately for a waiting reader',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();f.host.takeProfile();
 for(let revision=11;revision<=22;revision++)f.feed.frame({type:'replica.watermark',revision},1);
 await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect(f.watermarks).toEqual([10,22]);
 // One FULL transaction for all twelve controls, one for the reader's guard.
 expect(f.host.takeProfile().commits).toBe(2);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(22));
});

it('publishes no group watermark until its outer durable commit completes',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 let release!:()=>void,entered!:()=>void,armed=true;const barrier=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 const original=f.host.database.withTransactionAsync.bind(f.host.database);
 vi.spyOn(f.host.database,'withTransactionAsync').mockImplementation(run=>original(async()=>{await run();if(armed){armed=false;entered();await barrier;}}));
 f.feed.frame({type:'replica.watermark',revision:11},1);f.feed.frame({type:'replica.watermark',revision:12},1);f.feed.flushControls();await waiting;
 expect(f.watermarks).toEqual([10]);release();await f.feed.assertReadable();expect(f.watermarks).toEqual([10,12]);expect(f.failures).toEqual([]);
});

it('rolls back the whole control batch and emits no early certificate on commit failure',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const original=f.host.database.withTransactionAsync.bind(f.host.database);let armed=true;
 vi.spyOn(f.host.database,'withTransactionAsync').mockImplementation(run=>original(async()=>{await run();if(armed){armed=false;throw Error('simulated sync failure');}}));
 f.feed.frame({type:'replica.watermark',revision:11},1);f.feed.frame({type:'replica.watermark',revision:12},1);f.feed.flushControls();
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect(f.watermarks).toEqual([10]);
 expect((await f.runOwner(0,()=>new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all')))?.header.cursor).toEqual(cursor(10));
 expect(await f.engine.integrity()).toBe('ok');
});

it('cancels a delayed control group on disconnect without committing or returning coverage',async()=>{
 const f=await fixture();f.host.takeProfile();f.feed.frame({type:'replica.watermark',revision:10},1);f.feed.disconnected();
 await vi.waitFor(()=>expect(f.feed.stats().queued).toBe(0));
 expect(f.watermarks).toEqual([]);expect(f.failures).toEqual([]);expect(f.host.takeProfile().commits).toBe(0);
});


it('rolls back a deferred typed binding on a rejected first page without granting credit',async()=>{
 const f=await fixture();await f.start();await vi.waitFor(()=>expect(f.feed.pendingSource('source')).toBeDefined());
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence:1,last:true,canonicalRows:rows.map(stable),digest:await digest(),path:'tasks/all'})).buffer,1);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect(f.sends).toEqual([]);expect(f.watermarks).toEqual([]);
 expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_typed')).toBeNull();
 expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
});

it('advances an unchanged Ready cursor without snapshot, feed or membership writes',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const writes:string[]=[];const run=f.db.runAsync.bind(f.db);vi.spyOn(f.db,'runAsync').mockImplementation(async(sql,...args)=>{writes.push(sql);return run(sql,...args);});
 f.feed.frame({type:'replica.ready',id:'source',cursor:cursor(12),digest:await digest()},1);f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect(writes.filter(sql=>/gv_snapshot|gv_feed_group|gv_window_rows/.test(sql))).toEqual([]);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(12));
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(row=>row.row)).toEqual(rows);
});

it('keeps a small forty-control cold burst in one FULL commit within existing byte credit',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();f.host.takeProfile();
 for(let revision=11;revision<=50;revision++)f.feed.frame({type:'replica.watermark',revision},1);
 await f.feed.assertReadable();
 expect(f.failures).toEqual([]);expect(f.watermarks).toEqual([10,50]);
 // A byte-small burst needs one commit, plus the unchanged read guard.
 expect(f.host.takeProfile().commits).toBe(2);
 expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(50));
});


it('keeps simultaneous admitted snapshot headers inside the existing registry budget',async()=>{
 const f=await fixture(),certificate=await digest([]);
 for(let n=0;n<190;n++){
  const id='ref-'+n+'x'.repeat(90),path='refs/'+n+'y'.repeat(90);
  f.feed.register({id,signature:'signature-'+n,entity:'tasks',key:'id',path});
  f.feed.frame({type:'replica.snapshotStart',id,path,snapshotId:'snapshot-'+n+'z'.repeat(75),scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:certificate},1);
 }
 await f.feed.assertReadable();
 expect(f.failures).toEqual([]);
 expect(f.feed.stats().sources).toBe(191);
 expect(f.feed.stats().sourceBytes).toBeLessThanOrEqual(262144);
});
it.each(['transaction','delta'] as const)('rolls back earlier decoded chunks on a late %s fault without advancing authority',async kind=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 const wide=Array.from({length:80},(_,n)=>({id:'new'+n,name:'Search after create '+n,n,...Object.fromEntries(Array.from({length:52},(_,i)=>['extra'+i,null]))}));
 const original=f.canonical.applyTransaction.bind(f.canonical);let calls=0;
 vi.spyOn(f.canonical,'applyTransaction').mockImplementation(async(...args)=>{if(++calls===2)throw Error('late decoded chunk fault');return original(...args);});
 if(kind==='transaction')f.feed.frame({type:'replica.transactionEncoded',cursor:cursor(11),canonicalChanges:wide.map(row=>stable({entity:'tasks',id:row.id,operation:'insert',newValue:row}))},1);
 else f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(11),canonicalUpserts:wide.map(stable),deleted:[],digest:await digest([...rows,...wide])},1);
 f.feed.frame({type:'replica.watermark',revision:11},1);
 await vi.waitFor(()=>expect(f.failures.some(error=>String(error).includes('late decoded chunk fault'))).toBe(true));expect(calls).toBe(2);
 expect(await f.canonical.loadEntityRows('visible','tasks',['new0','new79'])).toEqual([]);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(rows[0]);
 expect((await f.db.getFirstAsync<{applied:number}>('SELECT applied FROM gv_native_feed_progress'))?.applied).toBe(10);
 expect(f.watermarks).toEqual([10]);expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.cursor).toEqual(cursor(10));expect(await f.engine.integrity()).toBe('ok');
});

it('soaks repeated reload-style control bursts beyond active slots without leaking ingress credit',async()=>{
 const budget=new SQLiteTransientBudget(2097152,128);
 const f=await fixture({reserve:bytes=>budget.reserve(bytes),acquire:bytes=>budget.acquire(bytes)});
 for(let cycle=0;cycle<12;cycle++){
  for(let n=0;n<200;n++)f.feed.frame({type:'replica.watermark',revision:cycle*200+n+1},1);
  await f.feed.assertReadable();
  await vi.waitFor(()=>expect(budget.stats()).toMatchObject({bytes:0,entries:0}));
  expect(f.failures).toEqual([]);
 }
 expect(budget.stats().refusals).toBe(0);budget.close();
},30000);

it('settles a mixed search-resubscription burst above300 controls while credited binary pages retain independent slots',async()=>{
 const budget=new SQLiteTransientBudget(2097152,128);
 const f=await fixture({reserve:bytes=>budget.reserve(bytes),reserveSnapshot:bytes=>budget.hold(bytes),acquire:bytes=>budget.acquire(bytes)});
 await f.start();await f.append(rows,0,true);
 for(let n=0;n<305;n++)f.feed.frame({type:'replica.watermark',revision:10},1);
 await f.ready();await f.feed.assertReadable();
 await vi.waitFor(()=>expect(budget.stats()).toMatchObject({bytes:0,entries:0,refusals:0}));
 expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(1);
 expect(f.feed.stats().binaryFrames).toBe(1);expect(f.feed.stats().controlKinds['replica.watermark']).toBe(305);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(row=>row.id)).toEqual(['a','b']);
 budget.close();
},30000);

it('coalesces ten wait-queue capacities of watermark frames without a budget failure',async()=>{
 const budget=new SQLiteTransientBudget(),f=await fixture({acquire:bytes=>budget.acquire(bytes)});await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await vi.waitFor(()=>expect(f.watermarks).toContain(10));
 for(let n=0;n<2560;n++)expect(()=>f.feed.frame({type:'replica.watermark',revision:11+n},1)).not.toThrow();
 await vi.waitFor(()=>expect(f.watermarks).toContain(2570));expect(f.failures).toEqual([]);expect(f.feed.stats().coalescedWatermarks).toBeGreaterThanOrEqual(2559);expect(budget.stats().peakBytes).toBeLessThanOrEqual(budget.maxBytes);expect(budget.stats().entries).toBe(0);expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual(rows.map(row=>({id:row.id,row})));
});
it('drops an overloaded source cohort to resync without throwing or losing committed rows',async()=>{
 const budget=new SQLiteTransientBudget(),f=await fixture({acquire:bytes=>budget.acquire(bytes)});await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await vi.waitFor(()=>expect(f.watermarks).toContain(10));
 const hash=await digest();for(let n=0;n<2560;n++)expect(()=>f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(11+n),canonicalUpserts:[],deleted:[],digest:hash},1)).not.toThrow();
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect(f.failures[0]).toMatchObject({code:'PAGE_CHANGED',resyncRequired:true});expect(f.feed.stats().queuedBytes).toBeLessThanOrEqual(2097152);await vi.waitFor(()=>expect(budget.stats().entries).toBe(0));expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual(rows.map(row=>({id:row.id,row})));
 f.feed.authenticating('identity',2);await f.feed.session({...f.session,connectionGeneration:2},()=>true);
 f.feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'recovered',scope:'tenant',visibilityScope:'visible',cursor:cursor(3000),key:'id',digest:hash,path:'tasks/all'},2);
 f.feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'recovered',scope:'tenant',visibilityScope:'visible',cursor:cursor(3000),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),digest:hash,path:'tasks/all'})).buffer,2);
 f.feed.frame({type:'replica.ready',id:'source',cursor:cursor(3000),digest:hash},2);f.feed.frame({type:'replica.watermark',revision:3000},2);
 await vi.waitFor(()=>expect(f.watermarks).toContain(3000));expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual(rows.map(row=>({id:row.id,row})));expect(budget.stats().peakBytes).toBeLessThanOrEqual(budget.maxBytes);
});
it('never moves a later watermark ahead of an intervening transaction',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await vi.waitFor(()=>expect(f.watermarks).toContain(10));
 f.feed.frame({type:'replica.watermark',revision:11},1);f.transaction(12);f.feed.frame({type:'replica.watermark',revision:12},1);
 await vi.waitFor(()=>expect(f.watermarks).toContain(12));expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row.n).toBe(12);expect(f.failures).toEqual([]);
});

it('retires an overflowing old-session close cohort without BUDGET or unbounded tombstones',async()=>{
 const f=await fixture();let registered=0;for(let n=0;n<250;n++){try{f.feed.register({id:'closing-'+n,signature:'closing-'+n,entity:'tasks',key:'id',path:'tasks/'+String(n)+'x'.repeat(900)});registered++;}catch(error){expect(error).toMatchObject({code:'BUDGET'});break;}}expect(registered).toBeGreaterThan(10);
 const closes:Promise<void>[]=[];for(let n=0;n<registered;n++){expect(()=>{closes.push(f.feed.closeSource('closing-'+n).catch(error=>{expect(error.code).toBe('FENCED');}));}).not.toThrow();}
 await Promise.all(closes);expect(f.failures).toHaveLength(1);expect(f.failures[0]).toMatchObject({code:'PAGE_CHANGED',resyncRequired:true});expect(f.failures.some(error=>(error as any).code==='BUDGET')).toBe(false);
});
