import {afterEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import {reducer,schema,getRow} from '@gonvex/module-sdk';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
import {SQLitePendingSnapshotReader} from './sqlite-pending-snapshot-reader.js';
import {reducerOutboxScope} from './client-auth-wire.js';
import {SQLiteClientProgram} from './sqlite-client-program.js';
import {replicaRowsHashes,replicaSumDigest,replicaAggregateDigest} from './replica-integrity.js';
import type {SQLiteOwnerProgramContext,SQLiteOwnerCaller} from './sqlite-owner-actor.js';
import type {FunctionReference} from './index.js';
class Socket {
 static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static instances:Socket[]=[];
 readyState=0;binaryType='blob';sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
 constructor(readonly url:string){Socket.instances.push(this);}
 addEventListener(type:string,listener:(event:any)=>void){const values=this.listeners.get(type)??[];values.push(listener);this.listeners.set(type,values);}
 emit(type:string,event:any){for(const listener of this.listeners.get(type)??[])listener(event);}
 send(text:string){this.sent.push(JSON.parse(text));}
 receive(message:unknown){this.emit('message',{data:message instanceof ArrayBuffer?message:JSON.stringify(message)});}
 open(){this.readyState=1;this.emit('open',{});}
 close(){if(this.readyState===3)return;this.readyState=3;this.emit('close',{});}
}
const sqlite=await initializePinnedSQLite(),cleanup:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const close of cleanup.splice(0))await close();vi.unstubAllGlobals();vi.restoreAllMocks();});
const typed={tasks:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[]},roles:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[]},taskViews:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[]}} as const;
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
const refs:Array<FunctionReference&{localDependencyTables?:string}>=[{kind:'query',path:'__local.tasks',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','n'],maxRows:1000,maxBytes:1000000}},{kind:'reducer',path:'tasks.increment',localExecution:1,localDependencyTables:'["tasks"]'},{kind:'reducer',path:'tasks.addAll',localExecution:1}];
const caller=(tabID:string,requestID=crypto.randomUUID()):SQLiteOwnerCaller=>({tabID,requestID,signal:new AbortController().signal});
const connect={method:'connect',url:'ws://synthetic-private.test/ws',auth:{project:'project',tenant:'tenant',identity:{sub:'provider-account'}}};
const rowDigest=async(row:{id:string;n:number})=>replicaSumDigest(await replicaRowsHashes([row],'id'));
async function fixture(autoEager=true,background=false,extraReferences:FunctionReference[]=[],dependencies?:string[]){
 vi.stubGlobal('indexedDB',new IDBFactory());vi.stubGlobal('IDBKeyRange',IDBKeyRange);vi.stubGlobal('WebSocket',Socket);vi.stubGlobal('postMessage',()=>{});Socket.instances=[];
 const raw=new sqlite.oo1.DB(':memory:'),host=officialSQLiteDatabase(sqlite,raw),db=sqliteOwnerTransactions(host.database),engine=new SQLiteTypedReplicaEngine(db,typed);await engine.open('program');const canonical=sqliteTypedReplicaStorage(db,typed);await canonical.ready();const fence=()=>engine.verifyOwner(),snapshots=new SQLiteSnapshotPages(db,fence,typed),activation=new SQLiteSnapshotActivator(db,canonical,fence,typed),reader=new SQLiteNativeReplicaReader(db,canonical,engine),pendingReader=new SQLitePendingSnapshotReader({database:db,canonical,snapshots,schema:typed,fence});
 let lane=Promise.resolve(),executions=0;const hints:string[]=[],owner={database:db,engine,canonical,snapshots,activation,reader,pendingReader,async notifyTab(tab:string){hints.push(tab);},runOwner<T>(_bytes:number,run:()=>Promise<T>){const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;}} as SQLiteOwnerProgramContext;
 const runtime=new PortableReducerRuntime({schema:{tasks:{key:'id',columns:{id:{type:'text',nullable:false},n:{type:'bigint',nullable:false}}}},artifactHash:'artifact',reducers:{'tasks.increment':reducer({args:schema.any(),result:schema.any(),run:async ctx=>{executions++;const row=await getRow<{id:string;n:number}>(ctx.db,'tasks','id','a');await ctx.db.update('tasks','a',{n:row!.n+1});return row!.n+1;}}),'tasks.addAll':reducer({args:schema.any(),result:schema.any(),run:async ctx=>{const rows=await ctx.db.select<{id:string;n:number}>({table:'tasks'}),n=rows.reduce((total,row)=>total+row.n,0)+1;await ctx.db.update('tasks','a',{n});return n;}})}});
 const program=new SQLiteClientProgram(owner,{artifactHash:'artifact',runtime,references:[...(background?[...refs,{...refs[0]!,path:'__local.backgroundTasks'}]:refs).map(ref=>dependencies&&ref.path==='tasks.increment'?{...ref,localDependencyTables:JSON.stringify(dependencies)}:ref),...extraReferences],collections:autoEager?background?['__local.backgroundTasks','__local.tasks']:['__local.tasks']:[],offlineMaxAgeMs:5000});cleanup.push(async()=>{program.close();await lane;await new Promise(resolve=>setTimeout(resolve,0));raw.close();});
 const call=(input:unknown,tab='one',request?:string)=>program.call(input,caller(tab,request));
 async function authenticate(withholdFinal=false,setDigest=true,admissionEpoch='e'){
  const response=await call(connect);await call({method:'watchReplica',path:'__local.tasks',args:{}});if(background)await call({method:'watchReplica',path:'__local.backgroundTasks',args:{}});const socket=Socket.instances.at(-1)!;socket.open();await vi.waitFor(async()=>expect((await call({method:'controls'}) as any).tokens).toHaveLength(1));const request=(await call({method:'controls'}) as any).tokens[0];expect(hints).toContain('one');await call({method:'token',response:{...request,token:'opaque'}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='auth')).toBe(true));const auth=socket.sent.find(message=>message.type==='auth');socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1,replicaResume:1,...(setDigest?{replicaSetDigest:1}:{})}});socket.receive({type:'auth.result',id:auth.id,result:{artifactHash:'artifact',localIdentity:identity,replica:{protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:admissionEpoch}}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='replica.open')).toBe(true));
  const snapshot=Socket.instances.length===1?'base':'base-'+Socket.instances.length,open=socket.sent.find(message=>message.type==='replica.open'),row={id:'a',n:1},digest=setDigest?await rowDigest(row):replicaAggregateDigest(await replicaRowsHashes([row],'id'));if(!open.cursor)expect(open.digest).toBe(setDigest?'sum128-v1:00000000000000000000000000000000':'xor1:'+'0'.repeat(64));socket.receive({type:'replica.watermark',revision:1});socket.receive({type:'replica.snapshotStart',id:open.id,snapshotId:snapshot,scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',digest,path:'__local.tasks'});socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:snapshot,scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:0,last:!withholdFinal,canonicalRows:[JSON.stringify(row)],...(!withholdFinal?{digest}:{}),path:'__local.tasks'})).buffer);if(!withholdFinal)socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:1},digest});await vi.waitFor(()=>expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(1));return {socket,open,response};
 }
 return {owner,program,call,authenticate,engine,db,canonical,hints,get executions(){return executions;}};
}
it('composes token-provider authentication, one shared connection, typed pages, local execution, server settlement and original reply replay',async()=>{
 const f=await fixture(),{socket,open,response}=await f.authenticate();expect(await f.call(connect,'two')).toEqual(response);expect(Socket.instances).toHaveLength(1);const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const page:any=await f.call({method:'replicaPage',signature:source.signature,limit:10});expect(page.rows).toEqual([{id:'a',value:{id:'a',n:1}}]);
 const first:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','command');expect(first.result).toBe(2);expect(f.executions).toBe(1);expect((await f.call({method:'entities',entity:'tasks',ids:['a']},'two') as any).rows).toEqual([{id:'a',value:{id:'a',n:2}}]);await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='reducer.call')).toBe(true));const intent=socket.sent.find(message=>message.type==='reducer.call');expect(intent.idempotencyKey).toBe('command');
 socket.receive({type:'reducer.result',id:intent.id,result:2,committedRevision:2,originCommandId:'command'});const row={id:'a',n:2},digest=await rowDigest(row);socket.receive({type:'replica.transactionEncoded',cursor:{epoch:'e',revision:2},canonicalChanges:[JSON.stringify({entity:'tasks',id:'a',operation:'update',newValue:row})]});socket.receive({type:'replica.delta',id:open.id,cursor:{epoch:'e',revision:2},canonicalUpserts:[JSON.stringify(row)],deleted:[],digest});socket.receive({type:'replica.watermark',revision:2});
 await vi.waitFor(async()=>expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_reducer_journal')))?.n).toBe(0));expect(await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','command')).toEqual(first);expect(f.executions).toBe(1);expect(await f.engine.integrity()).toBe('ok');f.program.detachTab('one');expect(socket.readyState).toBe(1);expect((await f.call({method:'entities',entity:'tasks',ids:['a']},'two') as any).rows[0].value.n).toBe(2);f.program.detachTab('two');expect(socket.readyState).toBe(3);
});
it('serves verified cached SQL pages and durably predicts an offline reducer while the socket is disconnected',async()=>{
 const f=await fixture(),{socket}=await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});socket.close();expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any).rows[0].value.n).toBe(1);const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','offline-command');expect(receipt.result).toBe(2);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);expect(socket.sent.filter(message=>message.type==='reducer.call')).toEqual([]);expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_reducer_journal')))?.n).toBe(1);expect(await f.engine.integrity()).toBe('ok');
});
it('retains certified full-entity projection coverage after unwatch so an offline reducer can scan cached SQL rows',async()=>{
 const f=await fixture(false),{socket,open}=await f.authenticate(),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const closing=f.call({method:'unwatchReplica',signature:source.signature});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='replica.close')).toBe(true));socket.receive({type:'replica.closed',id:open.id});await closing;socket.close();
 const result:any=await f.call({method:'reducer',path:'tasks.addAll',args:{},now:100},'one','offline-scan');expect(result.result).toBe(2);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);expect(socket.sent.filter(message=>message.type==='reducer.call')).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
});
it('rejects unknown function metadata, oversized tokens and unowned native windows',async()=>{
 const f=await fixture();await f.authenticate();await expect(f.call({method:'reducer',path:'unknown',args:{},now:100})).rejects.toMatchObject({code:'INVALID_ROW'});await expect(f.call({method:'replicaPage',signature:'other-user-source'})).rejects.toMatchObject({code:'FENCED'});await expect(f.call({method:'auth',auth:{tenant:'other'}})).rejects.toThrow('identity changes require a new connection');await expect(f.call({method:'auth',auth:{token:'x'.repeat(16385)}})).rejects.toThrow('Invalid native');
});

it('rolls back a failed local command atomically and retries the same immutable ID',async()=>{
 const f=await fixture(),{socket}=await f.authenticate();socket.close();await f.owner.runOwner(0,()=>f.db.execAsync("CREATE TRIGGER fail_native_receipt BEFORE INSERT ON gv_reducer_invocations BEGIN SELECT RAISE(ABORT,'late SQL failure'); END;"));
 const input={method:'reducer',path:'tasks.increment',args:{},now:100};await expect(f.call(input,'one','recover-command')).rejects.toThrow('late SQL failure');expect(f.executions).toBe(1);
 await f.owner.runOwner(0,()=>f.db.execAsync('DROP TRIGGER fail_native_receipt'));const receipt:any=await f.call(input,'one','recover-command');expect(receipt.result).toBe(2);expect(f.executions).toBe(2);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_reducer_journal')))?.n).toBe(1);expect(socket.sent.filter(message=>message.type==='reducer.call')).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
});

it('delivers authenticated first-sync rows as an incomplete page before activation without granting complete coverage',async()=>{
 const f=await fixture(),{socket,open}=await f.authenticate(true),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const page:any=await f.call({method:'replicaPendingPage',signature:source.signature,limit:10});expect(page).toMatchObject({source:'pending',complete:false,serverVerified:false,receivedRows:1,rows:[{id:'a',value:{id:'a',n:1}}]});expect(await f.owner.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a']))).toEqual([]);
 const digest=await rowDigest({id:'a',n:1});socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:1,last:true,canonicalRows:[],digest,path:'__local.tasks'})).buffer);
 await vi.waitFor(()=>expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(2));
 // Activation commits before the ready control arrives. A display poll in
 // this interval must switch to canonical, never decode mutable stage rows.
 expect(await f.call({method:'replicaPendingPage',signature:source.signature})).toBeNull();
 await expect(f.call({method:'replicaPendingPage',signature:source.signature,expected:page.generation})).rejects.toMatchObject({code:'PAGE_CHANGED'});
 socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:1},digest});
 expect(await f.call({method:'replicaPendingPage',signature:source.signature})).toBeNull();expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any).rows).toEqual([{id:'a',value:{id:'a',n:1}}]);expect(await f.engine.integrity()).toBe('ok');
});

it('keeps public and account Control Plane calls working without opening tenant replica sources',async()=>{
 const f=await fixture();await f.call({method:'connect',url:connect.url,auth:{project:'project'}});const socket=Socket.instances.at(-1)!;socket.open();await vi.waitFor(async()=>expect((await f.call({method:'controls'}) as any).tokens).toHaveLength(1));const token=(await f.call({method:'controls'}) as any).tokens[0];await f.call({method:'token',response:{...token,token:null}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='auth')).toBe(true));const auth=socket.sent.find(message=>message.type==='auth');socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,controlWatermark:1}});socket.receive({type:'auth.result',id:auth.id,result:{artifactHash:'artifact',account:{id:'account'}}});
 const login=f.call({method:'action',path:'control.auth.passwordLogin',args:{email:'synthetic@example.test',password:'fixture-only'}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='action.call')).toBe(true));const action=socket.sent.find(message=>message.type==='action.call');expect(action.scope).toBe('control');expect(action.idempotencyKey).toBe(action.id);socket.receive({type:'action.result',id:action.id,result:{accessToken:'synthetic-token'}});socket.receive({type:'control.watermark',id:action.id});expect(await login).toEqual({accessToken:'synthetic-token'});
 expect(socket.sent.filter(message=>message.type==='replica.open')).toEqual([]);const query=f.call({method:'query',path:'control.accounts.me',args:{}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='query.call')).toBe(true));const call=socket.sent.find(message=>message.type==='query.call');socket.receive({type:'query.result',id:call.id,result:{id:'account'}});expect(await query).toEqual({id:'account'});expect(socket.sent.filter(message=>message.type==='replica.open')).toEqual([]);
 const change=f.call({method:'reducer',path:'control.accounts.updatePassword',args:{currentPassword:'synthetic',newPassword:'synthetic-new'}},'one','stable-control-intent');await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='reducer.call')).toBe(true));const reducer=socket.sent.find(message=>message.type==='reducer.call');expect(reducer.idempotencyKey).toBe('stable-control-intent');expect(reducer.scope).toBe('control');socket.receive({type:'reducer.result',id:reducer.id,result:{updated:true},originCommandId:'stable-control-intent'});socket.receive({type:'control.watermark',id:reducer.id});expect(await change).toEqual({commandId:'stable-control-intent',result:{updated:true}});
});

it('automatically reauthenticates a reset native feed and reopens its sources while preserving cached offline reads',async()=>{
 const f=await fixture(),{socket,open}=await f.authenticate();socket.receive({type:'replica.reset',id:open.id,reason:'recover'});await vi.waitFor(()=>expect(socket.readyState).toBe(3));expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(1);
 await vi.waitFor(()=>expect(Socket.instances).toHaveLength(2),{timeout:2500});const replacement=Socket.instances[1];replacement.open();await vi.waitFor(async()=>expect((await f.call({method:'controls'}) as any).tokens).toHaveLength(1));const token=(await f.call({method:'controls'}) as any).tokens[0];await f.call({method:'token',response:{...token,token:'opaque'}});await vi.waitFor(()=>expect(replacement.sent.some(message=>message.type==='auth')).toBe(true));const auth=replacement.sent.find(message=>message.type==='auth');replacement.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1}});replacement.receive({type:'auth.result',id:auth.id,result:{artifactHash:'artifact',localIdentity:identity,replica:{protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'}}});await vi.waitFor(()=>expect(replacement.sent.some(message=>message.type==='replica.open')).toBe(true));expect(replacement.sent.find(message=>message.type==='replica.open').id).toBe(open.id);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(1);expect(await f.engine.integrity()).toBe('ok');
});

it('retains a rejected SQL command on reconnect without sending or executing it again',async()=>{
 const f=await fixture();await f.authenticate();const status:any=await f.call({method:'status'}),scope=status.admission.intentScope;
 await f.owner.runOwner(0,()=>f.program['reducer'].journal.append({scope,path:'tasks.increment',args:{},idempotencyKey:'legacy-rejected',entityKeys:[],patches:[],createdAt:100,attempts:1,nextAttemptAt:100,state:'rejected',lastError:'Synthetic authoritative rejection',lastErrorClass:'rejected'}));
 f.program.detachTab('one');const {socket}=await f.authenticate();
 expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(1);
 expect(socket.sent.filter(message=>message.type==='reducer.call')).toEqual([]);
 expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{state:string}>("SELECT state FROM gv_reducer_journal WHERE intent_key='legacy-rejected'")))?.state).toBe('rejected');
 expect(f.executions).toBe(0);expect(await f.engine.integrity()).toBe('ok');
});

it('restores cached pages and durable offline edits before a replacement socket can open',async()=>{
 const f=await fixture();await f.authenticate();f.program.detachTab('one');await f.call(connect);const replacement=Socket.instances.at(-1)!;expect(replacement.readyState).toBe(0);const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any).rows[0].value.n).toBe(1);expect((await f.call({method:'controls'}) as any).session).toBe('restored');
 const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','cold-offline-command');expect(receipt.result).toBe(2);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);expect(replacement.sent).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
});

it.each([false,true])('finishes cached restoration when an unopened socket disconnects during the session disk read, reconnect=%s',async reconnect=>{
 const f=await fixture();await f.authenticate();f.program.detachTab('one');
 const registry=(f.program as any).sessions,safety=registry.options.safety;
 const restore=registry.restore.bind(registry),revoked=safety.sessionRevoked.bind(safety);
 let reading=false,held=false,release!:()=>void,started!:()=>void;
 const gate=new Promise<void>(resolve=>{release=resolve;}),entered=new Promise<void>(resolve=>{started=resolve;});
 vi.spyOn(registry,'restore').mockImplementation(async(...args:unknown[])=>{reading=true;try{return await restore(...args);}finally{reading=false;}});
 vi.spyOn(safety,'sessionRevoked').mockImplementation(async(...args:unknown[])=>{if(reading&&!held){held=true;started();await gate;}return revoked(...args);});
 await f.call(connect);const replacement=Socket.instances.at(-1)!;await entered;
 const pending=f.call({method:'restoreSession'});void pending.catch(()=>{});replacement.close();if(reconnect)(f.program as any).connections.values().next().value.client.connect();release();
 await expect(pending).resolves.toBe(true);
 expect((await f.call({method:'controls'}) as any).session).toBe('restored');
 const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any).rows[0].value.n).toBe(1);
 expect(replacement.sent).toEqual([]);
});

it('opens a visible source before eager-only dependencies so its first page arrives first',async()=>{
 const f=await fixture(true,true);await f.call(connect);const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const {socket}=await f.authenticate(true);
 expect(socket.sent.filter(message=>message.type==='replica.open').map(message=>message.path)).toEqual(['__local.tasks','__local.backgroundTasks']);
 expect(await f.call({method:'replicaPendingPage',signature:source.signature})).toMatchObject({complete:false,receivedRows:1,rows:[{id:'a',value:{id:'a',n:1}}]});
});

it('automatically imports the fenced legacy outbox before admitting cold offline commands',async()=>{
 const f=await fixture(),scope=reducerOutboxScope(connect.url,connect.auth,'unused'),session={directive:{protocolVersion:1 as const,scope:'tenant',visibilityScope:'visible',epoch:'e'},identity,artifactHash:'artifact',lastOnlineAtMs:Date.now()};
 const open=(name:string,version:number,upgrade:(db:IDBDatabase)=>void)=>new Promise<IDBDatabase>((resolve,reject)=>{const request=indexedDB.open(name,version);request.onupgradeneeded=()=>upgrade(request.result);request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);});
 const replica=await open('gonvex-local-replica',50,db=>{db.createObjectStore('meta',{keyPath:['scope','key']});db.createObjectStore('entities',{keyPath:['scope','entity','id']});});
 await new Promise<void>((resolve,reject)=>{const tx=replica.transaction(['meta','entities'],'readwrite');tx.objectStore('meta').put({scope,key:'session',value:JSON.stringify(session)});tx.objectStore('meta').put({scope:'visible',key:'cursor',value:JSON.stringify({epoch:'e',revision:1})});tx.objectStore('entities').put({scope:'visible',entity:'tasks',id:'a',value:JSON.stringify({id:'a',n:1}),authority:{epoch:'e',base:1,fields:{}}});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});replica.close();
 const queue=await open('gonvex-outbox',20,db=>db.createObjectStore('entries',{keyPath:'id'}).createIndex('scope','scope'));
 await new Promise<void>((resolve,reject)=>{const tx=queue.transaction('entries','readwrite');tx.objectStore('entries').put({id:1,scope,path:'tasks.increment',args:{},idempotencyKey:'legacy-offline',entityKeys:[],patches:[],localExecution:{scope:'visible',commandId:'legacy-offline',now:100,identity,artifactHash:'old-artifact'},createdAt:100,attempts:0,nextAttemptAt:100,state:'pending'});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});queue.close();
 await f.call(connect);const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:101},'one','new-offline');expect(receipt.queuedOnly).toBe(true);
 const commands=await f.owner.runOwner(0,()=>f.db.getAllAsync<{command_id:string}>('SELECT command_id FROM gv_reducer_invocations ORDER BY command_id'));
 expect(commands.map(row=>row.command_id)).toEqual(['legacy-offline','new-offline']);expect(Socket.instances[0]!.sent).toEqual([]);
 await vi.waitFor(async()=>expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value).toEqual({id:'a',n:1}));expect(await f.engine.integrity()).toBe('ok');
});

it('shares live Control queries between tabs and clears protected results on definitive denial',async()=>{
 const f=await fixture(),{socket}=await f.authenticate();await f.call(connect,'two');const input={method:'watchControl',path:'control.tenants.mine',args:{}};const one:any=await f.call(input),two:any=await f.call(input,'two');expect(two).toEqual(one);const subscriptions=socket.sent.filter(message=>message.type==='query.subscribe');expect(subscriptions).toHaveLength(1);socket.receive({type:'query.result',id:subscriptions[0].id,result:[{id:'tenant',name:'Synthetic'}]});const state:any=await f.call({method:'controlState',signature:one.signature});expect(state.result).toEqual([{id:'tenant',name:'Synthetic'}]);expect(await f.call({method:'controlState',signature:one.signature,afterVersion:state.version})).toEqual({version:state.version});
 await f.call({method:'unwatchControl',signature:one.signature});expect(socket.sent.filter(message=>message.type==='query.unsubscribe')).toHaveLength(0);await expect(f.call({method:'controlState',signature:one.signature})).rejects.toMatchObject({code:'FENCED'});
 socket.receive({type:'auth.error',id:'membership-changed',error:'Revoked',code:'revoked'});expect((await f.call({method:'controlState',signature:one.signature},'two') as any).result).toBeUndefined();expect(socket.sent.filter(message=>message.type==='query.subscribe')).toHaveLength(1);await f.call({method:'unwatchControl',signature:one.signature},'two');expect(f.program.budgets().find(value=>value.name==='native-control-watches')?.bytes).toBe(0);
});

it('queues edits instead of executing against a published bootstrap source while another source awaits its cohort watermark',async()=>{
 const f=await fixture(true,true);await f.call(connect);await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const {socket}=await f.authenticate();const progress=await f.owner.runOwner(0,()=>f.db.getFirstAsync<{unclosed:number;applied:number}>('SELECT unclosed,applied FROM gv_native_feed_progress'));expect(progress).toMatchObject({unclosed:1,applied:0});
 const result:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','cohort-edit');expect(result.queuedOnly).toBe(true);expect(f.executions).toBe(0);
 const background=socket.sent.find(message=>message.type==='replica.open'&&message.path==='__local.backgroundTasks'),row={id:'a',n:1},digest=await rowDigest(row);socket.receive({type:'replica.snapshotStart',id:background.id,snapshotId:'background',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',digest,path:'__local.backgroundTasks'});socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:background.id,snapshotId:'background',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:0,last:true,canonicalRows:[JSON.stringify(row)],digest,path:'__local.backgroundTasks'})).buffer);socket.receive({type:'replica.ready',id:background.id,cursor:{epoch:'e',revision:1},digest});await vi.waitFor(async()=>expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT unclosed,applied FROM gv_native_feed_progress'))).toMatchObject({unclosed:0,applied:1}));expect(f.executions).toBe(0);expect(await f.engine.integrity()).toBe('ok');
});

it('queries a bounded authorized membership with typed filters and optimistic SQL values',async()=>{
 const f=await fixture();await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const plan={order:[{field:'n',direction:'desc'}],where:{field:'n',op:'gte',value:1},select:['id','n'],limit:1};expect((await f.call({method:'collectionPage',signature:source.signature,plan}) as any).rows).toEqual([{id:'a',n:1}]);await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','sql-page-edit');expect((await f.call({method:'collectionPage',signature:source.signature,plan}) as any).rows).toEqual([{id:'a',n:2}]);await expect(f.call({method:'collectionPage',signature:source.signature,plan:{...plan,table:'secret'}})).rejects.toThrow('Unsupported');await expect(f.call({method:'collectionPage',signature:'foreign-source',plan})).rejects.toMatchObject({code:'FENCED'});expect(await f.engine.integrity()).toBe('ok');
});

it('shares a replica source across independent view leases and closes it only after the last view releases',async()=>{
 const f=await fixture(false),{socket,open}=await f.authenticate();const watch={method:'watchReplica',path:'__local.tasks',args:{}};const one:any=await f.call({...watch,lease:'grid'}),two:any=await f.call({...watch,lease:'board'});expect(two).toEqual(one);const before=f.program.budgets().find(value=>value.name==='native-source-interests')!.bytes;await f.call({...watch,lease:'board'});expect(f.program.budgets().find(value=>value.name==='native-source-interests')!.bytes).toBe(before);expect(socket.sent.filter(message=>message.type==='replica.open')).toHaveLength(1);await f.call({method:'unwatchReplica',signature:one.signature});await f.call({method:'unwatchReplica',signature:one.signature,lease:'grid'});expect(socket.sent.some(message=>message.type==='replica.close')).toBe(false);expect((await f.call({method:'replicaPage',signature:two.signature}) as any).rows[0].value).toEqual({id:'a',n:1});const closing=f.call({method:'unwatchReplica',signature:two.signature,lease:'board'});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='replica.close')).toBe(true));socket.receive({type:'replica.closed',id:open.id});await closing;await vi.waitFor(()=>expect(f.program.budgets().find(value=>value.name==='native-source-interests')!.bytes).toBe(0));expect(await f.engine.integrity()).toBe('ok');
});

it('keeps Control-only connections separated from tenant sources even when a tenant route is present',async()=>{
 const f=await fixture();await f.call({...connect,role:'control'});const socket=Socket.instances.at(-1)!;socket.open();await vi.waitFor(async()=>expect((await f.call({method:'controls'}) as any).tokens).toHaveLength(1));const request=(await f.call({method:'controls'}) as any).tokens[0];await f.call({method:'token',response:{...request,token:'opaque'}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='auth')).toBe(true));const auth=socket.sent.find(message=>message.type==='auth');expect(auth.controlOnly).toBe(true);socket.receive({type:'session.ready',capabilities:{}});socket.receive({type:'auth.result',id:auth.id,result:{}});await vi.waitFor(async()=>expect((await f.call({method:'controls'}) as any).authentication).toBe('authenticated'));expect(socket.sent.some(message=>message.type==='replica.open')).toBe(false);await expect(f.call({method:'watchReplica',path:'__local.tasks'})).rejects.toMatchObject({code:'INVALID_ROW'});await expect(f.call({method:'reducer',path:'tasks.increment',args:{},now:100})).rejects.toMatchObject({code:'INVALID_ROW'});
});

it('refuses a reducer issued under a stale main-thread admission without recording or executing it',async()=>{
 const f=await fixture();await f.authenticate();const key=(await f.call({method:'controls'}) as any).admissionKey;expect(typeof key).toBe('string');await expect(f.call({method:'reducer',path:'tasks.increment',args:{},now:100,expectedAdmissionKey:'old-admission'},'one','stale-main-admission')).rejects.toMatchObject({code:'FENCED'});expect(f.executions).toBe(0);expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT 1 FROM gv_reducer_invocations WHERE command_id=?','stale-main-admission'))).toBeNull();expect((await f.call({method:'reducer',path:'tasks.increment',args:{},now:100,expectedAdmissionKey:key},'one','current-main-admission') as any).result).toBe(2);
});

it('publishes live query entities and ordered memberships with the replica terminal group and reads them from SQLite offline',async()=>{
 const ref:FunctionReference={kind:'query',path:'tasks/grid',delivery:'live',live:{entity:'tasks',key:'id',resultPath:['page'],plan:{columns:['id','n']} as any}},f=await fixture(true,false,[ref]),{socket,open}=await f.authenticate(false,true,'module-epoch');const source:any=await f.call({method:'watchLive',path:ref.path,args:{limit:20},lease:'grid'});await f.call({method:'watchLive',path:ref.path,args:{limit:20},lease:'preview'});const subscribed=socket.sent.find(message=>message.type==='query.subscribe'&&message.path===ref.path);expect(subscribed).toMatchObject({scope:'tenant',args:{limit:20}});
 socket.receive({type:'query.result',id:subscribed.id,result:{page:[{id:'a',n:2}],total:100000,offset:0,limit:20},replicaScope:'visible',windowRevision:'window-2',subscriptionRevision:{epoch:'e',sequence:2}});
 await vi.waitFor(async()=>expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_live_window_stages')))?.n).toBe(1));expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(1);const pending:any=await f.call({method:'livePendingPage',signature:source.signature,limit:10});expect(pending).toMatchObject({complete:false,serverVerified:false,rows:[{id:'a',value:{id:'a',n:2}}]});
 const row={id:'a',n:2},digest=await rowDigest(row);socket.receive({type:'replica.transactionEncoded',cursor:{epoch:'e',revision:2},canonicalChanges:[JSON.stringify({entity:'tasks',id:'a',operation:'update',newValue:row})]});socket.receive({type:'replica.delta',id:open.id,cursor:{epoch:'e',revision:2},canonicalUpserts:[JSON.stringify(row)],deleted:[],digest});socket.receive({type:'replica.watermark',revision:2});
 await vi.waitFor(async()=>expect((await f.call({method:'livePage',signature:source.signature,limit:10}) as any)?.rows).toEqual([{id:'a',value:row}]));const page:any=await f.call({method:'livePage',signature:source.signature,limit:10});expect(page.window.header).toMatchObject({kind:'live',resultSkeleton:{page:[],total:100000,offset:0,limit:20},subscriptionRevision:{epoch:'e',sequence:2}});expect(page.window.header.entityCoverage).toBeUndefined();expect(await f.call({method:'livePendingPage',signature:source.signature,limit:10})).toBeNull();socket.close();expect((await f.call({method:'livePage',signature:source.signature,limit:10}) as any).rows).toEqual(page.rows);
 const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','live-offline-edit');expect(receipt.result).toBe(3);expect((await f.call({method:'livePage',signature:source.signature,limit:10}) as any).rows[0].value.n).toBe(3);expect(await f.engine.integrity()).toBe('ok');await f.call({method:'unwatchLive',signature:source.signature,lease:'grid'});expect((await f.call({method:'livePage',signature:source.signature,limit:10}) as any).rows[0].value.n).toBe(3);await f.call({method:'unwatchLive',signature:source.signature,lease:'preview'});await expect(f.call({method:'livePage',signature:source.signature,limit:10})).rejects.toMatchObject({code:'FENCED'});
});

it('preserves the storage failure when a delivery wait is superseded by reconnect',async()=>{
 const f=await fixture();await f.authenticate();const connection=[...f.program['connections'].values()][0]!;
 connection.failure={code:'INVALID_ROW',message:'Original snapshot verification failure'};
 vi.spyOn(f.program as any,'recover').mockRejectedValueOnce(Object.assign(Error('Intent delivery admission changed'),{code:'FENCED'}));
 f.program['scheduleDelivery'](connection);await new Promise(resolve=>setTimeout(resolve,20));
 expect((await f.call({method:'controls'}) as any).failure).toEqual({code:'INVALID_ROW',message:'Original snapshot verification failure'});
});

it('negotiates the incremental digest and keeps legacy integrity for a native runtime without that capability',async()=>{
 const f=await fixture();const {open}=await f.authenticate(false,false);
 expect(open.digest).toBe('xor1:'+'0'.repeat(64));
 const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any).rows).toEqual([{id:'a',value:{id:'a',n:1}}]);
 expect(await f.engine.integrity()).toBe('ok');
});

it('refuses cache reset offline and preserves both cached rows and the durable intent',async()=>{
 const f=await fixture(),{socket}=await f.authenticate();socket.close();
 const receipt=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','offline-reset-command');
 const before=await f.call({method:'entities',entity:'tasks',ids:['a']});
 await expect(f.call({method:'resetLocalReplica',keepOutbox:false})).rejects.toMatchObject({code:'disconnected'});
 expect(await f.call({method:'entities',entity:'tasks',ids:['a']})).toEqual(before);
 expect(await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','offline-reset-command')).toEqual(receipt);
 expect(f.executions).toBe(1);expect(await f.engine.integrity()).toBe('ok');
});
it('resets the canonical cache online while preserving unknown inflight commands and their original receipts',async()=>{
 const f=await fixture();await f.authenticate();
 const receipt=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','reset-command');
 const result=await f.call({method:'resetLocalReplica'});
 expect(result).toMatchObject({discardedIntents:0,resubscribed:1});
 expect(await f.owner.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a']))).toEqual([]);
 expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT 1 FROM gv_reducer_invocations WHERE command_id=?','reset-command'))).not.toBeNull();
 expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_reducer_journal')))?.n).toBe(1);
 expect(Socket.instances).toHaveLength(2);expect(f.executions).toBe(1);expect(receipt).toMatchObject({result:2});
 expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT 1 FROM gv_native_feed_resets WHERE scope=?','visible'))).not.toBeNull();
 await f.authenticate();
 await vi.waitFor(async()=>expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT 1 FROM gv_native_feed_resets WHERE scope=?','visible'))).toBeNull());
 expect(await f.engine.integrity()).toBe('ok');
});

it('evaluates an admitted generated local live plan in SQLite with the same optimistic overlay',async()=>{
 const plan={table:'tasks',key:'id',columns:['id','n'],where:{operator:'gt',column:'n',value:{argument:'floor'}},sort:{defaultColumn:'n',defaultDirection:'asc',allowedColumns:['n']},window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:50,maxLimit:128,count:'exact'}};
 const ref={kind:'query',path:'tasks.localWindow',delivery:'live',live:{entity:'tasks',key:'id',resultPath:['page'],plan}} as FunctionReference;
 const f=await fixture(true,false,[ref]),{socket}=await f.authenticate(),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const read=()=>f.call({method:'localLivePage',signature:source.signature,path:ref.path,args:{floor:0,offset:0,limit:128}}) as Promise<any>;
 expect(await read()).toMatchObject({supported:true,complete:true,rows:[{id:'a',n:1}]});socket.close();
 await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','local-scope-edit');
 expect(await read()).toMatchObject({supported:true,complete:true,rows:[{id:'a',n:2}]});
 expect(await f.call({method:'localLivePage',path:ref.path,args:{floor:1,offset:0,limit:128}})).toMatchObject({supported:true,rows:[{id:'a',n:2}]});
 await expect(f.call({method:'localLivePage',signature:'foreign',path:ref.path,args:{}})).rejects.toMatchObject({code:'FENCED'});
 expect(await f.engine.integrity()).toBe('ok');
});

it('resumes a verified disk replica with its cursor and aggregate digest without loading row hashes',async()=>{
 const f=await fixture();await f.authenticate();const watched:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 await f.call({method:'replicaPage',signature:watched.signature,limit:10});
 f.program.detachTab('one');const {open}=await f.authenticate();
 expect(open.cursor).toEqual({epoch:'e',revision:1});expect(open.digest).toBe(await rowDigest({id:'a',n:1}));
 expect(open.keys).toBeUndefined();expect(open.hashes).toBeUndefined();
});

it('reopens a server-reset source from a fresh snapshot instead of repeating its rejected resume',async()=>{
 const f=await fixture();const first=await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});await f.call({method:'replicaPage',signature:source.signature});
 first.socket.receive({type:'replica.reset',id:first.open.id,reason:'recover'});
 await vi.waitFor(()=>expect(Socket.instances.length).toBe(2),{timeout:3000});
 const second=await f.authenticate();expect(second.open.cursor).toBeUndefined();expect(second.open.digest).toBe('sum128-v1:'+'0'.repeat(32));
 expect((await f.call({method:'replicaPage',signature:source.signature}) as any).rows[0].value).toEqual({id:'a',n:1});
});

it('answers a hash request with a fresh snapshot open on the same admitted socket',async()=>{
 const f=await fixture();const first=await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});await f.call({method:'replicaPage',signature:source.signature});
 first.socket.receive({type:'replica.needHashes',id:first.open.id});
 await vi.waitFor(()=>expect(first.socket.sent.filter(frame=>frame.type==='replica.open')).toHaveLength(2));
 const reopened=first.socket.sent.filter(frame=>frame.type==='replica.open').at(-1);
 expect(reopened.cursor).toBeUndefined();expect(reopened.digest).toBe('sum128-v1:'+'0'.repeat(32));expect(Socket.instances).toHaveLength(1);
 expect((await f.call({method:'controls'}) as any).failure).toBeNull();
});

it('demands generated disk sources on intent while code preparation opens no broad subscriptions',async()=>{
 const f=await fixture(false),{socket}=await f.authenticate();socket.close();f.program.detachTab('one');await f.call(connect);
 expect(f.program.budgets().find(value=>value.name==='native-source-interests')?.entries).toBe(0);
 await f.call({method:'prepareReducer',path:'tasks.increment'});
 expect(f.program.budgets().find(value=>value.name==='native-source-interests')?.entries).toBe(0);
 const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','prepared-offline');
 expect(receipt.result).toBe(2);expect(receipt.queuedOnly).toBeUndefined();
 expect(f.program.budgets().find(value=>value.name==='native-source-interests')?.entries).toBe(1);
});

it('opens no unused generated collection merely because its declared budget is small',async()=>{
 const f=await fixture(true,true);await f.call(connect);
 expect(f.program.budgets().find(value=>value.name==='native-source-interests')?.entries).toBe(0);
 await f.call({method:'prepareReducer',path:'tasks.increment'});
 expect(f.program.budgets().find(value=>value.name==='native-source-interests')?.entries).toBe(0);
 const {socket}=await f.authenticate();expect(socket.sent.filter(message=>message.type==='replica.open').map(message=>message.path)).toEqual(['__local.tasks','__local.backgroundTasks']);
});

it('serves an in-flight committed page across disconnect without accepting obsolete frame admission',async()=>{
 const f=await fixture(),{socket}=await f.authenticate(),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const read=f.owner.reader.window.bind(f.owner.reader);let release!:()=>void,started!:()=>void;const held=new Promise<void>(resolve=>release=resolve),entered=new Promise<void>(resolve=>started=resolve);
 vi.spyOn(f.owner.reader,'window').mockImplementationOnce(async input=>{started();await held;return read(input);});
 const page=f.call({method:'replicaPage',signature:source.signature,limit:10});await entered;socket.close();socket.receive({type:'replica.transactionEncoded',cursor:{epoch:'e',revision:2},canonicalChanges:[JSON.stringify({entity:'tasks',id:'a',operation:'update',newValue:{id:'a',n:999}})]});release();
 expect((await page as any).rows).toEqual([{id:'a',value:{id:'a',n:1}}]);
 expect((await f.call({method:'controls'}) as any).status.isWebSocketConnected).toBe(false);expect(await f.engine.integrity()).toBe('ok');
});

it('fences an in-flight cached page immediately when tenant admission is revoked',async()=>{
 const f=await fixture(),{socket}=await f.authenticate(),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const read=f.owner.reader.window.bind(f.owner.reader);let release!:()=>void,started!:()=>void;const held=new Promise<void>(resolve=>release=resolve),entered=new Promise<void>(resolve=>started=resolve);
 vi.spyOn(f.owner.reader,'window').mockImplementationOnce(async input=>{started();await held;return read(input);});
 const page=f.call({method:'replicaPage',signature:source.signature,limit:10}),rejected=expect(page).rejects.toMatchObject({code:'FENCED'});await entered;
 socket.receive({type:'auth.error',id:'membership-changed',error:'Member removed',code:'member_removed'});release();await rejected;
});

it('returns an obsolete entity-page generation without entering the feed bootstrap barrier',async()=>{
 const f=await fixture();await f.authenticate();
 const page:any=await f.call({method:'entities',entity:'tasks',ids:['a']});
 const connection=[...(f.program as any).connections.values()][0] as any;
 const barrier=vi.spyOn(connection.feed,'assertReadable').mockRejectedValue(Error('A new snapshot cannot repair a caller generation'));
 await expect(f.call({method:'entities',entity:'tasks',ids:['a'],expected:{...page.generation,sequence:page.generation.sequence+1}})).rejects.toMatchObject({code:'PAGE_CHANGED'});
 expect(barrier).not.toHaveBeenCalled();
 expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(1);
});

it('serves a cached page crossing token refresh while its verified identity remains unchanged',async()=>{
 const f=await fixture();await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const connection=[...(f.program as any).connections.values()][0] as any;
 let release!:()=>void;connection.restoring=new Promise<void>(resolve=>release=resolve);
 const page=f.call({method:'replicaPage',signature:source.signature,limit:10});
 await f.call({method:'auth',auth:{token:'refreshed-same-account'}});release();
 expect((await page as any).rows[0].value.n).toBe(1);
});

it.each(['reducer','replicaPendingPage'])('keeps %s admission strict across the same token refresh',async method=>{
 const f=await fixture();await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const connection=[...(f.program as any).connections.values()][0] as any;
 let release!:()=>void;connection.restoring=new Promise<void>(resolve=>release=resolve);
 const pending=f.call(method==='reducer'?{method,path:'tasks.increment',args:{},now:100}:{method,signature:source.signature});
 const fenced=expect(pending).rejects.toMatchObject({code:'FENCED'});
 await f.call({method:'auth',auth:{token:'refreshed-same-account'}});release();await fenced;
 expect(f.executions).toBe(0);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(1);
});

it('rejects a cached read crossing refresh when the member is definitively revoked',async()=>{
 const f=await fixture(),{socket}=await f.authenticate(),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const connection=[...(f.program as any).connections.values()][0] as any;
 let release!:()=>void;connection.restoring=new Promise<void>(resolve=>release=resolve);
 const pending=f.call({method:'replicaPage',signature:source.signature,limit:10}),fenced=expect(pending).rejects.toMatchObject({code:'FENCED'});
 await f.call({method:'auth',auth:{token:'refreshed-same-account'}});socket.receive({type:'auth.error',id:'membership-changed',error:'Member removed',code:'member_removed'});release();await fenced;
});

it('keeps verified per-source resume cursors while a durable global cohort is unfinished',async()=>{
 const f=await fixture();await f.authenticate();const watched:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 await f.call({method:'replicaPage',signature:watched.signature});
 await f.owner.runOwner(0,()=>f.db.withTransactionAsync(()=>f.db.runAsync('UPDATE gv_native_feed_progress SET unclosed=applied+1')));
 // Resume negotiation is not read authority. The unfinished cohort still blocks UI reads.
 await expect(f.call({method:'replicaPage',signature:watched.signature})).rejects.toMatchObject({code:'PAGE_CHANGED'});
 f.program.detachTab('one');const {open}=await f.authenticate();
 expect(open.cursor).toEqual({epoch:'e',revision:1});expect(open.digest).toBe(await rowDigest({id:'a',n:1}));
 expect(open.keys).toBeUndefined();expect(open.hashes).toBeUndefined();
});

it.each(['reset','artifact','projection'] as const)('refuses a cached resume offer after %s invalidates its source contract',async(reason)=>{
 const f=await fixture();await f.authenticate();const watched:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});await f.call({method:'replicaPage',signature:watched.signature});
 await f.owner.runOwner(0,()=>f.db.withTransactionAsync(async()=>{
  if(reason==='reset')await f.db.runAsync('INSERT INTO gv_native_feed_resets VALUES(?)','visible');
  if(reason==='artifact')await f.db.runAsync("UPDATE gv_window_generations SET header=json_set(header,'$.artifactHash','obsolete')");
  if(reason==='projection')await f.db.runAsync("UPDATE gv_window_generations SET header=json_set(header,'$.columns',json(?))",JSON.stringify(['id']));
 }));
 f.program.detachTab('one');const {open}=await f.authenticate();expect(open.cursor).toBeUndefined();expect(open.digest).toBe('sum128-v1:00000000000000000000000000000000');
});

it('opens a source added during session activation once, across the session source loop',async()=>{
 const lateRef={kind:'query' as const,path:'__local.lateTasks',delivery:'replica' as const,replica:{table:'tasks',key:'id',columns:['id','n'],maxRows:1000,maxBytes:1000000}};
 const f=await fixture(false,false,[lateRef]);let authenticated=false,armed=true,release!:()=>void,entered!:()=>void;
 const barrier=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 const receive=Socket.prototype.receive;vi.spyOn(Socket.prototype,'receive').mockImplementation(function(this:Socket,message:any){if(message?.type==='auth.result')authenticated=true;return receive.call(this,message);});
 const load=f.owner.canonical.loadSession.bind(f.owner.canonical);vi.spyOn(f.owner.canonical,'loadSession').mockImplementation(async(...args)=>{const saved=await load(...args);if(authenticated&&armed){armed=false;entered();await barrier;}return saved;});
 const auth=f.authenticate();await waiting;const late=f.call({method:'watchReplica',path:lateRef.path,args:{}});
 await vi.waitFor(()=>expect(f.program.budgets().find(row=>row.name==='native-source-descriptors')?.entries).toBe(2));
 release();const {socket}=await auth;await late;
 await vi.waitFor(()=>expect(socket.sent.filter(frame=>frame.type==='replica.open'&&frame.path===lateRef.path).length).toBeGreaterThan(0));
 expect(socket.sent.filter(frame=>frame.type==='replica.open'&&frame.path===lateRef.path)).toHaveLength(1);
});

it('accepts a Ready-only verified resume at a newer cursor without another row body',async()=>{
 const f=await fixture();await f.authenticate();const watched:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});await f.call({method:'replicaPage',signature:watched.signature});
 const socket=Socket.instances.at(-1)!;socket.receive({type:'replica.ready',id:socket.sent.find(frame=>frame.type==='replica.open').id,cursor:{epoch:'e',revision:2},digest:await rowDigest({id:'a',n:1})});socket.receive({type:'replica.watermark',revision:2});
 await vi.waitFor(async()=>expect(await f.call({method:'replicaPage',signature:watched.signature})).toMatchObject({rows:[{id:'a',value:{id:'a',n:1}}],window:{header:{cursor:{epoch:'e',revision:2}}}}));
 expect(await f.engine.integrity()).toBe('ok');
});

it('reuses a just-released source during a frame remount without close or another resume',async()=>{
 const f=await fixture(false);const {socket}=await f.authenticate();const watched:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});await f.call({method:'replicaPage',signature:watched.signature});
 const before=await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT total_changes() n'));
 await f.call({method:'unwatchReplica',signature:watched.signature});
 const again=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});expect(again).toEqual(watched);
 expect(socket.sent.filter(frame=>frame.type==='replica.open')).toHaveLength(1);expect(socket.sent.some(frame=>frame.type==='replica.close')).toBe(false);
 expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT total_changes() n'))).toEqual(before);
});

it('retires a quiet network lease without discarding its published snapshot or writing empty cancellation tombstones',async()=>{
 const f=await fixture(false);const {socket,open}=await f.authenticate();const watched:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});await f.call({method:'replicaPage',signature:watched.signature});
 const before=await f.owner.runOwner(0,()=>f.db.getAllAsync('SELECT subscription,snapshot,discarded,complete,digest FROM gv_snapshot_stages'));
 await f.call({method:'unwatchReplica',signature:watched.signature});
 await vi.waitFor(()=>expect(socket.sent.some(frame=>frame.type==='replica.close')).toBe(true));socket.receive({type:'replica.closed',id:open.id});
 await vi.waitFor(()=>expect(f.program.budgets().find(row=>row.name==='native-source-descriptors')?.entries).toBe(0));
 expect(await f.owner.runOwner(0,()=>f.db.getAllAsync('SELECT subscription,snapshot,discarded,complete,digest FROM gv_snapshot_stages'))).toEqual(before);
 expect(await f.owner.runOwner(0,()=>f.db.getFirstAsync('SELECT count(*) n FROM gv_deferred_replica_canceled'))).toEqual({n:0});
 expect(await f.engine.integrity()).toBe('ok');
});

it('pushes an admission-fenced bounded first page before FULL commit and revokes it after commit',async()=>{
 const f=await fixture(),{socket,open}=await f.authenticate(true),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});
 const connection=[...(f.program as any).connections.values()][0],selected=connection.sources.get(source.signature);selected.previewInterest=true;
 const previews:any[]=[];f.owner.publishPreviewTab=(_tab,value)=>{previews.push(structuredClone(value));return true;};
 const tx=f.db.withTransactionAsync.bind(f.db);let release!:()=>void,entered!:()=>void,block=true;
 const gate=new Promise<void>(resolve=>{release=resolve;}),staged=new Promise<void>(resolve=>{entered=resolve;});
 f.db.withTransactionAsync=run=>tx(async()=>{await run();if(block&&f.db.ownerTransactionDepth===1&&(await f.db.getFirstAsync<{next_page:number}>('SELECT next_page FROM gv_snapshot_stages'))?.next_page===2){block=false;entered();await gate;}});
 try{
  const digest=await rowDigest({id:'a',n:1});socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:1,last:true,canonicalRows:[],digest,path:'__local.tasks'})).buffer);
  await staged;expect(previews).toHaveLength(1);expect(previews[0]).toMatchObject({signature:source.signature,admissionKey:expect.any(String),page:{complete:false,serverVerified:false,rows:[{id:'a',value:{id:'a',n:1}}]}});
  expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(1);
  release();await vi.waitFor(()=>expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(2));
  expect(previews[1]).toMatchObject({id:previews[0].id,page:null});
 }finally{release();}
});


it('opens only the declared bounded reducer read-set and certifies empty required tables',async()=>{
 const extra:FunctionReference[]=['roles','taskViews'].map(table=>({kind:'query',path:'__local.'+table,delivery:'replica',replica:{table,key:'id',columns:['id','n'],maxRows:100,maxBytes:65536}}));
 const dependencies=['tasks','roles','taskViews'],f=await fixture(false,false,extra,dependencies),{socket}=await f.authenticate();
 expect(socket.sent.filter(m=>m.type==='replica.open').map(m=>m.path)).not.toContain('__local.roles');
 await f.call({method:'prepareReducer',path:'tasks.increment'});expect(socket.sent.filter(m=>m.type==='replica.open').map(m=>m.path)).not.toContain('__local.roles');
 await f.call({method:'declareReducerReadSet',path:'tasks.increment'});
 await vi.waitFor(()=>{for(const table of dependencies)expect(socket.sent.some(m=>m.type==='replica.open'&&m.path==='__local.'+table)).toBe(true);});
 const empty=replicaSumDigest(await replicaRowsHashes([],'id'));
 for(const table of ['roles','taskViews']){
  const open=socket.sent.find(m=>m.type==='replica.open'&&m.path==='__local.'+table),snapshot='prepared-'+table;
  socket.receive({type:'replica.snapshotStart',id:open.id,snapshotId:snapshot,scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',digest:empty,path:open.path});
  socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:snapshot,scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:0,last:true,canonicalRows:[],digest:empty,path:open.path})).buffer);
  socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:1},digest:empty});
 }
 socket.receive({type:'replica.watermark',revision:1});
 await vi.waitFor(async()=>{const rows=await f.owner.runOwner(0,()=>f.db.getAllAsync<{entity:string}>("SELECT DISTINCT json_extract(g.header,'$.entity') entity FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) WHERE json_extract(g.header,'$.entityCoverage')='complete' AND json_extract(g.header,'$.completeness')='complete'"));expect(rows.map(r=>r.entity)).toEqual(expect.arrayContaining(dependencies));});
 socket.close();const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','prepared-offline');expect(receipt.queuedOnly).toBeUndefined();expect(receipt.result).toBe(2);
});

it('moves cold incomplete data to certified online coverage and preserves it for a warm offline reconnect edit',async()=>{
 const f=await fixture(false),{socket,open}=await f.authenticate(true);
 const complete=()=>f.owner.runOwner(0,()=>f.db.getAllAsync<{entity:string}>("SELECT json_extract(g.header,'$.entity') entity FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) WHERE json_extract(g.header,'$.entityCoverage')='complete' AND json_extract(g.header,'$.completeness')='complete'"));
 expect((await complete()).some(row=>row.entity==='tasks')).toBe(false);
 const digest=await rowDigest({id:'a',n:1});
 socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:1,last:true,canonicalRows:[],digest,path:'__local.tasks'})).buffer);
 socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:1},digest});socket.receive({type:'replica.watermark',revision:1});
 await vi.waitFor(async()=>expect((await complete()).map(row=>row.entity)).toContain('tasks'));
 await f.call({method:'prepareReducer',path:'tasks.increment'});
 socket.close();f.program.detachTab('one');await f.call(connect);
 expect(await f.call({method:'restoreSession'})).toBe(true);
 expect((await complete()).map(row=>row.entity)).toContain('tasks');
 const reply:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','warm-offline-edit');
 expect(reply.queuedOnly).toBeUndefined();expect(reply.result).toBe(2);
 expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);
});
it('notifies followers when a late scoped empty collection becomes ready at the existing watermark',async()=>{
 const ref:FunctionReference={kind:'query',path:'taskResources.listTaskUsers',delivery:'replica',replica:{table:'roles',key:'id',columns:['id','n'],equalFilters:{taskId:'n'},maxRows:100,maxBytes:10000}};
 const f=await fixture(true,false,[ref]),{socket}=await f.authenticate();
 await vi.waitFor(async()=>expect((await f.call({method:'replicaPage',signature:(await f.call({method:'watchReplica',path:'__local.tasks',args:{}}) as any).signature,limit:10}) as any)?.rows).toHaveLength(1));
 const source:any=await f.call({method:'watchReplica',path:ref.path,args:{taskId:99}});
 const open=socket.sent.find(message=>message.type==='replica.open'&&message.path===ref.path),empty=replicaSumDigest(await replicaRowsHashes([],'id'));
 expect(open).toBeDefined();const before=f.hints.length;
 socket.receive({type:'replica.snapshotStart',id:open.id,snapshotId:'late-empty',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',digest:empty,path:ref.path});
 socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:'late-empty',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:0,last:true,canonicalRows:[],digest:empty,path:ref.path})).buffer);
 socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:1},digest:empty});
 await vi.waitFor(async()=>expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any)?.window.header.completeness).toBe('complete'));
 expect(f.hints.length).toBeGreaterThan(before);
 const page:any=await f.call({method:'replicaPage',signature:source.signature,limit:10});expect(page.rows).toEqual([]);expect(page.window.header.entityCoverage).not.toBe('complete');
});
it('accounts retained subscriptions within the same cap and returns dialog leases after repeated closes',async()=>{
 const extra:FunctionReference[]=Array.from({length:245},(_,i)=>({kind:'query',path:'fixture.collection'+i,delivery:'replica',replica:{table:'roles',key:'id',columns:['id','n'],equalFilters:{taskId:'n'},maxRows:100,maxBytes:10000}}));
 const f=await fixture(true,false,extra),{socket}=await f.authenticate();
 const send=socket.send.bind(socket);socket.send=text=>{send(text);const frame=JSON.parse(text);if(frame.type==='replica.close')queueMicrotask(()=>socket.receive({type:'replica.closed',id:frame.id}));};
 // 136 retained windows observed on the matched IDB workspace +106 dependency
 // sources. Keep the original256-source/256KiB limits while adding dialog pivots.
 for(let i=0;i<242;i++)await f.call({method:'watchReplica',path:extra[i].path,args:{taskId:1}});
 const baseline:any=await f.call({method:'subscriptionBudget'});expect(baseline.entries).toBe(243);expect(baseline.bytes).toBeLessThan(baseline.maxBytes);
 for(let n=0;n<12;n++){
  const dialogs=[];for(let i=242;i<245;i++)dialogs.push(await f.call({method:'watchReplica',path:extra[i].path,args:{taskId:n}}));
  for(const dialog of dialogs as any[])await f.call({method:'unwatchReplica',signature:dialog.signature});
  await vi.waitFor(async()=>expect((await f.call({method:'subscriptionBudget'}) as any).entries).toBe(baseline.entries));
  expect((await f.call({method:'subscriptionBudget'}) as any).bytes).toBe(baseline.bytes);
 }
 const closes=socket.sent.filter(frame=>frame.type==='replica.close').length;
 expect(()=>f.program.detachTab('one')).not.toThrow();expect(socket.readyState).toBe(3);
 expect(socket.sent.filter(frame=>frame.type==='replica.close')).toHaveLength(closes);
 expect(f.program.budgets().find(row=>row.name==='native-source-interests')?.entries).toBe(0);
});

it('bounds simultaneous snapshot opens and releases an admission after close acknowledgement',async()=>{
 const f=await fixture(),{socket}=await f.authenticate();const before=socket.sent.filter(m=>m.type==='replica.open').length;const watches:any[]=[];
 for(let n=0;n<70;n++)watches.push(await f.call({method:'watchReplica',path:'__local.tasks',args:{slice:n}}));
 expect(socket.sent.filter(m=>m.type==='replica.open').length-before).toBe(64);
 const close=f.call({method:'unwatchReplica',signature:watches[0].signature});await close;await vi.waitFor(()=>expect(socket.sent.some(m=>m.type==='replica.close')).toBe(true));const request=socket.sent.find(m=>m.type==='replica.close');socket.receive({type:'replica.closed',id:request.id});
 await vi.waitFor(()=>expect(socket.sent.filter(m=>m.type==='replica.open').length-before).toBe(65));
 const state=await f.call({method:'controls'}) as any;expect(state.failure?.code).not.toBe('BUDGET');
});

it('drains more than the ingress wait capacity of initial source frames before the terminal watermark',async()=>{
 const extra:FunctionReference[]=Array.from({length:110},(_,n)=>({kind:'query',path:'fixture.cold'+n,delivery:'replica',replica:{table:'roles',key:'id',columns:['id','n'],equalFilters:{slice:'n'},maxRows:10,maxBytes:10000}}));
 const f=await fixture(true,false,extra),{socket}=await f.authenticate();
 const empty=replicaSumDigest(await replicaRowsHashes([],'id'));let completed=0,running=false;const queue:any[]=[];let ack:undefined|(()=>void);
 const original=socket.send.bind(socket);
 const pump=async()=>{if(running)return;running=true;try{while(queue.length){const open=queue.shift(),snapshot='cold-'+completed;
 socket.receive({type:'replica.snapshotStart',id:open.id,snapshotId:snapshot,scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:2},key:'id',digest:empty,path:open.path});
 const committed=new Promise<void>(resolve=>{ack=resolve;});
 socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:snapshot,scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:2},key:'id',sequence:0,last:true,canonicalRows:[],digest:empty,path:open.path})).buffer);
 await committed;socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:2},digest:empty});completed++;
 }}finally{running=false;}};
 const failures:unknown[]=[];socket.send=text=>{original(text);const message=JSON.parse(text);if(message.type==='replica.open'&&message.path.startsWith('fixture.cold')){queue.push(message);void pump().catch(error=>failures.push(error));}if(message.type==='replica.snapshotAck'){ack?.();ack=undefined;}};
 const watches:any[]=[];for(let n=0;n<extra.length;n++)watches.push(await f.call({method:'watchReplica',path:extra[n].path,args:{slice:n}}));
 await vi.waitFor(()=>{expect(failures).toEqual([]);expect(completed).toBe(110);},{timeout:4000,interval:5});
 // All330 frames committed and returned page credit without terminal authority.
 socket.receive({type:'replica.watermark',revision:2});
 await vi.waitFor(async()=>{const page:any=await f.call({method:'replicaPage',signature:watches.at(-1).signature,limit:10});expect(page.window.header.completeness).toBe('complete');expect(page.rows).toEqual([]);},{timeout:4000,interval:5});
 expect((await f.call({method:'controls'}) as any).failure).toBeNull();
});
