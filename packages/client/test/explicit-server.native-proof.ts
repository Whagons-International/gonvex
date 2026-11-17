// Actual generated controlled-module + public SDK + native K12/capture/delivery proof. NOT Whagons.
import {SELF} from 'cloudflare:test';
import {expandReplicaBatch} from '@gonvex/protocol';
import {expect,it,vi} from 'vitest';
import {GonvexClient,MemoryLocalReplicaStorage} from '../src/index.js';
import {member,seed,stub,numbers} from '../../../cloudflare/kernel/reducer/test/client.ts';
import {api} from './explicit-server-fixture/gonvex/_generated/api.ts';
import {localRuntime} from './explicit-server-fixture/gonvex/_generated/local-executor.ts';
import manifests from './explicit-server-fixture/gonvex/_build/native-manifests.json';
class NativeSocket {
  static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static instances:NativeSocket[]=[];
  readyState=0;private socket?:WebSocket;private listeners=new Map<string,Array<(e:any)=>void>>();
  received:string[]=[];receiptFences:number[]=[];beforeReceipt?:()=>Promise<void>;
  sent:Array<{type:string;id:string;path?:string;idempotencyKey?:string;receiptPath?:string;artifactHash?:string;intentEntropy?:string;clientContract?:number}>=[];transactions:Map<string,{epoch:string;revision:number}>=new Map();
  constructor(_url:string){NativeSocket.instances.push(this);void this.open();}
  addEventListener(t:string,f:(e:any)=>void){this.listeners.set(t,[...this.listeners.get(t)??[],f]);}
  private emit(t:string,e:any={}){for(const f of this.listeners.get(t)??[])f(e);}
  private async open(){
    try {
      const response=await SELF.fetch('http://k07.test/ws',{headers:{upgrade:'websocket'}});this.socket=response.webSocket!;
      this.socket.addEventListener('message',e=>{const m=JSON.parse(String(e.data));this.received.push(m.type);if(m.type==='query.result'&&m.path==='__gonvex.reducerReceipt'&&m.result?.cursor)this.receiptFences.push(m.result.cursor.revision);for(const f of m.type==='replica.batch'?expandReplicaBatch(m,new Map()):[m])if(f.type==='replica.transaction'&&f.originCommandId)this.transactions.set(f.originCommandId,{...f.cursor});this.emit('message',{data:e.data});});
      this.socket.addEventListener('close',()=>{this.readyState=3;this.emit('close');});
      this.socket.addEventListener('error',()=>this.emit('error'));
      this.socket.accept();this.readyState=1;this.emit('open');
    }catch{this.emit('error');}
  }
  send(data:string){const m=JSON.parse(data);this.sent.push(m.type==='reducer.call'?{type:m.type,id:m.id,path:m.path,idempotencyKey:m.idempotencyKey,receiptPath:m.receiptPath,artifactHash:m.artifactHash,intentEntropy:m.intentEntropy,...(Object.hasOwn(m,'clientContract')?{clientContract:m.clientContract}:{})}:{type:m.type,id:m.id,path:m.path});if(m.type==='query.call'&&m.path==='__gonvex.reducerReceipt'&&this.beforeReceipt){const before=this.beforeReceipt;this.beforeReceipt=undefined;void before().then(()=>this.socket!.send(data));}else this.socket!.send(data);}
  close(){this.readyState=2;this.socket?.close(1000,'done');}
}

class HeldStorage extends MemoryLocalReplicaStorage {
 hold=false;entered=false;release=()=>{};
 override async applyTransaction(...args:Parameters<MemoryLocalReplicaStorage['applyTransaction']>){if(this.hold){this.entered=true;await new Promise<void>(r=>{this.release=r;});}return super.applyTransaction(...args);}
}
it('generated local refs execute once through the explicit native SDK evidence path, retain R4 at later fence R5, and refuse same-contract reload',async()=>{
 const manifest:any=manifests.v1;expect(localRuntime.artifactHash).toBe(manifest.module.hash);
 expect(api.board.add.localExecution).toBe(1);expect(api.board.add.offline.mode).toBe('allowed');
 const {projectId,tenant,accounts}=await seed('m05-generated-explicit',manifest);
 const raw=await member(projectId,tenant,accounts.alice!.email,3);
 expect(raw.auth.artifactHash).toBe(localRuntime.artifactHash);
 expect((await raw.client.call('seed-one','board.add',{id:'seed-one',title:'synthetic seed one'})).type).toBe('reducer.result');
 expect((await raw.client.call('seed-two','board.add',{id:'seed-two',title:'synthetic seed two'})).type).toBe('reducer.result');
 const original=globalThis.WebSocket;const storage=new HeldStorage();let client:GonvexClient|undefined;let fallbacks=0;
 try {
  globalThis.WebSocket=NativeSocket as unknown as typeof WebSocket;
  client=new GonvexClient('ws://controlled.invalid/ws',{project:projectId,tenant,token:raw.token,errorReporting:false,localRuntime,localDependencyResidency:'off',localReplica:{storage}});
  client.onLocalExecutionFallback(()=>{fallbacks++;});
  const w=client.watchReplica(api.board.all,{}),sock=NativeSocket.instances.at(-1)!;
  await vi.waitFor(()=>{expect(client!.localSessionState()).toBe('authenticated');expect(w.localReplicaState()?.isUpToDate).toBe(true);},{timeout:10000});
  const opts={offline:'reject' as const,optimistic:[] as [],timeoutMs:3000,execution:'server' as const};
  await expect(client.reducerWithTransactionEvidence(api.board.add,{id:'must-refuse',title:'synthetic'}, {offline:'reject',optimistic:[],timeoutMs:100})).rejects.toMatchObject({reason:'precondition',possibleWriteApplied:false});
  expect(sock.sent.filter(x=>x.type==='reducer.call')).toHaveLength(0);
  sock.beforeReceipt=async()=>{expect((await raw.client.call('unrelated','board.add',{id:'unrelated',title:'synthetic unrelated'})).type).toBe('reducer.result');};
  const created=await client.reducerWithTransactionEvidence(api.board.add,{id:'target',title:'created'},opts);
  expect(created.result).toMatchObject({id:'target',title:'created',archived:0});expect(created.confirmation.cursor.revision).toBe(4);expect(sock.receiptFences[0]).toBe(5);
  const renamed=await client.reducerWithTransactionEvidence(api.board.rename,{id:'target',title:'renamed'},opts);
  const archived=await client.reducerWithTransactionEvidence(api.board.archive,{id:'target'},opts);
  expect(renamed.result).toMatchObject({title:'renamed',archived:0});expect(archived.result).toMatchObject({title:'renamed',archived:1});
  expect(w.localReplicaResult()?.find((x:any)=>x.id==='target')).toMatchObject({title:'renamed',archived:1});
  const counters=await numbers(tenant);expect(counters.rowsWritten).toBeGreaterThan(0);
  const commands=sock.sent.filter(x=>x.type==='reducer.call');expect(commands).toHaveLength(3);expect(sock.sent.filter(x=>x.type==='query.call'&&x.path==='__gonvex.reducerReceipt')).toHaveLength(3);
  for(const [i,r] of [created,renamed,archived].entries()){const cmd=commands[i];expect(r.confirmation.cursor).toEqual(sock.transactions.get(cmd.id));expect(cmd.idempotencyKey).toBe(cmd.id);expect(cmd.receiptPath).toBe(cmd.path);expect(cmd.artifactHash).toBe(localRuntime.artifactHash);expect(cmd.intentEntropy).toMatch(/^[0-9a-f]{64}$/);expect(Object.hasOwn(cmd,'clientContract')).toBe(false);}expect(new Set(commands.map(x=>x.intentEntropy)).size).toBe(3);
  // Capture actual outbound bytes at the transport boundary; no SDK private fields are seeded.
  expect(new Set(commands.map(x=>x.id)).size).toBe(3);expect(fallbacks).toBe(0);expect(await client.listIntents()).toHaveLength(0);
  const beforeQueries=sock.sent.filter(x=>x.type==='query.call').length;
  await expect(client.reducerWithTransactionEvidence(api.board.noop,{}, {...opts,timeoutMs:100})).rejects.toMatchObject({outcome:'unknown',possibleWriteApplied:true});
  expect(sock.sent.filter(x=>x.type==='query.call')).toHaveLength(beforeQueries);
  storage.hold=true;
  const pending=client.reducerWithTransactionEvidence(api.board.rename,{id:'target',title:'actually-applied-unknown'},opts).catch(error=>error);
  await vi.waitFor(()=>expect(storage.entered).toBe(true));
  await stub('POST','sync',{...manifests.v2,project:projectId});
  expect(manifests.v2.module.hash).not.toBe(manifest.module.hash);
  expect(await pending).toMatchObject({outcome:'unknown',possibleWriteApplied:true});
  storage.release();storage.hold=false;
  const count=sock.sent.filter(x=>x.type==='reducer.call').length;
  await client.authenticate({project:projectId,tenant,token:raw.token});
  expect(client.activeArtifactHash()).toBe(manifests.v2.module.hash);await new Promise<void>(resolve=>setTimeout(resolve,0));
  await expect(client.reducerWithTransactionEvidence(api.board.rename,{id:'target',title:'must-not-send'},opts)).rejects.toMatchObject({possibleWriteApplied:false});
  expect(sock.sent.filter(x=>x.type==='reducer.call')).toHaveLength(count);
  await vi.waitFor(()=>{expect(client!.localSessionState()).toBe('authenticated');expect(w.localReplicaState()?.isUpToDate).toBe(true);},{timeout:10000});
  const installedV1=localRuntime.artifactHash;
  try {
    localRuntime.artifactHash=manifests.v2.module.hash;
    await expect(client.reducerWithTransactionEvidence(api.board.rename,{id:'target',title:'must-not-relabel-installed-binding'},opts)).rejects.toMatchObject({possibleWriteApplied:false});
    expect(sock.sent.filter(x=>x.type==='reducer.call')).toHaveLength(count);
  } finally { localRuntime.artifactHash=installedV1; }
  const rows=(await stub('GET',`tables/board?project=${projectId}&tenant=${tenant}`)).rows;
  expect(rows.find((x:any)=>x.id==='target')).toMatchObject({title:'actually-applied-unknown',archived:1});
  console.log('M05_EXPLICIT_NATIVE '+JSON.stringify({domain:'controlled-generated-module-native-sdk-NOT-Whagons',generatedArtifact:manifest.module.hash,confirmedCommands:3,initialTransaction:created.confirmation.cursor,laterReceiptFence:sock.receiptFences[0],nativeLastCall:counters,distinctKeys:3,receiptReads:3,ordinaryAuthContract:3,commandContractOmitted:true,localExecutionMetadataPreserved:true,fallbacks,noopUnknown:true,sameContractReloadUnknown:true,actualUnknownWriteRetained:true,oldBindingMismatchAfterFreshAuthRefused:true}));
 }finally{storage.release();client?.close();raw.client.close();await vi.waitFor(()=>{expect(raw.client.closed).toBe(true);expect(NativeSocket.instances.every(s=>s.readyState===3)).toBe(true);});globalThis.WebSocket=original;}
},30000);
