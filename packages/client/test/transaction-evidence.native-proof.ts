import {expandReplicaBatch} from '@gonvex/protocol';
import {archive} from './transaction-evidence.fixture.module.js';
import {SELF} from 'cloudflare:test';
import {expect,it,vi} from 'vitest';
import {GonvexClient,type FunctionReference} from '../src/index.js';
import {member,seed,stub} from '../../../cloudflare/kernel/reducer/test/client.ts';
import {boardVisibility} from '../../../cloudflare/conformance/fixtures/app/gonvex/board.ts';
import {fixtureManifest} from '../../../cloudflare/kernel/reducer/test/fixture.ts';

/** Transport adapter only: every byte goes through the original SELF Worker/socket.
 * No transaction, result, clock, receipt or reducer algorithm is implemented here.
 */
class NativeSocket {
  static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static instances:NativeSocket[]=[];
  readyState=0;private socket?:WebSocket;private listeners=new Map<string,Array<(e:any)=>void>>();
  received:string[]=[];receiptFences:number[]=[];beforeReceipt?:()=>Promise<void>;
  sent:Array<{type:string;id:string;path?:string}>=[];transactions:Map<string,{epoch:string;revision:number}>=new Map();
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
  send(data:string){const m=JSON.parse(data);this.sent.push({type:m.type,id:m.id,path:m.path});if(m.type==='query.call'&&m.path==='__gonvex.reducerReceipt'&&this.beforeReceipt){const before=this.beforeReceipt;this.beforeReceipt=undefined;void before().then(()=>this.socket!.send(data));}else this.socket!.send(data);}
  close(){this.readyState=2;this.socket?.close(1000,'done');}
}
it('actual native reducer/capture/feed + public SDK yields only the matching command transaction cursor',async()=>{
  const manifest:any=fixtureManifest();manifest.module.hash='m05-transaction-fixture-v1';
  manifest.visibility={board:boardVisibility};
  manifest.functions['m05Synthetic.archive']={kind:archive.kind,handler:'archive',file:'gonvex/m05Synthetic.ts',args:archive.options.args,result:archive.options.result,interactive:true};
  const {projectId,tenant,accounts}=await seed('m05-sdk-transaction',manifest);
  const raw=await member(projectId,tenant,accounts.alice!.email);const token=raw.token;
  await raw.client.call('seed-one','board.add',{id:'seed-one',title:'seed one',position:10});
  await raw.client.call('seed-two','board.add',{id:'seed-two',title:'seed two',position:11});
  const original=globalThis.WebSocket;let client:GonvexClient|undefined;
  try {
    globalThis.WebSocket=NativeSocket as unknown as typeof WebSocket;
    client=new GonvexClient('ws://controlled.invalid/ws',{project:projectId,tenant,token,errorReporting:false});
    const ref:FunctionReference={kind:'query',path:'board.all',delivery:'replica',replica:{table:'board',key:'id',columns:['id','title','position','bumps'],orderBy:'position',orderDirection:'asc',mode:'eager',maxRows:1000}};
    const watch=client.watchReplica(ref,{}),s=NativeSocket.instances.at(-1)!;
    await vi.waitFor(()=>{expect(client!.localSessionState()).toBe('authenticated');expect(watch.localReplicaState()?.isUpToDate).toBe(true);},{timeout:5000});
    s.beforeReceipt=async()=>{const r=await raw.client.call('unrelated','board.add',{id:'unrelated',title:'unrelated actual write',position:2});expect(r.type).toBe('reducer.result');};
    const options={offline:'reject' as const,optimistic:[] as [],timeoutMs:3000};
    const create=await client.reducerWithTransactionEvidence({kind:'reducer',path:'board.add'},{id:'synthetic',title:'created',position:1},options);
    expect(s.receiptFences[0]).toBeGreaterThan(create.confirmation.cursor.revision);
    expect(create.result).toMatchObject({id:'synthetic',title:'created'});
    const update=await client.reducerWithTransactionEvidence({kind:'reducer',path:'board.rename'},{id:'synthetic',title:'updated'},options);
    expect(update.result).toMatchObject({id:'synthetic',title:'updated'});
    const removed=await client.reducerWithTransactionEvidence({kind:'reducer',path:'m05Synthetic.archive'},{id:'synthetic'},options);
    expect(removed.result).toMatchObject({id:'synthetic',title:'[synthetic-archived]'});
    expect(watch.localReplicaResult()?.find((r:any)=>r.id==='synthetic')).toMatchObject({title:'[synthetic-archived]'});
    const calls=s.sent.filter(m=>m.type==='reducer.call');expect(calls).toHaveLength(3);
    for(const [i,r] of [create,update,removed].entries())expect(r.confirmation.cursor).toEqual(s.transactions.get(calls[i].id));
    expect(new Set(calls.map(m=>m.id)).size).toBe(3);
    expect(s.sent.filter(m=>m.type==='query.call'&&m.path==='__gonvex.reducerReceipt')).toHaveLength(3);
    expect(await client.reducer({kind:'reducer',path:'board.remove'},{id:'synthetic'},options)).toBe(null);
    const rows=(await stub('GET',`tables/board?project=${projectId}&tenant=${tenant}`)).rows;expect(rows.map((r:any)=>r.id).sort()).toEqual(['seed-one','seed-two','unrelated']);
    const receipts=(await stub('GET',`tables/_gonvex_reducer_idempotency?project=${projectId}&tenant=${tenant}`)).rows;
    expect(receipts).toHaveLength(3);
    const replay=await raw.client.call('replay','board.add',{id:'synthetic',title:'created',position:1},{idempotencyKey:calls[0].id,receiptPath:'board.add'});
    expect(replay.result).toEqual(create.result);
    expect((await stub('GET',`tables/board?project=${projectId}&tenant=${tenant}`)).rows.some((r:any)=>r.id==='synthetic')).toBe(false);
    expect(create.confirmation.cursor.revision).toBe(4);expect(s.receiptFences[0]).toBe(5);
    const beforeRefusalWrites=s.sent.filter(m=>m.type==='reducer.call').length;
    const beforeRefusalReads=s.sent.filter(m=>m.type==='query.call'&&m.path==='__gonvex.reducerReceipt').length;
    await expect(client.reducerWithTransactionEvidence({kind:'reducer',path:'board.generation'},{},{...options,timeoutMs:200})).rejects.toMatchObject({possibleWriteApplied:true,outcome:'unknown'});
    await expect(client.reducerWithTransactionEvidence({kind:'reducer',path:'notes.write'},{id:'unobserved',body:'synthetic unobserved note'},{...options,timeoutMs:200})).rejects.toMatchObject({possibleWriteApplied:true,outcome:'unknown'});
    expect(s.sent.filter(m=>m.type==='reducer.call')).toHaveLength(beforeRefusalWrites+2);
    expect(s.sent.filter(m=>m.type==='query.call'&&m.path==='__gonvex.reducerReceipt')).toHaveLength(beforeRefusalReads);
    expect((await stub('GET',`tables/notes?project=${projectId}&tenant=${tenant}`)).rows).toHaveLength(1);
    console.log('M05_TRANSACTION_EVIDENCE_NATIVE '+JSON.stringify({domain:'controlled-auth-native-sdk-workerd-sqlite-do-socket',operations:['create','rename','synthetic-retained-row-archive'],revisions:[create,update,removed].map(r=>r.confirmation.cursor.revision),confirmedCandidateWriteInvocations:3,refusedCandidateWriteInvocations:2,noOpAndUnsubscribedEntityRefused:true,ordinarySdkWrites:1,receiptReads:3,distinctCommands:3,matchingTransactionCursors:true,firstTransactionRevision:create.confirmation.cursor.revision,laterReceiptFence:s.receiptFences[0],unrelatedWrites:1,realWhagons:false,syntheticSoftArchiveExecuted:true,whagonsSoftArchiveExecuted:false,ordinarySdkPhysicalDeleteResult:null,separateExplicitReplayPreserved:true}));
  } finally {
    console.log('M05_NATIVE_STAGE '+JSON.stringify({session:client?.localSessionState(),admitted:!!client?.tenantAdmission(),received:NativeSocket.instances.at(-1)?.received,sent:NativeSocket.instances.at(-1)?.sent.map(x=>x.type)}));
    raw.client.close();await vi.waitFor(()=>expect(raw.client.closed).toBe(true));
    client?.close();await vi.waitFor(()=>expect(NativeSocket.instances.at(-1)?.readyState).toBe(3));globalThis.WebSocket=original;
  }
},15000);
