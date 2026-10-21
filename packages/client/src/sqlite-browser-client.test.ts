import {afterEach,expect,it,vi} from 'vitest';
import {SQLiteBrowserClient} from './sqlite-browser-client.js';
import type {SQLiteReplicaBroker} from './sqlite-broker.js';
const cleanup:Array<()=>void>=[];afterEach(()=>{for(const close of cleanup.splice(0))close();vi.restoreAllMocks();});
function fixture(fetchToken?:()=>Promise<string|null>){
 let admissionKey:string|null|undefined;let epoch=1,identity='a',connection='',tokenPending=false,closed=false;const changes=new Set<(...args:any[])=>void>(),calls:Array<{input:any;requestID?:string}>=[],responses:any[]=[];
 let handler:((input:any)=>Promise<unknown>|unknown)|undefined;
 const broker={async request<T>(_op:string,input:any,_timeout?:number,requestID?:string):Promise<T>{
  calls.push({input:structuredClone(input),requestID});if(handler){const result=await handler(input);if(result!==undefined)return result as T;}
  if(input.method==='connect'){identity=input.auth.identity?.sub??'public';connection=identity+':'+epoch;tokenPending=true;return {connection,identityKey:identity} as T;}
  if(input.method==='controls')return {...(admissionKey!==undefined?{admissionKey}:{}),tokens:tokenPending?[{id:'token:'+connection,connection,identityKey:identity,forceRefreshToken:false}]:[],status:{connected:true},session:'authenticated',failure:null} as T;
  if(input.method==='token'){responses.push(input.response);tokenPending=false;return null as T;}
  if(input.method==='watchReplica')return {id:'source:'+input.path,signature:identity+':'+input.path} as T;
  if(input.method==='replicaPage')return {generation:{epoch,sequence:1},rows:[{id:'row',value:{id:'row',identity}}],window:{generation:'window',total:1,header:{}}} as T;
  if(input.method==='replicaPendingPage')return null as T;
  if(input.method==='unwatchReplica'||input.method==='auth')return null as T;
  if(input.method==='reducer')return {commandId:requestID,result:2} as T;
  if(input.method==='query')return {identity} as T;
  throw Error('Unexpected mock RPC '+input.method);
 },subscribeChanges(listener:(...args:any[])=>void){changes.add(listener);return ()=>{changes.delete(listener);};},stats(){return {ownerEpoch:epoch} as ReturnType<SQLiteReplicaBroker['stats']>;},close(){closed=true;}};
 const client=new SQLiteBrowserClient('ws://private-synthetic.test/ws',{broker,auth:{project:'project',tenant:'tenant',identity:{sub:'a'}},fetchToken});cleanup.push(()=>client.close());
 return {client,calls,responses,get closed(){return closed;},set handler(next:typeof handler){handler=next;},admit(key:string|null){admissionKey=key;for(const notify of changes)notify('*',1,true);},preview(value:any){for(const notify of changes)notify('@preview',1,false,value);},control(){for(const notify of changes)notify('@control',1);},change(scope='scope'){for(const notify of changes)notify(scope,1);},replaceOwner(){epoch++;connection='';tokenPending=false;for(const notify of changes)notify('*',1,true);}};
}
it('services a single selected-tab token request, shares a watch, retains a bounded stable page snapshot and restores after owner change',async()=>{
 const fetchToken=vi.fn(async()=> 'opaque'),f=fixture(fetchToken),one=f.client.watchReplica({path:'tasks/all'}),two=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>{one.close();two.close();});
 const page=await one.page({limit:10});await vi.waitFor(()=>expect(f.responses).toHaveLength(1));expect(fetchToken).toHaveBeenCalledTimes(1);expect(one.snapshot()).toBe(one.snapshot());expect(one.snapshot().page).toBe(page);expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1);
 f.change();expect(one.snapshot().stale).toBe(true);expect(one.snapshot().page).toBe(page);await vi.waitFor(async()=>{await f.client.pollControls();expect(f.responses).toHaveLength(1);});
 f.replaceOwner();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='connect')).toHaveLength(2));await two.page();expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(2);one.close();expect(two.snapshot().page).toBeDefined();two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));expect(f.client.budgets().find(value=>value.name==='browser-row-pages')?.bytes).toBe(0);
});
it('fences stale page and credential results across an identity change and releases held providers on close',async()=>{
 let release!: (value:string)=>void;const held=new Promise<string>(resolve=>{release=resolve;}),fetch=vi.fn().mockImplementationOnce(()=>held).mockResolvedValue('new-token'),f=fixture(fetch),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());await watch.page();await vi.waitFor(()=>expect(fetch).toHaveBeenCalledTimes(1));
 await f.client.setAuth({identity:{sub:'b'}});expect(watch.snapshot().page).toBeUndefined();release('old-token');await vi.waitFor(()=>expect(f.responses).toHaveLength(1));expect(f.responses[0]).toMatchObject({identityKey:'b',token:'new-token'});expect((await watch.page())?.rows[0].value).toMatchObject({identity:'b'});
 let finish!:()=>void;const barrier=new Promise<void>(resolve=>{finish=resolve;});f.handler=async input=>{if(input.method==='query'){await barrier;return {identity:'b'};}};const result=f.client.query({path:'oneShot'});void result.catch(()=>{});await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='query')).toBe(true));await f.client.setAuth({identity:{sub:'c'}});finish();await expect(result).rejects.toMatchObject({code:'FENCED'});f.client.close();expect(f.closed).toBe(true);expect(watch.snapshot().page).toBeUndefined();
});
it('serializes last-watch cancellation with a new lease and preserves pinned pages when admission fails',async()=>{
 const f=fixture(),first=f.client.watchReplica({path:'tasks/all'});await first.page();let release!:()=>void;const waiting=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='unwatchReplica'){await waiting;return null;}};first.close();await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='unwatchReplica')).toBe(true));const replacement=f.client.watchReplica({path:'tasks/all'}),pending=replacement.page();cleanup.push(()=>replacement.close());await new Promise(resolve=>setTimeout(resolve,0));expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1);release();await pending;expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(2);expect(replacement.snapshot().page).toBeDefined();
 const before=replacement.snapshot().page;f.handler=async input=>input.method==='replicaPage'?{rows:[{id:'giant',value:{text:'x'.repeat(150000)}}]}:undefined;await expect(replacement.page()).rejects.toMatchObject({code:'BUDGET'});expect(replacement.snapshot().page).toBe(before);expect(f.client.budgets().every(value=>value.bytes<=value.maxBytes)).toBe(true);
});
it('bounds requests before waiting for a connection and preserves the caller command ID for immutable retries',async()=>{
 const f=fixture();await f.client.connect();const args={id:'a'};const result=await f.client.invokeReducer({path:'tasks/change'},args,{commandId:'stable-command',now:100});expect(result.commandId).toBe('stable-command');expect(f.calls.find(call=>call.input.method==='reducer')?.requestID).toBe('stable-command');expect(()=>f.client.invokeReducer({path:'tasks/change'},{},{commandId:'stable-command'})).toThrow('original invocation time');
 let release!:()=>void;const waiting=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='query'){await waiting;return null;}};const pending=Array.from({length:128},()=>f.client.query({path:'bounded'}));const queued=f.client.query({path:'queued'});await vi.waitFor(()=>expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.entries).toBe(129));release();await Promise.all([...pending,queued]);expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
});

it('cancels a token provider which never settles when the facade closes',async()=>{
 const provider=vi.fn(()=>new Promise<string|null>(()=>{})),f=fixture(provider);await f.client.connect();await vi.waitFor(()=>expect(provider).toHaveBeenCalledOnce());expect(f.client.budgets().find(value=>value.name==='browser-token-requests')?.entries).toBe(1);f.client.close();await new Promise(resolve=>setTimeout(resolve,0));expect(f.client.budgets().find(value=>value.name==='browser-token-requests')?.entries).toBe(0);expect(f.responses).toEqual([]);
});

it('clears protected pages on worker admission changes and fences a late page without reconnecting the logical client',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());f.admit('old-permissions');await f.client.pollControls();const original=await watch.page();expect(watch.snapshot().page).toBe(original);
 const connections=f.calls.filter(call=>call.input.method==='connect').length;
 let release!:()=>void;const held=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='replicaPage'){await held;return {...original,rows:[{id:'old',value:{id:'old',protected:true}}]};}};
 const pending=watch.page();const rejected=expect(pending).rejects.toMatchObject({code:'FENCED'});await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(2));
 f.admit('new-permissions');await f.client.pollControls();expect(watch.snapshot().page).toBeUndefined();release();await rejected;expect(watch.snapshot().page).toBeUndefined();expect(f.calls.filter(call=>call.input.method==='connect')).toHaveLength(connections);
 f.handler=undefined;await watch.page();expect(watch.snapshot().page).toBeDefined();f.admit(null);await f.client.pollControls();expect(watch.snapshot().page).toBeUndefined();
});

it('delivers correlated auth errors and developer credentials once, ACKs the secret, and retains no token in status',async()=>{
 const f=fixture(),errors:unknown[]=[],credentials:string[]=[];f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:true},session:'authenticated',failure:null,authError:{id:'error',message:'Denied',definitive:true,code:'revoked'},developerCredential:{id:'credential',token:'gvx_dev_synthetic'}};if(input.method==='credentialAck')return null;};
 const stopErrors=f.client.onAuthError((message,details)=>errors.push({message,...details})),stopCredentials=f.client.onDeveloperCredential(token=>credentials.push(token));await f.client.pollControls();await f.client.pollControls();expect(errors).toEqual([{message:'Denied',definitive:true,code:'revoked'}]);expect(credentials).toEqual(['gvx_dev_synthetic']);expect(f.calls.some(call=>call.input.method==='credentialAck'&&call.input.id==='credential')).toBe(true);expect(JSON.stringify(f.client.status())).not.toContain('gvx_dev');stopErrors();stopCredentials();
});
it('updates the main-thread token provider without cloning a function to the owner',async()=>{
 const old=vi.fn(async()=> 'old'),next=vi.fn(async()=> 'next'),f=fixture(old);await f.client.connect();await vi.waitFor(()=>expect(f.responses).toHaveLength(1));await f.client.setAuth({fetchToken:next});f.replaceOwner();await vi.waitFor(()=>expect(f.responses).toHaveLength(2));expect(next).toHaveBeenCalledOnce();expect(f.responses.at(-1).token).toBe('next');expect(f.calls.every(call=>!('fetchToken'in(call.input.auth??{})))).toBe(true);
});

it('retries a control poll superseded by the initial identity admission',async()=>{
 const f=fixture();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});let held=false;
 f.handler=async input=>{if(input.method==='controls'&&!held){held=true;await gate;return {tokens:[],status:{},session:'unavailable',failure:null};}};
 await f.client.connect();await vi.waitFor(()=>expect(held).toBe(true));
 const admission=f.client.setAuth({identity:{sub:'b'}});release();await admission;
 await expect(f.client.pollControls()).resolves.toBeUndefined();
 expect(f.calls.filter(call=>call.input.method==='controls').length).toBeGreaterThan(1);
});

it('waits for correlated authoritative authentication, rejects superseded waiters and cancels on close',async()=>{
 const f=fixture();let authentication='authenticating';f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:true},authentication,session:'unavailable',failure:null};};const first=f.client.authenticate({token:'first'});void first.catch(()=>{});await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='auth')).toBe(true));const second=f.client.authenticate({token:'second'});void second.catch(()=>{});await expect(first).rejects.toMatchObject({code:'superseded'});authentication='authenticated';f.change();await expect(second).resolves.toBeUndefined();expect(f.client.localSessionState()).toBe('unavailable');
 authentication='authenticating';const closing=f.client.authenticate({token:'third'});void closing.catch(()=>{});await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='auth')).toHaveLength(3));f.client.close();await expect(closing).rejects.toMatchObject({code:'closed'});
});

it('keeps reducer receipts internal while returning the public local result or queued outcome',async()=>{
 const f=fixture(),ref={kind:'reducer' as const,path:'tasks/change'};expect(await f.client.reducer(ref,{id:'a'})).toBe(2);f.handler=async input=>{if(input.method==='reducer')return {commandId:'queued-command',result:null,queuedOnly:true};};expect(await f.client.reducer(ref,{})).toEqual({status:'queued',reducerId:'queued-command'});
});

it('preserves an already displayed first page while its pending stage becomes canonical',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());const first=await watch.page();expect(await watch.pendingPage()).toBeNull();expect(watch.snapshot().page).toBe(first);
});

it('keeps separately filtered view pages pinned independently while sharing the owner source through distinct leases',async()=>{
 const f=fixture();f.handler=async input=>{if(input.method==='collectionPage')return {rows:[{id:'a',n:input.plan.where.value}],sequence:1,bytes:32,cursor:null,generation:'window'};};const one=f.client.watchReplica({path:'tasks/all'},{},{view:'grid'}),two=f.client.watchReplica({path:'tasks/all'},{},{view:'board'});cleanup.push(()=>{one.close();two.close();});const plan={order:[{field:'n',direction:'asc' as const}],limit:10};const first=await one.queryPage({...plan,where:{field:'n',op:'gte',value:1}}),second=await two.queryPage({...plan,where:{field:'n',op:'gte',value:2}});expect(one.snapshot().page).toBe(first);expect(two.snapshot().page).toBe(second);const opens=f.calls.filter(call=>call.input.method==='watchReplica');expect(opens).toHaveLength(1);one.close();await Promise.resolve();expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(0);expect(two.snapshot().page).toBe(second);two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));
});

it('binds queued reducer RPCs to the current opaque worker admission key',async()=>{
 const f=fixture();f.admit('current-admission');await f.client.pollControls();await f.client.invokeReducer({path:'tasks/change'},{id:'a'});expect(f.calls.find(call=>call.input.method==='reducer')?.input.expectedAdmissionKey).toBe('current-admission');
});

it('delivers each authoritative rejection once and acknowledges only delivered notifications',async()=>{
 const f=fixture(),events:unknown[]=[];const event={sequence:1,reducerId:'command',path:'tasks/change',error:'denied',errorClass:'rejected'};f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{},session:'authenticated',failure:null,rejections:[event]};if(input.method==='rejectionsAck')return null;};await f.client.connect();await f.client.pollControls();expect(f.calls.some(call=>call.input.method==='rejectionsAck')).toBe(false);
 const release=f.client.onReducerRejection(value=>{events.push(value);throw Error('observer failure');});await f.client.pollControls();await f.client.pollControls();expect(events).toEqual([event]);expect(f.calls.filter(call=>call.input.method==='rejectionsAck').every(call=>call.input.sequence===1)).toBe(true);release();
});
it('delivers support commands once per tab after a listener mounts and reads the durable outbox count',async()=>{
 const f=fixture(),commands:unknown[]=[];const command={sequence:1,id:'support-1',kind:'inspect',payload:{task:'synthetic'}};
 f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{},session:'authenticated',failure:null,supportCommands:[command]};if(input.method==='supportAck')return null;if(input.method==='outboxCount')return 2;};
 await f.client.connect();await f.client.pollControls();expect(f.calls.some(call=>call.input.method==='supportAck')).toBe(false);
 const release=f.client.onSupportCommand(value=>commands.push(value));await f.client.pollControls();await f.client.pollControls();
 expect(commands).toEqual([{id:'support-1',kind:'inspect',payload:{task:'synthetic'}}]);
 expect(f.calls.filter(call=>call.input.method==='supportAck').every(call=>call.input.sequence===1)).toBe(true);
 expect(await f.client.outboxCount()).toBe(2);release();
});

it('creates inert bounded React watches and releases their page/source with the last subscription',async()=>{
 const f=fixture(),args={workspace:'original'},watch=f.client.watchReplica({path:'tasks/all'},args,{deferStart:true});args.workspace='mutated';const empty=watch.snapshot();expect(watch.snapshot()).toBe(empty);expect(f.calls).toEqual([]);expect(f.client.budgets().find(value=>value.name==='browser-watch-metadata')?.entries).toBe(0);
 const stop=watch.subscribe(()=>{});await watch.page();expect(f.calls.find(call=>call.input.method==='watchReplica')?.input.args).toEqual({workspace:'original'});stop();expect(watch.snapshot()).toBe(empty);await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));expect(f.client.budgets().find(value=>value.name==='browser-row-pages')?.bytes).toBe(0);const again=watch.subscribe(()=>{});await watch.page();again();watch.close();
});
it('presents only a bounded compatibility collection page and marks larger sources incomplete',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'},{},{deferStart:true}),updates:unknown[]=[];
 f.handler=async input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:2},rows:[{id:'a',value:{id:'a'}}],nextOffset:1,window:{generation:'one',total:100000,header:{source:'server',completeness:'complete'}}}:undefined;
 const stop=watch.onUpdate(()=>updates.push(watch.localReplicaState()));
 await vi.waitFor(()=>expect(watch.localReplicaState()?.rows).toEqual([{id:'a'}]));
 expect(watch.localReplicaState()).toMatchObject({total:100000,truncated:true,completeness:'partial',isUpToDate:false});
 expect(f.calls.find(call=>call.input.method==='replicaPage')?.input.limit).toBe(128);
 stop();watch.close();expect(updates.length).toBeGreaterThan(0);
});
it('routes separately leased live views through bounded canonical and pending window pages',async()=>{
 const f=fixture(),ref={kind:'query',path:'tasks/grid',delivery:'live' as const,live:{entity:'tasks',key:'id',plan:{} as any}};f.handler=async input=>{if(input.method==='watchLive')return {signature:'live'};if(input.method==='livePage')return {generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a'}}]};if(input.method==='livePendingPage')return {generation:'pending',complete:false,serverVerified:false,rows:[{id:'a',value:{id:'a'}}]};if(input.method==='unwatchLive')return null;};const one=f.client.watchLiveWindow(ref,{},{view:'grid'}),two=f.client.watchLiveWindow(ref,{},{view:'preview'});const pending=await one.pendingPage({limit:10});expect(pending?.complete).toBe(false);await two.page({limit:10});expect(one.snapshot().page).toBe(pending);expect(f.calls.filter(call=>call.input.method==='watchLive')).toHaveLength(1);expect(f.calls.some(call=>call.input.method==='watchReplica')).toBe(false);one.close();two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchLive')).toHaveLength(1));
});
it('rebuilds a structured live result from two fenced bounded pages',async()=>{
 const f=fixture(),ref={kind:'query',path:'tasks/grid',delivery:'live' as const,live:{entity:'tasks',key:'id',resultPath:['page'],plan:{} as any}};
 f.handler=async input=>{if(input.method==='watchLive')return {signature:'live'};if(input.method==='livePage')return {generation:{epoch:1,sequence:1},window:{generation:'window',total:129,header:{resultSkeleton:{page:[],total:100000,limit:250},resultPath:['page']}},offset:input.offset??0,nextOffset:input.offset?null:128,rows:input.offset?[{id:'tail',value:{id:'tail'}}]:Array.from({length:128},(_,i)=>({id:String(i),value:{id:String(i)}}))};if(input.method==='unwatchLive')return null;};
 const watch=f.client.watchLiveQuery<{page:Array<{id:string}>;total:number}>(ref,{}, {deferStart:true});const stop=watch.onUpdate(()=>{});
 await vi.waitFor(()=>expect(watch.localLiveQueryResult()?.page).toHaveLength(129));
 expect(watch.localLiveQueryResult()?.total).toBe(100000);expect(watch.localLiveQueryResult()?.page[0]).toEqual({id:'0'});expect(watch.localLiveQueryResult()?.page.at(-1)).toEqual({id:'tail'});
 expect(f.calls.filter(call=>call.input.method==='livePage').map(call=>call.input.limit)).toEqual([128,128]);
 stop();watch.close();
});

it('admits a normal metadata-heavy route beyond 64 watches within the same byte ceilings and releases every watch',async()=>{
 const f=fixture(),watches=Array.from({length:100},(_,n)=>f.client.watchReplica({path:'metadata/table'+n}));
 await Promise.all(watches.map(watch=>watch.page()));
 expect(f.client.memoryCounters().replicaWindows).toBe(100);
 expect(f.client.budgets().find(value=>value.name==='browser-row-pages')).toMatchObject({maxBytes:2097152});
 expect(f.client.budgets().find(value=>value.name==='browser-watch-metadata')).toMatchObject({maxBytes:524288});
 for(const watch of watches)watch.close();await vi.waitFor(()=>expect(f.client.memoryCounters().replicaWindows).toBe(0));
 expect(f.client.budgets().find(value=>value.name==='browser-row-pages')?.bytes).toBe(0);
});

it('retains only canonical live IDs without opening an extra transport query and handles numeric display IDs',async()=>{
 const f=fixture(),ref={kind:'query' as const,path:'tasks/window',delivery:'live' as const,live:{entity:'tasks',key:'_id',resultPath:['page']}};
 const signature=f.client.replicaSignature(ref,{limit:250}),release=f.client.retainReplicaWindow(signature),notify=vi.fn(),stop=f.client.subscribeReplicaWindow(signature,notify);
 expect(f.client.replicaWindowMembership(signature)).toBeUndefined();expect(f.calls.some(call=>call.input.method==='watchLive')).toBe(false);
 f.handler=async input=>{if(input.method==='watchLive')return {signature:'server'};if(input.method==='livePage')return {generation:{epoch:1,sequence:1},window:{generation:'window',total:1,header:{resultSkeleton:{page:[],total:10000,offset:0,limit:250},resultPath:['page']}},offset:0,nextOffset:null,rows:[{id:'canonical',value:{_id:'canonical',id:42,name:'Visible'}}]};if(input.method==='unwatchLive')return null;};
 const watch=f.client.watchLiveQuery(ref,{limit:250}),unwatch=watch.onUpdate(()=>{});
 await vi.waitFor(()=>expect(watch.localLiveQueryResult()).toEqual({page:[{_id:'canonical',id:42,name:'Visible'}],total:10000,offset:0,limit:250}));
 expect(f.client.replicaWindowMembership(signature)).toMatchObject({entity:'tasks',ids:['canonical'],total:10000,limit:250});
 expect(f.calls.filter(call=>call.input.method==='watchLive')).toHaveLength(1);expect(notify).toHaveBeenCalled();
 unwatch();watch.close();stop();release();expect(f.client.budgets().find(value=>value.name==='browser-live-memberships')?.bytes).toBe(0);
});

it('services control hints without invalidating every mounted data page',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());
 const page=await watch.page();const notify=vi.fn(),stop=watch.subscribe(notify);
 f.control();await f.client.pollControls();
 expect(watch.snapshot().stale).toBe(false);expect(watch.snapshot().page).toBe(page);expect(notify).not.toHaveBeenCalled();
 f.change();expect(watch.snapshot().stale).toBe(true);expect(notify).toHaveBeenCalledOnce();stop();
});

it('does not spin the microtask queue after a replica read is fenced or the client closes',async()=>{
 const f=fixture();let reads=0;f.handler=input=>{if(input.method==='replicaPage'){reads++;throw Object.assign(Error('Admission unavailable'),{code:'FENCED'});}};
 const watch=f.client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(vi.fn());
 await new Promise(resolve=>setTimeout(resolve,50));expect(reads).toBe(1);
 f.change();await vi.waitFor(()=>expect(reads).toBe(2));
 f.client.close();await new Promise(resolve=>setTimeout(resolve,50));expect(reads).toBe(2);stop();watch.close();
});

it('retains a verified replica projection and waits for a feed hint when its next read crosses a cohort',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(vi.fn());cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaState()).toBeDefined());const before=watch.localReplicaState();let reads=0;
 f.handler=input=>{if(input.method==='replicaPage'){reads++;throw Object.assign(Error('Cohort not yet committed'),{code:'PAGE_CHANGED'});}};
 f.change();await vi.waitFor(()=>expect(reads).toBe(1));await new Promise(resolve=>setTimeout(resolve,75));
 expect(reads).toBe(1);expect(watch.localReplicaState()).toBe(before);
 f.handler=undefined;f.change();await vi.waitFor(()=>expect(watch.localReplicaState()).not.toBe(before));
});

it('coalesces identical mounted page reads and lets one lease close without canceling another',async()=>{
 const f=fixture(),one=f.client.watchReplica({path:'tasks/all'}),two=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>{one.close();two.close();});
 let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='replicaPage'){await gate;return {generation:{epoch:1,sequence:1},rows:[],window:{header:{}}};}};
 const first=one.page({limit:128}),second=two.page({limit:128}),rejected=expect(first).rejects.toMatchObject({code:'FENCED'});
 await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(1));one.close();release();await rejected;
 await expect(second).resolves.toMatchObject({rows:[]});expect(two.snapshot().page).toBeDefined();
 expect(f.client.budgets().find(value=>value.name==='browser-shared-reads')).toMatchObject({entries:0,bytes:0});
});

it('captures a page request before asynchronous source opening',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());
 const options={limit:10},pending=watch.page(options);options.limit=100;await pending;
 expect(f.calls.find(call=>call.input.method==='replicaPage')?.input.limit).toBe(10);
});

it('refreshes pending display pages while coalesced staging hints preserve complete cached pages',async()=>{
 const f=fixture(),one=f.client.watchReplica({path:'tasks/all'}),two=f.client.watchReplica({path:'statuses/all'});cleanup.push(()=>{one.close();two.close();});
 f.handler=input=>input.method==='replicaPage'&&input.signature==='a:tasks/all'?{complete:false,serverVerified:false,rows:[]}:undefined;
 const first=await one.page(),second=await two.page(),oneNotify=vi.fn(),twoNotify=vi.fn(),stopOne=one.subscribe(oneNotify),stopTwo=two.subscribe(twoNotify);
 f.change('@pending');expect(one.snapshot().stale).toBe(true);expect(oneNotify).toHaveBeenCalledOnce();
 expect(two.snapshot().stale).toBe(false);expect(two.snapshot().page).toBe(second);expect(one.snapshot().page).toBe(first);expect(twoNotify).not.toHaveBeenCalled();
 f.change();expect(two.snapshot().stale).toBe(true);expect(twoNotify).toHaveBeenCalledOnce();stopOne();stopTwo();
});

it('waits for canonical publication after a null canonical page while explicit pending readers receive progress',async()=>{
 const f=fixture(),canonical=f.client.watchReplica({path:'tasks/all'}),pending=f.client.watchReplica({path:'statuses/all'});
 cleanup.push(()=>{canonical.close();pending.close();});
 f.handler=input=>['replicaPage','replicaPendingPage'].includes(input.method)?null:undefined;
 await canonical.page();await pending.pendingPage();
 const canonicalNotify=vi.fn(),pendingNotify=vi.fn(),stopCanonical=canonical.subscribe(canonicalNotify),stopPending=pending.subscribe(pendingNotify);
 f.change('@pending');expect(canonicalNotify).not.toHaveBeenCalled();expect(pendingNotify).toHaveBeenCalledOnce();
 f.change();expect(canonicalNotify).toHaveBeenCalledOnce();
 stopCanonical();stopPending();
});

it('keeps connection snapshots stable across unchanged controls and publishes genuine session or admission changes',async()=>{
 const f=fixture();let connected=true,session='authenticated',serverVersion=1;
 f.handler=async input=>input.method==='controls'?{tokens:[],status:{isWebSocketConnected:connected,hasEverConnected:true,connectionCount:1,connectionRetries:0,hasInflightRequests:false,inflightReducers:0,inflightActions:0,inflightOneShotQueries:0},session,failure:null,serverInfo:{version:String(serverVersion)}}:undefined;
 await f.client.connect();await f.client.pollControls();await new Promise(resolve=>setTimeout(resolve,0));
 const original=f.client.connectionState(),seen:unknown[]=[],sessions:string[]=[],scopes:unknown[]=[];
 const releases=[f.client.subscribeToConnectionState(value=>seen.push(value)),f.client.onLocalSessionChange(()=>sessions.push(f.client.localSessionState())),f.client.onSessionScopeChange(()=>scopes.push('scope'))];
 await new Promise(resolve=>setTimeout(resolve,0));seen.length=0;sessions.length=0;
 for(let n=0;n<10;n++)await f.client.pollControls();
 expect(f.client.connectionState()).toBe(original);expect(seen).toEqual([]);expect(sessions).toEqual([]);
 serverVersion++;await f.client.pollControls();expect(f.client.connectionState()).toBe(original);expect(f.client.serverInfo()).toMatchObject({version:'2'});
 connected=false;session='restored';await f.client.pollControls();expect(f.client.connectionState()).not.toBe(original);expect(f.client.connectionState().isWebSocketConnected).toBe(false);expect(sessions.at(-1)).toBe('restored');
 f.handler=async input=>input.method==='controls'?{tokens:[],status:f.client.connectionState(),session,failure:null,serverInfo:{version:'2'},admissionKey:'first'}:undefined;await f.client.pollControls();
 f.handler=async input=>input.method==='controls'?{tokens:[],status:f.client.connectionState(),session,failure:null,serverInfo:{version:'2'},admissionKey:'changed'}:undefined;await f.client.pollControls();expect(scopes).toHaveLength(2);
 for(const release of releases)release();
});

it('publishes a cached-session control hint queued after the final successful poll',async()=>{
 const f=fixture();let session='authenticated';
 f.handler=input=>input.method==='controls'?{tokens:[],status:{},session,failure:null}:undefined;
 await f.client.connect();await f.client.pollControls();await new Promise(resolve=>setTimeout(resolve,0));
 let queued=false;const seen:string[]=[];
 const release=f.client.onLocalSessionChange(()=>{
  seen.push(f.client.localSessionState());
  if(session==='unavailable'&&!queued){queued=true;queueMicrotask(()=>{session='restored';f.control();});}
 });cleanup.push(release);
 session='unavailable';await f.client.pollControls();
 await vi.waitFor(()=>expect(seen).toEqual(['unavailable','restored']));
 expect(f.client.localSessionState()).toBe('restored');
 const reads=f.calls.filter(call=>call.input.method==='controls').length;
 await new Promise(resolve=>setTimeout(resolve,30));
 expect(f.calls.filter(call=>call.input.method==='controls')).toHaveLength(reads);
});

it('keeps live membership IDs while a mounted entity read is pending and publishes its late row',async()=>{
 const f=fixture(),ref={kind:'query' as const,path:'tasks/window',delivery:'live' as const,live:{entity:'tasks',key:'_id',resultPath:['page']}};
 const row={_id:'canonical',id:42,name:'Cached task'};
 let finish!:()=>void;const held=new Promise<void>(resolve=>{finish=resolve;});
 f.handler=async input=>{
  if(input.method==='watchLive')return {signature:'server'};
  if(input.method==='livePage')return {generation:{epoch:1,sequence:1},window:{generation:'window',total:1,header:{resultSkeleton:{page:[],total:1},resultPath:['page']}},offset:0,nextOffset:null,rows:[{id:'canonical',value:row}]};
  if(input.method==='entities'){await held;return {generation:{epoch:1,sequence:1},rows:[{id:'canonical',value:row}]};}
  if(input.method==='unwatchLive')return null;
 };
 const signature=f.client.replicaSignature(ref,{}),releaseMembership=f.client.retainReplicaWindow(signature);
 const changed=vi.fn(),stopEntity=f.client.localReplica.subscribe(changed,{entity:'tasks',ids:['canonical']}),releaseEntity=f.client.retainReplicaEntities('tasks',['canonical']);
 const watch=f.client.watchLiveQuery(ref,{}),stopWatch=watch.onUpdate(()=>{});
 cleanup.push(()=>{finish();stopWatch();watch.close();stopEntity();releaseEntity();releaseMembership();});
 await vi.waitFor(()=>expect(f.client.replicaWindowMembership(signature)?.ids).toEqual(['canonical']));
 const membership=f.client.replicaWindowMembership(signature);
 expect(f.client.replicaEntities('tasks',['canonical'])).toEqual([undefined]);
 expect(watch.localLiveQueryResult()).toEqual({page:[row],total:1});
 changed.mockClear();finish();
 await vi.waitFor(()=>expect(f.client.replicaEntities('tasks',['canonical'])).toEqual([row]));
 expect(changed).toHaveBeenCalled();
 expect(f.client.replicaWindowMembership(signature)).toBe(membership);
 expect(f.calls.filter(call=>call.input.method==='watchLive')).toHaveLength(1);
});

it('keeps a certified collection after a transient fenced refresh but clears it on revoked admission',async()=>{
 const f=fixture();
 f.handler=async input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a',name:'Certified'}}],window:{generation:'window',total:1,header:{completeness:'complete',source:'server'}},nextOffset:null}:undefined;
 const watch=f.client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(()=>{});
 cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[{id:'a',name:'Certified'}],completeness:'complete'}));
 const original=watch.localReplicaState();
 let fenced=false;
 f.handler=async input=>{if(input.method==='replicaPage'){fenced=true;throw Object.assign(Error('Superseded cached read'),{code:'FENCED'});}};
 f.change();await vi.waitFor(()=>expect(fenced).toBe(true));await new Promise(resolve=>setTimeout(resolve,0));
 expect(watch.localReplicaState()).toBe(original);
 f.admit(null);await f.client.pollControls();
 expect(watch.localReplicaState()).toBeUndefined();
});

it.each([{completeness:'partial',truncated:false},{completeness:'complete',truncated:true}])('does not retain uncertified coverage after a fenced refresh: %j',async header=>{
 const f=fixture();
 f.handler=async input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a'}}],window:{generation:'window',total:1,header:{...header,source:'server'}},nextOffset:null}:undefined;
 const watch=f.client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(()=>{});
 cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaState()?.rows).toEqual([{id:'a'}]));
 f.handler=async input=>{if(input.method==='replicaPage')throw Object.assign(Error('Superseded read'),{code:'FENCED'});};
 f.change();await vi.waitFor(()=>expect(watch.localReplicaState()).toBeUndefined());
});

it('paces mounted reducer code preparation while cached queries remain usable',async()=>{
 const f=fixture();await f.client.connect();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
 f.handler=async input=>{if(input.method==='prepareReducer'){await gate;return null;}};
 const first=f.client.prepareReducer({path:'background/one'}),second=f.client.prepareReducer({path:'background/two'});
 try{
  await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='prepareReducer')).toBe(true));
  await expect(f.client.query({path:'visible/query'})).resolves.toEqual({identity:'a'});
  await expect(f.client.invokeReducer({path:'visible/edit'},{})).resolves.toMatchObject({result:2});
  expect(f.calls.filter(call=>call.input.method==='prepareReducer')).toHaveLength(1);
 }finally{release();await Promise.all([first,second]);}
 expect(f.calls.filter(call=>call.input.method==='prepareReducer').map(call=>call.input.path)).toEqual(['background/one','background/two']);
 expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
});
it('coalesces concurrent preparation of the same reducer without retaining a failed promise',async()=>{
 const f=fixture();await f.client.connect();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});let fail=true;
 f.handler=async input=>{if(input.method==='prepareReducer'){await gate;if(fail)throw Error('Synthetic missing offline chunk');return null;}};
 const pending=Array.from({length:32},()=>f.client.prepareReducer({path:'tasks/edit'})),settled=Promise.allSettled(pending);
 await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='prepareReducer')).toBe(true));release();
 expect((await settled).every(value=>value.status==='rejected')).toBe(true);expect(f.calls.filter(call=>call.input.method==='prepareReducer')).toHaveLength(1);
 fail=false;await f.client.prepareReducer({path:'tasks/edit'});expect(f.calls.filter(call=>call.input.method==='prepareReducer')).toHaveLength(2);
 expect(f.client.budgets().find(value=>value.name==='browser-shared-reads')?.bytes).toBe(0);expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
});
it('keeps foreground requests usable while the background preparation queue is full',async()=>{
 const f=fixture();await f.client.connect();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
 f.handler=async input=>{if(input.method==='prepareReducer'){await gate;return null;}};
 const pending=Array.from({length:256},(_,i)=>f.client.prepareReducer({path:'background/'+i})),settled=Promise.allSettled(pending);
 const visible=f.client.query({path:'visible/query'});void visible.catch(()=>{});
 try{
  await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='query')).toBe(true));await expect(visible).resolves.toEqual({identity:'a'});
  await expect(f.client.invokeReducer({path:'visible/edit'},{})).resolves.toMatchObject({result:2});
  expect(f.calls.filter(call=>call.input.method==='prepareReducer')).toHaveLength(1);
  expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.entries).toBe(256);
 }finally{release();await settled;await Promise.allSettled([visible]);}
});
it('does not share a pending reducer preparation across owner generations',async()=>{
 const f=fixture();await f.client.connect();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});let calls=0;
 f.handler=async input=>{if(input.method==='prepareReducer'){if(++calls===1)await gate;return null;}};
 const first=f.client.prepareReducer({path:'tasks/edit'});void first.catch(()=>{});await vi.waitFor(()=>expect(calls).toBe(1));
 f.replaceOwner();const second=f.client.prepareReducer({path:'tasks/edit'});release();
 await expect(first).rejects.toMatchObject({code:'FENCED'});await second;expect(calls).toBe(2);
 expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
});

it('bounds queued preparation with the existing shared budget and cancels queued code loads on close',async()=>{
 const f=fixture();await f.client.connect();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
 f.handler=async input=>{if(input.method==='prepareReducer'){await gate;return null;}};
 const pending=Array.from({length:256},(_,i)=>f.client.prepareReducer({path:'background/'+i}));const settled=Promise.allSettled(pending);
 await vi.waitFor(()=>expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.entries).toBe(256));
 await expect(f.client.prepareReducer({path:'overflow'})).rejects.toMatchObject({code:'BUDGET'});
 f.client.close();release();expect((await settled).every(result=>result.status==='rejected')).toBe(true);
 expect(f.calls.filter(call=>call.input.method==='prepareReducer')).toHaveLength(1);
 expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
});


it('reuses a fresh bounded collection page for another mounted consumer without another owner read',async()=>{
 const f=fixture();f.handler=input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:1},offset:0,nextOffset:null,rows:[{id:'row',value:{id:'row'}}],window:{generation:'one',total:1,header:{source:'server',completeness:'complete'}}}:undefined;
 const one=f.client.watchReplica({path:'tasks/all'}),stopOne=one.onUpdate(()=>{});cleanup.push(()=>{stopOne();one.close();});
 await vi.waitFor(()=>expect(one.localReplicaResult()).toEqual([{id:'row'}]));
 const before=f.calls.filter(call=>call.input.method==='replicaPage').length;
 const two=f.client.watchReplica({path:'tasks/all'}),stopTwo=two.onUpdate(()=>{});cleanup.push(()=>{stopTwo();two.close();});
 await vi.waitFor(()=>expect(two.localReplicaResult()).toEqual([{id:'row'}]));expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(before);
 // Explicit public page reads still ask the owner for a fenced result.
 await two.page({limit:128});expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(before+1);
});

it('shares a not-yet-published collection page until canonical invalidation admits rows',async()=>{
 const f=fixture();let ready=false;f.handler=input=>input.method==='replicaPage'?(ready?{generation:{epoch:1,sequence:2},offset:0,nextOffset:null,rows:[{id:'row',value:{id:'row'}}],window:{generation:'one',total:1,header:{source:'server',completeness:'complete'}}}:null):undefined;
 const one=f.client.watchReplica({path:'tasks/all'}),stopOne=one.onUpdate(()=>{});cleanup.push(()=>{stopOne();one.close();});await vi.waitFor(()=>expect(one.snapshot().page).toBeNull());
 const before=f.calls.filter(call=>call.input.method==='replicaPage').length;
 const two=f.client.watchReplica({path:'tasks/all'}),stopTwo=two.onUpdate(()=>{});cleanup.push(()=>{stopTwo();two.close();});await new Promise(resolve=>setTimeout(resolve,0));
 expect(two.localReplicaResult()).toBeUndefined();expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(before);
 ready=true;f.change();await vi.waitFor(()=>expect(two.localReplicaResult()).toEqual([{id:'row'}]));expect(one.localReplicaResult()).toEqual([{id:'row'}]);expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(before+1);
});

it('rereads when canonical invalidation crosses a collection read instead of treating its old reply as fresh',async()=>{
 const f=fixture();let release!:()=>void;const held=new Promise<void>(resolve=>{release=resolve;});let reads=0;
 f.handler=async input=>{if(input.method!=='replicaPage')return;const value=++reads;if(value===1)await held;return {generation:{epoch:1,sequence:value},offset:0,nextOffset:null,rows:[{id:'row',value:{id:'row',value}}],window:{generation:'one',total:1,header:{source:'server',completeness:'complete'}}};};
 const one=f.client.watchReplica({path:'tasks/all'}),stop=one.onUpdate(()=>{});cleanup.push(()=>{stop();one.close();});await vi.waitFor(()=>expect(reads).toBe(1));f.change();release();await vi.waitFor(()=>expect(one.localReplicaResult()).toEqual([{id:'row',value:2}]));expect(reads).toBe(2);
});


it('does not substitute a differently sized explicit page for a mounted collection read',async()=>{
 const f=fixture();f.handler=input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:1},offset:0,nextOffset:null,rows:[{id:'row',value:{id:'row'}}],window:{generation:'one',total:1,header:{source:'server',completeness:'complete'}}}:undefined;
 const one=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>one.close());await one.page({limit:1});
 const two=f.client.watchReplica({path:'tasks/all'}),stop=two.onUpdate(()=>{});cleanup.push(()=>{stop();two.close();});await vi.waitFor(()=>expect(two.localReplicaResult()).toEqual([{id:'row'}]));
 expect(f.calls.filter(call=>call.input.method==='replicaPage').map(call=>call.input.limit)).toEqual([1,128]);
});


it('does not count another page read as a canonical invalidation',async()=>{
 const f=fixture();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});let started=false;
 f.handler=async input=>{if(input.method!=='replicaPage')return;if(input.limit===128){started=true;await gate;}return {generation:{epoch:1,sequence:1},offset:0,nextOffset:null,rows:[{id:'row',value:{id:'row'}}],window:{generation:'one',total:1,header:{source:'server',completeness:'complete'}}};};
 const watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());const full=watch.page({limit:128});await vi.waitFor(()=>expect(started).toBe(true));await watch.page({limit:1});release();await full;expect(watch.snapshot().stale).toBe(false);
});

it('keeps every client within its existing global reservation and releases registrations on close',()=>{
 for(let n=0;n<100;n++){
  const f=fixture(),budgets=f.client.budgets();
  expect(budgets.reduce((sum,entry)=>sum+entry.maxBytes,0)).toBeLessThanOrEqual(12582912);
  expect(budgets.find(entry=>entry.name==='browser-last-visible-pages')?.maxBytes).toBe(524288);
  f.client.close();expect(f.client.budgets().every(entry=>entry.bytes===0&&entry.entries===0)).toBe(true);
  f.client.close();expect(f.client.budgets().every(entry=>entry.bytes===0&&entry.entries===0)).toBe(true);
 }
});

it('shares one source and one in-flight page across identical arguments while retaining separate view pages',async()=>{
 const f=fixture(),ref={path:'tasks/all'},one=f.client.watchReplica(ref,{},{view:'one'}),two=f.client.watchReplica(ref,{},{view:'two'});
 cleanup.push(()=>{one.close();two.close();});
 const [a,b]=await Promise.all([one.page({limit:128}),two.page({limit:128})]);
 expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1);
 expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(1);
 expect(a).toBe(b);expect(one.snapshot().page).toBe(a);expect(two.snapshot().page).toBe(b);
 f.replaceOwner();await two.page({limit:128});expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(2);
 one.close();await Promise.resolve();expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(0);
 await two.page({offset:1,limit:10});expect(two.snapshot().page).toBeDefined();
 two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));
});

it('keeps a shared opening source alive when its first view closes',async()=>{
 const f=fixture();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
 f.handler=async input=>{if(input.method==='watchReplica')await gate;};
 const one=f.client.watchReplica({path:'tasks/all'},{},{view:'one'}),two=f.client.watchReplica({path:'tasks/all'},{},{view:'two'});cleanup.push(()=>{one.close();two.close();});
 const first=one.page({limit:128}),second=two.page({limit:128});const rejected=expect(first).rejects.toMatchObject({code:'FENCED'});
 await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1));one.close();release();
 await rejected;expect((await second)?.rows).toHaveLength(1);expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1);expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(0);
 two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));
});

it('keeps distinct argument subscriptions separate',async()=>{
 const f=fixture(),one=f.client.watchReplica({path:'tasks/all'},{q:1}),two=f.client.watchReplica({path:'tasks/all'},{q:2});cleanup.push(()=>{one.close();two.close();});
 await Promise.all([one.page({limit:128}),two.page({limit:128})]);const opens=f.calls.filter(call=>call.input.method==='watchReplica');expect(opens).toHaveLength(2);expect(opens[0].input.lease).not.toBe(opens[1].input.lease);
 one.close();two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(2));
});

it('refreshes only pending display interests on a preview hint and retains committed page identity',async()=>{
 const f=fixture(async()=> 'opaque'),pending=f.client.watchReplica({path:'tasks/pending'}),committed=f.client.watchReplica({path:'tasks/complete'});
 cleanup.push(()=>{pending.close();committed.close();});
 await pending.pendingPage();const page=await committed.page();
 let pendingHints=0,committedHints=0;const stopPending=pending.subscribe(()=>pendingHints++),stopCommitted=committed.subscribe(()=>committedHints++);
 cleanup.push(()=>{stopPending();stopCommitted();});
 const controls=f.calls.filter(call=>call.input.method==='controls').length;
 f.change('@preview');
 expect(pendingHints).toBe(1);expect(committedHints).toBe(0);expect(committed.snapshot().page).toBe(page);expect(committed.snapshot().stale).toBe(false);
 expect(f.calls.filter(call=>call.input.method==='controls')).toHaveLength(controls);
});

it('publishes a fenced partial page while the canonical RPC is blocked and revokes it without accepting coverage',async()=>{
 const f=fixture(async()=> 'opaque');f.admit('admitted');
 let release!:(value:unknown)=>void;const blocked=new Promise(resolve=>{release=resolve;});
 f.handler=input=>input.method==='replicaPage'?blocked:undefined;
 const watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());const stop=watch.onUpdate(()=>{});cleanup.push(stop);
 await vi.waitFor(()=>expect(f.calls.some(c=>c.input.method==='replicaPage')).toBe(true));await f.client.pollControls();
 const page={source:'pending' as const,complete:false as const,serverVerified:false,generation:{epoch:1,sequence:0,predictionSequence:0,snapshot:'s',receivedPages:1},receivedRows:1,nextPosition:null,rows:[{id:'a',position:0,value:{id:'a',name:'Before FULL commit'}}]};
 const preview={id:'preview',signature:'a:tasks/all',admissionKey:'foreign',page};
 f.preview(preview);expect(watch.localReplicaState()).toBeUndefined();
 f.preview({...preview,admissionKey:'admitted'});
 expect(watch.localReplicaState()).toMatchObject({rows:[{name:'Before FULL commit'}],completeness:'partial',truncated:true,isUpToDate:false,freshness:'verifying'});
 f.preview({...preview,admissionKey:'admitted',id:'old',page:null});expect(watch.localReplicaState()?.rows).toHaveLength(1);
 f.preview({...preview,admissionKey:'admitted',page:null});expect(watch.localReplicaState()).toBeUndefined();
 release({generation:{epoch:1,sequence:2},rows:[{id:'a',value:{id:'a',name:'Committed'}}],nextOffset:null,window:{total:1,generation:'g',header:{source:'server',completeness:'complete',truncated:false}}});
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[{name:'Committed'}],completeness:'complete',isUpToDate:true}));
});


it('revokes complete empty collection status when owner admission fails and restores it only after recovery',async()=>{
 const f=fixture();let refused=false;
 f.handler=async input=>{
  if(input.method==='controls')return {tokens:[],status:{connected:true},session:'authenticated',failure:refused?{code:'BUDGET',message:'Native source header registry exceeded budget'}:null};
  if(input.method==='replicaPage')return {generation:{epoch:1,sequence:1},rows:[],offset:0,nextOffset:null,window:{generation:'empty',total:0,header:{completeness:'complete',truncated:false,source:'server'}}};
 };
 const watch=f.client.watchReplica({path:'workspaces.list'}),stop=watch.onUpdate(()=>{});cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[],completeness:'complete',isUpToDate:true}));
 refused=true;await f.client.pollControls();
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[],completeness:'partial',isUpToDate:false}));
 expect(f.client.replicaCollectionState({path:'workspaces.list'})).toMatchObject({completeness:'partial',isUpToDate:false});
 refused=false;await f.client.pollControls();
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[],completeness:'complete',isUpToDate:true}));
});


it('keeps certified offline rows usable after a transient source-close timeout without granting online freshness',async()=>{
 const f=fixture();let offline=false;
 f.handler=async input=>{
  if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:!offline,hasEverConnected:true},session:offline?'restored':'authenticated',failure:offline?{code:'PAGE_CHANGED',message:'Native source close acknowledgement timed out'}:null};
  if(input.method==='replicaPage')return {generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a',name:'Certified'}}],offset:0,nextOffset:null,window:{generation:'cached',total:1,header:{completeness:'complete',truncated:false,source:'server'}}};
 };
 const watch=f.client.watchReplica({path:'tasks.all'}),stop=watch.onUpdate(()=>{});cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[{id:'a',name:'Certified'}],completeness:'complete'}));
 offline=true;await f.client.pollControls();
 await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[{id:'a',name:'Certified'}],completeness:'complete',freshness:'offline'}));
});


it('retries a follower storage read timeout without clearing its mounted rows or surfacing a render error',async()=>{
 const f=fixture();let timeout=false,reads=0;
 f.handler=async input=>{
  if(input.method==='replicaPage'){
   reads++;if(timeout){timeout=false;throw Object.assign(Error('Storage read timed out; retry the read'),{code:'TIMEOUT',outcome:'rejected'});}
   return {generation:{epoch:1,sequence:reads},rows:[{id:'a',value:{id:'a',name:'Retained'}}],offset:0,nextOffset:null,window:{generation:'w',total:1,header:{completeness:'complete',truncated:false,source:'server'}}};
  }
 };
 const watch=f.client.watchReplica({path:'workspaces.list'}),stop=watch.onUpdate(()=>{});cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaResult()).toEqual([{id:'a',name:'Retained'}]));
 timeout=true;f.change();await vi.waitFor(()=>expect(reads).toBeGreaterThanOrEqual(2));
 expect(()=>watch.localReplicaResult()).not.toThrow();expect(watch.localReplicaResult()).toEqual([{id:'a',name:'Retained'}]);
 await vi.waitFor(()=>expect(reads).toBeGreaterThanOrEqual(3));expect(watch.localReplicaState()).toMatchObject({completeness:'complete',rows:[{id:'a',name:'Retained'}]});
});

it('does not retry an unknown reducer commit outcome as though it were a read',async()=>{
 const f=fixture();let calls=0;f.handler=async input=>{if(input.method==='reducer'){calls++;throw Object.assign(Error('unknown write'),{code:'TIMEOUT',outcome:'unknown'});}};
 await expect(f.client.invokeReducer({path:'tasks.edit'},{},{commandId:'original',now:100})).rejects.toMatchObject({code:'TIMEOUT',outcome:'unknown'});expect(calls).toBe(1);
});


it('does not resend a completed token response when a retried controls read repeats the request',async()=>{
 const f=fixture();await f.client.connect();await vi.waitFor(()=>expect(f.responses).toHaveLength(1));const completed=f.responses[0];let tries=0;
 f.handler=async input=>{if(input.method==='controls'){if(tries++===0)throw Object.assign(Error('read timeout'),{code:'TIMEOUT',outcome:'rejected'});return {tokens:[{id:completed.id,connection:completed.connection,identityKey:completed.identityKey,forceRefreshToken:false}],status:{connected:true},session:'authenticated',failure:null};}};
 await f.client.pollControls();await new Promise(resolve=>setTimeout(resolve,20));expect(tries).toBeGreaterThanOrEqual(2);expect(f.responses).toHaveLength(1);
});
it('surfaces refused replica admission instead of leaving its consumer silently loading',async()=>{
 const f=fixture();f.handler=async input=>{if(input.method==='watchReplica')throw Object.assign(Error('Native subscription allowance exceeded'),{code:'BUDGET'});};
 const watch=f.client.watchReplica({path:'taskResources.listTaskUsers'},{taskId:'task'}),stop=watch.onUpdate(()=>{});cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(()=>watch.localReplicaResult()).toThrow('Native subscription allowance exceeded'));
});

it('answers offline live search from SQLite predicted rows independently of a truncated renderer page',async()=>{
 const f=fixture();const ref:any={kind:'query',path:'tasks.search',delivery:'live',live:{entity:'tasks',key:'id',resultPath:['page'],plan:{table:'tasks',key:'id',columns:['id','name'],window:{defaultLimit:50,maxLimit:128,limitArgument:'limit',count:'exact'},sort:{defaultColumn:'id',defaultDirection:'asc',allowedColumns:['id']}}}};
 f.handler=async input=>{
  if(input.method==='watchLive')return {signature:'disk-search'};
  if(input.method==='localLivePage')return {supported:true,complete:true,generation:'disk',rows:[{id:'predicted',name:'offline-created'}],cursor:null,bytes:128,sequence:3};
  if(input.method==='livePage')throw Error('Offline search must not wait for a server result');
  if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:false},authentication:'unavailable',session:'restored',failure:null};
  if(input.method==='unwatchLive')return null;
 };
 const watch=f.client.watchLiveQuery<any>(ref,{search:'offline-created',limit:50});const stop=watch.onUpdate(()=>{});
 await vi.waitFor(()=>expect(watch.localLiveQueryResult()?.page).toEqual([{id:'predicted',name:'offline-created'}]));
 expect(f.calls.some(call=>call.input.method==='livePage')).toBe(false);
 expect(f.client.replicaWindowMembership(f.client.replicaSignature(ref,{search:'offline-created',limit:50}))?.source).toBe('cache');
 stop();watch.close();
});

it('starts a fresh boot without inherited admission or mounted refresh ownership',async()=>{
 const first=fixture();await first.client.connect();first.admit('same-scope-artifact-hash');await vi.waitFor(()=>expect((first.client as any).admissionKey).toBe('same-scope-artifact-hash'));
 first.client.close();const next=fixture();expect((next.client as any).admissionKey).toBeUndefined();
 expect(next.client.localReplica.readDiagnostics()).toEqual({refreshOwner:null,reads:[],retryScheduled:false,discarded:{counts:{},recent:[]}});
 expect(next.client.memoryCounters().replicaRows).toBe(0);
});

it('uses two-second local-read attempts and surfaces exhaustion after bounded retries',async()=>{
 const f=fixture();await f.client.connect();
 const request=vi.spyOn((f.client as any).options.broker,'request');
 f.handler=input=>{if(input.method==='entities')throw Object.assign(Error('Storage read timed out'),{code:'TIMEOUT',outcome:'rejected'});};
 await expect(f.client.entities('tasks',['missing'])).rejects.toMatchObject({code:'TIMEOUT',outcome:'rejected'});
 const reads=request.mock.calls.filter(call=>(call[1] as any)?.method==='entities');
 expect(reads).toHaveLength(3);expect(reads.every(call=>call[2]===2000)).toBe(true);
});

it('does not spend the authentication deadline while the transport is disconnected',async()=>{
 vi.useFakeTimers();const f=fixture();let online=false;
 f.handler=input=>{if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:online},session:'restored',authentication:online?'authenticated':'unavailable',failure:null};};
 try{const done=vi.fn();const auth=f.client.authenticate({project:'project',tenant:'tenant',identity:{sub:'a'}});void auth.then(done,done);
 await vi.advanceTimersByTimeAsync(31000);expect(done).not.toHaveBeenCalled();online=true;f.control();await vi.advanceTimersByTimeAsync(10);await auth;expect(done).toHaveBeenCalledOnce();
 }finally{f.client.close();vi.useRealTimers();}
});

it('shares identical in-flight entity batches and releases them after settlement',async()=>{
 const f=fixture();let resolve!:(value:any)=>void;const pending=new Promise(resolvePending=>{resolve=resolvePending;});
 f.handler=input=>input.method==='entities'?pending:undefined;
 const first=f.client.entities('tasks',['a']),second=f.client.entities('tasks',['a']);expect(second).toBe(first);
 await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='entities')).toHaveLength(1));
 resolve({generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a'}}]});await first;
 await f.client.entities('tasks',['a']);expect(f.calls.filter(call=>call.input.method==='entities')).toHaveLength(2);
});

it('attaches owner timing to diagnostics without exposing it in entity values',async()=>{
 const f=fixture(),owner={received:10,sqlStart:11,sqlEnd:12,reply:13};f.handler=input=>input.method==='entities'?{generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a'}}],_sqliteReadTiming:owner}:undefined;
 const result=await f.client.entities('tasks',['a']);expect(result).not.toHaveProperty('_sqliteReadTiming');expect(f.client.entityReadDiagnostics().at(-1)?.owner).toEqual(owner);
});

it('does not republish an unchanged empty page into a consumer read loop',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'empty'});cleanup.push(()=>watch.close());
 f.handler=input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:1},rows:[],window:{generation:'empty',total:0,header:{}}}:undefined;
 await watch.page({limit:128});const listener=vi.fn();watch.subscribe(listener);
 for(let n=0;n<20;n++)await watch.page({limit:128});
 expect(listener).not.toHaveBeenCalled();
 f.change();expect(listener).toHaveBeenCalledTimes(1);await watch.page({limit:128});
 expect(watch.snapshot().stale).toBe(false);expect(listener).toHaveBeenCalledTimes(2);
});
