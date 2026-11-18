// Actual DO SQLite, R2, release compiler and fenced executor. Controlled target
// gates expose scheduling races; these are not platform deployment acceptance.
import {evictDurableObject,runInDurableObject} from "cloudflare:test";
import {expect,it,vi} from "vitest";
import {MemoryVersionTarget,PRELUDE_SPECIFIER,ReleaseBuilderCore,type BlobStore} from "../../../control/operator/builder/core";
import {PublicationExecutor} from "../../../control/operator/builder/publication";
import {bumpRetirementGeneration,installDeploymentSchema,TARGET_ATTEMPT_LEASE_MS} from "../../../control/operator/deployment";
import {SyncCoordinator,type SyncOutcome,type SyncPorts} from "../../../control/operator/sync";
import {HttpError} from "../../../control/operator/http";
import type {DeploymentRef,ReleaseBuilder} from "../../../control/operator/release";
import type {OperatorControlObject} from "../../../control/operator/object";
import {handleDevRequest,type DevRouterEnv} from "../routes";
import {manifest,testEnv} from "./helpers";

const gate=()=>{let release!:()=>void;const promise=new Promise<void>(resolve=>{release=resolve;});return {promise,release};};
type Staged=Extract<SyncOutcome,{ok:true}> & {publication:string;fence:number;deploy:string};
const staged=(result:SyncOutcome):Staged=>{expect(result).toMatchObject({ok:true});if(!result.ok||!result.publication||!result.fence||!result.deploy)throw Error("not staged");return result as Staged;};
const ref=(outcome:Staged):DeploymentRef=>({id:outcome.publication,fence:outcome.fence,version:outcome.deploy});
async function fixture(run:(f:Awaited<ReturnType<typeof setup>>)=>Promise<void>){
 const object=testEnv.CONTROL.getByName('deployment-'+crypto.randomUUID());
 await runInDurableObject(object,async(instance:OperatorControlObject,state)=>{await run(await setup(instance,state));});
}
async function setup(instance:OperatorControlObject,state:DurableObjectState){
 let versionGate:ReturnType<typeof gate>|undefined;
 let live='',prepareGate:ReturnType<typeof gate>|undefined,stageGate:ReturnType<typeof gate>|undefined,stageEntered:ReturnType<typeof gate>|undefined,deployGate:ReturnType<typeof gate>|undefined,failDeploy=false,retainDeploy=false,failPrepare=false,failNotify=false,notifyStatus:number|undefined,notifyGate:ReturnType<typeof gate>|undefined;
 const events:string[]=[],target=new MemoryVersionTarget();
 const baseDeploy=target.deploy.bind(target);
 target.deploy=async(version)=>{events.push('deploy:'+version);await deployGate?.promise;if(failDeploy)throw Object.assign(Error('target failed'),{retainPublication:retainDeploy});await baseDeploy(version);live=version;};
 const core=()=>new ReleaseBuilderCore({blobs:testEnv.RELEASES as unknown as BlobStore,target,preludeSpecifier:PRELUDE_SPECIFIER});
 let builder=core();
 const wrapped:ReleaseBuilder={inspect:(...a)=>builder.inspect(...a),prepare:async(...a)=>{events.push('prepare:'+a[1]);await prepareGate?.promise;if(failPrepare)throw Error('prepare failed');return builder.prepare(...a);},activate:async(...a)=>{const held=stageGate,entered=stageEntered,result=await builder.activate(...a);entered?.release();await held?.promise;return result;},deploy:version=>builder.deploy(version)};
 const ports:SyncPorts={sql:instance.sql,builder:wrapped,enqueue:async()=>{throw Error('unexpected tenant rollout');},versionReady:async version=>{await versionGate?.promise;return live===version;},versionAbsent:async version=>live!==version,
  activateTenants:async()=>true,moduleReloaded:async()=>{await notifyGate?.promise;if(failNotify)throw notifyStatus?new HttpError(notifyStatus,'holder unavailable'):Error('holder unavailable');},syncCrons:async()=>{},deadlineMs:1000,devSyncKey:'synthetic',acceptsKey:(_p,key)=>key==='synthetic'};
 let coordinator=new SyncCoordinator(ports);
 const authority={authorize:async(r:DeploymentRef)=>coordinator.deployments.authorize(r),settle:async(r:DeploymentRef,outcome:{ok:boolean;error?:string})=>{await coordinator.deployments.settle(r,outcome);},abort:async(r:DeploymentRef,error:string)=>{await coordinator.deployments.abort(r,error);}};
 const executor=()=>new PublicationExecutor(wrapped,authority,async(r,catalog)=>{
  if(target.staged.has(r.version))return;
  const result=await wrapped.activate(catalog);expect(result).toEqual({ok:true,version:r.version});
 });
 let worker=executor();
 const bodies=new Map<string,Record<string,unknown>>();
 const request=async(project:string,revision=1)=>{
  const id=crypto.randomUUID(),body=await manifest(project,{files:{'revision.txt':String(revision)}});bodies.set(id,body);
  await testEnv.RELEASES.put(`incoming/${id}.json`,JSON.stringify(body));
  return {incoming:id,requestId:id,headerProject:project,key:'synthetic'};
 };
 const serving=()=>target.staged.get(live)?.get('releases.js')??'';
 const publish=async(outcome:Staged)=>{expect(await worker.deploy(ref(outcome))).toEqual({ok:true});expect(await coordinator.publish(outcome.publication)).toEqual({ready:true});};
 return {instance,state,target,events,request,serving,publish,bodies,
  sync:(r:Parameters<SyncCoordinator['sync']>[0])=>coordinator.sync(r),
  get coordinator(){return coordinator;},get worker(){return worker;},get live(){return live;},
  versionGate:(g:ReturnType<typeof gate>|undefined)=>{versionGate=g;},
  prepareGate:(g:ReturnType<typeof gate>|undefined)=>{prepareGate=g;},deployGate:(g:ReturnType<typeof gate>|undefined)=>{deployGate=g;},notifyGate:(g:ReturnType<typeof gate>|undefined)=>{notifyGate=g;},
  stageGate:(g:ReturnType<typeof gate>|undefined,entered?:ReturnType<typeof gate>)=>{stageGate=g;stageEntered=entered;},
  setLive:(v:string)=>{live=v;},failDeploy:(v:boolean)=>{failDeploy=v;},retainDeploy:(v:boolean)=>{retainDeploy=v;},failPrepare:(v:boolean)=>{failPrepare=v;},failNotify:(v:boolean)=>{failNotify=v;},
  notifyStatus:(status:number)=>{notifyStatus=status;},
  reset:()=>{coordinator=new SyncCoordinator(ports);builder=core();target.staged.clear();worker=executor();},
  async http(r:Awaited<ReturnType<typeof request>>,publicIngress=false,transition=false,key='synthetic'){
   const control={syncKeyAccepted:async(_p:string,key:string)=>key==='synthetic',devSync:(q:Parameters<SyncCoordinator['sync']>[0])=>coordinator.sync(q),
    publishRelease:async(id:string)=>{try{return await coordinator.publish(id);}catch(cause){if(cause instanceof HttpError)return {ready:false,error:cause.message,status:cause.status};throw cause;}},publicationReference:(id:string)=>Promise.resolve(coordinator.deployments.reference(id))};
   const transport={fetch:async(input:RequestInfo|URL,init?:RequestInit)=>{
    const incoming=new Request(input,init),data=await incoming.json() as {publication:DeploymentRef;version:string;error:string};
    return Response.json(new URL(incoming.url).pathname==='/abort'?await worker.abort(data.publication,data.error):await worker.deploy(data.publication));
   }};
   const env={CONTROL:{getByName:()=>control},RELEASES:testEnv.RELEASES,RELEASE_BUILDER:transport,...(transition?{GONVEX_TOPOLOGY_TRANSITION:'1'}:{}),...(publicIngress?{GONVEX_PUBLIC_URL:'https://fixture',GONVEX_RELEASE_BUILDER_TOKEN:'synthetic-builder-token'}:{})} as unknown as DevRouterEnv;
   const fetch=publicIngress?vi.spyOn(globalThis,'fetch').mockImplementation(async(input,init)=>{
    const result=await handleDevRequest(new Request(input,init),env,state as unknown as ExecutionContext);
    if(!result)throw Error('unexpected ingress');return result;
   }):undefined;
   try{return await handleDevRequest(new Request('https://fixture/dev/sync',{method:'POST',headers:{authorization:`Bearer ${key}`,'content-type':'application/json','x-gonvex-project-id':r.headerProject,'x-gonvex-sync-request-id':r.requestId},body:JSON.stringify(bodies.get(r.requestId))}),env,state as unknown as ExecutionContext);}
   finally{fetch?.mockRestore();}
  },
 };
}

it('serializes overlapping preparations and publications; a delayed A deploy cannot remove B',async()=>fixture(async f=>{
 const a=await f.request('race-a'),b=await f.request('race-b');
 const preparing=gate();f.prepareGate(preparing);
 const pending=f.sync(a);await new Promise(resolve=>setTimeout(resolve,0));
 expect(await f.sync(b)).toMatchObject({ok:false,status:503,retryable:true});
 expect(f.events).toEqual(['prepare:race-a']);
 expect(await f.sync({...a,requestId:crypto.randomUUID()})).toMatchObject({ok:false,status:409});
 preparing.release();f.prepareGate(undefined);
 const first=staged(await pending);
 expect(await f.sync(b)).toMatchObject({ok:false,recovery:ref(first)});
 expect(f.events).toEqual(['prepare:race-a']);
 await f.publish(first);
 const second=staged(await f.sync(b));
 expect(f.target.staged.get(second.deploy)?.get('releases.js')).toContain('race-a');
 expect(f.target.staged.get(second.deploy)?.get('releases.js')).toContain('race-b');
 await f.publish(second);
 const calls=f.target.deployed.length;
 expect(await f.worker.deploy(ref(first))).toEqual({ok:true});
 expect(f.target.deployed).toHaveLength(calls);expect(f.live).toBe(second.deploy);
 expect(f.serving()).toContain('race-a');expect(f.serving()).toContain('race-b');
}));

it('router recovers A before capturing B, including overlapping target execution and notification ACK',async()=>fixture(async f=>{
 const a=await f.request('router-a'),b=await f.request('router-b');
 const first=staged(await f.sync(a)),deploying=gate(),notifying=gate();f.deployGate(deploying);f.notifyGate(notifying);
 const old=f.worker.deploy(ref(first));await new Promise(resolve=>setTimeout(resolve,0));
 const next=f.http(b);await new Promise(resolve=>setTimeout(resolve,0));
 expect(f.events.filter(event=>event==='prepare:router-b')).toHaveLength(0);
 deploying.release();f.deployGate(undefined);expect(await old).toEqual({ok:true});
 await new Promise(resolve=>setTimeout(resolve,0));
 expect(f.events.filter(event=>event==='prepare:router-b')).toHaveLength(0);
 notifying.release();f.notifyGate(undefined);
 const response=await next;expect(response?.status).toBe(200);await response!.arrayBuffer();
 expect(f.serving()).toContain('router-a');expect(f.serving()).toContain('router-b');
 expect(f.coordinator.activeReleases().map(release=>release.project)).toEqual(['router-a','router-b']);
}));

it.each(['rpc','rpc-status','http'])('retains a serving publication across a transient %s notification failure and same-ID retry',async mode=>fixture(async f=>{
 const a=await f.request('notify-retry'),publicIngress=mode==='http';
 if(mode==='rpc-status')f.notifyStatus(503);
 f.failNotify(true);
 const failed=await f.http(a,publicIngress);expect(failed?.status).toBe(503);
 expect(await failed!.json()).toMatchObject({retryable:true,error:expect.stringContaining('holder unavailable')});
 const current=f.coordinator.deployments.current()!,active=f.coordinator.activeRelease(a.headerProject)!;
 expect(current.id).toBe(a.requestId);expect(f.live).toBe(current.version);expect(active.generation).toBe(1);
 expect(f.target.deployed).toHaveLength(1);
 f.failNotify(false);
 const retried=await f.http(a,publicIngress);expect(retried?.status).toBe(200);await retried!.arrayBuffer();
 expect(f.coordinator.deployments.current()).toBeNull();
 expect(f.coordinator.activeRelease(a.headerProject)).toEqual(active);
 expect(f.target.deployed).toHaveLength(1);expect(f.events.filter(event=>event.startsWith('prepare:'))).toHaveLength(1);
 expect(f.instance.sql.value('SELECT completed FROM gonvex_cf_publications WHERE sync_id=$1',a.requestId)).toBe(1);
}));

it('a new same-project request recovers an abandoned publication but remains refused until retried',async()=>fixture(async f=>{
 const a=await f.request('same-project',1),first=staged(await f.sync(a)),next=await f.request('same-project',2);
 f.reset();
 expect(await f.sync({...next,key:'wrong'})).toMatchObject({ok:false,status:401});
 expect(await f.sync(next)).toMatchObject({ok:false,status:409,recovery:ref(first)});
 const refused=await f.http(next);expect(refused?.status).toBe(409);await refused!.arrayBuffer();
 expect(f.coordinator.deployments.current()).toBeNull();expect(f.coordinator.activeRelease('same-project')?.generation).toBe(1);
 expect(f.events.filter(event=>event.startsWith('prepare:'))).toHaveLength(1);
 expect(f.instance.sql.value('SELECT completed FROM gonvex_cf_publications WHERE sync_id=$1',a.requestId)).toBe(1);
 const retried=await f.http(next);expect(retried?.status).toBe(200);await retried!.arrayBuffer();
 expect(f.coordinator.activeRelease('same-project')?.generation).toBe(2);
 expect(f.events.filter(event=>event.startsWith('prepare:'))).toHaveLength(2);
}));

it('an overlapping upgrade preserves both latest hashes and cannot redeploy a previously completed snapshot',async()=>fixture(async f=>{
 await f.publish(staged(await f.sync(await f.request('upgrade-a',1))));
 const oldB=staged(await f.sync(await f.request('upgrade-b',1)));await f.publish(oldB);
 const oldHash=f.coordinator.activeRelease('upgrade-a')!.hash;
 const a=await f.request('upgrade-a',2),b=await f.request('upgrade-b',2),nextA=staged(await f.sync(a));
 expect(await f.sync({...b,key:'wrong'})).toMatchObject({ok:false,status:401});
 expect(await f.sync(b)).toMatchObject({ok:false,recovery:ref(nextA)});
 const response=await f.http(b);expect(response?.status).toBe(200);await response!.arrayBuffer();
 const active=f.coordinator.activeReleases();expect(active).toHaveLength(2);
 expect(active.find(release=>release.project==='upgrade-a')?.hash).not.toBe(oldHash);
 expect(active.map(release=>release.generation)).toEqual([2,2]);
 for(const release of active)expect(f.serving()).toContain(release.hash);
 expect(f.serving()).not.toContain(oldHash);
 const calls=f.target.deployed.length,version=f.live;
 expect(await f.worker.deploy(ref(oldB))).toEqual({ok:true});expect(f.target.deployed).toHaveLength(calls);expect(f.live).toBe(version);
}));

it('resumes the same pending fence after coordinator/builder restart and rebuilds its immutable catalog',async()=>fixture(async f=>{
 const a=await f.request('reset-a'),b=await f.request('reset-b'),first=staged(await f.sync(a));
 f.instance.sql.run("UPDATE gonvex_cf_syncs SET expires_at=0 WHERE sync_id=$1",a.requestId);
 f.reset();
 expect(await f.sync(b)).toMatchObject({ok:false,recovery:ref(first)});
 const resumed=staged(await f.sync(a));expect(ref(resumed)).toEqual(ref(first));
 expect(f.target.staged.size).toBe(0);await f.publish(resumed);
 const second=staged(await f.sync(b));await f.publish(second);
 expect(f.serving()).toContain('reset-a');expect(f.serving()).toContain('reset-b');
}));

it('an expired preparation cannot stage/deploy after a successor takes the durable fence',async()=>fixture(async f=>{
 const a=await f.request('expired-a'),b=await f.request('expired-b'),preparing=gate();f.prepareGate(preparing);
 const old=f.sync(a);await new Promise(resolve=>setTimeout(resolve,0));
 f.instance.sql.run("UPDATE gonvex_cf_syncs SET expires_at=0 WHERE sync_id=$1",a.requestId);
 f.prepareGate(undefined);const next=staged(await f.sync(b));
 preparing.release();await expect(old).rejects.toThrow('deployment fence is no longer current');
 expect(f.target.staged.size).toBe(1);await f.publish(next);expect(f.serving()).toContain('expired-b');expect(f.serving()).not.toContain('expired-a');
}));

it('a preparation past its lease, or whose catalog snapshot has a retired project, cannot move to publishing',async()=>fixture(async f=>{
 const x=staged(await f.sync(await f.request('snap-x')));await f.publish(x);
 // Lease expired while the build ran: refused at the running -> publishing step.
 const a=await f.request('snap-a'),heldA=gate(),builtA=gate();f.stageGate(heldA,builtA);
 const lateA=f.sync(a);await builtA.promise;
 f.instance.sql.run('UPDATE gonvex_cf_syncs SET expires_at=$2 WHERE sync_id=$1',a.requestId,Date.now()-1);
 heldA.release();await expect(lateA).rejects.toThrow('preparation lease expired');
 expect(f.instance.sql.value("SELECT state FROM gonvex_cf_syncs WHERE sync_id=$1",a.requestId)).not.toBe('publishing');
 // A project in the snapshot retired meanwhile (as a delete does): refused too, and nothing is deployed.
 const b=await f.request('snap-b'),heldB=gate(),builtB=gate();f.stageGate(heldB,builtB);
 const lateB=f.sync(b);await builtB.promise;
 f.instance.sql.run('DELETE FROM gonvex_cf_active_releases WHERE project_id=(SELECT project_id FROM gonvex_cf_syncs WHERE sync_id=$1)',x.publication);bumpRetirementGeneration(f.instance.sql);
 heldB.release();await expect(lateB).rejects.toThrow('a project in its catalog was retired');
 expect(f.target.deployed.some(v=>v!==x.deploy)).toBe(false);
 expect(f.coordinator.deployments.current()).toBeNull();
}));
it('a late completed build cannot publish or deploy over the successor catalog',async()=>fixture(async f=>{
 const a=await f.request('late-a'),b=await f.request('late-b'),held=gate(),built=gate();f.stageGate(held,built);
 const old=f.sync(a);await built.promise;expect(f.target.staged.size).toBe(1);
 const oldVersion=[...f.target.staged.keys()][0]!,oldFence=Number(f.instance.sql.value('SELECT rowid FROM gonvex_cf_syncs WHERE sync_id=$1',a.requestId));
 f.instance.sql.run('UPDATE gonvex_cf_syncs SET expires_at=0 WHERE sync_id=$1',a.requestId);
 f.stageGate(undefined);const next=staged(await f.sync(b));await f.publish(next);
 held.release();await expect(old).rejects.toThrow('deployment fence is no longer current');
 await expect(f.worker.deploy({id:a.requestId,fence:oldFence,version:oldVersion})).rejects.toThrow('deployment reference does not match its fence');
 expect(f.live).toBe(next.deploy);expect(f.target.deployed).toHaveLength(1);
 expect(f.serving()).toContain('late-b');expect(f.serving()).not.toContain('late-a');
}));

it('terminal target failure and a serialized abort release ownership; late failed requests remain fenced',async()=>fixture(async f=>{
 const a=await f.request('failure-a'),b=await f.request('failure-b');
 f.failPrepare(true);await expect(f.sync(a)).rejects.toThrow('prepare failed');f.failPrepare(false);
 const first=staged(await f.sync(b));f.failDeploy(true);
 expect(await f.worker.deploy(ref(first))).toMatchObject({ok:false});f.failDeploy(false);
 const c=await f.request('failure-c'),second=staged(await f.sync(c)),blocked=gate();f.deployGate(blocked);
 const pending=f.worker.deploy(ref(second)),aborting=f.worker.abort(ref(second),'operator terminal failure');
 await new Promise(resolve=>setTimeout(resolve,0));
 expect(f.coordinator.deployments.current()?.id).toBe(second.publication);
 blocked.release();f.deployGate(undefined);await pending;await aborting;
 expect(f.coordinator.deployments.current()).toBeNull();
 await expect(f.worker.deploy(ref(second))).rejects.toThrow('no longer current');
 const d=staged(await f.sync(await f.request('failure-d')));await f.publish(d);expect(f.serving()).toContain('failure-d');
}));

it('terminal notification failure preserves the already-active catalog for the next project',async()=>fixture(async f=>{
 const a=staged(await f.sync(await f.request('terminal-a')));
 expect(await f.worker.deploy(ref(a))).toEqual({ok:true});f.failNotify(true);
 await expect(f.coordinator.publish(a.publication)).rejects.toThrow('holder unavailable');
 const active=f.coordinator.activeRelease('terminal-a')!;
 expect(f.coordinator.deployments.current()?.id).toBe(a.publication);
 await f.worker.abort(ref(a),'terminal notification failure');f.failNotify(false);
 const b=staged(await f.sync(await f.request('terminal-b')));await f.publish(b);
 expect(f.serving()).toContain(active.hash);expect(f.serving()).toContain('terminal-b');
 await expect(f.worker.deploy(ref(a))).rejects.toThrow('no longer current');
 expect(f.live).toBe(b.deploy);
}));

it('a pending whole-deployment owner survives actual Control eviction until publication completes',async()=>{
 const object=testEnv.CONTROL.getByName('eviction-'+crypto.randomUUID()),target=new MemoryVersionTarget();
 const builder=new ReleaseBuilderCore({blobs:testEnv.RELEASES as unknown as BlobStore,target,preludeSpecifier:PRELUDE_SPECIFIER});
 const stage=async(project:string)=>{const id=crypto.randomUUID();await testEnv.RELEASES.put(`incoming/${id}.json`,JSON.stringify(await manifest(project)));return {incoming:id,requestId:id,headerProject:project,key:'k19-fallback-sync-key'};};
 await runInDurableObject(object,(instance:OperatorControlObject)=>instance.setReleaseBuilderForTest(builder));
 const a=await stage('eviction-a'),b=await stage('eviction-b'),first=staged(await object.devSync(a) as SyncOutcome);
 await evictDurableObject(object);
 await runInDurableObject(object,(instance:OperatorControlObject)=>instance.setReleaseBuilderForTest(builder));
 expect(await object.devSync(b)).toMatchObject({ok:false,recovery:ref(first)});
 expect(await object.devSync(a)).toMatchObject({ok:true,publication:first.publication,fence:first.fence});
 expect(await object.publishRelease(first.publication)).toEqual({ready:true});
 const second=staged(await object.devSync(b) as SyncOutcome);
 expect(target.staged.get(second.deploy)?.get('releases.js')).toContain('eviction-a');
 expect(target.staged.get(second.deploy)?.get('releases.js')).toContain('eviction-b');
 expect(await object.publishRelease(second.publication)).toEqual({ready:true});
});

it('completed deployment history does not turn acquisition/expiry into a table scan',async()=>fixture(async f=>{
 const a=await f.request('history-a');let mark=f.instance.sql.mark();const first=staged(await f.sync(a));
 const before=f.instance.sql.mark().read-mark.read;
 await f.coordinator.deployments.abort(ref(first),'test completed before target execution');
 for(let n=0;n<200;n++)f.instance.sql.run("INSERT INTO gonvex_cf_syncs(sync_id,project_id,state,expires_at) VALUES($1,'history','finished',0)",'history-'+n);
 const b=await f.request('history-b');mark=f.instance.sql.mark();const next=staged(await f.sync(b));
 const after=f.instance.sql.mark().read-mark.read;
 expect(after-before).toBeLessThanOrEqual(8);
 await f.publish(next);
}));

it('migrates pre-fence interrupted preparations once, while preserving modern publication owners on startup',async()=>fixture(async f=>{
 f.instance.sql.run('DROP INDEX gonvex_cf_one_deployment');
 for(const project of ['legacy-a','legacy-b'])f.instance.sql.run("INSERT INTO gonvex_cf_syncs(sync_id,project_id,state,expires_at) VALUES($1,$2,'running',$3)",crypto.randomUUID(),project,Date.now()+60000);
 installDeploymentSchema(f.instance.sql);
 expect(f.instance.sql.all("SELECT state FROM gonvex_cf_syncs WHERE project_id LIKE 'legacy-%'").map(row=>row.state)).toEqual(['abandoned','abandoned']);
 const first=staged(await f.sync(await f.request('modern-a')));
 installDeploymentSchema(f.instance.sql);
 expect(f.coordinator.deployments.current()).toEqual(ref(first));
 expect(await f.sync(await f.request('modern-b'))).toMatchObject({ok:false,recovery:ref(first)});
 await f.publish(first);
}));

it('durable topology freeze refuses new publication across coordinator reset, without expiring or replacing an owner',async()=>fixture(async f=>{
 const request=await f.request('topology-a');
 f.coordinator.deployments.freezeTopology();f.reset();
 expect(f.coordinator.deployments.topologyFrozen()).toBe(true);
 await expect(f.sync(request)).rejects.toThrow('topology migration');
 const mark=f.instance.sql.mark();f.coordinator.deployments.thawTopology();const cost=f.instance.sql.mark();
 console.log('topology thaw SQL rows',cost.read-mark.read,cost.written-mark.written);
 expect(cost.written-mark.written).toBe(1);
 const pending=staged(await f.sync(request));expect(()=>f.coordinator.deployments.freezeTopology()).toThrow('complete the current publication');
 f.reset();expect(f.coordinator.deployments.current()).toEqual(ref(pending));await f.publish(pending);
 f.coordinator.deployments.freezeTopology();expect(f.coordinator.deployments.topologyFrozen()).toBe(true);
}));
it('asynchronous serving-version observation cannot authorize a fence that ended while its RPC was in flight',async()=>fixture(async f=>{
 const pending=staged(await f.sync(await f.request('catalog-a'))),held=gate();f.versionGate(held);
 const grant=f.coordinator.deployments.authorize(ref(pending));
 await f.coordinator.deployments.abort(ref(pending),'target failed before read returned');
 const newer=staged(await f.sync(await f.request('catalog-b')));held.release();f.versionGate(undefined);
 await expect(grant).rejects.toThrow('fence is no longer current');await f.publish(newer);
}));
it('topology maintenance may expire interrupted preparation but cannot expire a staged deployment',async()=>fixture(async f=>{
 const request=await f.request('topology-expired');
 const lock=f.coordinator.deployments.acquire(request.headerProject,request.requestId);
 expect(lock.acquired).toBe(true);
 f.instance.sql.run('UPDATE gonvex_cf_syncs SET expires_at=0 WHERE sync_id=$1',request.requestId);
 f.coordinator.deployments.freezeTopology();f.reset();
 expect(f.coordinator.deployments.topologyFrozen()).toBe(true);
 expect(f.instance.sql.value('SELECT state FROM gonvex_cf_syncs WHERE sync_id=$1',request.requestId)).toBe('abandoned');
 f.coordinator.deployments.thawTopology();
 const pending=staged(await f.sync(await f.request('topology-staged')));
 f.instance.sql.run('UPDATE gonvex_cf_syncs SET expires_at=0 WHERE sync_id=$1',pending.publication);
 expect(()=>f.coordinator.deployments.freezeTopology()).toThrow('complete the current publication');
 await f.publish(pending);
}));
it('completed settlement needs no catalog RPC and late asynchronous failure cannot abort a successor',async()=>fixture(async f=>{
 const a=staged(await f.sync(await f.request('settle-a')));await f.publish(a);
 const held=gate();f.versionGate(held);
 // Completed operations return without touching an unavailable catalog service.
 await f.coordinator.deployments.settle(ref(a),{ok:false});
 held.release();f.versionGate(undefined);
 const b=staged(await f.sync(await f.request('settle-b'))),delayed=gate();f.versionGate(delayed);
 const late=f.coordinator.deployments.settle(ref(b),{ok:false,error:'late failure'});
 f.coordinator.deployments.abort(ref(b),'target barrier completed');
 const c=staged(await f.sync(await f.request('settle-c')));
 delayed.release();f.versionGate(undefined);await late;
 expect(f.coordinator.deployments.current()).toEqual(ref(c));await f.publish(c);
}));
it('a superseded abort needs an expired write lease and no serving component, and a late write is refused',async()=>fixture(async f=>{
 const age=(ms:number)=>f.instance.sql.run("UPDATE gonvex_cf_target_attempts SET authorized_at=authorized_at-$1",ms);
 // An authorized write still in flight: the superseded abort must not release the fence under it.
 const a=staged(await f.sync(await f.request('stranded-a'))),inFlight=gate();f.deployGate(inFlight);
 const running=f.worker.deploy(ref(a));
 await vi.waitFor(()=>expect(f.events).toContain('deploy:'+ref(a).version));
 f.setLive('another-catalog');
 await expect(f.coordinator.deployments.abort(ref(a),'superseded mid-write',{superseded:true})).rejects.toThrow('same-ID recovery before abort');
 f.failDeploy(true);f.retainDeploy(true);inFlight.release();f.deployGate(undefined);
 expect(await running).toMatchObject({ok:false,retain:true});
 // A plain abort still needs same-ID recovery; so does superseded inside the lease or while the version is served.
 await expect(f.coordinator.deployments.abort(ref(a),'plain')).rejects.toThrow('same-ID recovery before abort');
 await expect(f.coordinator.deployments.abort(ref(a),'inside lease',{superseded:true})).rejects.toThrow('same-ID recovery before abort');
 age(TARGET_ATTEMPT_LEASE_MS+1);f.setLive(ref(a).version);
 await expect(f.coordinator.deployments.abort(ref(a),'served',{superseded:true})).rejects.toThrow('same-ID recovery before abort');
 expect(f.coordinator.deployments.current()).toEqual(ref(a));
 // Lease expired and nothing serves it: the fence clears.
 f.setLive('another-catalog');
 await f.coordinator.deployments.abort(ref(a),'stranded: catalog format changed',{superseded:true});
 expect(f.coordinator.deployments.current()).toBeNull();
 // A late write of the aborted publication is refused before any target write.
 const writes=f.events.length;
 await expect(f.worker.deploy(ref(a))).rejects.toThrow('deployment fence is no longer current');
 expect(f.events.length).toBe(writes);
 await expect(f.coordinator.deployments.settle(ref(a),{ok:true})).resolves.toBeUndefined();
 expect(f.coordinator.deployments.current()).toBeNull();
 f.failDeploy(false);f.retainDeploy(false);
 const b=staged(await f.sync(await f.request('stranded-b')));await f.publish(b);
 expect(f.serving()).toContain('stranded-b');
}));
it('a partial target retains its durable fence across restart and same-ID recovery, refusing abort and the next project',async()=>fixture(async f=>{
 const request=await f.request('partial-a'),a=staged(await f.sync(request));
 f.failDeploy(true);f.retainDeploy(true);
 expect(await f.worker.deploy(ref(a))).toMatchObject({ok:false,retain:true});
 expect(f.coordinator.deployments.current()).toEqual(ref(a));
 await expect(f.worker.abort(ref(a),'caller gave up')).rejects.toThrow('same-ID recovery before abort');
 f.reset();expect(staged(await f.sync(request)).publication).toBe(a.publication);
 expect(await f.sync(await f.request('partial-b'))).toMatchObject({ok:false,status:503,recovery:ref(a)});
 // Losing a Container does not make a second ordinary failure proof of no write.
 f.retainDeploy(false);expect(await f.worker.deploy(ref(a))).toMatchObject({ok:false,retain:true});
 await expect(f.worker.abort(ref(a),'caller gave up again')).rejects.toThrow('same-ID recovery before abort');
 f.failDeploy(false);await f.publish(staged(await f.sync(request)));
 const b=staged(await f.sync(await f.request('partial-b')));await f.publish(b);
 expect(f.serving()).toContain('partial-a');expect(f.serving()).toContain('partial-b');
}));
it('authorization records a target attempt before mutation, so a lost builder cannot abandon it after restart',async()=>fixture(async f=>{
 const request=await f.request('lost-builder'),a=staged(await f.sync(request));
 expect(await f.coordinator.deployments.authorize(ref(a))).toMatchObject({allowed:true});
 f.reset();await expect(f.worker.abort(ref(a),'old process disappeared')).rejects.toThrow('same-ID recovery before abort');
 f.failDeploy(true);expect(await f.worker.deploy(ref(a))).toMatchObject({ok:false,retain:true});
 f.failDeploy(false);await f.publish(staged(await f.sync(request)));
 expect(f.coordinator.deployments.current()).toBeNull();
}));
it('writes one indexed attempt row before grant and removes it at a successful target barrier',async()=>fixture(async f=>{
 const a=staged(await f.sync(await f.request('attempt-cost'))),before=f.instance.sql.mark();
 expect(await f.coordinator.deployments.authorize(ref(a))).toMatchObject({allowed:true});
 const granted=f.instance.sql.mark();await f.coordinator.deployments.settle(ref(a),{ok:true});const settled=f.instance.sql.mark();
 const counts={grant:{read:granted.read-before.read,written:granted.written-before.written},settle:{read:settled.read-granted.read,written:settled.written-granted.written}};
 console.log('target attempt SQL rows',JSON.stringify(counts));
 // SQLite charges the metadata row and its TEXT-primary-key index on INSERT.
 expect(counts.grant.written).toBe(2);expect(counts.settle.written).toBe(1);
 expect(f.instance.sql.value('SELECT COUNT(*) FROM gonvex_cf_target_attempts')).toBe(0);
 await f.publish(a);
}));
it('the maintenance router refuses new preparations while permitting ordinary same-ID recovery with its original key/hash checks',async()=>fixture(async f=>{
 const a=await f.request('maintenance-a');const pending=staged(await f.sync(a));
 const fresh=await f.request('maintenance-b'),before=f.events.length;
 expect((await f.http(fresh,false,true))?.status).toBe(503);expect(f.events).toHaveLength(before);
 expect((await f.http(a,false,true,'wrong-key'))?.status).toBe(401);
 const original=f.bodies.get(a.requestId),different=await f.request('maintenance-a',2);
 f.bodies.set(a.requestId,f.bodies.get(different.requestId)!);
 expect((await f.http(a,false,true))?.status).toBe(409);f.bodies.set(a.requestId,original!);
 const resumed=await f.http(a,false,true);expect(resumed?.status).toBe(200);
 expect(f.coordinator.deployments.current()).toBeNull();
 expect(await f.coordinator.publish(pending.publication)).toEqual({ready:true});
}));
