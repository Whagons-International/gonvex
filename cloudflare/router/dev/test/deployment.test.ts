// Actual DO SQLite, R2, release compiler and fenced executor. Controlled target
// gates expose scheduling races; these are not platform deployment acceptance.
import {evictDurableObject,runInDurableObject} from "cloudflare:test";
import {expect,it} from "vitest";
import {MemoryVersionTarget,PRELUDE_SPECIFIER,ReleaseBuilderCore,type BlobStore} from "../../../control/operator/builder/core";
import {PublicationExecutor} from "../../../control/operator/builder/publication";
import {installDeploymentSchema} from "../../../control/operator/deployment";
import {SyncCoordinator,type SyncOutcome,type SyncPorts} from "../../../control/operator/sync";
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
 let live='',prepareGate:ReturnType<typeof gate>|undefined,stageGate:ReturnType<typeof gate>|undefined,stageEntered:ReturnType<typeof gate>|undefined,deployGate:ReturnType<typeof gate>|undefined,failDeploy=false,failPrepare=false,failNotify=false,notifyGate:ReturnType<typeof gate>|undefined;
 const events:string[]=[],target=new MemoryVersionTarget();
 const baseDeploy=target.deploy.bind(target);
 target.deploy=async(version)=>{events.push('deploy:'+version);await deployGate?.promise;if(failDeploy)throw Error('target failed');await baseDeploy(version);live=version;};
 const core=()=>new ReleaseBuilderCore({blobs:testEnv.RELEASES as unknown as BlobStore,target,preludeSpecifier:PRELUDE_SPECIFIER});
 let builder=core();
 const wrapped:ReleaseBuilder={inspect:(...a)=>builder.inspect(...a),prepare:async(...a)=>{events.push('prepare:'+a[1]);await prepareGate?.promise;if(failPrepare)throw Error('prepare failed');return builder.prepare(...a);},activate:async(...a)=>{const held=stageGate,entered=stageEntered,result=await builder.activate(...a);entered?.release();await held?.promise;return result;},deploy:version=>builder.deploy(version)};
 const ports:SyncPorts={sql:instance.sql,builder:wrapped,enqueue:async()=>{throw Error('unexpected tenant rollout');},versionReady:version=>live===version,
  activateTenants:async()=>true,moduleReloaded:async()=>{await notifyGate?.promise;if(failNotify)throw Error('holder unavailable');},syncCrons:async()=>{},deadlineMs:1000,devSyncKey:'synthetic',acceptsKey:(_p,key)=>key==='synthetic'};
 let coordinator=new SyncCoordinator(ports);
 const authority={authorize:async(r:DeploymentRef)=>coordinator.deployments.authorize(r),settle:async(r:DeploymentRef,outcome:{ok:boolean;error?:string})=>{coordinator.deployments.settle(r,outcome);},abort:async(r:DeploymentRef,error:string)=>{coordinator.deployments.abort(r,error);}};
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
  prepareGate:(g:ReturnType<typeof gate>|undefined)=>{prepareGate=g;},deployGate:(g:ReturnType<typeof gate>|undefined)=>{deployGate=g;},notifyGate:(g:ReturnType<typeof gate>|undefined)=>{notifyGate=g;},
  stageGate:(g:ReturnType<typeof gate>|undefined,entered?:ReturnType<typeof gate>)=>{stageGate=g;stageEntered=entered;},
  failDeploy:(v:boolean)=>{failDeploy=v;},failPrepare:(v:boolean)=>{failPrepare=v;},failNotify:(v:boolean)=>{failNotify=v;},
  reset:()=>{coordinator=new SyncCoordinator(ports);builder=core();target.staged.clear();worker=executor();},
  async http(r:Awaited<ReturnType<typeof request>>){
   const control={syncKeyAccepted:async(_p:string,key:string)=>key==='synthetic',devSync:(q:Parameters<SyncCoordinator['sync']>[0])=>coordinator.sync(q),
    publishRelease:(id:string)=>coordinator.publish(id),publicationReference:(id:string)=>Promise.resolve(coordinator.deployments.reference(id))};
   const transport={fetch:async(input:RequestInfo|URL,init?:RequestInit)=>{
    const incoming=new Request(input,init),data=await incoming.json() as {publication:DeploymentRef;version:string;error:string};
    return Response.json(new URL(incoming.url).pathname==='/abort'?await worker.abort(data.publication,data.error):await worker.deploy(data.publication));
   }};
   const env={CONTROL:{getByName:()=>control},RELEASES:testEnv.RELEASES,RELEASE_BUILDER:transport} as unknown as DevRouterEnv;
   return handleDevRequest(new Request('https://fixture/dev/sync',{method:'POST',headers:{authorization:'Bearer synthetic','content-type':'application/json','x-gonvex-project-id':r.headerProject,'x-gonvex-sync-request-id':r.requestId},body:JSON.stringify(bodies.get(r.requestId))}),env,state as unknown as ExecutionContext);
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
 f.coordinator.deployments.abort(ref(first),'test completed before target execution');
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
