// Synthetic baked fixture catalog; actual Control, AssemblyTenant, gateway,
// Session and WebSocket/RPC paths. Publication invokes real tenant activation
// and sends reload through the normal Control/gateway RPC.
import {env} from 'cloudflare:test';
import {expect,it} from 'vitest';
import {manifest as base,files} from './catalog';
import type {AssemblyTenant} from '../tenant';
import type {TestControl,TestGateway} from './stable-worker';
import {buildReleasePlan,migrationsFromFiles} from '../../migrate';
import {R2PlanStore} from '../../migrate/plan';
import type {TestRuntimeCatalog} from './runtime-worker';
import type {TestActionCatalog} from './action-catalog-worker';
import type {ReleaseSummary} from '../../../control/operator/release';

it('completed publication activates an existing tenant and notifies tenant/control-only sockets after Control eviction',async()=>{
 const bindings=env as unknown as {PUB_CONTROL:DurableObjectNamespace<TestControl>;PUB_TENANT:DurableObjectNamespace<AssemblyTenant>;PUB_GATEWAY:DurableObjectNamespace<TestGateway>;RELEASES:R2Bucket;RUNTIME_CATALOG:TestRuntimeCatalog;ACTIONS_CATALOG:TestActionCatalog};
 const project='publication-fixture',tenant='publication-'+crypto.randomUUID();let control=bindings.PUB_CONTROL.getByName('control');
 const catalog=(generation:number,contract:number|null=null)=>{
  const hash=`publication-fixture-${generation}`;
  const manifest={...base,functions:{'probe.generation':{kind:'reducer',handler:'generation',args:{kind:'any'},result:{kind:'any'}}},module:{...base.module,hash,generation,
   files:{...base.module.files,...(contract===null?{}:{'client-contract.json':btoa(JSON.stringify({version:contract,offlineMaxAgeMs:null}))})}}};
  return {project,hash,generation,manifest,exports:{generation:async()=>({generation})}};
 };
 const sockets:WebSocket[]=[];
 const open=async()=>{
  const response=await bindings.PUB_GATEWAY.getByName('publication-holder').fetch(new Request('https://fixture/ws',{headers:{upgrade:'websocket','x-gonvex-connection-id':crypto.randomUUID()}}));
  expect(response.status).toBe(101);const ws=response.webSocket!;sockets.push(ws);const frames:any[]=[];ws.accept();ws.addEventListener('message',event=>{frames.push(JSON.parse(String(event.data)));});
  const next=async(match:(frame:any)=>boolean)=>{for(let n=0;n<200;n++){const i=frames.findIndex(match);if(i>=0)return frames.splice(i,1)[0];await new Promise(resolve=>setTimeout(resolve,10));}throw Error('publication fixture frame timeout');};
  const call=async(frame:any)=>{ws.send(JSON.stringify(frame));return next(reply=>reply.id===frame.id);};
  await next(frame=>frame.type==='session.ready');return {ws,next,call,frames};
 };
 try{
  const first=catalog(1);await bindings.ACTIONS_CATALOG.select('catalog-1');await bindings.RUNTIME_CATALOG.install(project,first.manifest,1,'catalog-1');
  const plan=await buildReleasePlan(await migrationsFromFiles(files));await new R2PlanStore(bindings.RELEASES).write(plan);
  await bindings.PUB_TENANT.getByName(tenant).activate(project,first.manifest,plan.id);
  await control.devCreateProject({id:project,name:'Publication fixture'});await control.devSaveManifest(project,first.manifest);
  await control.fixtureTenant(project,tenant);
  await control.devE2eMember({projectId:project,tenantId:tenant,email:'publication@fixture.invalid',password:'synthetic-test-password'});
  expect(await bindings.PUB_TENANT.getByName(tenant).operatorInsertRow('board',{id:'preserved-row',title:'retained across adoption',position:0,bumps:0})).toMatchObject({ok:true});
  const retained=async()=>expect(await bindings.PUB_TENANT.getByName(tenant).operatorReadRows('board',{
   limit:10,offset:0,search:'',sort:'id',direction:'asc',filters:'',columns:'',count:'true',
  })).toMatchObject({ok:true,value:{total:1,rows:[{id:'preserved-row',title:'retained across adoption',position:0,bumps:0}]}});
  await retained();
  const data=await open();
  await data.call({type:'auth',id:'login-scope',project,controlOnly:true});
  const login=await data.call({type:'action.call',id:'login',idempotencyKey:crypto.randomUUID(),scope:'control',path:'control.auth.passwordLogin',args:{email:'publication@fixture.invalid',password:'synthetic-test-password'}});
  expect(login.type,login.error).toBe('action.result');const token=login.result.accessToken;
  expect((await data.call({type:'auth',id:'data',project,tenant,token})).type).toBe('auth.result');
  const onlyControl=await open();expect((await onlyControl.call({type:'auth',id:'control',project,controlOnly:true,token})).type).toBe('auth.result');
  const generation=async(id:string)=>data.call({type:'reducer.call',id,path:'probe.generation',args:{}});
  expect(await generation('v1')).toMatchObject({type:'reducer.result',result:{generation:1}});
  // The real holder registrations must survive the code-update boundary.
  const controlBefore=await control.runtimeInstance();
  await control.fixtureRestart().catch(()=>undefined);
  control=bindings.PUB_CONTROL.getByName('control');
  expect(await control.runtimeInstance()).not.toBe(controlBefore);
  const gateway=bindings.PUB_GATEWAY.getByName('publication-holder');
  const holderBefore=await gateway.fixtureLifecycle();
  const publish=async(n:number,contract:number|null=null)=>{
   const release=catalog(n,contract);
   const summary:ReleaseSummary={project,hash:release.hash,artifactGeneration:n,functionCount:1,schemaDefinition:{},migrations:[],controlMigrations:[],planId:null,
    clientContract:contract===null?null:{version:contract,offlineMaxAgeMs:null},crons:[],invitationAcceptanceReducer:'',storedManifest:release.manifest,bundleBytes:0};
   const id=crypto.randomUUID();
   await control.fixturePublication(id,summary,`catalog-${n}`,[tenant]);
   if(n===2){
    await control.fixtureRestart().catch(()=>undefined);control=bindings.PUB_CONTROL.getByName('control');
    expect((await control.currentDeployment())?.id).toBe(id);
    expect(await generation('still-v1')).toMatchObject({type:'reducer.result',result:{generation:1}});
   }
   expect(await control.publishRelease(id)).toEqual({ready:false});
   // A partial Actions-only deployment is not whole-composition readiness.
   await bindings.ACTIONS_CATALOG.select(`catalog-${n}`);
   expect(await control.publishRelease(id)).toEqual({ready:false});
   // A version label alone cannot substitute for the expected tenant artifact.
   await bindings.RUNTIME_CATALOG.install(project,{...release.manifest,module:{...release.manifest.module,hash:'wrong-hash'}},n,`catalog-${n}`);
   expect(await control.publishRelease(id)).toEqual({ready:false});
   await bindings.RUNTIME_CATALOG.install(project,release.manifest,n,`catalog-${n}`);
   // Only tenant application state is restarted. Both actual gateway WebSockets
   // retain their original holder and connection IDs throughout publication.
   // Controlled reconstruction of the socket-free auxiliary tenant, not proof
   // of managed deployment adoption. Auxiliary eviction is not exposed by Vitest.
   await bindings.PUB_TENANT.getByName(tenant).fixtureRestart().catch(()=>undefined);
   if(n===2){
    await gateway.fixtureReload(true);await gateway.fixtureReloadFailure();let completed=false;
    const publication=control.publishRelease(id).then(result=>{completed=true;return result;});
    void publication.catch(()=>undefined);
    for(let i=0;i<200&&(await gateway.fixtureReload()).pending===0;i++)await new Promise(resolve=>setTimeout(resolve,10));
    expect((await gateway.fixtureReload()).pending).toBe(1);expect(completed).toBe(false);
    expect((await control.currentDeployment())?.id).toBe(id);
    await gateway.fixtureReload(false);await expect(publication).rejects.toThrow('synthetic notification failure');
    expect((await control.currentDeployment())?.id).toBe(id);
    expect(await control.publishRelease(id)).toEqual({ready:true});
   }else expect(await control.publishRelease(id)).toEqual({ready:true});
   expect(await control.publishRelease(id)).toEqual({ready:true});
   await retained();
   return release;
  };
  const second=await publish(2);
  for(const socket of [data,onlyControl])expect(await socket.next(frame=>frame.type==='system.reload')).toMatchObject({artifactHash:second.hash});
  expect(await generation('v2')).toMatchObject({type:'reducer.result',result:{generation:2}});
  expect(await data.call({type:'reducer.call',id:'old-pin',path:'probe.generation',args:{},artifactHash:first.hash})).toMatchObject({type:'reducer.error',class:'update_required'});
  expect(await data.call({type:'reducer.call',id:'new-pin',path:'probe.generation',args:{},artifactHash:second.hash})).toMatchObject({type:'reducer.result',result:{generation:2}});
  expect(await bindings.PUB_TENANT.getByName(tenant).operatorActivateRelease({projectId:project,hash:'wrong',generation:2})).toBe(false);
  expect(await bindings.PUB_TENANT.getByName(tenant).operatorActivateRelease({projectId:project,hash:second.hash,generation:1})).toBe(false);
  expect(await bindings.PUB_TENANT.getByName(tenant).operatorActivateRelease({projectId:'unknown-project',hash:second.hash,generation:2})).toBe(false);
  const third=await publish(3);
  for(const socket of [data,onlyControl])expect(await socket.next(frame=>frame.type==='system.reload')).toMatchObject({artifactHash:third.hash});
  expect(await generation('v3')).toMatchObject({type:'reducer.result',result:{generation:3}});
  expect(data.ws.readyState).toBe(WebSocket.OPEN);expect(onlyControl.ws.readyState).toBe(WebSocket.OPEN);
  expect(await bindings.RUNTIME_CATALOG.tenantId('namespace-probe')).toBe(bindings.PUB_TENANT.idFromName('namespace-probe').toString());
  expect((await control.runtimeTopology('freeze')).frozen).toBe(true);
  expect(await control.runtimeTopology('thaw','wrong-version')).toMatchObject({frozen:true,status:503,error:'runtime catalog version not ready'});
  await bindings.RUNTIME_CATALOG.fixtureNamespace(true);
  expect(await control.runtimeTopology('thaw','catalog-3')).toMatchObject({frozen:true,status:409,error:'runtime tenant namespace or active catalog differs'});
  await bindings.RUNTIME_CATALOG.fixtureNamespace(false);
  await bindings.RUNTIME_CATALOG.install('extra-project',third.manifest,3,'catalog-3');
  expect(await control.runtimeTopology('thaw','catalog-3')).toMatchObject({frozen:true,status:409,error:'runtime tenant namespace or active catalog differs'});
  await bindings.RUNTIME_CATALOG.clear('extra-project');await bindings.RUNTIME_CATALOG.install(project,third.manifest,3,'catalog-3');
  await control.fixtureRestart().catch(()=>undefined);control=bindings.PUB_CONTROL.getByName('control');
  expect((await control.runtimeTopology('status')).frozen).toBe(true);
  expect((await control.runtimeTopology('thaw','catalog-3')).frozen).toBe(false);
  const holderAfter=await gateway.fixtureLifecycle();
  expect(holderAfter.holder).toBe(holderBefore.holder);expect(holderAfter.nativeCount).toBe(2);
  expect(holderAfter.sessions.map((s:any)=>s.connectionId).sort()).toEqual(holderBefore.sessions.map((s:any)=>s.connectionId).sort());
  const fourth=await publish(4,2);
  expect(await data.next(frame=>frame.type==='client.updateRequired')).toMatchObject({contract:2});
  expect(await onlyControl.next(frame=>frame.type==='system.reload')).toMatchObject({artifactHash:fourth.hash});
 }finally{
  await bindings.PUB_GATEWAY.getByName('publication-holder').fixtureReload(false);
  await bindings.RUNTIME_CATALOG.fixtureNamespace(false);
  for(const socket of sockets)if(socket.readyState<2)socket.close(1000);
  await bindings.RUNTIME_CATALOG.clear(project);
 }
});
