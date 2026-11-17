// Synthetic baked fixture catalog; actual Control, AssemblyTenant, gateway,
// Session and WebSocket/RPC paths. Publication invokes real tenant activation
// and sends reload through the normal Control/gateway RPC.
import {env,runInDurableObject,evictDurableObject} from 'cloudflare:test';
import {expect,it} from 'vitest';
import {releases} from '../releases';
import {manifest as base,files} from './catalog';
import {plans,type PublicationControl,type PublicationTenant} from './worker';
import {buildReleasePlan,migrationsFromFiles} from '../../migrate';
import type {ReleaseSummary} from '../../../control/operator/release';

it('completed publication activates an existing tenant and notifies tenant/control-only sockets after Control eviction',async()=>{
 const bindings=env as unknown as {PUB_CONTROL:DurableObjectNamespace<PublicationControl>;PUB_TENANT:DurableObjectNamespace<PublicationTenant>;PUB_GATEWAY:DurableObjectNamespace};
 const project='publication-fixture',tenant='publication-'+crypto.randomUUID(),control=bindings.PUB_CONTROL.getByName('control');
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
  const first=catalog(1);releases[project]=first;
  const plan=await buildReleasePlan(await migrationsFromFiles(files));plans.add(plan);
  await bindings.PUB_TENANT.getByName(tenant).activate(project,first.manifest,plan.id);
  await control.devCreateProject({id:project,name:'Publication fixture'});await control.devSaveManifest(project,first.manifest);
  await runInDurableObject(control,(_instance,state)=>{
   state.storage.sql.exec("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_name,database_url,status,description,provisioned,runtime_created) VALUES (?,?,?,'Fixture','fixture',?,'active','synthetic',1,1)",tenant,project,tenant,'do://tenant/'+tenant);
  });
  await control.devE2eMember({projectId:project,tenantId:tenant,email:'publication@fixture.invalid',password:'synthetic-test-password'});
  const data=await open();
  await data.call({type:'auth',id:'login-scope',project,controlOnly:true});
  const login=await data.call({type:'action.call',id:'login',idempotencyKey:crypto.randomUUID(),scope:'control',path:'control.auth.passwordLogin',args:{email:'publication@fixture.invalid',password:'synthetic-test-password'}});
  expect(login.type,login.error).toBe('action.result');const token=login.result.accessToken;
  expect((await data.call({type:'auth',id:'data',project,tenant,token})).type).toBe('auth.result');
  const onlyControl=await open();expect((await onlyControl.call({type:'auth',id:'control',project,controlOnly:true,token})).type).toBe('auth.result');
  const generation=async(id:string)=>data.call({type:'reducer.call',id,path:'probe.generation',args:{}});
  expect(await generation('v1')).toMatchObject({type:'reducer.result',result:{generation:1}});
  // The real holder registrations must survive the code-update boundary.
  await evictDurableObject(control);
  const publish=async(n:number,contract:number|null=null)=>{
   const release=catalog(n,contract);releases[project]=release;
   const summary:ReleaseSummary={project,hash:release.hash,artifactGeneration:n,functionCount:1,schemaDefinition:{},migrations:[],controlMigrations:[],planId:null,
    clientContract:contract===null?null:{version:contract,offlineMaxAgeMs:null},crons:[],invitationAcceptanceReducer:'',storedManifest:release.manifest,bundleBytes:0};
   const id=crypto.randomUUID();
   await runInDurableObject(control,(instance)=>{
    const owner=instance.syncs.deployments.acquire(project,id);
    if(!owner.acquired)throw new Error('publication fixture could not acquire deployment');
    instance.syncs.deployments.catalog(id,owner.fence,[{project,hash:release.hash,generation:n}]);
    instance.syncs.deployments.staged(id,owner.fence,'bootstrap');
    instance.sql.run("INSERT INTO gonvex_cf_releases(project_id,hash,summary) VALUES($1,$2,$3)",project,release.hash,JSON.stringify(summary));
    instance.sql.run("INSERT INTO gonvex_cf_publications(sync_id,project_id,hash,generation,version,tenants) VALUES($1,$2,$3,$4,'bootstrap',$5)",id,project,release.hash,n,JSON.stringify([tenant]));
   });
   expect(await control.publishRelease(id)).toEqual({ready:true});
   return release;
  };
  const second=await publish(2);
  for(const socket of [data,onlyControl])expect(await socket.next(frame=>frame.type==='system.reload')).toMatchObject({artifactHash:second.hash});
  expect(await generation('v2')).toMatchObject({type:'reducer.result',result:{generation:2}});
  const third=await publish(3,2);
  expect(await data.next(frame=>frame.type==='client.updateRequired')).toMatchObject({contract:2});
  expect(await onlyControl.next(frame=>frame.type==='system.reload')).toMatchObject({artifactHash:third.hash});
 }finally{
  for(const socket of sockets)if(socket.readyState<2)socket.close(1000);
  delete releases[project];
 }
});
