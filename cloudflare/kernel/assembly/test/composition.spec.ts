// Synthetic module and explicit test principals, actual AssemblyTenant/K04/K12
// execution on DO SQLite. This is module integration, not deployment proof.
import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { buildReleasePlan, migrationsFromFiles } from '../../migrate';
import { manifest, files } from './catalog';
import { plans, type CompositionTenant } from './worker';
import { serviceSession, type ServiceGrant } from '../../principals/grant';
import type { SocketView } from '../../session/ports';
import { systemSession } from '../../../actions/src/session';
import { directProvenance } from '../../../actions/src/provenance';
import { reportOf } from '../../../router/ingress/invoke';
const bindings = env as unknown as {TENANT:DurableObjectNamespace};
async function fixture(run:(object:CompositionTenant,state:DurableObjectState)=>Promise<void>) {
 const stub=bindings.TENANT.getByName(`assembly-${crypto.randomUUID()}`);
 await runInDurableObject(stub,async(object,state)=>{
  const tenant=object as CompositionTenant;
  const release=await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
  await tenant.activate('p',manifest,release.id);
  await (tenant as unknown as {setupScheduled():Promise<void>}).setupScheduled();
  await run(tenant,state);
 });
}
function view(tenantId:string,anonymous=false):SocketView {
 const tenant=serviceSession({principalId:'test',route:{projectId:'p',tenantId,databaseUrl:'sqlite://test'},functions:[],maxDelegationSeconds:0});
 if(anonymous){tenant.anonymous=true;tenant.identity.account.id='';tenant.member.id='';tenant.member.accountId='';}
 return {connectionId:crypto.randomUUID(),host:'fixture',tenant,connection:{projectId:'p',identity:tenant.identity,tenant,impersonationId:'',authToken:''},subscriptionEpoch:1,publicTransport:anonymous,
 negotiated:{snapshotPages:false,noOldValues:false,fieldPatches:false,batchFrames:false,nativeRows:false,setDigest:false,telemetryBatch:false,presence:false}};
}
it('N1 public Action declared tools preserve anonymity and roll back the refused nested invocation',async()=>fixture(async(object,state)=>{
 for(const kind of ['outbox','scheduler']){
  const socket=view(state.id.name!,true);
  const replies=await object.dispatch(socket,{type:'action.call',id:kind,path:'probe.publicTool',args:{id:kind,kind}});
  const reply=JSON.parse(replies[0]!);
  expect(reply).toMatchObject({type:'action.result',result:{denied:true}});
  expect(reply.result.error).toMatch(/undefined|capability/);
 }
 expect(state.storage.sql.exec('SELECT count(*) n FROM board').one().n).toBe(0);
 expect(state.storage.sql.exec('SELECT count(*) n FROM _gonvex_action_outbox').one().n).toBe(0);
 expect(state.storage.sql.exec('SELECT count(*) n FROM gonvex_scheduled_jobs').one().n).toBe(0);
}));
it('N1 catching an absent surface creates neither outbox nor scheduler authority',async()=>fixture(async(object,state)=>{
 for(const kind of ['outbox','scheduler']){
  const replies=await object.dispatch(view(state.id.name!,true),{type:'action.call',id:kind,path:'probe.publicTool',args:{id:kind,kind,catchMissing:true}});
  expect(JSON.parse(replies[0]!)).toMatchObject({type:'action.result',result:{denied:false}});
 }
 expect(state.storage.sql.exec('SELECT count(*) n FROM board').one().n).toBe(2);
 expect(state.storage.sql.exec('SELECT count(*) n FROM _gonvex_action_outbox').one().n).toBe(0);
 expect(state.storage.sql.exec('SELECT count(*) n FROM gonvex_scheduled_jobs').one().n).toBe(0);
}));
it('N2 actual nested and scheduled Reducers refuse writes while a migration remains between chunks',async()=>fixture(async(object,state)=>{
 const upgrade=await buildReleasePlan(await migrationsFromFiles({...files,'migrations/0002_gate.sql':'-- gonvex:scope tenant\n'+Array.from({length:40},(_,i)=>`INSERT INTO board (id,title,position,bumps) VALUES ('migration-${i}','migration',0,0);`).join('\n')}),{chunk:{maxStatements:1}});
 plans.add(upgrade);
 let status=await object.kernel.migrator.applyRelease(upgrade.id);
 for(let n=0;n<20&&!(status.phase==='migrations'&&status.chunk>0);n++) status=await object.kernel.migrator.alarm();
 expect(status).toMatchObject({state:'running',phase:'migrations',gateClosed:true});
 expect(object.kernel.migrator.gateClosed()).toBe(true);
 const replies=await object.dispatch(view(state.id.name!),{type:'action.call',id:'nested',path:'probe.nestedWrite',args:{id:'must-not-write'}});
 expect(JSON.parse(replies[0]!)).toMatchObject({type:'action.result',result:{denied:true}});
 expect(JSON.parse(replies[0]!).result.error).toContain('tenant schema migration in progress; retry');
 const session=systemSession('p',state.id.name!);
 const scheduled=await (object as unknown as {runScheduledReducer(input:unknown):Promise<unknown>}).runScheduledReducer({actor:{kind:'system'},function:'probe.write',args:{id:'scheduled-must-not-write'},provenance:directProvenance(session,'scheduler','scheduled',manifest.module.hash)});
 expect(scheduled).toMatchObject({ok:false,error:'module database capability failed: tenant schema migration in progress; retry'});
 expect(state.storage.sql.exec("SELECT count(*) n FROM board WHERE id IN ('must-not-write','scheduled-must-not-write')").one().n).toBe(0);
}));
it('N3 allowlisted internal service Actions preserve command/channel and await agent results/failures',async()=>fixture(async(object,state)=>{
 const grant:ServiceGrant={principalId:'ingress',route:{projectId:'p',tenantId:state.id.name!,databaseUrl:'sqlite://test'},functions:['probe.service','probe.serviceAgent'],maxDelegationSeconds:0,channel:'system'};
 for(const path of grant.functions){
  const result=await object.serviceCall(grant,{kind:'action',id:'ingress-command',path,args:{}});
  expect(result.ok).toBe(true);
  expect(reportOf(result)).toEqual({ok:true,value:{value:42,channel:'system',rootChannel:'system',command:'ingress-command',root:'ingress-command',account:'_gonvex_service:ingress'}});
 }
 expect(reportOf(await object.serviceCall(grant,{kind:'action',id:'failed',path:'probe.serviceAgent',args:{fail:true}}))).toMatchObject({ok:false,error:expect.stringContaining('service Action failed')});
 expect(reportOf(await object.serviceCall(grant,{kind:'action',id:'retry',path:'probe.serviceAgent',args:{}}))).toMatchObject({ok:true,value:{value:42,command:'retry'}});
 expect(reportOf({ok:true,result:'null',pending:true})).toMatchObject({ok:false,transient:true});
}));
it('N3 Workflow runner retains trusted internal access and system root provenance',async()=>fixture(async(object,state)=>{
 const grant:ServiceGrant={principalId:'workflow-service',route:{projectId:'p',tenantId:state.id.name!,databaseUrl:'sqlite://test'},functions:['probe.serviceAgent'],maxDelegationSeconds:0,channel:'system'};
 const pending=await object.serviceCall(grant,{kind:'action',id:'workflow-root',path:'probe.serviceAgent',args:{},reply:{host:'fixture',connectionId:'already-abandoned-physical-owner',epoch:1,generation:1,id:'workflow-root',path:'probe.serviceAgent'}});
 expect(pending).toMatchObject({ok:true,pending:true});
 let row:Record<string,SqlStorageValue>|undefined;
 for(let i=0;i<300;i++){
  row=state.storage.sql.exec("SELECT state,result,last_error,session FROM _gonvex_st_action_runs WHERE action_path='probe.serviceAgent'").toArray()[0];
  if(row?.state==='done'&&row.result!==null)break;
  await new Promise(r=>setTimeout(r,10));
 }
 expect(row?.state).toBe('done');expect(row?.last_error).toBeNull();
 expect(JSON.parse(String(row?.result))).toMatchObject({value:42,channel:'system',rootChannel:'system',command:'workflow-root',root:'workflow-root'});
}));
it('actual Action host accepts the 8-MiB result boundary and refuses one byte over it',async()=>fixture(async(object,state)=>{
 const grant:ServiceGrant={principalId:'size',route:{projectId:'p',tenantId:state.id.name!,databaseUrl:'sqlite://test'},functions:['probe.resultSize'],maxDelegationSeconds:0,channel:'system'};
 const accepted=await object.serviceCall(grant,{kind:'action',id:'size',path:'probe.resultSize',args:{bytes:(8<<20)-2}});
 expect(accepted.ok).toBe(true);if(accepted.ok)expect(accepted.result.length).toBe(8<<20);
 const refused=await object.serviceCall(grant,{kind:'action',id:'too-large',path:'probe.resultSize',args:{bytes:(8<<20)-1}});
 expect(refused).toMatchObject({ok:false,error:expect.stringMatching(/result.*(?:limit|exceed)/)});
}));
it('rejects stale real SocketViews rather than acknowledging an empty registration',async()=>fixture(async(object,state)=>{
 const socket=view(state.id.name!);object.delivery.reset(socket.connectionId,9);
 await expect(object.dispatch(socket,{type:'replica.openMany',opens:[]})).rejects.toThrow('stale tenant subscription scope');
 expect(object.delivery.subscriptions(socket.connectionId).size).toBe(0);
}));
