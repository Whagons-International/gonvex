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

it('R1 every Control membership writer and both invitation branches refuse between real migration chunks, then retry normally',async()=>fixture(async(object,state)=>{
 const owner=await object.writeMembership({kind:'provision',commandId:'owner',accountId:'owner',displayName:'Owner',role:'owner',permissions:{}});
 for(const accountId of ['developerRemove','demoDelete'])await object.writeMembership({kind:'provision',commandId:'seed-'+accountId,accountId,displayName:accountId,role:'member',permissions:{}});
 const writes:import('../../../control/src/tenant-port').MembershipWrite[]=[
  {kind:'provision',commandId:'provision',accountId:'provision',displayName:'P',role:'member',permissions:{}},
  {kind:'developerProvision',commandId:'developerProvision',accountId:'developerProvision',displayName:'D',avatarUrl:''},
  {kind:'developerRemove',commandId:'developerRemove',accountId:'developerRemove'},
  {kind:'tenantOwner',commandId:'tenantOwner',accountId:'tenantOwner',displayName:'O',avatarUrl:''},
  {kind:'demoCreate',commandId:'demoCreate',accountId:'demoCreate',displayName:'Demo'},
  {kind:'demoDelete',commandId:'demoDelete',accountId:'demoDelete'},
  {kind:'e2eMember',commandId:'e2eMember',accountId:'e2eMember',displayName:'E',avatarUrl:''},
 ];
 const invitation=(id:string,internal:boolean):import('../../../control/src/tenant-port').InvitationAcceptance=>({
  commandId:'invitation:'+id,invitationId:id,accountId:'account-'+id,accountEmail:id+'@fixture.invalid',accountName:id,accountAvatarUrl:'',idempotencyKey:'key-'+id,
  role:'member',permissions:{},teamIds:[],payload:{},inviterAccountId:'owner',inviterMemberId:owner.memberId,moduleAvailable:true,
  reducerPath:internal?'probe.invite':'',reducerDefinition:internal?{kind:'reducer',internal:true}:null,artifactHash:manifest.module.hash,
 });
 const replay=invitation('prior',true),prior=await object.acceptInvitation(replay);
 expect(prior.ok).toBe(true);
 const upgrade=await buildReleasePlan(await migrationsFromFiles({...files,'migrations/0002_membership_gate.sql':'-- gonvex:scope tenant\n'+Array.from({length:40},(_,i)=>`INSERT INTO board (id,title,position,bumps) VALUES ('membership-migration-${i}','migration',0,0);`).join('\n')}),{chunk:{maxStatements:1}});
 plans.add(upgrade);let status=await object.kernel.migrator.applyRelease(upgrade.id);
 for(let n=0;n<20&&!(status.phase==='migrations'&&status.chunk>0);n++)status=await object.kernel.migrator.alarm();
 expect(status).toMatchObject({state:'running',phase:'migrations',gateClosed:true});
 const members=state.storage.sql.exec('SELECT * FROM members ORDER BY id').toArray();
 const transactions=state.storage.sql.exec('SELECT count(*) n FROM _gonvex_sync_transactions').one();
 const outbox=state.storage.sql.exec('SELECT * FROM _gonvex_control_plane_membership_outbox ORDER BY account_id').toArray();
 for(const write of writes)await expect(object.writeMembership(write)).rejects.toThrow('tenant schema migration in progress; retry');
 for(const internal of [false,true])await expect(object.acceptInvitation(invitation(internal?'internal':'captured',internal))).rejects.toThrow('tenant schema migration in progress; retry');
 expect(await object.acceptInvitation(replay)).toEqual(prior); // stored result; no application invocation or mutation
 expect(state.storage.sql.exec('SELECT * FROM members ORDER BY id').toArray()).toEqual(members);
 expect(state.storage.sql.exec('SELECT count(*) n FROM _gonvex_sync_transactions').one()).toEqual(transactions);
 expect(state.storage.sql.exec('SELECT * FROM _gonvex_control_plane_membership_outbox ORDER BY account_id').toArray()).toEqual(outbox);
 expect(state.storage.sql.exec("SELECT count(*) n FROM board WHERE id IN ('invite-internal','invite-captured')").one().n).toBe(0);
 expect(state.storage.sql.exec("SELECT count(*) n FROM _gonvex_reducer_idempotency WHERE idempotency_key IN ('key-internal','key-captured')").one().n).toBe(0);
 for(let n=0;n<200&&object.kernel.migrator.gateClosed();n++)await object.kernel.migrator.alarm();
 expect(object.kernel.migrator.gateClosed()).toBe(false);
 for(const write of writes)expect((await object.writeMembership(write)).updated).toBe(true);
 for(const internal of [false,true]){
  const request=invitation(internal?'internal':'captured',internal),accepted=await object.acceptInvitation(request);
  expect(accepted.ok).toBe(true);expect(await object.acceptInvitation(request)).toEqual(accepted);
 }
 const failed=invitation('rollback',true);failed.payload={fail:true};
 expect(await object.acceptInvitation(failed)).toMatchObject({ok:false,release:true});
 expect(state.storage.sql.exec("SELECT count(*) n FROM members WHERE account_id='account-rollback'").one().n).toBe(0);
 expect(state.storage.sql.exec("SELECT count(*) n FROM board WHERE id='invite-rollback'").one().n).toBe(0);
 expect(state.storage.sql.exec("SELECT count(*) n FROM _gonvex_reducer_idempotency WHERE idempotency_key='key-rollback'").one().n).toBe(0);
 expect(await object.acceptInvitation({...failed,payload:{}})).toMatchObject({ok:true});
}));

it('R1 real Control-to-Tenant RPC retains the invitation claim without acceptance/index effects, then same-key retry completes',async()=>{
 for(const internal of [false,true]){
  const control=(env as unknown as {REAL_CONTROL:DurableObjectNamespace}).REAL_CONTROL.getByName('claim-'+crypto.randomUUID()) as unknown as import('../../../control/src/control-object').ControlObject;
  await control.devCreateProject({id:'p',name:'Membership gate fixture'});
  await control.devSaveManifest('p',{...manifest,module:{...manifest.module,invitationAcceptanceReducer:internal?'probe.invite':''}});
  const owner=await control.devE2eMember({projectId:'p',email:'owner@fixture.invalid',password:'synthetic-test-password',accountOnly:true});
  const invitee=await control.devE2eMember({projectId:'p',email:'invitee@fixture.invalid',password:'synthetic-test-password',accountOnly:true});
  const ownerId=owner.accountId,inviteeId=invitee.accountId;
  if(!ownerId||!inviteeId)throw new Error('synthetic accounts were not created');
  let tenantId='';
  await fixture(async(object,state)=>{
    tenantId=state.id.name!;
    await object.writeMembership({kind:'provision',commandId:'seed-owner',accountId:ownerId,displayName:'Owner',role:'owner',permissions:{}});
    const upgrade=await buildReleasePlan(await migrationsFromFiles({...files,'migrations/0002_control_gate.sql':'-- gonvex:scope tenant\n'+Array.from({length:40},(_,i)=>`INSERT INTO board (id,title,position,bumps) VALUES ('control-migration-${i}','migration',0,0);`).join('\n')}),{chunk:{maxStatements:1}});
    plans.add(upgrade);let status=await object.kernel.migrator.applyRelease(upgrade.id);
    for(let n=0;n<20&&!(status.phase==='migrations'&&status.chunk>0);n++)status=await object.kernel.migrator.alarm();
    expect(status).toMatchObject({state:'running',phase:'migrations',gateClosed:true});
  });
  const token='synthetic-invitation-'+crypto.randomUUID(),invitationId='gate-invitation';
  const {sha256Hex}=await import('../../capture/digest');
  await runInDurableObject(control as never,(_object,state)=>{
    state.storage.sql.exec("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_name,database_url,status,description,provisioned,runtime_created) VALUES (?,'p',?,'Fixture','fixture',?,'active','synthetic',1,1)",tenantId,tenantId,'do://tenant/'+tenantId);
    state.storage.sql.exec("INSERT INTO gonvex_auth_membership_invitations(project_id,tenant_id,email,role,invited_by,id,token_hash) VALUES ('p',?,'invitee@fixture.invalid','member',?,?,?)",tenantId,owner.accountId,invitationId,sha256Hex(token));
  });
  const connection:import('../../../control/src/types').ControlConnection={connectionId:'fixture-recipient',projectId:'p',identity:{projectId:'p',account:{id:inviteeId,email:'invitee@fixture.invalid',emailVerified:true,name:'Invitee',avatarUrl:'',provider:'password'}},tenant:null,impersonationId:'',authToken:''};
  const refused=await control.write(connection,'control.invitations.accept',{token},'same-key','reducer');
  expect(refused).toMatchObject({ok:false,errorClass:'transient',error:expect.stringContaining('tenant schema migration in progress; retry')});
  await runInDurableObject(control as never,(_object,state)=>{
    expect(state.storage.sql.exec('SELECT handoff_state,accepted_at,completed_at,accepted_account_id,accepted_idempotency_key FROM gonvex_auth_membership_invitations WHERE id=?',invitationId).one()).toEqual({handoff_state:'claimed',accepted_at:null,completed_at:null,accepted_account_id:invitee.accountId,accepted_idempotency_key:'same-key'});
    expect(state.storage.sql.exec('SELECT count(*) n FROM account_tenant_index WHERE account_id=?',invitee.accountId).one().n).toBe(0);
  });
  const tenant=bindings.TENANT.getByName(tenantId);
  await runInDurableObject(tenant,async(object,state)=>{
    const actual=object as CompositionTenant;
    expect(state.storage.sql.exec('SELECT count(*) n FROM members WHERE account_id=?',invitee.accountId).one().n).toBe(0);
    expect(state.storage.sql.exec("SELECT count(*) n FROM board WHERE id='invite-gate-invitation'").one().n).toBe(0);
    expect(actual.kernel.migrator.gateClosed()).toBe(true);
    for(let n=0;n<200&&actual.kernel.migrator.gateClosed();n++)await actual.kernel.migrator.alarm();
    expect(actual.kernel.migrator.gateClosed()).toBe(false);
  });
  const accepted=await control.write(connection,'control.invitations.accept',{token},'same-key','reducer');
  expect(accepted.ok).toBe(true);expect(await control.write(connection,'control.invitations.accept',{token},'same-key','reducer')).toEqual(accepted);
  await runInDurableObject(control as never,(_object,state)=>{
    expect(state.storage.sql.exec('SELECT handoff_state,accepted_at IS NOT NULL accepted,completed_at IS NOT NULL completed FROM gonvex_auth_membership_invitations WHERE id=?',invitationId).one()).toEqual({handoff_state:'completed',accepted:1,completed:1});
    expect(state.storage.sql.exec('SELECT count(*) n FROM account_tenant_index WHERE account_id=? AND tenant_id=?',invitee.accountId,tenantId).one().n).toBe(1);
  });
 }
});
