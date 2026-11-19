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
it('N3 the direct-call runner retains trusted internal access and system root provenance',async()=>fixture(async(object,state)=>{
 const grant:ServiceGrant={principalId:'workflow-service',route:{projectId:'p',tenantId:state.id.name!,databaseUrl:'sqlite://test'},functions:['probe.serviceAgent'],maxDelegationSeconds:0,channel:'system'};
 const answered=await object.serviceCall(grant,{kind:'action',id:'workflow-root',path:'probe.serviceAgent',args:{},reply:{host:'fixture',connectionId:'already-abandoned-physical-owner',epoch:1,generation:1,id:'workflow-root',path:'probe.serviceAgent'}});
 // X13: the run is durable but runs inline, as Rust runs it; its answer comes back in this call.
 expect(answered.ok).toBe(true);
 expect(JSON.parse(String((answered as {result:string}).result))).toMatchObject({value:42,channel:'system',rootChannel:'system',command:'workflow-root',root:'workflow-root'});
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
it('X03 DELETE /dev/tenants: the tenant reset after a completed wipe is success, not a 500 that strands the row in deleting',async()=>{
 const {deleteTenantStorage}=await import('../../../control/operator/tenant/port');
 const seed=async()=>{
  const name=`assembly-delete-${crypto.randomUUID()}`,stub=bindings.TENANT.getByName(name);
  await runInDurableObject(stub,async(object)=>{
   const release=await buildReleasePlan(await migrationsFromFiles(files));plans.add(release);
   await (object as CompositionTenant).activate('p',manifest,release.id);
  });
  return name;
 };
 const userTables=(name:string)=>runInDurableObject(bindings.TENANT.getByName(name),(_o,state)=>state.storage.sql.exec("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name IN ('board','gonvex_migrations')").one().n as number);
 // The object's own ctx.abort(TENANT_DELETED_REASON) rejects the raw RPC after deleteAll succeeded (previously a 500 that left the row 'deleting').
 // Control's deleteTenantStorage resolves for that exact reset and the storage is gone.
 const wrapped=await seed();expect(await userTables(wrapped)).toBeGreaterThan(0);
 await expect(deleteTenantStorage(bindings.TENANT.getByName(wrapped) as never)).resolves.toBeUndefined();
 expect(await userTables(wrapped)).toBe(0);
 // Any other failure still propagates.
 await expect(deleteTenantStorage({operatorDeleteAll:async()=>{throw new Error('storage unavailable');}})).rejects.toThrow('storage unavailable');
});
it('X03 the tenant wipe is made durable (storage.sync) before the reset that ends operatorDeleteAll',async()=>{
 const {TENANT_DELETED_REASON}=await import('../../../control/operator/tenant/port');
 const stub=bindings.TENANT.getByName(`assembly-wipe-${crypto.randomUUID()}`);
 await runInDurableObject(stub,async(object,state)=>{
  const release=await buildReleasePlan(await migrationsFromFiles(files));plans.add(release);
  await (object as CompositionTenant).activate('p',manifest,release.id);
  const order:string[]=[],storage=state.storage as unknown as Record<string,(...a:unknown[])=>unknown>,ctx=state as unknown as Record<string,(...a:unknown[])=>unknown>;
  for(const name of ['deleteAlarm','deleteAll','sync']){const real=storage[name]!.bind(state.storage);storage[name]=async(...a:unknown[])=>{order.push(name);return real(...a);};}
  // Record instead of resetting: the real reset is covered by the DELETE /dev/tenants test above.
  ctx.abort=(reason:unknown)=>{order.push('abort');throw new Error(String(reason));};
  await expect((object as unknown as {operatorDeleteAll():Promise<void>}).operatorDeleteAll()).rejects.toThrow(TENANT_DELETED_REASON);
  expect(order).toEqual(['deleteAlarm','deleteAll','sync','abort']);
  expect(state.storage.sql.exec("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name IN ('board','gonvex_migrations')").one().n).toBe(0);
 });
});
it('X12b DEV e2e member seed: defaults stay Rust\'s admin/{"e2e":true}; role/permissions overrides and the read go through real Control and tenant RPC',async()=>{
 const control=(env as unknown as {REAL_CONTROL:DurableObjectNamespace}).REAL_CONTROL.getByName('e2e-grants-'+crypto.randomUUID()) as unknown as import('../../../control/src/control-object').ControlObject;
 await control.devCreateProject({id:'p',name:'e2e grants fixture'});
 await control.devSaveManifest('p',manifest);
 let tenantId='';
 await fixture(async(_object,state)=>{tenantId=state.id.name!;});
 await runInDurableObject(control as never,(_object,state)=>{
  state.storage.sql.exec("INSERT INTO gonvex_runtime_tenants(relationship_id,project_id,tenant_id,name,database_name,database_url,status,description,provisioned,runtime_created) VALUES (?,'p',?,'Fixture','fixture',?,'active','synthetic',1,1)",tenantId,tenantId,'do://tenant/'+tenantId);
 });
 const seed=(extra:{role?:string;permissions?:Record<string,unknown>}={})=>control.devE2eMember({projectId:'p',tenantId,email:'Walkthrough@Fixture.invalid',password:'synthetic-test-password',...extra});
 const read=(selector:{accountId?:string;email?:string;memberId?:string})=>control.devE2eMemberRead({projectId:'p',tenantId,...selector});
 const stored=async()=>runInDurableObject(bindings.TENANT.getByName(tenantId),(_object,state)=>state.storage.sql.exec("SELECT id,role,permissions,membership_revision FROM members").toArray());

 const first=await seed();
 expect(await read({accountId:first.accountId!})).toEqual({projectId:'p',tenantId,accountId:first.accountId,memberId:first.memberId,status:'active',role:'admin',permissions:{e2e:true}});
 expect(await stored()).toEqual([{id:first.memberId,role:'admin',permissions:'{"e2e":true}',membership_revision:1}]);

 // Copy a real Member's grants: same Member row, new revision, exact JSON.
 const copied={'config.integrations':true,'settings.access':true,e2e:true};
 const granted=await seed({role:'owner',permissions:copied});
 expect(granted.memberId).toBe(first.memberId);
 expect(await read({email:'walkthrough@fixture.invalid'})).toMatchObject({memberId:first.memberId,role:'owner',permissions:copied});
 expect(await read({memberId:first.memberId!})).toMatchObject({accountId:first.accountId,role:'owner',permissions:copied});
 expect(await stored()).toEqual([{id:first.memberId,role:'owner',permissions:JSON.stringify(copied),membership_revision:2}]);

 // Omitting both restores exactly today's row.
 await seed();
 expect(await stored()).toEqual([{id:first.memberId,role:'admin',permissions:'{"e2e":true}',membership_revision:3}]);
 // Only one of the two: the other keeps its default.
 await seed({permissions:{}});
 expect(await read({accountId:first.accountId!})).toMatchObject({role:'admin',permissions:{}});

 expect(await read({accountId:'acct_missing'})).toBeNull();
 expect(await read({email:'nobody@fixture.invalid'})).toBeNull();
 expect(await read({memberId:'member_missing'})).toBeNull();
});
it('BUG-8 a row an Action writes through its nested Reducer tool is a committed, captured change like any Reducer write',async()=>fixture(async(object,state)=>{
 // apiKeys.create is an Action whose createRecord tool inserts the row; replicas see only committed change-log rows.
 const before=Number(state.storage.sql.exec("SELECT revision FROM _gonvex_sync_clock").one().revision);
 const replies=await object.dispatch(view(state.id.name!),{type:'action.call',id:'create',path:'probe.nestedWrite',args:{id:'bug8-key'}});
 expect(JSON.parse(replies[0]!)).toMatchObject({type:'action.result',result:{denied:false}});
 const changes=[...state.storage.sql.exec("SELECT revision,operation,command_id FROM _gonvex_sync_changes WHERE table_name='board' AND row_id='bug8-key'")];
 expect(changes).toHaveLength(1);
 expect(changes[0]).toMatchObject({operation:'insert'});
 expect(Number(changes[0]!.revision)).toBeGreaterThan(before);
 expect(Number(state.storage.sql.exec("SELECT revision FROM _gonvex_sync_clock").one().revision)).toBe(Number(changes[0]!.revision));
}));
it('X03 a tenant whose provisioning was interrupted before its schema exists still starts (no outbox query before migrations)',async()=>{
 // provision()/operatorApplyRelease() persist the project before the migrations that create the sync
 // infrastructure. A reset in between (a code update during provisioning) left a constructor that activated the
 // project and queried _gonvex_action_outbox on every start, so every call and DELETE /dev/tenants failed.
 const stub=bindings.TENANT.getByName(`assembly-unprovisioned-${crypto.randomUUID()}`);
 await runInDurableObject(stub,async(object,state)=>{
  const tenant=object as CompositionTenant as unknown as {activate(p:string,m:unknown,plan:string|null):Promise<unknown>;setupScheduled():Promise<void>;scheduler?:unknown;kernel:{module?:unknown}};
  const outbox=()=>state.storage.sql.exec("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name='_gonvex_action_outbox'").one().n;
  await tenant.activate('p',manifest,null);
  expect(tenant.kernel.module).toBeTruthy();
  expect(outbox()).toBe(0);
  await expect(tenant.setupScheduled()).resolves.toBeUndefined();
  expect(tenant.scheduler).toBeUndefined();
  // The normal provisioning path still installs the schema and then the scheduler.
  const release=await buildReleasePlan(await migrationsFromFiles(files));plans.add(release);
  await (object as CompositionTenant).activate('p',manifest,release.id);
  expect(outbox()).toBe(1);
  await tenant.setupScheduled();
  expect(tenant.scheduler).toBeDefined();
 });
});
it('X03 a tenant on a Worker version whose catalog lacks the release does not report its plan done without a module',async()=>{
 // Run8: a sync's rollout reached tenant objects still on the previous runtime version. They finished the
 // plan (its chunks come from the plan store), found no baked release, skipped activation and answered
 // `done`, so the sync succeeded while every call answered "no active TypeScript module" for about a minute.
 const {waitForRelease,TENANT_RELEASE_PENDING}=await import('../../../control/operator/tenant/port');
 const {releases}=await import('../releases');
 const release=await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
 const project=`stale-${crypto.randomUUID()}`;
 const stub=bindings.TENANT.getByName(`assembly-stale-${crypto.randomUUID()}`);
 const port=stub as unknown as import('../../../control/operator/tenant/port').OperatorTenantPort;
 // A native promise: vitest probing an RpcPromise's properties would pipeline extra (unhandled) calls.
 const status=async()=>{await port.operatorMigrationStatus();};
 const served=()=>runInDurableObject(stub,(object)=>!!(object as unknown as {kernel:{module?:unknown}}).kernel.module);
 // This version's catalog has no release for the project: the waiter keeps retrying until its deadline.
 await expect(waitForRelease(port,release.id,project,Date.now()+1500,20)).rejects.toThrow(TENANT_RELEASE_PENDING);
 await expect(status()).rejects.toThrow(TENANT_RELEASE_PENDING);
 expect(await served()).toBe(false);
 // A catalog release built from a different plan is the same stale code.
 releases[project]={project,hash:'h',generation:1,manifest:{...manifest,module:{...(manifest as {module:object}).module,release:{planId:'other-plan'}}},exports:{} as never};
 await expect(status()).rejects.toThrow(TENANT_RELEASE_PENDING);
 // Once the serving version bakes the release of this plan, the same wait activates it and reports done.
 releases[project]={project,hash:'h',generation:1,manifest:{...manifest,module:{...(manifest as {module:object}).module,release:{planId:release.id}}},exports:{} as never};
 try {
  await expect(waitForRelease(port,release.id,project,Date.now()+5000,20)).resolves.toMatchObject({state:'done',planId:release.id});
  expect(await served()).toBe(true);
 } finally { delete releases[project]; }
});
it('X03 dev fixture: service principals override merges with the environment and survives a new Control instance',async()=>{
 const {withFixturePrincipals,FIXTURE_PRINCIPALS_KEY}=await import('../../../control/src/control-object');
 const envList=[{id:'gateway',tokenSha256:'a'.repeat(64),projects:['p'],functions:['f'],maxDelegationSeconds:0,manifest:false},{id:'app',tokenSha256:'b'.repeat(64),projects:['q'],functions:['g'],maxDelegationSeconds:0,manifest:false}] as never;
 const fixture=JSON.stringify([{id:'gateway',tokenSha256:'c'.repeat(64),projects:['p2'],functions:['h']},{id:'reader',tokenSha256:'d'.repeat(64),projects:['p2'],functions:['h']}]);
 const merged=withFixturePrincipals(envList,fixture) as Array<{id:string;tokenSha256:string}>;
 expect(merged.map(p=>p.id).sort()).toEqual(['app','gateway','reader']);
 expect(merged.find(p=>p.id==='gateway')!.tokenSha256).toBe('c'.repeat(64));
 expect(withFixturePrincipals(envList,undefined)).toBe(envList);
 expect(()=>withFixturePrincipals(envList,'[{"id":""}]')).toThrow(/GONVEX_SERVICE_PRINCIPALS is invalid/);
 // The real Control object: applied at once, persisted, and read again by a fresh instance.
 const name='principals-'+crypto.randomUUID();
 const control=(env as unknown as {REAL_CONTROL:DurableObjectNamespace}).REAL_CONTROL.getByName(name) as unknown as {fixtureServicePrincipals(p:string):Promise<{count:number}|{error:string}>};
 expect((await control.fixtureServicePrincipals(fixture) as {count:number}).count).toBeGreaterThanOrEqual(2);
 await runInDurableObject(control as never,(object,state)=>{
  expect(state.storage.kv.get(FIXTURE_PRINCIPALS_KEY)).toBe(fixture);
  expect((object as unknown as {config:{servicePrincipals:Array<{id:string}>}}).config.servicePrincipals.map(p=>p.id)).toEqual(expect.arrayContaining(['gateway','reader']));
 });
 expect(await control.fixtureServicePrincipals('not json')).toEqual({error:expect.stringMatching(/GONVEX_SERVICE_PRINCIPALS is invalid/)});
 await runInDurableObject(control as never,(_o,state)=>{expect(state.storage.kv.get(FIXTURE_PRINCIPALS_KEY)).toBe(fixture);});
});
it('X03 dev fixture routes: gateway restart and service principals are 404 without the fixture key',async()=>{
 const {fixtureControl}=await import('../fixture-control');
 for(const path of ['/__assembly/gateway-restart','/__assembly/service-principals']){
  const request=()=>new Request('https://local'+path,{method:'POST',headers:{authorization:'Bearer guess'},body:JSON.stringify({holder:'gw-0',principals:'[]'})});
  expect((await fixtureControl(request(),{TENANT:bindings.TENANT,GATEWAY:bindings.TENANT,GONVEX_ASSEMBLY_FIXTURE_KEY:'real'} as never))!.status).toBe(404);
  expect((await fixtureControl(request(),{TENANT:bindings.TENANT,GATEWAY:bindings.TENANT} as never))!.status).toBe(404);
 }
});
it('X03 dev fixture route: purging revoked tokens is 404 without the exact fixture key',async()=>{
 const {fixtureControl}=await import('../fixture-control');
 const request=()=>new Request('https://local/__assembly/purge-revoked-tokens',{method:'POST',headers:{authorization:'Bearer guess'},body:'{}'});
 expect((await fixtureControl(request(),{TENANT:bindings.TENANT,GATEWAY:bindings.TENANT,GONVEX_ASSEMBLY_FIXTURE_KEY:'real'} as never))!.status).toBe(404);
 expect((await fixtureControl(request(),{TENANT:bindings.TENANT,GATEWAY:bindings.TENANT} as never))!.status).toBe(404);
});
it('X13c an Action tool Reducer answers after its commit, not behind serial work its commit queued; that work still runs in order',async()=>fixture(async(object,state)=>{
 const session=systemSession('p',state.id.name!);
 let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});const order:string[]=[];
 // Stands in for the commit's delivery or a replica open: serial work queued right after the commit.
 const stop=object.kernel.capture.wake.subscribe(()=>{void object.kernel.reducers.serial(async()=>{order.push('queued:start');await gate;order.push('queued:end');});});
 try{
  const before=Number(state.storage.sql.exec('SELECT revision FROM _gonvex_sync_clock WHERE singleton=1').one().revision);
  let answered=false;
  const call=object.call({v:1,function:'probe.write',kind:'reducer',args:{id:'x13c'},session,provenance:directProvenance(session,'agent','x13c',manifest.module.hash),allowInternal:true}).then(r=>{answered=true;order.push('answered');return r;});
  for(let i=0;i<100&&!answered;i++)await new Promise(r=>setTimeout(r,5));
  expect(answered).toBe(true);
  const report=await call;
  expect(report).toMatchObject({ok:true});
  // The answer carries the commit's revision, so the socket's watermark still covers it.
  expect((report as {committedRevision?:number}).committedRevision).toBe(before+1);
  expect(state.storage.sql.exec("SELECT count(*) n FROM board WHERE id='x13c'").one().n).toBe(1);
  release();
  // The queued work and the scheduler notification still run, after the commit, in queue order.
  await object.kernel.reducers.serial(async()=>{order.push('after');});
  expect(order.indexOf('answered')).toBeLessThan(order.indexOf('queued:end'));
  expect(order.slice(-2)).toEqual(['queued:end','after']);
  expect(order).toContain('queued:start');
 }finally{release();stop();}
}));
it('X14 a woken tenant whose stored module came from this baked release neither rewrites nor re-activates it',async()=>{
 // Dev cold sign-in: every cold tenant wake wrote the whole manifest twice (restore, then activate) and parsed it
 // twice before Control's `admit` could run. A woken object holding this exact release now writes nothing.
 const {releases}=await import('../releases');
 const {MODULE_KEY}=await import('../../reducer/tenant');
 const {exports}=await import('./catalog');
 const {fixtureSpans}=await import('../../delivery/spans');
 const project=`wake-${crypto.randomUUID()}`;
 releases[project]={project,hash:(manifest as {module:{hash:string}}).module.hash,generation:1,manifest,exports};
 const stub=bindings.TENANT.getByName(`assembly-wake-${crypto.randomUUID()}`);
 try {
  await runInDurableObject(stub,async(object,state)=>{
   const release=await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
   await (object as CompositionTenant).activate(project,manifest,release.id);
   state.storage.kv.put('assembly/project',project);
   const puts:string[]=[];const kv=state.storage.kv as unknown as {put(k:string,v:unknown):void};const put=kv.put.bind(kv);
   kv.put=(k,v)=>{puts.push(k);put(k,v);};
   fixtureSpans(true);
   const Ctor=object.constructor as new(ctx:DurableObjectState,env:unknown)=>CompositionTenant;
   const wake=async()=>{const woken=new Ctor(state,(object as unknown as {env:unknown}).env);await new Promise(r=>setTimeout(r,50));return woken;};
   // First wake after activation by another writer: restore stores the marked form once, activation is skipped.
   const first=await wake();
   expect(puts.filter(k=>k===MODULE_KEY)).toHaveLength(1);
   expect((state.storage.kv.get(MODULE_KEY) as {source?:string}).source).toBeTruthy();
   expect((first as unknown as {kernel:{module?:{projectId:string}}}).kernel.module?.projectId).toBe(project);
   // Every later wake writes nothing and does not activate again.
   puts.length=0;
   const second=await wake();
   expect(puts.filter(k=>k===MODULE_KEY)).toHaveLength(0);
   expect((second as unknown as {kernel:{module?:{projectId:string}}}).kernel.module?.projectId).toBe(project);
   const wakes=fixtureSpans(true).filter(l=>l.name==='tenant.wake');
   expect(wakes.map(l=>l.fields.activated)).toEqual([false,false]);
   expect(wakes.every(l=>typeof l.fields.restoreMs==='number'&&typeof l.fields.activateMs==='number')).toBe(true);
   // A different baked release (new generation) is stored and served again.
   releases[project]={...releases[project]!,generation:2};
   puts.length=0;
   await wake();
   expect(puts.filter(k=>k===MODULE_KEY)).toHaveLength(1);
   // Spans off (the default): a wake logs nothing.
   fixtureSpans(false);
   const logged:unknown[]=[];const log=console.log;console.log=(...a:unknown[])=>{logged.push(a);};
   try { await wake(); } finally { console.log=log; }
   expect(logged.filter(a=>String((a as unknown[])[0]).includes('tenant.wake'))).toEqual([]);
   kv.put=put;
  });
 } finally { fixtureSpans(false); delete releases[project]; }
});
it('X15 a woken tenant answers admit without evaluating the app bundle; the first Reducer call evaluates it once',async()=>{
 // Each sign-in re-admits the account on every tenant it belongs to. A cold tenant used to evaluate the release's
 // whole app bundle (several MB on dev) in its constructor before `admit`, one indexed SELECT, could answer.
 const {releases}=await import('../releases');
 const {exports}=await import('./catalog');
 const project=`lazy-${crypto.randomUUID()}`;let evaluated=0;
 releases[project]={project,hash:(manifest as {module:{hash:string}}).module.hash,generation:1,manifest,get exports(){evaluated++;return exports;}};
 const stub=bindings.TENANT.getByName(`assembly-lazy-${crypto.randomUUID()}`);
 try {
  await runInDurableObject(stub,async(object,state)=>{
   const release=await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
   await (object as CompositionTenant).activate(project,manifest,release.id);
   state.storage.kv.put('assembly/project',project);
   // The test class serves a fixed bundle; this wake uses AssemblyTenant's own release lookup.
   const {AssemblyTenant}=await import('../tenant');
   const real=(AssemblyTenant.prototype as unknown as {bundle(hash:string):unknown}).bundle;
   const Ctor=object.constructor as new(ctx:DurableObjectState,env:unknown)=>CompositionTenant;
   class Woken extends Ctor { protected override bundle(hash?:string){ return real.call(this,hash!) as never; } }
   evaluated=0;
   const woken=new Woken(state,(object as unknown as {env:unknown}).env);
   await new Promise(r=>setTimeout(r,50));
   expect(await woken.admit('no-such-account')).toBeNull();
   expect(evaluated).toBe(0);
   const session=systemSession(project,state.id.name!);
   const report=await woken.call({v:1,function:'probe.write',kind:'reducer',args:{id:'x15-lazy'},session,provenance:directProvenance(session,'agent','x15-lazy',manifest.module.hash),allowInternal:true});
   expect(report).toMatchObject({ok:true});
   expect(evaluated).toBe(1);
   await woken.call({v:1,function:'probe.write',kind:'reducer',args:{id:'x15-lazy-2'},session,provenance:directProvenance(session,'agent','x15-lazy-2',manifest.module.hash),allowInternal:true});
   expect(evaluated).toBe(1);
  });
 } finally { delete releases[project]; }
});
it('X17f the membership projection ack keeps a write committed while the push was in flight (#530 re-review)',async()=>fixture(async(object,state)=>{
 const sql=state.storage.sql;
 sql.exec("INSERT INTO members(id,account_id,status,role,membership_revision) VALUES('m-x17f','acct-x17f','active','admin',1)");
 const self=object as unknown as {env:{CONTROL:unknown};projectMemberships():Promise<number>};
 const real=self.env.CONTROL;
 const pushed:unknown[][]=[];
 const control=(during?:()=>void)=>({getByName:()=>({projectMemberships:async(_p:string,_t:string,rows:unknown[])=>{pushed.push(rows);during?.();return rows.length;}})});
 const outbox=()=>sql.exec("SELECT status,membership_revision FROM _gonvex_control_plane_membership_outbox WHERE account_id='acct-x17f'").toArray();
 try{
  // A Reducer write (no revision bump) commits while the push is in flight: its outbox row must survive the ack.
  self.env.CONTROL=control(()=>sql.exec("UPDATE members SET status='revoked' WHERE id='m-x17f'"));
  await self.projectMemberships();
  expect(outbox()).toEqual([{status:'revoked',membership_revision:1}]);
  // The next push carries it, and an untouched push is acked.
  self.env.CONTROL=control();
  await self.projectMemberships();
  expect(pushed.at(-1)).toContainEqual({accountId:'acct-x17f',memberId:'m-x17f',status:'revoked',membershipRevision:1});
  expect(outbox()).toEqual([]);
 }finally{self.env.CONTROL=real;}
}));
