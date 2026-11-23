// Co-tenant prewarm end to end: a real sign-in through a gateway socket (sessionDispatch → passwordLogin) records the
// account's other tenant, and the next page prewarm of the active tenant wakes it too.
import {env} from 'cloudflare:test';
import {expect,it} from 'vitest';
import {files} from './catalog';
import type {AssemblyTenant} from '../tenant';
import type {TestControl,TestGateway} from './stable-worker';
import {buildReleasePlan,migrationsFromFiles} from '../../migrate';
import {R2PlanStore} from '../../migrate/plan';
import type {TestRuntimeCatalog} from './runtime-worker';
import {X13_PROJECT,x13Manifest} from './x13-fixture';
const bindings=env as unknown as {PUB_CONTROL:DurableObjectNamespace<TestControl>;PUB_TENANT:DurableObjectNamespace<AssemblyTenant>;PUB_GATEWAY:DurableObjectNamespace<TestGateway>;RELEASES:R2Bucket;RUNTIME_CATALOG:TestRuntimeCatalog};

async function signIn(tenants:string[],sockets:WebSocket[],before?:(control:any)=>Promise<void>){
  await bindings.RUNTIME_CATALOG.install(X13_PROJECT,x13Manifest,1,'pwe-catalog');
  const plan=await buildReleasePlan(await migrationsFromFiles(files));await new R2PlanStore(bindings.RELEASES).write(plan);
  const control=bindings.PUB_CONTROL.getByName('control');
  await control.devCreateProject({id:X13_PROJECT,name:'X13 fixture'}).catch(()=>undefined);await control.devSaveManifest(X13_PROJECT,x13Manifest);
  const email=`pwe-${tenants[0]!.slice(-8)}@fixture.invalid`;
  for(const tenant of tenants){
   await bindings.PUB_TENANT.getByName(tenant).activate(X13_PROJECT,x13Manifest,plan.id);
   await control.fixtureTenant(X13_PROJECT,tenant);
   await control.devE2eMember({projectId:X13_PROJECT,tenantId:tenant,email,password:'synthetic-test-password'});
  }
  if(before)await before(control);
  const response=await bindings.PUB_GATEWAY.getByName('pwe-holder-'+tenants[0]!.slice(-6)).fetch(new Request('https://fixture/ws',{headers:{upgrade:'websocket','x-gonvex-connection-id':crypto.randomUUID()}}));
  expect(response.status).toBe(101);const ws=response.webSocket!;sockets.push(ws);
  const frames:any[]=[];ws.accept();ws.addEventListener('message',event=>{frames.push(JSON.parse(String(event.data)));});
  const call=async(frame:any)=>{ws.send(JSON.stringify(frame));const end=Date.now()+20_000;for(;;){const i=frames.findIndex(f=>f.id===frame.id);if(i>=0)return frames.splice(i,1)[0];if(Date.now()>end)throw Error('timeout');await new Promise(r=>setTimeout(r,2));}};
  await call({type:'auth',id:'scope',project:X13_PROJECT,controlOnly:true});
  const login=await call({type:'action.call',id:'login',idempotencyKey:crypto.randomUUID(),scope:'control',path:'control.auth.passwordLogin',args:{email,password:'synthetic-test-password'}});
  return {control,login};
}

it('a storage error in the co-tenant bookkeeping never costs the sign-in its reply',async()=>{
 const tenants=['pwf-'+crypto.randomUUID(),'pwf-'+crypto.randomUUID()];
 const sockets:WebSocket[]=[];
 let control:any;
 try{
  const signed=await signIn(tenants,sockets,async(c)=>{await c.fixtureFailCoTenantWrites(true);});
  control=signed.control;
  expect(signed.login.type,signed.login.error).toBe('action.result');
  expect(signed.login.result.tenants).toHaveLength(2);
 }finally{
  await control?.fixtureFailCoTenantWrites(false);
  for(const socket of sockets)if(socket.readyState<2)socket.close(1000);
  await bindings.RUNTIME_CATALOG.clear(X13_PROJECT);
 }
},120_000);

it('a sign-in that admits two tenants makes the page prewarm of the active one wake the other',async()=>{
 const tenants=['pwe-'+crypto.randomUUID(),'pwe-'+crypto.randomUUID()];
 const sockets:WebSocket[]=[];
 try{
  await bindings.RUNTIME_CATALOG.install(X13_PROJECT,x13Manifest,1,'pwe-catalog');
  const plan=await buildReleasePlan(await migrationsFromFiles(files));await new R2PlanStore(bindings.RELEASES).write(plan);
  const control=bindings.PUB_CONTROL.getByName('control');
  await control.devCreateProject({id:X13_PROJECT,name:'X13 fixture'}).catch(()=>undefined);await control.devSaveManifest(X13_PROJECT,x13Manifest);
  const email=`pwe-${tenants[0]!.slice(-8)}@fixture.invalid`;
  for(const tenant of tenants){
   await bindings.PUB_TENANT.getByName(tenant).activate(X13_PROJECT,x13Manifest,plan.id);
   await control.fixtureTenant(X13_PROJECT,tenant);
   await control.devE2eMember({projectId:X13_PROJECT,tenantId:tenant,email,password:'synthetic-test-password'});
  }
  const response=await bindings.PUB_GATEWAY.getByName('pwe-holder').fetch(new Request('https://fixture/ws',{headers:{upgrade:'websocket','x-gonvex-connection-id':crypto.randomUUID()}}));
  expect(response.status).toBe(101);const ws=response.webSocket!;sockets.push(ws);
  const frames:any[]=[];ws.accept();ws.addEventListener('message',event=>{frames.push(JSON.parse(String(event.data)));});
  const call=async(frame:any)=>{ws.send(JSON.stringify(frame));const end=Date.now()+20_000;for(;;){const i=frames.findIndex(f=>f.id===frame.id);if(i>=0)return frames.splice(i,1)[0];if(Date.now()>end)throw Error('timeout');await new Promise(r=>setTimeout(r,2));}};
  await call({type:'auth',id:'scope',project:X13_PROJECT,controlOnly:true});
  const login=await call({type:'action.call',id:'login',idempotencyKey:crypto.randomUUID(),scope:'control',path:'control.auth.passwordLogin',args:{email,password:'synthetic-test-password'}});
  expect(login.type,login.error).toBe('action.result');
  const active:string=login.result.activeTenantId, other=tenants.find(t=>t!==active)!;
  expect(tenants).toContain(active);
  expect(login.result.tenants.map((t:{id:string})=>t.id).sort()).toEqual([...tenants].sort());
  const prewarm=control as unknown as {prewarmTenant(p:string,s:string):Promise<string>};
  expect(await prewarm.prewarmTenant(X13_PROJECT,active)).toBe('warming');
  expect(await prewarm.prewarmTenant(X13_PROJECT,other)).toBe('recent'); // woken with the active tenant's page
 }finally{
  for(const socket of sockets)if(socket.readyState<2)socket.close(1000);
  await bindings.RUNTIME_CATALOG.clear(X13_PROJECT);
 }
},120_000);
