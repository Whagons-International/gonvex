/**
 * X13i: with dev spans on (GONVEX_SPAN_TIMING=1; here switched on by fixture in each object's isolate), one rewrite + create
 * logs the create's whole path in parts, and every span line is time-only: durations, a frame type
 * and a function path, never a tenant, connection, account, token or argument.
 */
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

it('X13i spans split a create behind an agent Action, time-only',async()=>{
 const tenant='x13-'+crypto.randomUUID();
 const sockets:WebSocket[]=[];
 const open=async(holder:string)=>{
  const response=await bindings.PUB_GATEWAY.getByName(holder).fetch(new Request('https://fixture/ws',{headers:{upgrade:'websocket','x-gonvex-connection-id':crypto.randomUUID()}}));
  expect(response.status).toBe(101);const ws=response.webSocket!;sockets.push(ws);
  const frames:Array<{at:number;frame:any}>=[];ws.accept();ws.addEventListener('message',event=>{frames.push({at:Date.now(),frame:JSON.parse(String(event.data))});});
  const next=async(match:(frame:any)=>boolean,ms=20_000)=>{const end=Date.now()+ms;for(;;){const i=frames.findIndex(entry=>match(entry.frame));if(i>=0)return frames.splice(i,1)[0]!;if(Date.now()>end)throw Error('x13 fixture frame timeout');await new Promise(resolve=>setTimeout(resolve,2));}};
  const send=(frame:unknown)=>ws.send(JSON.stringify(frame));
  const call=async(frame:any)=>{send(frame);return (await next(reply=>reply.id===frame.id)).frame;};
  await next(frame=>frame.type==='session.ready');return {ws,next,send,call,frames};
 };
 try{
  await bindings.RUNTIME_CATALOG.install(X13_PROJECT,x13Manifest,1,'x13-catalog');
  const plan=await buildReleasePlan(await migrationsFromFiles(files));await new R2PlanStore(bindings.RELEASES).write(plan);
  await bindings.PUB_TENANT.getByName(tenant).activate(X13_PROJECT,x13Manifest,plan.id);
  const control=bindings.PUB_CONTROL.getByName('control');
  await control.devCreateProject({id:X13_PROJECT,name:'X13 fixture'}).catch(()=>undefined);await control.devSaveManifest(X13_PROJECT,x13Manifest);
  await control.fixtureTenant(X13_PROJECT,tenant);
  const email=`x13-${tenant.slice(-8)}@fixture.invalid`;
  await control.devE2eMember({projectId:X13_PROJECT,tenantId:tenant,email,password:'synthetic-test-password'});
  const a=await open('x13-holder-a');
  await a.call({type:'auth',id:'login-scope',project:X13_PROJECT,controlOnly:true});
  const login=await a.call({type:'action.call',id:'login',idempotencyKey:crypto.randomUUID(),scope:'control',path:'control.auth.passwordLogin',args:{email,password:'synthetic-test-password'}});
  expect(login.type,login.error).toBe('action.result');const token=login.result.accessToken;
  expect((await a.call({type:'auth',id:'a',project:X13_PROJECT,tenant,token})).type).toBe('auth.result');

  const gateway=bindings.PUB_GATEWAY.getByName('x13-holder-a') as unknown as {fixtureSpans(on:boolean):Promise<Array<{name:string;fields:Record<string,unknown>}>>};
  const owner=bindings.PUB_TENANT.getByName(tenant) as unknown as {fixtureSpans(on:boolean):Promise<Array<{name:string;fields:Record<string,unknown>}>>};
  await gateway.fixtureSpans(true);await owner.fixtureSpans(true);
  const lines:Array<{name:string;fields:Record<string,unknown>}>=[];
  try{
   a.send({type:'action.call',id:'rw-1',path:'x13.rewrite',args:{ms:50,text:'secret words'}});
   a.send({type:'reducer.call',id:'cr-1',path:'x13.create',args:{id:'x13i-synthetic-1'}});
   expect((await a.next(frame=>frame.id==='cr-1')).frame).toMatchObject({type:'reducer.result'});
   for(let i=0;i<200;i++){lines.push(...await owner.fixtureSpans(true));if(lines.some(l=>l.name==='tenant.dispatch'))break;await new Promise(resolve=>setTimeout(resolve,10));}
  }finally{lines.push(...await gateway.fixtureSpans(false),...await owner.fixtureSpans(false));}
  const names=new Set(lines.map(l=>l.name));
  for(const name of ['gateway.call','tenant.reducer','tenant.dispatch','tenant.delivery'])expect(names,name).toContain(name);
  expect(lines.find(l=>l.name==='gateway.call'&&l.fields.path==='x13.create')?.fields).toMatchObject({kind:'reducer',turnWaitMs:expect.any(Number),rpcMs:expect.any(Number)});
  expect(lines.find(l=>l.name==='gateway.call'&&l.fields.path==='x13.rewrite')?.fields).toMatchObject({kind:'action'});
  expect(lines.find(l=>l.name==='tenant.dispatch')?.fields).toMatchObject({type:'reducer.call',dispatchMs:expect.any(Number),durableAfterMs:expect.any(Number)});
  // Time-only: numbers, or one of the frame types / function paths / call names listed here.
  const allowed=new Set(['reducer','action','client','tool','reducer.call','reducer.callMany','x13.create','x13.rewrite','dispatch','call','deliverReplica','deliverReplicaInitial','startCall','completeCall']);
  const text=JSON.stringify(lines);
  for(const secret of [tenant,email,token,'secret words','x13i-synthetic-1'])expect(text).not.toContain(secret);
  for(const line of lines)for(const [key,value] of Object.entries(line.fields))expect(typeof value==='number'||allowed.has(String(value)),`${line.name}.${key}=${String(value)}`).toBe(true);
 }finally{
  for(const socket of sockets)if(socket.readyState<2)socket.close(1000);
  await bindings.RUNTIME_CATALOG.clear(X13_PROJECT);
 }
},120_000);
