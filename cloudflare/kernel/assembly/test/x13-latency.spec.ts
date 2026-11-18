// X13: a task create sent right after the AI rewrite Action, on the same
// socket, through the real gateway Session (ordered frame queue), AssemblyTenant,
// the ACTIONS service binding, the Action Worker and (before X13) the Workflow.
// Rust handles a socket's frames in order and awaits an Action inline
// (lib.rs ClientMessage::ActionCall -> execute_tenant_action), so the create
// waits for the rewrite there too; what must match Rust is the time the Action
// adds on top of its own work. The rewrite's provider round trip is a timer
// (`REWRITE_MS`). Commit-to-second-client delivery is unchanged by X13 and
// measured live (PR description); here the create's commit answer is the mark.
//
// The benchmark prints per-segment medians (local workerd). Run the same file
// on the base commit for the "before" column of the PR.
import {env} from 'cloudflare:test';
import {expect,it} from 'vitest';
import {files} from './catalog';
import type {AssemblyTenant} from '../tenant';
import type {TestControl,TestGateway} from './stable-worker';
import {buildReleasePlan,migrationsFromFiles} from '../../migrate';
import {R2PlanStore} from '../../migrate/plan';
import type {TestRuntimeCatalog} from './runtime-worker';
import {X13_PROJECT,x13Manifest} from './x13-fixture';

const REWRITE_MS=300;
const ROUNDS=8;
const bindings=env as unknown as {PUB_CONTROL:DurableObjectNamespace<TestControl>;PUB_TENANT:DurableObjectNamespace<AssemblyTenant>;PUB_GATEWAY:DurableObjectNamespace<TestGateway>;RELEASES:R2Bucket;RUNTIME_CATALOG:TestRuntimeCatalog};
const median=(values:number[])=>[...values].sort((a,b)=>a-b)[Math.floor(values.length/2)]!;
const max=(values:number[])=>Math.max(...values);

it('X13 a create behind an agent Action waits only for the Action itself, as in Rust',async()=>{
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

  type Round={action:number;create:number};
  /** One task-dialog submit: the rewrite (when `rewrite`) then the create, back to back on socket A. */
  const round=async(n:string,rewrite:boolean):Promise<Round>=>{
   const id=`x13-synthetic-${n}`;const t0=Date.now();
   if(rewrite)a.send({type:'action.call',id:`rw-${n}`,path:'x13.rewrite',args:{ms:REWRITE_MS,text:'fix my speling'}});
   a.send({type:'reducer.call',id:`cr-${n}`,path:'x13.create',args:{id}});
   const [action,create]=await Promise.all([
    rewrite?a.next(frame=>frame.id===`rw-${n}`):Promise.resolve({at:t0,frame:{type:'action.result',result:'FIX MY SPELING'}}),
    a.next(frame=>frame.id===`cr-${n}`)]);
   expect(action.frame).toMatchObject({type:'action.result',result:'FIX MY SPELING'});
   expect(create.frame).toMatchObject({type:'reducer.result'});
   return {action:action.at-t0,create:create.at-t0};
  };
  await round('warm-1',true);await round('warm-2',false);
  const withRewrite:Round[]=[],alone:Round[]=[];
  for(let i=0;i<ROUNDS;i++){withRewrite.push(await round(`r${i}`,true));alone.push(await round(`s${i}`,false));}
  const summary=(rounds:Round[])=>({actionMs:median(rounds.map(r=>r.action)),createMs:median(rounds.map(r=>r.create)),createMaxMs:max(rounds.map(r=>r.create))});
  const result={rewriteMs:REWRITE_MS,rounds:ROUNDS,createAfterRewrite:summary(withRewrite),createAlone:summary(alone),
   actionOverheadMs:median(withRewrite.map(r=>r.action))-REWRITE_MS,
   rewriteAddsToCreateMs:median(withRewrite.map(r=>r.create))-median(alone.map(r=>r.create))};
  console.log(`X13 bench ${JSON.stringify(result)}`);
  // Rust: the create waits for the rewrite and nothing else. The Workflow path added its create,
  // instance start and claim on top (locally ~170 ms, on the platform 2.5-7 s); inline adds RPC hops
  // (locally 40-90 ms). The bounds catch a gross regression without flaking on a slow runner; the
  // structural check below is the gate.
  expect(result.actionOverheadMs).toBeLessThan(500);
  expect(result.rewriteAddsToCreateMs).toBeLessThan(REWRITE_MS+500);
  // No Workflow ran: every direct run was claimed by an inline runner as attempt 1 and is done.
  // X13b: each completion is recorded after its answer left, so let the last one land.
  let runs=await bindings.PUB_TENANT.getByName(tenant).fixtureActionRuns();
  for(let i=0;i<200&&runs.some(run=>run.state!=='done');i++){await new Promise(resolve=>setTimeout(resolve,10));runs=await bindings.PUB_TENANT.getByName(tenant).fixtureActionRuns();}
  expect(runs).toHaveLength(ROUNDS+1);
  expect(new Set(runs.map(run=>JSON.stringify(run)))).toEqual(new Set([JSON.stringify({state:'done',attempts:1,owner:null,dispatch_count:0})]));
 }finally{
  for(const socket of sockets)if(socket.readyState<2)socket.close(1000);
  await bindings.RUNTIME_CATALOG.clear(X13_PROJECT);
 }
},120_000);
