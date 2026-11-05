/** Local-only real SDK workload. Run inside an internal Docker network. */
import { createRequire } from 'node:module';
import { readFile, writeFile, appendFile, realpath } from 'node:fs/promises';
import { shardEpoch } from './shard-epoch.mjs';
import { qualifyExpanded, visibilityDelta, replicaLifecycle, sdkCacheEvidence } from './expanded-gate.mjs';
import { mixedPlan, qualifyMixed } from './mixed-gate.mjs';
import { randomUUID, createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { monitorEventLoopDelay, PerformanceObserver } from 'node:perf_hooks';
import { workerReceipt } from './worker-receipt.mjs';
import { integrityWork } from './thin-checkpoint.mjs';
import { AsyncLocalStorage } from 'node:async_hooks';
import { loopback, quantiles, passRule } from './safety.mjs';
import { memorySnapshotStaging } from './native-stage.mjs';
import { installWireRecorder } from './wire-record.mjs';
import { sdkArtifact } from './sdk-artifact.mjs';
import { ThinCheckpointStore } from './thin-checkpoint.mjs';
import { ThinClient, defaultPool as thinRowPool } from './thin-client.mjs';
import { firstViewed, viewSetsFirstViewedAt, unmetTaskUpdate } from './task-expectations.mjs';

async function main() {
 const harnessFiles=['run.ts','shards.sh','remote-shards.sh','stop-workers.mjs','worker-receipt.mjs','sdk-artifact.mjs','thin-client.mjs','thin-checkpoint.mjs','expanded-gate.mjs','mixed-gate.mjs','safety.mjs','native-stage.mjs','wire-record.mjs','shard-epoch.mjs','task-expectations.mjs'];
 const sourcePins=Object.fromEntries(await Promise.all(harnessFiles.map(async name=>[name,createHash('sha256').update(await readFile(new URL(name,import.meta.url))).digest('hex')])));
 const profileHash=createHash('sha256').update(JSON.stringify(sourcePins)).digest('hex');
 const app=process.env.WHAGONS_CHECKOUT!;
 if(!app) throw Error('WHAGONS_CHECKOUT is required');
 const require=createRequire(app+'/package.json');
 const appPackage=JSON.parse(await readFile(app+'/package.json','utf8'));const sdkPackage=JSON.parse(await readFile(app+'/node_modules/@gonvex/client/package.json','utf8'));
 if(appPackage.dependencies?.['@gonvex/client']!==sdkPackage.version)throw Error('Installed SDK must match the pinned app version');
 const lock=await readFile(app+'/pnpm-lock.yaml','utf8');const patchLine=lock.split('\n').find(line=>line.trim().startsWith("'@gonvex/client@"+sdkPackage.version+"':"));
 const expectedPatch=patchLine?.trim().split(': ').at(-1);const sdkPath=await realpath(app+'/node_modules/@gonvex/client');
 if(expectedPatch&&/^[a-f0-9]{64}$/.test(expectedPatch)&&!sdkPath.includes('patch_hash='+expectedPatch))throw Error('Installed SDK patch must match the app lock');
 const selectedSdk=await sdkArtifact(require.resolve('@gonvex/client'));
 const {GonvexClient,control,MemoryLocalReplicaStorage}=await import(pathToFileURL(selectedSdk.entry).href);
 const {api}=await import(pathToFileURL(app+'/gonvex/_generated/api.ts').href);
 // Authoritative timing never executes local reducers. Read the generated contract
 // without requiring the ignored portable reducer bundles in a clean checkout.
 const executor=await readFile(app+'/gonvex/_generated/local-executor.ts','utf8');
 const contract=executor.match(/clientContract: (\{[^}]+\})/);
 if(!contract)throw Error('Missing generated client contract');
 const localRuntime={clientContract:JSON.parse(contract[1])};
 const artifactPins={harness:sourcePins,sdkEntry:selectedSdk.sha256,sdkSelection:selectedSdk,appManifest:createHash('sha256').update(await readFile(app+'/gonvex/_generated/manifest.json')).digest('hex'),appApi:createHash('sha256').update(await readFile(app+'/gonvex/_generated/api.ts')).digest('hex'),appContract:createHash('sha256').update(executor).digest('hex'),appOpens:process.env.LOAD_APP_OPENS?createHash('sha256').update(await readFile(process.env.LOAD_APP_OPENS.startsWith('/')?process.env.LOAD_APP_OPENS:new URL(process.env.LOAD_APP_OPENS,import.meta.url))).digest('hex'):null};
 const url=loopback(process.env.LOAD_URL||'http://127.0.0.1:8080').origin;
 const auth=loopback(process.env.LOAD_AUTH_URL||'http://127.0.0.1:9099').origin;
 const state=process.env.LOAD_STATE!;
 const admin=process.env.LOAD_ADMIN_KEY!;
 const pause=(ms:number)=>new Promise(r=>setTimeout(r,ms));
 const post=async(path:string,body:any,key=admin,method='POST')=>{
  const r=await fetch(url+path,{method,headers:{authorization:'Bearer '+key,'content-type':'application/json'},body:JSON.stringify(body)});
  if(!r.ok) throw Error(path+' '+r.status+' '+(await r.text()).slice(0,500));return r.json() as Promise<any>;
 };
 let project:any;
 let fixtures:any[]=[];
 let projectId='';
 const lifecycle=replicaLifecycle();
 installWireRecorder(process.env.LOAD_WIRE_RECORD,{observe:lifecycle.frame,full:process.env.LOAD_WIRE_RECORD_FULL==='true'});
 // LOAD_CLIENT=thin drives the same workload through a protocol-level client
 // (thin-client.mjs) instead of the full SDK. It sends the same frames but
 // skips browser-side replica storage, so one process can hold many more users.
 const clientKind=process.env.LOAD_CLIENT||'sdk';
 if(!['sdk','thin'].includes(clientKind))throw Error('Unknown LOAD_CLIENT');
 const thin=clientKind==='thin';
 // Mixed mode: LOAD_SDK_USERS measured users (spread evenly) keep the real SDK
 // so client-side behaviour stays covered. Visibility oracle sessions use
 // LOAD_ORACLE_CLIENT (default sdk), independent of the thin client.
 const sdkUsers=thin?Number(process.env.LOAD_SDK_USERS||0):Infinity;
 const oracleKind=process.env.LOAD_ORACLE_CLIENT||'sdk';
 if(!['sdk','thin'].includes(oracleKind)||!(sdkUsers>=0))throw Error('Invalid LOAD_SDK_USERS or LOAD_ORACLE_CLIENT');
 const userKind=(i:number,target:number)=>!thin?'sdk':Math.floor((i+1)*Math.min(sdkUsers,target)/target)>Math.floor(i*Math.min(sdkUsers,target)/target)?'sdk':'thin';
 // Row encoding. Default: legacy JSON rows, as the Whagons app's SDK
 // negotiates without a native staging adapter. LOAD_NATIVE_ROWS=true makes
 // thin clients and SDK users (with in-memory staging, no fsync) use native rows.
 const nativeRequested=process.env.LOAD_NATIVE_ROWS==='true';const presenceV2=process.env.LOAD_PRESENCE_V2==='1';
 const sdkStaging=nativeRequested?memorySnapshotStaging():undefined;
 // Open-loop schedule (default for thin runs): foreground actions and
 // heartbeats fire at planned times whether or not earlier ones finished, and
 // latency counts from the planned start. LOAD_OPEN_LOOP=false keeps the
 // earlier closed loop for comparison with older runs.
 const openLoop=(process.env.LOAD_OPEN_LOOP??(thin?'true':'false'))==='true';
 const maxInFlightPerUser=Number(process.env.LOAD_MAX_INFLIGHT_PER_USER||16);
 const plannedStart=new AsyncLocalStorage<{at:number,used:boolean}>();
 const scheduleStats={kind:openLoop?'open':'closed',fired:0,overflow:0,skippedDuringReconnect:0,lateMs:[] as number[]};
 const modulePin=createHash('sha256').update(await readFile(app+'/gonvex/_generated/manifest.json')).digest('hex');
 const checkpointStores:any[]=[],thinDrivers:any[]=[];
 const client=(kind:string=clientKind,storage?:any,identity?:string)=>{if(kind==='thin'){const c=new ThinClient(url.replace('http:','ws:')+'/ws',{project:projectId,clientContract:localRuntime.clientContract,nativeRows:nativeRequested,ackDelayMs:Number(process.env.LOAD_THIN_ACK_DELAY_MS||0),presenceV2,...(storage?{checkpointStore:storage,checkpointIdentity:identity,modulePin}:{})});thinDrivers.push(c);c.connect();return c;}const staging=sdkStaging?.client();const c=new GonvexClient(url.replace('http:','ws:')+'/ws',{project:projectId,clientContract:localRuntime.clientContract,outbox:{enabled:false},...(storage?{localReplica:{storage}}:{}),...(staging?{nativeSnapshotStaging:staging.staging}:{})});c.connect();return c;};
 let recordReducer:((path:string,ms:number,failed:boolean)=>void)|undefined;
 const reducer=async(c:any,ref:any,args:any)=>{const at=performance.now();try{const value=await c.reducer({...ref,localExecution:undefined},args,{offline:'reject'});recordReducer?.(ref.path,performance.now()-at,false);return value;}catch(e){recordReducer?.(ref.path,performance.now()-at,true);throw e;}};
 const identity=async(email:string)=>{
  const body={email,password:'local-synthetic-password',returnSecureToken:true};
  let r=await fetch(auth+'/identitytoolkit.googleapis.com/v1/accounts:signUp?key=local',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  if(!r.ok) r=await fetch(auth+'/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=local',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  const b:any=await r.json();if(!r.ok) throw Error('Emulator identity failed');return b.idToken;
 };
 // LOAD_PRESENCE_V2=1: behave like the presence-v2 app (Whagons perf/presence-v2): when the
 // runtime grants presence v2, no presence/support heartbeat Reducers; transitions are
 // presence.set and delivery is timed from the presence feed (presence.patch).
 const login=async(email:string,tenant?:string,kind:string=clientKind,storage?:any)=>{
  const c=client(kind,storage,email);
  try {
   const grant=await c.action(control.auth.exchangeExternalToken,{provider:'firebase',token:await identity(email),...(tenant?{tenantId:tenant}:{})});
   if(tenant) await c.authenticate({project:projectId,token:grant.accessToken,tenant});
   return {c,grant};
  }catch(e){c.close();throw e;}
 };
 if(process.argv.includes('setup')) {
  try { const old=JSON.parse(await readFile(state,'utf8'));project={project:{id:old.projectId},projectKey:old.projectKey};fixtures=old.fixtures; } catch { project=await post('/dev/projects',{name:'Local synthetic load',databaseMode:'multiTenant'}); }
  projectId=project.project.id;
  await writeFile(state,JSON.stringify({projectId,projectKey:project.projectKey,fixtures}));
  await post(`/dev/projects/${projectId}/auth/providers/firebase`,{authMode:'firebase',enabled:true,signupMode:'personal',firebaseProjectId:'demo-whagons-e2e'},project.projectKey,'PUT');
  const manifest=JSON.parse(await readFile(app+'/gonvex/_generated/manifest.json','utf8'));manifest.project=projectId;
  await post('/dev/sync',manifest,project.projectKey);
  // No integrations are configured. Let the scheduled Action return configured:false.
  for(const [name,value] of Object.entries({MICROSOFT365_CREDENTIAL_KEY:'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',STRIPE_SECRET_KEY:'synthetic-local-only',STRIPE_SEAT_PRICE_ID:'synthetic-local-only'})) await post(`/dev/projects/${projectId}/env`,{name,value},project.projectKey);
  for(let i=fixtures.length;i<26;i++) {
   const tenant=randomUUID();const email=`load-owner-${i}@example.invalid`;
   await post('/dev/tenants',{projectId,id:tenant,name:`Synthetic ${i}`});
   const {c,grant}=await login(email);
   const attached=await post('/dev/internal/e2e/members',{projectId,tenantId:tenant,email});
   await c.authenticate({project:projectId,token:grant.accessToken,tenant});
   const memberId=attached.memberId;
   if(!memberId) throw Error('Missing fixture member');
   // Production-v2 tenant task counts (realism audit 2026-11-03), then 14 small tenants.
   const count=[8850,4038,4010,3273,3073,3043,2178,2130,2005,1866,930,700][i]||100+10*i;
   const fixture=await c.action(api.testing.invoke,{operation:'seedTaskUiWorkspaceWithTasks',args:{unique:`E2E load ${i}`,email,memberId,secondaryEmail:email,secondaryMemberId:memberId,roleKey:'admin',tasks:Array.from({length:50},(_,j)=>({name:`E2E synthetic task ${j}`}))}});
   fixtures.push({tenant,email,...fixture,taskCount:count});c.close();
   await writeFile(state,JSON.stringify({projectId,projectKey:project.projectKey,fixtures}));
   console.log(JSON.stringify({seededTenants:i+1,targetTasks:count,seededTasks:50}));
  }
  return;
 }
 ({projectId,fixtures}=JSON.parse(await readFile(state,'utf8')));
 if(process.argv.includes('verifyUpdates')) {
  const f=fixtures[0];const {c}=await login('load-user-0@example.invalid',f.tenant);
  const extras=[api.presence.list,api.workspaces.list,api.statuses.statuses,api.tags.tags,api.members.listMembers,api.members.listMemberTeams,api.taskResources.taskUsers,api.taskResources.taskTags,api.workspacePreferences.workspacePreferences].map(ref=>{const w=c.watchReplica(ref,{});const stop=w.onUpdate(()=>w.localReplicaResult());return stop;});
  const w=c.watchReplica(api.tasks.recentWorkspaceTasks,{});let pending:any;let initial:any;
  const ready=new Promise<void>((resolve,reject)=>{initial={resolve,reject};});
  const stop=w.onUpdate(()=>{try{
   const rows=w.localReplicaResult()||[];
   if(initial&&w.status().isUpToDate&&w.localReplicaState()?.completeness==='complete'){console.log(JSON.stringify({initialRows:rows.length}));initial.resolve();initial=undefined;}
   if(pending&&rows.some((r:any)=>r._id===pending.id&&r.name===pending.name)){pending.resolve(performance.now()-pending.at);pending=undefined;}
  }catch(e){initial?.reject(e);pending?.reject(e);}});
  try {
   await ready;
   const taskId=randomUUID();
   for(let i=0;i<6;i++){
    const name='E2E load update '+randomUUID();const at=performance.now();let timer:any;
    const update=new Promise<number>((resolve,reject)=>{pending={id:taskId,name,at,resolve,reject};timer=setTimeout(()=>reject(Error('Replica update timed out')),20000);});
    const ref=i?api.tasks.rename:api.tasks.create;
    const args=i?{taskId,name}:{entityId:taskId,name,workspaceId:f.workspaceId,categoryId:f.categoryId,priorityId:f.priorityId,statusId:f.statusId};
    await reducer(c,ref,args);const saveMs=performance.now()-at;const updateMs=await update;clearTimeout(timer);
    console.log(JSON.stringify({path:ref.path,saveMs,updateMs}));await pause(1000);
   }
  }finally{stop();for(const s of extras)s();c.close();}
  return;
 }
 if(process.argv.includes('verify')) {
  const f=fixtures[0];const {c,grant}=await login('load-user-0@example.invalid',f.tenant);
  const member=grant.tenants.find((t:any)=>t.id===f.tenant).memberId;
  const taskId=randomUUID();
  const calls=[
   [api.tasks.create,{entityId:taskId,name:'E2E load verify',workspaceId:f.workspaceId,categoryId:f.categoryId,priorityId:f.priorityId,statusId:f.statusId}],
   [api.taskResources.recordTaskViewByTaskId,{taskId}],
   [api.tasks.start,{taskId,statusId:f.nextStatusId}],
   [api.taskResources.addTagByTaskId,{taskTagId:randomUUID(),taskId,tagId:f.tag._id}],
   [api.taskResources.createNote,{entity:{_id:randomUUID(),_creationTime:Date.now(),taskId,userId:member,source:'web',note:'Synthetic protocol verification'}}],
   [api.workspacePreferences.save,{entity:{_id:randomUUID(),userId:member,workspaceId:f.workspaceId,preferences:{density:'compact'}}}],
   [api.tasks.requestTransition,{taskId,statusId:f.completedStatusId}],
   [api.presence.beat,{}],
   [api.presence.transition,{state:'idle'}],
   [control.support.heartbeat,{release:'synthetic-local',environment:'test'}],
  ];
  try {for(const [ref,args] of calls){const at=performance.now();await reducer(c,ref,args);console.log(JSON.stringify({path:ref.path,ok:true,durationMs:performance.now()-at}));}}
  finally{c.close();}
  return;
 }
 const mixed=process.env.LOAD_SIGNIN_SCHEDULE==='mixed-v1';
 const target=Number(process.env.LOAD_USERS||26),seconds=Number(process.env.LOAD_SECONDS||180),ramp=Number(process.env.LOAD_RAMP_SECONDS||60);
 if(!Number.isSafeInteger(target)||target<1||!Number.isFinite(seconds)||seconds<1) throw Error('Invalid load bounds');
 // One Node event loop saturates near 120 users, so large cases run several
 // shards (shards.sh). Shard k drives users i with i % shards === k; tenant
 // choice, ramp timing and task ordinals stay global, so the mix matches an
 // unsharded run. merge-shards.mjs combines the shards' raw samples.
 const shards=Number(process.env.LOAD_SHARDS||1),shard=Number(process.env.LOAD_SHARD||0);
 if(!Number.isSafeInteger(shards)||shards<1||!Number.isSafeInteger(shard)||shard<0||shard>=shards) throw Error('Invalid shard');
 const mine=(i:number)=>i%shards===shard;const myTarget=Array.from({length:target},(_,i)=>i).filter(mine).length;
 const expanded=process.env.LOAD_EXPANDED_GATE==='1';
 const reloadEvents:any[]=[],permissionCycles:any[]=[];
 const measurements:any={cachedReload:[],save:[],heartbeat:[],reducer:[],query:[],initialLoad:[],reconnect:[],permission:[],visibility:[],delivery:[],taskDelivery:[],login:[]},errors:Record<string,number>={};
 const samples:Record<string,{at:number,ms:number}[]>={};
 const sample=(kind:string,ms:number,at=Date.now()-ms)=>{measurements[kind].push(ms);(samples[kind]??=[]).push({at,ms});};
 const errorEvents:{at:number,key:string}[]=[]; const seenErrors=new WeakSet<object>();
 const error=(e:any,at=Date.now())=>{if(e&&typeof e==='object'){if(seenErrors.has(e))return;seenErrors.add(e);}const key=String(e?.message||e).slice(0,240);errors[key]=(errors[key]||0)+1;errorEvents.push({at,key});};
 // The first measurement an action takes starts at its planned time (open loop).
 const startTime=()=>{const p=plannedStart.getStore();if(p&&!p.used){p.used=true;return p.at;}return performance.now();};
 const operationAttempts:{kind:string,at:number,done:boolean}[]=[];
 const timed=async(kind:string,fn:()=>Promise<any>)=>{const t=startTime();const attempt={kind,at:performance.timeOrigin+t,done:false};operationAttempts.push(attempt);try{const v=await fn();sample(kind,performance.now()-t);return v;}catch(e){error(e,performance.timeOrigin+t);return undefined;}finally{attempt.done=true;}};
 const perPath:Record<string,{durations:number[],errors:number}>={};
 recordReducer=(path,ms,failed)=>{const p=perPath[path]??={durations:[],errors:0};if(failed)p.errors++;else{p.durations.push(ms);sample('reducer',ms);}};
 const periodicTimers:ReturnType<typeof setInterval>[]=[];
 const presenceStates=new Map<string,string>();const users:any[]=[];const snapshots:any[]=[];const pending=new Map<string,number>();// One expectation per task (the latest edit wins). state: the save's outcome; a
 // failed save (e.g. budget_exceeded before the Reducer ran) expects nothing.
 // Rules and their reasons: task-expectations.mjs.
 type TaskOp={at:number,matches:(row:any,at:number)=>boolean,path:string,args?:string,by:string,state:'pending'|'ok'|'failed',savedAt?:number,error?:string};
 const pendingTasks=new Map<string,TaskOp>();
 // Teardown diagnostics: every expectation still unmet after the delivery grace, per observer.
 const undelivered:any[]=[];const expectationStats={ops:0,failedSaves:0,lateDelivered:0,pendingSaveAtTeardown:0,ownTaskViews:0,undeliveredOps:new Set<string>()};
 const signIns:any[]=[];
 const plan=mixed?mixedPlan(target):undefined;
 let started=Date.now();
 if(mixed&&shards>1){
  const root=process.env.LOAD_RESULT_ROOT;if(!root)throw Error('mixed shards require LOAD_RESULT_ROOT');
  started=await shardEpoch(root,'signin',shard,shards,Date.now()+2000);
 }

 let stopping=false;let aborted=false;let deadline=Infinity;let remaining=myTarget;
 let stopExtra=()=>({});
 const stopMeasurement=()=>{
  const plateauEnd=deadline;aborted=true;stopping=true;deadline=Date.now();
  const partial=(process.env.LOAD_RESULT||state+'.result')+'.partial.json';
  const receipt={aborted:true,stoppedAt:Date.now(),workerPid:process.pid,profileHash,shard,shards,signinSchedule:mixed?'mixed-v1':'uniform',scheduleStartedMs:started,signIns,
   targetUsers:myTarget,connectedUsers:users.length,errors,errorEvents,operationAttempts,replicaEvents:lifecycle.events,reloadEvents,permissionCycles,
   artifactPins,sdkSelection:selectedSdk,latencyMs:Object.fromEntries(Object.entries(measurements).map(([k,v]:any)=>[k,{samples:v.length,...quantiles(v)}])),
   steadyStartMs:Number.isFinite(plateauEnd)?plateauEnd-seconds*1000:started+ramp*1000,steadyEndMs:Number.isFinite(plateauEnd)?plateauEnd:started+(ramp+seconds)*1000,
   rawSamples:samples,rawPaths:perPath,...stopExtra()};
  if(process.env.LOAD_PROGRESS)workerReceipt(process.env.LOAD_PROGRESS,{at:Date.now(),workerPid:process.pid,shard,signIns,stopping,checkpointFailures:ThinClient.stats.checkpointFailures},{durable:true});
  workerReceipt(partial,receipt,{durable:true});
 };
 process.on('SIGTERM',stopMeasurement);process.on('SIGINT',stopMeasurement);
 let presenceV2Users=0;
 const collectionReadiness={readyAtTeardown:0,notReadyAtTeardown:0};
 const ready=()=>{if(--remaining!==0||deadline!==Infinity)return;
  if(mixed&&shards>1){void shardEpoch(process.env.LOAD_RESULT_ROOT!,'plateau',shard,shards,started+ramp*1000).then(epoch=>{if(deadline===Infinity)deadline=epoch+seconds*1000;}).catch(e=>{error(e);if(deadline===Infinity)deadline=Date.now()+seconds*1000;});}
  else deadline=Math.max(Date.now(),mixed?started+ramp*1000:0)+seconds*1000;
 };
 // A user that never finishes connecting must not hold the plateau forever: start it
 // ramp + 120 s after launch anyway and count the users still missing.
 setTimeout(()=>{if(deadline===Infinity&&!stopping){for(let k=0;k<remaining;k++)error(new Error('User not connected by ramp + 120 s'));deadline=Date.now()+seconds*1000;}},Math.max(0,started+(ramp+120)*1000-Date.now())).unref?.();
 // Preserve the seven observed tenant weights. An optional stress profile
 // activates the other nineteen tenants too; it is not the observed mix.
 // Default: production write skew (two tenants carry 78%). LOAD_ALL_TENANTS_ACTIVE
 // spreads users evenly over all tenants as a stress profile.
 const weights=fixtures.map((_,i)=>process.env.LOAD_ALL_TENANTS_ACTIVE==='true'?1:([39,39,11,6,2,2,1][i]||0));
 const total=weights.reduce((a,b)=>a+b,0);
 const chooseTenant=(i:number)=>{let x=((i*0.61803398875)%1)*total;return fixtures[weights.findIndex(w=>(x-=w)<0)]||fixtures[0];};
 if(process.argv.includes('verifyConcurrentLogins')) {
  const clients=Array.from({length:target},()=>client());let timer:any;
  try {
   const tokens=await Promise.all(clients.map((_,i)=>identity(`load-user-${i}@example.invalid`)));
   const at=performance.now();
   await Promise.race([
    Promise.all(clients.map(async(c,i)=>{const f=chooseTenant(i);const grant=await c.action(control.auth.exchangeExternalToken,{provider:'firebase',token:tokens[i],tenantId:f.tenant});await c.authenticate({project:projectId,token:grant.accessToken,tenant:f.tenant});})),
    new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Concurrent sign-in exceeded the 5-second diagnostic deadline')),5000);}),
   ]);
   console.log(JSON.stringify({concurrentSignIns:target,passed:true,durationMs:performance.now()-at}));
  } finally {clearTimeout(timer);for(const c of clients)c.close();}
  return;
 }
 if(process.argv.includes('prepare')) {
  for(let i=0;i<target;i++){const f=chooseTenant(i);const email=`load-user-${i}@example.invalid`;const b=await login(email);b.c.close();await post('/dev/internal/e2e/members',{projectId,tenantId:f.tenant,email});}
  for(let k=0;k<fixtures.length;k++) {for(let j=0;j<(k<3?80:4);j++){const email=`load-filler-${k}-${j}@example.invalid`;const b=await login(email);b.c.close();await post('/dev/internal/e2e/members',{projectId,tenantId:fixtures[k].tenant,email});}}
  console.log(JSON.stringify({preparedUsers:target}));return;
 }
 // Generator self-cost, so SDK and thin clients can be compared per user.
 const generatorCost={samples:[] as any[],peakRssBytes:0,peakHeapUsedBytes:0} as any;const cpuAtStart=process.cpuUsage();const wallAtStart=performance.now();
 const eventLoop=monitorEventLoopDelay({resolution:10});eventLoop.enable();
 const gcCost={count:0,wallMs:0,maxWallMs:0};const gcObserver=new PerformanceObserver(list=>{for(const e of list.getEntries()){gcCost.count++;gcCost.wallMs+=e.duration;gcCost.maxWallMs=Math.max(gcCost.maxWallMs,e.duration);}});gcObserver.observe({entryTypes:['gc']});
 const driverDiagnostics=()=>({eventLoopDelayMs:{p95:eventLoop.percentile(95)/1e6,p99:eventLoop.percentile(99)/1e6,max:eventLoop.max/1e6},gc:{...gcCost},integrityWork:{...integrityWork}});
 stopExtra=()=>{const u=process.cpuUsage(cpuAtStart),m=process.memoryUsage();return {presenceV2Users,collectionReadiness,visibilityChecks,snapshots,pendingQualification:{targetUsers:myTarget,notAttempted:myTarget-signIns.filter(x=>x.attemptedAt!==undefined).length,notAuthenticated:myTarget-signIns.filter(x=>x.authAt!==undefined).length,notFullyReadyEver:myTarget-signIns.filter(x=>x.readyAt!==undefined).length,pendingOperations:operationAttempts.filter(x=>!x.done).length},
  taskExpectations:{...expectationStats,undeliveredOps:expectationStats.undeliveredOps.size,undelivered},
  schedule:{kind:scheduleStats.kind,fired:scheduleStats.fired,overflow:scheduleStats.overflow,skippedDuringReconnect:scheduleStats.skippedDuringReconnect,startLateMs:quantiles(scheduleStats.lateMs)},
  generatorCost:{...generatorCost,cpuSeconds:(u.user+u.system)/1e6,wallSeconds:(performance.now()-wallAtStart)/1000,peakRssBytes:Math.max(generatorCost.peakRssBytes,m.rss),peakHeapUsedBytes:Math.max(generatorCost.peakHeapUsedBytes,m.heapUsed)},
  driverDiagnostics:driverDiagnostics(),checkpointFailures:ThinClient.stats.checkpointFailures};};
 const costTimer=setInterval(()=>{if(process.env.LOAD_PROGRESS){const p=process.env.LOAD_PROGRESS;workerReceipt(p,{at:Date.now(),workerPid:process.pid,shard,signIns,stopping,checkpointFailures:ThinClient.stats.checkpointFailures,driverDiagnostics:driverDiagnostics()});}const m=process.memoryUsage(),u=process.cpuUsage(cpuAtStart);generatorCost.peakRssBytes=Math.max(generatorCost.peakRssBytes,m.rss);generatorCost.peakHeapUsedBytes=Math.max(generatorCost.peakHeapUsedBytes,m.heapUsed);generatorCost.samples.push({at:Date.now(),rss:m.rss,heapUsed:m.heapUsed,cpuMs:(u.user+u.system)/1000,checkpointLogicalBytes:checkpointStores.reduce((n,s)=>n+s.stats().bytes,0),checkpointWindows:checkpointStores.reduce((n,s)=>n+s.stats().windows,0),checkpointCpuSeconds:thinDrivers.reduce((n,c)=>n+c.checkpointCpuUs/1e6,0)});},5000);costTimer.unref();
 const metricTimer=shard!==0?undefined:setInterval(()=>{void fetch(url+'/dev/metrics?project='+encodeURIComponent(projectId),{headers:{authorization:'Bearer '+admin}}).then(r=>r.json()).then(x=>appendFile((process.env.LOAD_RESULT||state)+'.metrics.jsonl',JSON.stringify({at:Date.now(),metrics:{websocket:{accounts:x.websocket?.accounts,connections:x.websocket?.connections,subscriptions:x.websocket?.subscriptions},database:x.database,queryAdmission:x.queryAdmission}})+'\n')).catch(error);},10000);
 const tenantCounts=new Map<string,number>();const ordinals=Array.from({length:target},(_,i)=>{const f=chooseTenant(i);const n=tenantCounts.get(f.tenant)||0;tenantCounts.set(f.tenant,n+1);return n;});
 // ---- Simulated users (load-realism audit 2026-11-03 plus Gabriel's direction:
 // heavier than production on the expensive paths). See README "Action mix".
 type Handle={i:number,email:string,member:string,tenant:string,tw:()=>any,admin:boolean,busy:boolean};
 const tenantUsers=new Map<string,Handle[]>();
 const approvalQueue=new Map<string,string[]>();
 const visibilityChecks={checked:0,converged:0,mismatched:0,noChange:0,additions:0,removals:0};
 const pick=<T,>(xs:T[],r:()=>number)=>xs[Math.floor(r()*xs.length)];
 const reconnectAt=(fraction:number)=>deadline-seconds*1000*(1-fraction);
 const until=async(condition:()=>boolean,ms:number)=>{const end=Date.now()+ms;while(!condition()){if(Date.now()>end)return false;await pause(50);}return true;};
 const ids=(rows:any[])=>new Set(rows.map((r:any)=>r._id));
 // Real-app page-load collection set (LOAD_APP_OPENS=<json>): {opens:[{path,args}]}, '$member' -> member id.
 const appOpens:{path:string,args:any}[]|undefined=process.env.LOAD_APP_OPENS?JSON.parse(await readFile(process.env.LOAD_APP_OPENS.startsWith('/')?process.env.LOAD_APP_OPENS:new URL(process.env.LOAD_APP_OPENS,import.meta.url),'utf8')).opens:undefined;
 const refFor=(path:string)=>{const dot=path.indexOf('.');const ns=(api as any)[path.slice(0,dot)];const r=ns?.[path.slice(dot+1)];const ref=typeof r==='function'?r():r;if(!ref?.path)throw new Error('LOAD_APP_OPENS: no replica reference for '+path);return ref;};
 const mergeOpens=(base:[any,any][],opens:{path:string,args:any}[],member:string):[any,any][]=>{const seen=new Set(base.map(([r,a])=>r.path+'\0'+JSON.stringify(a)));const out=[...base];for(const o of opens){const args=JSON.parse(JSON.stringify(o.args).split('"$member"').join(JSON.stringify(member)));const key=o.path+'\0'+JSON.stringify(args);if(seen.has(key))continue;seen.add(key);out.push([refFor(o.path),args]);}return out;};
 const watchAll=(c:any,refs:[any,any][],onUpdate=()=>{})=>{const ws=refs.map(([ref,args])=>{const w=c.watchReplica(ref,args);const stop=w.onUpdate(()=>{try{w.localReplicaResult();onUpdate();}catch(e){error(e);}});return {w,stop};});return {ready:()=>ws.every(x=>x.w.status().isUpToDate&&x.w.localReplicaState()?.completeness==='complete'),close:()=>ws.forEach(x=>x.stop()),rows:(k:number)=>(ws[k].w.localReplicaResult()||[]) as any[]};};
 // The oracle: a fresh session computes the target's visible rows from scratch;
 // the target's long-lived replica (incrementally maintained) must converge to
 // exactly that set. A difference after 15 s is a leaked or missing row.
 const verifyVisibility=async(target:Handle,changedAt:number,before:Set<string>)=>{
  if(target.busy)return;target.busy=true;visibilityChecks.checked++;
  let fresh:any;let stop=()=>{};
  try{
   fresh=await login(target.email,target.tenant,oracleKind);
   const fw=fresh.c.watchReplica(api.tasks.recentWorkspaceTasks,{});stop=fw.onUpdate(()=>{});
   const complete=(w:any)=>w.status().isUpToDate&&w.localReplicaState()?.completeness==='complete';
   if(!await until(()=>complete(fw),60000)){visibilityChecks.mismatched++;error(new Error('Visibility oracle snapshot never completed'));return;}
   const same=()=>{const a=ids(target.tw().localReplicaResult()||[]),b=ids(fw.localReplicaResult()||[]);return complete(target.tw())&&a.size===b.size&&[...a].every(x=>b.has(x));};
   if(await until(same,15000)){
    const after=ids(fw.localReplicaResult()||[]);
    const delta=visibilityDelta(before,after);visibilityChecks.additions+=delta.additions;visibilityChecks.removals+=delta.removals;
    if(after.size===before.size&&[...after].every(x=>before.has(x)))visibilityChecks.noChange++;
    else{visibilityChecks.converged++;sample('visibility',Date.now()-changedAt,changedAt);}
   }else{
    const a=ids(target.tw().localReplicaResult()||[]),b=ids(fw.localReplicaResult()||[]);
    visibilityChecks.mismatched++;
    error(new Error(`Visibility mismatch after 15 s: ${[...a].filter(x=>!b.has(x)).length} leaked, ${[...b].filter(x=>!a.has(x)).length} missing`));
   }
  }catch(e){visibilityChecks.mismatched++;error(e);}
  finally{stop();fresh?.c.close();target.busy=false;}
 };
 const jobs=Array.from({length:target},(_,i)=>(async()=>{
  if(!mine(i))return;
  let seed=(i+1)*2654435761;const random=()=>{seed^=seed<<13;seed^=seed>>>17;seed^=seed<<5;return (seed>>>0)/4294967296;};
  let transitionSeed=(i+1)*2246822519;const transitionRandom=()=>{transitionSeed^=transitionSeed<<13;transitionSeed^=transitionSeed>>>17;transitionSeed^=transitionSeed<<5;return (transitionSeed>>>0)/4294967296;};
  const think=()=>{const knots=[[0,0],[.25,5.163],[.5,12.732],[.75,44.774],[.95,735.971],[1,1800]];const u=random();const hi=knots.findIndex(k=>k[0]>=u);const a=knots[Math.max(0,hi-1)],b=knots[Math.max(1,hi)];return (a[1]+(b[1]-a[1])*(u-a[0])/(b[0]-a[0]))*1.1*1000;};
  const scheduled=plan?.[i]; const plannedAt=started+(scheduled?.at??i*ramp*1000/target);
  const signIn:any={user:i,cohort:scheduled?.cohort??'uniform',plannedAt};signIns.push(signIn);
  await pause(Math.max(0,plannedAt-Date.now()));if(stopping)return;
  signIn.attemptedAt=Date.now();
  const f=chooseTenant(i),email=`load-user-${i}@example.invalid`;
  // A session: client, presence watch, metadata watches and the task replica.
  // The reconnect wave replaces it mid-run.
  let c:any,member='',tw:any,stopPresence=()=>{},stopTasks=()=>{},extras:any,refreshTimer:any;
  let cachedWindows=0,loadedSnapshot:any,expectedCacheKeys:string[]=[];
  const storage=expanded?(userKind(i,target)==='sdk'?new MemoryLocalReplicaStorage():new ThinCheckpointStore()):undefined;
  if(storage instanceof ThinCheckpointStore)checkpointStores.push(storage);
  if(storage&&!(storage instanceof ThinCheckpointStore)){const load=storage.load.bind(storage);storage.load=async(...args:any[])=>{const snap=await load(...args);loadedSnapshot=snap;cachedWindows=Object.keys(snap?.liveQueries||{}).length;return snap;};}
  let reloadEvent:any;
  const allReady=()=>extras?.ready()&&tw?.status().isUpToDate&&tw?.localReplicaState()?.completeness==='complete'&&presenceReadyAt!==Infinity;
  const maybeInitialReady=()=>{if(reloadEvent&&reloadEvent.readyAt===undefined&&allReady()){Object.assign(reloadEvent,typeof c.cacheEvidence==='function'?c.cacheEvidence(expectedCacheKeys):sdkCacheEvidence(loadedSnapshot,expectedCacheKeys));reloadEvent.readyAt=Date.now();sample('cachedReload',reloadEvent.readyAt-reloadEvent.plannedAt,reloadEvent.plannedAt);}if(signIn.readyAt===undefined&&allReady())signIn.readyAt=Date.now();};
  // Runtime access tokens live 15 min (control.rs session_result): refresh 2 min
  // before expiry and re-authenticate the socket, as the SDK's session does.
  // Without it every session drops 15 min after sign-in.
  const scheduleRefresh=(grant:any)=>{clearTimeout(refreshTimer);if(!grant?.refreshToken)return;const at=Number(grant.expiresAt)||Date.now()+900000;
   refreshTimer=setTimeout(async()=>{if(stopping)return;try{const g=await c.action(control.auth.refreshSession,{refreshToken:grant.refreshToken});await c.authenticate({project:projectId,token:g.accessToken,tenant:f.tenant});scheduleRefresh(g);}catch(e){error(e);}},Math.max(5000,at-Date.now()-120000));};
  let initial=true,presenceReadyAt=Infinity,actionAt=Infinity,subscribedAt=Infinity;
  const seen=new Map<string,number>(),seenTasks=new Map<string,number>(),firstSeen=new Map<string,number>();
  const connect=async(kind:string)=>{
   const session=await timed(kind,()=>login(email,f.tenant,userKind(i,target),storage));if(!session)return false;
   if(kind==='login')signIn.authAt=Date.now();
   c=session.c;if(kind==='login'&&presenceV2&&c.presenceSupported?.())presenceV2Users++;member=session.grant.tenants.find((t:any)=>t.id===f.tenant)?.memberId;scheduleRefresh(session.grant);
   presenceReadyAt=Infinity;
   if(presenceV2&&c.presenceSupported?.()){stopPresence=c.subscribePresence(()=>{try{const snap=c.presenceSnapshot();if(!snap.ready)return;if(presenceReadyAt===Infinity)presenceReadyAt=Date.now();maybeInitialReady();
    for(const [id,m] of snap.members as Map<string,any>){const ts=pending.get(id);if(ts&&ts>=presenceReadyAt&&m.state===presenceStates.get(id)&&seen.get(id)!==ts){seen.set(id,ts);sample('delivery',Date.now()-ts,ts);}}
   }catch(e){error(e);}});}else{
   const watch=c.watchReplica(api.presence.list,{});
   stopPresence=watch.onUpdate(()=>{try{
    if(presenceReadyAt===Infinity){if(!watch.status().isUpToDate||watch.localReplicaState()?.completeness!=='complete')return;presenceReadyAt=Date.now();maybeInitialReady();}
    for(const row of watch.localReplicaResult()||[]){const ts=pending.get(row.userId);if(ts&&ts>=presenceReadyAt&&row.lastSeenAt>=ts&&seen.get(row.userId)!==row.lastSeenAt){seen.set(row.userId,row.lastSeenAt);sample('delivery',Date.now()-ts,ts);}}
   }catch(e){error(e);}});}
   // LOAD_APP_OPENS: also open the real web app's page-load collection set
   // (captured from the browser, app-opens-*.json), deduplicated by path+args.
   const baseRefs:[any,any][]=[api.workspaces.list,api.statuses.statuses,api.tags.tags,api.members.listMembers,api.members.listMemberTeams,api.taskResources.taskUsers,api.taskResources.taskTags,api.workspacePreferences.workspacePreferences].map(ref=>[ref,{}] as [any,any]);
   const refs=appOpens?mergeOpens(baseRefs,appOpens,member):baseRefs;
   expectedCacheKeys=typeof c.replicaSignature==='function'?[...refs,[api.tasks.recentWorkspaceTasks,{}],...(!(presenceV2&&c.presenceSupported?.())?[[api.presence.list,{}]]:[])].map(([ref,args])=>c.replicaSignature(ref,args)):[];
   expectedCacheKeys=[...new Set(expectedCacheKeys)];
   extras=watchAll(c,refs,maybeInitialReady);
   const w=c.watchReplica(api.tasks.recentWorkspaceTasks,{});const q0=performance.now();let first=true;
   stopTasks=w.onUpdate(()=>{try{maybeInitialReady();
    if(first&&w.status().isUpToDate&&w.localReplicaState()?.completeness==='complete'){first=false;sample(kind==='login'?'initialLoad':'reconnect',performance.now()-q0);if(initial){initial=false;subscribedAt=Date.now();actionAt=Date.now()+think();}snapshots.push({received:(w.localReplicaResult()||[]).length,kind});}
    for(const row of w.localReplicaResult()||[]){if(!firstSeen.has(row._id))firstSeen.set(row._id,Date.now());const op=pendingTasks.get(f.tenant+':'+row._id);const ts=op?.at;if(op&&ts&&ts>=subscribedAt&&firstSeen.get(row._id)!<=ts&&op.matches(row,ts)&&seenTasks.get(row._id)!==ts){seenTasks.set(row._id,ts);sample('taskDelivery',Date.now()-ts,ts);}}
   }catch(e){error(e);}});
   tw=w;return true;
  };
  const disconnect=()=>{clearTimeout(refreshTimer);stopPresence();stopTasks();extras?.close();c?.close();};
  if(!await plannedStart.run({at:performance.now()-Math.max(0,Date.now()-plannedAt),used:false},()=>connect('login'))){ready();return;}
  if(stopping){disconnect();ready();return;}users.push({close:()=>c.close()});
  const admin=(f.adminMemberIds||[]).includes(member),approver=(f.approverMemberIds||[]).includes(member);
  const handle:Handle={i,email,member,tenant:f.tenant,tw:()=>tw,admin,busy:false};
  (tenantUsers.get(f.tenant)??tenantUsers.set(f.tenant,[]).get(f.tenant)!).push(handle);
  ready();
  const periodicPending=new Set<Promise<unknown>>();
  const launch=(fn:()=>Promise<unknown>)=>{const p=fn();periodicPending.add(p);void p.finally(()=>periodicPending.delete(p));};
  let beatInFlight=false,supportInFlight=false,transitionInFlight=false,idle=false,reconnecting=false;
  const v2=()=>presenceV2&&c?.presenceSupported?.();
  const sendBeat=async()=>{if(v2())return;if(stopping||reconnecting||Date.now()>=deadline||(beatInFlight&&!openLoop))return;beatInFlight=true;pending.set(member,Date.now());try{await timed('heartbeat',()=>reducer(c,api.presence.beat,{}));}finally{beatInFlight=false;}};
  const sendSupport=async()=>{if(v2())return;if(stopping||reconnecting||Date.now()>=deadline||(supportInFlight&&!openLoop))return;supportInFlight=true;try{await timed('heartbeat',()=>reducer(c,control.support.heartbeat,{release:'synthetic-local',environment:'test'}));}finally{supportInFlight=false;}};
  let transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;
  const sendTransition=async(state:string)=>{if(v2()){if(stopping||reconnecting||Date.now()>=deadline)return;pending.set(member,Date.now());presenceStates.set(member,state);c.setPresence(state);if(!openLoop)transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;return;}if(stopping||reconnecting||Date.now()>=deadline||(transitionInFlight&&!openLoop))return;transitionInFlight=true;pending.set(member,Date.now());try{await timed('heartbeat',()=>reducer(c,api.presence.transition,{state}));}finally{transitionInFlight=false;if(!openLoop)transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;}};
  // Match the independent app providers: fixed 30-second ticks, skip an
  // overlapping heartbeat, and continue them while foreground saves await.
  // Open loop: a timer's call starts its clock at the planned tick, and the
  // next transition is planned from the previous plan, not its completion.
  const planned=(fn:()=>Promise<unknown>,dueWall=Date.now())=>()=>plannedStart.run({at:performance.now()-Math.max(0,Date.now()-dueWall),used:false},fn);
  const sessionTimers=[setInterval(()=>launch(planned(sendBeat)),30000),setInterval(()=>launch(planned(sendSupport)),30000),setInterval(()=>{if(Date.now()>=transitionAt&&(!transitionInFlight||openLoop)){idle=!idle;const due=transitionAt;if(openLoop)transitionAt=due-Math.log(Math.max(transitionRandom(),.000001))*101000;launch(planned(()=>sendTransition(idle?'idle':'active'),openLoop?due:Date.now()));}},openLoop?20:200)];
  periodicTimers.push(...sessionTimers);launch(sendSupport);launch(()=>sendTransition('active'));
  const detailWindows=new Set<any>();
  const dwell=(win:any)=>{detailWindows.add(win);setTimeout(()=>{win.close();detailWindows.delete(win);},Math.min(think(),120000));};
  const rows=()=>(tw.localReplicaResult()||[]) as any[];
  const open=()=>rows().filter(r=>r.statusId!==f.completedStatusId);
  const expectTask=(id:string,matches:(row:any,at:number)=>boolean)=>{const op:TaskOp={at:Date.now(),matches,path:'',by:member,state:'pending'};pendingTasks.set(f.tenant+':'+id,op);expectationStats.ops++;return op;};
  // A save whose Reducer failed never changes the row: drop its expectation.
  const saveFor=async(op:TaskOp,ref:any,args:any)=>{op.path=ref.path||'';op.args=JSON.stringify(args).slice(0,240);let ok=false;const v=await timed('save',async()=>{try{const r=await reducer(c,ref,args);ok=true;return r;}catch(e:any){op.error=String(e?.message||e).slice(0,160);throw e;}});op.state=ok?'ok':'failed';op.savedAt=Date.now();if(!ok)expectationStats.failedSaves++;return v;};
  const save=(ref:any,args:any)=>timed('save',()=>reducer(c,ref,args));
  const others=()=>(tenantUsers.get(f.tenant)||[]).filter(h=>h.member!==member&&!h.admin);
  // Workspaces this user may create in (team-linked), from its workspace replica.
  const myWorkspaces=()=>new Set(extras.rows(0).map((w:any)=>w._id));
  // Weighted foreground actions for every user (weights in percent).
  // LOAD_MIX=production: task creates at the production telemetry rate (0.014 vs the
  // default mix's ~0.097 creates per person-minute, notification-fanout doc) and no
  // workspace chat sends (none in the sample). LOAD_CREATE_ASSIGN_FRACTION: share of
  // creates that carry the creator as assignee (the sample does not record it; default 0).
  const productionMix=process.env.LOAD_MIX==='production';
  const createWeight=6*(productionMix?Number(process.env.LOAD_CREATE_SCALE||0.014/0.097):1),chatWeight=productionMix?0:4;
  const assignFraction=Number(process.env.LOAD_CREATE_ASSIGN_FRACTION||0);
  const assignees=()=>random()<assignFraction&&member?{assigneeUserIds:[member]}:{};
  const actions:[number,()=>Promise<unknown>][]=[
   // Reads (25): task detail dwell, workspace switch, notifications, chat.
   [15,async()=>{const t=pick(rows(),random);if(!t)return;const win=watchAll(c,[[api.tasks.get,{id:t._id}],[api.taskResources.listTaskNotes,{taskId:t._id}],[api.taskResources.listTaskLogs,{taskId:t._id}],[api.taskResources.listTaskViewsByTaskId,{taskId:t._id}],[api.approvals.listTaskApprovals,{taskId:t._id}],[api.taskFindings.listForTask,{taskId:t._id}],[api.taskResources.listTaskUsers,{taskId:t._id}],[api.customFields.listTaskValues,{taskId:t._id}]]);const q=startTime();if(await until(win.ready,30000))sample('query',performance.now()-q);else error(new Error('Task detail queries never became current'),performance.timeOrigin+q);dwell(win);const cur=rows().find(r=>r._id===t._id)||t;if(viewSetsFirstViewedAt(cur,member))await saveFor(expectTask(t._id,row=>firstViewed(row)),api.taskResources.recordTaskViewByTaskId,{taskId:t._id});else{if(!firstViewed(cur))expectationStats.ownTaskViews++;await save(api.taskResources.recordTaskViewByTaskId,{taskId:t._id});}}],
   [4,async()=>{const win=watchAll(c,[[api.statuses.statuses,{}],[api.priorities.list,{}],[api.taskWorkspaceContexts.contexts,{}],[api.roles.memberTeams,{}],[api.taskResources.recentTaskAcknowledgments,{}]]);const q=startTime();if(await until(win.ready,30000))sample('query',performance.now()-q);else error(new Error('Query never became current'),performance.timeOrigin+q);dwell(win);}],
   [3,async()=>{const win=watchAll(c,[[api.notifications.listNotifications,{}]]);const q=startTime();if(await until(win.ready,30000))sample('query',performance.now()-q);else error(new Error('Query never became current'),performance.timeOrigin+q);dwell(win);if(random()<.3)await save(api.notifications.markAllRead,{});}],
   [3,async()=>{const win=watchAll(c,[[api.chat.workspaceChat,{}]]);const q=startTime();if(await until(win.ready,30000))sample('query',performance.now()-q);else error(new Error('Query never became current'),performance.timeOrigin+q);dwell(win);}],
   // Edits (40): field edits fan out to every subscriber that can see the task.
   [8,async()=>{const t=pick(open(),random);if(!t)return;const description='Updated '+randomUUID()+' '+(t.description||'').slice(0,300);await saveFor(expectTask(t._id,row=>row.description===description),api.tasks.updateDescription,{taskId:t._id,description});}],
   [6,async()=>{const t=pick(open(),random);if(!t)return;const dueDate=Date.now()+Math.floor(random()*14)*86400000;await saveFor(expectTask(t._id,row=>row.dueDate===dueDate),api.tasks.update,{taskId:t._id,updates:{dueDate}});}],
   [4,async()=>{const t=pick(open(),random);if(!t)return;const name='Load task '+randomUUID().slice(0,8);await saveFor(expectTask(t._id,row=>row.name===name),api.tasks.rename,{taskId:t._id,name});}],
   [4,async()=>{const t=pick(open(),random);if(!t)return;const priorityId=(f.priorityIds||[f.priorityId]).find((p:string)=>p!==t.priorityId)||f.priorityId;await saveFor(expectTask(t._id,row=>row.priorityId===priorityId),api.tasks.setPriority,{taskId:t._id,priorityId});}],
   [4,async()=>{const t=pick(open(),random);if(!t)return;const dueDate=Date.now()+Math.floor(random()*30)*86400000;await saveFor(expectTask(t._id,row=>row.dueDate===dueDate),api.tasks.reschedule,{taskId:t._id,dueDate});}],
   [5,async()=>{const t=pick(open().filter(r=>r.statusId!==f.nextStatusId),random);if(!t)return;await saveFor(expectTask(t._id,row=>row.statusId===f.nextStatusId),api.tasks.start,{taskId:t._id,statusId:f.nextStatusId});}],
   [4,async()=>{const t=pick(rows().filter(r=>r.statusId!==f.statusId),random);if(!t)return;const statusId=random()<.15?f.completedStatusId:f.statusId;await saveFor(expectTask(t._id,row=>row.statusId===statusId),api.tasks.requestTransition,{taskId:t._id,statusId});}],
   [3,async()=>{const t=pick(open(),random);if(!t)return;const tag=random()<.6;await save(tag?api.taskResources.addTagByTaskId:api.taskResources.removeTagByTaskId,{taskTagId:randomUUID(),taskId:t._id,tagId:f.tag._id});}],
   [2,async()=>{const t=pick(open().filter(r=>r.spotId),random);if(!t||!f.spotChildren)return;const spotId=pick(f.spotChildren as string[],random);await saveFor(expectTask(t._id,row=>row.spotId===spotId),api.tasks.setSpot,{taskId:t._id,spotId});}],
   // Assignments (5): assigning another member makes the task visible to them.
   [5,async()=>{const t=pick(open(),random),h=pick(others(),random);if(!t||!h)return;await save(api.taskResources.assignUser,{taskId:t._id,userId:h.member});}],
   // Creates (20): tasks, comments, relations, workspace chat.
   [createWeight,async()=>{const entityId=randomUUID();const allowed=(f.workspaceIds||[f.workspaceId]).map((w:string,k:number)=>[w,k] as [string,number]).filter(([w])=>myWorkspaces().has(w));if(!allowed.length)return;const workspace=pick(allowed,random)[1];await saveFor(expectTask(entityId,()=>true),api.tasks.create,{entityId,name:'E2E load '+randomUUID(),workspaceId:(f.workspaceIds||[f.workspaceId])[workspace],categoryId:(f.categoryIds||[f.categoryId])[workspace],priorityId:f.priorityId,spotId:random()<.8&&f.spotChildren?pick(f.spotChildren as string[],random):undefined,...assignees()});}],
   [8,async()=>{const t=pick(rows(),random);if(!t)return;await save(api.taskResources.createNote,{entity:{_id:randomUUID(),_creationTime:Date.now(),taskId:t._id,userId:member,source:'web',note:'Synthetic local comment '+randomUUID()}});}],
   [2,async()=>{const t=pick(open(),random),u=pick(open(),random);if(!t||!u||t._id===u._id)return;await save(api.taskResources.createTaskRelation,{relationId:randomUUID(),taskId:t._id,relatedTaskId:u._id,relationType:random()<.5?'blocks':'related'});}],
   [chatWeight,async()=>{const t=pick(rows(),random);if(!t)return;await save(api.chat.sendWorkspaceChat,{entity:{_id:randomUUID(),_creationTime:Date.now(),userId:member,workspaceId:t.workspaceId,message:'Synthetic chat '+randomUUID()}});}],
   // Approvals (10): tasks that enter approval, and approver decisions.
   [createWeight,async()=>{if(!f.approvalCategoryId||!myWorkspaces().has(f.workspaceId))return;const entityId=randomUUID();const v=await saveFor(expectTask(entityId,()=>true),api.tasks.create,{entityId,name:'Needs approval '+randomUUID().slice(0,8),workspaceId:f.workspaceId,categoryId:f.approvalCategoryId,priorityId:f.priorityId,...assignees()});if(v!==undefined)(approvalQueue.get(f.tenant)??approvalQueue.set(f.tenant,[]).get(f.tenant)!).push(entityId);}],
   [4,async()=>{if(!approver)return;const queue=approvalQueue.get(f.tenant)||[];const taskId=queue.find(id=>rows().some(r=>r._id===id));if(!taskId)return;queue.splice(queue.indexOf(taskId),1);await save(api.approvals.decideByTask,{taskId,decision:random()<.85?'approved':'rejected',comment:'Synthetic decision'});}],
  ];
  const totalWeight=actions.reduce((a,[w])=>a+w,0);
  const act=async()=>{let x=random()*totalWeight;for(const [w,fn] of actions){if((x-=w)<0)return fn();}};
  // Admins change permissions every 2-5 minutes; each change is followed by
  // the visibility oracle on the affected user.
  let permissionAt=Date.now()+(120+random()*180)*1000;let reorgDone=false;
  const changePermission=async()=>{
   const target=pick(others().filter(h=>!h.busy),random);if(!target)return;
   const before=ids(target.tw().localReplicaResult()||[]);const mine=rows();
   const spot=pick((f.spotParents||[]) as string[],random),team=pick((f.teamIds||[]) as string[],random);
   const hidden=mine.filter(r=>!before.has(r._id)&&r.statusId!==f.completedStatusId);
   const choice=random();let v:any;
   if(choice<.2&&spot)v=await timed('permission',()=>reducer(c,api.members.assignMemberSpot,{memberId:target.member,spotId:spot}));
   else if(choice<.35&&spot)v=await timed('permission',()=>reducer(c,api.members.removeMemberSpot,{memberId:target.member,spotId:spot}));
   else if(choice<.5&&team){if(extras.rows(4).some((r:any)=>r.memberId===target.member&&r.teamId===team&&!r.deletedAt))return;v=await timed('permission',()=>reducer(c,api.members.addMemberTeam,{memberId:target.member,teamId:team,roleKey:'member'}));}
   else if(choice<.65){const mt=extras.rows(4).find((r:any)=>r.memberId===target.member&&!r.deletedAt);if(!mt)return;v=await timed('permission',()=>reducer(c,api.members.removeMemberTeam,{id:mt._id}));}
   else if(choice<.8&&hidden.length){const t=pick(hidden,random);const share=await timed('permission',()=>reducer(c,api.taskResources.shareTask,{taskId:t._id,sharedToUserId:target.member}));v=share;if(typeof share==='string'&&random()<.5)setTimeout(()=>launch(async()=>{const at=Date.now(),b=ids(target.tw().localReplicaResult()||[]);if(await timed('permission',()=>reducer(c,api.taskResources.revokeShare,{id:share}))!==undefined)await verifyVisibility(target,at,b);}),60000);}
   else if(choice<.9&&hidden.length){const t=pick(hidden,random);v=await timed('permission',()=>reducer(c,api.tasks.assign,{taskId:t._id,userIds:[target.member]}));}
   else{const t=pick(mine.filter(r=>before.has(r._id)&&r.spotId),random);if(!t||!f.spotChildren)return;v=await timed('permission',()=>reducer(c,api.tasks.setSpot,{taskId:t._id,spotId:pick(f.spotChildren as string[],random)}));}
   if(v!==undefined)await verifyVisibility(target,Date.now(),before);
  };
  // One explicit share/revoke cycle per shard, using ordinary app reducers.
  // Failure to find a genuinely hidden row is a missing case, never a pass.
  let permissionCycleDone=false;
  const permissionCycle=async()=>{
   const target=others().find(h=>!h.busy);if(!target)return;
   const before=ids(target.tw().localReplicaResult()||[]);
   const row=rows().find(r=>!before.has(r._id)&&r.statusId!==f.completedStatusId);if(!row)return;
   permissionCycleDone=true;
   const cycle:any={user:target.i,row:row._id,at:Date.now(),additions:0,removals:0};permissionCycles.push(cycle);
   try{
    const at=Date.now();const share=await reducer(c,api.taskResources.shareTask,{taskId:row._id,sharedToUserId:target.member});
    if(typeof share!=='string')throw Error('Share did not return a revoke identifier');
    await verifyVisibility(target,at,before);
    const granted=ids(target.tw().localReplicaResult()||[]);
    cycle.granted=granted.has(row._id);cycle.grantMs=Date.now()-at;cycle.additions=visibilityDelta(before,granted).additions;
    await pause(10000);
    const revokedAt=Date.now();await reducer(c,api.taskResources.revokeShare,{id:share});
    await verifyVisibility(target,revokedAt,granted);
    const after=ids(target.tw().localReplicaResult()||[]);
    cycle.oldRowRemoved=!after.has(row._id);cycle.revoked=cycle.oldRowRemoved;cycle.revokeMs=Date.now()-revokedAt;cycle.removals=visibilityDelta(granted,after).removals;
    if(!cycle.granted||!cycle.oldRowRemoved)throw Error('Share/revoke old-row oracle failed');
   }catch(e){cycle.error=String(e);error(e,cycle.at);}
  };
  // One reorganization per generator process: reassign ~50 tasks to one member
  // and move that member to another team.
  const reorg=async()=>{
   const target=pick(others().filter(h=>!h.busy),random);if(!target)return;
   const before=ids(target.tw().localReplicaResult()||[]);
   const numbers=rows().filter(r=>r.statusId!==f.completedStatusId&&typeof r.id==='number').slice(0,50).map(r=>r.id);
   const at=Date.now();
   if(numbers.length)await timed('permission',()=>reducer(c,api.tasks.bulkReassign,{ids:numbers,userIds:[target.member]}));
   const team=pick((f.teamIds||[]) as string[],random);
   if(team&&!extras.rows(4).some((r:any)=>r.memberId===target.member&&r.teamId===team&&!r.deletedAt))await timed('permission',()=>reducer(c,api.members.addMemberTeam,{memberId:target.member,teamId:team,roleKey:'member'}));
   await verifyVisibility(target,at,before);
  };
  // Opt-in realistic reload rate replaces the legacy stress wave. Uniform phases
  // keep offered page loads independent of server response times.
  const reloadsPerHour=Number(process.env.LOAD_RELOADS_PER_USER_HOUR ?? 0);
  if(!Number.isFinite(reloadsPerHour)||reloadsPerHour<0)throw Error('Invalid reload rate');
  const reloadPeriod=reloadsPerHour>0?3600000/reloadsPerHour:Infinity;
  let deployReloadDone=false;
  const deployReloadAt=()=>reconnectAt(0)+Number(process.env.LOAD_DEPLOY_RELOAD_OFFSET_SECONDS||120)*1000;
  let reconnectTime=process.env.LOAD_RELOADS_PER_USER_HOUR!==undefined?Date.now()+random()*reloadPeriod:(random()<.3?reconnectAt(.5)+random()*10000:Infinity);
  // Open loop: the next action is planned from the previous plan, not from
  // when the previous action finished, so a slow server cannot lower load.
  let foregroundTimer:ReturnType<typeof setTimeout>|undefined,foregroundInFlight=0;const foregroundPending=new Set<Promise<unknown>>();
  const fireForeground=(due:number)=>{
   if(stopping||Date.now()>=deadline)return;
   if(reconnecting){scheduleStats.skippedDuringReconnect++;return;}
   if(foregroundInFlight>=maxInFlightPerUser){scheduleStats.overflow++;error(Error(`Open-loop overflow: ${maxInFlightPerUser} actions already in flight for one user`));return;}
   scheduleStats.fired++;if(scheduleStats.lateMs.length<200000)scheduleStats.lateMs.push(Date.now()-due);foregroundInFlight++;
   const p=Promise.resolve().then(planned(()=>act(),due)).catch(error).finally(()=>{foregroundInFlight--;foregroundPending.delete(p);});foregroundPending.add(p);
  };
  let foregroundArmed=false;
  const armForeground=()=>{if(stopping||Date.now()>=deadline)return;if(!foregroundArmed){foregroundArmed=true;if(actionAt<Date.now())actionAt=Date.now();}const due=actionAt;foregroundTimer=setTimeout(()=>{actionAt=due+think();fireForeground(due);armForeground();},Math.max(0,due-Date.now()));};
  while(!stopping&&Date.now()<deadline){
   maybeInitialReady();
   if(openLoop&&!initial&&!foregroundTimer)armForeground();
   if(expanded&&!deployReloadDone&&Date.now()>=deployReloadAt()&&!reconnecting&&!initial){
    deployReloadDone=true;reconnecting=true;
    reloadEvent={user:i,plannedAt:deployReloadAt(),attemptedAt:Date.now(),cacheSupported:!!storage,cachedWindows:0};reloadEvents.push(reloadEvent);
    cachedWindows=0;loadedSnapshot=undefined;
    await Promise.all([...periodicPending,...foregroundPending]);disconnect();tw=undefined;extras=undefined;
    if(!await connect('reconnect'))reloadEvent.error='reload login failed';reconnecting=false;
   }else if(Date.now()>=reconnectTime&&!reconnecting&&!initial){reconnecting=true;reconnectTime+=reloadPeriod;await Promise.all([...periodicPending,...foregroundPending]);disconnect();const connected=await connect('reconnect');reconnecting=false;if(!connected)break;}
   else if(expanded&&admin&&!initial&&!permissionCycleDone&&Date.now()>=reconnectAt(0)+30000&&(tenantUsers.get(f.tenant)||[]).find(h=>h.admin)===handle){await permissionCycle();}
   else if(admin&&!initial&&!reorgDone&&shard===0&&Date.now()>=reconnectAt(.3)&&(tenantUsers.get(f.tenant)||[]).find(h=>h.admin)===handle){reorgDone=true;await reorg();}
   else if(admin&&!initial&&Date.now()>=permissionAt){await changePermission();permissionAt=Date.now()+(120+random()*180)*1000;}
   else if(!openLoop&&!initial&&Date.now()>=actionAt){await act();actionAt=Date.now()+think();}
   await pause(200);
  }
  for(const timer of sessionTimers)clearInterval(timer);
  clearTimeout(foregroundTimer);await Promise.all([...foregroundPending]);
  await Promise.all([...periodicPending]);
  for(const win of detailWindows)win.close();
  await pause(1000);
  if(member && (pending.get(member)||0)>=presenceReadyAt && !seen.has(member))error(new Error('Own presence updates never delivered'));
  if(initial) error(new Error('task subscription never became current'));
  // Under visibility masks a user receives only rows it may see: an update is
  // missing only when the task is still in this replica without the change.
  // Edits made by other users in the last seconds of the plateau are still in
  // flight (their saves and deliveries lag under load): keep the subscription
  // open for a bounded grace and only then count what is still missing.
  const clockToleranceMs=Number(process.env.LOAD_CLOCK_TOLERANCE_MS||250);
  const unmet=()=>[...pendingTasks].filter(([key,op])=>key.startsWith(f.tenant+':')&&unmetTaskUpdate(op,rows().find(r=>r._id===key.slice(f.tenant.length+1)),{subscribedAt,clockToleranceMs}));
  const graceMs=Number(process.env.LOAD_TEARDOWN_DELIVERY_MS||30000),graceEnd=Date.now()+graceMs,before=unmet().length;
  while(Date.now()<graceEnd&&unmet().length)await pause(250);
  const missing=unmet();expectationStats.lateDelivered+=Math.max(0,before-missing.length);
  for(const [key,op] of missing){const id=key.slice(f.tenant.length+1),row=rows().find(r=>r._id===id);
   if(op.state==='pending'){expectationStats.pendingSaveAtTeardown++;error(new Error('Task update save still pending at teardown'));continue;}
   expectationStats.undeliveredOps.add(key+'@'+op.at);error(new Error('Task update was not delivered before teardown'));
   if(undelivered.length<2000)undelivered.push({tenant:f.tenant,task:id,observer:member,observerUser:i,by:op.by,path:op.path,args:op.args,at:op.at,savedAt:op.savedAt,checkedAt:Date.now(),expect:op.matches.toString().slice(0,160),row:{name:row.name,statusId:row.statusId,priorityId:row.priorityId,dueDate:row.dueDate,spotId:row.spotId,firstViewedAt:row.firstViewedAt,updatedAt:row.updatedAt,createdBy:row.createdBy,description:String(row.description??'').slice(0,48)}});}
  if(extras.ready()&&tw.status().isUpToDate)collectionReadiness.readyAtTeardown++;else collectionReadiness.notReadyAtTeardown++;
  if(!extras.ready())error(new Error('Metadata subscription never became current'));
  disconnect();
 })().catch(e=>{error(e);ready();}));
 // Bounded teardown: a job stuck on a reply that never comes (seen at the end of
 // 150-200 user plateaus) must not hang the shard; count it and write the result.
 {let settled=0;const drainMs=Number(process.env.LOAD_TEARDOWN_MS||90000);await Promise.race([Promise.all(jobs.map(j=>j.finally(()=>settled++))),(async()=>{while(Date.now()<deadline)await pause(1000);await pause(drainMs);})()]);
  if(settled<jobs.length)for(let k=settled;k<jobs.length;k++)error(new Error(`Load job still pending ${Math.round(drainMs/1000)} s after the plateau (teardown)`));}
 clearInterval(metricTimer);stopping=true;for(const timer of periodicTimers)clearInterval(timer);for(const c of users)c.close();
 if(!measurements.delivery.length) error(new Error('No update delivery samples'));
 clearInterval(costTimer);{const u=process.cpuUsage(cpuAtStart);Object.assign(generatorCost,{cpuSeconds:(u.user+u.system)/1e6,wallSeconds:(performance.now()-wallAtStart)/1000});}
 const lateSorted=scheduleStats.lateMs.sort((a,b)=>a-b);const schedule={kind:scheduleStats.kind,fired:scheduleStats.fired,overflow:scheduleStats.overflow,skippedDuringReconnect:scheduleStats.skippedDuringReconnect,startLateMs:quantiles(lateSorted)};
 for(const e of lifecycle.events)if(e.type==='refused')error(Error('Replica open refused: '+e.path),e.at);
 const deployReloadWindowMs=[deadline-seconds*1000+Number(process.env.LOAD_DEPLOY_RELOAD_OFFSET_SECONDS||120)*1000,deadline-seconds*1000+Number(process.env.LOAD_DEPLOY_RELOAD_OFFSET_SECONDS||120)*1000+5000];
 const checkpointCost={kind:'process-local immutable row pool; per-user window membership',logicalBytes:checkpointStores.reduce((n,s)=>n+s.stats().bytes,0),peakLogicalBytesSum:checkpointStores.reduce((n,s)=>n+s.stats().peakBytes,0),users:checkpointStores.length,windows:checkpointStores.reduce((n,s)=>n+s.stats().windows,0),checkpointCpuSeconds:thinDrivers.reduce((n,c)=>n+c.checkpointCpuUs/1e6,0),rowPoolCanonicalBytes:[...thinRowPool.rows.values()].reduce((n,e)=>n+Buffer.byteLength(e.canonical),0),rowPoolAdditionalSerializationBytes:[...thinRowPool.rows.values()].reduce((n,e)=>n+(e.serializedValue!==e.canonical?Buffer.byteLength(e.serializedValue):0),0)};
 const result={driverDiagnostics:driverDiagnostics(),checkpointFailures:thin?ThinClient.stats.checkpointFailures:[],checkpointCost,artifactPins,deployReloadWindowMs,reloadEvents,permissionCycles,operationAttempts,replicaEvents:lifecycle.events,signinSchedule:mixed?'mixed-v1':'uniform',scheduleStartedMs:started,signIns,presenceV2Users,collectionReadiness,taskExpectations:{ops:expectationStats.ops,failedSaves:expectationStats.failedSaves,lateDelivered:expectationStats.lateDelivered,pendingSaveAtTeardown:expectationStats.pendingSaveAtTeardown,ownTaskViews:expectationStats.ownTaskViews,undeliveredOps:expectationStats.undeliveredOps.size,undelivered},schedule,generatorCost,client:{kind:clientKind,rows:nativeRequested?'native':'legacy',sdkUsers:Array.from({length:target},(_,i)=>i).filter(i=>mine(i)&&userKind(i,target)==='sdk').length,oracle:oracleKind,...(thin?{thin:{...ThinClient.stats,rowPool:{distinctRows:thinRowPool.size,...thinRowPool.stats}}}:{})},sdkVersion:sdkPackage.version,sdkPatch:expectedPatch,steadyStartMs:deadline-seconds*1000,steadyEndMs:deadline,profileHash,distributionProfile:process.env.LOAD_ALL_TENANTS_ACTIVE==='true'?'all-tenants':'production-skew',pathLatencyMs:Object.fromEntries(Object.entries(perPath).map(([path,p])=>[path,{samples:p.durations.length,errors:p.errors,...quantiles(p.durations)}])),aborted,errorEvents,snapshots,visibilityChecks,reconnectWindowMs:process.env.LOAD_RELOADS_PER_USER_HOUR!==undefined?null:[reconnectAt(.5),reconnectAt(.5)+10000],targetUsers:myTarget,connectedUsers:users.length,shard,shards,...(shards>1?{rawSamples:samples,rawPaths:perPath}:{}),rampSeconds:ramp,steadySeconds:seconds,reloadsPerUserHour:process.env.LOAD_RELOADS_PER_USER_HOUR??null,errors,latencyMs:Object.fromEntries(Object.entries(measurements).map(([k,v]:any)=>[k,{samples:v.length,...quantiles(v)}])),note:'Delivery uses presence row timestamps on the same workstation. Task snapshot latency is separate. Setup excluded from sign-in timing.'};
 const steadyLatencyMs=Object.fromEntries(Object.keys(measurements).map(kind=>{const xs=(samples[kind]||[]).filter(x=>x.at>=deadline-seconds*1000&&x.at<deadline).map(x=>x.ms);return [kind,{samples:xs.length,...quantiles(xs)}];}));
 const phaseErrors=(start:number,end:number)=>Object.fromEntries([...new Set(errorEvents.filter(e=>e.at>=start&&e.at<end).map(e=>e.key))].map(key=>[key,errorEvents.filter(e=>e.key===key&&e.at>=start&&e.at<end).length]));
 const steadyErrors=phaseErrors(deadline-seconds*1000,deadline);
 Object.assign(result,{steadyErrors,rampErrors:phaseErrors(started,deadline-seconds*1000),steadyLatencyMs,qualificationLatencyScope:'steady plateau; operations started with all users connected'});
 const q:any=steadyLatencyMs;
 {const rule=passRule({latencyMs:q,presenceV2Users,errors:steadyErrors,connectedUsers:users.length,targetUsers:myTarget,aborted});Object.assign(result,{clientSloPassed:rule.passed&&ThinClient.stats.checkpointFailures.length===0&&visibilityChecks.mismatched===0&&expectationStats.undeliveredOps.size===0&&expectationStats.pendingSaveAtTeardown===0&&!operationAttempts.some(a=>!a.done)&&collectionReadiness.readyAtTeardown===myTarget,errorRate:rule.errorRate,failingKinds:rule.failingKinds});}
 if(mixed){const mixedQualification=qualifyMixed({started,signIns,samples,errors:errorEvents,retries:lifecycle.events,attempts:operationAttempts});Object.assign(result,{mixedQualification,clientSloPassed:result.clientSloPassed&&mixedQualification.every(w=>w.passed)});}
 if(expanded){const expandedQualification=qualifyExpanded({reloads:reloadEvents,targetUsers:myTarget,permissions:permissionCycles,samples,errors:errorEvents,attempts:operationAttempts,reloadWindow:deployReloadWindowMs});Object.assign(result,{expandedQualification,clientSloPassed:result.clientSloPassed&&expandedQualification.passed});}
 eventLoop.disable();gcObserver.disconnect();for(const store of checkpointStores)store.clear();
 console.log(JSON.stringify(result));workerReceipt(process.env.LOAD_RESULT||state+'.result',result,{durable:true});
 process.exit(0);
}
main().catch(e=>{console.error(e);process.exit(1);});
