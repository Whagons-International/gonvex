/** Local-only real SDK workload. Run inside an internal Docker network. */
import { createRequire } from 'node:module';
import { readFile, writeFile, appendFile, realpath } from 'node:fs/promises';
import { writeFileSync, renameSync } from 'node:fs';
import { randomUUID, createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { loopback, quantiles, passRule } from './safety.mjs';

async function main() {
 const profileHash=createHash('sha256').update(await readFile(process.argv[1])).digest('hex');
 const app=process.env.WHAGONS_CHECKOUT!;
 if(!app) throw Error('WHAGONS_CHECKOUT is required');
 const require=createRequire(app+'/package.json');
 const appPackage=JSON.parse(await readFile(app+'/package.json','utf8'));const sdkPackage=JSON.parse(await readFile(app+'/node_modules/@gonvex/client/package.json','utf8'));
 if(appPackage.dependencies?.['@gonvex/client']!==sdkPackage.version)throw Error('Installed SDK must match the pinned app version');
 const lock=await readFile(app+'/pnpm-lock.yaml','utf8');const patchLine=lock.split('\n').find(line=>line.trim().startsWith("'@gonvex/client@"+sdkPackage.version+"':"));
 const expectedPatch=patchLine?.trim().split(': ').at(-1);const sdkPath=await realpath(app+'/node_modules/@gonvex/client');
 if(expectedPatch&&/^[a-f0-9]{64}$/.test(expectedPatch)&&!sdkPath.includes('patch_hash='+expectedPatch))throw Error('Installed SDK patch must match the app lock');
 const {GonvexClient,control}=await import(pathToFileURL(require.resolve('@gonvex/client')).href);
 const {api}=await import(pathToFileURL(app+'/gonvex/_generated/api.ts').href);
 // Authoritative timing never executes local reducers. Read the generated contract
 // without requiring the ignored portable reducer bundles in a clean checkout.
 const executor=await readFile(app+'/gonvex/_generated/local-executor.ts','utf8');
 const contract=executor.match(/clientContract: (\{[^}]+\})/);
 if(!contract)throw Error('Missing generated client contract');
 const localRuntime={clientContract:JSON.parse(contract[1])};
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
 const client=()=>{const c=new GonvexClient(url.replace('http:','ws:')+'/ws',{project:projectId,clientContract:localRuntime.clientContract,outbox:{enabled:false}});c.connect();return c;};
 let recordReducer:((path:string,ms:number,failed:boolean)=>void)|undefined;
 const reducer=async(c:any,ref:any,args:any)=>{const at=performance.now();try{const value=await c.reducer({...ref,localExecution:undefined},args,{offline:'reject'});recordReducer?.(ref.path,performance.now()-at,false);return value;}catch(e){recordReducer?.(ref.path,performance.now()-at,true);throw e;}};
 const identity=async(email:string)=>{
  const body={email,password:'local-synthetic-password',returnSecureToken:true};
  let r=await fetch(auth+'/identitytoolkit.googleapis.com/v1/accounts:signUp?key=local',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  if(!r.ok) r=await fetch(auth+'/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=local',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  const b:any=await r.json();if(!r.ok) throw Error('Emulator identity failed');return b.idToken;
 };
 const login=async(email:string,tenant?:string)=>{
  const c=client();
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
   const count=[9000,5000,2000][i]||100+20*(i%10);
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
 const target=Number(process.env.LOAD_USERS||26),seconds=Number(process.env.LOAD_SECONDS||180),ramp=Number(process.env.LOAD_RAMP_SECONDS||60);
 if(!Number.isSafeInteger(target)||target<1||!Number.isFinite(seconds)||seconds<1) throw Error('Invalid load bounds');
 // One Node event loop saturates near 120 users, so large cases run several
 // shards (shards.sh). Shard k drives users i with i % shards === k; tenant
 // choice, ramp timing and task ordinals stay global, so the mix matches an
 // unsharded run. merge-shards.mjs combines the shards' raw samples.
 const shards=Number(process.env.LOAD_SHARDS||1),shard=Number(process.env.LOAD_SHARD||0);
 if(!Number.isSafeInteger(shards)||shards<1||!Number.isSafeInteger(shard)||shard<0||shard>=shards) throw Error('Invalid shard');
 const mine=(i:number)=>i%shards===shard;const myTarget=Array.from({length:target},(_,i)=>i).filter(mine).length;
 const measurements:any={save:[],heartbeat:[],reducer:[],query:[],delivery:[],taskDelivery:[],login:[]},errors:Record<string,number>={};
 const samples:Record<string,{at:number,ms:number}[]>={};
 const sample=(kind:string,ms:number,at=Date.now()-ms)=>{measurements[kind].push(ms);(samples[kind]??=[]).push({at,ms});};
 const error=(e:any)=>{const key=String(e?.message||e).slice(0,240);errors[key]=(errors[key]||0)+1;};
 const timed=async(kind:string,fn:()=>Promise<any>)=>{const t=performance.now();try{const v=await fn();sample(kind,performance.now()-t);return v;}catch(e){error(e);return undefined;}};
 const perPath:Record<string,{durations:number[],errors:number}>={};
 recordReducer=(path,ms,failed)=>{const p=perPath[path]??={durations:[],errors:0};if(failed)p.errors++;else{p.durations.push(ms);sample('reducer',ms);}};
 const periodicTimers:ReturnType<typeof setInterval>[]=[];
 const users:any[]=[];const snapshots:any[]=[];const pending=new Map<string,number>();const pendingTasks=new Map<string,{at:number,matches:(row:any,at:number)=>boolean}>();
 const started=Date.now();let stopping=false;let aborted=false;let deadline=Infinity;let remaining=myTarget;
 process.on('SIGTERM',()=>{aborted=true;stopping=true;deadline=Date.now();const partial=(process.env.LOAD_RESULT||state+'.result')+'.partial.json';writeFileSync(partial+'.tmp',JSON.stringify({aborted:true,profileHash,targetUsers:myTarget,connectedUsers:users.length,errors,latencyMs:Object.fromEntries(Object.entries(measurements).map(([k,v]:any)=>[k,{samples:v.length,...quantiles(v)}]))}));renameSync(partial+'.tmp',partial);});
 const ready=()=>{if(--remaining===0)deadline=Date.now()+seconds*1000;};
 // Preserve the seven observed tenant weights. An optional stress profile
 // activates the other nineteen tenants too; it is not the observed mix.
 const weights=fixtures.map((_,i)=>[6,5,4,3,2,2,1][i]||(process.env.LOAD_ALL_TENANTS_ACTIVE==='true'?1:0));
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
 const metricTimer=shard!==0?undefined:setInterval(()=>{void fetch(url+'/dev/metrics?project='+encodeURIComponent(projectId),{headers:{authorization:'Bearer '+admin}}).then(r=>r.json()).then(x=>appendFile((process.env.LOAD_RESULT||state)+'.metrics.jsonl',JSON.stringify({at:Date.now(),metrics:{websocket:{accounts:x.websocket?.accounts,connections:x.websocket?.connections,subscriptions:x.websocket?.subscriptions},database:x.database,queryAdmission:x.queryAdmission}})+'\n')).catch(error);},10000);
 const tenantCounts=new Map<string,number>();const ordinals=Array.from({length:target},(_,i)=>{const f=chooseTenant(i);const n=tenantCounts.get(f.tenant)||0;tenantCounts.set(f.tenant,n+1);return n;});
 const jobs=Array.from({length:target},(_,i)=>(async()=>{
  if(!mine(i))return;
  let seed=(i+1)*2654435761;const random=()=>{seed^=seed<<13;seed^=seed>>>17;seed^=seed<<5;return (seed>>>0)/4294967296;};
  let transitionSeed=(i+1)*2246822519;const transitionRandom=()=>{transitionSeed^=transitionSeed<<13;transitionSeed^=transitionSeed>>>17;transitionSeed^=transitionSeed<<5;return (transitionSeed>>>0)/4294967296;};
  const think=()=>{const knots=[[0,0],[.25,5.163],[.5,12.732],[.75,44.774],[.95,735.971],[1,1800]];const u=random();const hi=knots.findIndex(k=>k[0]>=u);const a=knots[Math.max(0,hi-1)],b=knots[Math.max(1,hi)];return (a[1]+(b[1]-a[1])*(u-a[0])/(b[0]-a[0]))*1.1*1000;};
  await pause(i*ramp*1000/target);if(stopping)return;
  const f=chooseTenant(i),email=`load-user-${i}@example.invalid`;
  const session=await timed('login',()=>login(email,f.tenant));if(!session){ready();return;}
  const {c,grant}=session;if(stopping){c.close();ready();return;}users.push(c);
  const member=grant.tenants.find((t:any)=>t.id===f.tenant)?.memberId;
  const watch=c.watchReplica(api.presence.list,{});
  let initial=true;let presenceReadyAt=Infinity;const seen=new Map<string,number>();
  const stop=watch.onUpdate(()=>{try{
   if(presenceReadyAt===Infinity){if(!watch.status().isUpToDate)return;presenceReadyAt=Date.now();}
   const rows=watch.localReplicaResult()||[];
   for(const row of rows){const ts=pending.get(row.userId);if(ts&&ts>=presenceReadyAt&&row.lastSeenAt>=ts&&seen.get(row.userId)!==row.lastSeenAt){seen.set(row.userId,row.lastSeenAt);sample('delivery',Date.now()-ts,ts);}}
  }catch(e){error(e);}});
  await timed('query',()=>c.query(control.auth.publicSettings,{}));
  if(stopping){stop();c.close();ready();return;}
  const extras=[api.workspaces.list,api.statuses.statuses,api.tags.tags,api.members.listMembers,api.members.listMemberTeams,api.taskResources.taskUsers,api.taskResources.taskTags,api.workspacePreferences.workspacePreferences].map(ref=>{
   const w=c.watchReplica(ref,{});const stop=w.onUpdate(()=>{try{w.localReplicaResult();}catch(e){error(e);}});return {w,stop};
  });
  // Keep real task data subscribed for the entire session.
  let actionAt=Infinity;let subscribedAt=Infinity;
  const tw=c.watchReplica(api.tasks.recentWorkspaceTasks,{});const q0=performance.now();
  const seenTasks=new Map<string,number>();
  const stopTasks=tw.onUpdate(()=>{try{if(tw.status().isUpToDate&&tw.localReplicaState()?.completeness==='complete'&&initial){initial=false;subscribedAt=Date.now();actionAt=Date.now()+think();const rows=tw.localReplicaResult()||[];snapshots.push({expected:f.taskCount,received:rows.length,completeness:tw.localReplicaState()?.completeness,truncated:tw.localReplicaState()?.truncated});if(rows.length<f.taskCount)error(new Error('Task subscription returned fewer rows than seeded'));sample('query',performance.now()-q0);}for(const row of tw.localReplicaResult()||[]) {const op=pendingTasks.get(f.tenant+':'+row._id);const ts=op?.at;if(op && ts && ts>=subscribedAt && op.matches(row,ts) && seenTasks.get(row._id)!==ts){seenTasks.set(row._id,ts);sample('taskDelivery',Date.now()-ts,ts);}}}catch(e){error(e);}});
  ready();
  let taskSequence=0,taskState='pending';
  const ownTask=()=>{const n=(ordinals[i]+taskSequence*tenantCounts.get(f.tenant)!)%f.taskCount;return n<f.tasks.length?f.tasks[n]._id:'synthetic-load-'+(n-f.tasks.length+1);};
  const periodicPending=new Set<Promise<unknown>>();
  const launch=(fn:()=>Promise<unknown>)=>{const p=fn();periodicPending.add(p);void p.finally(()=>periodicPending.delete(p));};
  let beatInFlight=false,supportInFlight=false,transitionInFlight=false,idle=false;
  const sendBeat=async()=>{if(stopping||Date.now()>=deadline||beatInFlight)return;beatInFlight=true;pending.set(member,Date.now());try{await timed('heartbeat',()=>reducer(c,api.presence.beat,{}));}finally{beatInFlight=false;}};
  const sendSupport=async()=>{if(stopping||Date.now()>=deadline||supportInFlight)return;supportInFlight=true;try{await timed('heartbeat',()=>reducer(c,control.support.heartbeat,{release:'synthetic-local',environment:'test'}));}finally{supportInFlight=false;}};
  let transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;
  const sendTransition=async(state:string)=>{if(stopping||Date.now()>=deadline||transitionInFlight)return;transitionInFlight=true;pending.set(member,Date.now());try{await timed('heartbeat',()=>reducer(c,api.presence.transition,{state}));}finally{transitionInFlight=false;transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;}};
  // Match the independent app providers: fixed 30-second ticks, skip an
  // overlapping heartbeat, and continue them while foreground saves await.
  const sessionTimers=[setInterval(()=>launch(sendBeat),30000),setInterval(()=>launch(sendSupport),30000),setInterval(()=>{if(Date.now()>=transitionAt&&!transitionInFlight){idle=!idle;launch(()=>sendTransition(idle?'idle':'active'));}},200)];
  periodicTimers.push(...sessionTimers);launch(sendSupport);launch(()=>sendTransition('active'));
  while(!stopping&&Date.now()<deadline){
   if(!initial&&Date.now()>=actionAt){
    const x=random()*126;
    if(x>=108&&x<114&&taskState!=='pending' || x>=119&&x<123&&taskState==='completed'){taskSequence++;taskState='pending';}
    const taskId=ownTask();
    const current=(tw.localReplicaResult()||[]).find((r:any)=>r._id===taskId);
    const expectTask=(id:string,matches:(row:any,at:number)=>boolean)=>pendingTasks.set(f.tenant+':'+id,{at:Date.now(),matches});
    if(x<72&&!current?.firstViewedAt&&current?.createdBy!==member)expectTask(taskId,(row,at)=>row.firstViewedAt>=at);
    else if(x>=108&&x<114&&current?.statusId!==f.nextStatusId)expectTask(taskId,row=>row.statusId===f.nextStatusId);
    else if(x>=119&&x<123&&current?.statusId!==f.completedStatusId)expectTask(taskId,row=>row.statusId===f.completedStatusId);
    if(x<72) await timed('save',()=>reducer(c,api.taskResources.recordTaskViewByTaskId,{taskId}));
    else if(x<101) await timed('save',()=>reducer(c,api.workspacePreferences.save,{entity:{_id:randomUUID(),userId:member,workspaceId:f.workspaceId,preferences:{density:random()<.5?'compact':'normal'}}}));
    else if(x<108) await timed('save',()=>reducer(c,api.taskResources.addTagByTaskId,{taskTagId:randomUUID(),taskId,tagId:f.tag._id}));
    else if(x<114) {const v=await timed('save',()=>reducer(c,api.tasks.start,{taskId,statusId:f.nextStatusId}));if(v!==undefined)taskState='working';}
    else if(x<119) {const entityId=randomUUID();expectTask(entityId,()=>true);await timed('save',()=>reducer(c,api.tasks.create,{entityId,name:'E2E load '+randomUUID(),workspaceId:f.workspaceId,categoryId:f.categoryId,priorityId:f.priorityId,statusId:f.statusId}));}
    else if(x<123) {const v=await timed('save',()=>reducer(c,api.tasks.requestTransition,{taskId,statusId:f.completedStatusId}));if(v!==undefined)taskState='completed';}
    else await timed('save',()=>reducer(c,api.taskResources.createNote,{entity:{_id:randomUUID(),_creationTime:Date.now(),taskId,userId:member,source:'web',note:'Synthetic local comment'}}));
    actionAt=Date.now()+think();
   }
   await pause(200);
  }
  for(const timer of sessionTimers)clearInterval(timer);
  await Promise.all([...periodicPending]);
  await pause(1000);
  if(member && (pending.get(member)||0)>=presenceReadyAt && !seen.has(member))error(new Error('Own presence updates never delivered'));
  if(initial) error(new Error('task subscription never became current'));
  for(const [key,op] of pendingTasks) if(key.startsWith(f.tenant+':')&&op.at>=subscribedAt&&seenTasks.get(key.slice(f.tenant.length+1))!==op.at) error(new Error('Task update was not delivered before teardown'));
  for(const e of extras){if(!e.w.status().isUpToDate)error(new Error('Metadata subscription never became current'));e.stop();}
  stop();stopTasks();c.close();
 })().catch(e=>{error(e);ready();}));
 await Promise.all(jobs);clearInterval(metricTimer);stopping=true;for(const timer of periodicTimers)clearInterval(timer);for(const c of users)c.close();
 if(!measurements.delivery.length) error(new Error('No update delivery samples'));
 const result={sdkVersion:sdkPackage.version,sdkPatch:expectedPatch,steadyStartMs:deadline-seconds*1000,steadyEndMs:deadline,profileHash,distributionProfile:process.env.LOAD_ALL_TENANTS_ACTIVE==='true'?'all-tenants':'observed-seven',pathLatencyMs:Object.fromEntries(Object.entries(perPath).map(([path,p])=>[path,{samples:p.durations.length,errors:p.errors,...quantiles(p.durations)}])),aborted,snapshots,targetUsers:myTarget,connectedUsers:users.length,shard,shards,...(shards>1?{rawSamples:samples,rawPaths:perPath}:{}),rampSeconds:ramp,steadySeconds:seconds,errors,latencyMs:Object.fromEntries(Object.entries(measurements).map(([k,v]:any)=>[k,{samples:v.length,...quantiles(v)}])),note:'Delivery uses presence row timestamps on the same workstation. Task snapshot latency is separate. Setup excluded from sign-in timing.'};
 const steadyLatencyMs=Object.fromEntries(Object.keys(measurements).map(kind=>{const xs=(samples[kind]||[]).filter(x=>x.at>=deadline-seconds*1000&&x.at<deadline).map(x=>x.ms);return [kind,{samples:xs.length,...quantiles(xs)}];}));
 Object.assign(result,{steadyLatencyMs,qualificationLatencyScope:'steady plateau; operations started with all users connected'});
 const q:any=steadyLatencyMs;
 {const rule=passRule({latencyMs:q,errors,connectedUsers:users.length,targetUsers:myTarget,aborted});Object.assign(result,{clientSloPassed:rule.passed,errorRate:rule.errorRate,failingKinds:rule.failingKinds});}
 console.log(JSON.stringify(result));await writeFile(process.env.LOAD_RESULT||state+'.result',JSON.stringify(result,null,2));
}
main().catch(e=>{console.error(e);process.exit(1);});
