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
 const target=Number(process.env.LOAD_USERS||26),seconds=Number(process.env.LOAD_SECONDS||180),ramp=Number(process.env.LOAD_RAMP_SECONDS||60);
 if(!Number.isSafeInteger(target)||target<1||!Number.isFinite(seconds)||seconds<1) throw Error('Invalid load bounds');
 // One Node event loop saturates near 120 users, so large cases run several
 // shards (shards.sh). Shard k drives users i with i % shards === k; tenant
 // choice, ramp timing and task ordinals stay global, so the mix matches an
 // unsharded run. merge-shards.mjs combines the shards' raw samples.
 const shards=Number(process.env.LOAD_SHARDS||1),shard=Number(process.env.LOAD_SHARD||0);
 if(!Number.isSafeInteger(shards)||shards<1||!Number.isSafeInteger(shard)||shard<0||shard>=shards) throw Error('Invalid shard');
 const mine=(i:number)=>i%shards===shard;const myTarget=Array.from({length:target},(_,i)=>i).filter(mine).length;
 const measurements:any={save:[],heartbeat:[],reducer:[],query:[],initialLoad:[],reconnect:[],permission:[],visibility:[],delivery:[],taskDelivery:[],login:[]},errors:Record<string,number>={};
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
 const metricTimer=shard!==0?undefined:setInterval(()=>{void fetch(url+'/dev/metrics?project='+encodeURIComponent(projectId),{headers:{authorization:'Bearer '+admin}}).then(r=>r.json()).then(x=>appendFile((process.env.LOAD_RESULT||state)+'.metrics.jsonl',JSON.stringify({at:Date.now(),metrics:{websocket:{accounts:x.websocket?.accounts,connections:x.websocket?.connections,subscriptions:x.websocket?.subscriptions},database:x.database,queryAdmission:x.queryAdmission}})+'\n')).catch(error);},10000);
 const tenantCounts=new Map<string,number>();const ordinals=Array.from({length:target},(_,i)=>{const f=chooseTenant(i);const n=tenantCounts.get(f.tenant)||0;tenantCounts.set(f.tenant,n+1);return n;});
 // ---- Simulated users (load-realism audit 2026-11-03 plus Gabriel's direction:
 // heavier than production on the expensive paths). See README "Action mix".
 type Handle={i:number,email:string,member:string,tenant:string,tw:()=>any,admin:boolean,busy:boolean};
 const tenantUsers=new Map<string,Handle[]>();
 const approvalQueue=new Map<string,string[]>();
 const visibilityChecks={checked:0,converged:0,mismatched:0,noChange:0};
 const pick=<T,>(xs:T[],r:()=>number)=>xs[Math.floor(r()*xs.length)];
 const reconnectAt=(fraction:number)=>deadline-seconds*1000*(1-fraction);
 const until=async(condition:()=>boolean,ms:number)=>{const end=Date.now()+ms;while(!condition()){if(Date.now()>end)return false;await pause(50);}return true;};
 const ids=(rows:any[])=>new Set(rows.map((r:any)=>r._id));
 const watchAll=(c:any,refs:[any,any][])=>{const ws=refs.map(([ref,args])=>{const w=c.watchReplica(ref,args);const stop=w.onUpdate(()=>{try{w.localReplicaResult();}catch(e){error(e);}});return {w,stop};});return {ready:()=>ws.every(x=>x.w.status().isUpToDate),close:()=>ws.forEach(x=>x.stop()),rows:(k:number)=>(ws[k].w.localReplicaResult()||[]) as any[]};};
 // The oracle: a fresh session computes the target's visible rows from scratch;
 // the target's long-lived replica (incrementally maintained) must converge to
 // exactly that set. A difference after 15 s is a leaked or missing row.
 const verifyVisibility=async(target:Handle,changedAt:number,before:Set<string>)=>{
  if(target.busy)return;target.busy=true;visibilityChecks.checked++;
  let fresh:any;let stop=()=>{};
  try{
   fresh=await login(target.email,target.tenant);
   const fw=fresh.c.watchReplica(api.tasks.recentWorkspaceTasks,{});stop=fw.onUpdate(()=>{});
   const complete=(w:any)=>w.status().isUpToDate&&w.localReplicaState()?.completeness==='complete';
   if(!await until(()=>complete(fw),60000)){error(new Error('Visibility oracle snapshot never completed'));return;}
   const same=()=>{const a=ids(target.tw().localReplicaResult()||[]),b=ids(fw.localReplicaResult()||[]);return complete(target.tw())&&a.size===b.size&&[...a].every(x=>b.has(x));};
   if(await until(same,15000)){
    const after=ids(fw.localReplicaResult()||[]);
    if(after.size===before.size&&[...after].every(x=>before.has(x)))visibilityChecks.noChange++;
    else{visibilityChecks.converged++;sample('visibility',Date.now()-changedAt,changedAt);}
   }else{
    const a=ids(target.tw().localReplicaResult()||[]),b=ids(fw.localReplicaResult()||[]);
    visibilityChecks.mismatched++;
    error(new Error(`Visibility mismatch after 15 s: ${[...a].filter(x=>!b.has(x)).length} leaked, ${[...b].filter(x=>!a.has(x)).length} missing`));
   }
  }catch(e){error(e);}
  finally{stop();fresh?.c.close();target.busy=false;}
 };
 const jobs=Array.from({length:target},(_,i)=>(async()=>{
  if(!mine(i))return;
  let seed=(i+1)*2654435761;const random=()=>{seed^=seed<<13;seed^=seed>>>17;seed^=seed<<5;return (seed>>>0)/4294967296;};
  let transitionSeed=(i+1)*2246822519;const transitionRandom=()=>{transitionSeed^=transitionSeed<<13;transitionSeed^=transitionSeed>>>17;transitionSeed^=transitionSeed<<5;return (transitionSeed>>>0)/4294967296;};
  const think=()=>{const knots=[[0,0],[.25,5.163],[.5,12.732],[.75,44.774],[.95,735.971],[1,1800]];const u=random();const hi=knots.findIndex(k=>k[0]>=u);const a=knots[Math.max(0,hi-1)],b=knots[Math.max(1,hi)];return (a[1]+(b[1]-a[1])*(u-a[0])/(b[0]-a[0]))*1.1*1000;};
  await pause(i*ramp*1000/target);if(stopping)return;
  const f=chooseTenant(i),email=`load-user-${i}@example.invalid`;
  // A session: client, presence watch, metadata watches and the task replica.
  // The reconnect wave replaces it mid-run.
  let c:any,member='',tw:any,stopPresence=()=>{},stopTasks=()=>{},extras:any;
  let initial=true,presenceReadyAt=Infinity,actionAt=Infinity,subscribedAt=Infinity;
  const seen=new Map<string,number>(),seenTasks=new Map<string,number>();
  const connect=async(kind:string)=>{
   const session=await timed(kind,()=>login(email,f.tenant));if(!session)return false;
   c=session.c;member=session.grant.tenants.find((t:any)=>t.id===f.tenant)?.memberId;
   const watch=c.watchReplica(api.presence.list,{});presenceReadyAt=Infinity;
   stopPresence=watch.onUpdate(()=>{try{
    if(presenceReadyAt===Infinity){if(!watch.status().isUpToDate)return;presenceReadyAt=Date.now();}
    for(const row of watch.localReplicaResult()||[]){const ts=pending.get(row.userId);if(ts&&ts>=presenceReadyAt&&row.lastSeenAt>=ts&&seen.get(row.userId)!==row.lastSeenAt){seen.set(row.userId,row.lastSeenAt);sample('delivery',Date.now()-ts,ts);}}
   }catch(e){error(e);}});
   extras=watchAll(c,[api.workspaces.list,api.statuses.statuses,api.tags.tags,api.members.listMembers,api.members.listMemberTeams,api.taskResources.taskUsers,api.taskResources.taskTags,api.workspacePreferences.workspacePreferences].map(ref=>[ref,{}] as [any,any]));
   const w=c.watchReplica(api.tasks.recentWorkspaceTasks,{});const q0=performance.now();let first=true;
   stopTasks=w.onUpdate(()=>{try{
    if(first&&w.status().isUpToDate&&w.localReplicaState()?.completeness==='complete'){first=false;sample(kind==='login'?'initialLoad':'reconnect',performance.now()-q0);if(initial){initial=false;subscribedAt=Date.now();actionAt=Date.now()+think();}snapshots.push({received:(w.localReplicaResult()||[]).length,kind});}
    for(const row of w.localReplicaResult()||[]){const op=pendingTasks.get(f.tenant+':'+row._id);const ts=op?.at;if(op&&ts&&ts>=subscribedAt&&op.matches(row,ts)&&seenTasks.get(row._id)!==ts){seenTasks.set(row._id,ts);sample('taskDelivery',Date.now()-ts,ts);}}
   }catch(e){error(e);}});
   tw=w;return true;
  };
  const disconnect=()=>{stopPresence();stopTasks();extras?.close();c?.close();};
  if(!await connect('login')){ready();return;}
  if(stopping){disconnect();ready();return;}users.push({close:()=>c.close()});
  const admin=(f.adminMemberIds||[]).includes(member),approver=(f.approverMemberIds||[]).includes(member);
  const handle:Handle={i,email,member,tenant:f.tenant,tw:()=>tw,admin,busy:false};
  (tenantUsers.get(f.tenant)??tenantUsers.set(f.tenant,[]).get(f.tenant)!).push(handle);
  ready();
  const periodicPending=new Set<Promise<unknown>>();
  const launch=(fn:()=>Promise<unknown>)=>{const p=fn();periodicPending.add(p);void p.finally(()=>periodicPending.delete(p));};
  let beatInFlight=false,supportInFlight=false,transitionInFlight=false,idle=false,reconnecting=false;
  const sendBeat=async()=>{if(stopping||reconnecting||Date.now()>=deadline||beatInFlight)return;beatInFlight=true;pending.set(member,Date.now());try{await timed('heartbeat',()=>reducer(c,api.presence.beat,{}));}finally{beatInFlight=false;}};
  const sendSupport=async()=>{if(stopping||reconnecting||Date.now()>=deadline||supportInFlight)return;supportInFlight=true;try{await timed('heartbeat',()=>reducer(c,control.support.heartbeat,{release:'synthetic-local',environment:'test'}));}finally{supportInFlight=false;}};
  let transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;
  const sendTransition=async(state:string)=>{if(stopping||reconnecting||Date.now()>=deadline||transitionInFlight)return;transitionInFlight=true;pending.set(member,Date.now());try{await timed('heartbeat',()=>reducer(c,api.presence.transition,{state}));}finally{transitionInFlight=false;transitionAt=Date.now()-Math.log(Math.max(transitionRandom(),.000001))*101000;}};
  // Match the independent app providers: fixed 30-second ticks, skip an
  // overlapping heartbeat, and continue them while foreground saves await.
  const sessionTimers=[setInterval(()=>launch(sendBeat),30000),setInterval(()=>launch(sendSupport),30000),setInterval(()=>{if(Date.now()>=transitionAt&&!transitionInFlight){idle=!idle;launch(()=>sendTransition(idle?'idle':'active'));}},200)];
  periodicTimers.push(...sessionTimers);launch(sendSupport);launch(()=>sendTransition('active'));
  const detailWindows=new Set<any>();
  const dwell=(win:any)=>{detailWindows.add(win);setTimeout(()=>{win.close();detailWindows.delete(win);},Math.min(think(),120000));};
  const rows=()=>(tw.localReplicaResult()||[]) as any[];
  const open=()=>rows().filter(r=>r.statusId!==f.completedStatusId);
  const expectTask=(id:string,matches:(row:any,at:number)=>boolean)=>pendingTasks.set(f.tenant+':'+id,{at:Date.now(),matches});
  const save=(ref:any,args:any)=>timed('save',()=>reducer(c,ref,args));
  const others=()=>(tenantUsers.get(f.tenant)||[]).filter(h=>h.member!==member&&!h.admin);
  // Weighted foreground actions for every user (weights in percent).
  const actions:[number,()=>Promise<unknown>][]=[
   // Reads (25): task detail dwell, workspace switch, notifications, chat.
   [15,async()=>{const t=pick(rows(),random);if(!t)return;const win=watchAll(c,[[api.tasks.get,{id:t._id}],[api.taskResources.listTaskNotes,{taskId:t._id}],[api.taskResources.listTaskLogs,{taskId:t._id}],[api.taskResources.listTaskViewsByTaskId,{taskId:t._id}],[api.approvals.listTaskApprovals,{taskId:t._id}],[api.taskFindings.listForTask,{taskId:t._id}],[api.taskResources.listTaskUsers,{taskId:t._id}],[api.customFields.listTaskValues,{taskId:t._id}]]);const q=performance.now();if(await until(win.ready,30000))sample('query',performance.now()-q);else error(new Error('Task detail queries never became current'));dwell(win);if(!t.firstViewedAt)expectTask(t._id,(row,at)=>row.firstViewedAt>=at);await save(api.taskResources.recordTaskViewByTaskId,{taskId:t._id});}],
   [4,async()=>{const win=watchAll(c,[[api.statuses.statuses,{}],[api.priorities.list,{}],[api.taskWorkspaceContexts.contexts,{}],[api.roles.memberTeams,{}],[api.taskResources.recentTaskAcknowledgments,{}]]);const q=performance.now();if(await until(win.ready,30000))sample('query',performance.now()-q);dwell(win);}],
   [3,async()=>{const win=watchAll(c,[[api.notifications.listNotifications,{}]]);const q=performance.now();if(await until(win.ready,30000))sample('query',performance.now()-q);dwell(win);if(random()<.3)await save(api.notifications.markAllRead,{});}],
   [3,async()=>{const win=watchAll(c,[[api.chat.workspaceChat,{}]]);const q=performance.now();if(await until(win.ready,30000))sample('query',performance.now()-q);dwell(win);}],
   // Edits (40): field edits fan out to every subscriber that can see the task.
   [8,async()=>{const t=pick(open(),random);if(!t)return;const description='Updated '+randomUUID()+' '+(t.description||'').slice(0,300);expectTask(t._id,row=>row.description===description);await save(api.tasks.updateDescription,{taskId:t._id,description});}],
   [6,async()=>{const t=pick(open(),random);if(!t)return;const dueDate=Date.now()+Math.floor(random()*14)*86400000;expectTask(t._id,row=>row.dueDate===dueDate);await save(api.tasks.update,{taskId:t._id,updates:{dueDate}});}],
   [4,async()=>{const t=pick(open(),random);if(!t)return;const name='Load task '+randomUUID().slice(0,8);expectTask(t._id,row=>row.name===name);await save(api.tasks.rename,{taskId:t._id,name});}],
   [4,async()=>{const t=pick(open(),random);if(!t)return;const priorityId=(f.priorityIds||[f.priorityId]).find((p:string)=>p!==t.priorityId)||f.priorityId;expectTask(t._id,row=>row.priorityId===priorityId);await save(api.tasks.setPriority,{taskId:t._id,priorityId});}],
   [4,async()=>{const t=pick(open(),random);if(!t)return;const dueDate=Date.now()+Math.floor(random()*30)*86400000;expectTask(t._id,row=>row.dueDate===dueDate);await save(api.tasks.reschedule,{taskId:t._id,dueDate});}],
   [5,async()=>{const t=pick(open().filter(r=>r.statusId!==f.nextStatusId),random);if(!t)return;expectTask(t._id,row=>row.statusId===f.nextStatusId);await save(api.tasks.start,{taskId:t._id,statusId:f.nextStatusId});}],
   [4,async()=>{const t=pick(rows().filter(r=>r.statusId!==f.statusId),random);if(!t)return;const statusId=random()<.15?f.completedStatusId:f.statusId;expectTask(t._id,row=>row.statusId===statusId);await save(api.tasks.requestTransition,{taskId:t._id,statusId});}],
   [3,async()=>{const t=pick(open(),random);if(!t)return;const tag=random()<.6;await save(tag?api.taskResources.addTagByTaskId:api.taskResources.removeTagByTaskId,{taskTagId:randomUUID(),taskId:t._id,tagId:f.tag._id});}],
   [2,async()=>{const t=pick(open().filter(r=>r.spotId),random);if(!t||!f.spotChildren)return;const spotId=pick(f.spotChildren as string[],random);expectTask(t._id,row=>row.spotId===spotId);await save(api.tasks.setSpot,{taskId:t._id,spotId});}],
   // Assignments (5): assigning another member makes the task visible to them.
   [5,async()=>{const t=pick(open(),random),h=pick(others(),random);if(!t||!h)return;await save(api.taskResources.assignUser,{taskId:t._id,userId:h.member});}],
   // Creates (20): tasks, comments, relations, workspace chat.
   [6,async()=>{const entityId=randomUUID();const workspace=rows().find(r=>r.workspaceId===f.workspaceIds?.[1])&&random()<.4?1:0;expectTask(entityId,()=>true);await save(api.tasks.create,{entityId,name:'E2E load '+randomUUID(),workspaceId:(f.workspaceIds||[f.workspaceId])[workspace],categoryId:(f.categoryIds||[f.categoryId])[workspace],priorityId:f.priorityId,spotId:random()<.8&&f.spotChildren?pick(f.spotChildren as string[],random):undefined});}],
   [8,async()=>{const t=pick(rows(),random);if(!t)return;await save(api.taskResources.createNote,{entity:{_id:randomUUID(),_creationTime:Date.now(),taskId:t._id,userId:member,source:'web',note:'Synthetic local comment '+randomUUID()}});}],
   [2,async()=>{const t=pick(open(),random),u=pick(open(),random);if(!t||!u||t._id===u._id)return;await save(api.taskResources.createTaskRelation,{relationId:randomUUID(),taskId:t._id,relatedTaskId:u._id,relationType:random()<.5?'blocks':'related'});}],
   [4,async()=>{const t=pick(rows(),random);if(!t)return;await save(api.chat.sendWorkspaceChat,{entity:{_id:randomUUID(),_creationTime:Date.now(),userId:member,workspaceId:t.workspaceId,message:'Synthetic chat '+randomUUID()}});}],
   // Approvals (10): tasks that enter approval, and approver decisions.
   [6,async()=>{if(!f.approvalCategoryId||!rows().some(r=>r.workspaceId===f.workspaceId))return;const entityId=randomUUID();expectTask(entityId,()=>true);const v=await save(api.tasks.create,{entityId,name:'Needs approval '+randomUUID().slice(0,8),workspaceId:f.workspaceId,categoryId:f.approvalCategoryId,priorityId:f.priorityId});if(v!==undefined)(approvalQueue.get(f.tenant)??approvalQueue.set(f.tenant,[]).get(f.tenant)!).push(entityId);}],
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
   else if(choice<.5&&team)v=await timed('permission',()=>reducer(c,api.members.addMemberTeam,{memberId:target.member,teamId:team,roleKey:'member'}));
   else if(choice<.65){const mt=extras.rows(4).find((r:any)=>r.memberId===target.member&&!r.deletedAt);if(!mt)return;v=await timed('permission',()=>reducer(c,api.members.removeMemberTeam,{id:mt._id}));}
   else if(choice<.8&&hidden.length){const t=pick(hidden,random);const share=await timed('permission',()=>reducer(c,api.taskResources.shareTask,{taskId:t._id,sharedToUserId:target.member}));v=share;if(typeof share==='string'&&random()<.5)setTimeout(()=>launch(async()=>{const at=Date.now(),b=ids(target.tw().localReplicaResult()||[]);if(await timed('permission',()=>reducer(c,api.taskResources.revokeShare,{id:share}))!==undefined)await verifyVisibility(target,at,b);}),60000);}
   else if(choice<.9&&hidden.length){const t=pick(hidden,random);v=await timed('permission',()=>reducer(c,api.tasks.assign,{taskId:t._id,userIds:[target.member]}));}
   else{const t=pick(mine.filter(r=>before.has(r._id)&&r.spotId),random);if(!t||!f.spotChildren)return;v=await timed('permission',()=>reducer(c,api.tasks.setSpot,{taskId:t._id,spotId:pick(f.spotChildren as string[],random)}));}
   if(v!==undefined)await verifyVisibility(target,Date.now(),before);
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
   if(team)await timed('permission',()=>reducer(c,api.members.addMemberTeam,{memberId:target.member,teamId:team,roleKey:'member'}));
   await verifyVisibility(target,at,before);
  };
  // Reconnect wave: ~30% of users reconnect within 10 s, mid-plateau.
  const reconnectTime=random()<.3?reconnectAt(.5)+random()*10000:Infinity;
  while(!stopping&&Date.now()<deadline){
   if(Date.now()>=reconnectTime&&!reconnecting&&!initial){reconnecting=true;disconnect();const connected=await connect('reconnect');reconnecting=false;if(!connected)break;}
   else if(admin&&!initial&&!reorgDone&&shard===0&&Date.now()>=reconnectAt(.3)&&(tenantUsers.get(f.tenant)||[]).find(h=>h.admin)===handle){reorgDone=true;await reorg();}
   else if(admin&&!initial&&Date.now()>=permissionAt){await changePermission();permissionAt=Date.now()+(120+random()*180)*1000;}
   else if(!initial&&Date.now()>=actionAt){await act();actionAt=Date.now()+think();}
   await pause(200);
  }
  for(const timer of sessionTimers)clearInterval(timer);
  await Promise.all([...periodicPending]);
  for(const win of detailWindows)win.close();
  await pause(1000);
  if(member && (pending.get(member)||0)>=presenceReadyAt && !seen.has(member))error(new Error('Own presence updates never delivered'));
  if(initial) error(new Error('task subscription never became current'));
  // Under visibility masks a user receives only rows it may see: an update is
  // missing only when the task is still in this replica without the change.
  for(const [key,op] of pendingTasks){if(!key.startsWith(f.tenant+':')||op.at<subscribedAt)continue;const row=rows().find(r=>r._id===key.slice(f.tenant.length+1));if(row&&!op.matches(row,op.at)&&!(Number(row.updatedAt)>=op.at))error(new Error('Task update was not delivered before teardown'));}
  if(!extras.ready())error(new Error('Metadata subscription never became current'));
  disconnect();
 })().catch(e=>{error(e);ready();}));
 await Promise.all(jobs);clearInterval(metricTimer);stopping=true;for(const timer of periodicTimers)clearInterval(timer);for(const c of users)c.close();
 if(!measurements.delivery.length) error(new Error('No update delivery samples'));
 const result={sdkVersion:sdkPackage.version,sdkPatch:expectedPatch,steadyStartMs:deadline-seconds*1000,steadyEndMs:deadline,profileHash,distributionProfile:process.env.LOAD_ALL_TENANTS_ACTIVE==='true'?'all-tenants':'production-skew',pathLatencyMs:Object.fromEntries(Object.entries(perPath).map(([path,p])=>[path,{samples:p.durations.length,errors:p.errors,...quantiles(p.durations)}])),aborted,snapshots,visibilityChecks,targetUsers:myTarget,connectedUsers:users.length,shard,shards,...(shards>1?{rawSamples:samples,rawPaths:perPath}:{}),rampSeconds:ramp,steadySeconds:seconds,errors,latencyMs:Object.fromEntries(Object.entries(measurements).map(([k,v]:any)=>[k,{samples:v.length,...quantiles(v)}])),note:'Delivery uses presence row timestamps on the same workstation. Task snapshot latency is separate. Setup excluded from sign-in timing.'};
 const steadyLatencyMs=Object.fromEntries(Object.keys(measurements).map(kind=>{const xs=(samples[kind]||[]).filter(x=>x.at>=deadline-seconds*1000&&x.at<deadline).map(x=>x.ms);return [kind,{samples:xs.length,...quantiles(xs)}];}));
 Object.assign(result,{steadyLatencyMs,qualificationLatencyScope:'steady plateau; operations started with all users connected'});
 const q:any=steadyLatencyMs;
 {const rule=passRule({latencyMs:q,errors,connectedUsers:users.length,targetUsers:myTarget,aborted});Object.assign(result,{clientSloPassed:rule.passed&&visibilityChecks.mismatched===0,errorRate:rule.errorRate,failingKinds:rule.failingKinds});}
 console.log(JSON.stringify(result));await writeFile(process.env.LOAD_RESULT||state+'.result',JSON.stringify(result,null,2));
}
main().catch(e=>{console.error(e);process.exit(1);});
