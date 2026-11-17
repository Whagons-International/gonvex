// Public declarative exports transfer, never namespace deletion/recreation.
// https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/#transfer-a-durable-object-class-between-workers
import {join,resolve,isAbsolute} from 'node:path';
import {readFile,writeFile,rename} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {servingConfigs,developmentName} from './deploy.mjs';
const live={type:'durable-object',storage:'sqlite'};
/** One-time framework maintenance. Subsequent app publication never uses this path.
 * Caller quiesces legacy publishers, supplies the captured whole-catalog entry,
 * and retains the durable Control freeze until namespace and catalog validation.
 * Reverse transfer uses the same public four-deploy protocol, without deletes. */
export async function transferConfigs(name,directory,{catalogEntry,actionCatalogEntry,kernelUrl,images,catalog,reverse=false}={}) {
 developmentName(name);if(!catalogEntry||!isAbsolute(catalogEntry))throw new Error('captured whole catalog required');
 if(!Array.isArray(catalog))throw new Error('captured active catalog tuples required');
 if(catalog.some(r=>typeof r.project!=='string'||!r.project||typeof r.hash!=='string'||!r.hash||!Number.isSafeInteger(r.generation)||r.generation<1)
   ||new Set(catalog.map(r=>r.project)).size!==catalog.length)throw new Error('invalid captured active catalog');
 if(!actionCatalogEntry||!isAbsolute(actionCatalogEntry))throw new Error('captured Actions catalog required');
 const captured=await readFile(catalogEntry,'utf8'),capturedActions=await readFile(actionCatalogEntry,'utf8');
 const versionOf=text=>{const match=/\bexport\s+const\s+version\s*=\s*("(?:[^"\\]|\\.)*")\s*;/.exec(text);return match?JSON.parse(match[1]):null;};
 const version=versionOf(captured);
 if(!version||versionOf(capturedActions)!==version)throw new Error('matching source-generated Runtime and Actions catalog versions required');
 const configs=await servingConfigs(name,directory,{kernelUrl,images});
 const sourceName=reverse?`${name}-runtime`:name,targetName=reverse?name:`${name}-runtime`;
 configs.runtime.config.alias['gonvex:releases']=resolve(catalogEntry);
 configs.actions.config.alias['gonvex:action-releases']=resolve(actionCatalogEntry);
 await writeFile(configs.actions.path,JSON.stringify(configs.actions.config),{mode:0o600});
 // Register ActionCatalog before the receiving runtime binds that entrypoint.
 // Until handoff its tenant callbacks still address the current namespace owner.
 const actionBridge={path:join(directory,'transfer-actions-bridge.json'),config:structuredClone(configs.actions.config)};
 actionBridge.config.durable_objects.bindings.find(b=>b.name==='TENANT').script_name=sourceName;
 await writeFile(actionBridge.path,JSON.stringify(actionBridge.config),{mode:0o600});
 const kernel=structuredClone(configs.kernel.config),runtime=structuredClone(configs.runtime.config);
 kernel.vars.GONVEX_TOPOLOGY_TRANSITION='1';
 kernel.main=resolve(new URL('../../src/transition.ts',import.meta.url).pathname);
 kernel.alias['gonvex:releases']=resolve(catalogEntry);
 kernel.exports.AssemblyTenant={...live};
 if(!reverse)kernel.services=kernel.services.filter(s=>s.binding!=='RUNTIME_CATALOG');
 kernel.durable_objects.bindings.find(b=>b.name==='TENANT').script_name=reverse?`${name}-runtime`:undefined;
 // Source bridge remains the old namespace's owner until the transfer commits.
 const source=reverse?runtime:kernel,target=reverse?kernel:runtime;
 // Both directions must pin retained Tenant releases while installing the
 // freeze API. A stale capture may refuse a wake, never adopt an older pin.
 source.vars={...source.vars,GONVEX_TOPOLOGY_TRANSITION:'1'};
 const phases=[];
 const add=(phase,config)=>phases.push({phase,config:structuredClone(config),path:join(directory,`transfer-${reverse?'reverse-':''}${phase}.json`)});
 add('bridge',source);
 target.exports.AssemblyTenant={...live,state:'expecting-transfer',transfer_from:sourceName};
 target.durable_objects.bindings=target.durable_objects.bindings.filter(b=>b.name!=='TENANT');
 add('receive',target);
 source.exports.AssemblyTenant={type:'durable-object',state:'transferred',transferred_to:targetName};
 const sourceTenant=source.durable_objects.bindings.find(b=>b.name==='TENANT');if(sourceTenant)sourceTenant.script_name=targetName;
 add('commit',source);
 target.exports.AssemblyTenant={...live};target.durable_objects.bindings.push({name:'TENANT',class_name:'AssemblyTenant'});
 add('bind',target);
 // Retain the source tombstone until Actions, Sandbox and the opposite Worker
 // have been repointed and fully rolled out. No automatic namespace cleanup.
 for(const phase of phases)await writeFile(phase.path,JSON.stringify(phase.config),{mode:0o600});
 const fingerprint=createHash('sha256').update(JSON.stringify({sourceName,targetName,catalog,phases,images,actionBridge,
   catalogEntry:captured,actions:capturedActions})).digest('hex');
 return {sourceName,targetName,phases,companions:configs,reverse,catalog,directory,fingerprint,version,actionBridge};
}
/** The caller's deploy function is the normal Wrangler operation; responses are
 * not simulated completion ACKs. Namespace equality is read from the provider.
 * Freeze/thaw are real Control RPC via the authenticated stable HTTP router. */
export async function transferTenantNamespace(plan,{deploy,topology,namespace,configureRuntime,stableNamespaces,refreshFrozenPlan=false}) {
 const journalPath=join(plan.directory,`transfer${plan.reverse?'-reverse':''}-state.json`);
 let journal;try{journal=JSON.parse(await readFile(journalPath,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}
 const sourceId=await namespace(plan.sourceName),targetId=await namespace(plan.targetName);
 const original=journal?.namespaceId??sourceId;if(!original)throw new Error('source tenant namespace missing');
 const stable=stableNamespaces?(journal?journal.stable:await stableNamespaces()):undefined;
 const signature=map=>JSON.stringify(Object.entries(map??{}).sort(([a],[b])=>a.localeCompare(b)));
 const verifyStable=async()=>{if(stableNamespaces&&(!stable||signature(stable)!==signature(await stableNamespaces())))throw new Error('stable socket-owner namespace changed; retain maintenance/freeze');};
 const apply=async path=>{await deploy(path);await verifyStable();};
 await verifyStable();
 if(journal&&(journal.source!==plan.sourceName||journal.target!==plan.targetName))throw new Error('transfer journal names differ');
 if(journal&&journal.fingerprint!==plan.fingerprint){
  // Capturing a newer active catalog is safe only before handoff, after an
  // actual durable freeze. Never reinterpret a committed namespace's plan.
  const state=refreshFrozenPlan&&sourceId===original&&targetId!==original?await topology('status'):null;
  if(!state?.frozen||state.deployment)throw new Error('transfer inputs changed; retain freeze and inspect the retained plan');
 }
 if(sourceId!==original&&targetId!==original)throw new Error('provider namespace differs from retained transfer');
 const save=async phase=>{journal={source:plan.sourceName,target:plan.targetName,namespaceId:original,phase,fingerprint:plan.fingerprint,...(stable?{stable}:{})};await writeFile(journalPath+'.tmp',JSON.stringify(journal),{mode:0o600});await rename(journalPath+'.tmp',journalPath);};
 const [bridge,receive,commit,bind]=plan.phases;
 if(targetId!==original){
  if(targetId)throw new Error('target already owns a tenant namespace');
  await save('bridge');await apply(bridge.path);
 }
 const state=await topology('freeze');if(!state.frozen||state.deployment)throw new Error('Control did not freeze an idle publisher');
 const sorted=rows=>JSON.stringify(rows.map(({project,hash,generation})=>({project,hash,generation})).sort((a,b)=>a.project.localeCompare(b.project)));if(sorted(state.releases)!==sorted(plan.catalog))throw new Error('active catalog changed; rebuild bridge while frozen');
 if(targetId!==original){

 // Target must not already own a tenant namespace: expecting-transfer, never create.
 await apply(plan.actionBridge.path);
 await apply(receive.path);await configureRuntime?.();await save('received');await apply(commit.path);
 if(await namespace(plan.targetName)!==original)throw new Error('tenant namespace changed during transfer; retain freeze');
 }
 await save('committed');await apply(bind.path);
 for(const kind of ['actions','sandbox']){
  const item=plan.companions[kind];
  for(const binding of item.config.durable_objects.bindings)if(binding.name==='TENANT')binding.script_name=plan.targetName;
  await writeFile(item.path,JSON.stringify(item.config),{mode:0o600});await apply(item.path);
 }
 // Caller validates its captured active catalog and invokes thaw(version) only
 // after the target/self bindings and stable Control environment have rolled out.
 await save('bound');return {namespaceId:original,frozen:true,target:plan.targetName,...(stable?{stable}:{})};
}

/** Production transport for the reviewed transfer procedure. Invoke only from
 * the approved Vault child during coordinated framework maintenance. */
export async function migrateExistingAssembly({name,directory,kernelUrl,images,catalogEntry,actionCatalogEntry,catalog,version,adminKey,storageSecret,accountId,accountName,reverse=false,refreshFrozenPlan=false}) {
 const {cloudflareCredentials,accountApi,deployConfig}=await import('./deploy.mjs');
 const credentials=await cloudflareCredentials();
 if(credentials.accountId!==accountId||!accountName||!adminKey||!version||(!reverse&&!storageSecret))throw new Error('exact account, retained auth/storage secret and catalog version required');
 for(const kind of ['builder','sandbox'])if(!/@sha256:[a-f0-9]{64}$/.test(images?.[kind]??''))throw new Error('matching immutable source images required');
 const api=accountApi(credentials),plan=await transferConfigs(name,directory,{kernelUrl,images,catalogEntry,actionCatalogEntry,catalog,reverse});
 if(plan.version!==version)throw new Error('captured catalog version differs from requested maintenance version');
 const verify=async()=>{const a=await api('');if(a.id!==accountId||a.name!==accountName)throw new Error('account identity changed');};
 const deploy=async path=>{await verify();const cfg=JSON.parse(await readFile(path,'utf8'));if(![name,...['runtime','actions','sandbox','builder'].map(k=>`${name}-${k}`)].includes(cfg.name))throw new Error('foreign Worker refused');await api(`/workers/scripts/${cfg.name}/settings`,{missing:true});await deployConfig(['deploy','--config',path,'--keep-vars'],credentials,directory);};
 const topology=async(action,extra='')=>{const deadline=Date.now()+360000;for(;;){
  let r;try{r=await fetch(`${kernelUrl}/_gonvex/k19/topology${action==='status'?'':`?action=${action}${extra}`}`,{method:action==='status'?'GET':'POST',headers:{authorization:`Bearer ${adminKey}`},signal:AbortSignal.timeout(15000)});}catch(error){if(Date.now()>=deadline)throw new Error('framework code adoption unavailable; retain freeze');}
  if(r?.ok)return r.json();
  if((r&&r.status<500)||Date.now()>=deadline)throw new Error(`topology ${action} HTTP ${r?.status??'timeout'}; retain maintenance/freeze`);
  await new Promise(resolve=>setTimeout(resolve,250));
 }};
 const inventory=async()=>{
  const all=[];for(let page=1;page<=10;page++){
   const rows=await api(`/workers/durable_objects/namespaces?per_page=1000&page=${page}`);
   if(!Array.isArray(rows))throw new Error('namespace API shape differs');
   all.push(...rows);if(rows.length<1000)return all;
  }throw new Error('namespace inventory exceeds bounded page budget');
 };
 const namespace=async script=>{const matches=(await inventory()).filter(n=>n.script===script&&n.class==='AssemblyTenant');if(matches.length>1)throw new Error('ambiguous tenant namespaces');return matches[0]?.id??null;};
 const stableNamespaces=async()=>{
  const rows=await inventory(),result={};
  for(const klass of ['AssemblyGateway','GatewayControllerObject','SessionControlObject','PublicLimiterObject','IngressObject']){
   const matches=rows.filter(n=>n.script===name&&n.class===klass);
   if(matches.length!==1||!matches[0].id)throw new Error('stable namespace mapping missing or ambiguous');
   result[klass]=matches[0].id;
  }return result;
 };
 await verify();
 const configureRuntime=reverse?undefined:async()=>{
  await verify();
  await api(`/workers/scripts/${name}-runtime/secrets`,{method:'PUT',body:{name:'GONVEX_STORAGE_SECRET',text:storageSecret,type:'secret_text'}});
 };
 // The transfer remains frozen on any error; no automatic SQL rollback or deletes.
 const result=await transferTenantNamespace(plan,{deploy,topology,namespace,configureRuntime,stableNamespaces,refreshFrozenPlan});
 if(!reverse){
  await deploy(plan.companions.builder.path);
  await deploy(plan.companions.kernel.path);
  if(Object.entries(await stableNamespaces()).some(([klass,id])=>result.stable?.[klass]!==id))throw new Error('stable namespace changed; retain freeze');
  if(await namespace(plan.targetName)!==result.namespaceId)throw new Error('target namespace changed; retain freeze');
  const thawed=await topology('thaw',`&version=${encodeURIComponent(version)}`);
  if(thawed.frozen)throw new Error('Control retained its topology freeze');
  return {...result,frozen:false};
 }
 // Reverse transfer deliberately retains the freeze: an isolated runtime
 // builder must never publish while its namespace is back on the monolith.
 return result;
}
