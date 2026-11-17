// Public declarative exports transfer, never namespace deletion/recreation.
// https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/#transfer-a-durable-object-class-between-workers
import {join,resolve,isAbsolute} from 'node:path';
import {readFile,writeFile,rename} from 'node:fs/promises';
import {servingConfigs,developmentName} from './deploy.mjs';
const live={type:'durable-object',storage:'sqlite'};
/** One-time framework maintenance. Subsequent app publication never uses this path.
 * Caller quiesces legacy publishers, supplies the captured whole-catalog entry,
 * and retains the durable Control freeze until namespace and catalog validation.
 * Reverse transfer uses the same public four-deploy protocol, without deletes. */
export async function transferConfigs(name,directory,{catalogEntry,actionCatalogEntry,kernelUrl,images,catalog,reverse=false}={}) {
 developmentName(name);if(!catalogEntry||!isAbsolute(catalogEntry))throw new Error('captured whole catalog required');
 if(!Array.isArray(catalog))throw new Error('captured active catalog tuples required');
 if(!actionCatalogEntry||!isAbsolute(actionCatalogEntry))throw new Error('captured Actions catalog required');
 await readFile(actionCatalogEntry);await readFile(catalogEntry);const configs=await servingConfigs(name,directory,{kernelUrl,images});
 const sourceName=reverse?`${name}-runtime`:name,targetName=reverse?name:`${name}-runtime`;
 configs.runtime.config.alias['gonvex:releases']=resolve(catalogEntry);
 configs.actions.config.alias['gonvex:action-releases']=resolve(actionCatalogEntry);
 await writeFile(configs.actions.path,JSON.stringify(configs.actions.config),{mode:0o600});
 const kernel=structuredClone(configs.kernel.config),runtime=structuredClone(configs.runtime.config);
 kernel.vars.GONVEX_TOPOLOGY_TRANSITION='1';
 kernel.main=resolve(new URL('../../src/transition.ts',import.meta.url).pathname);
 kernel.alias['gonvex:releases']=resolve(catalogEntry);
 kernel.exports.AssemblyTenant={...live};
 if(!reverse)kernel.services=kernel.services.filter(s=>s.binding!=='RUNTIME_CATALOG');
 kernel.durable_objects.bindings.find(b=>b.name==='TENANT').script_name=reverse?`${name}-runtime`:undefined;
 // Source bridge remains the old namespace's owner until the transfer commits.
 const source=reverse?runtime:kernel,target=reverse?kernel:runtime;
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
 return {sourceName,targetName,phases,companions:configs,reverse,catalog,directory};
}
/** The caller's deploy function is the normal Wrangler operation; responses are
 * not simulated completion ACKs. Namespace equality is read from the provider.
 * Freeze/thaw are real Control RPC via the authenticated stable HTTP router. */
export async function transferTenantNamespace(plan,{deploy,topology,namespace,configureRuntime}) {
 const journalPath=join(plan.directory,`transfer${plan.reverse?'-reverse':''}-state.json`);
 let journal;try{journal=JSON.parse(await readFile(journalPath,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}
 const sourceId=await namespace(plan.sourceName),targetId=await namespace(plan.targetName);
 const original=journal?.namespaceId??sourceId;if(!original)throw new Error('source tenant namespace missing');
 if(journal&&(journal.source!==plan.sourceName||journal.target!==plan.targetName))throw new Error('transfer journal names differ');
 if(sourceId!==original&&targetId!==original)throw new Error('provider namespace differs from retained transfer');
 const save=async phase=>{journal={source:plan.sourceName,target:plan.targetName,namespaceId:original,phase};await writeFile(journalPath+'.tmp',JSON.stringify(journal),{mode:0o600});await rename(journalPath+'.tmp',journalPath);};
 const [bridge,receive,commit,bind]=plan.phases;
 if(targetId!==original){
  if(targetId)throw new Error('target already owns a tenant namespace');
  await save('bridge');await deploy(bridge.path);
 }
 const state=await topology('freeze');if(!state.frozen||state.deployment)throw new Error('Control did not freeze an idle publisher');
 const sorted=rows=>JSON.stringify([...rows].sort((a,b)=>a.project.localeCompare(b.project)));if(sorted(state.releases)!==sorted(plan.catalog))throw new Error('active catalog changed; rebuild bridge while frozen');
 if(targetId!==original){

 // Target must not already own a tenant namespace: expecting-transfer, never create.
 await deploy(receive.path);await configureRuntime?.();await save('received');await deploy(commit.path);
 if(await namespace(plan.targetName)!==original)throw new Error('tenant namespace changed during transfer; retain freeze');
 }
 await save('committed');await deploy(bind.path);
 for(const kind of ['actions','sandbox']){
  const item=plan.companions[kind];
  for(const binding of item.config.durable_objects.bindings)if(binding.name==='TENANT')binding.script_name=plan.targetName;
  await writeFile(item.path,JSON.stringify(item.config),{mode:0o600});await deploy(item.path);
 }
 // Caller validates its captured active catalog and invokes thaw(version) only
 // after the target/self bindings and stable Control environment have rolled out.
 await save('bound');return {namespaceId:original,frozen:true,target:plan.targetName};
}

/** Production transport for the reviewed transfer procedure. Invoke only from
 * the approved Vault child during coordinated framework maintenance. */
export async function migrateExistingAssembly({name,directory,kernelUrl,images,catalogEntry,actionCatalogEntry,catalog,version,adminKey,storageSecret,accountId,accountName,reverse=false}) {
 const {cloudflareCredentials,accountApi,deployConfig}=await import('./deploy.mjs');
 const credentials=await cloudflareCredentials();
 if(credentials.accountId!==accountId||!accountName||!adminKey||!version||(!reverse&&!storageSecret))throw new Error('exact account, retained auth/storage secret and catalog version required');
 for(const kind of ['builder','sandbox'])if(!images?.[kind]?.includes('@sha256:'))throw new Error('matching immutable source images required');
 const api=accountApi(credentials),plan=await transferConfigs(name,directory,{kernelUrl,images,catalogEntry,actionCatalogEntry,catalog,reverse});
 const verify=async()=>{const a=await api('');if(a.id!==accountId||a.name!==accountName)throw new Error('account identity changed');};
 const deploy=async path=>{await verify();const cfg=JSON.parse(await readFile(path,'utf8'));if(![name,...['runtime','actions','sandbox','builder'].map(k=>`${name}-${k}`)].includes(cfg.name))throw new Error('foreign Worker refused');await api(`/workers/scripts/${cfg.name}/settings`,{missing:true});await deployConfig(['deploy','--config',path,'--keep-vars'],credentials,directory);};
 const topology=async(action,extra='')=>{const deadline=Date.now()+360000;for(;;){
  let r;try{r=await fetch(`${kernelUrl}/_gonvex/k19/topology${action==='status'?'':`?action=${action}${extra}`}`,{method:action==='status'?'GET':'POST',headers:{authorization:`Bearer ${adminKey}`},signal:AbortSignal.timeout(15000)});}catch(error){if(Date.now()>=deadline)throw new Error('framework code adoption unavailable; retain freeze');}
  if(r?.ok)return r.json();
  if((r&&r.status<500)||Date.now()>=deadline)throw new Error(`topology ${action} HTTP ${r?.status??'timeout'}; retain maintenance/freeze`);
  await new Promise(resolve=>setTimeout(resolve,250));
 }};
 const namespace=async script=>{
  const matches=[];for(let page=1;page<=10;page++){
   const rows=await api(`/workers/durable_objects/namespaces?per_page=1000&page=${page}`);
   if(!Array.isArray(rows))throw new Error('namespace API shape differs');
   matches.push(...rows.filter(n=>n.script===script&&n.class==='AssemblyTenant'));
   if(rows.length<1000){if(matches.length>1)throw new Error('ambiguous tenant namespaces');return matches[0]?.id??null;}
  }throw new Error('namespace inventory exceeds bounded page budget');
 };
 await verify();
 // The transfer remains frozen on any error; no automatic SQL rollback or deletes.
 const result=await transferTenantNamespace(plan,{deploy,topology,namespace,configureRuntime});
 if(!reverse){
  await deploy(plan.companions.builder.path);
  await deploy(plan.companions.kernel.path);
  if(await namespace(plan.targetName)!==result.namespaceId)throw new Error('target namespace changed; retain freeze');
  await topology('thaw',`&version=${encodeURIComponent(version)}`);
 }
 // Reverse transfer deliberately retains the freeze: an isolated runtime
 // builder must never publish while its namespace is back on the monolith.
 return result;
}
