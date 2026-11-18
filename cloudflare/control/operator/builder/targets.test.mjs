import {test} from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {WranglerTarget,releaseSourceLabel} from './targets.mjs';
import {transferConfigs,transferTenantNamespace} from '../../../kernel/assembly/namespace-transfer.mjs';
const owned=async fn=>{const dir=await mkdtemp(join(tmpdir(),'gonvex-topology-'));try{await fn(dir);}finally{await rm(dir,{recursive:true,force:true});}};
test('app deployment replaces only runtime and Actions; stable socket owner is never a target',()=>owned(async dir=>{
 const sent=[];const target=new WranglerTarget(dir,'gonvex-cf-dev-topology',async(path,name)=>sent.push({name,config:JSON.parse(await readFile(path))}));
 await target.stage(new Map([['releases.js','export const releases={}; export const version="bootstrap";'],['action-releases.js','export const releases={}; export const version="bootstrap";']]),'v1');await target.deploy('v1');
 assert.deepEqual(sent.map(x=>x.name),['gonvex-cf-dev-topology-actions','gonvex-cf-dev-topology-runtime']);
 const runtime=sent[1].config;assert.deepEqual(Object.keys(runtime.exports),['AssemblyTenant']);assert.equal(runtime.durable_objects.code_update_strategy.mode,'immediate');
 assert.equal(runtime.durable_objects.bindings.find(b=>b.name==='GATEWAY').script_name,'gonvex-cf-dev-topology');
 assert.equal(sent[0].config.durable_objects.bindings.find(b=>b.name==='TENANT').script_name,'gonvex-cf-dev-topology-runtime');
 assert.equal(runtime.limits.cpu_ms,30000); // 0089: pinned tenant-script CPU limit (stops a runaway Reducer).
 assert.equal(runtime.vars.GONVEX_TOPOLOGY_TRANSITION,'0'); // --keep-vars must clear a retained bridge guard.
}));
test('app deployment records the release source label, never the committed development default',()=>owned(async dir=>{
 const sha='af7e6b83ce19a1ee4d5f83e5c7a75b67402eebee',files=new Map([['releases.js','export const releases={};'],['action-releases.js','export const releases={};']]);
 const sent=[];const target=new WranglerTarget(dir,'gonvex-cf-dev-topology',async(path,name)=>sent.push({name,vars:JSON.parse(await readFile(path)).vars}),sha);
 await target.stage(files,'v1');await target.deploy('v1');
 assert.deepEqual(sent.map(x=>x.vars.GONVEX_RUNTIME_VERSION),[sha,sha]);
 for(const source of [undefined,'','development','af7e6b83']){
  const unlabeled=[];const t=new WranglerTarget(dir,'gonvex-cf-dev-topology',async path=>unlabeled.push(JSON.parse(await readFile(path)).vars),source);
  await t.stage(files,'v1');await t.deploy('v1');
  assert.ok(unlabeled.every(v=>!('GONVEX_RUNTIME_VERSION' in (v??{}))),'unlabeled builder must leave the deployed label to --keep-vars');
 }
 assert.equal(releaseSourceLabel('DEVELOPMENT'),null);
}));
test('component write failure is uncertain and a retry deploys the same catalog only to Actions/runtime',()=>owned(async dir=>{
 const calls=[];let fail=true;
 const target=new WranglerTarget(dir,'gonvex-cf-dev-topology',async(_path,name)=>{calls.push(name);if(fail&&name.endsWith('-runtime'))throw Error('response lost');});
 await target.stage(new Map([['releases.js','export const releases={};'],['action-releases.js','export const releases={};']]),'v1');
 await assert.rejects(target.deploy('v1'),error=>error.retainPublication===true);
 fail=false;await target.deploy('v1');
 assert.deepEqual(calls,['gonvex-cf-dev-topology-actions','gonvex-cf-dev-topology-runtime','gonvex-cf-dev-topology-actions','gonvex-cf-dev-topology-runtime']);
}));
test('transfer uses expecting/commit/bind without a create or delete and preserves namespace or retains freeze',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');
 const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[]});const [bridge,receive,commit,bind]=plan.phases;
 assert.equal(bridge.config.main.endsWith('/src/transition.ts'),true);assert.equal(bridge.config.exports.AssemblyTenant.state,undefined);
 assert.equal(bridge.config.vars.GONVEX_TOPOLOGY_TRANSITION,'1');assert.equal(bind.config.vars.GONVEX_TOPOLOGY_TRANSITION,'0');
 assert.equal(receive.config.exports.AssemblyTenant.state,'expecting-transfer');assert.ok(!receive.config.durable_objects.bindings.some(b=>b.name==='TENANT'));
 assert.deepEqual(commit.config.exports.AssemblyTenant,{type:'durable-object',state:'transferred',transferred_to:plan.targetName});
 assert.deepEqual(bind.config.exports.AssemblyTenant,{type:'durable-object',storage:'sqlite'});
 let idAtTarget=null,frozen=false;const calls=[];const options={namespace:async name=>name===plan.sourceName?'preserved':idAtTarget,topology:async()=>({frozen:frozen=true,deployment:null,releases:[]}),deploy:async path=>{calls.push(path);if(path===commit.path)idAtTarget='preserved';}};
 const result=await transferTenantNamespace(plan,options);assert.equal(result.namespaceId,'preserved');assert.equal(frozen,true);assert.ok(calls.indexOf(commit.path)>calls.indexOf(receive.path));
 await rm(join(dir,'transfer-state.json'));idAtTarget=null;await assert.rejects(transferTenantNamespace(plan,{...options,deploy:async path=>{if(path===commit.path)idAtTarget='WRONG';}}),/namespace changed/);assert.equal(frozen,true);
}));
test('reverse transfer names the same namespace owners and stays frozen; no deletion rollback',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[],reverse:true});
 assert.equal(plan.sourceName,'gonvex-cf-dev-topology-runtime');assert.equal(plan.targetName,'gonvex-cf-dev-topology');
 assert.equal(plan.phases[0].config.vars.GONVEX_TOPOLOGY_TRANSITION,'1');assert.equal(plan.phases[3].config.vars.GONVEX_TOPOLOGY_TRANSITION,'1');
 assert.equal(plan.phases[1].config.exports.AssemblyTenant.transfer_from,plan.sourceName);assert.equal(plan.phases[2].config.exports.AssemblyTenant.transferred_to,plan.targetName);
}));
test('a committed transfer resumes from retained namespace identity without repeating the handoff',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');
 const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[]});
 let source='original',target=null,failBind=true;const calls=[];
 const options={namespace:async name=>name===plan.sourceName?source:target,
  topology:async()=>({frozen:true,deployment:null,releases:[]}),deploy:async path=>{
   calls.push(path);if(path===plan.phases[2].path){source=null;target='original';}
   if(path===plan.phases[3].path&&failBind)throw Error('bind transport failed');
  }};
 await assert.rejects(transferTenantNamespace(plan,options),/bind transport failed/);
 failBind=false;calls.length=0;
 const result=await transferTenantNamespace(plan,options);
 assert.equal(result.namespaceId,'original');assert.equal(result.frozen,true);
 assert.equal(calls[0],plan.phases[3].path);
 assert.ok(!calls.includes(plan.phases[2].path));
 const journal=JSON.parse(await readFile(join(dir,'transfer-state.json')));
 assert.equal(journal.phase,'bound');assert.equal(journal.namespaceId,'original');
}));
test('changed inputs require a real frozen idle source and can never refresh a committed handoff',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');
 const args={catalogEntry:entry,actionCatalogEntry:entry,catalog:[]};
 let plan=await transferConfigs('gonvex-cf-dev-topology',dir,args),target=null,source='original';
 const options={namespace:async name=>name===plan.sourceName?source:target,
  topology:async()=>({frozen:true,deployment:null,releases:[]}),deploy:async path=>{if(path===plan.phases[1].path)throw Error('receive failed');}};
 await assert.rejects(transferTenantNamespace(plan,options),/receive failed/);
 await writeFile(entry,'export const releases={}; export const version="bootstrap"; // new captured immutable input');
 plan=await transferConfigs('gonvex-cf-dev-topology',dir,args);
 await assert.rejects(transferTenantNamespace(plan,options),/inputs changed/);
 await assert.rejects(transferTenantNamespace(plan,{...options,refreshFrozenPlan:true,topology:async()=>({frozen:false})}),/inputs changed/);
 await assert.rejects(transferTenantNamespace(plan,{...options,refreshFrozenPlan:true}),/receive failed/);
 source=null;target='original';await writeFile(entry,'export const releases={}; export const version="bootstrap"; // changed after commit');
 plan=await transferConfigs('gonvex-cf-dev-topology',dir,args);
 await assert.rejects(transferTenantNamespace(plan,{...options,refreshFrozenPlan:true}),/inputs changed/);
}));
test('changed active catalog or foreign target namespace prevents commit and leaves maintenance frozen',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');
 const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[]});
 const calls=[];let frozen=false;
 const options={namespace:async name=>name===plan.sourceName?'original':null,
  topology:async()=>({frozen:frozen=true,deployment:null,releases:[{project:'unexpected',hash:'hash',generation:1}]}),deploy:async path=>calls.push(path)};
 await assert.rejects(transferTenantNamespace(plan,options),/active catalog changed/);
 assert.equal(frozen,true);assert.deepEqual(calls,[plan.phases[0].path]);
 await rm(join(dir,'transfer-state.json'));calls.length=0;
 await assert.rejects(transferTenantNamespace(plan,{...options,namespace:async name=>name===plan.sourceName?'original':'foreign'}),/target already owns/);
 assert.deepEqual(calls,[]);
}));
test('a stable socket-owner namespace cannot change during transfer or be recaptured on resume',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');
 const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[]});
 let gateway='stable-id';const calls=[];
 const options={namespace:async name=>name===plan.sourceName?'tenant-id':null,stableNamespaces:async()=>({AssemblyGateway:gateway}),
  topology:async()=>({frozen:true,deployment:null,releases:[]}),deploy:async path=>{calls.push(path);gateway='unexpected-id';}};
 await assert.rejects(transferTenantNamespace(plan,options),/stable socket-owner namespace changed/);
 assert.deepEqual(calls,[plan.phases[0].path]);calls.length=0;
 await assert.rejects(transferTenantNamespace(plan,options),/stable socket-owner namespace changed/);
 assert.deepEqual(calls,[]);
}));

test('catalog equality ignores property insertion order while preserving tuple values',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={}; export const version="bootstrap";');
 const catalog=[{project:'project-a',hash:'hash-a',generation:1}];
 const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog});
 let target=null;
 const result=await transferTenantNamespace(plan,{
  namespace:async name=>name===plan.sourceName?'original':target,
  topology:async()=>({frozen:true,deployment:null,releases:[{generation:1,hash:'hash-a',project:'project-a'}]}),
  deploy:async path=>{if(path===plan.phases[2].path)target='original';},
 });
 assert.equal(result.namespaceId,'original');
 assert.equal(result.frozen,true);
}));
