import {test} from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {WranglerTarget} from './targets.mjs';
import {transferConfigs,transferTenantNamespace} from '../../../kernel/assembly/namespace-transfer.mjs';
const owned=async fn=>{const dir=await mkdtemp(join(tmpdir(),'gonvex-topology-'));try{await fn(dir);}finally{await rm(dir,{recursive:true,force:true});}};
test('app deployment replaces only runtime and Actions; stable socket owner is never a target',()=>owned(async dir=>{
 const sent=[];const target=new WranglerTarget(dir,'gonvex-cf-dev-topology',async(path,name)=>sent.push({name,config:JSON.parse(await readFile(path))}));
 await target.stage(new Map([['releases.js','export const releases={};'],['action-releases.js','export const releases={};']]),'v1');await target.deploy('v1');
 assert.deepEqual(sent.map(x=>x.name),['gonvex-cf-dev-topology-actions','gonvex-cf-dev-topology-runtime']);
 const runtime=sent[1].config;assert.deepEqual(Object.keys(runtime.exports),['AssemblyTenant']);assert.equal(runtime.durable_objects.code_update_strategy.mode,'immediate');
 assert.equal(runtime.durable_objects.bindings.find(b=>b.name==='GATEWAY').script_name,'gonvex-cf-dev-topology');
 assert.equal(sent[0].config.durable_objects.bindings.find(b=>b.name==='TENANT').script_name,'gonvex-cf-dev-topology-runtime');
 assert.equal(runtime.limits.cpu_ms,30000);
}));
test('transfer uses expecting/commit/bind without a create or delete and preserves namespace or retains freeze',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={};');
 const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[]});const [bridge,receive,commit,bind]=plan.phases;
 assert.equal(bridge.config.main.endsWith('/src/transition.ts'),true);assert.equal(bridge.config.exports.AssemblyTenant.state,undefined);
 assert.equal(receive.config.exports.AssemblyTenant.state,'expecting-transfer');assert.ok(!receive.config.durable_objects.bindings.some(b=>b.name==='TENANT'));
 assert.deepEqual(commit.config.exports.AssemblyTenant,{type:'durable-object',state:'transferred',transferred_to:plan.targetName});
 assert.deepEqual(bind.config.exports.AssemblyTenant,{type:'durable-object',storage:'sqlite'});
 let idAtTarget=null,frozen=false;const calls=[];const options={namespace:async name=>name===plan.sourceName?'preserved':idAtTarget,topology:async()=>({frozen:frozen=true,deployment:null,releases:[]}),deploy:async path=>{calls.push(path);if(path===commit.path)idAtTarget='preserved';}};
 const result=await transferTenantNamespace(plan,options);assert.equal(result.namespaceId,'preserved');assert.equal(frozen,true);assert.ok(calls.indexOf(commit.path)>calls.indexOf(receive.path));
 await rm(join(dir,'transfer-state.json'));idAtTarget=null;await assert.rejects(transferTenantNamespace(plan,{...options,deploy:async path=>{if(path===commit.path)idAtTarget='WRONG';}}),/namespace changed/);assert.equal(frozen,true);
}));
test('reverse transfer names the same namespace owners and stays frozen; no deletion rollback',()=>owned(async dir=>{
 const entry=join(dir,'catalog.js');await writeFile(entry,'export const releases={};');const plan=await transferConfigs('gonvex-cf-dev-topology',dir,{catalogEntry:entry,actionCatalogEntry:entry,catalog:[],reverse:true});
 assert.equal(plan.sourceName,'gonvex-cf-dev-topology-runtime');assert.equal(plan.targetName,'gonvex-cf-dev-topology');
 assert.equal(plan.phases[1].config.exports.AssemblyTenant.transfer_from,plan.sourceName);assert.equal(plan.phases[2].config.exports.AssemblyTenant.transferred_to,plan.targetName);
}));
