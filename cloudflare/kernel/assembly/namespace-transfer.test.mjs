import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import {join,resolve,dirname,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {transferConfigs,transferTenantNamespace} from './namespace-transfer.mjs';
const require=createRequire(import.meta.resolve('wrangler'));
const {Miniflare,Log,LogLevel}=require('miniflare'),{build}=require('esbuild');
const worker=fileURLToPath(new URL('./test/transfer-lifecycle-worker.ts',import.meta.url));
const project='maintenance-fixture';
const manifest=(generation,hash=`hash-${generation}`)=>({schema:{tables:{}},functions:{'probe.generation':{kind:'query',public:true,handler:'generation',args:{kind:'any'},result:{kind:'any'}}},module:{hash,generation,files:{}}});
async function modules(generation,inspect=false,hash=`hash-${generation}`){
 const wasm=[];
 const result=await build({entryPoints:[worker],bundle:true,write:false,format:'esm',platform:'neutral',target:'esnext',external:['cloudflare:workers','node:*'],loader:{'.sql':'text'},plugins:[{
  name:'captured-catalog',setup(b){
   b.onResolve({filter:/^gonvex:releases$/},()=>({path:'catalog',namespace:'fixture'}));
   b.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:`export const version="catalog-${generation}";export const releases={${JSON.stringify(project)}:{project:${JSON.stringify(project)},hash:${JSON.stringify(hash)},generation:${generation},manifest:${JSON.stringify(manifest(generation,hash))},exports:{generation:async()=>({generation:${generation}})}}};`,loader:'js'}));
   b.onResolve({filter:/\.wasm$/},async args=>{const path=basename(args.path);wasm.push({type:'CompiledWasm',path,contents:await readFile(resolve(dirname(args.importer),args.path))});return {path:`./${path}`,external:true};});
   if(inspect)b.onLoad({filter:/transfer-lifecycle-worker\.ts$/,namespace:'file'},()=>({contents:`import {DurableObject} from 'cloudflare:workers';import {Control,default as router} from ${JSON.stringify(worker+'?control')};export {Control};export default router;export class AssemblyTenant extends DurableObject {async fetch(){return Response.json({stored:this.ctx.storage.kv.get('gonvex/reducer/module'),rows:this.ctx.storage.sql.exec('SELECT * FROM fixture_retained ORDER BY id').toArray()});}}`,loader:'ts'}));
   b.onResolve({filter:/\?control$/},()=>({path:worker,namespace:'control-only'}));
   b.onLoad({filter:/.*/,namespace:'control-only'},async()=>({contents:await readFile(worker,'utf8'),loader:'ts',resolveDir:dirname(worker)}));
  },
 }]});
 return [{type:'ESModule',path:'worker.mjs',contents:result.outputFiles[0].text},...wasm];
}
for(const reverse of [false,true])for(const mismatch of ['hash','generation'])test(`stale ${reverse?'reverse':'forward'} ${mismatch} bridge cannot run or persist an older Tenant release across failed freeze, restart and frozen refresh`,{timeout:30000},async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gonvex-transfer-native-'));let mf;
 const name='gonvex-cf-dev-native-transfer',source=reverse?`${name}-runtime`:name;
 let target=null,options;
 const staleHash=mismatch==='hash'?'hash-1':'hash-2';
 const [current,stale,inspector]=await Promise.all([modules(2),modules(1,false,staleHash),modules(2,true)]);
 const update=async(bundle,transition=false)=>{
  const config={name,compatibilityDate:'2026-10-23',compatibilityFlags:['nodejs_compat'],
   manifest:{mainModule:'worker.mjs',modules:Object.fromEntries(bundle.map(m=>[m.path,{type:m.type==='CompiledWasm'?'wasm':'esm',contents:m.contents}]))},
   env:{CONTROL:{type:'durable-object',worker:name,exportName:'Control'},TENANT:{type:'durable-object',worker:source,exportName:'AssemblyTenant'},
    RELEASES:{type:'r2',name:'fixture-releases'},FILES:{type:'r2',name:'fixture-files'},...(transition?{GONVEX_TOPOLOGY_TRANSITION:{type:'text',value:'1'}}:{})},
   exports:{Control:{type:'durable-object',storage:'sqlite'},...(!reverse?{AssemblyTenant:{type:'durable-object',storage:'sqlite'}}:{})}};
  const workers=[{config}];
  if(reverse)workers.push({config:{...config,name:source,exports:{AssemblyTenant:{type:'durable-object',storage:'sqlite'}}}});
  options={workers,resourcePersistencePath:join(dir,'resources'),log:new Log(LogLevel.NONE)};
  if(mf)await mf.setOptions(options);else mf=new Miniflare(options);
 };
 const call=async(path,body)=>{const r=await mf.dispatchFetch('http://fixture'+path,{method:body?'POST':'GET',...(body?{body:JSON.stringify(body),headers:{'content-type':'application/json'}}:{})});return {status:r.status,body:r.ok?await r.json():await r.text()};};
 try{
  await update(current);
  const initial=(await call('/seed',{project,manifest:manifest(2)})).body;assert.equal(initial.hash,'hash-2');
  assert.equal((await call('/control?action=seed',{project,hash:'hash-2',generation:2})).status,200);
  const nativeId=(await mf.getDurableObjectNamespace('TENANT')).idFromName('original').toString();
  const entry=join(dir,'captured.js');await writeFile(entry,'export const releases={};export const version="catalog-1";');
  const args={catalogEntry:entry,actionCatalogEntry:entry,catalog:[{project,hash:staleHash,generation:1}],reverse};
  let plan=await transferConfigs(name,dir,args),freezeFault=true,wake;
  const ports={namespace:async script=>script===source?'original-namespace':target,topology:async action=>{
   if(action==='freeze'&&freezeFault){freezeFault=false;throw Error('freeze transport lost');}
   const response=await call(`/control?action=${action}`);assert.equal(response.status,200);return response.body;
  },deploy:async path=>{
   const cfg=JSON.parse(await readFile(path));
   if(path===plan.phases[0].path){await update(stale,cfg.vars.GONVEX_TOPOLOGY_TRANSITION==='1');wake=await call('/call');}
   else throw Error('receive interrupted');
  }};
  await assert.rejects(transferTenantNamespace(plan,ports),/freeze transport lost/);
  assert.equal(wake.status,503,`stale bridge executed: ${JSON.stringify(wake)}`);
  await update(inspector);assert.deepEqual((await call('/inspect')).body,{stored:initial.stored,rows:initial.rows});
  assert.equal((await call('/control?action=status')).body.frozen,false);
  await assert.rejects(transferTenantNamespace(plan,ports),/active catalog changed/);
  assert.equal(wake.status,503,'stale code must not run while a freeze/catalog check is pending');
  await update(inspector);assert.deepEqual((await call('/inspect')).body,{stored:initial.stored,rows:initial.rows});
  assert.equal((await call('/control?action=status')).body.frozen,true);
  // Explicit frozen refresh can replace the stale input, but a later failed
  // receiving deploy must still leave the original module/data and freeze.
  await writeFile(entry,'export const releases={};export const version="catalog-2";');
  plan=await transferConfigs(name,dir,{...args,catalog:[{project,hash:'hash-2',generation:2}]});
  const refreshed={...ports,refreshFrozenPlan:true,deploy:async path=>{
   if(path===plan.phases[0].path){
    const cfg=JSON.parse(await readFile(path));await update(current,cfg.vars.GONVEX_TOPOLOGY_TRANSITION==='1');
    const observed=await call('/inspect');assert.equal(observed.status,200,JSON.stringify(observed));assert.equal(observed.body.hash,'hash-2');
    const invoked=await call('/call');assert.equal(invoked.body.ok,true,JSON.stringify(invoked));assert.deepEqual(JSON.parse(invoked.body.result),{generation:2});
    assert.equal((await call('/ack',{project,manifest:manifest(2)})).body.ack,true);
    assert.equal((await call('/ack',{project,manifest:manifest(1)})).body.ack,false);
    for(const path of ['/activate','/plan','/provision'])assert.equal((await call(path,{project,manifest:manifest(1)})).status,409);
   }
   else throw Error('receive interrupted');
  }};
  await assert.rejects(transferTenantNamespace(plan,refreshed),/receive interrupted/);
  await update(inspector);assert.deepEqual((await call('/inspect')).body,{stored:initial.stored,rows:initial.rows});
  assert.equal((await call('/control?action=status')).body.frozen,true);
  assert.equal((await mf.getDurableObjectNamespace('TENANT')).idFromName('original').toString(),nativeId);
 }finally{await mf?.dispose();await rm(dir,{recursive:true,force:true});}
});
