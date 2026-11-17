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
async function modules(generation,inspect=false,hash=`hash-${generation}`,catalogGeneration=generation,capturedManifest=manifest(generation,hash)){
 const wasm=[];
 const result=await build({entryPoints:[worker],bundle:true,write:false,format:'esm',platform:'neutral',target:'esnext',external:['cloudflare:workers','node:*'],loader:{'.sql':'text'},plugins:[{
  name:'captured-catalog',setup(b){
   b.onResolve({filter:/^gonvex:releases$/},()=>({path:'catalog',namespace:'fixture'}));
   b.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:`export const version="catalog-${generation}";export const releases={${JSON.stringify(project)}:{project:${JSON.stringify(project)},hash:${JSON.stringify(hash)},generation:${catalogGeneration},manifest:${JSON.stringify(capturedManifest)},exports:{generation:async()=>({generation:${generation}})}}};`,loader:'js'}));
   b.onResolve({filter:/\.wasm$/},async args=>{const path=basename(args.path);wasm.push({type:'CompiledWasm',path,contents:await readFile(resolve(dirname(args.importer),args.path))});return {path:`./${path}`,external:true};});
   if(inspect)b.onLoad({filter:/transfer-lifecycle-worker\.ts$/,namespace:'file'},()=>({contents:`import {DurableObject} from 'cloudflare:workers';import {Control,default as router} from ${JSON.stringify(worker+'?control')};export {Control};export default router;export class AssemblyTenant extends DurableObject {async fetch(){return Response.json({stored:this.ctx.storage.kv.get('gonvex/reducer/module'),rows:this.ctx.storage.sql.exec('SELECT * FROM fixture_retained ORDER BY id').toArray()});}}`,loader:'ts'}));
   b.onResolve({filter:/\?control$/},()=>({path:worker,namespace:'control-only'}));
   b.onLoad({filter:/.*/,namespace:'control-only'},async()=>({contents:await readFile(worker,'utf8'),loader:'ts',resolveDir:dirname(worker)}));
  },
 }]});
 return [{type:'ESModule',path:'worker.mjs',contents:result.outputFiles[0].text},...wasm];
}

test('review: matching publication pin is usable when artifact format generation differs', {timeout:30000}, async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gonvex-review-format-'));let mf;
 try {
  const bundle=await modules(8,false,'hash-8',2);
  const config={name:'fixture',compatibilityDate:'2026-10-23',compatibilityFlags:['nodejs_compat'],
   manifest:{mainModule:'worker.mjs',modules:Object.fromEntries(bundle.map(m=>[m.path,{type:m.type==='CompiledWasm'?'wasm':'esm',contents:m.contents}]))},
   env:{CONTROL:{type:'durable-object',worker:'fixture',exportName:'Control'},TENANT:{type:'durable-object',worker:'fixture',exportName:'AssemblyTenant'},RELEASES:{type:'r2',name:'fixture-releases'},FILES:{type:'r2',name:'fixture-files'}},
   exports:{Control:{type:'durable-object',storage:'sqlite'},AssemblyTenant:{type:'durable-object',storage:'sqlite'}}};
  const options={workers:[{config}],resourcePersistencePath:join(dir,'resources'),log:new Log(LogLevel.NONE)};
  mf=new Miniflare(options);
  const call=async(path,body)=>{const r=await mf.dispatchFetch('http://fixture'+path,{method:body?'POST':'GET',...(body?{body:JSON.stringify(body),headers:{'content-type':'application/json'}}:{})});return {status:r.status,body:await r.text()};};
  const seeded=await call('/seed',{project,manifest:manifest(8)});assert.equal(seeded.status,200,seeded.body);
  const before=await call('/call');assert.equal(before.status,200,before.body);
  assert.equal((await call('/control?action=seed',{project,hash:'hash-8',generation:2})).status,200);
  config.env.GONVEX_TOPOLOGY_TRANSITION={type:'text',value:'1'};await mf.setOptions(options);
  const frozen=await call('/control?action=freeze');assert.equal(frozen.status,200,frozen.body);
  const after=await call('/call');console.log('review independent generation domains',JSON.stringify({artifactFormat:8,publication:2,before,after,frozen}));
  assert.equal(after.status,200,'same hash, correct publication counter, unchanged artifact format must remain usable: '+after.body);
  const retained=JSON.parse(seeded.body).stored;
  assert.deepEqual(JSON.parse((await call('/inspect')).body).stored,retained);
  // Same-ID recovery acknowledges Control's publication counter, not format8.
  const ack=async(publicationGeneration,body=manifest(8))=>JSON.parse((await call('/ack',{project,manifest:body,publicationGeneration})).body).ack;
  assert.equal(await ack(2),true);
  assert.equal(await ack(8),false);
  assert.equal(await ack(1),false);
  assert.equal(await ack(2,manifest(8,'wrong-hash')),false);
  assert.equal((await call('/activate',{project,manifest:manifest(7,'hash-8')})).status,409);
  assert.deepEqual(JSON.parse((await call('/inspect')).body).stored,retained);
 }finally{await mf?.dispose();await rm(dir,{recursive:true,force:true});}
});

import {ReleaseBuilderCore,PRELUDE_SPECIFIER} from '../../control/operator/builder/core.ts';
import {canonicalArtifactHash} from '../../control/operator/builder/manifest.ts';
import {incomingKey} from '../../control/operator/release.ts';
import {createHash} from 'node:crypto';
async function prepared(format){
 const code='export async function generation(){return {generation:null};}';
 const module={language:'typescript',entrypoint:'index.ts',files:{},functions:manifest(null).functions,javascript:{path:'module.js',hash:createHash('sha256').update(code).digest('hex'),code:Buffer.from(code).toString('base64')},...(format===null?{}:{generation:format})};
 module.hash=await canonicalArtifactHash(module);
 const input={project,functions:module.functions,schema:{tables:{}},module};
 const rows=new Map([[incomingKey('null-review'),JSON.stringify(input)]]);
 const blobs={get:async k=>rows.has(k)?{text:async()=>rows.get(k)}:null,put:async(k,v)=>rows.set(k,v),delete:async k=>rows.delete(k)};
 const core=new ReleaseBuilderCore({blobs,target:{stage:async()=>{},deploy:async()=>{}},preludeSpecifier:PRELUDE_SPECIFIER});
 assert.deepEqual(await core.inspect('null-review',project),{ok:true,project,hash:module.hash});
 const result=await core.prepare('null-review',project);assert.equal(result.ok,true,JSON.stringify(result));
 assert.equal(result.summary.storedManifest.module.generation,format);
 return result.summary.storedManifest;
}
for(const reverse of [false,true])for(const format of [null,0])test(`independent builder-prepared ${reverse?'reverse':'forward'} format${format} exact publication ACK`,{timeout:30000},async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gonvex-builder-domains-'));let mf;
 const counter=2,source=reverse?'fixture-runtime':'fixture';
 try{
  const retained=await prepared(format),hash=retained.module.hash;
  const bundle=await modules(format,false,hash,counter,retained);
  const config={name:'fixture',compatibilityDate:'2026-10-23',compatibilityFlags:['nodejs_compat'],
   manifest:{mainModule:'worker.mjs',modules:Object.fromEntries(bundle.map(m=>[m.path,{type:m.type==='CompiledWasm'?'wasm':'esm',contents:m.contents}]))},
   env:{CONTROL:{type:'durable-object',worker:'fixture',exportName:'Control'},TENANT:{type:'durable-object',worker:source,exportName:'AssemblyTenant'},RELEASES:{type:'r2',name:'fixture-releases'},FILES:{type:'r2',name:'fixture-files'}},
   exports:{Control:{type:'durable-object',storage:'sqlite'},...(!reverse?{AssemblyTenant:{type:'durable-object',storage:'sqlite'}}:{})}};
  const workers=[{config}];if(reverse)workers.push({config:{...config,name:source,exports:{AssemblyTenant:{type:'durable-object',storage:'sqlite'}}}});
  const options={workers,resourcePersistencePath:join(dir,'resources'),log:new Log(LogLevel.NONE)};mf=new Miniflare(options);
  const call=async(path,body)=>{const r=await mf.dispatchFetch('http://fixture'+path,{method:body?'POST':'GET',...(body?{body:JSON.stringify(body),headers:{'content-type':'application/json'}}:{})});return {status:r.status,body:await r.text()};};
  const seed=await call('/seed',{project,manifest:retained});assert.equal(seed.status,200,seed.body);const initial=JSON.parse(seed.body);
  const ack=async()=>{const result=await call('/ack',{project,manifest:retained,publicationGeneration:counter});assert.equal(result.status,200,result.body);return JSON.parse(result.body).ack;};
  const before=await ack();assert.equal(before,true);
  assert.equal((await call('/control?action=seed',{project,hash,generation:counter})).status,200);
  for(const {config:workerConfig} of workers)workerConfig.env={...workerConfig.env,GONVEX_TOPOLOGY_TRANSITION:{type:'text',value:'1'}};
  await mf.setOptions(options);
  assert.equal((await call('/control?action=freeze')).status,200);
  const invoked=await call('/call');assert.equal(invoked.status,200,invoked.body);
  const observed=JSON.parse((await call('/inspect')).body);assert.deepEqual(observed,initial);
  const after=await ack(),activate=await call('/activate',{project,manifest:retained});
  console.log('PREPARED_NATIVE',JSON.stringify({reverse,format,counter,hash,before,after,invokeStatus:invoked.status,activeGeneration:observed.generation,exactActivateStatus:activate.status,preserved:true}));
  assert.equal(after,true,'same prepared artifact + exact publication counter must acknowledge after bridge adoption');
  assert.equal(activate.status,200,'exact prepared artifact activation is a no-op during maintenance');
  assert.deepEqual(JSON.parse((await call('/inspect')).body),initial);
  for(const generation of [1,8])assert.equal(JSON.parse((await call('/ack',{project,manifest:retained,publicationGeneration:generation})).body).ack,false);
  assert.equal((await call('/activate',{project,manifest:{...retained,module:{...retained.module,generation:8}}})).status,409);
 }finally{await mf?.dispose();await rm(dir,{recursive:true,force:true});}
});
