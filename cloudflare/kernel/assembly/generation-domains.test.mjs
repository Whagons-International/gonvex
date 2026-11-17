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
async function modules(generation,inspect=false,hash=`hash-${generation}`,catalogGeneration=generation){
 const wasm=[];
 const result=await build({entryPoints:[worker],bundle:true,write:false,format:'esm',platform:'neutral',target:'esnext',external:['cloudflare:workers','node:*'],loader:{'.sql':'text'},plugins:[{
  name:'captured-catalog',setup(b){
   b.onResolve({filter:/^gonvex:releases$/},()=>({path:'catalog',namespace:'fixture'}));
   b.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:`export const version="catalog-${generation}";export const releases={${JSON.stringify(project)}:{project:${JSON.stringify(project)},hash:${JSON.stringify(hash)},generation:${catalogGeneration},manifest:${JSON.stringify(manifest(generation,hash))},exports:{generation:async()=>({generation:${generation}})}}};`,loader:'js'}));
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
