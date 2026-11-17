import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import { wrapBundle } from '../host/src/bundle.ts';
import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import {resolve,dirname,basename} from 'node:path';
const globals = fileURLToPath(new URL('../host/src/module-globals.ts', import.meta.url));
export default defineConfig(async()=>{
 const {build}=createRequire(import.meta.resolve('@cloudflare/vitest-plugin'))('esbuild');
 const compile=async entry=>{const wasm=[];const result=await build({entryPoints:[fileURLToPath(new URL(entry,import.meta.url))],bundle:true,write:false,
  format:'esm',platform:'neutral',target:'esnext',external:['cloudflare:workers','node:*'],loader:{'.sql':'text'},
  alias:{'gonvex:releases':fileURLToPath(new URL('./test/runtime-catalog.ts',import.meta.url)),'gonvex:action-releases':fileURLToPath(new URL('./test/runtime-catalog.ts',import.meta.url))},plugins:[{name:'native-wasm',setup(builder){
   builder.onResolve({filter:/\.wasm$/},async args=>{const name=basename(args.path);wasm.push({type:'CompiledWasm',path:name,contents:await readFile(resolve(dirname(args.importer),args.path))});return {path:`./${name}`,external:true};});
  }}]});return [{type:'ESModule',path:'worker.mjs',contents:result.outputFiles[0].text},...wasm];};
 const [bundle,stable,actions]=await Promise.all([compile('./test/runtime-worker.ts'),compile('./test/stable-worker.ts'),compile('./test/action-catalog-worker.ts')]);
 return {
 plugins:[{name:'module-prelude',enforce:'pre',transform(code,id){return id.split('?')[0].endsWith('.module.js')?{code:wrapBundle(code,globals),map:null}:null;}},cloudflareTest({wrangler:{configPath:'./test/wrangler.jsonc'},miniflare:{workers:[{
  name:'gonvex-cf-dev-k22-test-runtime',modules:bundle,compatibilityDate:'2026-10-23',compatibilityFlags:['nodejs_compat'],
  durableObjects:{TENANT:{className:'AssemblyTenant',useSQLite:true},OTHER_TENANT:{className:'WrongTenant',useSQLite:true},
   CONTROL:{className:'TestControl',scriptName:'gonvex-cf-dev-k22-test-stable'},GATEWAY:{className:'TestGateway',scriptName:'gonvex-cf-dev-k22-test-stable'}},
  r2Buckets:{RELEASES:'gonvex-cf-dev-k22-composition-releases',FILES:'gonvex-cf-dev-k22-composition-files'},
  serviceBindings:{ACTIONS:{name:'gonvex-cf-dev-k22-test-stable',entrypoint:'ActionWorker'},ACTIONS_CATALOG:{name:'gonvex-cf-dev-k22-test-actions',entrypoint:'TestActionCatalog'}},
 },{
  name:'gonvex-cf-dev-k22-test-stable',modules:stable,compatibilityDate:'2026-10-23',compatibilityFlags:['nodejs_compat'],
  durableObjects:{CONTROL:{className:'TestControl',useSQLite:true},GATEWAY:{className:'TestGateway',useSQLite:true},TENANT:{className:'AssemblyTenant',scriptName:'gonvex-cf-dev-k22-test-runtime'}},
  bindings:{GONVEX_DASHBOARD_SESSION_SECRET:'synthetic-publication-secret',GONVEX_ASSEMBLY_FIXTURE_KEY:'synthetic-inspection'},
  serviceBindings:{RUNTIME_CATALOG:{name:'gonvex-cf-dev-k22-test-runtime',entrypoint:'TestRuntimeCatalog'}},
 },{
  name:'gonvex-cf-dev-k22-test-actions',modules:actions,compatibilityDate:'2026-10-23',
 }]}})],
 resolve:{alias:{'gonvex:releases':fileURLToPath(new URL('./releases.ts',import.meta.url))}},
 test:{include:['test/**/*.spec.ts'],testTimeout:30000},
};});
