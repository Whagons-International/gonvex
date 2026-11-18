// K22: the generated release entries must not evaluate every app at Worker startup.
// With all apps imported statically, startup grew with the app count until uploads failed
// Cloudflare's startup validation (code 10021) at ~110 apps.
import {test} from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join,dirname} from 'node:path';
import {createRequire} from 'node:module';import {pathToFileURL} from 'node:url';
import {ReleaseBuilderCore,RELEASES_ENTRY,ACTION_RELEASES_ENTRY} from './core.ts';
// The same bundler wrangler uses for the runtime and Actions Workers.
const esbuild=createRequire(createRequire(import.meta.url).resolve('wrangler/package.json'))('esbuild');

const projects=['alpha','beta','gamma'];
const blobs=(dir)=>({get:async key=>{const [, project]=key.split('/'),file=key.split('/').at(-1);
 const text={'app.js':`globalThis.__evaluated.push(${JSON.stringify(project+':app')});export const ping=()=>${JSON.stringify(project)};`,
  'actions.js':`globalThis.__evaluated.push(${JSON.stringify(project+':actions')});export const act=()=>${JSON.stringify(project)};`,
  'functions.json':JSON.stringify({ping:{kind:'query'}}),
  'release.json':JSON.stringify({storedManifest:{project,module:{hash:'h-'+project}},clientContract:null})}[file];
 return text===undefined?null:{text:async()=>text};},put:async()=>{},delete:async()=>{}});

test('generated entries have no static per-app imports and load each app once, on first use',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gonvex-lazy-'));
 try{
  let staged;const core=new ReleaseBuilderCore({blobs:blobs(dir),preludeSpecifier:'gonvex:module-globals',target:{stage:async files=>{staged=files;},deploy:async()=>{}}});
  const built=await core.activate(projects.map((project,i)=>({project,hash:'h-'+project,generation:i+1})));
  assert.equal(built.ok,true,built.error);
  for(const entry of [RELEASES_ENTRY,ACTION_RELEASES_ENTRY]){
   const text=staged.get(entry);
   assert.doesNotMatch(text,/^\s*import\s/m,`${entry} must not import apps statically`);
   assert.equal((text.match(/require\("\.\/releases\//g)??[]).length>=projects.length,true);
  }
  for(const [path,text] of staged)await mkdir(dirname(join(dir,path)),{recursive:true}).then(()=>writeFile(join(dir,path),text));
  for(const [entry,piece,field] of [[RELEASES_ENTRY,'app','exports'],[ACTION_RELEASES_ENTRY,'actions','exports']]){
   const out=join(dir,entry+'.bundle.mjs');
   await esbuild.build({entryPoints:[join(dir,entry)],bundle:true,format:'esm',platform:'neutral',outfile:out,logLevel:'silent'});
   globalThis.__evaluated=[];
   const {releases}=await import(pathToFileURL(out).href);
   assert.deepEqual(globalThis.__evaluated,[],`no ${piece} module may run at startup`);
   // X13b: an Action isolate reads a project's manifest without evaluating its code.
   if(piece==='actions'){assert.equal(releases.gamma.manifest.project,'gamma');assert.deepEqual(globalThis.__evaluated,[],'manifest alone evaluates no Action code');}
   assert.equal(releases.beta[field][piece==='app'?'ping':'act'](),'beta');
   assert.deepEqual(globalThis.__evaluated,[`beta:${piece}`]);
   assert.equal(releases.beta.manifest.project,'beta');
   void releases.beta[field];void releases.beta.manifest;
   assert.deepEqual(globalThis.__evaluated,[`beta:${piece}`],'cached per isolate');
   if(piece==='app'){assert.equal(releases.alpha.hash,'h-alpha');assert.deepEqual(Object.keys(releases.alpha.functions),['ping']);}
   assert.deepEqual(globalThis.__evaluated,piece==='app'?['beta:app','alpha:app']:['beta:actions']);
  }
 }finally{await rm(dir,{recursive:true,force:true});delete globalThis.__evaluated;}
});
