import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {startLocalAssembly} from './local.mjs';
import {canonicalArtifactHash} from '../../control/operator/builder/manifest.ts';
import {postDevSync} from '../../../packages/gonvex/src/sync-request.ts';

test('actual local composition boots and its Node builder publishes three whole catalogs through HTTP', {timeout:180000}, async()=>{
 const state=await mkdtemp(join(tmpdir(),'gonvex-local-composition-'));let runtime;
 try{
  runtime=await startLocalAssembly({state});
  const call=async(method,path,body,status=200)=>{
   const response=await fetch(runtime.url+path,{method,headers:{authorization:`Bearer ${runtime.adminKey}`,...(body?{'content-type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});
   assert.equal(response.status,status,`${method} ${path}`);return response.json();
  };
  const project=await call('POST','/dev/projects',{name:'Local isolated runtime',databaseMode:'multiTenant'},201);
  for(const generation of [1,2,3]){
   const functions={'probe.generation':{kind:'reducer',handler:'generation',args:{kind:'any'},result:{kind:'any'}}};
   const code=`export async function generation(){return {generation:${generation}};}`;
   const module={language:'typescript',generation,entrypoint:'gonvex/index.ts',functions,visibility:{},files:{},crons:[],invitationAcceptanceReducer:'',
    javascript:{path:'gonvex/_generated/module.js',hash:createHash('sha256').update(code).digest('hex'),code:Buffer.from(code).toString('base64')}};
   module.hash=await canonicalArtifactHash(module);
   const response=await postDevSync(runtime.url+'/dev/sync',{headers:{authorization:`Bearer ${project.projectKey}`,'content-type':'application/json','x-gonvex-project-id':project.project.id},
    body:JSON.stringify({project:project.project.id,functions,schema:{tables:{}},module,visibility:{}})});
   assert.equal(response.status,200,`generation ${generation} sync`);assert.equal((await response.json()).ok,true);
   const identity=await call('GET','/__assembly/version');
   assert.equal(identity.releases[project.project.id],module.hash);assert.equal(identity.ready,true);
   assert.ok(identity.runtimeWorker);assert.ok(identity.worker);
  }
 }finally{await runtime?.stop();await rm(state,{recursive:true,force:true});}
});
