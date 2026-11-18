// X13d: an artifact that carries the Action-only bundle in files["actions/bundle.js"] (signed by the artifact
// hash) gives the Actions Worker only that code; the runtime Worker's app.js is unchanged; without the file the
// Actions Worker keeps the complete bundle; a changed file no longer matches the artifact hash.
import {test} from 'node:test';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {ReleaseBuilderCore,PRELUDE_SPECIFIER,ACTION_BUNDLE_FILE} from './core.ts';
import {canonicalArtifactHash} from './manifest.ts';
import {incomingKey} from '../release.ts';

const project='x13d-fixture';
const complete='export const tasks={rename:async()=>"REDUCER_CODE"};export const assistant={ask:async()=>"ACTION_CODE"};';
const actionOnly='const ask=async()=>"ACTION_CODE";export{ask as "assistant.ask"};';
const b64=s=>Buffer.from(s).toString('base64');
async function prepare(files,tamper){
 const module={language:'typescript',entrypoint:'index.ts',generation:8,files,
  functions:{'tasks.rename':{kind:'reducer',handler:'rename',file:'gonvex/tasks.ts',export:'rename',args:{kind:'any'},result:{kind:'any'}},'assistant.ask':{kind:'action',handler:'ask',file:'gonvex/assistant.ts',export:'ask',args:{kind:'any'},result:{kind:'any'}}},
  javascript:{path:'module.js',hash:createHash('sha256').update(complete).digest('hex'),code:b64(complete)}};
 module.hash=await canonicalArtifactHash(module);
 if(tamper)module.files={...module.files,[ACTION_BUNDLE_FILE]:b64('export const x=1;')};
 const rows=new Map([[incomingKey('x13d'),JSON.stringify({project,functions:module.functions,schema:{tables:{}},module})]]);
 const blobs={get:async k=>rows.has(k)?{text:async()=>rows.get(k)}:null,put:async(k,v)=>rows.set(k,v),delete:async k=>rows.delete(k)};
 const core=new ReleaseBuilderCore({blobs,target:{stage:async()=>{},deploy:async()=>{}},preludeSpecifier:PRELUDE_SPECIFIER});
 const result=await core.prepare('x13d',project);
 const piece=name=>[...rows].find(([k])=>k.endsWith('/'+name))?.[1];
 return {result,hash:module.hash,actions:piece('actions.js'),app:piece('app.js')};
}

test('X13d the Actions Worker gets the signed Action-only bundle when the artifact carries it',async()=>{
 const withFile=await prepare({[ACTION_BUNDLE_FILE]:b64(actionOnly)});
 const without=await prepare({});
 assert.equal(withFile.result.ok,true,JSON.stringify(withFile.result));assert.equal(without.result.ok,true,JSON.stringify(without.result));
 assert.notEqual(withFile.hash,without.hash,'the file is part of the signed contract');
 assert.match(withFile.actions,/"assistant.ask"/);assert.doesNotMatch(withFile.actions,/REDUCER_CODE/);
 assert.match(without.actions,/REDUCER_CODE/);assert.match(without.actions,/ACTION_CODE/);
 assert.equal(withFile.app,without.app,'the runtime Worker bundle is unchanged');
 // The stored manifest the serving kernel reads keeps no copy of the bundle.
 assert.deepEqual(Object.keys(withFile.result.summary.storedManifest.module.files),[]);
});

test('X13d a changed Action bundle file does not match the artifact hash',async()=>{
 const tampered=await prepare({[ACTION_BUNDLE_FILE]:b64(actionOnly)},true);
 assert.equal(tampered.result.ok,false);
});

test('X13d review r4620859237: a correctly hashed but undecodable Action bundle is a 422 invalid artifact, and nothing is written',async()=>{
 for(const bad of ['%','//4=',42]){
  const out=await prepare({[ACTION_BUNDLE_FILE]:bad});
  assert.equal(out.result.ok,false,JSON.stringify(out.result));
  assert.equal(out.result.status,422);
  assert.match(out.result.error,/invalid module artifact: actions\/bundle\.js/);
  assert.equal(out.actions,undefined);assert.equal(out.app,undefined);
 }
});
