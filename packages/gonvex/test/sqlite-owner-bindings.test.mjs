import assert from 'node:assert/strict';
import test from 'node:test';
import {mkdtemp,mkdir,writeFile,symlink,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import ts from 'typescript';
import {sqliteOwnerBindings,sqliteOwnerReferences} from '../dist/sqlite-owner-bindings.js';
import {sqliteBindings} from '../dist/sqlite-bindings.js';
const local={things:{key:'id',columns:{id:{type:'text',nullable:false},title:{type:'text',nullable:true}}}};
const declaration={table:'things',key:'id',columns:['id','title'],mode:'eager',maxRows:100,maxBytes:10000};
const manifest={functions:{'__local.things':{kind:'query',internal:true,replica:declaration},list:{kind:'query',replica:declaration},rename:{kind:'reducer',localExecution:1}},module:{hash:'synthetic-artifact',files:{'client-contract.json':Buffer.from(JSON.stringify({version:1,offlineMaxAgeMs:1800000})).toString('base64')}}};
test('emits and typechecks dedicated-owner/browser graphs against the published package entry points',async t=>{
 const root=await mkdtemp(join(tmpdir(),'sqlite-owner-codegen-'));t.after(()=>rm(root,{recursive:true,force:true}));await mkdir(join(root,'node_modules','@gonvex'),{recursive:true});
 for(const name of ['client','local-runtime','module-sdk','protocol'])await symlink(fileURLToPath(new URL('../../'+name,import.meta.url)),join(root,'node_modules','@gonvex',name));
 await writeFile(join(root,'package.json'),JSON.stringify({type:'module'}));const outputs={...sqliteBindings(local,manifest),...sqliteOwnerBindings(manifest)};
 outputs['local-schema.ts']='export const localSchema='+JSON.stringify(local)+' as const;';
 outputs['api.ts']='export const api={list:{kind:"query",path:"list",delivery:"replica",replica:'+JSON.stringify(declaration)+'},rename:{kind:"reducer",path:"rename",localExecution:1}} as const;';
 outputs['local-reducers.ts']='import {reducer,schema} from "@gonvex/module-sdk";export const localReducers={rename:reducer({args:schema.any(),result:schema.any(),run:async()=>null})};';
 for(const [name,body] of Object.entries(outputs))await writeFile(join(root,name),body);
 const program=ts.createProgram([join(root,'sqlite-client-worker.ts'),join(root,'sqlite-browser.ts'),join(root,'sqlite-client.ts')],{module:ts.ModuleKind.NodeNext,moduleResolution:ts.ModuleResolutionKind.NodeNext,target:ts.ScriptTarget.ES2022,strict:true,skipLibCheck:true,noEmit:true,types:[]});
 assert.equal(ts.getPreEmitDiagnostics(program).map(value=>ts.flattenDiagnosticMessageText(value.messageText,'\n')).join('\n'),'');
 assert.match(outputs['sqlite-client.ts'],/extends SQLiteBrowserClient/);assert.doesNotMatch(outputs['sqlite-client.ts'],/localRuntime|new.*BaseClient/);assert.match(outputs['sqlite-client-worker.ts'],/new SQLiteClientProgram/);assert.equal(outputs['indexeddb-client-worker.ts'],undefined);assert.doesNotMatch(outputs['sqlite-browser.ts'],/createFallbackWorker|indexeddb/);assert.match(outputs['sqlite-client-worker.ts'],/offlineMaxAgeMs:1800000/);assert.match(outputs['sqlite-client-worker.ts'],/new PortableReducerRuntime/);assert.doesNotMatch(outputs['sqlite-browser.ts'],/PortableReducerRuntime|localReducers|sqlite-module|GonvexClient|sqlite-schema\.js/);
 assert.match(outputs['sqlite-browser.ts'],/new Worker\(new URL\("\.\/sqlite-client-worker.ts"/);assert.equal(outputs['sqlite-router-worker.ts'].includes('sqlite/router-worker'),true);
});
test('readonly projects receive a worker without importing nonexistent portable reducer files',()=>{
 const readonly=structuredClone(manifest);delete readonly.functions.rename;const outputs=sqliteOwnerBindings(readonly);assert.doesNotMatch(outputs['sqlite-client-worker.ts'],/local-schema|local-reducers|PortableReducerRuntime/);assert.match(outputs['sqlite-client-worker.ts'],/No portable Reducer registered/);
 const empty=structuredClone(readonly);empty.functions={};assert.deepEqual(sqliteOwnerBindings(empty),{});
});

test('emits a plain public owner catalog without traversing the React API or admitting unrelated internals',()=>{
 const input=structuredClone(manifest);
 input.functions.secret={kind:'reducer',internal:true,localExecution:1};
 input.functions.list.args={type:'object',properties:{huge:{type:'string'}}};
 input.functions.list.result={type:'array',items:{type:'string'}};
 input.functions.rename.offline={mode:'allowed'};
 input.functions.rename.localDependencies=['things'];
 const refs=sqliteOwnerReferences(input);
 assert.deepEqual(refs.map(ref=>ref.path),['__local.things','list','rename']);
 assert.equal(refs.find(ref=>ref.path==='list').replica.residency,'paged');
 assert.deepEqual(refs.find(ref=>ref.path==='rename').offline,{mode:'allowed'});
 assert.deepEqual(JSON.parse(refs.find(ref=>ref.path==='rename').localDependencyTables),['things']);
 assert.doesNotMatch(JSON.stringify(refs),/argsSchema|resultSchema|properties|huge|secret/);
 const worker=sqliteOwnerBindings(input)['sqlite-client-worker.ts'];
 assert.doesNotMatch(worker,/import.*api\.js|function visit|references\.set/);
 assert.match(worker,/const references=\[/);
});

test('binds historical storage names in worker code rather than accepting a browser RPC database selector',()=>{
 const output=sqliteOwnerBindings(manifest,{replicaName:'app-cache',outboxName:'app-queue'});assert.match(output['sqlite-client-worker.ts'],/legacyStorage:\{"replicaName":"app-cache","outboxName":"app-queue"\}/);assert.doesNotMatch(output['sqlite-client.ts'],/app-cache|app-queue/);assert.throws(()=>sqliteOwnerBindings(manifest,{replicaName:'same',outboxName:'same'}),/must differ/);
});
