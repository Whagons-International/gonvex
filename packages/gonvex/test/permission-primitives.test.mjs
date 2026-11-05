import assert from 'node:assert/strict';
import { readFile, mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import test from 'node:test';
import { compilePermissionPrimitives } from '../dist/permission-primitives.js';
import { buildModuleArtifact } from '../dist/module-artifact.js';
const fixture = JSON.parse(await readFile(new URL('../../../rust/crates/runtime/testdata/permission-source-text.json', import.meta.url),'utf8'));
test('literal source binding derives the exact runtime old/new dependency certificate',()=>{
  assert.deepEqual(JSON.parse(JSON.stringify(compilePermissionPrimitives(fixture.declaration))),fixture);
  const staged=structuredClone(fixture.declaration); staged.collections={};
  assert.deepEqual(compilePermissionPrimitives(staged).declaration.collections,{});
});
test('namespaced group fixture preserves membership and typed NULL dependencies',async()=>{
  const declaration=JSON.parse(await readFile(new URL('../../../rust/crates/runtime/testdata/permission-source-groups.declaration.json',import.meta.url),'utf8'));
  const expected=JSON.parse(await readFile(new URL('../../../rust/crates/runtime/testdata/permission-source-groups.json',import.meta.url),'utf8'));
  assert.deepEqual(JSON.parse(JSON.stringify(compilePermissionPrimitives(declaration))),expected);
  const memberships=expected.dependencies.groups.filter(x=>x.source==='memberships');
  assert.deepEqual(memberships,[{source:'memberships',table:'memberTeams',key:'_id',alias:'source',columns:['_id','deletedAt','memberId','teamId'],images:'oldAndNew'}]);
  assert.equal(expected.declaration.providers.groups.sources.find(x=>x.id==='teams').target.namespace,'whagons.team');
});
test('unknown/coercing/unbound source inputs and opaque policies reject',()=>{
  const cases=[
    x=>x.providers.self.adapter='callback',
    x=>x.providers.self.sources[0].target.kind='account',
    x=>x.providers.self.sources[1].atoms[0].atom.resource.type='number',
    x=>x.providers.self.sources[1].atoms[0].atom.resource.onNull='guess',
    x=>x.providers.self.sources[1].atoms[0].atom.resource.field.alias='unknown',
    x=>x.providers.self.sources[1].sql='SELECT private',
    x=>x.collections['owned.list'].policy={op:'slot',slot:'opaque-app-mask'},
    x=>x.collections['owned.list'].policy={op:'has',atom:'undeclared'},
    x=>x.collections['owned.list'].policy={op:'all',children:[]},
    x=>x.collections['owned.list'].policy={op:'except',universe:'other',allow:{op:'deny'},remove:{op:'deny'}},
    x=>x.collections['owned.list'].policy={op:'parent',callback:'app-code'},
  ];
  for(const mutate of cases){const input=structuredClone(fixture.declaration);mutate(input);assert.throws(()=>compilePermissionPrimitives(input));}
});
test('extractor signs sources/dependencies and rejects stale collection table/key bindings',async(t)=>{
  const root=await mkdtemp('/mnt/data/work/gonvex-permission-fixtures/cli-source-');
  t.after(()=>rm(root,{recursive:true,force:true}));
  const backendDir=join(root,'gonvex'); await mkdir(backendDir);
  const entrypoint=join(backendDir,'index.ts');
  const program=structuredClone(fixture.declaration);program.collections={};
  const source=(value)=>`function permissionPrimitives(x){return x;}\nexport const permissions=permissionPrimitives(${JSON.stringify(value)});`;
  await writeFile(entrypoint,source(program));
  const options={root,backendDir,files:[entrypoint],migrations:[],entrypoint:"gonvex/index.ts"};
  const before=await buildModuleArtifact(options);
  assert.deepEqual(JSON.parse(JSON.stringify(before.permissionPrimitives)),JSON.parse(JSON.stringify(compilePermissionPrimitives(program))));
  program.providers.self.version='new-source-version';await writeFile(entrypoint,source(program));
  const after=await buildModuleArtifact(options);assert.notEqual(before.hash,after.hash);
  program.collections=fixture.declaration.collections;await writeFile(entrypoint,source(program));
  await assert.rejects(()=>buildModuleArtifact(options),/real replica table\/key/);
});
