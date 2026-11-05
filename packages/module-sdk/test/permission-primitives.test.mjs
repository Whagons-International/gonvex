import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { permissionPrimitives } from '../dist/index.js';
const fixture=JSON.parse(await readFile(new URL('../../../rust/crates/runtime/testdata/permission-source-text.json',import.meta.url),'utf8'));
test('source-bound permission declaration is exported, copied and deeply immutable',()=>{
  const original=structuredClone(fixture.declaration);
  const value=permissionPrimitives(original);
  assert.deepEqual(JSON.parse(JSON.stringify(value)),original);
  assert.equal(Object.isFrozen(value.providers.self.sources[0].target),true);
  original.providers.self.version='changed';assert.notEqual(value.providers.self.version,'changed');
});
test('permission declaration refuses callback/accessor/cyclic authority',()=>{
  assert.throws(()=>permissionPrimitives({...fixture.declaration,callback:()=>true}));
  const cycle={...fixture.declaration};cycle.self=cycle;assert.throws(()=>permissionPrimitives(cycle));
  const accessor={...fixture.declaration};Object.defineProperty(accessor,'private',{enumerable:true,get(){throw new Error('getter executed');}});
  assert.throws(()=>permissionPrimitives(accessor),/accessors/);
});
