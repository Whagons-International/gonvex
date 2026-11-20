// X13g (0094 B): the ActionHost lives in the Actions Worker script, so Actions keep that script's 300 s CPU
// limit and bindings; the runtime binds it there, and the dev deploy points the binding at `<name>-actions`.
import {test} from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {servingConfigs} from './deploy.mjs';

test('X13g the ActionHost is an Actions Worker class with its limits and bindings; the runtime and deploy bind it there',async()=>{
 const dir=await mkdtemp(join(tmpdir(),'gonvex-x13g-'));
 try{
  const configs=await servingConfigs('gonvex-cf-dev-x13g',dir);
  const actions=configs.actions.config,runtime=configs.runtime.config;
  assert.deepEqual(actions.exports?.ActionHost,{type:'durable-object',storage:'sqlite'});
  assert.equal(actions.limits.cpu_ms,300000,'Actions keep the Actions Worker CPU limit');
  assert.deepEqual(actions.durable_objects.bindings.map(b=>b.name).sort(),['CONTROL','TENANT']);
  assert.deepEqual(actions.r2_buckets.map(b=>b.binding),['FILES']);
  assert.deepEqual(actions.services.map(b=>b.binding),['SANDBOX']);
  assert.deepEqual(runtime.durable_objects.bindings.find(b=>b.name==='ACTION_HOST'),{name:'ACTION_HOST',class_name:'ActionHost',script_name:'gonvex-cf-dev-x13g-actions'});
  assert.equal(runtime.limits.cpu_ms,30000,'the tenant keeps its own limit; Actions do not run under it');
 }finally{await rm(dir,{recursive:true,force:true});}
});
