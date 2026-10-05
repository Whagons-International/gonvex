import assert from 'node:assert/strict';
import test from 'node:test';
import { action, internalQuery, internalReducer, liveQuery, ModuleBuilder, ModuleRuntimeRegistry, query, reducer } from '../dist/index.js';

const plan = { table: 'jobs', key: 'id', columns: ['id'] };

test('public declarations preserve their manifest flag and nullable context', async () => {
  const builder = new ModuleBuilder({ name: 'public', version: '1' });
  const run = async ctx => ({ tenant: ctx.tenant.id, account: ctx.auth.account, member: ctx.member });
  builder.query('jobs.list', { public: true, run });
  builder.reducer('jobs.apply', { public: true, interactive: false, run });
  builder.action('jobs.upload', { public: true, run });
  const manifest = builder.manifest();
  for (const name of ['jobs.list', 'jobs.apply', 'jobs.upload']) assert.equal(manifest.functions[name].public, true);
  assert.equal(manifest.functions['jobs.apply'].interactive, false);
  assert.equal(manifest.functions['jobs.apply'].offline.mode, 'forbidden');
  assert.equal(manifest.functions['jobs.apply'].localExecution, undefined);
  assert.doesNotThrow(() => new ModuleRuntimeRegistry(builder));
  const declared = query({ public: true, run });
  assert.deepEqual(await declared.handler({ tenant: { id: 'tenant' }, auth: { account: null }, member: null }, {}), { tenant: 'tenant', account: null, member: null });
  assert.equal(action({ public: true, run }).options.public, true);
});

test('public functions reject internal, streaming and local reducer execution', () => {
  assert.throws(() => internalQuery({ public: true, liveQueryPlan: plan }), /cannot be internal/);
  assert.throws(() => internalReducer({ public: true }), /cannot be internal/);
  assert.throws(() => liveQuery({ public: true, liveQueryPlan: plan }), /one-shot/);
  assert.throws(() => reducer({ public: true }), /interactive: false/);
  assert.throws(() => reducer({ public: true, interactive: true }), /interactive: false/);
  assert.throws(() => reducer({ public: true, interactive: false, offline: { mode: 'allowed' } }), /server-only/);
  assert.doesNotThrow(() => reducer({ public: true, interactive: false }));
  const builder = new ModuleBuilder({ name: 'test', version: '1' });
  assert.throws(() => builder.reducer('unsafe', { public: true }), /interactive: false/);
  assert.throws(() => builder.query('stream', { public: true, delivery: 'live', liveQueryPlan: plan }), /one-shot/);
});
