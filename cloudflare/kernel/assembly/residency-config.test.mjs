// Data residency in the deploy configs (deploy.mjs stackOptions/applyStack): every component of a jurisdiction stack.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { servingConfigs, stackOptions, stackOptionsFromEnv, DEFAULT_BACKUP_CRON } from './deploy.mjs';

const KINDS = ['kernel', 'runtime', 'actions', 'sandbox', 'builder'];
async function configs(options) {
  const dir = await mkdtemp(join(tmpdir(), 'gonvex-residency-'));
  try { return await servingConfigs('gonvex-cf-dev-residency', dir, { kernelUrl: 'https://k.example', ...options }); }
  finally { await rm(dir, { recursive: true, force: true }); }
}

test('a stack without a jurisdiction keeps its configs (observability and telemetry on, no jurisdiction anywhere)', async () => {
  const c = await configs({});
  for (const kind of KINDS) {
    assert.equal(c[kind].config.vars.GONVEX_JURISDICTION, undefined, kind);
    for (const b of c[kind].config.r2_buckets ?? []) assert.equal(b.jurisdiction, undefined, `${kind} ${b.binding}`);
    assert.equal(c[kind].config.vars.GONVEX_STACK_NAME, 'gonvex-cf-dev-residency');
  }
  assert.equal(c.kernel.config.observability.enabled, true);
  assert.ok(c.kernel.config.analytics_engine_datasets?.length);
  assert.equal(c.kernel.config.triggers, undefined);
  assert.ok(!c.kernel.config.r2_buckets.some((b) => b.binding === 'BACKUPS'));
});

test('a jurisdiction reaches every Worker and every R2 binding; logs and telemetry stores are off', async () => {
  const c = await configs({ jurisdiction: 'eu' });
  for (const kind of KINDS) {
    assert.equal(c[kind].config.vars.GONVEX_JURISDICTION, 'eu', kind);
    for (const b of c[kind].config.r2_buckets ?? []) assert.equal(b.jurisdiction, 'eu', `${kind} ${b.binding}`);
    assert.deepEqual(c[kind].config.observability, { enabled: false, logs: { enabled: false, invocation_logs: false } }, kind);
    assert.equal(c[kind].config.analytics_engine_datasets, undefined, kind);
  }
  // The fresh-deploy bootstraps inherit the same settings.
  assert.ok(c.bootstrap.config.r2_buckets.every((b) => b.jurisdiction === 'eu'));
  assert.ok(c.runtimeBootstrap.config.r2_buckets.every((b) => b.jurisdiction === 'eu'));
  assert.deepEqual(JSON.parse(c.builder.config.vars.GONVEX_STACK_OPTIONS), stackOptions({ jurisdiction: 'eu' }));
  assert.equal(c.builder.config.vars.GONVEX_BUILDER_RESIDENCY, undefined);
  assert.equal((await configs({ jurisdiction: 'eu', builderResidency: true })).builder.config.vars.GONVEX_BUILDER_RESIDENCY, '1');
  // Observability can be opted back into explicitly.
  assert.equal((await configs({ jurisdiction: 'eu', observability: true })).kernel.config.observability.enabled, true);
});

test('backups bind BACKUPS to the kernel and runtime only, in the jurisdiction, with a nightly cron', async () => {
  const c = await configs({ jurisdiction: 'eu', backups: true });
  for (const kind of ['kernel', 'runtime']) {
    assert.deepEqual(c[kind].config.r2_buckets.find((b) => b.binding === 'BACKUPS'), { binding: 'BACKUPS', bucket_name: 'gonvex-cf-dev-residency-backups', jurisdiction: 'eu' });
    assert.equal(c[kind].config.vars.GONVEX_BACKUP_PREFIX, 'nightly');
  }
  for (const kind of ['actions', 'sandbox', 'builder']) assert.ok(!(c[kind].config.r2_buckets ?? []).some((b) => b.binding === 'BACKUPS'), kind);
  assert.deepEqual(c.kernel.config.triggers, { crons: [DEFAULT_BACKUP_CRON] });
  assert.equal(c.runtime.config.triggers, undefined);
  // A stack without a jurisdiction may still keep its backups (or a one-time copy) in one.
  const out = await configs({ backups: { bucket: 'gonvex-cf-dev-other-backups', jurisdiction: 'eu', prefix: 'migration' } });
  assert.deepEqual(out.kernel.config.r2_buckets.find((b) => b.binding === 'BACKUPS'), { binding: 'BACKUPS', bucket_name: 'gonvex-cf-dev-other-backups', jurisdiction: 'eu' });
  assert.ok(out.kernel.config.r2_buckets.filter((b) => b.binding !== 'BACKUPS').every((b) => b.jurisdiction === undefined));
});

test('stack options refuse what cannot be honoured', () => {
  assert.throws(() => stackOptions({ jurisdiction: 'us' }), /jurisdiction must be one of/);
  assert.throws(() => stackOptions({ jurisdiction: 'EU' }), /jurisdiction must be one of/);
  assert.throws(() => stackOptions({ jurisdiction: 'eu', backups: { jurisdiction: 'fedramp' } }), /keeps its backups in eu/);
  assert.throws(() => stackOptions({ backups: { cron: '* *' } }), /five fields/);
  assert.throws(() => stackOptions({ backups: { bucket: 'Bad_Name' } }), /bucket name/);
  assert.deepEqual(stackOptionsFromEnv(''), stackOptions());
  assert.deepEqual(stackOptionsFromEnv(JSON.stringify(stackOptions({ jurisdiction: 'eu', backups: true }))), stackOptions({ jurisdiction: 'eu', backups: true }));
});
