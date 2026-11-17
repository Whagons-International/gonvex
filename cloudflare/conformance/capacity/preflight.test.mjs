import { it, expect } from 'vitest';
import { mkdtemp, writeFile, mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { preflight, hash } from './binding.mjs';
import { binding } from './test-data.mjs';
import { runCell, cleanup } from './driver.mjs';
import { main } from './cli.mjs';
import { loadFixture } from './fixture.mjs';

it('executable preflight/runCell/cleanup remain inert and explicit when real ports are missing', async () => {
  const base = process.env.GONVEX_CAPACITY_TEST_ROOT ?? process.env.RUNNER_TEMP ?? '/mnt/data/work/gonvex-x01/assembled-load-driver-20261117';
  await mkdir(base, { recursive: true }); const root = await mkdtemp(join(base, 'DATA-'));
  try {
    const module = join(root, 'never-import.mjs'), raw = 'throw Error("MUST_NOT_IMPORT");'; await writeFile(module, raw);
    const fixture = { ownedIds: ['DATA'], replica: { ref: ['DATA'], args: {}, keyPath: ['id'] }, liveQueries: [],
      operations: [{ alias: 'rename', kind: 'reducer', ref: ['DATA'], identityPaths: [['id']], cases: [{ args: { id: 'DATA' }, expect: { key: 'DATA', valuePath: ['name'], value: 'DATA' } }] }],
      cleanup: [{ alias: 'cleanup', kind: 'reducer', ref: ['DATA'], identityPaths: [['id']], cases: [{ args: { id: 'DATA' }, expect: { key: 'DATA', valuePath: ['name'], value: 'old' } }] }], finalChecks: [{ key: 'DATA', valuePath: ['name'], value: 'DATA' }] };
    const rawFixture = JSON.stringify(fixture), fixturePath = join(root, 'fixture.json'); await writeFile(fixturePath, rawFixture);
    const b = binding(), f = { path: module, sha256: hash(raw) }; b.sources.sdk = f; b.sources.refs = f; b.sources.closure = [f];
    b.tenants[0].fixture = { path: fixturePath, sha256: hash(rawFixture) };
    const path = join(root, 'binding.json'); await writeFile(path, JSON.stringify(b), { mode: 0o600 });
    expect((await loadFixture(b.tenants[0].fixture)).operations[0].cases).toHaveLength(1);
    expect((await preflight(path)).state).toBe('DORMANT'); expect((await main(['preflight', path])).missing).toContain('authenticated-assembled-capacity-telemetry-v1');
    await expect(runCell({ cell: { id: 'DATA' }, manifest: { driverBinding: path, driverOutput: root }, signal: new AbortController().signal, emit() { throw Error('must not emit'); } })).rejects.toMatchObject({ code: 'UNSUPPORTED' });
    const args = { cell: { id: 'DATA' }, manifest: { driverBinding: path, driverOutput: root }, signal: new AbortController().signal, emit() {} };
    const pair = await Promise.allSettled([runCell(args), runCell(args)]);
    expect(pair[1]).toMatchObject({ status: 'rejected', reason: { field: 'serial-cell-per-process' } });
    expect((await cleanup({ manifest: { driverBinding: path } })).admittedServerEffects).toBe('UNKNOWN');
    b.sources.sdk.sha256 = 'f'.repeat(64); await writeFile(path, JSON.stringify(b)); await expect(preflight(path)).rejects.toThrow();
  } finally { await rm(root, { recursive: true }); }
});
