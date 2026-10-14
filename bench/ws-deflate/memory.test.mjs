import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('runtime startup failure exits with the original error after cleanup', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'gonvex-memory-exit-'));
  try {
    const binary = join(directory, 'runtime');
    await writeFile(binary, '#!/bin/sh\nexit 42\n', { mode: 0o700 });
    const result = await new Promise(resolve => {
      execFile(process.execPath, [new URL('./memory.mjs', import.meta.url).pathname], {
        env: { ...process.env, RUNTIME_BINARY: binary },
        timeout: 5000,
      }, (error, stdout, stderr) => resolve({ error, stdout, stderr }));
    });
    assert.equal(result.error?.killed, false, 'benchmark must finish without a timeout');
    assert.equal(result.error?.code, 1);
    assert.match(result.stderr, /runtime exit 42/);
    assert.doesNotMatch(result.stderr, /unsettled top-level await/i);
    assert.equal(result.stdout, '');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
