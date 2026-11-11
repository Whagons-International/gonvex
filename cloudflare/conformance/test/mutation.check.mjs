/**
 * Harness self-test: each mutation of the Rust runtime's frames must make
 * the harness fail with a diff that names the drift. Needs the Rust binary
 * (see README.md). Run: node --test test/mutation.check.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const CLI = fileURLToPath(new URL('../src/cli.mjs', import.meta.url));
const run = (mutation, scenario) => {
  const work = mkdtempSync(join(process.env.GONVEX_CONFORMANCE_TMP ?? tmpdir(), 'gonvex-conformance-mutant-'));
  try {
    return spawnSync(process.execPath, [CLI, '--work', work, '--backend', 'rust=rust', '--backend', `mutant=${fileURLToPath(new URL('../src/backends/mutant.mjs', import.meta.url))}`, '--scenario', scenario],
      // A mutated backend makes every wait time out; short waits keep this quick.
      { encoding: 'utf8', env: { ...process.env, GONVEX_CONFORMANCE_MUTATION: mutation, GONVEX_CONFORMANCE_UNTIL_MS: '3000' }, timeout: 300_000, maxBuffer: 64 << 20 });
  } finally { rmSync(work, { recursive: true, force: true }); }
};

const CASES = [
  ['reducer-error-class', 'reducer-errors', /class: "rejected" != "transient"/],
  ['row-value', 'reducer-second-client', /title: "first" != "first!"|"renamed" != "renamed!"/],
  ['drop-watermarks', 'reducer-second-client', /in replica.watermark: rust sent, mutant none/],
  // Digests are masked in diffs; the per-backend self-check must catch this.
  ['digest-drift', 'reducer-second-client', /\[mutant\] digest self-check, session [a-z-]+: digest mismatch on replica\.(ready|delta)/],
];

for (const [mutation, scenario, expected] of CASES) {
  test(`mutation ${mutation} is caught by ${scenario}`, () => {
    const r = run(mutation, scenario);
    assert.equal(r.status, 1, `harness should fail on ${mutation}:\n${r.stdout}\n${r.stderr}`);
    assert.match(r.stdout, expected, r.stdout);
  });
}
