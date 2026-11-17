import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const source = readFileSync(new URL('../.github/workflows/runtime-regression.yml', import.meta.url), 'utf8');
const commands = [
  'cargo test --locked -p gonvex-runtime --features duckdb --lib data_ingest::tests',
  'cargo test --locked -p gonvex-sandbox-worker --features duckdb --all-targets',
  'cargo clippy --locked -p gonvex-runtime -p gonvex-sandbox-worker --features gonvex-runtime/duckdb,gonvex-sandbox-worker/duckdb --all-targets -- -D warnings',
];
function job(text, name) {
  const start = text.indexOf(`\n  ${name}:\n`);
  assert.notEqual(start, -1, `${name} job required`);
  return text.slice(start + 1).split(/\n(?=  [\w-]+:\n)/)[0];
}
function contract(text) {
  const duckdb = job(text, 'duckdb'), ci = job(text, 'ci');
  assert.doesNotMatch(duckdb, /^    (needs|if|continue-on-error):|^        (if|continue-on-error):/m,
    'DuckDB must start independently and must not skip or tolerate failure');
  assert.match(duckdb, /runs-on: ubuntu-latest/);
  assert.match(duckdb, /dtolnay\/rust-toolchain@1\.94\.0/);
  for (const prerequisite of ['actions/checkout@v7', 'actions/setup-node@v7', 'Swatinem/rust-cache@v2',
    'node-version: "24"', 'components: clippy,rustfmt', 'workspaces: rust',
    'cache-workspace-crates: true', 'cache-on-failure: true', 'pnpm@11.1.2',
    'pnpm install --frozen-lockfile', 'pnpm build:packages', 'working-directory: rust']) {
    assert.ok(duckdb.includes(prerequisite), `retain prerequisite ${prerequisite}`);
  }
  const cargoLines = duckdb.split('\n').map(line => line.trim()).filter(line => line.startsWith('cargo '));
  assert.deepEqual(cargoLines, commands, 'retain all three exact commands without ignored exits');
  for (const command of commands) assert.ok(!job(text, 'test').includes(command), 'remove serial duplication');
  assert.match(ci, /if: always\(\)/);
  assert.match(ci, /needs: \[test, policies, native, duckdb\]/);
  for (const [env, name] of [['RUNTIME', 'test'], ['POLICY', 'policies'], ['NATIVE', 'native'], ['DUCKDB', 'duckdb']]) {
    assert.ok(ci.includes(`${env}_RESULT: \${{ needs.${name}.result }}`));
    assert.ok(ci.includes(`test "$${env}_RESULT" = success`));
  }
  assert.doesNotMatch(ci, /continue-on-error:/);
  return ci.split('        run: |\n')[1].split('\n').map(line => line.slice(10)).join('\n');
}
test('DuckDB runs concurrently with unchanged commands and required prerequisites', () => contract(source));
test('aggregate shell rejects failed, cancelled and skipped dependencies', () => {
  const script = contract(source), env = { RUNTIME_RESULT: 'success', POLICY_RESULT: 'success', NATIVE_RESULT: 'success', DUCKDB_RESULT: 'success' };
  assert.equal(spawnSync('bash', ['-e', '-c', script], { env }).status, 0);
  for (const key of Object.keys(env)) for (const state of ['failure', 'cancelled', 'skipped']) {
    assert.notEqual(spawnSync('bash', ['-e', '-c', script], { env: { ...env, [key]: state } }).status, 0, `${key}=${state}`);
  }
});
test('contract rejects missing checks, serialization, skip paths and waived outcomes', () => {
  for (const command of commands) assert.throws(() => contract(source.replace(command, 'echo removed')));
  for (const insertion of ['    needs: [test]\n', '    if: false\n', '    continue-on-error: true\n']) {
    assert.throws(() => contract(source.replace('  duckdb:\n', '  duckdb:\n' + insertion)));
  }
  assert.throws(() => contract(source.replace('      - name: Test optional DuckDB analysis\n', '      - name: Test optional DuckDB analysis\n        if: false\n')));
  assert.throws(() => contract(source.replace('          test "$DUCKDB_RESULT" = success', '          true')));
  assert.throws(() => contract(source.replace('needs: [test, policies, native, duckdb]', 'needs: [test, policies, native]')));
});
