import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { auditAllocations } from '../dist/allocation-audit.js';

const rules = text => auditAllocations(text, 'hot.ts').map(f => f.rule);
test('finds copying accumulators, bulk copies and intermediate arrays', () => {
  assert.ok(rules('xs.reduce((acc, x) => [...acc, x], [])').includes('copying-accumulator'));
  assert.ok(rules('xs.reduce((acc, x) => ({...acc, [x.id]: x}), {})').includes('copying-accumulator'));
  assert.ok(rules('xs.map(x => ({...x, title: x.name}))').includes('bulk-row-copy'));
  assert.ok(rules('xs.filter(isActive).map(name)').includes('array-pipeline'));
  assert.deepEqual(rules('xs.map(name); xs.reduce((acc, x) => { acc.push(x); return acc; }, result)'), []);
});
test('distinguishes typed buffers from ordinary arrays and reports the retention tradeoff', () => {
  const findings = auditAllocations('function f(out: Uint8Array) { return out.slice(0, 2); }', 'hot.ts');
  assert.equal(findings[0].rule, 'buffer-slice');
  assert.match(findings[0].message, /retain the entire/);
  assert.deepEqual(rules('const a = [1, 2]; a.slice(); const b = new Uint8Array(2); b.subarray(0, 1);'), []);
});
test('finds repeated encoders and loop containers, leaving module singletons alone', () => {
  assert.deepEqual(rules('const encoder = new TextEncoder();'), []);
  assert.ok(rules('function encode() { return new TextEncoder(); }').includes('repeated-encoder'));
  assert.ok(rules('for (const x of xs) { work({x}); }').includes('loop-allocation'));
  assert.deepEqual(rules('for (const x of xs) { function lazy() { return {}; } }'), []);
  assert.deepEqual(rules('for (const x of [1, 2]) { use(x); } for (let out = []; ok; next()) { use(out); }'), []);
});
test('annotations opt in functions and exemptions require a named rule and reason', () => {
  assert.deepEqual(auditAllocations('const f = () => new TextEncoder();', 'ui.ts', false), []);
  assert.equal(auditAllocations('/** @allocation-hot */\nconst f = () => new TextEncoder();', 'ui.ts', false)[0].rule, 'repeated-encoder');
  assert.deepEqual(rules('// allocation-allow bulk-row-copy: editable drafts need ownership\nxs.map(x => ({ ...x }));'), []);
  assert.ok(rules('// allocation-allow bulk-row-copy:\nxs.map(x => ({ ...x }));').includes('bulk-row-copy'));
});
test('fingerprints survive line shifts but distinguish allocation expressions', () => {
  const first = auditAllocations('xs.map(x => ({...x}));', 'hot.ts')[0];
  const moved = auditAllocations('\n\nxs.map(x => ({...x}));', 'hot.ts')[0];
  assert.equal(first.fingerprint, moved.fingerprint);
  assert.equal(moved.line, 3);
});
test('reports deep copies in hot paths without treating ordinary disk decoding as cloning', () => {
  assert.deepEqual(rules('function clone(row) { return structuredClone(row); }'), ['deep-clone']);
  assert.deepEqual(rules('JSON.parse(JSON.stringify(row));'), ['deep-clone']);
  assert.deepEqual(rules('JSON.parse(record.value);'), []);
  assert.ok(auditAllocations('const view = <Widget rows={xs.map(x => ({...x}))} />;', 'hot.jsx').some(f => f.rule === 'bulk-row-copy'));
});
test('strict hot annotations reject per-call allocations and cannot leak to neighboring methods', () => {
  const strict = text => auditAllocations(text, 'hot.ts', false, true);
  for (const expression of ['{}', '[]', 'new Uint8Array(5)', 'Uint8Array.from(input)', 'Object.create(null)', 'JSON.stringify(row)', 'Object.values(row)', 'rows.map(identity)', 'rows.toSorted()', 'bytes.subarray(0, 2)', 'decoder.decode(bytes)', '() => row', '`row-${index}`', '/row/', '"row-" + index']) {
    assert.ok(strict(`/** @allocation-hot */\nfunction hot() { return ${expression}; }`).some(f => f.rule === 'hot-allocation'), expression);
  }
  assert.deepEqual(strict('function cold() { return {}; }'), []);
  assert.deepEqual(strict('/** @allocation-hot */\nfunction hot(out, index, value) { out[index] = value; return out; }'), []);
  assert.deepEqual(strict('class Window { /** @allocation-hot */\nget(i) { return this.rows[i]; }\ncold() { return new TextEncoder(); } }'), []);
  assert.deepEqual(strict('/** @allocation-hot */\nfunction hot() {\n// allocation-allow hot-allocation: invalid input only\nthrow new Error("bad");\n}'), []);
  assert.ok(strict('/** @allocation-hot */\nasync function hot() { return 1; }').length);
});
test('CLI emits JSON, warns by default, and only gates findings outside its baseline', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'allocation-audit-'));
  const file = join(dir, 'hot.ts'), baseline = join(dir, 'baseline.json');
  const run = (...args) => spawnSync(process.execPath, ['packages/gonvex/dist/index.js', 'perf', 'allocations', file, ...args], { encoding: 'utf8', cwd: new URL('../../..', import.meta.url) });
  try {
    await writeFile(file, 'xs.map(x => ({...x}));');
    const warning = run('--json', '--write-baseline', baseline);
    assert.equal(warning.status, 0, warning.stderr);
    assert.equal(JSON.parse(warning.stdout).newFindings, 1);
    assert.equal(JSON.parse(run('--json', '--marked-only').stdout).newFindings, 0);
    assert.equal(run('--baseline', baseline, '--check').status, 0);
    await writeFile(file, 'xs.map(x => ({...x}));\nfunction f() { return new TextEncoder(); }');
    assert.equal(run('--baseline', baseline, '--check').status, 1);
    await writeFile(file, '/** @allocation-hot */\nfunction f() { return new TextEncoder(); }');
    assert.equal(JSON.parse(run('--json', '--marked-only').stdout).newFindings, 1);
    assert.notEqual(run('--unexpected').status, 0);
    await writeFile(file, '/** @allocation-hot */\nfunction hot() { return {}; }');
    assert.equal(run('--strict-hot', '--check').status, 1);
    assert.match(run('--strict-hot', '--baseline', baseline).stderr, /does not accept a baseline/);
    assert.match(run('--strict-hot', '--write-baseline', baseline).stderr, /does not accept a baseline/);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
