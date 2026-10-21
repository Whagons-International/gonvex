import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { createRuntimeGlobals } from '../runtime-contract/globals.js';
import { contract } from '../runtime-contract/runner.js';
import { runtimeDate, runtimeMath, withReducerRuntimeGlobals } from '../dist/runtime-globals.js';

const suite = JSON.parse(await readFile(new URL('../runtime-contract/vectors.json', import.meta.url)));
test('published crypto cases agree with Node WebCrypto independently of the Rust host', async () => {
  for (const vector of suite.cases.filter(vector => ['digest', 'signature', 'cached-key', 'aes', 'crypto-errors', 'base64'].includes(vector.run))) {
    assert.deepEqual(await contract({}, vector), vector.expected, vector.id);
  }
});

test('local and isolate globals use the published clock and entropy cases without modifying process globals', async () => {
  const originalDate = globalThis.Date, originalRandom = Math.random;
  for (const vector of suite.cases.filter(vector => ['query', 'reducer'].includes(vector.kind))) {
    const invocation = { kind: vector.kind, now: vector.now ?? 1792324800123, intentEntropy: vector.intentEntropy };
    const globals = createRuntimeGlobals(() => invocation);
    if (vector.run === 'clock') {
      const Date = globals.Date;
      class ChildDate extends Date {}
      assert.ok(Number.isNaN(Date.prototype.getTime()));
      assert.deepEqual({ now: Date.now(), constructed: +new Date(), called: Date() === new Date(invocation.now).toString(), explicit: +new Date(1234), utc: Date.UTC(1970, 0, 1), parsed: Date.parse('1970-01-01T00:00:00.000Z'), subclass: +new ChildDate(), instance: new Date() instanceof Date, constructorNow: new Date().constructor.now() }, vector.expected, vector.id);
    } else if (vector.run === 'random') assert.deepEqual([globals.Math.random(), globals.Math.random(), globals.Math.random()], vector.expected, vector.id);
    else if (vector.run === 'random-denied') assert.throws(() => globals.Math.random(), /intentEntropy/);
  }
  await assert.rejects(withReducerRuntimeGlobals({ now: 1234 }, async () => {
    await Promise.resolve();
    assert.equal(runtimeDate.now(), 1234);
    assert.equal(globalThis.Date, originalDate);
    assert.equal(Math.random, originalRandom);
    throw new Error('handler failed');
  }), /handler failed/);
  assert.throws(() => runtimeDate.now(), /active runtime invocation/);
  assert.throws(() => runtimeMath.random(), /active runtime invocation/);
});

test('overlapping generated invocations keep their clock across awaits and recover after failure', async () => {
  const first = withReducerRuntimeGlobals({ now: 1234 }, async () => {
    const start = runtimeDate.now();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(runtimeDate.now(), start);
    throw new Error('first intent failed');
  });
  const second = withReducerRuntimeGlobals({ now: 5678 }, async () => {
    const start = runtimeDate.now();
    await new Promise(resolve => setImmediate(resolve));
    return [start, runtimeDate.now()];
  });
  await assert.rejects(first, /first intent failed/);
  assert.deepEqual(await second, [5678, 5678]);
  assert.throws(() => runtimeDate.now(), /active runtime invocation/);
});
