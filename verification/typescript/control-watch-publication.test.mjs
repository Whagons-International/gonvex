import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { extractControlWatch, verifyControlWatch } from './control-watch-publication.mjs';

const source = readFileSync(new URL('../../packages/client/src/index.ts', import.meta.url), 'utf8');
test('Control Query watch retains its complete owner and notification contract', () => {
  const result = verifyControlWatch(source);
  assert.equal(result.structuralControlsRejected.length, 14);
  assert.deepEqual(result.sessionScopeAllocations, ['initialization', 'resetReplicaScopeState']);
});
for (const [name, before, after] of [
  ['side effect during stale read', 'if (!ownsSnapshot() && (result !== undefined || error !== undefined)) clearSnapshot();', 'hidden(); if (!ownsSnapshot() && (result !== undefined || error !== undefined)) clearSnapshot();'],
  ['await before watch callback', 'if (listeners.has(listener)) listener();', 'await hidden(); if (listeners.has(listener)) listener();'],
]) test('rejects unsupported Control Query watch: ' + name, () => {
  assert.equal(source.split(before).length, 2);
  assert.throws(() => extractControlWatch(source.replace(before, after)));
});

test('preserves string literal content in the closed Control Query watch contract', () => {
  const method = extractControlWatch(source).method;
  const before = 'ref.scope !== "control"';
  assert.equal(method.split(before).length, 2);
  assert.throws(() => extractControlWatch(source.replace(method, method.replace(before, 'ref.scope !== " control"'))));
});
