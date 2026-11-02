import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { extractQueryPublication, verifyQueryPublication } from './query-publication.mjs';

const source = readFileSync(new URL('../../packages/client/src/index.ts', import.meta.url), 'utf8');
test('query publication matches the complete guard and effect contract', () => {
  const result = verifyQueryPublication(source);
  assert.equal(Object.keys(result.methods).length, 7);
  assert.equal(result.checkedCacheAndResetStatements, 5);
  assert.equal(result.structuralControlsRejected.length, 14);
});
for (const [name, before, after] of [
  ['extra await before query callback', 'if (subscription.listeners.has(listener)) listener(outgoing);', 'await hidden(); if (subscription.listeners.has(listener)) listener(outgoing);'],
  ['unknown owner side effect', 'const outboxGeneration = this.outboxScopeGeneration;\n    const socketGeneration', 'hidden(); const outboxGeneration = this.outboxScopeGeneration;\n    const socketGeneration'],
  ['extra cache microtask await', 'queueMicrotask(() => {\n          if (current() && existing.publishedMessage', 'queueMicrotask(async () => {\n          await hidden(); if (current() && existing.publishedMessage'],
  ['unknown publication target', 'subscription.publishedMessage = message;', 'hidden.publishedMessage = message;'],
]) test('rejects unsupported query publication: ' + name, () => {
  assert.equal(source.split(before).length, 2);
  assert.throws(() => extractQueryPublication(source.replace(before, after)));
});
