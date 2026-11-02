import { extractStreamGenerations } from './stream-generations.mjs';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { extract } from './verify-ownership.mjs';
import { extractGenerations, extractAuthGenerations } from './owner-generations.mjs';
const source = readFileSync(new URL('../../packages/client/src/ownership-policy.ts', import.meta.url), 'utf8');
test('extracts the actual ownership decision with opaque activation equality', () => {
  const result = extract(source);
  assert.match(result, /= capturedGeneration currentGeneration/);
  assert.doesNotMatch(result, /FloatingPoint|fp\.eq/);
  assert.match(result, /= capturedScope currentScope/);
});
for (const [name, before, after] of [
  ['extra declaration', 'export function', 'const hidden = true; export function'],
  ['extra statement', 'return !closed', 'console.log(closed); return !closed'],
  ['wrong parameter domain', 'capturedScope: string', 'capturedScope: number'],
  ['numeric activation', 'capturedGeneration: symbol', 'capturedGeneration: number'],
  ['default parameter', 'closed: boolean', 'closed: boolean = false'],
  ['coercive equality', 'capturedScope ===', 'capturedScope =='],
  ['wrong comparison sorts', 'capturedScope === currentScope', 'capturedScope === closed'],
  ['unknown call', '!closed', 'Boolean(closed)'],
  ['type assertion', '!closed', '(!closed as boolean)'],
  ['unknown identifier', '!closed', '!hidden'],
]) test(`rejects ${name}`, () => {
  const candidate = source.replace(before, after); assert.notEqual(candidate, source);
  assert.throws(() => extract(candidate));
});
const client = readFileSync(new URL('../../packages/client/src/index.ts', import.meta.url), 'utf8');
test('finds fresh initialization and every owner invalidation site', () => {
  const result = extractGenerations(client);
  assert.equal(result.initialization, 'Symbol()');
  assert.equal(result.writes.length, 4);
});
for (const [name, before, after] of [
  ['numeric initializer', 'outboxScopeGeneration = Symbol()', 'outboxScopeGeneration = 0'],
  ['global registry token', 'outboxScopeGeneration = Symbol()', 'outboxScopeGeneration = Symbol.for("shared")'],
  ['reused activation', 'this.outboxScopeGeneration = Symbol()', 'this.outboxScopeGeneration = this.outboxScopeGeneration'],
  ['numeric update', 'this.outboxScopeGeneration = Symbol()', 'this.outboxScopeGeneration += 1'],
  ['unexpected invalidation site', '  close() {', '  unexpectedReset() { this.outboxScopeGeneration = Symbol(); }\n  close() {'],
  ['missing invalidation', '    this.outboxScopeGeneration = Symbol();', ''],
]) test(`rejects ${name}`, () => {
  const candidate = client.replace(before, after); assert.notEqual(candidate, client);
  assert.throws(() => extractGenerations(candidate));
});

test('extracts actual retirement effects and the originating journal target', async () => {
  const { extractRetirement } = await import('./owner-retirement.mjs');
  const effects = extractRetirement(client);
  assert.equal(effects.target, 'originalId');
  assert.equal(effects.clears, '(and current (and mapPresent (= mappedId originalId)))');
});
for (const [name, before, after] of [
  ['extra awaited operation', 'await this.reducerOutbox.ack(entry.id);', 'await this.reducerOutbox.ack(entry.id); await hidden();'],
  ['side effect in guard', 'current() && this.optimisticOutboxEntryIds.get(entry.idempotencyKey) === entry.id', 'hidden() && current()'],
  ['wrong cleared command key', 'this.optimisticOutboxEntryIds.delete(entry.idempotencyKey);', 'this.optimisticOutboxEntryIds.delete("other");'],
  ['unknown retirement target', 'await this.reducerOutbox.ack(entry.id);', 'await this.reducerOutbox.ack(hidden());'],
  ['wrong acceptance cleanup target', 'this.knownAcceptedEntries.delete(entry.id);', 'this.knownAcceptedEntries.delete(hidden());'],
  ['acceptance cleanup before durable ACK', 'await this.reducerOutbox.ack(entry.id);\n    this.knownAcceptedEntries.delete(entry.id);', 'this.knownAcceptedEntries.delete(entry.id);\n    await this.reducerOutbox.ack(entry.id);'],
  ['else effect', 'await this.reducerOutbox.ack(entry.id);', 'else { hidden(); } await this.reducerOutbox.ack(entry.id);'],
]) test(`rejects unsupported retirement ${name}`, async () => {
  const { extractRetirement } = await import('./owner-retirement.mjs');
  const method = extractRetirement(client).source;
  assert.equal(method.split(before).length, 2);
  const candidate = client.replace(method, method.replace(before, after));
  assert.throws(() => extractRetirement(candidate));
});

test('retirement fallback reads the map after guarded cleanup', async () => {
  const { extractRetirement } = await import('./owner-retirement.mjs');
  const method = extractRetirement(client).source;
  const changed = method.replace('await this.reducerOutbox.ack(entry.id);',
    'await this.reducerOutbox.ack(this.optimisticOutboxEntryIds.get(entry.idempotencyKey) ?? entry.id);');
  assert.notEqual(changed, method);
  assert.equal(extractRetirement(client.replace(method, changed)).target,
    '(ite (and mapPresent (not clears)) mappedId originalId)');
});

test('auth activations allocate fresh identities at every installation/fetch boundary', () => {
  const result = extractAuthGenerations(client);
  assert.equal(result.initialization, 'Symbol()');
  assert.equal(result.writes.length, 7);
});
for (const [name, before, after] of [
  ['numeric auth initializer', 'authFetchGeneration = Symbol()', 'authFetchGeneration = 0'],
  ['counter auth invalidation', 'this.authFetchGeneration = Symbol();', 'this.authFetchGeneration += 1;'],
  ['reused auth identity', 'this.authFetchGeneration = Symbol();', 'this.authFetchGeneration = this.authFetchGeneration;'],
  ['registry auth identity', 'this.authFetchGeneration = Symbol();', 'this.authFetchGeneration = Symbol.for("auth");'],
]) test(`rejects ${name}`, () => {
  const candidate = client.replace(before, after); assert.notEqual(candidate, client);
  assert.throws(() => extractAuthGenerations(candidate));
});

test('stream allocation and capture boundaries use opaque activation identities', () => {
  const result = extractStreamGenerations(client);
  assert.equal(result.initialization, 'Symbol()'); assert.equal(result.allocations.length, 9);
});
for (const [name, before, after] of [
  ['numeric socket birth', 'private socketGeneration = Symbol()', 'private socketGeneration = 0'],
  ['socket increment', 'this.socketGeneration = Symbol();', '++this.socketGeneration;'],
  ['verification increment', 'subscription.verificationGeneration = Symbol();', 'subscription.verificationGeneration += 1;'],
  ['reused verification token', 'subscription.verificationGeneration = Symbol();', 'subscription.verificationGeneration = subscription.verificationGeneration;'],
  ['reset increment', 'subscription.resetGeneration = Symbol();', 'subscription.resetGeneration = (subscription.resetGeneration ?? 0) + 1;'],
  ['false authenticated token', 'this.authenticatedSocketGeneration = undefined;', 'this.authenticatedSocketGeneration = 0;'],
]) test(`rejects ${name}`, () => {
  const candidate = client.replace(before, after); assert.notEqual(candidate, client);
  assert.throws(() => extractStreamGenerations(candidate));
});


test('pins the complete intent publication await and ownership bridge', async () => {
  const { verifyIntentPublication } = await import('./intent-publication.mjs');
  const result = verifyIntentPublication(client);
  assert.equal(result.structuralControlsRejected.length, 6);
});
