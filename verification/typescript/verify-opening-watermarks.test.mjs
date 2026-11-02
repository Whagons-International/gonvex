import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { extractOpeningWatermarks, watermarkMutations } from './verify-opening-watermarks.mjs';
import { extractReadyPublication, extractWatermarkPublication, publicationMutations } from './watermark-publication.mjs';

const source = readFileSync(new URL('../../packages/client/src/index.ts', import.meta.url), 'utf8');
const original = extractOpeningWatermarks(source);
const publication = extractWatermarkPublication(source);
test('extracts the actual post-await watermark publication guard', () => {
  assert.match(publication.definition, /define-fun canPublish/);
  assert.equal(publication.freshActivationWrites, 3);
  for (const name of ['scope', 'currentScope', 'generation', 'currentGeneration', 'activation', 'currentActivation', 'closed', 'connected'])
    assert(publication.definition.includes(name), name);
});
for (const [name, before, after] of publicationMutations) test(`translates changed post-await guard for ${name}`, () => {
  assert.equal(publication.source.split(before).length, 2);
  const method = publication.source.replace(before, after);
  assert.notEqual(extractWatermarkPublication(source.replace(publication.source, method)).definition, publication.definition);
});
for (const [name, before, after] of [
  ['late owner capture', 'const scope = this.replicaScope;', 'let scope = this.replicaScope;'],
  ['rerouted storage', 'advanceWatermark(revision, eligibleSignatures, scope)', 'advanceWatermark(revision, eligibleSignatures, this.replicaScope)'],
  ['unawaited storage', 'await this.replica.advanceWatermark', 'void this.replica.advanceWatermark'],
  ['missing guard', '|| this.manuallyClosed || !this.isWebSocketConnected) return;', '|| this.manuallyClosed || !this.isWebSocketConnected) {}'],
  ['early publication', 'await this.replica.advanceWatermark(revision, eligibleSignatures, scope);', 'this.processedReplicaWatermarkRevision = revision; await this.replica.advanceWatermark(revision, eligibleSignatures, scope);'],
  ['extra publication await', 'await this.retireReconciledReceipts();', 'await Promise.resolve(); await this.retireReconciledReceipts();'],
]) test(`refuses ${name} in the watermark publication path`, () => {
  assert.equal(publication.source.split(before).length, 2);
  const method = publication.source.replace(before, after);
  assert.throws(() => extractWatermarkPublication(source.replace(publication.source, method)));
});
test('refuses a reused receipt activation on scope reset', () => {
  const before = 'this.receiptReconciliationGeneration = Symbol();';
  assert(source.includes(before));
  assert.throws(() => extractWatermarkPublication(source.replaceAll(before, 'this.receiptReconciliationGeneration = this.socketGeneration;')));
});
test('binds the ready certificate to the captured owner after its sole persistence await', () => {
  assert.match(extractReadyPublication(source).guard, /subscription\.id !== publicationId/);
});
for (const [name, before, after] of [
  ['missing ready guard', publication.readyPublication.guard, 'if (false) return;'],
  ['new ready owner capture', 'const publicationId = subscription.id;', 'let publicationId = subscription.id;'],
  ['new ready scope', '...window, scope, rows:', '...window, scope: this.replicaScope, rows:'],
  ['unawaited ready write', 'await this.replica.replaceWindow(', 'void this.replica.replaceWindow('],
]) test(`refuses ${name}`, () => {
  const method = publication.readyPublication.source;
  assert.equal(method.split(before).length, 2);
  assert.throws(() => extractReadyPublication(source.replace(method, method.replace(before, after))));
});
test('extracts the actual deferred-watermark owner, opening and blocker guards', () => {
  assert.match(original.definition, /define-fun canDrainOne/);
  for (const name of ['scope', 'generation', 'closed', 'connected', 'opening', 'registered', 'listeners', 'readyEpoch', 'readyRevision'])
    assert(original.definition.includes(name), name);
  assert.match(original.reset, /unconditionally clears readyCursor/);
});

for (const [name, before, after] of watermarkMutations) test(`translates changed source for ${name} rather than a copied canonical guard`, () => {
  assert.equal(original.drainSource.split(before).length, 2);
  const method = original.drainSource.replace(before, after);
  const changed = extractOpeningWatermarks(source.replace(original.drainSource, method));
  assert.notEqual(changed.definition, original.definition);
});

for (const [name, before, after, message] of [
  ['retained readiness', 'subscription.readyCursor = undefined;', '', /unconditionally clear/],
  ['conditional scope clear', 'subscription.readyCursor = undefined;', 'if (subscription.isUpToDate) subscription.readyCursor = undefined;', /unconditionally clear/],
  ['new opening predicate', 'subscription.opening || !subscription.isUpToDate', 'subscription.opening', /Expected values/],
  ['missing deferred check', 'if (!deferred) return;', '', /control flow changed/],
  ['lazy generator drain', 'async drainOpeningReplicaWatermark()', 'async *drainOpeningReplicaWatermark()', /assert/],
  ['early persistence', 'this.openingReplicaWatermark = undefined;\n    await this.persistReplicaWatermark(deferred.revision);',
    'await this.persistReplicaWatermark(deferred.revision);\n    this.openingReplicaWatermark = undefined;', /Expected values/],
]) test(`refuses ${name}`, () => {
  let candidate;
  if (name === 'new opening predicate') {
    candidate = source.replace(original.drainSource, original.drainSource.replace(before, after));
  } else candidate = source.replace(before, after);
  assert.notEqual(candidate, source);
  assert.throws(() => extractOpeningWatermarks(candidate), message);
});
