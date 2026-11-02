import assert from "node:assert/strict";

export function queryPublicationControls(source) {
const replace = (code, before, after) => {
  assert.equal(code.split(before).length, 2, 'control must change exactly one source site: ' + before);
  return code.replace(before, after);
};
const inMethod = (name, before, after) => {
  const start = source.indexOf('  private ' + name);
  assert(start >= 0, name);
  const end = source.indexOf('\n  private ', start + 1);
  const method = source.slice(start, end < 0 ? undefined : end);
  return source.replace(method, replace(method, before, after));
};
return [
  { name: 'candidate', pattern: '.', passed: 27 },
  { name: 'query-listener-once', pattern: 'live query listener.*reentrant (account|tenant|return|close)$', failed: 4,
    candidate: inMethod('async publishQueryMessage(', '        if (!current()) return;\n        if (subscription.listeners.has(listener))', '        if (subscription.listeners.has(listener))'),
    failure: /old query message reached a later listener after owner invalidation/ },
  { name: 'query-cache-without-owner', pattern: 'cached live query replay.*(account|tenant|return|close)$', failed: 4,
    candidate: replace(source, 'current() && existing.publishedMessage === cached && existing.listeners.has(onMessage)', 'existing.listeners.has(onMessage)'),
    failure: /queued cache replay crossed its original owner/ },
  { name: 'received-cache-before-commit', pattern: '^late query listeners', failed: 1,
    candidate: replace(replace(source, 'const cached = existing.publishedMessage;', 'const cached = existing.lastMessage;'),
      'current() && existing.publishedMessage === cached', 'current() && existing.lastMessage === cached'),
    failure: /late listener received a query result before persistence/ },
  { name: 'publish-failed-write', pattern: '^query materialization failure', failed: 1,
    candidate: inMethod('async publishQueryMessage(', '          if (current()) this.requestSubscriptionSnapshot(subscription);\n          return;',
      '          if (current()) this.requestSubscriptionSnapshot(subscription);'),
    failure: /query listener received a result whose storage write failed/ },
  { name: 'query-scope-only', pattern: '^deferred query publication.*(return|close)$', failed: 2,
    candidate: inMethod('captureQueryPublication(', 'return () => current() && this.querySubscriptions.get(subscription.key) === subscription\n      && subscription.id === id && scope === this.replicaScope && subscription.scope === scope;',
      'return () => scope === this.replicaScope && subscription.scope === scope;'),
    failure: /old query completion published into a later activation/ },
  { name: 'replica-listener-once', pattern: '^replica listener publication.*reentrant (account|tenant|return|close)$', failed: 4,
    candidate: inMethod('emitReplicaMessage(', '      if (!current()) return;\n      if (!subscription.listeners.has(listener)) continue;', '      if (!subscription.listeners.has(listener)) continue;'),
    failure: /old replica message reached a later listener after owner invalidation/ },
  { name: 'replica-cache-without-owner', pattern: '^cached replica replay.*(account|tenant|return|close)$', failed: 4,
    candidate: replace(source, 'current() && existing.lastMessage === cached && existing.listeners.has(onMessage)', 'existing.listeners.has(onMessage)'),
    failure: /queued replica cache replay crossed its original owner/ },
  { name: 'keep-previous-owner-revision', pattern: 'live query listener.*reentrant (account|tenant)$', failed: 2,
    candidate: inMethod('resetReplicaScopeState(', '      subscription.lastRevision = undefined;\n      subscription.revisionSocketGeneration = undefined;\n', ''),
    failure: /fresh owner result did not publish/ },
  { name: 'bypass-publication-order', pattern: '^query publication preserves wire order', failed: 2,
    candidate: replace(source, 'const publication = previous ? previous.catch(() => undefined).then(publish) : publish();', 'const publication = publish();'),
    failure: /later query publication passed a preceding durable write/ },
];
}
