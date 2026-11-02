import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';

const ts = createRequire(new URL('../../packages/gonvex/package.json', import.meta.url))('typescript-api');
assert.equal(ts.version, '6.0.3');
const printer = ts.createPrinter({ removeComments: true });
const hash = value => createHash('sha256').update(value).digest('hex');
const contract = readFileSync(new URL('./query-publication-contract.ts.txt', import.meta.url), 'utf8');
function parse(source, name) {
  const file = ts.createSourceFile(name + '.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  assert.equal(file.parseDiagnostics.length, 0);
  const classes = file.statements.filter(n => ts.isClassDeclaration(n) && n.name?.text === name);
  assert.equal(classes.length, 1);
  const methods = new Map(classes[0].members.filter(ts.isMethodDeclaration).map(n => [n.name.getText(file), n]));
  return { file, methods };
}
const compact = (node, file) => printer.printNode(ts.EmitHint.Unspecified, node, file).replace(/\s/g, '');
export function extractQueryPublication(source) {
  const actual = parse(source, 'GonvexClient'), expected = parse(contract, 'PublicationContract');
  const methods = {};
  for (const [name, node] of expected.methods) {
    assert(actual.methods.has(name), name);
    assert.equal(compact(actual.methods.get(name), actual.file), compact(node, expected.file), 'query publication contract changed: ' + name);
    methods[name] = actual.methods.get(name).getText(actual.file);
  }
  const snippets = [
    ['subscribeLiveQuery', 'const cached = existing.publishedMessage;'],
    ['subscribeLiveQuery', `const current = this.captureQueryPublication(existing, existing.scope ?? this.replicaScope, cached.id);
      queueMicrotask(() => {
        if (current() && existing.publishedMessage === cached && existing.listeners.has(onMessage)) {
          onMessage(this.materializeQueryMessage(existing, cached));
        }
      });`],
    ['subscribeReplicaTransport', `const cached = existing.lastMessage;`],
    ['subscribeReplicaTransport', `const current = this.captureReplicaPublication(existing, existing.scope ?? this.replicaScope, cached.id);
      queueMicrotask(() => {
        if (current() && existing.lastMessage === cached && existing.listeners.has(onMessage)) {
          onMessage(metadataOnly ? cached : this.materializeReplicaMessage(existing, cached));
        }
      });`],
    ['resetReplicaScopeState', `for (const subscription of this.querySubscriptions.values()) {
      subscription.lastMessage = undefined;
      subscription.publishedMessage = undefined;
      subscription.publication = undefined;
      subscription.lastRevision = undefined;
      subscription.revisionSocketGeneration = undefined;
      subscription.serverSettled = false;
    }`],
  ];
  for (const [name, snippet] of snippets) {
    assert(actual.methods.has(name));
    const nodes = [];
    const visit = node => { nodes.push(compact(node, actual.file)); ts.forEachChild(node, visit); };
    visit(actual.methods.get(name));
    // Match complete statements or a contiguous sequence of complete statements.
    const fixture = ts.createSourceFile('snippet.ts', snippet, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
    assert.equal(fixture.parseDiagnostics.length, 0);
    const expectedNodes = fixture.statements.map(node => compact(node, fixture));
    const blocks = [];
    const find = node => { if (ts.isBlock(node)) blocks.push(node.statements.map(n => compact(n, actual.file))); ts.forEachChild(node, find); };
    find(actual.methods.get(name));
    assert(blocks.some(block => block.some((_, start) => expectedNodes.every((value, offset) => block[start + offset] === value))), 'cache or reset contract changed: ' + name);
  }
  return { methods, checkedCacheAndResetStatements: snippets.length,
    boundary: 'Closed AST correspondence for seven production methods and complete cache/reset statements. Captures opaque owner identities before yielding, checks them at storage completion and before every callback, orders query publication after persistence, and separates received patch bases from published cache. Reuses the verified ownsCurrentScope decision. This is structural guard/effect correspondence, not a new scheduler theorem, whole-program proof, native storage proof or backend authorization proof.' };
}
export function verifyQueryPublication(source) {
  const extracted = extractQueryPublication(source);
  const controls = [
    ['owner-captured-after-completion', 'const scope = this.replicaScope;\n    const activation = this.receiptReconciliationGeneration;\n    const outboxGeneration', 'const scope = this.replicaScope;\n    const activation = Symbol();\n    const outboxGeneration'],
    ['ignore-owner-close', 'this.receiptReconciliationGeneration, this.manuallyClosed)', 'this.receiptReconciliationGeneration, false)'],
    ['ignore-reconnect', ' && socketGeneration === this.socketGeneration;', ';'],
    ['ignore-query-routing', '&& subscription.id === id && scope === this.replicaScope && subscription.scope === scope;', '&& scope === this.replicaScope && subscription.scope === scope;'],
    ['ignore-replica-verification', '&& generation === subscription.verificationGeneration;', ';'],
    ['publish-before-storage', 'await this.materializeLiveQuery(subscription, message, scope);', 'void this.materializeLiveQuery(subscription, message, scope);'],
    ['ignore-failed-storage', 'if (current()) this.requestSubscriptionSnapshot(subscription);\n          return;', 'if (current()) this.requestSubscriptionSnapshot(subscription);'],
    ['query-listener-once', 'if (!current()) return;\n        if (subscription.listeners.has(listener))', 'if (subscription.listeners.has(listener))'],
    ['replica-listener-once', 'if (!current()) return;\n      if (!subscription.listeners.has(listener))', 'if (!subscription.listeners.has(listener))'],
    ['late-query-reads-wire-cache', 'const cached = existing.publishedMessage;', 'const cached = existing.lastMessage;'],
    ['old-query-microtask', 'current() && existing.publishedMessage === cached && existing.listeners.has(onMessage)', 'existing.listeners.has(onMessage)'],
    ['old-replica-microtask', 'current() && existing.lastMessage === cached && existing.listeners.has(onMessage)', 'existing.listeners.has(onMessage)'],
    ['bypass-query-order', 'const publication = previous ? previous.catch(() => undefined).then(publish) : publish();', 'const publication = publish();'],
    ['fresh-owner-keeps-old-revision', 'subscription.lastRevision = undefined;\n      subscription.revisionSocketGeneration = undefined;', ''],
  ];
  for (const [name, before, after] of controls) {
    assert.equal(source.split(before).length, 2, name);
    assert.throws(() => extractQueryPublication(source.replace(before, after)), undefined, name);
  }
  return { ...extracted, sourceSha256: hash(source), verifierSha256: hash(readFileSync(new URL(import.meta.url))),
    contractSha256: hash(contract), structuralControlsRejected: controls.map(([name]) => name) };
}
