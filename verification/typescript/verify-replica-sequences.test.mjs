import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { extractReplicaSequence, extractStartupSequenceGate, sequenceMutations, extractSequenceIntegrity, extractSequenceCallSites } from './verify-replica-sequences.mjs';

for (const [file, cls] of [['indexeddb-replica.ts', 'IndexedDBLocalReplicaStorage'], ['indexeddb-engine.ts', 'IndexedDBEngineReplicaStorage']]) {
  const source = readFileSync(new URL('../../packages/client/src/' + file, import.meta.url), 'utf8');
  const original = extractReplicaSequence(source, cls);
  test(`${file}: extracts the actual counter admission and result`, () => {
    assert.match(original.definition, /safe previous finite/);
    assert.match(original.definition, /\(\+ previous 1\)/);
  });
  test(`${file}: validates six public readers and five writers in their storage transactions`, () => {
    const sites = extractSequenceCallSites(source, cls);
    assert.equal(sites.length, 11); assert(sites.every(site => site.validatedFirst));
  });
  for (const [name, before, after] of [
    ['missing writer integrity validation', 'await assertReplicaSequence(this.database, scope, prior, previousSequence);', ''],
    ['untrusted imported validator', 'assertReplicaSequence, readReplicaSequence', 'other as assertReplicaSequence, readReplicaSequence'],
  ]) test(`${file}: refuses ${name}`, () => {
    const candidate = source.replace(before, after); assert.notEqual(candidate, source);
    assert.throws(() => extractReplicaSequence(candidate, cls));
  });
  for (const [name, before, after] of [
    ['row read without counter validation', 'await readReplicaSequence(this.database,scope);', ''],
    ['watermark without entity table', 'transaction("rw", this.database.entities, this.database.windows', 'transaction("rw", this.database.windows'],
    ['validated a different owner', 'const sequence=await readReplicaSequence(this.database,scope);', "const sequence=await readReplicaSequence(this.database,'other');"],
    ['read validation omitted from import', 'assertReplicaSequence, readReplicaSequence', 'assertReplicaSequence, other as readReplicaSequence'],
    ['snapshot publishes an unchecked position', '      storageSequence,', '      storageSequence: 0,'],
    ['peer reader publishes an unchecked position', '{sequence,reset,entities:', '{sequence:0,reset,entities:'],
  ]) test(`${file}: refuses integrity bridge with ${name}`, () => {
    const candidate = source.replace(before, after); assert.notEqual(candidate, source);
    assert.throws(() => extractSequenceCallSites(candidate, cls));
  });
  for (const [name, before, after] of sequenceMutations) test(`${file}: translates ${name} from changed source`, () => {
    const method = original.method.replace(before, after); assert.notEqual(method, original.method);
    const candidate = extractReplicaSequence(source.replace(original.method, method), cls);
    assert.notEqual(candidate.definition, original.definition);
  });
  for (const [name, before, after] of [
    ['lazy generator', 'async nextSequence(', 'async *nextSequence('],
    ['silent failure', 'throw new Error(', 'return new Error('],
    ['unchecked increment', 'const sequence=previousSequence+1;', 'const sequence=Math.ceil(previousSequence)+1;'],
    ['extra write', 'return sequence;', "await this.database.meta.clear();return sequence;"],
  ]) test(`${file}: refuses ${name}`, () => {
    const candidate = source.replace(original.method, original.method.replace(before, after));
    assert.notEqual(candidate, source); assert.throws(() => extractReplicaSequence(candidate, cls));
  });
}

const integrity = readFileSync(new URL('../../packages/client/src/indexeddb-sequence.ts', import.meta.url), 'utf8');
test('extracts the actual counter integrity policy and canonical scoped read bridge', () => {
  const extracted = extractSequenceIntegrity(integrity);
  assert.match(extracted.definition, /safe sequence sequenceFinite/);
  assert.match(extracted.definition, /<= entity sequence/);
  assert.equal(extracted.sourceBridge.scopedIndexReads, 2);
});
for (const [name, before, after] of [
  ['encoding coerces corrupt metadata', "typeof record.value === 'string'", 'true'],
  ['reset omitted from coverage', 'entitySequence, windowSequence, resetSequence', 'entitySequence, windowSequence, 0'],
  ['row payload read for integrity', '.reverse().limit(1).keys()', '.last()'],
  ['unbounded index read', '.limit(1)', '.limit(Infinity)'],
  ['index reads different owner', '[scope, -Infinity]', "['other', -Infinity]"],
  ['current value returned before validation', '  await assertReplicaSequence(database, scope, prior, sequence);\n  return sequence;', '  return sequence;'],
  ['global runtime binding', "import type { IndexableType } from 'dexie';", "import type { IndexableType } from 'dexie';\nconst Number = globalThis.Number;"],
  ['truthy numeric admission', 'Number.isSafeInteger(entity)', 'Boolean(entity)'],
  ['coercive comparison', 'entity <= sequence', '(entity as any) <= sequence'],
]) test(`counter integrity refuses ${name}`, () => {
  const candidate = integrity.replace(before, after); assert.notEqual(candidate, integrity);
  assert.throws(() => extractSequenceIntegrity(candidate));
});

const startup = readFileSync(new URL('../../packages/client/src/local-replica.ts', import.meta.url), 'utf8');
test('startup capture is checked before scope publication and settled handles are owned', () => {
  const result = extractStartupSequenceGate(startup);
  assert.equal(result.predicate, 'isReplicaRevision');
  assert.equal(result.publication.length, 4);
  assert.equal(result.activationCounter.initialization, '0');
  assert.deepEqual(result.activationCounter.increments, ['activateScope:prefix', 'dispose:postfix']);
});
for (const [name, before, after] of [
  ['missing validation', 'if (!isReplicaRevision(storageSequence)) throw new Error("Invalid replica storage sequence.");', ''],
  ['validated a different value', '!isReplicaRevision(storageSequence)', '!isReplicaRevision(0)'],
  ['reread after validation', 'this.storageSequence = storageSequence;', 'this.storageSequence = snapshot?.storageSequence ?? 0;'],
  ['made corrupt scope readable', 'this.scopeLoaded = true;', 'this.scopeLoaded = false;'],
  ['missing cleanup identity', 'if (this.scopeActivation !== activation) return;', ''],
  ['cleanup only old handle', 'this.scopeActivation !== activation', 'this.scopeActivation === activation'],
  ['retain rejected handle', 'activation.then(clearActivation, clearActivation)', 'activation.then(clearActivation)'],
]) test(`startup refuses ${name}`, () => {
  const candidate = startup.replace(before, after); assert.notEqual(candidate, startup);
  assert.throws(() => extractStartupSequenceGate(candidate));
});

for (const value of ['true', 'false']) test(`translates changed activation admission ${value} for solver checks`, () => {
  const candidate = startup.replace('!isTransactionRevision(generation)', value);
  assert.notEqual(candidate, startup);
  assert.equal(extractStartupSequenceGate(candidate).allocationRejection, value);
});
const allocationGuard = 'if (!isTransactionRevision(generation)) return Promise.reject(new Error("Replica scope activation sequence exhausted."));';
const generation = 'const generation = ++this.scopeActivationGeneration;';
const earlyCacheClear = generation + '\n    this.scopeActivation = undefined;\n    this.scopeActivationName = undefined;';
for (const [name, change] of [
  ['missing allocation guard', source => source.replace(allocationGuard, '')],
  ['missing listener fence', source => source.replace('if (generation !== this.scopeActivationGeneration) return Promise.resolve();', '')],
  ['late identity capture', source => source.replace(generation, '').replace('const activation = this.enqueueApplication', generation + '\n    const activation = this.enqueueApplication')],
  ['counter reset', source => source.replace(generation, 'this.scopeActivationGeneration = 0;\n    ' + generation)],
  ['counter decrement', source => source.replace('this.scopeActivationGeneration++;', 'this.scopeActivationGeneration--;')],
  ['postfix capture', source => source.replace(generation, 'const generation = this.scopeActivationGeneration++;')],
  ['failed allocation leaves old scope readable', source => source.replace(allocationGuard, '').replace('this.scopeLoaded = false;', allocationGuard + '\n    this.scopeLoaded = false;')],
  ['superseded handle remains cached', source => source.replace(earlyCacheClear, generation)],
  ['closed replica can coalesce an old handle', source => source.replace(
    'activateScope(scope: ReplicaScope = defaultReplicaScope, forceReload = false, restoreFromStorage = true): Promise<void> {\n    if (this.disposed) return Promise.reject(new Error("Local replica is closed"));',
    'activateScope(scope: ReplicaScope = defaultReplicaScope, forceReload = false, restoreFromStorage = true): Promise<void> {')],
  ['dispose retains the old handle', source => source.replace('this.scopeActivationGeneration++;\n    this.scopeActivation = undefined;\n    this.scopeActivationName = undefined;', 'this.scopeActivationGeneration++;')],
]) test(`startup refuses ${name}`, () => {
  const candidate = change(startup); assert.notEqual(candidate, startup);
  assert.throws(() => extractStartupSequenceGate(candidate));
});
