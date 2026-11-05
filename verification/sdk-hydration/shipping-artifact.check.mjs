// Prepared for a SEPARATELY GRANTED artifact verification job. Not run by
// ordinary source Vitest discovery; no fallback to packages/client/src.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFileSync, realpathSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { join } from 'node:path';

const root = process.env.GONVEX_COMPOSED_SDK_ROOT;
assert(root, 'explicit isolated composed-artifact root required');
assert(realpathSync(root).startsWith('/mnt/data/work/'), 'never load mutable shipping source or a plain main SDK');
const provenance = JSON.parse(readFileSync(join(root, 'composition-provenance.json'), 'utf8'));
const js = readFileSync(join(root, 'dist/index.js'));
assert.equal(provenance.baselineClientJsSha256, '4f9ce8be47c21742b49d760ebf0c695da13ab4c2f814d6df5a7d843a416fa2b4');
assert.equal(provenance.existingPatchSha256, '06786316f912c262f7638b8f3c82cff02a8787f71eabcafca84b4087a3333334');
assert.equal(provenance.coreSourceCommit, '728b857e456d6d7a9f299883fdf1ba17c8e594b9');
assert.equal(createHash('sha256').update(js).digest('hex'), provenance.composedClientJsSha256);
assert.notEqual(provenance.composedClientJsSha256, provenance.baselineClientJsSha256);
const { GonvexClient, MemoryLocalReplicaStorage } = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const { LocalReplica } = await import(pathToFileURL(join(root, 'dist/local-replica.js')).href);
const { replicaRowsHashes, replicaAggregateDigest } = await import(pathToFileURL(join(root, 'dist/replica-integrity.js')).href);
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };

async function fixture() {
  const storage = new MemoryLocalReplicaStorage();
  const seed = new LocalReplica(storage); await seed.activateScope('visible');
  const refs = ['empty', 'populated'].map(name => ({ kind: 'query', path: `tasks.${name}`, delivery: 'replica', replica: { table: 'tasks', key: 'id', columns: ['id'] } }));
  const expected = new Map();
  for (const [i, ref] of refs.entries()) {
    const key = `${ref.path}\0{}\0${JSON.stringify({ delivery: 'replica', live: null, scope: 'tenant' })}`;
    const rows = i ? [{ id: 'a' }] : [], hashes = await replicaRowsHashes(rows, 'id'), cursor = { epoch: 'e', revision: 17 };
    await seed.replaceWindow({ signature: key, kind: 'replica', entity: 'tasks', key: 'id', rows, hashes, cursor, source: 'server', completeness: 'complete' });
    expected.set(ref.path, { cursor, digest: replicaAggregateDigest(hashes) });
  }
  const entered = deferred(), release = deferred(), load = storage.load.bind(storage);
  storage.load = async scope => { if (scope === 'visible') { entered.resolve(); await release.promise; } return load(scope); };
  const client = new GonvexClient('ws://artifact-only.invalid/ws', { localReplica: { storage }, errorReporting: false, deferReplicaOpen: () => true });
  client.connect = () => {}; // Explicit transport sink, never an endpoint.
  await client.replicaReady;
  client.replicaScope = 'visible'; client.hasAuthoritativeReplicaScope = true; client.isWebSocketConnected = true;
  client.serverCapabilities = { replicaBatch: 1, replicaResume: 1 };
  const frames = []; client.send = message => frames.push(message);
  client.setReplicaReady(client.replica.activateScope('visible')); await entered.promise;
  const stops = refs.map(ref => client.subscribeReplica(ref, {}, () => {}));
  assert.equal(client.deferredReplicaOpens.length, 2, 'actual shipping overlay must hold the subscriptions');
  const waiters = () => Promise.all([...client.replicaOpenHydrations.values()].map(entry => entry.task));
  const flush = () => { clearTimeout(client.replicaOpenFlushTimer); client.flushReplicaOpens(); };
  const opens = () => frames.flatMap(frame => frame.type === 'replica.open' ? [frame] : frame.type === 'replica.openMany' ? frame.opens : []);
  return { client, refs, stops, release, expected, waiters, flush, opens };
}

for (const when of ['before-hydration', 'after-hydration']) test(`actual defer/release preserves checkpoints: ${when}`, async () => {
  const f = await fixture();
  try {
    if (when === 'after-hydration') { f.release.resolve(); await f.client.replicaReady; }
    f.client.releaseDeferredReplicaOpens();
    const waiting = f.waiters();
    if (when === 'before-hydration') { f.flush(); assert.equal(f.opens().length, 0); f.release.resolve(); }
    await waiting; f.flush();
    assert.equal(f.opens().length, 2);
    for (const open of f.opens()) { assert.deepEqual(open.cursor, f.expected.get(open.path).cursor); assert.equal(open.digest, f.expected.get(open.path).digest); }
    f.client.releaseDeferredReplicaOpens(); f.flush(); assert.equal(f.opens().length, 2, 'release is single-flight');
  } finally { f.release.resolve(); f.client.close(); }
});

test('actual deferred unsubscribe cancels only its own intent', async () => {
  const f = await fixture();
  try { f.stops[0](); f.client.releaseDeferredReplicaOpens(); const waiting = f.waiters(); f.release.resolve(); await waiting; f.flush(); assert.equal(f.opens().length, 1); assert.equal(f.opens()[0].path, f.refs[1].path); }
  finally { f.release.resolve(); f.client.close(); }
});

for (const retire of ['close', 'scope-failure', 'scope-replacement']) test(`deferred requests cannot resurrect after ${retire}`, async () => {
  const f = await fixture();
  try {
    if (retire === 'close') f.client.close();
    else if (retire === 'scope-failure') f.client.quarantineReplicaScope();
    else { f.client.replicaScope = 'replacement'; f.client.resetReplicaScopeState(); f.client.setReplicaReady(f.client.replica.activateScope('replacement')); }
    f.client.releaseDeferredReplicaOpens();
    const waiting = f.waiters(); f.release.resolve(); await waiting; await f.client.replicaReady.catch(() => undefined); f.flush();
    assert.equal(f.opens().length, 0); assert.equal(f.client.replicaOpenHydrations.size, 0);
    assert.equal(f.client.deferredReplicaOpens.length, 0);
  } finally { f.release.resolve(); f.client.close(); }
});
