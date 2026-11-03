import test from 'node:test';
import assert from 'node:assert/strict';
import { ThinClient, RowPool } from './thin-client.mjs';

const ref = { path: 'tasks.recentWorkspaceTasks', replica: { table: 'tasks', key: '_id' } };
const row = (id, name) => JSON.stringify({ _id: id, name });

function connected() {
  const sent = [];
  const c = new ThinClient('ws://unused', { project: 'p', clientContract: { version: 1 }, pool: new RowPool() });
  c.socket = { readyState: 1, send: (s) => sent.push(JSON.parse(s)), close() {} };
  c.auth = { token: 't', tenant: 'x' }; c.authReady = true;
  c.serverCapabilities = { replicaWatermark: 1, controlWatermark: 1 };
  return { c, sent };
}
const tick = () => new Promise((r) => setTimeout(r, 5));

test('opens are batched, pages are acknowledged and activation is atomic', async () => {
  const { c, sent } = connected();
  const w = c.watchReplica(ref, {});
  w.onUpdate(() => {});
  await tick();
  const open = sent.find((m) => m.type === 'replica.openMany');
  assert.equal(open.opens.length, 1);
  assert.equal(open.opens[0].digest, 'xor1:' + '0'.repeat(64));
  const id = open.opens[0].id;
  c.dispatch({ type: 'replica.snapshotStart', id, snapshotId: 's', cursor: { epoch: 'e', revision: 10 }, key: '_id' });
  c.dispatch({ type: 'replica.snapshotEncodedPage', id, snapshotId: 's', sequence: 0, last: false, canonicalRows: [row('a', 'A')] });
  assert.equal(w.status().isUpToDate, false);
  assert.deepEqual(sent.at(-1), { type: 'replica.snapshotAck', id, snapshotId: 's', sequence: 0 });
  c.dispatch({ type: 'replica.snapshotEncodedPage', id, snapshotId: 's', sequence: 1, last: true, canonicalRows: [row('b', 'B')] });
  c.dispatch({ type: 'replica.ready', id, cursor: { epoch: 'e', revision: 10 }, digest: 'd', truncated: false });
  assert.equal(w.status().isUpToDate, true);
  assert.equal(w.localReplicaState().completeness, 'complete');
  assert.deepEqual(w.localReplicaResult().map((r) => r._id), ['a', 'b']);
});

test('frames that arrive during a snapshot apply after activation, newer than the snapshot only', async () => {
  const { c, sent } = connected();
  const w = c.watchReplica(ref, {}); w.onUpdate(() => {}); await tick();
  const id = sent.find((m) => m.type === 'replica.openMany').opens[0].id;
  c.dispatch({ type: 'replica.snapshotStart', id, snapshotId: 's', cursor: { epoch: 'e', revision: 10 }, key: '_id' });
  // Transaction for a row the snapshot has not delivered yet (it arrives on a later page).
  c.dispatch({ type: 'replica.transactionEncoded', cursor: { epoch: 'e', revision: 11 }, canonicalChanges: [JSON.stringify({ entity: 'tasks', id: 'b', newValue: { _id: 'b', name: 'B2' }, operation: 'update' })] });
  // A transaction already contained in the snapshot must not apply again.
  c.dispatch({ type: 'replica.transactionEncoded', cursor: { epoch: 'e', revision: 9 }, canonicalChanges: [JSON.stringify({ entity: 'tasks', id: 'a', newValue: { _id: 'a', name: 'old' }, operation: 'update' })] });
  c.dispatch({ type: 'replica.delta', id, cursor: { epoch: 'e', revision: 12 }, canonicalUpserts: [row('c', 'C')], deleted: [] });
  c.dispatch({ type: 'replica.snapshotEncodedPage', id, snapshotId: 's', sequence: 0, last: true, canonicalRows: [row('a', 'A'), row('b', 'B')] });
  c.dispatch({ type: 'replica.ready', id, cursor: { epoch: 'e', revision: 10 }, digest: 'd' });
  const byId = Object.fromEntries(w.localReplicaResult().map((r) => [r._id, r.name]));
  assert.deepEqual(byId, { a: 'A', b: 'B2', c: 'C' });
});

test('a reducer settles only after the covering watermark, deferred while a collection opens', async () => {
  const { c, sent } = connected();
  let done = false;
  const p = c.reducer({ path: 'tasks.rename' }, { taskId: 'a' }).then(() => { done = true; });
  const call = sent.find((m) => m.type === 'reducer.call');
  assert.equal(call.clientContract, 1);
  assert.equal('idempotencyKey' in call, false);
  c.dispatch({ type: 'reducer.result', id: call.id, result: null, committedRevision: 20 });
  await tick(); assert.equal(done, false);
  const w = c.watchReplica(ref, {}); w.onUpdate(() => {}); await tick();
  c.dispatch({ type: 'replica.watermark', revision: 20 });
  await tick(); assert.equal(done, false, 'watermark during an open cannot certify');
  const id = sent.find((m) => m.type === 'replica.openMany').opens[0].id;
  c.dispatch({ type: 'replica.snapshotStart', id, snapshotId: 's', cursor: { epoch: 'e', revision: 20 }, key: '_id' });
  c.dispatch({ type: 'replica.snapshotEncodedPage', id, snapshotId: 's', sequence: 0, last: true, canonicalRows: [] });
  c.dispatch({ type: 'replica.ready', id, cursor: { epoch: 'e', revision: 20 }, digest: 'd' });
  await p; assert.equal(done, true);
});

test('control calls settle on control.watermark and carry an idempotency key', async () => {
  const { c, sent } = connected();
  let done = false;
  const p = c.reducer({ path: 'control.support.heartbeat' }, {}).then(() => { done = true; });
  const call = sent.find((m) => m.type === 'reducer.call');
  assert.equal(call.scope, 'control'); assert.equal(call.idempotencyKey, call.id);
  c.dispatch({ type: 'reducer.result', id: call.id, result: {} });
  await tick(); assert.equal(done, false);
  c.dispatch({ type: 'control.watermark', id: call.id });
  await p; assert.equal(done, true);
});

test('released collections close after the retention delay and rows are shared between users', async () => {
  const pool = new RowPool();
  const a = connected(), b = connected();
  a.c.pool = pool; b.c.pool = pool;
  for (const { c, sent } of [a, b]) {
    const w = c.watchReplica(ref, {}); const stop = w.onUpdate(() => {}); await tick();
    const id = sent.find((m) => m.type === 'replica.openMany').opens[0].id;
    c.dispatch({ type: 'replica.snapshotStart', id, snapshotId: 's', cursor: { epoch: 'e', revision: 1 }, key: '_id' });
    c.dispatch({ type: 'replica.snapshotEncodedPage', id, snapshotId: 's', sequence: 0, last: true, canonicalRows: [row('a', 'A')] });
    c.dispatch({ type: 'replica.ready', id, cursor: { epoch: 'e', revision: 1 }, digest: 'd' });
    c.stop = stop;
  }
  assert.equal(pool.size, 1);
  a.c.stop();
  await new Promise((r) => setTimeout(r, 300));
  assert.ok(a.sent.some((m) => m.type === 'replica.close'));
  assert.equal(pool.size, 1, 'still held by the second user');
  b.c.stop();
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(pool.size, 0);
});

test('replica.batch items apply like the separate frames, through stream bindings', async () => {
  const { c, sent } = connected();
  const w = c.watchReplica(ref, {}); w.onUpdate(() => {}); await tick();
  const id = sent.find((m) => m.type === 'replica.openMany').opens[0].id;
  c.dispatch({ type: 'replica.snapshotStart', id, snapshotId: 's', cursor: { epoch: 'e', revision: 10 }, key: '_id' });
  c.dispatch({ type: 'replica.snapshotEncodedPage', id, snapshotId: 's', sequence: 0, last: true, canonicalRows: [row('a', 'A')] });
  c.dispatch({ type: 'replica.ready', id, cursor: { epoch: 'e', revision: 10 }, digest: 'd' });
  let done = false;
  const p = c.reducer({ path: 'tasks.rename' }, { taskId: 'a' }).then(() => { done = true; });
  const call = sent.find((m) => m.type === 'reducer.call');
  c.dispatch({ type: 'reducer.result', id: call.id, result: null, committedRevision: 11 });
  c.dispatch({ type: 'replica.batch', tenant: 'x', class: '0', from: 10, items: [
    { type: 'replica.transactionEncoded', cursor: { epoch: 'e', revision: 11 }, canonicalChanges: [JSON.stringify({ entity: 'tasks', id: 'a', newValue: { _id: 'a', name: 'A2' }, operation: 'update' })] },
    { type: 'replica.stream', id, stream: '~1' },
    { type: 'replica.delta', id: '~1', cursor: { epoch: 'e', revision: 11 }, canonicalUpserts: [row('b', 'B')], deleted: [] },
    { type: 'replica.ready', id: '~1', cursor: { epoch: 'e', revision: 11 }, digest: 'd2' },
    { type: 'replica.delta', id: '~unbound', cursor: { epoch: 'e', revision: 11 }, canonicalUpserts: [row('z', 'Z')], deleted: [] },
  ], watermark: 11 });
  await p; assert.equal(done, true);
  const byId = Object.fromEntries(w.localReplicaResult().map((r) => [r._id, r.name]));
  assert.deepEqual(byId, { a: 'A2', b: 'B' });
  assert.deepEqual(ThinClient.stats.unsupported, {});
});
