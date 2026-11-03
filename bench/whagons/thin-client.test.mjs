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

// Legacy JSON rows: what the Whagons app's SDK negotiates without a native
// staging adapter (no replicaNativeRows capability, no snapshot ACKs).
test('legacy rows: auth capabilities match the SDK without native staging', () => {
  const sent = [];
  const c = new ThinClient('ws://unused', { project: 'p', clientContract: { version: 1 } });
  c.socket = { readyState: 1, send: (s) => sent.push(s), close() {} };
  c.sendAuth();
  const auth = JSON.parse(sent[0]);
  assert.equal(auth.capabilities.replicaNativeRows, undefined);
  assert.equal(auth.capabilities.replicaSetDigest, 1);
  assert.equal(auth.capabilities.replicaSnapshotPages, 1);
  // Same key order as the SDK's auth frame.
  assert.deepEqual(Object.keys(auth), ['type', 'clientContract', 'id', 'project', 'controlOnly', 'device', 'capabilities'].filter((k) => k !== 'device' || auth.device !== undefined));
});

test('legacy rows: JSON pages stage without ACKs, transactions and deltas defer until ready', async () => {
  const { c, sent } = connected();
  const w = c.watchReplica(ref, {}); w.onUpdate(() => {}); await tick();
  const id = sent.find((m) => m.type === 'replica.openMany').opens[0].id;
  const cursor = (revision) => ({ epoch: 'e', revision });
  c.dispatch({ type: 'replica.snapshotPage', id, snapshotId: 's', sequence: 0, last: false, cursor: cursor(10), key: '_id', result: [{ _id: 'a', name: 'A' }] });
  c.dispatch({ type: 'replica.transaction', cursor: cursor(11), changes: [{ entity: 'tasks', id: 'b', operation: 'update', newValue: { _id: 'b', name: 'B2' } }] });
  c.dispatch({ type: 'replica.transaction', cursor: cursor(9), changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { _id: 'a', name: 'old' } }] });
  c.dispatch({ type: 'replica.delta', id, cursor: cursor(12), upserts: [{ _id: 'c', name: 'C' }], deleted: [] });
  c.dispatch({ type: 'replica.snapshotPage', id, snapshotId: 's', sequence: 1, last: true, cursor: cursor(10), key: '_id', result: [{ _id: 'b', name: 'B' }] });
  assert.equal(w.status().isUpToDate, false);
  assert.equal(sent.some((m) => m.type === 'replica.snapshotAck'), false);
  c.dispatch({ type: 'replica.ready', id, cursor: cursor(10), digest: 'xor1:' + '0'.repeat(64) });
  assert.equal(w.status().isUpToDate, true);
  assert.deepEqual(Object.fromEntries(w.localReplicaResult().map((r) => [r._id, r.name])), { a: 'A', b: 'B2', c: 'C' });
  c.dispatch({ type: 'replica.transaction', cursor: cursor(13), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] });
  c.dispatch({ type: 'replica.syncing', id, reason: 'x' });
  assert.equal(w.status().isUpToDate, false);
  assert.deepEqual(w.localReplicaResult().map((r) => r._id).sort(), ['b', 'c']);
  assert.deepEqual(c.constructor.stats.unsupported, {});
});

test('legacy rows: a single-frame replica.snapshot activates on ready', async () => {
  const { c, sent } = connected();
  const w = c.watchReplica(ref, {}); w.onUpdate(() => {}); await tick();
  const id = sent.find((m) => m.type === 'replica.openMany').opens[0].id;
  c.dispatch({ type: 'replica.snapshot', id, cursor: { epoch: 'e', revision: 3 }, key: '_id', result: [{ _id: 'x', name: 'X' }] });
  c.dispatch({ type: 'replica.ready', id, cursor: { epoch: 'e', revision: 3 }, digest: 'd' });
  assert.deepEqual(w.localReplicaResult(), [{ _id: 'x', name: 'X' }]);
});

test('presence v2: capability, transitions only, re-announce after auth, snapshot and patches', async () => {
  const { c, sent } = connected();
  // Same capability object (and key order) as the SDK auth frame.
  c.socket.readyState = 1; c.authReady = false; c.presenceV2 = true;
  c.sendAuth();
  const auth = sent.at(-1);
  assert.equal(auth.capabilities.presence, 1);
  assert.equal(Object.keys(auth.capabilities).at(-1), 'presence');
  c.serverCapabilities = { replicaWatermark: 1, controlWatermark: 1, presence: 1 };
  assert.equal(c.presenceSupported(), true);
  c.setPresence('active');
  assert.equal(sent.filter((m) => m.type === 'presence.set').length, 0, 'not before auth.result');
  const updates = [];
  const stop = c.subscribePresence(() => updates.push(c.presenceSnapshot()));
  c.dispatch({ type: 'auth.result', id: auth.id, result: {} });
  const presence = () => sent.filter((m) => String(m.type).startsWith('presence.'));
  assert.deepEqual(presence(), [{ type: 'presence.set', state: 'active' }, { type: 'presence.subscribe', id: 'presence-1' }]);
  c.setPresence('active');
  c.setPresence('idle');
  assert.deepEqual(presence().at(-1), { type: 'presence.set', state: 'idle' });
  assert.equal(presence().length, 3);
  c.dispatch({ type: 'presence.snapshot', id: 'presence-1', serverTime: 1, members: [{ memberId: 'm1', state: 'active', since: 1 }] });
  assert.equal(c.presenceSnapshot().ready, true);
  c.dispatch({ type: 'presence.patch', serverTime: 2, changes: [{ memberId: 'm1', state: 'offline', since: 2 }, { memberId: 'm2', state: 'idle', since: 2 }] });
  assert.deepEqual([...c.presenceSnapshot().members.keys()], ['m2']);
  assert.ok(updates.length >= 2);
  // Re-auth (e.g. session-expired) re-announces state and resubscribes.
  c.authReady = false; c.sendAuth(); const again = sent.at(-1);
  c.dispatch({ type: 'auth.result', id: again.id, result: {} });
  assert.deepEqual(presence().slice(-2), [{ type: 'presence.set', state: 'idle' }, { type: 'presence.subscribe', id: 'presence-2' }]);
  stop();
  assert.deepEqual(presence().at(-1), { type: 'presence.unsubscribe' });
  assert.deepEqual(ThinClient.stats.unsupported, {});
});

test('presence v2: nothing is sent to a runtime without the capability', () => {
  const { c, sent } = connected();
  c.setPresence('active');
  c.subscribePresence(() => {});
  assert.equal(c.presenceSupported(), false);
  assert.equal(sent.filter((m) => String(m.type).startsWith('presence.')).length, 0);
});
