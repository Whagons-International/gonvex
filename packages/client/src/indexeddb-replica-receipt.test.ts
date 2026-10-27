// Proposed regression artifact. Runtime validation UNRUN.
import 'fake-indexeddb/auto';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { IndexedDBLocalReplicaStorage } from './indexeddb-replica.js';
import type { ReplicaRow, ReplicaSnapshot, ReplicaStorageCommit, ReplicaWindow } from './local-replica.js';

const stores: IndexedDBLocalReplicaStorage[] = [];
const scope = 'receipt-tenant';
const cursor = (revision: number, epoch = 'one') => ({ epoch, revision });
const owner = (signature = 'tasks', revision = 10, ids = ['a']): ReplicaWindow => ({
  signature, kind: 'replica', entity: 'tasks', key: 'id', ids,
  completeness: 'complete', source: 'server', cursor: cursor(revision),
  windowRevision: 'certificate', hashes: { a: 'projected-hash' }, aggregateDigest: 'projected-digest',
  orderBy: 'title', orderDirection: 'desc', mode: 'progressive', truncated: false,
});
const snapshot = (window: ReplicaWindow, rows: Record<string, ReplicaRow> = {}): ReplicaSnapshot => ({
  cursor: window.cursor, entities: { tasks: rows }, liveQueries: { [window.signature]: window },
});
function open(name = `legacy-receipt-${crypto.randomUUID()}`) {
  const storage = new IndexedDBLocalReplicaStorage(name);
  stores.push(storage);
  return storage;
}
function authoritative(receipt: unknown): ReplicaStorageCommit {
  expect(receipt).toHaveProperty('changes');
  return receipt as ReplicaStorageCommit;
}
class ChannelStub {
  onmessage: unknown = null;
  postMessage = vi.fn();
  close() {}
}
beforeEach(() => { vi.stubGlobal('BroadcastChannel', ChannelStub); });
afterEach(() => {
  vi.restoreAllMocks();
  for (const storage of stores.splice(0)) storage.close();
  vi.unstubAllGlobals();
});

it.each(['transaction', 'replacement', 'delta'] as const)(
  '%s returns unchanged old-sequence rows, then the full result of an older partial projection', async path => {
    const storage = open();
    const initial = owner();
    const row = { id: 'a', title: 'Newer', priority: 'high' };
    await storage.replaceSnapshot(snapshot(initial, { a: row }), scope);
    const prior = (await storage.load(scope))!;
    const candidate = { ...initial, cursor: cursor(9), completeness: 'partial' as const, ids: ['a', 'candidate-only'] };
    const write = async (incoming: ReplicaRow) => {
      const proposed = snapshot(candidate, { a: incoming });
      if (path === 'transaction') return storage.applyTransaction({ cursor: cursor(9), changes: [
        { entity: 'tasks', id: 'a', operation: 'update', newValue: incoming },
      ] }, proposed, scope);
      if (path === 'replacement') return storage.replaceWindow(candidate, proposed, scope, [incoming]);
      return storage.applyWindowDelta(candidate, { upserts: [incoming], deleted: [] }, proposed, scope);
    };
    const table = (storage as any).database.entities;
    const put = vi.spyOn(table, 'bulkPut');
    const rejected = authoritative(await write({ id: 'a', title: 'Stale' }));
    expect(rejected).toMatchObject({ scope, previousSequence: prior.storageSequence, sequence: 2,
      changes: { sequence: 2, reset: false, entities: { tasks: { a: row } } } });
    expect(put).not.toHaveBeenCalled();
    // Incremental catch-up alone cannot return this unchanged old record.
    expect((await storage.readChanges(scope, prior.storageSequence!)).entities.tasks?.a).toBeUndefined();
    expect((await storage.load(scope))?.entities.tasks?.a).toEqual(row);
    if (path !== 'transaction') expect(rejected.changes.windows.tasks).toEqual(initial);
    const partial = authoritative(await write({ id: 'a', title: 'Stale', description: 'Older but unseen' }));
    const resolved = { ...row, description: 'Older but unseen' };
    expect(partial.changes.entities.tasks?.a).toEqual(resolved);
    expect((await storage.load(scope))?.entities.tasks?.a).toEqual(resolved);
    expect(partial.sequence).toBe(3);
    expect(partial.changes.sequence).toBe(partial.sequence);
  },
);

it('restores every unchanged owner after a rejected delete, then applies explicit memberships last', async () => {
  const storage = open();
  const first = owner('first', 10, ['a', 'b']);
  const second = owner('second', 10, ['b', 'a']);
  const row = { id: 'a', title: 'Newer' };
  await storage.replaceSnapshot({ ...snapshot(first, { a: row, b: { id: 'b' } }),
    liveQueries: { first, second } }, scope);
  const before = (await storage.load(scope))!;
  const deletion = { cursor: cursor(9), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' as const }] };
  const receipt = await storage.applyTransaction(deletion, { entities: {}, liveQueries: {} }, scope);
  expect(receipt.changes.entities.tasks?.a).toEqual(row);
  expect(receipt.changes.windows).toEqual({ first, second });
  expect((await storage.readChanges(scope, before.storageSequence!)).windows).toEqual({});
  const explicit = { ...first, ids: ['b'], cursor: cursor(11), windowRevision: 'explicit' };
  const next = await storage.applyTransaction({ ...deletion, memberships: [explicit] }, { entities: {}, liveQueries: {} }, scope);
  expect(next.changes.windows).toEqual({ first: explicit, second });
  expect((await storage.load(scope))?.liveQueries).toEqual({ first: explicit, second });
});

it('keeps a newer owner certificate even when an entity delete is accepted', async () => {
  const storage = open();
  const initial = owner();
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'a', title: 'At ten' } }), scope);
  const newer = { ...initial, cursor: cursor(20) };
  await storage.advanceWatermark([newer], newer.cursor, scope);
  const receipt = await storage.applyTransaction({ cursor: cursor(11), changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
  ] }, { entities: {}, liveQueries: {} }, scope);
  expect(receipt.changes.entities.tasks?.a).toBeNull();
  expect(receipt.changes.windows.tasks).toEqual(newer);
  const stored = (await storage.load(scope))!;
  expect(stored.entities.tasks?.a).toBeUndefined();
  expect(stored.liveQueries.tasks).toEqual(newer);
});

it('returns an accepted tombstone and never infers membership removal from a rejected resurrection', async () => {
  const storage = open();
  const initial = owner();
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'a', title: 'At ten' } }), scope);
  const deletion = await storage.applyTransaction({ cursor: cursor(11), changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
  ] }, { entities: {}, liveQueries: {} }, scope);
  expect(deletion.changes.entities.tasks?.a).toBeNull();
  expect(deletion.changes.windows.tasks).toEqual({ ...initial, ids: [] });
  const incoming = owner('delayed-owner', 10);
  const receipt = await storage.replaceWindow(incoming, snapshot(incoming), scope, [{ id: 'a', title: 'Stale' }]);
  expect(receipt.changes.entities.tasks?.a).toBeNull();
  expect(receipt.changes.windows['delayed-owner']).toEqual(incoming);
  const stored = (await storage.load(scope))!;
  expect(stored.entities.tasks?.a).toBeUndefined();
  expect(stored.liveQueries['delayed-owner']?.ids).toEqual(['a']);
  expect((await storage.readChanges(scope, deletion.sequence)).entities.tasks?.a).toBeUndefined();
});

it.each(['replacement', 'delta'] as const)('%s captures pruned tombstones and rows retained by another owner', async path => {
  const storage = open();
  const first = owner('first', 10, ['a', 'b']);
  const other = owner('other', 10, ['a']);
  const row = { id: 'a', title: 'Retained' };
  await storage.replaceSnapshot({ ...snapshot(first, { a: row, b: { id: 'b', title: 'Prune' } }),
    liveQueries: { first, other } }, scope);
  const next = { ...first, ids: [], cursor: cursor(11) };
  const proposed = snapshot(next);
  const result = path === 'replacement'
    ? await storage.replaceWindow(next, proposed, scope, [])
    : await storage.applyWindowDelta(next, { upserts: [], deleted: ['a', 'b'] }, proposed, scope);
  const receipt = authoritative(result);
  expect(receipt.changes.entities.tasks).toEqual({ a: row, b: null });
  expect(receipt.changes.windows.first).toEqual(next);
  const stored = (await storage.load(scope))!;
  expect(stored.entities.tasks).toEqual({ a: row });
  expect(stored.liveQueries.other).toEqual(other);
  expect(stored.liveQueries.first).toEqual(next);
});

it('preserves legacy pruning before a rejected window write and reports the actual outcome', async () => {
  const storage = open();
  const old = owner('tasks', 1);
  await storage.replaceSnapshot(snapshot(old, { a: { id: 'a', title: 'Old row' } }), scope);
  const certificate = { ...old, cursor: cursor(10) };
  await storage.advanceWatermark([certificate], certificate.cursor, scope);
  const rejected = { ...old, cursor: cursor(9), ids: [] };
  const receipt = await storage.replaceWindow(rejected, snapshot(rejected), scope, []);
  // Window arbitration and row arbitration remain separate legacy decisions.
  expect(receipt.changes.windows.tasks).toEqual(certificate);
  expect(receipt.changes.entities.tasks?.a).toBeNull();
  const stored = (await storage.load(scope))!;
  expect(stored.liveQueries.tasks).toEqual(certificate);
  expect(stored.entities.tasks?.a).toBeUndefined();
});

it('uses metadata receipts only for exact row-free non-resetting deltas', async () => {
  const storage = open();
  const initial = owner();
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'a' } }), scope);
  const next = { ...initial, cursor: cursor(11) };
  const noRows = snapshot(next);
  Object.defineProperty(noRows, 'entities', { get() { throw new Error('Metadata read rows'); } });
  expect(await storage.applyWindowDelta(next, { upserts: [], deleted: [] }, noRows, scope))
    .toEqual({ scope, previousSequence: 1, sequence: 2 });
  const rejected = authoritative(await storage.applyWindowDelta(initial, { upserts: [], deleted: [] }, snapshot(initial), scope));
  expect(rejected.changes.entities).toEqual({});
  expect(rejected.changes.windows.tasks).toEqual(next);
  const reset = { ...next, cursor: cursor(1, 'two'), ids: [] };
  const receipt = authoritative(await storage.applyWindowDelta(reset, { upserts: [], deleted: [] }, snapshot(reset), scope));
  expect(receipt.changes).toEqual({ sequence: 4, reset: true, entities: {}, windows: { tasks: reset } });
  expect((await storage.load(scope))?.entities).toEqual({});
});

it('allocates durable scope sequences across peer gaps, clear and epoch reset, and rejects retired epochs atomically', async () => {
  const name = `legacy-sequences-${crypto.randomUUID()}`;
  const storage = open(name), peer = open(name);
  const initial = owner();
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'a', title: 'Newer' } }), scope);
  const checkpoint = (await storage.load(scope))!.storageSequence!;
  const peerReceipt = await peer.applyTransaction({ cursor: cursor(11), changes: [
    { entity: 'notes', id: 'peer', operation: 'insert', newValue: { id: 'peer' } },
  ] }, { entities: {}, liveQueries: {} }, scope);
  const own = await storage.applyTransaction({ cursor: cursor(9), changes: [
    { entity: 'tasks', id: 'a', operation: 'update', newValue: { id: 'a', title: 'Stale' } },
  ] }, { entities: {}, liveQueries: {} }, scope);
  expect(own.previousSequence).toBe(peerReceipt.sequence);
  expect(own.previousSequence).toBeGreaterThan(checkpoint);
  expect(own.changes.entities.tasks?.a).toEqual({ id: 'a', title: 'Newer' });
  const catchup = await storage.readChanges(scope, checkpoint);
  expect(catchup.sequence).toBe(own.sequence);
  expect(catchup.entities.notes?.peer).toEqual({ id: 'peer' });
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'other-scope' } }), 'other');
  await storage.clear(scope);
  const reset = { ...initial, ids: ['new'], cursor: cursor(1, 'two') };
  const receipt = await storage.replaceWindow(reset, snapshot(reset), scope, [{ id: 'new' }]);
  expect(receipt.previousSequence).toBe(own.sequence + 1);
  expect(receipt.sequence).toBe(own.sequence + 2);
  expect(receipt.changes).toEqual({ sequence: receipt.sequence, reset: true,
    entities: { tasks: { new: { id: 'new' } } }, windows: { tasks: reset } });
  expect((await storage.readChanges(scope, checkpoint)).reset).toBe(true);
  const before = await storage.load(scope);
  const broadcast = (storage as any).channel.postMessage;
  broadcast.mockClear();
  await expect(storage.applyTransaction({ cursor: cursor(12), changes: [] }, { entities: {}, liveQueries: {} }, scope))
    .rejects.toThrow('obsolete replica epoch');
  expect(await storage.load(scope)).toEqual(before);
  expect(broadcast).not.toHaveBeenCalled();
  expect((await storage.load('other'))?.entities.tasks?.a).toEqual({ id: 'other-scope' });
});

it.each(['transaction', 'replacement', 'delta'] as const)('%s aborts rows, windows and sequence on a late failure without broadcasting', async path => {
  const storage = open();
  const initial = owner();
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'a', title: 'Before' } }), scope);
  const before = await storage.load(scope);
  const next = { ...initial, cursor: cursor(11) };
  const row = { id: 'a', title: 'After' };
  const write = () => path === 'transaction'
    ? storage.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'update', newValue: row },
    ], memberships: [next] }, snapshot(next, { a: row }), scope)
    : path === 'replacement'
      ? storage.replaceWindow(next, snapshot(next, { a: row }), scope, [row])
      : storage.applyWindowDelta(next, { upserts: [row], deleted: [] }, snapshot(next, { a: row }), scope);
  const failure = vi.spyOn((storage as any).database.windows, 'bulkPut').mockRejectedValueOnce(new Error('Late failure'));
  const broadcast = (storage as any).channel.postMessage;
  broadcast.mockClear();
  await expect(write()).rejects.toThrow('Late failure');
  expect(await storage.load(scope)).toEqual(before);
  expect(broadcast).not.toHaveBeenCalled();
  failure.mockRestore();
  const receipt = authoritative(await write());
  expect(receipt.previousSequence).toBe(before!.storageSequence);
  expect(receipt.sequence).toBe(before!.storageSequence! + 1);
  expect(receipt.changes.sequence).toBe(receipt.sequence);
  expect((await storage.load(scope))?.entities.tasks?.a).toEqual(row);
  expect(broadcast).toHaveBeenCalledTimes(1);
});

it('returns the last actual batch post-image when a duplicate no-op follows a changed projection', async () => {
  const storage = open();
  const initial = owner();
  await storage.replaceSnapshot(snapshot(initial, { a: { id: 'a', title: 'Original' } }), scope);
  const receipt = await storage.replaceWindow(initial, snapshot(initial), scope, [
    { id: 'a', description: 'Added at same revision' }, { id: 'a', title: 'Original' },
  ]);
  const expected = { id: 'a', title: 'Original', description: 'Added at same revision' };
  expect(receipt.changes.entities.tasks?.a).toEqual(expected);
  expect((await storage.load(scope))?.entities.tasks?.a).toEqual(expected);
});
