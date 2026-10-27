import { afterEach, expect, it, vi } from 'vitest';
import { LocalReplica, type LocalReplicaStorage, type ReplicaSnapshot, type ReplicaWindow, type ReplicaStoragePostImages, type ReplicaStorageChanges } from './local-replica.js';

const cursor = (revision: number) => ({ epoch: 'e', revision });
const window = (signature: string, revision: number): ReplicaWindow => ({ signature, kind: 'replica', entity: 'tasks', key: 'id', ids: ['a'], cursor: cursor(revision), completeness: 'complete', source: 'server' });
const original: ReplicaSnapshot = { storageSequence: 5, cursor: cursor(1), entities: { tasks: { a: { id: 'a', value: 1 } } }, liveQueries: { tasks: window('tasks', 1) } };
const empty: ReplicaStorageChanges = { sequence: 5, entities: {}, windows: {} };
const replicas: LocalReplica[] = [];
afterEach(() => { for (const replica of replicas.splice(0)) replica.dispose(); });
async function fixture(overrides: Partial<LocalReplicaStorage> = {}) {
  const storage: LocalReplicaStorage = { load: async () => structuredClone(original), applyTransaction: async () => undefined, ...overrides };
  const replica = new LocalReplica(storage); replicas.push(replica); await replica.activateScope('tenant');
  return { replica, storage };
}
const correction = (value: number): ReplicaStoragePostImages => ({ scope: 'tenant', postImages: { entities: { tasks: { a: { id: 'a', value } } }, windows: { tasks: window('tasks', 1) } } });

it('applies unchanged own-write post-images before catch-up without advancing its checkpoint', async () => {
  const reads = vi.fn(async (_scope: string, after: number) => {
    expect(after).toBe(5);
    return { sequence: 8, entities: { tasks: { a: { id: 'a', value: 8 }, b: { id: 'b', value: 6 } } }, windows: { tasks: { ...window('tasks', 8), ids: ['a', 'b'] } } };
  });
  const { replica } = await fixture({ applyTransaction: async () => correction(1), readChanges: reads });
  const seen: unknown[] = []; replica.subscribe(() => seen.push(replica.entity('tasks', 'a')));
  await replica.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] });
  expect(reads).toHaveBeenCalledTimes(1);
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 8 });
  expect(replica.entity('tasks', 'b')).toEqual({ id: 'b', value: 6 });
  expect(replica.getWindow('tasks')!.ids).toEqual(['a', 'b']);
  expect(seen).toEqual([{ id: 'a', value: 8 }]);
  expect(replica.cursor()).toEqual(cursor(2));
});

it('does not acknowledge peer window revisions when snapshot catch-up advances every stored window', async () => {
  const snapshot = structuredClone(original); snapshot.liveQueries.anchor = { ...window('anchor', 1), entity: 'anchor', ids: [] };
  const reads = vi.fn(async () => ({ sequence: 8, entities: {}, windows: { tasks: window('tasks', 100), anchor: { ...window('anchor', 100), entity: 'anchor', ids: [] } } }));
  const { replica } = await fixture({ load: async () => snapshot, replaceWindow: async () => ({ ...correction(2), postImages: { entities: correction(2).postImages.entities, windows: { tasks: window('tasks', 2) } } }), readChanges: reads });
  await replica.replaceWindow({ ...window('tasks', 2), rows: [{ id: 'a', value: 2 }] });
  expect(reads).toHaveBeenCalledTimes(1);
  expect(replica.getWindow('tasks')!.cursor).toEqual(cursor(100));
  expect(replica.cursor()).toEqual(cursor(1));
});

it('publishes nothing and retries catch-up from the old checkpoint after a read failure', async () => {
  const reads = vi.fn<NonNullable<LocalReplicaStorage['readChanges']>>().mockRejectedValueOnce(new Error('catch-up unavailable')).mockResolvedValueOnce({ ...empty, sequence: 7 });
  const { replica } = await fixture({ applyTransaction: async () => correction(3), readChanges: reads });
  const notify = vi.fn(); replica.subscribe(notify);
  const transaction = { cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'update' as const, newValue: { id: 'a', value: 9 } }] };
  await expect(replica.applyTransaction(transaction)).rejects.toThrow('catch-up unavailable');
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 1 });
  expect(replica.cursor()).toEqual(cursor(1)); expect(notify).not.toHaveBeenCalled();
  await replica.applyTransaction(transaction);
  expect(reads.mock.calls.map(call => call[1])).toEqual([5, 5]);
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 3 });
});

it('rejects wrong-scope post-images before publishing a candidate', async () => {
  const { replica } = await fixture({ applyTransaction: async () => ({ ...correction(9), scope: 'other-tenant' }) });
  const notify = vi.fn(); replica.subscribe(notify);
  await expect(replica.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] })).rejects.toThrow('wrong replica scope');
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 1 }); expect(notify).not.toHaveBeenCalled();
});

it.each(['sequence mismatch', 'gap without catch-up'] as const)('rejects a sequenced receipt with %s', async kind => {
  const previousSequence = kind === 'sequence mismatch' ? 5 : 6;
  const sequence = previousSequence + 1;
  const { replica } = await fixture({ applyTransaction: async () => ({ scope: 'tenant', previousSequence, sequence,
    changes: { sequence: kind === 'sequence mismatch' ? sequence + 1 : sequence, entities: {}, windows: {} } }) });
  await expect(replica.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] })).rejects.toThrow(kind === 'sequence mismatch' ? 'invalid storage commit receipt' : 'requires peer catch-up');
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 1 });
});

it('detaches nested corrected window values from a retained adapter result', async () => {
  const receipt = correction(2);
  receipt.postImages.windows.tasks = { ...window('tasks', 1), resultSkeleton: { nested: { value: 2 } }, scalar: { values: [2] } };
  const { replica } = await fixture({ applyTransaction: async () => receipt });
  await replica.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { id: 'a', value: 9 } }] });
  (receipt.postImages.windows.tasks!.resultSkeleton as { nested: { value: number } }).nested.value = 99;
  (receipt.postImages.windows.tasks!.scalar as { values: number[] }).values.push(99);
  expect(replica.getWindow('tasks')!.resultSkeleton).toEqual({ nested: { value: 2 } });
  expect(replica.getWindow('tasks')!.scalar).toEqual({ values: [2] });
});

it('does not publish completeness or row-version bookkeeping when catch-up fails', async () => {
  const receipt = correction(2); receipt.postImages.windows.tasks = { ...window('tasks', 1), ids: ['a', 'missing'] };
  const { replica } = await fixture({ applyTransaction: async () => receipt, readChanges: async () => { throw new Error('late read failed'); } });
  const version = replica.executionReadVersion('tasks', 'a');
  expect(replica.entityCompleteness('tasks')).toBe('complete');
  await expect(replica.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] })).rejects.toThrow('late read failed');
  expect(replica.entityCompleteness('tasks')).toBe('complete'); expect(replica.executionReadVersion('tasks', 'a')).toBe(version);
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 1 }); expect(replica.getWindow('tasks')!.ids).toEqual(['a']);
});

it.each([5, 6])('rejects a catch-up barrier at %s before its own commit sequence', async sequence => {
  const { replica } = await fixture({ applyTransaction: async () => ({ scope: 'tenant', previousSequence: 6, sequence: 7, changes: { sequence: 7, entities: {}, windows: {} } }), readChanges: async () => ({ ...empty, sequence }) });
  await expect(replica.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] })).rejects.toThrow('required sequence');
  expect(replica.entity('tasks', 'a')).toEqual({ id: 'a', value: 1 }); expect(replica.cursor()).toEqual(cursor(1));
});
