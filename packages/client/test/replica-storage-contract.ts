import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { LocalReplicaStorage, ReplicaSnapshot, ReplicaTransaction } from '../src/local-replica.js';

export type StorageFixture = {
  storage: LocalReplicaStorage;
  close(): void;
  reopen?(): LocalReplicaStorage;
  /** Reject after some writes in the next transaction have already succeeded. */
  interruptNextCommit?(): void;
};

const cursor = (revision: number) => ({ epoch: 'contract-epoch', revision });
const snapshot = (revision: number, title: string): ReplicaSnapshot => ({
  cursor: cursor(revision),
  entities: { tasks: { a: { id: 'a', title, nested: { value: title } } } },
  liveQueries: { tasks: { signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id',
    ids: ['a'], completeness: 'complete', source: 'server', cursor: cursor(revision) } },
});
const logical = (value: ReplicaSnapshot | undefined) => value && ({
  cursor: value.cursor, entities: value.entities, liveQueries: value.liveQueries,
});

/** Shared executable contract. A replacement adapter should run this same suite.
 * The snapshot argument is the already resolved post-image supplied by LocalReplica.
 * Commit sequence receipts are adapter-specific and excluded from logical equality. */
export function replicaStorageContract(name: string, create: () => StorageFixture, options: { reopen?: boolean; rollback?: boolean } = {}) {
  describe(name, () => {
    let fixture: StorageFixture;
    beforeEach(() => { fixture = create(); });
    afterEach(() => { fixture.close(); });

    it('isolates identical row/window keys and clear operations by scope', async () => {
      await fixture.storage.replaceSnapshot!(snapshot(1, 'tenant-a'), 'tenant-a');
      await fixture.storage.replaceSnapshot!(snapshot(2, 'tenant-b'), 'tenant-b');
      expect(logical(await fixture.storage.load('tenant-a'))).toEqual(snapshot(1, 'tenant-a'));
      expect(logical(await fixture.storage.load('tenant-b'))).toEqual(snapshot(2, 'tenant-b'));
      await fixture.storage.clear!('tenant-a');
      expect(await fixture.storage.load('tenant-a')).toBeUndefined();
      expect(logical(await fixture.storage.load('tenant-b'))).toEqual(snapshot(2, 'tenant-b'));
    });

    it('commits row updates, membership and cursor together and replays idempotently', async () => {
      const before = snapshot(1, 'before');
      const after = snapshot(2, 'after');
      await fixture.storage.replaceSnapshot!(before, 'tenant');
      const transaction: ReplicaTransaction = { cursor: cursor(2), memberships: [after.liveQueries.tasks],
        changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: after.entities.tasks.a }] };
      await fixture.storage.applyTransaction(transaction, after, 'tenant');
      expect(logical(await fixture.storage.load('tenant'))).toEqual(after);
      await fixture.storage.applyTransaction(transaction, after, 'tenant');
      expect(logical(await fixture.storage.load('tenant'))).toEqual(after);
    });

    it('keeps a tombstone and current cursor when an older insert is replayed', async () => {
      const before = snapshot(1, 'before');
      await fixture.storage.replaceSnapshot!(before, 'tenant');
      const deleted: ReplicaSnapshot = { cursor: cursor(3), entities: {}, liveQueries: {
        tasks: { ...before.liveQueries.tasks, ids: [], cursor: cursor(3) },
      } };
      await fixture.storage.applyTransaction({ cursor: cursor(3), memberships: [deleted.liveQueries.tasks],
        changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] }, deleted, 'tenant');
      await fixture.storage.applyTransaction({ cursor: cursor(2), changes: [
        { entity: 'tasks', id: 'a', operation: 'insert', newValue: before.entities.tasks.a },
      ] }, deleted, 'tenant');
      const loaded = await fixture.storage.load('tenant');
      expect(loaded?.entities.tasks?.a).toBeUndefined();
      expect(loaded?.cursor).toEqual(cursor(3));
      expect(loaded?.liveQueries.tasks.ids).toEqual([]);
    });

    it('detaches persisted and loaded values from caller mutations', async () => {
      const input = snapshot(1, 'original');
      await fixture.storage.replaceSnapshot!(input, 'tenant');
      input.entities.tasks.a.title = 'changed input';
      input.liveQueries.tasks.ids.push('extra');
      const loaded = await fixture.storage.load('tenant');
      expect(logical(loaded)).toEqual(snapshot(1, 'original'));
      loaded!.entities.tasks.a.title = 'changed output';
      loaded!.liveQueries.tasks.ids.length = 0;
      expect(logical(await fixture.storage.load('tenant'))).toEqual(snapshot(1, 'original'));
    });

    if (options.reopen) it('loads the complete committed state through a new adapter instance', async () => {
      await fixture.storage.replaceSnapshot!(snapshot(1, 'committed'), 'tenant');
      fixture.storage = fixture.reopen!();
      expect(logical(await fixture.storage.load('tenant'))).toEqual(snapshot(1, 'committed'));
    });

    if (options.rollback) it('rolls back rows, windows and cursor after a partially executed commit fails', async () => {
      const before = snapshot(1, 'before');
      await fixture.storage.replaceSnapshot!(before, 'tenant');
      // Cross the SQLite 100-row batch as well as both IndexedDB write batches.
      const rows = Object.fromEntries(Array.from({ length: 101 }, (_, i) => [String(i), { id: String(i), title: 'new' }]));
      const after: ReplicaSnapshot = { cursor: cursor(2), entities: { tasks: rows }, liveQueries: {
        tasks: { ...before.liveQueries.tasks, ids: Object.keys(rows), cursor: cursor(2) },
      } };
      fixture.interruptNextCommit!();
      await expect(fixture.storage.applyTransaction({ cursor: cursor(2), memberships: [after.liveQueries.tasks],
        changes: Object.entries(rows).map(([id, newValue]) => ({ entity: 'tasks', id, operation: 'insert', newValue })),
      }, after, 'tenant')).rejects.toThrow('contract interrupted commit');
      expect(logical(await fixture.storage.load('tenant'))).toEqual(before);
    });
  });
}
