import { afterEach, describe, expect, it } from 'vitest';
import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
// The class is source-exported, intentionally not exported by @gonvex/client.
// Exercise only its public methods, never private maps or authority helpers.
import { LocalReplica, type ReplicaRow } from '../../client/src/local-replica.js';
import { ExpoSQLiteLocalReplicaStorage, type ExpoSQLiteDatabase } from './index.js';

// Artifact baseline: 79668483b46fd8901aa0beacaf9f6e837d2f92e6.
// Proposed regression expectations, unrun when authored. No runtime fix here.
const disposals: Array<() => void> = [];
afterEach(() => { for (const dispose of disposals.splice(0).reverse()) dispose(); });
const scope = 'publication-tenant';
const cursor = (revision: number) => ({ epoch: 'publication-epoch', revision });
const latest = { _id: 'a', status: 'working' };
const input = (signature: string, revision: number, rows: ReplicaRow[]) => ({
  signature, kind: 'replica' as const, entity: 'tasks', key: '_id', rows,
  completeness: 'complete' as const, source: 'server' as const,
  cursor: cursor(revision), scope,
});

async function fixture() {
  // Real SQLite, no SQL parser mock. This is not Expo native transaction testing.
  const db = new DatabaseSync(':memory:');
  disposals.push(() => db.close());
  const database: ExpoSQLiteDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...params) { return db.prepare(sql).run(...params as SQLInputValue[]); },
    async getAllAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).all(...params as SQLInputValue[]) as T[]; },
    async getFirstAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).get(...params as SQLInputValue[]) as T ?? null; },
    async withTransactionAsync(run) {
      db.exec('BEGIN');
      try { await run(); db.exec('COMMIT'); }
      catch (error) { db.exec('ROLLBACK'); throw error; }
    },
  };
  const storage = new ExpoSQLiteLocalReplicaStorage(database);
  const replica = new LocalReplica(storage);
  disposals.push(() => replica.dispose());
  await replica.activateScope(scope);
  // Materialize this FIRST. A later low window cannot rewind an existing floor.
  await replica.replaceWindow({ ...input('anchor', 1, []), entity: 'anchors' });
  async function cold() {
    // New replica and adapter, same SQLite connection. Fresh decode, not a crash.
    const restored = new LocalReplica(new ExpoSQLiteLocalReplicaStorage(database));
    disposals.push(() => restored.dispose());
    await restored.activateScope(scope);
    return restored;
  }
  return { replica, storage, cold };
}

function observe(replica: LocalReplica) {
  const seen: Array<ReplicaRow | undefined> = [];
  const stop = replica.subscribe(() => seen.push(replica.entity('tasks', 'a')));
  disposals.push(stop);
  return seen;
}

// Snapshot, delta and transaction all need the storage-resolved post-image.
// Filling an unseen field must still succeed while an older shared field loses.
describe('LocalReplica publishes SQLite row authority', () => {
  it.each(['snapshot', 'delta', 'transaction'] as const)(
    'publishes the merged row after a stale %s projection', async (method) => {
      const { replica, storage, cold } = await fixture();
      await replica.replaceWindow(input('details', 2, [{ _id: 'a', status: 'new' }]));
      await replica.replaceWindow(input('tasks', 10, [latest]));
      expect(replica.cursor()).toEqual(cursor(1));
      const seen = observe(replica);
      const projection = { _id: 'a', status: 'new', name: 'Renamed' };
      if (method === 'snapshot') {
        await replica.replaceWindow(input('details', 9, [projection]));
      } else if (method === 'delta') {
        await replica.applyWindowDelta({ ...input('details', 9, []), upserts: [projection], deleted: [] });
      } else {
        await replica.applyTransaction({ cursor: cursor(9), changes: [
          { entity: 'tasks', id: 'a', operation: 'update', newValue: projection },
        ] }, scope);
      }
      const published = replica.entity('tasks', 'a');
      const durable = await storage.loadEntityRows(scope, 'tasks', ['a']);
      const restored = await cold();
      const expected = { ...latest, name: 'Renamed' };
      expect(durable).toEqual([{ id: 'a', row: expected }]);
      expect(restored.entity('tasks', 'a')).toEqual(expected);
      expect(published).toEqual(expected);
      for (const row of seen) expect(row).toEqual(expected);
    },
  );

  it('keeps the row AND its existing window after SQLite rejects an older delete', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    expect(replica.cursor()).toEqual(cursor(1));
    const seen = observe(replica);
    await replica.applyTransaction({ cursor: cursor(9), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ] }, scope);
    const publishedRow = replica.entity('tasks', 'a');
    const publishedWindow = replica.getWindow('tasks');
    const durableRow = await storage.loadEntityRows(scope, 'tasks', ['a']);
    const durableWindow = await storage.loadWindowRows(scope, 'tasks');
    const restored = await cold();
    expect(durableRow).toEqual([{ id: 'a', row: latest }]);
    expect(durableWindow?.window).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    expect(restored.entity('tasks', 'a')).toEqual(latest);
    expect(restored.getWindow('tasks')).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    expect(publishedRow).toEqual(latest);
    expect(publishedWindow).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    for (const row of seen) expect(row).toEqual(latest);
    // The feed floor advances to 9 even though this individual delete loses.
    expect(replica.cursor()).toEqual(cursor(9));
    // Do not compare restored.cursor(): hydration conservatively clamps to 1.
  });

  it('restores window metadata as well as rows when a whole stale window is rejected', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    const seen = observe(replica);
    await replica.replaceWindow(input('tasks', 9, [{ _id: 'a', status: 'new' }]));
    const publishedRow = replica.entity('tasks', 'a');
    const publishedWindow = replica.getWindow('tasks');
    const durable = await storage.loadWindowRows(scope, 'tasks');
    const restored = await cold();
    expect(durable?.window).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    expect(durable?.rows).toEqual([latest]);
    expect(restored.entity('tasks', 'a')).toEqual(latest);
    expect(publishedRow).toEqual(latest);
    expect(publishedWindow).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    for (const row of seen) expect(row).toEqual(latest);
  });

  it('keeps an accepted tombstone when another older window supplies the deleted row', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    // Supply explicit committed membership to isolate this test from audit bug7.
    await replica.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ], memberships: [{ ...replica.getWindow('tasks')!, ids: [], cursor: cursor(11) }] }, scope);
    expect(await storage.loadEntityRows(scope, 'tasks', ['a'])).toEqual([]);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    const seen = observe(replica);
    await replica.replaceWindow(input('late', 9, [{ _id: 'a', status: 'new' }]));
    const published = replica.entity('tasks', 'a');
    const durable = await storage.loadWindowRows(scope, 'late');
    const restored = await cold();
    expect(await storage.loadEntityRows(scope, 'tasks', ['a'])).toEqual([]);
    expect(restored.entity('tasks', 'a')).toBeUndefined();
    expect(published).toBeUndefined();
    for (const row of seen) expect(row).toBeUndefined();
    // Window membership and entity existence have separate authority. SQLite
    // accepts this other window but keeps the newer row tombstone. Do not
    // silently invent a membership removal or a newer collection certificate.
    expect(durable?.window).toMatchObject({ ids: ['a'], cursor: cursor(9) });
    expect(durable?.rows).toEqual([]);
    expect(replica.getWindow('late')).toMatchObject({ ids: ['a'], cursor: cursor(9) });
  });

  it('publishes an accepted newer delete instead of restoring the old row', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    await replica.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ], memberships: [{ ...replica.getWindow('tasks')!, ids: [], cursor: cursor(11) }] }, scope);
    const restored = await cold();
    expect(await storage.loadEntityRows(scope, 'tasks', ['a'])).toEqual([]);
    expect((await storage.loadWindowRows(scope, 'tasks'))?.window.ids).toEqual([]);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(restored.entity('tasks', 'a')).toBeUndefined();
    expect(replica.getWindow('tasks')?.ids).toEqual([]);
    expect(replica.cursor()).toEqual(cursor(11));
  });

  it('persists the accepted delete membership even without an explicit membership payload', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    await replica.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ] }, scope);
    const restored = await cold();
    expect(replica.getWindow('tasks')!.ids).toEqual([]);
    expect((await storage.loadWindowRows(scope, 'tasks'))!.window.ids).toEqual([]);
    expect(restored.getWindow('tasks')!.ids).toEqual([]);
  });

  it('keeps a newer window certificate while publishing an accepted entity tombstone', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 1, [latest]));
    await replica.advanceWatermark(10, ['tasks']);
    expect(replica.cursor()).toEqual(cursor(1));
    await replica.applyTransaction({ cursor: cursor(9), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ] }, scope);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect((await storage.loadWindowRows(scope, 'tasks'))!.window).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    expect(replica.getWindow('tasks')).toMatchObject({ ids: ['a'], cursor: cursor(10) });
    expect((await cold()).getWindow('tasks')).toMatchObject({ ids: ['a'], cursor: cursor(10) });
  });

  it('preserves an explicit membership override after resolving a rejected delete', async () => {
    const { replica, storage } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    await replica.applyTransaction({ cursor: cursor(9), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ], memberships: [{ ...replica.getWindow('tasks')!, ids: [], cursor: cursor(12) }] }, scope);
    expect(replica.entity('tasks', 'a')).toEqual(latest);
    expect(replica.getWindow('tasks')!.ids).toEqual([]);
    expect((await storage.loadWindowRows(scope, 'tasks'))!.window.ids).toEqual([]);
  });

  it('removes one window member without deleting a row still owned by another window', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    await replica.replaceWindow(input('other-owner', 10, [latest]));
    await replica.replaceWindow({ ...input('tasks', 11, []), removedIDs: ['a'] });
    const restored = await cold();
    expect(await storage.loadEntityRows(scope, 'tasks', ['a'])).toEqual([{ id: 'a', row: latest }]);
    expect(replica.entity('tasks', 'a')).toEqual(latest);
    expect(restored.entity('tasks', 'a')).toEqual(latest);
    expect(replica.getWindow('tasks')?.ids).toEqual([]);
    expect(replica.getWindow('other-owner')?.ids).toEqual(['a']);
    expect((await storage.loadWindowRows(scope, 'tasks'))?.window.ids).toEqual([]);
    expect((await storage.loadWindowRows(scope, 'other-owner'))?.window.ids).toEqual(['a']);
  });
});
