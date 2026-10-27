import { afterEach, describe, expect, it } from 'vitest';
import { LocalReplica, type LocalReplicaStorage, type ReplicaRow } from '../src/local-replica.js';

// Public publication regressions shared by the persistent adapters.
// These are public LocalReplica method contracts, not server reachability claims.
export type PublicationStorage = LocalReplicaStorage & Required<Pick<LocalReplicaStorage,
  'loadEntityRows' | 'loadWindowRows'>>;
export type PublicationFixture = {
  storage: PublicationStorage;
  fresh(): PublicationStorage;
  close(): void;
};
export type PublicationFactory = () => PublicationFixture;
export const publicationScope = 'publication-tenant';
export const publicationCursor = (revision: number) => ({ epoch: 'publication-epoch', revision });
export const publicationLatest = { _id: 'a', status: 'working' };
const scope = publicationScope;
const cursor = publicationCursor;
const latest = publicationLatest;
const input = (signature: string, revision: number, rows: ReplicaRow[]) => ({
  signature, kind: 'replica' as const, entity: 'tasks', key: '_id', rows,
  completeness: 'complete' as const, source: 'server' as const,
  cursor: cursor(revision), scope,
});

export function publicationContract(name: string, create: PublicationFactory) {
 describe(`${name}: public publication contract`, () => {
  const disposals: Array<() => void> = [];
  afterEach(() => { for (const dispose of disposals.splice(0).reverse()) dispose(); });
  async function fixture() {
    const owner = create();
    disposals.push(() => owner.close());
    const storage = owner.storage;
    const replica = new LocalReplica(storage);
    disposals.push(() => replica.dispose());
    await replica.activateScope(scope);
    // Anchor FIRST. A low window cannot rewind an already advanced floor.
    await replica.replaceWindow({ ...input('anchor', 1, []), entity: 'anchors' });
    async function cold() {
      // New adapter and replica, same database namespace. No crash simulation.
      const restored = new LocalReplica(owner.fresh());
      disposals.push(() => restored.dispose());
      await restored.activateScope(scope);
      return restored;
    }
    return { replica, storage, cold };
  }
  function observe(replica: LocalReplica) {
    const seen: Array<ReplicaRow | undefined> = [];
    disposals.push(replica.subscribe(() => seen.push(replica.entity('tasks', 'a'))));
    return seen;
  }
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

  it('keeps the row AND its existing window after storage rejects an older delete', async () => {
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
    // Window membership and entity existence have separate authority. Storage
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

  it.each(['snapshot', 'delta'] as const)('restores a resident removal rejected by an older %s window', async method => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    if (method === 'snapshot') await replica.replaceWindow({ ...input('tasks', 9, []), removedIDs: ['a'] });
    else await replica.applyWindowDelta({ ...input('tasks', 9, []), upserts: [], deleted: ['a'] });
    const durable = await storage.loadWindowRows(scope, 'tasks');
    expect(durable!.window.ids).toEqual(['a']); expect(durable!.rows).toEqual([latest]);
    expect(replica.entity('tasks', 'a')).toEqual(latest); expect(replica.getWindow('tasks')!.ids).toEqual(['a']);
    expect((await cold()).entity('tasks', 'a')).toEqual(latest);
  });

  it('keeps a newer owner certificate with an accepted entity tombstone', async () => {
    const { replica, storage } = await fixture();
    await replica.replaceWindow(input('tasks', 1, [latest]));
    await replica.advanceWatermark(10, ['tasks']);
    await replica.applyTransaction({ cursor: cursor(9), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] }, scope);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(replica.getWindow('tasks')!.ids).toEqual(['a']);
    expect((await storage.loadWindowRows(scope, 'tasks'))!.window.ids).toEqual(['a']);
  });

  it.each([['a', 'b'], ['b', 'a']])('publishes final membership for mixed deletes ordered %s then %s', async (first, second) => {
    const { replica, storage, cold } = await fixture();
    const a = { _id: 'a', status: 'old' }, b = { _id: 'b', status: 'working' };
    await replica.replaceWindow(input('tasks', 1, [a, b]));
    await replica.replaceWindow(input('newer', 10, [b]));
    const seen: string[][] = [];
    disposals.push(replica.subscribe(() => seen.push([...replica.getWindow('tasks')!.ids])));
    await replica.applyTransaction({ cursor: cursor(9), changes: [first, second].map(id => ({ entity: 'tasks', id, operation: 'delete' as const })) }, scope);
    expect((await storage.loadWindowRows(scope, 'tasks'))!.window.ids).toEqual(['b']);
    expect(replica.getWindow('tasks')!.ids).toEqual(['b']);
    expect((await cold()).getWindow('tasks')!.ids).toEqual(['b']);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(replica.entity('tasks', 'b')).toEqual(b);
    expect(seen.length).toBeGreaterThan(0);
    for (const ids of seen) expect(ids).toEqual(['b']);
  });

  it('applies explicit membership after an accepted entity deletion', async () => {
    const { replica, storage } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    const membership = { ...replica.getWindow('tasks')!, cursor: cursor(11) };
    await replica.applyTransaction({ cursor: cursor(11), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }], memberships: [membership] }, scope);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(replica.getWindow('tasks')!.ids).toEqual(['a']);
    expect((await storage.loadWindowRows(scope, 'tasks'))!.window.ids).toEqual(['a']);
  });

  it.each([['snapshot', 9], ['delta', 9], ['snapshot', 12], ['delta', 12]] as const)('corrects an affected row outside old membership during %s replacement at %s', async (method, revision) => {
    const { replica, storage, cold } = await fixture();
    const orphan = { _id: 'c', status: 'orphan' };
    await replica.replaceWindow(input('orphan', 5, [orphan]));
    await replica.removeWindow('orphan');
    await replica.replaceWindow(input('tasks', 10, [latest]));
    if (method === 'snapshot') await replica.replaceWindow({ ...input('tasks', revision, []), removedIDs: ['c'] });
    else await replica.applyWindowDelta({ ...input('tasks', revision, []), upserts: [], deleted: ['c'] });
    // Legacy pruning can accept a row deletion even when its window loses.
    // Compare the actual committed outcome rather than imposing a new policy.
    const committed = (await storage.loadEntityRows(scope, 'tasks', ['c']))[0]?.row;
    expect(replica.entity('tasks', 'c')).toEqual(committed);
    expect((await cold()).entity('tasks', 'c')).toEqual(committed);
    const committedWindow = (await storage.loadWindowRows(scope, 'tasks'))!.window;
    expect(replica.getWindow('tasks')!.ids).toEqual(committedWindow.ids);
  });

  it('replaces an old epoch with consecutive cursor-bearing live windows without acknowledging their revisions', async () => {
    const { replica, storage, cold } = await fixture();
    await replica.replaceWindow(input('old', 7, [latest]));
    const next = (revision: number) => ({ epoch: 'next-publication-epoch', revision });
    await replica.replaceWindow({ ...input('live-one', 50, [{ _id: 'b', status: 'new epoch' }]), kind: 'live', cursor: next(50) });
    expect(replica.cursor()).toEqual(next(0));
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    expect(await storage.loadEntityRows(scope, 'tasks', ['a'])).toEqual([]);
    expect(await storage.loadWindowRows(scope, 'old')).toBeUndefined();
    await replica.replaceWindow({ ...input('live-two', 60, [{ _id: 'c', status: 'second' }]), kind: 'live', cursor: next(60) });
    expect(replica.cursor()).toEqual(next(0));
    expect(replica.entity('tasks', 'b')).toEqual({ _id: 'b', status: 'new epoch' });
    await replica.applyTransaction({ cursor: next(1), changes: [{ entity: 'tasks', id: 'b', operation: 'update', newValue: { _id: 'b', applied: true } }] }, scope);
    expect(replica.cursor()).toEqual(next(1));
    const restored = await cold();
    expect(restored.entity('tasks', 'a')).toBeUndefined();
    expect(restored.entity('tasks', 'b')).toEqual({ _id: 'b', status: 'new epoch', applied: true });
    expect(restored.getWindow('live-one')).toBeDefined();
    expect(restored.getWindow('live-two')).toBeDefined();
  });

  it.each(['snapshot', 'delta', 'transaction'] as const)(
    'corrects an unchanged rejected row after the checkpoint for a stale %s', async (method) => {
      const { replica, storage, cold } = await fixture();
      await replica.replaceWindow(input('details', 2, [{ _id: 'a', status: 'new' }]));
      await replica.replaceWindow(input('tasks', 10, [latest]));
      const seen = observe(replica);
      // No unseen field changes the durable row's modification sequence.
      const projection = { _id: 'a', status: 'new' };
      if (method === 'snapshot') await replica.replaceWindow(input('details', 9, [projection]));
      else if (method === 'delta') await replica.applyWindowDelta({ ...input('details', 9, []), upserts: [projection], deleted: [] });
      else await replica.applyTransaction({ cursor: cursor(9), changes: [
        { entity: 'tasks', id: 'a', operation: 'update', newValue: projection },
      ] }, scope);
      const hot = replica.entity('tasks', 'a');
      const durable = await storage.loadEntityRows(scope, 'tasks', ['a']);
      const restored = await cold();
      expect.soft(durable).toEqual([{ id: 'a', row: latest }]);
      expect.soft(restored.entity('tasks', 'a')).toEqual(latest);
      expect.soft(hot).toEqual(latest);
      expect(seen.length).toBeGreaterThan(0);
      for (const row of seen) expect.soft(row).toEqual(latest);
    },
  );

  it('isolates identical keys, ignores a foreign-scope operation, and clears only that scope', async () => {
    const { replica, storage } = await fixture();
    await replica.replaceWindow(input('tasks', 10, [latest]));
    const original = await storage.load(scope);
    const other = 'publication-other-tenant';
    await replica.activateScope(other);
    expect(replica.entity('tasks', 'a')).toBeUndefined();
    await replica.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ] }, scope);
    await replica.replaceWindow(input('tasks', 11, [{ _id: 'a', status: 'foreign' }]));
    expect(await storage.load(other)).toBeUndefined();
    await replica.replaceWindow({ ...input('tasks', 10, [{ _id: 'a', status: 'other' }]), scope: other });
    expect(replica.entity('tasks', 'a')).toEqual({ _id: 'a', status: 'other' });
    expect(await storage.load(scope)).toEqual(original);
    await replica.activateScope(scope);
    expect(replica.entity('tasks', 'a')).toEqual(latest);
    await storage.clear!(other);
    expect(await storage.load(other)).toBeUndefined();
    expect(await storage.load(scope)).toEqual(original);
  });

  it('publishes nothing on a rejected persistence call and permits a subsequent commit', async () => {
    const owner = create();
    disposals.push(() => owner.close());
    let rejectNext = false;
    // Deliberate public storage-interface rejection BEFORE the adapter is called.
    // This checks LocalReplica's failed-publication boundary, not DB rollback.
    const gate = new Proxy(owner.storage, {
      get(target, key) {
        const value = Reflect.get(target, key);
        if (key === 'applyTransaction') return (...args: Parameters<LocalReplicaStorage['applyTransaction']>) => {
          if (rejectNext) { rejectNext = false; return Promise.reject(new Error('publication control rejection')); }
          return target.applyTransaction(...args);
        };
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const replica = new LocalReplica(gate);
    disposals.push(() => replica.dispose());
    await replica.activateScope(scope);
    await replica.replaceWindow({ ...input('anchor', 1, []), entity: 'anchors' });
    await replica.replaceWindow(input('tasks', 10, [latest]));
    const hotBefore = replica.snapshot();
    const durableBefore = await owner.storage.load(scope);
    const seen = observe(replica);
    rejectNext = true;
    await expect(replica.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'delete' },
    ], memberships: [{ ...replica.getWindow('tasks')!, ids: [], cursor: cursor(11) }] }, scope))
      .rejects.toThrow('publication control rejection');
    expect(replica.snapshot()).toEqual(hotBefore);
    expect(await owner.storage.load(scope)).toEqual(durableBefore);
    expect(seen).toEqual([]);
    const restored = new LocalReplica(owner.fresh());
    disposals.push(() => restored.dispose());
    await restored.activateScope(scope);
    expect(restored.entity('tasks', 'a')).toEqual(latest);
    expect(restored.getWindow('tasks')?.ids).toEqual(['a']);
    await replica.applyTransaction({ cursor: cursor(11), changes: [
      { entity: 'tasks', id: 'a', operation: 'update', newValue: { _id: 'a', status: 'done' } },
    ] }, scope);
    expect(replica.entity('tasks', 'a')).toEqual({ _id: 'a', status: 'done' });
    expect(await owner.storage.loadEntityRows(scope, 'tasks', ['a']))
      .toEqual([{ id: 'a', row: { _id: 'a', status: 'done' } }]);
  });
 });
}
