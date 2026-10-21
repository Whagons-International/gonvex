import { expect, it, vi } from 'vitest';
import { LocalReplica, type LocalReplicaStorage, type ReplicaWindow } from './local-replica';

it('publishes retained-window rows hydrated through an entity lease before a window lease', async () => {
  const window: ReplicaWindow = {
    signature: 'grid', kind: 'live', entity: 'tasks', key: 'id', ids: ['a'],
    completeness: 'complete', source: 'server',
    resultSkeleton: { rows: [], total: 1, offset: 0, limit: 250 }, resultPath: ['rows'],
  };
  const row = { id: 'a', name: 'Cached task' };
  const other = { ...window, signature: 'other', ids: ['b'] };
  const loadWindowRows = vi.fn(async () => ({ window, rows: [row] }));
  const storage: LocalReplicaStorage = {
    load: async () => ({ entities: { tasks: { b: { id: 'b' } } }, liveQueries: { grid: window, other } }),
    loadEntityRows: async () => [{ id: 'a', row }],
    loadWindowRows,
    applyTransaction: async () => {},
  };
  const replica = new LocalReplica(storage);
  await replica.hydrate();
  const before = replica.liveQuerySnapshot('grid');
  expect(before.ids).toEqual([]);
  const unrelated = replica.liveQuerySnapshot('other');
  const unrelatedChanged = vi.fn();
  const unsubscribeOther = replica.subscribe(unrelatedChanged, { window: 'other' });
  const changed = vi.fn();
  const unsubscribe = replica.subscribe(changed, { window: 'grid' });
  const releaseRows = replica.retainRows('tasks', ['a']);
  await replica.withReadView(() => ({}), false, async () => undefined);
  expect(replica.entity('tasks', 'a')).toEqual(row);
  expect(replica.liveQuery('grid').ids).toEqual(['a']);

  // The rows are already resident, so this later lease legitimately skips disk.
  const releaseWindow = replica.retainWindow('grid');
  await replica.withReadView(() => ({}), false, async () => undefined);
  expect(loadWindowRows).not.toHaveBeenCalled();
  expect(replica.liveQuerySnapshot('grid')).toMatchObject({
    ids: ['a'], rows: [row], completeness: 'complete', total: 1,
  });
  await vi.waitFor(() => expect(changed).toHaveBeenCalled());
  expect(replica.liveQuerySnapshot('other')).toBe(unrelated);
  expect(unrelatedChanged).not.toHaveBeenCalled();
  unsubscribeOther();
  releaseWindow();
  releaseRows();
  unsubscribe();
});

it('invalidates a cached live projection when another retained window hydrates the same rows', async () => {
  const grid: ReplicaWindow = {
    signature: 'grid', kind: 'live', entity: 'tasks', key: 'id', ids: ['a'],
    completeness: 'complete', source: 'server',
  };
  const collection: ReplicaWindow = { ...grid, signature: 'tasks', kind: 'replica' };
  const row = { id: 'a', name: 'Cached task' };
  const loadWindowRows = vi.fn(async (_scope: string, signature: string) => ({
    window: signature === 'tasks' ? collection : grid, rows: [row],
  }));
  const replica = new LocalReplica({
    load: async () => ({ entities: {}, liveQueries: { grid, tasks: collection } }),
    loadWindowRows, applyTransaction: async () => {},
  });
  await replica.hydrate();
  expect(replica.liveQuerySnapshot('grid').ids).toEqual([]);
  const releaseCollection = replica.retainWindow('tasks');
  await replica.withReadView(() => ({}), false, async () => undefined);
  expect(replica.liveQuery('grid').ids).toEqual(['a']);
  const releaseGrid = replica.retainWindow('grid');
  await replica.withReadView(() => ({}), false, async () => undefined);
  expect(loadWindowRows).toHaveBeenCalledTimes(1);
  expect(replica.liveQuerySnapshot('grid')).toMatchObject({ ids: ['a'], rows: [row] });
  releaseGrid();
  releaseCollection();
});


it('keeps shared-adapter hydration rows isolated unless ownership was explicitly transferred', async () => {
  const row = { id: 'a', title: 'Original' };
  const window: ReplicaWindow = {signature:'shared',entity:'tasks',kind:'replica',key:'id',ids:['a'],completeness:'complete',source:'cache'};
  const replica = new LocalReplica({load:async()=>({entities:{},liveQueries:{shared:window}}),loadWindowRows:async()=>({window,rows:[row]}),applyTransaction:async()=>{}});
  await replica.activateScope('shared-scope');
  const release = replica.retainWindow('shared');
  await replica.withReadView(()=>({}),false,async()=>undefined);
  row.title='Adapter mutation';
  expect(replica.entity('tasks','a')?.title).toBe('Original');
  release();
});
