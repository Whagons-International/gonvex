import { act, cleanup, renderHook } from '@testing-library/react';
import { type ReactNode } from 'react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { columnPageReader, type GonvexClient } from '@gonvex/client';
import { LocalReplica } from '../../client/src/local-replica.js';
import { GonvexProvider, useEntityColumns } from './index.js';

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { cleanup(); vi.useRealTimers(); });
const limits = { maxRows: 200, maxPayloadBytes: 64 * 1024 };

async function mounted() {
  const replica = new LocalReplica();
  await replica.replaceWindow({signature:'grid',kind:'live',entity:'tasks',key:'_id',rows:[{_id:'a',title:'Before',description:{cold:true}},{_id:'b',title:'Other'}],completeness:'partial',source:'server'});
  const release = vi.fn();
  const client = { localReplica: replica, retainReplicaEntities: vi.fn(() => release) };
  const wrapper = ({children}: {children:ReactNode}) => <GonvexProvider client={client as unknown as GonvexClient}>{children}</GonvexProvider>;
  return { replica, client, release, wrapper };
}

it('subscribes once, retains the bounded window and publishes optimistic edits/rollback', async () => {
  const { replica, client, release, wrapper } = await mounted();
  const projected = vi.spyOn(replica, 'entityColumns');
  const {result, rerender, unmount} = renderHook(() => useEntityColumns('tasks',['a'],{title:{kind:'string'}},limits),{wrapper});
  const initial = result.current;
  expect(columnPageReader(initial).value('title',0)).toBe('Before');
  expect(client.retainReplicaEntities).toHaveBeenCalledWith('tasks',['a']);
  rerender();
  expect(result.current).toBe(initial);
  expect(projected).toHaveBeenCalledTimes(1);
  act(() => { replica.applyOptimistic('edit',[{entity:'tasks',rowId:'a',op:'patch',fields:{title:'Predicted'}}]); vi.advanceTimersToNextFrame(); });
  expect(columnPageReader(result.current).value('title',0)).toBe('Predicted');
  expect(columnPageReader(initial).value('title',0)).toBe('Before');
  act(() => { replica.rejectCommand('edit'); vi.advanceTimersToNextFrame(); });
  expect(columnPageReader(result.current).value('title',0)).toBe('Before');
  unmount();
  expect(release).toHaveBeenCalledTimes(1);
});

it('preserves page identity for unrelated row changes and releases changed windows', async () => {
  const { replica, client, release, wrapper } = await mounted();
  const {result, rerender} = renderHook(({ids}) => useEntityColumns('tasks',ids,{title:{kind:'string'}},limits),{wrapper,initialProps:{ids:['a']}});
  const first = result.current;
  act(() => { replica.applyOptimistic('other',[{entity:'tasks',rowId:'b',op:'patch',fields:{title:'Unrelated'}}]); vi.advanceTimersToNextFrame(); });
  expect(result.current).toBe(first);
  rerender({ids:['b','missing']});
  expect(release).toHaveBeenCalledTimes(1);
  expect(client.retainReplicaEntities).toHaveBeenLastCalledWith('tasks',['b','missing']);
  expect(columnPageReader(result.current).value('title',0)).toBe('Unrelated');
  expect(result.current.completeProjection).toBe(false);
});

it('fails closed when the identity scope changes', async () => {
  const { replica, wrapper } = await mounted();
  const {result} = renderHook(() => useEntityColumns('tasks',['a'],{title:{kind:'string'}},limits),{wrapper});
  await act(async () => { await replica.activateScope('another-account'); vi.advanceTimersToNextFrame(); });
  expect(result.current.completeProjection).toBe(false);
  expect(columnPageReader(result.current).value('title',0)).toBeUndefined();
});
