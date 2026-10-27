import { expect, it, vi } from 'vitest';
import { visitRowsWhile } from './local-view-stream';

it('keeps sync streams synchronous and closes exactly once on short circuit', () => {
  const values: number[] = []; let closed = 0;
  const rows = (function* () { try { yield 1; yield 2; yield 3; } finally { closed++; } })();
  expect(visitRowsWhile(rows, value => { values.push(value); return value < 2; })).toBeUndefined();
  expect(values).toEqual([1, 2]); expect(closed).toBe(1);
});

it('awaits async cleanup of a sync iterator after an asynchronous visitor', async () => {
  let next = 0, closed = false; const done = vi.fn(async () => { await Promise.resolve(); closed = true; return { value: undefined, done: true }; });
  const rows = { [Symbol.iterator]: () => ({ next: () => ({ value: ++next, done: false }), return: done }) } as unknown as Iterable<number>;
  await visitRowsWhile(rows, async value => value < 2);
  expect(next).toBe(2); expect(closed).toBe(true); expect(done).toHaveBeenCalledTimes(1);
});

it.each([false, true])('preserves visitor failures while closing once even if cleanup fails, async=%s', async asynchronous => {
  const failure = Error('visitor failed'), done = vi.fn(() => { throw Error('cleanup failed'); });
  const rows = { [Symbol.iterator]: () => ({ next: () => ({ value: 1, done: false }), return: done }) };
  if (asynchronous) await expect(visitRowsWhile(rows, async () => { throw failure; })).rejects.toBe(failure);
  else expect(() => visitRowsWhile(rows, () => { throw failure; })).toThrow(failure);
  expect(done).toHaveBeenCalledTimes(1);
});

it('closes an asynchronous source before returning a successful short circuit', async () => {
  let closed = false, visited = 0;
  const rows = (async function* () { try { yield 1; throw Error('unused tail read'); } finally { await Promise.resolve(); closed = true; } })();
  await visitRowsWhile(rows, async () => { visited++; return false; });
  expect(visited).toBe(1); expect(closed).toBe(true);
});

it('closes an async iterator after next fails and preserves that failure over cleanup errors', async () => {
  const failure = Error('next failed'), done = vi.fn(async () => { throw Error('cleanup failed'); });
  const rows = { [Symbol.asyncIterator]: () => ({ next: async () => { throw failure; }, return: done }) };
  await expect(visitRowsWhile(rows, () => true)).rejects.toBe(failure); expect(done).toHaveBeenCalledTimes(1);
});
