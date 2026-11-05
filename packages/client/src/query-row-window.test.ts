import { expect, it } from 'vitest';
import { QueryRowWindow } from './query-row-window.js';

it('keeps 10,000 candidate rows and reference keys in bounded parallel slots', () => {
  const capacity = 23;
  const window = new QueryRowWindow<{ id: number }>(capacity, (_left, _right, left, right) => Number(left![0]) - Number(right![0]));
  const values: unknown[] = [0];
  const rows = Array.from({ length: 10_000 }, (_, id) => ({ id }));
  let retained: Set<unknown> | undefined;
  for (const row of rows) {
    values[0] = rows.length - row.id;
    window.add(row, values);
    expect(window.size).toBeLessThanOrEqual(capacity);
    if (window.size === capacity && !retained) retained = new Set(window['sortValues']);
  }
  // Every later candidate displaced the root, without allocating another
  // per-row key container or retaining the discarded row.
  expect(new Set(window['sortValues'])).toEqual(retained);
  expect(window['positions']).toHaveLength(capacity);
  expect(window['sortValues']).toHaveLength(capacity);
  values[0] = -1;
  const result = window.orderedRows(3);
  expect(result).toEqual(rows.slice(-capacity).reverse().slice(3));
  expect(result[0]).toBe(rows.at(-4));
});

it('matches native stable sorting without reference keys across replacement patterns', () => {
  const rows = Array.from({ length: 503 }, (_, position) => ({ position, rank: (position * 71) % 13 }));
  for (const capacity of [0, 1, 2, 23, 503, 1000]) {
    for (const direction of [-1, 1]) {
      const compare = (left: typeof rows[number], right: typeof rows[number]) => direction * (left.rank - right.rank);
      const window = new QueryRowWindow(capacity, compare);
      for (const row of rows) window.add(row);
      expect(window.orderedRows(0)).toEqual(rows.slice().sort(compare).slice(0, capacity));
      expect(window.orderedRows(capacity + 1)).toEqual([]);
    }
  }
});

it('defers heap construction when all rows fit an untrimmed export', () => {
  let comparisons = 0;
  const window = new QueryRowWindow<number>(10_000, (left, right) => { comparisons++; return left - right; });
  for (let i = 10_000; i > 0; i--) window.add(i);
  expect(comparisons).toBe(0);
  expect(window.orderedRows(9_997)).toEqual([9_998, 9_999, 10_000]);
});
