import { expect, it } from 'vitest';
import { RowWindowBuffer } from './row-window-buffer.js';

it('borrows row identities and keeps at most 200 references through 10,000 positions', () => {
  const window = new RowWindowBuffer<object>(200);
  for (let position = 0; position < 10_000; position++) {
    const row = Object.freeze({ id: position });
    window.set(position, row);
    expect(window.get(position)).toBe(row);
    expect(window.size).toBeLessThanOrEqual(200);
    if (position >= 200) expect(window.get(position - 200)).toBeUndefined();
  }
  expect(window.size).toBe(200);
  expect(window.delete(9799)).toBe(false); // A stale position cannot delete its replacement.
  expect(window.get(9999)).toEqual({ id: 9999 });
});

it('clears references outside the range and on navigation, including wrapped slots', () => {
  const window = new RowWindowBuffer<object>(4);
  for (let position = 3; position < 7; position++) window.set(position, { id: position });
  window.retainRange(5, 2);
  expect(window.size).toBe(2);
  expect(window.has(3)).toBe(false);
  expect(window.has(5)).toBe(true);
  // Stale storage must release objects, not merely hide them behind flags.
  expect((window as any).rows.filter((row: unknown) => row !== undefined)).toHaveLength(2);
  window.clear();
  expect(window.size).toBe(0);
  expect((window as any).rows.every((row: unknown) => row === undefined)).toBe(true);
});

it('rejects invalid bounds and distinguishes an occupied undefined value', () => {
  expect(() => new RowWindowBuffer(0)).toThrow(RangeError);
  const window = new RowWindowBuffer<undefined>(2);
  for (const index of [-1, 0.5, NaN, 0x100000000]) expect(() => window.set(index, undefined)).toThrow(RangeError);
  window.set(0xffffffff, undefined);
  expect(window.has(0xffffffff)).toBe(true);
  expect(window.has(1)).toBe(false);
  expect(() => window.retainRange(0, 3)).toThrow(RangeError);
  expect(() => window.retainRange(0xffffffff, 2)).toThrow(RangeError);
});
