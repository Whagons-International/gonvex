import { expect, it } from 'vitest';
import { CompactRelationSet } from './compact-relation-set.js';
it('matches Set through growth, duplicates, hash collisions and exact UTF-16 strings', () => {
  let retained = 0, peak = 0, work = 0;
  const set = new CompactRelationSet(bytes => { retained += bytes; peak = Math.max(peak, retained); }, () => work++);
  const collision = ['key-1ypu7ht-2c47', 'key-1d02u0h-2w9j'];
  const hash = (value: string) => { let h = 2166136261; for (let i = 0; i < value.length; i++) { const c = value.charCodeAt(i); h = Math.imul(h ^ (c & 255), 16777619); h = Math.imul(h ^ (c >>> 8), 16777619); } return h >>> 0; };
  expect(hash(collision[0]!)).toBe(hash(collision[1]!));
  const values = ['', '\0', 'a\0b', 'é', 'ÿ', 'Ā', '中', '😀', '\ud800', '\udc00', '\ufffd', ...collision,
    ...Array.from({ length: 10000 }, (_, n) => `task:${n}:${String.fromCharCode(n % 65536)}`)];
  const oracle = new Set<string>();
  for (const value of values) { expect(set.has(value)).toBe(oracle.has(value)); set.add(value); oracle.add(value); expect(set.has(value)).toBe(true); }
  const before = retained;
  for (const value of values) { set.add(value); expect(set.get(value)).toBe(true); }
  expect(retained).toBe(before); expect(peak).toBeGreaterThan(retained); expect(work).toBeGreaterThan(values.length);
  for (const value of [...values, 'absent', '\ud800x', 'a', 'a\0', 'Ā\0']) expect(set.has(value)).toBe(oracle.has(value));
});
it('preserves non-string SameValueZero and object identity without string coercion', () => {
  const set = new CompactRelationSet(() => {}), object = {}, another = {};
  for (const value of [undefined, null, false, true, NaN, -0, 123, object]) set.add(value);
  for (const value of [undefined, null, false, true, NaN, 0, -0, 123, object]) expect(set.has(value)).toBe(true);
  for (const value of ['undefined', 'null', 'false', 'NaN', '0', '123', another]) expect(set.has(value)).toBe(false);
});
it('charges allocation peaks before growing and bounds work while hashing long keys', () => {
  let retained = 0;
  const set = new CompactRelationSet(bytes => { if (retained + bytes > 96) throw Error('budget'); retained += bytes; });
  set.add('a'); expect(retained).toBe(96);
  expect(() => set.add('x'.repeat(40))).toThrow('budget'); expect(set.has('a')).toBe(true); expect(set.has('x'.repeat(40))).toBe(false);
  let work = 0;
  const bounded = new CompactRelationSet(() => {}, () => { if (++work > 10) throw Error('work'); });
  expect(() => bounded.add('x'.repeat(100))).toThrow('work');
});
