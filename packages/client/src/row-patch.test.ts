import { expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { RowPatchDecoder } from './row-patch.js';

function transaction(sequence: number, rows: any[], evicted: number[] = []) {
  return { type: 'replica.rowPatch' as const, sequence, rows, evicted,
    message: { type: 'replica.transaction' as const, cursor: { epoch: 'e', revision: sequence },
      changes: rows.map((_, index) => ({ entity: 'tasks', id: String(index), operation: 'update' as const })) } };
}

it('reconstructs nulls, deletions, nested values and prototype-looking field names without mutating bases', () => {
  const decoder = new RowPatchDecoder();
  const original = JSON.parse('{"id":"a","gone":true,"nil":1,"__proto__":"data","nested":{"old":1}}');
  const before = decoder.decode(transaction(1, [{ index: 0, version: 1, value: original }])) as any;
  const set = JSON.parse('{"nil":null,"__proto__":"next","nested":{"new":2},"constructor":false}');
  const next = decoder.decode(transaction(2, [{ index: 0, version: 2, base: 1, set, unset: ['gone'] }], [1])) as any;
  expect(next.changes[0].newValue).toEqual(JSON.parse('{"id":"a","nil":null,"__proto__":"next","nested":{"new":2},"constructor":false}'));
  expect(before.changes[0].newValue).toEqual(original);
  expect(Object.isFrozen(before.changes[0].newValue.nested)).toBe(true);
  expect(decoder.retainedBytes).toBe(new TextEncoder().encode(JSON.stringify(next.changes[0].newValue)).length);
  expect({}.constructor).toBe(Object);
  expect(decoder.retainedEntries).toBe(1);
});

it('ignores duplicate and delayed frames, rejects gaps and missing bases, and resets only explicitly', () => {
  const decoder = new RowPatchDecoder();
  const first = transaction(1, [{ index: 0, version: 1, value: { id: 'a' } }]);
  const second = transaction(2, [{ index: 0, version: 2, base: 1, set: { x: 1 } }], [1]);
  decoder.decode(first); decoder.decode(second);
  expect(decoder.decode(first)).toBeUndefined(); expect(decoder.decode(second)).toBeUndefined();
  expect(() => decoder.decode(transaction(4, []))).toThrow('invalid-replica-row-patch');
  expect(() => decoder.decode(transaction(3, [{ index: 0, version: 3, base: 1, set: {} }]))).toThrow('missing-replica-row-base');
  decoder.clear();
  expect(() => decoder.decode(second)).toThrow();
  expect(decoder.decode(first)).toBeDefined();
});

it('validates the complete frame before publishing dictionary writes or advancing sequence', () => {
  const decoder = new RowPatchDecoder();
  decoder.decode(transaction(1, [{ index: 0, version: 1, value: { id: 'a' } }]));
  const invalid = transaction(2, [{ index: 0, version: 2, base: 1, set: { x: 2 } },
    { index: 1, version: 3, base: 99, set: {} }], [1]);
  expect(() => decoder.decode(invalid)).toThrow('missing-replica-row-base');
  expect(decoder.retainedEntries).toBe(1);
  const valid = transaction(2, [{ index: 0, version: 2, base: 1, set: { x: 3 } }], [1]);
  expect((decoder.decode(valid) as any).changes[0].newValue).toEqual({ id: 'a', x: 3 });
});

it('reconstructs complete ordered delta upserts and preserves delete and integrity metadata', () => {
  const decoder = new RowPatchDecoder();
  const frame: any = { type: 'replica.rowPatch', sequence: 1, rows: [{ index: 0, version: 1, value: { customKey: 8 } }], evicted: [],
    message: { type: 'replica.delta', id: 'subscription', cursor: { epoch: 'e', revision: 7 }, upserts: [], deleted: ['gone'], digest: 'digest', originCommandIds: ['command'] } };
  expect(decoder.decode(frame)).toEqual({ ...frame.message, upserts: [{ customKey: 8 }] });
  expect(() => new RowPatchDecoder().decode({ ...frame, rows: [{ ...frame.rows[0], index: 2 }] })).toThrow();
});

it('keeps byte accounting exact over 10,000 seeded changes, explicit removals and nulls', () => {
  const decoder = new RowPatchDecoder();
  let value: any = { id: 'a', text: '東京😀', one: 1, two: null }, seed = 123456;
  decoder.decode(transaction(1, [{ index: 0, version: 1, value }]));
  for (let i = 2; i < 10002; i++) {
    seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
    const key = seed & 1 ? 'one' : 'two', unset = (seed >>> 2) % 3 === 0 && Object.hasOwn(value, key) ? [key] : [];
    const set = unset.length ? {} : { [key]: seed & 2 ? null : seed };
    value = { ...value, ...set }; for (const key of unset) delete value[key];
    const result: any = decoder.decode(transaction(i, [{ index: 0, version: i, base: i - 1, set, unset }], [i - 1]));
    expect(result.changes[0].newValue).toEqual(value);
    expect(decoder.retainedBytes).toBe(new TextEncoder().encode(JSON.stringify(value)).length);
  }
});

it('rejects cache growth beyond the agreed entry/byte budgets, while permitting uncached large rows', () => {
  const decoder = new RowPatchDecoder();
  const value = { description: 'x'.repeat(4 * 1024 * 1024) };
  expect(() => decoder.decode(transaction(1, [{ index: 0, version: 1, value }]))).toThrow();
  expect(decoder.decode(transaction(1, [{ index: 0, value }]))).toBeDefined();
  expect(decoder.retainedBytes).toBe(0);
  const rows = Array.from({ length: 4097 }, (_, index) => ({ index, version: index + 1, value: { id: String(index) } }));
  expect(() => new RowPatchDecoder().decode(transaction(1, rows))).toThrow();
});

it('decodes the Rust-generated shared contract using either complete or patched alternatives', () => {
  const fixture = JSON.parse(readFileSync(new URL('../../../rust/crates/protocol/fixtures/row-patch-v1.json', import.meta.url), 'utf8'));
  const full = new RowPatchDecoder(), patch = new RowPatchDecoder();
  for (const row of fixture.cases) {
    expect(full.decode(row.full)).toEqual(row.logical);
    expect(patch.decode(row.patch ?? row.full)).toEqual(row.logical);
    expect(full.retainedBytes).toBe(patch.retainedBytes);
  }
});

it('preserves signed zero just like native JSON decoding', () => {
  const decoder = new RowPatchDecoder();
  const before: any = decoder.decode(transaction(1, [{ index: 0, version: 1, value: JSON.parse('{"zero":-0.0}') }]));
  const after: any = decoder.decode(transaction(2, [{ index: 0, version: 2, base: 1, set: { zero: 0 } }], [1]));
  expect(Object.is(before.changes[0].newValue.zero, -0)).toBe(true);
  expect(Object.is(after.changes[0].newValue.zero, 0)).toBe(true);
});

it('permits complete uncached rows even when the sender chooses a lower cache budget', () => {
  const decoder = new RowPatchDecoder();
  expect(decoder.decode(transaction(1, [{ index: 0, value: { id: 'small' } }]))).toBeDefined();
  expect(decoder.retainedEntries).toBe(0);
});
