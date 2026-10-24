import { test } from 'node:test';
import assert from 'node:assert/strict';
import { applyPatch } from './patch-model.mjs';

test('null, deletion, duplicates, reordering, base loss and full-row recovery', () => {
  const first = { revision: 1, value: { id: 'a', nullable: 4, removed: 1 } };
  const patch = { baseRevision: 1, revision: 2, set: { nullable: null }, unset: ['removed'] };
  const second = applyPatch(first, patch);
  assert.deepEqual(second.value, { id: 'a', nullable: null });
  assert.equal(applyPatch(second, patch), second);
  assert.throws(() => applyPatch(undefined, patch), /missing-base/);
  assert.throws(() => applyPatch(first, { ...patch, baseRevision: 2, revision: 3 }), /missing-base/);
  const repaired = { revision: 3, value: { id: 'a', nullable: 7 } };
  assert.equal(applyPatch(repaired, patch), repaired);
  assert.deepEqual(first.value, { id: 'a', nullable: 4, removed: 1 });
});

test('seeded version transition model agrees with full-row application', () => {
  let state = { revision: 1, value: { id: 'a', a: 0, b: null } };
  let seed = 123456;
  for (let i = 0; i < 10000; i++) {
    seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5;
    const key = seed & 1 ? 'a' : 'b';
    const remove = (seed >>> 2) % 3 === 0;
    const set = remove ? {} : { [key]: seed & 2 ? null : seed };
    const unset = remove ? [key] : [];
    const expected = { ...state.value, ...set };
    if (remove) delete expected[key];
    const patch = { baseRevision: state.revision, revision: state.revision + 1, set, unset };
    const next = applyPatch(state, patch);
    assert.deepEqual(next.value, expected);
    assert.equal(applyPatch(next, patch), next);
    if (i > 0) assert.throws(() => applyPatch({ ...state, revision: state.revision - 1 }, patch), /missing-base/);
    state = next;
  }
});
