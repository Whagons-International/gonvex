import test from 'node:test';
import assert from 'node:assert/strict';
import { firstViewed, viewSetsFirstViewedAt, unmetTaskUpdate } from './task-expectations.mjs';

const op = (over = {}) => ({ at: 1_000_000, state: 'ok', matches: (row) => row.name === 'new', ...over });

test('a delivered edit is met; a missing one is unmet', () => {
  assert.equal(unmetTaskUpdate(op(), { name: 'new', updatedAt: 1 }), false);
  assert.equal(unmetTaskUpdate(op(), { name: 'old', updatedAt: 1 }), true);
});

test('a failed save expects nothing (budget_exceeded before the Reducer ran)', () => {
  assert.equal(unmetTaskUpdate(op({ state: 'failed' }), { name: 'old', updatedAt: 1 }), false);
});

test('rows outside the replica and edits before the subscription are ignored', () => {
  assert.equal(unmetTaskUpdate(op(), undefined), false);
  assert.equal(unmetTaskUpdate(op({ at: 5 }), { name: 'old', updatedAt: 1 }, { subscribedAt: 10 }), false);
});

test('a later edit excuses the expectation within the clock tolerance', () => {
  assert.equal(unmetTaskUpdate(op(), { name: 'other', updatedAt: 1_000_000 - 100 }), false);
  assert.equal(unmetTaskUpdate(op(), { name: 'other', updatedAt: 1_000_000 - 5_000 }), true);
});

// loss-200: the generator clock ran ~10 ms ahead of the runtime, so the
// server-stamped firstViewedAt was below the generator's `at` on 85/93 views
// and the old `firstViewedAt >= at` rule failed every first view.
test('first views match on presence, not on cross-host timestamps', () => {
  const at = 1_793_763_643_647;
  const view = op({ at, matches: (row) => firstViewed(row) });
  const committed = { firstViewedAt: at - 10, updatedAt: 1_793_747_710_568 };
  assert.equal(unmetTaskUpdate(view, committed), false);
  assert.equal(unmetTaskUpdate(view, { firstViewedAt: null, updatedAt: 1_793_747_710_568 }), true);
  const oldRule = op({ at, matches: (row, a) => row.firstViewedAt >= a });
  assert.equal(unmetTaskUpdate(oldRule, committed, { clockToleranceMs: 0 }), true, 'old rule: false loss');
});

test('a view by the creator, or of an already viewed task, changes nothing', () => {
  assert.equal(viewSetsFirstViewedAt({ createdBy: 'm1' }, 'm1'), false);
  assert.equal(viewSetsFirstViewedAt({ createdBy: 'm2', firstViewedAt: 5 }, 'm1'), false);
  assert.equal(viewSetsFirstViewedAt({ createdBy: 'm2', firstViewedAt: null }, 'm1'), true);
});
