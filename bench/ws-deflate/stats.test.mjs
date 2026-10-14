import test from 'node:test';
import assert from 'node:assert/strict';
import { ratioCI } from './stats.mjs';

test('identical paired samples have zero relative change', () => {
  const values = [1, 3, 7, 9, 25, 40, 100, 500];
  assert.deepEqual(ratioCI(values, values).ci95, [0, 0]);
});

test('paired bootstrap retains a constant multiplier', () => {
  const values = [1, 3, 7, 9, 25, 40, 100, 500];
  const result = ratioCI(values, values.map(value => value * 2));
  assert.equal(result.mean, 100);
  assert.deepEqual(result.ci95, [100, 100]);
});

test('alternating latency spikes have wide uncertainty', () => {
  const result = ratioCI([1, 100, 1, 100, 1, 100, 1, 100], [100, 1, 100, 1, 100, 1, 100, 1]);
  assert.equal(result.mean, 0);
  assert(result.ci95[0] < -50);
  assert(result.ci95[1] > 50);
});
