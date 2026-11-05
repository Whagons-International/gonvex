import { test } from 'node:test';
import assert from 'node:assert/strict';
import { assertBudget } from './gates.mjs';

const good = () => ({ tasks: 10_000, workspaces: 5, activeMs: 20_100, peakPrivateMB: 120, peakRssMB: 200, retainedGrowthMiB: 4, taskCpuSeconds: 1, errors: [],
  visits: Array.from({ length: 10 }, (_, index) => ({ workspace: (index + 1) % 5, steps: 10, residentRows: 200, metadataIds: 2000, queryRows: 200 })) });
test('accepts complete observations on the exact budget boundary', () => assert.doesNotThrow(() => assertBudget(good())));
test('rejects peaks, retained growth, absent metrics and incomplete workloads', () => {
  for (const change of [{ peakPrivateMB: 120.01 }, { retainedGrowthMiB: 4.01 }, { peakPrivateMB: NaN }, { peakPrivateMB: 0 }, { peakRssMB: NaN }, { peakRssMB: 0 }, { taskCpuSeconds: NaN }, { errors: undefined }, { activeMs: 19999 }, { activeMs: 25001 }, { visits: [] }, { errors: ['Application error'] }]) {
    assert.throws(() => assertBudget({ ...good(), ...change }));
  }
  const missingRows = good(); missingRows.visits[0].residentRows = 0;
  assert.throws(() => assertBudget(missingRows));
  const repeatedWorkspace = good(); repeatedWorkspace.visits[0].workspace = 0;
  assert.throws(() => assertBudget(repeatedWorkspace));
  const absentSteps = good(); delete absentSteps.visits[0].steps;
  assert.throws(() => assertBudget(absentSteps));
  const absentQuery = good(); delete absentQuery.visits[0].queryRows;
  assert.throws(() => assertBudget(absentQuery));
});
