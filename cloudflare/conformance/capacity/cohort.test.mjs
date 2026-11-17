import { it, expect } from 'vitest';
import { cohort } from './cohort.mjs';
it('opens a bounded concurrent readiness cohort and awaits all owners', async () => {
  let active = 0, peak = 0, finished = 0;
  await cohort([0, 1, 2, 3, 4, 5], async () => { active++; peak = Math.max(peak, active); await new Promise(r => setTimeout(r, 15)); active--; finished++; },
    { limit: 3, intervalMs: 1, check() {}, signal: new AbortController().signal, cancel() {} });
  expect(peak).toBe(3); expect(finished).toBe(6); expect(active).toBe(0);
});
