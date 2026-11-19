// X17e (#529 review): a gateway refresh of Control queries that fails is retried with backoff, not dropped.
import { describe, expect, it } from 'vitest';
import { REFRESH_ATTEMPTS, refreshSessions } from '../gateway';

type Reason = 'control-change' | 'presence-change';
function session(failures: number, closed = false) {
  const calls: Reason[] = [];
  return {
    calls, closed,
    async controlChanged(_project: string, reason: Reason) {
      calls.push(reason);
      if (failures-- > 0) throw new Error('Subrequest depth limit exceeded');
    },
  };
}

describe('X17e gateway refresh retries', () => {
  it('retries only the sessions that failed, with growing delays, until they succeed', async () => {
    const ok = session(0), flaky = session(2);
    const delays: number[] = [];
    await refreshSessions([ok, flaky], 'p', 'presence-change', { sleep: async (ms) => { delays.push(ms); } });
    expect(ok.calls).toEqual(['presence-change']);
    expect(flaky.calls).toEqual(['presence-change', 'presence-change', 'presence-change']);
    expect(delays).toEqual([250, 1000]);
  });

  it('gives up after REFRESH_ATTEMPTS and does not retry closed sessions', async () => {
    const broken = session(Infinity), closed = session(Infinity, true);
    const delays: number[] = [];
    await refreshSessions([broken, closed], 'p', 'control-change', { sleep: async (ms) => { delays.push(ms); } });
    expect(broken.calls.length).toBe(REFRESH_ATTEMPTS);
    expect(closed.calls.length).toBe(1);
    expect(delays.length).toBe(REFRESH_ATTEMPTS - 1);
  });
});
