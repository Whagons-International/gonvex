/**
 * Scheduler (CONTRACT.md E.b, package K11), through raw frames. One Reducer
 * schedules work with runAfter, runAt and three identical wakeAt calls; the
 * scheduled Reducers write the checksummed `board` table:
 *
 *  - runAfter(0) and runAt(+300 ms) each run once (bumps = 2);
 *  - the three wakeAt calls coalesce into one run ("planned+wake");
 *  - a Reducer that reschedules itself with runAfter(0) is stopped by the
 *    durable hop limit after 32 hops (position = 32, bumps = 32);
 *  - nothing else runs afterwards.
 *
 * The kernel's side of the same rules is unit-tested in workerd
 * (cloudflare/kernel/scheduler/test/). The scenario passes on the kernel once
 * the Reducer path (K07) runs the scheduler's Reducers.
 */
import { rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const settle = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const SETTLE_MS = Number(process.env.GONVEX_CONFORMANCE_SCHEDULER_SETTLE_MS ?? 5_000);

export default {
  name: 'scheduler',
  description: 'Scheduler: runAfter, runAt, coalescing wakeAt and the durable hop limit, on the board table',
  tenants: { a: ['alice'] },
  async run(ctx) {
    let raw;
    await ctx.step('connect', async () => {
      raw = await ctx.raw('raw');
      const token = (await ctx.rawLogin(raw, 'alice')).accessToken;
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token, capabilities: RAW_CAPABILITIES });
    });
    await ctx.step('schedule', async () => {
      ctx.observe('plan', await rawReducer(raw, 'scheduler.plan', { id: 'sched' }));
      ctx.observe('chain', await rawReducer(raw, 'scheduler.chain', { id: 'chain', n: 0 }));
    });
    await ctx.step('scheduled work runs', async () => {
      // Reading through a call would record a timing-dependent number of frames, so the scenario waits a fixed
      // time (the work takes about 1 s on a debug Rust build), then reads once.
      await settle(SETTLE_MS);
      const rows = (await rawReducer(raw, 'scheduler.rows', {})).result ?? [];
      const sched = rows.find(r => r.id === 'sched');
      const chain = rows.find(r => r.id === 'chain');
      ctx.expect(sched?.bumps === 2 && sched?.title === 'planned+wake' && chain?.position === 32,
        `expected two bumps, one coalesced wake and a 32-hop chain, got ${JSON.stringify(rows)}`);
      ctx.observe('board', rows);
    });
  },
};
