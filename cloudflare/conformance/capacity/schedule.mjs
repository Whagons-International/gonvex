import { Refusal } from './binding.mjs';
export const sleep = (ms, signal) => new Promise((resolve, reject) => {
  if (signal?.aborted) return reject(new Refusal('STOPPED', 'cancelled'));
  const done = () => { signal?.removeEventListener('abort', abort); resolve(); };
  const timer = setTimeout(done, ms);
  const abort = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); reject(new Refusal('STOPPED', 'cancelled')); };
  signal?.addEventListener('abort', abort, { once: true });
});
/** Constant-size lane clocks, no preallocated operation queue. No retries or coordinated omission. */
export async function schedule(phase, { signal, check, dispatch, emit, maxPending, maxLagMs, now = () => performance.now(), wait = sleep }) {
  const start = now(), pending = new Set(); let failure;
  const lanes = phase.tenants.flatMap(t => t.rates.map(r => ({ tenant: t, operation: r.operation, interval: 1000 / r.perSecond, n: 0 })));
  try {
    for (;;) {
      check(); if (signal.aborted) throw new Refusal('STOPPED', 'cancelled'); if (failure) throw failure;
      const lane = lanes.reduce((a, b) => !a || a.n * a.interval > b.n * b.interval ? b : a, undefined);
      if (!lane || lane.n * lane.interval >= phase.durationMs) break;
      const intended = start + lane.n++ * lane.interval;
      await wait(Math.max(0, intended - now()), signal); check();
      const lag = now() - intended;
      emit({ type: 'offered', tenantAlias: lane.tenant.alias, operation: lane.operation, offered: 1, generatorLagMs: lag });
      if (lag > maxLagMs) throw new Refusal('STOPPED', 'generator-scheduling-lag');
      if (pending.size >= maxPending) { emit({ type: 'censored-inflight', tenantAlias: lane.tenant.alias, operation: lane.operation, timeouts: 1 }); continue; }
      const p = Promise.resolve().then(() => dispatch(lane.tenant, lane.operation, intended)).catch(e => { failure = e; }).finally(() => pending.delete(p));
      pending.add(p);
    }
    await wait(Math.max(0, start + phase.durationMs - now()), signal);
    await Promise.all(pending); if (failure) throw failure;
  } finally { await Promise.allSettled(pending); }
}
