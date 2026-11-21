import { sleep } from './schedule.mjs';
/** Paced starts with bounded concurrent readiness, not serial handshakes or an unbounded promise list. */
export async function cohort(indices, launch, { limit, intervalMs, check, signal, cancel }) {
  const pending = new Set(); let failure;
  const guard = () => { check(); if (failure) throw failure; };
  try {
    for (const index of indices) {
      guard(); if (pending.size >= limit) await Promise.race(pending);
      guard(); await sleep(intervalMs, signal); guard();
      const p = Promise.resolve().then(() => launch(index)).catch(e => { failure = e; cancel(); }).finally(() => pending.delete(p));
      pending.add(p);
    }
    await Promise.all(pending); guard();
  } finally { await Promise.allSettled(pending); }
}
