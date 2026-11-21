/** Missing/censored observations never become zero latency. Intended-send latency is supplied by engine. */
export function distribution(samples) {
  if (samples.some(n => !Number.isFinite(n) || n < 0)) throw new Error('invalid latency sample');
  const sorted = [...samples].sort((a, b) => a - b);
  const q = p => sorted.length ? sorted[Math.ceil(sorted.length * p) - 1] : null;
  return { samples: sorted.length, p50: q(.5), p95: q(.95), p99: q(.99) };
}
