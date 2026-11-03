export function loopback(value, protocols = ['http:', 'ws:']) {
  const u = new URL(value);
  if (!/^\w+:\/\/(127\.0\.0\.1|\[::1\])(?=[:/]|$)/.test(value) || !protocols.includes(u.protocol) || !['127.0.0.1', '[::1]'].includes(u.hostname) || u.username || u.password) {
    throw new Error('Load testing requires a literal loopback URL without credentials');
  }
  return u;
}
export function quantiles(values) {
  const xs = [...values].sort((a,b)=>a-b);
  return Object.fromEntries([50,95,99].map(p=>[`p${p}`, xs.length ? xs[Math.ceil(xs.length*p/100)-1] : null]));
}
// Pass rule modeled on the $12-server challenge: every client-visible latency
// kind has p95 < LOAD_P95_MS (500) and p99 < LOAD_P99_MS (1000), and failed
// operations stay under LOAD_MAX_ERROR_RATE (1%) of all operations. The query
// kind includes the initial task snapshot a user waits for after sign-in.
export const GATED_KINDS = ['save', 'heartbeat', 'reducer', 'query', 'login', 'delivery', 'taskDelivery'];
export function passRule({ latencyMs, errors, connectedUsers, targetUsers, aborted }, env = process.env) {
  const p95 = Number(env.LOAD_P95_MS || 500), p99 = Number(env.LOAD_P99_MS || 1000);
  const maxErrorRate = Number(env.LOAD_MAX_ERROR_RATE || 0.01);
  const failed = Object.values(errors || {}).reduce((a, b) => a + b, 0);
  const operations = ['reducer', 'query', 'login'].reduce((a, k) => a + (latencyMs[k]?.samples || 0), 0) + failed;
  const errorRate = operations ? failed / operations : 1;
  const slow = GATED_KINDS.filter(k => !(latencyMs[k]?.samples > 0) || latencyMs[k].p95 >= p95 || latencyMs[k].p99 >= p99);
  const passed = !aborted && connectedUsers === targetUsers && errorRate < maxErrorRate && slow.length === 0;
  return { passed, errorRate, failingKinds: slow };
}
