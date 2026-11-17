import { quantiles } from './safety.mjs';

// User IDs remain global, so the existing tenant/user assignment is unchanged.
export function mixedPlan(target = 1000) {
  if (target !== 1000) throw Error('mixed-v1 requires exactly 1000 unique users');
  const cohorts = [
    { name: 'spread', first: 0, count: 700, start: 0, duration: 900000 },
    { name: 'burst-75-a', first: 700, count: 75, start: 180000, duration: 5000 },
    { name: 'burst-75-b', first: 775, count: 75, start: 450000, duration: 5000 },
    { name: 'burst-150', first: 850, count: 150, start: 720000, duration: 5000 },
  ];
  return cohorts.flatMap(c => Array.from({ length: c.count }, (_, j) => ({
    user: c.first + j, cohort: c.name, at: c.start + j * c.duration / c.count,
  })));
}
export const mixedWindows = [
  { name: 'burst-75-a', start: 180000, end: 185000, users: 75 },
  { name: 'burst-75-b', start: 450000, end: 455000, users: 75 },
  { name: 'burst-150', start: 720000, end: 725000, users: 150 },
  { name: 'mixed-ramp', start: 0, end: 900000, users: 1000 },
];

export function qualifyMixed({ started, signIns, samples, errors, retries = [], attempts = [] }) {
  return mixedWindows.map(w => {
    const start = started + w.start, end = started + w.end;
    const joins = signIns.filter(s => w.name === 'mixed-ramp' || s.cohort === w.name);
    const latencyMs = Object.fromEntries(Object.entries(samples).map(([k, xs]) => {
      // Attribute by operation START; retain completions after the window ends.
      const v = xs.filter(x => x.at >= start && x.at < end).map(x => x.ms);
      return [k, { samples: v.length, ...quantiles(v) }];
    }));
    const joinLatency = joins.filter(s => Number.isFinite(s.authAt)).map(s => s.authAt - s.plannedAt);
    const readiness = joins.filter(s => Number.isFinite(s.readyAt)).map(s => s.readyAt - s.plannedAt);
    latencyMs.signIn = { samples: joinLatency.length, ...quantiles(joinLatency) };
    latencyMs.readiness = { samples: readiness.length, ...quantiles(readiness) };
    const failed = errors.filter(e => e.at >= start && e.at < end).length;
    const operations = ['reducer', 'query', 'login'].reduce((n, k) => n + (latencyMs[k]?.samples || 0), 0);
    const errorRate = operations + failed ? failed / (operations + failed) : 1;
    const gated = ['signIn', 'readiness', 'save', 'heartbeat', 'reducer', 'query', 'login', 'delivery', 'taskDelivery'];
    const slow = gated.filter(k => latencyMs[k]?.samples > 0 && (latencyMs[k].p95 >= 500 || latencyMs[k].p99 >= 1000));
    const readyUsers = readiness.length;
    const pendingOperations = attempts.filter(a=>a.at>=start&&a.at<end&&!a.done).length;
    return { name: w.name, startMs: start, endMs: end, expectedUsers: w.users, attemptedUsers: joins.filter(s=>Number.isFinite(s.attemptedAt)).length,
      authenticatedUsers: joinLatency.length, readyUsers, latencyMs, pendingOperations, errors: failed, errorRate,
      retryCounts: Object.fromEntries(['refused','retry','recovered'].map(type=>[type,retries.filter(e=>e.type===type&&e.at>=start&&e.at<end).length])),
      recoveredAfterWindow: retries.filter(e=>e.type==='recovered'&&e.failedAt>=start&&e.failedAt<end&&e.at>=end).length, failingKinds: slow,
      passed: !pendingOperations && joins.filter(s=>Number.isFinite(s.attemptedAt)).length === w.users && joinLatency.length === w.users && readyUsers === w.users && errorRate < 0.01 && !slow.length };
  });
}
