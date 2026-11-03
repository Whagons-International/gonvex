// Merges per-shard generator results (timestamped raw samples) into one result
// with the same scopes and pass rule as an unsharded run: latencyMs covers the
// whole run, steadyLatencyMs only operations that started on the plateau when
// every shard had all its users connected.
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { quantiles, passRule } from './safety.mjs';

const [out, count] = [process.argv[2], Number(process.argv[3])];
const parts = [];
let partial = false;
for (let k = 0; k < count; k++) {
  const file = `${out}.shard-${k}`;
  if (existsSync(file)) parts.push(JSON.parse(readFileSync(file, 'utf8')));
  else if (existsSync(file + '.partial.json')) { partial = true; parts.push(JSON.parse(readFileSync(file + '.partial.json', 'utf8'))); }
  else partial = true;
}
const sum = (key) => parts.reduce((a, p) => a + (p[key] || 0), 0);
const errors = {};
for (const p of parts) for (const [k, v] of Object.entries(p.errors || {})) errors[k] = (errors[k] || 0) + v;
if (parts.length < count) errors['generator shard produced no result'] = count - parts.length;
const samples = {}, paths = {};
for (const p of parts) {
  for (const [k, v] of Object.entries(p.rawSamples || {})) (samples[k] ??= []).push(...v);
  for (const [k, v] of Object.entries(p.rawPaths || {})) {
    const t = (paths[k] ??= { durations: [], errors: 0 });
    t.durations.push(...v.durations);
    t.errors += v.errors;
  }
}
const steadyStartMs = Math.max(...parts.map(p => p.steadyStartMs ?? Infinity));
const steadyEndMs = Math.min(...parts.map(p => p.steadyEndMs ?? -Infinity));
const scope = (filter) => Object.fromEntries(Object.entries(samples).map(([k, v]) => {
  const xs = v.filter(filter).map(x => x.ms);
  return [k, { samples: xs.length, ...quantiles(xs) }];
}));
const sampleCounts = () => Object.fromEntries([...new Set(parts.flatMap(p => Object.keys(p.latencyMs || {})))]
  .map(k => [k, { samples: parts.reduce((a, p) => a + (p.latencyMs?.[k]?.samples || 0), 0) }]));
const latencyMs = partial ? sampleCounts() : scope(() => true);
const steadyLatencyMs = partial ? sampleCounts() : scope(x => x.at >= steadyStartMs && x.at < steadyEndMs);
const first = parts[0] || {};
const result = {
  ...Object.fromEntries(Object.entries(first).filter(([k]) => !['rawSamples', 'rawPaths', 'shard', 'snapshots'].includes(k))),
  shards: count,
  aborted: partial || parts.some(p => p.aborted),
  steadyStartMs,
  steadyEndMs,
  targetUsers: sum('targetUsers'),
  connectedUsers: sum('connectedUsers'),
  snapshots: parts.flatMap(p => p.snapshots || []),
  visibilityChecks: parts.reduce((a, p) => { for (const [k, v] of Object.entries(p.visibilityChecks || {})) a[k] = (a[k] || 0) + v; return a; }, {}),
  pathLatencyMs: Object.fromEntries(Object.entries(paths).map(([k, p]) => [k, { samples: p.durations.length, errors: p.errors, ...quantiles(p.durations) }])),
  errors,
  latencyMs,
  steadyLatencyMs,
};
// Generator self-cost and thin-client counters, per shard and summed.
if (parts.some(p => p.client?.kind !== first.client?.kind || p.schedule?.kind !== first.schedule?.kind)) errors['shards ran different client kinds or schedules'] = 1;
result.generatorCostByShard = parts.map((p, k) => ({ shard: k, users: p.targetUsers, cpuSeconds: p.generatorCost?.cpuSeconds, wallSeconds: p.generatorCost?.wallSeconds, peakRssBytes: p.generatorCost?.peakRssBytes }));
result.generatorCost = { cpuSeconds: parts.reduce((n, p) => n + (p.generatorCost?.cpuSeconds || 0), 0), peakRssBytesSum: parts.reduce((n, p) => n + (p.generatorCost?.peakRssBytes || 0), 0) };
if (first.schedule) result.schedule = { kind: first.schedule.kind, ...Object.fromEntries(['fired', 'overflow', 'skippedDuringReconnect'].map(k => [k, parts.reduce((n, p) => n + (p.schedule?.[k] || 0), 0)])), startLateMsByShard: parts.map(p => p.schedule?.startLateMs) };
if (first.client?.thin) {
  const sumObj = (get) => parts.reduce((a, p) => { for (const [k, v] of Object.entries(get(p) || {})) if (typeof v === 'number') a[k] = (a[k] || 0) + v; return a; }, {});
  result.client = { ...first.client, sdkUsers: parts.reduce((n, p) => n + (p.client?.sdkUsers || 0), 0), thin: { ...sumObj(p => p.client?.thin), unsupported: sumObj(p => p.client?.thin?.unsupported) } };
}
const rule = passRule({ ...result, latencyMs: steadyLatencyMs });
Object.assign(result, { clientSloPassed: rule.passed && !(result.visibilityChecks.mismatched > 0), errorRate: rule.errorRate, failingKinds: rule.failingKinds });
writeFileSync(partial ? out + '.partial.json' : out, JSON.stringify(result, null, 2));
console.log(JSON.stringify({ merged: parts.length, of: count, connectedUsers: result.connectedUsers, passed: result.clientSloPassed }));
