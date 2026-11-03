// Merges per-shard generator results (raw samples) into one result with the
// same pass rule as an unsharded run.
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { quantiles, passRule } from './safety.mjs';
const [out, count] = [process.argv[2], Number(process.argv[3])];
const parts = [];
let partial = false;
for (let k = 0; k < count; k++) {
  const file = `${out}.shard-${k}`;
  if (existsSync(file)) parts.push(JSON.parse(readFileSync(file, 'utf8')));
  else if (existsSync(file + '.partial.json')) { partial = true; parts.push(JSON.parse(readFileSync(file + '.partial.json', 'utf8'))); }
  else { partial = true; }
}
const sum = (key) => parts.reduce((a, p) => a + (p[key] || 0), 0);
const errors = {};
for (const p of parts) for (const [k, v] of Object.entries(p.errors || {})) errors[k] = (errors[k] || 0) + v;
if (parts.length < count) errors['generator shard produced no result'] = count - parts.length;
const raw = {}, paths = {};
for (const p of parts) {
  for (const [k, v] of Object.entries(p.raw || {})) (raw[k] ??= []).push(...v);
  for (const [k, v] of Object.entries(p.rawPaths || {})) { const t = (paths[k] ??= { durations: [], errors: 0 }); t.durations.push(...v.durations); t.errors += v.errors; }
}
const latencyMs = partial
  ? Object.fromEntries([...new Set(parts.flatMap(p => Object.keys(p.latencyMs || {})))].map(k => [k, { samples: parts.reduce((a, p) => a + (p.latencyMs?.[k]?.samples || 0), 0) }]))
  : Object.fromEntries(Object.entries(raw).map(([k, v]) => [k, { samples: v.length, ...quantiles(v) }]));
const first = parts[0] || {};
const result = {
  ...Object.fromEntries(Object.entries(first).filter(([k]) => !['raw', 'rawPaths', 'shard', 'snapshots'].includes(k))),
  shards: count,
  aborted: partial || parts.some(p => p.aborted),
  steadyStartMs: Math.max(...parts.map(p => p.steadyStartMs || 0)),
  steadyEndMs: Math.min(...parts.map(p => p.steadyEndMs || Infinity)),
  targetUsers: sum('targetUsers'),
  connectedUsers: sum('connectedUsers'),
  snapshots: parts.flatMap(p => p.snapshots || []),
  pathLatencyMs: Object.fromEntries(Object.entries(paths).map(([k, p]) => [k, { samples: p.durations.length, errors: p.errors, ...quantiles(p.durations) }])),
  errors,
  latencyMs,
};
const rule = passRule(result);
Object.assign(result, { clientSloPassed: rule.passed, errorRate: rule.errorRate, failingKinds: rule.failingKinds });
writeFileSync(partial ? out + '.partial.json' : out, JSON.stringify(result, null, 2));
console.log(JSON.stringify({ merged: parts.length, of: count, connectedUsers: result.connectedUsers, passed: result.clientSloPassed }));
