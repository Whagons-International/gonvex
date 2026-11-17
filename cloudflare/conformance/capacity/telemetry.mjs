import { shape, integer, list, text, refuse, Refusal, jsonFile, checkedFile, hash } from './binding.mjs';
import { readFileSync } from 'node:fs';
export const METRICS = ['sockets', 'frames', 'bytes', 'deliveryLagMs', 'queueAgeMs', 'rpcMs', 'cpuMs', 'isolateMemoryBytes', 'sqlStatements', 'rowsRead', 'rowsWritten', 'backlog'];
export const SCOPES = ['writer', 'control', 'auth', 'limiter', 'routing', 'gateway', 'builder', 'workflow', 'sandbox'];
// Driver input profile, NOT an assertion that a serving endpoint implements it. K22 owns that adapter contract.
export const CONTRACT_SHA256 = hash(readFileSync(new URL('./telemetry.mjs', import.meta.url)));
export function validateTelemetry(s, b, previous, now = Date.now()) {
  shape(s, ['v', 'runId', 'sourceSha', 'moduleSha256', 'configSha256', 'resources', 'sequence', 'sampledAtUnixMs', 'sampleLoss', 'observation', 'induced', 'services', 'holders', 'events', 'correctness'], 'telemetry');
  if (s.v !== 1 || s.runId !== b.runId || s.sourceSha !== b.deployment.sourceSha || s.moduleSha256 !== b.deployment.moduleSha256 || s.configSha256 !== b.deployment.configSha256
    || JSON.stringify(s.resources) !== JSON.stringify(b.deployment.resources)) refuse('telemetry.deployment-identity');
  integer(s.sequence, 0, Number.MAX_SAFE_INTEGER, 'telemetry.sequence');
  if (s.sampleLoss !== null) integer(s.sampleLoss, 0, Number.MAX_SAFE_INTEGER, 'sampleLoss');
  integer(s.sampledAtUnixMs, 0, now, 'sampledAtUnixMs');
  shape(s.induced, ['rpc', 'wakeups', 'snapshots'], 'induced');
  for (const value of Object.values(s.induced)) integer(value, 0, 2048, 'induced.work');
  if (!['cached', 'bounded-fresh'].includes(s.observation) || (s.observation === 'cached' && Object.values(s.induced).some(v => v !== 0))) refuse('observation.effects');
  if (now - s.sampledAtUnixMs > b.budget.maxTelemetryAgeMs || s.sampleLoss === null || s.sampleLoss > 0
    || (previous && (s.sequence <= previous.sequence || s.sampledAtUnixMs < previous.sampledAtUnixMs))) refuse('telemetry.stale/unknown-loss/regression');
  list(s.services, 256, 'services', SCOPES.length);
  for (const scope of SCOPES) if (!s.services.some(v => v.scope === scope)) refuse(`telemetry.${scope}`);
  for (const v of s.services) {
    shape(v, ['scope', 'id', ...METRICS], 'service'); if (!SCOPES.includes(v.scope)) refuse('service.scope'); text(v.id, 'service.id');
    for (const k of METRICS) if (v[k] !== null && (!Number.isFinite(v[k]) || v[k] < 0 || v[k] > Number.MAX_SAFE_INTEGER)) refuse('service.metric');
    if (v.scope === 'gateway' && ['sockets', 'frames', 'bytes', 'deliveryLagMs', 'queueAgeMs', 'rpcMs'].some(k => v[k] === null)) refuse('gateway.pressure-unavailable');
  }
  list(s.holders, 2048, 'holders', 0);
  list(s.events, 2048, 'events', 0);
  for (const e of s.events) {
    shape(e, ['controller', 'holder', 'epoch', 'at', 'action'], 'scale-event'); text(e.controller, 'controller'); text(e.holder, 'holder');
    integer(e.epoch, 0, Number.MAX_SAFE_INTEGER, 'epoch'); integer(e.at, 0, now, 'at');
    if (!['allocate', 'drain', 'retire'].includes(e.action)) refuse('scale-event.action');
  }
  for (const h of s.holders) {
    shape(h, ['controller', 'holder', 'epoch', 'at', 'mode', 'windowInstance', 'windowSequence', 'windowStart', 'windowEnd', 'drainBlocked'], 'holder');
    text(h.controller, 'controller'); text(h.holder, 'holder');
    if (!['active', 'draining', 'retired'].includes(h.mode)) refuse('holder.mode');
    for (const k of ['epoch', 'at', 'windowInstance', 'windowSequence', 'windowStart', 'windowEnd', 'drainBlocked']) if (h[k] !== null) integer(h[k], 0, Number.MAX_SAFE_INTEGER, k);
    if (h.epoch === null) refuse('holder.epoch');
  }
  shape(s.correctness, ['duplicateWrites', 'lostWrites'], 'correctness');
  for (const k of ['duplicateWrites', 'lostWrites']) if (s.correctness[k] !== null) { integer(s.correctness[k], 0, Number.MAX_SAFE_INTEGER, k); if (s.correctness[k]) throw new Refusal('STOPPED', 'correctness'); }
  return s;
}
export function validateCost(c, b, previous, now = Date.now()) {
  shape(c, ['v', 'resourceVersions', 'costAsOfUnixMs', 'measuredUsd', 'facts', 'attribution'], 'provider-cost');
  if (c.v !== 1 || JSON.stringify(c.resourceVersions) !== JSON.stringify(b.deployment.resources)) refuse('provider-cost.resource-identity');
  integer(c.costAsOfUnixMs, 0, now, 'costAsOfUnixMs');
  if (now - c.costAsOfUnixMs > b.budget.maxCostAgeMs || !Number.isFinite(c.measuredUsd) || c.measuredUsd < 0
    || (previous && (c.costAsOfUnixMs < previous.costAsOfUnixMs || c.measuredUsd < previous.measuredUsd))) refuse('provider-cost.unavailable/stale/regressed');
  if (c.measuredUsd >= b.budget.maxUsd) throw new Refusal('STOPPED', 'observed-cost-budget');
  return c;
}
export async function readCost(b, previous) {
  if (!b.telemetry.providerFile) throw new Refusal('UNSUPPORTED', 'provider-cost-collector');
  const { value } = await jsonFile(b.telemetry.providerFile, 1024 * 1024, true);
  validateCost(value, b, previous); await checkedFile(value.facts); await checkedFile(value.attribution);
  return value; // Attribution is the separately reviewed collector's evidence, never client runId authority.
}
export async function sampleTelemetry(b, token, previous, signal) {
  if (!b.telemetry.url) throw new Refusal('UNSUPPORTED', 'authenticated-assembled-capacity-telemetry-v1');
  const response = await fetch(b.telemetry.url, { signal, redirect: 'error', headers: { authorization: `Bearer ${token}`, 'x-gonvex-capacity-run': b.runId } });
  if (!response.ok || !response.body) refuse('telemetry.response');
  const reader = response.body.getReader(); let length = 0; const parts = [];
  try { for (;;) { const { done, value } = await reader.read(); if (done) break; length += value.byteLength;
    if (length > 512 * 1024) refuse('telemetry.byte-bound'); parts.push(value); }
  } finally { await reader.cancel(); }
  return validateTelemetry(JSON.parse(Buffer.concat(parts).toString('utf8')), b, previous);
}
