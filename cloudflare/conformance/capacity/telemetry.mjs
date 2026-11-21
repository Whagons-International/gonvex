import { shape, integer, list, text, refuse, Refusal, jsonFile, checkedFile, hash } from './binding.mjs';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { readJSON } from './identity.mjs';
import { METRICS, SCOPES } from './profile.mjs';
export { METRICS, SCOPES } from './profile.mjs';
// Local normalized profile, not the serving HTTP schema. Source951's schema is frozen separately for tests/types.
export const CONTRACT_SHA256 = hash(readFileSync(new URL('./telemetry.mjs', import.meta.url)));
export function validateTelemetry(s, b, previous, now = Date.now()) {
  shape(s, ['v','runId','sourceSha','moduleSha256','configSha256','resources','sequence','sampledAtUnixMs','sampler','sourceHistory','observation','induced','services','holders','events','correctness','port','eligibility'], 'telemetry');
  if (s.v !== 1 || s.runId !== b.runId || s.moduleSha256 !== b.deployment.moduleSha256 || JSON.stringify(s.resources) !== JSON.stringify(b.deployment.resources)
    || (s.sourceSha !== null && s.sourceSha !== b.deployment.sourceSha) || (s.configSha256 !== null && s.configSha256 !== b.deployment.configSha256)) refuse('telemetry.observed-identity');
  integer(s.sequence, 0, Number.MAX_SAFE_INTEGER, 'sequence'); integer(s.sampledAtUnixMs, 0, now, 'sampledAtUnixMs');
  shape(s.sampler, ['requests','responses','requestLoss','errors'], 'sampler'); for (const n of Object.values(s.sampler)) integer(n, 0, 64, 'sampler.count');
  if(s.sampler.responses>s.sampler.requests||s.sampler.requestLoss!==s.sampler.requests-s.sampler.responses)refuse('sampler.accounting');
  if (s.sourceHistory !== 'UNKNOWN') refuse('source-history.NOT-exported-by-v1');
  shape(s.induced, ['rpcAttempts','catalogAttempts','controllerAttempts','capacityAttempts','drainStatusAttempts','actualControllerWakeups','actualHolderWakeups','snapshotMayAdvance','socketSampleMayRecord','requestsWithUnknownRpcEffects','controllerReadMayWake','holdersMayWake'], 'induced');
  for (const k of ['rpcAttempts','catalogAttempts','controllerAttempts','capacityAttempts','drainStatusAttempts']) integer(s.induced[k], 0, 640, k);
  integer(s.induced.requestsWithUnknownRpcEffects,0,64,'unknownRequestEffects');
  if(typeof s.induced.controllerReadMayWake!=='boolean'||typeof s.induced.holdersMayWake!=='boolean')refuse('possible-wakeup-label');
  if (s.induced.actualControllerWakeups !== null || s.induced.actualHolderWakeups !== null) refuse('wakeups.NOT-measured-by-v1');
  if (typeof s.induced.snapshotMayAdvance !== 'boolean' || typeof s.induced.socketSampleMayRecord !== 'boolean' || !['cached','bounded-fresh'].includes(s.observation)
    || (s.observation === 'cached' && (s.induced.capacityAttempts || s.induced.drainStatusAttempts || s.induced.snapshotMayAdvance || s.induced.socketSampleMayRecord))) refuse('observation.effects');
  if (now - s.sampledAtUnixMs > b.budget.maxTelemetryAgeMs || (previous && s.sequence <= previous.sequence)) refuse('telemetry.stale/replay');
  list(s.services, 256, 'services', SCOPES.length);
  for (const scope of SCOPES) if (!s.services.some(v => v.scope === scope)) refuse('service.scope-missing');
  for (const v of s.services) {
    shape(v, ['scope','id',...METRICS], 'service'); if (!SCOPES.includes(v.scope)) refuse('service.scope'); text(v.id, 'service.id');
    for (const k of METRICS) if (v[k] !== null && (!Number.isFinite(v[k]) || v[k] < 0 || v[k] > Number.MAX_SAFE_INTEGER)) refuse('service.metric');
    // Native v1 reports application UTF8 rates, NEVER cumulative frame/byte counters.
    if (v.scope === 'gateway' && (v.frames !== null || v.bytes !== null)) refuse('gateway.rate-is-not-count');
  }
  list(s.holders, 2048, 'holders', 0); list(s.events, 2048, 'events', 0); list(s.port.pages, 64, 'port.pages', 0);
  shape(s.correctness, ['duplicateWrites','lostWrites'], 'correctness');
  for (const k of ['duplicateWrites','lostWrites']) if (s.correctness[k] !== null) { integer(s.correctness[k],0,Number.MAX_SAFE_INTEGER,k); if (s.correctness[k]) throw new Refusal('STOPPED','correctness'); }
  shape(s.eligibility, ['boundedExecution','fullCapacity','reasons'], 'eligibility'); list(s.eligibility.reasons, 64, 'eligibility.reasons', 0);
  if(typeof s.eligibility.boundedExecution!=='boolean')refuse('boundedExecution.type');
  if (s.eligibility.fullCapacity !== 'NOT_ELIGIBLE' || !s.eligibility.reasons.includes('SOURCE_EVENT_HISTORY_UNKNOWN')) refuse('v1.full-capacity-not-proven');
  if (!s.eligibility.boundedExecution || (b.telemetry.judgment === 'full-capacity')) throw new Refusal('NOT_ELIGIBLE','required-monitor/full-capacity-facts-unavailable');
  return s;
}
export function validateCost(c, b, previous, now = Date.now()) {
  shape(c, ['v','resourceVersions','identityAsOfUnixMs','sourceSha','configurationSha256','window','facts','attribution'], 'provider-observation');
  if (c.v !== 1 || JSON.stringify(c.resourceVersions) !== JSON.stringify(b.deployment.resources)) refuse('provider-observed-five-component-identity');
  integer(c.identityAsOfUnixMs,0,now,'identityAsOfUnixMs');
  if (now-c.identityAsOfUnixMs > b.budget.maxTelemetryAgeMs || (c.sourceSha !== null && c.sourceSha !== b.deployment.sourceSha)
    || (c.configurationSha256 !== null && c.configurationSha256 !== b.deployment.configSha256)) refuse('provider.identity-stale/mismatch');
  const w=c.window;
  shape(w,['id','startUnixMs','asOfUnixMs','baselineCostUsd','totalCostUsd','coverage'],'provider.window');
  if (w.id !== b.telemetry.providerWindowId) refuse('provider.window-id');
  integer(w.startUnixMs,0,now,'window.start'); integer(w.asOfUnixMs,w.startUnixMs,now,'window.asOf');
  if (![w.baselineCostUsd,w.totalCostUsd].every(n=>Number.isFinite(n)&&n>=0) || w.totalCostUsd<w.baselineCostUsd || now-w.asOfUnixMs>b.budget.maxCostAgeMs) refuse('provider.cost-unavailable/stale');
  if (!['COMPLETE_TO_ASOF','INCOMPLETE','UNKNOWN'].includes(w.coverage)) refuse('provider.coverage');
  if (previous && (w.id!==previous.window.id || w.startUnixMs!==previous.window.startUnixMs || w.baselineCostUsd!==previous.window.baselineCostUsd
    || w.asOfUnixMs<previous.window.asOfUnixMs || w.totalCostUsd<previous.window.totalCostUsd)) refuse('provider.baseline/regression');
  const delta=w.totalCostUsd-w.baselineCostUsd;
  if (delta>=b.budget.maxUsd) throw new Refusal('STOPPED','observed-run-cost-budget');
  if (w.coverage!=='COMPLETE_TO_ASOF') throw new Refusal('NOT_ELIGIBLE','incomplete-provider-cost-coverage');
  return delta;
}
export async function readCost(b, previous) {
  if (!b.telemetry.providerFile) throw new Refusal('UNSUPPORTED','provider-cost-collector');
  const {value}=await jsonFile(b.telemetry.providerFile,1024*1024,true);
  const runDeltaUsd=validateCost(value,b,previous); await checkedFile(value.facts); await checkedFile(value.attribution);
  return {...value,runDeltaUsd}; // Window attribution is separately observed/reviewed, never granted by client runId.
}
export async function sampleTelemetry(b, token, previous, signal, providerObservation, runtimeIdentity) {
  if (!b.telemetry.url||!b.telemetry.adapter) throw new Refusal('UNSUPPORTED','K22-typed-capacity-handoff-adapter');
  await checkedFile(b.telemetry.adapter);
  const adapter=await import(pathToFileURL(b.telemetry.adapter.path).href);
  if(typeof adapter.readCapacity!=='function') throw new Refusal('UNSUPPORTED','capacity-adapter.readCapacity');
  const sample=await adapter.readCapacity({url:b.telemetry.url,bearer:token,signal,fetch,readJSON,binding:b,providerObservation,runtimeIdentity});
  if(Buffer.byteLength(JSON.stringify(sample))>512*1024) refuse('telemetry.byte-bound');
  return validateTelemetry(sample,b,previous);
}
