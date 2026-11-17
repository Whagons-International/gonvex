import { createHmac, createHash } from 'node:crypto';
import { requireCheck } from '../checks/contract.mjs';
export const stages = ['browser-intent','edge','admission','gateway-queue','tenant-queue','auth','reducer','sqlite-transaction','durable-commit','receipt','ordered-delivery','second-client-applied','action-claim','workflow-step','scheduler','outbox'];
export const limits = Object.freeze({ healthyRate:0.01, spansPerSegment:64, attributeBytes:1024, spansPerDeployDay:20000, bytesPerDeployDay:20*1024*1024, diagnosticDays:7, rawDays:35, aggregateDays:90 });
const hex = (x,n) => typeof x === 'string' && new RegExp(`^[a-f0-9]{${n}}$`).test(x);
const uuid = x => typeof x === 'string' && /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(x);
const bytes = x => Buffer.byteLength(JSON.stringify(x));
export function alias(secret, raw) {
  requireCheck(typeof secret === 'string' && secret.length >= 32 && typeof raw === 'string' && raw.length <= 1024, 'alias-input');
  return createHmac('sha256',secret).update(raw).digest('hex').slice(0,32);
}
/** Versioned internal diagnostic links. Does not alter SDK frames, auth or receipt/idempotency semantics. */
export function envelope({ traceId, operationId, spanId, parentSpanId, stage, attempt, links = [], attributes = {}, provenance }) {
  requireCheck(hex(traceId,32) && uuid(operationId) && hex(spanId,16) && (!parentSpanId || hex(parentSpanId,16)), 'correlation-id');
  requireCheck(traceId !== operationId.replaceAll('-','') && stages.includes(stage) && Number.isSafeInteger(attempt) && attempt >= 0 && attempt <= 1, 'correlation-fields');
  requireCheck(Array.isArray(links) && links.length <= 8, 'link-cap');
  const safeProvenance={};
  if(provenance) {
    requireCheck(Object.keys(provenance).every(k=>['sourceSha','deploy','environment','region','executor'].includes(k)),'provenance-fields');
    for(const [k,v] of Object.entries(provenance)) {requireCheck(typeof v==='string' && /^[a-zA-Z0-9._-]{1,128}$/.test(v),'provenance-value');safeProvenance[k]=v;}
  }
  const safeLinks = links.map(l => {
    requireCheck(Object.keys(l).every(k => ['traceId','spanId','kind'].includes(k)) && hex(l.traceId,32) && hex(l.spanId,16) && ['parent','retry','action','workflow','scheduler','outbox'].includes(l.kind), 'unsafe-link');
    return { traceId:l.traceId,spanId:l.spanId,kind:l.kind };
  });
  const safe = {};
  for (const [key,value] of Object.entries(attributes)) {
    if (['queueMs','durationMs','revision','epoch','rowsRead','rowsWritten','bytes','clockUncertaintyMs'].includes(key)) requireCheck(Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER, 'unsafe-number');
    else if (['tenantAlias','gatewayAlias','controllerAlias'].includes(key)) requireCheck(hex(value,32), 'unsafe-alias');
    else if (key === 'outcome') requireCheck(['ok','rejected','timeout','unknown','obsolete'].includes(value), 'unsafe-outcome');
    else throw new Error('attribute-not-allowlisted');
    safe[key] = value;
  }
  requireCheck(bytes(safe) <= limits.attributeBytes, 'attribute-byte-cap');
  return { schema:'gonvex.correlation.v1',traceId,operationId,spanId,...(parentSpanId ? { parentSpanId } : {}),stage,attempt,provenance:safeProvenance,links:safeLinks,attributes:safe };
}
export function metricLabels(labels) {
  const allowed = { component:['Web','API','Sync','Files','Background','Monitoring'], environment:['local-synthetic','dev-synthetic'],region:['local','us-east-1','us-west-1','eu-west-1'],releaseClass:['candidate','baseline'],failureClass:['none','http','auth','write','sync','job','file','resource','heartbeat','unknown'] };
  requireCheck(Object.keys(labels).every(k => allowed[k]?.includes(labels[k])), 'unbounded-metric-label');
  return { ...labels };
}
/** One ledger per deployment/day, persisted by owner. No per-tenant metrics or unbounded segment map.
 * Segment arrays are admitted atomically; exporter must settle. Counters count outcomes even if unsampled.
 */
export class TraceBudget {
  constructor(state, persist) { this.state = state; this.persist = persist; this.busy = false; }
  async export(segment, outcome, sink) {
    requireCheck(!this.busy, 'export-singleflight'); this.busy = true;
    const s = this.state;
    try {
      requireCheck(['ok','failure','slow'].includes(outcome), 'export-outcome');
      s.counters.outcomes++;
      requireCheck(Array.isArray(segment) && segment.length > 0 && segment.length <= limits.spansPerSegment, 'span-cap');
      const safe = segment.map(envelope);
      requireCheck(safe.every(x=>x.traceId===safe[0].traceId && x.operationId===safe[0].operationId), 'mixed-segment');
      const sample = parseInt(createHash('sha256').update(safe[0].traceId).digest('hex').slice(0,8),16)/0x100000000;
      if (outcome === 'ok' && sample >= limits.healthyRate) { s.counters.sampledOut++; return false; }
      const n = bytes(safe);
      if (s.failed || s.spans + safe.length > limits.spansPerDeployDay || s.bytes + n > limits.bytesPerDeployDay) { s.counters.dropped += safe.length; return false; }
      // Reserve before export; failed sends still spend the budget, never retry unboundedly.
      s.spans += safe.length; s.bytes += n;
      try { await this.persist(structuredClone(s)); } catch { s.failed = true; s.counters.exportFailed += safe.length; return false; }
      try { await sink(safe); s.counters.exported += safe.length; return true; }
      catch { s.counters.exportFailed += safe.length; return false; }
    } finally {
      try { await this.persist(structuredClone(s)); } catch { s.failed = true; }
      this.busy = false;
    }
  }
}
/** Local monotonic duration only. Cross-process interval retains both clock error bounds. */
export const localDuration = (start,end) => { requireCheck(Number.isFinite(start) && Number.isFinite(end) && end>=start,'clock'); return end-start; };
export function crossClockBounds(a,b) {
  for (const x of [a,b]) requireCheck(Number.isFinite(x.wallMs) && Number.isFinite(x.uncertaintyMs) && x.uncertaintyMs>=0,'clock');
  return { lowerMs:b.wallMs-a.wallMs-a.uncertaintyMs-b.uncertaintyMs, upperMs:b.wallMs-a.wallMs+a.uncertaintyMs+b.uncertaintyMs, synchronized:false };
}
