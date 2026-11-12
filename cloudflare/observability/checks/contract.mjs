import { randomUUID } from 'node:crypto';

export class CheckError extends Error {
  constructor(code, status = 'fail') { super(code); this.code = code; this.status = ['fail','unknown','blocked'].includes(status) ? status : 'fail'; }
}
export const requireCheck = (ok, code, status) => { if (!ok) throw new CheckError(code, status); };
export const ids = () => ({ operationId: randomUUID(), traceId: randomUUID().replaceAll('-', ''), idempotencyKey: randomUUID() });
export const finite = (n, min = 0) => Number.isFinite(n) && n >= min;
export function validateContext(c) {
  requireCheck(c?.environment === 'local-synthetic' || c?.environment === 'dev-synthetic', 'environment-blocked', 'blocked');
  requireCheck(c.syntheticOnly === true && c.sideEffectsAudited === true && c.coldFixture === false, 'side-effect-preflight', 'blocked');
  requireCheck(c.tenantAlias?.startsWith('synthetic-'), 'synthetic-tenant-required', 'blocked');
  const roles = c.identities;
  requireCheck(roles && ['writer', 'reader', 'unauthorized'].every(k => typeof roles[k] === 'string' && roles[k].startsWith('synthetic-')), 'identities-required', 'blocked');
  requireCheck(new Set(Object.values(roles)).size === 3 && c.operatorCredentials === false, 'least-privilege-required', 'blocked');
  requireCheck(['sourceSha', 'deploy', 'region', 'executor', 'configSha'].every(k => typeof c[k] === 'string' && /^[a-zA-Z0-9._-]{1,128}$/.test(c[k])), 'provenance-required', 'blocked');
  requireCheck(['local-handler-fixture', 'real-sdk', 'real-browser'].includes(c.domain), 'domain-required', 'blocked');
  requireCheck(finite(c.scheduledAt) && finite(c.clockUncertaintyMs ?? 0) && [0,1].includes(c.attempt ?? 0),'schedule-required','blocked');
  requireCheck(finite(c.deadlineMs, 1) && c.deadlineMs <= 120000, 'deadline-required', 'blocked');
}

/** Persist state via save before admitting effects. One guard per tenant across regions/checks.
 * Single owner only: production scheduling needs an external serialized ledger/lease, not per-run memory.
 */
export class WriteGuard {
  constructor(state, save) { this.state = state; this.save = save; }
  async begin(now, fixtureId) {
    const s = this.state;
    requireCheck(!s.paused && !s.active && s.cleanupHealthy === true, 'writes-paused', 'unknown');
    requireCheck(Number.isSafeInteger(s.remainingWrites) && s.remainingWrites >= 2 && s.fixtures.length < 100 && s.fixtures.every(x => now - x.createdAt < 86400000), 'fixture-budget', 'unknown');
    s.active = fixtureId; s.remainingWrites -= 2;
    s.fixtures.push({ id: fixtureId, createdAt: now });
    try { await this.save(structuredClone(s)); } catch { s.paused = true; throw new CheckError('ledger-unavailable', 'unknown'); }
  }
  async finish(fixtureId, cleaned) {
    const s = this.state;
    if (cleaned) s.fixtures = s.fixtures.filter(x => x.id !== fixtureId);
    else { s.paused = true; s.cleanupHealthy = false; }
    s.active = null;
    try { await this.save(structuredClone(s)); } catch { s.paused = true; throw new CheckError('ledger-unavailable', 'unknown'); }
  }
}

/** Global deadline includes all assertions; adapters MUST honor abort and settle before lease release.
 * A hung adapter remains charged/paused; cleanup is attempted only after actual settlement.
 */
export async function runCheck(c, check, adapter, guard) {
  validateContext(c);
  const operations = [];
  const runIds = ids(), controller = new AbortController(), start = performance.now();
  const raw = { schema: 'gonvex.monitor.v1', ...Object.fromEntries(['sourceSha','deploy','region','executor','configSha','domain','environment'].map(k => [k,c[k]])), check, scheduledAt: c.scheduledAt, startedAt: Date.now(), deadlineMs: c.deadlineMs, clockUncertaintyMs: c.clockUncertaintyMs ?? 0, attempt: c.attempt ?? 0, ...runIds };
  let timer;
  const execution = Promise.resolve().then(() => adapter({ ...c, ...runIds, signal: controller.signal, guard, operations }));
  try {
    const outcome = await Promise.race([execution, new Promise((_, reject) => { timer = setTimeout(() => { controller.abort(); if (guard) guard.state.paused = true; reject(new CheckError('deadline', 'fail')); }, c.deadlineMs); })]);
    raw.status = 'pass'; raw.code = 'ok';
    // Only enumerated safe evidence leaves the adapter; arbitrary exceptions and data never escape.
    raw.operations = operations;
  } catch (e) {
    raw.status = e instanceof CheckError ? e.status : 'fail';
    raw.code = e instanceof CheckError && /^[a-z-]{1,64}$/.test(e.code) ? e.code : 'adapter-error';
  } finally { clearTimeout(timer); }
  raw.operations = operations.map(o => ({ ...o }));
  raw.durationMs = performance.now() - start; raw.finishedAt = Date.now();
  return raw;
}
