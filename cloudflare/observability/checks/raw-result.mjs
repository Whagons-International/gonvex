/** Dependency-free, one-way privacy/integrity boundary shared by producer and replay. */
const integrityCodes=new Set(['raw-schema','raw-provenance','raw-outcome','raw-timing','raw-ids','raw-settlement','raw-settlement-timing','raw-cleanup','raw-deadline','raw-observed-status','raw-observed-code','operation-cap','operation-array','operation-schema','operation-id','operation-timing','operation-elapsed','operation-clock','operation-observation','operation-elapsed-order','operation-success-proof','operation-deadline-contract','raw-integrity']);
export class RawIntegrityError extends Error {
  constructor(code) { const safe=integrityCodes.has(code)?code:'raw-integrity'; super(safe); this.name='RawIntegrityError'; this.code=safe; this.status='unknown'; }
}
const requireRaw=(ok,code)=>{if(!ok) throw new RawIntegrityError(code);};
const bounded=(value,min=0)=>typeof value==='number' && Number.isFinite(value) && value>=min && value<=Number.MAX_SAFE_INTEGER;
function plainData(value,allowed,code) {
  requireRaw(value!==null && typeof value==='object' && [Object.prototype,null].includes(Object.getPrototypeOf(value)),code);
  const descriptors=Object.getOwnPropertyDescriptors(value);
  requireRaw(Reflect.ownKeys(descriptors).every(key=>typeof key==='string' && allowed.includes(key) && Object.hasOwn(descriptors[key],'value')),code);
  return Object.fromEntries(Object.keys(descriptors).map(key=>[key,descriptors[key].value]));
}
const keys = ['schema','sourceSha','deploy','region','executor','configSha','domain','environment','check','scheduledAt','startedAt','finishedAt','deadlineMs','clockUncertaintyMs','attempt','operationId','traceId','idempotencyKey','status','code','durationMs','operations','settlement','settledElapsedMs','cleanupState','deadlineExceeded','observedStatus','observedCode'];
/** For validated scalar records only. An offset shares the check's monotonic
 * origin; its wall projection still carries the recorded clock uncertainty.
 * Undefined preserves the explicitly labeled legacy timing fallback. */
export const operationAttemptAt=(record,operation)=>operation.attemptedAt
  ?? (operation.attemptedAfterMs===undefined ? undefined : record.startedAt+operation.attemptedAfterMs);
/** Strict ingress: arbitrary provider bodies/errors/headers cannot enter retained raw records. */
function validateRecord(r) {
  r=plainData(r,keys,'raw-schema');
  requireRaw(r && Object.keys(r).every(k=>keys.includes(k)) && r.schema === 'gonvex.monitor.v1','raw-schema');
  requireRaw(['sourceSha','deploy','region','executor','configSha','domain','environment','check','code'].every(k=>typeof r[k]==='string' && /^[a-zA-Z0-9._-]{1,128}$/.test(r[k])),'raw-provenance');
  requireRaw(['pass','fail','unknown','blocked','export-failed','paused'].includes(r.status) && [0,1].includes(r.attempt),'raw-outcome');
  requireRaw(['scheduledAt','startedAt','finishedAt','deadlineMs','clockUncertaintyMs','durationMs'].every(k=>bounded(r[k])) && r.deadlineMs>0 && r.finishedAt>=r.startedAt,'raw-timing');
  requireRaw(typeof r.traceId==='string' && /^[a-f0-9]{32}$/.test(r.traceId) && [r.operationId,r.idempotencyKey].every(x=>typeof x==='string' && /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(x)),'raw-ids');
  if (Object.hasOwn(r,'settlement')) requireRaw(['fulfilled','rejected','unsettled'].includes(r.settlement),'raw-settlement');
  if (Object.hasOwn(r,'settledElapsedMs')) requireRaw(bounded(r.settledElapsedMs) && r.settledElapsedMs<=r.durationMs && r.settlement !== 'unsettled','raw-settlement-timing');
  if (Object.hasOwn(r,'cleanupState')) requireRaw(['not-applicable','unknown','failed','retained','ledger-clear'].includes(r.cleanupState),'raw-cleanup');
  if (Object.hasOwn(r,'deadlineExceeded')) requireRaw(typeof r.deadlineExceeded === 'boolean','raw-deadline');
  if (Object.hasOwn(r,'observedStatus')) requireRaw(['pass','fail','unknown','blocked'].includes(r.observedStatus),'raw-observed-status');
  if (Object.hasOwn(r,'observedCode')) requireRaw(typeof r.observedCode==='string' && /^[a-z-]{1,64}$/.test(r.observedCode),'raw-observed-code');
  requireRaw(Array.isArray(r.operations) && Object.getPrototypeOf(r.operations)===Array.prototype,'operation-cap');
  const array=Object.getOwnPropertyDescriptors(r.operations), count=array.length?.value;
  requireRaw(Number.isInteger(count) && count>=0 && count<=2,'operation-cap');
  requireRaw(Reflect.ownKeys(array).every(k=>k==='length'||typeof k==='string'&&/^[01]$/.test(k)&&Number(k)<count),'operation-array');
  const safeOperations=[];
  for (let i=0;i<count;i++) {
    requireRaw(array[i] && Object.hasOwn(array[i],'value'),'operation-array');
    const o=plainData(array[i].value,['operationId','kind','attempted','durableMs','appliedMs','deadlineMs','status','attemptedAt','attemptedAfterMs'],'operation-schema');
    requireRaw(Object.keys(o).every(k=>['operationId','kind','attempted','durableMs','appliedMs','deadlineMs','status','attemptedAt','attemptedAfterMs'].includes(k)) && ['create','update'].includes(o.kind) && o.attempted === true && ['pass','fail'].includes(o.status),'operation-schema');
    requireRaw(typeof o.operationId==='string' && /^[a-f0-9-]{36}$/.test(o.operationId) && bounded(o.deadlineMs,1),'operation-id');
    if (o.attemptedAt !== undefined) requireRaw(bounded(o.attemptedAt) && o.attemptedAt>=r.startedAt-r.clockUncertaintyMs && o.attemptedAt<=r.finishedAt+r.clockUncertaintyMs,'operation-timing');
    for (const key of ['durableMs','appliedMs','attemptedAfterMs']) if (Object.hasOwn(o,key)) requireRaw(bounded(o[key]) && o[key]<=r.durationMs,o.status==='pass'?'operation-success-proof':'operation-elapsed');
    if (Object.hasOwn(o,'attemptedAt')) requireRaw(bounded(o.attemptedAt),'operation-timing');
    if (o.attemptedAfterMs !== undefined && o.attemptedAt !== undefined) requireRaw(Math.abs(o.attemptedAt-r.startedAt-o.attemptedAfterMs)<=r.clockUncertaintyMs,'operation-clock');
    // A failed attempt may have no completion timings. Its own offset must
    // still fit both clocks, independently of durable/application evidence.
    if (o.attemptedAfterMs !== undefined) requireRaw(bounded(r.startedAt+o.attemptedAfterMs)
      && r.startedAt+o.attemptedAfterMs<=r.finishedAt+r.clockUncertaintyMs,'operation-observation');
    for (const key of ['durableMs','appliedMs']) if (Object.hasOwn(o,key)) {
      const offset=o.attemptedAfterMs ?? Math.max(0,(o.attemptedAt ?? r.startedAt)-r.startedAt-r.clockUncertaintyMs);
      requireRaw(offset+o[key]<=r.durationMs,'operation-observation');
      if (o.attemptedAt !== undefined) requireRaw(o.attemptedAt+o[key]<=r.finishedAt+r.clockUncertaintyMs,'operation-observation');
      // Legacy and offset-only operations still cannot complete before the
      // earliest possible attempt fits the actual wall observation. Monotonic
      // duration alone cannot certify an application beyond finishedAt/asOf.
      else requireRaw(r.startedAt+(o.attemptedAfterMs ?? 0)+o[key]<=r.finishedAt+r.clockUncertaintyMs,'operation-observation');
    }
    if (o.durableMs !== undefined && o.appliedMs !== undefined) requireRaw(o.durableMs<=o.appliedMs,'operation-elapsed-order');
    if (o.status==='pass') requireRaw(bounded(o.durableMs) && bounded(o.appliedMs) && o.durableMs<=o.appliedMs && o.appliedMs<=o.deadlineMs && o.appliedMs<=r.durationMs,'operation-success-proof');
    requireRaw(o.deadlineMs<=r.deadlineMs,'operation-deadline-contract');
    safeOperations.push(o);
  }
  r.operations=safeOperations;
  return r;
}
export function validateRaw(value) {
  try { return validateRecord(value); }
  catch(error) {
    // Never echo input values, getter/proxy exceptions, provider errors, headers or bodies.
    let code='raw-integrity';
    try {
      const descriptor=error instanceof RawIntegrityError ? Object.getOwnPropertyDescriptor(error,'code') : null;
      if (descriptor && Object.hasOwn(descriptor,'value') && integrityCodes.has(descriptor.value)) code=descriptor.value;
    } catch {}
    throw new RawIntegrityError(code);
  }
}
