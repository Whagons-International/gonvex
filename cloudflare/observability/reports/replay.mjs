import { requireCheck, finite } from '../checks/contract.mjs';
const keys = ['schema','sourceSha','deploy','region','executor','configSha','domain','environment','check','scheduledAt','startedAt','finishedAt','deadlineMs','clockUncertaintyMs','attempt','operationId','traceId','idempotencyKey','status','code','durationMs','operations'];
/** Strict ingress: arbitrary provider bodies/errors/headers cannot enter retained raw records. */
export function validateRaw(r) {
  requireCheck(r && Object.keys(r).every(k=>keys.includes(k)) && r.schema === 'gonvex.monitor.v1','raw-schema');
  requireCheck(['sourceSha','deploy','region','executor','configSha','domain','environment','check','code'].every(k=>typeof r[k]==='string' && /^[a-zA-Z0-9._-]{1,128}$/.test(r[k])),'raw-provenance');
  requireCheck(['pass','fail','unknown','blocked','export-failed','paused'].includes(r.status) && [0,1].includes(r.attempt),'raw-outcome');
  requireCheck(['scheduledAt','startedAt','finishedAt','deadlineMs','clockUncertaintyMs','durationMs'].every(k=>finite(r[k])) && r.deadlineMs>0 && r.finishedAt>=r.startedAt,'raw-timing');
  requireCheck(/^[a-f0-9]{32}$/.test(r.traceId) && [r.operationId,r.idempotencyKey].every(x=>typeof x==='string' && /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(x)),'raw-ids');
  requireCheck(Array.isArray(r.operations) && r.operations.length<=2,'operation-cap');
  for (const o of r.operations) {
    requireCheck(Object.keys(o).every(k=>['operationId','kind','attempted','durableMs','appliedMs','deadlineMs','status'].includes(k)) && ['create','update'].includes(o.kind) && o.attempted === true && ['pass','fail'].includes(o.status),'operation-schema');
    requireCheck(typeof o.operationId==='string' && /^[a-f0-9-]{36}$/.test(o.operationId) && finite(o.deadlineMs,1),'operation-id');
    if (o.status==='pass') requireCheck(finite(o.durableMs) && finite(o.appliedMs) && o.durableMs<=o.deadlineMs && o.appliedMs<=o.deadlineMs,'operation-success-proof');
  }
  return r;
}
const inside = (t, windows) => windows.some(w=>t>=w.start && t<w.end);
/** One check/component/region/deploy per call. Scheduled slots use the explicit piecewise-constant
 * estimator, NOT continuous-health proof. Missing, late, provider-heartbeat loss are UNKNOWN.
 * Retries corroborate but never replace the first attempt. No cross-region quorum.
 */
export function replay({ schedule, records, heartbeat = [], maintenance = [], asOf }) {
  const { start,end,intervalMs,deadlineMs,lagMs,clockUncertaintyMs,check,region,deploy,component } = schedule;
  requireCheck([start,end,intervalMs,deadlineMs,lagMs,clockUncertaintyMs,asOf].every(x=>finite(x)) && end<=asOf && end>start && intervalMs>0 && deadlineMs>0 && (end-start)/intervalMs<=100000, 'schedule');
  for (const m of maintenance) requireCheck(finite(m.start) && finite(m.end) && finite(m.declaredAt) && m.start<m.end && m.declaredAt<=m.start,'maintenance-not-predeclared');
  requireCheck(Array.isArray(records) && records.length<=200000,'record-cap');
  const relevant = records.map(validateRaw).filter(r=>r.check===check && r.region===region && r.deploy===deploy && r.finishedAt<=asOf);
  for(const key of ['sourceSha','configSha','environment','domain']) requireCheck(new Set(relevant.map(r=>r[key])).size<=1,'mixed-provenance');
  const bySlot=new Map();
  for(const r of relevant) {const list=bySlot.get(r.scheduledAt) ?? [];list.push(r);bySlot.set(r.scheduledAt,list);}
  const ordered=[...maintenance].sort((a,b)=>a.start-b.start);
  requireCheck(ordered.every((m,i)=>i===0 || m.start>=ordered[i-1].end),'maintenance-overlap');
  const slots = []; const operations = new Map();
  for (let t=start;t<end;t+=intervalMs) {
    const attempts = (bySlot.get(t) ?? []).sort((a,b)=>a.attempt-b.attempt);
    requireCheck(new Set(attempts.map(r=>r.attempt)).size===attempts.length,'duplicate-attempt');
    const first = attempts.find(r=>r.attempt===0);
    const heartbeatOk = heartbeat.some(h=>h.region===region && h.executor===first?.executor && finite(h.at) && Math.abs(h.at-t)<=lagMs+intervalMs && h.ok === true && h.independentWitness === true && h.witness !== first?.executor);
    const ontime = first && first.startedAt>=t-clockUncertaintyMs && first.startedAt<=t+lagMs+clockUncertaintyMs && first.finishedAt<=t+lagMs+deadlineMs+clockUncertaintyMs && first.finishedAt<=asOf && first.durationMs<=deadlineMs;
    let state = 'unknown';
    if (ontime && heartbeatOk && ['pass','fail'].includes(first.status)) state = first.status === 'pass' ? 'healthy' : 'failed';
    for (const a of attempts.filter(r=>r.finishedAt<=asOf)) for (const o of a.operations) {
      requireCheck(!operations.has(o.operationId),'duplicate-operation');
      operations.set(o.operationId,o); // Later reconciliation must be a separate stream, never overwrite.
    }
    const slotEnd=Math.min(t+intervalMs,end);
    const maintenanceMs=maintenance.reduce((n,m)=>n+Math.max(0,Math.min(slotEnd,m.end)-Math.max(t,m.start)),0);
    slots.push({ at:t,ms:slotEnd-t,maintenanceMs,state,maintenance:inside(t,maintenance),observedAt:first?.finishedAt ?? null,retryObservedAt:attempts.find(r=>r.attempt===1)?.finishedAt ?? null,firstStatus:first?.status ?? 'missing', exportFailed:first?.status==='export-failed', retryStatus:attempts.find(r=>r.attempt===1)?.status ?? null,late:!!first&&!ontime,heartbeatOk });
  }
  const summarize = list => {
    const total = list.reduce((s,x)=>s+x.ms,0), healthy = list.filter(x=>x.state==='healthy').reduce((s,x)=>s+x.ms,0), unknown = list.filter(x=>x.state==='unknown').reduce((s,x)=>s+x.ms,0);
    return { eligibleMs:total,healthyMs:healthy,unknownMs:unknown,failedMs:total-healthy-unknown,lower:total?healthy/total:null,upper:total?(healthy+unknown)/total:null,expectedSamples:list.length,firstPass:list.filter(x=>x.firstStatus==='pass').length,firstFail:list.filter(x=>x.firstStatus==='fail').length,exportFailed:list.filter(x=>x.exportFailed).length,missing:list.filter(x=>x.firstStatus==='missing').length,late:list.filter(x=>x.late).length,retries:list.filter(x=>x.retryStatus!==null).length };
  };
  const incidents = []; let open = null, previous = null, recovery = 0;
  for (const s of slots) {
    if (s.state==='failed') {
      recovery=0;
      if (!open) { open={ onset:{ after:previous?.state==='healthy'?previous.at:null,by:s.at+lagMs+deadlineMs+clockUncertaintyMs }, firstFailureAt:s.observedAt,confirmedAt:null,lastFailureAt:s.observedAt,firstRecoveryAt:null,recoveredAt:null }; incidents.push(open); }
      open.lastFailureAt=s.observedAt;
      if (s.retryStatus==='fail') open.confirmedAt ??= s.retryObservedAt;
    } else if (s.state==='healthy' && open) {
      open.firstRecoveryAt ??= s.observedAt;
      if (++recovery===2) { open.recoveredAt=s.observedAt;open=null;recovery=0; }
    } else if (s.state==='unknown') recovery=0;
    previous=s;
  }
  const ops=[...operations.values()];
  return { schema:'gonvex.availability.v1',component,region,deploy,check,objective:'99.9% monthly owner proposal; not achieved/SLA',estimator:'scheduled-slot-hold; samples do not prove continuous health', inclusive:summarize(slots),maintenanceAdjusted:summarize(slots.map(x=>({...x,ms:x.ms-x.maintenanceMs})).filter(x=>x.ms>0)),operations:{ attempted:ops.length,success:ops.filter(x=>x.status==='pass').length,failed:ops.filter(x=>x.status!=='pass').length },bounds:{ maxDetectionMs:intervalMs+lagMs+deadlineMs+2*clockUncertaintyMs,maxConfirmationMs:intervalMs+lagMs+2*deadlineMs+2*clockUncertaintyMs+(schedule.retryBackoffMs ?? 5000),recoveryHealthySamples:2,notificationReceipt:'unobserved' },incidents,slots };
}
