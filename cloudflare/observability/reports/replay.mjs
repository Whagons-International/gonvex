import { requireCheck, finite } from '../checks/contract.mjs';
import { validateRaw, operationAttemptAt } from '../checks/raw-result.mjs';
export { validateRaw } from '../checks/raw-result.mjs';
const inside = (t, windows) => windows.some(w=>t>=w.start && t<w.end);
/** One check/component/region/deploy per call. Scheduled slots use the explicit piecewise-constant
 * estimator, NOT continuous-health proof. Missing, late, provider-heartbeat loss are UNKNOWN.
 * Retries corroborate but never replace the first attempt. No cross-region quorum.
 */
export function replay({ schedule, records, heartbeat = [], maintenance = [], asOf }) {
  requireCheck(schedule && typeof schedule==='object','schedule');
  const { start,end,intervalMs,deadlineMs,lagMs,clockUncertaintyMs,check,region,deploy,component } = schedule;
  requireCheck([start,end,intervalMs,deadlineMs,lagMs,clockUncertaintyMs,asOf].every(x=>finite(x) && x<=Number.MAX_SAFE_INTEGER) && end<=asOf && end>start && intervalMs>0 && deadlineMs>0 && (end-start)/intervalMs<=100000, 'schedule');
  requireCheck(['Web','API','Sync','Files','Background','Monitoring'].includes(component) && [check,region,deploy].every(x=>typeof x==='string' && /^[a-zA-Z0-9._-]{1,128}$/.test(x)),'schedule-labels');
  if (schedule.retryBackoffMs!==undefined) requireCheck(finite(schedule.retryBackoffMs) && schedule.retryBackoffMs<=Number.MAX_SAFE_INTEGER,'schedule-backoff');
  for (const m of maintenance) requireCheck(finite(m.start) && finite(m.end) && finite(m.declaredAt) && m.start<m.end && m.declaredAt<=m.start,'maintenance-not-predeclared');
  requireCheck(Array.isArray(records) && records.length<=200000,'record-cap');
  const scoped = records.map(validateRaw).filter(r=>r.check===check && r.region===region && r.deploy===deploy);
  const overlaps = r => r.startedAt < end && r.finishedAt >= start;
  const scheduledInside = r => r.scheduledAt >= start && r.scheduledAt < end;
  const onGrid = r => scheduledInside(r) && Number.isInteger((r.scheduledAt-start)/intervalMs);
  const candidates=new Map(scoped.map(r=>[r,r.operations.map(o=>{
    const uncertainty=Math.max(clockUncertaintyMs,r.clockUncertaintyMs);
    const at=operationAttemptAt(r,o);
    const from=(at ?? r.startedAt)-uncertainty;
    const to=(at ?? r.finishedAt)+uncertainty;
    return { o,at,from,to,eligible:from<end && to>=start && from<=asOf,
      windowUncertain:from<start || to>=end };
  })]));
  // Inspect operation timestamps on ALL scoped records before selecting check observations.
  const relevant=scoped.filter(r=>scheduledInside(r) || overlaps(r) || candidates.get(r).some(c=>c.eligible));
  for (const key of ['sourceSha','configSha','environment','domain']) requireCheck(new Set(relevant.map(r=>r[key])).size<=1,'mixed-provenance');
  const bySlot = new Map(), operations = new Map(), observations = [];
  const recordAccounting = { scoped:scoped.length, eligible:relevant.length, outsideWindow:scoped.length-relevant.length,
    offGrid:0, late:0, unobservedAtAsOf:0, unsettled:0, firstFail:0, firstPass:0, retries:0,
    operationTimeFallback:0, operationWindowUncertain:0, operationsOutsideWindow:0, operationsUnconfirmedAtAsOf:0,
    operationsPending:0, operationsUnknown:0 };
  const slotCount = Math.ceil((end-start)/intervalMs), issueDeltas = new Map();
  // Range deltas bound work to records + slots, even if an anomalous check spans many slots.
  const affectRange = (from,to,reason) => {
    if (!(from<end && to>=start)) return;
    const first=Math.max(0,Math.floor((from-start)/intervalMs));
    const last=Math.min(slotCount-1,Math.max(first,Math.floor((to-start)/intervalMs)));
    let delta = issueDeltas.get(reason);
    if (!delta) { delta = new Int32Array(slotCount+1); issueDeltas.set(reason,delta); }
    delta[first]++; delta[last+1]--;
  };
  const affectCoverage=(r,reason)=>affectRange(r.startedAt,r.finishedAt,reason);
  for (const r of relevant) {
    const grid = onGrid(r), observed = r.finishedAt<=asOf;
    const uncertainty = Math.max(clockUncertaintyMs,r.clockUncertaintyMs);
    const late = r.startedAt<r.scheduledAt-uncertainty || r.startedAt>r.scheduledAt+lagMs+uncertainty
      || r.finishedAt>r.scheduledAt+lagMs+deadlineMs+uncertainty || r.durationMs>deadlineMs || r.deadlineExceeded===true;
    const group = bySlot.get(r.scheduledAt) ?? [];
    requireCheck(!group.some(a=>a.attempt===r.attempt),'duplicate-attempt');
    group.push(r); bySlot.set(r.scheduledAt,group);
    if (!grid) { recordAccounting.offGrid++; affectCoverage(r,'off-grid'); }
    if (late) { recordAccounting.late++; affectCoverage(r,'late'); }
    if (!observed) { recordAccounting.unobservedAtAsOf++; affectCoverage(r,'unobserved-at-cutoff'); }
    if (r.settlement==='unsettled') { recordAccounting.unsettled++; affectCoverage(r,'unsettled'); }
    if (r.attempt===1) recordAccounting.retries++;
    if (observed && r.attempt===0 && r.status==='fail') recordAccounting.firstFail++;
    if (observed && r.attempt===0 && r.status==='pass') recordAccounting.firstPass++;
    observations.push({ operationId:r.operationId,scheduledAt:r.scheduledAt,startedAt:r.startedAt,finishedAt:r.finishedAt,
      attempt:r.attempt,status:r.status,onGrid:grid,late,observedAtAsOf:observed,settlement:r.settlement ?? 'unrecorded' });
  }
  for (const r of scoped) for (const {o,at,from,to,eligible,windowUncertain} of candidates.get(r)) {
    if (!eligible) { recordAccounting.operationsOutsideWindow++; continue; }
    requireCheck(o.deadlineMs<=deadlineMs && o.deadlineMs<=r.deadlineMs,'operation-deadline-contract');
    requireCheck(!operations.has(o.operationId),'duplicate-operation');
    const knownTime=at!==undefined, observed=r.finishedAt<=asOf;
    if (o.attemptedAt===undefined) recordAccounting.operationTimeFallback++;
    if (windowUncertain) { recordAccounting.operationWindowUncertain++; affectRange(from,to,'operation-window-uncertain'); }
    if (!observed) recordAccounting.operationsUnconfirmedAtAsOf++;
    const expired=asOf>=(at ?? r.startedAt)+Math.max(clockUncertaintyMs,r.clockUncertaintyMs)+o.deadlineMs;
    // Use the same declared attempt origin for eligibility, expiry and cutoff
    // proof. Only records lacking BOTH forms retain the legacy fallback.
    const appliedUpper=knownTime ? at+(o.appliedMs ?? 0)+r.clockUncertaintyMs : r.finishedAt+r.clockUncertaintyMs;
    const proofByCutoff=observed && appliedUpper<=asOf;
    const status=!observed ? expired?'fail':'pending' : o.status==='fail'?'fail':windowUncertain||!proofByCutoff?'unknown':'pass';
    if (status==='pending') recordAccounting.operationsPending++;
    if (status==='unknown') recordAccounting.operationsUnknown++;
    operations.set(o.operationId,{status});
    if (status!=='pass') affectRange(from,Math.max(to,Math.min(r.finishedAt,end)),status==='fail'?'failed-operation':'unconfirmed-operation');
  }
  const ordered = [...maintenance].sort((a,b)=>a.start-b.start);
  requireCheck(ordered.every((m,i)=>i===0 || m.start>=ordered[i-1].end),'maintenance-overlap');
  const slots = [], activeIssues = new Map();
  for (let index=0;index<slotCount;index++) {
    const t=start+index*intervalMs;
    const attempts=(bySlot.get(t) ?? []).filter(r=>r.finishedAt<=asOf).sort((a,b)=>a.attempt-b.attempt);
    const first=attempts.find(r=>r.attempt===0);
    const heartbeatOk=heartbeat.some(h=>h.region===region && h.executor===first?.executor && finite(h.at) && Math.abs(h.at-t)<=lagMs+intervalMs && h.ok===true && h.independentWitness===true && h.witness!==first?.executor);
    const uncertainty=Math.max(clockUncertaintyMs,first?.clockUncertaintyMs ?? 0);
    const ontime=first && first.startedAt>=t-uncertainty && first.startedAt<=t+lagMs+uncertainty
      && first.finishedAt<=t+lagMs+deadlineMs+uncertainty && first.durationMs<=deadlineMs && first.deadlineExceeded!==true && first.settlement!=='unsettled';
    let state='unknown';
    if (ontime && heartbeatOk && ['pass','fail'].includes(first.status)) state=first.status==='pass'?'healthy':'failed';
    const coverageIssues=[];
    for (const [reason,delta] of issueDeltas) {
      const active=(activeIssues.get(reason) ?? 0)+delta[index]; activeIssues.set(reason,active);
      if (active>0) coverageIssues.push(reason);
    }
    // Conflicting/off-grid observations cannot prove a whole slot failed, but they MUST
    // remove its healthy certificate. Exact on-time failed first attempts remain failed.
    if (coverageIssues.length && state==='healthy') state='unknown';
    const slotEnd=Math.min(t+intervalMs,end);
    const maintenanceMs=maintenance.reduce((n,m)=>n+Math.max(0,Math.min(slotEnd,m.end)-Math.max(t,m.start)),0);
    slots.push({ at:t,ms:slotEnd-t,maintenanceMs,state,coverageIssues,maintenance:inside(t,maintenance),observedAt:first?.finishedAt ?? null,
      retryObservedAt:attempts.find(r=>r.attempt===1)?.finishedAt ?? null,firstStatus:first?.status ?? 'missing',exportFailed:first?.status==='export-failed',
      retryStatus:attempts.find(r=>r.attempt===1)?.status ?? null,late:!!first&&!ontime,heartbeatOk });
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
  return { schema:'gonvex.availability.v1',component,region,deploy,check,objective:'99.9% monthly owner proposal; not achieved/SLA',estimator:'scheduled-slot-hold; samples do not prove continuous health', inclusive:summarize(slots),maintenanceAdjusted:summarize(slots.map(x=>({...x,ms:x.ms-x.maintenanceMs})).filter(x=>x.ms>0)),recordAccounting,observations,operations:{ attempted:ops.length,success:ops.filter(x=>x.status==='pass').length,failed:ops.filter(x=>x.status==='fail').length },bounds:{ maxDetectionMs:intervalMs+lagMs+deadlineMs+2*clockUncertaintyMs,maxConfirmationMs:intervalMs+lagMs+2*deadlineMs+2*clockUncertaintyMs+(schedule.retryBackoffMs ?? 5000),recoveryHealthySamples:2,notificationReceipt:'unobserved' },incidents,slots };
}
