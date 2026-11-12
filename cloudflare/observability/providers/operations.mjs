import { createHash } from 'node:crypto';
import { requireCheck } from '../checks/contract.mjs';
export const originalPaging = Array.from({ length:5 }, (_,i)=>({ slot:`P${i+1}`, name:null,threshold:null,window:null,state:'unresolved',source:'docs/cloudflare/verification/M05-original-paging-source-audit.md',importCommit:'368300f96f85aada9825225f48cfaa3dad89f8c5',designUrl:'https://claude.ai/artifact/DKcAWtgSJx4ZG5bP4oa3A6' }));
export const addedConditions = ['http','auth','durable-write','http200-stalled-sync','job-deadline','file-owner-bytes','resource','heartbeat'];
export const operations = {
  activated:false,
  alerts:{ destinations:[],owner:null,ackDeadlineMs:null,escalationOrder:[],recoveryHealthySamples:2,regionalQuorum:false,conditions:addedConditions,originalPaging },
  status:{ published:false,hostname:'status.whagons.com',provider:null,fallbackHostname:null,components:['Web','API','Sync','Files','Background'],operatorUpdatePath:null,dependencies:{ zoneDns:'Cloudflare (existing zone)',hosting:'unverified',cdn:'unverified',login:'unverified',independentUpdateNetwork:'unverified' } },
  dashboards:{ activated:false,dimensions:['component','environment','region','releaseClass','failureClass'],panels:['eligible/healthy/failed/unknown time bounds','expected/missing/late/first/retry samples','attempted/durable/second-applied task outcomes','monotonic p50/p95/p99 and censored operations','independent provider heartbeat/export failures','SQL rows/CPU/memory/bandwidth','sampling/dropped/export bytes and costs'],overallComponentAverage:false },
  analyticsEngine:{ activated:false,resourceName:null,datasetVersion:1,bindings:[],retentionDays:{diagnostics:7,raw:35,aggregates:90},proposedOnly:true,columns:{blobs:['schema','component','environment','region','releaseClass','failureClass'],doubles:['count','durationMs','bytes','rowsRead','rowsWritten'],indexes:['bounded-component-region']},perTenantCosts:'redacted bounded reports only; aliases never metric labels' },
};
/** Local alert-test-sink payload only: intentionally has no sending method. */
export function alertPayload({ environment,component,region,failureClass,phase,incidentId }) {
  requireCheck(['local-synthetic','dev-synthetic'].includes(environment) && operations.status.components.concat('Monitoring').includes(component) && ['local','us-east-1','us-west-1','eu-west-1'].includes(region) && addedConditions.includes(failureClass) && ['symptom','confirmed','recovery','unknown'].includes(phase),'alert-enums');
  requireCheck(/^[a-f0-9]{32}$/.test(incidentId),'incident-id');
  const key = createHash('sha256').update([environment,component,region,failureClass,incidentId].join(':')).digest('hex');
  return { schema:'gonvex.alert-test.v1',delivery:'disabled-local-payload-only',destination:null,dedupeKey:key,environment,component,region,failureClass,phase,incidentId,receivedAt:null,owner:null,escalationConfigured:false };
}
export function healthSweep(reports) {
  return reports.map(r=>({component:r.component,region:r.region,check:r.check,state:r.inclusive.unknownMs>0?'unknown':r.inclusive.failedMs>0?'degraded':'sampled-healthy',expected:r.inclusive.expectedSamples,missing:r.inclusive.missing,late:r.inclusive.late}));
}
/** No activation command. Every field must be observed/reviewed by lead before a separate release. */
export const activation = { enabled:false, required:[
  'canonical P1-P5 definitions and original drills','exact assembled local/dev handler proof','actual SDK/browser auth/visibility/persistence paths','K15/K22 coordinated hooks and span gaps','external 3-region schedules with 2 Americas','independent heartbeat/account/status failure-domain audit','verified account/plan/rates and approved hard budget','dedicated synthetic identities/no external side effects/rotation','persisted serialized shared fixture ledger/cleanup proof','documented per-job deadlines','authorized test alert receipt/recovery/escalation drill','V02/V05/X01/V03/V04/R01 original gates','fresh independent immutable-artifact review','explicit live activation authorization'],
  rollback:['pause new synthetic writes first; paused coverage UNKNOWN','keep raw outcomes/incidents and outstanding fixture ledger','disable only newly enabled trace/export/provider schedules','restore previous reviewed config/schedules','cleanup only owned synthetic fixtures after actual settlement','verify residuals and independent status/operator path; never erase incidents'],
};
