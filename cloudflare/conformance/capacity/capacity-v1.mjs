import { PATH, readPage } from './capacity-wire.mjs';
import { METRICS, SCOPES } from './profile.mjs';
import { hash } from './binding.mjs';
let state;
const blank=(scope,id)=>({scope,id,...Object.fromEntries(METRICS.map(k=>[k,null]))});
export function capacityFetchAllowed(target,b){
 try{const u=new URL(target),base=new URL(b.telemetry.url);if(u.origin!==base.origin||u.pathname!==PATH||u.hash||u.username||u.password)return false;
 const keys=['cell','cursor','limit','eventCursor','eventLimit','fresh','runId'];if([...u.searchParams.keys()].some(k=>!keys.includes(k)||u.searchParams.getAll(k).length!==1))return false;
 const n=k=>{const s=u.searchParams.get(k);return s!==null&&/^(0|[1-9][0-9]*)$/.test(s)?Number(s):NaN;};
 return b.telemetry.cells.includes(n('cell'))&&n('cursor')<=64&&n('eventCursor')<=64&&n('limit')===4&&n('eventLimit')===16&&n('fresh')===(b.telemetry.fresh?1:0)&&u.searchParams.get('runId')===b.runId;
 }catch{return false;}
}
/** Actual native K22 v1 → driver-owned profile. This code never calls a mutation or infers unknown counters. */
export async function readCapacity({url,bearer,signal,fetch,binding:b,providerObservation:provider,runtimeIdentity}){
 const key=hash(JSON.stringify([b.runId,url,b.telemetry.cells,b.telemetry.fresh]));
 if(state?.key!==key)state={key,sequence:0,next:0,cells:new Map(),windows:new Map()};
 const pages=[],holders=new Map(),services=new Map(),events=[],reasons=new Set(['SOURCE_EVENT_HISTORY_UNKNOWN','INNER_DO_VERSIONS_UNAVAILABLE','GENERATIONS_UNAVAILABLE','SERVICE_CPU_MEMORY_SQL_UNAVAILABLE']);
 const sampler={requests:0,responses:0,requestLoss:0,errors:0};
 const induced={rpcAttempts:0,catalogAttempts:0,controllerAttempts:0,capacityAttempts:0,drainStatusAttempts:0,actualControllerWakeups:null,actualHolderWakeups:null,snapshotMayAdvance:false,socketSampleMayRecord:false,requestsWithUnknownRpcEffects:0,controllerReadMayWake:false,holdersMayWake:false};
 let boundedExecution=true;
 for(let n=0;n<b.telemetry.maxPages;n++){
  if(signal.aborted){boundedExecution=false;reasons.add('SAMPLER_ABORTED');break;}
  const cell=b.telemetry.cells[state.next++%b.telemetry.cells.length];
  const previous=state.cells.get(cell)??{cursor:0,eventCursor:0,digest:null};
  const q={cell,cursor:previous.cursor,eventCursor:previous.eventCursor,fresh:b.telemetry.fresh,runId:b.runId};
  const target=new URL(url);target.search=new URLSearchParams({cell:String(cell),cursor:String(q.cursor),limit:'4',eventCursor:String(q.eventCursor),eventLimit:'16',fresh:q.fresh?'1':'0',runId:b.runId}).toString();
  sampler.requests++;let received=false;
  try{
   const response=await fetch(target.href,{signal,method:'GET',redirect:'error',headers:{authorization:`Bearer ${bearer}`}});received=true;sampler.responses++;
   const decoded=await readPage(response,q);
   if(decoded.error){sampler.errors++;boundedExecution=false;induced.requestsWithUnknownRpcEffects++;reasons.add('TYPED_HTTP_ERROR');pages.push({cell,cursor:q.cursor,eventCursor:q.eventCursor,error:decoded.error,httpStatus:decoded.httpStatus});state.cells.set(cell,{cursor:0,eventCursor:0,digest:null});continue;}
   const p=decoded.page,v=p.provenance;
   induced.catalogAttempts+=v.catalogIdentityCalls;induced.controllerAttempts+=v.controllerStatusCalls;induced.capacityAttempts+=v.capacityCalls;induced.drainStatusAttempts+=v.drainStatusCalls;
   induced.rpcAttempts+=v.catalogIdentityCalls+v.controllerStatusCalls+v.capacityCalls+v.drainStatusCalls;
   induced.controllerReadMayWake ||= v.controllerReadMayWake&&v.controllerStatusCalls>0;induced.holdersMayWake ||= v.freshMayWakeHolders;
   induced.snapshotMayAdvance ||=v.metricsSnapshotMayAdvance;induced.socketSampleMayRecord ||=v.socketSampleMayBeRecorded;
   const identity=p.identity,catalog=identity.catalog;
   if(identity.worker?.id!==b.deployment.resources.find(r=>r.role==='kernel').version||identity.runtimeWorker?.id!==b.deployment.resources.find(r=>r.role==='runtime').version
     ||catalog.status!=='OK'||catalog.value.version!==runtimeIdentity.catalogVersion||catalog.value.releasesSha256!==runtimeIdentity.releasesSha256||catalog.value.projects!==runtimeIdentity.projects){boundedExecution=false;reasons.add('NATIVE_PARTIAL_IDENTITY_MISMATCH');}
   if(p.controller.status!=='OK'){boundedExecution=false;reasons.add(`CONTROLLER_${p.controller.status}`);pages.push({cell,controllerStatus:p.controller.status,pageStatus:p.pageStatus,httpAt:p.at});continue;}
   const digest=p.controller.value.snapshotSha256,changed=previous.digest!==null&&previous.digest!==digest&&(q.cursor>0||q.eventCursor>0);
   if(changed){reasons.add('CONTROLLER_SNAPSHOT_CHANGED');state.cells.set(cell,{cursor:0,eventCursor:0,digest});}
   else state.cells.set(cell,{cursor:p.coverage.slots.nextCursor??0,eventCursor:p.coverage.events.nextCursor??0,digest});
   pages.push({cell,httpAt:p.at,pageStatus:p.pageStatus,snapshotSha256:digest,snapshotChanged:changed,coverage:p.coverage,
    identity:{worker:identity.worker,runtimeWorker:identity.runtimeWorker,catalog:catalog,limitedConfigFacts:identity.config,objectCodeVersions:identity.objectCodeVersions},unavailable:p.unavailable});
   if(p.coverage.slots.hasMore||p.coverage.events.hasMore)reasons.add('PARTIAL_PAGE_COVERAGE');
   for(const slot of p.slots){
    const observation=q.fresh&&slot.fresh.capacity.status==='OK'?slot.fresh.capacity.value:slot.cached;
    if(q.fresh&&slot.mode!=='retired'&&slot.fresh.capacity.status!=='OK'){boundedExecution=false;reasons.add(`HOLDER_${slot.fresh.capacity.status}`);}
    if(observation.state!=='FRESH')reasons.add(`SAMPLE_${observation.state}`);
    if(observation.windowState!=='FRESH')reasons.add(`WINDOW_${observation.windowState}`);
    const sample=observation.sample,old=state.windows.get(slot.name);
    if(sample?.windowInstance!==undefined){
     const w=Object.fromEntries(Object.keys(sample).filter(k=>k.startsWith('window')).sort().map(k=>[k,sample[k]]));
     if(old?.epoch===slot.epoch&&old.instance===sample.windowInstance){
      if(sample.windowSequence>old.sequence+1)reasons.add('SOURCE_WINDOW_GAP');
      if(sample.windowSequence<old.sequence)reasons.add('SOURCE_WINDOW_REGRESSION');
      if(sample.windowSequence===old.sequence&&JSON.stringify(w)!==old.window)reasons.add('SOURCE_WINDOW_MUTATED');
     }
     state.windows.set(slot.name,{epoch:slot.epoch,instance:sample.windowInstance,sequence:sample.windowSequence,window:JSON.stringify(w)});
    }
    holders.set(slot.name,{controller:`gateway-cell-${cell}`,holder:slot.name,epoch:slot.epoch,mode:slot.mode,changedAt:slot.changedAt,cached:slot.cached,fresh:slot.fresh,
     sample,windowMeaning:'LATEST_COMPLETED_ONLY_SAMPLED_MIN_NOT_CONTINUOUS',drainAge:'UNKNOWN'});
    const row=blank('gateway',slot.name);
    if(sample)Object.assign(row,{sockets:sample.sockets,framesPerSecond:sample.framesPerSecond,bytesPerSecond:sample.bytesPerSecond,deliveryLagMs:sample.deliveryMs,queueAgeMs:sample.queueAgeMs,rpcMs:sample.rpcMs});
    services.set(slot.name,row); // No integration of rates, duplicate windows or unreported intervals into totals.
   }
   for(const e of p.events)events.push({controller:`gateway-cell-${cell}`,...e});
  }catch{
   if(!received)sampler.requestLoss++;else sampler.errors++;
   induced.requestsWithUnknownRpcEffects++;boundedExecution=false;reasons.add('SAMPLER_REQUEST_OR_PROJECTION_FAILURE');pages.push({cell,cursor:q.cursor,eventCursor:q.eventCursor,status:'UNKNOWN'});
   if(signal.aborted)break;
  }
 }
 if(!provider)throw Error('provider observation required');
 if(provider.sourceSha===null)reasons.add('PROVIDER_SOURCE_LABEL_UNAVAILABLE');if(provider.configurationSha256===null)reasons.add('FULL_CONFIG_DIGEST_UNAVAILABLE');
 if(b.telemetry.cells.length!==32||new Set(pages.map(p=>p.cell)).size!==32)reasons.add('NOT_ONE_COMPLETE_FLEET_SNAPSHOT');
 const serviceRows=[...SCOPES.filter(s=>s!=='gateway').map(s=>blank(s,'UNAVAILABLE_v1')),...services.values()];
 if(!services.size)serviceRows.push(blank('gateway','UNAVAILABLE_v1'));
 return {v:1,runId:b.runId,sourceSha:provider.sourceSha,moduleSha256:runtimeIdentity.moduleSha256,configSha256:provider.configurationSha256,resources:provider.resourceVersions,
  sequence:++state.sequence,sampledAtUnixMs:Date.now(),sampler,sourceHistory:'UNKNOWN',observation:b.telemetry.fresh?'bounded-fresh':'cached',induced,
  services:serviceRows,holders:[...holders.values()],events,correctness:{duplicateWrites:null,lostWrites:null},port:{source:'K22_CAPACITY_HTTP_V1',sourceCommit:'951f53d6516e6a7651b4ff6ad4344b2022451a87',pages},
  eligibility:{boundedExecution,fullCapacity:'NOT_ELIGIBLE',reasons:[...reasons]}};
}
