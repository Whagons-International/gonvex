import { shape, integer, text, hex, list, refuse } from './binding.mjs';
export const PATH='/__assembly/capacity/v1';
const failures=['NOT_BOUND','TIMEOUT','ABORTED','RPC_FAILED','MALFORMED','OVERSIZED','EPOCH_MISMATCH','NOT_REQUESTED','NOT_SELECTED','RETIRED'];
const windowKeys=['windowInstance','windowSequence','windowStart','windowEnd','windowSockets','windowSocketSamples','windowFramesPerSecond','windowBytesPerSecond','windowDeliveryMs','windowQueueAgeMs','windowRpcMs'];
const finite=n=>typeof n==='number'&&Number.isFinite(n)&&n>=0&&n<=Number.MAX_SAFE_INTEGER;
function optionalShape(v,required,optional,field){if(!v||typeof v!=='object'||Array.isArray(v)||required.some(k=>!Object.hasOwn(v,k))||Object.keys(v).some(k=>!required.includes(k)&&!optional.includes(k)))refuse(field);}
function read(r,validate){if(r?.status==='OK'){shape(r,['status','value'],'read.OK');validate(r.value);}else{shape(r,['status'],'read.failure');if(!failures.includes(r.status))refuse('read.status');}}
function worker(w){if(w!==null){shape(w,['id','tag','timestamp'],'worker');text(w.id,'worker.id',128);if(typeof w.tag!=='string'||w.tag.length>128||typeof w.timestamp!=='string'||w.timestamp.length>64)refuse('worker.metadata');}}
function observation(o){
 shape(o,['sample','state','ageMs','windowState'],'observation');
 for(const k of ['state','windowState'])if(!['FRESH','STALE','UNKNOWN','CLOCK_SKEW'].includes(o[k]))refuse('recency');
 if(o.ageMs!==null)integer(o.ageMs,0,Number.MAX_SAFE_INTEGER,'ageMs');
 if(o.sample===null)return;
 const s=o.sample,required=['v','epoch','at','sockets','framesPerSecond','bytesPerSecond','deliveryMs','queueAgeMs','rpcMs'];
 optionalShape(s,required,['currentPressure',...windowKeys],'sample');
 if(s.v!==1||!Object.values(s).every(finite))refuse('sample.numeric');
 for(const k of ['epoch','at','sockets'])integer(s[k],0,Number.MAX_SAFE_INTEGER,k);
 if(s.currentPressure!==undefined&&s.currentPressure!==0&&s.currentPressure!==1)refuse('currentPressure');
 const count=windowKeys.filter(k=>Object.hasOwn(s,k)).length;
 if(count&&count!==windowKeys.length)refuse('window.group');
 if(count){for(const k of windowKeys.slice(0,6))integer(s[k],0,Number.MAX_SAFE_INTEGER,k);if(s.windowEnd-s.windowStart!==5000||s.windowEnd>s.at)refuse('window.interval');}
}
function cover(c,event=false){shape(c,['cursor','limit','returned','total','nextCursor','hasMore',...(event?['sourceLimit','loss','oldestAt','newestAt']:[])],'coverage');
 for(const k of ['cursor','returned','total'])integer(c[k],0,64,k);integer(c.limit,1,event?16:4,'limit');
 if(c.returned>c.limit||c.cursor+c.returned>c.total||c.hasMore!==(c.cursor+c.returned<c.total)||c.nextCursor!==(c.hasMore?c.cursor+c.returned:null))refuse('coverage.range');
 if(event&&(c.sourceLimit!==64||c.loss!=='UNKNOWN'))refuse('history.loss');
 if(event)for(const k of ['oldestAt','newestAt'])if(c[k]!==null)integer(c[k],0,Number.MAX_SAFE_INTEGER,k);
}
export function validatePage(p,q){
 shape(p,['v','at','runId','mode','pageStatus','identity','controller','coverage','slots','events','provenance','unavailable'],'CapacityPage');
 if(p.v!==1||p.runId!==q.runId||p.mode!==(q.fresh?'fresh':'cached')||!['COMPLETE','PARTIAL'].includes(p.pageStatus))refuse('page.binding');integer(p.at,0,Number.MAX_SAFE_INTEGER,'page.at');
 const i=p.identity;shape(i,['originService','worker','runtimeWorker','runtimeLabel','catalog','config','objectCodeVersions'],'identity');
 if(i.originService!=='kernel-http'||i.objectCodeVersions!=='UNAVAILABLE_NO_OBJECT_VERSION_PORT')refuse('identity.source');worker(i.worker);worker(i.runtimeWorker);if(i.runtimeLabel!==null)text(i.runtimeLabel,'runtimeLabel',128);
 read(i.catalog,c=>{shape(c,['originService','sourceInterface','version','projects','releasesSha256','generation'],'catalog');if(c.originService!=='runtime-catalog'||c.sourceInterface!=='RuntimeCatalog.identity'||c.generation!=='UNAVAILABLE_IDENTITY_PORT_HAS_HASHES_ONLY')refuse('catalog.source');text(c.version,'catalog.version',128);integer(c.projects,0,2048,'projects');hex(c.releasesSha256,64,'catalog.hash');});
 shape(i.config,['controllerBindingPresent','runtimeCatalogBindingPresent','staticGatewayShards','factsSha256'],'config.facts');
 if(typeof i.config.controllerBindingPresent!=='boolean'||typeof i.config.runtimeCatalogBindingPresent!=='boolean')refuse('config.presence');integer(i.config.staticGatewayShards,1,4096,'shards');hex(i.config.factsSha256,64,'limitedConfigFacts');
 read(p.controller,c=>{shape(c,['cell','epoch','pending','readAt','snapshotSha256'],'controller');if(c.cell!==q.cell)refuse('controller.cell');integer(c.epoch,0,Number.MAX_SAFE_INTEGER,'epoch');integer(c.pending,0,256,'pending');integer(c.readAt,0,Number.MAX_SAFE_INTEGER,'readAt');hex(c.snapshotSha256,64,'snapshotSha256');});
 const holder=name=>{if(typeof name!=='string'||!new RegExp(`^auto-gw-${q.cell}-(0|[1-9][0-9]*)$`).test(name)||Number(name.split('-').at(-1))>63)refuse('holder.name');};
 if(p.coverage!==null){const c=p.coverage;shape(c,['cell','totalCells','fleet','slots','events','pagination'],'page.coverage');if(c.cell!==q.cell||c.totalCells!==32||c.fleet!=='ONE_CELL_PAGE_ONLY'||c.pagination!=='COMPARE_CONTROLLER_SNAPSHOT_SHA256')refuse('coverage.scope');cover(c.slots);cover(c.events,true);if(c.slots.cursor!==q.cursor||c.events.cursor!==q.eventCursor||c.slots.limit!==4||c.events.limit!==16)refuse('coverage.cursor/limit');}
 if((p.controller.status==='OK')!==(p.coverage!==null))refuse('controller.coverage');
 list(p.slots,4,'slots',0);list(p.events,16,'events',0);
 if(p.coverage&&(p.coverage.slots.returned!==p.slots.length||p.coverage.events.returned!==p.events.length))refuse('coverage.length');
 for(const s of p.slots){shape(s,['name','epoch','mode','changedAt','pressuredSince','replacement','nextCheck','pressureEvidence','cached','fresh'],'slot');holder(s.name);integer(s.epoch,1,Number.MAX_SAFE_INTEGER,'slot.epoch');if(!['active','draining','retired'].includes(s.mode))refuse('slot.mode');for(const k of ['changedAt','pressuredSince','nextCheck'])if(s[k]!==null)integer(s[k],0,Number.MAX_SAFE_INTEGER,k);if(s.replacement!==null)holder(s.replacement);
 if(s.pressureEvidence!==null){shape(s.pressureEvidence,['epoch','window','since'],'pressureEvidence');if(s.pressureEvidence.epoch!==s.epoch||!Array.isArray(s.pressureEvidence.window)||s.pressureEvidence.window.length!==11||!s.pressureEvidence.window.every(finite))refuse('pressureEvidence.window');integer(s.pressureEvidence.since,0,Number.MAX_SAFE_INTEGER,'since');}
 observation(s.cached);shape(s.fresh,['capacity','drain'],'fresh');read(s.fresh.capacity,observation);read(s.fresh.drain,d=>{shape(d,['closed','remaining','blocked'],'drain');integer(d.closed,0,Number.MAX_SAFE_INTEGER,'closed');integer(d.remaining,0,Number.MAX_SAFE_INTEGER,'remaining');optionalShape(d.blocked,[],['inflight','queued','direct-action','session-hook-unavailable','replacement-or-admission-unavailable'],'blocked');for(const n of Object.values(d.blocked))integer(n,0,Number.MAX_SAFE_INTEGER,'blocked.count');});
 for(const o of [s.cached,s.fresh.capacity.status==='OK'?s.fresh.capacity.value:null])if(o?.sample&&o.sample.epoch!==s.epoch)refuse('sample.epoch');}
 for(const e of p.events){shape(e,['at','action','gateway','epoch'],'event');holder(e.gateway);integer(e.at,0,Number.MAX_SAFE_INTEGER,'event.at');integer(e.epoch,1,Number.MAX_SAFE_INTEGER,'event.epoch');if(!['allocate','drain','retire'].includes(e.action))refuse('event.action');}
 const v=p.provenance;shape(v,['clock','catalogIdentityCalls','controllerStatusCalls','capacityCalls','drainStatusCalls','controllerReadMayWake','freshMayWakeHolders','metricsSnapshotMayAdvance','socketSampleMayBeRecorded','drainResult','cancellation','runId','history','windowHistory','sockets'],'provenance');
 for(const [k,max] of [['catalogIdentityCalls',1],['controllerStatusCalls',1],['capacityCalls',4],['drainStatusCalls',4]])integer(v[k],0,max,k);
 for(const k of ['controllerReadMayWake','freshMayWakeHolders','metricsSnapshotMayAdvance','socketSampleMayBeRecorded'])if(typeof v[k]!=='boolean')refuse('provenance.effects');
 if(v.clock!=='UNIX_MS_SERVICE_CLOCKS_NOT_SYNCHRONIZED'||v.history!=='LAST_64_EVENTS_NO_COMPLETE_TIMELINE_OR_LOSS_COUNTER'||v.windowHistory!=='LATEST_COMPLETED_BUCKET_ONLY_GAPS_REQUIRE_CLIENT_COMPARISON'||v.runId!=='CORRELATION_ONLY_NOT_COST_ATTRIBUTION'||v.drainResult!=='LAST_TRACKED_RESULT_NO_RESULT_TIMESTAMP'||v.cancellation!=='STOPS_FURTHER_CALLS_INFLIGHT_RPC_NOT_CANCELLED'||v.sockets!=='CURRENT_GAUGE_AND_BUCKET_SAMPLED_MIN_NOT_CONTINUOUS_OCCUPANCY')refuse('provenance.semantics');
 if(!q.fresh&&(v.capacityCalls||v.drainStatusCalls||v.metricsSnapshotMayAdvance||v.socketSampleMayBeRecorded))refuse('cached.holder-effects');
 list(p.unavailable,32,'unavailable',0);for(const x of p.unavailable){shape(x,['metric','status','sourceInterface','reason'],'unavailable');if(x.status!=='UNAVAILABLE')refuse('unavailable.status');for(const k of ['metric','sourceInterface','reason'])text(x[k],k,512);}
 return p;
}
export async function readPage(response,q){
 if(!response.body)refuse('HTTP.body');const reader=response.body.getReader();let n=0;const parts=[];
 try{for(;;){const r=await reader.read();if(r.done)break;n+=r.value.byteLength;if(n>32768)refuse('HTTP.response-byte-bound');parts.push(r.value);}}finally{await reader.cancel();}
 const p=JSON.parse(Buffer.concat(parts).toString('utf8'));
 if(!response.ok){shape(p,['v','error'],'CapacityError');if(p.v!==1||!['BAD_QUERY','BAD_CURSOR','METHOD','ABORTED','RESPONSE_BUDGET'].includes(p.error))refuse('CapacityError.code');return {error:p.error,httpStatus:response.status};}
 return {page:validatePage(p,q)};
}
