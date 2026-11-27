import { it, expect } from 'vitest';
import { capacityPage } from './fixtures/k22-capacity';
import type { CapacityPage } from './fixtures/k22-contract';
import { readCapacity, capacityFetchAllowed } from './capacity-v1.mjs';
import { validateTelemetry, validateCost } from './telemetry.mjs';
import { binding } from './test-data.mjs';
import { verifyIdentity } from './identity.mjs';
import { readFileSync } from 'node:fs';
const metadata={id:'DATA-only',tag:'DATA',timestamp:'2026-11-17T00:00:00Z'};
function setup(count=1){
 const b=binding();Object.assign(b.telemetry,{url:'https://gonvex-cf-dev-data.whagons-international.workers.dev/__assembly/capacity/v1',maxPages:1});b.runId=`DATA-${Math.random().toString(16).slice(2)}`;
 const catalog={version:'DATA',worker:metadata,releases:{DATA:b.deployment.moduleSha256}};
 const runtimeIdentity=verifyIdentity({...catalog,worker:metadata,runtimeWorker:metadata,ready:true},b);
 let current:any={v:1,epoch:count,pending:0,slots:Array.from({length:count},(_,i)=>({name:`auto-gw-0-${i}`,epoch:i+1,mode:'active',changedAt:10,pressuredSince:null,replacement:null,
  sample:{v:1,epoch:i+1,at:Date.now(),sockets:3,framesPerSecond:4,bytesPerSecond:8,deliveryMs:2,queueAgeMs:1,rpcMs:1,currentPressure:0}})),events:[]};
 let statusCalls=0,capacityCalls=0,hold=false,change=false;let release!:()=>void;const held=new Promise<void>(r=>{release=r;});
 const env:any={WORKER_VERSION:metadata,GONVEX_RUNTIME_VERSION:'DATA',RUNTIME_CATALOG:{identity:async()=>catalog},
  GATEWAY_CONTROLLER:{getByName:()=>({status:async()=>{statusCalls++;if(hold)await held;if(change&&statusCalls>1)current={...current,pending:1};return structuredClone(current);}})},
  GATEWAY:{getByName:()=>({capacity:async(epoch:number)=>{capacityCalls++;return {...current.slots[0].sample,epoch};},drainStatus:async()=>({closed:0,remaining:3,blocked:{}})})}};
 const fetch=async(url:string,options:any)=>capacityPage(new Request(url,{signal:options.signal}),env);
 const providerObservation={resourceVersions:b.deployment.resources,sourceSha:null,configurationSha256:null};
 const options=()=>({url:b.telemetry.url,bearer:'DATA',signal:new AbortController().signal,fetch,binding:b,providerObservation,runtimeIdentity});
 return {b,env,current,options,statusCalls:()=>statusCalls,capacityCalls:()=>capacityCalls,hold:()=>{hold=true;},change:()=>{change=true;},release:()=>release()};
}
it('translates actual frozen951 cached fixture pages without invented zero wakeups, counts or identities',async()=>{
 const c=setup(),s=await readCapacity(c.options());
 expect(s.induced).toMatchObject({rpcAttempts:2,controllerAttempts:1,capacityAttempts:0,actualControllerWakeups:null,actualHolderWakeups:null,controllerReadMayWake:true});
 const gateway=s.services.find((r:any)=>r.scope==='gateway');expect(gateway).toMatchObject({frames:null,bytes:null,framesPerSecond:4,bytesPerSecond:8,cpuMs:null});
 expect(s.sourceSha).toBeNull();expect(s.configSha256).toBeNull();expect(s.holders[0].sample.at).toBe(c.current.slots[0].sample.at);
 expect(s.sourceHistory).toBe('UNKNOWN');expect(s.sampler.requestLoss).toBe(0);expect(c.capacityCalls()).toBe(0);
 expect(validateTelemetry(s,c.b,undefined).eligibility.fullCapacity).toBe('NOT_ELIGIBLE');
 c.b.telemetry.judgment='full-capacity';expect(()=>validateTelemetry(s,c.b,undefined)).toThrow();
});
it('explicit fresh mode labels actual call attempts and possible snapshot effects, not actual wakeups',async()=>{
 const c=setup();c.b.telemetry.fresh=true;const s=await readCapacity(c.options());
 expect(s.induced).toMatchObject({rpcAttempts:4,capacityAttempts:1,drainStatusAttempts:1,holdersMayWake:true,snapshotMayAdvance:true,actualHolderWakeups:null});expect(c.capacityCalls()).toBe(1);
});
it('never joins changed controller snapshots as a complete fleet or lossless event timeline',async()=>{
 const c=setup(6);c.b.telemetry.maxPages=2;c.change();const s=await readCapacity(c.options());
 expect(s.port.pages[0].coverage.slots.hasMore).toBe(true);expect(s.port.pages[1].snapshotChanged).toBe(true);
 expect(s.eligibility.reasons).toContain('CONTROLLER_SNAPSHOT_CHANGED');expect(s.sourceHistory).toBe('UNKNOWN');expect(s.eligibility.fullCapacity).toBe('NOT_ELIGIBLE');
});
it('controller timeout is typed partial source failure, not request loss or zero holder demand',async()=>{
 const c=setup();c.hold();try{const s=await readCapacity(c.options());expect(s.port.pages[0].controllerStatus).toBe('TIMEOUT');expect(s.sampler.requestLoss).toBe(0);expect(s.eligibility.boundedExecution).toBe(false);expect(s.services.find((r:any)=>r.scope==='gateway').sockets).toBeNull();expect(()=>validateTelemetry(s,c.b,undefined)).toThrow();}finally{c.release();}
});
it('actual typed BAD_CURSOR and RESPONSE_BUDGET errors cannot become zero-cost successful pages',async()=>{
 const c=setup(6);await readCapacity(c.options());c.current.slots=[];
 const s=await readCapacity(c.options());expect(s.port.pages[0].error).toBe('BAD_CURSOR');expect(s.induced.requestsWithUnknownRpcEffects).toBe(1);expect(s.eligibility.boundedExecution).toBe(false);
 const d=setup(),o=d.options();o.fetch=async()=>Response.json({v:1,error:'RESPONSE_BUDGET'},{status:503}) as any;
 expect((await readCapacity(o)).port.pages[0].error).toBe('RESPONSE_BUDGET');
});
it('transport loss and malformed native pages have bounded error facts without reflecting arbitrary payloads',async()=>{
 for(const failure of ['transport','malformed']){const c=setup(),o=c.options();o.fetch=async()=>{if(failure==='transport')throw Error('PRIVATE');return Response.json({PRIVATE:'DO_NOT_ECHO'}) as any;};
 const s=await readCapacity(o);expect(s.eligibility.boundedExecution).toBe(false);expect(s.sampler.requestLoss).toBe(failure==='transport'?1:0);expect(JSON.stringify(s)).not.toContain('PRIVATE');}
});
it('preserves duplicated latest-window rates without integrating a missing interval',async()=>{
 const c=setup(),now=Date.now();Object.assign(c.current.slots[0].sample,{windowInstance:1,windowSequence:1,windowStart:now-5000,windowEnd:now,windowSockets:3,windowSocketSamples:1,windowFramesPerSecond:4,windowBytesPerSecond:8,windowDeliveryMs:2,windowQueueAgeMs:1,windowRpcMs:1});
 const a=await readCapacity(c.options()),same=await readCapacity(c.options());expect(same.services.find((r:any)=>r.scope==='gateway').frames).toBeNull();expect(a.holders[0].sample.windowSequence).toBe(same.holders[0].sample.windowSequence);
 c.current.slots[0].sample.windowSequence=4;const gap=await readCapacity(c.options());expect(gap.eligibility.reasons).toContain('SOURCE_WINDOW_GAP');
});
it('allows only the exact bound native GET query shape',()=>{
 const c=setup(),base=c.b.telemetry.url;
 expect(capacityFetchAllowed(`${base}?cell=0&cursor=0&limit=4&eventCursor=0&eventLimit=16&fresh=0&runId=${c.b.runId}`,c.b)).toBe(true);
 expect(capacityFetchAllowed(`${base}?cell=0&holder=foreign`,c.b)).toBe(false);
});
it.skipIf(!process.env.GONVEX_CAPACITY_NATIVE_CAPTURE)('translates captured real local workerd pages while keeping missing identities and history NOT_ELIGIBLE',async()=>{
 const captured=JSON.parse(readFileSync(process.env.GONVEX_CAPACITY_NATIVE_CAPTURE!,'utf8'));
 const c=setup(),o=c.options();c.b.runId='DATA-native';c.b.telemetry.cells=[28];c.b.telemetry.fresh=true;
 o.fetch=async()=>Response.json(captured.fresh) as any;
 const s=await readCapacity(o);expect(s.holders[0].sample.sockets).toBe(1);expect(s.induced.capacityAttempts).toBe(1);expect(s.induced.actualHolderWakeups).toBeNull();
 expect(s.sourceHistory).toBe('UNKNOWN');expect(s.eligibility.fullCapacity).toBe('NOT_ELIGIBLE');
});
