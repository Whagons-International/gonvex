// Reviewer SOURCE regression: finite virtual workload, not capacity/performance evidence.
import { env, runInDurableObject } from "cloudflare:test";
import { expect, it, vi } from "vitest";
import type { GatewayControllerObject } from "../controller";
import { controllerCell, pressured } from "../placement";
import { GatewayMetrics } from "../metrics";
import { GatewayMetrics as OldMetrics } from "./fixtures/bc247-metrics";
import { GatewayControllerObject as OldController } from "./fixtures/bc247-controller";
const bindings=env as Env & {GATEWAY_CONTROLLER:DurableObjectNamespace<GatewayControllerObject>};
function key(cell:number){for(let n=0;;n++)if(controllerCell(`review-throughput-${n}`)===cell)return `review-throughput-${n}`;}
async function exercise(kind:string,old:boolean){
  const cell=(kind==="frames"?13:15)+(old?0:1);
  const c=bindings.GATEWAY_CONTROLLER.getByName(`gateway-cell-${cell}`);
  const a=await c.acquire(key(cell));await c.release(a);
  return runInDurableObject(c,async(object,state)=>{
    const o=object as any,proto=Object.getPrototypeOf(o),originalEnv=o.env;
    const start=Date.now(),clock=vi.spyOn(Date,"now");let probes=0,drains=0;
    const reports:any[]=[];
    try{
      clock.mockReturnValue(start);
      const m=old?new OldMetrics():new GatewayMetrics();m.rotate();
      const stored=await state.storage.get<any>("placement");stored.slots[0].changedAt=start-61000;
      stored.slots[0].sample=null;await state.storage.put("placement",stored);
      if(old)Object.setPrototypeOf(o,OldController.prototype);
      Object.defineProperty(o,"env",{configurable:true,value:{...originalEnv,GATEWAY:{getByName(name:string){return {
        async capacity(epoch:number){probes++;return name===a.gateway?m.snapshot(epoch,1):
          {v:1,epoch,at:Date.now(),sockets:0,framesPerSecond:0,bytesPerSecond:0,deliveryMs:0,queueAgeMs:0,rpcMs:0};},
        async drain(){drains++;return {closed:0,remaining:1,blocked:{}};}
      };}}}});
      // Normal event cadence: one batch per 100ms; report after delivery when due.
      // frames: 5,000/s; bytes: 5 MiB/s. Latency/queue/RPC all below pressure thresholds.
      const text=kind==="bytes"?"x".repeat(512*1024):"x";
      for(let elapsed=100;elapsed<=20000;elapsed+=100){
        clock.mockReturnValue(start+elapsed);
        for(let i=0;i<(kind==="frames"?500:1);i++)m.sent(text);
        m.delivery(1,1,1);
        if(m.due()){
          const p=m.snapshot(a.epoch,1);m.rotate();
          reports.push({at:elapsed,scalarHot:pressured(p),current:p.currentPressure,windowFrames:p.windowFramesPerSecond,windowBytes:p.windowBytesPerSecond});
          await o.observe(a.gateway,p);
        }
      }
      return {status:await o.status(),probes,drains,reports};
    }finally{clock.mockRestore();Object.setPrototypeOf(o,proto);Object.defineProperty(o,"env",{configurable:true,value:originalEnv});}
  });
}
it.each(["frames","bytes"])("reviewer: sustained %s pressure retains automatic replacement at the actual reporting cadence",async(kind)=>{
  const baseline=await exercise(kind,true),candidate=await exercise(kind,false);
  console.log(JSON.stringify({kind,baseline:{slots:baseline.status.slots.length,modes:baseline.status.slots.map((s:any)=>s.mode),reports:baseline.reports,probes:baseline.probes},candidate:{slots:candidate.status.slots.length,modes:candidate.status.slots.map((s:any)=>s.mode),reports:candidate.reports,probes:candidate.probes}}));
  expect(baseline.status.slots).toHaveLength(2);
  expect(baseline.status.slots.find((s:any)=>s.mode==="draining")).toBeDefined();
  expect(candidate.status.slots).toHaveLength(2);
  expect(candidate.status.slots.find((s:any)=>s.mode==="draining")).toBeDefined();
});
