import { env, runInDurableObject } from 'cloudflare:test';
import { it, expect } from 'vitest';
import { capacityPage } from './fixtures/k22-capacity';
import { controllerCell, ASSIGNMENT_HEADER } from '../../kernel/gateway/placement';

it('captures real local Controller/Gateway cached and fresh v1 pages, never a remote load result',async()=>{
 const e=env as any,c=e.GATEWAY_CONTROLLER.getByName('gateway-cell-28');
 const connection=Array.from({length:2048},(_,i)=>`capacity-adapter-native-${i}`).find(id=>controllerCell(id)===28)!;
 expect(connection).toBeTypeOf('string');const assignment=await c.acquire(connection),holder=e.GATEWAY.getByName(assignment.gateway);
 const response=await holder.fetch(new Request('https://local/ws',{headers:{upgrade:'websocket','x-gonvex-connection-id':connection,[ASSIGNMENT_HEADER]:JSON.stringify(assignment)}}));
 expect(response.status).toBe(101);response.webSocket.accept();
 try{
  const bindings={...e,RUNTIME_CATALOG:{identity:async()=>({version:'DATA',worker:{id:'DATA-only',tag:'DATA',timestamp:'2026-11-17T00:00:00Z'},releases:{DATA:'0'.repeat(64)}})}};
  const request=(fresh:number)=>new Request(`https://local/__assembly/capacity/v1?cell=28&fresh=${fresh}&runId=DATA-native`);
  const sampleCount=()=>runInDurableObject(holder,(o:any)=>o.metrics.socketSamples);
  const before=await sampleCount();const cached=await(await capacityPage(request(0),bindings)).json<any>();
  expect(cached.provenance).toMatchObject({catalogIdentityCalls:1,controllerStatusCalls:1,capacityCalls:0});expect(await sampleCount()).toBe(before);
  const fresh=await(await capacityPage(request(1),bindings)).json<any>();
  expect(fresh.slots[0].fresh.capacity).toMatchObject({status:'OK',value:{sample:{sockets:1,epoch:assignment.epoch}}});expect(await sampleCount()).toBe(before+1);
  // Content-free LOCAL DATA page capture only; no credentials, socket IDs, protocol frames or customer data.
  console.info('NATIVE_CAPACITY_CAPTURE='+JSON.stringify({cached,fresh}));
 }finally{response.webSocket.close(1000,'local fixture cleanup');await c.release(assignment);}
});
