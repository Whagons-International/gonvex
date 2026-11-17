import { env, evictDurableObject, runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";
import { expect, it } from "vitest";
import type { GatewayControllerObject } from "../controller";
import { ASSIGNMENT_HEADER, controllerCell, WINDOW_FIELDS } from "../placement";
const bindings = env as Env & { GATEWAY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject> };
function key(cell: number) { for (let n = 0; ; n++) { const id = `fresh-pressure-${n}`; if (controllerCell(id) === cell) return id; } }

it("does not turn an expired transient observation into a hot-holder drain on alarm/restart", async () => {
  const controller = bindings.GATEWAY_CONTROLLER.getByName("gateway-cell-20");
  const assignment = await controller.acquire(key(20));
  const holder = bindings.GATEWAY.getByName(assignment.gateway);
  const response = await holder.fetch(new Request("https://test/ws", { headers: { upgrade: "websocket",
    "x-gonvex-connection-id": assignment.connectionId, [ASSIGNMENT_HEADER]: JSON.stringify(assignment) } }));
  expect(response.status).toBe(101); response.webSocket!.accept();
  try {
    await controller.observe(assignment.gateway, { v: 1, epoch: assignment.epoch, at: Date.now(), sockets: 1,
      framesPerSecond: 0, bytesPerSecond: 0, deliveryMs: 150, queueAgeMs: 0, rpcMs: 0 });
    // Local-only durable-data seam: elapsed idle time, lease cleanup and cooldown, no new activity.
    await runInDurableObject(controller, async (_object, state) => {
      const stored = (await state.storage.get<any>("placement"))!;
      stored.leases = [];
      stored.slots[0].changedAt = Date.now() - 61_000;
      stored.slots[0].pressuredSince = Date.now() - 31_000;
      stored.slots[0].sample.at = Date.now() - 31_000;
      await state.storage.put("placement", stored);
    });
    await evictDurableObject(controller);
    expect(await runDurableObjectAlarm(controller)).toBe(true);
    const status = await controller.status();
    expect(status.slots).toHaveLength(1);
    expect(status.slots[0]!.mode).toBe("active");
    expect(status.events.filter(event => event.action === "drain")).toHaveLength(0);
    expect((await (holder as any).capacity(assignment.epoch)).sockets).toBe(1);
  } finally { response.webSocket!.close(); }
});

// Exact completed measurement inputs; finite local data seams, never a serving clock/deadline policy.
function packet(epoch: number, end: number, sequence = 0, instance = 1) {
  return { v: 1 as const, epoch, at: Date.now(), sockets: 1, framesPerSecond: 0, bytesPerSecond: 0,
    deliveryMs: 150, queueAgeMs: 0, rpcMs: 0, currentPressure: 1 as const,
    windowInstance: instance, windowSequence: sequence, windowStart: end - 5_000, windowEnd: end,
    windowSockets: 1, windowSocketSamples: 1, windowFramesPerSecond: 0, windowBytesPerSecond: 0,
    windowDeliveryMs: 150, windowQueueAgeMs: 0, windowRpcMs: 0 };
}
async function setup(n: number) {
  const controller = bindings.GATEWAY_CONTROLLER.getByName(`gateway-cell-${n}`);
  const assignment = await controller.acquire(key(n));
  await controller.release(assignment);
  await runInDurableObject(controller, async (_o, state) => {
    const stored = (await state.storage.get<any>("placement"))!;
    stored.slots[0].changedAt = Date.now() - 61_000; stored.slots[0].sample = null;
    await state.storage.put("placement", stored);
  });
  return { controller, assignment };
}

it("never matures a repeatedly restamped single completed bucket", async () => {
  const {controller, assignment} = await setup(21), end = Date.now() - 6_000;
  for (let n = 0; n < 3; n++) await controller.observe(assignment.gateway, packet(assignment.epoch, end));
  const s = await controller.status();
  expect(s.slots).toHaveLength(1); expect(s.slots[0]!.pressuredSince).toBe(end - 5_000);
  expect(s.events.filter(e => e.action === "drain")).toHaveLength(0);
});

it("revalidates a mature hot slot BEFORE any replacement allocation when its actual holder is quiet", async () => {
  const {controller, assignment} = await setup(22), end = Date.now() - 100;
  const first = packet(assignment.epoch, end - 5_000), second = packet(assignment.epoch, end, 1);
  await controller.observe(assignment.gateway, first);
  await controller.observe(assignment.gateway, second);
  const s = await controller.status();
  expect(s.slots).toHaveLength(1); expect(s.slots[0]!.mode).toBe("active");
  expect(s.slots[0]!.pressuredSince).toBeNull();
  expect(s.events.filter(e => e.action === "allocate")).toHaveLength(1);
});

it("resets source maturity on skipped buckets, new instances and sequence regression", async () => {
  const {controller, assignment} = await setup(23), end = Date.now() - 100;
  await controller.observe(assignment.gateway, packet(assignment.epoch, end - 5_000));
  await controller.observe(assignment.gateway, packet(assignment.epoch, end, 2)); // one bucket was unreported
  expect((await controller.status()).slots[0]!.pressuredSince).toBe(end - 5_000);
  await controller.observe(assignment.gateway, packet(assignment.epoch, end, 0, 2)); // restart, overlapping old instance is refused
  expect((await controller.status()).slots[0]!.pressuredSince).toBeNull();
  await controller.observe(assignment.gateway, packet(assignment.epoch, end, 1)); // regression cannot rewrite high-water bucket
  expect((await controller.status()).slots[0]!.sample!.windowSequence).toBe(2);
  expect((await controller.status()).slots[0]!.pressuredSince).toBeNull();
  expect((await controller.status()).slots).toHaveLength(1);
});

it("refuses partial, nonnumeric, unsafe, future and revised same-sequence evidence", async () => {
  const {controller, assignment} = await setup(24), original = packet(assignment.epoch, Date.now() - 100);
  await controller.observe(assignment.gateway, original);
  const before = await controller.status();
  const partial = {...original} as any; delete partial.windowRpcMs;
  for (const invalid of [partial, {...original, windowInstance: NaN}, {...original, windowSocketSamples: 0.5},
    {...original, windowSequence: Number.MAX_SAFE_INTEGER + 1}, {...original, windowEnd: Date.now() + 10_000},
    {...original, windowInstance: "not-numeric"}, {...original, currentPressure: 2}]) {
    await controller.observe(assignment.gateway, invalid as any);
    expect((await controller.status()).slots[0]!.sample).toEqual(before.slots[0]!.sample);
  }
  await controller.observe(assignment.gateway, {...original, windowDeliveryMs: 999});
  expect((await controller.status()).slots[0]!.pressuredSince).toBeNull();
  expect((await controller.status()).slots[0]!.sample!.windowDeliveryMs).toBe(150);
});

it("accepts genuinely legacy flat samples for admission but never invents their window history", async () => {
  const {controller, assignment} = await setup(25);
  const {GatewayMetrics: PreviousMetrics} = await import("./fixtures/bc247-metrics");
  const old = await runInDurableObject(bindings.GATEWAY.getByName(assignment.gateway), object => {
    const metrics = new PreviousMetrics(); metrics.delivery(150);
    Object.defineProperty(object, "metrics", {value: metrics});
    return metrics.snapshot(assignment.epoch, 0);
  });
  await controller.observe(assignment.gateway, old);
  const state = await controller.status();
  expect(state.slots[0]!.sample).toEqual(old); expect(state.slots[0]!.pressuredSince).toBeNull();
  expect(Object.hasOwn(state.slots[0]!.sample!, "windowInstance")).toBe(false);
  expect(Object.hasOwn(state.slots[0]!.sample!, "currentPressure")).toBe(false);
  const next = await controller.acquire(key(25)); // actual holder probe remains usable, no disabled admission
  expect(next.gateway).not.toBe(assignment.gateway); expect(await controller.claim(next)).toBe(true);
  expect((await controller.status()).slots).toHaveLength(2);
});

it("delivers numeric new observations through the exact bc247 old-controller implementation", async () => {
  const {GatewayControllerObject: Previous} = await import("./fixtures/bc247-controller");
  const {controller, assignment} = await setup(26);
  const modern = packet(assignment.epoch, Date.now() - 100);
  await runInDurableObject(controller, async (object) => {
    // Native startup already ran the constructor (unchanged from bc247). Execute the exact frozen
    // previous implementation on that native state/env and single serial tail, not a second controller.
    const prototype = Object.getPrototypeOf(object);
    Object.setPrototypeOf(object, Previous.prototype);
    try {
      await object.observe(assignment.gateway, modern);
      expect((await object.status()).slots[0]!.sample).toEqual(modern);
    } finally { Object.setPrototypeOf(object, prototype); }
  });
  await evictDurableObject(controller); // current controller can read state written by previous code
  expect((await controller.status()).slots[0]!.sample).toEqual(modern);
  await controller.observe(assignment.gateway, {...modern, at: Date.now()});
  const marker = (await controller.status()).slots[0]!.pressureEvidence;
  expect(marker?.epoch).toBe(assignment.epoch);
  await runInDurableObject(controller, async (object) => {
    const prototype = Object.getPrototypeOf(object);
    Object.setPrototypeOf(object, Previous.prototype);
    try {
      expect((await object.status()).slots[0]!.sample).toEqual({...modern,at:(await object.status()).slots[0]!.sample!.at});
      expect((await object.status()).slots[0]!.pressureEvidence).toEqual(marker);
    }
    finally { Object.setPrototypeOf(object, prototype); }
  });
});

it("clears maturity on a nonoverlapping new metrics instance", async () => {
  const {controller, assignment} = await setup(27), end = Date.now() - 100;
  await controller.observe(assignment.gateway, packet(assignment.epoch, end - 5_000, 5, 1));
  await controller.observe(assignment.gateway, packet(assignment.epoch, end, 0, 2));
  const s = await controller.status();
  expect(s.slots[0]!.pressuredSince).toBe(end - 5_000); expect(s.slots[0]!.sample!.windowInstance).toBe(2);
  expect(s.slots).toHaveLength(1);
});

it("does not allocate or publish drain on a failed authoritative hot re-probe", async () => {
  const {controller, assignment} = await setup(28), end = Date.now() - 100;
  const previous = packet(assignment.epoch, end - 5_000), latest = packet(assignment.epoch, end, 1);
  await controller.observe(assignment.gateway, previous);
  const work = await runInDurableObject(controller, async (_object, state) => {
    let probes = 0, drains = 0;
    const actual = _object as any, originalEnv = actual.env;
    Object.defineProperty(actual, "env", {configurable:true,value:{...originalEnv,GATEWAY:{getByName(){return {
      async capacity(){probes++;throw new Error("holder unavailable");},
      async drain(){drains++;throw new Error("must not dispatch");},
    };}}}});
    try { await actual.observe(assignment.gateway, latest); return {probes,drains,status:await actual.status()}; }
    finally { Object.defineProperty(actual,"env",{configurable:true,value:originalEnv}); }
  });
  expect(work.probes).toBe(1); expect(work.drains).toBe(0);
  expect(work.status.slots).toHaveLength(1); expect(work.status.slots[0]!.pressuredSince).toBeNull();
});

it("copies current observations before persistence and does not extend out-of-order source evidence", async () => {
  const {controller, assignment} = await setup(29), input = packet(assignment.epoch, Date.now() - 100);
  await runInDurableObject(controller, async (object) => {
    await object.observe(assignment.gateway, input);
    input.windowDeliveryMs = 987;
    expect((await object.status()).slots[0]!.sample!.windowDeliveryMs).toBe(150);
  });
  const before = await controller.status();
  await controller.observe(assignment.gateway, {...input, at: before.slots[0]!.sample!.at - 1, windowDeliveryMs: 1});
  expect((await controller.status()).slots[0]!.sample).toEqual(before.slots[0]!.sample);
});

it("keeps delayed nonempty quiet scale-in and bounded maintenance work, then retires only after close", async () => {
  const controller = bindings.GATEWAY_CONTROLLER.getByName("gateway-cell-30");
  const assignment = await controller.acquire(key(30));
  const holder = bindings.GATEWAY.getByName(assignment.gateway);
  const response = await holder.fetch(new Request("https://test/ws", {headers:{upgrade:"websocket",
    "x-gonvex-connection-id":assignment.connectionId,[ASSIGNMENT_HEADER]:JSON.stringify(assignment)}}));
  expect(response.status).toBe(101); response.webSocket!.accept();
  // Local fixture data only: model an already-authenticated idle holder, rather than timing a long
  // allocation fixture against an unrelated pending-auth deadline. Runtime auth/deadlines are unchanged.
  await runInDurableObject(holder, object => {
    for (const [socket, session] of (object as any).host.sessions) {
      session.record.pendingUntil = null;
      socket.serializeAttachment({...socket.deserializeAttachment(), session: session.record});
    }
  });
  try {
    const leases = [];
    for (let n = 0; n < 10_000; n++) {
      const id = `quiet-pool-${n}`; if (controllerCell(id) !== 30) continue;
      const next = await controller.acquire(id); leases.push(next);
      if (next.gateway !== assignment.gateway) break;
    }
    expect(leases.some(a => a.gateway !== assignment.gateway)).toBe(true);
    for (const lease of leases) await controller.release(lease);
    expect((await (holder as any).capacity(assignment.epoch)).sockets).toBe(1);
    const work = await runInDurableObject(controller, async (_object, state) => {
      const stored = (await state.storage.get<any>("placement"))!; stored.leases = [];
      for (const slot of stored.slots) {slot.quietSince = Date.now() - 121_000;slot.changedAt = Date.now() - 121_000;}
      await state.storage.put("placement",stored);
      let probes = 0, drains = 0;
      const actual = _object as any, originalEnv = actual.env;
      Object.defineProperty(actual,"env",{configurable:true,value:{...originalEnv,GATEWAY:{getByName(name:string){
        const rpc=bindings.GATEWAY.getByName(name) as any;return {
          async capacity(epoch:number){probes++;return rpc.capacity(epoch);},
          async drain(command:any){drains++;return rpc.drain(command);},
        };}}}});
      try { await actual.alarm(); return {probes,drains,status:await actual.status()}; }
      finally { Object.defineProperty(actual,"env",{configurable:true,value:originalEnv}); }
    });
    expect(work.probes).toBe(2); expect(work.drains).toBe(1);
    expect(work.status.slots.find((s: {name:string;mode:string})=>s.name===assignment.gateway)!.mode).toBe("draining");
    expect(work.status.slots.filter((s: {mode:string})=>s.mode==="active")).toHaveLength(1);
    expect(work.status.slots.filter((s: {mode:string})=>s.mode==="retired")).toHaveLength(0);
    response.webSocket!.close();
    await runInDurableObject(holder,async(object)=>{
      for(const socket of (object as any).host.sessions.keys()) await object.webSocketClose(socket,1000,"");
    });
    await runInDurableObject(controller,object=>object.alarm());
    expect((await controller.status()).slots.find(s=>s.name===assignment.gateway)!.mode).toBe("retired");
  } finally {response.webSocket!.close();}
});

it("never trusts a legacy wall-clock timer merely because a new single bucket is present in state-v1", async () => {
  const controller = bindings.GATEWAY_CONTROLLER.getByName("gateway-cell-31");
  const assignment = await controller.acquire(key(31)), holder = bindings.GATEWAY.getByName(assignment.gateway);
  const response = await holder.fetch(new Request("https://test/ws",{headers:{upgrade:"websocket",
    "x-gonvex-connection-id":assignment.connectionId,[ASSIGNMENT_HEADER]:JSON.stringify(assignment)}}));
  expect(response.status).toBe(101); response.webSocket!.accept();
  try {
    const one = await runInDurableObject(holder, async object => {
      const {GatewayMetrics} = await import("../metrics"); const {vi} = await import("vitest");
      const now=Date.now(),clock=vi.spyOn(Date,"now");
      try {
        clock.mockReturnValue(now-5_000);const metrics=new GatewayMetrics();metrics.delivery(150);
        clock.mockReturnValue(now);metrics.delivery(150);Object.defineProperty(object,"metrics",{value:metrics});
        return metrics.snapshot(assignment.epoch,1);
      } finally {clock.mockRestore();}
    });
    await runInDurableObject(controller,async(_object,state)=>{
      const stored=(await state.storage.get<any>("placement"))!;stored.leases=[];
      stored.slots[0].changedAt=Date.now()-61_000;stored.slots[0].pressuredSince=Date.now()-61_000;
      stored.slots[0].sample=one;delete stored.slots[0].pressureEvidence;
      await state.storage.put("placement",stored);
    });
    await evictDurableObject(controller);expect(await runDurableObjectAlarm(controller)).toBe(true);
    const status=await controller.status();expect(status.slots).toHaveLength(1);
    expect(status.slots[0]!.mode).toBe("active");expect(status.slots[0]!.pressuredSince).toBeNull();
  } finally {response.webSocket!.close();}
});

it("does not carry proof after rollback mutates the streak, window or marker epoch", async () => {
  const {controller,assignment}=await setup(19), sample=packet(assignment.epoch,Date.now()-100);
  await controller.observe(assignment.gateway,sample);
  for(const kind of ["absent","since","window","epoch"]){
    await runInDurableObject(controller,async(_object,state)=>{
      const stored=(await state.storage.get<any>("placement"))!,s=stored.slots[0];
      s.sample={...sample};s.pressuredSince=Date.now()-61_000;
      s.pressureEvidence={epoch:s.epoch,window:WINDOW_FIELDS.map(key=>sample[key]),since:s.pressuredSince};
      if(kind==="absent")delete s.pressureEvidence;
      if(kind==="since")s.pressureEvidence.since--;
      if(kind==="window")s.sample.windowDeliveryMs++;
      if(kind==="epoch")s.pressureEvidence.epoch++;
      await state.storage.put("placement",stored);
    });
    await controller.observe(assignment.gateway,{...sample,at:Date.now()});
    const status=await controller.status();expect(status.slots).toHaveLength(1);
    if(kind==="window")expect(status.slots[0]!.pressuredSince).toBeNull();
    else expect(status.slots[0]!.pressuredSince).toBe(sample.windowStart);
  }
});

it("keeps the 120-KiB routing-state guard and rolls back a refused admission's publication",async()=>{
  const {controller,assignment}=await setup(18);
  await runInDurableObject(controller,async(_object,state)=>{
    const stored=(await state.storage.get<any>("placement"))!;stored.testPadding="";
    const bytes=new TextEncoder().encode(JSON.stringify(stored)).byteLength;
    stored.testPadding="p".repeat(120*1024-bytes-100);
    await state.storage.put("placement",stored);
  });
  await expect((async()=>await controller.acquire(key(18)))()).rejects.toThrow("routing state byte ceiling");
  const status=await controller.status();expect(status.pending).toBe(0);
  expect(status.slots).toHaveLength(1);expect(status.slots[0]!.name).toBe(assignment.gateway);
});
