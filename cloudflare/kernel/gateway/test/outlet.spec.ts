import { expect, it } from "vitest";
import { GatewayOutlet, DELIVERY_CAPACITY_CODE, deliveryPressure } from "../../delivery/outlet";
import type { ReplicaDelivery } from "../../delivery/protocol";

const payload = (instance: string, text = ""): ReplicaDelivery => ({ v: 1, instance, shared: [text], sockets: {} });
const recovery = (): ReplicaDelivery => ({ v: 1, instance: "reset", shared: [], sockets: {}, recovery: { reason: "change-feed-lagged" } });

it("discards normal bytes immediately, coalesces recovery, and preserves in-flight order", async () => {
  let unblock!: () => void;
  const gate = new Promise<void>(r => { unblock = r; });
  const sent: string[] = [];
  const outlet = new GatewayOutlet("tenant", () => ({ async deliverReplica(_tenant, p) {
    sent.push(p.instance);
    if (p.instance === "held") await gate;
    return { v: 1, sockets: 1, ...(p.recovery ? { recoveryAck: 1 as const } : {}) };
  } }), async () => ["gw"]);
  const first = outlet.send("gw", payload("held"));
  await Promise.resolve();
  const obsolete = outlet.send("gw", payload("obsolete", "a".repeat(512 * 1024)));
  const rejected = outlet.send("gw", payload("overflow", "b".repeat(1024 * 1024)));
  await expect(rejected).rejects.toMatchObject({ code: DELIVERY_CAPACITY_CODE });
  const reset = outlet.reset("gw", recovery());
  expect(outlet.reset("gw", recovery())).toBe(reset);
  expect(await obsolete).toBeUndefined();
  expect(sent).toEqual(["held"]);
  let idle = false;
  void outlet.idle("gw").then(() => { idle = true; });
  await Promise.resolve(); expect(idle).toBe(false);
  unblock(); await Promise.all([first, reset]); await outlet.idle("gw");
  expect(sent).toEqual(["held", "reset"]);
  expect(outlet.queued("gw")).toBe(0);
});

it("requires explicit recovery acknowledgment across old-holder skew and retries only explicitly", async () => {
  let understands = false, calls = 0;
  const outlet = new GatewayOutlet("t", () => ({ async deliverReplica() {
    calls++; return { v: 1, sockets: 0, ...(understands ? { recoveryAck: 1 as const } : {}) };
  } }), async () => ["gw"]);
  await expect(outlet.reset("gw", recovery())).rejects.toMatchObject({ code: "GONVEX_DELIVERY_RECOVERY_UNSUPPORTED" });
  await expect(outlet.send("gw", payload("normal"))).rejects.toMatchObject({ code: DELIVERY_CAPACITY_CODE });
  expect(calls).toBe(1);
  understands = true;
  await outlet.reset("gw", recovery());
  await outlet.send("gw", payload("normal"));
  expect(calls).toBe(3);
});

it("shares oversized-singleton credits across different tenant outlets without lowering the legal frame limit", async () => {
  let unblock!: () => void;
  const gate = new Promise<void>(r => { unblock = r; });
  const one = new GatewayOutlet("one", () => ({ async deliverReplica() { await gate; return { v: 1, sockets: 1 }; } }), async () => ["a"]);
  const two = new GatewayOutlet("two", () => ({ async deliverReplica(_t, p) { return { v: 1, sockets: 1, ...(p.recovery ? { recoveryAck: 1 as const } : {}) }; } }), async () => ["b"]);
  // Legal text exceeds pending budget; only one such retained payload in this isolate at a time.
  const large = one.send("a", payload("large", "x".repeat(3 * 1024 * 1024)));
  await expect(two.send("b", payload("also-large", "x".repeat(3 * 1024 * 1024)))).rejects.toMatchObject({ code: DELIVERY_CAPACITY_CODE });
  await two.reset("b", recovery());
  unblock(); await large;
  await two.send("b", payload("recovered"));
});

it("supports weak ownership accounting in the pinned runtime", () => {
  const owner = {};
  expect(new WeakRef(owner).deref()).toBe(owner);
});

it("charges membership-only payloads before retaining them and preserves the legal singleton", async () => {
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const baseline = deliveryPressure();
  const outlet = new GatewayOutlet("membership", () => ({ async deliverReplica(_t, p) {
    if (!p.recovery) await held;
    return { v: 1, sockets: 0, ...(p.recovery ? { recoveryAck: 1 as const } : {}) };
  } }), async () => ["gw"]);
  // No socket work/shared rows: these arrays alone exceed the pending byte budget.
  const membership = { revision: 1, members: Array.from({ length: 30_000 }, (_, i) => `member-${i}-xxxxxxxxxxxx`),
    accounts: Array.from({ length: 30_000 }, (_, i) => `account-${i}-xxxxxxxxxxx`) };
  const first = outlet.send("gw", { ...payload("membership"), membership });
  try {
    expect(deliveryPressure().normal - baseline.normal).toBeGreaterThan(8 * 1024 * 1024);
    await expect(outlet.send("gw", { ...payload("rejected"), membership })).rejects.toMatchObject({ code: DELIVERY_CAPACITY_CODE });
    expect(deliveryPressure().count - baseline.count).toBe(1);
    const reset = outlet.reset("gw", recovery());
    release(); await Promise.all([first, reset]);
    expect(deliveryPressure().normal).toBe(baseline.normal);
    expect(deliveryPressure().count).toBe(baseline.count);
  } finally { release(); await first; }
});

it("settles idle and frees host metadata when queued work is discarded before dispatch", async () => {
  const baseline = deliveryPressure();
  let calls = 0;
  const outlet = new GatewayOutlet("discard", () => ({ async deliverReplica() { calls++; return { v: 1, sockets: 0 }; } }), async () => []);
  const work = outlet.send("gw", payload("discard"));
  let idle = false;
  const drained = outlet.idle("gw").then(() => { idle = true; });
  outlet.discard("gw");
  await work; await Promise.resolve();
  expect(idle).toBe(true);
  await drained;
  expect(calls).toBe(0);
  expect(deliveryPressure()).toMatchObject({ normal: baseline.normal, count: baseline.count, hosts: baseline.hosts });
});

it("settles an empty failed recovery queue while retaining its required-recovery fence", async () => {
  const baseline = deliveryPressure();
  let supported = false;
  const outlet = new GatewayOutlet("blocked", () => ({ async deliverReplica() {
    return { v: 1, sockets: 0, ...(supported ? { recoveryAck: 1 as const } : {}) };
  } }), async () => []);
  await expect(outlet.reset("gw", recovery())).rejects.toMatchObject({ code: "GONVEX_DELIVERY_RECOVERY_UNSUPPORTED" });
  outlet.discard("gw");
  await outlet.idle("gw");
  expect(deliveryPressure().hosts).toBe(baseline.hosts + 1);
  await expect(outlet.send("gw", payload("still-blocked"))).rejects.toMatchObject({ code: DELIVERY_CAPACITY_CODE });
  supported = true;
  await outlet.reset("gw", recovery());
  expect(deliveryPressure().hosts).toBe(baseline.hosts);
});

it("gives quiet tenants shared RPC credits while a busy tenant still has queued holders", async () => {
  const held: Array<() => void> = [];
  let releasing = false, quietStarted = false;
  const busy = new GatewayOutlet("busy", () => ({ async deliverReplica() {
    if (!releasing) await new Promise<void>(resolve => { held.push(resolve); });
    return { v: 1, sockets: 1 };
  } }), async () => []);
  const quiet = new GatewayOutlet("quiet", () => ({ async deliverReplica() { quietStarted = true; return { v: 1, sockets: 1 }; } }), async () => []);
  const work = Array.from({ length: 12 }, (_, i) => busy.send(`busy-${i}`, payload("busy")));
  try {
    for (let i = 0; i < 12; i++) await Promise.resolve();
    expect(held.length).toBe(8);
    const quietWork = quiet.send("quiet", payload("quiet"));
    held[0]!(); held[1]!();
    for (let i = 0; i < 20; i++) await Promise.resolve();
    expect(quietStarted).toBe(true);
    expect(held.length).toBeLessThan(12);
    await quietWork;
  } finally { releasing = true; for (const release of held) release(); await Promise.all(work); }
});

const initialFailure = (instance: string, requests: Record<string, readonly import('../../delivery/protocol').InitialRequestRef[]>): ReplicaDelivery => ({v:1,instance,shared:[],sockets:{},initialFailure:{v:1,reason:'delivery-recovery-failed',requests}});
it('initial-failure skew never calls an old ordinary receiver, including local placement', async()=>{
 let ordinary=0;
 const old=new GatewayOutlet('old',()=>({async deliverReplica(){ordinary++;return {v:1,sockets:1};}}),async()=>[]);
 await expect(old.send('holder',initialFailure('must-not-apply',{c:[{kind:'query',id:'q',path:'p',epoch:1,generation:1}]}))).rejects.toThrow('GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED');
 expect(ordinary).toBe(0);
 const {LocalOutlet}=await import('../../delivery/outlet');
 const local=new LocalOutlet(()=>{ordinary++;return {v:1,sockets:1};},()=>[]);
 await expect(local.send('local',initialFailure('must-not-apply',{}))).rejects.toThrow('GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED');
 expect(ordinary).toBe(0);
});
it('marker-only delivery rejects mixed data and requires processing acknowledgment', async()=>{
 let calls=0;
 const outlet=new GatewayOutlet('new',()=>({async deliverReplica(){throw new Error('wrong receiver');},async deliverInitialFailures(){calls++;return {v:1,sockets:0};}}),async()=>[]);
 const marker=initialFailure('not-instance-authority',{});
 await expect(outlet.send('holder',{...marker,watermark:5})).rejects.toThrow('invalid initial-failure-only');
 expect(calls).toBe(0);
 await expect(outlet.send('holder',marker)).rejects.toThrow('GONVEX_DELIVERY_INITIAL_FAILURE_UNSUPPORTED');
 expect(calls).toBe(1);
});
it('charges distinct failure ref paths/envelopes across two outlet owners before retention',async()=>{
 let release!:()=>void;const gate=new Promise<void>(r=>{release=r;});
 const receiver={async deliverReplica(_t:string,p:ReplicaDelivery){return {v:1,sockets:0,...(p.recovery?{recoveryAck:1 as const}:{})};},async deliverInitialFailures(){await gate;return {v:1,sockets:1,initialFailureAck:1 as const};}};
 const a=new GatewayOutlet('large-failure-a',()=>receiver,async()=>[]),b=new GatewayOutlet('large-failure-b',()=>receiver,async()=>[]);
 const refs=(n:number)=>Array.from({length:n},(_,i)=>({kind:'query' as const,id:`q-${i}`,path:`${i}`+'p'.repeat(100000),epoch:1,generation:i+1}));
 const first=a.send('a',initialFailure('a',{socket:refs(12)}));
 try{
  expect(deliveryPressure().normal).toBeGreaterThan(9_600_000);
  await expect(b.send('b',initialFailure('b',{other:refs(10)}))).rejects.toMatchObject({code:DELIVERY_CAPACITY_CODE});
  await b.reset('b',recovery());
 }finally{release();await first;}
});
it('initial data/completion skew never invokes an old ordinary receiver',async()=>{
 let ordinary=0;
 const outlet=new GatewayOutlet('old-initial',()=>({async deliverReplica(){ordinary++;return {v:1,sockets:1};}}),async()=>[]);
 const request={kind:'query' as const,id:'q',path:'items.live',epoch:1,generation:1};
 const frame='{"type":"query.result","id":"q","path":"items.live","reason":"initial","result":[]}';
 const p:ReplicaDelivery={v:1,instance:'must-not-apply',shared:[],sockets:{c:{subscriptionEpoch:1,initialScope:request,ops:[['f',frame]],initialCompletions:[{request,opIndex:0,outcome:'accepted'}]}}};
 await expect(outlet.send('old',p)).rejects.toThrow('GONVEX_INITIAL_DELIVERY_UNSUPPORTED');expect(ordinary).toBe(0);
});

it('0070 recovery offers share the existing reserve across owners and cannot use oversized-singleton admission',async()=>{
 let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
 const receiver={async deliverReplica(_tenant:string,p:ReplicaDelivery){if(p.instance==='held-recovery')await gate;return {v:1,sockets:0,recoveryAck:1 as const};}};
 const a=new GatewayOutlet('recovery-facts-a',()=>receiver,async()=>[]),b=new GatewayOutlet('recovery-facts-b',()=>receiver,async()=>[]);
 const refs=(n:number)=>Array.from({length:n},(_,i)=>({kind:'query' as const,id:'q'+i,path:'p',epoch:1,generation:i+1}));
 const first=a.reset('a',{...recovery(),instance:'held-recovery',recoveryInitials:{connection:refs(4000)}});
 try{
  expect(deliveryPressure().recovery).toBeGreaterThan(6_000_000);
  await expect(b.reset('b',{...recovery(),recoveryInitials:{connection:refs(2000)}})).rejects.toMatchObject({code:DELIVERY_CAPACITY_CODE});
  await expect(b.reset('oversized',{...recovery(),recoveryInitials:{connection:[{...refs(1)[0]!,path:'x'.repeat(2_000_000)}]}})).rejects.toMatchObject({code:DELIVERY_CAPACITY_CODE});
 }finally{release();await first;await b.reset('b',recovery());await b.reset('oversized',recovery());}
 expect(deliveryPressure().recovery).toBe(0);
});
