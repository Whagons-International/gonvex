// #550 review: accounting of the 04-hang reclaims when the RPC involved does settle later. Plain outlets in the test
// isolate (a real reset object's RPCs never settle, so these paths need a holder that answers late).
import { expect, it } from "vitest";
import { GatewayOutlet, MAX_RPCS, WAKE_SLACK_MS, deliveryPressure, dropWakeTimersForTests, setRpcSlotStaleMs, type ReplicaHolderRpc } from "../../delivery/outlet";
import type { ReplicaDelivery } from "../../delivery/protocol";

const payload = (instance: string): ReplicaDelivery => ({ v: 1, instance, shared: ["x".repeat(64)], sockets: {} });
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** A holder that holds every call until `release`, recording what it received. */
function lateHolder() {
  const gates: Array<() => void> = [], received: string[] = [];
  const rpc: ReplicaHolderRpc = { async deliverReplica(_t, p) { received.push(p.instance); await new Promise<void>((r) => gates.push(r)); return { v: 1, sockets: 0 }; } };
  return { rpc, received, release: () => { for (const g of gates.splice(0)) g(); } };
}
/** Waits (real time, bounded) until `condition` holds; fails with `what` instead of hanging into the test timeout. */
async function until(condition: () => boolean, what: string, ms = 5_000): Promise<void> {
  const end = Date.now() + ms;
  while (!condition()) { if (Date.now() > end) throw new Error(`timed out waiting for ${what}`); await sleep(2); }
}
const quick: ReplicaHolderRpc = { async deliverReplica() { return { v: 1, sockets: 0 }; } };
const totals = () => { const p = deliveryPressure(); return { running: p.running, count: p.count, normal: p.normal, recovery: p.recovery }; };

it("a live RPC that goes stale and then settles returns every slot and credit exactly once", async () => {
  const previous = setRpcSlotStaleMs(300);
  try {
    const before = totals(), staleBefore = deliveryPressure().staleSlots;
    const h = lateHolder();
    const slow = new GatewayOutlet("slow", () => h.rpc, async () => []);
    const first = slow.send("g", payload("a"));
    const behind = slow.send("g", payload("b"));
    await sleep(350);
    // Any later schedule() call finds the stale slot (its deadline passed) and returns it with the queued credits.
    await new GatewayOutlet("other", () => quick, async () => []).send("q", payload("q"));
    expect(deliveryPressure().staleSlots - staleBefore).toBe(1);
    expect(totals()).toEqual(before);
    // The slow holder answers: the stale RPC settles (its slot is not returned twice), the queued one goes and settles.
    h.release(); await first; await sleep(5); h.release(); await behind;
    expect(h.received).toEqual(["a", "b"]);
    expect(totals()).toEqual(before);
    expect(slow.queued("g")).toBe(0);
  } finally { setRpcSlotStaleMs(previous); }
});

it("a retired outlet's late batch refusal (requeue path) never puts it back in the scheduler", async () => {
  const before = totals();
  const gates: Array<() => void> = [], received: string[] = [];
  const hold = () => new Promise<void>((r) => gates.push(r));
  const release = () => { for (const g of gates.splice(0)) g(); };
  const rpc: ReplicaHolderRpc = {
    async deliverReplica(_t, p) { received.push(p.instance); await hold(); return { v: 1, sockets: 0 }; },
    async deliverReplicaBatch(_t, ps) {
      received.push(ps.map((p) => p.instance).join("+")); await hold();
      throw new Error('The RPC receiver does not implement the method "deliverReplicaBatch".');
    },
  };
  const dead = new GatewayOutlet("t", () => rpc, async () => [], "object-y");
  void dead.send("g", payload("a")).catch(() => {});
  await sleep(5);
  void dead.send("g", payload("b")).catch(() => {}); void dead.send("g", payload("c")).catch(() => {});
  release(); await sleep(5);
  expect(received).toEqual(["a", "b+c"]);
  new GatewayOutlet("t", () => quick, async () => [], "object-y"); // the next instance retires `dead`
  expect(totals()).toEqual(before);
  // The batch is refused late (deployment skew): a live outlet would requeue b and c and send them singly.
  release(); await sleep(20); release(); await sleep(20);
  expect(received).toEqual(["a", "b+c"]);
  expect(totals()).toEqual(before);
});

it("a retired outlet's late settlement returns its slot once and never re-enters the scheduler", async () => {
  const before = totals(), retiredBefore = deliveryPressure().retiredSlots;
  const h = lateHolder();
  const dead = new GatewayOutlet("t", () => h.rpc, async () => [], "object-x");
  void dead.send("g", payload("a")); void dead.send("g", payload("b")); void dead.send("g2", payload("c"));
  await sleep(5);
  expect(h.received).toEqual(["a", "c"]);
  // The object's next instance: the predecessor's outlet is retired, all its bookkeeping returned.
  const next = new GatewayOutlet("t", () => quick, async () => [], "object-x");
  expect(deliveryPressure().retiredSlots - retiredBefore).toBe(2);
  expect(totals()).toEqual(before);
  // The "dead" holder answers anyway: nothing is returned twice and "b" (behind "a") is never started.
  h.release(); await sleep(20);
  expect(totals()).toEqual(before);
  expect(h.received).toEqual(["a", "c"]);
  expect(await next.send("g", payload("n"))).toEqual({ v: 1, sockets: 0 });
  expect(totals()).toEqual(before);
});

const never: ReplicaHolderRpc = { deliverReplica: () => new Promise(() => {}) }; // a reset object's RPC: never settles
const within = <T>(promise: Promise<T>, ms: number) => Promise.race([promise.then(() => "delivered"), sleep(ms).then(() => "timeout")]);

it("a payload queued before the stale deadline is delivered at the deadline with no later activity (#550 re-review 2)", async () => {
  const previous = setRpcSlotStaleMs(300);
  try {
    const before = totals();
    const dead = new GatewayOutlet("dead", () => never, async () => []);
    for (let i = 0; i < MAX_RPCS; i++) void dead.send(`h${i}`, payload(`d${i}`));
    await sleep(20);
    // A quiet tenant: one send while every slot is held, then nothing else happens in the isolate.
    const sent = new GatewayOutlet("quiet", () => quick, async () => []).send("q", payload("q"));
    expect(await within(sent, 2_000)).toBe("delivered");
    expect(totals()).toEqual(before);
  } finally { setRpcSlotStaleMs(previous); }
});

it("a slot the stale rule returned is not returned again when its object retires (#550 re-review 4a)", async () => {
  const previous = setRpcSlotStaleMs(100);
  try {
    const before = totals(), p0 = deliveryPressure();
    void new GatewayOutlet("t", () => never, async () => [], "object-stale-retire").send("g", payload("s"));
    await sleep(150);
    await new GatewayOutlet("kick", () => quick, async () => []).send("k", payload("k")); // the sweep runs
    expect(deliveryPressure().staleSlots - p0.staleSlots).toBe(1);
    new GatewayOutlet("t", () => quick, async () => [], "object-stale-retire");
    expect(deliveryPressure().retiredOutlets - p0.retiredOutlets).toBe(1);
    expect(deliveryPressure().retiredSlots - p0.retiredSlots).toBe(0);
    expect(totals()).toEqual(before);
  } finally { setRpcSlotStaleMs(previous); }
});

it("a younger slot is still reclaimed after a sweep found only an older one stale (#550 re-review 4b)", async () => {
  const previous = setRpcSlotStaleMs(300);
  try {
    const before = totals(), p0 = deliveryPressure();
    const dead = new GatewayOutlet("staggered", () => never, async () => []);
    void dead.send("old", payload("old"));
    await sleep(200);
    void dead.send("young", payload("young"));
    await sleep(150); // old is stale, young is not
    await new GatewayOutlet("kick1", () => quick, async () => []).send("k", payload("k"));
    expect(deliveryPressure().staleSlots - p0.staleSlots).toBe(1);
    await sleep(250); // young is stale now
    await new GatewayOutlet("kick2", () => quick, async () => []).send("k", payload("k"));
    expect(deliveryPressure().staleSlots - p0.staleSlots).toBe(2);
    expect(totals()).toEqual(before);
  } finally { setRpcSlotStaleMs(previous); }
});

it("pruning keeps a live outlet's entry, so its object's next outlet still retires it (#550 re-review 4c)", async () => {
  const p0 = deliveryPressure();
  const live = new GatewayOutlet("t", () => quick, async () => [], "object-prune-live");
  for (let i = 0; i < 1_100; i++) new GatewayOutlet("t", () => quick, async () => [], `object-prune-${i}`); // past the prune threshold
  new GatewayOutlet("t", () => quick, async () => [], "object-prune-live");
  expect(deliveryPressure().retiredOutlets - p0.retiredOutlets).toBe(1);
  expect(live).toBeDefined();
});

it("a lost wake timer is replaced on the outlet's next own call once overdue (#550 round 3, item 1)", async () => {
  const previous = setRpcSlotStaleMs(300);
  dropWakeTimersForTests(true);
  try {
    const before = totals();
    const dead = new GatewayOutlet("dead-timer", () => never, async () => []);
    for (let i = 0; i < MAX_RPCS; i++) void dead.send(`h${i}`, payload(`d${i}`));
    await sleep(20);
    const quiet = new GatewayOutlet("quiet-timer", () => quick, async () => []);
    const sent = quiet.send("q", payload("q"));
    // Its wake timer is lost (the seam records it and never sets it): past the stale deadline nothing moves.
    expect(await within(sent, 300 + WAKE_SLACK_MS + 100)).toBe("timeout");
    dropWakeTimersForTests(false);
    // The owner's next call on its outlet (any: here a `queued` probe) finds the timer overdue and pumps from its context.
    quiet.queued("q");
    expect(await within(sent, 1_000)).toBe("delivered");
    expect(totals()).toEqual(before);
  } finally { dropWakeTimersForTests(false); setRpcSlotStaleMs(previous); }
});

it("dead waiters ahead in line (objects reset while waiting) do not block a live tenant", async () => {
  const previous = setRpcSlotStaleMs(300);
  const runningBefore = deliveryPressure().running;
  try {
    const dead = new GatewayOutlet("dw-holder", () => never, async () => []);
    for (let i = 0; i < MAX_RPCS; i++) void dead.send(`h${i}`, payload(`d${i}`));
    await sleep(10);
    // MAX_RPCS tenants queue behind the full slots, then their objects are reset: their wake timers die with them.
    dropWakeTimersForTests(true);
    const ghosts = Array.from({ length: MAX_RPCS }, (_, i) => new GatewayOutlet(`dw-ghost${i}`, () => never, async () => []));
    for (const g of ghosts) void g.send("g", payload("g"));
    await sleep(10);
    dropWakeTimersForTests(false);
    const live = new GatewayOutlet("dw-live", () => quick, async () => []);
    expect(await within(live.send("l", payload("l")), 2_000)).toBe("delivered");
  } finally {
    dropWakeTimersForTests(false);
    // Leave no slot behind: `dead`'s RPCs never settle, so their slots come back only through the 300 ms stale rule.
    // The live send needed one, and on a slow runner one sweep can find only the oldest stale (CI run 45124327662:
    // "stale RPC slots returned { slots: 1, running: 7 }"). Restoring the 60 s bound then would leave 7 slots held for
    // the next test. Keep calling schedule() until every slot is back, then restore it.
    const sweep = new GatewayOutlet("dw-sweep", () => quick, async () => []);
    await until(() => { void sweep.send("s", payload("s")); return deliveryPressure().running <= runningBefore; }, "dead RPC slots to be reclaimed");
    setRpcSlotStaleMs(previous);
  }
});

it("dead waiters ahead in line are skipped by a live pump within WAITER_LIVE_MS, not only by the 60 s sweep", async () => {
  // As above, but the slots come back by settlement (no stale sweep runs: the bound stays 60 s), so only `pump`'s
  // liveness check can let the live tenant past the dead waiters within the 2 s window.
  const h = lateHolder();
  const holder = new GatewayOutlet("dw2-holder", () => h.rpc, async () => []);
  const held = Array.from({ length: MAX_RPCS }, (_, i) => holder.send(`h${i}`, payload(`d${i}`)));
  // Every held RPC must be in flight (holding a slot) before the ghosts queue. One still waiting for a slot would have
  // its wake timer dropped with the ghosts', never start, and `Promise.all(held)` would hang into the 30 s test timeout
  // (CI run 45124327662, where the previous test had left 7 of the MAX_RPCS slots held). Fail fast and clearly instead.
  await until(() => h.received.length === MAX_RPCS, "all held RPCs to start");
  dropWakeTimersForTests(true);
  try {
    const ghosts = Array.from({ length: MAX_RPCS }, (_, i) => new GatewayOutlet(`dw2-ghost${i}`, () => never, async () => []));
    for (const g of ghosts) void g.send("g", payload("g"));
    await sleep(10);
  } finally { dropWakeTimersForTests(false); }
  h.release(); await Promise.all(held); // every slot is free again; the ghosts never poll
  const live = new GatewayOutlet("dw2-live", () => quick, async () => []);
  expect(await within(live.send("l", payload("l")), 2_000)).toBe("delivered");
});
