// #550 review: accounting of the 04-hang reclaims when the RPC involved does settle later. Plain outlets in the test
// isolate (a real reset object's RPCs never settle, so these paths need a holder that answers late).
import { expect, it } from "vitest";
import { GatewayOutlet, deliveryPressure, setRpcSlotStaleMs, type ReplicaHolderRpc } from "../../delivery/outlet";
import type { ReplicaDelivery } from "../../delivery/protocol";

const payload = (instance: string): ReplicaDelivery => ({ v: 1, instance, shared: ["x".repeat(64)], sockets: {} });
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** A holder that holds every call until `release`, recording what it received. */
function lateHolder() {
  const gates: Array<() => void> = [], received: string[] = [];
  const rpc: ReplicaHolderRpc = { async deliverReplica(_t, p) { received.push(p.instance); await new Promise<void>((r) => gates.push(r)); return { v: 1, sockets: 0 }; } };
  return { rpc, received, release: () => { for (const g of gates.splice(0)) g(); } };
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
