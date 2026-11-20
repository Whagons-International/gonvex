// #550 follow-up: the work-conserving handoff. A settling outlet that would leave a freed slot idle for a waiter
// ahead grants it a reservation and uses the slot itself; the waiter claims it from its own context at its next poll.
import { expect, it } from "vitest";
import { GatewayOutlet, MAX_RPCS, WAKE_SLACK_MS, deliveryPressure, dropWakeTimersForTests, type ReplicaHolderRpc } from "../../delivery/outlet";
import type { ReplicaDelivery } from "../../delivery/protocol";

const payload = (instance: string): ReplicaDelivery => ({ v: 1, instance, shared: ["x".repeat(64)], sockets: {} });
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const within = <T>(promise: Promise<T>, ms: number) => Promise.race([promise.then(() => "delivered"), sleep(ms).then(() => "timeout")]);
/** A holder whose calls stay in flight until released. */
function gated() {
  const held: Array<() => void> = [];
  let open = false;
  const rpc: ReplicaHolderRpc = { async deliverReplica() { if (!open) await new Promise<void>((r) => held.push(r)); return { v: 1, sockets: 0 }; } };
  return { rpc, held, releaseAll: () => { open = true; for (const r of held.splice(0)) r(); } };
}
const quick: ReplicaHolderRpc = { async deliverReplica() { return { v: 1, sockets: 0 }; } };
/** Samples `running` and `reservations` every ms while `work` runs. */
async function watch<T>(work: Promise<T>) {
  let maxRunning = 0, maxReserved = 0, done = false;
  void work.finally(() => { done = true; });
  while (!done) { const p = deliveryPressure(); maxRunning = Math.max(maxRunning, p.running); maxReserved = Math.max(maxReserved, p.reservations); await sleep(1); }
  return { maxRunning, maxReserved };
}

it("a settling tenant keeps its freed slot busy and the waiter ahead still starts from its own poll (work-conserving)", async () => {
  const busy = gated();
  const b = new GatewayOutlet("rs-busy", () => busy.rpc, async () => []);
  const work = Array.from({ length: 12 }, (_, i) => b.send(`b${i}`, payload(`b${i}`)));
  try {
    await sleep(5);
    expect(busy.held.length).toBe(MAX_RPCS);
    let quietStarted = false;
    const q = new GatewayOutlet("rs-quiet", () => ({ async deliverReplica() { quietStarted = true; return { v: 1, sockets: 0 }; } }), async () => []);
    const quietWork = q.send("q", payload("q"));
    await sleep(10); // the quiet tenant is in line
    // The busy tenant queued first: its first settlement is its own turn, then it goes behind the quiet tenant.
    busy.held.shift()!();
    await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
    const p0 = deliveryPressure();
    busy.held.shift()!();
    await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
    // Before: the busy tenant yielded the slot and it idled until the quiet tenant's timer. Now it dispatches again at once.
    expect(busy.held.length).toBe(MAX_RPCS);
    expect(deliveryPressure().reservationsGranted - p0.reservationsGranted).toBe(1);
    expect(await within(quietWork, 2_000)).toBe("delivered");
    expect(quietStarted).toBe(true);
    expect(deliveryPressure().reservationsClaimed - p0.reservationsClaimed).toBe(1);
    expect(deliveryPressure().reservations).toBe(0);
  } finally { busy.releaseAll(); await Promise.all(work); }
  expect(deliveryPressure().running).toBe(0);
});

it("reservations stay at most MAX_RPCS and running under 2 x MAX_RPCS, and every waiter is served", async () => {
  const busy = gated();
  const b = new GatewayOutlet("rs-busy2", () => busy.rpc, async () => []);
  const work = Array.from({ length: 40 }, (_, i) => b.send(`b${i}`, payload(`b${i}`)));
  await sleep(5);
  const waiters = Array.from({ length: MAX_RPCS + 6 }, (_, i) => new GatewayOutlet(`rs-w${i}`, () => quick, async () => []).send("w", payload(`w${i}`)));
  await sleep(10);
  const all = Promise.all([...waiters, ...work]);
  const seen = watch(all);
  // Release the busy tenant's calls one by one, as they would settle.
  for (let i = 0; i < 40; i++) { await sleep(1); busy.held.shift()?.(); }
  busy.releaseAll();
  await all;
  const { maxRunning, maxReserved } = await seen;
  expect(maxReserved).toBeLessThanOrEqual(MAX_RPCS);
  expect(maxRunning).toBeLessThan(2 * MAX_RPCS);
  expect(deliveryPressure()).toMatchObject({ running: 0, reservations: 0 });
});

it("a reservation granted to a dead waiter lapses after WAITER_LIVE_MS and blocks nobody", async () => {
  const busy = gated();
  const b = new GatewayOutlet("rs-busy3", () => busy.rpc, async () => []);
  const work = Array.from({ length: 12 }, (_, i) => b.send(`b${i}`, payload(`b${i}`)));
  await sleep(5);
  dropWakeTimersForTests(true); // the ghost's object is reset while it waits: it never polls again
  let ghostDelivered = false;
  const ghost = new GatewayOutlet("rs-ghost", () => ({ async deliverReplica() { ghostDelivered = true; return { v: 1, sockets: 0 }; } }), async () => []);
  void ghost.send("g", payload("g"));
  await sleep(10);
  dropWakeTimersForTests(false);
  busy.held.shift()!(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); // the busy tenant's own turn (it queued first)
  const p0 = deliveryPressure();
  busy.held.shift()!(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
  expect(deliveryPressure().reservationsGranted - p0.reservationsGranted).toBe(1); // granted to the ghost, never claimed
  await sleep(300); // past WAITER_LIVE_MS
  const live = new GatewayOutlet("rs-live", () => quick, async () => []).send("l", payload("l"));
  await sleep(10);
  // Two more settlements: the busy tenant's own turn (it is ahead of the live one), then the live tenant's.
  busy.held.shift()!(); await sleep(5);
  busy.held.shift()!();
  expect(await within(live, 2_000)).toBe("delivered");
  busy.releaseAll(); await Promise.all(work);
  // The ghost's reservation lapsed (a live pump walking past it dropped it); the ghost never started its RPC.
  expect(deliveryPressure().reservations).toBe(0);
  expect(deliveryPressure().reservationsLapsed - p0.reservationsLapsed).toBeGreaterThanOrEqual(1);
  expect(ghostDelivered).toBe(false);
});

it("at most MAX_RPCS reservations are outstanding even when waiters cannot claim them yet", async () => {
  const busy = gated();
  const b = new GatewayOutlet("rs-busy4", () => busy.rpc, async () => []);
  const work = Array.from({ length: 40 }, (_, i) => b.send(`b${i}`, payload(`b${i}`)));
  await sleep(5);
  // MAX_RPCS + 6 waiters whose wake timers are lost for now: they stay "live" (just seen) but cannot claim.
  dropWakeTimersForTests(true);
  const outlets = Array.from({ length: MAX_RPCS + 6 }, (_, i) => new GatewayOutlet(`rs-c${i}`, () => quick, async () => []));
  const waiting = outlets.map((o, i) => o.send("w", payload(`c${i}`)));
  await sleep(5);
  dropWakeTimersForTests(false);
  for (let i = 0; i < 20; i++) { busy.held.shift()?.(); await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); }
  const p = deliveryPressure();
  expect(p.reservations).toBe(MAX_RPCS);
  expect(p.running).toBeLessThan(2 * MAX_RPCS);
  // Cleanup: the waiters heal (an owner call after the lost timer is overdue) and everything drains.
  await sleep(WAKE_SLACK_MS + 50);
  for (const o of outlets) o.queued("w");
  busy.releaseAll();
  await Promise.all([...waiting, ...work]);
  expect(deliveryPressure()).toMatchObject({ running: 0, reservations: 0 });
});
