// #550 re-review: the 04-hang reclaims must not depend on the reset object's outlet staying reachable. In workerd a
// reset object's outlet is referenced only by RPC promises of its dead I/O context; once V8 collects it, a reclaim that
// finds it through a WeakRef can no longer return its slots. Node with a real `gc()` (workerd exposes none), so this
// runs under `node --expose-gc --experimental-transform-types --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { GatewayOutlet, MAX_RPCS, deliveryPressure, setRpcSlotStaleMs } from "./outlet.ts";

const payload = (instance) => ({ v: 1, instance, shared: ["x".repeat(64)], sockets: {} });
const never = { deliverReplica: () => new Promise(() => {}) }; // a dead I/O context: the RPC never settles in JS
const quick = { async deliverReplica() { return { v: 1, sockets: 0 }; } };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const settle = async () => { for (let i = 0; i < 3; i++) { await sleep(10); globalThis.gc(); } };
const within = (promise, ms) => Promise.race([promise.then(() => "delivered"), sleep(ms).then(() => "timeout")]);

/** A reset instance: every isolate slot in flight plus one queued, then nothing references the outlet. */
async function deadOutlet(object) {
  const outlet = new GatewayOutlet("t", () => never, async () => [], object);
  for (let i = 0; i <= MAX_RPCS; i++) void outlet.send(i === MAX_RPCS ? "h0" : `h${i}`, payload(`p${i}`));
  await sleep(5);
}

test("the stale rule reclaims the slots of a reset object's outlet that GC could otherwise collect", async () => {
  const previous = setRpcSlotStaleMs(300);
  try {
    const before = deliveryPressure();
    await deadOutlet(undefined);
    await settle();
    await sleep(320);
    const other = new GatewayOutlet("other", () => quick, async () => []);
    assert.equal(await within(other.send("q", payload("q")), 2_000), "delivered");
    const after = deliveryPressure();
    assert.deepEqual({ running: after.running, count: after.count, normal: after.normal }, { running: before.running, count: before.count, normal: before.normal });
    assert.equal(after.staleSlots - before.staleSlots, MAX_RPCS);
  } finally { setRpcSlotStaleMs(previous); }
});

test("retirement reclaims a collected predecessor's slots when the same object comes back", async () => {
  const before = deliveryPressure();
  await deadOutlet("object-gc");
  await settle();
  const next = new GatewayOutlet("t", () => quick, async () => [], "object-gc");
  assert.equal(await within(next.send("g", payload("n")), 2_000), "delivered");
  const after = deliveryPressure();
  assert.equal(after.running, before.running);
  assert.equal(after.retiredSlots - before.retiredSlots, MAX_RPCS);
});
