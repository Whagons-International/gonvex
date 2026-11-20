// 2026-11-20 dev incident (devcopy-stg1117-04, 14:30-14:50Z): the tenant object was reset ("exceeded its CPU time
// limit") with delivery RPCs in flight. The outlet's RPC slots and byte credits are isolate-global; the reset object's
// RPC continuations never run, so their slots were never returned, and every later delivery from that isolate waited
// for a slot forever (opens ran, payloads queued, no delivery RPC ever started) until a redeploy replaced the isolate.
// Real Durable Objects in one workerd isolate: `OutletSender` owns a GatewayOutlet, `OutletHolder` answers its RPCs.
import { env } from "cloudflare:test";
import { afterEach, expect, it } from "vitest";
import { MAX_RPCS } from "../../delivery/outlet";
import type { OutletHolder, OutletSender } from "./worker";

type Env = { OUTLET_SENDER: DurableObjectNamespace<OutletSender>; OUTLET_HOLDER: DurableObjectNamespace<OutletHolder> };
const e = env as unknown as Env;
const sender = (name: string) => e.OUTLET_SENDER.get(e.OUTLET_SENDER.idFromName(name));
const holder = () => e.OUTLET_HOLDER.get(e.OUTLET_HOLDER.idFromName("holder"));
/** The platform resets the object; the RPC that asked for it fails with the reset. */
const reset = async (name: string) => {
  const error = await sender(name).die().then(() => undefined, (failure: unknown) => failure);
  expect(error).toBeInstanceOf(Error);
};

afterEach(async () => { await holder().release(); await holder().hold(); });

it("a tenant reset with every isolate RPC slot in flight does not wedge its next instance or other tenants", async () => {
  const before = await sender("probe-a").pressure();
  const filled = await sender("tenant-04").fill(MAX_RPCS);
  expect(filled.running - before.running).toBe(MAX_RPCS);
  await reset("tenant-04");
  // The holder answers the reset object's RPCs; nobody is left to receive the answers.
  expect(await holder().release()).toBe(MAX_RPCS);
  // The platform re-creates the object in the same isolate and it delivers again (before the fix: "timeout", with
  // `running` stuck at MAX_RPCS, as on dev from 14:30:52Z until the 14:50Z redeploy).
  const back = await sender("tenant-04").sendOne("h0", 3_000);
  expect(back.outcome).toBe("delivered");
  // Every slot, credit and host entry the reset instance held was returned.
  expect(back.pressure).toMatchObject({ running: before.running, count: before.count, normal: before.normal, hosts: before.hosts });
  expect(back.pressure.retiredSlots - before.retiredSlots).toBe(MAX_RPCS);
  expect((await sender("tenant-11").sendOne("h1", 3_000)).outcome).toBe("delivered");
});

it("a reset object that never comes back holds the isolate's RPC slots only up to the stale bound", async () => {
  const probe = sender("probe-b");
  const previous = await probe.staleMs(2_000);
  try {
    const before = await probe.pressure();
    await sender("tenant-gone").fill(MAX_RPCS);
    await reset("tenant-gone");
    expect(await holder().release()).toBe(MAX_RPCS);
    // Another tenant of the isolate waits while the slots are fresh, then delivers once they are stale.
    const early = await sender("tenant-12").sendOne("h2", 100);
    expect(early.outcome).toBe("timeout");
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    const late = await sender("tenant-13").sendOne("h3", 3_000);
    expect(late.outcome).toBe("delivered");
    expect(late.pressure.staleSlots - before.staleSlots).toBe(MAX_RPCS);
    // The early send went out too, and the answers the reset object never received return no slot twice.
    await new Promise((resolve) => setTimeout(resolve, 50));
    const after = await probe.pressure();
    expect(after.running).toBe(before.running);
    expect(after.running).toBeGreaterThanOrEqual(0);
  } finally {
    await probe.staleMs(previous);
  }
});

it("a reset object that never comes back also gives back the credits of what it had queued (review #550-1)", async () => {
  const probe = sender("probe-d");
  const previous = await probe.staleMs(2_000);
  try {
    const before = await probe.pressure();
    await sender("tenant-gone2").fill(MAX_RPCS);
    const queued = await sender("tenant-gone2").enqueueMore("h0", 3);
    expect(queued.count - before.count).toBe(MAX_RPCS + 3);
    await reset("tenant-gone2");
    expect(await holder().release()).toBe(MAX_RPCS);
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    const late = await sender("tenant-15").sendOne("h5", 3_000);
    expect(late.outcome).toBe("delivered");
    // Before: { running: 0, count: 3, normal: >0 } for good: `fits()` and `backlogged()` would see a dead object's queue.
    expect(late.pressure).toMatchObject({ running: before.running, count: before.count, normal: before.normal, recovery: before.recovery });
  } finally {
    await probe.staleMs(previous);
  }
});
