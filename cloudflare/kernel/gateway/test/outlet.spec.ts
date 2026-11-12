import { expect, it } from "vitest";
import { GatewayOutlet, DELIVERY_CAPACITY_CODE } from "../../delivery/outlet";
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
