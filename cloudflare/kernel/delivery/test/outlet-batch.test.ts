// X14: the gateway outlet sends the payloads queued for one holder in one ordered `deliverReplicaBatch` RPC
// instead of one RPC per payload (a cold load's snapshot pages were a serial chain of ~30 ms delivery RPCs).

import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { BATCH_MAX_ITEMS, GatewayOutlet, SINGLE_ONLY_TTL_MS, batchUnsupported, type ReplicaBatchResult, type ReplicaHolderRpc } from "../outlet.ts";
import { REPLICA_DELIVERY_VERSION, type ReplicaDelivery, type ReplicaReceipt } from "../protocol.ts";

const payload = (n: number): ReplicaDelivery => ({
  v: REPLICA_DELIVERY_VERSION, instance: "i", scopeFence: 1, shared: [],
  sockets: { c1: { ops: [["f", `{"type":"replica.snapshotPage","n":${n}}`]], subscriptionEpoch: 0 } as never },
});
const receipt = (n: number): ReplicaReceipt => ({ v: REPLICA_DELIVERY_VERSION, sockets: n });
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const nth = (p: ReplicaDelivery) => p.recovery ? 0 : Number(/"n":(\d+)/.exec((p.sockets.c1!.ops[0] as [string, string])[1])![1]);
const barrier = (): ReplicaDelivery => ({ v: REPLICA_DELIVERY_VERSION, instance: "i", scopeFence: 1, shared: [], sockets: {}, recovery: { reason: "change-feed-lagged" } });
const big = (n: number): ReplicaDelivery => ({ ...payload(n), sockets: { c1: { ops: [["f", `{"type":"replica.snapshotPage","n":${n},"pad":"${"x".repeat(1_100_000)}"}`]], subscriptionEpoch: 0 } as never } });

/** A holder whose RPCs stay in flight until released, recording what each call carried. */
function holder(batch: boolean) {
  const calls: Array<{ kind: "single" | "batch"; items: number[] }> = [];
  const gates: Array<() => void> = [];
  const hold = () => new Promise<void>((resolve) => gates.push(resolve));
  const rpc: ReplicaHolderRpc = {
    async deliverReplica(_tenant, p) { calls.push({ kind: "single", items: [nth(p)] }); await hold(); return p.recovery ? { ...receipt(0), recoveryAck: 1 } : receipt(nth(p)); },
    ...(batch ? {
      async deliverReplicaBatch(_tenant: string, ps: ReplicaDelivery[]): Promise<ReplicaBatchResult[]> {
        calls.push({ kind: "batch", items: ps.map(nth) }); await hold();
        return ps.map((p) => nth(p) === 13 ? { error: "boom 13" } : { receipt: receipt(nth(p)) });
      },
    } : {}),
  };
  const release = async () => { while (gates.length) { gates.shift()!(); await tick(); } };
  return { rpc, calls, release };
}

describe("X14 gateway outlet batching", () => {
  it("coalesces the payloads queued behind the in-flight RPC into one ordered batch", async () => {
    const h = holder(true);
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const sent = Array.from({ length: 12 }, (_, i) => outlet.send("g", payload(i + 1)));
    await tick();
    // The first payload goes at once; the 11 queued behind it become one batch, not 11 RPCs.
    await h.release(); await h.release();
    const results = await Promise.all(sent);
    expect(h.calls).toEqual([{ kind: "batch", items: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }]);
    expect(results.map((r) => r?.sockets)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
  });

  it("keeps order across batches and rejects only the item the holder failed", async () => {
    const h = holder(true);
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const first = outlet.send("g", payload(10));
    await tick();
    const rest = [11, 12, 13, 14].map((n) => outlet.send("g", payload(n)).then((r) => r?.sockets, (e: Error) => e.message));
    await h.release();
    expect(await first).toEqual(receipt(10));
    expect(await Promise.all(rest)).toEqual([11, 12, "boom 13", 14]);
    expect(h.calls.map((c) => c.items).flat()).toEqual([10, 11, 12, 13, 14]);
    expect(h.calls.every((c) => c.items.length <= BATCH_MAX_ITEMS)).toBe(true);
  });

  it("falls back to single RPCs, in order, when the holder predates the batch method", async () => {
    const h = holder(false);
    let batchCalls = 0;
    const skewed = { ...h.rpc, async deliverReplicaBatch() { batchCalls++; throw new TypeError('The RPC receiver does not implement the method "deliverReplicaBatch".'); } };
    const outlet = new GatewayOutlet("t", () => skewed, async () => ["g"]);
    const sent = [1, 2, 3, 4].map((n) => outlet.send("g", payload(n)));
    for (let i = 0; i < 6; i++) { await tick(); await h.release(); }
    expect((await Promise.all(sent)).map((r) => r?.sockets)).toEqual([1, 2, 3, 4]);
    expect(h.calls.map((c) => c.items[0])).toEqual([1, 2, 3, 4]);
    expect(h.calls.every((c) => c.kind === "single")).toBe(true);
    expect(batchCalls).toBe(1); // remembered per holder: no second batch attempt within the TTL
  });

  it("tries a batch again once the single-only mark expires (the holder was updated meanwhile)", async () => {
    const h = holder(true);
    let skewed = true, refused = 0;
    const rpc = { ...h.rpc, async deliverReplicaBatch(t: string, ps: ReplicaDelivery[]) {
      if (skewed) { refused++; throw new TypeError('The RPC receiver does not implement the method "deliverReplicaBatch".'); }
      return h.rpc.deliverReplicaBatch!(t, ps);
    } };
    const outlet = new GatewayOutlet("t", () => rpc, async () => ["g"]);
    const realNow = Date.now;
    try {
      const sent = [1, 2, 3].map((n) => outlet.send("g", payload(n)));
      for (let i = 0; i < 6; i++) { await tick(); await h.release(); }
      await Promise.all(sent);
      expect(refused).toBe(1);
      skewed = false;
      const later = realNow() + SINGLE_ONLY_TTL_MS + 1;
      Date.now = () => later;
      const again = [4, 5, 6].map((n) => outlet.send("g", payload(n)));
      for (let i = 0; i < 4; i++) { await tick(); await h.release(); }
      expect((await Promise.all(again)).map((r) => r?.sockets)).toEqual([4, 5, 6]);
      expect(h.calls.at(-1)).toEqual({ kind: "batch", items: [4, 5, 6] });
    } finally { Date.now = realNow; }
  });

  it("settles as discarded, never resends, items a recovery barrier superseded while a refused batch was in flight", async () => {
    const h = holder(false);
    let gate!: () => void;
    const rpc = { ...h.rpc, async deliverReplicaBatch() {
      await new Promise<void>((resolve) => { gate = resolve; });
      throw new TypeError('The RPC receiver does not implement the method "deliverReplicaBatch".');
    } };
    const outlet = new GatewayOutlet("t", () => rpc, async () => ["g"]);
    const sent = [1, 2, 3].map((n) => outlet.send("g", payload(n)));
    await tick();
    const reset = outlet.reset("g", barrier());
    gate();
    for (let i = 0; i < 6; i++) { await tick(); await h.release(); }
    expect(await Promise.all(sent)).toEqual([undefined, undefined, undefined]);
    expect(await reset).toMatchObject({ recoveryAck: 1 });
    expect(h.calls).toEqual([{ kind: "single", items: [0] }]); // only the barrier, alone
  });

  it("never batches a recovery barrier; ordinary work batches again after it", async () => {
    const h = holder(true);
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const first = outlet.send("g", payload(1));
    await tick();
    const reset = outlet.reset("g", barrier());
    await h.release(); await h.release();
    await first; await reset;
    const after = [2, 3, 4].map((n) => outlet.send("g", payload(n)));
    await tick(); await h.release();
    await Promise.all(after);
    expect(h.calls).toEqual([{ kind: "single", items: [1] }, { kind: "single", items: [0] }, { kind: "batch", items: [2, 3, 4] }]);
  });

  it("caps a batch at BATCH_MAX_ITEMS and sends a payload over 8 MiB alone", async () => {
    const h = holder(true);
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const sent = Array.from({ length: BATCH_MAX_ITEMS + 6 }, (_, i) => outlet.send("g", payload(i + 100)));
    for (let i = 0; i < 4; i++) { await tick(); await h.release(); }
    await Promise.all(sent);
    expect(h.calls.map((c) => [c.kind, c.items.length])).toEqual([["batch", BATCH_MAX_ITEMS], ["batch", 6]]);
    expect(outlet.queued("g")).toBe(0);
    const large = outlet.send("g", big(99));
    await tick(); await h.release();
    expect((await large)?.sockets).toBe(99);
    expect(h.calls.at(-1)).toEqual({ kind: "single", items: [99] });
  });

  it("counts every payload of a batch in flight in queued()", async () => {
    const h = holder(true);
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const sent = [1, 2, 3, 4].map((n) => outlet.send("g", payload(n)));
    await tick();
    expect(outlet.queued("g")).toBe(4);
    await h.release(); await Promise.all(sent);
    expect(outlet.queued("g")).toBe(0);
  });

  it("the real gateway applies a batch item by item: one failing item is answered with its error, the rest apply", async () => {
    const gateway = (env as unknown as { GATEWAY: DurableObjectNamespace }).GATEWAY.getByName(`x14-batch-${crypto.randomUUID()}`) as unknown as ReplicaHolderRpc;
    const empty = (): ReplicaDelivery => ({ v: REPLICA_DELIVERY_VERSION, instance: "i", scopeFence: 1, shared: [], sockets: {} });
    // Not initial-failure-only (it carries a socket): the marker-only receiver refuses it.
    const bad = { ...empty(), sockets: { c1: { ops: [], subscriptionEpoch: 0 } }, initialFailure: { v: 1, reason: "delivery-recovery-failed", requests: {} } } as unknown as ReplicaDelivery;
    const results = await gateway.deliverReplicaBatch!("x14-tenant", [empty(), bad, empty()]);
    expect(results).toHaveLength(3);
    expect(results[0]).toMatchObject({ receipt: { v: REPLICA_DELIVERY_VERSION, sockets: 0 } });
    expect(results[1]).toEqual({ error: "invalid initial-failure-only delivery", name: "Error" });
    expect(results[2]).toMatchObject({ receipt: { v: REPLICA_DELIVERY_VERSION, sockets: 0 } });
  });

  it("recognizes the platform's missing-method error only for the batch method", () => {
    expect(batchUnsupported(new TypeError('The RPC receiver does not implement the method "deliverReplicaBatch".'))).toBe(true);
    expect(batchUnsupported(new Error("delivery recovery scope changed"))).toBe(false);
    expect(batchUnsupported(new TypeError('The RPC receiver does not implement the method "deliverReplica".'))).toBe(false);
    expect(batchUnsupported(new Error("deliverReplicaBatch is not a function"))).toBe(false);
  });
});
