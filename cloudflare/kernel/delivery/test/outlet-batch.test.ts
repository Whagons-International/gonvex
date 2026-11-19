// X14: the gateway outlet sends the payloads queued for one holder in one ordered `deliverReplicaBatch` RPC
// instead of one RPC per payload (a cold load's snapshot pages were a serial chain of ~30 ms delivery RPCs).

import { describe, expect, it } from "vitest";
import { BATCH_MAX_ITEMS, GatewayOutlet, batchUnsupported, type ReplicaBatchResult, type ReplicaHolderRpc } from "../outlet.ts";
import { REPLICA_DELIVERY_VERSION, type ReplicaDelivery, type ReplicaReceipt } from "../protocol.ts";

const payload = (n: number): ReplicaDelivery => ({
  v: REPLICA_DELIVERY_VERSION, instance: "i", scopeFence: 1, shared: [],
  sockets: { c1: { ops: [["f", `{"type":"replica.snapshotPage","n":${n}}`]], subscriptionEpoch: 0 } as never },
});
const receipt = (n: number): ReplicaReceipt => ({ v: REPLICA_DELIVERY_VERSION, sockets: n });
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const nth = (p: ReplicaDelivery) => Number(/"n":(\d+)/.exec((p.sockets.c1!.ops[0] as [string, string])[1])![1]);

/** A holder whose RPCs stay in flight until released, recording what each call carried. */
function holder(batch: boolean) {
  const calls: Array<{ kind: "single" | "batch"; items: number[] }> = [];
  const gates: Array<() => void> = [];
  const hold = () => new Promise<void>((resolve) => gates.push(resolve));
  const rpc: ReplicaHolderRpc = {
    async deliverReplica(_tenant, p) { calls.push({ kind: "single", items: [nth(p)] }); await hold(); return receipt(nth(p)); },
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
    expect(batchCalls).toBe(1); // remembered per holder: no second batch attempt
  });

  it("recognizes the platform's missing-method error only for the batch method", () => {
    expect(batchUnsupported(new TypeError('The RPC receiver does not implement the method "deliverReplicaBatch".'))).toBe(true);
    expect(batchUnsupported(new Error("delivery recovery scope changed"))).toBe(false);
    expect(batchUnsupported(new TypeError('The RPC receiver does not implement the method "deliverReplica".'))).toBe(false);
  });
});
