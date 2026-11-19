// X17: `sendFirst` puts a payload ahead of the ordinary payloads waiting for a holder (a live query's initial reply
// ahead of a first load's snapshot pages), never ahead of the RPC in flight, and keeps earlier priority payloads first.
import { describe, expect, it } from "vitest";
import { FIRST_BYTES, GatewayOutlet, type ReplicaBatchResult, type ReplicaHolderRpc } from "../outlet.ts";
import { REPLICA_DELIVERY_VERSION, type ReplicaDelivery, type ReplicaReceipt } from "../protocol.ts";

const payload = (n: number): ReplicaDelivery => ({
  v: REPLICA_DELIVERY_VERSION, instance: "i", scopeFence: 1, shared: [],
  sockets: { c1: { ops: [["f", `{"type":"replica.snapshotPage","n":${n}}`]], subscriptionEpoch: 0 } as never },
});
const nth = (p: ReplicaDelivery) => Number(/"n":(\d+)/.exec((p.sockets.c1!.ops[0] as [string, string])[1])![1]);
const receipt = (n: number): ReplicaReceipt => ({ v: REPLICA_DELIVERY_VERSION, sockets: n });
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

function holder() {
  const calls: number[][] = [];
  const gates: Array<() => void> = [];
  const hold = () => new Promise<void>((resolve) => gates.push(resolve));
  const rpc: ReplicaHolderRpc = {
    async deliverReplica(_t, p) { calls.push([nth(p)]); await hold(); return receipt(nth(p)); },
    async deliverReplicaBatch(_t: string, ps: ReplicaDelivery[]): Promise<ReplicaBatchResult[]> { calls.push(ps.map(nth)); await hold(); return ps.map((p) => ({ receipt: receipt(nth(p)) })); },
  };
  const release = async () => { while (gates.length) { gates.shift()!(); await tick(); } };
  return { rpc, calls, release };
}

describe("X17 outlet priority lane", () => {
  it("sends a sendFirst payload right after the RPC in flight, ahead of queued payloads, in priority order", async () => {
    const h = holder();
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const inFlight = outlet.send("g", payload(1));
    await tick();
    const queued = [2, 3, 4].map((n) => outlet.send("g", payload(n)));
    const firsts = [100, 101].map((n) => outlet.sendFirst("g", payload(n)));
    expect(outlet.queued("g")).toBe(6); // the RPC in flight counts too
    await h.release(); await h.release();
    await Promise.all([inFlight, ...queued, ...firsts]);
    expect(h.calls).toEqual([[1], [100, 101, 2, 3, 4]]);
    expect((await firsts[0])?.sockets).toBe(100);
  });

  it("an idle holder sends it at once, like send", async () => {
    const h = holder();
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const sent = outlet.sendFirst("g", payload(7));
    await tick(); await h.release();
    expect((await sent)?.sockets).toBe(7);
    expect(h.calls).toEqual([[7]]);
  });

  it("admits a small sendFirst payload when the holder's byte budget is full, as send would not", async () => {
    const h = holder();
    const outlet = new GatewayOutlet("t", () => h.rpc, async () => ["g"]);
    const big = (n: number): ReplicaDelivery => ({ ...payload(n), sockets: { c1: { ops: [["f", `{"type":"replica.snapshotPage","n":${n},"pad":"${"x".repeat(1_100_000)}"}`]], subscriptionEpoch: 0 } as never } });
    const sent: Array<Promise<unknown>> = [];
    for (let n = 1; outlet.admits("g", big(n)); n++) sent.push(outlet.send("g", big(n)));
    expect(outlet.admits("g", payload(99))).toBe(false);
    expect(outlet.admitsFirst("g", payload(99))).toBe(true);
    expect(JSON.stringify(payload(99)).length).toBeLessThan(FIRST_BYTES);
    const first = outlet.sendFirst("g", payload(99));
    await tick();
    for (let i = 0; i < 20 && h.calls.flat().length <= sent.length; i++) await h.release();
    expect((await first)?.sockets).toBe(99);
    await Promise.all(sent);
    expect(h.calls.flat().indexOf(99)).toBeLessThanOrEqual(1);
  });
});
