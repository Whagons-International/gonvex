// Outlet contention benchmark (#550 round 4 shape): `tenants` outlets cold-load at once, 45 pages each over `holders`
// holders, holder RPCs take 25 ms, and each tenant enqueues its pages as the engine posts them (waiting for `admits`). Makespan = when the last tenant's last page settled. One scenario per run (BENCH binding, e.g. "16x3").
import { env } from "cloudflare:test";
import { expect, it } from "vitest";
import { GatewayOutlet, deliveryPressure, type ReplicaHolderRpc } from "../../delivery/outlet";
import type { ReplicaDelivery } from "../../delivery/protocol";

const PAGES = 45, RPC_MS = 25;
const [tenants, holders] = String((env as { BENCH?: string }).BENCH ?? "4x3").split("x").map(Number) as [number, number];
const payload = (instance: string): ReplicaDelivery => ({ v: 1, instance, shared: ["x".repeat(256)], sockets: {} });
const holder: ReplicaHolderRpc = { async deliverReplica() { await new Promise((r) => setTimeout(r, RPC_MS)); return { v: 1, sockets: 0 }; } };

it(`contention ${tenants} tenants x ${holders} holders`, async () => {
  const run = Date.now().toString(36);
  const start = performance.now();
  const finish = await Promise.all(Array.from({ length: tenants }, async (_, t) => {
    const outlet = new GatewayOutlet(`bench-${run}-${t}`, () => holder, async () => []);
    const sent: Array<Promise<unknown>> = [];
    for (let page = 0; page < PAGES; page++) {
      const host = `h${page % holders}`, p = payload(`t${t}p${page}`);
      while (!outlet.admits(host, p)) await new Promise((r) => setTimeout(r, 1)); // the engine's `postWhenRoom`
      sent.push(outlet.send(host, p));
    }
    await Promise.all(sent);
    return performance.now() - start;
  }));
  const makespan = Math.round(Math.max(...finish));
  console.log(`BENCH ${JSON.stringify({ tenants, holders, makespan, median: Math.round(finish.sort((a, b) => a - b)[finish.length >> 1]!), running: deliveryPressure().running })}`);
  expect(deliveryPressure().running).toBe(0);
});
