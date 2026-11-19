import { env, runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import { FRESH_SAMPLE_MS, type GatewayControllerObject } from "../controller";
import { controllerCell } from "../placement";
const bindings = env as Env & { GATEWAY_CONTROLLER: DurableObjectNamespace<GatewayControllerObject> };
function keys(cell: number, prefix: string, count: number) {
  const out: string[] = [];
  for (let n = 0; out.length < count; n++) { const id = `${prefix}-${n}`; if (controllerCell(id) === cell) out.push(id); }
  return out;
}

/** Runs `fn` with the controller's holder probes counted, and its storage writes' options recorded. */
async function observed<T>(controller: DurableObjectStub<GatewayControllerObject>, fn: () => Promise<T>) {
  const probes: string[] = [];
  const writes: Array<{ kind: string; unconfirmed: boolean }> = [];
  let restore!: () => void;
  await runInDurableObject(controller, async (object, state) => {
    const actual = object as any, originalEnv = actual.env;
    Object.defineProperty(actual, "env", { configurable: true, value: { ...originalEnv, GATEWAY: { getByName(name: string) {
      const rpc = bindings.GATEWAY.getByName(name) as any;
      return { capacity: (epoch: number) => { probes.push(name); return rpc.capacity(epoch); }, drain: (command: unknown) => rpc.drain(command) };
    } } } });
    const storage = state.storage as any, put = storage.put, setAlarm = storage.setAlarm, deleteAlarm = storage.deleteAlarm;
    storage.put = (key: string, value: unknown, options?: { allowUnconfirmed?: boolean }) => {
      if (key === "placement") writes.push({ kind: "put", unconfirmed: options?.allowUnconfirmed === true });
      return put.call(storage, key, value, options);
    };
    storage.setAlarm = (at: number, options?: { allowUnconfirmed?: boolean }) => { writes.push({ kind: "alarm", unconfirmed: options?.allowUnconfirmed === true }); return setAlarm.call(storage, at, options); };
    storage.deleteAlarm = (options?: { allowUnconfirmed?: boolean }) => { writes.push({ kind: "alarm", unconfirmed: options?.allowUnconfirmed === true }); return deleteAlarm.call(storage, options); };
    restore = () => { Object.defineProperty(actual, "env", { configurable: true, value: originalEnv }); storage.put = put; storage.setAlarm = setAlarm; storage.deleteAlarm = deleteAlarm; };
  });
  try { return { result: await fn(), probes, writes }; } finally { restore(); }
}

it("X17: a fresh admitting capacity report stands in for the probe; a stale one is probed live", async () => {
  const controller = bindings.GATEWAY_CONTROLLER.getByName("gateway-cell-11");
  const [first, second, third] = keys(11, "x17-fresh", 3);
  // First admission: no slot yet, so one is allocated (durable before the router uses it).
  const allocated = await observed(controller, () => controller.acquire(first!));
  expect(allocated.probes).toHaveLength(0);
  expect(allocated.writes.length).toBeGreaterThan(0);
  expect(allocated.writes.every(w => !w.unconfirmed)).toBe(true);
  const a = allocated.result;
  await controller.observe(a.gateway, { v: 1, epoch: a.epoch, at: Date.now(), sockets: 3,
    framesPerSecond: 0, bytesPerSecond: 0, deliveryMs: 0, queueAgeMs: 0, rpcMs: 0 });

  // Fresh, admitting report: no probe, and a lease-only write that does not wait for replication.
  const fresh = await observed(controller, () => controller.acquire(second!));
  expect(fresh.result.gateway).toBe(a.gateway);
  expect(fresh.probes).toHaveLength(0);
  expect(fresh.writes.some(w => w.kind === "put")).toBe(true);
  expect(fresh.writes.every(w => w.unconfirmed)).toBe(true);

  // The report ages out: the holder is probed live again.
  await runInDurableObject(controller, async (_object, state) => {
    const stored = (await state.storage.get<any>("placement"))!;
    for (const slot of stored.slots) if (slot.sample) slot.sample.at = Date.now() - FRESH_SAMPLE_MS - 1_000;
    await state.storage.put("placement", stored);
  });
  const stale = await observed(controller, () => controller.acquire(third!));
  expect(stale.result.gateway).toBe(a.gateway);
  expect(stale.probes).toEqual([a.gateway]);

  // Claims and releases are lease-only changes too.
  const claimed = await observed(controller, () => controller.claim(stale.result));
  expect(claimed.result).toBe(true);
  expect(claimed.writes.length).toBeGreaterThan(0);
  expect(claimed.writes.every(w => w.unconfirmed)).toBe(true);
  const released = await observed(controller, () => controller.release(fresh.result));
  expect(released.writes.every(w => w.unconfirmed)).toBe(true);
  await controller.release(a);
  expect((await controller.status()).pending).toBe(1); // the claimed lease stays until it expires
});

it("X17: a fresh report that refuses (pressured) is checked with a live probe, as before", async () => {
  const controller = bindings.GATEWAY_CONTROLLER.getByName("gateway-cell-12");
  const [first, second] = keys(12, "x17-refuse", 2);
  const a = await controller.acquire(first!);
  await controller.observe(a.gateway, { v: 1, epoch: a.epoch, at: Date.now(), sockets: 1,
    framesPerSecond: 0, bytesPerSecond: 0, deliveryMs: 150, queueAgeMs: 0, rpcMs: 0 });
  const next = await observed(controller, () => controller.acquire(second!));
  expect(next.probes).toEqual([a.gateway]);
  // The live sample of the idle holder is not pressured, so it still admits.
  expect(next.result.gateway).toBe(a.gateway);
  await controller.release(a); await controller.release(next.result);
});
