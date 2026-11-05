import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient } from "./index.js";
import { LocalReplica, MemoryLocalReplicaStorage } from "./local-replica.js";
import { replicaRowsHashes, replicaAggregateDigest } from "./replica-integrity.js";

function deferred() {
  let resolve!: () => void, reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const clients: GonvexClient[] = [];
afterEach(() => {
  clients.splice(0).forEach(client => client.close());
  vi.clearAllTimers(); vi.useRealTimers(); vi.restoreAllMocks();
});

async function fixture(batch: boolean, count = 2) {
  vi.useFakeTimers();
  const storage = new MemoryLocalReplicaStorage();
  const seed = new LocalReplica(storage);
  await seed.activateScope("visible");
  const expected = new Map<string, { cursor: { epoch: string; revision: number }; digest: string }>();
  for (let i = 0; i < count; i++) {
    const key = i === 0 ? "empty" : i === 1 ? "populated" : `checkpoint-${i}`;
    const rows = i % 2 === 0 ? [] : [{ id: `row-${i}`, value: 7 }];
    const hashes = await replicaRowsHashes([...rows], "id");
    const cursor = { epoch: "db", revision: 17 };
    await seed.replaceWindow({ signature: key, kind: "replica", entity: "tasks", key: "id",
      rows: [...rows], hashes, cursor, completeness: "complete", source: "server" });
    expected.set(key, { cursor, digest: replicaAggregateDigest(hashes) });
  }
  const entered = deferred(), release = deferred();
  const load = storage.load.bind(storage);
  const loading = vi.spyOn(storage, "load").mockImplementation(async scope => {
    if (scope === "visible") { entered.resolve(); await release.promise; }
    return load(scope);
  });
  const client = new GonvexClient("ws://hydration.test/ws", {
    localReplica: { storage }, errorReporting: false,
  });
  clients.push(client);
  const state = client as any;
  await state.replicaReady;
  state.replicaScope = "visible";
  state.hasAuthoritativeReplicaScope = true;
  state.isWebSocketConnected = true;
  state.serverCapabilities = { replicaResume: 1, replicaBatch: batch ? 1 : undefined };
  state.send = vi.fn();
  state.setReplicaReady(state.replica.activateScope("visible"));
  await entered.promise;
  const subscriptions = [...expected.keys()].map(key => {
    const subscription = { key, id: `r-${key}`, path: `tasks.${key}`, args: {},
      listeners: new Set([() => {}]), scope: "visible", opening: false,
      verificationGeneration: Symbol() };
    state.replicaSubscriptions.set(key, subscription);
    return subscription;
  });
  const waiting = () => Promise.all([...state.replicaOpenHydrations.values()].map((v: any) => v.task));
  const flush = () => { vi.runOnlyPendingTimers(); };
  const opens = () => state.send.mock.calls.flatMap(([message]: any[]) =>
    message.type === "replica.open" ? [message] : message.type === "replica.openMany" ? message.opens : []);
  return { client, state, subscriptions, expected, loading, release, waiting, flush, opens };
}

it.each([false, true])("waits once for durable empty/nonempty checkpoints before constructing opens, batch=%s", async batch => {
  const f = await fixture(batch, 41);
  const construct = vi.spyOn(f.state, "replicaOpenRequest");
  for (const subscription of f.subscriptions) {
    f.state.sendReplicaOpen(subscription);
    f.state.sendReplicaOpen(subscription);
  }
  f.flush();
  expect(construct).not.toHaveBeenCalled();
  expect(f.opens()).toEqual([]);
  expect(f.state.replicaOpenHydrations.size).toBe(41);
  const waiting = f.waiting();
  f.release.resolve(); await waiting; f.flush();
  expect(f.loading.mock.calls.filter(([scope]) => scope === "visible")).toHaveLength(1);
  expect(f.opens()).toHaveLength(41);
  for (const open of f.opens()) {
    const key = open.path.slice("tasks.".length);
    expect(open).toMatchObject(f.expected.get(key)!);
    expect(open.fullIntegrity).toBeUndefined();
  }
  expect(f.state.replicaOpenHydrations.size).toBe(0);
  expect(f.state.send).toHaveBeenCalledTimes(batch ? 1 : 41);
});

it.each(["auth", "socket", "scope", "disconnect", "unsubscribe", "replace", "cancel", "close"] as const)
("retires a hydration waiter on %s before any request construction", async change => {
  const f = await fixture(true);
  const subscription = f.subscriptions[0]!;
  const construct = vi.spyOn(f.state, "replicaOpenRequest");
  f.state.sendReplicaOpen(subscription);
  const waiting = f.waiting();
  switch (change) {
    case "auth": f.state.activeAuthFrameId = "new-auth"; break;
    case "socket": f.state.socketGeneration = Symbol(); break;
    case "scope": f.state.replicaScope = "other"; f.state.sessionScopeGeneration = Symbol(); break;
    case "disconnect": f.state.isWebSocketConnected = false; break;
    case "unsubscribe": subscription.listeners.clear(); break;
    case "replace": f.state.replicaSubscriptions.set(subscription.key, { ...subscription }); break;
    case "cancel": subscription.verificationGeneration = Symbol(); break;
    case "close": f.client.close(); break;
  }
  f.release.resolve(); await waiting; f.flush();
  expect(construct).not.toHaveBeenCalled();
  expect(f.opens()).toEqual([]);
  expect(subscription.opening).toBe(false);
});

it("keeps a retained subscription eligible after its last listener defers and later returns", async () => {
  const f = await fixture(true);
  const subscription = f.subscriptions[0]!;
  f.state.sendReplicaOpen(subscription);
  const waiting = f.waiting();
  subscription.listeners.clear();
  f.release.resolve(); await waiting; f.flush();
  expect(f.opens()).toEqual([]);
  expect(subscription.opening).toBe(false);
  subscription.listeners.add(() => {});
  f.state.sendReplicaOpen(subscription); f.flush();
  expect(f.opens()).toHaveLength(1);
  expect(f.opens()[0]).toMatchObject(f.expected.get("empty")!);
});

it("releases an auth-deferred batch for preserved subscriptions exactly once", async () => {
  const f = await fixture(true);
  const subscription = f.subscriptions[0]!;
  f.state.sendReplicaOpen(subscription);
  const waiting = f.waiting();
  f.release.resolve(); await waiting;
  f.state.authInFlight = true;
  f.flush();
  expect(f.opens()).toEqual([]);
  expect(subscription).toMatchObject({ opening: false, socketGeneration: undefined });
  f.state.authInFlight = false;
  f.state.activeAuthFrameId = "refreshed";
  f.state.sendReplicaOpen(subscription); f.state.sendReplicaOpen(subscription); f.flush();
  expect(f.opens()).toHaveLength(1);
  expect(f.opens()[0]).toMatchObject(f.expected.get("empty")!);
});

it("drops an old activation without removing its replacement waiter", async () => {
  const f = await fixture(true);
  const subscription = f.subscriptions[0]!;
  f.state.sendReplicaOpen(subscription);
  const old = f.waiting();
  f.state.socketGeneration = Symbol();
  f.state.sendReplicaOpen(subscription);
  const replacement = f.waiting();
  f.release.resolve(); await old; await replacement; f.flush();
  expect(f.opens()).toHaveLength(1);
  expect(f.state.replicaOpenHydrations.size).toBe(0);
});

it("never falls back to a cold request when durable hydration rejects", async () => {
  const f = await fixture(false);
  f.state.sendReplicaOpen(f.subscriptions[0]);
  const waiting = f.waiting();
  f.release.reject(Error("storage unavailable"));
  await waiting;
  expect(f.opens()).toEqual([]);
  expect(f.state.replicaOpenHydrations.size).toBe(0);
});

it.each(["auth", "socket", "scope", "cancel", "unsubscribe"] as const)
("rechecks %s ownership after hydration but before the deferred batch flush", async change => {
  const f = await fixture(true);
  const subscription = f.subscriptions[0]!;
  const construct = vi.spyOn(f.state, "replicaOpenRequest");
  f.state.sendReplicaOpen(subscription);
  const waiting = f.waiting(); f.release.resolve(); await waiting;
  expect(construct).not.toHaveBeenCalled();
  switch (change) {
    case "auth": f.state.activeAuthFrameId = "replacement"; break;
    case "socket": f.state.socketGeneration = Symbol(); break;
    case "scope": f.state.replicaScope = "replacement"; break;
    case "cancel": subscription.verificationGeneration = Symbol(); break;
    case "unsubscribe": subscription.listeners.clear(); break;
  }
  f.flush();
  expect(construct).not.toHaveBeenCalled();
  expect(f.opens()).toEqual([]);
  expect(subscription.opening).toBe(false);
});

it("admits the outbox journal without waiting for collection hydration", async () => {
  const f = await fixture(true);
  // Empty subscriptions avoid unrelated handler rotation in this bootstrap
  // assertion. activateReplicaDirective must finish journal admission even
  // while a prior storage activation is blocked; tenant row opens still wait.
  f.state.replicaSubscriptions.clear();
  await f.state.activateReplicaDirective({ protocolVersion: 1, scope: "tenant",
    visibilityScope: "next", epoch: "e" });
  expect(f.opens()).toEqual([]);
  expect(f.state.hydratedReplicaReady).not.toBe(f.state.replicaReady);
  f.release.resolve();
  await f.state.replicaReady;
});

it("does not resurrect a batch queued by a formerly hydrated activation after scope failure", async () => {
  const f = await fixture(true);
  f.state.sendReplicaOpen(f.subscriptions[0]);
  const waiting = f.waiting(); f.release.resolve(); await waiting;
  const failed = deferred();
  f.state.setReplicaReady(failed.promise);
  const observed = f.state.replicaReady.catch(() => undefined);
  failed.reject(Error("replacement scope unavailable")); await observed;
  f.flush();
  expect(f.opens()).toEqual([]);
  expect(f.state.pendingReplicaOpens.size).toBe(0);
  expect(f.state.replicaOpenHydrations.size).toBe(0);
  expect(f.subscriptions[0]!.opening).toBe(false);
});

it("does not use a superseded hydration completion as the latest readiness certificate", async () => {
  const f = await fixture(false);
  f.state.sendReplicaOpen(f.subscriptions[0]);
  const old = f.waiting();
  f.state.setReplicaReady(f.state.replica.activateScope("next"));
  f.release.resolve(); await old; await f.state.replicaReady;
  expect(f.opens()).toEqual([]);
  expect(f.state.replicaOpenHydrations.size).toBe(0);
});
