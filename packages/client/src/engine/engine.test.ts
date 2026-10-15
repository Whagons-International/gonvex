import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { reducer, schema } from "@gonvex/module-sdk";
import { createPortableReducer } from "@gonvex/local-runtime/portable-client";
import { GonvexClient, GonvexClientError, IndexedDBLocalReplicaStorage, type FunctionReference } from "../index.js";
import { EngineRpc, EngineRpcTimeout, sendEngineMessage, type EngineEndpoint } from "./rpc.js";
import { diffEngineValue, applyEnginePatches } from "./patch.js";
import { EngineReceipts } from "./receipts.js";
import { serveEngineHost, type EngineInit } from "./host.js";
import { EngineClient, createEngineClient } from "./client.js";

class Endpoint implements EngineEndpoint {
  peer!: Endpoint;
  readonly listeners = new Map<string, Set<(event: any) => void>>();
  sent: any[] = [];
  postMessage(data: unknown) { this.sent.push(data); queueMicrotask(() => this.peer.emit("message", { data: structuredClone(data) })); }
  addEventListener(type: string, listener: (event: any) => void) { let set = this.listeners.get(type); if (!set) { set = new Set(); this.listeners.set(type, set); } set.add(listener); }
  removeEventListener(type: string, listener: (event: any) => void) { this.listeners.get(type)?.delete(listener); }
  emit(type: string, event: unknown) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
}
function pair() { const ui = new Endpoint(), worker = new Endpoint(); ui.peer = worker; worker.peer = ui; return { ui, worker }; }
class Socket {
  static OPEN = 1; static CONNECTING = 0; static CLOSED = 3; static CLOSING = 2;
  static all: Socket[] = [];
  readyState = 0; sent: any[] = []; listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.all.push(this); }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners.set(type, [...this.listeners.get(type) ?? [], listener]); }
  emit(type: string, event: any = {}) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
  open() { this.readyState = 1; this.emit("open"); }
  send(data: string) { this.sent.push(JSON.parse(data)); }
  receive(data: unknown) { this.emit("message", { data: JSON.stringify(data) }); }
  close() { this.readyState = 3; this.emit("close"); }
}
const cleanups: Array<() => void | Promise<void>> = [];
beforeEach(() => {
  const indexedDB = new IDBFactory(); vi.stubGlobal("indexedDB", indexedDB); vi.stubGlobal("IDBKeyRange", IDBKeyRange);
  Dexie.dependencies.indexedDB = indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
  vi.stubGlobal("navigator", { locks: { request: (_name: string, options: any, callback?: any) => Promise.resolve((callback ?? options)({ name: _name })) } });
  vi.stubGlobal("WebSocket", Socket); Socket.all = [];
});
afterEach(async () => { for (const close of cleanups.splice(0)) await close(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it("correlates out-of-order replies, rejects old generations, and reuses request identity after timeout", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "generation", "scope"); cleanups.push(() => rpc.close());
  const first = rpc.request("reducer", [{ engineCommand: { id: "stable", now: 100, entropy: "fixed" } }], "first");
  const second = rpc.request("query", [], "second");
  const a = rpc.call(first, 5); const b = rpc.call(second);
  worker.postMessage({ generation: "old", scope: "scope", id: "first", result: "wrong" });
  worker.postMessage({ generation: "generation", scope: "scope", id: "second", result: 2 });
  expect(await b).toBe(2); await expect(a).rejects.toBeInstanceOf(EngineRpcTimeout);
  const retried = rpc.call(first); worker.postMessage({ generation: "generation", scope: "scope", id: "first", result: 1 });
  expect(await retried).toBe(1); expect(ui.sent.flatMap(frame => frame.admissions ?? [frame]).filter(frame => frame.id === "first")).toEqual([first, first]);
});

it("bounds chunk delivery and reassembles a large reply atomically", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const result = "😀".repeat(80_000); const pending = rpc.call(rpc.request("query", [], "large"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "large", result });
  expect(await pending).toBe(result);
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
  expect(ui.sent.filter(frame => frame.deliveryAck).length).toBe(worker.sent.length);
});

it.each(["\u0000", "\\\"", "\u0800"])("bounds serialized packets for escaped and multibyte strings, %j", async character => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const shared = { text: character.repeat(16_000), missing: undefined };
  const result = { first: shared, second: shared, rows: [shared, undefined] };
  const pending = rpc.call<typeof result>(rpc.request("query", [], "escaped"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "escaped", result });
  const actual = await pending;
  expect(actual).toEqual(result); expect(actual.first).toBe(actual.second); expect(actual.rows[0]).toBe(actual.first);
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
  expect(ui.sent.filter(frame => frame.deliveryAck).length).toBe(worker.sent.length);
});

it("rejects outstanding RPCs on a worker crash", async () => {
  const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("reducer", [])); ui.emit("error", {});
  await expect(pending).rejects.toThrow("retry the same command");
});

const url = "ws://engine.test/ws";
const auth = { project: "p", tenant: "t", identity: { sub: "a", iss: "issuer" } };
const owner = ["identity", url, "p", "t", "issuer", "a"].join("\u0000");
const ref: FunctionReference = { kind: "reducer", path: "insert", localExecution: 1, offline: { mode: "allowed" } };
const insert = reducer({ args: schema.object({}), result: schema.number(), run: async ctx => { await ctx.db.insert("tasks", { value: 1 }); return 7; } });
const binding = { mode: "portable" as const, artifactHash: "artifact", tables: ["tasks"], create: () => createPortableReducer({ artifactHash: "artifact", reducers: { insert }, schema: { tasks: { key: "id", columns: { id: { type: "text" as const, nullable: false }, value: { type: "bigint" as const, nullable: false } } } } }) };
async function setup() {
  const name = crypto.randomUUID(); const storage = new IndexedDBLocalReplicaStorage(name);
  await storage.saveSession(owner, { directive: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, artifactHash: "artifact",
    identity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } });
  storage.close();
  const init: EngineInit = { protocolVersion: 1, url, options: auth, storage: { replicaName: name, outboxName: `${name}-outbox`, contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 }, artifactHash: "artifact", fetchToken: false };
  const create = async () => {
    const endpoints = pair(); const stop = serveEngineHost(endpoints.worker, binding);
    const rpc = new EngineRpc(endpoints.ui, crypto.randomUUID(), "private");
    const state = await rpc.call<{ capabilities?: { admissionBatch?: number } }>(rpc.request("init", [init]));
    rpc.setAdmissionBatching(state.capabilities?.admissionBatch === 1);
    const close = async () => { await rpc.call(rpc.request("close", [])); rpc.close(); stop(); };
    return { rpc, endpoints, close };
  };
  return { init, create };
}

it("recovers an admitted local reducer from the canonical journal with its original outcome and entropy", async () => {
  const { create, init } = await setup(); let host = await create();
  const command = { id: "original", now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "aa".repeat(32) };
  const request = host.rpc.request("reducer", [ref, {}, { engineCommand: command }]);
  expect(await host.rpc.call(request)).toBe(7);
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  const entries = await host.rpc.call<any[]>(host.rpc.request("listIntents", []));
  expect(entries).toHaveLength(1); expect(entries[0].id).toBe("original");
  const database = new Dexie(init.storage.outboxName); await database.open();
  const [entry] = await database.table("entries").toArray(); database.close();
  expect(entry.localExecution).toMatchObject({ commandId: "original", now: command.now, intentEntropy: "aa".repeat(32) });
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  expect(await receipts.get(owner, "original")).toBeUndefined();
  expect(entry.engineResult).toBe(7);
  expect(entry.engineOutcome).toBeUndefined();
  await expect(host.rpc.call(host.rpc.request("reducer", [ref, { different: true }, { engineCommand: command }]))).rejects.toThrow("different arguments");
});

it("proxies auth fetch over the private endpoint before sending the private token", async () => {
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  const received: any[] = []; host.rpc.onEvent.add((method, args) => {
    received.push([method, args]);
    if (method === "fetchToken") void host.rpc.call(host.rpc.request("fetchTokenResult", [args[0], "private-token"]));
  });
  await host.rpc.call(host.rpc.request("setAuth", [{}, true, url]));
  await host.rpc.call(host.rpc.request("connect", [])); const socket = Socket.all[0]; socket.open();
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth" && frame.token === "private-token")).toBe(true));
  expect(received.some(([name]) => name === "fetchToken")).toBe(true);
});

it("falls back without coordination and keeps entity absence distinct from a pending cold read", async () => {
  const { init } = await setup();
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact" }, auth, storage: init.storage, fallbackBinding: binding });
  cleanups.push(() => client.close()); expect(client.engineMode).toBe("legacy");
  const watch = client.watchEntities("tasks", ["missing"]); expect(watch.getSnapshot()).toEqual({ status: "pending" });
  const stop = watch.onUpdate(() => undefined); cleanups.push(stop);
  await vi.waitFor(() => expect(watch.getSnapshot()).toEqual({ status: "ready", rows: [undefined] }));
});

it("the default client uses the original auth handshake and creates no worker", async () => {
  const worker = vi.fn(); vi.stubGlobal("Worker", worker);
  const client = new GonvexClient(url, auth); cleanups.push(() => client.close()); client.connect(); Socket.all[0].open();
  await vi.waitFor(() => expect(Socket.all[0].sent.some(frame => frame.type === "auth")).toBe(true));
  expect(worker).not.toHaveBeenCalled(); expect(Socket.all[0].sent.find(frame => frame.type === "auth").capabilities).not.toHaveProperty("engine");
});

it("preserves undefined cold rows in chunked results", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("readEntities", [], "rows"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "rows", result: [{ payload: "x".repeat(40_000) }, undefined] });
  expect(await pending).toEqual([{ payload: "x".repeat(40_000) }, undefined]);
});

it("patch batches preserve unchanged rows/nested fields and do not alter the prior snapshot", () => {
  const nested = Object.freeze({ label: "same" });
  const unchanged = Object.freeze({ id: "2", count: 0 });
  const previous = Object.freeze({ rows: Object.freeze([Object.freeze({ id: "1", count: 0, nested }), unchanged]) });
  const next = { rows: [{ id: "1", count: 1, nested }, unchanged] };
  const patches = diffEngineValue(previous, next)!;
  expect(patches).toEqual([{ path: ["rows", "0", "count"], value: 1 }]);
  const result = applyEnginePatches(previous, patches) as typeof next;
  expect(result.rows[0].nested).toBe(nested); expect(result.rows[1]).toBe(unchanged);
  expect(previous.rows[0].count).toBe(0); expect(Object.isFrozen(result.rows[0])).toBe(true);
});

it("shares mounted row objects, evicts all unmounted row metadata, and clears account caches immediately", async () => {
  const { init } = await setup();
  const storage = new IndexedDBLocalReplicaStorage(init.storage.replicaName);
  await storage.replaceSnapshot({ entities: { tasks: { one: { id: "one", value: 1, nested: { label: "same" } } } }, liveQueries: {} }, "visible"); storage.close();
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact" }, auth, storage: init.storage, fallbackBinding: binding }); cleanups.push(() => client.close());
  const a = client.watchEntities("tasks", ["one"]), b = client.watchEntities("tasks", ["one"]);
  const stopA = a.onUpdate(() => undefined), stopB = b.onUpdate(() => undefined);
  await vi.waitFor(() => { expect(a.getSnapshot().status).toBe("ready"); expect(b.getSnapshot().status).toBe("ready"); });
  const before = a.getSnapshot() as Extract<ReturnType<typeof a.getSnapshot>, { status: "ready" }>;
  expect((b.getSnapshot() as typeof before).rows[0]).toBe(before.rows[0]); expect(client.cacheStats().rows).toBe(1);
  client.setAuth({ identity: { sub: "other", iss: "issuer" } });
  expect(a.getSnapshot()).toEqual({ status: "pending" }); expect(b.getSnapshot()).toEqual({ status: "pending" }); expect(client.cacheStats().rows).toBe(0);
  await new Promise(resolve => setTimeout(resolve, 20)); expect(a.getSnapshot()).toEqual({ status: "pending" });
  stopA(); stopB(); expect(client.cacheStats().rows).toBe(0);
});

it("retains the canonical journal entry if a retirement receipt cannot persist", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  const remove = vi.fn(async () => undefined);
  const entry: any = { id: 1, scope: "s", path: "p", args: {}, idempotencyKey: "c", state: "committed", engineOutcome: { result: 1 } };
  const store = receipts.wrap({ load: async () => [entry], put: async () => undefined, delete: remove, clear: async () => undefined });
  vi.spyOn(receipts, "put").mockRejectedValueOnce(new Error("Disk full"));
  await expect(store.delete(1)).rejects.toThrow("Disk full"); expect(remove).not.toHaveBeenCalled();
});

it("preserves typed superseded errors across RPC so hooks can ignore old-scope requests", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("query", [], "query"));
  worker.postMessage({ generation: "g", scope: "s", id: "query", error: { name: "GonvexClientError", message: "Account changed", code: "superseded", operation: "query", path: "tasks" } });
  await expect(pending).rejects.toBeInstanceOf(GonvexClientError);
});

it("streams nested arrays and object maps without losing shared large subtrees or JSON key semantics", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const rows = Array.from({ length: 100 }, (_, index) => ({ id: String(index), nested: { value: "x".repeat(1000) } }));
  const result = { first: rows, second: rows, dictionary: Object.fromEntries(rows.map(row => [row.id, row])), special: JSON.parse('{"__proto__":{"value":1}}') };
  const pending = rpc.call<typeof result>(rpc.request("query", [], "graph"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "graph", result });
  const actual = await pending; expect(actual).toEqual(result); expect(actual.first).toBe(actual.second); expect(Object.hasOwn(actual.special, "__proto__")).toBe(true);
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
});

it("publishes a whole committed watch batch before notifying either mounted view", async () => {
  const endpoints = pair();
  const watchIds: string[] = [];
  let envelope: any;
  endpoints.worker.addEventListener("message", ({ data }) => {
    if (!data.method) return;
    envelope = data;
    if (data.method === "watch") watchIds.push(data.args[0]);
    endpoints.worker.postMessage({ generation: data.generation, scope: data.scope, id: data.id, result: data.method === "init" ? {
      connection: { isWebSocketConnected: false, hasEverConnected: false, connectionCount: 0, connectionRetries: 0, hasInflightRequests: false, inflightReducers: 0, inflightActions: 0, inflightOneShotQueries: 0 }, session: "restored", commandScope: owner,
    } : undefined });
  });
  vi.stubGlobal("Worker", class {});
  vi.stubGlobal("navigator", { locks: { request: (_name: string, _options: any, callback: any) => Promise.resolve(callback({ name: _name })), query: async () => ({ held: [] }) } });
  const worker = Object.assign(endpoints.ui, { terminate: vi.fn() }) as unknown as Worker;
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact", workerFactory: () => worker }, auth,
    storage: { replicaName: "atomic", outboxName: "atomic-outbox", contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 }, fallbackBinding: binding }); cleanups.push(() => client.close());
  const a = client.watchEntities("tasks", ["1"]), b = client.watchEntities("tasks", ["2"]);
  const observed: unknown[] = [];
  const read = () => observed.push([a.getSnapshot(), b.getSnapshot()]);
  const stopA = a.onUpdate(read), stopB = b.onUpdate(read); cleanups.push(stopA, stopB);
  await vi.waitFor(() => expect(watchIds).toHaveLength(2));
  endpoints.worker.postMessage({ generation: envelope.generation, scope: envelope.scope, id: "commit", event: "watchBatch", args: [1, watchIds.map((id, index) => ({ id, value: { status: "ready", rows: [{ id: String(index + 1), count: 1 }] } }))] });
  await vi.waitFor(() => expect(observed).toHaveLength(2));
  for (const states of observed as any[]) expect(states.map((state: any) => state.rows[0].count)).toEqual([1, 1]);
});

it("coalesces admissions without waiting for replies and preserves control ordering", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  rpc.setAdmissionBatching(true);
  const requests = Array.from({ length: 3 }, (_, index) => rpc.request("reducer", [{ index }], String(index)));
  const pending = requests.map(request => rpc.call(request));
  const control = rpc.call(rpc.request("query", [], "control"));
  expect(ui.sent[0]).toEqual({ admissions: requests });
  expect(ui.sent[1].id).toBe("control");
  for (const request of requests.toReversed()) worker.postMessage({ ...request, result: Number(request.id) });
  worker.postMessage({ generation: "g", scope: "s", id: "control", result: 9 });
  expect(await Promise.all(pending)).toEqual([0, 1, 2]); expect(await control).toBe(9);
});

it("recovers every command whose batch replies were lost without duplicating durable entries", async () => {
  const { create } = await setup(); let host = await create();
  const commands = Array.from({ length: 3 }, (_, index) => ({ id: `batch-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const requests = commands.map(command => host.rpc.request("reducer", [ref, {}, { engineCommand: command }]));
  const reply = host.endpoints.worker.postMessage.bind(host.endpoints.worker);
  host.endpoints.worker.postMessage = data => { if (!requests.some(request => request.id === (data as any).id)) reply(data); };
  const results = await Promise.allSettled(requests.map(request => host.rpc.call(request, 100)));
  expect(results.every(result => result.status === "rejected" && result.reason instanceof EngineRpcTimeout)).toBe(true);
  expect(host.endpoints.ui.sent.filter(frame => frame.admissions).map(frame => frame.admissions.length)).toEqual([3]);
  const before = await host.rpc.call<any[]>(host.rpc.request("listIntents", []));
  expect(before.map(entry => entry.id)).toEqual(commands.map(command => command.id));
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await Promise.all(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))))).toEqual([7, 7, 7]);
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual(before);
});

it("batches receipt retirement into one transaction and bounds recovery with a durable retry floor", async () => {
  const name = crypto.randomUUID(); let receipts = new EngineReceipts(name, 2, 1000);
  const database = (receipts as any).database as Dexie;
  const transaction = vi.spyOn(database, "transaction");
  const now = Date.now();
  await Promise.all([0, 1, 2].map(index => receipts.put("s", String(index), "p", {}, index, now - 10 + index)));
  expect(transaction).toHaveBeenCalledTimes(1);
  expect(await database.table("receipts").count()).toBe(2);
  const remaining = await database.table("receipts").toArray();
  const removed = [0, 1, 2].find(index => !remaining.some(receipt => receipt.key === JSON.stringify(["s", String(index)])))!;
  receipts.close(); receipts = new EngineReceipts(name, 2, 1000); cleanups.push(() => receipts.close());
  await expect(receipts.assertCanAdmit(now - 10 + removed)).rejects.toThrow("receipt expired");
  await expect(receipts.assertCanAdmit(now + 1)).resolves.toBeUndefined();
  const spy = vi.spyOn(Date, "now").mockReturnValue(now + 2000);
  expect(await receipts.get("s", String(remaining[0].result))).toBeUndefined();
  await expect(receipts.assertCanAdmit(now)).rejects.toThrow("receipt expired");
  await receipts.put("s", "new", "p", {}, 9);
  spy.mockRestore();
  expect(await (receipts as any).database.table("receipts").count()).toBe(1);
});

it("keeps all canonical entries when a batched receipt transaction fails", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  const entries: any[] = [1, 2].map(id => ({ id, scope: "s", path: "p", args: {}, idempotencyKey: String(id), state: "committed", engineOutcome: { result: id }, createdAt: Date.now() }));
  const remove = vi.fn(async () => undefined);
  const store = receipts.wrap({ load: async () => entries, put: async () => undefined, delete: remove, deleteMany: remove, clear: async () => undefined });
  vi.spyOn((receipts as any).database, "transaction").mockRejectedValueOnce(new Error("Crash before commit"));
  await expect(store.deleteMany!([1, 2])).rejects.toThrow("Crash before commit");
  expect(remove).not.toHaveBeenCalled();
  expect(await receipts.get("s", "1")).toBeUndefined(); expect(await receipts.get("s", "2")).toBeUndefined();
});

it("preserves sibling admissions when storage fails mid-batch and retries the original missing command", async () => {
  const { create } = await setup(); let host = await create();
  const commands = [0, 1, 2].map(index => ({ id: `partial-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "bc".repeat(32) }));
  const wrap = EngineReceipts.prototype.wrap;
  let calls = 0;
  const failure = vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    const append = wrapped.append!;
    return { ...wrapped, append: async entry => {
      if (++calls === 2) throw new Error("Crash before durable admission");
      return append(entry);
    } };
  });
  await host.close(); host = await create();
  const results = await Promise.allSettled(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }]))));
  expect(results.map(result => result.status)).toEqual(["fulfilled", "rejected", "fulfilled"]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(["partial-0", "partial-2"]);
  failure.mockRestore(); await host.close(); host = await create(); cleanups.push(host.close);
  expect(await Promise.all(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))))).toEqual([7, 7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id).sort()).toEqual(commands.map(command => command.id));
});

it("migrates old receipts before pruning and refuses ambiguous retries after eviction", async () => {
  const name = crypto.randomUUID();
  const old = new Dexie(`${name}-engine-receipts`);
  old.version(1).stores({ receipts: "&key" });
  await old.table("receipts").bulkPut([0, 1].map(index => ({ key: JSON.stringify(["s", String(index)]), path: "p", args: "{}", result: index })));
  old.close();
  const receipts = new EngineReceipts(name, 1); cleanups.push(() => receipts.close());
  const existing = await receipts.get("s", "0");
  const migrated = await (receipts as any).database.table("receipts").toArray();
  expect(migrated).toHaveLength(1); expect([0, 1]).toContain(migrated[0].result);
  if (existing) expect(existing.result).toBe(0);
  await expect(receipts.assertCanAdmit(Date.now() - 1000)).rejects.toThrow("receipt expired");
  await receipts.put("s", "new", "p", {}, 9);
  expect(await (receipts as any).database.table("receipts").count()).toBe(1);
  await expect(receipts.assertCanAdmit(Date.now() - 1000)).rejects.toThrow("receipt expired");
});

it("keeps individual reducer envelopes until batching is negotiated", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const requests = [rpc.request("reducer", [], "old-a"), rpc.request("reducer", [], "old-b")];
  const pending = requests.map(request => rpc.call(request));
  expect(ui.sent).toEqual(requests);
  for (const request of requests) worker.postMessage({ ...request, result: 1 });
  expect(await Promise.all(pending)).toEqual([1, 1]);
});

it("orders lookup starts without making later calls wait for the first durable acknowledgement", async () => {
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const starts: string[] = [];
  const lookup = vi.spyOn(EngineReceipts.prototype, "findReceipt").mockImplementation(async (_scope, id) => { starts.push(id); return undefined; });
  const admit = EngineReceipts.prototype.assertCanAdmit;
  let checks = 0;
  const check = vi.spyOn(EngineReceipts.prototype, "assertCanAdmit").mockImplementation(async function(now) {
    if (++checks === 1) await blocked;
    return admit.call(this, now);
  });
  const commands = [0, 1].map(index => ({ id: `pipeline-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "cd".repeat(32) }));
  const pending = commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }])));
  await vi.waitFor(() => expect(starts.slice(0, 2)).toEqual(["pipeline-0", "pipeline-1"]));
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  release(); expect(await Promise.all(pending)).toEqual([7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
  lookup.mockRestore(); check.mockRestore();
});

it("delivers preceding watch batches before resolving a small reducer reply", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const events: string[] = []; rpc.onEvent.add(method => events.push(method));
  const pending = rpc.call(rpc.request("reducer", [], "result"));
  const watch = sendEngineMessage(worker, { generation: "g", scope: "s", id: "watch", event: "watchBatch", args: ["x".repeat(40_000)] });
  const reply = sendEngineMessage(worker, { generation: "g", scope: "s", id: "result", result: 7 });
  expect(await pending).toBe(7); expect(events).toEqual(["watchBatch"]);
  await Promise.all([watch, reply]);
});

it("amortizes pruning while never exceeding the hard receipt cap", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  await Promise.all(Array.from({ length: 1024 }, (_, index) => receipts.put("s", String(index), "p", {}, index)));
  const database = (receipts as any).database;
  const readFloor = vi.spyOn(database.metadata, "get");
  for (let index = 1024; index < 1088; index++) {
    await receipts.put("s", String(index), "p", {}, index);
    expect(await database.receipts.count()).toBeLessThanOrEqual(1024);
  }
  expect(readFloor).toHaveBeenCalledTimes(1);
  expect(await database.receipts.count()).toBe(1024);
});

it("preserves an online queued-capable reducer's actual server result after retirement and reopen", async () => {
  const { create } = await setup(); let host = await create();
  await host.rpc.call(host.rpc.request("connect", [])); const socket = Socket.all[0]; socket.open();
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
  const frame = socket.sent.find(frame => frame.type === "auth");
  socket.receive({ type: "auth.result", id: frame.id, result: { artifactHash: "artifact", replica: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, localIdentity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } } });
  await vi.waitFor(() => expect(host.endpoints.worker.sent.some(frame => frame.event === "session" && frame.args[0] === "authenticated")).toBe(true));
  const remote: FunctionReference = { kind: "reducer", path: "online", offline: { mode: "allowed" } };
  const command = { id: "server-result", now: Date.now(), scope: owner, entropy: "de".repeat(32) };
  const first = host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command }]));
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "reducer.call" && frame.path === "online")).toBe(true));
  const call = socket.sent.find(frame => frame.type === "reducer.call" && frame.path === "online");
  socket.receive({ type: "reducer.result", id: call.id, result: { value: 42 } });
  expect(await first).toEqual({ value: 42 });
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  await host.close(); host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command }]))).toEqual({ value: 42 });
  expect(socket.sent.filter(frame => frame.type === "reducer.call" && frame.path === "online")).toHaveLength(1);
});

it("does not turn a definitive server rejection into a queued receipt on retry or reopen", async () => {
  const { create, init } = await setup(); let host = await create();
  cleanups.push(() => host.close());
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  const remote: FunctionReference = { kind: "reducer", path: "online", offline: { mode: "allowed" } };
  const command = { id: "server-rejection", now: Date.now(), scope: owner, entropy: "de".repeat(32) };
  const connect = async () => {
    await host.rpc.call(host.rpc.request("connect", [])); const socket = Socket.all.at(-1)!; socket.open();
    await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
    const frame = socket.sent.find(frame => frame.type === "auth");
    socket.receive({ type: "auth.result", id: frame.id, result: { artifactHash: "artifact", replica: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, localIdentity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } } });
    await vi.waitFor(() => expect(host.endpoints.worker.sent.some(frame => frame.event === "session" && frame.args[0] === "authenticated")).toBe(true));
    return socket;
  };
  let socket = await connect();
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt === 2) { await host.close(); host = await create(); socket = await connect(); }
    const count = socket.sent.filter(frame => frame.type === "reducer.call").length;
    const pending = host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command, offline: "queue" }]));
    const rejected = expect(pending).rejects.toMatchObject({ code: "server", errorClass: "rejected", message: "Task is archived" });
    await vi.waitFor(() => expect(socket.sent.filter(frame => frame.type === "reducer.call")).toHaveLength(count + 1));
    const call = socket.sent.filter(frame => frame.type === "reducer.call").at(-1)!;
    expect(call.idempotencyKey).toBe(command.id);
    socket.receive({ type: "reducer.error", id: call.id, path: call.path, error: "Task is archived", class: "rejected", retryable: false });
    await rejected;
    expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
    expect(await receipts.get(owner, command.id)).toBeUndefined();
  }
});

it("rejects an expired remote command without reporting a nonexistent queued intent", async () => {
  const { create, init } = await setup(); const host = await create(); cleanups.push(host.close);
  const remote: FunctionReference = { kind: "reducer", path: "online", offline: { mode: "allowed" } };
  const command = { id: "expired-remote", now: Date.now() - 31 * 24 * 60 * 60 * 1000, scope: owner, entropy: "de".repeat(32) };
  await expect(host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command, offline: "queue" }]))).rejects.toThrow("receipt expired");
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  expect(await receipts.get(owner, command.id)).toBeUndefined();
  expect(Socket.all).toHaveLength(0);
});

it("propagates remote admission storage failures instead of reporting a queued intent", async () => {
  const { create, init } = await setup();
  const wrap = EngineReceipts.prototype.wrap;
  vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    return { ...wrap.call(this, store), put: async () => { throw new Error("Disk full before admission"); } };
  });
  const host = await create(); cleanups.push(host.close);
  const remote: FunctionReference = { kind: "reducer", path: "online", offline: { mode: "allowed" } };
  const command = { id: "failed-remote", now: Date.now(), scope: owner, entropy: "de".repeat(32) };
  await expect(host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command, offline: "queue", engineFresh: true }]))).rejects.toThrow("Disk full before admission");
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  expect(await receipts.get(owner, command.id)).toBeUndefined();
});

it("returns queued only after durable remote admission and recovers that outcome after reopen", async () => {
  const { create } = await setup(); let host = await create(); cleanups.push(() => host.close());
  const remote: FunctionReference = { kind: "reducer", path: "online", offline: { mode: "allowed" } };
  const command = { id: "queued-remote", now: Date.now(), scope: owner, entropy: "de".repeat(32) };
  const queued = { status: "queued", reducerId: command.id };
  expect(await host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command, offline: "queue" }]))).toEqual(queued);
  const entries = await host.rpc.call<any[]>(host.rpc.request("listIntents", []));
  expect(entries).toEqual([expect.objectContaining({ id: command.id, state: "pending" })]);
  await host.close(); host = await create();
  expect(await host.rpc.call(host.rpc.request("reducer", [remote, {}, { engineCommand: command, offline: "queue" }]))).toEqual(queued);
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual(entries);
});

it("rechecks receipts after the canonical read so retirement cannot race an explicit retry", async () => {
  const { create, init } = await setup(); const host = await create(); cleanups.push(host.close);
  const receipts = new EngineReceipts(init.storage.outboxName); cleanups.push(() => receipts.close());
  const command = { id: "retired-race", now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ef".repeat(32) };
  await receipts.put(owner, command.id, ref.path, {}, 7, command.now);
  const lookup = vi.spyOn(EngineReceipts.prototype, "findReceipt").mockResolvedValueOnce(undefined);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  expect(await host.rpc.call(host.rpc.request("listIntents", []))).toEqual([]);
  expect(lookup).toHaveBeenCalledTimes(2); lookup.mockRestore();
});

it("keeps loaded entity reads ready across same-account offline session transitions", async () => {
  const { init } = await setup();
  const storage = new IndexedDBLocalReplicaStorage(init.storage.replicaName);
  await storage.replaceSnapshot({ entities: { tasks: { one: { id: "one", value: 1 } } }, liveQueries: {} }, "visible"); storage.close();
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact" }, auth, storage: init.storage, fallbackBinding: binding }); cleanups.push(() => client.close());
  const watch = client.watchEntities("tasks", ["one"]); const states: string[] = [];
  const stop = watch.onUpdate(() => states.push(watch.getSnapshot().status)); cleanups.push(stop);
  await vi.waitFor(() => expect(watch.getSnapshot().status).toBe("ready"));
  client.connect(); await vi.waitFor(() => expect(Socket.all).toHaveLength(1)); const socket = Socket.all[0]; socket.open();
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
  const frame = socket.sent.find(frame => frame.type === "auth");
  socket.receive({ type: "auth.result", id: frame.id, result: { artifactHash: "artifact", replica: { protocolVersion: 1, scope: "t", visibilityScope: "visible", epoch: "e" }, localIdentity: { auth: { account: { id: "a" } }, tenant: { id: "t" }, member: { id: "m", accountId: "a", permissions: {} } } } });
  await vi.waitFor(() => expect(client.localSessionState()).toBe("authenticated"));
  states.length = 0; const before = watch.getSnapshot(); socket.close();
  await vi.waitFor(() => expect(client.localSessionState()).toBe("restored"));
  await new Promise(resolve => setTimeout(resolve, 20));
  expect(states).not.toContain("pending"); expect(watch.getSnapshot()).toBe(before);
});

it("coalesces a 500-call burst into one negotiated packet and retains the older 128-call limit", async () => {
  for (const limit of [undefined, 1024]) {
    const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); rpc.setAdmissionBatching(true, limit);
    const pending = Array.from({ length: 500 }, (_, index) => rpc.call(rpc.request("reducer", [], String(index)), 0));
    const settled = Promise.allSettled(pending); await Promise.resolve();
    expect(ui.sent.map(frame => frame.admissions.length)).toEqual(limit ? [500] : [128, 128, 128, 116]);
    rpc.close(); await settled;
  }
});

it("recovers the original outcome from a journal written in the older wrapper format", async () => {
  const { create, init } = await setup(); let host = await create();
  const command = { id: "older-format", now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) };
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  await host.close();
  const database = new Dexie(init.storage.outboxName); await database.open();
  const [entry] = await database.table("entries").toArray();
  const { engineResult, ...older } = entry;
  await database.table("entries").put({ ...older, engineOutcome: { result: engineResult } }); database.close();
  host = await create(); cleanups.push(host.close);
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command }]))).toBe(7);
  expect(await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).toHaveLength(1);
});

it("shares one intent lease for a fresh burst while acknowledging each durable entry in order", async () => {
  const { create } = await setup();
  const locks: string[] = [];
  const wrap = EngineReceipts.prototype.wrap;
  vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    return { ...wrapped, coordinate: (scope, lane, run) => {
      locks.push(lane); return wrapped.coordinate!(scope, lane, run);
    } };
  });
  const host = await create(); cleanups.push(host.close); locks.length = 0;
  const commands = Array.from({ length: 8 }, (_, index) => ({ id: `cohort-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const acknowledgements: string[] = [];
  expect(await Promise.all(commands.map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }])).then(result => { acknowledgements.push(command.id); return result; })))).toEqual(Array(8).fill(7));
  expect(acknowledgements).toEqual(commands.map(command => command.id));
  expect(locks.filter(name => name.includes("intent"))).toHaveLength(1);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(acknowledgements);
});

it("does not let a fresh cohort overtake an explicit retry awaiting lookup", async () => {
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const lookup = EngineReceipts.prototype.findReceipt;
  const waiting = vi.spyOn(EngineReceipts.prototype, "findReceipt").mockImplementation(async function(scope, id, path, args) {
    if (id === "ordered-0") await blocked;
    return lookup.call(this, scope, id, path, args);
  });
  const commands = Array.from({ length: 3 }, (_, index) => ({ id: `ordered-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const first = host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: commands[0] }]));
  await vi.waitFor(() => expect(waiting).toHaveBeenCalled());
  const rest = commands.slice(1).map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }])));
  await new Promise(resolve => setTimeout(resolve, 0));
  release(); expect(await Promise.all([first, ...rest])).toEqual([7, 7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
});

it("keeps a later packet behind a cohort waiting for its delivery lease", async () => {
  const { create } = await setup();
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const wrap = EngineReceipts.prototype.wrap; let waiting = false;
  vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    return { ...wrapped, coordinate: async (scope, lane, run) => {
      if (lane === "delivery" && !waiting) { waiting = true; await blocked; }
      return wrapped.coordinate!(scope, lane, run);
    } };
  });
  const host = await create(); cleanups.push(host.close);
  const commands = Array.from({ length: 3 }, (_, index) => ({ id: `packet-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  const pending = commands.slice(0, 2).map(command => host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }])));
  await vi.waitFor(() => expect(waiting).toBe(true));
  pending.push(host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: commands[2], engineFresh: true }])));
  await new Promise(resolve => setTimeout(resolve, 0));
  release(); expect(await Promise.all(pending)).toEqual([7, 7, 7]);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
});

it("clears a watch error when its next successful host read has the same value", async () => {
  let failed = false, publish = () => {};
  vi.spyOn(GonvexClient.prototype, "watchControlQuery").mockImplementation(() => ({
    onUpdate: (notify: () => void) => { publish = notify; return () => {}; },
    getSnapshot: () => { if (failed) throw new Error("Temporary read failure"); return { result: 1, version: 1 }; },
  }) as any);
  const { create } = await setup(); const host = await create(); cleanups.push(host.close);
  const updates: any[] = [];
  host.rpc.onEvent.add((method, args) => { if (method === "watchBatch") updates.push(...args[1]); });
  await host.rpc.call(host.rpc.request("watch", ["control", "control", [{ kind: "query", path: "control.test" }, {}]]));
  await vi.waitFor(() => expect(updates.at(-1)?.value).toEqual({ result: 1, version: 1 }));
  failed = true; publish();
  await vi.waitFor(() => expect(updates.at(-1)?.error?.message).toBe("Temporary read failure"));
  failed = false; publish();
  await vi.waitFor(() => expect(updates.at(-1)).toEqual({ id: "control", value: { result: 1, version: 1 } }));
});

it("uses the declared Live Query key for offline membership", async () => {
  const { init, create } = await setup();
  const storage = new IndexedDBLocalReplicaStorage(init.storage.replicaName);
  await storage.replaceSnapshot({ entities: { tasks: { one: { slug: "one", title: "Task", id: "other-id" } } }, liveQueries: {} }, "visible"); storage.close();
  const host = await create(); cleanups.push(host.close);
  const liveRef: FunctionReference = { kind: "query", path: "tasks.bySlug", delivery: "live", live: {
    entity: "tasks", key: "slug", plan: { table: "tasks", key: "slug", columns: ["slug", "title", "id"] },
  } };
  let snapshot: any;
  host.rpc.onEvent.add((method, args) => { if (method === "watchBatch") for (const update of args[1]) if (update.id === "live") snapshot = update.patches ? applyEnginePatches(snapshot, update.patches) : update.value; });
  await host.rpc.call(host.rpc.request("watch", ["live", "liveState", [liveRef, {}]]));
  await vi.waitFor(() => expect(snapshot?.ids).toEqual(["one"]));
  expect(snapshot.rows).toEqual([{ slug: "one", title: "Task", id: "other-id" }]);
  expect(snapshot.freshness).toBe("offline");
});

async function mockEngine(intercept?: (request: any, endpoint: Endpoint) => boolean) {
  const hosts: Array<{ endpoints: ReturnType<typeof pair>; envelope: any; watchIds: string[] }> = [];
  vi.stubGlobal("Worker", class {});
  vi.stubGlobal("navigator", { locks: { request: (_name: string, _options: any, callback: any) => Promise.resolve(callback({ name: _name })), query: async () => ({ held: [] }) } });
  const workerFactory = () => {
    const host = { endpoints: pair(), envelope: undefined as any, watchIds: [] as string[] }; hosts.push(host);
    host.endpoints.worker.addEventListener("message", ({ data }) => {
      if (!data.method) return;
      host.envelope = data;
      if (data.method === "watch") host.watchIds.push(data.args[0]);
      if (intercept?.(data, host.endpoints.worker)) return;
      host.endpoints.worker.postMessage({ generation: data.generation, scope: data.scope, id: data.id, result: data.method === "init" ? {
        connection: { isWebSocketConnected: false, hasEverConnected: false, connectionCount: 0, connectionRetries: 0, hasInflightRequests: false, inflightReducers: 0, inflightActions: 0, inflightOneShotQueries: 0 }, session: "restored", commandScope: owner,
      } : undefined });
    });
    return Object.assign(host.endpoints.ui, { terminate: vi.fn() }) as unknown as Worker;
  };
  const client = await createEngineClient(url, { engine: { workerUrl: "worker.js", artifactHash: "artifact", workerFactory }, auth,
    storage: { replicaName: "mock", outboxName: "mock-outbox", contract: { version: 1, offlineMaxAgeMs: null }, initialVersion: 1 }, fallbackBinding: binding });
  cleanups.push(() => client.close());
  const publish = (sequence: number, value: unknown) => {
    const host = hosts.at(-1)!;
    host.endpoints.worker.postMessage({ generation: host.envelope.generation, scope: host.envelope.scope, id: crypto.randomUUID(), event: "watchBatch",
      args: [sequence, [{ id: host.watchIds.at(-1), value }]] });
  };
  return { client, hosts, publish };
}

it.each(["setAuth", "authenticate", "scope", "reset", "recover"] as const)("clears every watch and intent before notifying subscribers during %s", async method => {
  const { client, hosts } = await mockEngine();
  const a = client.watchEntities("tasks", ["one"]), b = client.watchEntities("tasks", ["two"]);
  const observed: unknown[] = [];
  const read = () => { observed.push([a.getSnapshot(), b.getSnapshot(), client.intentsSnapshot(), client.cacheStats().rows]); };
  cleanups.push(a.onUpdate(read), b.onUpdate(read), client.subscribeIntents(read), client.onSessionScopeChange(read));
  const host = hosts[0];
  await vi.waitFor(() => expect(host.watchIds).toHaveLength(2));
  const event = (name: string, args: unknown[]) => host.endpoints.worker.postMessage({ generation: host.envelope.generation, scope: host.envelope.scope, id: crypto.randomUUID(), event: name, args });
  event("watchBatch", [42, host.watchIds.map((id, index) => ({ id, value: { status: "ready", rows: [{ id: index ? "two" : "one", value: 1 }] } }))]);
  event("intents", [[{ id: "old-account-intent" }]]);
  await vi.waitFor(() => {
    expect(a.getSnapshot().status).toBe("ready"); expect(b.getSnapshot().status).toBe("ready");
    expect(client.intentsSnapshot()).toHaveLength(1); expect(client.cacheStats().rows).toBe(2);
  });
  observed.length = 0;
  if (method === "setAuth") client.setAuth({ identity: { sub: "other", iss: "issuer" } });
  else if (method === "authenticate") await client.authenticate({ identity: { sub: "other", iss: "issuer" } });
  else if (method === "scope") event("scope", ["new-scope"]);
  else if (method === "reset") await client.resetLocalReplica();
  else await client.recover();
  await vi.waitFor(() => expect(observed).toHaveLength(4));
  for (const snapshot of observed) expect(snapshot).toEqual([{ status: "pending" }, { status: "pending" }, [], 0]);
});

it.each(["watchBatch", "watchError"] as const)("rejects collection readiness on %s errors without interrupting other subscribers", async method => {
  const { client, hosts } = await mockEngine();
  const retain = client.retainCollection({ kind: "query", path: "tasks.all" }); cleanups.push(retain);
  const ready = retain.ready.then(() => ({ resolved: true }), error => ({ error }));
  const watch = client.watchEntities("tasks", ["one"]), notified = vi.fn();
  cleanups.push(watch.onUpdate(notified));
  const host = hosts[0];
  await vi.waitFor(() => expect(host.watchIds).toHaveLength(2));
  const error = { name: "GonvexClientError", code: "storage", message: "Collection read failed" };
  const healthy = { id: host.watchIds[1], value: { status: "ready", rows: [{ id: "one", value: 1 }] } };
  const event = (name: string, args: unknown[]) => host.endpoints.ui.emit("message", { data: {
    generation: host.envelope.generation, scope: host.envelope.scope, id: crypto.randomUUID(), event: name, args,
  } });
  expect(() => {
    if (method === "watchBatch") event(method, [1, [{ id: host.watchIds[0], error }, healthy]]);
    else { event(method, [host.watchIds[0], error]); event("watchBatch", [1, [healthy]]); }
  }).not.toThrow();
  expect(await ready).toMatchObject({ error: { name: "GonvexClientError", code: "storage", message: "Collection read failed" } });
  expect(notified).toHaveBeenCalledOnce();
  expect(watch.getSnapshot()).toEqual(healthy.value);
});

it("resolves collection readiness only when its watch becomes up to date", async () => {
  const { client, hosts, publish } = await mockEngine();
  const retain = client.retainCollection({ kind: "query", path: "tasks.all" }); cleanups.push(retain);
  const resolved = vi.fn(); void retain.ready.then(resolved);
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  publish(1, { isUpToDate: false });
  await Promise.resolve();
  expect(resolved).not.toHaveBeenCalled();
  publish(2, { isUpToDate: true });
  await retain.ready;
  expect(resolved).toHaveBeenCalledOnce();
});

it.each(["query", "action"] as const)("honors longer and disabled per-call %s timeouts", async method => {
  const calls: Array<{ request: any; endpoint: Endpoint }> = [];
  const { client } = await mockEngine((request, endpoint) => {
    if (request.method !== method) return false;
    calls.push({ request, endpoint }); return true;
  });
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    for (const timeoutMs of [30_000, 0]) {
      let settled = false;
      const pending = client[method]({ kind: method, path: "slow.test" }, {}, { timeoutMs }).then(
        result => { settled = true; return { result }; }, error => { settled = true; return { error }; });
      await vi.advanceTimersByTimeAsync(21_000);
      const timedOut = settled;
      const { request, endpoint } = calls.at(-1)!;
      endpoint.postMessage({ generation: request.generation, scope: request.scope, id: request.id, result: 42 });
      const outcome = await pending;
      expect(timedOut).toBe(false);
      expect(outcome).toEqual({ result: 42 });
      expect(request.args[2]).toEqual({ timeoutMs });
    }
  } finally { vi.useRealTimers(); }
});

it.each([false, true])("resubscribes mounted watches after replica reset, rejected=%s", async rejected => {
  const { client, hosts, publish } = await mockEngine((request, endpoint) => {
    if (request.method !== "resetLocalReplica") return false;
    endpoint.postMessage({ generation: request.generation, scope: request.scope, id: request.id,
      ...(rejected ? { error: { name: "GonvexClientError", code: "storage", message: "Reset failed" } }
        : { result: { discardedIntents: 0, resubscribed: 1 } }) }); return true;
  });
  const watch = client.watchEntities("tasks", ["one"]);
  cleanups.push(watch.onUpdate(() => {}));
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  const previousId = hosts[0].watchIds[0];
  publish(42, { status: "ready", rows: [{ id: "one", value: 1 }] });
  await vi.waitFor(() => expect(watch.getSnapshot().status).toBe("ready"));
  const reset = client.resetLocalReplica();
  if (rejected) await expect(reset).rejects.toMatchObject({ code: "storage", message: "Reset failed" });
  else expect(await reset).toEqual({ discardedIntents: 0, resubscribed: 1 });
  expect(watch.getSnapshot()).toEqual({ status: "pending" });
  expect(hosts[0].watchIds).toHaveLength(2);
  expect(hosts[0].watchIds[1]).not.toBe(previousId);
  publish(1, { status: "ready", rows: [{ id: "one", value: 2 }] });
  await vi.waitFor(() => expect(watch.getSnapshot()).toEqual({ status: "ready", rows: [{ id: "one", value: 2 }] }));
});

it("accepts object results after truthy scalars and keeps new undefined own fields", async () => {
  const { client, hosts, publish } = await mockEngine();
  const watch = client.watchControlQuery({ kind: "query", path: "control.test" });
  cleanups.push(watch.onUpdate(() => {}));
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  publish(1, { result: 1, version: 1 });
  await vi.waitFor(() => expect(watch.getSnapshot()?.result).toBe(1));
  publish(2, { result: {}, version: 2 });
  await vi.waitFor(() => expect(watch.getSnapshot()?.result).toEqual({}));
  publish(3, { result: { a: undefined }, version: 3 });
  await vi.waitFor(() => expect(watch.getSnapshot()?.result).toEqual({ a: undefined }));
  publish(4, { result: { b: undefined }, version: 4 });
  await vi.waitFor(() => expect(Object.keys(watch.getSnapshot()!.result as object)).toEqual(["b"]));
});

it("accepts a new host's sequence when a detached watch is subscribed again", async () => {
  const { client, hosts, publish } = await mockEngine();
  const watch = client.watchEntities("tasks", ["one"]);
  const stop = watch.onUpdate(() => {});
  await vi.waitFor(() => expect(hosts[0].watchIds).toHaveLength(1));
  publish(42, { status: "ready", rows: [{ id: "one", value: 1 }] });
  await vi.waitFor(() => expect(watch.getSnapshot().status).toBe("ready"));
  stop(); await client.recover();
  cleanups.push(watch.onUpdate(() => {}));
  await vi.waitFor(() => expect(hosts[1].watchIds).toHaveLength(1));
  publish(1, { status: "ready", rows: [{ id: "one", value: 2 }] });
  await vi.waitFor(() => expect(watch.getSnapshot()).toEqual({ status: "ready", rows: [{ id: "one", value: 2 }] }));
});

it("patches own __proto__ fields without changing prototypes or earlier snapshots", () => {
  const previous = Object.freeze({ nested: Object.freeze({}) });
  const added = { nested: JSON.parse('{"__proto__":{"value":1},"missing":null}') };
  added.nested.missing = undefined;
  const first = applyEnginePatches(previous, diffEngineValue(previous, added)!) as typeof added;
  expect(Object.hasOwn(first.nested, "__proto__")).toBe(true);
  expect(Object.hasOwn(first.nested, "missing")).toBe(true);
  expect(Object.getPrototypeOf(first.nested)).toBe(Object.prototype);
  const changed = { nested: JSON.parse('{"__proto__":{"value":2}}') };
  const second = applyEnginePatches(first, diffEngineValue(first, changed)!) as typeof changed;
  expect(Object.getPrototypeOf(second.nested)).toBe(Object.prototype);
  expect(second.nested.__proto__.value).toBe(2);
  expect(first.nested.__proto__.value).toBe(1);
  expect(previous.nested).toEqual({});
});

it("preserves aliases and empty-name undefined fields in the long-key text codec", async () => {
  const { ui, worker } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const shared = { "": undefined, nested: { value: "x".repeat(40_000) } };
  const result = { ["k".repeat(1100)]: shared, first: shared, second: shared, "": undefined };
  const pending = rpc.call<typeof result>(rpc.request("query", [], "text"));
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "text", result });
  const actual = await pending;
  expect(actual).toEqual(result); expect(actual.first).toBe(actual.second); expect(actual.first).toBe(actual["k".repeat(1100)]);
  expect(Object.hasOwn(actual, "")).toBe(true); expect(actual[""]).toBeUndefined();
  expect(Object.hasOwn(actual.first, "")).toBe(true); expect(actual.first[""]).toBeUndefined();
  expect(worker.sent.every(frame => new TextEncoder().encode(JSON.stringify(frame)).byteLength <= 64 * 1024)).toBe(true);
});

it.each([false, true])("finishes late chunked replies after timeout, text codec=%s", async text => {
  const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const pending = rpc.call(rpc.request("query", [], "late"), 5);
  const envelope = { generation: "g", scope: "s", id: "late" };
  const receive = (packet: unknown) => ui.emit("message", { data: { ...envelope, packet } });
  receive(text ? { kind: "textBegin" } : { kind: "begin", value: { ...envelope, result: "" } });
  await expect(pending).rejects.toBeInstanceOf(EngineRpcTimeout);
  expect(() => {
    receive(text ? { kind: "text", value: JSON.stringify({ payload: { ...envelope, result: "late" }, undefinedPaths: [], aliasPaths: [] }) }
      : { kind: "string", path: ["result"], value: "late" });
    receive({ kind: text ? "textEnd" : "end" });
  }).not.toThrow();
  const next = rpc.call(rpc.request("query", [], "next"));
  ui.emit("message", { data: { ...envelope, id: "next", result: "fresh" } });
  expect(await next).toBe("fresh");
});

it("accepts text replies from older hosts without the optional alias metadata", async () => {
  const { ui } = pair(); const rpc = new EngineRpc(ui, "g", "s"); cleanups.push(() => rpc.close());
  const request = rpc.request("query", [], "older-host"); const result = rpc.call(request);
  const receive = (packet: unknown) => ui.emit("message", { data: { generation: "g", scope: "s", id: request.id, packet } });
  receive({ kind: "textBegin" });
  receive({ kind: "text", value: JSON.stringify({ payload: { ...request, result: { value: 7 } }, undefinedPaths: [] }) });
  receive({ kind: "textEnd" });
  expect(await result).toEqual({ value: 7 });
});

it("reuses an admission lease across awaited replies and releases it after idle", async () => {
  const { create } = await setup(); const leases: string[] = [];
  const wrap = EngineReceipts.prototype.wrap;
  vi.spyOn(EngineReceipts.prototype, "wrap").mockImplementation(function(store) {
    const wrapped = wrap.call(this, store);
    return { ...wrapped, coordinate: (scope, lane, run) => {
      leases.push(lane); return wrapped.coordinate!(scope, lane, run);
    } };
  });
  const host = await create(); cleanups.push(host.close); leases.length = 0;
  const commands = Array.from({ length: 4 }, (_, index) => ({ id: `awaited-${index}`, now: Date.now(), artifactHash: "artifact", scope: owner, entropy: "ab".repeat(32) }));
  for (const command of commands.slice(0, 3)) expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: command, engineFresh: true }]))).toBe(7);
  expect(leases.filter(lane => lane === "intent")).toHaveLength(1);
  await new Promise(resolve => setTimeout(resolve, 15));
  expect(await host.rpc.call(host.rpc.request("reducer", [ref, {}, { engineCommand: commands[3], engineFresh: true }]))).toBe(7);
  expect(leases.filter(lane => lane === "intent")).toHaveLength(2);
  expect((await host.rpc.call<any[]>(host.rpc.request("listIntents", []))).map(entry => entry.id)).toEqual(commands.map(command => command.id));
});

it("keeps complete text payloads for older receivers that ignore alias metadata", async () => {
  const { ui, worker } = pair(); let text = "", decoded: any;
  ui.addEventListener("message", ({ data }: MessageEvent) => {
    ui.postMessage({ generation: data.generation, scope: data.scope, deliveryAck: data.deliveryId });
    if (data.packet?.kind === "text") text += data.packet.value;
    if (data.packet?.kind === "textEnd") {
      const envelope = JSON.parse(text); decoded = envelope.payload;
      for (const path of envelope.undefinedPaths) {
        let parent = decoded; for (const key of path.slice(0, -1)) parent = parent[key];
        Object.defineProperty(parent, path.at(-1)!, { value: undefined, enumerable: true, configurable: true });
      }
    }
  });
  const row = { value: "v".repeat(20_000), missing: undefined };
  await sendEngineMessage(worker, { generation: "g", scope: "s", id: "older-receiver", result: { ["x".repeat(2000)]: row, other: row } });
  expect(decoded.result.other).toEqual(row);
  expect(Object.hasOwn(decoded.result.other, "missing")).toBe(true);
  expect(decoded.result["x".repeat(2000)]).toEqual(row);
});

it("retires durable local outcomes from the fenced cache without rereading the journal", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  const entry: any = { id: 1, scope: "s", path: "p", args: {}, idempotencyKey: "c", state: "committed", localExecution: { now: Date.now() }, engineResult: 7 };
  receipts.cachedEntry = () => entry;
  const load = vi.fn(async () => [entry]); const getMany = vi.fn(async () => [entry]); const remove = vi.fn(async () => undefined);
  const store = receipts.wrap({ load, getMany, put: async () => undefined, delete: remove, clear: async () => undefined });
  await store.delete(1);
  expect(load).not.toHaveBeenCalled(); expect(getMany).not.toHaveBeenCalled(); expect(remove).toHaveBeenCalledWith(1);
  expect((await receipts.get("s", "c"))?.result).toBe(7);
});

it("rereads remote retirement outcomes instead of trusting a cached queued placeholder", async () => {
  const receipts = new EngineReceipts(crypto.randomUUID()); cleanups.push(() => receipts.close());
  const entry: any = { id: 1, scope: "s", path: "p", args: {}, idempotencyKey: "c", state: "committed", engineResult: 42, createdAt: Date.now() };
  receipts.cachedEntry = () => ({ ...entry, engineResult: { status: "queued" } });
  const load = vi.fn(async () => [entry]);
  const store = receipts.wrap({ load, put: async () => undefined, delete: async () => undefined, clear: async () => undefined });
  await store.delete(1);
  expect(load).toHaveBeenCalledOnce(); expect((await receipts.get("s", "c"))?.result).toBe(42);
});
