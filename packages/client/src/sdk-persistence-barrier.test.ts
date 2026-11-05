import { DatabaseSync } from "node:sqlite";
import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { IndexedDBLocalReplicaStorage } from "./indexeddb-replica.js";
import { IndexedDBEngineReplicaStorage } from "./indexeddb-engine.js";
import type { LocalReplicaStorage } from "./local-replica.js";
import { ExpoSQLiteLocalReplicaStorage, type ExpoSQLiteDatabase } from "../../expo-sqlite/src/index.js";
import { afterEach, expect, it, vi } from "vitest";
import type { ClientMessage, ServerMessage } from "@gonvex/protocol";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference } from "./index.js";
import { replicaRowsHashes, replicaHashesDigest } from "./replica-integrity.js";
import type { SnapshotPage } from "./snapshot-pages.js";
import { createKvOutboxStore, createMemoryGonvexKv } from "./kv-stores.js";
import { createReducerOutbox } from "./outbox.js";

class Socket {
  static CONNECTING = 0;
  static OPEN = 1;
  static instances: Socket[] = [];
  static delayedClose = false;
  readyState = Socket.CONNECTING;
  sent: ClientMessage[] = [];
  private listeners = new Map<string, Array<() => void>>();
  private messages: Array<(event: { data: string }) => void> = [];
  constructor(readonly url: string) { Socket.instances.push(this); }
  addEventListener(type: string, listener: any) {
    if (type === "message") this.messages.push(listener);
    else this.listeners.set(type, [...this.listeners.get(type) ?? [], listener]);
  }
  send(data: string) { this.sent.push(JSON.parse(data) as ClientMessage); }
  open() { this.readyState = Socket.OPEN; for (const listener of this.listeners.get("open") ?? []) listener(); }
  close() { if (Socket.delayedClose) this.readyState = 2; else this.finishClose(); }
  finishClose() { this.readyState = 3; for (const listener of this.listeners.get("close") ?? []) listener(); }
  receive(message: ServerMessage | SnapshotPage) { for (const listener of this.messages) listener({ data: JSON.stringify(message) }); }
}
const clients: GonvexClient[] = [];
const stores: Array<{ close(): void }> = [];
const originalIDB = Dexie.dependencies.indexedDB, originalRange = Dexie.dependencies.IDBKeyRange;
const kinds = ["memory", "legacy", "engine", "sqlite"] as const;
type Kind = typeof kinds[number];
function storageFor(kind: Kind): LocalReplicaStorage {
  if (kind === "memory") return new MemoryLocalReplicaStorage();
  if (kind === "sqlite") {
    const db = new DatabaseSync(":memory:"); stores.push(db);
    const adapter: ExpoSQLiteDatabase = {
      async execAsync(sql) { db.exec(sql); },
      async runAsync(sql, ...params) { return db.prepare(sql).run(...params as any[]); },
      async getAllAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).all(...params as any[]) as T[]; },
      async getFirstAsync<T>(sql: string, ...params: unknown[]) { return (db.prepare(sql).get(...params as any[]) as T) ?? null; },
      async withTransactionAsync(task) { db.exec("BEGIN"); try { await task(); db.exec("COMMIT"); } catch (error) { db.exec("ROLLBACK"); throw error; } },
    };
    return new ExpoSQLiteLocalReplicaStorage(adapter);
  }
  vi.stubGlobal("indexedDB", new IDBFactory()); vi.stubGlobal("IDBKeyRange", IDBKeyRange);
  Dexie.dependencies.indexedDB = globalThis.indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
  const storage = kind === "legacy" ? new IndexedDBLocalReplicaStorage(`barrier-${crypto.randomUUID()}`) : new IndexedDBEngineReplicaStorage(`barrier-${crypto.randomUUID()}`);
  stores.push(storage); return storage;
}
afterEach(async () => {
  const closing = clients.splice(0); for (const client of closing) client.close();
  await Promise.all(closing.map(join)); for (const storage of stores.splice(0)) storage.close();
  Socket.instances = []; Socket.delayedClose = false; vi.restoreAllMocks(); vi.unstubAllGlobals();
  Dexie.dependencies.indexedDB = originalIDB; Dexie.dependencies.IDBKeyRange = originalRange;
});
const ref: FunctionReference = { kind: "query", path: "statuses.all", delivery: "replica", replica: { table: "statuses", key: "id", columns: ["id", "name"], maxRows: 100 } };
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "e" };
function session(socket: Socket, receiptLookup = true) {
  socket.open(); socket.receive({ type: "session.ready", capabilities: { replicaWatermark: 1, replicaSnapshotPages: 1, reducerReceiptLookup: receiptLookup ? 1 : undefined }, replica: directive });
  const auth = socket.sent.find(m => m.type === "auth");
  if (auth?.type === "auth") socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
}
async function join(client: GonvexClient) {
  // Read-only synchronization with frames delivered through the public wire.
  await (client as unknown as { readonly replicaFrames: Promise<void> }).replicaFrames;
}
it.each(kinds)("reconciles accepted work after returning from a different tenant database epoch, %s", async kind => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = storageFor(kind), outbox = createKvOutboxStore(createMemoryGonvexKv());
  const client = new GonvexClient("ws://runtime.test/ws", { project: "project", tenant: "a", token: "fixture",
    identity: { iss: "fixture", sub: "account" }, errorReporting: false,
    localReplica: { storage }, outbox: { store: outbox } });
  clients.push(client);
  const watch = client.watchReplica(ref, {}), socket = Socket.instances.at(-1)!;
  session(socket);
  const lastOpen = () => {
    const open = socket.sent.filter(message => message.type === "replica.open").at(-1)!;
    if (!open || open.type !== "replica.open") throw Error("missing replica open");
    return open;
  };
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "replica.open")).toBe(true));
  const firstRows = [{ id: "status-1", name: "A" }];
  const firstHashes = await replicaRowsHashes(firstRows, "id"), firstDigest = await replicaHashesDigest(firstHashes);
  socket.receive({ type: "replica.snapshot", id: lastOpen().id, path: ref.path, result: firstRows,
    cursor: { epoch: "e", revision: 18 }, key: "id", maxRows: 100, hashes: firstHashes, digest: firstDigest, truncated: false });
  socket.receive({ type: "replica.ready", id: lastOpen().id, path: ref.path, cursor: { epoch: "e", revision: 18 }, digest: firstDigest, truncated: false });
  await join(client);
  const result = client.reducer({ kind: "reducer", path: "statuses.create", offline: { mode: "allowed" } }, {}, { offline: "queue" })
    .then(value => ({ value }), error => ({ error }));
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "reducer.call")).toBe(true));
  const call = socket.sent.find(message => message.type === "reducer.call")!;
  if (call.type !== "reducer.call") throw Error("missing reducer");
  socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 19 });
  await vi.waitFor(async () => expect((await outbox.load())[0]).toMatchObject({ state: "committed", awaitingReplica: true }));
  const rotate = async (tenant: string, visibilityScope: string) => {
    const previous = lastOpen().id;
    client.setAuth({ tenant, token: "fixture" });
    const auth = socket.sent.filter(message => message.type === "auth").at(-1)!;
    if (!auth || auth.type !== "auth") throw Error("missing tenant auth");
    socket.receive({ type: "auth.result", id: auth.id, result: { replica: { ...directive, visibilityScope } } });
    await vi.waitFor(() => expect(lastOpen().id).not.toBe(previous));
    return lastOpen();
  };
  const b = await rotate("b", "visible-b");
  const bRows = [{ id: "status-b", name: "B" }], bHashes = await replicaRowsHashes(bRows, "id");
  const bDigest = await replicaHashesDigest(bHashes);
  socket.receive({ type: "replica.snapshot", id: b.id, path: ref.path, result: bRows,
    cursor: { epoch: "database-b", revision: 3 }, key: "id", maxRows: 100, hashes: bHashes, digest: bDigest, truncated: false });
  socket.receive({ type: "replica.ready", id: b.id, path: ref.path, cursor: { epoch: "database-b", revision: 3 }, digest: bDigest, truncated: false });
  await join(client); expect(watch.status().isUpToDate).toBe(true);
  expect(client.localReplica.entity("statuses", "status-1")).toBeUndefined();
  const a = await rotate("a", "visible");
  await result;
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "query.call")).toBe(true));
  const probe = socket.sent.find(message => message.type === "query.call")!;
  if (probe.type !== "query.call") throw Error("missing receipt lookup");
  socket.receive({ type: "query.result", id: probe.id, path: probe.path, result: { found: true, cursor: { epoch: "e", revision: 19 } } });
  socket.receive({ type: "replica.watermark", revision: 19 });
  await join(client);
  expect(await outbox.load()).toHaveLength(1);
  socket.receive({ type: "replica.ready", id: a.id, path: ref.path, cursor: { epoch: "e", revision: 18 }, digest: firstDigest, truncated: false });
  await join(client); expect(await outbox.load()).toHaveLength(1);
  const rows = [...firstRows, { id: "status-2", name: "Accepted A" }], hashes = await replicaRowsHashes(rows, "id");
  socket.receive({ type: "replica.delta", id: a.id, path: ref.path, cursor: { epoch: "e", revision: 19 }, upserts: [rows[1]!], deleted: [], hashes });
  socket.receive({ type: "replica.ready", id: a.id, path: ref.path, cursor: { epoch: "e", revision: 19 }, digest: await replicaHashesDigest(hashes), truncated: false });
  await join(client);
  await vi.waitFor(async () => expect(await outbox.load()).toEqual([]));
  expect(client.localReplica.entity("statuses", "status-b")).toBeUndefined();
  expect((await storage.load("visible"))?.entities.statuses?.["status-2"]).toEqual(rows[1]);
  expect(socket.sent.filter(message => message.type === "reducer.call")).toHaveLength(1);
});
async function setup(queued: boolean, kind: Kind = "memory", receiveResult = true) {
  vi.stubGlobal("WebSocket", Socket);
  const storage = storageFor(kind);
  const outbox = createKvOutboxStore(createMemoryGonvexKv());
  const client = new GonvexClient("ws://runtime.test/ws", { errorReporting: false, localReplica: { storage }, outbox: { store: outbox } });
  clients.push(client);
  const watch = client.watchReplica(ref, {});
  const socket = Socket.instances.at(-1)!;
  session(socket);
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "replica.open")).toBe(true));
  const open = socket.sent.find(m => m.type === "replica.open")!;
  if (open.type !== "replica.open") throw Error("missing open");
  const rows = [{ id: "status-1", name: "Working" }];
  const hashes = await replicaRowsHashes(rows, "id"), digest = await replicaHashesDigest(hashes);
  socket.receive({ type: "replica.snapshot", id: open.id, path: ref.path, result: rows, cursor: { epoch: "e", revision: 18 }, key: "id", maxRows: 100, hashes, digest, truncated: false });
  socket.receive({ type: "replica.ready", id: open.id, path: ref.path, cursor: { epoch: "e", revision: 18 }, digest, truncated: false });
  await join(client);
  expect(watch.localReplicaState()).toMatchObject({ computedRevision: 18 });
  const reducer: FunctionReference = { kind: "reducer", path: "statuses.create", ...(queued ? { offline: { mode: "allowed" as const } } : {}) };
  let outcome = "pending";
  const result = client.reducer(reducer, {}, queued ? { offline: "queue" as const } : {}).then(value => { outcome = value === "accepted" ? "success" : "queued"; return value; }, error => { outcome = "error"; return error; });
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "reducer.call")).toBe(true));
  const call = socket.sent.find(m => m.type === "reducer.call")!;
  if (call.type !== "reducer.call") throw Error("missing call");
  if (receiveResult) socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 19 });
  await Promise.resolve(); expect(outcome).toBe("pending");
  return { client, storage, outbox, watch, socket, result, outcome: () => outcome, commandId: call.id };
}

it.each(kinds)("does not publish an old tenant's watermark after its storage completion, %s", async kind => {
  const f = await setup(false, kind, false);
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>(done => { enter = done; });
  const held = new Promise<void>(done => { release = done; });
  const persist = f.storage.advanceWatermark!.bind(f.storage);
  vi.spyOn(f.storage, "advanceWatermark").mockImplementationOnce(async (...args) => {
    const receipt = await persist(...args);
    enter();
    await held;
    return receipt;
  });
  try {
    f.socket.receive({ type: "replica.watermark", revision: 1000 });
    await entered;
    f.client.setAuth({ tenant: "replacement", identity: { iss: "fixture", sub: "replacement" } });
    const auth = f.socket.sent.filter(message => message.type === "auth").at(-1)!;
    if (auth.type !== "auth") throw Error("missing replacement auth");
    f.socket.receive({ type: "auth.result", id: auth.id, result: {
      replica: { ...directive, visibilityScope: "replacement-visible", epoch: "replacement-definitions" },
    } });
    release();
    await join(f.client);
    await vi.waitFor(() => expect(f.socket.sent.filter(message => message.type === "replica.open")).toHaveLength(2));
    const open = f.socket.sent.filter(message => message.type === "replica.open").at(-1)!;
    if (open.type !== "replica.open") throw Error("missing replacement open");
    const rows = [{ id: "replacement-row", name: "Replacement" }];
    const hashes = await replicaRowsHashes(rows, "id"), digest = await replicaHashesDigest(hashes);
    f.socket.receive({ type: "replica.snapshot", id: open.id, path: ref.path, result: rows,
      cursor: { epoch: "replacement-database", revision: 3 }, key: "id", maxRows: 100, hashes, digest, truncated: false });
    f.socket.receive({ type: "replica.ready", id: open.id, path: ref.path,
      cursor: { epoch: "replacement-database", revision: 3 }, digest, truncated: false });
    await join(f.client);
    let outcome = "pending";
    const result = f.client.reducer({ kind: "reducer", path: "statuses.create" }, {})
      .then(() => { outcome = "success"; }, () => { outcome = "error"; });
    await vi.waitFor(() => expect(f.socket.sent.filter(message => message.type === "reducer.call")).toHaveLength(2));
    const call = f.socket.sent.filter(message => message.type === "reducer.call").at(-1)!;
    if (call.type !== "reducer.call") throw Error("missing replacement reducer");
    f.socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted",
      originCommandId: call.id, committedRevision: 4 });
    await new Promise(done => setTimeout(done, 0));
    expect({ outcome, savedCursor: (await f.storage.load("replacement-visible"))?.cursor }).toEqual({
      outcome: "pending", savedCursor: { epoch: "replacement-database", revision: 3 },
    });
    f.socket.receive({ type: "replica.watermark", revision: 4 });
    await join(f.client);
    await result;
    expect(outcome).toBe("success");
    expect((await f.storage.load("replacement-visible"))?.cursor).toEqual({ epoch: "replacement-database", revision: 4 });
  } finally { release(); }
});

it.each(kinds)("requires the current activation after returning to the same replica scope, %s", async kind => {
  const f = await setup(false, kind, false);
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>(done => { enter = done; });
  const held = new Promise<void>(done => { release = done; });
  const persist = f.storage.advanceWatermark!.bind(f.storage);
  vi.spyOn(f.storage, "advanceWatermark").mockImplementationOnce(async (...args) => {
    const receipt = await persist(...args);
    enter(); await held;
    return receipt;
  });
  const rotate = (scope: string, tenant: string) => {
    f.client.setAuth({ tenant });
    const auth = f.socket.sent.filter(message => message.type === "auth").at(-1)!;
    if (auth.type !== "auth") throw Error("missing rotation auth");
    f.socket.receive({ type: "auth.result", id: auth.id, result: { replica: { ...directive, visibilityScope: scope } } });
  };
  try {
    f.socket.receive({ type: "replica.watermark", revision: 1000 });
    await entered;
    rotate("intermediate-visible", "intermediate");
    rotate("visible", "original");
    release();
    await join(f.client);
    const state = f.client as unknown as { readonly processedReplicaWatermarkRevision: number };
    expect(state.processedReplicaWatermarkRevision).toBe(0);
    // The original write really committed to its original scope. Its completion
    // cannot itself certify the new activation's subscription readiness.
    expect((await f.storage.load("visible"))?.cursor).toEqual({ epoch: "e", revision: 1000 });
  } finally { release(); }
});

it.each(kinds)("does not let old ready persistence constrain a new tenant, %s", async kind => {
  const f = await setup(false, kind, false);
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>(done => { enter = done; });
  const held = new Promise<void>(done => { release = done; });
  const persist = f.storage.replaceWindow!.bind(f.storage);
  vi.spyOn(f.storage, "replaceWindow").mockImplementationOnce(async (...args) => {
    const receipt = await persist(...args);
    enter(); await held;
    return receipt;
  });
  try {
    const originalOpen = f.socket.sent.find(message => message.type === "replica.open")!;
    if (originalOpen.type !== "replica.open") throw Error("missing original open");
    const oldWindow = f.client.localReplica.listWindows()[0]!;
    f.socket.receive({ type: "replica.ready", id: originalOpen.id, path: ref.path,
      cursor: { epoch: "e", revision: 18 }, digest: await replicaHashesDigest(oldWindow.hashes!), truncated: true });
    await entered;
    f.client.setAuth({ tenant: "ready-replacement", identity: { iss: "fixture", sub: "replacement" } });
    const auth = f.socket.sent.filter(message => message.type === "auth").at(-1)!;
    if (auth.type !== "auth") throw Error("missing replacement auth");
    f.socket.receive({ type: "auth.result", id: auth.id, result: { replica: {
      ...directive, visibilityScope: "ready-replacement-visible", epoch: "ready-replacement-definitions",
    } } });
    release(); await join(f.client);
    await vi.waitFor(() => expect(f.socket.sent.filter(message => message.type === "replica.open")).toHaveLength(2));
    const open = f.socket.sent.filter(message => message.type === "replica.open").at(-1)!;
    if (open.type !== "replica.open") throw Error("missing replacement open");
    let outcome = "pending";
    const result = f.client.reducer({ kind: "reducer", path: "statuses.create" }, {})
      .then(() => { outcome = "success"; }, () => { outcome = "error"; });
    await vi.waitFor(() => expect(f.socket.sent.filter(message => message.type === "reducer.call")).toHaveLength(2));
    const call = f.socket.sent.filter(message => message.type === "reducer.call").at(-1)!;
    if (call.type !== "reducer.call") throw Error("missing replacement reducer");
    f.socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 4 });
    f.socket.receive({ type: "replica.watermark", revision: 4 });
    await join(f.client);
    const rows = [{ id: "replacement-row", name: "Accepted replacement" }];
    const hashes = await replicaRowsHashes(rows, "id"), digest = await replicaHashesDigest(hashes);
    f.socket.receive({ type: "replica.snapshot", id: open.id, path: ref.path, result: rows,
      cursor: { epoch: "ready-replacement-database", revision: 4 }, key: "id", maxRows: 100, hashes, digest, truncated: false });
    f.socket.receive({ type: "replica.ready", id: open.id, path: ref.path,
      cursor: { epoch: "ready-replacement-database", revision: 4 }, digest, truncated: false });
    await join(f.client);
    await new Promise(done => setTimeout(done, 0));
    expect({ outcome, savedCursor: (await f.storage.load("ready-replacement-visible"))?.cursor }).toEqual({
      outcome: "success", savedCursor: { epoch: "ready-replacement-database", revision: 4 },
    });
    await result;
  } finally { release(); }
});

it.each(kinds.flatMap(kind => [{ queued: false, delayed: false }, { queued: true, delayed: false }, { queued: false, delayed: true }, { queued: true, delayed: true }].map(config => ({ ...config, kind }))))("does not certify failed data, %j", async ({ queued, delayed, kind }) => {
  const f = await setup(queued, kind);
  Socket.delayedClose = delayed;
  const apply = vi.spyOn(f.storage, "applyTransaction").mockRejectedValueOnce(Error("injected transaction persistence failure"));
  f.socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Blocked" } }] });
  f.socket.receive({ type: "replica.watermark", revision: 19 });
  await join(f.client);
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(apply).toHaveBeenCalledOnce();
  expect(f.client.localReplica.entity("statuses", "status-2")).toBeUndefined();
  const saved = await f.storage.load(apply.mock.calls[0]![2]);
  expect({
    reportedSuccess: f.outcome() === "success",
    memoryCursor: f.client.localReplica.cursor(),
    savedCursor: saved?.cursor,
    savedRow: saved?.entities.statuses?.["status-2"],
    retainedIntents: queued ? (await f.outbox.load()).length : undefined,
  }).toEqual({
    reportedSuccess: false,
    memoryCursor: { epoch: "e", revision: 18 },
    savedCursor: { epoch: "e", revision: 18 },
    savedRow: undefined,
    retainedIntents: queued ? 1 : undefined,
  });
  expect(f.watch.localReplicaState()).toMatchObject({ computedRevision: 18 });
});

it.each(kinds.flatMap(kind => [false, true].map(knownAcceptance => ({ kind, knownAcceptance }))))("recovers data before retirement, $kind knownAcceptance=$knownAcceptance", async ({ kind, knownAcceptance }) => {
  const f = await setup(true, kind, knownAcceptance);
  const apply = vi.spyOn(f.storage, "applyTransaction").mockRejectedValueOnce(Error("disk unavailable"));
  const transaction: ServerMessage = { type: "replica.transaction", originCommandId: f.commandId, cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Blocked" } }] };
  f.socket.receive(transaction); f.socket.receive({ type: "replica.watermark", revision: 19 });
  await join(f.client); await f.result;
  const retained = await f.outbox.load(); expect(retained).toHaveLength(1);
  expect(retained[0]).toMatchObject(knownAcceptance ? { state: "committed", awaitingReplica: true } : { state: "pending" });
  f.client.connect(); const socket = Socket.instances.at(-1)!;
  expect(socket).not.toBe(f.socket);
  session(socket);
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "replica.open")).toBe(true));
  const open = socket.sent.find(m => m.type === "replica.open")!;
  if (open.type !== "replica.open") throw Error("missing reopen");
  expect(open.cursor).toEqual({ epoch: "e", revision: 18 });
  const hashes = await replicaRowsHashes([{ id: "status-1", name: "Working" }], "id");
  socket.receive({ type: "replica.ready", id: open.id, path: ref.path, cursor: { epoch: "e", revision: 18 }, digest: await replicaHashesDigest(hashes), truncated: false });
  const type = knownAcceptance ? "query.call" : "reducer.call";
  if (!knownAcceptance) {
    await vi.waitFor(() => expect(socket.sent.some(m => m.type === "query.call")).toBe(true), { timeout: 3_000 });
    const probe = socket.sent.find(m => m.type === "query.call")!;
    if (probe.type !== "query.call") throw Error("missing unknown-delivery probe");
    socket.receive({ type: "query.result", id: probe.id, path: probe.path, result: { found: false } });
  }
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === type)).toBe(true), { timeout: 3_000 });
  const call = socket.sent.find(m => m.type === type)!;
  if (call.type === "query.call") {
    expect(call.path).toBe("__gonvex.reducerReceipt");
    expect(call.args).toMatchObject({ idempotencyKey: retained[0]!.idempotencyKey, receiptPath: "statuses.create" });
    socket.receive({ type: "query.result", id: call.id, path: call.path, result: { found: true, cursor: { epoch: "e", revision: 19 } } });
    expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(0);
  } else if (call.type === "reducer.call") {
    expect(call.idempotencyKey).toBe(retained[0]!.idempotencyKey);
    socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 19 });
  } else throw Error("missing replay or receipt reconciliation");
  socket.receive(transaction); await join(f.client);
  expect(f.client.localReplica.entity("statuses", "status-2")).toEqual({ id: "status-2", name: "Blocked" });
  expect(await f.outbox.load()).toHaveLength(1);
  socket.receive({ type: "replica.watermark", revision: 19 }); await join(f.client);
  await vi.waitFor(async () => expect(await f.outbox.load()).toEqual([]));
  const saved = await f.storage.load(apply.mock.calls.at(-1)![2]);
  expect(saved?.cursor).toEqual({ epoch: "e", revision: 19 });
  expect(saved?.entities.statuses?.["status-2"]).toEqual({ id: "status-2", name: "Blocked" });
});

it.each(kinds.flatMap(kind => [false, true].flatMap(laggingReady => [false, true].map(knownAcceptance => ({ kind, laggingReady, knownAcceptance })))))
("holds a recovery watermark until reopened data is durable, $kind laggingReady=$laggingReady knownAcceptance=$knownAcceptance", async ({ kind, laggingReady, knownAcceptance }) => {
  const first = await setup(true, kind, knownAcceptance);
  first.client.close();
  await first.result;
  let retirementBeforeData = false;
  const remove = first.outbox.delete;
  let client: GonvexClient;
  const outbox = { ...first.outbox, delete: async (id: number) => {
    if (!client.localReplica.entity("statuses", "status-2")) retirementBeforeData = true;
    await remove(id);
  } };
  client = new GonvexClient("ws://runtime.test/ws", { errorReporting: false,
    localReplica: { storage: first.storage }, outbox: { store: outbox } });
  clients.push(client);
  const watch = client.watchReplica(ref, {});
  const socket = Socket.instances.at(-1)!;
  session(socket);
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "replica.open")).toBe(true));
  const type = knownAcceptance ? "query.call" : "reducer.call";
  if (!knownAcceptance) {
    await vi.waitFor(() => expect(socket.sent.some(m => m.type === "query.call")).toBe(true), { timeout: 3_000 });
    const probe = socket.sent.find(m => m.type === "query.call")!;
    if (probe.type !== "query.call") throw Error("missing unknown-delivery probe");
    socket.receive({ type: "query.result", id: probe.id, path: probe.path, result: { found: false } });
  }
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === type)).toBe(true));
  const open = socket.sent.find(message => message.type === "replica.open")!;
  const call = socket.sent.find(message => message.type === type)!;
  if (open.type !== "replica.open") throw Error("missing actual SDK open");
  if (call.type === "query.call") {
    expect(call.path).toBe("__gonvex.reducerReceipt");
    socket.receive({ type: "query.result", id: call.id, path: call.path, result: { found: true, cursor: { epoch: "e", revision: 19 } } });
    expect(socket.sent.filter(message => message.type === "reducer.call")).toHaveLength(0);
  } else if (call.type === "reducer.call") {
    socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 19 });
  } else throw Error("missing SDK recovery operation");
  socket.receive({ type: "replica.watermark", revision: 19 });
  await join(client);
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(await first.outbox.load()).toHaveLength(1);
  expect(retirementBeforeData).toBe(false);
  if (laggingReady) {
    const hashes = await replicaRowsHashes([{ id: "status-1", name: "Working" }], "id");
    socket.receive({ type: "replica.ready", id: open.id, path: ref.path, cursor: { epoch: "e", revision: 18 }, digest: await replicaHashesDigest(hashes), truncated: false });
    await join(client);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(await first.outbox.load()).toHaveLength(1);
    expect(retirementBeforeData).toBe(false);
  }
  const rows = [{ id: "status-1", name: "Working" }, { id: "status-2", name: "Recovered" }];
  const hashes = await replicaRowsHashes(rows, "id");
  socket.receive({ type: "replica.delta", id: open.id, path: ref.path, cursor: { epoch: "e", revision: 19 },
    upserts: [rows[1]!], deleted: [], hashes });
  socket.receive({ type: "replica.ready", id: open.id, path: ref.path, cursor: { epoch: "e", revision: 19 }, digest: await replicaHashesDigest(hashes), truncated: false });
  await join(client);
  await vi.waitFor(async () => expect(await first.outbox.load()).toEqual([]));
  expect(retirementBeforeData).toBe(false);
  expect(watch.status().isUpToDate).toBe(true);
  expect((await first.storage.load("visible"))?.entities.statuses?.["status-2"]).toEqual(rows[1]);
});

it.each(kinds)("keeps known backend acceptance out of recovered delivery before replica completion, %s", async kind => {
  const f = await setup(true, kind);
  // setup delivered the real public reducer.result path; only the data
  // watermark remains outstanding. A restarted queue must not send it again.
  await join(f.client);
  await new Promise(resolve => setTimeout(resolve, 0));
  const [entry] = await f.outbox.load();
  expect(entry?.idempotencyKey).toBe(f.commandId);
  const recovered = createReducerOutbox({ store: f.outbox });
  await recovered.loadAll(entry!.scope);
  expect(await recovered.nextReady(entry!.scope, Number.MAX_SAFE_INTEGER)).toBeUndefined();
  expect(f.outcome()).toBe("pending");
  recovered.dispose?.();
});

async function reopenKnownAcceptance(kind: Kind, supportsLookup = true) {
  const first = await setup(true, kind);
  await vi.waitFor(async () => expect(await first.outbox.load()).toMatchObject([{ state: "committed", awaitingReplica: true }]));
  first.client.close(); await first.result;
  const client = new GonvexClient("ws://runtime.test/ws", { errorReporting: false,
    localReplica: { storage: first.storage }, outbox: { store: first.outbox } });
  clients.push(client);
  client.watchReplica(ref, {});
  const socket = Socket.instances.at(-1)!;
  if (supportsLookup) session(socket);
  else {
    socket.open(); socket.receive({ type: "session.ready", capabilities: { replicaWatermark: 1 }, replica: directive });
    const auth = socket.sent.find(m => m.type === "auth");
    if (auth?.type === "auth") socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
  }
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "replica.open")).toBe(true));
  return { first, client, socket };
}

it.each(kinds.flatMap(kind => [
  { found: false },
  { found: true, cursor: { epoch: "old-epoch", revision: 19 } },
  { found: true, cursor: { epoch: "e", revision: -1 } },
  { found: true, cursor: { epoch: "e", revision: 19.5 } },
  { found: true, cursor: { epoch: "e", revision: Number.MAX_SAFE_INTEGER + 1 } },
].map(status => ({ kind, status }))))("retains accepted history when its read-only receipt cannot prove the current barrier, $kind $status", async ({ kind, status }) => {
  const { first, client, socket } = await reopenKnownAcceptance(kind);
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "query.call")).toBe(true));
  const call = socket.sent.find(m => m.type === "query.call")!;
  if (call.type !== "query.call") throw Error("missing receipt lookup");
  socket.receive({ type: "query.result", id: call.id, path: call.path, result: status });
  socket.receive({ type: "replica.watermark", revision: 19 }); await join(client);
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(await first.outbox.load()).toMatchObject([{ state: "committed", awaitingReplica: true }]);
  expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(0);
});

it.each(kinds)("retains unresolved acceptance on a runtime without read-only receipt recovery, %s", async kind => {
  const { first, client, socket } = await reopenKnownAcceptance(kind, false);
  await join(client); await new Promise(resolve => setTimeout(resolve, 0));
  expect(await first.outbox.load()).toMatchObject([{ state: "committed", awaitingReplica: true }]);
  expect(socket.sent.filter(m => m.type === "query.call" || m.type === "reducer.call")).toHaveLength(0);
});

it.each(kinds)("recovers backend acceptance when its journal transition failed before shutdown, %s", async kind => {
  const first = await setup(true, kind, false);
  const update = first.outbox.update!.bind(first.outbox);
  const fault = vi.spyOn(first.outbox, "update").mockImplementation((id, change) => update(id, entry => {
    const next = change(entry);
    if (next.awaitingReplica) {
      // No committed acceptance bytes exist. A new SDK must recover the
      // backend receipt instead of relying on the old process's memory.
      first.client.close();
      throw Error("injected acceptance journal rollback");
    }
    return next;
  }));
  first.socket.receive({ type: "reducer.result", id: first.commandId, path: "statuses.create",
    result: "accepted", originCommandId: first.commandId, committedRevision: 19 });
  await first.result;
  expect(await first.outbox.load()).toMatchObject([{ state: "inflight" }]);
  fault.mockRestore();
  const client = new GonvexClient("ws://runtime.test/ws", { errorReporting: false,
    localReplica: { storage: first.storage }, outbox: { store: first.outbox } });
  clients.push(client); client.watchReplica(ref, {});
  const socket = Socket.instances.at(-1)!; session(socket);
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "query.call" || m.type === "reducer.call")).toBe(true));
  expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(0);
  const query = socket.sent.find(m => m.type === "query.call")!;
  if (query.type !== "query.call") throw Error("missing receipt recovery after acceptance journal rollback");
  expect(query.path).toBe("__gonvex.reducerReceipt");
  expect(query.args).toMatchObject({ idempotencyKey: first.commandId, receiptPath: "statuses.create" });
  socket.receive({ type: "query.result", id: query.id, path: query.path,
    result: { found: true, cursor: { epoch: "e", revision: 19 } } });
  await vi.waitFor(async () => expect(await first.outbox.load()).toMatchObject([{ state: "committed", awaitingReplica: true }]));
  expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(0);
});

it.each(kinds)("does not close a replacement connection when an old data write fails late, %s", async kind => {
  const f = await setup(false, kind);
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const apply = vi.spyOn(f.storage, "applyTransaction").mockImplementationOnce(async () => { await gate; throw Error("late old-owner failure"); });
  try {
    f.socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 19 }, changes: [] });
    await vi.waitFor(() => expect(apply).toHaveBeenCalledOnce());
    f.socket.close(); f.client.connect(); const replacement = Socket.instances.at(-1)!;
    session(replacement);
    await vi.waitFor(() => expect(replacement.sent.some(m => m.type === "replica.open")).toBe(true));
    release(); await join(f.client);
    expect(replacement.readyState).toBe(Socket.OPEN);
    replacement.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Replayed" } }] });
    await join(f.client);
    expect(apply).toHaveBeenCalledTimes(2);
    expect(f.client.localReplica.entity("statuses", "status-2")).toEqual({ id: "status-2", name: "Replayed" });
  } finally { release(); }
});

async function staged(f: Awaited<ReturnType<typeof setup>>) {
  const path = "statuses.paged";
  f.client.watchReplica({ ...ref, path }, {});
  await vi.waitFor(() => expect(f.socket.sent.some(m => m.type === "replica.open" && m.path === path)).toBe(true));
  const open = f.socket.sent.find(m => m.type === "replica.open" && m.path === path)!;
  if (open.type !== "replica.open") throw Error("missing paged open");
  const rows = [{ id: "status-1", name: "Working" }];
  const hashes = await replicaRowsHashes(rows, "id"), digest = await replicaHashesDigest(hashes);
  const page: SnapshotPage = { type: "replica.snapshotPage", id: open.id, path, snapshotId: "staged", scope: "tenant", visibilityScope: "visible", key: "id", cursor: { epoch: "e", revision: 18 }, mode: "eager", maxRows: 100, sequence: 0, last: false, result: rows, truncated: false };
  f.socket.receive(page); await join(f.client);
  f.socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Deferred" } }] });
  await join(f.client);
  return { ...page, sequence: 1, last: true, result: [], digest };
}

it("fences a failed deferred transaction before a later watermark even when close is delayed", async () => {
  const f = await setup(true);
  const final = await staged(f);
  const apply = vi.spyOn(f.storage, "applyTransaction").mockRejectedValueOnce(Error("deferred write failed"));
  Socket.delayedClose = true;
  f.socket.receive(final); f.socket.receive({ type: "replica.watermark", revision: 19 });
  await join(f.client); await new Promise(resolve => setTimeout(resolve, 0));
  expect(apply).toHaveBeenCalledOnce();
  expect(f.outcome()).not.toBe("success");
  expect(f.client.localReplica.cursor()).toEqual({ epoch: "e", revision: 18 });
  expect((await f.storage.load(apply.mock.calls[0]![2]))?.cursor).toEqual({ epoch: "e", revision: 18 });
  expect(await f.outbox.load()).toHaveLength(1);
});

it("does not let a late deferred write failure close a replacement connection", async () => {
  const f = await setup(false); const final = await staged(f);
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const apply = vi.spyOn(f.storage, "applyTransaction").mockImplementationOnce(async () => { await gate; throw Error("old deferred failure"); });
  try {
    f.socket.receive(final); await vi.waitFor(() => expect(apply).toHaveBeenCalledOnce());
    f.socket.close(); f.client.connect(); const replacement = Socket.instances.at(-1)!;
    session(replacement);
    await vi.waitFor(() => expect(replacement.sent.some(m => m.type === "replica.open")).toBe(true));
    release(); await join(f.client);
    expect(replacement.readyState).toBe(Socket.OPEN);
    replacement.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Replayed" } }] });
    await join(f.client);
    expect(apply).toHaveBeenCalledTimes(2);
    expect(f.client.localReplica.entity("statuses", "status-2")).toEqual({ id: "status-2", name: "Replayed" });
  } finally { release(); }
});

it("still closes the failed stream if a verifying observer throws", async () => {
  const f = await setup(false);
  const unsubscribe = f.client.localReplica.subscribe(() => {
    if (f.client.localReplica.freshness() === "verifying") throw Error("application observer failed");
  });
  vi.spyOn(f.storage, "applyTransaction").mockRejectedValueOnce(Error("write failed"));
  try {
    f.socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Blocked" } }] });
    await expect(join(f.client)).resolves.toBeUndefined();
    expect(f.socket.readyState).toBe(3);
    expect(f.client.localReplica.cursor()).toEqual({ epoch: "e", revision: 18 });
  } finally { unsubscribe(); }
});

it.each(kinds)("keeps an intent when an old data write succeeds while replacement capabilities are unset, %s", async kind => {
  const f = await setup(true, kind);
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const persist = f.storage.applyTransaction.bind(f.storage);
  const apply = vi.spyOn(f.storage, "applyTransaction").mockImplementationOnce(async (...args) => { await gate; return persist(...args); });
  try {
    f.socket.receive({ type: "replica.transaction", originCommandId: f.commandId, cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Saved late" } }] });
    await vi.waitFor(() => expect(apply).toHaveBeenCalledOnce());
    f.socket.close(); await f.result;
    f.client.connect(); const replacement = Socket.instances.at(-1)!;
    expect(replacement.readyState).toBe(Socket.CONNECTING);
    release(); await join(f.client);
    expect(f.client.localReplica.entity("statuses", "status-2")).toEqual({ id: "status-2", name: "Saved late" });
    expect(await f.outbox.load()).toHaveLength(1);
    expect(f.outcome()).toBe("queued");
  } finally { release(); }
});

it.each(kinds)("fresh quiet-cohort watermark retires a global barrier only after storage, %s", async kind => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = storageFor(kind), outbox = createKvOutboxStore(createMemoryGonvexKv());
  const client = new GonvexClient("ws://runtime.test/ws", { errorReporting: false,
    localReplica: { storage }, outbox: { store: outbox } });
  clients.push(client);
  client.watchReplica(ref, { window: "A" });
  client.watchReplica(ref, { window: "B" });
  const socket = Socket.instances.at(-1)!;
  session(socket, false);
  await vi.waitFor(() => expect(socket.sent.filter(m => m.type === "replica.open")).toHaveLength(2));
  const opens = socket.sent.filter(m => m.type === "replica.open");
  const reducer: FunctionReference = { kind: "reducer", path: "quiet.save", offline: { mode: "allowed" } };
  // No dependency metadata: this intent is the global outbox barrier.
  const first = client.reducer(reducer, {}, { offline: "queue" }).catch(error => error);
  await vi.waitFor(() => expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(1));
  const call = socket.sent.find(m => m.type === "reducer.call")!;
  if (call.type !== "reducer.call") throw Error("missing accepted call");
  socket.receive({ type: "reducer.result", id: call.id, result: "accepted", originCommandId: call.id, committedRevision: 259 });
  socket.receive({ type: "replica.watermark", revision: 259 });
  // Queue the independent operation while transport is temporarily unavailable.
  // Restoring transport cannot bypass the already accepted global barrier.
  socket.readyState = Socket.CONNECTING;
  const second = await client.reducer({ ...reducer, path: "independent.create" }, {}, { offline: "queue" });
  expect(second).toMatchObject({ status: "queued" });
  socket.readyState = Socket.OPEN;
  const rows = [{ id: "status-1", name: "Unchanged" }];
  const hashes = await replicaRowsHashes(rows, "id"), digest = await replicaHashesDigest(hashes);
  for (const [index, open] of opens.entries()) {
    if (open.type !== "replica.open") throw Error("missing open");
    const cursor = { epoch: "e", revision: 258 + index };
    socket.receive({ type: "replica.snapshot", id: open.id, result: rows, cursor, key: "id", hashes, digest, truncated: false });
    socket.receive({ type: "replica.ready", id: open.id, cursor, digest, truncated: false });
  }
  await join(client);
  await vi.waitFor(async () => expect(await outbox.load()).toHaveLength(2));
  expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(1);
  // The earlier W259 cannot certify A258. Hold the NEW W259's durable write.
  let enter!: () => void, release!: () => void;
  const entered = new Promise<void>(resolve => { enter = resolve; });
  const held = new Promise<void>(resolve => { release = resolve; });
  const persist = storage.advanceWatermark!.bind(storage);
  vi.spyOn(storage, "advanceWatermark").mockImplementationOnce(async (...args) => {
    enter(); await held; return persist(...args);
  });
  try {
    socket.receive({ type: "replica.watermark", revision: 259 });
    await entered;
    expect(await outbox.load()).toHaveLength(2);
    expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(1);
    release(); await join(client); await first;
    await vi.waitFor(() => expect(socket.sent.filter(m => m.type === "reducer.call")).toHaveLength(2));
    expect(socket.sent.some(m => m.type === "query.call")).toBe(false);
    const next = socket.sent.filter(m => m.type === "reducer.call").at(-1)!;
    if (next.type !== "reducer.call") throw Error("missing independent call");
    socket.receive({ type: "reducer.result", id: next.id, result: "accepted", originCommandId: next.id, committedRevision: 259 });
    await join(client); await second;
    await vi.waitFor(async () => expect(await outbox.load()).toEqual([]));
  } finally { release(); }
});
