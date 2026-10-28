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
function session(socket: Socket) {
  socket.open(); socket.receive({ type: "session.ready", capabilities: { replicaWatermark: 1, replicaSnapshotPages: 1 }, replica: directive });
  const auth = socket.sent.find(m => m.type === "auth");
  if (auth?.type === "auth") socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
}
async function join(client: GonvexClient) {
  // Read-only synchronization with frames delivered through the public wire.
  await (client as unknown as { readonly replicaFrames: Promise<void> }).replicaFrames;
}
async function setup(queued: boolean, kind: Kind = "memory") {
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
  socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 19 });
  await Promise.resolve(); expect(outcome).toBe("pending");
  return { client, storage, outbox, watch, socket, result, outcome: () => outcome, commandId: call.id };
}

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

it.each(kinds)("replays from the durable checkpoint and retires the same intent only after a successful watermark, %s", async kind => {
  const f = await setup(true, kind);
  const apply = vi.spyOn(f.storage, "applyTransaction").mockRejectedValueOnce(Error("disk unavailable"));
  const transaction: ServerMessage = { type: "replica.transaction", originCommandId: f.commandId, cursor: { epoch: "e", revision: 19 }, changes: [{ entity: "statuses", id: "status-2", operation: "insert", newValue: { id: "status-2", name: "Blocked" } }] };
  f.socket.receive(transaction); f.socket.receive({ type: "replica.watermark", revision: 19 });
  await join(f.client); await f.result;
  const retained = await f.outbox.load(); expect(retained).toHaveLength(1);
  expect(retained[0]).toMatchObject({ state: "pending" });
  f.client.connect(); const socket = Socket.instances.at(-1)!;
  expect(socket).not.toBe(f.socket);
  session(socket);
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "replica.open")).toBe(true));
  const open = socket.sent.find(m => m.type === "replica.open")!;
  if (open.type !== "replica.open") throw Error("missing reopen");
  expect(open.cursor).toEqual({ epoch: "e", revision: 18 });
  const hashes = await replicaRowsHashes([{ id: "status-1", name: "Working" }], "id");
  socket.receive({ type: "replica.ready", id: open.id, path: ref.path, cursor: { epoch: "e", revision: 18 }, digest: await replicaHashesDigest(hashes), truncated: false });
  await vi.waitFor(() => expect(socket.sent.some(m => m.type === "reducer.call")).toBe(true), { timeout: 3_000 });
  const call = socket.sent.find(m => m.type === "reducer.call")!;
  if (call.type !== "reducer.call") throw Error("missing replay");
  expect(call.idempotencyKey).toBe(retained[0]!.idempotencyKey);
  socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 19 });
  socket.receive(transaction); await join(f.client);
  expect(f.client.localReplica.entity("statuses", "status-2")).toEqual({ id: "status-2", name: "Blocked" });
  expect(await f.outbox.load()).toHaveLength(1);
  socket.receive({ type: "replica.watermark", revision: 19 }); await join(f.client);
  await vi.waitFor(async () => expect(await f.outbox.load()).toEqual([]));
  const saved = await f.storage.load(apply.mock.calls.at(-1)![2]);
  expect(saved?.cursor).toEqual({ epoch: "e", revision: 19 });
  expect(saved?.entities.statuses?.["status-2"]).toEqual({ id: "status-2", name: "Blocked" });
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
