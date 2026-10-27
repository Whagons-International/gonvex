import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference } from "./index.js";
import { replicaAggregateDigest, replicaRowsHashes } from "./replica-integrity.js";

// Drive public subscriptions through the same fake-WebSocket path as snapshot-pages.test.ts.
class Socket {
  static CONNECTING = 0;
  static OPEN = 1;
  static instances: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.instances.push(this); }
  addEventListener(type: string, handler: (event: any) => void) {
    this.listeners.set(type, [...this.listeners.get(type) ?? [], handler]);
  }
  send(message: string) { this.sent.push(JSON.parse(message)); }
  emit(type: string, event = {}) { for (const handler of this.listeners.get(type) ?? []) handler(event); }
  open() { this.readyState = Socket.OPEN; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(message: unknown) { this.emit("message", { data: JSON.stringify(message) }); }
}

const clients: GonvexClient[] = [];
afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  Socket.instances = [];
});
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "e" };
const reference: FunctionReference = {
  kind: "query", path: "tasks.all", delivery: "replica",
  replica: { table: "tasks", key: "id", columns: ["id", "value"], maxRows: 20_000 },
};
const otherReference: FunctionReference = {
  kind: "query", path: "other.all", delivery: "replica",
  replica: { table: "other", key: "id", columns: ["id", "value"], maxRows: 20_000 },
};
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
function makeClient(storage = new MemoryLocalReplicaStorage()) {
  vi.stubGlobal("WebSocket", Socket);
  const client = new GonvexClient("ws://runtime.test/ws", {
    token: "session", tenant: "tenant", project: "project", errorReporting: false,
    localReplica: { storage }, replicaSubscriptionRetentionMs: 0,
  });
  clients.push(client);
  return client;
}
const turn = () => new Promise<void>(resolve => setTimeout(resolve, 0));
// Queue observation only. All frames and lifecycle operations use public APIs.
async function settled(client: GonvexClient) { await (client as any).replicaFrames; }
function opens(socket: Socket): any[] {
  return socket.sent.flatMap(message => message.type === "replica.open" ? [message]
    : message.type === "replica.openMany" ? message.opens : []);
}
async function authenticate(socket: Socket, capabilities: Record<string, number> = {}) {
  socket.open();
  socket.receive({ type: "session.ready", replica: directive, capabilities });
  await turn();
  const auth = socket.sent.find(message => message.type === "auth");
  expect(auth).toBeDefined();
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
}
async function opened(socket: Socket, count = 1) {
  await vi.waitFor(() => expect(opens(socket)).toHaveLength(count));
  return opens(socket);
}
function snapshot(id: string, value: number, rowId = "a", revision = 10) {
  return { type: "replica.snapshot", id, key: "id", cursor: { epoch: "e", revision }, result: [{ id: rowId, value }] };
}

async function cachedResume(client: GonvexClient, batch: boolean) {
  const capabilities: Record<string, number> = batch ? { replicaBatch: 1, replicaResume: 1 } : { replicaResume: 1 };
  const first = Socket.instances.at(-1)!;
  await authenticate(first, capabilities);
  const [cold] = await opened(first);
  const hashes = await replicaRowsHashes([{ id: "a", value: 1 }], "id");
  const digest = replicaAggregateDigest(hashes);
  first.receive({ ...snapshot(cold.id, 1), hashes, digest });
  first.receive({ type: "replica.ready", id: cold.id, cursor: { epoch: "e", revision: 10 }, digest });
  await settled(client);
  first.close();
  await vi.waitFor(() => expect(Socket.instances).toHaveLength(2));
  const socket = Socket.instances.at(-1)!;
  await authenticate(socket, capabilities);
  const [open] = await opened(socket);
  expect(open).toMatchObject({ cursor: { epoch: "e", revision: 10 }, digest });
  expect(open.fullIntegrity).toBeUndefined();
  expect(open.hashes).toBeUndefined();
  return { socket, open, hashes, digest };
}

it.each([false, true])("drops a queued page after retention-zero unsubscribe and lets another feed progress, replacement=%s", async replacement => {
  const storage = new MemoryLocalReplicaStorage();
  const client = makeClient(storage);
  const observer = vi.fn();
  const unsubscribe = client.subscribeReplica(reference, {}, vi.fn());
  client.subscribeReplica(otherReference, {}, observer);
  const socket = Socket.instances.at(-1)!;
  await authenticate(socket, { replicaSnapshotPages: 1 });
  const requests = await opened(socket, 2);
  const abandoned = requests.find(message => message.path === reference.path)!;
  const other = requests.find(message => message.path === otherReference.path)!;
  socket.receive(snapshot(other.id, 1, "b"));
  await settled(client);

  const entered = deferred();
  const release = deferred();
  const apply = storage.applyWindowDelta.bind(storage);
  vi.spyOn(storage, "applyWindowDelta").mockImplementationOnce(async (...args) => {
    entered.resolve();
    await release.promise;
    return apply(...args);
  });
  socket.receive({ type: "replica.delta", id: other.id, cursor: { epoch: "e", revision: 11 }, upserts: [{ id: "b", value: 2 }], deleted: [] });
  await entered.promise;
  try {
    // This page is routed while its handler is still behind B's storage await.
    // No final page is sent, as when the runtime cancels delivery on replica.close.
    socket.receive({
      type: "replica.snapshotPage", id: abandoned.id, snapshotId: "abandoned", scope: "tenant", visibilityScope: "visible",
      sequence: 0, last: false, key: "id", cursor: { epoch: "e", revision: 10 }, result: [{ id: "orphan", value: 1 }],
      mode: "eager", path: reference.path, maxRows: 20_000, maxBytes: 100_000_000, truncated: false,
    });
    unsubscribe();
    await vi.waitFor(() => expect(socket.sent).toContainEqual({ type: "replica.close", id: abandoned.id }));
    // A fresh owner can reuse the key while the obsolete handler remains queued.
    if (replacement) {
      client.subscribeReplica(reference, {}, vi.fn());
      const next = await opened(socket, 3);
      expect(next.at(-1)!.id).not.toBe(abandoned.id);
    }
  } finally {
    release.resolve();
    await settled(client);
  }
  socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 12 }, changes: [
    { entity: "other", id: "b", operation: "update", newValue: { id: "b", value: 3 } },
  ] });
  socket.receive({ type: "replica.delta", id: other.id, cursor: { epoch: "e", revision: 12 }, upserts: [{ id: "b", value: 3 }], deleted: [] });
  await settled(client);
  expect(client.localReplica.entity("other", "b")).toEqual({ id: "b", value: 3 });
  expect(observer.mock.calls.at(-1)![0]).toMatchObject({ type: "replica.snapshot", result: [{ id: "b", value: 3 }] });
  expect(client.localReplica.entity("tasks", "orphan")).toBeUndefined();
  expect(socket.readyState).toBe(Socket.OPEN);
  expect(Socket.instances).toHaveLength(1);
  // Supplement the public feed assertion with ownership/queue invariants.
  expect((client as any).snapshotPageSubscriptions.size).toBe(0);
  expect((client as any).snapshotDeferredFrames).toEqual([]);
});

it.each([false, true])("does not reopen after a needHashes observer closes the client, batch=%s", async batch => {
  const client = makeClient();
  const observer = vi.fn(message => {
    if (message.type === "replica.syncing" && message.reason === "integrity-reconciling") client.close();
  });
  client.subscribeReplica(reference, {}, observer);
  const { socket, open } = await cachedResume(client, batch);
  socket.receive({ type: "replica.needHashes", id: open.id });
  await settled(client);
  await turn();
  expect(observer).toHaveBeenCalledWith(expect.objectContaining({ type: "replica.syncing", reason: "integrity-reconciling" }));
  expect(socket.readyState).toBe(3);
  expect(Socket.instances).toHaveLength(2);
  expect(opens(socket)).toHaveLength(1);
});

it.each([false, true])("does not notify or reopen when close interrupts reset storage, batch=%s", async batch => {
  const storage = new MemoryLocalReplicaStorage();
  const client = makeClient(storage);
  const observer = vi.fn();
  client.subscribeReplica(reference, {}, observer);
  const socket = Socket.instances.at(-1)!;
  await authenticate(socket, batch ? { replicaBatch: 1 } : {});
  const [open] = await opened(socket);
  socket.receive(snapshot(open.id, 1));
  await settled(client);
  observer.mockClear();
  const entered = deferred();
  const release = deferred();
  const remove = storage.removeWindow.bind(storage);
  vi.spyOn(storage, "removeWindow").mockImplementationOnce(async (...args) => {
    entered.resolve();
    await release.promise;
    return remove(...args);
  });
  socket.receive({ type: "replica.reset", id: open.id, path: reference.path, reason: "integrity-mismatch" });
  await entered.promise;
  let notificationsAtClose = 0;
  try {
    client.close();
    notificationsAtClose = observer.mock.calls.length;
  } finally {
    release.resolve();
    await settled(client);
  }
  await turn();
  expect(observer.mock.calls).toHaveLength(notificationsAtClose);
  expect(socket.readyState).toBe(3);
  expect(Socket.instances).toHaveLength(1);
  expect(opens(socket)).toHaveLength(1);
});

it.each([false, true])("keeps current-owner needHashes and reset recovery working, batch=%s", async batch => {
  const client = makeClient();
  const observer = vi.fn();
  client.subscribeReplica(reference, {}, observer);
  const { socket, open, hashes, digest } = await cachedResume(client, batch);
  socket.receive({ type: "replica.needHashes", id: open.id });
  await settled(client);
  const afterHashes = await opened(socket, 2);
  expect(afterHashes[1]).toMatchObject({ id: open.id, path: reference.path, cursor: open.cursor, keys: ["a"], hashes, digest, fullIntegrity: true });
  socket.receive(snapshot(open.id, 1));
  await settled(client);
  socket.receive({ type: "replica.reset", id: open.id, path: reference.path, reason: "integrity-mismatch" });
  await settled(client);
  const afterReset = await opened(socket, 3);
  expect(afterReset[2]).toMatchObject({ id: open.id, path: reference.path });
  expect(afterReset[2].cursor).toBeUndefined();
  expect(observer).toHaveBeenCalledWith(expect.objectContaining({ type: "replica.reset" }));
  expect(Socket.instances).toHaveLength(2);
});

it("keeps public connect after close available", async () => {
  const client = makeClient();
  client.subscribeReplica(reference, {}, vi.fn());
  const socket = Socket.instances.at(-1)!;
  await authenticate(socket);
  await opened(socket);
  client.close();
  client.connect();
  expect(Socket.instances).toHaveLength(2);
  expect(Socket.instances.at(-1)!.readyState).toBe(Socket.CONNECTING);
  expect(socket.readyState).toBe(3);
});

it("still reconnects an owned subscription after transport loss", async () => {
  const client = makeClient();
  const observer = vi.fn();
  client.subscribeReplica(reference, {}, observer);
  const socket = Socket.instances.at(-1)!;
  await authenticate(socket, { replicaSnapshotPages: 1 });
  const [open] = await opened(socket);
  socket.receive(snapshot(open.id, 1));
  await settled(client);
  socket.close();
  await vi.waitFor(() => expect(Socket.instances).toHaveLength(2));
  const resumed = Socket.instances.at(-1)!;
  // A rollback to an unbatched, unpaged runtime must still work.
  await authenticate(resumed);
  const [reopen] = await opened(resumed);
  socket.receive(snapshot(open.id, 99, "a", 12));
  resumed.receive(snapshot(reopen.id, 2, "a", 11));
  await settled(client);
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 2 });
  expect(observer.mock.calls.at(-1)![0]).toMatchObject({ type: "replica.snapshot", result: [{ id: "a", value: 2 }] });
  expect(resumed.readyState).toBe(Socket.OPEN);
});
