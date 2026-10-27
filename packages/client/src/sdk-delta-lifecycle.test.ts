import { afterEach, expect, it, vi } from "vitest";
import type { ClientMessage, ReplicaOpenRequest, ServerCapabilities, ServerMessage } from "@gonvex/protocol";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference, type ReplicaMessage } from "./index.ts";
import { replicaAggregateDigest, replicaRowsHashes } from "./replica-integrity.ts";

// Only the frame-lane promise is observed privately, for joining.
// No private handler calls, map edits, mutable LocalReplica access or stager injection.
class Socket {
  static CONNECTING = 0;
  static OPEN = 1;
  static instances: Socket[] = [];
  readyState = Socket.CONNECTING;
  sent: ClientMessage[] = [];
  private listeners = new Map<string, Array<(event: { data?: string }) => void>>();
  constructor(readonly url: string) { Socket.instances.push(this); }
  addEventListener(type: string, handler: (event: { data?: string }) => void) {
    this.listeners.set(type, [...this.listeners.get(type) ?? [], handler]);
  }
  send(message: string) { this.sent.push(JSON.parse(message) as ClientMessage); }
  private emit(type: string, event: { data?: string } = {}) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
  open() { this.readyState = Socket.OPEN; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(message: ServerMessage) { this.emit("message", { data: JSON.stringify(message) }); }
}
const clients: GonvexClient[] = [];
const releases: Array<() => void> = [];
const turn = () => new Promise<void>(resolve => setTimeout(resolve, 0));
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
async function settled(client: GonvexClient) {
  // A read-only join of frames already delivered through the public wire path.
  await (client as unknown as { readonly replicaFrames: Promise<void> }).replicaFrames;
}
afterEach(async () => {
  for (const release of releases.splice(0)) release();
  for (const client of clients) client.close();
  await Promise.all(clients.splice(0).map(settled));
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  Socket.instances = [];
});
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "e" };
const reference: FunctionReference = {
  kind: "query", path: "tasks.all", delivery: "replica",
  replica: { table: "tasks", key: "id", columns: ["id", "value"], maxRows: 20_000 },
};
function opens(socket: Socket): ReplicaOpenRequest[] {
  return socket.sent.flatMap(message => message.type === "replica.open" ? [message]
    : message.type === "replica.openMany" ? message.opens : []);
}
async function opened(socket: Socket, count = 1) {
  await vi.waitFor(() => expect(opens(socket)).toHaveLength(count));
  return opens(socket);
}
async function authenticate(socket: Socket, batch: boolean) {
  socket.open();
  const capabilities: ServerCapabilities = batch ? { replicaResume: 1, replicaBatch: 1 } : { replicaResume: 1 };
  socket.receive({ type: "session.ready", replica: directive, capabilities });
  await turn();
  const auth = socket.sent.find(message => message.type === "auth");
  if (!auth) throw new Error("Missing public auth request");
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
}
async function image(id: string, value: number, revision: number) {
  const result = [{ id: "a", value }];
  const hashes = await replicaRowsHashes(result, "id");
  const digest = replicaAggregateDigest(hashes);
  const snapshot: Extract<ServerMessage, { type: "replica.snapshot" }> = {
    type: "replica.snapshot", id, path: reference.path, key: "id", result,
    cursor: { epoch: "e", revision }, hashes, digest, mode: "eager", truncated: false,
    maxRows: 20_000,
  };
  const ready: Extract<ServerMessage, { type: "replica.ready" }> = {
    type: "replica.ready", id, path: reference.path, cursor: snapshot.cursor, digest, truncated: false,
  };
  return { snapshot, ready, digest };
}
async function setup(batch = false, retentionMs = 0) {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage();
  const client = new GonvexClient("ws://runtime.test/ws", {
    token: "session", tenant: "tenant", project: "project", errorReporting: false,
    localReplica: { storage }, replicaSubscriptionRetentionMs: retentionMs,
  });
  clients.push(client);
  const observer = vi.fn<(message: ReplicaMessage) => void>();
  const unsubscribe = client.subscribeReplica(reference, {}, observer);
  const socket = Socket.instances.at(-1)!;
  await authenticate(socket, batch);
  const [open] = await opened(socket);
  if (!open) throw new Error("Missing public replica.open");
  const seed = await image(open.id, 1, 10);
  socket.receive(seed.snapshot);
  socket.receive(seed.ready);
  await settled(client);
  const signature = client.replicaSignature(reference, {});
  expect(client.localReplica.getWindow(signature)).toMatchObject({ cursor: { epoch: "e", revision: 10 }, completeness: "complete" });
  expect(client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 1 });
  expect(observer.mock.calls.at(-1)?.[0].type).toBe("replica.ready");
  observer.mockClear();
  return { storage, client, observer, unsubscribe, socket, open, signature };
}
async function gatedDelta(f: Awaited<ReturnType<typeof setup>>, committedBeforeHold = false) {
  const next = await image(f.open.id, 2, 11);
  const release = deferred();
  releases.push(release.resolve);
  let entered = false;
  let finished = false;
  const apply = f.storage.applyWindowDelta.bind(f.storage);
  vi.spyOn(f.storage, "applyWindowDelta").mockImplementationOnce(async (...args) => {
    if (committedBeforeHold) await apply(...args);
    entered = true;
    await release.promise;
    if (!committedBeforeHold) await apply(...args);
    finished = true;
  });
  // Runtime live updates insert a normalized transaction before delta/ready.
  // replica_delta emits digest, truncated and an empty originCommandIds array.
  f.socket.receive({ type: "replica.transaction", cursor: { epoch: "e", revision: 11 }, changes: [
    { entity: "tasks", id: "a", operation: "update", newValue: { id: "a", value: 2 } },
  ] });
  const delta: Extract<ServerMessage, { type: "replica.delta" }> & { truncated: boolean } = {
    type: "replica.delta", id: f.open.id, path: reference.path, cursor: { epoch: "e", revision: 11 },
    upserts: [{ id: "a", value: 2 }], deleted: [], originCommandIds: [], digest: next.digest, truncated: false,
  };
  f.socket.receive(delta);
  f.socket.receive(next.ready);
  await vi.waitFor(() => expect(entered).toBe(true));
  expect(f.client.localReplica.getWindow(f.signature)?.cursor?.revision).toBe(10);
  expect(observerSnapshots(f.observer.mock.calls)).toEqual([]);
  return { release: release.resolve, finished: () => finished };
}
function observerSnapshots(calls: Array<[ReplicaMessage]>) {
  return calls.map(([message]) => message).filter(message => message.type === "replica.snapshot");
}
async function durableDelta(f: Awaited<ReturnType<typeof setup>>) {
  const saved = await f.storage.load("visible");
  expect(saved?.entities.tasks?.a).toEqual({ id: "a", value: 2 });
  expect(saved?.liveQueries[f.signature]?.cursor).toEqual({ epoch: "e", revision: 11 });
}

it.each([
  { batch: false, reconnect: false }, { batch: true, reconnect: false },
  { batch: false, reconnect: true }, { batch: true, reconnect: true },
])("fences an active delta after close while allowing its durable write, %j", async ({ batch, reconnect }) => {
  const f = await setup(batch);
  const gate = await gatedDelta(f);
  const localObserver = vi.fn();
  f.client.localReplica.subscribe(localObserver);
  let atClose = 0;
  let localAtClose = 0;
  try {
    // close is void. It disposes now, and does not join an active storage write.
    expect(f.client.close()).toBeUndefined();
    expect(gate.finished()).toBe(false);
    atClose = f.observer.mock.calls.length;
    localAtClose = localObserver.mock.calls.length;
    if (reconnect) f.client.connect();
  } finally {
    gate.release();
    await settled(f.client);
  }
  await turn();
  expect(gate.finished()).toBe(true);
  await durableDelta(f);
  expect(f.observer.mock.calls).toHaveLength(atClose);
  expect(localObserver.mock.calls).toHaveLength(localAtClose);
  expect(f.client.localReplica.entity("tasks", "a")).toBeUndefined();
  expect(f.socket.readyState).toBe(3);
  expect(opens(f.socket)).toHaveLength(1);
  expect(Socket.instances).toHaveLength(reconnect ? 2 : 1);
  if (reconnect) {
    // Preserve transport construction only. Reusing disposed LocalReplica is not promised.
    expect(Socket.instances[1]?.readyState).toBe(Socket.CONNECTING);
    expect(Socket.instances[1]?.sent).toEqual([]);
  }
});

it.each([false, true])("publishes a successful current-owner delta after persistence, batch=%s", async batch => {
  const f = await setup(batch);
  const gate = await gatedDelta(f);
  try { expect(gate.finished()).toBe(false); }
  finally { gate.release(); await settled(f.client); }
  await durableDelta(f);
  expect(observerSnapshots(f.observer.mock.calls)).toHaveLength(1);
  expect(observerSnapshots(f.observer.mock.calls)[0]).toMatchObject({ cursor: { epoch: "e", revision: 11 }, result: [{ id: "a", value: 2 }] });
  expect(f.observer.mock.calls.at(-1)?.[0].type).toBe("replica.ready");
  expect(f.client.localReplica.getWindow(f.signature)?.cursor?.revision).toBe(11);
  expect(f.socket.readyState).toBe(Socket.OPEN);
  expect(Socket.instances).toHaveLength(1);
});

it.each([false, true])("publishes an admitted committed delta after socket loss, reconnect=%s", async reconnect => {
  const f = await setup();
  const gate = await gatedDelta(f, true);
  // Storage committed before transport loss; only its returned promise is held.
  await durableDelta(f);
  try {
    f.socket.close();
    if (reconnect) {
      f.client.connect();
      expect(Socket.instances).toHaveLength(2);
      expect(Socket.instances[1]!.readyState).toBe(Socket.CONNECTING);
    }
  } finally { gate.release(); await settled(f.client); }
  expect(observerSnapshots(f.observer.mock.calls)).toHaveLength(1);
  expect(observerSnapshots(f.observer.mock.calls)[0]).toMatchObject({ cursor: { epoch: "e", revision: 11 }, result: [{ id: "a", value: 2 }] });
  expect(f.client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 2 });
  if (reconnect) {
    const socket = Socket.instances[1]!;
    await authenticate(socket, false);
    const [open] = await opened(socket);
    if (!open) throw new Error("Missing reopen");
    expect(open.cursor).toEqual({ epoch: "e", revision: 11 });
    // An unchanged resume can send only ready, with no replacement snapshot.
    socket.receive((await image(open.id, 2, 11)).ready);
    await settled(f.client);
    expect(observerSnapshots(f.observer.mock.calls)).toHaveLength(1);
    expect(f.observer.mock.calls.at(-1)?.[0]).toMatchObject({ type: "replica.ready", cursor: { epoch: "e", revision: 11 } });
  }
});

it("allows the old durable write after expiry without notifying a replacement owner", async () => {
  const f = await setup();
  const gate = await gatedDelta(f);
  const replacement = vi.fn<(message: ReplicaMessage) => void>();
  let nextOpen: ReplicaOpenRequest | undefined;
  let atReplacement = 0;
  try {
    f.unsubscribe();
    await vi.waitFor(() => expect(f.socket.sent).toContainEqual({ type: "replica.close", id: f.open.id }));
    f.client.subscribeReplica(reference, {}, replacement);
    nextOpen = (await opened(f.socket, 2))[1];
    expect(nextOpen?.id).not.toBe(f.open.id);
    await turn(); // Join the replacement's legitimate cache replay at revision 10.
    atReplacement = replacement.mock.calls.length;
  } finally { gate.release(); await settled(f.client); }
  await durableDelta(f);
  expect(f.observer).not.toHaveBeenCalled();
  expect(replacement.mock.calls).toHaveLength(atReplacement);
  expect(opens(f.socket)).toHaveLength(2);
  if (!nextOpen) throw new Error("Missing replacement open");
  const next = await image(nextOpen.id, 3, 12);
  f.socket.receive(next.snapshot); f.socket.receive(next.ready);
  await settled(f.client);
  expect(replacement.mock.calls.at(-1)?.[0].type).toBe("replica.ready");
  expect(f.client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 3 });
});

it("keeps a retained owner live when a listener returns before expiry", async () => {
  const f = await setup();
  const gate = await gatedDelta(f);
  const returned = vi.fn<(message: ReplicaMessage) => void>();
  try {
    f.unsubscribe();
    f.client.subscribeReplica(reference, {}, returned);
    await turn();
    returned.mockClear();
  } finally { gate.release(); await settled(f.client); }
  await durableDelta(f);
  expect(observerSnapshots(returned.mock.calls)).toHaveLength(1);
  expect(observerSnapshots(returned.mock.calls)[0]).toMatchObject({ cursor: { epoch: "e", revision: 11 }, result: [{ id: "a", value: 2 }] });
  expect(opens(f.socket)).toHaveLength(1);
  expect(f.socket.sent.filter(message => message.type === "replica.close")).toEqual([]);
});

it("records committed replay while a retained owner temporarily has no listeners", async () => {
  const f = await setup(false, 60_000);
  const gate = await gatedDelta(f);
  f.unsubscribe();
  gate.release();
  await settled(f.client);
  await durableDelta(f);
  const returned = vi.fn<(message: ReplicaMessage) => void>();
  f.client.subscribeReplica(reference, {}, returned);
  await turn();
  expect(returned.mock.calls).toContainEqual([expect.objectContaining({ type: "replica.snapshot", cursor: { epoch: "e", revision: 11 }, result: [{ id: "a", value: 2 }] })]);
  expect(opens(f.socket)).toHaveLength(1);
});

it("does not restore old-scope replay metadata after an active old-scope write", async () => {
  const f = await setup();
  const gate = await gatedDelta(f);
  const nextDirective = { ...directive, scope: "tenant-2", visibilityScope: "visible-2" };
  try {
    f.client.setAuth({ tenant: "tenant-2", token: "session-2" });
    const auths = f.socket.sent.filter(message => message.type === "auth");
    const auth = auths.at(-1);
    if (!auth || auths.length < 2) throw new Error("Missing scope-change auth request");
    f.socket.receive({ type: "auth.result", id: auth.id, result: { replica: nextDirective } });
    // Auth rotates synchronously; activation waits behind the real storage write.
    expect(f.client.localReplica.entity("tasks", "a")).toBeUndefined();
  } finally { gate.release(); await settled(f.client); }
  await durableDelta(f);
  const nextOpen = (await opened(f.socket, 2))[1];
  if (!nextOpen) throw new Error("Missing new-scope open");
  expect(nextOpen.id).not.toBe(f.open.id);
  // Immediate old-scope emit is already blocked on the base. A NEW listener
  // detects whether the old continuation silently restored lastMessage instead.
  const fresh = vi.fn<(message: ReplicaMessage) => void>();
  f.client.subscribeReplica(reference, {}, fresh);
  await turn();
  expect(f.observer).not.toHaveBeenCalled();
  expect(fresh).not.toHaveBeenCalled();
  expect(f.client.localReplica.entity("tasks", "a")).toBeUndefined();
  const next = await image(nextOpen.id, 3, 12);
  f.socket.receive(next.snapshot); f.socket.receive(next.ready);
  await settled(f.client);
  expect(fresh.mock.calls).toContainEqual([expect.objectContaining({ type: "replica.snapshot", cursor: { epoch: "e", revision: 12 }, result: [{ id: "a", value: 3 }] })]);
  expect(f.client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 3 });
  expect(Socket.instances).toHaveLength(1);
});
