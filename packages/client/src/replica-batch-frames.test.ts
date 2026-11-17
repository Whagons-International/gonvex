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
const turn = () => new Promise<void>(resolve => setTimeout(resolve, 0));
async function settled(client: GonvexClient) {
  // A read-only join of frames already delivered through the public wire path.
  await (client as unknown as { readonly replicaFrames: Promise<void> }).replicaFrames;
}
afterEach(async () => {
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
// Delivery v2 (`replicaBatchFrames`): one `replica.batch` per tick carrying
// shared, stream-addressed items must apply exactly like the frames it replaces.
it("advertises replicaBatchFrames and applies a replica.batch like the separate frames", async () => {
  const f = await setup();
  const auth = f.socket.sent.find(message => message.type === "auth");
  expect(auth).toMatchObject({ capabilities: { replicaBatchFrames: 1, replicaNoOldValues: 1 } });
  const next = await image(f.open.id, 2, 11);
  const third = await image(f.open.id, 3, 12);
  const stream = "~0123456789abcdef";
  f.socket.receive({
    type: "replica.batch", tenant: "tenant", class: "00000000000000aa", from: 10,
    items: [
      { type: "replica.transaction", cursor: { epoch: "e", revision: 11 }, changes: [
        { entity: "tasks", id: "a", operation: "update", newValue: { id: "a", value: 2 } },
      ] },
      { type: "replica.stream", id: f.open.id, stream },
      { type: "replica.delta", id: stream, path: reference.path, cursor: { epoch: "e", revision: 11 },
        upserts: [{ id: "a", value: 2 }], deleted: [], digest: next.digest },
      { ...next.ready, id: stream },
      { type: "replica.transaction", cursor: { epoch: "e", revision: 12 }, changes: [
        { entity: "tasks", id: "a", operation: "update", newValue: { id: "a", value: 3 } },
      ] },
      { type: "replica.delta", id: stream, path: reference.path, cursor: { epoch: "e", revision: 12 },
        upserts: [{ id: "a", value: 3 }], deleted: [], digest: third.digest },
      { ...third.ready, id: stream },
    ],
    watermark: 13,
  } as ServerMessage);
  await settled(f.client);
  await vi.waitFor(() => expect(f.client.localReplica.getWindow(f.signature)?.cursor?.revision).toBe(12));
  expect(f.client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 3 });
  expect(f.observer.mock.calls.at(-1)?.[0]).toMatchObject({ type: "replica.ready", id: f.open.id });
  // Items for an unbound stream are ignored, not misrouted.
  f.socket.receive({
    type: "replica.batch", tenant: "tenant", class: "00000000000000aa", from: 13,
    items: [{ type: "replica.delta", id: "~unbound", path: reference.path, cursor: { epoch: "e", revision: 14 },
      upserts: [{ id: "a", value: 9 }], deleted: [], digest: third.digest }],
  } as ServerMessage);
  await settled(f.client);
  expect(f.client.localReplica.entity("tasks", "a")).toEqual({ id: "a", value: 3 });
  expect(f.socket.readyState).toBe(Socket.OPEN);
});
