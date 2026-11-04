import { afterEach, expect, it, vi } from "vitest";
import { restoreOmittedNulls, restoreReplicaFrameNulls, type ClientMessage, type JsonValue, type ReplicaOpenRequest, type ServerCapabilities, type ServerMessage } from "@gonvex/protocol";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference, type ReplicaMessage } from "./index.ts";
import { replicaAggregateDigest, replicaRowsHashes } from "./replica-integrity.ts";

// `replicaOmitNulls`: rows arrive without null-valued columns; the SDK restores
// them before hashing, storage and publication, so the application and the
// integrity checks see the rows a runtime without the capability sends.
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
  receive(message: unknown) { this.emit("message", { data: JSON.stringify(message) }); }
}
const clients: GonvexClient[] = [];
const turn = () => new Promise<void>(resolve => setTimeout(resolve, 0));
async function settled(client: GonvexClient) {
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
const columns = ["id", "title", "note", "doneAt", "value"];
const reference: FunctionReference = {
  kind: "query", path: "tasks.all", delivery: "replica",
  replica: { table: "tasks", key: "id", columns, maxRows: 20_000 },
};
const full = (id: string, value: number, note: string | null = null): Record<string, JsonValue> =>
  ({ id, title: `T${id}`, note, doneAt: null, value });
const strip = (row: Record<string, JsonValue>) => Object.fromEntries(Object.entries(row).filter(([, v]) => v !== null));
function opens(socket: Socket): ReplicaOpenRequest[] {
  return socket.sent.flatMap(message => message.type === "replica.open" ? [message]
    : message.type === "replica.openMany" ? message.opens : []);
}
async function setup(options: { omit?: boolean; batch?: boolean } = {}) {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage();
  const client = new GonvexClient("ws://runtime.test/ws", {
    token: "session", tenant: "tenant", project: "project", errorReporting: false,
    localReplica: { storage }, replicaSubscriptionRetentionMs: 0,
    ...(options.omit === false ? { replicaOmitNulls: false } : {}),
  });
  clients.push(client);
  const observer = vi.fn<(message: ReplicaMessage) => void>();
  client.subscribeReplica(reference, {}, observer);
  const socket = Socket.instances.at(-1)!;
  socket.open();
  const capabilities: ServerCapabilities = { replicaResume: 1, replicaOmitNulls: 1, replicaSnapshotPages: 1, ...(options.batch ? { replicaBatch: 1 } : {}) };
  socket.receive({ type: "session.ready", replica: directive, capabilities });
  await turn();
  const auth = socket.sent.find(message => message.type === "auth");
  if (!auth) throw new Error("Missing auth");
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive } });
  await vi.waitFor(() => expect(opens(socket)).toHaveLength(1));
  const signature = client.replicaSignature(reference, {});
  return { client, socket, open: opens(socket)[0]!, observer, signature, auth };
}
const rowsOf = (client: GonvexClient, ids: string[]) => ids.map(id => client.localReplica.entity("tasks", id));

it("advertises replicaOmitNulls by default and not when disabled", async () => {
  const on = await setup();
  expect(on.auth).toMatchObject({ capabilities: { replicaOmitNulls: 1 } });
  const off = await setup({ omit: false });
  expect((off.auth as { capabilities?: Record<string, unknown> }).capabilities?.replicaOmitNulls).toBeUndefined();
});

it("restores omitted nulls of an unpaged snapshot before verifying its digest (no inline hashes)", async () => {
  const f = await setup();
  const rows = [full("a", 1), full("b", 2, "n"), { id: "c", title: null, note: null, doneAt: null, value: null }];
  const digest = replicaAggregateDigest(await replicaRowsHashes(rows, "id"));
  f.socket.receive({ type: "replica.snapshot", id: f.open.id, path: reference.path, key: "id", result: rows.map(strip),
    cursor: { epoch: "e", revision: 10 }, digest, mode: "eager", truncated: false, maxRows: 20_000, columns });
  f.socket.receive({ type: "replica.ready", id: f.open.id, path: reference.path, cursor: { epoch: "e", revision: 10 }, digest, truncated: false, columns });
  await settled(f.client);
  await vi.waitFor(() => expect(f.client.localReplica.getWindow(f.signature)).toMatchObject({ completeness: "complete", cursor: { revision: 10 } }));
  const restored = rowsOf(f.client, ["a", "b", "c"]);
  expect(restored).toEqual(rows);
  // Same key order as the full rows (column order).
  expect(restored.map(row => Object.keys(row ?? {}))).toEqual(rows.map(row => Object.keys(row)));
  expect(f.socket.sent.some(message => message.type === "replica.open" && message.id !== f.open.id)).toBe(false);
  expect(f.observer.mock.calls.some(([message]) => message.type === "replica.reset")).toBe(false);

  // A later feed delta carries no columns; a column that became null is
  // simply absent and is restored as null.
  const next = [full("a", 5, null), full("b", 2, null)];
  const after = replicaAggregateDigest(await replicaRowsHashes([...next, rows[2]!], "id"));
  f.socket.receive({ type: "replica.delta", id: f.open.id, path: reference.path, cursor: { epoch: "e", revision: 11 },
    upserts: next.map(strip), deleted: [], digest: after });
  f.socket.receive({ type: "replica.ready", id: f.open.id, path: reference.path, cursor: { epoch: "e", revision: 11 }, digest: after, truncated: false });
  await settled(f.client);
  await vi.waitFor(() => expect(f.client.localReplica.getWindow(f.signature)?.cursor?.revision).toBe(11));
  expect(rowsOf(f.client, ["a", "b"])).toEqual(next);
  expect(f.observer.mock.calls.some(([message]) => message.type === "replica.reset")).toBe(false);
});

it("restores rows of paged snapshots (columns on the first page) and batch deltas, and merges compact transactions", async () => {
  const f = await setup({ batch: true });
  const rows = [full("a", 1), full("b", 2, "n"), full("c", 3)];
  const digest = replicaAggregateDigest(await replicaRowsHashes(rows, "id"));
  const page = { type: "replica.snapshotPage", id: f.open.id, snapshotId: "s", scope: "tenant", visibilityScope: "visible",
    path: reference.path, key: "id", cursor: { epoch: "e", revision: 10 }, mode: "eager", truncated: false, maxRows: 20_000 };
  f.socket.receive({ ...page, sequence: 0, last: false, result: rows.slice(0, 2).map(strip), columns });
  f.socket.receive({ ...page, sequence: 1, last: true, result: rows.slice(2).map(strip), digest });
  f.socket.receive({ type: "replica.ready", id: f.open.id, path: reference.path, cursor: { epoch: "e", revision: 10 }, digest, truncated: false, columns });
  await settled(f.client);
  await vi.waitFor(() => expect(f.client.localReplica.getWindow(f.signature)).toMatchObject({ completeness: "complete", cursor: { revision: 10 } }));
  expect(rowsOf(f.client, ["a", "b", "c"])).toEqual(rows);

  // An update: `note` became null (explicit, in changedColumns), `doneAt`
  // stayed null (omitted from the transaction, restored in the delta).
  const updated = full("b", 7, null);
  const after = replicaAggregateDigest(await replicaRowsHashes([rows[0]!, updated, rows[2]!], "id"));
  const stream = "~0123456789abcdef";
  f.socket.receive({ type: "replica.batch", tenant: "tenant", class: "00", from: 10, items: [
    { type: "replica.transaction", cursor: { epoch: "e", revision: 11 }, changes: [
      { entity: "tasks", id: "b", operation: "update", newValue: { id: "b", title: "Tb", note: null, value: 7 }, changedColumns: ["note", "value"] },
    ] },
    { type: "replica.stream", id: f.open.id, stream },
    { type: "replica.delta", id: stream, path: reference.path, cursor: { epoch: "e", revision: 11 }, upserts: [strip(updated)], deleted: [] },
    { type: "replica.ready", id: stream, path: reference.path, cursor: { epoch: "e", revision: 11 }, digest: after, truncated: false },
  ], watermark: 11 });
  await settled(f.client);
  await vi.waitFor(() => expect(f.client.localReplica.getWindow(f.signature)?.cursor?.revision).toBe(11));
  const b = f.client.localReplica.entity("tasks", "b");
  expect(b).toEqual(updated);
  expect(Object.keys(b ?? {})).toEqual(columns);
  expect(f.observer.mock.calls.some(([message]) => message.type === "replica.reset")).toBe(false);
});

it("restoreOmittedNulls keeps complete rows, column order and extra members", () => {
  const row = { id: "a", value: 1 };
  expect(restoreOmittedNulls(row, ["id", "value"])).toBe(row);
  expect(restoreOmittedNulls({ value: 1, extra: true, id: "a" }, ["id", "n", "value"])).toEqual({ id: "a", n: null, value: 1, extra: true });
  expect(Object.keys(restoreOmittedNulls({ value: 1, extra: true, id: "a" }, ["id", "n", "value"]) as object)).toEqual(["id", "n", "value", "extra"]);
  expect(restoreOmittedNulls(null, ["id"])).toBe(null);
  const known = new Map<string, readonly string[]>();
  const delta = { type: "replica.delta", id: "s", cursor: { epoch: "e", revision: 1 }, upserts: [{ id: "a" }] } as ServerMessage;
  restoreReplicaFrameNulls(delta, known);
  expect((delta as { upserts: JsonValue[] }).upserts).toEqual([{ id: "a" }]);
  const ready = { type: "replica.ready", id: "s", cursor: { epoch: "e", revision: 1 }, digest: "d", columns: ["id", "x"] } as ServerMessage;
  restoreReplicaFrameNulls(ready, known);
  expect("columns" in ready).toBe(false);
  restoreReplicaFrameNulls(delta, known);
  expect((delta as { upserts: JsonValue[] }).upserts).toEqual([{ id: "a", x: null }]);
});
