import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { reducer, schema } from "@gonvex/module-sdk";
import { LocalReducerRuntime } from "@gonvex/local-runtime";
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv, type FunctionReference } from "./index.js";

class Socket {
  static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
  static all: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.all.push(this); }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]); }
  send(value: string) { this.sent.push(JSON.parse(value)); }
  emit(type: string, event: any = {}) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
  open() { this.readyState = 1; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(value: unknown) { this.emit("message", { data: JSON.stringify(value) }); }
}
const directive = { protocolVersion: 1 as const, scope: "scope", visibilityScope: "visible", epoch: "epoch" };
const identity = { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", permissions: {} } };
const url = "ws://localhost:9999";
const owner = ["identity", url, "project", "tenant", "issuer", "account"].join("\u0000");
const collection: FunctionReference = { kind: "query", path: "__local.tasks", delivery: "replica", replica: { table: "tasks", key: "_id", columns: ["_id", "count", "statusId"] } };
const collectionKey = `${collection.path}\u0000{}\u0000{"delivery":"replica","live":null,"scope":"tenant"}`;
const increment = reducer({
  args: schema.object({ amount: schema.optional(schema.number()) }), result: schema.number(),
  run: async (ctx, args: { amount?: number }) => {
    const [row] = await ctx.db.query<any>('SELECT * FROM "tasks" WHERE "_id" = $1', ["t1"]);
    if (!row) throw new Error("Task not found");
    await ctx.db.update("tasks", "t1", { count: row.count + (args.amount ?? 1) });
    return row.count + (args.amount ?? 1);
  },
});
const ref: FunctionReference = { kind: "reducer", path: "increment", offline: { mode: "allowed" }, localExecution: 1 };
const clients: GonvexClient[] = [];
const hosts: LocalReducerRuntime[] = [];

beforeEach(() => { Socket.all = []; vi.stubGlobal("WebSocket", Socket); });
afterEach(async () => { clients.splice(0).forEach((client) => client.close()); await Promise.all(hosts.splice(0).map((host) => host.close())); vi.unstubAllGlobals(); });

async function fixture() {
  const storage = new MemoryLocalReplicaStorage();
  const kv = createMemoryGonvexKv();
  await storage.saveSession(owner, { directive, identity, artifactHash: "artifact" });
  await storage.replaceSnapshot({ entities: { tasks: { t1: { _id: "t1", count: 0, statusId: "todo" } } },
    liveQueries: { [collectionKey]: { signature: collectionKey, kind: "replica", entity: "tasks", key: "_id", ids: ["t1"], completeness: "complete", source: "server" } },
  }, "visible");
  const create = (store = createKvOutboxStore(kv), clientContract?: { version: number; offlineMaxAgeMs: number | null }) => {
    const host = new LocalReducerRuntime({ tables: { tasks: { _id: "text", count: "bigint", statusId: "text" } }, reducers: { increment }, artifactHash: "artifact" });
    hosts.push(host);
    const client = new GonvexClient(url, {
      clientContract,
      project: "project", tenant: "tenant", identity: { sub: "account", iss: "issuer" },
      localReplica: { storage }, outbox: { store },
      localRuntime: { artifactHash: "artifact", tables: ["tasks"], collections: [collection], create: () => ({
        ready: host.initializeReady(), execute: (...args) => host.execute(...args), replay: (...args) => host.replay(...args), close: () => undefined,
      }) },
    });
    clients.push(client);
    return client;
  };
  return { create, kv, storage };
}
async function connect(client: GonvexClient, watermark = false) {
  client.connect();
  const socket = Socket.all.at(-1)!;
  socket.open();
  if (watermark) socket.receive({ type: "session.ready", capabilities: { replicaWatermark: 1 }, replica: directive });
  await vi.waitFor(() => expect(socket.sent.some((message) => message.type === "auth")).toBe(true));
  const auth = socket.sent.find((message) => message.type === "auth");
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive, localIdentity: identity, artifactHash: "artifact", accountId: "account", tenantId: "tenant" } });
  return socket;
}

describe("SDK-owned local reducer lifecycle", () => {
  it.each([8, 3650, -1])("persists offline edits with unlimited admission after %i days, including clock rollback", async (days) => {
    const { create, kv, storage } = await fixture();
    const saved = (await storage.loadSession(owner))!;
    await storage.saveSession(owner, { ...saved, lastOnlineAtMs: Date.now() - days * 86400000 });
    const client = create(createKvOutboxStore(kv), { version: 1, offlineMaxAgeMs: null });
    expect(await client.reducer(ref, {})).toBe(1);
    expect(client.localReplica.entity("tasks", "t1")?.count).toBe(1);
    const [queued] = await createKvOutboxStore(kv).load();
    expect(queued).toBeDefined();
    client.close();
    const restored = create(createKvOutboxStore(kv), { version: 1, offlineMaxAgeMs: null });
    const socket = await connect(restored);
    await vi.waitFor(() => expect(socket.sent.some(message => message.type === "reducer.call")).toBe(true));
    const sent = socket.sent.find(message => message.type === "reducer.call");
    expect(sent.id).toBe(queued!.idempotencyKey);
    expect(sent.args).toEqual({});
  });

  it("stops new offline edits after the admission window without deleting cached data", async () => {
    const { create, kv, storage } = await fixture();
    const saved = (await storage.loadSession(owner))!;
    await storage.saveSession(owner, { ...saved, lastOnlineAtMs: Date.now() - 8 * 86400000 });
    const client = create(createKvOutboxStore(kv), { version: 1, offlineMaxAgeMs: 7 * 86400000 });
    await expect(client.reducer(ref, {})).rejects.toThrow("Offline editing window expired");
    expect(client.localReplica.entity("tasks", "t1")?.count).toBe(0);
  });

  it("continues local editing within the configured offline window", async () => {
    const { create, kv, storage } = await fixture();
    const saved = (await storage.loadSession(owner))!;
    await storage.saveSession(owner, { ...saved, lastOnlineAtMs: Date.now() - 2 * 86400000 });
    const client = create(createKvOutboxStore(kv), { version: 1, offlineMaxAgeMs: 7 * 86400000 });
    expect(await client.reducer(ref, {})).toBe(1);
    expect((await createKvOutboxStore(kv).load())).toHaveLength(1);
  });
  it("waits for the durable server watermark before sending a dependent intent", async () => {
    const { create, kv } = await fixture();
    const client = create();
    await client.reducer(ref, {});
    await client.reducer(ref, {});
    const socket = await connect(client, true);
    const calls = () => socket.sent.filter(message => message.type === "reducer.call");
    await vi.waitFor(() => expect(calls()).toHaveLength(1));
    const first = calls()[0];
    socket.receive({ type: "reducer.result", id: first.id, path: first.path, result: 1, originCommandId: first.id, committedRevision: 4 });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(calls()).toHaveLength(1);
    socket.receive({ type: "replica.transaction", cursor: { epoch: "epoch", revision: 4 }, originCommandId: first.id,
      changes: [{ entity: "tasks", id: "t1", operation: "update", newValue: { _id: "t1", count: 1, statusId: "todo" } }],
    });
    socket.receive({ type: "replica.watermark", revision: 4 });
    await vi.waitFor(() => expect(calls()).toHaveLength(2), { timeout: 10000 });
    expect(client.localReplica.entity("tasks", "t1")?.count).toBe(2);
    const pending = await createKvOutboxStore(kv).load();
    expect(pending).toHaveLength(1);
    expect(pending[0]?.idempotencyKey).toBe(calls()[1].id);
  });
  it("returns the reducer result offline and stages it without handwritten optimistic metadata", async () => {
    const { create } = await fixture();
    const client = create();
    expect(await client.reducer(ref, {})).toBe(1);
    expect(client.localReplica.entity("tasks", "t1")?.count).toBe(1);
    expect(Socket.all.flatMap((socket) => socket.sent).filter((message) => message.type === "reducer.call")).toEqual([]);
  });

  it("restores pending reducer bodies and their effects after a client restart", async () => {
    const { create } = await fixture();
    const first = create();
    await first.reducer(ref, {});
    await first.reducer(ref, {});
    first.close();
    const restarted = create();
    await vi.waitFor(() => expect(restarted.localReplica.entity("tasks", "t1")?.count).toBe(2), { timeout: 10000 });
    expect(await restarted.reducer(ref, {})).toBe(3);
  });

  it("rolls back a server rejection and re-executes the later dependent intent", async () => {
    const { create } = await fixture();
    const client = create();
    await client.reducer(ref, {});
    await client.reducer(ref, {});
    const rejected = vi.fn(); client.onReducerRejection(rejected);
    const socket = await connect(client);
    await vi.waitFor(() => expect(socket.sent.filter((message) => message.type === "reducer.call")).toHaveLength(1), { timeout: 10000 });
    const call = socket.sent.find((message) => message.type === "reducer.call");
    expect(call.artifactHash).toBe("artifact");
    socket.receive({ type: "reducer.error", id: call.id, error: "Permission revoked" });
    await vi.waitFor(() => expect(client.localReplica.entity("tasks", "t1")?.count).toBe(1), { timeout: 10000 });
    expect(rejected).toHaveBeenCalledWith(expect.objectContaining({ reducerId: call.id, error: "Permission revoked" }));
    await vi.waitFor(() => expect(socket.sent.filter((message) => message.type === "reducer.call")).toHaveLength(2));
  });

  it("keeps offline work when a deployment rejects its reducer artifact", async () => {
    const { create, kv } = await fixture();
    const client = create();
    await client.reducer(ref, {});
    const socket = await connect(client);
    await vi.waitFor(() => expect(socket.sent.some(message => message.type === "reducer.call")).toBe(true));
    const call = socket.sent.find(message => message.type === "reducer.call");
    socket.receive({ type: "reducer.error", id: call.id, error: "STALE_REDUCER_ARTIFACT: backend upgraded" });
    await vi.waitFor(async () => {
      const queue = await createKvOutboxStore(kv).load();
      expect(queue).toHaveLength(1);
      expect(queue[0]).toMatchObject({ idempotencyKey: call.id, state: "pending" });
    });
    expect(socket.readyState).toBe(Socket.CLOSED);
    client.connect();
    expect(Socket.all.at(-1)).toBe(socket);
  });

  it("does not expose edits when durable persistence fails", async () => {
    const { create, kv } = await fixture();
    const store = createKvOutboxStore(kv);
    store.put = async () => { throw new Error("Disk full"); };
    const client = create(store);
    await expect(client.reducer(ref, {})).rejects.toThrow("Disk full");
    expect(client.localReplica.entity("tasks", "t1")?.count).toBe(0);
  });
});

it("serializes simultaneous offline intents against each other's local results", async () => {
  const { create } = await fixture();
  const client = create();
  expect(await Promise.all([client.reducer(ref, {}), client.reducer(ref, {}), client.reducer(ref, {})])).toEqual([1, 2, 3]);
  expect(client.localReplica.entity("tasks", "t1")?.count).toBe(3);
});

it("keeps an ambiguous network attempt durable with the same command ID", async () => {
  const { create } = await fixture();
  const client = create();
  await client.reducer(ref, {});
  const socket = await connect(client);
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === "reducer.call")).toBe(true));
  const firstCall = socket.sent.find(message => message.type === "reducer.call");
  expect(firstCall.intentEntropy).toMatch(/^[0-9a-f]{64}$/);
  client.close();
  const restarted = create();
  await vi.waitFor(() => expect(restarted.localReplica.entity("tasks", "t1")?.count).toBe(1), { timeout: 10000 });
  const reconnected = await connect(restarted);
  await vi.waitFor(() => expect(reconnected.sent.some(message => message.type === "reducer.call")).toBe(true), { timeout: 10000 });
  const retry = reconnected.sent.find(message => message.type === "reducer.call");
  expect(retry.id).toBe(firstCall.id);
  expect(retry.idempotencyKey).toBe(firstCall.idempotencyKey);
  expect(retry.intentEntropy).toBe(firstCall.intentEntropy);
});

it("does not expose a different account's pending edits", async () => {
  const { create } = await fixture();
  const client = create();
  await client.reducer(ref, {});
  client.setAuth({ identity: { sub: "different-account", iss: "issuer" } });
  expect(client.localReplica.entity("tasks", "t1")).toBeUndefined();
  await expect(client.reducer(ref, {})).rejects.toThrow(/authenticated local session/);
});

it("captures arguments at admission before initialization or persistence awaits", async () => {
  const { create } = await fixture(); const client = create();
  const args = { amount: 4 };
  const pending = client.reducer(ref, args);
  args.amount = 99;
  expect(await pending).toBe(4);
  expect(client.localReplica.entity("tasks", "t1")?.count).toBe(4);
});
