import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient, type FunctionReference } from "./index";
import { LocalReplica, MemoryLocalReplicaStorage } from "./local-replica";
import { replicaAggregateDigest, replicaHashesDigest, replicaRowsHashes } from "./replica-integrity";

class Socket extends EventTarget {
  static OPEN = 1;
  static instances: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  constructor(_url: string) { super(); Socket.instances.push(this); }
  send(frame: string) { this.sent.push(JSON.parse(frame)); }
  open() { this.readyState = 1; this.dispatchEvent(new Event("open")); }
  receive(frame: unknown) { this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(frame) })); }
  close() { this.readyState = 3; }
}
const clients: GonvexClient[] = [];
function client() {
  vi.stubGlobal("WebSocket", Socket);
  const value = new GonvexClient("ws://runtime.test/ws", { errorReporting: false });
  clients.push(value);
  return value;
}
afterEach(() => { clients.splice(0).forEach(value => value.close()); Socket.instances = []; vi.unstubAllGlobals(); });

it("restores persisted aggregate metadata and upgrades legacy hash-only windows", async () => {
  const storage = new MemoryLocalReplicaStorage();
  const replica = new LocalReplica(storage);
  const rows = [{ id: "a", value: 1 }];
  const hashes = await replicaRowsHashes(rows, "id");
  const cursor = { epoch: "e", revision: 1 };
  await replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows,
    hashes, cursor, completeness: "complete", source: "server" });
  const persisted = (await storage.load())!;
  expect(persisted.liveQueries.tasks?.aggregateDigest).toBe(replicaAggregateDigest(hashes));
  const restored = new LocalReplica(storage);
  await restored.hydrate();
  expect(restored.windowResumeState("tasks")).toEqual({ cursor, digest: replicaAggregateDigest(hashes) });
  delete persisted.liveQueries.tasks!.aggregateDigest;
  const legacy = new LocalReplica({ load: async () => persisted, applyTransaction: async () => {} });
  await legacy.hydrate();
  expect(legacy.windowResumeState("tasks")).toEqual({ cursor, digest: replicaAggregateDigest(hashes) });
});

it("uploads a cached aggregate without copying membership, with legacy and needHashes fallback", async () => {
  const value = client();
  const rows = Array.from({ length: 20_000 }, (_, i) => ({ id: `row-${i}`, value: i }));
  const hashes = await replicaRowsHashes(rows, "id");
  const state = value as any;
  await state.replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows,
    hashes, cursor: { epoch: "e", revision: 1 }, completeness: "complete", source: "server" });
  const subscription = { key: "tasks", id: "r", path: "tasks.list", args: {}, forceFullIntegrity: false };
  const legacy = state.replicaOpenRequest(subscription);
  expect(legacy).toMatchObject({ hashes, keys: rows.map(row => row.id), fullIntegrity: true });
  state.serverCapabilities = { replicaResume: 1 };
  const copying = vi.spyOn(state.replica, "getWindow").mockImplementation(() => { throw Error("must not copy rows or hashes"); });
  const resume = state.replicaOpenRequest(subscription);
  expect(resume).toEqual({ id: "r", path: "tasks.list", args: {}, cursor: { epoch: "e", revision: 1 }, digest: replicaAggregateDigest(hashes) });
  copying.mockRestore();
  state.sendReplicaOpen = vi.fn();
  state.emitReplicaMessage = vi.fn();
  await state.handleReplicaMessage(subscription, { type: "replica.needHashes", id: "r" });
  expect(state.replicaOpenRequest(subscription)).toEqual({ ...legacy, digest: resume.digest });
  const before = Buffer.byteLength(JSON.stringify({ type: "replica.open", ...legacy }));
  const after = Buffer.byteLength(JSON.stringify({ type: "replica.open", ...resume }));
  console.log(`20k resume upload: ${before} -> ${after} bytes`);
  expect(before).toBe(1_797_919);
  expect(after).toBe(179);
  expect(before / after).toBeGreaterThan(8_000);
}, 20_000);

it("sends live query revisions only when negotiated and suppresses them during recovery", () => {
  const state = client() as any;
  const subscription = { id: "q", key: "q", path: "tasks.live", args: {}, listeners: new Set([() => {}]), executionScope: "tenant", lastMessage: { type: "query.result", result: [], windowRevision: "cached" } };
  state.send = vi.fn();
  state.socketGeneration = 1;
  state.sendSubscription(subscription);
  expect(state.send.mock.calls.at(-1)[0].windowRevision).toBeUndefined();
  state.serverCapabilities = { queryResume: 1 };
  state.socketGeneration = 2;
  state.sendSubscription(subscription);
  expect(state.send.mock.calls.at(-1)[0].windowRevision).toBe("cached");
  state.querySubscriptions.set("q", subscription);
  state.serverCapabilities.queryBatch = 1;
  state.socketGeneration = 3;
  state.sendSubscription(subscription);
  state.flushQuerySubscribes();
  expect(state.send.mock.calls.at(-1)[0]).toMatchObject({ type: "query.subscribeMany", subscribes: [{ windowRevision: "cached" }] });
  expect(state.queryResumeRevision({ ...subscription, forceSnapshot: true })).toBeUndefined();
  state.socketGeneration = 2;
  const held = { ...subscription, lastRevision: { epoch: "e", sequence: 1 }, revisionSocketGeneration: 1 };
  expect(state.normalizeSubscriptionMessage(held, { type: "query.progress", id: "q", throughRevision: held.lastRevision })).toBeUndefined();
  expect(held).toMatchObject({ serverSettled: true, revisionSocketGeneration: 2 });
});

it("persists the aggregate of committed delta rows for the next resume", async () => {
  const state = client() as any;
  const rows = [{ id: "a", value: 1 }, { id: "b", value: 2 }];
  const hashes = await replicaRowsHashes(rows, "id");
  await state.replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows,
    hashes, cursor: { epoch: "e", revision: 1 }, completeness: "complete", source: "server" });
  const subscription = { key: "tasks", id: "r", path: "tasks.list", args: {}, entity: "tasks",
    columns: ["id", "value"], retiredEpochs: new Set(), verificationGeneration: 1 };
  state.emitReplicaMessage = vi.fn();
  const upserts = [{ id: "a", value: 3 }, { id: "c", value: 4 }];
  const expected = await replicaRowsHashes(upserts, "id");
  await state.handleReplicaMessage(subscription, { type: "replica.delta", id: "r",
    cursor: { epoch: "e", revision: 2 }, upserts, deleted: ["b"] });
  expect(state.replica.getWindow("tasks")).toMatchObject({ hashes: expected, aggregateDigest: replicaAggregateDigest(expected) });
  state.serverCapabilities = { replicaResume: 1 };
  expect(state.replicaOpenRequest(subscription)).toMatchObject({ cursor: { epoch: "e", revision: 2 }, digest: replicaAggregateDigest(expected) });
  expect(state.replica.committedWindowRows("tasks")).toEqual(upserts);
});

it.each([true, false])("honors the token refresh preservation acknowledgement: %s", async (preserved) => {
  const value = client();
  value.setAuth({ token: "first", identity: { sub: "account", iss: "project" } });
  const ref: FunctionReference = { kind: "query", path: "tasks.list", delivery: "replica",
    replica: { table: "tasks", key: "id", columns: ["id", "value"] } };
  value.watchReplica(ref, {});
  value.subscribeLiveQuery({ kind: "query", path: "tasks.live", delivery: "live",
    live: { entity: "tasks", key: "id", plan: { table: "tasks", key: "id", columns: ["id", "value"] } } }, {}, () => {});
  const socket = Socket.instances.at(-1)!;
  socket.open();
  const directive = { protocolVersion: 1, scope: "project:tenant", visibilityScope: "visible", epoch: "e" };
  socket.receive({ type: "session.ready", capabilities: { replicaResume: 1, preserveSubscriptions: 1 }, replica: directive });
  socket.receive({ type: "auth.result", id: socket.sent.find(frame => frame.type === "auth").id, result: { replica: directive } });
  await vi.waitFor(() => expect(socket.sent.filter(frame => frame.type === "replica.open")).toHaveLength(1));
  const open = socket.sent.find(frame => frame.type === "replica.open");
  const rows = [{ id: "one", value: 1 }];
  const hashes = await replicaRowsHashes(rows, "id");
  const digest = await replicaHashesDigest(hashes);
  socket.receive({ type: "replica.snapshot", id: open.id, result: rows, cursor: { epoch: "e", revision: 1 }, key: "id", hashes, digest });
  socket.receive({ type: "replica.ready", id: open.id, cursor: { epoch: "e", revision: 1 }, digest });
  await vi.waitFor(() => expect((value as any).replicaSubscriptions.values().next().value.isUpToDate).toBe(true));
  const queryOpens = socket.sent.filter(frame => frame.type === "query.subscribe").length;
  value.setAuth({ token: "refreshed", identity: { sub: "account", iss: "project" } });
  const auth = socket.sent.findLast(frame => frame.type === "auth");
  expect(auth.capabilities.preserveSubscriptions).toBe(1);
  expect(auth.capabilities.replicaNoOldValues).toBe(1);
  socket.receive({ type: "auth.result", id: auth.id, result: { replica: directive, ...(preserved ? { subscriptionsPreserved: true } : {}) } });
  await vi.waitFor(() => expect((value as any).authInFlight).toBe(false));
  expect(socket.sent.filter(frame => frame.type === "replica.open")).toHaveLength(preserved ? 1 : 2);
  expect(socket.sent.filter(frame => frame.type === "query.subscribe")).toHaveLength(queryOpens + (preserved ? 0 : 1));
});

it("applies transactions without old images across duplicates, deletes, reload and scope changes", async () => {
  const storage = new MemoryLocalReplicaStorage();
  const replica = new LocalReplica(storage);
  await replica.activateScope("tenant-a");
  await replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id",
    rows: [{ id: "a", title: "Cached", nullable: 1 }, { id: "b" }],
    cursor: { epoch: "e", revision: 1 }, completeness: "complete", source: "server" });
  const transaction = { cursor: { epoch: "e", revision: 3 }, changes: [
    { entity: "tasks", id: "a", operation: "update" as const, newValue: { id: "a", nullable: null } },
    { entity: "tasks", id: "b", operation: "delete" as const },
  ] };
  await replica.applyTransaction(transaction, "tenant-a");
  await replica.applyTransaction(transaction, "tenant-a");
  await replica.applyTransaction({ cursor: { epoch: "e", revision: 2 }, changes: [
    { entity: "tasks", id: "a", operation: "update", newValue: { nullable: 2 } },
    { entity: "tasks", id: "b", operation: "insert", newValue: { id: "b" } },
  ] }, "tenant-a");
  expect(replica.entity("tasks", "a")).toEqual({ id: "a", title: "Cached", nullable: null });
  expect(replica.entity("tasks", "b")).toBeUndefined();
  const restored = new LocalReplica(storage);
  await restored.activateScope("tenant-a");
  expect(restored.entity("tasks", "a")).toEqual(replica.entity("tasks", "a"));
  expect(restored.entity("tasks", "b")).toBeUndefined();
  await restored.activateScope("tenant-b");
  await restored.applyTransaction(transaction, "tenant-a");
  expect(restored.entityRows("tasks")).toEqual([]);
  replica.dispose(); restored.dispose();
});

it("accepts an equal-revision full result during snapshot recovery or reconnect", () => {
  const state = client() as any;
  state.socketGeneration = 2;
  const revision = { epoch: "e", sequence: 1 };
  const message = { type: "query.result", id: "q", result: [], subscriptionRevision: revision, windowRevision: "held" };
  const subscription = { lastRevision: revision, revisionSocketGeneration: 2, forceSnapshot: true };
  expect(state.normalizeSubscriptionMessage(subscription, message)).toBe(message);
  expect(subscription.forceSnapshot).toBe(false);
  expect(state.normalizeSubscriptionMessage(subscription, message)).toBeUndefined();
  subscription.revisionSocketGeneration = 1;
  expect(state.normalizeSubscriptionMessage(subscription, message)).toBe(message);
  const stale = { ...message, subscriptionRevision: { epoch: "e", sequence: 0 } };
  subscription.forceSnapshot = true;
  expect(state.normalizeSubscriptionMessage(subscription, stale)).toBeUndefined();
  expect(subscription.forceSnapshot).toBe(true);
});

it.each(["replica.ready", "replica.delta"])("does not accept %s when reconnect invalidates chunked hashing", async (type) => {
  const state = client() as any;
  const rows = Array.from({ length: 129 }, (_, i) => ({ id: String(i) }));
  const hashes = await replicaRowsHashes(rows, "id");
  const digest = await replicaHashesDigest(hashes);
  await state.replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows,
    cursor: { epoch: "e", revision: 1 }, completeness: "partial", source: "cache", ...(type === "replica.delta" ? { hashes } : {}) });
  const subscription = { key: "tasks", id: "r", path: "tasks.list", retiredEpochs: new Set(),
    verificationGeneration: 1, isUpToDate: false, columns: ["id"] };
  const timer = setTimeout(() => { subscription.verificationGeneration += 1; }, 0);
  await state.handleReplicaMessage(subscription, { type, id: "r", cursor: { epoch: "e", revision: 1 }, digest, upserts: rows });
  clearTimeout(timer);
  expect(subscription.isUpToDate).toBe(false);
  expect(state.replica.getWindow("tasks").source).toBe("cache");
});

it("retries unsent subscription batches after a refresh preserves the existing subscriptions", () => {
  const state = client() as any;
  state.socketGeneration = 1;
  state.hasAuthoritativeReplicaScope = true;
  state.serverCapabilities = { queryBatch: 1, replicaBatch: 1 };
  state.send = vi.fn();
  const query = { id: "q", key: "q", path: "tasks.live", args: {}, listeners: new Set([() => {}]), executionScope: "tenant" };
  const replica = { id: "r", key: "r", path: "tasks.list", args: {}, listeners: new Set([() => {}]), opening: false };
  state.querySubscriptions.set("q", query);
  state.replicaSubscriptions.set("r", replica);
  state.sendSubscription(query);
  state.sendReplicaOpen(replica);
  state.authInFlight = true;
  state.flushQuerySubscribes();
  state.flushReplicaOpens();
  expect(query).toMatchObject({ socketGeneration: undefined });
  expect(replica).toMatchObject({ socketGeneration: undefined, opening: false });
  state.authInFlight = false;
  state.resumeQuerySubscriptions();
  state.sendReplicaOpen(replica);
  state.flushQuerySubscribes();
  state.flushReplicaOpens();
  expect(state.send.mock.calls.map((call: any[]) => call[0].type)).toEqual(["query.subscribeMany", "replica.openMany"]);
});

it("opts cold opens into aggregate verification only on a supporting runtime", () => {
  const state = client() as any;
  const subscription = { key: "empty", id: "r", path: "tasks.list", args: {} };
  expect(state.replicaOpenRequest(subscription).digest).toBeUndefined();
  state.serverCapabilities = { replicaResume: 1 };
  expect(state.replicaOpenRequest(subscription)).toMatchObject({ digest: `xor1:${"00".repeat(32)}` });
});

it("repairs a cursor with missing hashes without repeatedly requesting hashes", async () => {
  const state = client() as any;
  await state.replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id",
    rows: [{ id: "a" }], cursor: { epoch: "e", revision: 1 }, completeness: "partial", source: "cache" });
  const subscription = { key: "tasks", id: "r", path: "tasks.list", args: {} };
  expect(state.replicaOpenRequest(subscription).fullIntegrity).toBeUndefined();
  state.serverCapabilities = { replicaResume: 1 };
  expect(state.replicaOpenRequest(subscription)).toMatchObject({ keys: ["a"], fullIntegrity: true });
  expect(state.replicaOpenRequest({ ...subscription, forceFullIntegrity: true })).toMatchObject({ keys: ["a"], fullIntegrity: true });
});

it("verifies an aggregate ready after a resumed delta without rehashing the held rows", async () => {
  const state = client() as any;
  const rows = [{ id: "a", value: 1 }];
  const hashes = await replicaRowsHashes(rows, "id");
  await state.replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows,
    hashes, cursor: { epoch: "e", revision: 1 }, completeness: "complete", source: "server" });
  const subscription = { key: "tasks", id: "r", path: "tasks.list", args: {}, entity: "tasks",
    columns: ["id", "value"], retiredEpochs: new Set(), verificationGeneration: 1, forceFullIntegrity: true };
  state.emitReplicaMessage = vi.fn();
  const upserts = [{ id: "a", value: 2 }];
  const nextHashes = await replicaRowsHashes(upserts, "id");
  const cursor = { epoch: "e", revision: 2 };
  await state.handleReplicaMessage(subscription, { type: "replica.delta", id: "r", cursor, upserts, deleted: [] });
  const committedRows = vi.spyOn(state.replica, "committedWindowRows").mockImplementation(() => { throw Error("must use cached integrity"); });
  await state.handleReplicaMessage(subscription, { type: "replica.ready", id: "r", cursor, digest: replicaAggregateDigest(nextHashes) });
  expect(subscription).toMatchObject({ isUpToDate: true, forceFullIntegrity: false });
  committedRows.mockRestore();
});
