import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GonvexClient, GonvexClientError, MemoryLocalReplicaStorage, control, type FunctionReference } from "./index";
import { replicaHashesDigest, replicaRowsHashes } from "./replica-integrity";

const captureReportedError = vi.hoisted(() => vi.fn());
vi.mock("./error-reporter.js", () => ({
  GonvexErrorReporter: class {
    captureException = captureReportedError;
    close() {}
    setTenant() {}
    setProject() {}
  },
}));

type Listener = (event: { data?: string }) => void;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  static instances: FakeWebSocket[] = [];

  readonly sent: string[] = [];
  readyState = FakeWebSocket.CONNECTING;
  private readonly listeners = new Map<string, Array<{ listener: Listener; once: boolean }>>();

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: Listener, options?: { once?: boolean }) {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push({ listener, once: Boolean(options?.once) });
    this.listeners.set(type, listeners);
  }

  send(message: string) {
    this.sent.push(message);
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  disconnect() {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit("close", {});
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  receive(message: unknown) {
    this.emit("message", { data: typeof message === "string" ? message : JSON.stringify(message) });
  }

  private emit(type: string, event: { data?: string }) {
    const listeners = this.listeners.get(type) ?? [];
    this.listeners.set(type, listeners.filter((entry) => !entry.once));
    for (const entry of listeners) entry.listener(event);
  }
}

const ref: FunctionReference = {
  kind: "query",
  path: "tasks.list",
  delivery: "live",
  live: { entity: "tasks", key: "id", resultPath: [], plan: { table: "tasks", key: "id", columns: ["id"] } },
};
const testReplicaDirective = {
  protocolVersion: 1 as const,
  scope: "project:test:tenant:test",
  visibilityScope: "visibility:test",
  epoch: "epoch:test",
};

function authenticatedResult(result: Record<string, unknown>) {
  return { ...result, replica: testReplicaDirective };
}

async function flushMicrotasks() {
  for (let index = 0; index < 12; index += 1) await Promise.resolve();
}

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
  vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
  captureReportedError.mockClear();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

function latestSocket() {
  const socket = FakeWebSocket.instances.at(-1);
  if (!socket) throw new Error("expected WebSocket instance");
  return socket;
}

function sentMessages(socket = latestSocket()) {
  return socket.sent.map((message) => JSON.parse(message));
}

describe("GonvexClient", () => {
	function installBrowserOnlineEvents() {
		const listeners = new Set<() => void>();
		vi.stubGlobal("addEventListener", (type: string, listener: () => void) => {
			if (type === "online") listeners.add(listener);
		});
		vi.stubGlobal("removeEventListener", (type: string, listener: () => void) => {
			if (type === "online") listeners.delete(listener);
		});
		return () => { for (const listener of listeners) listener(); };
	}

	it("admits offline reducers without sending on an open stale socket and replays once online", async () => {
		const emitOnline = installBrowserOnlineEvents();
		vi.stubGlobal("navigator", { onLine: false });
		const client = new GonvexClient("ws://runtime.test/ws", { outbox: { enabled: false } });
		client.connect();
		const socket = latestSocket();
		socket.open();
		await flushMicrotasks();
		const reducerRef: FunctionReference = {
			kind: "reducer",
			path: "tasks.update",
			offline: { mode: "allowed", conflict: "merge" },
			optimistic: {
				transaction: {
					effects: [{
						operation: "upsert",
						entity: "tasks",
						id: ["id"],
						value: { id: { $arg: "id" }, title: { $arg: "title" } },
					}],
				},
			},
		};

		await expect(client.reducer(reducerRef, { id: "task-a", title: "Offline" })).resolves.toMatchObject({ status: "queued" });
		expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(0);
		expect(client.localReplica.entity("tasks", "task-a")).toMatchObject({ title: "Offline" });
		expect(await client.outboxCount()).toBe(1);

		vi.stubGlobal("navigator", { onLine: true });
		emitOnline();
		await vi.waitFor(() => expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(1));
		const call = sentMessages(socket).find((message) => message.type === "reducer.call")!;
		socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "task-a" });
		await vi.waitFor(() => expect(client.outboxCount()).resolves.toBe(0));
		expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(1);
		client.close();
	});

	it("queues an offline reducer before opening a socket when the current socket is closed", async () => {
		installBrowserOnlineEvents();
		vi.stubGlobal("navigator", { onLine: false });
		const client = new GonvexClient("ws://runtime.test/ws", { outbox: { enabled: false } });
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.close();
		const reducerRef: FunctionReference = {
			kind: "reducer",
			path: "tasks.update",
			offline: { mode: "allowed" },
			optimistic: { transaction: { effects: [{ operation: "upsert", entity: "tasks", id: ["id"], value: { id: { $arg: "id" }, title: { $arg: "title" } } }] } },
		};
		await expect(client.reducer(reducerRef, { id: "task-a", title: "Offline" })).resolves.toMatchObject({ status: "queued" });
		expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(0);
		expect(await client.outboxCount()).toBe(1);
		client.close();
	});

	it("wakes a queued reducer after same-scope reauthentication succeeds", async () => {
		vi.stubGlobal("navigator", { onLine: true });
		const client = new GonvexClient("ws://runtime.test/ws", {
			project: "shop", tenant: "tenant-a", token: "initial-token", outbox: { enabled: false },
		});
		client.connect();
		const socket = latestSocket();
		socket.open();
		const initialAuth = sentMessages(socket).find((message) => message.type === "auth")!;
		socket.receive({ type: "auth.result", id: initialAuth.id, result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }) });
		await flushMicrotasks();

		client.setAuth({ token: "rotated-token" });
		const rotatingAuth = sentMessages(socket).filter((message) => message.type === "auth").at(-1)!;
		const reducerRef: FunctionReference = {
			kind: "reducer", path: "priorities.create", offline: { mode: "allowed" },
			optimistic: {
				transaction: {
					effects: [{ operation: "upsert", entity: "priorities", id: ["id"], value: { id: { $arg: "id" }, name: { $arg: "name" } } }],
				},
			},
		};
		const queued = await client.reducer(reducerRef, { id: "offline-priority", name: "Offline" });
		expect(queued).toMatchObject({ status: "queued" });
		expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(0);

		socket.receive({ type: "auth.result", id: rotatingAuth.id, result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }) });
		await vi.waitFor(() => expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(1));
		const replay = sentMessages(socket).find((message) => message.type === "reducer.call")!;
		socket.receive({ type: "reducer.result", id: replay.id, path: replay.path, result: "offline-priority", originCommandId: (queued as { reducerId: string }).reducerId, committedRevision: 2 });
		socket.receive({
			type: "replica.transaction", cursor: { epoch: "epoch:test", revision: 2 },
			originCommandId: (queued as { reducerId: string }).reducerId,
			changes: [{ entity: "priorities", id: "offline-priority", operation: "insert", newValue: { id: "offline-priority", name: "Offline" } }],
		});
		await vi.waitFor(() => expect(client.localReplica.hasPendingCommand((queued as { reducerId: string }).reducerId)).toBe(false));
		client.close();
	});

	it("tracks the active artifact hash across authentication and hot reload without reconnecting", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.setAuth({ project: "shop", tenant: "tenant-a", token: "session" });
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
		const auth = sentMessages(socket).find((message) => message.type === "auth");
		socket.receive({ type: "auth.result", id: auth.id, result: authenticatedResult({ artifactHash: "artifact-1" }) });
		await flushMicrotasks();
		expect(client.activeArtifactHash()).toBe("artifact-1");

		socket.receive({ type: "system.reload", reason: "module generation changed", artifactHash: "artifact-2" });
		await flushMicrotasks();
		expect(client.activeArtifactHash()).toBe("artifact-2");
		expect(FakeWebSocket.instances).toHaveLength(1);
	});

	it("subscribes and resumes live Control Plane Queries on the persistent connection", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.setAuth({project:"shop",tenant:"tenant-a",token:"session"});
		const socket=(client.connect(),latestSocket());socket.open();socket.receive({type:"session.ready",capabilities:{queryBatch:0},replica:testReplicaDirective});
		const firstAuth = sentMessages(socket).find((message) => message.type === "auth")!;
		socket.receive({type:"auth.result",id:firstAuth.id,result:authenticatedResult({accountId:"account-a",tenantId:"tenant-a"})});
		await flushMicrotasks();
		const updates=vi.fn();client.subscribeLiveQuery(control.tenants.mine,{},updates);
		await vi.waitFor(() => expect(sentMessages(socket).at(-1)).toMatchObject({type:"query.subscribe",path:"control.tenants.mine",scope:"control"}));
		const subscription=sentMessages(socket).at(-1);socket.receive({type:"query.result",id:subscription.id,path:"control.tenants.mine",result:[],reason:"initial"});await flushMicrotasks();expect(updates).toHaveBeenCalled();
		socket.disconnect();vi.advanceTimersByTime(1_000);const resumed=latestSocket();resumed.open();resumed.receive({type:"session.ready",capabilities:{queryBatch:0},replica:testReplicaDirective});
		const resumedAuth = sentMessages(resumed).find((message) => message.type === "auth")!;
		resumed.receive({type:"auth.result",id:resumedAuth.id,result:authenticatedResult({accountId:"account-a",tenantId:"tenant-a"})});await flushMicrotasks();expect(sentMessages(resumed).some((message)=>message.type==="query.subscribe"&&message.scope==="control")).toBe(true);
	});
	it("batches query subscribes into one frame when the server advertises queryBatch", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const socket = (client.connect(), latestSocket());
		socket.open();
		socket.receive({ type: "session.ready", capabilities: { queryBatch: 1 }, replica: testReplicaDirective });

		client.subscribeLiveQuery(ref, {}, vi.fn());
		client.subscribeLiveQuery({
			kind: "query",
			path: "teams.list",
			delivery: "live",
			live: { entity: "teams", key: "id", resultPath: [], plan: { table: "teams", key: "id", columns: ["id"] } },
		}, {}, vi.fn());
		await vi.advanceTimersByTimeAsync(0);

		const frames = sentMessages(socket);
		expect(frames.filter((frame) => frame.type === "query.subscribe")).toHaveLength(0);
		const batches = frames.filter((frame) => frame.type === "query.subscribeMany");
		expect(batches).toHaveLength(1);
		expect(batches[0].subscribes.map((subscribe: { path: string }) => subscribe.path))
			.toEqual(["tasks.list", "teams.list"]);
	});

	it("fails closed when the runtime omits the authoritative Local Replica scope", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const onAuthError = vi.fn();
		client.onAuthError(onAuthError);
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: {} });

		expect(onAuthError).toHaveBeenCalledWith(
			"Runtime did not provide an authoritative Local Replica visibility scope",
		);
		expect(client.localReplica.freshness()).toBe("verifying");
	});

	it("flushes reducerMany as one frame and settles entries independently", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: { reducerBatch: 1 }, replica: testReplicaDirective });

		const outcome = client.reducerMany([
			{ ref: { kind: "reducer", path: "tasks.create" }, args: { name: "a" } },
			{ ref: { kind: "reducer", path: "tasks.create" }, args: { name: "b" } },
		]);
		await vi.advanceTimersByTimeAsync(0);

		const batches = sentMessages(socket).filter((frame) => frame.type === "reducer.callMany");
		expect(batches).toHaveLength(1);
		expect(batches[0].calls).toHaveLength(2);
		const [first, second] = batches[0].calls;
		socket.receive({ type: "reducer.result", id: first.id, path: first.path, result: "id-a" });
		socket.receive({ type: "reducer.error", id: second.id, path: second.path, error: "boom" });

		const results = await outcome;
		expect(results[0]).toEqual({ status: "ok", result: "id-a" });
		expect(results[1]).toMatchObject({ status: "error" });
		expect((results[1] as { error: GonvexClientError }).error.message).toBe("boom");
	});

	it("falls back to sequential reducers when reducerBatch is not advertised", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });

		const outcome = client.reducerMany([
			{ ref: { kind: "reducer", path: "tasks.create" }, args: { name: "a" } },
			{ ref: { kind: "reducer", path: "tasks.create" }, args: { name: "b" } },
		]);
		await vi.advanceTimersByTimeAsync(0);

		let calls = sentMessages(socket).filter((frame) => frame.type === "reducer.call");
		expect(calls).toHaveLength(1);
		socket.receive({ type: "reducer.result", id: calls[0].id, path: calls[0].path, result: "id-a" });
		await vi.advanceTimersByTimeAsync(0);

		calls = sentMessages(socket).filter((frame) => frame.type === "reducer.call");
		expect(calls).toHaveLength(2);
		socket.receive({ type: "reducer.result", id: calls[1].id, path: calls[1].path, result: "id-b" });

		const results = await outcome;
		expect(results).toEqual([
			{ status: "ok", result: "id-a" },
			{ status: "ok", result: "id-b" },
		]);
	});

	it("sends the outbox idempotency key on replayable reducers and omits it on one-shots", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
		await flushMicrotasks();

		const optimistic = client.reducer(
			{ kind: "reducer", path: "tasks.update" },
			{ id: "a", title: "T" },
			{ optimistic: [{ collection: "tasks.list", rowId: "a", op: "patch", fields: { title: "T" } }] },
		);
		await vi.advanceTimersByTimeAsync(0);
		const [replayable] = sentMessages(socket).filter((frame) => frame.type === "reducer.call");
		expect(replayable.idempotencyKey).toBe(replayable.id);
		socket.receive({ type: "reducer.result", id: replayable.id, path: replayable.path, result: null });
		await optimistic;

		const oneShot = client.reducer({ kind: "reducer", path: "tasks.create" }, { title: "x" });
		await vi.advanceTimersByTimeAsync(0);
		const oneShotCall = sentMessages(socket).filter((frame) => frame.type === "reducer.call").at(-1);
		expect(oneShotCall.idempotencyKey).toBeUndefined();
		socket.receive({ type: "reducer.result", id: oneShotCall.id, path: oneShotCall.path, result: "id-x" });
		await expect(oneShot).resolves.toBe("id-x");
	});

	it("does not replay a new optimistic reducer while its Replica scope is restoring", async () => {
		const client = new GonvexClient("ws://runtime.test/ws", {
			project: "shop",
			tenant: "tenant-a",
			token: "session-token",
			outbox: { enabled: false },
		});
		client.connect();
		const reducer = client.reducer(
			{ kind: "reducer", path: "tasks.update" },
			{ id: "task-a", title: "Updated" },
			{
				offline: "reject",
				optimistic: [{
					collection: "tasks",
					rowId: "task-a",
					op: "patch",
					fields: { title: "Updated" },
				}],
			},
		);
		const socket = latestSocket();
		socket.open();
		const auth = sentMessages(socket).find((message) => message.type === "auth")!;

		// The runtime sends the connection directive before the accepted auth
		// result. Both can activate the same Replica scope while the reducer is
		// already waiting behind authentication.
		socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
		socket.receive({
			type: "auth.result",
			id: auth.id,
			result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }),
		});

		await vi.waitFor(() => {
			expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(1);
		});
		const call = sentMessages(socket).find((message) => message.type === "reducer.call")!;
		socket.receive({
			type: "reducer.result",
			id: call.id,
			path: call.path,
			result: { id: "task-a" },
			originCommandId: call.id,
			committedRevision: 2,
		});

		await expect(reducer).resolves.toEqual({ id: "task-a" });
		await vi.advanceTimersByTimeAsync(0);
		expect(sentMessages(socket).filter((message) => message.type === "reducer.call")).toHaveLength(1);
		client.close();
	});

	it("rejects stale revisions and advances progress without notifying listeners", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeLiveQuery(ref, {}, handler);
		const socket = latestSocket();
		socket.open();
		const [{ id }] = sentMessages(socket);
		socket.receive({ type: "query.result", id, result: [{ id: "a", title: "new" }], subscriptionRevision: { epoch: "runtime-a", sequence: 2 } });
		socket.receive({ type: "query.result", id, result: [{ id: "a", title: "old" }], subscriptionRevision: { epoch: "runtime-a", sequence: 1 } });
		socket.receive({ type: "query.progress", id, throughRevision: { epoch: "runtime-a", sequence: 3 } });
		await flushMicrotasks();
		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler.mock.calls[0][0].result[0].title).toBe("new");
	});

	it("requests an authoritative snapshot when progress arrives without a local result", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeLiveQuery(ref, {}, handler);
		const socket = latestSocket();
		socket.open();
		const [{ id }] = sentMessages(socket);

		socket.receive({
			type: "query.progress",
			id,
			path: ref.path,
			reason: "initial",
			throughRevision: { epoch: "runtime-a", sequence: 1 },
		});

		expect(handler).not.toHaveBeenCalled();
		expect(sentMessages(socket)).toHaveLength(2);
		expect(sentMessages(socket).at(-1)).toEqual({
			type: "query.subscribe",
			id,
			path: ref.path,
			args: {},
		});
	});

	it("applies keyed patches only to the matching base revision", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeLiveQuery(ref, {}, handler);
		const socket = latestSocket();
		socket.open();
		const [{ id }] = sentMessages(socket);
		socket.receive({
			type: "query.result", id,
			result: [{ id: "a", title: "old" }, { id: "b", title: "keep" }],
			subscriptionRevision: { epoch: "runtime-a", sequence: 10 },
		});
		socket.receive({
			type: "query.patch", id,
			baseRevision: { epoch: "runtime-a", sequence: 10 },
			subscriptionRevision: { epoch: "runtime-a", sequence: 11 },
			inserted: [{ id: "c", title: "added" }],
			updated: [{ id: "a", title: "new" }],
			deleted: ["b"],
			order: ["c", "a"],
		});
		await flushMicrotasks();
		expect(handler).toHaveBeenCalledTimes(2);
		expect(handler.mock.calls[1][0]).toMatchObject({
			type: "query.result",
			result: [{ id: "c", title: "added" }, { id: "a", title: "new" }],
		});

		const sentBeforeMismatch = sentMessages(socket).length;
		socket.receive({
			type: "query.patch", id,
			baseRevision: { epoch: "runtime-a", sequence: 9 },
			subscriptionRevision: { epoch: "runtime-a", sequence: 12 },
			order: ["a"],
		});
		expect(handler).toHaveBeenCalledTimes(2);
		expect(sentMessages(socket).length).toBe(sentBeforeMismatch + 1);
		expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", id });
	});

	it("atomically applies keyed patches to object collections", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeLiveQuery(ref, {}, handler);
		const socket = latestSocket();
		socket.open();
		const [{ id }] = sentMessages(socket);
		socket.receive({
			type: "query.result", id,
			result: {
				taskUsers: [{ id: "u1", taskId: "a" }],
				taskTags: [{ id: "t1", taskId: "a" }],
				taskCustomFieldValues: [],
			},
			subscriptionRevision: { epoch: "runtime-a", sequence: 20 },
		});
		socket.receive({
			type: "query.objectPatch", id,
			baseRevision: { epoch: "runtime-a", sequence: 20 },
			subscriptionRevision: { epoch: "runtime-a", sequence: 21 },
			collections: {
				taskUsers: {
					updated: [{ id: "u1", taskId: "b" }],
					inserted: [{ id: "u2", taskId: "c" }],
					order: ["u2", "u1"],
				},
			},
		});
		await flushMicrotasks();
		expect(handler).toHaveBeenCalledTimes(2);
		expect(handler.mock.calls[1][0]).toMatchObject({
			type: "query.result",
			result: {
				taskUsers: [{ id: "u2", taskId: "c" }, { id: "u1", taskId: "b" }],
				taskTags: [{ id: "t1", taskId: "a" }],
				taskCustomFieldValues: [],
			},
		});
	});

	it("applies compact keyed prepend order deltas", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeLiveQuery(ref, {}, handler);
		const socket = latestSocket();
		socket.open();
		const [{ id }] = sentMessages(socket);
		socket.receive({
			type: "query.result", id,
			result: [{ id: "b" }, { id: "a" }],
			subscriptionRevision: { epoch: "runtime-a", sequence: 30 },
		});
		socket.receive({
			type: "query.patch", id,
			baseRevision: { epoch: "runtime-a", sequence: 30 },
			subscriptionRevision: { epoch: "runtime-a", sequence: 31 },
			inserted: [{ id: "c" }],
			prepend: ["c"],
		});
		await flushMicrotasks();
		expect(handler.mock.calls[1][0]).toMatchObject({
			type: "query.result",
			result: [{ id: "c" }, { id: "b" }, { id: "a" }],
		});
	});

  it("reuses an existing connecting socket instead of opening duplicates", () => {
    const client = new GonvexClient("ws://runtime.test/ws");

    client.connect();
    client.connect();

    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it("reconnects and restores live query subscriptions after a socket closes", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeLiveQuery(ref, { status: "open" }, handler);
    const firstSocket = latestSocket();
    firstSocket.open();
    const [firstSubscription] = sentMessages(firstSocket);
    firstSocket.receive({ type: "query.result", id: firstSubscription.id, result: [{ id: "before" }] });
    await flushMicrotasks();

    firstSocket.disconnect();
    vi.advanceTimersByTime(249);
    expect(FakeWebSocket.instances).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(FakeWebSocket.instances).toHaveLength(2);

    const secondSocket = latestSocket();
    secondSocket.open();
    const subscriptions = sentMessages(secondSocket).filter((message) => message.type === "query.subscribe");
    expect(subscriptions).toHaveLength(1);
    expect(subscriptions[0]).toMatchObject({
      id: firstSubscription.id,
      path: "tasks.list",
      args: { status: "open" },
    });

    secondSocket.receive({ type: "query.result", id: firstSubscription.id, result: [{ id: "after" }] });
    await flushMicrotasks();
    expect(handler).toHaveBeenLastCalledWith({
      type: "query.result",
      id: firstSubscription.id,
      result: [{ id: "after" }],
    });
  });

  it("reauthenticates before restoring subscriptions after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token", tenant: "tenant-a" });

    client.subscribeLiveQuery(ref, {}, () => undefined);
    const firstSocket = latestSocket();
    firstSocket.open();
    const [firstAuth] = sentMessages(firstSocket);
    firstSocket.receive({ type: "auth.result", id: firstAuth.id, result: authenticatedResult({ accountId: "account-a" }) });
    await vi.waitFor(() => {
      expect(sentMessages(firstSocket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    });

    firstSocket.disconnect();
    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();

    expect(sentMessages(secondSocket)).toHaveLength(1);
    expect(sentMessages(secondSocket)[0]).toMatchObject({
      type: "auth",
      token: "session-token",
      tenant: "tenant-a",
    });
    secondSocket.receive({ type: "auth.result", id: sentMessages(secondSocket)[0].id, result: authenticatedResult({ accountId: "account-a" }) });
    await vi.waitFor(() => {
      expect(sentMessages(secondSocket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    });
  });

  it("does not reconnect after an explicit close", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.close();
    vi.advanceTimersByTime(10_000);

    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it("queues subscription messages until the socket opens", () => {
    const client = new GonvexClient("ws://runtime.test/ws");

    client.subscribeLiveQuery(ref, { status: "open" }, () => undefined);
    const socket = latestSocket();
    expect(socket.sent).toHaveLength(0);

    socket.open();

    expect(sentMessages(socket)).toMatchObject([
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("sends auth before queued messages when the socket opens", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token", tenant: "tenant-a" });

    client.subscribeLiveQuery(ref, { status: "open" }, () => undefined);
    const socket = latestSocket();
    socket.open();

    expect(sentMessages(socket)).toMatchObject([
      {
        type: "auth",
        token: "session-token",
        tenant: "tenant-a",
        capabilities: { replicaReadyMany: 1, replicaWatermark: 1 },
      },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }) });

    await vi.waitFor(() => {
      expect(sentMessages(socket)).toMatchObject([
        { type: "auth", token: "session-token", tenant: "tenant-a" },
        { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
      ]);
    });
  });

  it("identifies a project before signed-out queries so project auth can be enforced", () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "secure-app" });

    client.subscribeLiveQuery(ref, {}, () => undefined);
    const socket = latestSocket();
    socket.open();

    expect(sentMessages(socket)).toMatchObject([{ type: "auth", project: "secure-app" }]);
    expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(false);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.error", id: authID, error: "a Gonvex app session is required" });
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", path: "tasks.list" });
  });

  it("hides the Local Replica immediately when tenant authorization is revoked", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "secure-app",
      tenant: "tenant-a",
      token: "session-token",
    });
    client.connect();
    const socket = latestSocket();
    socket.open();
    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }) });
    await flushMicrotasks();
    socket.receive({
      type: "replica.transaction",
      cursor: { epoch: "runtime", revision: 1 },
      changes: [{
        entity: "tasks",
        id: "task-a",
        operation: "insert",
        newValue: { id: "task-a", title: "restricted" },
      }],
    });
    await flushMicrotasks();
    expect(client.localReplica.entity("tasks", "task-a")).toMatchObject({ title: "restricted" });

    socket.receive({ type: "auth.error", id: authID, error: "tenant membership changed; authenticate again" });

    expect(client.localReplica.entity("tasks", "task-a")).toBeUndefined();
  });

  it("rotates Replica subscriptions when membership changes during reauthentication", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 100 },
    };
    const identity = { sub: "account-a", iss: "https://identity.test" };
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "secure-app",
      tenant: "tenant-a",
      token: "session-token-1",
      identity,
    });
    const watch = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    const initialAuth = sentMessages(socket).find((message) => message.type === "auth");
    socket.receive({
      type: "auth.result",
      id: initialAuth.id,
      result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }),
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const firstOpen = sentMessages(socket).find((message) => message.type === "replica.open");

    client.setAuth({ token: "session-token-2", identity });
    const rotatingAuth = sentMessages(socket).filter((message) => message.type === "auth").at(-1);
    expect(rotatingAuth.id).not.toBe(initialAuth.id);

    socket.receive({
      type: "auth.error",
      id: "membership-changed",
      error: "tenant membership changed; authenticate again",
    });
    expect(client.localReplica.entity("tasks", "task-a")).toBeUndefined();

    // The runtime can still emit failures for opens it discarded with the old
    // scope. Their retired IDs must not become errors in the current watch.
    socket.receive({
      type: "replica.error",
      id: firstOpen.id,
      path: collectionRef.path,
      error: "authenticate with an active tenant before opening a Replica Collection",
    });
    expect(() => watch.localReplicaResult()).not.toThrow();

    socket.receive({
      type: "auth.result",
      id: rotatingAuth.id,
      result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }),
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    const reopened = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1);
    expect(reopened.id).not.toBe(firstOpen.id);
    client.close();
  });

  it("exposes a runtime read-only Local Replica facade", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const replica = client.localReplica as unknown as Record<string, unknown>;

    expect(Object.isFrozen(replica)).toBe(true);
    expect(replica.applyTransaction).toBeUndefined();
    expect(replica.applyOptimistic).toBeUndefined();
    expect(replica.materializeWindow).toBeUndefined();
    expect(replica.replaceWindow).toBeUndefined();
    expect(replica.clear).toBeUndefined();
  });

  it("queues subscription messages while an auth update is in flight", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.setAuth({ token: "next-token", tenant: "tenant-b" });
    client.subscribeLiveQuery(ref, { status: "open" }, () => undefined);

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "next-token", tenant: "tenant-b" },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: authenticatedResult({ accountId: "account-b", tenantId: "tenant-b" }) });

    await vi.waitFor(() => {
      expect(sentMessages(socket)).toMatchObject([
        { type: "auth", token: "next-token", tenant: "tenant-b" },
        { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
      ]);
    });
  });

  it("opens tenant Replica Collections only after the accepted auth scope is active", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 100 },
    };
    const client = new GonvexClient("ws://runtime.test/ws");
    client.watchReplica(collectionRef, {});
    client.connect();
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });

    client.setAuth({ project: "shop", token: "next-token", tenant: "tenant-b" });
    expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);

    const auth = sentMessages(socket).findLast((message) => message.type === "auth");
    socket.receive({
      type: "auth.result",
      id: auth.id,
      result: authenticatedResult({ accountId: "account-b", tenantId: "tenant-b" }),
    });

    // Receiving auth.result starts Local Replica scope activation. It does not
    // make tenant subscriptions sendable in the middle of that state change.
    expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    client.close();
  });

  it("sends auth updates on an open socket", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.setAuth({ token: "next-token", tenant: "tenant-b" });

    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "auth", token: "next-token", tenant: "tenant-b" });
  });

  it("does not send another auth frame when only local refresh metadata changes", () => {
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "shop",
      tenant: "tenant-b",
      token: "next-token",
    });
    client.connect();
    const socket = latestSocket();
    socket.open();
    const initialAuth = sentMessages(socket).filter((message) => message.type === "auth");
    const fetchToken = vi.fn(async () => "next-token");

    client.setAuth({ token: "next-token", fetchToken });

    expect(sentMessages(socket).filter((message) => message.type === "auth")).toEqual(initialAuth);
  });

  it("ignores an obsolete project-only auth response after a newer tenant auth frame", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop" });
    const authErrors = vi.fn();
    client.onAuthError(authErrors);
    client.subscribeLiveQuery(ref, {}, vi.fn());
    const socket = latestSocket();
    socket.open();
    const projectOnlyAuth = sentMessages(socket).find((message) => message.type === "auth")!;

    client.setAuth({
      project: "shop",
      tenant: "tenant-b",
      token: "tenant-token",
      identity: { sub: "account-b", iss: "shop" },
    });
    expect(sentMessages(socket).filter((message) => message.type === "auth")).toHaveLength(1);

    socket.receive({
      type: "auth.result",
      id: projectOnlyAuth.id,
      result: authenticatedResult({ accountId: "account-b" }),
    });
    const authFrames = sentMessages(socket).filter((message) => message.type === "auth");
    expect(authFrames).toHaveLength(2);
    const tenantAuth = authFrames.at(-1)!;
    expect(tenantAuth.id).not.toBe(projectOnlyAuth.id);

    socket.receive({
      type: "auth.result",
      id: tenantAuth.id,
      result: authenticatedResult({ accountId: "account-b", tenantId: "tenant-b" }),
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    });

    socket.receive({ type: "auth.error", id: projectOnlyAuth.id, error: "authentication is required" });
    expect(authErrors).not.toHaveBeenCalled();
    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
  });

  it("fetches a token through the installed fetcher before sending auth", async () => {
    const fetchToken = vi.fn(async () => "fresh-token");
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });

    client.subscribeLiveQuery(ref, {}, vi.fn());
    const socket = latestSocket();
    socket.open();

    // Nothing goes out until the fetch resolves; subscriptions queue behind it.
    expect(socket.sent).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(0);

    expect(fetchToken).toHaveBeenCalledWith({ forceRefreshToken: false });
    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "fresh-token", tenant: "tenant-a" },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: authenticatedResult({ accountId: "account-a" }) });
    await vi.waitFor(() => expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(true));
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", path: "tasks.list" });
  });

  it("re-fetches the token on reconnect instead of replaying the original", async () => {
    const tokens = ["token-1", "token-2"];
    const fetchToken = vi.fn(async () => tokens.shift() ?? null);
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });

    client.connect();
    const firstSocket = latestSocket();
    firstSocket.open();
    await vi.advanceTimersByTimeAsync(0);
    expect(sentMessages(firstSocket)[0]).toMatchObject({ type: "auth", token: "token-1" });

    firstSocket.disconnect();
    await vi.advanceTimersByTimeAsync(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    await vi.advanceTimersByTimeAsync(0);

    expect(fetchToken).toHaveBeenCalledTimes(2);
    expect(sentMessages(secondSocket)[0]).toMatchObject({ type: "auth", token: "token-2" });
  });

  it("rotates a single-use developer credential in memory and uses it on reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const firstSocket = latestSocket();
    firstSocket.open();
    firstSocket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });

    const activation = client.authenticate({ project: "shop", tenant: "tenant-a", token: "gvx_imp_activation" });
    const firstAuth = sentMessages(firstSocket).at(-1);
    expect(firstAuth).toMatchObject({ type: "auth", token: "gvx_imp_activation", tenant: "tenant-a" });
    firstSocket.receive({ type: "auth.result", id: firstAuth.id, result: authenticatedResult({
      accountId: "account-a", tenantId: "tenant-a", developerSessionToken: "gvx_dev_rotation_1",
    }) });
    await expect(activation).resolves.toBeUndefined();

    firstSocket.disconnect();
    await vi.advanceTimersByTimeAsync(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    secondSocket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
    const secondAuth = sentMessages(secondSocket)[0];
    expect(secondAuth).toMatchObject({ type: "auth", token: "gvx_dev_rotation_1", tenant: "tenant-a" });
    secondSocket.receive({ type: "auth.result", id: secondAuth.id, result: authenticatedResult({
      accountId: "account-a", tenantId: "tenant-a", developerSessionToken: "gvx_dev_rotation_2",
    }) });

    secondSocket.disconnect();
    await vi.advanceTimersByTimeAsync(250);
    const thirdSocket = latestSocket();
    thirdSocket.open();
    expect(sentMessages(thirdSocket)[0]).toMatchObject({ type: "auth", token: "gvx_dev_rotation_2", tenant: "tenant-a" });
  });

  it("rejects an awaited authentication transition without installing a partial success", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();
    const activation = client.authenticate({ project: "shop", tenant: "tenant-a", token: "gvx_imp_rejected" });
    const auth = sentMessages(socket).at(-1);
    socket.receive({ type: "auth.error", id: auth.id, error: "grant already used" });
    await expect(activation).rejects.toThrow("grant already used");
  });

  it("keeps an awaited tenant transition when React installs its fetcher for the same credentials", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();
    const fetchToken = vi.fn(async () => "session-token");
    const activation = client.authenticate({
      project: "shop",
      tenant: "tenant-a",
      token: "session-token",
      identity: { sub: "account-a", iss: "shop" },
    });
    const auth = sentMessages(socket).at(-1);

    client.setAuth({ token: "session-token", fetchToken });

    expect(sentMessages(socket).filter((message) => message.type === "auth")).toHaveLength(1);
    socket.receive({
      type: "auth.result",
      id: auth.id,
      result: authenticatedResult({ accountId: "account-a", tenantId: "tenant-a" }),
    });
    await expect(activation).resolves.toBeUndefined();
  });

  it("force-refreshes the token and re-sends auth when the server rejects it", async () => {
    let current = "expired-token";
    const fetchToken = vi.fn(async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
      if (forceRefreshToken) current = "fresh-token";
      return current;
    });
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });

    client.subscribeLiveQuery(ref, {}, vi.fn());
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    const [firstAuth] = sentMessages(socket);
    expect(firstAuth).toMatchObject({ type: "auth", token: "expired-token" });

    socket.receive({ type: "auth.error", id: firstAuth.id, error: "token expired" });
    await vi.advanceTimersByTimeAsync(0);

    expect(fetchToken).toHaveBeenLastCalledWith({ forceRefreshToken: true });
    const auths = sentMessages(socket).filter((message) => message.type === "auth");
    expect(auths).toHaveLength(2);
    expect(auths[1]).toMatchObject({ token: "fresh-token" });

    // Subscriptions stay queued through the retry and flush once auth settles.
    expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(false);
    socket.receive({ type: "auth.result", id: auths[1].id, result: authenticatedResult({ accountId: "account-a" }) });
    await vi.waitFor(() => expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(true));
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", path: "tasks.list" });
  });

  it("re-subscribes Live Queries after same-socket token rotation", async () => {
    const token = (marker: string) => {
      const payload = globalThis.btoa(JSON.stringify({ sub: "account-a", iss: "shop", marker }));
      return `header.${payload}.signature`;
    };
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "shop",
      tenant: "tenant-a",
      token: token("first"),
    });

    client.subscribeLiveQuery(ref, {}, vi.fn());
    const socket = latestSocket();
    socket.open();
    const firstAuth = sentMessages(socket).find((message) => message.type === "auth");
    socket.receive({
      type: "auth.result",
      id: firstAuth.id,
      result: authenticatedResult({ accountId: "account-a" }),
    });
    await vi.advanceTimersByTimeAsync(0);
    const initialSubscribeCount = sentMessages(socket).filter((message) => message.type === "query.subscribe").length;
    expect(initialSubscribeCount).toBeGreaterThan(0);

    client.setAuth({ token: token("second") });
    const secondAuth = sentMessages(socket).filter((message) => message.type === "auth").at(-1);
    socket.receive({
      type: "auth.result",
      id: secondAuth.id,
      result: authenticatedResult({ accountId: "account-a" }),
    });
    await vi.advanceTimersByTimeAsync(0);

    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(initialSubscribeCount + 1);
  });

  it("can refresh an expired access token through the same native Control Plane socket", async () => {
    let client!: GonvexClient;
    const fetchToken = vi.fn(async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
      if (!forceRefreshToken) return "expired-token";
      const session = await client.action(control.auth.refreshSession, { refreshToken: "refresh-token" });
      return session.accessToken;
    });
    client = new GonvexClient("ws://runtime.test/ws", { project: "shop", tenant: "tenant-a", fetchToken });

    client.connect();
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    const firstAuth = sentMessages(socket).find((message) => message.type === "auth");
    socket.receive({ type: "auth.error", id: firstAuth.id, error: "token expired" });
    await vi.advanceTimersByTimeAsync(0);

    const refresh = sentMessages(socket).find((message) => message.type === "action.call");
    expect(refresh).toMatchObject({ path: "control.auth.refreshSession", scope: "control", args: { refreshToken: "refresh-token" } });
    socket.receive({ type: "action.result", id: refresh.id, result: { accessToken: "fresh-token" } });
    await vi.advanceTimersByTimeAsync(0);

    const auths = sentMessages(socket).filter((message) => message.type === "auth");
    expect(auths).toHaveLength(2);
    expect(auths[1]).toMatchObject({ token: "fresh-token", tenant: "tenant-a" });
  });

  it("gives up and notifies onAuthError when the forced refresh returns the rejected token", async () => {
    const fetchToken = vi.fn(async () => "always-bad");
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });
    const onAuthError = vi.fn();
    client.onAuthError(onAuthError);

    client.subscribeLiveQuery(ref, {}, vi.fn());
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    const [firstAuth] = sentMessages(socket);

    socket.receive({ type: "auth.error", id: firstAuth.id, error: "token expired" });
    await vi.advanceTimersByTimeAsync(0);

    // Re-sending the very token the server just refused would loop forever.
    expect(sentMessages(socket).filter((message) => message.type === "auth")).toHaveLength(1);
    expect(onAuthError).toHaveBeenCalledWith("token expired");
    // Tenant reads stay fail-closed until a valid authenticated scope exists.
    expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(false);
  });

  it("retries a rejected token only once per rejection cycle", async () => {
    let count = 0;
    const fetchToken = vi.fn(async () => `bad-token-${++count}`);
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });
    const onAuthError = vi.fn();
    client.onAuthError(onAuthError);

    client.connect();
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    const [firstAuth] = sentMessages(socket);
    socket.receive({ type: "auth.error", id: firstAuth.id, error: "bad token" });
    await vi.advanceTimersByTimeAsync(0);

    const auths = sentMessages(socket).filter((message) => message.type === "auth");
    expect(auths).toHaveLength(2);
    socket.receive({ type: "auth.error", id: auths[1].id, error: "still bad" });
    await vi.advanceTimersByTimeAsync(0);

    expect(sentMessages(socket).filter((message) => message.type === "auth")).toHaveLength(2);
    expect(fetchToken).toHaveBeenCalledTimes(2);
    expect(onAuthError).toHaveBeenCalledWith("still bad");
  });

  it("sends a token provided to setAuth as-is and keeps the fetcher for reconnects", async () => {
    const fetchToken = vi.fn(async () => "fetched-token");
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const firstSocket = latestSocket();
    firstSocket.open();

    client.setAuth({ token: "provided-token", tenant: "tenant-a", fetchToken });
    expect(fetchToken).not.toHaveBeenCalled();
    expect(sentMessages(firstSocket).at(-1)).toMatchObject({ type: "auth", token: "provided-token" });

    firstSocket.disconnect();
    await vi.advanceTimersByTimeAsync(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    await vi.advanceTimersByTimeAsync(0);

    expect(fetchToken).toHaveBeenCalledWith({ forceRefreshToken: false });
    expect(sentMessages(secondSocket)[0]).toMatchObject({ type: "auth", token: "fetched-token" });
  });

  it("treats a null fetch result as signed out", async () => {
    const fetchToken = vi.fn(async () => null);
    const client = new GonvexClient("ws://runtime.test/ws", { token: "stale-token", tenant: "tenant-a", fetchToken });
    client.connect();
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);

    const [auth] = sentMessages(socket);
    expect(auth).toMatchObject({ type: "auth", tenant: "tenant-a" });
    expect(auth.token).toBeUndefined();
  });

  it("keeps the installed token when the fetcher rejects", async () => {
    const fetchToken = vi.fn(async () => {
      throw new Error("identity provider unreachable");
    });
    const client = new GonvexClient("ws://runtime.test/ws", { token: "cached-token", tenant: "tenant-a", fetchToken });
    client.connect();
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);

    expect(sentMessages(socket)[0]).toMatchObject({ type: "auth", token: "cached-token", tenant: "tenant-a" });
  });

  it("routes Live Query results to the matching handler", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeLiveQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: [{ id: "task" }] });
    await flushMicrotasks();

    expect(handler).toHaveBeenCalledWith({ type: "query.result", id, result: [{ id: "task" }] });
  });

  it("coalesces identical Live Queries and fans out results", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeLiveQuery(ref, { status: "open" }, first);
    client.subscribeLiveQuery(ref, { status: "open" }, second);
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    expect(messages.filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    const [{ id }] = messages;
    socket.receive({ type: "query.result", id, result: [{ id: "task" }] });
    await flushMicrotasks();

    expect(first).toHaveBeenCalledWith({ type: "query.result", id, result: [{ id: "task" }] });
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: [{ id: "task" }] });
  });

  it("replays the latest result to a late joiner without resubscribing", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeLiveQuery(ref, { status: "open" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: [{ id: "task" }], reason: "initial" });
    await flushMicrotasks();
    expect(first).toHaveBeenCalledWith({ type: "query.result", id, result: [{ id: "task" }], reason: "initial" });

    // A component mounting after the initial result must still receive the cached value;
    // the coalesced subscription only gets `initial` once from the server.
    client.subscribeLiveQuery(ref, { status: "open" }, second);
    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    expect(second).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: [{ id: "task" }], reason: "initial" });
  });

  it("does not replay a stale result to a late joiner that unsubscribes synchronously", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeLiveQuery(ref, { status: "open" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: [{ id: "task" }] });
    await flushMicrotasks();

    const unsubscribeSecond = client.subscribeLiveQuery(ref, { status: "open" }, second);
    unsubscribeSecond();
    await Promise.resolve();
    expect(second).not.toHaveBeenCalled();
  });

  it("keeps a coalesced Live Query subscribed until the last listener leaves", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    const unsubscribeFirst = client.subscribeLiveQuery(ref, { status: "open" }, first);
    const unsubscribeSecond = client.subscribeLiveQuery(ref, { status: "open" }, second);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    unsubscribeFirst();
    vi.advanceTimersByTime(300);
    expect(sentMessages(socket).filter((message) => message.type === "query.unsubscribe")).toHaveLength(0);

    socket.receive({ type: "query.result", id, result: [{ id: "task" }] });
    await flushMicrotasks();
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);

    unsubscribeSecond();
    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("keeps network telemetry disabled unless explicitly enabled", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeLiveQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(sentMessages(socket).some((message) => message.type === "telemetry.event")).toBe(false);
  });

  it("renders a persisted Live Query window after the server confirms its visibility scope", async () => {
    const storage = new MemoryLocalReplicaStorage();
    const liveRef: FunctionReference<Record<string, never>, Array<{ id: string; title: string }>> = {
      kind: "query",
      path: "tasks.grid",
      delivery: "live",
      live: { entity: "tasks", key: "id", resultPath: [], plan: { table: "tasks", key: "id", columns: ["id", "title"] } },
    };
    const directive = {
      protocolVersion: 1 as const,
      scope: "deployment-scope",
      visibilityScope: "visibility-scope",
      epoch: "module-epoch",
    };

    const first = new GonvexClient("ws://runtime.test/ws", { localReplica: { storage } });
    const firstWatch = first.watchLiveQuery(liveRef, {});
    const firstSocket = latestSocket();
    firstSocket.open();
    firstSocket.receive({ type: "session.ready", replica: directive });
    await vi.waitFor(() => {
      expect(sentMessages(firstSocket).filter((message) => message.type === "query.subscribe")).toHaveLength(2);
    });
    const firstSubscribe = sentMessages(firstSocket).filter((message) => message.type === "query.subscribe").at(-1);
    expect(firstSubscribe).toBeDefined();
    firstSocket.receive({
      type: "query.result",
      id: firstSubscribe!.id,
      path: "tasks.grid",
      result: [{ id: "task-1", title: "Persisted" }],
      subscriptionRevision: { epoch: "runtime", sequence: 1 },
      windowRevision: "window-1",
    });
    await flushMicrotasks();
    expect(first.localReplica.snapshot().liveQueries).not.toEqual({});
    expect(firstWatch.localLiveQueryResult()).toEqual([{ id: "task-1", title: "Persisted" }]);
    first.close();

    const second = new GonvexClient("ws://runtime.test/ws", { localReplica: { storage } });
    const secondWatch = second.watchLiveQuery(liveRef, {});
    const secondSocket = latestSocket();
    secondSocket.open();
    secondSocket.receive({ type: "session.ready", replica: directive });
    await vi.waitFor(() => {
      expect(secondWatch.localLiveQueryResult()).toBeDefined();
    });

    expect(secondWatch.localLiveQueryResult()).toEqual([{ id: "task-1", title: "Persisted" }]);
    second.close();
  });

  it("keeps Replica Collection state partial until ready and stable between revisions", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 1 },
    };
    const client = new GonvexClient("ws://runtime.test/ws");
    const watch = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const open = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1);
    expect(open).toBeDefined();
    const rows = [{ id: "task-1", title: "Cached" }];
    socket.receive({
      type: "replica.snapshot", id: open.id, path: "tasks.recent", result: rows,
      cursor: { epoch: "epoch:test", revision: 12 }, key: "id", maxRows: 1,
    });
    await vi.waitFor(() => expect(watch.localReplicaState()).toBeDefined());

    const verifying = watch.localReplicaState();
    expect(verifying).toMatchObject({ completeness: "partial", truncated: false, computedRevision: 12 });
    expect(watch.localReplicaState()).toBe(verifying);

    const digest = await replicaHashesDigest(await replicaRowsHashes(rows, "id"));
    socket.receive({
      type: "replica.ready", id: open.id, path: "tasks.recent",
      cursor: { epoch: "epoch:test", revision: 12 }, digest, truncated: true,
    });
    await vi.waitFor(() => {
      expect(watch.localReplicaState()).toMatchObject({ completeness: "partial", truncated: true, computedRevision: 12 });
    });
    client.close();
  });

  it("keeps a ready Replica Collection authoritative while applying its delta", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 100 },
    };
    const client = new GonvexClient("ws://runtime.test/ws");
    const watch = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const open = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1)!;
    const initialRows = [{ id: "task-1", title: "Pending" }];
    const initialHashes = await replicaRowsHashes(initialRows, "id");
    const initialDigest = await replicaHashesDigest(initialHashes);
    socket.receive({
      type: "replica.snapshot", id: open.id, path: collectionRef.path,
      result: initialRows, cursor: { epoch: "epoch:test", revision: 1 }, key: "id",
      maxRows: 100, hashes: initialHashes, digest: initialDigest, truncated: false,
    });
    socket.receive({
      type: "replica.ready", id: open.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 1 }, digest: initialDigest, truncated: false,
    });
    await vi.waitFor(() => {
      expect(watch.localReplicaState()).toMatchObject({
        source: "server", freshness: "current", isUpToDate: true,
      });
    });

    const states: Array<{ source: string; freshness: string; isUpToDate: boolean }> = [];
    const stop = watch.onUpdate(() => {
      const state = watch.localReplicaState();
      if (state) states.push({
        source: state.source,
        freshness: state.freshness,
        isUpToDate: state.isUpToDate,
      });
    });
    await flushMicrotasks();
    states.length = 0;

    const updatedRows = [{ id: "task-1", title: "In progress" }];
    const updatedHashes = await replicaRowsHashes(updatedRows, "id");
    const updatedDigest = await replicaHashesDigest(updatedHashes);
    socket.receive({
      type: "replica.delta", id: open.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 2 }, upserts: updatedRows, deleted: [],
    });
    await vi.waitFor(() => {
      expect(watch.localReplicaState()).toMatchObject({
        source: "server", freshness: "current", isUpToDate: true,
      });
      expect(watch.localReplicaResult()).toEqual(updatedRows);
    });
    expect(states.length).toBeGreaterThan(0);
    expect(states.some((state) => state.source === "cache" || state.freshness === "verifying" || !state.isUpToDate)).toBe(false);

    socket.receive({
      type: "replica.ready", id: open.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 2 }, digest: updatedDigest, truncated: false,
    });
    await vi.waitFor(() => expect(watch.localReplicaState()).toMatchObject({
      source: "server", freshness: "current", isUpToDate: true,
    }));
    stop();
    client.close();
  });

  it("keeps each hydrated Replica window verifying until its own ready frame", async () => {
    const statusesRef: FunctionReference = {
      kind: "query",
      path: "statuses.all",
      delivery: "replica",
      replica: { table: "statuses", key: "id", columns: ["id", "name"], maxRows: 100 },
    };
    const teamsRef: FunctionReference = {
      kind: "query",
      path: "teams.all",
      delivery: "replica",
      replica: { table: "teams", key: "id", columns: ["id", "name"], maxRows: 100 },
    };
    const statusesRows = [{ id: "status-1", name: "Open" }];
    const teamsRows = [{ id: "team-1", name: "Team A" }];
    const statusesHashes = await replicaRowsHashes(statusesRows, "id");
    const teamsHashes = await replicaRowsHashes(teamsRows, "id");
    const statusesDigest = await replicaHashesDigest(statusesHashes);
    const teamsDigest = await replicaHashesDigest(teamsHashes);
    const storage = new MemoryLocalReplicaStorage();

    const first = new GonvexClient("ws://runtime.test/ws", { localReplica: { storage } });
    const firstStatuses = first.watchReplica(statusesRef, {});
    const firstTeams = first.watchReplica(teamsRef, {});
    const firstSocket = latestSocket();
    firstSocket.open();
    firstSocket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(firstSocket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    const firstOpens = sentMessages(firstSocket).filter((message) => message.type === "replica.open");
    const firstStatusesOpen = firstOpens.find((message) => message.path === statusesRef.path)!;
    const firstTeamsOpen = firstOpens.find((message) => message.path === teamsRef.path)!;
    firstSocket.receive({
      type: "replica.snapshot", id: firstStatusesOpen.id, path: statusesRef.path,
      result: statusesRows, cursor: { epoch: "epoch:test", revision: 10 }, key: "id",
      maxRows: 100, hashes: statusesHashes, digest: statusesDigest, truncated: false,
    });
    firstSocket.receive({
      type: "replica.ready", id: firstStatusesOpen.id, path: statusesRef.path,
      cursor: { epoch: "epoch:test", revision: 10 }, digest: statusesDigest, truncated: false,
    });
    firstSocket.receive({
      type: "replica.snapshot", id: firstTeamsOpen.id, path: teamsRef.path,
      result: teamsRows, cursor: { epoch: "epoch:test", revision: 10 }, key: "id",
      maxRows: 100, hashes: teamsHashes, digest: teamsDigest, truncated: false,
    });
    firstSocket.receive({
      type: "replica.ready", id: firstTeamsOpen.id, path: teamsRef.path,
      cursor: { epoch: "epoch:test", revision: 10 }, digest: teamsDigest, truncated: false,
    });
    await vi.waitFor(() => {
      expect(firstStatuses.localReplicaState()?.isUpToDate).toBe(true);
      expect(firstTeams.localReplicaState()?.isUpToDate).toBe(true);
    });
    first.close();

    const second = new GonvexClient("ws://runtime.test/ws", { localReplica: { storage } });
    const secondStatuses = second.watchReplica(statusesRef, {});
    const secondTeams = second.watchReplica(teamsRef, {});
    const secondSocket = latestSocket();
    secondSocket.open();
    secondSocket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(secondStatuses.localReplicaState()).toMatchObject({
        rows: statusesRows, source: "cache", freshness: "verifying", isUpToDate: false,
      });
      expect(secondTeams.localReplicaState()).toMatchObject({
        rows: teamsRows, source: "cache", freshness: "verifying", isUpToDate: false,
      });
      expect(sentMessages(secondSocket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    const secondOpens = sentMessages(secondSocket).filter((message) => message.type === "replica.open");
    const secondStatusesOpen = secondOpens.find((message) => message.path === statusesRef.path)!;
    const secondTeamsOpen = secondOpens.find((message) => message.path === teamsRef.path)!;

    secondSocket.receive({
      type: "replica.ready", id: secondStatusesOpen.id, path: statusesRef.path,
      cursor: { epoch: "epoch:test", revision: 10 }, digest: statusesDigest, truncated: false,
    });
    await vi.waitFor(() => {
      expect(second.localReplica.freshness()).toBe("current");
      expect(secondStatuses.localReplicaState()).toMatchObject({
        source: "server", freshness: "current", isUpToDate: true,
      });
      expect(secondTeams.localReplicaState()).toMatchObject({
        source: "cache", freshness: "verifying", isUpToDate: false,
      });
    });

    secondSocket.receive({
      type: "replica.ready", id: secondTeamsOpen.id, path: teamsRef.path,
      cursor: { epoch: "epoch:test", revision: 10 }, digest: teamsDigest, truncated: false,
    });
    await vi.waitFor(() => {
      expect(secondTeams.localReplicaState()).toMatchObject({
        source: "server", freshness: "current", isUpToDate: true,
      });
    });
    second.close();
  });

  it("persists a bounded snapshot only once when inline integrity already proves ready", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "statuses.all",
      delivery: "replica",
      replica: { table: "statuses", key: "id", columns: ["id", "name"], maxRows: 100 },
    };
    const storage = new MemoryLocalReplicaStorage();
    const replaceWindow = vi.spyOn(storage, "replaceWindow");
    const client = new GonvexClient("ws://runtime.test/ws", { localReplica: { storage } });
    const watch = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const open = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1)!;
    const rows = [{ id: "status-1", name: "Working" }];
    const hashes = await replicaRowsHashes(rows, "id");
    const digest = await replicaHashesDigest(hashes);

    socket.receive({
      type: "replica.snapshot", id: open.id, path: collectionRef.path,
      result: rows, cursor: { epoch: "epoch:test", revision: 18 }, key: "id",
      maxRows: 100, hashes, digest, truncated: false,
    });
    socket.receive({
      type: "replica.ready", id: open.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 18 }, digest, truncated: false,
    });

    await vi.waitFor(() => {
      expect(watch.localReplicaState()).toMatchObject({ completeness: "complete", computedRevision: 18 });
      expect(replaceWindow).toHaveBeenCalledTimes(1);
    });
    client.close();
  });

  it("applies a received transaction before a following Replica watermark", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "statuses.all",
      delivery: "replica",
      replica: { table: "statuses", key: "id", columns: ["id", "name"], maxRows: 100 },
    };
    const client = new GonvexClient("ws://runtime.test/ws", {
      localReplica: { storage: new MemoryLocalReplicaStorage() },
    });
    const watch = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({
      type: "session.ready",
      capabilities: { replicaWatermark: 1 },
      replica: testReplicaDirective,
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const open = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1)!;
    const rows = [{ id: "status-1", name: "Working" }];
    const hashes = await replicaRowsHashes(rows, "id");
    const digest = await replicaHashesDigest(hashes);

    socket.receive({
      type: "replica.snapshot",
      id: open.id,
      path: collectionRef.path,
      result: rows,
      cursor: { epoch: "epoch:test", revision: 18 },
      key: "id",
      maxRows: 100,
      hashes,
      digest,
      truncated: false,
    });
    socket.receive({
      type: "replica.ready",
      id: open.id,
      path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 18 },
      digest,
      truncated: false,
    });
    await vi.waitFor(() => {
      expect(watch.localReplicaState()).toMatchObject({ completeness: "complete", computedRevision: 18 });
    });

    // These frames arrive back-to-back. The transaction is queued for durable
    // application, so the following watermark must wait behind it instead of
    // raising the cursor first and making revision 19 look stale.
    socket.receive({
      type: "replica.transaction",
      cursor: { epoch: "epoch:test", revision: 19 },
      changes: [{
        entity: "statuses",
        id: "status-2",
        operation: "insert",
        newValue: { id: "status-2", name: "Blocked" },
      }],
    });
    socket.receive({ type: "replica.watermark", revision: 19 });

    await vi.waitFor(() => {
      expect(client.localReplica.entity("statuses", "status-2")).toEqual({
        id: "status-2",
        name: "Blocked",
      });
      expect(watch.localReplicaState()).toMatchObject({ computedRevision: 19 });
    });
    client.close();
  });

  it("settles a tenant Reducer only after its collection membership delta is durable", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "members.listMemberSpots",
      delivery: "replica",
      replica: {
        table: "memberSpots",
        key: "_id",
        columns: ["_id", "memberId", "spotId"],
        maxRows: 10_000,
      },
    };
    const storage = new MemoryLocalReplicaStorage();
    let releaseWatermark!: () => void;
    const watermarkPersisted = new Promise<void>((resolve) => { releaseWatermark = resolve; });
    const persistWatermark = storage.advanceWatermark.bind(storage);
    const advanceWatermark = vi.spyOn(storage, "advanceWatermark").mockImplementation(async (...args) => {
      await watermarkPersisted;
      await persistWatermark(...args);
    });
    const client = new GonvexClient("ws://runtime.test/ws", { localReplica: { storage } });
    const memberSpots = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({
      type: "session.ready",
      capabilities: { replicaWatermark: 1 },
      replica: testReplicaDirective,
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const open = sentMessages(socket).find((message) => message.type === "replica.open")!;
    const emptyDigest = await replicaHashesDigest({});
    socket.receive({
      type: "replica.snapshot",
      id: open.id,
      path: collectionRef.path,
      result: [],
      cursor: { epoch: "epoch:test", revision: 1 },
      key: "_id",
      hashes: {},
      digest: emptyDigest,
      truncated: false,
    });
    socket.receive({
      type: "replica.ready",
      id: open.id,
      path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 1 },
      digest: emptyDigest,
      truncated: false,
    });
    await vi.waitFor(() => expect(memberSpots.localReplicaResult()).toEqual([]));

    const reducer = client.reducer(
      { kind: "reducer", path: "members.saveProfileAndTeams" },
      { memberId: "member-1", spots: ["spot-1"] },
    );
    await vi.advanceTimersByTimeAsync(0);
    const call = sentMessages(socket).findLast((message) => message.type === "reducer.call")!;
    let settled = false;
    void reducer.then(() => { settled = true; });
    socket.receive({
      type: "reducer.result",
      id: call.id,
      path: call.path,
      result: { memberId: "member-1" },
      originCommandId: call.id,
      committedRevision: 4,
    });
    await flushMicrotasks();
    expect(settled).toBe(false);

    const rows = [{ _id: "member-spot-1", memberId: "member-1", spotId: "spot-1" }];
    const digest = await replicaHashesDigest(await replicaRowsHashes(rows, "_id"));
    socket.receive({
      type: "replica.transaction",
      cursor: { epoch: "epoch:test", revision: 3 },
      originCommandId: call.id,
      changes: [{
        entity: "memberSpots",
        id: "member-spot-1",
        operation: "insert",
        newValue: rows[0],
      }],
    });
    socket.receive({
      type: "replica.delta",
      id: open.id,
      path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 3 },
      upserts: rows,
      deleted: [],
      digest,
    });
    socket.receive({
      type: "replica.ready",
      id: open.id,
      path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 3 },
      digest,
      truncated: false,
    });
    await vi.waitFor(() => expect(memberSpots.localReplicaResult()).toEqual(rows));
    expect(settled).toBe(false);

    socket.receive({ type: "replica.watermark", revision: 4 });
    await flushMicrotasks();
    expect(advanceWatermark).toHaveBeenCalledTimes(1);
    expect(settled).toBe(false);
    releaseWatermark();
    await expect(reducer).resolves.toEqual({ memberId: "member-1" });
    expect(settled).toBe(true);
    client.close();
  });

  it("settles a Control Reducer only after refreshed Control Query state is applied", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const invitations = client.watchControlQuery(control.invitations.list, {});
    const stop = invitations.onUpdate(() => undefined);
    const socket = latestSocket();
    socket.open();
    socket.receive({
      type: "session.ready",
      capabilities: { controlWatermark: 1 },
      replica: testReplicaDirective,
    });
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();
    const subscribe = sentMessages(socket).findLast(
      (message) => message.type === "query.subscribe" && message.path === "control.invitations.list",
    )!;
    expect(subscribe).toBeDefined();
    const before = [{
      id: "invitation-1",
      email: "person@example.test",
      role: "member",
      permissions: {},
      teamIds: ["team-old"],
      allowedAuthProviders: ["firebase"],
      expiresAt: "2026-09-01T00:00:00Z",
      revoked: false,
      accepted: false,
      state: "pending",
      createdAt: "2026-08-01T00:00:00Z",
      updatedAt: "2026-08-01T00:00:00Z",
    }];
    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: subscribe.path,
      result: before,
      reason: "initial",
    });
    await flushMicrotasks();
    expect(invitations.getSnapshot().result).toEqual(before);

    const reducer = client.reducer(control.invitations.update, {
      id: "invitation-1",
      role: "member",
      permissions: {},
      teamIds: ["team-new"],
      allowedAuthProviders: ["firebase"],
      payload: {},
    });
    await vi.advanceTimersByTimeAsync(0);
    const call = sentMessages(socket).findLast(
      (message) => message.type === "reducer.call" && message.path === "control.invitations.update",
    )!;
    let settled = false;
    void reducer.then(() => { settled = true; });
    const after = [{ ...before[0], teamIds: ["team-new"], updatedAt: "2026-08-29T00:00:00Z" }];
    socket.receive({
      type: "query.result",
      id: subscribe.id,
      path: subscribe.path,
      result: after,
      reason: "control-change",
    });
    await flushMicrotasks();
    expect(invitations.getSnapshot().result).toEqual(after);
    expect(settled).toBe(false);

    socket.receive({
      type: "reducer.result",
      id: call.id,
      path: call.path,
      result: { updated: true },
      originCommandId: call.id,
    });
    await flushMicrotasks();
    expect(settled).toBe(false);

    socket.receive({ type: "control.watermark", id: call.id });
    await expect(reducer).resolves.toEqual({ updated: true });
    expect(settled).toBe(true);
    stop();
    client.close();
  });

  it("settles Control Reducers immediately for runtimes without Control watermarks", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer(control.invitations.revoke, {
      id: "invitation-1",
      email: "person@example.test",
    });
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
    await vi.advanceTimersByTimeAsync(0);
    const call = sentMessages(socket).findLast(
      (message) => message.type === "reducer.call" && message.path === "control.invitations.revoke",
    )!;
    socket.receive({
      type: "reducer.result",
      id: call.id,
      path: call.path,
      result: { updated: true },
      originCommandId: call.id,
    });
    await expect(reducer).resolves.toEqual({ updated: true });
    client.close();
  });

  it("uses a Replica watermark to settle Reducers with no visible subscriptions", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();
    socket.receive({
      type: "session.ready",
      capabilities: { replicaWatermark: 1 },
      replica: testReplicaDirective,
    });
    const reducer = client.reducer({ kind: "reducer", path: "preferences.save" }, { value: true });
    await vi.advanceTimersByTimeAsync(0);
    const call = sentMessages(socket).findLast((message) => message.type === "reducer.call")!;
    let settled = false;
    void reducer.then(() => { settled = true; });
    socket.receive({
      type: "reducer.result",
      id: call.id,
      path: call.path,
      result: { saved: true },
      originCommandId: call.id,
      committedRevision: 7,
    });
    await flushMicrotasks();
    expect(settled).toBe(false);

    socket.receive({ type: "replica.watermark", revision: 7 });
    await expect(reducer).resolves.toEqual({ saved: true });
    client.close();
  });

  it("settles an Action that invoked Reducers only after their Replica watermark", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();
    socket.receive({
      type: "session.ready",
      capabilities: { replicaWatermark: 1 },
      replica: testReplicaDirective,
    });
    const action = client.action(
      { kind: "action", path: "testing.invoke" },
      { operation: "seedE2ESettingRow", args: { kind: "teams", name: "Team A" } },
    );
    await vi.advanceTimersByTimeAsync(0);
    const call = sentMessages(socket).findLast((message) => message.type === "action.call")!;
    let settled = false;
    void action.then(() => { settled = true; });
    socket.receive({
      type: "action.result",
      id: call.id,
      path: call.path,
      result: { _id: "team-a", name: "Team A" },
      committedRevision: 8,
    });
    await flushMicrotasks();
    expect(settled).toBe(false);

    socket.receive({ type: "replica.watermark", revision: 7 });
    await flushMicrotasks();
    expect(settled).toBe(false);

    socket.receive({ type: "replica.watermark", revision: 8 });
    await expect(action).resolves.toEqual({ _id: "team-a", name: "Team A" });
    expect(settled).toBe(true);
    client.close();
  });

  it("settles Reducers immediately when an older runtime does not advertise watermarks", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", capabilities: {}, replica: testReplicaDirective });
    const reducer = client.reducer({ kind: "reducer", path: "preferences.save" }, { value: true });
    await vi.advanceTimersByTimeAsync(0);
    const call = sentMessages(socket).findLast((message) => message.type === "reducer.call")!;
    socket.receive({
      type: "reducer.result",
      id: call.id,
      path: call.path,
      result: { saved: true },
      originCommandId: call.id,
      committedRevision: 7,
    });
    await expect(reducer).resolves.toEqual({ saved: true });
    client.close();
  });

  it("waits for an authoritative visibility scope before opening a Replica Collection", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 1 },
    };
    const client = new GonvexClient("ws://runtime.test/ws");
    client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();

    expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(0);

    socket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    client.close();
  });

  it("applies transaction, delta, and ready frames in order without clearing other collections", async () => {
    const reactionsRef: FunctionReference = {
      kind: "query",
      path: "chat.messageReactions",
      delivery: "replica",
      replica: { table: "messageReactions", key: "_id", columns: ["_id", "emoji"] },
    };
    const workspacesRef: FunctionReference = {
      kind: "query",
      path: "workspaces.list",
      delivery: "replica",
      replica: { table: "workspaces", key: "_id", columns: ["_id", "name"] },
    };
    const client = new GonvexClient("ws://runtime.test/ws");
    const reactions = client.watchReplica(reactionsRef, {});
    const workspaces = client.watchReplica(workspacesRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    const opens = sentMessages(socket).filter((message) => message.type === "replica.open");
    const reactionsOpen = opens.filter((message) => message.path === reactionsRef.path).at(-1)!;
    const workspacesOpen = opens.filter((message) => message.path === workspacesRef.path).at(-1)!;
    const reactionRows = [{ _id: "reaction-1", emoji: "thumbs-up" }];
    const workspaceRows = [{ _id: "workspace-1", name: "Operations" }];
    const reactionDigest = await replicaHashesDigest(await replicaRowsHashes(reactionRows, "_id"));
    const workspaceDigest = await replicaHashesDigest(await replicaRowsHashes(workspaceRows, "_id"));

    socket.receive({
      type: "replica.snapshot", id: reactionsOpen.id, path: reactionsRef.path,
      result: reactionRows, cursor: { epoch: "epoch:test", revision: 1 }, key: "_id",
    });
    socket.receive({
      type: "replica.snapshot", id: workspacesOpen.id, path: workspacesRef.path,
      result: workspaceRows, cursor: { epoch: "epoch:test", revision: 1 }, key: "_id",
    });
    socket.receive({
      type: "replica.ready", id: reactionsOpen.id, path: reactionsRef.path,
      cursor: { epoch: "epoch:test", revision: 1 }, digest: reactionDigest, truncated: false,
    });
    socket.receive({
      type: "replica.ready", id: workspacesOpen.id, path: workspacesRef.path,
      cursor: { epoch: "epoch:test", revision: 1 }, digest: workspaceDigest, truncated: false,
    });
    await vi.waitFor(() => {
      expect(reactions.localReplicaResult()).toEqual(reactionRows);
      expect(workspaces.localReplicaResult()).toEqual(workspaceRows);
    });
    const stableWorkspaceRows = workspaces.localReplicaResult();

    const nextReactionRows = [...reactionRows, { _id: "reaction-2", emoji: "heart" }];
    const nextReactionDigest = await replicaHashesDigest(await replicaRowsHashes(nextReactionRows, "_id"));
    socket.receive({
      type: "replica.transaction",
      cursor: { epoch: "epoch:test", revision: 2 },
      changes: [{
        entity: "messageReactions", id: "reaction-2", operation: "insert",
        newValue: { _id: "reaction-2", emoji: "heart" },
      }],
    });
    socket.receive({
      type: "replica.delta", id: reactionsOpen.id, path: reactionsRef.path,
      cursor: { epoch: "epoch:test", revision: 2 },
      upserts: [{ _id: "reaction-2", emoji: "heart" }], deleted: [],
    });
    socket.receive({
      type: "replica.ready", id: reactionsOpen.id, path: reactionsRef.path,
      cursor: { epoch: "epoch:test", revision: 2 }, digest: nextReactionDigest, truncated: false,
    });
    socket.receive({
      type: "replica.ready", id: workspacesOpen.id, path: workspacesRef.path,
      cursor: { epoch: "epoch:test", revision: 2 }, digest: workspaceDigest, truncated: false,
    });

    await flushMicrotasks();
    await vi.waitFor(() => {
      expect(reactions.localReplicaResult()).toEqual(nextReactionRows);
      expect(workspaces.localReplicaResult()).toEqual(workspaceRows);
      expect(workspaces.localReplicaResult()).toBe(stableWorkspaceRows);
      expect(client.localReplica.entity("messageReactions", "reaction-2")).toMatchObject({ emoji: "heart" });
    });
    client.close();
  });

  it("keeps Replica delta and ready ordered after same-socket authentication rotation", async () => {
    const collectionRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 100 },
    };
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "shop",
      tenant: "tenant-a",
      token: "first-token",
      identity: { sub: "account-a", iss: "shop" },
    });
    const tasks = client.watchReplica(collectionRef, {});
    const socket = latestSocket();
    socket.open();
    const firstAuth = sentMessages(socket).find((message) => message.type === "auth");
    socket.receive({
      type: "auth.result",
      id: firstAuth.id,
      result: authenticatedResult({ accountId: "account-a" }),
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(1);
    });
    const firstOpen = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1)!;
    const initialRows = [{ id: "task-1", title: "Pending" }];
    const initialDigest = await replicaHashesDigest(await replicaRowsHashes(initialRows, "id"));
    socket.receive({
      type: "replica.snapshot", id: firstOpen.id, path: collectionRef.path,
      result: initialRows, cursor: { epoch: "epoch:test", revision: 1 }, key: "id", maxRows: 100,
    });
    socket.receive({
      type: "replica.ready", id: firstOpen.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 1 }, digest: initialDigest, truncated: false,
    });
    await vi.waitFor(() => expect(tasks.localReplicaResult()).toEqual(initialRows));

    client.setAuth({ token: "second-token", identity: { sub: "account-a", iss: "shop" } });
    const secondAuth = sentMessages(socket).filter((message) => message.type === "auth").at(-1)!;
    socket.receive({
      type: "auth.result",
      id: secondAuth.id,
      result: authenticatedResult({ accountId: "account-a" }),
    });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    const secondOpen = sentMessages(socket).filter((message) => message.type === "replica.open").at(-1)!;
    expect(secondOpen).toMatchObject({ hashes: await replicaRowsHashes(initialRows, "id"), fullIntegrity: true });
    const updatedRows = [{ id: "task-1", title: "Working" }];
    const updatedDigest = await replicaHashesDigest(await replicaRowsHashes(updatedRows, "id"));
    socket.receive({
      type: "replica.delta", id: secondOpen.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 2 }, upserts: updatedRows, deleted: [],
    });
    socket.receive({
      type: "replica.ready", id: secondOpen.id, path: collectionRef.path,
      cursor: { epoch: "epoch:test", revision: 2 }, digest: updatedDigest, truncated: false,
    });

    await vi.waitFor(() => {
      expect(tasks.localReplicaResult()).toEqual(updatedRows);
      expect(client.localReplica.entity("tasks", "task-1")).toEqual(updatedRows[0]);
    });
    expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    client.close();
  });

  it("validates each Replica Collection against its own table projection", async () => {
    const tasksRef: FunctionReference = {
      kind: "query",
      path: "tasks.recent",
      delivery: "replica",
      replica: { table: "tasks", key: "_id", columns: ["_id", "name", "workspaceId", "approvalId"] },
    };
    const workplanTasksRef: FunctionReference = {
      kind: "query",
      path: "workplans.tasks",
      delivery: "replica",
      replica: { table: "tasks", key: "_id", columns: ["_id", "name", "workplanId"] },
    };
    const client = new GonvexClient("ws://runtime.test/ws");
    const tasks = client.watchReplica(tasksRef, {});
    const workplanTasks = client.watchReplica(workplanTasksRef, {});
    const socket = latestSocket();
    socket.open();
    socket.receive({ type: "session.ready", replica: testReplicaDirective });
    await vi.waitFor(() => {
      expect(sentMessages(socket).filter((message) => message.type === "replica.open")).toHaveLength(2);
    });
    const opens = sentMessages(socket).filter((message) => message.type === "replica.open");
    const tasksOpen = opens.filter((message) => message.path === tasksRef.path).at(-1)!;
    const workplanTasksOpen = opens.filter((message) => message.path === workplanTasksRef.path).at(-1)!;
    const initialTaskOpenCount = opens.filter((message) => message.path === tasksRef.path).length;
    const initialWorkplanTaskOpenCount = opens.filter((message) => message.path === workplanTasksRef.path).length;
    const taskRows = [{ _id: "task-1", name: "Inspect freezer", workspaceId: "workspace-1", approvalId: "approval-1" }];
    const workplanRows = [{ _id: "task-1", name: "Inspect freezer", workplanId: "workplan-1" }];
    const taskDigest = await replicaHashesDigest(await replicaRowsHashes(taskRows, "_id"));
    const workplanDigest = await replicaHashesDigest(await replicaRowsHashes(workplanRows, "_id"));

    socket.receive({
      type: "replica.snapshot", id: tasksOpen.id, path: tasksRef.path,
      result: taskRows, cursor: { epoch: "epoch:test", revision: 1 }, key: "_id",
    });
    socket.receive({
      type: "replica.snapshot", id: workplanTasksOpen.id, path: workplanTasksRef.path,
      result: workplanRows, cursor: { epoch: "epoch:test", revision: 1 }, key: "_id",
    });
    await vi.waitFor(() => {
      expect(client.localReplica.entity("tasks", "task-1")).toMatchObject({
        workspaceId: "workspace-1",
        approvalId: "approval-1",
        workplanId: "workplan-1",
      });
    });
    socket.receive({
      type: "replica.ready", id: tasksOpen.id, path: tasksRef.path,
      cursor: { epoch: "epoch:test", revision: 1 }, digest: taskDigest, truncated: false,
    });
    socket.receive({
      type: "replica.ready", id: workplanTasksOpen.id, path: workplanTasksRef.path,
      cursor: { epoch: "epoch:test", revision: 1 }, digest: workplanDigest, truncated: false,
    });

    await vi.waitFor(() => {
      expect(client.localReplica.entity("tasks", "task-1")).toEqual({
        _id: "task-1",
        name: "Inspect freezer",
        workspaceId: "workspace-1",
        approvalId: "approval-1",
        workplanId: "workplan-1",
      });
      expect(tasks.localReplicaState()?.freshness).toBe("current");
      expect(workplanTasks.localReplicaState()?.freshness).toBe("current");
    });
    expect(sentMessages(socket).filter((message) => message.type === "replica.open" && message.path === tasksRef.path)).toHaveLength(initialTaskOpenCount);
    expect(sentMessages(socket).filter((message) => message.type === "replica.open" && message.path === workplanTasksRef.path)).toHaveLength(initialWorkplanTaskOpenCount);
    client.close();
  });

  it("runs the generated Live Query plan against normalized cached entities while offline", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const liveRef: FunctionReference = {
      kind: "query",
      path: "tasks.grid",
      delivery: "live",
      live: {
        entity: "tasks",
        key: "id",
        resultPath: ["rows"],
        plan: {
          table: "tasks",
          key: "id",
          columns: ["id", "title", "priority"],
          where: { operator: "eq", column: "priority", value: { argument: "priority" } },
          search: { argument: "search", columns: ["title"] },
          window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 100, count: "exact" },
        },
      },
    };
    const watch = client.watchLiveQuery(liveRef, { priority: "urgent", search: "FREEZER" });
    const socket = latestSocket();
    socket.open();
    const subscription = sentMessages(socket).filter((message) => message.type === "query.subscribe").at(-1);
    expect(subscription).toBeDefined();
    socket.receive({
      type: "query.result",
      id: subscription!.id,
      path: "tasks.grid",
      result: {
        rows: [
          { id: "1", title: "Broken freezer", priority: "urgent" },
          { id: "2", title: "Replace light", priority: "normal" },
        ],
        total: 2,
        offset: 0,
        limit: 2,
      },
      subscriptionRevision: { epoch: "runtime", sequence: 1 },
      windowRevision: "window-1",
    });
    await flushMicrotasks();
    socket.disconnect();
    expect(client.localReplica.freshness()).toBe("offline");

    expect(client.offlineLiveQuery(liveRef, { priority: "urgent", search: "FREEZER" })).toEqual({
      rows: [{ id: "1", title: "Broken freezer", priority: "urgent" }],
      total: 1,
      offset: 0,
      limit: 100,
      completeness: "partial",
      supported: true,
    });
    expect(watch.localLiveQueryResult()).toEqual({
      rows: [{ id: "1", title: "Broken freezer", priority: "urgent" }],
      total: 1,
      offset: 0,
      limit: 100,
    });
    client.close();
  });

  it("ignores invalid JSON messages from the socket", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeLiveQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive("{not json");

    expect(handler).not.toHaveBeenCalled();
  });

  it("sends unsubscribe after a short grace period and removes the handler after in-flight results", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    const unsubscribe = client.subscribeLiveQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    unsubscribe();
    expect(sentMessages(socket).at(-1)).not.toMatchObject({ type: "query.unsubscribe", id });

    socket.receive({ type: "query.result", id, result: "in-flight" });
    expect(handler).toHaveBeenCalledTimes(0);

    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });

    vi.advanceTimersByTime(500);
    socket.receive({ type: "query.result", id, result: "late" });
    expect(handler).toHaveBeenCalledTimes(0);
  });

  it("reuses an orphaned live query within a configured warm retention window", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", {
      querySubscriptionRetentionMs: 30_000,
    });
    const first = vi.fn();
    const second = vi.fn();

    const unsubscribe = client.subscribeLiveQuery(ref, { workspaceId: "workspace-a" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: { count: 4 } });

    unsubscribe();
    vi.advanceTimersByTime(10_000);
    expect(sentMessages(socket).filter((message) => message.type === "query.unsubscribe")).toHaveLength(0);

    client.subscribeLiveQuery(ref, { workspaceId: "workspace-a" }, second);
    await Promise.resolve();

    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: { count: 4 } });
  });

  it("eventually releases a configured warm query that is not revisited", () => {
    const client = new GonvexClient("ws://runtime.test/ws", {
      querySubscriptionRetentionMs: 30_000,
    });
    const unsubscribe = client.subscribeLiveQuery(ref, { workspaceId: "workspace-a" }, vi.fn());
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    unsubscribe();
    vi.advanceTimersByTime(29_999);
    expect(sentMessages(socket).at(-1)).not.toMatchObject({ type: "query.unsubscribe", id });
    vi.advanceTimersByTime(1);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("re-subscribes instead of replaying a cached error when a listener remounts during the grace period", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const firstHandler = vi.fn();

    const unsubscribe = client.subscribeLiveQuery(ref, {}, firstHandler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "query is not implemented" });
    expect(firstHandler).toHaveBeenCalledTimes(1);

    unsubscribe();
    const secondHandler = vi.fn();
    client.subscribeLiveQuery(ref, {}, secondHandler);
    await Promise.resolve();

    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(2);
    expect(secondHandler).not.toHaveBeenCalled();
  });

  it("resolves a one-shot query without creating a subscription", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref, { status: "open" });
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.result", id, result: { count: 2 } });

    await expect(promise).resolves.toEqual({ count: 2 });
    expect(sentMessages(socket)).toHaveLength(1);
    expect(sentMessages(socket)[0]).toMatchObject({ type: "query.call", id });
  });

  it("rejects one-shot queries on query errors", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "boom" });

    await expect(promise).rejects.toThrow("boom");
    expect(sentMessages(socket)).toHaveLength(1);
    expect(sentMessages(socket)[0]).toMatchObject({ type: "query.call", id });
  });

  it("replays an in-flight one-shot query after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref, { status: "open" });
    const firstSocket = latestSocket();
    firstSocket.open();
    const [firstSubscription] = sentMessages(firstSocket);

    firstSocket.disconnect();
    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();

    const [secondSubscription] = sentMessages(secondSocket);
    expect(secondSubscription).toMatchObject({
      type: "query.call",
      id: firstSubscription.id,
      path: "tasks.list",
      args: { status: "open" },
    });
    secondSocket.receive({ type: "query.result", id: firstSubscription.id, result: { count: 3 } });
    await expect(promise).resolves.toEqual({ count: 3 });
  });

  it("replays an auth-queued one-shot query only after reconnect authentication", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token", tenant: "tenant-a" });
    const promise = client.query(ref, { status: "open" });
    const firstSocket = latestSocket();
    firstSocket.open();
    expect(sentMessages(firstSocket)).toHaveLength(1);
    expect(sentMessages(firstSocket)[0]).toMatchObject({ type: "auth" });

    firstSocket.disconnect();
    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    const [secondAuth] = sentMessages(secondSocket);
    expect(secondAuth).toMatchObject({ type: "auth" });
    expect(sentMessages(secondSocket).some((message) => message.type === "query.call")).toBe(false);

    secondSocket.receive({ type: "auth.result", id: secondAuth.id, result: authenticatedResult({ accountId: "account-a" }) });
    await vi.waitFor(() => {
      expect(sentMessages(secondSocket).filter((message) => message.type === "query.call")).toHaveLength(1);
    });
    const calls = sentMessages(secondSocket).filter((message) => message.type === "query.call");
    expect(calls).toHaveLength(1);
    secondSocket.receive({ type: "query.result", id: calls[0].id, result: { count: 4 } });
    await expect(promise).resolves.toEqual({ count: 4 });
  });

  it("rejects unresolved one-shot queries when explicitly closed", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref);

    client.close();

    await expect(promise).rejects.toThrow("Gonvex client was closed");
  });

  it("resolves reducers and actions from matching response types", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" }, { title: "Ship" });
    const action = client.action({ kind: "action", path: "jobs.run" }, { id: "job_1" });
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    expect(messages[0]).toMatchObject({ type: "reducer.call", path: "tasks.create", args: { title: "Ship" } });
    expect(messages[1]).toMatchObject({ type: "action.call", path: "jobs.run", args: { id: "job_1" } });

    socket.receive({ type: "reducer.result", id: messages[0].id, result: { id: "task_1" } });
    socket.receive({ type: "action.result", id: messages[1].id, result: "queued" });

    await expect(reducer).resolves.toEqual({ id: "task_1" });
    await expect(action).resolves.toBe("queued");
  });

  it("reports browser and device telemetry for received reducer results", async () => {
    vi.stubGlobal("performance", { timeOrigin: 1_000, now: vi.fn(() => 25.5) });
    vi.stubGlobal("navigator", {
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
      platform: "Win32",
      language: "en-US",
      hardwareConcurrency: 12,
      maxTouchPoints: 0,
    });
    vi.stubGlobal("innerWidth", 1440);
    vi.stubGlobal("innerHeight", 900);
    const client = new GonvexClient("ws://runtime.test/ws", { telemetry: true });
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" }, { title: "Ship" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    socket.receive({
      type: "reducer.result",
      id: call.id,
      result: { id: "task_1" },
      trace: {
        clientSentAtMs: call.trace.clientSentAtMs,
        serverReducerCommittedAtMs: 1_010.25,
        serverCompletedAtMs: 1_012.5,
      },
    });

    await expect(reducer).resolves.toEqual({ id: "task_1" });
    const telemetry = sentMessages(socket).find((message) => message.type === "telemetry.event");
    expect(telemetry).toMatchObject({
      kind: "reducer",
      path: "tasks.create",
      outcome: "ok",
      clientReceivedAtMs: 1_025.5,
      device: {
        browserName: "Chrome",
        browserVersion: "126.0.0.0",
        deviceType: "desktop",
        platform: "Win32",
        viewportWidth: 1440,
        viewportHeight: 900,
      },
    });
  });

  it("rejects reducers and actions from matching error response types", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    const action = client.action({ kind: "action", path: "jobs.run" });
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    socket.receive({ type: "reducer.error", id: messages[0].id, error: "reducer failed" });
    socket.receive({ type: "action.error", id: messages[1].id, error: "action failed" });

    await expect(reducer).rejects.toThrow("reducer failed");
    await expect(action).rejects.toThrow("action failed");
  });

  it("automatically reports failed Gonvex operations when error reporting is enabled", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop", tenant: "acme", errorReporting: { release: "1.2.3", captureGlobalErrors: false } });
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();
    const [auth] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: auth.id, result: authenticatedResult({ ok: true }) });
    await vi.waitFor(() => {
      expect(sentMessages(socket).some((message) => message.type === "reducer.call")).toBe(true);
    });
    const call = sentMessages(socket).at(-1)!;
    socket.receive({ type: "reducer.error", id: call.id, error: "permission denied" });
    await expect(reducer).rejects.toThrow("permission denied");
    expect(captureReportedError).toHaveBeenCalledWith(expect.objectContaining({ message: "permission denied" }), expect.objectContaining({
      gonvexOperation: expect.objectContaining({ type: "reducer", path: "tasks.create" }),
    }));
  });

  it("rejects one-shot queries that never receive a response with a typed timeout error", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref, { status: "open" });
    latestSocket().open();

    vi.advanceTimersByTime(20_000);

    await expect(promise).rejects.toMatchObject({
      name: "GonvexClientError",
      code: "timeout",
      operation: "query",
      path: "tasks.list",
    });
  });

  it("honors per-call timeout overrides for one-shot queries", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref, {}, { timeoutMs: 1_000 });
    latestSocket().open();

    vi.advanceTimersByTime(999);
    let settled = false;
    void promise.catch(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    vi.advanceTimersByTime(1);
    await expect(promise).rejects.toMatchObject({ code: "timeout" });
  });

  it("rejects reducers that never receive a response with a typed timeout error", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" }, { title: "Ship" });
    latestSocket().open();

    vi.advanceTimersByTime(20_000);

    await expect(reducer).rejects.toMatchObject({
      name: "GonvexClientError",
      code: "timeout",
      operation: "reducer",
      path: "tasks.create",
    });
  });

  it("gives actions a longer default timeout than reducers", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const action = client.action({ kind: "action", path: "jobs.run" });
    latestSocket().open();

    vi.advanceTimersByTime(59_999);
    let settled = false;
    void action.catch(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    vi.advanceTimersByTime(1);
    await expect(action).rejects.toMatchObject({ code: "timeout", operation: "action" });
  });

  it("ignores late responses after an operation timed out", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    vi.advanceTimersByTime(20_000);
    await expect(reducer).rejects.toMatchObject({ code: "timeout" });

    expect(() => socket.receive({ type: "reducer.result", id: call.id, result: { id: "task_1" } })).not.toThrow();
  });

  it("fails pending reducers closed when the socket disconnects and never replays them", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" }, { title: "Ship" });
    const firstSocket = latestSocket();
    firstSocket.open();
    expect(sentMessages(firstSocket)[0]).toMatchObject({ type: "reducer.call", path: "tasks.create" });

    firstSocket.disconnect();

    await expect(reducer).rejects.toMatchObject({
      name: "GonvexClientError",
      code: "disconnected",
      operation: "reducer",
      path: "tasks.create",
    });

    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    expect(sentMessages(secondSocket).some((message) => message.type === "reducer.call")).toBe(false);
  });

  it("fails pending actions closed when the socket disconnects", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const action = client.action({ kind: "action", path: "jobs.run" });
    const socket = latestSocket();
    socket.open();

    socket.disconnect();

    await expect(action).rejects.toMatchObject({ code: "disconnected", operation: "action" });
  });

  it("fails auth-queued reducers closed on disconnect instead of firing them after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token" });
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    const firstSocket = latestSocket();
    firstSocket.open();
    // Only auth was sent; the reducer is still queued behind authentication.
    expect(sentMessages(firstSocket)).toMatchObject([{ type: "auth" }]);

    firstSocket.disconnect();
    await expect(reducer).rejects.toMatchObject({ code: "disconnected", operation: "reducer" });

    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    const [secondAuth] = sentMessages(secondSocket);
    secondSocket.receive({ type: "auth.result", id: secondAuth.id, result: authenticatedResult({ accountId: "account-a" }) });
    expect(sentMessages(secondSocket).some((message) => message.type === "reducer.call")).toBe(false);
  });

  it("rejects pending reducers with a typed closed error on explicit close", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    latestSocket().open();

    client.close();

    await expect(reducer).rejects.toMatchObject({ code: "closed", operation: "reducer" });
  });

  it("rejects server reducer errors with a typed server error", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    socket.receive({ type: "reducer.error", id: call.id, error: "permission denied" });

    const error = await reducer.then(
      () => {
        throw new Error("expected rejection");
      },
      (cause: unknown) => cause,
    );
    expect(error).toBeInstanceOf(GonvexClientError);
    expect(error).toMatchObject({ code: "server", operation: "reducer", message: "permission denied" });
  });

  it("tracks connection state across connect, disconnect, and reconnect", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    expect(client.connectionState()).toMatchObject({
      isWebSocketConnected: false,
      hasEverConnected: false,
      connectionCount: 0,
      connectionRetries: 0,
    });

    client.connect();
    expect(client.connectionState().isWebSocketConnected).toBe(false);

    const firstSocket = latestSocket();
    firstSocket.open();
    expect(client.connectionState()).toMatchObject({
      isWebSocketConnected: true,
      hasEverConnected: true,
      connectionCount: 1,
      connectionRetries: 0,
    });

    firstSocket.disconnect();
    expect(client.connectionState()).toMatchObject({
      isWebSocketConnected: false,
      hasEverConnected: true,
      connectionCount: 1,
      connectionRetries: 1,
    });

    vi.advanceTimersByTime(250);
    latestSocket().open();
    expect(client.connectionState()).toMatchObject({
      isWebSocketConnected: true,
      connectionCount: 2,
      connectionRetries: 0,
    });
  });

  it("notifies connection state subscribers and supports unsubscribing", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const states: Array<{ isWebSocketConnected: boolean }> = [];
    const unsubscribe = client.subscribeToConnectionState((state) => states.push(state));

    client.connect();
    const socket = latestSocket();
    socket.open();
    expect(states.at(-1)).toMatchObject({ isWebSocketConnected: true });

    socket.disconnect();
    expect(states.at(-1)).toMatchObject({ isWebSocketConnected: false });

    const count = states.length;
    unsubscribe();
    vi.advanceTimersByTime(250);
    latestSocket().open();
    expect(states).toHaveLength(count);
  });

  it("reports inflight requests while reducers are pending", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const reducer = client.reducer({ kind: "reducer", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();

    expect(client.connectionState()).toMatchObject({ hasInflightRequests: true, inflightReducers: 1 });

    const [call] = sentMessages(socket);
    socket.receive({ type: "reducer.result", id: call.id, result: { id: "task_1" } });
    await reducer;

    expect(client.connectionState()).toMatchObject({ hasInflightRequests: false, inflightReducers: 0 });
  });

  it("re-requests an active Live Query via retryLiveQuery", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();
    client.subscribeLiveQuery(ref, { status: "open" }, handler);
    const socket = latestSocket();
    socket.open();
    const [subscribe] = sentMessages(socket);
    socket.receive({ type: "query.error", id: subscribe.id, error: "boom" });

    client.retryLiveQuery(ref, { status: "open" });

    const subscribes = sentMessages(socket).filter((message) => message.type === "query.subscribe");
    expect(subscribes).toHaveLength(2);
    expect(subscribes[1]).toMatchObject({ id: subscribe.id, path: "tasks.list", args: { status: "open" } });

    socket.receive({ type: "query.result", id: subscribe.id, result: [{ id: "recovered" }] });
    await flushMicrotasks();
    expect(handler).toHaveBeenLastCalledWith({ type: "query.result", id: subscribe.id, result: [{ id: "recovered" }] });
  });

  it("ignores retryLiveQuery for queries without subscribers", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.retryLiveQuery(ref, { status: "open" });

    expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(false);
  });

  it("drops handlers and closes the socket when closed", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeLiveQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    client.close();
    socket.receive({ type: "query.result", id, result: "ignored" });

    expect(socket.readyState).toBe(FakeWebSocket.CLOSED);
    expect(handler).not.toHaveBeenCalled();
  });

  it("routes typed Control Plane queries over the existing socket", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop", token: "gvx_session_test" });
    const pending = client.query(control.accounts.me, {});
    const socket = latestSocket();
    socket.open();
    const auth = sentMessages(socket).find((message) => message.type === "auth");
    expect(auth).toMatchObject({ project: "shop", controlOnly: true });
    socket.receive({ type: "auth.result", id: auth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const call = sentMessages(socket).find((message) => message.type === "query.call");
    expect(call).toMatchObject({ path: "control.accounts.me", scope: "control" });
    socket.receive({ type: "query.result", id: call.id, result: { id: "acct-1", email: "owner@example.test", name: "Owner", avatarUrl: "" } });
    await expect(pending).resolves.toMatchObject({ id: "acct-1" });
  });

  it("binds signed-out clients to public Control Plane calls without an auth error", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop" });
    const onAuthError = vi.fn();
    client.onAuthError(onAuthError);
    const pending = client.query(control.auth.publicSettings, {});
    const socket = latestSocket();
    socket.open();
    const auth = sentMessages(socket).find((message) => message.type === "auth");
    expect(auth).toMatchObject({ project: "shop", controlOnly: true });
    expect(auth.token).toBeUndefined();
    socket.receive({ type: "auth.result", id: auth.id, result: { accountId: "", projectId: "shop", tenantId: "" } });
    const call = sentMessages(socket).find((message) => message.type === "query.call");
    expect(call).toMatchObject({ path: "control.auth.publicSettings", scope: "control" });
    socket.receive({ type: "query.result", id: call.id, result: { providers: ["google"] } });
    await expect(pending).resolves.toEqual({ providers: ["google"] });
    expect(onAuthError).not.toHaveBeenCalled();
  });

  it("replays a Control Plane reducer with the same idempotency key after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop", token: "gvx_session_test" });
    const pending = client.reducer(control.accounts.updatePassword, { currentPassword: "old-password", newPassword: "a-new-password" });
    const first = latestSocket();
    first.open();
    const firstAuth = sentMessages(first).find((message) => message.type === "auth");
    first.receive({ type: "auth.result", id: firstAuth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const original = sentMessages(first).find((message) => message.type === "reducer.call");
    first.disconnect();

    await vi.advanceTimersByTimeAsync(250);
    const second = latestSocket();
    second.open();
    const secondAuth = sentMessages(second).find((message) => message.type === "auth");
    second.receive({ type: "auth.result", id: secondAuth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const replay = sentMessages(second).find((message) => message.type === "reducer.call");
    expect(replay).toMatchObject({ id: original.id, idempotencyKey: original.idempotencyKey, scope: "control" });
    second.receive({ type: "reducer.result", id: replay.id, result: { updated: true } });
    await expect(pending).resolves.toEqual({ updated: true });
  });

  it("replays an in-flight Control Plane query after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop", token: "gvx_session_test" });
    const pending = client.query(control.accounts.me, {});
    const first = latestSocket();
    first.open();
    const firstAuth = sentMessages(first).find((message) => message.type === "auth");
    first.receive({ type: "auth.result", id: firstAuth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const original = sentMessages(first).find((message) => message.type === "query.call");
    first.disconnect();

    await vi.advanceTimersByTimeAsync(250);
    const second = latestSocket();
    second.open();
    const secondAuth = sentMessages(second).find((message) => message.type === "auth");
    second.receive({ type: "auth.result", id: secondAuth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const replay = sentMessages(second).find((message) => message.type === "query.call");
    expect(replay).toMatchObject({ id: original.id, scope: "control", path: "control.accounts.me" });
    second.receive({ type: "query.result", id: replay.id, result: { id: "acct-1", email: "a@example.test", name: "A", avatarUrl: "" } });
    await expect(pending).resolves.toMatchObject({ id: "acct-1" });
  });

  it("replays a Control Plane action with the same idempotency key after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop", token: "gvx_session_test" });
    const pending = client.action(control.auth.refreshSession, { refreshToken: "refresh-1" });
    const first = latestSocket();
    first.open();
    const firstAuth = sentMessages(first).find((message) => message.type === "auth");
    first.receive({ type: "auth.result", id: firstAuth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const original = sentMessages(first).find((message) => message.type === "action.call");
    first.disconnect();

    await vi.advanceTimersByTimeAsync(250);
    const second = latestSocket();
    second.open();
    const secondAuth = sentMessages(second).find((message) => message.type === "auth");
    second.receive({ type: "auth.result", id: secondAuth.id, result: { accountId: "acct-1", projectId: "shop", tenantId: "" } });
    const replay = sentMessages(second).find((message) => message.type === "action.call");
    expect(replay).toMatchObject({ id: original.id, idempotencyKey: original.idempotencyKey, scope: "control" });
    second.receive({ type: "action.result", id: replay.id, result: { accessToken: "next" } });
    await expect(pending).resolves.toEqual({ accessToken: "next" });
  });

  it("rejects an in-flight Control Plane call instead of replaying it in another tenant", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", {
      project: "shop", tenant: "tenant-a", token: "gvx_session_a", identity: { sub: "acct-1", iss: "shop" },
    });
    const pending = client.reducer(control.tenants.updateTimezone, { timezone: "UTC" });
    const rejected = expect(pending).rejects.toMatchObject({ code: "superseded" });
    const socket = latestSocket();
    socket.open();
    const auth = sentMessages(socket).find((message) => message.type === "auth");
    socket.receive({ type: "auth.result", id: auth.id, result: authenticatedResult({ accountId: "acct-1", projectId: "shop", tenantId: "tenant-a" }) });
    await vi.waitFor(() => {
      expect(sentMessages(socket).some((message) => message.type === "reducer.call")).toBe(true);
    });

    client.setAuth({ tenant: "tenant-b", token: "gvx_session_b", identity: { sub: "acct-1", iss: "shop" } });

    await rejected;
  });
});
