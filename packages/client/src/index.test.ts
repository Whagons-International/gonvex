import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConvexReactClient, GonvexClient, GonvexClientError, type FunctionReference } from "./index";

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

const ref: FunctionReference = { kind: "query", path: "tasks.list" };

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
	it("batches query subscribes into one frame when the server advertises queryBatch", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const socket = (client.connect(), latestSocket());
		socket.open();
		socket.receive({ type: "session.ready", capabilities: { queryBatch: 1 } });

		client.subscribeQuery(ref, {}, vi.fn());
		client.subscribeQuery({ kind: "query", path: "teams.list" }, {}, vi.fn());
		await vi.advanceTimersByTimeAsync(0);

		const frames = sentMessages(socket);
		expect(frames.filter((frame) => frame.type === "query.subscribe")).toHaveLength(0);
		const batches = frames.filter((frame) => frame.type === "query.subscribeMany");
		expect(batches).toHaveLength(1);
		expect(batches[0].subscribes.map((subscribe: { path: string }) => subscribe.path))
			.toEqual(["tasks.list", "teams.list"]);
	});

	it("flushes mutationMany as one frame and settles entries independently", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: { mutationBatch: 1 } });

		const outcome = client.mutationMany([
			{ ref: { kind: "mutation", path: "tasks.create" }, args: { name: "a" } },
			{ ref: { kind: "mutation", path: "tasks.create" }, args: { name: "b" } },
		]);
		await vi.advanceTimersByTimeAsync(0);

		const batches = sentMessages(socket).filter((frame) => frame.type === "mutation.callMany");
		expect(batches).toHaveLength(1);
		expect(batches[0].calls).toHaveLength(2);
		const [first, second] = batches[0].calls;
		socket.receive({ type: "mutation.result", id: first.id, path: first.path, result: "id-a" });
		socket.receive({ type: "mutation.error", id: second.id, path: second.path, error: "boom" });

		const results = await outcome;
		expect(results[0]).toEqual({ status: "ok", result: "id-a" });
		expect(results[1]).toMatchObject({ status: "error" });
		expect((results[1] as { error: GonvexClientError }).error.message).toBe("boom");
	});

	it("falls back to sequential mutations when mutationBatch is not advertised", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: {} });

		const outcome = client.mutationMany([
			{ ref: { kind: "mutation", path: "tasks.create" }, args: { name: "a" } },
			{ ref: { kind: "mutation", path: "tasks.create" }, args: { name: "b" } },
		]);
		await vi.advanceTimersByTimeAsync(0);

		let calls = sentMessages(socket).filter((frame) => frame.type === "mutation.call");
		expect(calls).toHaveLength(1);
		socket.receive({ type: "mutation.result", id: calls[0].id, path: calls[0].path, result: "id-a" });
		await vi.advanceTimersByTimeAsync(0);

		calls = sentMessages(socket).filter((frame) => frame.type === "mutation.call");
		expect(calls).toHaveLength(2);
		socket.receive({ type: "mutation.result", id: calls[1].id, path: calls[1].path, result: "id-b" });

		const results = await outcome;
		expect(results).toEqual([
			{ status: "ok", result: "id-a" },
			{ status: "ok", result: "id-b" },
		]);
	});

	it("sends the outbox idempotency key on replayable mutations and omits it on one-shots", async () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		client.connect();
		const socket = latestSocket();
		socket.open();
		socket.receive({ type: "session.ready", capabilities: {} });

		const optimistic = client.mutation(
			{ kind: "mutation", path: "tasks.update" },
			{ id: "a", title: "T" },
			{ optimistic: [{ collection: "tasks.list", rowId: "a", op: "patch", fields: { title: "T" } }] },
		);
		await vi.advanceTimersByTimeAsync(0);
		const [replayable] = sentMessages(socket).filter((frame) => frame.type === "mutation.call");
		expect(replayable.idempotencyKey).toBe(replayable.id);
		socket.receive({ type: "mutation.result", id: replayable.id, path: replayable.path, result: null });
		await optimistic;

		const oneShot = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "x" });
		await vi.advanceTimersByTimeAsync(0);
		const oneShotCall = sentMessages(socket).filter((frame) => frame.type === "mutation.call").at(-1);
		expect(oneShotCall.idempotencyKey).toBeUndefined();
		socket.receive({ type: "mutation.result", id: oneShotCall.id, path: oneShotCall.path, result: "id-x" });
		await expect(oneShot).resolves.toBe("id-x");
	});

	it("rejects stale revisions and advances progress without notifying listeners", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeQuery(ref, {}, handler);
		const socket = latestSocket();
		socket.open();
		const [{ id }] = sentMessages(socket);
		socket.receive({ type: "query.result", id, result: [{ id: "a", title: "new" }], subscriptionRevision: { epoch: "runtime-a", sequence: 2 } });
		socket.receive({ type: "query.result", id, result: [{ id: "a", title: "old" }], subscriptionRevision: { epoch: "runtime-a", sequence: 1 } });
		socket.receive({ type: "query.progress", id, throughRevision: { epoch: "runtime-a", sequence: 3 } });
		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler.mock.calls[0][0].result[0].title).toBe("new");
	});

	it("requests an authoritative snapshot when progress arrives without a local result", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeQuery(ref, {}, handler);
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

	it("applies keyed patches only to the matching base revision", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeQuery(ref, {}, handler);
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

	it("atomically applies keyed patches to object collections", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeQuery(ref, {}, handler);
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

	it("applies compact keyed prepend order deltas", () => {
		const client = new GonvexClient("ws://runtime.test/ws");
		const handler = vi.fn();
		client.subscribeQuery(ref, {}, handler);
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
		expect(handler.mock.calls[1][0]).toMatchObject({
			type: "query.result",
			result: [{ id: "c" }, { id: "b" }, { id: "a" }],
		});
	});

  it("converts http runtime URLs to websocket URLs for ConvexReactClient compatibility", () => {
    const client = new ConvexReactClient("https://runtime.example.com/");
    client.connect();

    expect(latestSocket().url).toBe("wss://runtime.example.com/ws");
  });

  it("keeps explicit websocket URLs unchanged", () => {
    const client = new ConvexReactClient("ws://localhost:8080/ws");
    client.connect();

    expect(latestSocket().url).toBe("ws://localhost:8080/ws");
  });

  it("reuses an existing connecting socket instead of opening duplicates", () => {
    const client = new GonvexClient("ws://runtime.test/ws");

    client.connect();
    client.connect();

    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it("reconnects and restores live query subscriptions after a socket closes", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, handler);
    const firstSocket = latestSocket();
    firstSocket.open();
    const [firstSubscription] = sentMessages(firstSocket);
    firstSocket.receive({ type: "query.result", id: firstSubscription.id, result: ["before"] });

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

    secondSocket.receive({ type: "query.result", id: firstSubscription.id, result: ["after"] });
    expect(handler).toHaveBeenLastCalledWith({
      type: "query.result",
      id: firstSubscription.id,
      result: ["after"],
    });
  });

  it("reauthenticates before restoring subscriptions after reconnect", () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token", tenant: "tenant-a" });

    client.subscribeQuery(ref, {}, () => undefined);
    const firstSocket = latestSocket();
    firstSocket.open();
    const [firstAuth] = sentMessages(firstSocket);
    firstSocket.receive({ type: "auth.result", id: firstAuth.id, result: { userId: "user-a" } });

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
    secondSocket.receive({ type: "auth.result", id: sentMessages(secondSocket)[0].id, result: { userId: "user-a" } });
    expect(sentMessages(secondSocket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
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

    client.subscribeQuery(ref, { status: "open" }, () => undefined);
    const socket = latestSocket();
    expect(socket.sent).toHaveLength(0);

    socket.open();

    expect(sentMessages(socket)).toMatchObject([
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("sends auth before queued messages when the socket opens", () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token", tenant: "tenant-a" });

    client.subscribeQuery(ref, { status: "open" }, () => undefined);
    const socket = latestSocket();
    socket.open();

    expect(sentMessages(socket)).toMatchObject([
      {
        type: "auth",
        token: "session-token",
        tenant: "tenant-a",
        capabilities: { syncReadyMany: 1, syncWatermark: 1 },
      },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: { userId: "user-a", tenantId: "tenant-a" } });

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "session-token", tenant: "tenant-a" },
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("identifies a project before signed-out queries so project auth can be enforced", () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "secure-app" });

    client.subscribeQuery(ref, {}, () => undefined);
    const socket = latestSocket();
    socket.open();

    expect(sentMessages(socket)).toMatchObject([{ type: "auth", project: "secure-app" }]);
    expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(false);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.error", id: authID, error: "a Gonvex app session is required" });
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", path: "tasks.list" });
  });

  it("queues subscription messages while an auth update is in flight", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.setAuth({ token: "next-token", tenant: "tenant-b" });
    client.subscribeQuery(ref, { status: "open" }, () => undefined);

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "next-token", tenant: "tenant-b" },
    ]);

    const [{ id: authID }] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: authID, result: { userId: "user-b", tenantId: "tenant-b" } });

    expect(sentMessages(socket)).toMatchObject([
      { type: "auth", token: "next-token", tenant: "tenant-b" },
      { type: "query.subscribe", path: "tasks.list", args: { status: "open" } },
    ]);
  });

  it("sends auth updates on an open socket", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.setAuth({ token: "next-token", tenant: "tenant-b" });

    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "auth", token: "next-token", tenant: "tenant-b" });
  });

  it("fetches a token through the installed fetcher before sending auth", async () => {
    const fetchToken = vi.fn(async () => "fresh-token");
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });

    client.subscribeQuery(ref, {}, vi.fn());
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
    socket.receive({ type: "auth.result", id: authID, result: { userId: "user-a" } });
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

  it("force-refreshes the token and re-sends auth when the server rejects it", async () => {
    let current = "expired-token";
    const fetchToken = vi.fn(async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
      if (forceRefreshToken) current = "fresh-token";
      return current;
    });
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });

    client.subscribeQuery(ref, {}, vi.fn());
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
    socket.receive({ type: "auth.result", id: auths[1].id, result: { userId: "user-a" } });
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", path: "tasks.list" });
  });

  it("gives up and notifies onAuthError when the forced refresh returns the rejected token", async () => {
    const fetchToken = vi.fn(async () => "always-bad");
    const client = new GonvexClient("ws://runtime.test/ws", { tenant: "tenant-a", fetchToken });
    const onAuthError = vi.fn();
    client.onAuthError(onAuthError);

    client.subscribeQuery(ref, {}, vi.fn());
    const socket = latestSocket();
    socket.open();
    await vi.advanceTimersByTimeAsync(0);
    const [firstAuth] = sentMessages(socket);

    socket.receive({ type: "auth.error", id: firstAuth.id, error: "token expired" });
    await vi.advanceTimersByTimeAsync(0);

    // Re-sending the very token the server just refused would loop forever.
    expect(sentMessages(socket).filter((message) => message.type === "auth")).toHaveLength(1);
    expect(onAuthError).toHaveBeenCalledWith("token expired");
    // The session degrades to the unauthenticated flow: queued messages flush.
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.subscribe", path: "tasks.list" });
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

  it("routes query subscription results to the matching handler", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(handler).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"] });
  });

  it("coalesces identical query subscriptions and fans out results", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, first);
    client.subscribeQuery(ref, { status: "open" }, second);
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    expect(messages.filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    const [{ id }] = messages;
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(first).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"] });
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"] });
  });

  it("replays the latest result to a late joiner without resubscribing", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"], reason: "initial" });
    expect(first).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"], reason: "initial" });

    // A component mounting after the initial result must still receive the cached value;
    // the coalesced subscription only gets `initial` once from the server.
    client.subscribeQuery(ref, { status: "open" }, second);
    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    expect(second).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: ["task"], reason: "initial" });
  });

  it("does not replay a stale result to a late joiner that unsubscribes synchronously", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    client.subscribeQuery(ref, { status: "open" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    const unsubscribeSecond = client.subscribeQuery(ref, { status: "open" }, second);
    unsubscribeSecond();
    await Promise.resolve();
    expect(second).not.toHaveBeenCalled();
  });

  it("keeps a coalesced query subscribed until the last listener leaves", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const first = vi.fn();
    const second = vi.fn();

    const unsubscribeFirst = client.subscribeQuery(ref, { status: "open" }, first);
    const unsubscribeSecond = client.subscribeQuery(ref, { status: "open" }, second);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    unsubscribeFirst();
    vi.advanceTimersByTime(300);
    expect(sentMessages(socket).filter((message) => message.type === "query.unsubscribe")).toHaveLength(0);

    socket.receive({ type: "query.result", id, result: ["task"] });
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);

    unsubscribeSecond();
    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("keeps network telemetry disabled unless explicitly enabled", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(sentMessages(socket).some((message) => message.type === "telemetry.event")).toBe(false);
  });

  it("supports Convex-compatible watchQuery updates", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const watch = client.watchQuery<string[]>(ref, { status: "open" });
    const onUpdate = vi.fn();
    const offUpdate = watch.onUpdate(onUpdate);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.result", id, result: ["task"] });

    expect(onUpdate).toHaveBeenCalledTimes(1);
    expect(watch.localQueryResult()).toEqual(["task"]);

    offUpdate();
    vi.advanceTimersByTime(250);
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("throws the latest watchQuery error from localQueryResult", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const watch = client.watchQuery(ref);
    const onUpdate = vi.fn();
    watch.onUpdate(onUpdate);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "watch failed" });

    expect(onUpdate).toHaveBeenCalledTimes(1);
    expect(() => watch.localQueryResult()).toThrow("watch failed");
  });

  it("ignores invalid JSON messages from the socket", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    socket.receive("{not json");

    expect(handler).not.toHaveBeenCalled();
  });

  it("sends unsubscribe after a short grace period and removes the handler after in-flight results", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    const unsubscribe = client.subscribeQuery(ref, {}, handler);
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

    const unsubscribe = client.subscribeQuery(ref, { workspaceId: "workspace-a" }, first);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);
    socket.receive({ type: "query.result", id, result: { count: 4 } });

    unsubscribe();
    vi.advanceTimersByTime(10_000);
    expect(sentMessages(socket).filter((message) => message.type === "query.unsubscribe")).toHaveLength(0);

    client.subscribeQuery(ref, { workspaceId: "workspace-a" }, second);
    await Promise.resolve();

    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(1);
    expect(second).toHaveBeenCalledWith({ type: "query.result", id, result: { count: 4 } });
  });

  it("eventually releases a configured warm query that is not revisited", () => {
    const client = new GonvexClient("ws://runtime.test/ws", {
      querySubscriptionRetentionMs: 30_000,
    });
    const unsubscribe = client.subscribeQuery(ref, { workspaceId: "workspace-a" }, vi.fn());
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

    const unsubscribe = client.subscribeQuery(ref, {}, firstHandler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "query is not implemented" });
    expect(firstHandler).toHaveBeenCalledTimes(1);

    unsubscribe();
    const secondHandler = vi.fn();
    client.subscribeQuery(ref, {}, secondHandler);
    await Promise.resolve();

    expect(sentMessages(socket).filter((message) => message.type === "query.subscribe")).toHaveLength(2);
    expect(secondHandler).not.toHaveBeenCalled();
  });

  it("resolves one-shot queries and unsubscribes after the first result", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref, { status: "open" });
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.result", id, result: { count: 2 } });

    await expect(promise).resolves.toEqual({ count: 2 });
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
  });

  it("rejects one-shot queries on query errors", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    socket.receive({ type: "query.error", id, error: "boom" });

    await expect(promise).rejects.toThrow("boom");
    expect(sentMessages(socket).at(-1)).toMatchObject({ type: "query.unsubscribe", id });
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
      type: "query.subscribe",
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
    expect(sentMessages(secondSocket).some((message) => message.type === "query.subscribe")).toBe(false);

    secondSocket.receive({ type: "auth.result", id: secondAuth.id, result: { userId: "user-a" } });
    const subscriptions = sentMessages(secondSocket).filter((message) => message.type === "query.subscribe");
    expect(subscriptions).toHaveLength(1);
    secondSocket.receive({ type: "query.result", id: subscriptions[0].id, result: { count: 4 } });
    await expect(promise).resolves.toEqual({ count: 4 });
  });

  it("rejects unresolved one-shot queries when explicitly closed", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const promise = client.query(ref);

    client.close();

    await expect(promise).rejects.toThrow("Gonvex client was closed");
  });

  it("resolves mutations and actions from matching response types", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "Ship" });
    const action = client.action({ kind: "action", path: "jobs.run" }, { id: "job_1" });
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    expect(messages[0]).toMatchObject({ type: "mutation.call", path: "tasks.create", args: { title: "Ship" } });
    expect(messages[1]).toMatchObject({ type: "action.call", path: "jobs.run", args: { id: "job_1" } });

    socket.receive({ type: "mutation.result", id: messages[0].id, result: { id: "task_1" } });
    socket.receive({ type: "action.result", id: messages[1].id, result: "queued" });

    await expect(mutation).resolves.toEqual({ id: "task_1" });
    await expect(action).resolves.toBe("queued");
  });

  it("reports browser and device telemetry for received mutation results", async () => {
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
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "Ship" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    socket.receive({
      type: "mutation.result",
      id: call.id,
      result: { id: "task_1" },
      trace: {
        clientSentAtMs: call.trace.clientSentAtMs,
        serverMutationCommittedAtMs: 1_010.25,
        serverCompletedAtMs: 1_012.5,
      },
    });

    await expect(mutation).resolves.toEqual({ id: "task_1" });
    const telemetry = sentMessages(socket).find((message) => message.type === "telemetry.event");
    expect(telemetry).toMatchObject({
      kind: "mutation",
      path: "tasks.create",
      outcome: "ok",
      clientReceivedAtMs: 1_025.5,
      device: {
        deliveryDiagnostics: {
          messageDecodeMs: expect.any(Number),
          messageProcessingMs: expect.any(Number),
          longTaskSupported: expect.any(Boolean),
          bufferedAmount: 0,
        },
        browserName: "Chrome",
        browserVersion: "126.0.0.0",
        deviceType: "desktop",
        platform: "Win32",
        viewportWidth: 1440,
        viewportHeight: 900,
      },
    });
  });

  it("rejects mutations and actions from matching error response types", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const action = client.action({ kind: "action", path: "jobs.run" });
    const socket = latestSocket();
    socket.open();
    const messages = sentMessages(socket);

    socket.receive({ type: "mutation.error", id: messages[0].id, error: "mutation failed" });
    socket.receive({ type: "action.error", id: messages[1].id, error: "action failed" });

    await expect(mutation).rejects.toThrow("mutation failed");
    await expect(action).rejects.toThrow("action failed");
  });

  it("automatically reports failed Gonvex operations when error reporting is enabled", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { project: "shop", tenant: "acme", errorReporting: { release: "1.2.3", captureGlobalErrors: false } });
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();
    const [auth] = sentMessages(socket);
    socket.receive({ type: "auth.result", id: auth.id, result: { ok: true } });
    const call = sentMessages(socket).at(-1)!;
    socket.receive({ type: "mutation.error", id: call.id, error: "permission denied" });
    await expect(mutation).rejects.toThrow("permission denied");
    expect(captureReportedError).toHaveBeenCalledWith(expect.objectContaining({ message: "permission denied" }), expect.objectContaining({
      gonvexOperation: expect.objectContaining({ type: "mutation", path: "tasks.create" }),
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

  it("rejects mutations that never receive a response with a typed timeout error", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "Ship" });
    latestSocket().open();

    vi.advanceTimersByTime(20_000);

    await expect(mutation).rejects.toMatchObject({
      name: "GonvexClientError",
      code: "timeout",
      operation: "mutation",
      path: "tasks.create",
    });
  });

  it("gives actions a longer default timeout than mutations", async () => {
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
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    vi.advanceTimersByTime(20_000);
    await expect(mutation).rejects.toMatchObject({ code: "timeout" });

    expect(() => socket.receive({ type: "mutation.result", id: call.id, result: { id: "task_1" } })).not.toThrow();
  });

  it("fails pending mutations closed when the socket disconnects and never replays them", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" }, { title: "Ship" });
    const firstSocket = latestSocket();
    firstSocket.open();
    expect(sentMessages(firstSocket)[0]).toMatchObject({ type: "mutation.call", path: "tasks.create" });

    firstSocket.disconnect();

    await expect(mutation).rejects.toMatchObject({
      name: "GonvexClientError",
      code: "disconnected",
      operation: "mutation",
      path: "tasks.create",
    });

    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    expect(sentMessages(secondSocket).some((message) => message.type === "mutation.call")).toBe(false);
  });

  it("fails pending actions closed when the socket disconnects", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const action = client.action({ kind: "action", path: "jobs.run" });
    const socket = latestSocket();
    socket.open();

    socket.disconnect();

    await expect(action).rejects.toMatchObject({ code: "disconnected", operation: "action" });
  });

  it("fails auth-queued mutations closed on disconnect instead of firing them after reconnect", async () => {
    const client = new GonvexClient("ws://runtime.test/ws", { token: "session-token" });
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const firstSocket = latestSocket();
    firstSocket.open();
    // Only auth was sent; the mutation is still queued behind authentication.
    expect(sentMessages(firstSocket)).toMatchObject([{ type: "auth" }]);

    firstSocket.disconnect();
    await expect(mutation).rejects.toMatchObject({ code: "disconnected", operation: "mutation" });

    vi.advanceTimersByTime(250);
    const secondSocket = latestSocket();
    secondSocket.open();
    const [secondAuth] = sentMessages(secondSocket);
    secondSocket.receive({ type: "auth.result", id: secondAuth.id, result: { userId: "user-a" } });
    expect(sentMessages(secondSocket).some((message) => message.type === "mutation.call")).toBe(false);
  });

  it("rejects pending mutations with a typed closed error on explicit close", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    latestSocket().open();

    client.close();

    await expect(mutation).rejects.toMatchObject({ code: "closed", operation: "mutation" });
  });

  it("rejects server mutation errors with a typed server error", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();
    const [call] = sentMessages(socket);

    socket.receive({ type: "mutation.error", id: call.id, error: "permission denied" });

    const error = await mutation.then(
      () => {
        throw new Error("expected rejection");
      },
      (cause: unknown) => cause,
    );
    expect(error).toBeInstanceOf(GonvexClientError);
    expect(error).toMatchObject({ code: "server", operation: "mutation", message: "permission denied" });
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

  it("reports inflight requests while mutations are pending", async () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const mutation = client.mutation({ kind: "mutation", path: "tasks.create" });
    const socket = latestSocket();
    socket.open();

    expect(client.connectionState()).toMatchObject({ hasInflightRequests: true, inflightMutations: 1 });

    const [call] = sentMessages(socket);
    socket.receive({ type: "mutation.result", id: call.id, result: { id: "task_1" } });
    await mutation;

    expect(client.connectionState()).toMatchObject({ hasInflightRequests: false, inflightMutations: 0 });
  });

  it("re-requests an active live query via retryQuery", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();
    client.subscribeQuery(ref, { status: "open" }, handler);
    const socket = latestSocket();
    socket.open();
    const [subscribe] = sentMessages(socket);
    socket.receive({ type: "query.error", id: subscribe.id, error: "boom" });

    client.retryQuery(ref, { status: "open" });

    const subscribes = sentMessages(socket).filter((message) => message.type === "query.subscribe");
    expect(subscribes).toHaveLength(2);
    expect(subscribes[1]).toMatchObject({ id: subscribe.id, path: "tasks.list", args: { status: "open" } });

    socket.receive({ type: "query.result", id: subscribe.id, result: ["recovered"] });
    expect(handler).toHaveBeenLastCalledWith({ type: "query.result", id: subscribe.id, result: ["recovered"] });
  });

  it("ignores retryQuery for queries without subscribers", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    client.connect();
    const socket = latestSocket();
    socket.open();

    client.retryQuery(ref, { status: "open" });

    expect(sentMessages(socket).some((message) => message.type === "query.subscribe")).toBe(false);
  });

  it("drops handlers and closes the socket when closed", () => {
    const client = new GonvexClient("ws://runtime.test/ws");
    const handler = vi.fn();

    client.subscribeQuery(ref, {}, handler);
    const socket = latestSocket();
    socket.open();
    const [{ id }] = sentMessages(socket);

    client.close();
    socket.receive({ type: "query.result", id, result: "ignored" });

    expect(socket.readyState).toBe(FakeWebSocket.CLOSED);
    expect(handler).not.toHaveBeenCalled();
  });
});
