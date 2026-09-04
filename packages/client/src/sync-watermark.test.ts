import { setTimeout as delay } from "node:timers/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./sync-store.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./sync-store.js")>();
  return { ...actual, syncRowsHashes: vi.fn(actual.syncRowsHashes) };
});

import { GonvexClient, syncHashesDigest, type FunctionReference } from "./index";
import { syncRowsHashes } from "./sync-store.js";
import type {
  JsonValue,
  QueryCacheDirective,
  ServerMessage,
  SyncCursor,
} from "@gonvex/protocol";
import type { StoredSyncCollection, SyncStore } from "./sync-store.js";

type SocketListener = (event: { data?: string }) => void;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readonly sent: string[] = [];
  readyState = FakeWebSocket.CONNECTING;
  private readonly listeners = new Map<string, SocketListener[]>();

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: SocketListener) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  send(message: string) {
    this.sent.push(message);
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit("open", {});
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
  }

  receive(message: ServerMessage) {
    this.emit("message", { data: JSON.stringify(message) });
  }

  private emit(type: string, event: { data?: string }) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

class WatermarkSyncStore implements SyncStore {
  readonly collections = new Map<string, StoredSyncCollection>();
  readonly replacements: Array<{ path: string; value: StoredSyncCollection }> = [];
  directive: QueryCacheDirective | undefined;

  async load(_scope: string, path: string) {
    return this.collections.get(path);
  }

  async replace(_scope: string, path: string, _args: JsonValue, value: StoredSyncCollection) {
    const stored = { ...value, rows: value.rows.slice(), hashes: { ...value.hashes } };
    this.collections.set(path, stored);
    this.replacements.push({ path, value: stored });
  }

  async applyDelta(
    _scope: string,
    _path: string,
    _args: JsonValue,
    _value: {
      cursor: SyncCursor;
      keyField: string;
      upserts: JsonValue[];
      deleted: string[];
    },
  ) {}

  async delete(_scope: string, path: string) {
    this.collections.delete(path);
  }

  async loadDirective() {
    return this.directive;
  }

  async saveDirective(_identity: string, directive: QueryCacheDirective) {
    this.directive = directive;
  }

  async clear() {
    this.collections.clear();
  }

  close() {}
}

const directive: QueryCacheDirective = {
  protocolVersion: 1,
  scope: "scope-user-a-0000000000000000000000000000000000000000000000000000",
  epoch: "epoch-a-00000000000000000000000000000000000000000000000000000",
  maxAgeMs: 86_400_000,
};

function latestSocket() {
  const socket = FakeWebSocket.instances.at(-1);
  if (!socket) throw new Error("expected a websocket");
  return socket;
}

function sentMessages(socket = latestSocket()) {
  return socket.sent.map((message) => JSON.parse(message));
}

async function flushAsyncWork() {
  for (let barrier = 0; barrier < 3; barrier += 1) {
    await syncHashesDigest({});
    for (let microtask = 0; microtask < 5; microtask += 1) await Promise.resolve();
  }
}

async function verifiedDigest(rows: JsonValue[]) {
  return syncHashesDigest(await syncRowsHashes(rows, "id"));
}

type TestSyncSubscription = {
  path: string;
  cursor?: SyncCursor;
  opening: boolean;
  forceFullIntegrity: boolean;
  integrityEpoch?: string;
};

function subscriptions(client: GonvexClient) {
  return Array.from((client as unknown as {
    syncSubscriptions: Map<string, TestSyncSubscription>;
  }).syncSubscriptions.values());
}

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
  vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("sync revision watermarks", () => {
  it.each([0, 25])("advances only verified settled cursors without hashing or listener emission with %i ms hashing delay", async (hashDelayMs) => {
    const actual = await vi.importActual<typeof import("./sync-store.js")>("./sync-store.js");
    vi.mocked(syncRowsHashes).mockImplementation(async (...args) => {
      if (hashDelayMs) await delay(hashDelayMs);
      return actual.syncRowsHashes(...args);
    });
    const store = new WatermarkSyncStore();
    const paths = ["sync.qualifying", "sync.opening", "sync.fullIntegrity", "sync.staleEpoch"];
    for (const path of paths) {
      store.collections.set(path, {
        rows: [{ id: path }],
        cursor: { epoch: "sync-epoch", revision: 2 },
        keyField: "id",
      });
    }
    const client = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    const listeners = new Map<string, ReturnType<typeof vi.fn>>();
    for (const path of paths) {
      const listener = vi.fn();
      listeners.set(path, listener);
      client.subscribeSync({ kind: "sync", path } satisfies FunctionReference, {}, listener);
    }

    const socket = latestSocket();
    socket.open();
    socket.receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncIntegrity: 1, syncWatermark: 1 },
    });
    // Web Crypto completion is independent of microtask and digest barriers.
    const opens = await vi.waitFor(() => {
      const messages = sentMessages().filter((message) => message.type === "sync.open");
      expect(messages).toHaveLength(paths.length);
      return messages;
    });
    for (const open of opens) {
      const rows = store.collections.get(open.path)!.rows;
      socket.receive({
        type: "sync.ready",
        id: open.id,
        path: open.path,
        cursor: { epoch: "sync-epoch", revision: 2 },
        digest: await verifiedDigest(rows),
      });
    }
    await flushAsyncWork();

    const states = subscriptions(client);
    states.find((subscription) => subscription.path === "sync.opening")!.opening = true;
    states.find((subscription) => subscription.path === "sync.fullIntegrity")!.forceFullIntegrity = true;
    states.find((subscription) => subscription.path === "sync.staleEpoch")!.integrityEpoch = "old-epoch";
    for (const listener of listeners.values()) listener.mockClear();
    store.replacements.length = 0;
    vi.mocked(syncRowsHashes).mockClear();

    socket.receive({ type: "sync.watermark", revision: 8 });
    socket.receive({ type: "sync.watermark", revision: 9 });

    expect(vi.mocked(syncRowsHashes)).not.toHaveBeenCalled();
    expect(Array.from(listeners.values()).every((listener) => listener.mock.calls.length === 0)).toBe(true);
    expect(states.find((subscription) => subscription.path === "sync.qualifying")!.cursor?.revision).toBe(9);
    expect(states.filter((subscription) => subscription.path !== "sync.qualifying").map((subscription) => (
      subscription.cursor?.revision
    ))).toEqual([2, 2, 2]);

    await vi.advanceTimersByTimeAsync(999);
    expect(store.replacements).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    await flushAsyncWork();
    expect(store.replacements).toEqual([
      expect.objectContaining({
        path: "sync.qualifying",
        value: expect.objectContaining({
          cursor: { epoch: "sync-epoch", revision: 9 },
          rowsUnchanged: true,
        }),
      }),
    ]);
  });

  it("reuses a cursor persisted only by a watermark on the next client", async () => {
    const store = new WatermarkSyncStore();
    const path = "sync.resumeAfterWatermark";
    const rows = [{ id: "cached" }];
    store.collections.set(path, {
      rows,
      cursor: { epoch: "sync-epoch", revision: 3 },
      keyField: "id",
    });
    const ref: FunctionReference = { kind: "sync", path };
    const first = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    first.subscribeSync(ref, {}, vi.fn());
    const firstSocket = latestSocket();
    firstSocket.open();
    firstSocket.receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncIntegrity: 1, syncWatermark: 1 },
    });
    await flushAsyncWork();
    const firstOpen = await vi.waitFor(() => {
      const message = sentMessages(firstSocket).find((candidate) => candidate.type === "sync.open");
      expect(message).toBeDefined();
      return message!;
    });
    firstSocket.receive({
      type: "sync.ready",
      id: firstOpen.id,
      path,
      cursor: { epoch: "sync-epoch", revision: 3 },
      digest: await verifiedDigest(rows),
    });
    await flushAsyncWork();
    store.replacements.length = 0;

    firstSocket.receive({ type: "sync.watermark", revision: 12 });
    await vi.advanceTimersByTimeAsync(1_000);
    await flushAsyncWork();
    expect(store.collections.get(path)?.cursor).toEqual({ epoch: "sync-epoch", revision: 12 });
    first.close();

    const second = new GonvexClient("ws://runtime.test/ws", { sync: { store } });
    second.subscribeSync(ref, {}, vi.fn());
    const secondSocket = latestSocket();
    secondSocket.open();
    secondSocket.receive({
      type: "session.ready",
      queryCache: directive,
      capabilities: { syncIntegrity: 1, syncWatermark: 1 },
    });
    await flushAsyncWork();

    await vi.waitFor(() => {
      expect(sentMessages(secondSocket).find((message) => message.type === "sync.open")).toEqual(
        expect.objectContaining({
          path,
          cursor: { epoch: "sync-epoch", revision: 12 },
          digest: expect.any(String),
        }),
      );
    });
  });
});
