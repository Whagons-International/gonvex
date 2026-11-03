import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GonvexClient, GonvexErrorReporter } from "./index";

type Listener = (event: { data?: string }) => void;

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: FakeWebSocket[] = [];
  readonly sent: string[] = [];
  readyState = FakeWebSocket.CONNECTING;
  private readonly listeners = new Map<string, Listener[]>();
  constructor(readonly url: string) { FakeWebSocket.instances.push(this); }
  addEventListener(type: string, listener: Listener) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }
  send(message: string) { this.sent.push(message); }
  close() { this.disconnect(); }
  disconnect() { this.readyState = FakeWebSocket.CLOSED; this.emit("close", {}); }
  open() { this.readyState = FakeWebSocket.OPEN; this.emit("open", {}); }
  receive(message: unknown) { this.emit("message", { data: JSON.stringify(message) }); }
  private emit(type: string, event: { data?: string }) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

const clients: GonvexClient[] = [];
const messages = (socket: FakeWebSocket) => socket.sent.map((value) => JSON.parse(value));
const presenceFrames = (socket: FakeWebSocket) => messages(socket).filter((m) => String(m.type).startsWith("presence."));
const tick = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };

function client() {
  // Control-only keeps the test focused on presence (no Local Replica scope).
  const result = new GonvexClient("ws://runtime.test/ws", { role: "control", project: "p", tenant: "t", token: "session" });
  clients.push(result);
  return result;
}

async function connect(sdk: GonvexClient, presence: boolean) {
  sdk.connect();
  const socket = FakeWebSocket.instances.at(-1)!;
  socket.open();
  socket.receive({ type: "session.ready", capabilities: presence ? { presence: 1 } : {} });
  const auth = messages(socket).find((m) => m.type === "auth");
  socket.receive({ type: "auth.result", id: auth.id, result: { accountId: "a", tenantId: "t" } });
  await tick();
  return socket;
}

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
});
afterEach(() => {
  for (const sdk of clients.splice(0)) sdk.close();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("presence v2", () => {
  it("advertises the capability and sends nothing to a runtime without it", async () => {
    const sdk = client();
    sdk.setPresence("active");
    const listener = vi.fn();
    sdk.subscribePresence(listener);
    const socket = await connect(sdk, false);
    expect(messages(socket)[0].capabilities.presence).toBe(1);
    expect(sdk.presenceSupported()).toBe(false);
    expect(sdk.presenceSnapshot().supported).toBe(false);
    expect(presenceFrames(socket)).toEqual([]);
  });

  it("re-announces state and resubscribes after every auth, sending transitions only", async () => {
    const sdk = client();
    sdk.setPresence("active");
    const listener = vi.fn();
    sdk.subscribePresence(listener);
    const socket = await connect(sdk, true);
    expect(sdk.presenceSupported()).toBe(true);
    expect(presenceFrames(socket)).toEqual([
      { type: "presence.set", state: "active" },
      { type: "presence.subscribe", id: "presence-1" },
    ]);
    // Repeating the same state is not a transition.
    sdk.setPresence("active");
    sdk.setPresence("idle");
    expect(presenceFrames(socket).slice(2)).toEqual([{ type: "presence.set", state: "idle" }]);
    // No periodic traffic.
    await vi.advanceTimersByTimeAsync(120_000);
    expect(presenceFrames(socket)).toHaveLength(3);

    // Reconnect: the latest state and the subscription are sent again.
    socket.disconnect();
    await vi.advanceTimersByTimeAsync(30_000);
    const next = FakeWebSocket.instances.at(-1)!;
    expect(next).not.toBe(socket);
    next.open();
    next.receive({ type: "session.ready", capabilities: { presence: 1 } });
    const auth = messages(next).find((m) => m.type === "auth");
    next.receive({ type: "auth.result", id: auth.id, result: { accountId: "a", tenantId: "t" } });
    await tick();
    expect(presenceFrames(next)).toEqual([
      { type: "presence.set", state: "idle" },
      { type: "presence.subscribe", id: "presence-2" },
    ]);
  });

  it("applies snapshots and patches and ignores stale subscription ids", async () => {
    const sdk = client();
    const listener = vi.fn();
    const stop = sdk.subscribePresence(listener);
    const socket = await connect(sdk, true);
    expect(sdk.presenceSnapshot().ready).toBe(false);
    socket.receive({ type: "presence.snapshot", id: "stale", serverTime: 1, members: [{ memberId: "x", state: "active", since: 1 }] });
    expect(sdk.presenceSnapshot().members.size).toBe(0);
    socket.receive({ type: "presence.snapshot", id: "presence-1", serverTime: 2, members: [
      { memberId: "m1", state: "active", since: 1 },
      { memberId: "m2", state: "idle", since: 2 },
    ] });
    const first = sdk.presenceSnapshot();
    expect(first.ready).toBe(true);
    expect([...first.members.keys()]).toEqual(["m1", "m2"]);
    socket.receive({ type: "presence.patch", serverTime: 3, changes: [
      { memberId: "m1", state: "idle", since: 3 },
      { memberId: "m2", state: "offline", since: 3 },
      { memberId: "m3", state: "active", since: 3 },
    ] });
    const second = sdk.presenceSnapshot();
    expect(second).not.toBe(first);
    expect(Object.fromEntries([...second.members].map(([id, m]) => [id, m.state]))).toEqual({ m1: "idle", m3: "active" });
    expect(sdk.presenceSnapshot()).toBe(second);
    expect(listener).toHaveBeenCalled();
    // The socket closing keeps the last view but marks it not ready.
    socket.disconnect();
    expect(sdk.presenceSnapshot().ready).toBe(false);
    expect(sdk.presenceSnapshot().members.size).toBe(2);
    stop();
    expect(sdk.presenceSnapshot().members.size).toBe(0);
  });

  it("notifies connection-state listeners when the presence capability arrives", async () => {
    const sdk = client();
    const states = vi.fn();
    sdk.subscribeToConnectionState(states);
    sdk.connect();
    const socket = FakeWebSocket.instances.at(-1)!;
    socket.open();
    const before = states.mock.calls.length;
    socket.receive({ type: "session.ready", capabilities: { presence: 1 } });
    expect(states.mock.calls.length).toBe(before + 1);
    expect(sdk.presenceSupported()).toBe(true);
  });

  it("the last listener unsubscribes", async () => {
    const sdk = client();
    const socket = await connect(sdk, true);
    const stopA = sdk.subscribePresence(() => undefined);
    const stopB = sdk.subscribePresence(() => undefined);
    stopA();
    expect(presenceFrames(socket).map((m) => m.type)).toEqual(["presence.subscribe"]);
    stopB();
    expect(presenceFrames(socket).map((m) => m.type)).toEqual(["presence.subscribe", "presence.unsubscribe"]);
  });
});

describe("error reporter heartbeat", () => {
  it("stops periodic heartbeats when the runtime supports presence v2", async () => {
    vi.stubGlobal("window", { addEventListener: () => undefined, removeEventListener: () => undefined });
    for (const supported of [false, true]) {
      const reportError = vi.fn(async () => undefined);
      const reporter = new GonvexErrorReporter({
        project: "p",
        captureGlobalErrors: false,
        client: { reportError, presenceSupported: () => supported },
      });
      await vi.advanceTimersByTimeAsync(61_000);
      const heartbeats = reportError.mock.calls.filter(([type]) => type === "heartbeat").length;
      expect(heartbeats).toBe(supported ? 0 : 2);
      reporter.close();
    }
  });
});
