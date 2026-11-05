// Real SDK protocol contract for bounded runtime refusal; no network/load.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GonvexClient, type FunctionReference } from "../../packages/client/src/index";
import { replicaAggregateDigest, replicaRowsHashes } from "../../packages/client/src/replica-integrity";
vi.mock("../../packages/client/src/error-reporter.js", () => ({ GonvexErrorReporter: class { captureException() {} close() {} setTenant() {} setProject() {} } }));
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


beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.useFakeTimers();
  vi.stubGlobal("WebSocket", FakeWebSocket);
  vi.stubGlobal("window", { setTimeout: globalThis.setTimeout });
});
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });
const paths = ["presence.list", "workspaces.list", "statuses.statuses", "tags.tags", "members.listMembers", "members.listMemberTeams", "taskResources.taskUsers", "taskResources.taskTags", "workspacePreferences.workspacePreferences", "tasks.recentWorkspaceTasks", "notifications.listNotifications"];
it("retries the required collection set, retains cached cursors, and waits for every ready", async () => {
  const client = new GonvexClient("ws://runtime.test/ws");
  try {
    const refs = paths.map((path, i): FunctionReference => ({ kind: "query", path, delivery: "replica", replica: { table: `table_${i}`, key: "id", columns: ["id", "value"] } }));
    const watches = refs.map(ref => client.watchReplica(ref, {}));
    const socket = FakeWebSocket.instances.at(-1)!;
    socket.open();
    socket.receive({ type: "session.ready", capabilities: { replicaResume: 1 }, replica: { protocolVersion: 1, scope: "project:test:tenant:test", visibilityScope: "visibility:test", epoch: "epoch:test" } });
    await vi.advanceTimersByTimeAsync(0);
    const opens = () => socket.sent.map(s => JSON.parse(s)).filter(m => m.type === "replica.open");
    expect(opens()).toHaveLength(paths.length);
    const initial = opens();
    const rows = [{ id: "cached", value: "kept across refusal" }];
    const hashes = await replicaRowsHashes(rows, "id");
    const digest = replicaAggregateDigest(hashes);
    for (const open of initial) {
      socket.receive({ type: "replica.snapshot", id: open.id, path: open.path, result: rows, key: "id", hashes, digest, cursor: { epoch: "epoch:test", revision: 7 } });
      socket.receive({ type: "replica.ready", id: open.id, path: open.path, digest, truncated: false, cursor: { epoch: "epoch:test", revision: 7 } });
    }
    await vi.waitFor(() => expect(watches.every(w => w.status().isUpToDate)).toBe(true));
    for (const delay of [250, 500, 1000, 2000, 4000, 5000, 5000]) {
      for (const open of initial) socket.receive({ type: "replica.error", id: open.id, path: open.path, error: "replica snapshot admission overloaded (too many pending replica opens); retry" });
      await vi.advanceTimersByTimeAsync(0);
      expect(watches.every(w => w.status().isUpToDate)).toBe(false);
      for (const ref of refs) expect(client.localReplica.entity(ref.replica!.table, "cached")).toEqual(rows[0]);
      const before = opens().length;
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(opens()).toHaveLength(before);
      await vi.advanceTimersByTimeAsync(1);
      expect(opens()).toHaveLength(before + paths.length);
      for (const open of opens().slice(-paths.length)) {
        expect(open.cursor).toMatchObject({ epoch: "epoch:test", revision: 7 });
        expect(open.digest).toBe(digest);
      }
    }
    for (const open of initial.slice(0, -1)) socket.receive({ type: "replica.ready", id: open.id, path: open.path, digest, truncated: false, cursor: { epoch: "epoch:test", revision: 7 } });
    await vi.advanceTimersByTimeAsync(0);
    expect(watches.every(w => w.status().isUpToDate)).toBe(false);
    const last = initial.at(-1)!;
    socket.receive({ type: "replica.ready", id: last.id, path: last.path, digest, truncated: false, cursor: { epoch: "epoch:test", revision: 7 } });
    await vi.waitFor(() => expect(watches.every(w => w.status().isUpToDate)).toBe(true));
  } finally { client.close(); }
});
