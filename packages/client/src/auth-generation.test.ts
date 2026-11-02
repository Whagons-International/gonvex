import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient } from "./index.js";

class Socket extends EventTarget {
  static OPEN = 1; static instances: Socket[] = [];
  readyState = 0; sent: any[] = [];
  constructor(_url: string) { super(); Socket.instances.push(this); }
  send(frame: string) { this.sent.push(JSON.parse(frame)); }
  open() { this.readyState = 1; this.dispatchEvent(new Event("open")); }
  receive(frame: unknown) { this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(frame) })); }
  close() { this.readyState = 3; }
}
const clients: GonvexClient[] = [];
afterEach(() => { for (const client of clients.splice(0)) client.close(); Socket.instances = []; vi.unstubAllGlobals(); });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

it.each((["initial", "rejected"] as const).flatMap(phase => [false, true].map(boundary => ({ phase, boundary }))))
("auth fetch cannot replace a newer token at $phase completion, precisionBoundary=$boundary", async ({ phase, boundary }) => {
  vi.stubGlobal("WebSocket", Socket);
  const entered = deferred<void>(), release = deferred<string>();
  const fetchToken = vi.fn(async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
    if (phase === "rejected" && !forceRefreshToken) return "first-token";
    entered.resolve(); return release.promise;
  });
  const client = new GonvexClient("ws://auth-generation.test/ws", {
    project: "project", tenant: "tenant", token: "original", fetchToken,
    identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
  });
  clients.push(client);
  const state = client as unknown as { authFetchGeneration: number | symbol; auth: { token?: string } };
  try {
    if (phase === "initial") state.authFetchGeneration = boundary ? Number.MAX_SAFE_INTEGER : 0;
    client.connect(); const socket = Socket.instances.at(-1)!; socket.open();
    if (phase === "rejected") {
      await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
      const auth = socket.sent.find(frame => frame.type === "auth");
      state.authFetchGeneration = boundary ? Number.MAX_SAFE_INTEGER : 0;
      socket.receive({ type: "auth.error", id: auth.id, error: "expired" });
    }
    await entered.promise;
    client.setAuth({ token: "new-owner-token", identity: { sub: "bob", iss: "issuer" } });
    expect(state.auth.token).toBe("new-owner-token");
    const authCount = socket.sent.filter(frame => frame.type === "auth").length;
    release.resolve("stale-alice-token");
    // The fetch continuation has no further awaited work before credential
    // installation. Drain its promise jobs by reaching the next event-loop turn.
    await new Promise(done => setTimeout(done, 0));
    expect(state.auth.token).toBe("new-owner-token");
    expect(socket.sent.filter(frame => frame.type === "auth")).toHaveLength(authCount);
  } finally { release.resolve("stale-alice-token"); client.close(); }
});
