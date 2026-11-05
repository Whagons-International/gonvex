import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage } from "@gonvex/client";
import { GonvexAuthProvider, useGonvexAuth, useReplicaEntities } from "./index.js";

class Socket {
  static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
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
const runtimeUrl = "https://cached-render.test";
const url = "wss://cached-render.test/ws";
const storageKey = "gonvex-auth:https%3A%2F%2Fcached-render.test:project";
const ids = ["task"];
const clients: GonvexClient[] = [];
const session = () => ({
  accessToken: "access", expiresAt: Date.now() + 900_000,
  refreshToken: "refresh", refreshExpiresAt: Date.now() + 86_400_000,
  account: { id: "account", email: "a@example.test", emailVerified: true, provider: "password" },
  tenants: [{ id: "tenant", name: "Tenant", role: "admin", permissions: {}, domain: "tenant", timezone: "UTC", description: "", profile: {} }],
  activeTenantId: "tenant",
});
let currentAuth: ReturnType<typeof useGonvexAuth>;
function CachedApp() {
  const auth = useGonvexAuth();
  currentAuth = auth;
  const rows = useReplicaEntities("tasks", ids);
  return <div data-testid="app">{auth.isAuthenticated ? `${auth.sessionState}:${rows[0]?.title ?? "empty"}` : "signedOut"}</div>;
}
async function fixture() {
  const storage = new MemoryLocalReplicaStorage();
  const owner = ["identity", url, "project", "tenant", "project", "account"].join("\u0000");
  await storage.saveSession(owner, {
    directive: { protocolVersion: 1, scope: "tenant", visibilityScope: "visible", epoch: "epoch" },
    identity: { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", permissions: {} } },
    artifactHash: "artifact",
  });
  await storage.replaceSnapshot({ entities: { tasks: { task: { _id: "task", title: "cached task" } } }, liveQueries: {} }, "visible");
  localStorage.setItem(storageKey, JSON.stringify(session()));
  const client = new GonvexClient(url, { project: "project", localReplica: { storage } });
  clients.push(client);
  const rendered = render(<GonvexAuthProvider client={client} runtimeUrl={runtimeUrl} projectId="project"><CachedApp /></GonvexAuthProvider>);
  await act(async () => { await client.localSessionReady; });
  act(() => { vi.advanceTimersToNextFrame(); });
  return { client, rendered, storage, owner };
}
beforeEach(() => { Socket.all = []; vi.useFakeTimers(); vi.stubGlobal("WebSocket", Socket); localStorage.clear(); });
afterEach(() => { cleanup(); clients.splice(0).forEach(client => client.close()); vi.useRealTimers(); vi.unstubAllGlobals(); });

it("renders persisted rows while offline, then signs out and hides them on terminal rejection", async () => {
  const { client, rendered } = await fixture();
  expect(rendered.getByTestId("app").textContent).toBe("reconnecting:cached task");
  expect(Socket.all.every(socket => socket.sent.length === 0)).toBe(true);
  client.setAuth({ fetchToken: undefined });
  const socket = Socket.all.at(-1)!;
  await act(async () => { socket.open(); });
  const auth = socket.sent.find(message => message.type === "auth");
  expect(auth).toBeDefined();
  act(() => { socket.receive({ type: "auth.error", id: auth.id, error: "revoked", code: "revoked" }); vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("signedOut");
  expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  expect(localStorage.getItem(storageKey)).toBeNull();
});

it("removes the previous account's rows immediately when the canonical account switches", async () => {
  const { client, rendered } = await fixture();
  expect(rendered.getByTestId("app").textContent).toContain("cached task");
  const next = { ...session(), accessToken: "other-access", account: { ...session().account, id: "other" } };
  localStorage.setItem(storageKey, JSON.stringify(next));
  await act(async () => {
    window.dispatchEvent(new StorageEvent("storage", { key: storageKey, newValue: JSON.stringify(next) }));
    await client.localSessionReady;
  });
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  expect(rendered.getByTestId("app").textContent).not.toContain("cached task");
});


it("forgets cached local authority on explicit provider sign-out", async () => {
  const { client, rendered, storage, owner } = await fixture();
  expect(rendered.getByTestId("app").textContent).toContain("cached task");
  await act(async () => { await currentAuth.signOut(); });
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("signedOut");
  expect(await storage.loadSession(owner)).toBeUndefined();
  await expect(client.restoreSession({ project: "project", tenant: "tenant", token: "access", identity: { sub: "account", iss: "project" } })).resolves.toBe(false);
  expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
});

it("keeps the restored provider session through refresh failure and repeated startup closes", async () => {
  const { client, rendered, storage, owner } = await fixture();
  client.setAuth({ fetchToken: async () => { throw new Error("network unavailable"); } });
  const socket = Socket.all.at(-1)!;
  await act(async () => { socket.open(); });
  const frame = socket.sent.find(message => message.type === "auth");
  await act(async () => {
    socket.receive({ type: "auth.error", id: frame.id, error: "expired", code: "credential_rejected" });
  });
  for (let i = 0; i < 3; i += 1) {
    await act(async () => { vi.advanceTimersByTime(300); });
    const next = Socket.all.at(-1)!;
    await act(async () => { next.open(); next.close(); });
  }
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("reconnecting:cached task");
  expect(client.localSessionState()).toBe("restored");
  expect(await storage.loadSession(owner)).toBeDefined();
  expect(localStorage.getItem(storageKey)).not.toBeNull();
});

it.each([undefined, "transient"])("keeps the provider session after a refreshed-token error with code=%s", async (code) => {
  const { client, rendered, storage, owner } = await fixture();
  client.setAuth({ fetchToken: async ({ forceRefreshToken }) => forceRefreshToken ? "fresh-access" : "access" });
  const socket = Socket.all.at(-1)!;
  await act(async () => { socket.open(); });
  await act(async () => {
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
  });
  expect(socket.sent[1].token).toBe("fresh-access");
  await act(async () => {
    socket.receive({ type: "auth.error", id: socket.sent[1].id, error: "authentication unavailable", code });
  });
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("reconnecting:cached task");
  expect(client.localSessionState()).toBe("restored");
  expect(await storage.loadSession(owner)).toBeDefined();
  expect(localStorage.getItem(storageKey)).not.toBeNull();
});

it("signs out when a forced fresh credential is rejected again", async () => {
  const { client, rendered, storage, owner } = await fixture();
  client.setAuth({ fetchToken: async ({ forceRefreshToken }) => forceRefreshToken ? "fresh-access" : "access" });
  const socket = Socket.all.at(-1)!;
  await act(async () => { socket.open(); });
  await act(async () => {
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
  });
  expect(socket.sent[1].token).toBe("fresh-access");
  await act(async () => {
    socket.receive({ type: "auth.error", id: socket.sent[1].id, error: "invalid", code: "credential_rejected" });
  });
  act(() => { vi.advanceTimersToNextFrame(); });
  expect(rendered.getByTestId("app").textContent).toBe("signedOut");
  expect(await storage.loadSession(owner)).toBeUndefined();
  expect(localStorage.getItem(storageKey)).toBeNull();
});

it('signing out on tenant A then opening tenant B with its stale session reaches signed-out recovery without a socket loop', async () => {
  const { client, rendered } = await fixture();
  const siblingSession = localStorage.getItem(storageKey)!;
  await act(async () => { await currentAuth.signOut(); });
  expect(localStorage.getItem(storageKey)).toBeNull();
  rendered.unmount();
  client.close();
  // Sibling origins retain independent storage after tenant A logs out.
  localStorage.setItem(storageKey, siblingSession);
  const sibling = new GonvexClient(url, { project: 'project', role: 'control' });
  clients.push(sibling);
  const next = render(<GonvexAuthProvider client={sibling} runtimeUrl={runtimeUrl} projectId="project" initialTenantId={null}><CachedApp /></GonvexAuthProvider>);
  await act(async () => { await Promise.resolve(); });
  act(() => { vi.advanceTimersToNextFrame(); });
  // Definitive revocation of B's stale canonical credential must clear both
  // its session and the tenant hint before routing can offer sign-in/picker.
  const socket = Socket.all.at(-1)!;
  await act(async () => { socket.open(); await Promise.resolve(); });
  const authFrame = socket.sent.find(frame => frame.type === 'auth');
  expect(authFrame).toBeDefined();
  await act(async () => {
    socket.receive({ type: 'auth.error', id: authFrame.id, code: 'revoked', error: 'login was revoked' });
    await Promise.resolve();
    vi.advanceTimersToNextFrame();
  });
  expect(next.getByTestId('app').textContent).toBe('signedOut');
  expect(localStorage.getItem(storageKey)).toBeNull();
  expect(currentAuth.activeTenant).toBeNull();
  const count = Socket.all.length;
  await act(async () => { await vi.advanceTimersByTimeAsync(74_000); });
  expect(Socket.all.length).toBeLessThanOrEqual(count + 1);
});
