import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, type GonvexClientAuth, type ReplicaSnapshot } from "./index.js";

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
const url = "ws://startup.test";
const auth: GonvexClientAuth = { project: "project", tenant: "tenant", token: "opaque-session", identity: { sub: "account", iss: "project" } };
const owner = ["identity", url, "project", "tenant", "project", "account"].join("\u0000");
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible", epoch: "epoch" };
const identity = { auth: { account: { id: "account" } }, tenant: { id: "tenant" }, member: { id: "member", accountId: "account", permissions: {} } };
const clients: GonvexClient[] = [];
class Storage extends MemoryLocalReplicaStorage {
  loadWorkingSet = vi.fn((scope: string) => this.load(scope));
}
async function fixture(initialAuth = auth) {
  const storage = new Storage();
  await storage.saveSession(owner, { directive, identity, artifactHash: "artifact" });
  await storage.replaceSnapshot({ entities: { tasks: { task: { _id: "task", title: "cached" } } }, liveQueries: {} }, "visible");
  const client = new GonvexClient(url, { ...initialAuth, localReplica: { storage } });
  clients.push(client);
  return { storage, client };
}
function socketFor(client: GonvexClient) {
  client.connect();
  const socket = Socket.all.at(-1)!;
  socket.open();
  return socket;
}
beforeEach(() => { Socket.all = []; vi.stubGlobal("WebSocket", Socket); });
afterEach(() => { clients.splice(0).forEach(client => client.close()); vi.unstubAllGlobals(); });

describe("cached session startup", () => {
  it("reads cached data offline without a local reducer runtime and loads only the real scope", async () => {
    const { client, storage } = await fixture();
    await expect(client.localSessionReady).resolves.toBe(true);
    expect(client.localSessionState()).toBe("restored");
    expect(client.localReplica.entity("tasks", "task")).toEqual({ _id: "task", title: "cached" });
    expect(client.localReplica.freshness()).toBe("offline");
    expect(storage.loadWorkingSet.mock.calls.map(([scope]) => scope)).toEqual(["visible"]);
    expect(Socket.all).toHaveLength(0);
  });

  it("keeps authenticate server-only while local readiness resolves before auth and transitions online", async () => {
    const { client, storage } = await fixture();
    const states: string[] = [];
    client.onLocalSessionChange(() => states.push(client.localSessionState()));
    let accepted = false;
    const pending = client.authenticate(auth).then(() => { accepted = true; });
    await expect(client.localSessionReady).resolves.toBe(true);
    expect(accepted).toBe(false);
    const socket = socketFor(client);
    const watch = client.watchReplica({ kind: "query", path: "tasks.rows", delivery: "replica", replica: { table: "tasks", key: "_id", columns: ["_id", "title"] } }, {});
    const unsubscribe = watch.onUpdate(() => undefined);
    expect(socket.sent.map(message => message.type)).toEqual(["auth"]);
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    await pending;
    expect(client.localSessionState()).toBe("authenticated");
    expect(states).toEqual(["restored", "authenticated"]);
    expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1);
    expect(socket.sent.some(message => message.type === "replica.open")).toBe(true);
    unsubscribe();
  });

  it("keeps local readiness unavailable during hydration even when admission accepts and refresh attaches", async () => {
    const { client, storage } = await fixture({ ...auth, tenant: "new-tenant" });
    await client.localSessionReady;
    let release: () => void = () => undefined;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { release = () => resolve(undefined); }));
    const accepted = client.authenticate({ ...auth, tenant: "new-tenant" });
    const socket = socketFor(client);
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    await vi.waitFor(() => expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1));
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entityCompleteness("tasks")).toBe("partial");
    client.setAuth({ fetchToken: async () => "opaque-session" });
    expect(client.localSessionState()).toBe("unavailable");
    release();
    await accepted;
    await vi.waitFor(() => expect(client.localSessionState()).toBe("authenticated"));
  });

  it("admits without a saved session but certifies readiness and persists only after local recovery", async () => {
    const { client, storage } = await fixture({ ...auth, tenant: "new-tenant" });
    await expect(client.localSessionReady).resolves.toBe(false);
    let release!: () => void;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { release = () => resolve(undefined); }));
    const pending = client.authenticate({ ...auth, tenant: "new-tenant" });
    const socket = socketFor(client);
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: { ...directive, visibilityScope: "new-visible" }, localIdentity: identity } });
    await pending;
    // Accepted identity is not a readable/complete cached workspace certificate.
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entityCompleteness("tasks")).toBe("partial");
    expect(await storage.loadSession(owner.replace("\u0000tenant\u0000", "\u0000new-tenant\u0000"))).toBeUndefined();
    release();
    await vi.waitFor(() => expect(client.localSessionState()).toBe("authenticated"));
    expect(storage.loadWorkingSet.mock.calls.map(([scope]) => scope)).toEqual(["new-visible"]);
    const nextOwner = owner.replace("\u0000tenant\u0000", "\u0000new-tenant\u0000");
    await vi.waitFor(async () => expect(await storage.loadSession(nextOwner)).toBeDefined());
  });

  it("quarantines the replica and forgets restored authority after a server rejection", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "revoked", code: "revoked" });
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    await expect(client.localSessionReady).resolves.toBe(false);
    expect(rejected).toHaveBeenCalledWith("revoked", { definitive: true, code: "revoked" });
    expect(await storage.loadSession(owner)).toBeUndefined();
    expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1);
  });

  it.each(["throws", "missing", "null", "unchanged"])("keeps restored rows and session when forced refresh %s", async (failure) => {
    const fetchToken = vi.fn(async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => {
      if (!forceRefreshToken) return auth.token;
      if (failure === "throws") throw new Error("offline");
      return failure === "missing" ? undefined : failure === "null" ? null : auth.token;
    });
    const { client, storage } = await fixture({ ...auth, fetchToken });
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
    await vi.waitFor(() => expect(socket.readyState).toBe(Socket.CLOSED));
    expect(client.localSessionState()).toBe("restored");
    expect(client.localReplica.entity("tasks", "task")?.title).toBe("cached");
    expect(await storage.loadSession(owner)).toBeDefined();
    expect(rejected).not.toHaveBeenCalled();
  });

  it.each([true, false])("force-refreshes a rejected credential and handles accepted=%s", async (accepted) => {
    const fetchToken = vi.fn(async ({ forceRefreshToken }: { forceRefreshToken: boolean }) => forceRefreshToken ? "fresh-token" : auth.token);
    const { client, storage } = await fixture({ ...auth, fetchToken });
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2));
    expect(socket.sent[1].token).toBe("fresh-token");
    socket.receive(accepted
      ? { type: "auth.result", id: socket.sent[1].id, result: { replica: directive, localIdentity: identity } }
      : { type: "auth.error", id: socket.sent[1].id, error: "invalid", code: "credential_rejected" });
    await vi.waitFor(() => expect(client.localSessionState()).toBe(accepted ? "authenticated" : "unavailable"));
    expect(Boolean(await storage.loadSession(owner))).toBe(accepted);
    expect(rejected).toHaveBeenCalledTimes(accepted ? 0 : 1);
  });

  it("recognizes a forced token installed by its provider before the fetch resolves", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    client.setAuth({ fetchToken: async ({ forceRefreshToken }) => {
      if (!forceRefreshToken) return auth.token;
      client.setAuth({ token: "fresh-token" });
      return "fresh-token";
    } });
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2));
    await Promise.resolve();
    socket.receive({ type: "auth.error", id: socket.sent[1].id, error: "invalid", code: "credential_rejected" });
    expect(client.localSessionState()).toBe("unavailable");
    expect(await storage.loadSession(owner)).toBeUndefined();
    expect(rejected).toHaveBeenCalledOnce();
  });

  it.each([undefined, "transient"])("keeps restored access after a non-definitive refreshed-token error with code=%s", async (code) => {
    const { client, storage } = await fixture({ ...auth, fetchToken: async ({ forceRefreshToken }) => forceRefreshToken ? "fresh-token" : auth.token });
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2));
    socket.receive({ type: "auth.error", id: socket.sent[1].id, error: "database admission timeout", code });
    expect(client.localSessionState()).toBe("restored");
    expect(client.localReplica.entity("tasks", "task")?.title).toBe("cached");
    expect(await storage.loadSession(owner)).toBeDefined();
    // Reported for diagnostics, never as a definitive rejection.
    expect(rejected).toHaveBeenCalled();
    for (const [, details] of rejected.mock.calls) expect(details).toMatchObject({ definitive: false });
  });

  it("does not mark an already accepted provider token as a pending forced refresh", async () => {
    const { client } = await fixture();
    await client.localSessionReady;
    let finish: (token: string) => void = () => undefined;
    let refreshes = 0;
    client.setAuth({ fetchToken: async ({ forceRefreshToken }) => {
      if (!forceRefreshToken) return refreshes ? "fresh-token" : auth.token;
      refreshes += 1;
      if (refreshes > 1) return "newer-token";
      client.setAuth({ token: "fresh-token" });
      return new Promise<string>(resolve => { finish = resolve; });
    } });
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "expired", code: "credential_rejected" });
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2));
    socket.receive({ type: "auth.result", id: socket.sent[1].id, result: { replica: directive, localIdentity: identity } });
    await vi.waitFor(() => expect(client.localSessionState()).toBe("authenticated"));
    finish("fresh-token");
    await Promise.resolve();
    socket.receive({ type: "auth.error", id: socket.sent[1].id, error: "expired again", code: "credential_rejected" });
    await vi.waitFor(() => expect(socket.sent).toHaveLength(3));
    expect(socket.sent[2].token).toBe("newer-token");
    expect(rejected).not.toHaveBeenCalled();
  });

  it("preserves startup across repeated socket closures", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    for (let i = 0; i < 3; i += 1) {
      socketFor(client).close();
      expect(client.localSessionState()).toBe("restored");
      expect(client.localReplica.entity("tasks", "task")?.title).toBe("cached");
      expect(await storage.loadSession(owner)).toBeDefined();
    }
    expect(rejected).not.toHaveBeenCalled();
  });

  it("keeps restored rows when session revalidation and forced refresh are unavailable", async () => {
    const { client, storage } = await fixture({ ...auth, fetchToken: async () => { throw new Error("offline"); } });
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));
    socket.receive({ type: "auth.result", id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    await vi.waitFor(() => expect(client.localSessionState()).toBe("authenticated"));
    socket.receive({ type: "auth.error", id: "session-expired", error: "authentication expired or was revoked", code: "transient" });
    await vi.waitFor(() => expect(socket.readyState).toBe(Socket.CLOSED));
    expect(client.localSessionState()).toBe("restored");
    expect(client.localReplica.entity("tasks", "task")?.title).toBe("cached");
    expect(await storage.loadSession(owner)).toBeDefined();
    expect(rejected).not.toHaveBeenCalled();
  });

  it("keeps restored access for a legacy rejection without a refreshed credential", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    const socket = socketFor(client);
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "authentication unavailable" });
    expect(client.localSessionState()).toBe("restored");
    expect(client.localReplica.entity("tasks", "task")?.title).toBe("cached");
    expect(await storage.loadSession(owner)).toBeDefined();
  });

  it("does not restore a control client without replica storage", async () => {
    const client = new GonvexClient(url, auth);
    clients.push(client);
    await expect(client.localSessionReady).resolves.toBe(false);
    expect(client.localSessionState()).toBe("unavailable");
  });

  it("ignores pending auth results after an unsolicited definitive rejection", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    const rejected = vi.fn();
    client.onAuthError(rejected);
    const socket = socketFor(client);
    const frame = socket.sent[0];
    socket.receive({ type: "auth.error", id: "session-expired", error: "revoked", code: "revoked" });
    socket.receive({ type: "auth.result", id: frame.id, result: { replica: directive, localIdentity: identity } });
    await Promise.resolve();
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    expect(await storage.loadSession(owner)).toBeUndefined();
    expect(rejected).toHaveBeenCalledOnce();
  });

  it("does not expose the previous account or tenant while switching", async () => {
    const { client } = await fixture();
    await client.localSessionReady;
    const pending = client.restoreSession({ ...auth, identity: { sub: "other", iss: "project" } });
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    await expect(pending).resolves.toBe(false);
    expect(client.localSessionState()).toBe("unavailable");
    await expect(client.restoreSession(auth)).resolves.toBe(true);
    await expect(client.restoreSession({ ...auth, tenant: "other-tenant" })).resolves.toBe(false);
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  });

  it("does not restore either account when token claims conflict with the account hint", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    await storage.saveSession(owner.replace("\u0000account", "\u0000other"), { directive, identity, artifactHash: "artifact" });
    const token = `header.${btoa(JSON.stringify({ sub: "other", iss: "project" }))}.signature`;
    await expect(client.restoreSession({ ...auth, token })).resolves.toBe(false);
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  });

  it("drops the old JWT account immediately when its identity hint switches accounts", async () => {
    const token = `header.${btoa(JSON.stringify({ sub: "account", iss: "project" }))}.signature`;
    const { client } = await fixture({ ...auth, token });
    await expect(client.localSessionReady).resolves.toBe(true);
    const ready = client.restoreSession({ identity: { sub: "other", iss: "project" } });
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    await expect(ready).resolves.toBe(false);
  });

  it("sign-out forgets the saved directive and never restores the prior account", async () => {
    const { client, storage } = await fixture();
    await client.localSessionReady;
    await expect(client.restoreSession({ token: undefined, identity: undefined, fetchToken: undefined, tenant: undefined })).resolves.toBe(false);
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
    expect(await storage.loadSession(owner)).toBeUndefined();
    await expect(client.restoreSession(auth)).resolves.toBe(false);
  });

  it("ignores a delayed hydration that completes after rejection", async () => {
    const { client, storage } = await fixture();
    let finish: (snapshot: ReplicaSnapshot | undefined) => void = () => undefined;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const ready = client.localSessionReady;
    await vi.waitFor(() => expect(storage.loadWorkingSet).toHaveBeenCalledTimes(1));
    const snapshot = await storage.load("visible");
    const socket = socketFor(client);
    socket.receive({ type: "auth.error", id: socket.sent[0].id, error: "revoked", code: "revoked" });
    finish(snapshot);
    await expect(ready).resolves.toBe(false);
    expect(client.localSessionState()).toBe("unavailable");
    expect(client.localReplica.entity("tasks", "task")).toBeUndefined();
  });
});

describe('server tenant admission display signal', () => {
  it('hides rows from the previous visibility scope before notifying admission observers', async () => {
    const { client, storage } = await fixture(); await client.localSessionReady;
    expect(client.localReplica.entity('tasks', 'task')).toBeDefined();
    let release!: () => void;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { release = () => resolve(undefined); }));
    const observed: unknown[] = [];
    client.onTenantAdmissionChange(() => {
      if (client.tenantAdmission()) observed.push(client.localReplica.entity('tasks', 'task'));
    });
    const pending = client.authenticate(auth); const socket = socketFor(client);
    socket.receive({ type: 'auth.result', id: socket.sent[0].id,
      result: { replica: { ...directive, visibilityScope: 'changed-permissions' }, localIdentity: identity } });
    expect(observed).toEqual([undefined]);
    await vi.waitFor(() => expect(storage.loadWorkingSet).toHaveBeenCalledTimes(2));
    release(); await pending;
    expect(client.localReplica.entity('tasks', 'task')).toBeUndefined();
  });

  it('publishes accepted identity before blocked recovery without releasing authenticate or reducers', async () => {
    const { client } = await fixture();
    await client.localSessionReady;
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const restore = vi.spyOn(client as any, 'activateReplicaDirective').mockReturnValue(blocked);
    const changes = vi.fn(); client.onTenantAdmissionChange(changes);
    let settled = false;
    const pending = client.authenticate(auth).then(() => { settled = true; });
    const socket = socketFor(client);
    socket.receive({ type: 'auth.result', id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    expect(client.tenantAdmission()).toEqual({ project: 'project', accountId: 'account', tenantId: 'tenant', memberId: 'member' });
    expect(changes).toHaveBeenCalledTimes(1);
    await Promise.resolve();
    expect(settled).toBe(false);
    expect((client as any).authInFlight).toBe(true);
    expect(socket.sent.every(message => message.type === 'auth')).toBe(true);
    release(); await pending; restore.mockRestore();
  });

  it('revokes display admission immediately on credentials replacement and ignores old auth replies', async () => {
    const { client } = await fixture(); await client.localSessionReady;
    const first = client.authenticate(auth); const socket = socketFor(client);
    const id = socket.sent[0].id;
    socket.receive({ type: 'auth.result', id, result: { replica: directive, localIdentity: identity } });
    await first;
    expect(client.tenantAdmission()).toBeDefined();
    client.setAuth({ ...auth, tenant: 'other' });
    expect(client.tenantAdmission()).toBeUndefined();
    socket.receive({ type: 'auth.result', id, result: { replica: directive, localIdentity: identity } });
    expect(client.tenantAdmission()).toBeUndefined();
  });

  it('clears admitted display on server revocation, disconnect and close', async () => {
    for (const end of ['revoked', 'disconnect', 'close']) {
      const { client } = await fixture(); await client.localSessionReady;
      const pending = client.authenticate(auth); const socket = socketFor(client);
      socket.receive({ type: 'auth.result', id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
      await pending; expect(client.tenantAdmission()).toBeDefined();
      if (end === 'revoked') socket.receive({ type: 'auth.error', id: 'membership-changed', code: 'revoked', error: 'removed' });
      else if (end === 'disconnect') socket.close();
      else client.close();
      expect(client.tenantAdmission()).toBeUndefined();
    }
  });
});

describe('admission independent of working-set hydration', () => {
  it('accepts the server session while row hydration remains blocked', async () => {
    const { client, storage } = await fixture({ ...auth, tenant: 'new-tenant' });
    await client.localSessionReady;
    let release!: () => void;
    storage.loadWorkingSet.mockImplementationOnce(() => new Promise(resolve => { release = () => resolve(undefined); }));
    const accepted = client.authenticate({ ...auth, tenant: 'new-tenant' });
    const socket = socketFor(client);
    socket.receive({ type: 'auth.result', id: socket.sent[0].id, result: { replica: directive, localIdentity: identity } });
    await expect(accepted).resolves.toBeUndefined();
    expect(client.tenantAdmission()).toBeDefined();
    expect(client.localSessionState()).not.toBe('authenticated');
    expect(client.localReplica.entity('tasks', 'task')).toBeUndefined();
    let recoveryFinished = false;
    void (client as any).outboxReady.then(() => { recoveryFinished = true; });
    await Promise.resolve(); expect(recoveryFinished).toBe(false);
    const direct = client.reducer({ kind: 'reducer', path: 'tasks.edit' }, {}).catch(() => undefined);
    (client as any).serverCapabilities.reducerBatch = 1;
    const batch = client.reducerMany([{ ref: { kind: 'reducer', path: 'tasks.editMany' }, args: {} }]).catch(() => undefined);
    await Promise.resolve(); await Promise.resolve();
    expect(socket.sent.some(message => message.type === 'reducer.call' || message.type === 'reducer.callMany')).toBe(false);
    release();
    await vi.waitFor(() => expect(client.localSessionState()).toBe('authenticated'));
    expect(recoveryFinished).toBe(true);
    await vi.waitFor(() => expect(socket.sent.some(message => message.type === 'reducer.call')).toBe(true));
    await vi.waitFor(() => expect(socket.sent.some(message => message.type === 'reducer.callMany')).toBe(true));
    client.close(); await direct; await batch;
  });
});
