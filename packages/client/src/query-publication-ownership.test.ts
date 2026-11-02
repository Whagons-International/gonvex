import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, type FunctionReference } from "./index.js";

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
const reference: FunctionReference = { kind: "query", path: "tasks.list", delivery: "live",
  live: { entity: "tasks", key: "id", resultPath: [], plan: { table: "tasks", key: "id", columns: ["id", "title"] } } };
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
async function authenticate(socket: Socket, account: string, tenant = "tenant", kind = "query.subscribe") {
  await vi.waitFor(() => expect(socket.sent.some(frame => frame.type === "auth")).toBe(true));
  let auth = socket.sent.filter(frame => frame.type === "auth").at(-1);
  const before = socket.sent.filter(frame => frame.type === kind).length;
  const accept = () => socket.receive({ type: "auth.result", id: auth.id, result: { accountId: account, tenantId: tenant,
    replica: { protocolVersion: 1, scope: "tenant", visibilityScope: `visible-${account}-${tenant}`, epoch: "db" } } });
  accept();
  // A synchronous A -> B -> A change serializes the latest auth behind B.
  const latest = socket.sent.filter(frame => frame.type === "auth").at(-1);
  if (latest.id !== auth.id) { auth = latest; accept(); }
  await vi.waitFor(() => expect(socket.sent.filter(frame => frame.type === kind).length).toBeGreaterThan(before));
  return socket.sent.filter(frame => frame.type === kind).at(-1);
}
const result = (id: string, title: string, sequence = 1) => ({ type: "query.result", id, path: reference.path,
  result: [{ id: "private", title }], subscriptionRevision: { epoch: "db", sequence } });

it.each(["unchanged", "account", "tenant", "return", "close"] as const)(
  "live query listener publication retains ownership across reentrant %s", async change => {
    vi.stubGlobal("WebSocket", Socket);
    const client = new GonvexClient("ws://query-publication.test/ws", { project: "project", tenant: "tenant",
      token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
      localReplica: { storage: new MemoryLocalReplicaStorage() } });
    clients.push(client);
    const entered = deferred(); let acted = false;
    const later: unknown[] = [];
    const first = client.subscribeLiveQuery(reference, {}, message => {
      if (message.type !== "query.result" || acted) return;
      acted = true;
      if (change === "account" || change === "return") {
        client.setAuth({ token: "bob-session", identity: { sub: "bob", iss: "issuer" } });
        if (change === "return") client.setAuth({ token: "alice-new-session", identity: { sub: "alice", iss: "issuer" } });
      } else if (change === "tenant") client.setAuth({ tenant: "other" });
      else if (change === "close") client.close();
      entered.resolve();
    });
    const second = client.subscribeLiveQuery(reference, {}, message => {
      if (message.type === "query.result") later.push(message.result);
    });
    try {
      const socket = Socket.instances.at(-1)!; socket.open();
      const query = await authenticate(socket, "alice");
      socket.receive(result(query.id, "Alice private data"));
      await entered.promise;
      await new Promise(done => setTimeout(done, 0));
      if (change === "unchanged") expect(later).toEqual([[{ id: "private", title: "Alice private data" }]]);
      else expect(later, "old query message reached a later listener after owner invalidation").toEqual([]);
      if (change !== "close" && change !== "unchanged") {
        const fresh = await authenticate(socket, change === "account" ? "bob" : "alice", change === "tenant" ? "other" : "tenant");
        expect(fresh.id).not.toBe(query.id);
        socket.receive(result(fresh.id, "Current owner data"));
        await vi.waitFor(() => expect(later, "fresh owner result did not publish").toEqual([[{ id: "private", title: "Current owner data" }]]));
      }
    } finally { first(); second(); client.close(); }
  },
);


it.each(["unchanged", "account", "tenant", "return", "close"] as const)(
  "cached live query replay retains its scheduling owner across %s", async change => {
    vi.stubGlobal("WebSocket", Socket);
    const client = new GonvexClient("ws://query-cache.test/ws", { project: "project", tenant: "tenant",
      token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
      localReplica: { storage: new MemoryLocalReplicaStorage() } });
    clients.push(client); const initial = vi.fn(); const later: unknown[] = [];
    const first = client.subscribeLiveQuery(reference, {}, initial); let second = () => {};
    try {
      const socket = Socket.instances.at(-1)!; socket.open();
      const query = await authenticate(socket, "alice");
      socket.receive(result(query.id, "Alice cached data"));
      await vi.waitFor(() => expect(initial).toHaveBeenCalledTimes(1));
      second = client.subscribeLiveQuery(reference, {}, message => { if (message.type === "query.result") later.push(message.result); });
      if (change === "account" || change === "return") {
        client.setAuth({ token: "bob-session", identity: { sub: "bob", iss: "issuer" } });
        if (change === "return") client.setAuth({ token: "alice-new-session", identity: { sub: "alice", iss: "issuer" } });
      } else if (change === "tenant") client.setAuth({ tenant: "other" });
      else if (change === "close") client.close();
      await new Promise(done => setTimeout(done, 0));
      expect(later, "queued cache replay crossed its original owner").toEqual(change === "unchanged" ? [[{ id: "private", title: "Alice cached data" }]] : []);
      if (change !== "close" && change !== "unchanged") {
        const fresh = await authenticate(socket, change === "account" ? "bob" : "alice", change === "tenant" ? "other" : "tenant");
        socket.receive(result(fresh.id, "Current owner data", 2));
        await vi.waitFor(() => expect(later, "fresh owner result did not publish").toEqual([[{ id: "private", title: "Current owner data" }]]));
      }
    } finally { first(); second(); client.close(); }
  },
);

it("late query listeners wait for their cached result to reach durable storage", async () => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage();
  const original = storage.replaceWindow.bind(storage); const entered = deferred(), release = deferred();
  storage.replaceWindow = async (...args) => { entered.resolve(); await release.promise; return original(...args); };
  const client = new GonvexClient("ws://query-durability.test/ws", { project: "project", tenant: "tenant",
    token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false, localReplica: { storage } });
  clients.push(client); const first = vi.fn(); const later = vi.fn(); let stopLater = () => {};
  const stopFirst = client.subscribeLiveQuery(reference, {}, first);
  try {
    const socket = Socket.instances.at(-1)!; socket.open(); const query = await authenticate(socket, "alice");
    socket.receive(result(query.id, "Durable query data")); await entered.promise;
    stopLater = client.subscribeLiveQuery(reference, {}, later);
    await new Promise(done => setTimeout(done, 0));
    expect(first).not.toHaveBeenCalled();
    expect(later, "late listener received a query result before persistence").not.toHaveBeenCalled();
    expect((await storage.load("visible-alice-tenant"))?.entities.tasks?.private).toBeUndefined();
    release.resolve(); await vi.waitFor(() => expect(first).toHaveBeenCalledTimes(1));
    expect(later).toHaveBeenCalledTimes(1);
    expect((await storage.load("visible-alice-tenant"))?.entities.tasks?.private).toEqual({ id: "private", title: "Durable query data" });
  } finally { release.resolve(); stopFirst(); stopLater(); client.close(); }
});

it("query materialization failure cannot publish an unpersisted result and recovery still progresses", async () => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage(); const original = storage.replaceWindow.bind(storage);
  const entered = deferred(); let fail = true;
  storage.replaceWindow = async (...args) => { if (fail) { entered.resolve(); throw new Error("injected query window write failure"); } return original(...args); };
  const client = new GonvexClient("ws://query-failure.test/ws", { project: "project", tenant: "tenant",
    token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false, localReplica: { storage } });
  clients.push(client); const observed = vi.fn(); const stop = client.subscribeLiveQuery(reference, {}, observed);
  try {
    const socket = Socket.instances.at(-1)!; socket.open(); const query = await authenticate(socket, "alice");
    socket.receive(result(query.id, "Failed query data")); await entered.promise;
    await new Promise(done => setTimeout(done, 0));
    expect(observed, "query listener received a result whose storage write failed").not.toHaveBeenCalled();
    expect((await storage.load("visible-alice-tenant"))?.entities.tasks?.private).toBeUndefined();
    fail = false; socket.receive(result(query.id, "Recovered query data", 2));
    await vi.waitFor(() => expect(observed).toHaveBeenCalledTimes(1));
    expect(observed.mock.calls[0]![0].result).toEqual([{ id: "private", title: "Recovered query data" }]);
  } finally { stop(); client.close(); }
});

it.each(["unchanged", "return", "close"] as const)("deferred query publication retains its original activation across %s", async change => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage(); const original = storage.replaceWindow.bind(storage);
  const entered = deferred(), release = deferred(); let hold = true;
  storage.replaceWindow = async (...args) => {
    await original(...args);
    if (hold) { hold = false; entered.resolve(); await release.promise; }
  };
  const client = new GonvexClient("ws://query-deferred.test/ws", { project: "project", tenant: "tenant",
    token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false, localReplica: { storage } });
  clients.push(client); const observed = vi.fn(); const stop = client.subscribeLiveQuery(reference, {}, observed);
  try {
    const socket = Socket.instances.at(-1)!; socket.open(); const query = await authenticate(socket, "alice");
    socket.receive(result(query.id, "Old activation data")); await entered.promise;
    let fresh = query;
    if (change === "return") {
      client.setAuth({ token: "bob-session", identity: { sub: "bob", iss: "issuer" } });
      await authenticate(socket, "bob");
      client.setAuth({ token: "alice-new-session", identity: { sub: "alice", iss: "issuer" } });
      fresh = await authenticate(socket, "alice");
      expect(fresh.id).not.toBe(query.id);
    } else if (change === "close") client.close();
    release.resolve(); await new Promise(done => setTimeout(done, 0));
    expect(observed, "old query completion published into a later activation").toHaveBeenCalledTimes(change === "unchanged" ? 1 : 0);
    if (change === "return") {
      socket.receive(result(fresh.id, "Current activation data", 2));
      await vi.waitFor(() => expect(observed).toHaveBeenCalledTimes(1));
      expect(observed.mock.calls[0]![0].result).toEqual([{ id: "private", title: "Current activation data" }]);
    }
  } finally { release.resolve(); stop(); client.close(); }
});


const replicaReference: FunctionReference = { kind: "query", path: "tasks.all", delivery: "replica",
  replica: { table: "tasks", key: "id", columns: ["id", "title"], maxRows: 100 } };
const snapshot = (id: string, title: string, revision = 1) => ({ type: "replica.snapshot", id, path: replicaReference.path,
  result: [{ id: "private", title }], key: "id", cursor: { epoch: "db", revision }, maxRows: 100 });

it.each(["unchanged", "account", "tenant", "return", "close"] as const)(
  "replica listener publication retains ownership across reentrant %s", async change => {
    vi.stubGlobal("WebSocket", Socket);
    const client = new GonvexClient("ws://replica-publication.test/ws", { project: "project", tenant: "tenant",
      token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
      localReplica: { storage: new MemoryLocalReplicaStorage() } });
    clients.push(client); const entered = deferred(); let acted = false; const later: unknown[] = [];
    const first = client.subscribeReplica(replicaReference, {}, message => {
      if (message.type !== "replica.snapshot" || acted) return;
      acted = true;
      if (change === "account" || change === "return") {
        client.setAuth({ token: "bob-session", identity: { sub: "bob", iss: "issuer" } });
        if (change === "return") client.setAuth({ token: "alice-new-session", identity: { sub: "alice", iss: "issuer" } });
      } else if (change === "tenant") client.setAuth({ tenant: "other" });
      else if (change === "close") client.close();
      entered.resolve();
    });
    const second = client.subscribeReplica(replicaReference, {}, message => { if (message.type === "replica.snapshot") later.push(message.result); });
    try {
      const socket = Socket.instances.at(-1)!; socket.open(); const open = await authenticate(socket, "alice", "tenant", "replica.open");
      socket.receive(snapshot(open.id, "Alice replica data")); await entered.promise;
      await new Promise(done => setTimeout(done, 0));
      expect(later, "old replica message reached a later listener after owner invalidation").toEqual(change === "unchanged" ? [[{ id: "private", title: "Alice replica data" }]] : []);
      if (change !== "close" && change !== "unchanged") {
        const fresh = await authenticate(socket, change === "account" ? "bob" : "alice", change === "tenant" ? "other" : "tenant", "replica.open");
        socket.receive(snapshot(fresh.id, "Current replica data", 2));
        await vi.waitFor(() => expect(later).toEqual([[{ id: "private", title: "Current replica data" }]]));
      }
    } finally { first(); second(); client.close(); }
  },
);

it.each(["unchanged", "account", "tenant", "return", "close"] as const)(
  "cached replica replay retains its scheduling owner across %s", async change => {
    vi.stubGlobal("WebSocket", Socket);
    const client = new GonvexClient("ws://replica-cache.test/ws", { project: "project", tenant: "tenant",
      token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
      localReplica: { storage: new MemoryLocalReplicaStorage() } });
    clients.push(client); const initial = vi.fn(); const later: unknown[] = [];
    const first = client.subscribeReplica(replicaReference, {}, initial); let second = () => {};
    try {
      const socket = Socket.instances.at(-1)!; socket.open(); const open = await authenticate(socket, "alice", "tenant", "replica.open");
      socket.receive(snapshot(open.id, "Alice replica cache"));
      await vi.waitFor(() => expect(initial.mock.calls.some(([message]) => message.type === "replica.snapshot")).toBe(true));
      second = client.subscribeReplica(replicaReference, {}, message => { if (message.type === "replica.snapshot") later.push(message.result); });
      if (change === "account" || change === "return") {
        client.setAuth({ token: "bob-session", identity: { sub: "bob", iss: "issuer" } });
        if (change === "return") client.setAuth({ token: "alice-new-session", identity: { sub: "alice", iss: "issuer" } });
      } else if (change === "tenant") client.setAuth({ tenant: "other" });
      else if (change === "close") client.close();
      await new Promise(done => setTimeout(done, 0));
      expect(later, "queued replica cache replay crossed its original owner").toEqual(change === "unchanged" ? [[{ id: "private", title: "Alice replica cache" }]] : []);
      if (change !== "close" && change !== "unchanged") {
        const fresh = await authenticate(socket, change === "account" ? "bob" : "alice", change === "tenant" ? "other" : "tenant", "replica.open");
        socket.receive(snapshot(fresh.id, "Current replica cache", 2));
        await vi.waitFor(() => expect(later).toEqual([[{ id: "private", title: "Current replica cache" }]]));
      }
    } finally { first(); second(); client.close(); }
  },
);

// Non-row results and errors perform no entity write, but must still follow
// the preceding committed result instead of overtaking its storage promise.
it.each(["result", "error"] as const)("query publication preserves wire order when a later %s has no rows", async kind => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage(); const original = storage.replaceWindow.bind(storage);
  const entered = deferred(), release = deferred();
  storage.replaceWindow = async (...args) => { entered.resolve(); await release.promise; return original(...args); };
  const client = new GonvexClient("ws://query-order.test/ws", { project: "project", tenant: "tenant",
    token: "alice-session", identity: { sub: "alice", iss: "issuer" }, errorReporting: false, localReplica: { storage } });
  clients.push(client); const observed: any[] = []; const stop = client.subscribeLiveQuery(reference, {}, message => observed.push(message));
  try {
    const socket = Socket.instances.at(-1)!; socket.open(); const query = await authenticate(socket, "alice");
    socket.receive(result(query.id, "First durable result")); await entered.promise;
    socket.receive(kind === "result" ? { ...result(query.id, "unused", 2), result: "Second result" }
      : { type: "query.error", id: query.id, error: "Second result failed" });
    await new Promise(done => setTimeout(done, 0));
    expect(observed, "later query publication passed a preceding durable write").toEqual([]);
    release.resolve(); await vi.waitFor(() => expect(observed).toHaveLength(2));
    expect(observed[0].result).toEqual([{ id: "private", title: "First durable result" }]);
    expect(observed[1].type).toBe(kind === "result" ? "query.result" : "query.error");
  } finally { release.resolve(); stop(); client.close(); }
});
