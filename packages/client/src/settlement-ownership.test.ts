import { afterEach, expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv, type FunctionReference } from "./index.js";
import type { ReducerOutboxEntry } from "./outbox.js";

class Socket {
  static CONNECTING = 0; static OPEN = 1; static CLOSED = 3;
  static all: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.all.push(this); }
  addEventListener(type: string, handler: (event: any) => void) { this.listeners.set(type, [...this.listeners.get(type) ?? [], handler]); }
  send(data: string) { this.sent.push(JSON.parse(data)); }
  emit(type: string, event = {}) { for (const handler of this.listeners.get(type) ?? []) handler(event); }
  open() { this.readyState = 1; this.emit("open"); }
  close() { this.readyState = 3; this.emit("close"); }
  receive(message: unknown) { this.emit("message", { data: JSON.stringify(message) }); }
}
const clients: GonvexClient[] = [];
afterEach(() => { for (const c of clients.splice(0)) c.close(); vi.unstubAllGlobals(); Socket.all = []; });
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
const url = "ws://settlement.test/ws";
const scopeFor = (name: string) => ["identity", url, "project", "tenant", "issuer", name].join("\u0000");
const directiveFor = (name: string) => ({ protocolVersion: 1 as const, scope: "tenant", visibilityScope: `visible-${name}`, epoch: "e" });
const identityFor = (name: string) => ({ auth: { account: { id: name } }, tenant: { id: "tenant" }, member: { id: name, accountId: name, permissions: {} } });
const ref: FunctionReference = { kind: "reducer", path: "tasks.save", offline: { mode: "allowed" } };
const modes = ["direct", "queued", "batch"] as const;

type Outcome = "accepted" | "update_required" | "unauthenticated" | "transient";
const outcomes: Outcome[] = ["accepted", "update_required", "unauthenticated", "transient"];
it.each(outcomes.flatMap(outcome => modes.flatMap(mode => [false, true].flatMap(rotate => (outcome === "accepted" ? ["wire", "replica"] as const : ["replica"] as const).map(stage => ({ outcome, mode, rotate, stage }))))))
("settles only the originating journal after $outcome persistence, $mode rotate=$rotate stage=$stage", async ({ mode, rotate, outcome, stage }) => {
  vi.stubGlobal("WebSocket", Socket);
  const storage = new MemoryLocalReplicaStorage();
  for (const name of ["alice", "bob"]) await storage.saveSession(scopeFor(name), {
    directive: directiveFor(name), identity: identityFor(name), artifactHash: "artifact",
  });
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const entered = deferred<ReducerOutboxEntry>(), release = deferred(), retired = deferred<number>();
  let held = false;
  const updateRequired = vi.fn();
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" },
    errorReporting: false, localReplica: { storage }, onUpdateRequired: updateRequired,
    outbox: { store: { ...base, shared: true, strictPersistence: true,
      put: async entry => {
        await base.put(entry);
        if (entry.scope === scopeFor("alice") && (outcome === "accepted"
          ? entry.state === "committed" && (stage === "wire" || !entry.awaitingReplica) : entry.lastErrorClass === outcome)) {
          held = true; entered.resolve(structuredClone(entry)); await release.promise;
        }
      },
      update: async (id, change) => {
        const entry = await base.update!(id, change);
        if (entry?.scope === scopeFor("alice") && (outcome === "accepted"
          ? entry.state === "committed" && (stage === "wire" || !entry.awaitingReplica) : entry.lastErrorClass === outcome)) {
          held = true; entered.resolve(structuredClone(entry)); await release.promise;
        }
        return entry;
      },
      delete: async id => { await base.delete(id); if (held) retired.resolve(id); },
    } },
  });
  clients.push(client);
  const options = { offline: "queue" as const, optimistic: [{ entity: "tasks", rowId: "shared", op: "upsert" as const, fields: { id: "shared", title: "alice" } }] };
  let direct: Promise<unknown> | undefined;
  try {
    await client.localSessionReady;
    if (mode !== "direct") await expect(client.reducer(ref, { name: "alice" }, options)).resolves.toMatchObject({ status: "queued" });
    client.connect();
    const socket = Socket.all.at(-1)!;
    socket.open();
    socket.receive({ type: "session.ready", capabilities: { replicaWatermark: 1, ...(mode === "batch" ? { reducerBatch: 1 } : {}) }, replica: directiveFor("alice") });
    await vi.waitFor(() => expect(socket.sent.some(m => m.type === "auth")).toBe(true));
    const auth = socket.sent.find(m => m.type === "auth");
    socket.receive({ type: "auth.result", id: auth.id, result: { replica: directiveFor("alice"), localIdentity: identityFor("alice"), accountId: "alice", tenantId: "tenant" } });
    await client.localSessionReady;
    if (mode === "direct") direct = client.reducer(ref, { name: "alice" }, options).then(value => ({ value }), error => ({ error }));
    const calls = () => socket.sent.flatMap(m => m.type === "reducer.callMany" ? m.calls : m.type === "reducer.call" ? [m] : []);
    await vi.waitFor(() => expect(calls()).toHaveLength(1));
    const call = calls()[0]!;
    if (outcome === "accepted") {
      socket.receive({ type: "reducer.result", id: call.id, path: call.path, result: "accepted", originCommandId: call.id, committedRevision: 8 });
      socket.receive({ type: "replica.watermark", revision: 8 });
    } else {
      socket.receive({ type: "reducer.error", id: call.id, error: `old-${outcome}`, class: outcome, retryable: true });
    }
    const alice = await entered.promise;
    let bob: ReducerOutboxEntry | undefined;
    if (rotate) {
      bob = { ...alice, id: alice.id + 1, scope: scopeFor("bob"), args: { name: "bob" }, state: outcome === "accepted" ? "pending" : "failed", awaitingReplica: undefined, lastError: undefined, lastErrorClass: undefined,
        patches: [{ entity: "tasks", rowId: "shared", op: "upsert", fields: { id: "shared", title: "bob" } }] };
      await base.put(bob);
      client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
      await client.localSessionReady;
      expect(client.localReplica.entity("tasks", "shared")).toEqual({ id: "shared", title: "bob" });
      if (outcome !== "accepted") {
        const bobAuth = socket.sent.filter(m => m.type === "auth").at(-1)!;
        socket.receive({ type: "auth.result", id: bobAuth.id, result: { replica: directiveFor("bob"), localIdentity: identityFor("bob"), accountId: "bob", tenantId: "tenant" } });
        await client.localSessionReady;
      }
    }
    release.resolve();
    const shouldRetire = outcome === "accepted" && (!rotate || stage === "replica");
    if (shouldRetire) {
      // Observe retirement itself, not elapsed time after release.
      expect(await retired.promise).toBe(alice.id);
    }
    const result = direct ? await direct : undefined;
    // All continuations after the released store write are promise jobs. Reach
    // the next turn before inspecting auth sends and update notifications.
    await new Promise(resolve => setTimeout(resolve, 0));
    const rows = await base.load();
    expect(rows).toEqual(shouldRetire ? (bob ? [bob] : []) : (bob ? [alice, bob] : [alice]));
    if (mode === "direct" && rotate) expect(result).toMatchObject({ error: { code: "superseded" } });
    if (outcome === "update_required") expect(updateRequired).toHaveBeenCalledTimes(rotate ? 0 : 1);
    if (outcome === "unauthenticated") {
      // setAuth sends Bob's auth immediately. Alice's delayed error must not
      // schedule another authentication for Bob after that request settles.
      expect(socket.sent.filter(m => m.type === "auth")).toHaveLength(2);
    }
    if (rotate) {
      expect(client.localReplica.entity("tasks", "shared")).toEqual({ id: "shared", title: "bob" });
      expect(await client.listIntents()).toEqual([expect.objectContaining({ id: bob!.idempotencyKey, entryId: bob!.id, state: bob!.state })]);
    }
  } finally { release.resolve(); client.close(); if (direct) await direct; }
});

