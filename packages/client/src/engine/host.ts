import { EngineAdmissionCohort } from "./cohort.js";
import { connectEngine } from "./connect.js";
import { installEngineAdmissions } from "./admissions.js";
import { engineScope, readReplicaEntities, closeEngine } from "./legacy.js";
import { GonvexClient, GonvexClientError, type GonvexClientOptions, type FunctionReference, type EngineCommand } from "../index.js";
import type { JsonValue } from "@gonvex/protocol";
import { browserUpgradeStorage, type BrowserUpgradeOptions } from "../browser-upgrades.js";
import { diffEngineValue } from "./patch.js";
import { EngineReceipts } from "./receipts.js";
import { serializeEngineError, sendEngineMessage, type EngineEndpoint, type EngineEnvelope } from "./rpc.js";

export type EngineStorageOptions = Omit<BrowserUpgradeOptions, "migrations" | "onRequired">;
export type EngineInit = { protocolVersion: 1; url: string; options: Omit<GonvexClientOptions, "localRuntime" | "localReplica" | "outbox" | "fetchToken" | "webSocketConstructor" | "onUpdateRequired" | "onLocalExecutionFallback">;
  storage: EngineStorageOptions; residency?: { maxResidentRows?: number; maxResidentBytes?: number }; ownerLock?: string; fetchToken: boolean; artifactHash?: string };
export type StaticEngineBinding = NonNullable<GonvexClientOptions["localRuntime"]>;

/** Used by the worker and by the asynchronous legacy fallback. */
export function serveEngineHost(endpoint: EngineEndpoint, binding?: StaticEngineBinding, networkConstructor?: typeof WebSocket, migrations: BrowserUpgradeOptions["migrations"] = []) {
  let client: GonvexClient | undefined;
  let stores: ReturnType<typeof browserUpgradeStorage> | undefined;
  let receipts: EngineReceipts | undefined;
  let generation = "", scope = "";
  let admission = Promise.resolve();
  let admissionStart = Promise.resolve();
  let pendingStarts = 0;
  const commands = new Map<string, { fingerprint: string; operation: Promise<unknown> }>();
  let sequence = 0;
  let unsubscribeIntents: (() => void) | undefined;
  let releaseOwnership: (() => void) | undefined;
  const subscriptions = new Map<string, () => void>();
  const readers = new Map<string, () => unknown>();
  const previous = new Map<string, unknown>();
  const dirty = new Set<string>();
  let flushing = false, queued = false;
  const calls = new Set<Promise<unknown>>();
  const tokenWaiters = new Map<string, { resolve(value: string | null): void; reject(error: Error): void; timer: ReturnType<typeof setTimeout> }>();
  const event = (name: string, args: unknown[]) => {
    const delivery = sendEngineMessage(endpoint, { generation, scope, id: crypto.randomUUID(), event: name, args });
    void delivery.catch(() => undefined); return delivery;
  };
  const scheduleBatch = () => {
    if (flushing || queued || !dirty.size) return;
    queued = true;
    queueMicrotask(() => {
      queued = false;
      const updates: unknown[] = [];
      for (const id of dirty) {
        const read = readers.get(id); if (!read) continue;
        try {
          const value = read();
          const patches = previous.has(id) ? diffEngineValue(previous.get(id), value) : undefined;
          if (patches?.length === 0) continue;
          updates.push(patches ? { id, patches } : { id, value }); previous.set(id, value);
        } catch (error) { previous.delete(id); updates.push({ id, error: serializeEngineError(error) }); }
      }
      dirty.clear();
      if (!updates.length) return;
      flushing = true;
      void event("watchBatch", [++sequence, updates]).finally(() => { flushing = false; scheduleBatch(); }).catch(() => undefined);
    });
  };
  const fetchToken = (args: { forceRefreshToken: boolean }) => new Promise<string | null>((resolve, reject) => {
    const id = crypto.randomUUID();
    const timer = setTimeout(() => { tokenWaiters.delete(id); reject(new Error("UI auth fetch timed out")); }, 20_000);
    tokenWaiters.set(id, { resolve, reject, timer }); event("fetchToken", [id, args]);
  });
  const watch = (id: string, kind: string, args: any[]) => {
    subscriptions.get(id)?.();
    const c = client!;
    let read: () => unknown, release: () => void;
    const publish = () => { dirty.add(id); scheduleBatch(); };
    if (kind === "entities") {
      const [entity, ids] = args as [string, string[]];
      const retain = c.retainReplicaEntities(entity, ids);
      let ready = false, readGeneration = 0;
      read = () => ready ? ({ status: "ready", rows: c.replicaEntities(entity, ids) }) : ({ status: "pending" });
      const coldRead = () => {
        const expected = ++readGeneration; ready = false; publish();
        if (c.localSessionState() === "unavailable") return;
        void readReplicaEntities(c, entity, ids).then(() => {
          if (expected !== readGeneration) return;
          ready = true; publish();
        }, error => { if (expected === readGeneration) void event("watchError", [id, serializeEngineError(error)]); });
      };
      const unsubscribe = c.localReplica.subscribe(publish, { entity, ids });
      let session = c.localSessionState();
      const unsubscribeSession = c.onLocalSessionChange(() => {
        const previousSession = session; session = c.localSessionState();
        if (session === "unavailable" || previousSession === "unavailable") coldRead();
        else publish();
      });
      const unsubscribeScope = c.onSessionScopeChange(coldRead);
      coldRead();
      release = () => { readGeneration++; unsubscribe(); unsubscribeSession(); unsubscribeScope(); retain(); };
    } else if (kind === "retained" || kind === "liveState") {
      const ref = args[0] as FunctionReference;
      const signature = typeof args[0] === "string" ? args[0] : c.replicaSignature(ref, args[1]);
      const retain = c.retainReplicaWindow(signature);
      const closeTransport = kind === "liveState" ? c.subscribeLiveQuery(ref, args[1], () => undefined) : () => undefined;
      let ready = false;
      void c.localSessionReady.then(value => { ready = value; publish(); });
      const unsubscribeSession = c.onLocalSessionChange(() => { ready = c.localSessionState() !== "unavailable"; publish(); });
      read = () => {
        if (!ready) return undefined;
        if (kind !== "liveState" || c.localReplica.freshness() !== "offline") return c.localReplica.liveQuerySnapshot(signature);
        const offline = c.offlineLiveQuery(ref, args[1]);
        return { rows: offline.rows, ids: offline.rows.map(row => String(row[ref.live!.key] ?? "")).filter(Boolean),
          ...(offline.total === undefined ? {} : { total: offline.total }), ...(offline.offset === undefined ? {} : { offset: offline.offset }),
          ...(offline.limit === undefined ? {} : { limit: offline.limit }), source: "cache", completeness: offline.completeness,
          freshness: "offline", supported: offline.supported, ...(offline.unsupportedOperator ? { unsupportedOperator: offline.unsupportedOperator } : {}) };
      };
      const unsubscribe = c.localReplica.subscribe(publish, { window: signature, offlineGlobal: kind === "liveState" });
      release = () => { unsubscribe(); unsubscribeSession(); closeTransport(); retain(); };
    } else if (kind === "collectionRetention") {
      const w = c.watchReplica(args[0], args[1]);
      read = () => w.status();
      release = w.onUpdate(publish);
    } else if (kind === "replica") {
      const w = c.watchReplica(args[0], args[1]);
      read = () => ({ rows: w.localReplicaResult(), state: w.localReplicaState(), status: w.status() });
      release = w.onUpdate(publish);
    } else if (kind === "live") {
      const w = c.watchLiveQuery(args[0], args[1]);
      read = () => ({ result: w.localLiveQueryResult() }); release = w.onUpdate(publish);
    } else if (kind === "control") {
      const w = c.watchControlQuery(args[0], args[1]);
      read = () => w.getSnapshot(); release = w.onUpdate(publish);
    } else throw new Error("Unknown engine watch");
    subscriptions.set(id, release); readers.set(id, read); previous.delete(id);
    void c.localSessionReady.then(publish);
    publish();
  };
  const run = async (request: EngineEnvelope, cohort?: EngineAdmissionCohort): Promise<unknown> => {
    const args = request.args as any[];
    if (request.method === "init") {
      if (client) throw new Error("Engine already initialized");
      generation = request.generation; scope = request.scope;
      const init = args[0] as EngineInit;
      if (init.protocolVersion !== 1) throw new Error("Engine RPC protocol version mismatch");
      if (init.artifactHash !== binding?.artifactHash || (binding && binding.mode !== "portable")) throw new Error("Engine requires matching static portable reducer artifact");
      if (networkConstructor) {
        if (!init.ownerLock || !globalThis.navigator?.locks) throw new Error("Worker ownership lock unavailable");
        await new Promise<void>((resolve, reject) => {
          void navigator.locks.request(init.ownerLock!, { mode: "exclusive", ifAvailable: true }, async lock => {
            if (!lock) { reject(new Error("Another engine owns this journal")); return; }
            resolve(); await new Promise<void>(release => { releaseOwnership = release; });
          }).catch(reject);
        });
      }
      stores = browserUpgradeStorage({ ...init.storage, migrations }); await stores.ready;
      receipts = new EngineReceipts(init.storage.outboxName); await receipts.openJournal();
      client = new GonvexClient(init.url, { ...init.options, localRuntime: binding,
        fetchToken: init.fetchToken ? fetchToken : undefined,
        localReplica: { storage: stores.storage, ...init.residency },
        outbox: { store: receipts.wrap(stores.store) } });
      installEngineAdmissions(client, receipts);
      if (networkConstructor) {
        const hostClient = client;
        Object.defineProperty(client, "connect", { value: () => connectEngine(hostClient, networkConstructor) });
      }
      // The initial restore activates the cached visibility scope after init answered. That first activation
      // is marked so the UI does not supersede RPCs issued during hydration; later changes invalidate as before.
      let restoring = true, scopeActivated = false;
      client.onSessionScopeChange(() => {
        previous.clear(); const restoreActivation = restoring && !scopeActivated; scopeActivated = true;
        void event("scope", restoreActivation ? [engineScope(client!), "restore"] : [engineScope(client!)]);
      });
      client.subscribeToConnectionState(state => event("connection", [state]));
      client.onDeveloperCredential(token => event("developerCredential", [token]));
      client.onAuthError((message, details) => event("authError", [message, details]));
      client.onLocalSessionChange(() => event("session", [client!.localSessionState()]));

      client.onLocalExecutionFallback(value => event("fallback", [value]));
      client.onReducerRejection(value => event("rejection", [value]));
      // Restoring hydrates the whole local scope. Answer init first so the UI can register its visible
      // watches during hydration; later transitions arrive as session events, and a failed restore
      // falls back to legacy as a failed init did.
      void client.localSessionReady.then(() => { restoring = false; }, error => { restoring = false; void event("startupError", [serializeEngineError(error)]); });
      return { connection: client.connectionState(), session: client.localSessionState(), commandScope: engineScope(client), capabilities: { admissionBatch: 1, admissionBatchLimit: 1024 } };
    }
    if (!client) throw new Error("Engine is not initialized");
    if (request.method === "fetchTokenResult") {
      const waiter = tokenWaiters.get(args[0]); if (waiter) { tokenWaiters.delete(args[0]); clearTimeout(waiter.timer); args[2] ? waiter.reject(new Error(args[2])) : waiter.resolve(args[1]); } return;
    }
    if (request.method === "watch") { watch(args[0], args[1], args[2]); return; }
    if (request.method === "unwatch") { subscriptions.get(args[0])?.(); subscriptions.delete(args[0]); readers.delete(args[0]); previous.delete(args[0]); dirty.delete(args[0]); return; }
    if (request.method === "observeIntents") {
      unsubscribeIntents?.(); unsubscribeIntents = undefined;
      if (args[0]) { unsubscribeIntents = client.subscribeIntents(() => { void event("intents", [client!.intentsSnapshot()]); }); void event("intents", [client.intentsSnapshot()]); }
      return;
    }
    if (request.method === "resnapshot") { previous.delete(args[0]); dirty.add(args[0]); scheduleBatch(); return; }
    if (request.method === "reducer") {
      const [ref, value, options] = args as [FunctionReference, JsonValue, { engineCommand: EngineCommand; engineFresh?: boolean; offline?: "queue" | "reject" }];
      const command = options.engineCommand;
      if (!command || typeof command.id !== "string" || !command.id || command.id.length > 200 || !Number.isSafeInteger(command.now) || !/^[0-9a-f]{64}$/.test(command.entropy)) throw new Error("Invalid engine command identity");
      if (ref.localExecution === 1 && command.artifactHash !== binding?.artifactHash) throw new Error("Engine command artifact mismatch");
      const durable = ref.scope !== "control" && (ref.localExecution === 1 || ref.offline?.mode === "allowed" || options.offline === "queue");
      const admittedScope = engineScope(client);
      if (command.scope !== admittedScope) throw new GonvexClientError("Engine command belongs to another account scope", { code: "superseded" });
      const begin = async () => {
        if (engineScope(client!) !== admittedScope) throw new GonvexClientError("Account changed before engine admission", { code: "superseded" });
        const prior = durable && !options.engineFresh ? await receipts!.findReceipt(admittedScope, options.engineCommand.id, ref.path, value) : undefined;
        if (engineScope(client!) !== admittedScope) throw new GonvexClientError("Account changed during engine admission", { code: "superseded" });
        return { operation: prior ? Promise.resolve(prior.result) : client!.reducer(ref, value, options) };
      };
      const key = JSON.stringify([admittedScope, command.id]);
      const fingerprint = JSON.stringify([ref.path, value]);
      const running = commands.get(key);
      if (running) {
        if (running.fingerprint !== fingerprint) throw new Error("Engine command ID reused with different arguments");
        return running.operation;
      }
      // The client orders atomic admissions. Do not serialize here across
      // their durable acknowledgements, which would stall reducer preparation.
      // Serialize only lookup/start so disk reads cannot reorder dependent
      // calls. Release this lane before waiting for durable acknowledgement.
      const ordered = ref.localExecution === 1 && (!options.engineFresh || pendingStarts > 0);
      const started = cohort ? cohort.enqueue(options, begin) : ordered ? admissionStart.then(begin) : begin();
      if (ordered && !cohort) {
        pendingStarts++;
        admissionStart = started.then(() => { pendingStarts--; }, () => { pendingStarts--; });
      }
      const result = started.then(({ operation }) => operation).then(async result => {
        if (engineScope(client!) !== admittedScope) throw new GonvexClientError("Account changed during engine result persistence", { code: "superseded" });
        if (durable && ref.localExecution !== 1) {
          const receipt = await receipts!.findReceipt(admittedScope, command.id, ref.path, value);
          if (!receipt || JSON.stringify(receipt.result) !== JSON.stringify(result)) await receipts!.put(admittedScope, command.id, ref.path, value, result, command.now);
          if (engineScope(client!) !== admittedScope) throw new GonvexClientError("Account changed during engine result persistence", { code: "superseded" });
        }
        return result;
      });
      if (ref.localExecution === 1) admission = Promise.allSettled([admission, result]).then(() => undefined);
      commands.set(key, { fingerprint, operation: result });
      void result.finally(() => commands.delete(key)).catch(() => undefined);
      return result;
    }
    if (request.method === "settle") { await admission; await Promise.allSettled([...calls]); return; }
    if (request.method === "close") {
      await admission;
      for (const release of subscriptions.values()) release(); subscriptions.clear(); readers.clear(); previous.clear(); dirty.clear();
      // Release storage and ownership even when a failed restore rejects a client lane, so a fallback can reopen them.
      try { unsubscribeIntents?.(); await closeEngine(client); } finally { stores?.close(); receipts?.close(); releaseOwnership?.(); }
      return;
    }
    if (request.method === "localSessionReady") return client.localSessionReady;
    if (request.method === "connect") { client.connect(); return; }
    if (request.method === "readEntities") return readReplicaEntities(client, args[0], args[1]);
    if (request.method === "setAuth" || request.method === "authenticate") {
      const auth = { ...args[0], fetchToken: args[1] ? fetchToken : undefined };
      return request.method === "authenticate" ? client.authenticate(auth) : client.setAuth(auth);
    }
    const allowed = ["query", "action", "prepareReducer", "retryIntent", "discardIntent", "resetLocalReplica", "listIntents", "listOutboxScopes", "purgeOutboxScope", "purgeForeignOutboxScopes"] as const;
    if (!allowed.includes(request.method as any)) throw new Error("Unknown engine operation");
    return (client[request.method as typeof allowed[number]] as (...args: any[]) => unknown).apply(client, args);
  };
  let activeCohort: EngineAdmissionCohort | undefined;
  const receive = ({ data }: MessageEvent, cohort?: EngineAdmissionCohort) => {
    if (Array.isArray(data?.admissions) && data.admissions.length <= 1024) {
      for (const admission of data.admissions) if (admission?.method === "reducer") receive({ data: admission } as MessageEvent);
      return;
    }
    if (!data || typeof data.id !== "string" || !Array.isArray(data.args)) return;
    const request = data as EngineEnvelope;
    if (request.method !== "init" && (request.generation !== generation || request.scope !== scope)) return;
    if (!cohort && client && request.method === "reducer" && (request.args[0] as FunctionReference)?.localExecution === 1) {
      const group = activeCohort?.canAccept() ? activeCohort : pendingStarts === 0 ? new EngineAdmissionCohort(client) : undefined;
      if (group) {
        const existing = group === activeCohort;
        activeCohort = group;
        receive({ data } as MessageEvent, group);
        if (!existing) {
          pendingStarts++;
          admissionStart = group.flush().then(() => {
            pendingStarts--; if (activeCohort === group) activeCohort = undefined;
          }, () => { pendingStarts--; if (activeCohort === group) activeCohort = undefined; });
        }
        return;
      }
    }
    if (!cohort) activeCohort?.seal();
    const operation = run(request, cohort);
    if (request.method !== "settle") calls.add(operation);
    void operation.then(result => sendEngineMessage(endpoint, { generation: request.generation, scope: request.scope, id: request.id, result }),
      error => sendEngineMessage(endpoint, { generation: request.generation, scope: request.scope, id: request.id,
        error: serializeEngineError(error) }))
      .finally(() => calls.delete(operation)).catch(() => undefined);
  };
  endpoint.addEventListener("message", receive); endpoint.start?.();
  return () => { endpoint.removeEventListener("message", receive); endpoint.close?.(); };
}
