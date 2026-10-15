import { reducerOutboxScope } from "../auth-scope.js";
import type { CallOptions, ConnectionState, EngineCommand, QueuedReducerOutcome, FunctionReference, GonvexClientAuth, GonvexClientOptions, OutboxIntent, ResetLocalReplicaOptions, ResetLocalReplicaResult, LiveQueryResult, ReplicaCollectionSubscriptionState, ReplicaRow } from "../index.js";
import type { JsonValue } from "@gonvex/protocol";
import { setEngineProperty, applyEnginePatches } from "./patch.js";
import { deserializeEngineError, EngineRpcUnavailable, EngineRpcTimeout, EngineRpc, type EngineEndpoint } from "./rpc.js";
import type { EngineInit, EngineStorageOptions, StaticEngineBinding } from "./host.js";
export { EngineRpcTimeout, EngineRpcUnavailable } from "./rpc.js";
export type { EngineStorageOptions } from "./host.js";
export type EngineEntitiesState<T extends ReplicaRow = ReplicaRow> = { status: "pending" } | { status: "ready"; rows: Array<T | undefined> };

export type EngineClientOptions = {
  engine: { workerUrl: string | URL; artifactHash?: string; timeoutMs?: number; workerFactory?: () => Worker };
  auth: GonvexClientAuth;
  storage: EngineStorageOptions;
  /** Required to continue on the UI when workers/coordination are unavailable. */
  fallbackBinding?: StaticEngineBinding;
  residency?: { maxResidentRows?: number; maxResidentBytes?: number };
  options?: Pick<GonvexClientOptions, "clientContract" | "telemetry" | "timeouts" | "replicaSnapshotPages" | "localDependencyResidency">;
  onDiagnostic?: (message: string) => void;
};

type WatchState = { id: string; kind: string; args: unknown[]; value?: any; error?: Error; sequence: number; listeners: Set<() => void>; rowKeys: Set<string> };

function reconcile(previous: any, next: any, ownedRows?: WeakSet<object>): any {
  if (Object.is(previous, next)) return previous;
  if (ownedRows?.has(next)) return next;
  if (next === null || typeof next !== "object") return next;
  const array = Array.isArray(next);
  let equal = previous !== null && typeof previous === "object" && array === Array.isArray(previous) && Object.keys(previous).length === Object.keys(next).length;
  const result: any = array ? [] : {};
  for (const key of Object.keys(next)) { setEngineProperty(result, key, reconcile(previous?.[key], next[key], ownedRows)); if (!Object.hasOwn(previous ?? {}, key) || result[key] !== previous?.[key]) equal = false; }
  return equal ? previous : Object.freeze(result);
}

/** Only mounted results live here. Cold reads always cross an awaited boundary. */
export class EngineClient {
  engineMode: "worker" | "legacy" = "worker";
  private rpc!: EngineRpc;
  private endpoint?: EngineEndpoint;
  private worker?: Worker;
  private releaseFallback?: () => void;
  private releasePresence?: () => void;
  private channel?: BroadcastChannel;
  private switching?: Promise<void>;
  private readonly ownedRows = new WeakSet<object>();
  private readonly rowKeys = new WeakMap<object, { entity: string; key: string }>();
  private readonly rowCache = new Map<string, { value: ReplicaRow; owners: Set<string> }>();
  private readonly watches = new Map<string, WatchState>();
  private readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  private readonly inflight = new Set<Promise<unknown>>();
  private closed = false;
  private connection!: ConnectionState;
  private connectionOffset = 0;
  private session: "unavailable" | "restored" | "authenticated" = "unavailable";
  private intents: readonly OutboxIntent[] = [];
  private fetchToken?: GonvexClientAuth["fetchToken"];
  private auth: GonvexClientAuth;
  private readonly generation = crypto.randomUUID();
  private readonly scope = crypto.randomUUID();
  private epoch = 0;
  private connected = false;
  private commandScope = "";
  private ownerLock?: string;
  constructor(private readonly url: string, private readonly config: EngineClientOptions) { this.auth = config.auth; this.fetchToken = config.auth.fetchToken; }
  private emit(name: string, ...args: unknown[]) { for (const listener of this.listeners.get(name) ?? []) listener(...args); }
  private listen(name: string, listener: (...args: any[]) => void) { let set = this.listeners.get(name); if (!set) { set = new Set(); this.listeners.set(name, set); } set.add(listener); return () => { set!.delete(listener); }; }
  private invalidate(retire = false) {
    this.epoch += 1;
    this.rowCache.clear();
    for (const watch of this.watches.values()) { watch.value = undefined; watch.sequence = 0; watch.error = undefined; watch.rowKeys.clear(); }
    if (retire) {
      const active = [...this.watches.values()]; this.watches.clear();
      for (const watch of active) { const old = watch.id; watch.id = crypto.randomUUID(); this.watches.set(watch.id, watch); void this.admitted("unwatch", [old]).catch(() => undefined); }
    }
    this.intents = [];
    for (const watch of this.watches.values()) for (const listener of watch.listeners) listener();
    this.emit("intents"); this.emit("scope");
  }
  private attach(endpoint: EngineEndpoint) {
    this.endpoint = endpoint;
    this.rpc = new EngineRpc(endpoint, `${this.generation}:${this.epoch}`, this.scope);
    this.rpc.onCrash.add(error => {
      this.connection = { ...this.connection, isWebSocketConnected: false };
      if (this.session === "authenticated") this.session = "restored";
      this.emit("connection", this.connection); this.emit("session"); this.emit("error", error);
      for (const watch of this.watches.values()) {
        if (!watch.value) continue;
        const value = watch.value;
        watch.value = reconcile(value, watch.kind === "replica" ? { ...value, status: { ...value.status, isUpToDate: false },
          state: value.state ? { ...value.state, isUpToDate: false, source: "cache", freshness: "offline" } : undefined }
          : watch.kind === "retained" || watch.kind === "liveState" ? { ...value, source: "cache", freshness: "offline" } : value, this.ownedRows);
        for (const listener of watch.listeners) listener();
      }
    });
    this.rpc.onEvent.add((method, args) => {
      if (method === "fetchToken") {
        const [id, request] = args;
        void Promise.resolve().then(() => this.fetchToken?.(request)).then(token => this.call("fetchTokenResult", [id, token ?? null]), error => this.call("fetchTokenResult", [id, null, String(error)])).catch(() => undefined);
      } else if (method === "watchBatch") {
        const [sequence, updates] = args;
        const changed: WatchState[] = [];
        for (const update of updates) {
          const watch = this.watches.get(update.id);
          if (!watch || sequence <= watch.sequence) continue;
          watch.sequence = sequence;
          if (update.error) watch.error = deserializeEngineError(update.error);
          else {
            try {
              const value = update.patches ? applyEnginePatches(watch.value, update.patches) : update.value;
              watch.value = reconcile(watch.value, this.internRows(watch, value), this.ownedRows); watch.error = undefined;
            } catch {
              void this.admitted("resnapshot", [watch.id]).catch(() => undefined); continue;
            }
          }
          changed.push(watch);
        }
        // Every cache/result swap completes before the first subscriber wakes.
        for (const watch of changed) for (const listener of watch.listeners) listener();
      } else if (method === "watchError") {
        const watch = this.watches.get(args[0]); if (!watch) return;
        watch.error = deserializeEngineError(args[1]); for (const listener of watch.listeners) listener();
      } else if (method === "scope") { this.commandScope = args[0]; this.invalidate(); }
      else if (method === "connection") { this.connection = { ...args[0], hasEverConnected: this.connection?.hasEverConnected || args[0].hasEverConnected, connectionCount: this.connectionOffset + args[0].connectionCount }; this.emit(method, this.connection); }
      else if (method === "session") { this.session = args[0]; this.emit(method); }
      else if (method === "intents") { this.intents = args[0]; this.emit(method); }
      else this.emit(method, ...args);
    });
  }
  private internRows(watch: WatchState, value: any) {
    if (value === undefined) return undefined;
    const selected = new Set<string>();
    const ref = watch.args[0] as FunctionReference;
    const entity = watch.kind === "entities" ? watch.args[0] : ref?.replica?.table ?? ref?.live?.entity;
    const keyField = watch.kind === "entities" ? undefined : ref?.replica?.key ?? ref?.live?.key;
    const intern = (row: any, index = 0) => {
      if (typeof entity !== "string" || !row || typeof row !== "object") return row;
      const id = row[keyField ?? "_id"] ?? row.id ?? (watch.kind === "entities" ? (watch.args[1] as string[])[index] : undefined);
      if (id === undefined) return row;
      const known = this.rowKeys.get(row);
      const key = known?.entity === entity ? known.key : JSON.stringify([entity, String(id), Object.keys(row).sort()]);
      this.rowKeys.set(row, { entity, key });
      selected.add(key);
      const previous = this.rowCache.get(key);
      const retained = reconcile(previous?.value, row); this.ownedRows.add(retained);
      const owners = previous?.owners ?? new Set<string>(); owners.add(watch.id);
      this.rowCache.set(key, { value: retained, owners }); this.rowKeys.set(retained, { entity, key }); return retained;
    };
    const next = { ...value };
    if (Array.isArray(value?.rows)) next.rows = value.rows.map(intern);
    if (watch.kind === "live" && ref.live) {
      const materialize = (result: any, path: readonly string[]): any => {
        if (!path.length) return Array.isArray(result) ? result.map(intern) : intern(result);
        const [key, ...rest] = path;
        return result && typeof result === "object" ? { ...result, [key]: materialize(result[key], rest) } : result;
      };
      next.result = materialize(value.result, ref.live.resultPath ?? []);
    }
    if (value?.state?.rows) next.state = { ...value.state, rows: value.state.rows.map(intern) };
    for (const key of watch.rowKeys) if (!selected.has(key)) { const entry = this.rowCache.get(key); entry?.owners.delete(watch.id); if (!entry?.owners.size) this.rowCache.delete(key); }
    watch.rowKeys = selected; return next;
  }
  private releaseRows(watch: WatchState) {
    for (const key of watch.rowKeys) { const entry = this.rowCache.get(key); entry?.owners.delete(watch.id); if (!entry?.owners.size) this.rowCache.delete(key); }
    watch.rowKeys.clear();
  }
  private init(): EngineInit {
    const { fetchToken, ...auth } = this.auth;
    return { protocolVersion: 1, url: this.url, options: { ...this.config.options, ...auth }, fetchToken: Boolean(this.fetchToken), storage: this.config.storage, residency: this.config.residency, ownerLock: this.engineMode === "worker" ? this.ownerLock : undefined, artifactHash: this.config.engine.artifactHash };
  }
  private async initializeEndpoint() {
    const state = await this.call<{ connection: ConnectionState; session: "unavailable" | "restored" | "authenticated"; commandScope: string; capabilities?: { admissionBatch?: number; admissionBatchLimit?: number } }>("init", [this.init()]);
    this.rpc.setAdmissionBatching(state.capabilities?.admissionBatch === 1, state.capabilities?.admissionBatchLimit);
    this.connection = { ...state.connection, hasEverConnected: this.connection?.hasEverConnected || state.connection.hasEverConnected, connectionCount: this.connectionOffset + state.connection.connectionCount }; this.session = state.session; this.commandScope = state.commandScope;
  }
  async initialize(): Promise<void> {
    const locks = globalThis.navigator?.locks;
    if (!locks || typeof Worker !== "function" || typeof BroadcastChannel !== "function") { await this.useLegacy("Worker coordination unavailable"); return; }
    const endpoint = new URL(this.url, globalThis.location?.href); endpoint.hash = ""; endpoint.search = "";
    const bytes = new TextEncoder().encode(JSON.stringify([endpoint.href, this.auth.project, this.config.storage.replicaName, this.config.storage.outboxName]));
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    const name = `gonvex-engine-tabs:${Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join("")}`;
    this.ownerLock = name.replace("gonvex-engine-tabs:", "gonvex-engine:");
    await new Promise<void>((resolve, reject) => { void locks.request(name, { mode: "shared" }, async () => { resolve(); await new Promise<void>(release => { this.releasePresence = release; }); }).catch(reject); });
    this.channel = new BroadcastChannel(name);
    this.channel.onmessage = event => { if (event.data?.type === "multiple") void this.useLegacy("Multiple engine tabs detected").catch(error => this.emit("error", error)); };
    const held = (await locks.query()).held?.filter(lock => lock.name === name) ?? [];
    if (held.length > 1) { this.channel.postMessage({ type: "multiple" }); await this.useLegacy("Multiple engine tabs detected"); return; }
    try {
      this.worker = this.config.engine.workerFactory?.() ?? new Worker(this.config.engine.workerUrl, { type: "module", name: "gonvex-sync-engine" });
      this.attach(this.worker); await this.initializeEndpoint();
    } catch (error) {
      this.config.onDiagnostic?.(String(error));
      if (error instanceof EngineRpcTimeout) { this.rpc.close(); this.worker?.terminate(); this.releasePresence?.(); this.channel?.close(); throw error; }
      await this.useLegacy("Engine startup failed");
    }
  }
  private useLegacy(reason: string): Promise<void> {
    if (this.engineMode === "legacy") return Promise.resolve();
    if (this.switching) return this.switching.then(() => this.useLegacy(reason));
    this.switching = Promise.resolve().then(async () => {
      this.config.onDiagnostic?.(reason);
      // Stop admission first. Existing RPCs settle before releasing the worker.
      await Promise.allSettled([...this.inflight]);
      if (this.worker) {
        try { await this.call("settle", []); await this.call("close", []); } catch { /* Durable IDB admission remains recoverable. */ }
        this.rpc.close(); this.worker.terminate(); this.worker = undefined;
      }
      this.connectionOffset = this.connection?.connectionCount ?? 0;
      this.invalidate(); this.engineMode = "legacy";
      const { serveEngineHost } = await import("./host.js");
      const pair = new MessageChannel();
      this.releaseFallback = serveEngineHost(pair.port1, this.config.fallbackBinding);
      this.attach(pair.port2); await this.initializeEndpoint();
      await this.restartWatches(true);
      if (this.connected) await this.call("connect", []);
    }).finally(() => { this.switching = undefined; });
    return this.switching;
  }
  private call<T = unknown>(method: string, args: unknown[], timeout = this.config.engine.timeoutMs ?? 20_000): Promise<T> {
    const operation = this.rpc.call<T>(this.rpc.request(method, args), timeout);
    this.inflight.add(operation); void operation.finally(() => this.inflight.delete(operation)).catch(() => undefined); return operation;
  }
  private async admitted<T>(method: string, args: unknown[], timeout?: number): Promise<T> {
    await this.switching; if (this.closed) throw new Error("Engine client is closed");
    const epoch = this.epoch;
    const result = await this.call<T>(method, args, timeout);
    if (epoch !== this.epoch && ["query", "action", "reducer", "readEntities"].includes(method)) throw deserializeEngineError({ name: "GonvexClientError", message: "Account or visibility changed while awaiting engine RPC", code: "superseded" });
    return result;
  }
  createCommand(): EngineCommand { return { id: crypto.randomUUID(), now: Date.now(), artifactHash: this.config.engine.artifactHash, scope: this.commandScope, entropy: Array.from(crypto.getRandomValues(new Uint8Array(32)), byte => byte.toString(16).padStart(2, "0")).join("") }; }
  reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args, options: CallOptions & { offline: "queue" }): Promise<T | QueuedReducerOutcome>;
  reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args?: Args, options?: CallOptions): Promise<T>;
  async reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, options: CallOptions = {}): Promise<T | QueuedReducerOutcome> {
    // Expose createCommand so an application can retain the identity across a crash/reopen.
    try {
      return await this.admitted("reducer", [structuredClone(ref), structuredClone(args), structuredClone({ ...options, engineFresh: options.engineCommand === undefined, engineCommand: options.engineCommand ?? this.createCommand() })], options.timeoutMs);
    } catch (error) {
      if (error instanceof EngineRpcTimeout) void this.recover().catch(failure => this.emit("error", failure));
      throw error;
    }
  }
  async retryReducer<T extends JsonValue = JsonValue>(error: EngineRpcTimeout | EngineRpcUnavailable): Promise<T> {
    if (error.request.method !== "reducer") throw new Error("Only reducer RPCs can retry a durable command");
    if (this.rpc.isClosed()) await this.recover();
    const [ref, args, options] = error.request.args as [FunctionReference, JsonValue, CallOptions];
    return this.reducer<T>(ref, args, options);
  }
  query<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, options?: CallOptions): Promise<T> { return this.admitted("query", [ref, args, options], options?.timeoutMs); }
  action<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, options?: CallOptions): Promise<T> { return this.admitted("action", [ref, args, options], options?.timeoutMs); }
  prepareReducer(ref: FunctionReference): Promise<void> { return this.admitted("prepareReducer", [ref]); }
  async readEntity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): Promise<T | undefined> { return (await this.readEntities<T>(entity, [id]))[0]; }
  readEntities<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Promise<Array<T | undefined>> { return this.admitted("readEntities", [entity, ids]); }
  connect() { this.connected = true; void this.admitted("connect", []).catch(error => this.emit("error", error)); }
  setAuth(auth: GonvexClientAuth) {
    const changed = reducerOutboxScope(this.url, this.auth, this.scope) !== reducerOutboxScope(this.url, { ...this.auth, ...auth }, this.scope);
    if (changed) { this.commandScope = reducerOutboxScope(this.url, { ...this.auth, ...auth }, this.scope); this.invalidate(true); this.session = "unavailable"; this.emit("session"); }
    this.auth = { ...this.auth, ...auth }; if ("fetchToken" in auth) this.fetchToken = auth.fetchToken;
    const { fetchToken, ...serializable } = auth;
    void this.admitted("setAuth", [serializable, Boolean(this.fetchToken), this.url]).then(() => changed ? this.restartWatches() : undefined).catch(error => this.emit("error", error));
  }
  async authenticate(auth: GonvexClientAuth): Promise<void> {
    const changed = reducerOutboxScope(this.url, this.auth, this.scope) !== reducerOutboxScope(this.url, { ...this.auth, ...auth }, this.scope);
    if (changed) { this.commandScope = reducerOutboxScope(this.url, { ...this.auth, ...auth }, this.scope); this.invalidate(true); this.session = "unavailable"; this.emit("session"); }
    this.auth = { ...this.auth, ...auth }; if ("fetchToken" in auth) this.fetchToken = auth.fetchToken;
    const { fetchToken, ...serializable } = auth;
    const result = await this.admitted<void>("authenticate", [serializable, Boolean(this.fetchToken), this.url]);
    if (changed) await this.restartWatches(); return result;
  }
  private async restartWatches(bypass = false) {
    const send = (method: string, args: unknown[]) => bypass ? this.call(method, args) : this.admitted(method, args);
    if (this.listeners.get("intents")?.size) await send("observeIntents", [true]);
    await Promise.all([...this.watches.values()].filter(watch => watch.listeners.size).map(async watch => {
      try { await send("watch", [watch.id, watch.kind, watch.args]); }
      catch (error) { watch.error = error instanceof Error ? error : new Error(String(error)); for (const listener of watch.listeners) listener(); }
    }));
  }
  private watch(kind: string, args: unknown[]) {
    const state: WatchState = { id: crypto.randomUUID(), kind, args, sequence: 0, listeners: new Set(), rowKeys: new Set() };
    const read = () => { if (state.error) throw state.error; return state.value; };
    return { read, onUpdate: (listener: () => void) => {
      if (!state.listeners.size) { state.sequence = 0; state.error = undefined; this.watches.set(state.id, state); const id = state.id; const generation = this.rpc.generation;
        void this.admitted("watch", [id, kind, args]).catch(error => { if (generation !== this.rpc.generation || id !== state.id || !this.watches.has(id)) return; state.error = error; listener(); }); }
      state.listeners.add(listener);
      return () => { state.listeners.delete(listener); if (!state.listeners.size) { this.watches.delete(state.id); this.releaseRows(state); state.value = undefined; void this.admitted("unwatch", [state.id]).catch(() => undefined); } };
    } };
  }
  watchRetained<T extends ReplicaRow = ReplicaRow>(signature: string | FunctionReference, args: JsonValue = {}, live = false) {
    const w = this.watch(live ? "liveState" : "retained", [signature, args]);
    return { onUpdate: w.onUpdate, getSnapshot: (): LiveQueryResult<T> | undefined => w.read() };
  }
  retainCollection(ref: FunctionReference, args: JsonValue = {}): (() => void) & { ready: Promise<void> } {
    const watch = this.watch("collectionRetention", [ref, args]);
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const ready = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
    const release = watch.onUpdate(() => {
      try { if (watch.read()?.isUpToDate) resolve(); }
      catch (error) { reject(error); }
    });
    return Object.assign(release, { ready });
  }
  watchEntities<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]) {
    const w = this.watch("entities", [entity, ids]); const pending = Object.freeze({ status: "pending" as const });
    return { onUpdate: w.onUpdate, getSnapshot: (): EngineEntitiesState<T> => w.read() ?? pending };
  }
  watchReplica<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, _options: { deferStart?: boolean } = {}) {
    const w = this.watch("replica", [ref, args]);
    return { onUpdate: w.onUpdate, localReplicaResult: (): T[] | undefined => w.read()?.rows,
      localReplicaState: (): ReplicaCollectionSubscriptionState | undefined => w.read()?.state,
      status: () => w.read()?.status ?? { isLoading: true, isUpToDate: false } };
  }
  watchLiveQuery<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, _options: { deferStart?: boolean } = {}) {
    const w = this.watch("live", [ref, args]); return { onUpdate: w.onUpdate, localLiveQueryResult: (): T | undefined => w.read()?.result };
  }
  watchControlQuery<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args) {
    const w = this.watch("control", [ref, args]); return { onUpdate: w.onUpdate, getSnapshot: (): { result: T | undefined; version: number } | undefined => w.read() };
  }
  cacheStats() { return { rows: this.rowCache.size, estimatedBytes: [...this.rowCache.values()].reduce((total, entry) => total + JSON.stringify(entry.value).length * 2, 0), unmountedRows: 0 }; }
  onError(listener: (error: Error) => void) { return this.listen("error", listener); }
  connectionState() { return this.connection; }
  subscribeToConnectionState(listener: (state: ConnectionState) => void) { return this.listen("connection", listener); }
  onSessionScopeChange(listener: () => void) { return this.listen("scope", listener); }
  get localSessionReady(): Promise<boolean> { const epoch = this.epoch; return this.admitted<boolean>("localSessionReady", []).then(ready => epoch === this.epoch && ready); }
  restoreSession(auth: GonvexClientAuth): Promise<boolean> { this.setAuth(auth); this.connect(); return this.localSessionReady; }
  resetLocalReplica(options: ResetLocalReplicaOptions = {}): Promise<ResetLocalReplicaResult> {
    this.invalidate(true);
    return this.admitted<ResetLocalReplicaResult>("resetLocalReplica", [options]).finally(() =>
      this.restartWatches().catch(error => this.emit("error", error)));
  }
  retryIntent(id: string): Promise<boolean> { return this.admitted("retryIntent", [id]); }
  discardIntent(id: string): Promise<boolean> { return this.admitted("discardIntent", [id]); }
  onDeveloperCredential(listener: (token: string) => void) { return this.listen("developerCredential", listener); }
  localSessionState() { return this.session; }
  onLocalSessionChange(listener: () => void) { return this.listen("session", listener); }
  onAuthError(listener: (message: string, details?: { definitive: boolean; code?: string }) => void) { return this.listen("authError", listener); }
  listIntents(): Promise<OutboxIntent[]> { return this.admitted("listIntents", []); }
  subscribeIntents(listener: () => void) {
    const first = !this.listeners.get("intents")?.size;
    const release = this.listen("intents", listener);
    if (first) void this.admitted("observeIntents", [true]).catch(error => this.emit("error", error));
    return () => { release(); if (!this.listeners.get("intents")?.size) void this.admitted("observeIntents", [false]).catch(() => undefined); };
  }
  intentsSnapshot() { return this.intents; }
  onLocalExecutionFallback(listener: (event: any) => void) { return this.listen("fallback", listener); }
  onReducerRejection(listener: (event: any) => void) { return this.listen("rejection", listener); }
  /** Reopen after a crash, then retry reducers with their original EngineCommand. */
  recover(): Promise<void> {
    if (this.closed) return Promise.reject(new Error("Engine client is closed"));
    if (this.switching) return this.switching;
    if (this.engineMode === "legacy") return Promise.reject(new Error("Legacy engine fallback must be recreated after a crash"));
    this.switching = Promise.resolve().then(async () => {
      this.connectionOffset = this.connection?.connectionCount ?? 0;
      this.rpc.close(); this.worker?.terminate(); this.invalidate();
      this.worker = this.config.engine.workerFactory?.() ?? new Worker(this.config.engine.workerUrl, { type: "module", name: "gonvex-sync-engine" });
      this.attach(this.worker);
      try {
        await this.initializeEndpoint(); await this.restartWatches(true);
        if (this.connected) await this.call("connect", []);
      } catch (error) { this.rpc.close(); this.worker.terminate(); throw error; }
    }).finally(() => { this.switching = undefined; });
    return this.switching;
  }
  async close() {
    if (this.closed) return;
    this.closed = true;
    try {
      await this.switching;
      await Promise.allSettled([...this.inflight]);
      if (!this.rpc.isClosed()) { await this.call("settle", []); await this.call("close", []); }
    } finally {
      this.rpc.close(); this.worker?.terminate(); this.endpoint?.close?.(); this.releaseFallback?.(); this.releasePresence?.(); this.channel?.close();
      this.listeners.clear(); this.watches.clear(); this.rowCache.clear();
    }
  }
}

export async function createEngineClient(url: string, options: EngineClientOptions): Promise<EngineClient> {
  if (!options.auth.identity?.sub || !options.auth.project || !options.auth.tenant) throw new Error("Phase 2 engine requires explicit project, tenant and account identity");
  if (options.engine.artifactHash !== options.fallbackBinding?.artifactHash) throw new Error("Worker and fallback reducer artifacts must match");
  const client = new EngineClient(url, options); await client.initialize(); return client;
}
