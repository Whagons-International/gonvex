/**
 * Thin protocol load client.
 *
 * Speaks the same WebSocket protocol as the real Gonvex SDK and sends the same
 * frames at the same points (auth, action.call, reducer.call, replica.openMany,
 * replica.snapshotAck, replica.close), so the server does the same work.
 * It skips the work that only matters inside a browser:
 *   - no browser LocalReplica/IndexedDB; optional harness checkpoint storage
 *     validates projected row hashes, membership, pages and authoritative scope;
 *   - rows are parsed once per process and shared between users who receive
 *     the same canonical row (RowPool), so thousands of users fit in one shard.
 *
 * Two row encodings, as negotiated by the SDK:
 *   - legacy rows (default, `nativeRows: false`): the SDK without a native
 *     staging adapter, which is what the Whagons app ships. The server sends
 *     replica.snapshotPage / replica.snapshot, replica.transaction and
 *     replica.delta with JSON rows; nothing is acknowledged.
 *   - native rows (`nativeRows: true`): replica.snapshotStart, credited
 *     replica.snapshotEncodedPage frames acknowledged with
 *     replica.snapshotAck, and replica.transactionEncoded.
 *
 * It reproduces the client behaviour that changes server load or measured
 * latency: reducer/action promises settle only after the covering replica or
 * control watermark, watermarks are deferred while a collection is opening,
 * opens are batched per event-loop turn, released collections close after the
 * SDK's 250 ms retention, and lost connections reconnect with the SDK's
 * backoff and re-open collections from complete validated checkpoints when
 * checkpoint storage is configured (otherwise from full snapshots).
 *
 * Presence v2 (runtime capability `presence`, option `presenceV2`): like a v2
 * SDK, the client advertises `presence: 1`, reports its own state with `presence.set` only on
 * transitions (re-sent after every auth.result), subscribes with
 * `presence.subscribe` and applies `presence.snapshot`/`presence.patch`.
 * Liveness is the socket: the runtime Pings it and Node's WebSocket answers.
 * Callers check `presenceSupported()` and then send no presence/support
 * heartbeat Reducers (see docs/performance/presence-v2.md).
 *
 * Unsupported server frames are counted in `ThinClient.stats.unsupported` and
 * reported by the harness, so a protocol drift cannot pass silently.
 */
import { randomUUID, createHash } from 'node:crypto';
import { canonical, windowKey, integrity, validCursor, sha, WindowIntegrity, trustImmutableEntry } from './thin-checkpoint.mjs';

const ZERO_DIGEST = 'xor1:' + '0'.repeat(64);
const MAX_BATCH_OPENS = 256;
const RETENTION_MS = 250;
const now = () => performance.timeOrigin + performance.now();

const CAPABILITIES = { replicaNoOldValues: 1, replicaSnapshotPages: 1, replicaNativeRows: 1, replicaSetDigest: 1,
  tenantControl: 1, telemetryBatch: 1, replicaReadyMany: 1, replicaWatermark: 1, replicaBatchFrames: 1, queryPagePatch: 1,
  queryObjectPatch: 1, queryOrderDelta: 1, queryFanout: 1, queryResultBatch: 1, preserveSubscriptions: 1 };
const CAPABILITIES_LEGACY_ROWS = { ...CAPABILITIES, replicaNativeRows: undefined };
const withoutBatchFrames = (capabilities) => ({ ...capabilities, replicaBatchFrames: undefined });

function deviceInfo() {
  const n = globalThis.navigator;
  if (!n) return undefined;
  const ua = n.userAgent || '';
  return { userAgent: ua, browserName: '', browserVersion: '', deviceType: /Mobi|Android|iPhone|iPad/i.test(ua) ? 'mobile' : 'desktop',
    platform: n.platform || '', language: n.language || '', timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || '',
    hardwareConcurrency: n.hardwareConcurrency };
}

const jsonKeys=new WeakMap();
const sameJson=(a,b)=>{
 if(a===b)return true;
 if(a===null||b===null||typeof a!=='object'||typeof b!=='object')return false;
 if(Array.isArray(a)){if(!Array.isArray(b)||a.length!==b.length)return false;for(let i=0;i<a.length;i++)if(!sameJson(a[i],b[i]))return false;return true;}
 if(Array.isArray(b))return false;
 const keys=jsonKeys.get(a)||Object.keys(a),other=Object.keys(b);if(keys.length!==other.length)return false;
 for(let i=0;i<keys.length;i++){const key=keys[i];if(other[i]!==key){for(const k of keys)if(!Object.hasOwn(b,k)||!sameJson(a[k],b[k]))return false;return true;}if(!sameJson(a[key],b[key]))return false;}return true;
};
const freezeRow=value=>{if(value&&typeof value==='object'&&!jsonKeys.has(value)){const keys=Object.keys(value);jsonKeys.set(value,keys);for(const k of keys)freezeRow(value[k]);Object.freeze(value);}return value;};

/** Process-wide intern table for immutable row values, reference counted. */
export class RowPool {
  constructor() { this.rows = new Map();this.identities=new Map(); this.stats = { parsed: 0, shared: 0,identityComparisons:0,identityHits:0,serializations:0 }; }
  /** `value`, when given, is the already parsed row (legacy JSON frames). */
  acquire(canonical, value, serialized) {
    let entry = this.rows.get(canonical);
    if (entry) { entry.refs++; this.stats.shared++; return entry; }
    const row=freezeRow(value ?? JSON.parse(canonical));
    entry = { canonical, canonicalBytes:Buffer.byteLength(canonical),value: row, serializedValue:serialized??JSON.stringify(row), refs: 1,pool:this };
    const id=row?._id??row?.id;if(id!==undefined){entry.identity=typeof id+':'+id;const bucket=this.identities.get(entry.identity)||new Set();bucket.add(entry);this.identities.set(entry.identity,bucket);}
    this.stats.parsed++;
    trustImmutableEntry(entry);this.rows.set(canonical, entry);
    return entry;
  }
  acquireValue(value) { const id=value?._id??value?.id;const bucket=id===undefined?undefined:this.identities.get(typeof id+':'+id);if(bucket)for(const e of bucket){this.stats.identityComparisons++;if(sameJson(e.value,value)){e.refs++;this.stats.shared++;this.stats.identityHits++;return e;}}this.stats.serializations++;const text=JSON.stringify(value);return this.acquire(text,value,text); }
  retain(entry){if(entry.pool===this){entry.refs++;return entry;}return this.acquire(entry.canonical,entry.value);}
  release(entry) { if(entry?.pool&&entry.pool!==this){entry.pool.release(entry);return;}if (entry && --entry.refs === 0){this.rows.delete(entry.canonical);const bucket=this.identities.get(entry.identity);if(bucket){bucket.delete(entry);if(!bucket.size)this.identities.delete(entry.identity);}} }
  get size() { return this.rows.size; }
}

const changeOf = (c) => ({ entity: c.entity, id: String(c.id), operation: c.operation, newValue: c.newValue, newCanonical: c.newValue === undefined ? undefined : JSON.stringify(c.newValue) });

/** Shared cache of parsed canonical transaction changes (identical frames fan out to many users). */
class ChangeCache {
  constructor(limit = 4096) { this.map = new Map(); this.limit = limit; }
  get(canonical) {
    let change = this.map.get(canonical);
    if (change) return change;
    const c = JSON.parse(canonical);
    change = changeOf(c);
    this.map.set(canonical, change);
    if (this.map.size > this.limit) this.map.delete(this.map.keys().next().value);
    return change;
  }
}

// The SDK's replica-integrity stableStringify (sorted keys by UTF-8 bytes, U+2028/9 escaped).
const utf8Compare = (a, b) => (!/[^\x00-\x7f]/.test(a) && !/[^\x00-\x7f]/.test(b)) ? (a < b ? -1 : a > b ? 1 : 0) : Buffer.compare(Buffer.from(a), Buffer.from(b));
function stableStringify(value) {
  if (typeof value === 'string') return JSON.stringify(value).replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  return `{${Object.keys(value).sort(utf8Compare).map((k) => `${stableStringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}

export const defaultPool = new RowPool();
const changeCache = new ChangeCache();
const keyOf = (path, args) => path + '\u0000' + canonical(JSON.parse(JSON.stringify(args ?? {})));

class Subscription {
  constructor(client, ref, args) {
    this.client = client; this.ref = freezeRow({path:ref.path,scope:ref.scope,delivery:ref.delivery,replica:ref.replica?JSON.parse(JSON.stringify(ref.replica)):undefined}); this.path = ref.path; this.args = freezeRow(JSON.parse(JSON.stringify(args ?? {})));
    this.entity = ref.replica?.table; this.keyField = ref.replica?.key || '_id';
    this.key = keyOf(this.path, this.args);
    this.id = undefined; this.opening = false; this.isUpToDate = false; this.complete = false;
    this.checkpointKey=windowKey(this.ref,this.args);this.rowChanges=new Map();this.checkpointReset=true;this.checkpointVersion=undefined;this.checksum=new WindowIntegrity(this.ref.replica?.columns);
    this.rows = new Map(); // row key -> pool entry (committed window)
    this.staging = undefined; // { snapshotId, rows: Map, truncated }
    this.listeners = new Set(); this.unsubscribeTimer = undefined;
    this.version = 0; this.cachedVersion = -1; this.cachedRows = undefined; this.notifyQueued = false;
    this.readyCursor = undefined; this.digest = undefined; this.error = undefined;
    this.retryAttempt = 0; this.retryTimer = undefined; this.failedAt = undefined;
  }
  setRow(id, entry) { const old = this.rows.get(id); if (old === entry) { this.client.pool.release(entry); return; } this.rows.set(id, entry); this.client.pool.release(old);if(this.client.checkpointStore){this.checksum.set(id,entry);this.rowChanges.set(id,entry);} this.version++; }
  deleteRow(id) { const old = this.rows.get(id); if (!old) return; this.rows.delete(id); this.client.pool.release(old);if(this.client.checkpointStore){this.checksum.delete(id);this.rowChanges.set(id,null);} this.version++; }
  clearRows() { for (const entry of this.rows.values()) this.client.pool.release(entry); this.rows.clear();this.cachedRows=undefined;this.rowChanges.clear();this.checksum=new WindowIntegrity(this.ref.replica?.columns);this.checkpointReset=true; this.version++; }
  dropStaging() { if (!this.staging) return; for (const entry of this.staging.rows.values()) this.client.pool.release(entry); this.staging = undefined; }
  result() {
    if (this.cachedVersion !== this.version) { this.cachedRows = Array.from(this.rows.values(), e => e.value); this.cachedVersion = this.version; }
    return this.cachedRows;
  }
  notify() {
    if (this.notifyQueued) return; this.notifyQueued = true;
    queueMicrotask(() => { this.notifyQueued = false; for (const fn of [...this.listeners]) fn(); });
  }
}

export class ThinClient {
  static stats = { unsupported: {}, framesIn: 0, framesOut: 0, bytesIn: 0, snapshotPages: 0, reconnects: 0, reopens: 0, resets: 0, batches: 0, batchItems: 0,
    checkpointFailures: [], replicaEvents: [], replicaErrors: 0, replicaRetries: 0, replicaRecoveries: 0, replicaRecoveryMs: 0,
    presence: { snapshots: 0, patches: 0, changes: 0 } };

  // nativeRows defaults to false: the Whagons app's SDK (0.5.2-staging.31 +
  // patch) negotiates native rows only with a staging adapter, which the app
  // does not configure, so production clients use legacy JSON rows.
  // presenceV2: advertise `presence: 1` (SDKs with presence v2 only; the
  // Whagons lab SDK 0.5.2-staging.31 does not, so it defaults to false).
  // batchFrames: advertise replicaBatchFrames (LOAD_THIN_BATCH_FRAMES=0 disables).
  constructor(url, { project, clientContract = 1, pool = defaultPool, nativeRows = false, ackDelayMs = 0, presenceV2 = false, batchFrames = process.env.LOAD_THIN_BATCH_FRAMES !== '0', checkpointStore, checkpointIdentity, modulePin } = {}) {
    this.checkpointStore=checkpointStore;this.checkpointIdentity=checkpointIdentity;this.modulePin=modulePin;
    if(checkpointStore&&(!checkpointIdentity||!modulePin))throw Error('checkpoint identity and exact module pin required');
    for(const name of ['restoreCheckpoint','checkpointOpen','validateReady','validatePage','flushCheckpoints']){const run=this[name].bind(this);this[name]=(...args)=>{const at=process.cpuUsage();try{return run(...args);}finally{const cpu=process.cpuUsage(at);this.checkpointCpuUs+=cpu.user+cpu.system;}};}
    this.cacheBinding=undefined;this.replicaDirective=undefined;this.batchDepth=0;this.checkpointDirty=new Set();this.restoredWindows=new Map();this.checkpointCpuUs=0;
    this.presenceV2 = presenceV2;
    this.url = url; this.project = project; this.pool = pool;
    // The SDK sends the generated contract's version number.
    this.clientContract = typeof clientContract === 'object' && clientContract !== null ? clientContract.version : clientContract;
    this.nativeRows = nativeRows; this.ackDelayMs = ackDelayMs; this.batchFrames = batchFrames;
    // Delivery v2: stream token -> subscription ids, scoped to one socket.
    this.streams = new Map();
    this.auth = { token: undefined, tenant: undefined };
    this.socket = undefined; this.manuallyClosed = false; this.reconnectAttempt = 0; this.reconnectTimer = undefined;
    this.serverCapabilities = {}; this.authReady = false; this.authInFlight = undefined; this.outbox = [];
    this.pending = new Map(); this.subscriptions = new Map(); this.byId = new Map(); this.pendingOpens = new Set(); this.openFlushTimer = undefined;
    this.processedWatermark = 0; this.deferredWatermark = undefined;
    this.telemetry = new Set();
    // Presence v2, same state machine as the SDK's PresenceFeed.
    this.presenceReported = undefined; this.presenceListeners = new Set(); this.presenceMembers = new Map();
    this.presenceSubscriptionId = undefined; this.presenceCounter = 0; this.presenceReady = false;
    this.presenceView = { supported: false, ready: false, members: this.presenceMembers };
  }

  // ---- transport -------------------------------------------------------------
  connect() {
    if (this.manuallyClosed) return;
    const socket = new globalThis.WebSocket(this.url);
    socket.binaryType = 'arraybuffer';
    this.socket = socket; this.authReady = false; this.serverCapabilities = {}; this.streams = new Map();
    socket.addEventListener('open', () => {
      if (this.socket !== socket) return;
      this.reconnectAttempt = 0;
      this.sendAuth();
    });
    socket.addEventListener('message', (event) => { if (this.socket === socket) this.receive(event.data); });
    socket.addEventListener('close', () => {
      if (this.socket !== socket) return;
      this.socket = undefined; this.authReady = false;
      this.presenceDisconnected();
      this.dropConnectionState();
      if (!this.manuallyClosed) this.scheduleReconnect();
    });
    socket.addEventListener('error', () => {});
  }

  close() {
    this.manuallyClosed = true;
    clearTimeout(this.reconnectTimer); clearTimeout(this.openFlushTimer);
    for (const sub of this.subscriptions.values()) { clearTimeout(sub.unsubscribeTimer); this.clearReplicaRetry(sub, true);if(this.checkpointStore&&!sub.checkpointConsistent)this.checkpointStore.invalidate(sub.checkpointKey); sub.clearRows(); sub.dropStaging(); }
    this.subscriptions.clear();this.entityIndex=undefined; this.byId.clear(); this.pendingOpens.clear();
    for (const call of this.pending.values()) call.fail(new Error(`Client closed while waiting for ${call.kind} ${call.path}.`));
    this.pending.clear();
    try { this.socket?.close(); } catch {}
    this.socket = undefined;
  }

  scheduleReconnect() {
    if (this.manuallyClosed || this.reconnectTimer) return;
    const delay = Math.min(250 * 2 ** this.reconnectAttempt, 5000);
    this.reconnectAttempt++;
    ThinClient.stats.reconnects++;
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = undefined; this.connect(); }, delay);
  }

  dropConnectionState() {
    clearTimeout(this.openFlushTimer); this.openFlushTimer = undefined; this.pendingOpens.clear();
    // Same contract as the SDK: tenant calls fail with "may or may not have been applied".
    for (const [id, call] of this.pending) {
      if (call.scope === 'control') continue;
      this.pending.delete(id);
      call.fail(new Error(`Connection lost while waiting for ${call.kind} ${call.path}. The operation may or may not have been applied.`));
    }
    this.processedWatermark = 0; this.deferredWatermark = undefined;
    for (const sub of this.subscriptions.values()) {
      this.clearReplicaRetry(sub, true);
      if (sub.id) this.byId.delete(sub.id);
      sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging(); sub.notify();
    }
  }

  rawSend(message) {
    const text = JSON.stringify(message);
    ThinClient.stats.framesOut++;
    this.socket.send(text);
  }
  send(message) {
    if (this.socket && this.socket.readyState === 1 && this.authReady) this.rawSend(message);
    else this.outbox.push(message);
  }
  flushOutbox() { const queued = this.outbox.splice(0); for (const m of queued) this.send(m); }

  sendAuth() {
    const id = randomUUID();
    this.authInFlight = id;
    const tenantAuth = !!this.auth.token;
    this.rawSend({ type: 'auth', clientContract: this.clientContract, id, ...(tenantAuth ? { token: this.auth.token } : {}), project: this.project,
      ...(tenantAuth ? { tenant: this.auth.tenant } : {}), controlOnly: !tenantAuth, device: deviceInfo(),
      capabilities: (({ nativeRows, batchFrames, presenceV2 }) => {
        const capabilities = nativeRows ? CAPABILITIES : CAPABILITIES_LEGACY_ROWS;
        return { ...(batchFrames ? capabilities : withoutBatchFrames(capabilities)), ...(presenceV2 ? { presence: 1 } : {}) };
      })(this) });
    return id;
  }

  // ---- public SDK surface used by run.ts ------------------------------------
  onTelemetry(handler) { this.telemetry.add(handler); return () => this.telemetry.delete(handler); }

  authenticate({ project, token, tenant }) {
    if(this.checkpointStore&&(project&&project!==this.project||this.auth.tenant!==undefined&&tenant!==this.auth.tenant)){this.quarantineCache();}
    if (project) this.project = project;
    this.auth = { token, tenant };
    return new Promise((resolve, reject) => {
      this.authWaiter = { resolve, reject };
      if (this.socket && this.socket.readyState === 1) { this.authReady = false; this.sendAuth(); }
    });
  }

  // ---- presence v2 (SDK: presenceSupported/setPresence/subscribePresence/presenceSnapshot)
  presenceSupported() { return this.serverCapabilities.presence === 1; }
  presenceCanSend() { return this.authReady && this.socket?.readyState === 1; }
  setPresence(state) {
    if (this.presenceReported === state) return;
    this.presenceReported = state;
    if (this.presenceSupported() && this.presenceCanSend()) this.rawSend({ type: 'presence.set', state });
  }
  subscribePresence(listener) {
    this.presenceListeners.add(listener);
    if (this.presenceListeners.size === 1) this.presenceSendSubscribe();
    return () => {
      if (!this.presenceListeners.delete(listener) || this.presenceListeners.size > 0) return;
      if (this.presenceSubscriptionId && this.presenceSupported() && this.presenceCanSend()) this.rawSend({ type: 'presence.unsubscribe' });
      this.presenceSubscriptionId = undefined; this.presenceReady = false; this.presenceMembers = new Map();
      this.presencePublish(false);
    };
  }
  presenceSnapshot() {
    if (this.presenceView.supported !== this.presenceSupported()) this.presencePublish(false);
    return this.presenceView;
  }
  presenceSendSubscribe() {
    if (!this.presenceSupported() || !this.presenceCanSend()) return;
    this.presenceSubscriptionId = `presence-${++this.presenceCounter}`;
    this.rawSend({ type: 'presence.subscribe', id: this.presenceSubscriptionId });
  }
  presenceAuthenticated() {
    this.presenceReady = false; this.presenceSubscriptionId = undefined;
    if (!this.presenceSupported()) { this.presencePublish(); return; }
    if (this.presenceReported) this.rawSend({ type: 'presence.set', state: this.presenceReported });
    if (this.presenceListeners.size > 0) this.presenceSendSubscribe();
    this.presencePublish();
  }
  presenceDisconnected() {
    this.presenceSubscriptionId = undefined;
    if (!this.presenceReady) return;
    this.presenceReady = false;
    this.presencePublish();
  }
  presencePublish(notify = true) {
    this.presenceView = { supported: this.presenceSupported(), ready: this.presenceReady, members: this.presenceMembers };
    if (notify) for (const listener of [...this.presenceListeners]) listener();
  }
  onPresence(m) {
    const stats = ThinClient.stats.presence;
    if (m.type === 'presence.snapshot') {
      if (m.id !== this.presenceSubscriptionId) return;
      stats.snapshots++;
      const members = new Map();
      for (const member of m.members) if (member.state !== 'offline') members.set(member.memberId, member);
      this.presenceMembers = members; this.presenceReady = true; this.presencePublish();
    } else if (m.type === 'presence.patch') {
      if (!this.presenceSubscriptionId || !this.presenceReady || m.changes.length === 0) return;
      stats.patches++; stats.changes += m.changes.length;
      const members = new Map(this.presenceMembers);
      for (const change of m.changes) { if (change.state === 'offline') members.delete(change.memberId); else members.set(change.memberId, change); }
      this.presenceMembers = members; this.presencePublish();
    } else if (m.id === this.presenceSubscriptionId) {
      this.presenceSubscriptionId = undefined; this.presenceReady = false; this.presencePublish();
      this.unsupported('presence.error: ' + String(m.error).slice(0, 160));
    }
  }

  action(ref, args) { return this.call('action', ref, args); }
  reducer(ref, args) { return this.call('reducer', ref, args); }

  call(kind, ref, args) {
    const id = randomUUID(), path = ref.path, scope = path.startsWith('control.') ? 'control' : 'tenant';
    const clientSentAtMs = now();
    return new Promise((resolve, reject) => {
      const call = { kind, path, scope, clientSentAtMs, accepted: false,
        fail: (error) => { this.emitTelemetry(kind, id, path, 'error', clientSentAtMs, undefined, error.message); reject(error); },
        resolve, reject };
      this.pending.set(id, call);
      const trace = { clientSentAtMs };
      if (kind === 'action') this.send({ type: 'action.call', id, path, args, ...(scope === 'control' ? { scope } : {}), idempotencyKey: id, trace });
      else this.send({ type: 'reducer.call', id, path, args, ...(scope === 'control' ? { scope } : {}), trace,
        ...(scope === 'control' ? { idempotencyKey: id } : {}), clientContract: this.clientContract });
    });
  }

  watchReplica(ref, args = {}) {
    const key = keyOf(ref.path, args);
    let sub = this.subscriptions.get(key);
    if(this.checkpointStore&&sub&&sub.checkpointKey!==windowKey(ref,args))throw Error('collection-contract-changed');
    if (!sub) { sub = new Subscription(this, ref, args); this.subscriptions.set(key, sub);this.entityIndex=undefined;if(this.checkpointStore)this.restoreCheckpoint(sub); }
    const handlers = new Set();
    let started = false, releaseTimer;
    const listener = () => { for (const h of handlers) h(); };
    const start = () => {
      if (started) return; started = true;
      const live = this.subscriptions.get(key) ?? (this.subscriptions.set(key, sub), sub);
      sub = live;
      clearTimeout(sub.unsubscribeTimer); sub.unsubscribeTimer = undefined;
      sub.listeners.add(listener);
      if (sub.error && sub.id) this.scheduleReplicaRetry(sub);
      else this.openSubscription(sub);
    };
    const stop = () => {
      if (!started) return; started = false;
      sub.listeners.delete(listener);
      if (sub.listeners.size === 0 && !sub.unsubscribeTimer) sub.unsubscribeTimer = setTimeout(() => this.releaseSubscription(sub), RETENTION_MS);
    };
    start();
    return {
      localReplicaResult: () => { if (sub.error) throw sub.error; return sub.cachedVersion === -1 && sub.rows.size === 0 && !sub.isUpToDate && sub.version === 0 ? undefined : sub.result(); },
      /** Thin-client extra: O(1) row lookup by key (not part of the SDK surface). */
      localReplicaGet: (id) => sub.rows.get(String(id))?.value,
      localReplicaState: () => { if (sub.error) throw sub.error; if (sub.version === 0 && !sub.isUpToDate) return undefined; return { completeness: sub.isUpToDate && sub.complete ? 'complete' : 'partial', isUpToDate: sub.isUpToDate }; },
      status: () => ({ isLoading: sub.version === 0 && !sub.isUpToDate, isUpToDate: sub.isUpToDate }),
      onUpdate: (handler) => {
        start(); clearTimeout(releaseTimer); releaseTimer = undefined;
        handlers.add(handler);
        queueMicrotask(() => { if (handlers.has(handler)) handler(); });
        return () => {
          handlers.delete(handler);
          if (handlers.size > 0 || releaseTimer) return;
          releaseTimer = setTimeout(() => { releaseTimer = undefined; if (handlers.size === 0) stop(); }, 0);
        };
      },
    };
  }

  // ---- replica subscriptions --------------------------------------------------
  openSubscription(sub, retry = false) {
    if (this.manuallyClosed || sub.retryTimer || sub.opening || (sub.id && !retry) || this.subscriptions.get(sub.key) !== sub || sub.listeners.size === 0) return;
    if (!this.authReady || !this.auth.token) return; // reopened from auth.result
    sub.id ??= randomUUID(); sub.opening = true; sub.isUpToDate = false;
    this.byId.set(sub.id, sub);
    this.pendingOpens.add(sub);
    if (!this.openFlushTimer) this.openFlushTimer = setTimeout(() => this.flushOpens(), 0);
  }
  flushOpens() {
    this.openFlushTimer = undefined;
    const opens = [...this.pendingOpens].filter(s => s.opening && s.listeners.size > 0 && this.subscriptions.get(s.key) === s)
      .map(s => this.checkpointStore?this.checkpointOpen(s):({ id: s.id, path: s.path, args: s.args, digest: ZERO_DIGEST }));
    this.pendingOpens.clear();
    for (let i = 0; i < opens.length; i += MAX_BATCH_OPENS) this.send({ type: 'replica.openMany', opens: opens.slice(i, i + MAX_BATCH_OPENS) });
  }
  releaseSubscription(sub) {
    sub.unsubscribeTimer = undefined;
    if (this.subscriptions.get(sub.key) !== sub || sub.listeners.size > 0) return;
    this.clearReplicaRetry(sub, true);
    this.subscriptions.delete(sub.key);this.entityIndex=undefined; this.pendingOpens.delete(sub);
    if (sub.id) { this.byId.delete(sub.id); if (this.socket?.readyState === 1 && this.authReady) this.rawSend({ type: 'replica.close', id: sub.id }); }
    if(this.checkpointStore&&!sub.checkpointConsistent)this.checkpointStore.invalidate(sub.checkpointKey);sub.clearRows(); sub.dropStaging();
    this.drainDeferredWatermark();
  }
  reopenAll() {
    for (const sub of this.subscriptions.values()) {
      this.clearReplicaRetry(sub, true);
      if (sub.id) this.byId.delete(sub.id);
      sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging();
      if (sub.listeners.size > 0) { ThinClient.stats.reopens++; this.openSubscription(sub); }
    }
  }

  clearReplicaRetry(sub, reset = false) {
    clearTimeout(sub.retryTimer); sub.retryTimer = undefined;
    if (reset) sub.retryAttempt = 0;
  }
  onReplicaError(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    if(this.checkpointStore){this.checkpointStore.invalidate(sub.checkpointKey);sub.cacheValidated=false;}
    ThinClient.stats.replicaErrors++;
    ThinClient.stats.replicaEvents.push({type:'refused',at:now(),path:sub.path});
    sub.failedAt ??= now();
    sub.error = new Error(m.error); sub.opening = false; sub.isUpToDate = false;
    sub.dropStaging(); sub.notify();
    this.scheduleReplicaRetry(sub);
  }
  scheduleReplicaRetry(sub) {
    if (this.manuallyClosed || sub.retryTimer || !sub.listeners.size || this.subscriptions.get(sub.key) !== sub) return;
    // SDK exponential backoff; bounded downward jitter disperses refused opens
    // while retaining the requested hard 5 s maximum.
    const delay = Math.min(250 * 2 ** Math.min(sub.retryAttempt++, 5), 5000) * (0.8 + Math.random() * 0.2);
    sub.retryTimer = setTimeout(() => {
      sub.retryTimer = undefined;
      if (this.manuallyClosed || !this.authReady || this.socket?.readyState !== 1 || !sub.listeners.size || this.subscriptions.get(sub.key) !== sub) return;
      sub.opening = false;
      ThinClient.stats.replicaRetries++;
      ThinClient.stats.replicaEvents.push({type:'retry',at:now(),path:sub.path});
      this.openSubscription(sub, true);
    }, delay);
  }

  cacheFailure(error) {
    ThinClient.stats.checkpointFailures.push({at:now(),error:error.message});
    this.unsupported('checkpoint:'+error.message);
    this.checkpointStore?.clear();this.checkpointDirty.clear();this.batchDepth=0;
    for(const sub of this.subscriptions.values()){
      this.clearReplicaRetry(sub,true);sub.dropStaging();sub.clearRows();sub.readyCursor=undefined;sub.cacheValidated=false;sub.restored=false;
      sub.isUpToDate=false;sub.error=error;sub.opening=false;sub.notify();
    }
    this.authWaiter?.reject(error);this.authWaiter=undefined;
    // Recover via a fresh authenticated transport, never a poisoned cursor.
    this.socket?.close();
  }
  quarantineCache(){
    this.checkpointStore?.clear();this.cacheBinding=undefined;this.replicaDirective=undefined;this.checkpointDirty.clear();this.restoredWindows.clear();
    for(const sub of this.subscriptions.values())this.invalidateCheckpoint(sub,true);
  }
  activateCache(d){
    if(!d||d.protocolVersion!==1||![d.scope,d.visibilityScope,d.epoch].every(s=>typeof s==='string'&&s.trim().length))throw Error('missing-authoritative-cache-scope');
    const binding={origin:this.url,project:this.project,tenant:this.auth.tenant,identity:this.checkpointIdentity,contract:this.clientContract,module:this.modulePin,scope:d.visibilityScope,definitionEpoch:d.epoch};
    const next=canonical(binding);
    if(this.cacheBinding&&this.cacheBinding!==next)this.quarantineCache();
    this.replicaDirective={...d};this.cacheBinding=this.checkpointStore.bind(binding);
    for(const sub of this.subscriptions.values())if(!sub.rows.size&&!sub.readyCursor)this.restoreCheckpoint(sub);
  }
  restoreCheckpoint(sub){
    if(!this.cacheBinding)return;
    const saved=this.checkpointStore.load(sub.checkpointKey,this.cacheBinding,this.pool);
    if(!saved)return;
    sub.clearRows();sub.rows=saved.rows;sub.checksum=saved.state;sub.rowChanges.clear();sub.checkpointReset=false;sub.checkpointVersion=saved.version;sub.version++;sub.readyCursor=saved.cursor;sub.digest=saved.digest;sub.complete=true;sub.cacheValidated=true;sub.checkpointConsistent=true;sub.restored=true;
    this.restoredWindows.set(sub.checkpointKey,{rows:saved.rows.size,cursor:{...saved.cursor},digest:saved.digest,membershipHash:sha(canonical([...saved.rows.keys()].sort()))});
    // Cached rows are never server-current until a fresh ready validates them.
    sub.isUpToDate=false;
  }
  replicaSignature(ref,args={}){return windowKey(ref,args);}
  cacheEvidence(signatures){return {cachedWindows:this.restoredWindows.size,cachedFullSetVerified:signatures.length>0&&[...this.restoredWindows.values()].some(w=>w.rows>0)&&signatures.every(k=>this.restoredWindows.has(k)),cachedRows:[...this.restoredWindows.values()].reduce((n,w)=>n+w.rows,0),authorityHash:sha(this.cacheBinding||''),expectedWindows:signatures.length,checkpointInventory:signatures.map(k=>({signatureHash:sha(k),...this.restoredWindows.get(k)}))};}
  checkpointOpen(sub,force=false){
    const base={id:sub.id,path:sub.path,args:sub.args};
    if(!this.cacheBinding||!sub.complete||!sub.checkpointConsistent||!validCursor(sub.readyCursor))return {...base,digest:ZERO_DIGEST};
    const actual=sub.checksum.result('xor1',force||this.serverCapabilities.replicaResume!==1);
    if(!force&&this.serverCapabilities.replicaResume===1)return {...base,cursor:{...sub.readyCursor},digest:actual.digest};
    return {...base,cursor:{...sub.readyCursor},keys:[...sub.rows.keys()],hashes:actual.hashes,digest:actual.digest,fullIntegrity:true};
  }
  invalidateCheckpoint(sub,clear=false){
    if(clear)this.checkpointStore?.invalidate(sub.checkpointKey);else this.checkpointStore?.suspend(sub.checkpointKey);this.checkpointDirty.add(sub);sub.checkpointConsistent=false;
    if(clear){this.restoredWindows.delete(sub.checkpointKey);sub.dropStaging();sub.clearRows();sub.readyCursor=undefined;sub.digest=undefined;sub.complete=false;sub.cacheValidated=false;sub.restored=false;sub.isUpToDate=false;sub.notify();}
  }
  acceptCursor(sub,cursor){
    if(!validCursor(cursor))throw Error('invalid-checkpoint-cursor');
    if(sub.retiredEpochs?.has(cursor.epoch))return false;
    const current=sub.readyCursor;
    if(current&&current.epoch!==cursor.epoch){this.invalidateCheckpoint(sub,true);throw Error('unexpected-checkpoint-epoch');}
    return !current||cursor.revision>=current.revision;
  }
  validateSnapshotStart(sub,m){
    if(!validCursor(m.cursor)||m.key!==sub.keyField||m.path&&m.path!==sub.path)throw Error('invalid-checkpoint-snapshot');
    if(sub.readyCursor?.epoch===m.cursor.epoch&&m.cursor.revision<sub.readyCursor.revision)throw Error('stale-snapshot-cursor');
    if(m.snapshotId!==undefined&&(!m.snapshotId||m.visibilityScope!==this.replicaDirective?.visibilityScope||m.scope!==this.replicaDirective?.scope))throw Error('unscoped-snapshot-pages');
    if(m.visibilityScope!==undefined&&m.visibilityScope!==this.replicaDirective?.visibilityScope)throw Error('snapshot-visibility-scope');
    if(m.scope!==undefined&&m.scope!==this.replicaDirective?.scope)throw Error('snapshot-scope');
    if(sub.readyCursor&&sub.readyCursor.epoch!==m.cursor.epoch){sub.retiredEpochs??=new Set();sub.retiredEpochs.add(sub.readyCursor.epoch);}
    if(sub.retiredEpochs?.has(m.cursor.epoch))throw Error('retired-snapshot-epoch');
  }
  validatePage(sub,m,native){
    const st=sub.staging;
    if(st.last||m.sequence!==st.sequence||typeof m.last!=='boolean'||m.snapshotId!==st.snapshotId)throw Error('snapshot-page-order');
    {
      const a=st.meta;
      if(m.key!==a.key||canonical(m.cursor)!==canonical(a.cursor)||m.scope!==a.scope||m.visibilityScope!==a.visibilityScope||m.truncated!==a.truncated||m.path!==a.path||['maxRows','maxBytes','mode','orderBy','orderDirection'].some(k=>m[k]!==a[k]))throw Error('snapshot-page-metadata');
    }
    const rows=native?m.canonicalRows.map(x=>JSON.parse(x)):m.result;
    if(!Array.isArray(rows))throw Error('snapshot-page-rows');
    if(m.snapshotId!==undefined&&((m.last&&typeof m.digest!=='string')||(!m.last&&(rows.length===0||m.digest!==undefined))))throw Error('snapshot-page-final-digest');
    const seen=new Set();

    if((m.snapshotId!==undefined&&rows.length>512)||((st.meta.maxRows??sub.ref.replica?.maxRows)>0&&st.rows.size+rows.length>(st.meta.maxRows??sub.ref.replica?.maxRows)))throw Error('snapshot-page-budget');
    for(const row of rows){const id=row?.[st.key];if(!['string','number'].includes(typeof id)||st.rows.has(String(id))||seen.has(String(id)))throw Error('snapshot-page-key');seen.add(String(id));}
    st.sequence++;st.last=m.last;
    st.lastDigest=m.digest;
  }
  validateReady(sub,m){
    if(!validCursor(m.cursor)||!this.cacheBinding)throw Error('unscoped-ready');
    const st=sub.staging;
    if(!st&&!this.acceptCursor(sub,m.cursor))return false;
    if(st&&(!st.last||st.meta.cursor.epoch!==m.cursor.epoch||st.revision>m.cursor.revision))throw Error('incomplete-snapshot-ready');
    if(!st&&!sub.complete)throw Error('ready-without-complete-rows');
    const rows=st?st.rows:sub.rows;
    const actual=(st?st.checksum:sub.checksum).result(m.digest?.startsWith('xor1:')?'xor1':'sha256',false);
    if(!m.digest||actual.digest!==m.digest)throw Error('ready-integrity-mismatch');
    if(st?.lastDigest&&st.lastDigest!==actual.digest)throw Error('snapshot-page-digest');
    if(st?.truncated||m.truncated){this.invalidateCheckpoint(sub);sub.cacheValidated=false;}
    return true;
  }
  flushCheckpoints(){
    if(!this.checkpointStore||!this.cacheBinding||this.batchDepth)return;
    const windows=[];
    for(const sub of this.checkpointDirty){
      if(!sub.cacheValidated||!sub.checkpointConsistent||!sub.isUpToDate||!sub.complete||sub.opening||sub.staging||sub.error||!validCursor(sub.readyCursor))continue;
      const mode=sub.digest?.startsWith('xor1:')?'xor1':'sha256';const actual=sub.checksum.result(mode,false);
      windows.push({key:sub.checkpointKey,rows:sub.rows,pool:this.pool,keyField:sub.keyField,cursor:sub.readyCursor,baseVersion:sub.checkpointVersion,reset:sub.checkpointReset,changes:sub.rowChanges,digest:actual.digest,mode,columns:sub.ref.replica?.columns,complete:true});
    }
    if(windows.length){this.checkpointStore.publish(windows);const published=new Set(windows.map(w=>w.key));for(const sub of this.checkpointDirty)if(published.has(sub.checkpointKey)){sub.checkpointVersion=this.checkpointStore.versionFor(sub.checkpointKey);sub.rowChanges.clear();sub.checkpointReset=false;}}
    this.checkpointDirty.clear();
  }

  // ---- inbound ---------------------------------------------------------------
  receive(data) {
    ThinClient.stats.framesIn++;
    let message;
    try {
      if (typeof data === 'string') { ThinClient.stats.bytesIn += data.length; message = JSON.parse(data); }
      else { ThinClient.stats.bytesIn += data.byteLength; message = JSON.parse(Buffer.from(data).toString('utf8')); }
    } catch { this.unsupported('unparseable-frame'); return; }
    try{this.dispatch(message);}catch(e){if(!this.checkpointStore)throw e;this.cacheFailure(e);}
  }

  unsupported(kind) { ThinClient.stats.unsupported[kind] = (ThinClient.stats.unsupported[kind] || 0) + 1; }

  dispatch(m) {
    switch (m.type) {
      case 'session.ready': this.serverCapabilities = m.capabilities ?? {}; return;
      case 'auth.result': return this.onAuthResult(m);
      case 'auth.error': return this.onAuthError(m);
      case 'action.result': case 'reducer.result': return this.onCallResult(m);
      case 'action.error': case 'reducer.error': return this.onCallError(m);
      case 'control.watermark': { const call = this.pending.get(m.id); if (call?.completeAfterControlWatermark) call.completeAfterControlWatermark(); return; }
      case 'replica.watermark': return this.onWatermark(m.revision);
      case 'replica.batch': return this.onBatch(m);
      case 'replica.snapshotStart': return this.onSnapshotStart(m);
      case 'replica.snapshotEncodedPage': return this.onSnapshotPage(m);
      case 'replica.snapshotPage': return this.onLegacySnapshotPage(m);
      case 'replica.snapshot': return this.onLegacySnapshotPage({ ...m, sequence: 0, last: true, snapshotId: undefined });
      case 'replica.ready': return this.onReady(m);
      case 'replica.readyMany': for (const r of m.ready) this.onReady(r); return;
      case 'replica.delta': return this.onDelta(m);
      case 'replica.transactionEncoded': return this.onTransaction(m);
      case 'replica.transaction': return this.onTransaction(m);
      case 'replica.syncing': { const sub = this.byId.get(m.id); if (sub) { sub.isUpToDate = false; sub.notify(); } return; }
      case 'replica.closed': return;
      case 'presence.snapshot': case 'presence.patch': case 'presence.error': return this.onPresence(m);
      case 'replica.needHashes': return this.onNeedHashes(m);
      case 'replica.reset': { if(this.checkpointStore){const sub=this.byId.get(m.id);if(sub)this.invalidateCheckpoint(sub,true);}
        ThinClient.stats.resets++; const sub = this.byId.get(m.id); if (sub) { this.clearReplicaRetry(sub, true); this.byId.delete(sub.id); sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging(); this.openSubscription(sub); sub.notify(); } return; }
      case 'replica.error': return this.onReplicaError(m);
      default: this.unsupported(m.type || 'untyped');
    }
  }

  /** Delivery v2: shared, stream-addressed items, one message per tick (same expansion as the SDK). */
  onBatch(m) {
    this.batchDepth++;
    ThinClient.stats.batches++; ThinClient.stats.batchItems += m.items?.length ?? 0;
    for (const item of m.items ?? []) {
      if (item.type === 'replica.stream') {
        for (const [stream, ids] of this.streams) {
          if (stream !== item.stream && ids.includes(item.id)) {
            ids.splice(ids.indexOf(item.id), 1);
            if (ids.length === 0) this.streams.delete(stream);
          }
        }
        const ids = this.streams.get(item.stream);
        if (!ids) this.streams.set(item.stream, [item.id]); else if (!ids.includes(item.id)) ids.push(item.id);
      } else if (item.type === 'replica.delta' || item.type === 'replica.ready') {
        for (const id of this.streams.get(item.id) ?? []) this.dispatch({ ...item, id });
      } else {
        this.dispatch(item);
      }
    }
    if (typeof m.watermark === 'number') this.onWatermark(m.watermark);
    this.batchDepth--;if(!this.batchDepth)this.flushCheckpoints();
  }

  onAuthResult(m) {
    if (m.id !== this.authInFlight) return;
    if(this.checkpointStore&&this.auth.token)this.activateCache(m.result?.replica);
    this.authInFlight = undefined; this.authReady = true;
    this.presenceAuthenticated();
    const waiter = this.authWaiter; this.authWaiter = undefined;
    if (this.auth.token && !m.result?.subscriptionsPreserved) this.reopenAll();
    this.flushOutbox();
    waiter?.resolve(m.result);
  }
  onAuthError(m) {
    if (m.id === 'membership-changed' || m.id === 'session-expired') {
      if(this.checkpointStore)this.quarantineCache();
      if (['revoked', 'member_removed', 'account_disabled'].includes(m.code ?? '')) { this.unsupported('auth.revoked');if(this.checkpointStore){this.authReady=false;this.close();}return; }
      // Same as the SDK: authenticate again on this socket, then re-open collections.
      this.authReady = false;
      this.presenceDisconnected();
      for (const sub of this.subscriptions.values()) { this.clearReplicaRetry(sub, true); if (sub.id) this.byId.delete(sub.id); sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging(); sub.notify(); }
      this.processedWatermark = 0; this.deferredWatermark = undefined;
      if (this.socket?.readyState === 1) this.sendAuth();
      return;
    }
    if (m.id !== this.authInFlight) return;
    this.authInFlight = undefined;
    const waiter = this.authWaiter; this.authWaiter = undefined;
    waiter?.reject(new Error(m.error));
  }

  onCallResult(m) {
    const call = this.pending.get(m.id);
    if (!call || call.accepted) return;
    call.accepted = true;
    call.serverResultReceivedAtMs = now();
    const complete = () => {
      if (this.pending.get(m.id) !== call) return;
      this.pending.delete(m.id);
      this.emitTelemetry(call.kind, m.id, call.path, 'ok', call.clientSentAtMs, m.trace, undefined, call);
      call.resolve(m.result);
    };
    if (call.scope === 'control' && this.serverCapabilities.controlWatermark === 1) { call.completeAfterControlWatermark = complete; return; }
    const rev = m.committedRevision;
    if (call.scope === 'tenant' && this.serverCapabilities.replicaWatermark === 1 && Number.isSafeInteger(rev) && rev > this.processedWatermark) {
      call.committedRevision = rev; call.completeAfterReplicaWatermark = complete; return;
    }
    complete();
  }
  onCallError(m) {
    const call = this.pending.get(m.id);
    if (!call || call.accepted) return;
    this.pending.delete(m.id);
    this.emitTelemetry(call.kind, m.id, call.path, 'error', call.clientSentAtMs, m.trace, m.error);
    call.reject(new Error(m.error));
  }
  emitTelemetry(kind, id, path, outcome, clientSentAtMs, serverTrace, error, call) {
    if (this.telemetry.size === 0) return;
    const clientReceivedAtMs = now();
    const event = { type: kind, id, path, outcome, error, clientSentAtMs, clientReceivedAtMs, clientDurationMs: clientReceivedAtMs - clientSentAtMs, serverTrace,
      serverResultReceivedAtMs: call?.serverResultReceivedAtMs, replicaWatermarkReceivedAtMs: call?.replicaWatermarkReceivedAtMs };
    for (const h of this.telemetry) h(event);
  }

  onWatermark(revision) {
    if (!Number.isSafeInteger(revision)) return;
    const at = now();
    for (const call of this.pending.values()) if (call.scope === 'tenant' && call.committedRevision !== undefined && call.committedRevision <= revision && call.replicaWatermarkReceivedAtMs === undefined) call.replicaWatermarkReceivedAtMs = at;
    const opening = [...this.subscriptions.values()].filter(s => s.listeners.size > 0 && (s.opening || !s.isUpToDate));
    if (opening.length) {
      // SDK rule: a watermark received while a collection opens cannot certify it.
      const d = this.deferredWatermark ??= { revision, blockers: new Set() };
      d.revision = Math.max(d.revision, revision);
      for (const s of opening) d.blockers.add(s);
      return;
    }
    if (this.deferredWatermark && revision >= this.deferredWatermark.revision) this.deferredWatermark = undefined;
    this.applyWatermark(revision);
  }
  drainDeferredWatermark() {
    const d = this.deferredWatermark;
    if (!d) return;
    if ([...this.subscriptions.values()].some(s => s.listeners.size > 0 && (s.opening || !s.isUpToDate))) return;
    for (const s of d.blockers) {
      if (this.subscriptions.get(s.key) !== s || s.listeners.size === 0) continue;
      if (!s.readyCursor || s.readyCursor.revision < d.revision) return;
    }
    this.deferredWatermark = undefined;
    this.applyWatermark(d.revision);
  }
  applyWatermark(revision) {
    if(this.checkpointStore){const advances=new Map();
     for(const sub of this.subscriptions.values())if(sub.cacheValidated&&!sub.opening&&sub.isUpToDate&&sub.readyCursor&&revision>=sub.readyCursor.revision&&(revision>sub.readyCursor.revision||!sub.checkpointConsistent)){
      const cursor={epoch:sub.readyCursor.epoch,revision};sub.readyCursor=cursor;sub.checkpointConsistent=true;
      if(sub.rowChanges.size||sub.checkpointReset||this.checkpointStore.invalid.has(sub.checkpointKey))this.checkpointDirty.add(sub);else{const group=advances.get(cursor.epoch)||{cursor,windows:[]};group.windows.push({key:sub.checkpointKey,version:sub.checkpointVersion});advances.set(cursor.epoch,group);}
     }this.flushCheckpoints();for(const {windows,cursor}of advances.values())this.checkpointStore.advance(windows,cursor);
    }
    this.processedWatermark = Math.max(this.processedWatermark, revision);
    for (const call of [...this.pending.values()]) {
      if (call.committedRevision === undefined || call.committedRevision > this.processedWatermark) continue;
      const complete = call.completeAfterReplicaWatermark; call.completeAfterReplicaWatermark = undefined; complete?.();
    }
  }

  onSnapshotStart(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    if(this.checkpointStore){this.invalidateCheckpoint(sub);this.validateSnapshotStart(sub,m);this.restoredWindows.delete(sub.checkpointKey);sub.readyCursor=undefined;}
    sub.dropStaging();
    sub.staging = { checksum:new WindowIntegrity(sub.ref.replica?.columns),sequence:0,last:false,meta:m,snapshotId: m.snapshotId, rows: new Map(), truncated: m.truncated === true, key: m.key || sub.keyField, revision: m.cursor?.revision ?? 0, deferred: [] };
  }
  onSnapshotPage(m) {
    ThinClient.stats.snapshotPages++;
    const sub = this.byId.get(m.id);
    if (sub?.staging && sub.staging.snapshotId === m.snapshotId) {
      if(this.checkpointStore)this.validatePage(sub,m,true);
      const key = sub.staging.key;
      for (const canonical of m.canonicalRows) {
        const entry = this.pool.acquire(canonical);
        const id = String(entry.value[key]);
        const old = sub.staging.rows.get(id);
        sub.staging.rows.set(id, entry);if(this.checkpointStore){sub.staging.checksum.set(id,entry);sub.staging.bytes=(sub.staging.bytes||0)+entry.canonicalBytes;const max=sub.staging.meta.maxBytes??sub.ref.replica?.maxBytes;if(max>0&&sub.staging.bytes>max)throw Error('snapshot-page-budget');} this.pool.release(old);
      }
      if (m.truncated) sub.staging.truncated = true;
    }
    // Acknowledge every page, including ones for collections closed meanwhile:
    // the server's credit accounting needs the cumulative ACK either way.
    const ack = { type: 'replica.snapshotAck', id: m.id, snapshotId: m.snapshotId, sequence: m.sequence };
    if (this.ackDelayMs > 0) setTimeout(() => { if (this.socket?.readyState === 1) this.rawSend(ack); }, this.ackDelayMs);
    else this.rawSend(ack);
  }
  /** replica.needHashes (also unsolicited on an open subscription since
   * perf/replica-state): like the SDK, keep the rows and reopen the same id
   * with cursor, keys, row hashes and their xor1 aggregate (fullIntegrity), so
   * the server streams only differing rows. Hashes use the SDK's
   * stableStringify + SHA-256 over the row projected to the declared columns. */
  onNeedHashes(m) {
    ThinClient.stats.needHashes = (ThinClient.stats.needHashes || 0) + 1;
    const sub = this.byId.get(m.id);
    if (!sub) return;
    this.clearReplicaRetry(sub);
    sub.dropStaging();
    if(this.checkpointStore){sub.opening=true;sub.isUpToDate=false;sub.notify();if(this.authReady&&this.socket?.readyState===1)this.rawSend({type:'replica.openMany',opens:[this.checkpointOpen(sub,true)]});return;}
    const cols = sub.ref.replica?.columns;
    const keys = [], hashes = {}, aggregate = Buffer.alloc(32);
    for (const [key, entry] of sub.rows) {
      let row = entry.value;
      if (cols?.length) { const projected = {}; for (const c of cols) if (Object.prototype.hasOwnProperty.call(row, c)) projected[c] = row[c]; row = projected; }
      const hash = createHash('sha256').update(stableStringify(row)).digest();
      for (let i = 0; i < 32; i++) aggregate[i] ^= hash[i];
      keys.push(key); hashes[key] = hash.toString('hex');
    }
    sub.opening = true; sub.isUpToDate = false; sub.notify();
    const cursor = sub.readyCursor;
    const open = cursor
      ? { id: sub.id, path: sub.path, args: sub.args, cursor, keys, hashes, digest: 'xor1:' + aggregate.toString('hex'), fullIntegrity: true }
      : { id: sub.id, path: sub.path, args: sub.args, digest: ZERO_DIGEST };
    if (this.socket?.readyState === 1 && this.authReady) this.rawSend({ type: 'replica.openMany', opens: [open] });
  }
  /** Legacy JSON pages (and single-frame replica.snapshot): stage, no ACK. */
  onLegacySnapshotPage(m) {
    ThinClient.stats.snapshotPages++;
    const sub = this.byId.get(m.id);
    if (!sub || !sub.opening) return;
    if(this.checkpointStore&&sub.staging&&(sub.staging.snapshotId!==m.snapshotId||m.sequence===0))throw Error('snapshot-page-restart');
    if (!sub.staging || sub.staging.snapshotId !== m.snapshotId || m.sequence === 0) {
      sub.dropStaging();
      if(this.checkpointStore){this.invalidateCheckpoint(sub);this.validateSnapshotStart(sub,m);this.restoredWindows.delete(sub.checkpointKey);sub.readyCursor=undefined;}
      sub.staging = { checksum:new WindowIntegrity(sub.ref.replica?.columns),sequence:0,last:false,meta:m,snapshotId: m.snapshotId, rows: new Map(), truncated: m.truncated === true, key: m.key || sub.keyField, revision: m.cursor?.revision ?? 0, deferred: [] };
    }
    if(this.checkpointStore)this.validatePage(sub,m,false);
    const key = sub.staging.key;
    for (const row of m.result || []) {
      const entry = this.pool.acquireValue(row);
      const id = String(entry.value[key]);
      const old = sub.staging.rows.get(id);
      sub.staging.rows.set(id, entry);if(this.checkpointStore){sub.staging.checksum.set(id,entry);sub.staging.bytes=(sub.staging.bytes||0)+entry.canonicalBytes;const max=sub.staging.meta.maxBytes??sub.ref.replica?.maxBytes;if(max>0&&sub.staging.bytes>max)throw Error('snapshot-page-budget');} this.pool.release(old);
    }
    if (m.truncated) sub.staging.truncated = true;
  }
  onReady(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    if(this.checkpointStore&&!this.validateReady(sub,m))return;
    let deferredCheckpoint=false;
    if (sub.staging) {
      // Activate the completed snapshot atomically.
      const next = sub.staging.rows;if(this.checkpointStore){sub.checksum=sub.staging.checksum;sub.rowChanges.clear();sub.checkpointReset=true;}sub.snapshotTruncated=sub.staging.truncated;
      const deferred = sub.staging.deferred, snapshotRevision = sub.staging.revision;deferredCheckpoint=this.checkpointStore&&deferred.some(op=>op.delta||op.revision>snapshotRevision); sub.staging = undefined;
      const old = sub.rows; sub.rows = next; for (const entry of old.values()) this.pool.release(entry);
      sub.version++;
      sub.complete = !(m.truncated === true || (this.checkpointStore&&sub.snapshotTruncated));
      // Like the SDK, frames that arrived while the snapshot was staging apply
      // after activation, in arrival order.
      for (const op of deferred) {
        if (op.delta) this.applyDelta(sub, op.delta);
        else if (op.revision > snapshotRevision) this.applyChange(sub, op.change);
      }
    } else if (m.truncated !== undefined) sub.complete = m.truncated !== true;
    this.clearReplicaRetry(sub, true);
    if (sub.failedAt !== undefined) {
      ThinClient.stats.replicaRecoveries++;
      ThinClient.stats.replicaEvents.push({type:'recovered',at:now(),path:sub.path,failedAt:sub.failedAt,ms:now()-sub.failedAt});
      ThinClient.stats.replicaRecoveryMs += now() - sub.failedAt;
      sub.failedAt = undefined;
    }
    const newer=this.checkpointStore&&sub.readyCursor?.epoch===m.cursor?.epoch&&sub.readyCursor.revision>m.cursor.revision;
    if(!newer){sub.readyCursor = m.cursor; sub.digest = m.digest;} sub.opening = false; sub.isUpToDate = true; sub.error = undefined;
    sub.notify();
    if(this.checkpointStore){sub.cacheValidated=true;sub.checkpointConsistent=!deferredCheckpoint;this.checkpointDirty.add(sub);this.flushCheckpoints();}
    this.drainDeferredWatermark();
  }
  onDelta(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    if (sub.staging) { sub.staging.deferred.push({ delta: m }); return; }
    this.applyDelta(sub, m);
  }
  applyDelta(sub, m) {
    if(this.checkpointStore){if(!this.acceptCursor(sub,m.cursor))return;this.invalidateCheckpoint(sub);}
    if (m.canonicalUpserts) for (const canonical of m.canonicalUpserts) { const entry = this.pool.acquire(canonical); sub.setRow(String(entry.value[sub.keyField]), entry); }
    else if (m.upserts) for (const row of m.upserts) { const entry = this.pool.acquireValue(row); sub.setRow(String(entry.value[sub.keyField]), entry); }
    if (m.deleted) for (const id of m.deleted) sub.deleteRow(String(id));
    if (m.digest) sub.digest = m.digest;
    if (m.cursor) sub.readyCursor = m.cursor;
    sub.notify();
  }
  onTransaction(m) {
    // Entity values are normalized across collections, as in LocalReplica:
    // a change updates that entity in every collection already holding it.
    // A collection that is staging a snapshot keeps the change and applies it
    // after activation (SDK deferSnapshotFrame); a change at or below the
    // snapshot's read revision is already contained in it.
    if(this.checkpointStore&&!validCursor(m.cursor))throw Error('transaction-without-cursor');
    if(!this.entityIndex){this.entityIndex=new Map();for(const sub of this.subscriptions.values()){const list=this.entityIndex.get(sub.entity)||[];list.push(sub);this.entityIndex.set(sub.entity,list);}}
    const revision = m.cursor?.revision ?? Infinity;
    const changes = m.canonicalChanges ? m.canonicalChanges.map((canonical) => changeCache.get(canonical)) : (m.changes || []).map(changeOf);
    for (const change of changes) {
      for (const sub of this.entityIndex.get(change.entity)||[]) {
        if(this.checkpointStore&&sub.readyCursor&&sub.readyCursor.epoch!==m.cursor.epoch)throw Error('transaction-epoch-mismatch');
        if (sub.staging) {sub.staging.deferred.push({ revision, change });if(this.checkpointStore)continue;}
        if (revision <= (sub.readyCursor?.revision ?? 0)) continue;
        this.applyChange(sub, change);
      }
    }
  }
  applyChange(sub, change) {
    if (!sub.rows.has(change.id)) return;
    if(this.checkpointStore)this.invalidateCheckpoint(sub);
    if (change.operation === 'delete' || change.newCanonical === undefined) sub.deleteRow(change.id);
    else sub.setRow(change.id, this.pool.acquire(change.newCanonical, change.newValue));
    sub.notify();
  }
}
