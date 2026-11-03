/**
 * Thin protocol load client.
 *
 * Speaks the same WebSocket protocol as the real Gonvex SDK and sends the same
 * frames at the same points (auth, action.call, reducer.call, replica.openMany,
 * replica.snapshotAck, replica.close), so the server does the same work.
 * It skips the work that only matters inside a browser:
 *   - no LocalReplica normalization, durable staging or IndexedDB-like stores;
 *   - no client-side digest verification (the server's digest is kept as-is);
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
 * backoff and re-open every collection from a full snapshot.
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

/** Process-wide intern table for immutable row values, reference counted. */
export class RowPool {
  constructor() { this.rows = new Map(); this.stats = { parsed: 0, shared: 0 }; }
  /** `value`, when given, is the already parsed row (legacy JSON frames). */
  acquire(canonical, value) {
    let entry = this.rows.get(canonical);
    if (entry) { entry.refs++; this.stats.shared++; return entry; }
    entry = { canonical, value: value ?? JSON.parse(canonical), refs: 1 };
    this.stats.parsed++;
    this.rows.set(canonical, entry);
    return entry;
  }
  acquireValue(value) { return this.acquire(JSON.stringify(value), value); }
  release(entry) { if (entry && --entry.refs === 0) this.rows.delete(entry.canonical); }
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
const keyOf = (path, args) => path + '\u0000' + JSON.stringify(args ?? {});

class Subscription {
  constructor(client, ref, args) {
    this.client = client; this.ref = ref; this.path = ref.path; this.args = args ?? {};
    this.entity = ref.replica?.table; this.keyField = ref.replica?.key || '_id';
    this.key = keyOf(this.path, this.args);
    this.id = undefined; this.opening = false; this.isUpToDate = false; this.complete = false;
    this.rows = new Map(); // row key -> pool entry (committed window)
    this.staging = undefined; // { snapshotId, rows: Map, truncated }
    this.listeners = new Set(); this.unsubscribeTimer = undefined;
    this.version = 0; this.cachedVersion = -1; this.cachedRows = undefined; this.notifyQueued = false;
    this.readyCursor = undefined; this.digest = undefined; this.error = undefined;
  }
  setRow(id, entry) { const old = this.rows.get(id); if (old === entry) { this.client.pool.release(entry); return; } this.rows.set(id, entry); this.client.pool.release(old); this.version++; }
  deleteRow(id) { const old = this.rows.get(id); if (!old) return; this.rows.delete(id); this.client.pool.release(old); this.version++; }
  clearRows() { for (const entry of this.rows.values()) this.client.pool.release(entry); this.rows.clear(); this.version++; }
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
  static stats = { unsupported: {}, framesIn: 0, framesOut: 0, bytesIn: 0, snapshotPages: 0, reconnects: 0, reopens: 0, resets: 0,
    presence: { snapshots: 0, patches: 0, changes: 0 } };

  // nativeRows defaults to false: the Whagons app's SDK (0.5.2-staging.31 +
  // patch) negotiates native rows only with a staging adapter, which the app
  // does not configure, so production clients use legacy JSON rows.
  // presenceV2: advertise `presence: 1` (SDKs with presence v2 only; the
  // Whagons lab SDK 0.5.2-staging.31 does not, so it defaults to false).
  // batchFrames: advertise replicaBatchFrames (LOAD_THIN_BATCH_FRAMES=0 disables).
  constructor(url, { project, clientContract = 1, pool = defaultPool, nativeRows = false, ackDelayMs = 0, presenceV2 = false, batchFrames = process.env.LOAD_THIN_BATCH_FRAMES !== '0' } = {}) {
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
    for (const sub of this.subscriptions.values()) { clearTimeout(sub.unsubscribeTimer); sub.clearRows(); sub.dropStaging(); }
    this.subscriptions.clear(); this.byId.clear();
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
    // Same contract as the SDK: tenant calls fail with "may or may not have been applied".
    for (const [id, call] of this.pending) {
      if (call.scope === 'control') continue;
      this.pending.delete(id);
      call.fail(new Error(`Connection lost while waiting for ${call.kind} ${call.path}. The operation may or may not have been applied.`));
    }
    this.processedWatermark = 0; this.deferredWatermark = undefined;
    for (const sub of this.subscriptions.values()) {
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
    if (!sub) { sub = new Subscription(this, ref, args); this.subscriptions.set(key, sub); }
    const handlers = new Set();
    let started = false, releaseTimer;
    const listener = () => { for (const h of handlers) h(); };
    const start = () => {
      if (started) return; started = true;
      const live = this.subscriptions.get(key) ?? (this.subscriptions.set(key, sub), sub);
      sub = live;
      clearTimeout(sub.unsubscribeTimer); sub.unsubscribeTimer = undefined;
      sub.listeners.add(listener);
      this.openSubscription(sub);
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
  openSubscription(sub) {
    if (sub.opening || sub.id || this.subscriptions.get(sub.key) !== sub || sub.listeners.size === 0) return;
    if (!this.authReady || !this.auth.token) return; // reopened from auth.result
    sub.id = randomUUID(); sub.opening = true; sub.isUpToDate = false;
    this.byId.set(sub.id, sub);
    this.pendingOpens.add(sub);
    if (!this.openFlushTimer) this.openFlushTimer = setTimeout(() => this.flushOpens(), 0);
  }
  flushOpens() {
    this.openFlushTimer = undefined;
    const opens = [...this.pendingOpens].filter(s => s.opening && s.listeners.size > 0 && this.subscriptions.get(s.key) === s)
      .map(s => ({ id: s.id, path: s.path, args: s.args, digest: ZERO_DIGEST }));
    this.pendingOpens.clear();
    for (let i = 0; i < opens.length; i += MAX_BATCH_OPENS) this.send({ type: 'replica.openMany', opens: opens.slice(i, i + MAX_BATCH_OPENS) });
  }
  releaseSubscription(sub) {
    sub.unsubscribeTimer = undefined;
    if (this.subscriptions.get(sub.key) !== sub || sub.listeners.size > 0) return;
    this.subscriptions.delete(sub.key); this.pendingOpens.delete(sub);
    if (sub.id) { this.byId.delete(sub.id); if (this.socket?.readyState === 1 && this.authReady) this.rawSend({ type: 'replica.close', id: sub.id }); }
    sub.clearRows(); sub.dropStaging();
    this.drainDeferredWatermark();
  }
  reopenAll() {
    for (const sub of this.subscriptions.values()) {
      if (sub.id) this.byId.delete(sub.id);
      sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging();
      if (sub.listeners.size > 0) { ThinClient.stats.reopens++; this.openSubscription(sub); }
    }
  }

  // ---- inbound ---------------------------------------------------------------
  receive(data) {
    ThinClient.stats.framesIn++;
    let message;
    try {
      if (typeof data === 'string') { ThinClient.stats.bytesIn += data.length; message = JSON.parse(data); }
      else { ThinClient.stats.bytesIn += data.byteLength; message = JSON.parse(Buffer.from(data).toString('utf8')); }
    } catch { this.unsupported('unparseable-frame'); return; }
    this.dispatch(message);
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
      case 'replica.reset': { ThinClient.stats.resets++; const sub = this.byId.get(m.id); if (sub) { this.byId.delete(sub.id); sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging(); this.openSubscription(sub); sub.notify(); } return; }
      case 'replica.error': { const sub = this.byId.get(m.id); if (sub) { sub.error = new Error(m.error); sub.notify(); } this.unsupported('replica.error: ' + String(m.error).slice(0, 160)); return; }
      default: this.unsupported(m.type || 'untyped');
    }
  }

  /** Delivery v2: shared, stream-addressed items, one message per tick (same expansion as the SDK). */
  onBatch(m) {
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
  }

  onAuthResult(m) {
    if (m.id !== this.authInFlight) return;
    this.authInFlight = undefined; this.authReady = true;
    this.presenceAuthenticated();
    const waiter = this.authWaiter; this.authWaiter = undefined;
    if (this.auth.token && !m.result?.subscriptionsPreserved) this.reopenAll();
    this.flushOutbox();
    waiter?.resolve(m.result);
  }
  onAuthError(m) {
    if (m.id === 'membership-changed' || m.id === 'session-expired') {
      if (['revoked', 'member_removed', 'account_disabled'].includes(m.code ?? '')) { this.unsupported('auth.revoked'); return; }
      // Same as the SDK: authenticate again on this socket, then re-open collections.
      this.authReady = false;
      this.presenceDisconnected();
      for (const sub of this.subscriptions.values()) { if (sub.id) this.byId.delete(sub.id); sub.id = undefined; sub.opening = false; sub.isUpToDate = false; sub.dropStaging(); sub.notify(); }
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
    this.processedWatermark = Math.max(this.processedWatermark, revision);
    for (const call of [...this.pending.values()]) {
      if (call.committedRevision === undefined || call.committedRevision > this.processedWatermark) continue;
      const complete = call.completeAfterReplicaWatermark; call.completeAfterReplicaWatermark = undefined; complete?.();
    }
  }

  onSnapshotStart(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    sub.dropStaging();
    sub.staging = { snapshotId: m.snapshotId, rows: new Map(), truncated: m.truncated === true, key: m.key || sub.keyField, revision: m.cursor?.revision ?? 0, deferred: [] };
  }
  onSnapshotPage(m) {
    ThinClient.stats.snapshotPages++;
    const sub = this.byId.get(m.id);
    if (sub?.staging && sub.staging.snapshotId === m.snapshotId) {
      const key = sub.staging.key;
      for (const canonical of m.canonicalRows) {
        const entry = this.pool.acquire(canonical);
        const id = String(entry.value[key]);
        const old = sub.staging.rows.get(id);
        sub.staging.rows.set(id, entry); this.pool.release(old);
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
    sub.dropStaging();
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
    if (!sub.staging || sub.staging.snapshotId !== m.snapshotId || m.sequence === 0) {
      sub.dropStaging();
      sub.staging = { snapshotId: m.snapshotId, rows: new Map(), truncated: m.truncated === true, key: m.key || sub.keyField, revision: m.cursor?.revision ?? 0, deferred: [] };
    }
    const key = sub.staging.key;
    for (const row of m.result || []) {
      const entry = this.pool.acquireValue(row);
      const id = String(entry.value[key]);
      const old = sub.staging.rows.get(id);
      sub.staging.rows.set(id, entry); this.pool.release(old);
    }
    if (m.truncated) sub.staging.truncated = true;
  }
  onReady(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    if (sub.staging) {
      // Activate the completed snapshot atomically.
      const next = sub.staging.rows;
      const deferred = sub.staging.deferred, snapshotRevision = sub.staging.revision; sub.staging = undefined;
      const old = sub.rows; sub.rows = next; for (const entry of old.values()) this.pool.release(entry);
      sub.version++;
      sub.complete = !(m.truncated === true);
      // Like the SDK, frames that arrived while the snapshot was staging apply
      // after activation, in arrival order.
      for (const op of deferred) {
        if (op.delta) this.applyDelta(sub, op.delta);
        else if (op.revision > snapshotRevision) this.applyChange(sub, op.change);
      }
    } else if (m.truncated !== undefined) sub.complete = m.truncated !== true;
    sub.readyCursor = m.cursor; sub.digest = m.digest; sub.opening = false; sub.isUpToDate = true; sub.error = undefined;
    sub.notify();
    this.drainDeferredWatermark();
  }
  onDelta(m) {
    const sub = this.byId.get(m.id);
    if (!sub) return;
    if (sub.staging) { sub.staging.deferred.push({ delta: m }); return; }
    this.applyDelta(sub, m);
  }
  applyDelta(sub, m) {
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
    const revision = m.cursor?.revision ?? Infinity;
    const changes = m.canonicalChanges ? m.canonicalChanges.map((canonical) => changeCache.get(canonical)) : (m.changes || []).map(changeOf);
    for (const change of changes) {
      for (const sub of this.subscriptions.values()) {
        if (sub.entity !== change.entity) continue;
        if (sub.staging) sub.staging.deferred.push({ revision, change });
        if (revision <= (sub.readyCursor?.revision ?? 0)) continue;
        this.applyChange(sub, change);
      }
    }
  }
  applyChange(sub, change) {
    if (!sub.rows.has(change.id)) return;
    if (change.operation === 'delete' || change.newCanonical === undefined) sub.deleteRow(change.id);
    else sub.setRow(change.id, this.pool.acquire(change.newCanonical, change.newValue));
    sub.notify();
  }
}
