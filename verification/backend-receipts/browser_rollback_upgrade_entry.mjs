import { GonvexClient, browserUpgradeStorage } from '../../packages/client/dist/index.js';

let client, stores, watch, lastState;
let loseRenamedReply = false, droppedResult, blockIncoming = false;
const calls = [], results = [], events = [];
const NativeSocket = window.WebSocket;
class Socket extends NativeSocket {
  send(data) { const message = JSON.parse(data); calls.push(message); super.send(data); }
  addEventListener(type, listener, options) {
    if (type !== 'message') return super.addEventListener(type, listener, options);
    return super.addEventListener(type, event => {
      if (blockIncoming) return;
      const message = JSON.parse(event.data);
      events.push(message);
      if (['reducer.error', 'reducer.result'].includes(message.type)) results.push(message);
      if (loseRenamedReply && message.type === 'reducer.result' && message.path === 'write') {
        droppedResult = message;
        blockIncoming = true;
        client.close();
        return;
      }
      if (typeof listener === 'function') listener.call(this, event); else listener.handleEvent(event);
    }, options);
  }
}
window.WebSocket = Socket;
const ref = { kind: 'query', path: 'all', delivery: 'replica', replica: {
  table: 'receipt_records', key: 'id', columns: ['id', 'value'], maxRows: 100,
} };
window.rollbackUpgrade = {
  async setup(url, version) {
    stores = browserUpgradeStorage({ replicaName: 'rollback-replica', outboxName: 'rollback-outbox',
      initialVersion: 1, contract: { version, offlineMaxAgeMs: null },
      migrations: [{ from: 1, to: 2, intent: ({ args }) => ({ path: 'write', args }) }],
    });
    await stores.ready;
    const entries = await stores.store.load();
    const snapshots = {};
    for (const entry of entries) snapshots[entry.scope] = await stores.storage.load(entry.scope);
    // Replica storage uses a distinct visibility scope; enumerate it too.
    for (const scope of await stores.storage.listScopes()) snapshots[scope] = await stores.storage.load(scope);
    client = new GonvexClient(url, { project: 'project', tenant: 'tenant', token: 'gvx_session_receipt_sdk_fixture',
      identity: { iss: 'receipt-fixture', sub: 'account' }, errorReporting: false, replicaFieldPatches: false,
      localReplica: { storage: stores.storage }, outbox: { store: stores.store } });
    return { entries, snapshots };
  },
  enqueueAbort() { return client.reducer({ kind: 'reducer', path: 'abort', offline: { mode: 'allowed' } },
    { id: 'sdk-upgrade-rollback', value: 7 }, { offline: 'queue' }); },
  connect() { watch = client.watchReplica(ref, {}); client.connect(); },
  loseRenamedReply() { loseRenamedReply = true; },
  async state() { return lastState = { entries: await stores.store.load(), calls, results, droppedResult,
    events, connection: client.connectionState(), cursor: client.localReplica.cursor(),
    ready: watch?.status().isUpToDate ?? false, row: client.localReplica.entity('receipt_records', 'sdk-upgrade-rollback') }; },
  async diagnostics() {
    // A failed storage operation may itself be waiting on a Web Lock. Do not
    // put the failure report behind another read of that same storage lane.
    // The retained profile is the durable oracle; this snapshot is only RAM.
    return { lastCompletedStorageRead: lastState, calls, results, events,
      connection: client.connectionState(), memorySnapshot: client.localReplica.snapshot(),
      locks: await navigator.locks.query(),
      recovery: { authInFlight: client.authInFlight,
        outboxRecoveryPending: client.outboxRecoveryPending, drainingOutbox: client.drainingOutbox,
        authoritativeScope: client.hasAuthoritativeReplicaScope,
        pendingReplicaOpens: client.pendingReplicaOpens.size,
        snapshotSubscriptions: client.snapshotPageSubscriptions.size,
        deferredFrames: client.snapshotDeferredFrames.length } };
  },
  retry(key) { return client.retryIntent(key); },
  transportLog() { return calls; },
  async stop() {
    client.close(); await client.replicaFrames;
    const entries = await stores.store.load(), snapshots = {};
    for (const scope of await stores.storage.listScopes()) snapshots[scope] = await stores.storage.load(scope);
    stores.close(); return { entries, snapshots, calls, results };
  },
};
