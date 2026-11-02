import { GonvexClient, browserUpgradeStorage } from '../../packages/client/dist/index.js';

const nativeSocket = window.WebSocket;
let socketOwner = 'main', client, observer, stores, watch, lostResult, observerUpdate;
const calls = [], events = [];
let counterFault, holdReplicaFrames = false;
const heldReplicaFrames = [], acknowledgements = [], retirementAttempts = [], storageFailures = [];

const requestValue = request => new Promise((resolve, reject) => {
  request.onsuccess = () => resolve(request.result);
  request.onerror = () => reject(request.error);
});
async function rawDatabase(name, mode, run) {
  const request = indexedDB.open(name);
  request.onupgradeneeded = () => request.transaction.abort();
  const database = await requestValue(request);
  const transaction = database.transaction([...database.objectStoreNames], mode);
  const completed = new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error ?? Error('raw oracle transaction aborted'));
  });
  void completed.catch(() => undefined);
  try { const value = await run(transaction); await completed; return value; }
  finally { database.close(); }
}
const rawReplica = scope => rawDatabase('module-upgrade-replica', 'readonly', async transaction =>
  Object.fromEntries(await Promise.all([...transaction.objectStoreNames].map(async name =>
    [name, (await requestValue(transaction.objectStore(name).getAll())).filter(row => row.scope === scope)]))));
const rawEntries = () => rawDatabase('module-upgrade-outbox', 'readonly', transaction => requestValue(transaction.objectStore('entries').getAll()));
class Socket extends nativeSocket {
  constructor(...args) { super(...args); this.owner = socketOwner; }
  send(data) { calls.push({ owner: this.owner, message: JSON.parse(data) }); super.send(data); }
  addEventListener(type, listener, options) {
    if (type !== 'message') return super.addEventListener(type, listener, options);
    return super.addEventListener(type, event => {
      if (this.blocked) return;
      const message = JSON.parse(event.data);
      events.push({ owner: this.owner, message });
      const dispatch = () => {
        if (typeof listener === 'function') listener.call(this, event); else listener.handleEvent(event);
      };
      if (this.owner === 'main' && holdReplicaFrames && message.type.startsWith('replica.')) {
        heldReplicaFrames.push({ message, dispatch }); return;
      }
      if (this.owner === 'main' && message.type === 'reducer.result' && message.path === 'write') {
        lostResult = message;
        if (!counterFault) { this.blocked = true; client.close(); return; }
      }
      dispatch();
    }, options);
  }
}
window.WebSocket = Socket;
const auth = { project: 'project', tenant: 'tenant', token: 'gvx_session_receipt_sdk_fixture',
  identity: { iss: 'receipt-fixture', sub: 'account' }, errorReporting: false, replicaFieldPatches: false };
const ref = version => ({ kind: 'query', path: 'all', delivery: 'replica', replica: {
  table: 'receipt_records', key: 'id', columns: ['id', version === 1 ? 'value' : 'count'], maxRows: 100,
} });
const contract = version => ({ version, offlineMaxAgeMs: null });
window.moduleUpgrade = {
  async setup(url, version) {
    stores = browserUpgradeStorage({ replicaName: 'module-upgrade-replica', outboxName: 'module-upgrade-outbox',
      initialVersion: 1, contract: contract(version), migrations: [{ from: 1, to: 2,
        intent: ({ path, args }) => ({ path: path === 'write' ? 'writeV2' : 'editV2', args: { id: args.id, count: args.value } }),
        replica: snapshot => {
          for (const row of Object.values(snapshot.entities.receipt_records ?? {})) {
            row.count = row.value; delete row.value;
          }
          return snapshot;
        },
      }] });
    await stores.ready;
    const observedStorage = new Proxy(stores.storage, {
      get(target, property) {
        const value = Reflect.get(target, property);
        if (typeof value !== 'function' || !['applyTransaction', 'applyWindowDelta', 'replaceWindow', 'replaceSnapshot', 'advanceWatermark'].includes(property)) return value;
        return (...args) => value(...args).catch(error => {
          if (counterFault && !counterFault.repaired) storageFailures.push({ method: property, error: `${error.name}: ${error.message}` });
          throw error;
        });
      },
    });
    const remove = stores.store.delete.bind(stores.store);
    stores.store.delete = id => { retirementAttempts.push({ id, unrepairedCounter: Boolean(counterFault && !counterFault.repaired) }); return remove(id); };
    client = new GonvexClient(url, { ...auth, clientContract: contract(version),
      localReplica: { storage: observedStorage }, outbox: { store: stores.store } });
    const acknowledge = client.replica.acknowledgeCommand.bind(client.replica);
    client.replica.acknowledgeCommand = (...args) => {
      acknowledgements.push({ key: args[0], revision: args[1], unrepairedCounter: Boolean(counterFault && !counterFault.repaired) });
      return acknowledge(...args);
    };
    return this.saved();
  },
  connect(version) { socketOwner = 'main'; watch = client.watchReplica(ref(version), {}); client.connect(); },
  observeOld(url) {
    socketOwner = 'observer';
    observer = new GonvexClient(url, { ...auth, clientContract: contract(1), outbox: { enabled: false },
      onUpdateRequired: reason => { observerUpdate = reason; } });
    observer.watchReplica(ref(1), {}); observer.connect();
  },
  async stage() {
    // Use the public offline admission path while keeping the authenticated
    // snapshot connected. Both intents must become durable before delivery.
    const descriptor = Object.getOwnPropertyDescriptor(navigator, 'onLine');
    Object.defineProperty(navigator, 'onLine', { value: false, configurable: true });
    try {
      const first = await client.reducer({ kind: 'reducer', path: 'write', offline: { mode: 'allowed' } },
        { id: 'upgrade-intent', value: 7 }, { offline: 'queue' });
      const second = await client.reducer({ kind: 'reducer', path: 'edit', offline: { mode: 'allowed' } },
        { id: 'upgrade-intent', value: 8 }, { offline: 'queue' });
      return { first, second, entries: await stores.store.load() };
    } finally {
      if (descriptor) Object.defineProperty(navigator, 'onLine', descriptor); else delete navigator.onLine;
    }
  },
  deliver() { window.dispatchEvent(new Event('online')); },
  async armCounterFault() {
    const saved = await this.saved();
    const scope = Object.entries(saved.snapshots).find(([, snapshot]) => snapshot?.entities.receipt_records?.['confirmed-before-upgrade'])?.[0];
    if (!scope) throw Error('counter fault has no confirmed persisted owner');
    const beforeReplica = await rawReplica(scope);
    holdReplicaFrames = true;
    const originalCounter = await rawDatabase('module-upgrade-replica', 'readwrite', async transaction => {
      const meta = transaction.objectStore('meta');
      const before = await requestValue(meta.get([scope, 'sequence']));
      if (!Number.isSafeInteger(Number(before?.value)) || Number(before.value) < 1) throw Error('counter fault has no prior durable position');
      await requestValue(meta.put({ ...before, value: '0' }));
      return before;
    });
    counterFault = { scope, originalCounter, beforeReplica, corruptedReplica: await rawReplica(scope), repaired: false };
    return counterFault;
  },
  async counterState() {
    return { entries: await rawEntries(), rawReplica: await rawReplica(counterFault.scope), counterFault,
      calls, events, lostResult, acknowledgements, retirementAttempts, storageFailures,
      heldFrames: heldReplicaFrames.map(frame => frame.message), connection: client.connectionState(),
      drainingOutbox: client.drainingOutbox, processedWatermark: client.processedReplicaWatermarkRevision,
      row: client.localReplica.entity('receipt_records', 'upgrade-intent'), cursor: client.localReplica.cursor() };
  },
  releaseCounterFrames() {
    holdReplicaFrames = false;
    const frames = heldReplicaFrames.splice(0);
    for (const frame of frames) frame.dispatch();
    return frames.map(frame => frame.message);
  },
  async freezeCounterFailure() { client.close(); await client.replicaFrames; },
  async repairCounter() {
    await rawDatabase('module-upgrade-replica', 'readwrite', transaction => requestValue(transaction.objectStore('meta').put(counterFault.originalCounter)));
    counterFault.repaired = true;
    return rawReplica(counterFault.scope);
  },
  async saved() {
    const snapshots = {};
    for (const scope of await stores.storage.listScopes()) snapshots[scope] = await stores.storage.load(scope);
    return { entries: await stores.store.load(), snapshots };
  },
  async state() {
    await client.replicaFrames;
    return { ...await this.saved(), calls, events, lostResult, observerUpdate,
      observerConnection: observer?.connectionState(), connection: client.connectionState(), ready: watch?.status().isUpToDate,
      drainingOutbox: client.drainingOutbox,
      row: client.localReplica.entity('receipt_records', 'upgrade-intent'),
      confirmed: client.localReplica.entity('receipt_records', 'confirmed-before-upgrade') };
  },
  async close() { client.close(); observer?.close(); await client.replicaFrames; stores.close(); },
};
