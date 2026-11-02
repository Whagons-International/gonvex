import { GonvexClient } from '../src/index.ts';
import { IndexedDBLocalReplicaStorage } from '../src/indexeddb-replica.ts';
import { IndexedDBEngineReplicaStorage } from '../src/indexeddb-engine.ts';

const storageFor = (kind, name) => kind === 'legacy' ? new IndexedDBLocalReplicaStorage(name) : new IndexedDBEngineReplicaStorage(name);
const live = { kind: 'query', path: 'tasks.list', delivery: 'live', live: {
  entity: 'tasks', key: 'id', resultPath: [], plan: { table: 'tasks', key: 'id', columns: ['id', 'title'] } } };
const replica = { kind: 'query', path: 'tasks.all', delivery: 'replica', replica: {
  table: 'tasks', key: 'id', columns: ['id', 'title'], maxRows: 100 } };
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const tick = () => new Promise(done => setTimeout(done, 0));
async function until(check) {
  const deadline = Date.now() + 5000;
  while (!check()) { if (Date.now() > deadline) throw new Error('publication fixture timed out'); await tick(); }
}
class Socket extends EventTarget {
  static OPEN = 1; static latest;
  readyState = 0; sent = [];
  constructor() { super(); Socket.latest = this; }
  send(text) { this.sent.push(JSON.parse(text)); }
  open() { this.readyState = 1; this.dispatchEvent(new Event('open')); }
  receive(frame) { this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(frame) })); }
  close() { this.readyState = 3; }
}
async function authenticate(socket, account = 'alice', tenant = 'tenant', collection = false) {
  const type = collection ? 'replica.open' : 'query.subscribe';
  const before = socket.sent.filter(frame => frame.type === type).length;
  await until(() => socket.sent.some(frame => frame.type === 'auth'));
  let auth = socket.sent.filter(frame => frame.type === 'auth').at(-1);
  const accept = () => socket.receive({ type: 'auth.result', id: auth.id, result: { accountId: account, tenantId: tenant,
    replica: { protocolVersion: 1, scope: 'tenant', visibilityScope: `visible-${account}-${tenant}`, epoch: 'db' } } });
  accept();
  const latest = socket.sent.filter(frame => frame.type === 'auth').at(-1);
  if (latest.id !== auth.id) { auth = latest; accept(); }
  await until(() => socket.sent.filter(frame => frame.type === type).length > before);
  return socket.sent.filter(frame => frame.type === type).at(-1);
}
const frame = (id, title, collection, sequence = 1) => collection
  ? { type: 'replica.snapshot', id, path: replica.path, result: [{ id: 'private', title }], key: 'id', maxRows: 100, cursor: { epoch: 'db', revision: sequence } }
  : { type: 'query.result', id, path: live.path, result: [{ id: 'private', title }], subscriptionRevision: { epoch: 'db', sequence } };
function rotate(client, change) {
  if (change === 'account' || change === 'return') {
    client.setAuth({ token: 'bob-session', identity: { sub: 'bob', iss: 'issuer' } });
    if (change === 'return') client.setAuth({ token: 'alice-new-session', identity: { sub: 'alice', iss: 'issuer' } });
  } else if (change === 'tenant') client.setAuth({ tenant: 'other' });
  else if (change === 'close') client.close();
}
function abortNativeEntityWrite(name) {
  const original = IDBObjectStore.prototype.put, aborted = deferred();
  let witnessed = 0, aborts = 0;
  IDBObjectStore.prototype.put = function (...args) {
    const request = original.apply(this, args);
    if (!witnessed && this.name === 'entities' && this.transaction.db.name.startsWith(name)) {
      witnessed = -1;
      request.addEventListener('success', () => {
        witnessed = 1;
        this.transaction.addEventListener('abort', () => { aborts++; aborted.resolve(); }, { once: true });
        this.transaction.abort();
      });
    }
    return request;
  };
  return { aborted: aborted.promise, result: () => ({ witnessed, aborts }), restore: () => { IDBObjectStore.prototype.put = original; } };
}

window.queryPublicationOwnership = async ({ kind, name, family, change = 'unchanged', collection = false }) => {
  const nativeWebSocket = window.WebSocket; window.WebSocket = Socket;
  const storage = storageFor(kind, name), reader = storageFor(kind, name);
  const held = deferred(), release = deferred(); let stop = () => {}, stopLater = () => {}, fault;
  const observed = [], initial = [];
  const client = new GonvexClient('ws://query-browser.test/ws', { project: 'project', tenant: 'tenant', token: 'alice-session',
    identity: { sub: 'alice', iss: 'issuer' }, errorReporting: false, localReplica: { storage }, outbox: { databaseName: name + '-outbox' } });
  let acted = false, holding = true;
  const subscribe = handler => collection ? client.subscribeReplica(replica, {}, handler) : client.subscribeLiveQuery(live, {}, handler);
  const isResult = message => message.type === (collection ? 'replica.snapshot' : 'query.result');
  const capture = target => message => { if (isResult(message) || message.type === 'query.error') target.push({ type: message.type, result: message.result, error: message.error }); };
  try {
    if (family === 'reentrant') {
      stop = subscribe(message => { if (!isResult(message) || acted) return; acted = true; rotate(client, change); held.resolve(); });
      stopLater = subscribe(capture(observed));
    } else stop = subscribe(capture(initial));
    const socket = Socket.latest; socket.open(); const open = await authenticate(socket, 'alice', 'tenant', collection);
    if (['durability', 'deferred', 'order'].includes(family)) {
      const original = storage.replaceWindow.bind(storage);
      storage.replaceWindow = async (...args) => {
        if (!holding) return original(...args);
        holding = false;
        if (family === 'deferred') await original(...args);
        held.resolve(); await release.promise;
        if (family !== 'deferred') return original(...args);
      };
    }
    if (family === 'failure') fault = abortNativeEntityWrite(name);
    socket.receive(frame(open.id, 'Old data', collection));
    let before, durableBefore, fresh;
    if (family === 'reentrant') await held.promise;
    else if (family === 'cache') {
      await until(() => initial.length === 1);
      stopLater = subscribe(capture(observed)); rotate(client, change);
    } else if (family === 'failure') {
      await fault.aborted; fault.restore();
      await until(() => socket.sent.filter(frame => frame.type === 'query.subscribe').length > 1);
      await tick(); before = [...initial];
      durableBefore = (await reader.load('visible-alice-tenant'))?.entities.tasks?.private ?? null;
      socket.receive(frame(open.id, 'Recovered data', false, 2));
      await until(() => initial.some(row => row.result?.[0]?.title === 'Recovered data'));
    } else if (family === 'durability') {
      await held.promise; stopLater = subscribe(capture(observed)); await tick();
      before = [...observed];
      durableBefore = (await reader.load('visible-alice-tenant'))?.entities.tasks?.private ?? null;
      release.resolve(); await until(() => initial.length === 1 && observed.length >= 1);
    } else if (family === 'deferred') {
      await held.promise;
      // Observe completion of the actual pending SDK operation. Native read-view
      // work can take several event-loop turns after the storage gate opens.
      const pendingPublication = [...client.querySubscriptions.values()][0].publication;
      if (change === 'return') {
        client.setAuth({ token: 'bob-session', identity: { sub: 'bob', iss: 'issuer' } }); await authenticate(socket, 'bob');
        client.setAuth({ token: 'alice-new-session', identity: { sub: 'alice', iss: 'issuer' } }); fresh = await authenticate(socket);
      } else rotate(client, change);
      release.resolve(); await pendingPublication; before = [...initial];
      if (change === 'return') { socket.receive(frame(fresh.id, 'Fresh data', false, 2)); await until(() => initial.some(row => row.result?.[0]?.title === 'Fresh data')); }
    } else if (family === 'order') {
      await held.promise;
      socket.receive(change === 'error' ? { type: 'query.error', id: open.id, error: 'Later error' }
        : { ...frame(open.id, 'unused', false, 2), result: 'Later result' });
      await tick(); before = [...initial];
      release.resolve(); await until(() => initial.length === 2);
    }
    await tick(); await tick();
    const stale = [...observed];
    if (['reentrant', 'cache'].includes(family) && !['unchanged', 'close'].includes(change)) {
      fresh = await authenticate(socket, change === 'account' ? 'bob' : 'alice', change === 'tenant' ? 'other' : 'tenant', collection);
      socket.receive(frame(fresh.id, 'Fresh data', collection, collection ? 2 : 1)); await until(() => observed.some(row => row.result?.[0]?.title === 'Fresh data'));
    }
    const durable = (await reader.load('visible-alice-tenant'))?.entities.tasks?.private ?? null;
    return { kind, family, change, collection, observed, initial, stale, before, durableBefore, durable, nativeFailure: fault?.result() };
  } finally {
    release.resolve(); fault?.restore(); stop(); stopLater(); client.close(); reader.close(); storage.close(); window.WebSocket = nativeWebSocket;
  }
};
