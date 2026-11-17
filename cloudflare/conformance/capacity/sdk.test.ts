import { it, expect, vi } from 'vitest';
import { GonvexClient } from '../../../packages/client/src/index.js';
import { api } from 'capacity-app-refs';
import { installEffects } from './effects.mjs';
import { reference } from './fixture.mjs';
import { replicaRowsHashes, replicaHashesDigest } from '../../../packages/client/src/replica-integrity.js';
class DataSocket extends EventTarget {
  static OPEN = 1; static CONNECTING = 0; static CLOSING = 2; static CLOSED = 3;
  static instances: DataSocket[] = []; readyState = 0; sent: string[] = [];
  constructor(readonly url: string) { super(); DataSocket.instances.push(this); }
  send(text: string) { this.sent.push(text); }
  close() { this.readyState = 3; this.dispatchEvent(new Event('close')); }
  open() { this.readyState = 1; this.dispatchEvent(new Event('open')); }
  receive(frame: unknown) { const e = new Event('message'); Object.defineProperty(e, 'data', { value: JSON.stringify(frame) }); this.dispatchEvent(e); }
}
it('resolves genuine generated task refs without stripping local execution metadata', async () => {
  const client = new GonvexClient('ws://DATA.invalid/ws', { errorReporting: false, outbox: { enabled: false } });
  try {
    for (const name of ['create', 'assign', 'requestTransition', 'rename', 'archive']) {
      const ref = reference(api, ['tasks', name], 'reducer'); expect(ref.path).toBe(`tasks.${name}`); expect(ref.localExecution).toBe(1);
      await expect(client.reducerWithTransactionEvidence(ref, {}, { offline: 'reject', optimistic: [], timeoutMs: 100, execution: 'server' }))
        .rejects.toMatchObject({ possibleWriteApplied: false, outcome: 'unknown' });
    }
  } finally { client.close(); }
});
it('actual SDK deferred auth cannot send after owner stop, including a later reconnect call', async () => {
  let resolve!: (token: string) => void;
  const url = 'ws://DATA.invalid/ws', controller = new AbortController();
  const f = installEffects({ WebSocket: DataSocket, fetch: async () => {}, socketUrl: url, httpUrls: [], deadline: Infinity, signal: controller.signal,
    budget: { maxConnections: 2, opensPerSecond: 100, maxSentBytes: 100000, maxReceivedBytes: 100000 } });
  vi.stubGlobal('WebSocket', f.WebSocket);
  const client = new GonvexClient(url, { project: 'DATA', tenant: 'DATA', fetchToken: () => new Promise<string>(r => { resolve = r; }), errorReporting: false, outbox: { enabled: false } });
  try {
    client.connect(); const socket = DataSocket.instances.at(-1)!; socket.open();
    expect(socket.sent).toHaveLength(0); controller.abort(); client.close(); f.stop(); resolve('DATA-late');
    await Promise.resolve(); await Promise.resolve(); expect(socket.sent).toHaveLength(0);
    expect(() => client.connect()).toThrow();
  } finally { client.close(); f.stop(); vi.unstubAllGlobals(); DataSocket.instances = []; }
});
it('actual SDK auth fallback, direct auth and direct receipt send obey the independent lifetime boundary', async () => {
  let now = 0; const url = 'ws://DATA.invalid/ws';
  const f = installEffects({ WebSocket: DataSocket, fetch: async () => {}, socketUrl: url, httpUrls: [], deadline: 10,
    budget: { maxConnections: 2, opensPerSecond: 100, maxSentBytes: 1000000, maxReceivedBytes: 1000000 }, now: () => now });
  vi.stubGlobal('WebSocket', f.WebSocket);
  const client = new GonvexClient(url, { project: 'DATA', tenant: 'DATA', token: 'DATA', fetchToken: async () => { throw Error('DATA-offline'); }, errorReporting: false, outbox: { enabled: false } });
  try {
    const collection = { kind: 'query' as const, path: 'DATA.rows', delivery: 'replica' as const, replica: { table: 'DATA', key: 'id', columns: ['id', 'name'], maxRows: 1 } };
    const watch = client.watchReplica(collection, {}), socket = DataSocket.instances.at(-1)!; socket.open();
    await vi.waitFor(() => expect(socket.sent.some(s => JSON.parse(s).type === 'auth')).toBe(true));
    const directive = { protocolVersion: 1, scope: 'tenant', visibilityScope: 'DATA', epoch: 'DATA' };
    socket.receive({ type: 'session.ready', capabilities: { replicaWatermark: 1, reducerReceiptLookup: 1 }, replica: directive });
    const auth = socket.sent.map(s => JSON.parse(s)).find(s => s.type === 'auth');
    socket.receive({ type: 'auth.result', id: auth.id, result: { replica: directive, localIdentity: { auth: { account: { id: 'DATA-a' } }, tenant: { id: 'DATA' }, member: { id: 'DATA-m' } } } });
    await vi.waitFor(() => expect(socket.sent.some(s => JSON.parse(s).type === 'replica.open')).toBe(true));
    const opening = socket.sent.map(s => JSON.parse(s)).find(s => s.type === 'replica.open');
    const hashes = await replicaRowsHashes([], 'id'), digest = await replicaHashesDigest(hashes);
    socket.receive({ type: 'replica.snapshot', id: opening.id, path: collection.path, result: [], cursor: { epoch: 'DATA-db', revision: 3 }, key: 'id', hashes, digest, truncated: false });
    socket.receive({ type: 'replica.ready', id: opening.id, path: collection.path, cursor: { epoch: 'DATA-db', revision: 3 }, digest, truncated: false });
    socket.receive({ type: 'replica.watermark', revision: 3 });
    await vi.waitFor(() => expect(watch.localReplicaState()?.isUpToDate).toBe(true));
    const promise = client.reducerWithTransactionEvidence({ kind: 'reducer', path: 'DATA.rename' }, {}, { offline: 'reject', optimistic: [], timeoutMs: 1000 }).catch(e => e);
    const call = socket.sent.map(s => JSON.parse(s)).find(s => s.type === 'reducer.call'); expect(call).toBeDefined();
    socket.receive({ type: 'reducer.result', id: call.id, path: call.path, result: {}, originCommandId: call.id, committedRevision: 4 });
    socket.receive({ type: 'replica.transaction', originCommandId: call.id, cursor: { epoch: 'DATA-db', revision: 4 }, provenance: { rootCommandId: call.id, channel: 'ui', rootChannel: 'ui', actorAccountId: 'DATA-a', actorMemberId: 'DATA-m' }, changes: [{ entity: 'DATA', id: 'row', operation: 'update', newValue: { id: 'row', name: 'new' } }] });
    now = 10; socket.receive({ type: 'replica.watermark', revision: 4 });
    expect(await promise).toMatchObject({ outcome: 'unknown', possibleWriteApplied: true });
    expect(socket.sent.map(s => JSON.parse(s)).filter(s => s.type === 'query.call')).toHaveLength(0);
    const count = socket.sent.length; client.setAuth({ token: 'DATA-new' });
    expect(socket.sent).toHaveLength(count); expect(() => new f.WebSocket(url)).toThrow();
  } finally { client.close(); f.stop(); vi.unstubAllGlobals(); DataSocket.instances = []; }
});

it('actual SDK direct setAuth bypass cannot send on an OPEN socket after expiry', async () => {
  let now = 0; const url = 'ws://DATA.invalid/ws';
  const f = installEffects({ WebSocket: DataSocket, fetch: async () => {}, socketUrl: url, httpUrls: [], deadline: 10,
    budget: { maxConnections: 2, opensPerSecond: 100, maxSentBytes: 100000, maxReceivedBytes: 100000 }, now: () => now });
  vi.stubGlobal('WebSocket', f.WebSocket);
  const c = new GonvexClient(url, { project: 'DATA', tenant: 'DATA', token: 'DATA', errorReporting: false, outbox: { enabled: false } });
  try {
    c.connect(); const socket = DataSocket.instances.at(-1)!; socket.open();
    const auth = socket.sent.map(s => JSON.parse(s)).find(s => s.type === 'auth');
    const directive = { protocolVersion: 1, scope: 'tenant', visibilityScope: 'DATA', epoch: 'DATA' };
    socket.receive({ type: 'session.ready', capabilities: { replicaWatermark: 1, reducerReceiptLookup: 1 }, replica: directive });
    socket.receive({ type: 'auth.result', id: auth.id, result: { replica: directive, localIdentity: { auth: { account: { id: 'a' } }, tenant: { id: 'DATA' }, member: { id: 'm' } } } });
    await vi.waitFor(() => expect(c.localSessionState()).toBe('authenticated'));
    expect(socket.readyState).toBe(1); const count = socket.sent.length; now = 10;
    expect(() => c.setAuth({ token: 'DATA-rotated' })).toThrow(); expect(socket.sent).toHaveLength(count);
  } finally { c.close(); f.stop(); vi.unstubAllGlobals(); DataSocket.instances = []; }
});
