import { afterEach, expect, it, vi } from 'vitest';
import { GonvexClient } from './index.js';

class Socket extends EventTarget {
  static OPEN = 1;
  static instances: Socket[] = [];
  readyState = 0;
  sent: any[] = [];
  constructor(_url: string) { super(); Socket.instances.push(this); }
  send(text: string) { this.sent.push(JSON.parse(text)); }
  open() { this.readyState = 1; this.dispatchEvent(new Event('open')); }
  receive(frame: unknown) { this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(frame) })); }
  close(code?: number) {
    if (code !== undefined && code !== 1000 && (code < 3000 || code > 4999)) throw new DOMException('Invalid browser close code');
    this.readyState = 3; this.dispatchEvent(new Event('close'));
  }
}
const clients: GonvexClient[] = [];
afterEach(() => { clients.forEach(client => client.close()); clients.length = 0; Socket.instances = []; vi.unstubAllGlobals(); });

async function setup(enabled: boolean) {
  vi.stubGlobal('WebSocket', Socket);
  const client = new GonvexClient('ws://test/ws', { errorReporting: false, replicaFieldPatches: enabled });
  clients.push(client);
  client.setAuth({ token: 'token', identity: { sub: 'account', iss: 'project' } });
  (client as any).connect();
  const socket = Socket.instances.at(-1)!;
  await auth(socket, client);
  return { client, state: client as any, socket };
}
async function auth(socket: Socket, client: GonvexClient) {
  socket.open();
  const replica = { protocolVersion: 1, scope: 'project:tenant', visibilityScope: 'visible', epoch: 'e' };
  socket.receive({ type: 'session.ready', capabilities: { replicaFieldPatches: 1, replicaWatermark: 1 }, replica });
  socket.receive({ type: 'auth.result', id: socket.sent.findLast(frame => frame.type === 'auth').id, result: { replica } });
  await vi.waitFor(() => expect((client as any).authInFlight).toBe(false));
}
function frame(sequence: number, revision: number, row: object, evicted: number[] = []) {
  return { type: 'replica.rowPatch', sequence, rows: [{ index: 0, ...row }], evicted,
    message: { type: 'replica.transaction', cursor: { epoch: 'e', revision },
      changes: [{ entity: 'tasks', id: 'a', operation: 'update' }] } };
}

it.each([true, false])('advertises patches only when explicitly enabled: %s', async enabled => {
  const { socket } = await setup(enabled);
  expect(socket.sent.find(frame => frame.type === 'auth').capabilities.replicaFieldPatches).toBe(enabled ? 1 : undefined);
});

it('applies decoded complete projections in order and handles duplicate wire delivery', async () => {
  const { socket, state } = await setup(true);
  socket.receive(frame(1, 1, { version: 1, value: { id: 'a', status: 'open', nested: { old: 1 } } }));
  const update = frame(2, 2, { version: 2, base: 1, set: { status: null, nested: {} } }, [1]);
  socket.receive(update); socket.receive(update);
  socket.receive({ type: 'replica.watermark', revision: 2 });
  await vi.waitFor(() => expect(state.replica.entity('tasks', 'a')).toEqual({ id: 'a', status: null, nested: {} }));
  expect(state.replica.cursor()).toEqual({ epoch: 'e', revision: 2 });
});

it('keeps a missing-base frame and later watermark out of storage, then reconnects without patches', async () => {
  const { socket, state, client } = await setup(true);
  socket.receive(frame(1, 1, { version: 1, value: { id: 'a', status: 'open' } }));
  await vi.waitFor(() => expect(state.replica.entity('tasks', 'a')?.status).toBe('open'));
  socket.receive(frame(2, 2, { version: 2, base: 99, set: { status: 'bad' } }));
  socket.receive({ type: 'replica.watermark', revision: 2 });
  expect(state.replicaFieldPatchesEnabled).toBe(false);
  expect(state.replica.entity('tasks', 'a')?.status).toBe('open');
  expect(state.replica.cursor().revision).toBe(1);
  await vi.waitFor(() => expect(Socket.instances).toHaveLength(2), { timeout: 3000 });
  const next = Socket.instances.at(-1)!;
  await auth(next, client);
  expect(next.sent.find(frame => frame.type === 'auth').capabilities.replicaFieldPatches).toBeUndefined();
  next.receive({ type: 'replica.transaction', cursor: { epoch: 'e', revision: 2 }, changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { id: 'a', status: 'repaired' } }] });
  await vi.waitFor(() => expect(state.replica.entity('tasks', 'a')?.status).toBe('repaired'));
});

it('resets wire bases on successful reauthentication, including a preserved subscription session', async () => {
  const { socket, state, client } = await setup(true);
  socket.receive(frame(1, 1, { version: 1, value: { id: 'a', status: 'old' } }));
  await vi.waitFor(() => expect(state.replica.entity('tasks', 'a')?.status).toBe('old'));
  client.setAuth({ token: 'refreshed', identity: { sub: 'account', iss: 'project' } });
  const auth = socket.sent.findLast(frame => frame.type === 'auth');
  socket.receive({ type: 'auth.result', id: auth.id, result: { subscriptionsPreserved: true,
    replica: { protocolVersion: 1, scope: 'project:tenant', visibilityScope: 'visible', epoch: 'e' } } });
  await vi.waitFor(() => expect(state.authInFlight).toBe(false));
  socket.receive(frame(1, 2, { version: 1, value: { id: 'a', status: 'new baseline' } }));
  await vi.waitFor(() => expect(state.replica.entity('tasks', 'a')?.status).toBe('new baseline'));
});

it('cannot use a previous tenant wire base after a scope switch', async () => {
  const { socket, state, client } = await setup(true);
  socket.receive(frame(1, 1, { version: 1, value: { id: 'a', private: 'tenant-one' } }));
  await vi.waitFor(() => expect(state.replica.entity('tasks', 'a')?.private).toBe('tenant-one'));
  client.setAuth({ tenant: 'other', token: 'other-token', identity: { sub: 'account', iss: 'project' } });
  const auth = socket.sent.findLast(frame => frame.type === 'auth');
  socket.receive({ type: 'auth.result', id: auth.id, result: {
    replica: { protocolVersion: 1, scope: 'project:other', visibilityScope: 'other-visible', epoch: 'e' } } });
  await vi.waitFor(() => expect(state.authInFlight).toBe(false));
  socket.receive(frame(1, 2, { version: 1, base: 1, set: { id: 'a' } }));
  expect(state.replicaFieldPatchesEnabled).toBe(false);
  expect(state.replica.entity('tasks', 'a')).toBeUndefined();
});
