import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv, type FunctionReference } from './index';
import { createPortableReducer } from '@gonvex/local-runtime/portable-client';
import { getRow, insertDataRows, reducer, schema, selectRows, type JsonValue } from '@gonvex/module-sdk';

// A complete replica window holds the rows this member may see. It proves a
// row does not exist only where the table's visibility plan admits every row
// for this identity (`unrestrictedVisibility`, emitted by codegen). Elsewhere a
// missing row can still feed a prediction, but cannot reject the intent: the
// intent waits for the server, which holds every row.

class Socket {
  static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
  static all: Socket[] = [];
  readyState = 0;
  sent: Record<string, any>[] = [];
  listeners = new Map<string, Array<(event: any) => void>>();
  constructor(readonly url: string) { Socket.all.push(this); }
  addEventListener(type: string, listener: (event: any) => void) { this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]); }
  send(value: string) { this.sent.push(JSON.parse(value)); }
  emit(type: string, event: any = {}) { for (const listener of this.listeners.get(type) ?? []) listener(event); }
  open() { this.readyState = 1; this.emit('open'); }
  close() { this.readyState = 3; this.emit('close'); }
  receive(value: unknown) { this.emit('message', { data: JSON.stringify(value) }); }
}

type Member = { id: string; accountId: string; status?: string; role?: string; permissions: Record<string, JsonValue> };
const directive = { protocolVersion: 1 as const, scope: 'scope', visibilityScope: 'visible', epoch: 'epoch' };
const url = 'ws://localhost:9999';
const sessionKey = ['identity', url, 'project', 'tenant', 'issuer', 'account'].join('\u0000');
const columns = ['id', 'name', 'ownerId', 'deletedAt'];
const collection: FunctionReference = { kind: 'query', path: '__local.spaces', delivery: 'replica', replica: { table: 'spaces', key: 'id', columns } };
const collectionKey = `${collection.path}\u0000{}\u0000{"delivery":"replica","live":null,"scope":"tenant"}`;
const spacesSchema = { spaces: { key: 'id', columns: { id: { type: 'text', nullable: false }, name: { type: 'text', nullable: false }, ownerId: { type: 'text', nullable: true }, deletedAt: { type: 'bigint', nullable: true } } } };
const ref = (path: string): FunctionReference => ({ kind: 'reducer', path, offline: { mode: 'allowed' }, localExecution: 1 });

const reducers = {
  // Known-present lookup by primary key, as restoring into a live parent does.
  restoreInto: reducer({
    args: schema.object({ spaceId: schema.string() }), result: schema.string(),
    run: async (ctx, args: { spaceId: string }) => {
      const space = await getRow<{ deletedAt: number | null }>(ctx.db, 'spaces', 'id', args.spaceId);
      if (!space || space.deletedAt != null) throw new Error('Space was deleted');
      return 'restored';
    },
  }),
  // Absence over a scan: a hidden row can hold the name.
  claimName: reducer({
    args: schema.object({ name: schema.string() }), result: schema.string(),
    run: async (ctx, args: { name: string }) => {
      if ((await selectRows(ctx.db, { key: 'id', table: 'spaces', where: { column: 'name', op: 'eq', value: args.name } })).length) throw new Error('Name taken');
      return 'claimed';
    },
  }),
  // Absence within one owner's rows.
  requireOwned: reducer({
    args: schema.object({ ownerId: schema.string() }), result: schema.string(),
    run: async (ctx, args: { ownerId: string }) => {
      if (!(await selectRows(ctx.db, { key: 'id', table: 'spaces', where: { column: 'ownerId', op: 'eq', value: args.ownerId } })).length) throw new Error('Owns no space');
      return 'owned';
    },
  }),
  // Caller-chosen and intent-allocated inserts.
  create: reducer({
    args: schema.object({ id: schema.optional(schema.string()), name: schema.string() }), result: schema.string(),
    run: async (ctx, args: { id?: string; name: string }) => {
      const [row] = await insertDataRows(ctx, 'spaces', [{ ...(args.id ? { id: args.id } : {}), name: args.name, deletedAt: null }], 'id');
      return String(row.id);
    },
  }),
};

const clients: GonvexClient[] = [];
beforeEach(() => { Socket.all = []; vi.stubGlobal('WebSocket', Socket); });
afterEach(() => { clients.splice(0).forEach(client => client.close()); vi.unstubAllGlobals(); });

const member = (overrides: Partial<Member> = {}): Member => ({ id: 'member', accountId: 'account', status: 'active', role: 'member', permissions: {}, ...overrides });
const visible = { live: { id: 'live', name: 'Live', ownerId: 'someone', deletedAt: null }, gone: { id: 'gone', name: 'Gone', ownerId: 'someone', deletedAt: 900 } };

type Conditions = Record<string, unknown>;
async function fixture(options: { member?: Member; unrestrictedVisibility?: Conditions | (() => Promise<Conditions>); mode?: 'portable'; deferReplicaOpen?: (path: string) => boolean } = {}) {
  const identity = { auth: { account: { id: 'account' } }, tenant: { id: 'tenant' }, member: options.member ?? member() };
  const storage = new MemoryLocalReplicaStorage();
  const store = createKvOutboxStore(createMemoryGonvexKv());
  await storage.saveSession(sessionKey, { directive, identity, artifactHash: 'artifact' });
  // The server delivered this member's complete window: two visible spaces. A
  // space named "Hidden" (id "hidden") exists but is outside their visibility.
  await storage.replaceSnapshot({ entities: { spaces: visible },
    liveQueries: { [collectionKey]: { signature: collectionKey, kind: 'replica', entity: 'spaces', key: 'id', ids: Object.keys(visible), completeness: 'complete', source: 'server' } },
  }, 'visible');
  const host = createPortableReducer({ schema: spacesSchema as any, reducers, artifactHash: 'artifact' });
  const client = new GonvexClient(url, {
    project: 'project', tenant: 'tenant', identity: { sub: 'account', iss: 'issuer' },
    localReplica: { storage }, outbox: { store },
    ...(options.deferReplicaOpen ? { deferReplicaOpen: options.deferReplicaOpen } : {}),
    localRuntime: {
      ...('mode' in options ? { mode: options.mode } : { mode: 'portable' as const }),
      artifactHash: 'artifact', tables: ['spaces'], collections: [collection], create: () => host,
      ...(options.unrestrictedVisibility ? { unrestrictedVisibility: options.unrestrictedVisibility } : {}),
    },
  });
  clients.push(client);
  const fallbacks: Array<{ reason: string; table?: string }> = [];
  client.onLocalExecutionFallback(event => fallbacks.push(event));
  return { client, store, fallbacks, identity };
}

async function connect(client: GonvexClient, identity: unknown) {
  client.connect();
  const socket = Socket.all.at(-1)!;
  socket.open();
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === 'auth')).toBe(true));
  const auth = socket.sent.find(message => message.type === 'auth')!;
  socket.receive({ type: 'auth.result', id: auth.id, result: { replica: directive, localIdentity: identity, artifactHash: 'artifact', accountId: 'account', tenantId: 'tenant' } });
  return socket;
}

const queued = async (store: ReturnType<typeof createKvOutboxStore>) => (await store.load()).map(entry => ({ path: entry.path, args: entry.args, patches: entry.patches ?? [], state: entry.state }));
const restricted = {}; // No condition: the plan depends on rows, so no identity sees every row.
const everyone = { spaces: { operator: 'public' } };
const ownRows = { spaces: { operator: 'or', children: [{ operator: 'permission', value: 'view-spaces' }, { operator: 'eqContext', column: 'ownerId', context: 'member.id' }] } };
const admins = { spaces: { operator: 'or', children: [{ operator: 'role', value: 'admin' }, { operator: 'permission', value: 'view-spaces' }] } };

describe.each([['portable', 'portable' as const], ['snapshot', undefined]])('%s local execution', (_name, mode) => {
  it('queues, rather than rejects, an intent whose parent row is outside this member\'s visibility', async () => {
    const { client, store, fallbacks } = await fixture({ mode, unrestrictedVisibility: restricted });
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).resolves.toMatchObject({ status: 'queued' });
    expect(await queued(store)).toEqual([{ path: 'restoreInto', args: { spaceId: 'hidden' }, patches: [], state: 'pending' }]);
    expect(fallbacks).toEqual([expect.objectContaining({ reason: 'incompleteReplica', table: 'spaces' })]);
  });

  it('queues a scan whose absence a hidden row could contradict', async () => {
    const { client, store } = await fixture({ mode, unrestrictedVisibility: restricted });
    await expect(client.reducer(ref('claimName'), { name: 'Hidden' })).resolves.toBe('claimed');
    // A successful prediction is kept: the server corrects or rejects it on replay.
    expect(await queued(store)).toEqual([{ path: 'claimName', args: { name: 'Hidden' }, patches: [], state: 'pending' }]);
  });

  it('still rejects from a row it looks up by key, even where visibility is restricted', async () => {
    const { client, store } = await fixture({ mode, unrestrictedVisibility: restricted });
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'gone' })).rejects.toThrow('Space was deleted');
    expect(await store.load()).toHaveLength(0);
  });

  it('leaves a restricted scan\'s rejection to the server, but rejects it locally where every row is visible', async () => {
    // A scan cannot show that the rows it found are all the rows; only the server can decide.
    const limited = await fixture({ mode, unrestrictedVisibility: restricted });
    await expect(limited.client.reducer(ref('claimName'), { name: 'Live' })).resolves.toMatchObject({ status: 'queued' });
    expect(await queued(limited.store)).toEqual([{ path: 'claimName', args: { name: 'Live' }, patches: [], state: 'pending' }]);
    const open = await fixture({ mode, unrestrictedVisibility: everyone });
    await expect(open.client.reducer(ref('claimName'), { name: 'Live' })).rejects.toThrow('Name taken');
    expect(await open.store.load()).toHaveLength(0);
  });

  it('predicts from a row it holds, where visibility is restricted', async () => {
    const { client, store } = await fixture({ mode, unrestrictedVisibility: restricted });
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'live' })).resolves.toBe('restored');
    expect(await store.load()).toHaveLength(1);
  });

  it('rejects a missing row locally where every row is visible', async () => {
    const { client, store, fallbacks } = await fixture({ mode, unrestrictedVisibility: everyone });
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).rejects.toThrow('Space was deleted');
    expect(await store.load()).toHaveLength(0);
    expect(fallbacks).toEqual([]);
  });

  it('keeps caller-chosen and newly allocated inserts predicted where visibility is restricted', async () => {
    const { client, store } = await fixture({ mode, unrestrictedVisibility: restricted });
    await expect(client.reducer(ref('create'), { id: 'mine', name: 'Mine' })).resolves.toBe('mine');
    const allocated = await client.reducer<string>(ref('create'), { name: 'Fresh' });
    expect(allocated).toEqual(expect.any(String));
    expect((await queued(store)).map(entry => entry.patches.map(patch => [patch.op, patch.rowId]))).toEqual([[['insert', 'mine']], [['insert', allocated]]]);
    expect(client.localReplica.entity('spaces', 'mine')).toMatchObject({ name: 'Mine' });
    // A caller-chosen ID that this member holds is a genuine duplicate.
    await expect(client.reducer(ref('create'), { id: 'live', name: 'Again' })).rejects.toThrow('already exists');
  });
});

describe('identity-only visibility conditions', () => {
  it.each([
    ['an admin by role rejects a missing row locally', member({ role: 'admin' }), true],
    ['a member granted the permission (true) rejects locally', member({ permissions: { 'view-spaces': true } }), true],
    ['a member granted the permission ("1") rejects locally', member({ permissions: { 'view-spaces': '1' } }), true],
    ['a member without the permission queues', member(), false],
    ['a member whose permission value is not read as granted everywhere queues', member({ permissions: { 'view-spaces': 2 } }), false],
    // SQL reads lower(value) and grants "TRUE"; the in-memory mask does not.
    ['a member whose permission is "TRUE" queues', member({ permissions: { 'view-spaces': 'TRUE' } }), false],
    // A JSON 1 and 1.0 both arrive as 1, but SQL reads 1.0 as '1.0' and denies it.
    ['a member whose permission is the number 1 queues', member({ permissions: { 'view-spaces': 1 } }), false],
    ['an admin who is not active queues', member({ role: 'admin', status: 'suspended' }), false],
  ])('%s', async (_case, who, rejectsLocally) => {
    const { client, store } = await fixture({ member: who, unrestrictedVisibility: admins });
    const call = client.reducer(ref('restoreInto'), { spaceId: 'hidden' });
    if (rejectsLocally) {
      await expect(call).rejects.toThrow('Space was deleted');
      expect(await store.load()).toHaveLength(0);
    } else {
      await expect(call).resolves.toMatchObject({ status: 'queued' });
      expect(await store.load()).toHaveLength(1);
    }
  });

  // `not` turns a wrong "denied" into a wrong "every row is visible". It may prove only from a value
  // that the runtime's SQL and in-memory paths both read as denied.
  const unlessHidden = { spaces: { operator: 'not', children: [{ operator: 'permission', value: 'hide-spaces' }] } };
  it.each([
    ['the permission is absent', member(), true],
    ['the permission is false', member({ permissions: { 'hide-spaces': false } }), true],
    ['the permission is null', member({ permissions: { 'hide-spaces': null } }), true],
    ['the permission is "0"', member({ permissions: { 'hide-spaces': '0' } }), true],
    ['the permission is 0', member({ permissions: { 'hide-spaces': 0 } }), true],
    ['the permission is granted as "1"', member({ permissions: { 'hide-spaces': '1' } }), false],
    ['the permission is "TRUE"', member({ permissions: { 'hide-spaces': 'TRUE' } }), false],
    ['the permission is the number 1, maybe 1.0', member({ permissions: { 'hide-spaces': 1 } }), false],
    ['the permission is 2', member({ permissions: { 'hide-spaces': 2 } }), false],
    ['the permission is not ASCII', member({ permissions: { 'hide-spaces': 'TRUE\u0130' } }), false],
    ['the member is not active', member({ status: 'suspended' }), false],
    ['the member has no status', member({ status: undefined }), false],
    ['the member has no permissions', { ...member(), permissions: undefined as unknown as Record<string, JsonValue> }, false],
  ])('under not, a member for whom %s', async (_case, who, rejectsLocally) => {
    const { client, store } = await fixture({ member: who, unrestrictedVisibility: unlessHidden });
    const call = client.reducer(ref('restoreInto'), { spaceId: 'hidden' });
    if (rejectsLocally) await expect(call).rejects.toThrow('Space was deleted');
    else await expect(call).resolves.toMatchObject({ status: 'queued' });
    expect(await store.load()).toHaveLength(rejectsLocally ? 0 : 1);
  });

  it.each([
    ['proves for an active member with another role', member({ role: 'member' }), true],
    ['queues for a member without a role', member({ role: undefined }), false],
    ['queues for a member who holds the role', member({ role: 'guest' }), false],
  ])('not role %s', async (_case, who, rejectsLocally) => {
    const { client, store } = await fixture({ member: who, unrestrictedVisibility: { spaces: { operator: 'not', children: [{ operator: 'role', value: 'guest' }] } } });
    const call = client.reducer(ref('restoreInto'), { spaceId: 'hidden' });
    if (rejectsLocally) await expect(call).rejects.toThrow('Space was deleted');
    else await expect(call).resolves.toMatchObject({ status: 'queued' });
    expect(await store.load()).toHaveLength(rejectsLocally ? 0 : 1);
  });

  it('treats an unknown condition and a binding without conditions as restricted', async () => {
    for (const unrestrictedVisibility of [{ spaces: { operator: 'inSet', column: 'id', set: 'mine' } }, { spaces: { operator: 'not', children: [{ operator: 'eqContext' }] } }, undefined]) {
      const { client, store } = await fixture({ member: member({ role: 'admin' }), unrestrictedVisibility });
      await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).resolves.toMatchObject({ status: 'queued' });
      expect(await store.load()).toHaveLength(1);
    }
  });
});

describe('own-row visibility', () => {
  it('proves absence among the member\'s own rows, and only there', async () => {
    const { client, store } = await fixture({ unrestrictedVisibility: ownRows });
    // Every row whose ownerId is this member is visible to them, so none exists.
    await expect(client.reducer(ref('requireOwned'), { ownerId: 'member' })).rejects.toThrow('Owns no space');
    expect(await store.load()).toHaveLength(0);
    // Another owner's rows may be hidden: the server decides.
    await expect(client.reducer(ref('requireOwned'), { ownerId: 'other' })).resolves.toMatchObject({ status: 'queued' });
    expect(await queued(store)).toEqual([{ path: 'requireOwned', args: { ownerId: 'other' }, patches: [], state: 'pending' }]);
  });
});

describe('the queued intent reaches the server on reconnect', () => {
  it.each(['accepted', 'rejected'])('%s by the server', async outcome => {
    const { client, store, identity } = await fixture({ unrestrictedVisibility: restricted });
    const rejection = vi.fn();
    client.onReducerRejection(rejection);
    await client.reducer(ref('restoreInto'), { spaceId: 'hidden' });
    const [intent] = await store.load();
    const socket = await connect(client, identity);
    await vi.waitFor(() => expect(socket.sent.some(message => message.type === 'reducer.call')).toBe(true));
    const call = socket.sent.find(message => message.type === 'reducer.call')!;
    expect(call).toMatchObject({ id: intent.idempotencyKey, args: { spaceId: 'hidden' } });
    if (outcome === 'accepted') {
      socket.receive({ type: 'reducer.result', id: call.id, result: 'restored', originCommandId: call.id });
      await vi.waitFor(async () => expect(await store.load()).toHaveLength(0));
      expect(rejection).not.toHaveBeenCalled();
    } else {
      socket.receive({ type: 'reducer.error', id: call.id, error: 'Space was deleted', class: 'rejected', retryable: false });
      await vi.waitFor(() => expect(rejection).toHaveBeenCalledWith(expect.objectContaining({ reducerId: call.id, error: 'Space was deleted' })));
      expect((await store.load())[0]?.state).toBe('rejected');
    }
  });
});

// Codegen ships the conditions as a loader so they stay out of the boot bundle.
describe.each([['portable', 'portable' as const], ['snapshot', undefined]])('%s local execution with generated conditions loaded on demand', (_name, mode) => {
  it('loads them only when a rejection needs an absence proof, and once', async () => {
    const load = vi.fn(async () => everyone);
    const { client, store } = await fixture({ mode, unrestrictedVisibility: load });
    // Successful predictions, with caller-chosen and allocated IDs, never need them.
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'live' })).resolves.toBe('restored');
    await expect(client.reducer(ref('create'), { id: 'mine', name: 'Mine' })).resolves.toBe('mine');
    await expect(client.reducer<string>(ref('create'), { name: 'Fresh' })).resolves.toEqual(expect.any(String));
    expect(load).not.toHaveBeenCalled();
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).rejects.toThrow('Space was deleted');
    // Offline, the loaded conditions are reused rather than fetched again.
    await expect(client.reducer(ref('claimName'), { name: 'Live' })).rejects.toThrow('Name taken');
    expect(load).toHaveBeenCalledTimes(1);
    expect(await store.load()).toHaveLength(3);
  });

  it('queues while they cannot load, and loads them again for a later rejection', async () => {
    let available = false;
    const load = vi.fn(async () => {
      if (!available) throw new Error('Failed to fetch dynamically imported module');
      return everyone;
    });
    const { client, store } = await fixture({ mode, unrestrictedVisibility: load });
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).resolves.toMatchObject({ status: 'queued' });
    expect(await queued(store)).toEqual([{ path: 'restoreInto', args: { spaceId: 'hidden' }, patches: [], state: 'pending' }]);
    // A rejection from a row this device holds still stands without them.
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'gone' })).rejects.toThrow('Space was deleted');
    const failedLoads = load.mock.calls.length;
    available = true;
    await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).rejects.toThrow('Space was deleted');
    expect(load).toHaveBeenCalledTimes(failedLoads + 1);
    expect(await store.load()).toHaveLength(1);
  });

  it.each([
    ['an admin demoted', member({ role: 'admin' }), member({ role: 'member' }), true],
    ['a member promoted', member(), member({ role: 'admin' }), false],
  ])('proves absence for the identity the intent captured, for %s while they load', async (_case, before, after, rejectsLocally) => {
    let release!: (conditions: Conditions) => void;
    const load = vi.fn(() => new Promise<Conditions>((resolve) => { release = resolve; }));
    const { client, store, identity } = await fixture({ mode, member: before, unrestrictedVisibility: load });
    const call = client.reducer(ref('restoreInto'), { spaceId: 'hidden' });
    await vi.waitFor(() => expect(load).toHaveBeenCalled());
    (client as unknown as { localIdentity: unknown }).localIdentity = { ...identity, member: after };
    release(admins);
    if (rejectsLocally) await expect(call).rejects.toThrow('Space was deleted');
    else await expect(call).resolves.toMatchObject({ status: 'queued' });
    expect(await store.load()).toHaveLength(rejectsLocally ? 0 : 1);
  });

  it('does not rerun the intent after the session closes while they load', async () => {
    let release!: (conditions: Conditions) => void;
    const load = vi.fn(() => new Promise<Conditions>((resolve) => { release = resolve; }));
    const { client } = await fixture({ mode, unrestrictedVisibility: load });
    const call = client.reducer(ref('restoreInto'), { spaceId: 'hidden' });
    await vi.waitFor(() => expect(load).toHaveBeenCalled());
    client.close();
    release(everyone);
    // With every row visible the rerun would reject locally; a closed session must not decide it.
    await expect(call).rejects.not.toThrow('Space was deleted');
  });
});

it('delivers an intent while dependency collection opens remain deferred', async () => {
  const hold = vi.fn(() => true);
  const { client, store, identity } = await fixture({ unrestrictedVisibility: restricted, deferReplicaOpen: hold });
  await expect(client.reducer(ref('restoreInto'), { spaceId: 'hidden' })).resolves.toMatchObject({ status: 'queued' });
  const [intent] = await store.load();
  expect(intent).toBeDefined();
  expect(hold).toHaveBeenCalledWith('__local.spaces');
  const socket = await connect(client, identity);
  await vi.waitFor(() => expect(socket.sent.some(message => message.type === 'reducer.call')).toBe(true));
  const call = socket.sent.find(message => message.type === 'reducer.call')!;
  expect(call).toMatchObject({ id: intent.idempotencyKey, args: { spaceId: 'hidden' } });
  expect(JSON.stringify(socket.sent)).not.toContain('"path":"__local.spaces"');
  socket.receive({ type: 'reducer.result', id: call.id, result: 'restored', originCommandId: call.id });
  await vi.waitFor(async () => expect(await store.load()).toHaveLength(0));
  expect(JSON.stringify(socket.sent)).not.toContain('"path":"__local.spaces"');
});
