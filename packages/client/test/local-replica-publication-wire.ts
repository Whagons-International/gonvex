import { afterEach, describe, expect, it, vi } from 'vitest';
import { GonvexClient, type FunctionReference, type ReplicaMessage } from '../src/index.js';
import { LocalReplica, type ReplicaRow } from '../src/local-replica.js';
import { replicaAggregateDigest, replicaHashesDigest, replicaRowsHashes } from '../src/replica-integrity.js';
import { publicationCursor as cursor, publicationLatest as latest, publicationScope as scope,
  type PublicationFactory } from './local-replica-publication-contract.js';

// Real GonvexClient message dispatch and public view. Only the WebSocket transport
// and UUID source are controlled. No private client/replica methods or maps.
// See reachability.md for a finite Rust-source schedule and invalid input limits.
class PublicationSocket extends EventTarget {
  static OPEN = 1;
  static instances: PublicationSocket[] = [];
  readyState = 0;
  sent: Array<Record<string, any>> = [];
  constructor(_url: string) { super(); PublicationSocket.instances.push(this); }
  send(frame: string) { this.sent.push(JSON.parse(frame)); }
  open() { this.readyState = 1; this.dispatchEvent(new Event('open')); }
  receive(frame: unknown) { this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(frame) })); }
  close() { this.readyState = 3; }
}

type Open = { id: string; path: string };
export function publicationWireContract(name: string, create: PublicationFactory) {
 describe(`${name}: GonvexClient wire publication`, () => {
  const disposals: Array<() => void> = [];
  afterEach(() => {
    for (const dispose of disposals.splice(0).reverse()) dispose();
    PublicationSocket.instances = [];
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });
  async function fixture(paged: boolean) {
    const owner = create();
    disposals.push(() => owner.close());
    vi.stubGlobal('WebSocket', PublicationSocket);
    // Runtime normalization visits subscription IDs in BTreeMap order. Select
    // one possible ordering deterministically, high before details. Auth scope
    // rotation preserves subscription insertion order while assigning new IDs.
    let uuid = 0;
    vi.spyOn(globalThis.crypto, 'randomUUID').mockImplementation(() =>
      `00000000-0000-4000-8000-${String(++uuid).padStart(12, '0')}`);
    const client = new GonvexClient('ws://publication.test/ws', {
      errorReporting: false, telemetry: false, replicaSnapshotPages: paged,
      outbox: { enabled: false }, localReplica: { storage: owner.storage },
      replicaSubscriptionRetentionMs: 0,
    });
    disposals.push(() => client.close());
    client.setAuth({ token: 'fixture-token', project: 'publication-project', tenant: 'publication-tenant',
      identity: { sub: 'publication-account', iss: 'publication-project' } });
    const delivered: Record<string, ReplicaMessage[]> = {};
    const refs: Array<[string, string, string[]]> = [
      ['high', 'tasks', ['_id', 'status']],
      ['details', 'tasks', ['_id', 'status', 'name']],
      ['anchor', 'anchors', ['_id']],
    ];
    for (const [path, table, columns] of refs) {
      const ref: FunctionReference = { kind: 'query', path, delivery: 'replica',
        replica: { table, key: '_id', columns } };
      delivered[path] = [];
      disposals.push(client.subscribeReplica(ref, {}, message => delivered[path]!.push(message)));
    }
    const socket = PublicationSocket.instances.at(-1)!;
    socket.open();
    const directive = { protocolVersion: 1, scope: 'publication-project:publication-tenant',
      visibilityScope: scope, epoch: cursor(1).epoch };
    socket.receive({ type: 'session.ready', capabilities: {
      replicaSnapshotPages: 1, replicaResume: 1, replicaWatermark: 1, replicaBatch: 1,
    } });
    const auth = socket.sent.find(frame => frame.type === 'auth')!;
    expect(auth.capabilities.replicaSnapshotPages).toBe(paged ? 1 : undefined);
    socket.receive({ type: 'auth.result', id: auth.id, result: { replica: directive } });
    const opens = () => socket.sent.flatMap(frame => frame.type === 'replica.openMany' ? frame.opens
      : frame.type === 'replica.open' ? [frame] : []) as Open[];
    await vi.waitFor(() => expect(opens()).toHaveLength(3));
    const open = (path: string) => opens().find(request => request.path === path)!;
    expect(open('high').id < open('details').id).toBe(true);
    async function snapshot(path: string, revision: number, rows: ReplicaRow[]) {
      const request = open(path);
      const hashes = await replicaRowsHashes(rows, '_id');
      const digest = replicaAggregateDigest(hashes);
      const snapshotDigest = await replicaHashesDigest(hashes);
      const before = delivered[path]!.filter(message => message.type === 'replica.ready').length;
      const common = { id: request.id, path, result: rows, cursor: cursor(revision), key: '_id',
        mode: 'eager', orderBy: '', orderDirection: 'asc', maxRows: 0, maxBytes: 0, truncated: false };
      socket.receive(paged ? { ...common, type: 'replica.snapshotPage', snapshotId: `${path}-${revision}`,
        sequence: 0, last: true, scope: directive.scope, visibilityScope: scope, digest } : { ...common, type: 'replica.snapshot', hashes, digest: snapshotDigest });
      socket.receive({ type: 'replica.ready', id: request.id, path, cursor: cursor(revision), digest, mode: 'eager', truncated: false });
      await vi.waitFor(() => expect(delivered[path]!.filter(message => message.type === 'replica.ready')).toHaveLength(before + 1));
    }
    async function cold() {
      const replica = new LocalReplica(owner.fresh());
      disposals.push(() => replica.dispose());
      await replica.activateScope(scope);
      return replica;
    }
    const observe = () => {
      const seen: Array<ReplicaRow | undefined> = [];
      disposals.push(client.localReplica.subscribe(() => seen.push(client.localReplica.entity('tasks', 'a'))));
      return seen;
    };
    return { client, socket, owner, open, snapshot, cold, observe, delivered };
  }

  for (const paged of [false, true]) {
   const transport = paged ? 'JSON page' : 'rollback JSON snapshot';
   it(`${transport}: publishes resolved rows when a different opening snapshot at 9 completes after 10`, async () => {
    const { client, snapshot, cold, owner, observe } = await fixture(paged);
    await snapshot('anchor', 1, []);
    // details begins its repeatable-read snapshot at 9, but completes after high.
    await snapshot('high', 10, [latest]);
    expect(client.localReplica.cursor()).toEqual(cursor(1));
    const seen = observe();
    await snapshot('details', 9, [{ _id: 'a', status: 'new', name: 'Renamed' }]);
    const hot = client.localReplica.entity('tasks', 'a');
    const durable = await owner.storage.loadEntityRows(scope, 'tasks', ['a']);
    const restored = await cold();
    const expected = { ...latest, name: 'Renamed' };
    expect.soft(durable).toEqual([{ id: 'a', row: expected }]);
    expect.soft(restored.entity('tasks', 'a')).toEqual(expected);
    expect.soft(hot).toEqual(expected);
    expect(seen.length).toBeGreaterThan(0);
    for (const row of seen) expect.soft(row).toEqual(expected);
   });

   it(`${transport}: reconciles transaction 9 and its details delta after a high snapshot at 10`, async () => {
    const { client, socket, open, snapshot, owner, cold, observe, delivered } = await fixture(paged);
    await snapshot('anchor', 1, []);
    // SQL snapshots include every declared column. name is NULL at 2 and changes at 9.
    await snapshot('details', 2, [{ _id: 'a', status: 'new', name: null }]);
    await snapshot('high', 10, [latest]);
    expect(client.localReplica.cursor()).toEqual(cursor(1));
    // Event 2 is covered by both task snapshots. Revisions 3..8 affect neither
    // table. Only ordered watermarks are visible. Feed consumption starts after high.
    for (let revision = 2; revision <= 8; revision++) socket.receive({ type: 'replica.watermark', revision });
    await vi.waitFor(() => expect(client.localReplica.cursor()).toEqual(cursor(8)));
    const seen = observe();
    const projection = { _id: 'a', status: 'new', name: 'Renamed' };
    const hashes = await replicaRowsHashes([projection], '_id');
    const digest = replicaAggregateDigest(hashes);
    const expected = { ...latest, name: 'Renamed' };
    // high skips event 9 because its server cursor is 10. details advances 2->9.
    // Its larger ID makes its projection last in normalize_entity_changes.
    socket.receive({ type: 'replica.transaction', cursor: cursor(9), changes: [
      { entity: 'tasks', id: 'a', operation: 'update', oldValue: { _id: 'a', status: 'new', name: null }, newValue: projection, changedColumns: ['name'] },
    ] });
    await vi.waitFor(() => expect(client.localReplica.cursor()).toEqual(cursor(9)));
    // Collect transaction-only publication before the collection delta arrives.
    const transactionHot = client.localReplica.entity('tasks', 'a');
    const transactionDurable = await owner.storage.loadEntityRows(scope, 'tasks', ['a']);
    const transactionSeen = seen.splice(0);
    const before = delivered.details!.filter(message => message.type === 'replica.ready').length;
    socket.receive({ type: 'replica.delta', id: open('details').id, path: 'details',
      cursor: cursor(9), upserts: [projection], deleted: [], originCommandIds: [], digest, truncated: false });
    socket.receive({ type: 'replica.ready', id: open('details').id, path: 'details', cursor: cursor(9), digest, mode: 'eager', truncated: false });
    socket.receive({ type: 'replica.watermark', revision: 9 });
    await vi.waitFor(() => expect(delivered.details!.filter(message => message.type === 'replica.ready')).toHaveLength(before + 1));
    const hot = client.localReplica.entity('tasks', 'a');
    const durable = await owner.storage.loadEntityRows(scope, 'tasks', ['a']);
    const restored = await cold();
    expect.soft(transactionDurable).toEqual([{ id: 'a', row: expected }]);
    expect.soft(transactionHot).toEqual(expected);
    expect(transactionSeen.length).toBeGreaterThan(0);
    for (const row of transactionSeen) expect.soft(row).toEqual(expected);
    expect.soft(durable).toEqual([{ id: 'a', row: expected }]);
    expect.soft(restored.entity('tasks', 'a')).toEqual(expected);
    expect.soft(hot).toEqual(expected);
    expect(seen.length).toBeGreaterThan(0);
    for (const row of seen) expect.soft(row).toEqual(expected);
   });

   it(`${transport}: filters stale same-subscription frames before storage`, async () => {
    const { client, socket, open, snapshot, owner } = await fixture(paged);
    await snapshot('anchor', 1, []);
    await snapshot('details', 2, [{ _id: 'a', status: 'new', name: null }]);
    await snapshot('high', 10, [latest]);
    const before = await owner.storage.load(scope);
    const high = client.localReplica.listWindows().find(window => window.entity === 'tasks' && window.cursor?.revision === 10)!;
    const seen: Array<ReplicaRow | undefined> = [];
    disposals.push(client.localReplica.subscribe(() => seen.push(client.localReplica.entity('tasks', 'a')), { entity: 'tasks', ids: ['a'] }));
    const stale = { id: open('high').id, path: 'high', cursor: cursor(9) };
    // Deliberate invalid late frame controls, not claimed runtime emissions.
    socket.receive({ ...stale, type: 'replica.snapshot', result: [{ _id: 'a', status: 'stale' }], key: '_id' });
    socket.receive({ ...stale, type: 'replica.delta', upserts: [{ _id: 'a', status: 'stale' }], deleted: [] });
    // A valid watermark seals the dispatch queue without advancing beyond high.
    socket.receive({ type: 'replica.watermark', revision: 8 });
    await vi.waitFor(() => expect(client.localReplica.cursor()).toEqual(cursor(8)));
    expect(client.localReplica.entity('tasks', 'a')).toEqual({ ...latest, name: null });
    expect(client.localReplica.getWindow(high.signature)).toEqual(high);
    expect(await owner.storage.loadEntityRows(scope, 'tasks', ['a'])).toEqual([{ id: 'a', row: { ...latest, name: null } }]);
    expect((await owner.storage.load(scope))?.entities).toEqual(before?.entities);
    for (const row of seen) expect(row).toEqual({ ...latest, name: null });
   });
  }
 });
}
