import { Dexie } from 'dexie';
import { browserUpgradeStorage } from '../src/browser-upgrades.ts';
import { IndexedDBLocalReplicaStorage } from '../src/indexeddb-replica.ts';
import { DexieReducerOutbox, createReducerOutbox } from '../src/outbox.ts';
import { createKvOutboxStore, kvReducerOutboxTable } from '../src/kv-stores.ts';

const entry = { id: 1, scope: 'alice', path: 'tasks.save', args: { title: 'accepted' },
  idempotencyKey: 'stable-command', entityKeys: ['tasks:a'], createdAt: 1, attempts: 1, nextAttemptAt: 0, state: 'committed' };
const options = (version, migrations = []) => ({ replicaName: 'upgrade-replica', outboxName: 'upgrade-queue',
  initialVersion: 1, contract: { version, offlineMaxAgeMs: null }, migrations });

window.upgradeRecovery = {
  async seed() {
    const old = browserUpgradeStorage(options(1)); await old.ready;
    await old.storage.replaceSnapshot({ entities: { tasks: { a: { title: 'durable' } } }, liveQueries: {}, cursor: { epoch: 'old', revision: 8 } }, 'alice');
    await old.store.put(entry); old.close();
  },
  async pauseUpgrade() {
    const replace = IndexedDBLocalReplicaStorage.prototype.replaceSnapshot;
    IndexedDBLocalReplicaStorage.prototype.replaceSnapshot = async function(...args) {
      await replace.apply(this, args);
      window.upgradeWriteObserved = true;
      await new Promise(() => {});
    };
    const next = browserUpgradeStorage(options(2, [{ from: 1, to: 2,
      replica(snapshot) { snapshot.entities.tasks.a.name = snapshot.entities.tasks.a.title; delete snapshot.entities.tasks.a.title; return snapshot; },
      intent(intent) {
        window.upgradeTransformCount = (window.upgradeTransformCount ?? 0) + 1;
        return { path: 'tasks.v2', args: { name: intent.args.title } };
      },
    }]));
    await next.ready;
    throw Error('upgrade unexpectedly completed before browser crash');
  },
  async resumeUpgrade() {
    const next = browserUpgradeStorage(options(2, [{ from: 1, to: 2, intent() { throw Error('staged migration ran twice'); } }]));
    await next.ready;
    const result = { entries: await next.store.load(), snapshot: await next.storage.load('alice') };
    next.close(); return result;
  },
  async legacyOwnerless() {
    const name = 'legacy-ownerless'; const raw = new Dexie(name);
    raw.version(1).stores({ entries: '++id, state, nextAttemptAt' });
    const { scope, ...unowned } = entry;
    await raw.table('entries').put(unowned); raw.close();
    const outbox = new DexieReducerOutbox({ databaseName: name });
    let error;
    try { await outbox.loadAll('new-owner'); } catch (cause) { error = String(cause); }
    const inspect = new Dexie(name); await inspect.open();
    const result = { error, version: inspect.verno, rows: await inspect.table('entries').toArray() };
    inspect.close(); outbox.database?.close(); return result;
  },
  async legacyScoped(damage = false) {
    const raw = new Dexie('legacy-scoped');
    raw.version(1).stores({ entries: '++id, state, nextAttemptAt' });
    const original = ['pending', 'inflight', 'committed', 'failed', 'rejected'].map((state, index) => ({
      ...entry, id: [11, 23, 37, 41, 59][index], state,
      idempotencyKey: 'historical-' + state, args: { title: state },
      createdAt: index + 1, attempts: index, nextAttemptAt: index + 10,
    }));
    original.push({ ...entry, id: 88, scope: 'bob', idempotencyKey: original[0].idempotencyKey });
    await raw.table('entries').bulkPut(original);
    if (damage) await raw.table('entries').put({ ...original[2], idempotencyKey: 'damaged-accepted-key' });
    raw.close();
    return { original, ...await this.inspectLegacyScoped() };
  },
  async inspectLegacyScoped() {
    const outbox = new DexieReducerOutbox({ databaseName: 'legacy-scoped' });
    try {
      const alice = await outbox.loadAll('alice'), bob = await outbox.loadAll('bob');
      const raw = new Dexie('legacy-scoped'); await raw.open();
      try { return { alice, bob, version: raw.verno, rows: await raw.table('entries').toArray() }; }
      finally { raw.close(); }
    } finally { outbox.database?.close(); }
  },
  async corruptJournal() {
    const database = new Dexie('native-kv'); database.version(1).stores({ rows: '&key' });
    const kv = {
      async get(table, key) { return (await database.table('rows').get(table + ':' + key))?.value; },
      async set(table, key, value) { await database.table('rows').put({ key: table + ':' + key, value }); },
      async delete(table, key) { await database.table('rows').delete(table + ':' + key); },
      async list(table) { return (await database.table('rows').toArray()).filter(row => row.key.startsWith(table + ':')).map(row => ({ key: row.key.slice(table.length + 1), value: row.value })); },
      async clear() { throw Error('corrupt recovery must not clear stored history'); },
    };
    await kv.set(kvReducerOutboxTable, '1', '{invalid accepted bytes');
    const outbox = createReducerOutbox({ store: createKvOutboxStore(kv) });
    const errors = [];
    for (const run of [() => outbox.loadAll('alice'), () => outbox.enqueue({ scope: 'alice', path: 'tasks.save', args: {} })]) {
      try { await run(); errors.push(null); } catch (error) { errors.push(String(error)); }
    }
    const result = { errors, raw: await kv.get(kvReducerOutboxTable, '1'), count: await database.table('rows').count() };
    outbox.dispose?.(); database.close(); return result;
  },
};
