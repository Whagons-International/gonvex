// The Expo/Hermes fixture and the Node SQLite process runner execute this same
// schedule with the real SDK adapters. Neither supplies a replacement SQL store.
export const historicalPhases = {
  'historical-seed': 'historical-seeded',
  'historical-replica-held': 'historical-replica-write-held',
  'historical-journal-held': 'historical-journal-write-held',
  'historical-recover': 'historical-recovered',
  'historical-reopen': 'historical-reopened',
};
const signature = 'tasks.all\0{"parentId":"p"}';
const variants = ['unscoped', 'scoped'];
const entries = () => ['pending', 'inflight', 'committed', 'failed', 'rejected', 'committed'].map((state, i) => ({
  id: i + 1, scope: i === 5 ? 'bob' : 'alice', path: 'tasks.write', receiptPath: 'tasks.oldWrite',
  args: { key: `historical-${i + 1}`, text: 'preserve\0suffix' }, idempotencyKey: `historical-${i + 1}`,
  entityKeys: ['tasks:a'], patches: [], createdAt: 100 + i, attempts: 2, nextAttemptAt: 200 + i,
  state, knownUnsent: true, sentAt: 90 + i,
  ...(i === 2 ? { awaitingReplica: true } : {}),
  ...(['failed', 'rejected'].includes(state) ? { lastError: 'retained rejection', lastErrorClass: state === 'failed' ? 'transient' : 'rejected' } : {}),
}));

async function raw(database) {
  const schema = await database.getAllAsync("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name LIKE '_gonvex_replica_%' ORDER BY type,name");
  const tables = {};
  for (const name of ['entities', 'queries', 'meta']) {
    const table = '_gonvex_replica_' + name;
    const columns = await database.getAllAsync(`PRAGMA table_info(${table})`);
    if (!columns.length) continue;
    // hex() witnesses all bytes even on a driver with C-string truncation.
    const projection = columns.map(column => `hex(${column.name}) AS ${column.name}`).join(',');
    tables[name] = await database.getAllAsync(`SELECT ${projection} FROM ${table} ORDER BY ${columns.map(column => column.name).join(',')}`);
  }
  return { schema, tables, appVersion: await database.getFirstAsync('PRAGMA user_version') };
}

async function seed(database, variant) {
  const scoped = variant === 'scoped';
  await database.execAsync(`PRAGMA user_version=700;
    CREATE TABLE _historical_legacy(id INTEGER PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE _historical_expected(id INTEGER PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE _gonvex_replica_entities(${scoped ? 'scope TEXT,' : ''}entity TEXT,id TEXT,value TEXT,PRIMARY KEY(${scoped ? 'scope,' : ''}entity,id));
    CREATE TABLE _gonvex_replica_queries(${scoped ? 'scope TEXT,' : ''}signature TEXT,value TEXT,PRIMARY KEY(${scoped ? 'scope,' : ''}signature));
    CREATE TABLE _gonvex_replica_meta(${scoped ? 'scope TEXT,' : ''}key TEXT,value TEXT,PRIMARY KEY(${scoped ? 'scope,' : ''}key));`);
  for (const owner of scoped ? ['alice', 'bob'] : ['default']) {
    const id = owner === 'bob' ? 'b' : 'a';
    const row = { id, parentId: 'p', title: owner + '\0private' };
    const cursor = { epoch: 'historical-' + owner, revision: 7 };
    const window = { signature, kind: 'replica', entity: 'tasks', key: 'id', ids: [id], cursor, completeness: 'complete', source: 'server' };
    const prefix = scoped ? [owner] : [];
    for (const [table, values] of [
      ['entities', [...prefix, 'tasks', id, JSON.stringify(row)]],
      ['entities', [...prefix, 'tasks', 'gone', 'null']],
      ['queries', [...prefix, signature, JSON.stringify(window)]],
      ['meta', [...prefix, 'cursor', JSON.stringify(cursor)]],
    ]) {
      // Seed intact historical bytes even when the native TEXT binder truncates
      // at NUL. Corrupt prefix-only keys are a separate refusal control.
      const key = table === 'queries' ? prefix.length : -1;
      await database.runAsync(`INSERT INTO _gonvex_replica_${table} VALUES(${values.map((_, i) => i === key ? "json_extract(?, '$')" : '?').join(',')})`,
        ...values.map((value, i) => i === key ? JSON.stringify(value) : value));
    }
  }
  await database.runAsync('INSERT INTO _historical_legacy VALUES(1,?)', JSON.stringify(entries()));
  await database.runAsync('INSERT INTO _historical_expected VALUES(1,?)', JSON.stringify(await raw(database)));
  await database.runAsync('INSERT INTO _historical_expected VALUES(2,?)', JSON.stringify(entries()));
}

const legacyStorage = database => ({
  async getItem() { return (await database.getFirstAsync('SELECT value FROM _historical_legacy WHERE id=1'))?.value ?? null; },
  async removeItem() { await database.runAsync('DELETE FROM _historical_legacy WHERE id=1'); },
});

async function views(database, Replica, Outbox, Queue, variant) {
  const storage = new Replica(database), store = new Outbox(database);
  const owners = {};
  for (const owner of ['default', 'alice', 'bob']) {
    const snapshot = await storage.load(owner);
    const selected = await storage.withReadView(owner, { tasks: { key: 'id', complete: true } },
      view => view.select({ table: 'tasks', where: { column: 'parentId', op: 'eq', value: 'p' } }));
    owners[owner] = { snapshot: snapshot ?? null, selected };
  }
  const queue = new Queue(store);
  let restored;
  try { restored = { alice: await queue.loadAll('alice'), bob: await queue.loadAll('bob') }; }
  finally { queue.dispose?.(); }
  return { variant, owners, entries: await store.load(), restored,
    windowBindings: await windowBindings(database, Replica),
    tombstones: await database.getAllAsync("SELECT scope,entity,id,value FROM _gonvex_replica_entities WHERE value='null' ORDER BY scope,id"),
    legacy: (await database.getFirstAsync('SELECT value FROM _historical_expected WHERE id=2')).value,
    appVersion: await database.getFirstAsync('PRAGMA user_version'),
    checkpoint: await database.getAllAsync('SELECT id,version FROM _gonvex_replica_schema'),
    integrity: await database.getFirstAsync('PRAGMA integrity_check'),
    sync: await database.getFirstAsync('PRAGMA synchronous'), journal: await database.getFirstAsync('PRAGMA journal_mode') };
}

// Exercise production key writers and lookups on both actual native drivers.
// The oracle is in contract.py, separate from this adapter-driving schedule.
async function windowBindings(database, Replica) {
  const storage = new Replica(database), scope = 'window-binding-proof';
  const signatures = ['tasks\0a', 'tasks\0b'];
  const rows = [{ id: 'bind-a' }, { id: 'bind-b' }];
  const cursor = { epoch: 'bindings', revision: 5 };
  const windows = signatures.map((signature, i) => ({ signature, kind: 'replica', entity: 'tasks', key: 'id',
    ids: [rows[i].id], cursor, completeness: 'complete', source: 'server' }));
  const snapshot = { entities: { tasks: Object.fromEntries(rows.map(row => [row.id, row])) },
    liveQueries: Object.fromEntries(windows.map(window => [window.signature, window])), cursor };
  const modes = [];
  for (const mode of ['replaceWindow', 'replaceSnapshot', 'applyTransaction', 'advanceWatermark']) {
    await storage.clear(scope);
    if (mode === 'replaceWindow') for (const window of windows) await storage.replaceWindow(window, snapshot, scope);
    else if (mode === 'replaceSnapshot') await storage.replaceSnapshot(snapshot, scope);
    else {
      await storage.applyTransaction({ cursor, changes: rows.map(row => ({ entity: 'tasks', id: row.id, operation: 'insert', newValue: row })),
        ...(mode === 'applyTransaction' ? { memberships: windows } : {}) }, snapshot, scope);
      if (mode === 'advanceWatermark') await storage.advanceWatermark(windows, cursor, scope);
    }
    const full = await storage.load(scope), working = await storage.loadWorkingSet(scope, { maxRows: 100, maxBytes: 100_000 });
    const cold = new Replica(database), points = [];
    for (const signature of signatures) points.push(await cold.loadWindowRows(scope, signature));
    modes.push({ mode, full: Object.keys(full.liveQueries).sort(), working: Object.keys(working.liveQueries).sort(), points,
      indexed: await database.getAllAsync('SELECT hex(signature) AS bytes FROM _gonvex_replica_queries WHERE scope=? ORDER BY signature', scope) });
  }
  await storage.applyTransaction({ cursor: { epoch: 'bindings', revision: 6 },
    changes: [{ entity: 'tasks', id: 'bind-a', operation: 'delete' }] }, snapshot, scope);
  const beforeRemove = await storage.load(scope);
  await storage.removeWindow(signatures[0], beforeRemove, scope);
  const afterRemove = await storage.load(scope);
  await storage.clear(scope);
  return { modes, beforeRemove, afterRemove };
}

export async function historicalPhase(config, emit, { SQLite, Replica, Outbox, Queue, migrateLegacyOutbox }) {
  if (!Object.hasOwn(historicalPhases, config.phase)) throw Error('unknown historical phase');
  const cases = [];
  for (const variant of variants) {
    const name = `gonvex-historical-${config.runId}-${variant}.db`;
    if (config.phase === 'historical-seed') await SQLite.deleteDatabaseAsync(name).catch(() => {});
    const database = await SQLite.openDatabaseAsync(name);
    await database.execAsync('PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;');
    if (config.phase === 'historical-seed') {
      await seed(database, variant);
      cases.push({ variant, raw: await raw(database), legacy: (await database.getFirstAsync('SELECT value FROM _historical_legacy WHERE id=1')).value,
        sync: await database.getFirstAsync('PRAGMA synchronous'), journal: await database.getFirstAsync('PRAGMA journal_mode') });
    } else if (config.phase === 'historical-replica-held') {
      if (variant !== 'unscoped') { await database.closeAsync(); continue; }
      const execute = database.execAsync.bind(database);
      database.execAsync = async sql => {
        await execute(sql);
        if (sql.includes('INSERT INTO _gonvex_replica_entities (scope, entity, id, value)')) {
          await emit({ type: historicalPhases[config.phase], variant, successfulEntityStatement: true,
            original: JSON.parse((await database.getFirstAsync('SELECT value FROM _historical_expected WHERE id=1')).value),
            migratedRowsInsideTransaction: await database.getAllAsync('SELECT scope,entity,id,value FROM _gonvex_replica_entities'),
            checkpointInsideTransaction: await database.getAllAsync('SELECT id,version FROM _gonvex_replica_schema'),
            sync: await database.getFirstAsync('PRAGMA synchronous'), journal: await database.getFirstAsync('PRAGMA journal_mode') });
          await new Promise(() => {});
        }
      };
      await new Replica(database).load();
      throw Error('historical replica migration did not reach its native write boundary');
    } else if (config.phase === 'historical-journal-held') {
      if (variant !== 'unscoped') { await database.closeAsync(); continue; }
      const original = JSON.parse((await database.getFirstAsync('SELECT value FROM _historical_expected WHERE id=1')).value);
      const beforeMigration = await raw(database);
      await new Replica(database).load();
      const execute = database.runAsync.bind(database);
      database.runAsync = async (sql, ...params) => {
        const result = await execute(sql, ...params);
        if (sql.includes('INTO _gonvex_outbox ') && params[0] === 3) {
          await emit({ type: historicalPhases[config.phase], variant, successfulAcceptedStatement: true, original, beforeMigration,
            importedRowsInsideTransaction: await database.getAllAsync('SELECT id,state,value FROM _gonvex_outbox ORDER BY id'),
            legacy: (await database.getFirstAsync('SELECT value FROM _historical_legacy WHERE id=1')).value,
            sync: await database.getFirstAsync('PRAGMA synchronous'), journal: await database.getFirstAsync('PRAGMA journal_mode') });
          await new Promise(() => {});
        }
        return result;
      };
      await migrateLegacyOutbox({ store: new Outbox(database), storage: legacyStorage(database), key: 'history' });
      throw Error('historical journal import did not reach its native acceptance boundary');
    } else {
      const store = new Outbox(database);
      const beforeImport = await store.load();
      const sourceBefore = await legacyStorage(database).getItem();
      const migrated = await migrateLegacyOutbox({ store, storage: legacyStorage(database), key: 'history' });
      const sourceAfter = await legacyStorage(database).getItem();
      const imported = await store.importLegacy((await database.getFirstAsync('SELECT value FROM _historical_expected WHERE id=2')).value);
      const result = await views(database, Replica, Outbox, Queue, variant);
      cases.push({ ...result, beforeImport, migrated, imported, sourceBefore, sourceAfter, nextId: await store.allocateId() });
    }
    await database.closeAsync();
    if (config.phase === 'historical-reopen') await SQLite.deleteDatabaseAsync(name);
  }
  await emit({ type: historicalPhases[config.phase], cases });
}
