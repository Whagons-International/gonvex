import { IndexedDBLocalReplicaStorage } from '../dist/indexeddb-replica.js';
import { LocalReplica } from '../dist/local-replica.js';
import { Dexie } from 'dexie';
const measure = async run => { const start = performance.now(); const value = await run(); return { ms: performance.now() - start, value }; };
const tasksFor = count => Object.fromEntries(Array.from({ length: count }, (_, i) => {
  const row = { _id: `t${String(i).padStart(6, '0')}`, statusId: `s${i % 20}`, workspaceId: `w${i % 100}` };
  for (let column = 3; column < 50; column++) row[`field${column}`] = column % 3 === 0 ? i + column : column % 3 === 1 ? `value-${column}-${i % 100}` : i % 2 === 0;
  return [row._id, row];
}));
globalThis.runBenchmark = async counts => {
  const results = [];
  for (const count of counts) {
    const name = `native-${count}-${Date.now()}`;
    let storage = new IndexedDBLocalReplicaStorage(name);
    const coverage = { tasks: { key: '_id', complete: true } };
    const equality = { column: 'workspaceId', op: 'eq', value: 'w42' };
    const query = where => storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where }));
    if (storage.configureLookupColumns) storage.configureLookupColumns({ tasks: ['workspaceId'] });
    else await query(equality);
    const write = await measure(() => storage.replaceSnapshot({ entities: { tasks: tasksFor(count) }, liveQueries: {} }, 'tenant'));
    const expected = Math.ceil((count - 42) / 100);
    const indexed = [], scans = [];
    for (let i = 0; i < 5; i++) {
      const index = await measure(() => query(equality));
      const scan = await measure(() => query({ or: [equality] }));
      if (index.value.rows.length !== expected || scan.value.rows.length !== expected) throw Error('Incorrect equality results');
      indexed.push(index.ms); scans.push(scan.ms);
    }
    let backfillDurationMs = 0;
    const method = storage.learnColumn ? 'learnColumn' : 'learnColumns';
    const learn = storage[method].bind(storage);
    storage[method] = async (...args) => {
      const start = performance.now();
      try { return await learn(...args); }
      finally { backfillDurationMs = Math.max(backfillDurationMs, performance.now() - start); }
    };
    const start = performance.now();
    const demand = await measure(() => query({ column: 'statusId', op: 'eq', value: 's7' }));
    const statusExpected = Math.max(0, Math.ceil((count - 7) / 20));
    if (demand.value.rows.length !== statusExpected) throw Error('Incorrect demand results');
    const during = await measure(() => query({ column: 'statusId', op: 'eq', value: 's7' }));
    if (during.value.rows.length !== statusExpected) throw Error('Incorrect pending results');
    await storage.waitForIndexBackfills?.();
    const backfillMs = performance.now() - start;
    const inspection = new Dexie(name); await inspection.open();
    const ready = JSON.parse((await inspection.table('meta').get(['tenant', 'lookupColumns:tasks'])).value);
    if (!ready.includes('workspaceId') || !ready.includes('statusId')) throw Error('Backfill did not publish complete policy');
    const lookupEntries = await new Promise((resolve, reject) => {
      const request = inspection.backendDB().transaction('entities').objectStore('entities').index('lookupKeys').count();
      request.onsuccess = () => resolve(request.result); request.onerror = () => reject(request.error);
    });
    if (lookupEntries !== count * 2) throw Error('Unexpected index entries');
    inspection.close();
    storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
    const cold = await measure(() => storage.loadWorkingSet('tenant', { maxRows: Number.MAX_SAFE_INTEGER, maxBytes: 200 * 1048576 }));
    if (Object.keys(cold.value.entities.tasks).length !== count) throw Error('Incomplete working set');
    const replica = new LocalReplica(storage, { maxResidentRows: Number.MAX_SAFE_INTEGER, maxResidentBytes: 200 * 1048576 });
    await replica.activateScope('tenant');
    const observers = Array.from({ length: 1000 }, () => replica.subscribe(() => {}));
    const result = { rows: count, columns: 50, observers: observers.length, snapshotWriteMs: write.ms, indexedEqualityMs: indexed.sort((a,b)=>a-b)[2], scanEqualityMs: scans.sort((a,b)=>a-b)[2], coldOpenLoadMs: cold.ms, firstDemandReadMs: demand.ms, pendingReadMs: during.ms, backfillTotalMs: backfillMs, backfillDurationMs, lookupEntries };
    results.push(result); console.log(JSON.stringify(result));
    observers.forEach(stop => stop()); storage.close(); await Dexie.delete(name);
  }
  const name = `upgrade-${Date.now()}`;
  const legacy = new Dexie(name);
  legacy.version(5).stores({ entities: '[scope+entity+id], scope, [scope+entity], *lookupKeys, [scope+sequence]', windows: '[scope+signature], scope, [scope+sequence]', meta: '[scope+key], scope', snapshots: '&scope' });
  const rows = Object.values(tasksFor(50000)).map(row => ({ scope: 'tenant', entity: 'tasks', id: row._id, value: JSON.stringify(row), sequence: 1, lookupKeys: Object.entries(row).map(([column, value]) => ['tenant', 'tasks', column, typeof value === 'number' ? 2 : typeof value === 'boolean' ? 1 : 3, typeof value === 'boolean' ? Number(value) : value]) }));
  for (let offset = 0; offset < rows.length; offset += 256) await legacy.table('entities').bulkPut(rows.slice(offset, offset + 256));
  legacy.close();
  const upgraded = new IndexedDBLocalReplicaStorage(name);
  const upgrade = await measure(() => upgraded.listScopes());
  const db = new Dexie(name); await db.open();
  if (await db.table('entities').count() !== 50000 || (await db.table('entities').toCollection().first()).lookupKeys.length) throw Error('Upgrade lost rows or retained keys');
  upgraded.close(); db.close(); await Dexie.delete(name);
  return { workloads: results, v5ToV6Upgrade50kMs: upgrade.ms, userAgent: navigator.userAgent };
};
