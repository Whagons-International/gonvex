// Run after building workspace dependencies: nice -n 10 node --expose-gc packages/client/bench/replica-idb.mjs
// fake-indexeddb measures JS work and retained heap, not browser disk/native IDB cost.
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb';
import { Dexie } from 'dexie';
const clientDist = process.env.CLIENT_DIST ?? new URL('../dist/', import.meta.url).href;
const { IndexedDBLocalReplicaStorage } = await import(new URL('indexeddb-replica.js', clientDist));
const { LocalReplica } = await import(new URL('local-replica.js', clientDist));
Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
Dexie.dependencies.indexedDB = indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
const heap = () => { globalThis.gc(); return process.memoryUsage().heapUsed / 1048576; };
const measure = async run => { const start = performance.now(); const value = await run(); return { ms: performance.now() - start, value }; };
for (const count of (process.env.ROWS ?? '10000,50000').split(',').map(Number)) {
  const name = `bench-${count}-${Date.now()}`;
  let storage = new IndexedDBLocalReplicaStorage(name);
  const coverage = { tasks: { key: '_id', complete: true } };
  const query = () => storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'workspaceId', op: 'eq', value: 'w42' } }));
  // Observe the benchmark's actual reducer read on an empty replica before
  // ingesting rows. This compares steady-state storage with a known workload,
  // without hiding backfill cost inside a snapshot or equality measurement.
  if (!process.env.LEARN_AFTER_SNAPSHOT && !process.env.NO_READS) {
    await query();
    await storage.waitForIndexBackfills?.();
  }
  const startHeap = heap();
  let tasks = Object.fromEntries(Array.from({ length: count }, (_, i) => {
    const row = { _id: `t${String(i).padStart(6, '0')}`, statusId: `s${i % 20}`, workspaceId: `w${i % 100}` };
    for (let column = 3; column < 50; column++) row[`field${column}`] = column % 3 === 0 ? i + column : column % 3 === 1 ? `value-${column}-${i % 100}` : i % 2 === 0;
    return [row._id, row];
  }));
  const write = await measure(() => storage.replaceSnapshot({ entities: { tasks }, liveQueries: {} }, 'tenant'));
  tasks = undefined;
  const first = process.env.NO_READS ? { ms: null, value: null } : await measure(query);
  if (first.value && first.value.rows.length !== Math.max(0, Math.ceil((count - 42) / 100))) throw new Error('Equality lookup lost rows');
  const backfillStart = performance.now();
  await storage.waitForIndexBackfills?.();
  const backfillAfterReadMs = performance.now() - backfillStart;
  const times = [];
  for (let i = 0; !process.env.NO_READS && i < 5; i++) times.push((await measure(query)).ms);
  storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
  const cold = await measure(() => storage.loadWorkingSet('tenant', { maxRows: Number.MAX_SAFE_INTEGER, maxBytes: 200 * 1048576 }));
  if (Object.keys(cold.value?.entities.tasks ?? {}).length !== count) throw new Error('Working set did not retain every task');
  // Model 1000 observers sharing the complete task replica.
  const replica = new LocalReplica(storage, { maxResidentRows: Number.MAX_SAFE_INTEGER, maxResidentBytes: 200 * 1048576 });
  await replica.activateScope('tenant');
  const observers = Array.from({ length: 1000 }, () => replica.subscribe(() => {}));
  cold.value = undefined; first.value = undefined;
  const retainedHeap = heap() - startHeap;
  const db = new Dexie(name); await db.open();
  const entries = await db.table('entities').toCollection().primaryKeys();
  const indexCount = await new Promise((resolve, reject) => { const tx = db.backendDB().transaction('entities'); const req = tx.objectStore('entities').index('lookupKeys').count(); req.onsuccess = () => resolve(req.result); req.onerror = () => reject(req.error); });
  console.log(JSON.stringify({ rows: count, columns: 50, workloadKnownBeforeSnapshot: !process.env.LEARN_AFTER_SNAPSHOT && !process.env.NO_READS, observers: observers.length, writeMs: write.ms, firstEqualityMs: first.ms, backfillAfterReadMs, equalityMedianMs: times.sort((a,b)=>a-b)[2] ?? null, coldOpenLoadMs: cold.ms, lookupEntries: indexCount, retainedHeapMiB: retainedHeap, entityRecords: entries.length }));
  observers.forEach(stop => stop()); storage.close(); db.close(); await Dexie.delete(name);
}
