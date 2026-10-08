// Run after pnpm build: nice -n 10 node --expose-gc bench/replica-residency.mjs
const { LocalReplica } = await import(process.env.REPLICA_BENCH_MODULE ?? '../dist/local-replica.js');
import { performance } from 'node:perf_hooks';
if (!global.gc) throw new Error('Run with --expose-gc');
const gc = () => { for (let i = 0; i < 3; i++) global.gc(); return process.memoryUsage().heapUsed / 2 ** 20; };
const row = i => Object.fromEntries([['id', String(i)], ...Array.from({ length: 49 }, (_, c) => [`column${c}`, c % 3 === 0 ? i + c : `value-${i}-${c}`])]);
// Indexed adapter capabilities enable the residency limiter; persistence itself
// is excluded from the hot-path measurement. Cold-start below uses real IDB.
const storage = { load: async () => undefined, applyTransaction: async () => {}, replaceWindow: async () => {}, withReadView: async (_s, run) => run({}), loadWindowRows: async () => undefined };
const output = { node: process.version, samples: [] };
for (const count of [10000, 50000]) {
  const empty = gc();
  const replica = new LocalReplica(storage, { maxResidentRows: Number.MAX_SAFE_INTEGER, maxResidentBytes: 200 * 2 ** 20 });
  let rows = Array.from({ length: count }, (_, i) => row(i));
  await replica.materializeWindow({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', rows, completeness: 'complete', source: 'server' });
  await replica.materializeWindow({ signature: 'visible', entity: 'tasks', key: 'id', rows: rows.slice(0, 250), completeness: 'partial', source: 'server' });
  rows = null;
  const release = replica.retainWindow('visible');
  const visible = replica.watchRows('visible', new Map());
  const reads = replica.entityBatch('tasks', visible.map(r => r.id));
  const heapMB = gc() - empty;
  for (let e = 1; e < 20; e++) await replica.materializeWindow({ signature: `e${e}`, entity: `e${e}`, key: 'id', rows: [row(0)], completeness: 'complete', source: 'server' });
  let wakes = 0;
  const unsubscribes = Array.from({ length: 1000 }, (_, i) => replica.subscribe(() => { wakes++; }, { entity: i % 20 === 0 ? 'tasks' : `e${i % 20}`, ids: ['0'] }));
  let revision = 0;
  const upsert = () => replica.applyTransaction({ cursor: { epoch: 'bench', revision: ++revision }, changes: [{ entity: 'tasks', id: '0', operation: 'update', newValue: { column0: revision } }] });
  for (let i = 0; i < 30; i++) await upsert();
  wakes = 0;
  const start = performance.now();
  for (let i = 0; i < 200; i++) await upsert();
  const upsertMs = (performance.now() - start) / 200;
  let checksum = 0;
  for (let i = 0; i < 1000; i++) checksum += Number(replica.entity('tasks', String(i % count)).column0);
  const readStart = performance.now();
  for (let i = 0; i < 100000; i++) checksum += Number(replica.entity('tasks', String(i % count)).column0);
  const readNs = (performance.now() - readStart) * 1e6 / 100000;
  output.samples.push({ count, heapMB, upsertMs, wakesPerUpsert: wakes / 200, entityReadNs: readNs, checksum, visible: visible.length, reads: reads.length });
  unsubscribes.forEach(fn => fn()); release(); replica.dispose();
}
// Measure the adapter's actual working-set decoding and activation cold path.
// --hot-only skips durable fixture creation for quick reruns.
if (!process.argv.includes('--hot-only')) {
const { IDBFactory, IDBKeyRange } = await import('fake-indexeddb');
const { Dexie } = await import('dexie');
const { IndexedDBLocalReplicaStorage } = await import('../dist/indexeddb-replica.js');
Dexie.dependencies.indexedDB = new IDBFactory(); Dexie.dependencies.IDBKeyRange = IDBKeyRange;
for (const count of [10000, 50000]) {
  const adapter = new IndexedDBLocalReplicaStorage(`bench-${count}`);
  let entities = { tasks: Object.fromEntries(Array.from({ length: count }, (_, i) => [String(i), row(i)])) };
  await adapter.replaceSnapshot({ entities, liveQueries: {} }, 'bench'); entities = null; gc();
  let start = performance.now();
  let working = await adapter.loadWorkingSet('bench', { maxRows: Number.MAX_SAFE_INTEGER, maxBytes: 200 * 2 ** 20 });
  const workingSetMs = performance.now() - start;
  working = null; gc();
  const replica = new LocalReplica(adapter, { maxResidentRows: Number.MAX_SAFE_INTEGER, maxResidentBytes: 200 * 2 ** 20 });
  start = performance.now(); await replica.activateScope('bench');
  output.samples.find(s => s.count === count).cold = { workingSetMs, activateMs: performance.now() - start };
  replica.dispose(); adapter.close();
}
}
console.log(JSON.stringify(output, null, 2));
