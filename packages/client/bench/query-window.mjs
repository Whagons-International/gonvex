// Run after building the client: node --expose-gc bench/query-window.mjs
// An optional module URL allows comparison with a separately built baseline.
import { performance } from 'node:perf_hooks';
const moduleURL = process.argv[2] ?? new URL('../dist/query-expression.js', import.meta.url).href;
const { runOfflineLiveQuery } = await import(moduleURL);
const rowCount = 100_000;
const rows = Array.from({ length: rowCount }, (_, i) => Object.freeze({ id: String(i), statusId: String(i), priority: i % 2 ? 'urgent' : 'normal' }));
const referenceRows = Array.from({ length: rowCount }, (_, i) => Object.freeze({ order: rowCount - i }));
const window = { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 50, maxLimit: 50, count: 'exact' };
const referencePlan = {
  table: 'tasks', key: 'id', window,
  index: { table: 'taskGrid', key: 'id', columns: [], sortColumns: { statusId: ['statusOrder'] },
    referenceFields: { statusOrder: { table: 'statuses', foreignKey: 'statusId', column: 'order' } } },
  sort: { allowedColumns: ['statusId'], defaultColumn: 'statusId', defaultDirection: 'asc' },
};
const choicePlan = { table: 'tasks', key: 'id', window,
  filters: { argument: 'filters', allowedColumns: ['priority'], allowedOperators: ['oneOf'] } };

function measure(name, execute) {
  for (let i = 0; i < 3; i++) execute(() => {});
  globalThis.gc?.();
  const initialHeap = process.memoryUsage().heapUsed;
  let peakHeap = initialHeap;
  const sample = () => { peakHeap = Math.max(peakHeap, process.memoryUsage().heapUsed); };
  const started = performance.now();
  const result = execute(sample);
  sample();
  const elapsedMs = performance.now() - started;
  if (result.rows.length !== 50 || result.rows[0] !== rows[name === 'oneOf' ? 1 : rowCount - 1]) throw new Error('Incorrect query window');
  globalThis.gc?.();
  return { name, rowCount, resultRows: result.rows.length, total: result.total, elapsedMs,
    sampledHeapIncreaseBytes: peakHeap - initialHeap,
    retainedHeapIncreaseAfterGCBytes: process.memoryUsage().heapUsed - initialHeap,
    forcedGC: typeof globalThis.gc === 'function' };
}

const reference = measure('reference-sort', sample => {
  let visited = 0;
  return runOfflineLiveQuery(rows, referencePlan, {}, 'complete', (_table, id) => {
    if (++visited % 1024 === 0) sample();
    return referenceRows[Number(id)];
  });
});
const choices = measure('oneOf', () => runOfflineLiveQuery(rows, choicePlan,
  { filters: [{ column: 'priority', operator: 'oneOf', value: '["urgent"]' }] }, 'complete'));
console.log(JSON.stringify({ runtime: process.version, note: 'Sampled Node heap high-water marks measure temporary working memory; after-GC deltas are noisy and do not prove browser retention.', results: [reference, choices] }, null, 2));
