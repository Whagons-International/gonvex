import { IndexedDBLocalReplicaStorage } from '../../dist/indexeddb-replica.js';
import { RowWindowBuffer } from '../../dist/row-window-buffer.js';
import { runOfflineLiveQuery } from '../../dist/query-expression.js';
import { seedFixture } from './fixture.mjs';

let storage;
const rows = new RowWindowBuffer(200);
const scope = 'synthetic-memory-fixture';
const coverage = { tasks: { key: '_id', complete: true } };
let ids = [], metadata = [], positions = new Map(), workspace = -1, offset = 0, metadataScans = 0;
const plan = { table: 'tasks', key: '_id',
  sort: { allowedColumns: ['rank'], defaultColumn: 'rank', defaultDirection: 'desc' },
  window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 200, maxLimit: 200, count: 'exact' } };
const select = read => storage.withReadView(scope, coverage, view => view.select({ table: 'tasks', ...read }));

window.seed = async fixtureHash => {
  // Isolate changed fixture data instead of clearing thousands of indexed rows.
  // Runtime/query edits reuse the same fixture because only fixture.mjs is hashed.
  storage = new IndexedDBLocalReplicaStorage(`memory-smoke-${fixtureHash}`);
  return seedFixture(storage, scope, fixtureHash);
};

async function load(start, count) {
  const wanted = ids.slice(start, start + count);
  const result = await select({ where: { column: '_id', op: 'in', values: wanted } });
  if (!result.complete || result.rows.length !== wanted.length) throw new Error('Missing disk-backed window rows');
  for (const row of result.rows) rows.set(positions.get(row._id), row);
}
window.navigate = async next => {
  workspace = next; offset = 0; rows.clear();
  const result = await select({ columns: ['_id', 'rank'], where: { column: 'workspaceId', op: 'eq', value: next }, orderBy: [{ column: 'rank', direction: 'desc' }] });
  if (!result.complete || result.rows.length !== 2000 || result.rows.some(row => Object.keys(row).length !== 2 || !Number.isInteger(row.rank))) throw new Error('Incorrect compact membership query');
  metadataScans++;
  metadata = result.rows;
  ids = result.rows.map(row => row._id);
  positions = new Map();
  for (let index = 0; index < ids.length; index++) positions.set(ids[index], index);
  await load(0, 200);
};
window.scroll = async () => {
  if (offset + 220 >= ids.length) { offset = 0; rows.clear(); await load(0, 200); }
  else { offset += 20; rows.retainRange(offset, 200); await load(offset + 180, 20); }
  const query = runOfflineLiveQuery(metadata, plan, { offset, limit: 200 }, 'complete');
  if (!query.supported || query.completeness !== 'complete' || query.total !== 2000 || query.rows.length !== 200
    || query.rows.some((row, index) => row !== metadata[offset + index] || row._id !== ids[offset + index])) throw new Error('Incorrect local sorted query window');
  for (let index = offset; index < offset + 200; index++) {
    const row = rows.get(index);
    if (row?._id !== ids[index] || row.workspaceId !== workspace || !row.description.startsWith(`Distinct reference ${row.rank}.`) || row.rank < 2000 && row.description.length < 16_000) throw new Error('Stale or missing ring-buffer row');
  }
  if (rows.size !== 200) throw new Error('Task residency exceeded its fixed 200-row window');
  return { residentRows: rows.size, metadataIds: ids.length, metadataScans, queryRows: query.rows.length };
};
