import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

export const seed = 0x67f34219;
let randomState = seed;
function random() {
  randomState ^= randomState << 13; randomState ^= randomState >>> 17; randomState ^= randomState << 5;
  return randomState >>> 0;
}
const words = 'inspection equipment scheduled maintenance customer assigned warehouse delivery invoice completed followup description service location report photo technician urgent review safety'.split(' ');
function description(bytes) {
  let text = '';
  while (text.length < bytes) text += `${words[random() % words.length]} ${random().toString(36)} `;
  return text.slice(0, bytes);
}
function row(i, large = false) {
  return { id: `task-${i.toString().padStart(6, '0')}`, title: `Inspection ${i}`, status: 'open',
    description: description(large ? 16384 : 160), assignee: i % 45,
    priority: i % 5, tags: ['maintenance', `area-${i % 20}`], estimate: null,
    nested: { location: `site-${i % 9}`, checked: false }, updatedAt: 1792713600000 + i };
}
function updateFixture(name, count, large) {
  const before = Array.from({ length: count }, (_, i) => row(i, large));
  const after = before.map(value => ({ ...value, status: 'done' }));
  const cursor = { epoch: 'benchmark-epoch', revision: 2 };
  const transaction = { type: 'replica.transaction', cursor, originCommandId: 'benchmark-command',
    changes: after.map((value, i) => ({ entity: 'tasks', id: value.id, operation: 'update',
      oldValue: before[i], newValue: value, changedColumns: ['status'] })) };
  const delta = { type: 'replica.delta', id: 'tasks', path: 'tasks.all', cursor,
    upserts: after, deleted: [], originCommandIds: [], digest: 'benchmark-digest', truncated: false };
  const tail = [{ type: 'replica.ready', id: 'tasks', path: 'tasks.all', cursor, digest: 'benchmark-digest', truncated: false },
    { type: 'replica.watermark', revision: 2 }];
  const noOld = { ...transaction, changes: transaction.changes.map(({ oldValue, ...change }) => change) };
  const patches = after.map(value => ({ id: value.id, baseRevision: 1, revision: 2, set: { status: 'done' }, unset: [] }));
  return { name, before, after, variants: {
    baseline: [transaction, delta, ...tail],
    'no-old': [noOld, delta, ...tail],
    // Experimental only. Both entity and collection consumers need a base.
    patch: [{ ...transaction, changes: patches.map(patch => ({ entity: 'tasks', operation: 'patch', ...patch })) },
      { ...delta, upserts: [], patches }, ...tail],
  } };
}
const fixtures = [updateFixture('small-update', 1, false), updateFixture('large-update', 1, true),
  updateFixture('batch-100', 100, false), updateFixture('batch-1000', 1000, false)];
const rows = Array.from({ length: 10000 }, (_, i) => row(i));
const snapshot = { type: 'replica.snapshot', id: 'tasks', path: 'tasks.all', cursor: { epoch: 'benchmark-epoch', revision: 2 },
  key: 'id', scope: 'bench', visibilityScope: 'bench', truncated: false, maxRows: 10000, maxBytes: 10000000 };
const pages = [];
for (let i = 0; i < rows.length; i += 512) pages.push({ ...snapshot, type: 'replica.snapshotPage',
  snapshotId: 'bench-snapshot', sequence: pages.length, last: i + 512 >= rows.length, result: rows.slice(i, i + 512) });
fixtures.push({ name: 'snapshot-10000', before: [], after: rows, variants: {
  baseline: [{ ...snapshot, result: rows }], paged: pages,
  canonical: pages.map(({ result, ...page }) => ({ ...page, type: 'replica.snapshotEncodedPage', canonicalRows: result.map(row => JSON.stringify(row)) })),
} });
// Expired resume cursor: the safe recovery path is a new complete snapshot.
fixtures.push({ ...fixtures.at(-1), name: 'reconnect-expired-cursor' });
const output = resolve(process.argv[2] ?? 'scripts/bench-websocket/results');
await mkdir(output, { recursive: true });
const data = JSON.stringify({ seed, training: Array.from({ length: 200 }, (_, i) => row(i + 100000)), fixtures });
await writeFile(`${output}/fixtures.json`, data);
await writeFile(`${output}/fixture-sha256.txt`, createHash('sha256').update(data).digest('hex') + '\n');
console.log(`Wrote ${fixtures.length} deterministic workloads to ${output}`);
