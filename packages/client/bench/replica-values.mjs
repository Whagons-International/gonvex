// Isolate row value encoding, with identical rows, schema and request batching.
// nice -n 10 node --expose-gc packages/client/bench/replica-values.mjs
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb';
import { Dexie } from 'dexie';
Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
Dexie.dependencies.indexedDB = indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
const gcHeap = () => { globalThis.gc(); return process.memoryUsage().heapUsed / 1048576; };
for (const count of (process.env.ROWS ?? '10000,50000').split(',').map(Number)) {
  for (const encoding of ['json', 'object']) {
    const times = [];
    for (let trial = 0; trial < 3; trial++) {
      const db = new Dexie(`values-${count}-${encoding}-${trial}`, { cache: 'disabled' });
      db.unuse({ stack: 'dbcore', name: 'Cache' }); db.unuse({ stack: 'dbcore', name: 'Observability' });
      db.version(1).stores({ rows: '&id' }); await db.open();
      const startHeap = gcHeap();
      let rows = Array.from({ length: count }, (_, i) => {
        const row = { _id: `t${i}`, statusId: `s${i % 20}`, workspaceId: `w${i % 100}` };
        for (let c = 3; c < 50; c++) row[`field${c}`] = c % 3 === 0 ? i + c : c % 3 === 1 ? `value-${c}-${i % 100}` : i % 2 === 0;
        return row;
      });
      const writeStart = performance.now();
      await db.transaction('rw', db.table('rows'), async () => {
        for (let i = 0; i < rows.length; i += 32) await db.table('rows').bulkPut(rows.slice(i, i + 32).map(row => ({ id: row._id, value: encoding === 'json' ? JSON.stringify(row) : row })));
      });
      const writeMs = performance.now() - writeStart;
      rows = undefined;
      const storedHeapMiB = gcHeap() - startHeap;
      db.close(); await db.open();
      const readStart = performance.now(); let after; let decoded = [];
      for (;;) {
        const records = await (after ? db.table('rows').where('id').above(after) : db.table('rows').orderBy('id')).limit(256).toArray();
        if (!records.length) break;
        for (const record of records) decoded.push(encoding === 'json' ? JSON.parse(record.value) : record.value);
        after = records.at(-1).id;
      }
      const readMs = performance.now() - readStart;
      if (decoded.length !== count) throw new Error('Lost rows');
      decoded = undefined;
      times.push({ writeMs, readMs, storedHeapMiB }); await db.delete();
    }
    const median = key => times.map(time => time[key]).sort((a,b)=>a-b)[1];
    console.log(JSON.stringify({ rows: count, encoding, writeMs: median('writeMs'), readMs: median('readMs'), storedHeapMiB: median('storedHeapMiB') }));
  }
}
