import 'fake-indexeddb/auto';
import { readFileSync } from 'node:fs';
import { expect, it } from 'vitest';
import { IndexedDBEngineReplicaStorage } from './indexeddb-engine.js';
import { LocalReplica } from './local-replica.js';
import { RowPatchDecoder } from './row-patch.js';

it('persists the same projections, per-field authority and tombstones as complete messages across reload', async () => {
  const fixture = JSON.parse(readFileSync(new URL('../../../rust/crates/protocol/fixtures/row-patch-v1.json', import.meta.url), 'utf8'));
  const fullStore = new IndexedDBEngineReplicaStorage('row-patch-full'), patchStore = new IndexedDBEngineReplicaStorage('row-patch-patched');
  const full = new LocalReplica(fullStore), patched = new LocalReplica(patchStore), decoder = new RowPatchDecoder();
  try {
    await full.activateScope('tenant'); await patched.activateScope('tenant');
    // Another subscription supplies a field absent from this wire projection.
    const window = { signature: 'other-projection', kind: 'replica' as const, entity: 'tasks', key: 'id',
      rows: [{ id: 'a', other: 'retain' }], cursor: { epoch: 'e', revision: 1 }, completeness: 'complete' as const, source: 'server' as const };
    await full.replaceWindow(window); await patched.replaceWindow(window);
    for (const row of fixture.cases) {
      const decoded: any = decoder.decode(row.patch ?? row.full);
      expect(decoded).toEqual(row.logical);
      if (decoded.type === 'replica.transaction') {
        await full.applyTransaction(row.logical); await patched.applyTransaction(decoded);
      } else {
        const delta = (message: any) => ({ signature: 'collection', kind: 'replica' as const, entity: 'tasks', key: 'id',
          upserts: message.upserts, deleted: message.deleted, cursor: message.cursor, completeness: 'complete' as const, source: 'server' as const });
        await full.applyWindowDelta(delta(row.logical)); await patched.applyWindowDelta(delta(decoded));
      }
      expect(await patchStore.load('tenant')).toEqual(await fullStore.load('tenant'));
      expect(await patchStore.load('different-tenant')).toBeUndefined();
    }
    const reloaded = new LocalReplica(patchStore);
    await reloaded.activateScope('tenant');
    expect(reloaded.entity('tasks', 'a')).toBeUndefined();
    expect((await patchStore.load('tenant'))?.cursor).toEqual({ epoch: 'e', revision: 4 });
    reloaded.dispose();
  } finally {
    full.dispose(); patched.dispose(); fullStore.close(); patchStore.close();
    for (const name of ['row-patch-full:engine-members-v1', 'row-patch-patched:engine-members-v1']) {
      await new Promise<void>((resolve, reject) => { const request = indexedDB.deleteDatabase(name); request.onsuccess = () => resolve(); request.onerror = () => reject(request.error); });
    }
  }
});
