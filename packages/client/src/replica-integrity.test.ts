import { expect, it } from 'vitest';
import { applyReplicaHashDelta, replicaRowsHashes } from './replica-integrity';
it('incremental hashes equal a full hash after updates, additions and removals', async () => {
  const original = [{ id: 'a', name: 'á' }, { id: 'b', name: 'B' }, { id: 'c', name: 'C' }];
  const hashes = await replicaRowsHashes(original, 'id');
  const upserts = [{ id: 'a', name: 'changed' }, { id: 'd', name: 'new' }];
  const next = await applyReplicaHashDelta(hashes, upserts, ['b'], 'id');
  expect(next).toEqual(await replicaRowsHashes([upserts[0], original[2], upserts[1]], 'id'));
  expect(hashes).toHaveProperty('b');
});
