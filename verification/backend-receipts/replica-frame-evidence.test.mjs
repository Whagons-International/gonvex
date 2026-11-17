import assert from 'node:assert/strict';
import test from 'node:test';
import { hasHeldReplicaData } from './replica-frame-evidence.mjs';

const transaction = {
  type: 'replica.transaction', cursor: { epoch: 'e', revision: 3 },
  changes: [{ entity: 'receipt_records', id: 'upgrade-intent', operation: 'insert',
    newValue: { id: 'upgrade-intent', value: 7 } }],
};
const held = frames => hasHeldReplicaData(frames, 3, 'upgrade-intent');

test('the actual shared delivery envelope retains a withheld committed row', () => {
  const batch = { type: 'replica.batch', tenant: 'tenant', class: 'shared', from: 0,
    items: [transaction, { type: 'replica.stream', id: 'subscription', stream: '~reference' },
      { type: 'replica.watermark', revision: 3 }] };
  assert.equal(held([batch]), true);
  assert.equal(held([transaction]), true);
});

test('empty batches, stream references and barriers cannot stand in for row data', () => {
  for (const frames of [[], [{ type: 'replica.batch', items: [] }],
    [{ type: 'replica.batch', items: [{ type: 'replica.watermark', revision: 3 }] }],
    [{ type: 'replica.stream', stream: '~reference' }],
    [{ ...transaction, changes: [] }],
    [{ ...transaction, changes: [{ newValue: { id: 'another-row' } }] }],
    [{ ...transaction, cursor: { revision: 2 } }]]) assert.equal(held(frames), false);
});

test('snapshot page evidence still requires the committed row and revision', () => {
  assert.equal(held([{ type: 'replica.snapshotPage', cursor: { revision: 3 }, result: [{ id: 'upgrade-intent' }] }]), true);
  assert.equal(held([{ type: 'replica.snapshotPage', cursor: { revision: 2 }, result: [{ id: 'upgrade-intent' }] }]), false);
});
