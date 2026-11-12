import { test } from 'node:test';
import assert from 'node:assert/strict';
import { checkWatermarks } from '../src/watermark-check.mjs';

const rec = (tag, dir, frame, step = 's', socket = 1) => ({ tag, dir, frame, socket, step });
const open = (tag) => rec(tag, 'out', { type: 'replica.openMany', opens: [{ id: 'x', path: 'p' }] });

test('a subscribed socket: monotonic, within the committed revision, not below the applied cursor', () => {
  const ok = checkWatermarks([
    open('r'),
    rec('w', 'in', { type: 'reducer.result', id: 'c', committedRevision: 5 }),
    rec('r', 'in', { type: 'replica.transaction', cursor: { epoch: 'e', revision: 5 }, changes: [] }),
    rec('r', 'in', { type: 'replica.watermark', revision: 5 }),
    rec('r', 'in', { type: 'replica.watermark', revision: 5 }),
  ]);
  assert.deepEqual(ok, { checked: 2, problems: [] });
  const down = checkWatermarks([open('r'), rec('w', 'in', { type: 'reducer.result', committedRevision: 6 }), rec('r', 'in', { type: 'replica.watermark', revision: 6 }), rec('r', 'in', { type: 'replica.watermark', revision: 5 })]);
  assert.match(down.problems.join('\n'), /went down/);
  const beyond = checkWatermarks([open('r'), rec('w', 'in', { type: 'reducer.result', committedRevision: 3 }), rec('r', 'in', { type: 'replica.watermark', revision: 4 })]);
  assert.match(beyond.problems.join('\n'), /beyond the highest committed revision/);
  const below = checkWatermarks([open('r'), rec('r', 'in', { type: 'replica.transaction', cursor: { epoch: 'e', revision: 7 }, changes: [] }), rec('r', 'in', { type: 'replica.watermark', revision: 6 })]);
  assert.match(below.problems.join('\n'), /below the applied feed cursor/);
});

test('batch watermarks are checked; an auth without preserved subscriptions starts over; unsubscribed sockets are not checked', () => {
  const batch = checkWatermarks([open('r'), rec('w', 'in', { type: 'reducer.result', committedRevision: 2 }),
    rec('r', 'in', { type: 'replica.batch', tenant: 't', class: '0', from: 0, items: [], watermark: 2 }),
    rec('r', 'in', { type: 'auth.result', id: 'a', result: {} }),
    rec('r', 'in', { type: 'replica.watermark', revision: 1 })]);
  assert.deepEqual(batch, { checked: 2, problems: [] });
  const writer = checkWatermarks([rec('w', 'in', { type: 'reducer.result', committedRevision: 4 }), rec('w', 'in', { type: 'replica.watermark', revision: 4 }), rec('w', 'in', { type: 'replica.watermark', revision: 3 })]);
  assert.deepEqual(writer, { checked: 0, problems: [] });
});
