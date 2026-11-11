import { test } from 'node:test';
import assert from 'node:assert/strict';
import { checkDigests, expectedDigest, rowHash } from '../src/digest-check.mjs';

const rows = [{ id: 'a', title: 'x' }, { id: 'b', title: 'y' }];
const hashes = (rs) => new Map(rs.map(r => [r.id, rowHash(r)]));
const xor = (rs) => expectedDigest(hashes(rs), 'xor1');
const rec = (dir, frame, socket = 1) => ({ dir, frame, socket });
const open = rec('out', { type: 'replica.openMany', opens: [{ id: 's', path: 'board.all', args: {} }] });

test('xor1 digest is order independent and matches the SDK aggregate of row hashes', () => {
  assert.equal(xor(rows), xor([...rows].reverse()));
  assert.equal(expectedDigest(new Map(), 'xor1'), 'xor1:' + '0'.repeat(64));
});

test('correct digests on snapshot, delta and resume pass', () => {
  const after = [{ id: 'a', title: 'x2' }];
  const r = checkDigests([
    open,
    rec('in', { type: 'replica.snapshotPage', id: 's', snapshotId: 'p', sequence: 0, last: true, key: 'id', result: rows, digest: xor(rows) }),
    rec('in', { type: 'replica.ready', id: 's', digest: xor(rows) }),
    rec('in', { type: 'replica.delta', id: 's', upserts: after, deleted: ['b'], digest: xor(after) }),
    // Resume on a new socket: the client's claimed hashes are its state.
    rec('out', { type: 'replica.openMany', opens: [{ id: 't', path: 'board.all', args: {}, keys: ['a'], hashes: { a: rowHash(after[0]) } }] }, 2),
    rec('in', { type: 'replica.ready', id: 't', digest: xor(after) }, 2),
  ]);
  assert.deepEqual(r, { checked: 3, problems: [] });
});

test('a drifted digest is reported, also inside replica.batch and in legacy format', () => {
  const drift = (d) => d.slice(0, -1) + (d.endsWith('0') ? '1' : '0');
  const r = checkDigests([
    open,
    rec('in', { type: 'replica.snapshot', id: 's', key: 'id', result: rows }),
    rec('in', { type: 'replica.batch', items: [{ type: 'replica.stream', id: 's', stream: 'S' }, { type: 'replica.ready', id: 'S', digest: drift(xor(rows)) }] }),
    rec('in', { type: 'replica.ready', id: 's', digest: drift(expectedDigest(hashes(rows), 'legacy')) }),
    rec('in', { type: 'replica.ready', id: 's', digest: expectedDigest(hashes(rows), 'legacy') }),
  ]);
  assert.equal(r.checked, 3);
  assert.equal(r.problems.length, 2);
  assert.match(r.problems[0], /digest mismatch on replica.ready for board.all/);
});

test('a re-auth without subscriptionsPreserved drops the socket subscriptions', () => {
  const tenantB = [{ id: 'b', title: 'tenant B' }];
  const r = checkDigests([
    open,
    rec('in', { type: 'replica.batch', items: [{ type: 'replica.stream', id: 's', stream: 'S' }, { type: 'replica.snapshot', id: 's', key: 'id', result: rows }, { type: 'replica.ready', id: 'S', digest: xor(rows) }] }),
    rec('in', { type: 'auth.result', id: 'a2', result: {} }),
    rec('out', { type: 'replica.openMany', opens: [{ id: 't', path: 'board.all', args: {} }] }),
    rec('in', { type: 'replica.snapshot', id: 't', key: 'id', result: [] }),
    // The stream token is reused; the client's binding still lists the dropped 's'.
    rec('in', { type: 'replica.batch', items: [{ type: 'replica.stream', id: 't', stream: 'S' }, { type: 'replica.delta', id: 'S', upserts: tenantB, deleted: [], digest: xor(tenantB) }] }),
  ]);
  assert.deepEqual(r, { checked: 2, problems: [] });
  // With subscriptionsPreserved the state carries over and is still checked.
  const kept = checkDigests([open, rec('in', { type: 'replica.snapshot', id: 's', key: 'id', result: rows }), rec('in', { type: 'auth.result', id: 'a2', result: { subscriptionsPreserved: true } }), rec('in', { type: 'replica.ready', id: 's', digest: xor(rows) })]);
  assert.deepEqual(kept, { checked: 1, problems: [] });
});
