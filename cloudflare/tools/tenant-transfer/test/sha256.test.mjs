// X10: a checksum that spans several requests equals node:crypto's one-pass checksum.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { test } from 'node:test';
import { RowHasher } from '../src/codec.mjs';
import { Sha256, createResumableHash } from '../src/sha256.mjs';

test('Sha256 equals node:crypto across block boundaries and save/load at any point', () => {
  for (const n of [0, 1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 1000, 70_000]) {
    const bytes = randomBytes(n);
    let hash = new Sha256();
    for (let i = 0; i < n; ) {
      const k = Math.min(n - i, 1 + ((i * 7919) % 97));
      hash.update(bytes.subarray(i, i + k));
      i += k;
      hash = Sha256.load(JSON.parse(JSON.stringify(hash.save())));
    }
    assert.equal(hash.digest('hex'), createHash('sha256').update(bytes).digest('hex'), `length ${n}`);
  }
  assert.throws(() => Sha256.load({ h: [1], tail: '', bytes: 0 }), /invalid hash state/);
  assert.throws(() => Sha256.load({ ...new Sha256().update(new Uint8Array(3)).save(), bytes: 4 }), /invalid hash state/);
});

test('a RowHasher resumed from saved state gives the one-pass digest', () => {
  const rows = Array.from({ length: 500 }, (_, i) => ['integer', String(i), 'text', new TextEncoder().encode(`row ${i}`), 'real', i / 3, 'null', null, 'blob', new Uint8Array([i & 255])]);
  const reference = new RowHasher(createHash);
  for (const row of rows) reference.addTyped(row);
  let paged = new RowHasher(createResumableHash);
  rows.forEach((row, i) => {
    paged.addTyped(row);
    if (i % 37 === 0) paged = RowHasher.resume(JSON.parse(JSON.stringify(paged.save())), (h) => Sha256.load(h));
  });
  assert.deepEqual(paged.digest(), reference.digest());
  assert.throws(() => new RowHasher(createHash).save(), /cannot save/);
});
