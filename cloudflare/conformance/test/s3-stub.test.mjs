import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startS3Stub } from '../src/s3-stub.mjs';

test('the S3 stub stores, answers, heads and deletes objects (path-style)', async () => {
  const s3 = await startS3Stub();
  try {
    const url = `${s3.endpoint}/conformance/tenant%2Ffile`;
    assert.equal((await fetch(url, { method: 'HEAD' })).status, 404);
    assert.equal((await fetch(url)).status, 404);
    const put = await fetch(url, { method: 'PUT', headers: { 'content-type': 'text/plain', authorization: 'AWS4-HMAC-SHA256 ignored' }, body: 'hi!!' });
    assert.equal(put.status, 200); assert.match(put.headers.get('etag'), /^"[0-9a-f]{32}"$/);
    const head = await fetch(url, { method: 'HEAD' });
    assert.equal(head.status, 200); assert.equal(head.headers.get('content-length'), '4'); assert.equal(head.headers.get('content-type'), 'text/plain');
    assert.equal(await (await fetch(url)).text(), 'hi!!');
    assert.equal((await fetch(url, { method: 'DELETE' })).status, 204);
    assert.equal((await fetch(url)).status, 404);
  } finally { await s3.close(); }
});
