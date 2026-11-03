import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loopback, quantiles } from './safety.mjs';
test('reject external destinations, DNS names, disguised credentials and alternate IP spellings',()=>{
  for(const u of ['https://example.com','http://65.109.93.58','http://app.whagons.com','http://localhost','http://127.0.0.1.example.com','http://user@127.0.0.1','file:///tmp/test','http://127.1','http://2130706433']) assert.throws(()=>loopback(u));
  assert.equal(loopback('http://127.0.0.1:8080').port,'8080');
});
test('nearest-rank quantiles and absent samples',()=>{
 assert.deepEqual(quantiles([]),{p50:null,p95:null,p99:null});
 assert.deepEqual(quantiles([4,1,3,2]),{p50:2,p95:4,p99:4});
});
