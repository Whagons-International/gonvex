import {test} from 'node:test';import assert from 'node:assert/strict';
import {publicOrigin} from './deploy.mjs';
test('public origin defaults to workers.dev and accepts only a bare https origin override',()=>{
 assert.equal(publicOrigin('gonvex-cf-dev-x','acct'),'https://gonvex-cf-dev-x.acct.workers.dev');
 assert.equal(publicOrigin('gonvex-cf-dev-x','acct',''),'https://gonvex-cf-dev-x.acct.workers.dev');
 assert.equal(publicOrigin('gonvex-cf-dev-x','acct','https://gonvex-cf-dev-x.example.com'),'https://gonvex-cf-dev-x.example.com');
 assert.equal(publicOrigin('gonvex-cf-dev-x','acct','https://gonvex-cf-dev-x.example.com/'),'https://gonvex-cf-dev-x.example.com');
 for(const bad of ['http://h.example.com','https://h.example.com/path','https://u:p@h.example.com','https://h.example.com/?q=1','https://h.example.com/#x','h.example.com'])
  assert.throws(()=>publicOrigin('gonvex-cf-dev-x','acct',bad),/public URL override/);
});
