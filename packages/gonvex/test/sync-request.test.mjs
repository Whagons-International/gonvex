import {test} from 'node:test';
import assert from 'node:assert/strict';
import {postDevSync} from '../dist/sync-request.js';
test('a Worker restart retries the identical request id and body until the persisted publication answers',async()=>{
 const seen=[];
 const result=await postDevSync('http://localhost/dev/sync',{body:'{"module":"same"}',headers:{authorization:'Bearer synthetic'}},async(_url,init)=>{
  seen.push({id:init.headers.get('x-gonvex-sync-request-id'),body:init.body});
  return seen.length===1?new Response('Your worker restarted mid-request. Please try sending the request again.',{status:503}):Response.json({ok:true});
 });
 assert.equal(result.status,200);assert.equal(seen.length,2);assert.match(seen[0].id,/^[0-9a-f-]{36}$/);assert.deepEqual(seen[1],seen[0]);
});
test('ordinary validation and unknown upstream failures are returned without replay',async()=>{
 for(const status of [422,500,503]){
  let calls=0;
  const result=await postDevSync('http://localhost/dev/sync',{body:'{}'},async()=>{calls++;return Response.json({error:'migration failed'},{status});});
  assert.equal(calls,1);assert.equal(result.status,status);
 }
});
