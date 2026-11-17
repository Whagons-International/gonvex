import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { handleTenantTransfer, transferIntoTenant, transferAllowed } from '../tenant-transfer';
import { containerRequest } from '../builder';
import { manifest } from './catalog';
import type { CompositionTenant } from './worker';
const config = { GONVEX_DEV_TRANSFER_TARGET:'gonvex-cf-dev-test',GONVEX_DEV_TRANSFER_TOKEN:'test-only',GONVEX_DEV_TRANSFER_TENANTS:'devcopy-test,devcopy-refusal' };
const request = (tenant:string, op:string, body:unknown={}, token='test-only') => new Request(`https://dev/__tenant-transfer/t/${tenant}/${op}`, {method:'POST',headers:{authorization:`Bearer ${token}`},body:JSON.stringify(body)});
it('forwards HTTPS service requests to Container HTTP without changing method, body or auth', async()=>{
 const result=containerRequest(new Request('https://release-builder/inspect?x=1',{method:'POST',headers:{authorization:'Bearer test'},body:'{"incoming":"id"}'}));
 expect(result.url).toBe('http://release-builder/inspect?x=1');expect(result.method).toBe('POST');expect(result.headers.get('authorization')).toBe('Bearer test');expect(await result.text()).toBe('{"incoming":"id"}');
});
it('keeps transfer disabled without explicit dev target, exact fresh ID and correct token',async()=>{
 expect(transferAllowed({},'devcopy-test')).toBe(false);expect(transferAllowed({...config,GONVEX_DEV_TRANSFER_TARGET:'production'},'devcopy-test')).toBe(false);
 for(const [tenant,token] of [['other','test-only'],['devcopy-test','wrong'],['devcopy-test','é'.repeat(9)]]){
  expect((await handleTenantTransfer(request(tenant!, 'wipe',{},token),{...config,TENANT:env.TENANT}))?.status).toBe(404);
 }
});
it('imports through AssemblyTenant, checks exact storage classes, refuses incomplete/checksum-mismatched completion, and serves the same SQLite',async()=>{
 const stub=env.TENANT.getByName('devcopy-test');
 const call=async(op:string,body:unknown={})=>{
  const response=(await handleTenantTransfer(request('devcopy-test',op,body),{...config,TENANT:env.TENANT}))!;
  // Drain the real RPC response before the next request.
  return new Response(await response.arrayBuffer(),{status:response.status,headers:response.headers});
 };
 expect((await call('wipe'))?.status).toBe(200);
 await runInDurableObject(stub,async(instance)=>{await expect((instance as CompositionTenant).operatorApplyRelease('unused','p')).rejects.toThrow('P03 tenant import is incomplete');});
 expect((await call('schema',{statements:[{name:'board',sql:'CREATE TABLE board (id TEXT PRIMARY KEY, title TEXT, exact INTEGER, bytes BLOB)'}]}))?.status).toBe(200);
 expect((await call('insert',{parts:[{table:'board',columns:['id','title','exact','bytes'],rows:[['copied','staging copy',{i:'9223372036854775806'},{b:'AP8='}]]}]}))?.status).toBe(200);
 expect((await call('status'))?.status).toBe(200);
 expect((await call('complete'))?.status).toBe(409);
 expect((await call('check',{fts:[]}))?.status).toBe(200);
 const catalog=await call('catalog');const details=await catalog!.json() as {error?:string};expect(details.error).toBeUndefined();
 expect((await call('complete'))?.status).toBe(409);
 expect((await call('verify',{checksums:{board:{rows:1,sha256:'0'.repeat(64)}}}))?.status).toBe(409);
 const {checksums}=await (await call('checksum',{tables:['board']}))!.json() as {checksums:Record<string,{rows:number;sha256:string}>};
 expect((await call('verify',{checksums}))?.status).toBe(200);
 expect((await call('complete'))?.status).toBe(200);
 expect((await call('wipe'))?.status).toBe(409);expect((await call('insert',{parts:[]}))?.status).toBe(409);
 await runInDurableObject(stub,async(instance,state)=>{
  const actual=instance as CompositionTenant;await actual.activate('p',manifest,null);
  expect(actual.kernel.module?.projectId).toBe('p');
  const read=await actual.operatorReadRows('board',{limit:10,offset:0,search:'',sort:'',direction:'',filters:'',columns:'',count:''});expect(read.ok).toBe(true);
  expect(state.storage.sql.exec("SELECT id,title,CAST(exact AS TEXT) exact,hex(bytes) bytes FROM board").one()).toEqual({id:'copied',title:'staging copy',exact:'9223372036854775806',bytes:'00FF'});
  expect((await transferIntoTenant(state,request('devcopy-test','wipe'),true)).status).toBe(409);
 });
});
it('retains platform SQL refusals and refuses to complete an import containing them',async()=>{
 const call=async(op:string,body:unknown={})=>handleTenantTransfer(request('devcopy-refusal',op,body),{...config,TENANT:env.TENANT});
 await call('wipe');const r=await call('schema',{statements:[{name:'bad',sql:'CREATE TABLE impossible ('},{name:'good',sql:'CREATE TABLE good (id INTEGER)'}]});
 expect(await r!.json()).toMatchObject({applied:1,failed:[{name:'bad'}]});await call('check');
 expect((await call('complete'))?.status).toBe(409);
});
