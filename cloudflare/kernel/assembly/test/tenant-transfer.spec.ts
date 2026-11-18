import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { handleTenantTransfer, transferIntoTenant, transferAllowed } from '../tenant-transfer';
import { devTransferAllowed, operatorConfig } from '../../../control/operator/config';
import { containerRequest } from '../builder';
import { readTypedRows, RowHasher } from '../../../tools/tenant-transfer/src/codec.mjs';
import { createHash } from 'node:crypto';
import { manifest } from './catalog';
import type { CompositionTenant } from './worker';
const config = { GONVEX_DEV_TRANSFER_TARGET:'gonvex-cf-dev-test',GONVEX_DEV_TRANSFER_TOKEN:'test-only',GONVEX_DEV_TRANSFER_TENANTS:'devcopy-test,devcopy-refusal,devcopy-large' };
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
it('shares one dev gate between /__tenant-transfer and the dev-only operator routes (contract-changes/0091)',()=>{
 const gate=(env:Record<string,string>)=>{const c=operatorConfig(env);return {target:c.devTransferTarget,tenants:c.devTransferTenants};};
 expect(devTransferAllowed(gate(config),'devcopy-test')).toBe(true);
 expect(devTransferAllowed(gate({}),'devcopy-test')).toBe(false);
 expect(devTransferAllowed(gate({...config,GONVEX_DEV_TRANSFER_TARGET:'gonvex-stg'}),'devcopy-test')).toBe(false);
 for(const tenant of ['devcopy-other','tenant_a937','devcopy-test,devcopy-refusal','']){expect(devTransferAllowed(gate(config),tenant)).toBe(false);expect(transferAllowed(config,tenant)).toBe(false);}
 expect(devTransferAllowed(gate({...config,GONVEX_DEV_TRANSFER_TENANTS:'x,devcopy-only'}),'devcopy-only')).toBe(true);
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
  // No restart, activation, or lazy files RPC may create these on our behalf.
  expect(state.storage.sql.exec('SELECT count(*) AS n FROM _gonvex_files').one().n).toBe(0);
  expect(state.storage.sql.exec("SELECT name FROM sqlite_schema WHERE type='table' AND name='_gonvex_storage_uploads'").toArray()).toHaveLength(1);
  const actual=instance as CompositionTenant;await actual.activate('p',manifest,null);
  expect(actual.kernel.module?.projectId).toBe('p');
  const read=await actual.operatorReadRows('board',{limit:10,offset:0,search:'',sort:'',direction:'',filters:'',columns:'',count:''});expect(read.ok).toBe(true);
  expect(state.storage.sql.exec("SELECT id,title,CAST(exact AS TEXT) exact,hex(bytes) bytes FROM board").one()).toEqual({id:'copied',title:'staging copy',exact:'9223372036854775806',bytes:'00FF'});
  expect((await transferIntoTenant(state,request('devcopy-test','wipe'),true)).status).toBe(409);
  // X11b (contract-changes/0091): the dev file remap is admitted on this listed copy, for its own project only.
  // This composition configures no storage, so the admitted call stops at the files layer.
  expect(await actual.operatorRemapFiles('other-project','tenant_source',true)).toEqual({ok:false,status:409,error:'tenant does not belong to this project'});
  expect(await actual.operatorRemapFiles('p','tenant_source',true)).toEqual({ok:false,status:503,error:'storage is not configured'});
 });
 // An object whose name the gate does not list refuses before anything else.
 await runInDurableObject(env.TENANT.getByName('devcopy-unlisted'),async(instance)=>{
  expect(await (instance as CompositionTenant).operatorRemapFiles('p','tenant_source',true)).toEqual({ok:false,status:404,error:'file remap is not available on this deployment'});
 });
});
it('retains platform SQL refusals and refuses to complete an import containing them',async()=>{
 const call=async(op:string,body:unknown={})=>handleTenantTransfer(request('devcopy-refusal',op,body),{...config,TENANT:env.TENANT});
 await call('wipe');const r=await call('schema',{statements:[{name:'bad',sql:'CREATE TABLE impossible ('},{name:'good',sql:'CREATE TABLE good (id INTEGER)'}]});
 expect(await r!.json()).toMatchObject({applied:1,failed:[{name:'bad'}]});await call('check');
 expect((await call('complete'))?.status).toBe(409);
});

it('accepts a legal base64 BLOB row whose wire request exceeds 2 MiB and still bounds requests',async()=>{
 const call=async(op:string,body:unknown={})=>{
  const response=(await handleTenantTransfer(request('devcopy-large',op,body),{...config,TENANT:env.TENANT}))!;
  return new Response(await response.arrayBuffer(),{status:response.status});
 };
 expect((await call('wipe')).status).toBe(200);
 expect((await call('schema',{statements:[{name:'large',sql:'CREATE TABLE large (id TEXT PRIMARY KEY, bytes BLOB)'}]})).status).toBe(200);
 const body={parts:[{table:'large',columns:['id','bytes'],rows:[['legal',{b:btoa('a'.repeat(1_600_000))}]]}]};
 expect(new TextEncoder().encode(JSON.stringify(body)).byteLength).toBeGreaterThan(2*1024*1024);
 expect((await call('insert',body)).status).toBe(200);
 await runInDurableObject(env.TENANT.getByName('devcopy-large'),async(_instance,state)=>{
  expect(state.storage.sql.exec('SELECT length(bytes) AS n FROM large WHERE id=?','legal').one().n).toBe(1_600_000);
 });
 expect((await call('insert','a'.repeat(4*1024*1024))).status).toBe(413);
});


it('checksums, pages and completes a 100-column serving import with exact values across column slices',async()=>{
 const tenant='devcopy-wide', settings={...config,GONVEX_DEV_TRANSFER_TENANTS:tenant};
 const call=async(op:string,body:unknown={})=>{
  const response=(await handleTenantTransfer(request(tenant,op,body),{...settings,TENANT:env.TENANT}))!;
  return new Response(await response.arrayBuffer(),{status:response.status});
 };
 expect((await call('wipe')).status).toBe(200);
 const columns=['id',...Array.from({length:99},(_,i)=>`v${i}`)];
 expect((await call('schema',{statements:[{name:'wide',sql:`CREATE TABLE wide (id INTEGER PRIMARY KEY,${columns.slice(1).join(',')})`}]})).status).toBe(200);
 const rows=Array.from({length:7},(_,r)=>columns.map((_,i)=>i===0?r+1:i%5===0?{i:'9223372036854775807'}:i%5===1?{t:'/wD+'}:i%5===2?{b:'AP8='}:i%5===3?{r:3}:null));
 expect((await call('insert',{parts:[{table:'wide',columns,rows}]})).status).toBe(200);
 const checksumResponse=await call('checksum',{tables:['wide']});expect(checksumResponse.status).toBe(200);
 const {checksums}=await checksumResponse.json() as {checksums:Record<string,{rows:number;sha256:string}>};expect(checksums.wide?.rows).toBe(7);
 const read:unknown[]=[];let after:null|unknown[]=null;
 do{
  const response=await call('read',{tables:[{table:'wide',after}],maxBytes:2000});expect(response.status).toBe(200);
  const {pages}=await response.json() as {pages:{rows:unknown[][];next:null|unknown[]}[]};
  read.push(...pages[0]!.rows);after=pages[0]!.next;
 }while(after);
 expect(read).toEqual(rows);
 await runInDurableObject(env.TENANT.getByName(tenant),async(_instance,state)=>{
  // The same row order/hash as the unchanged two-field codec. The table is
  // scanned twice, with no writes and no SELECT in the per-row loop.
  const cursors:ReturnType<SqlStorage['exec']>[]=[];
  const storage={exec:(sql:string,...params:SqlStorageValue[])=>{const cursor=state.storage.sql.exec(sql,...params);cursors.push(cursor);expect(cursor.columnNames.length).toBeLessThanOrEqual(100);return cursor;}};
  const hasher=new RowHasher(createHash);
  for(const row of readTypedRows(storage,{name:'wide',copyColumns:columns,key:['id']}))hasher.addTyped(row);
  expect(hasher.digest()).toEqual(checksums.wide);expect(cursors).toHaveLength(2);
  expect(cursors.reduce((n,c)=>n+c.rowsRead,0)).toBe(14);expect(cursors.reduce((n,c)=>n+c.rowsWritten,0)).toBe(0);
 });
 expect((await call('check')).status).toBe(200);expect((await call('verify',{checksums})).status).toBe(200);expect((await call('complete')).status).toBe(200);
});

it('wipes an object whose empty files schema was created while the gate was off, not one with rows (X10, devcopy-stg1117-12x)', async () => {
 const tenant='devcopy-files-schema', settings={...config,GONVEX_DEV_TRANSFER_TENANTS:tenant};
 const call=async(op:string,body:unknown={})=>{const r=(await handleTenantTransfer(request(tenant,op,body),{...settings,TENANT:env.TENANT}))!;return {status:r.status,body:await r.json() as Record<string,unknown>};};
 const { FILES_SCHEMA } = await import('../../files/schema');
 await runInDurableObject(env.TENANT.getByName(tenant),async(_instance,state)=>{
  // What the constructor runs when the gate does not admit the name (tenant.ts: files.ensureSchema()).
  state.storage.transactionSync(()=>{for(const statement of FILES_SCHEMA)state.storage.sql.exec(statement);});
  expect(state.storage.sql.exec("SELECT count(*) AS n FROM sqlite_schema WHERE type='table' AND name='_gonvex_storage_uploads'").one().n).toBe(1);
  state.storage.sql.exec("INSERT INTO _gonvex_storage_uploads(object_key) VALUES('k')");
 });
 expect(await call('wipe')).toEqual({status:409,body:{error:'tenant storage is not empty'}});
 await runInDurableObject(env.TENANT.getByName(tenant),async(_instance,state)=>{state.storage.sql.exec('DELETE FROM _gonvex_storage_uploads');});
 expect(await call('wipe')).toEqual({status:200,body:{wiped:true}});
});
