import { expect, it, vi } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

const mocked = vi.hoisted(() => ({ host: undefined as any }));
vi.mock('./sqlite-official-host.js', () => ({ openOfficialSQLiteHost: async () => mocked.host }));

it('routes restartable first-sync frames through the real owner queue and publishes only on verified activation', async () => {
  vi.stubGlobal('navigator',{locks:{request:async(_name:string,_options:unknown,run:()=>Promise<void>)=>run()}});const db = new DatabaseSync(':memory:');
  const database: SQLiteReplicaDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...args) { return db.prepare(sql).run(...args as any[]); },
    async getFirstAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).get(...args as any[]) as T ?? null; },
    async getAllAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).all(...args as any[]) as T[]; },
    async withTransactionAsync(run) { db.exec('BEGIN'); try { await run(); db.exec('COMMIT'); } catch (error) { db.exec('ROLLBACK'); throw error; } },
  };
  db.function('gv_ingress_sha256',value=>value===null?null:createHash('sha256').update(typeof value==='string'?value:value as Uint8Array).digest('hex'));
  Object.assign(database,{ingressNativeHash:true});
  mocked.host = { database, memoryBytes: () => 0, close: async () => {} };
  const awaiting = new Map<string, (reply: any) => void>(), changes: any[] = [];
  vi.stubGlobal('postMessage', (message: any) => {
    if (message.type === 'change'){if(!message.reset)changes.push(message);queueMicrotask(()=>(globalThis as any).onmessage?.({data:{type:'change-ack',protocol:2,token:'one',tab:'owner-tab',notification:message.notification,epoch:message.epoch,sequence:message.sequence}}));}
    else { const resolve = awaiting.get(message.id); awaiting.delete(message.id); resolve?.(message); }
  });
  const interval = vi.spyOn(globalThis, 'setInterval').mockImplementation(() => 0 as any);
  let id = 0;
  try {
    await import('./sqlite-owner-worker.js');
    interval.mockRestore();
    const request = (op: string, args: unknown, key=String(++id),requestID?:string) => new Promise<any>(resolve => { awaiting.set(key, resolve);
      (globalThis as any).onmessage({ data: { id: key, op, args,requestID,tabID:'owner-tab' } });
    });
    const schema = { tasks: { key: '_id', fields: { _id: { kind: 'text' }, name: { kind: 'text' } }, indexes: [], search: ['name'], counts: [{ columns: ['name'] }] } };
    expect((await request('open', { protocol:2, owner: 'one', schema })).error).toBeUndefined();
    expect((await request('bootstrapBegin', { scope: 's', token: 'first' })).value.nextPage).toBe(0);
    const encoded = JSON.stringify([{ table: 'tasks', id: 'a', version: 1, row: { _id: 'a', name: 'Needle' } }]);
    // Concurrent requests must preserve the actor's FIFO transaction order.
    const frame = request('bootstrapAppend', { scope: 's', token: 'first', page: 0, encoded:new TextEncoder().encode(encoded) });
    const count = request('count', { scope: 's', entity: 'tasks', group: 0, values: ['Needle'] });
    expect((await frame).value.rows).toBe(1); expect(await count).toMatchObject({ value: 0 });
    expect(changes).toHaveLength(0);
    expect((await request('bootstrapAppend', { scope: 's', token: 'first', page: 0, encoded })).value.rows).toBe(1);
    const hash = createHash('sha256').update(encoded).digest('hex');
    const root = createHash('sha256').update('0'.repeat(64) + hash).digest('hex');
    const expected = { pages: 1, rows: 1, root };
    expect((await request('bootstrapFinish', { scope: 's', token: 'first', expected: { ...expected, rows: 2 } })).error.code).toBe('INVALID_ROW');
    expect(changes).toHaveLength(0);
    expect((await request('bootstrapFinish', { scope: 's', token: 'first', expected })).value.complete).toBe(true);
    expect((await request('count', { scope: 's', entity: 'tasks', group: 0, values: ['Needle'] })).value).toBe(1);
    expect(changes).toHaveLength(1);
    expect((await request('page', { scope: 's', plan: { table: 'tasks', order: [{ field: '_id', direction: 'asc' }], textOrder: 'binary', limit: 2, search: 'needle' } })).value.rows.map((row: any) => row._id)).toEqual(['a']);
    expect((await request('bootstrapFinish', { scope: 's', token: 'first', expected })).value.complete).toBe(true);
    expect((await request('count', { scope: 's', entity: 'tasks', group: 0, values: ['Needle'] })).value).toBe(1);
    const canonicalPage={scope:'s',method:'readViewPage',args:['s','grid',{table:'tasks',order:[{field:'_id',direction:'asc'}],textOrder:'binary',limit:2},0]};
    expect((await request('canonicalCall',canonicalPage)).value.value.rows[0].name).toBe('Needle');
    const published=changes.length;
    const canonicalWrite={scope:'s',method:'applyTransaction',args:[{cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{name:'Updated needle'}}]},{entities:{},liveQueries:{}},'s']};
    const write=request('canonicalCall',canonicalWrite,'transport-one','canonical-retry');
    const following=request('page',{scope:'s',plan:{table:'tasks',order:[{field:'_id',direction:'asc'}],textOrder:'binary',limit:2,search:'updated'}});
    const committed=await write;
    expect(committed.error).toBeUndefined();expect((await following).value.rows[0].name).toBe('Updated needle');
    expect((await request('canonicalCall',canonicalWrite,'canonical-retry')).value).toEqual(committed.value);
    const generation=(await request('canonicalGeneration',{scope:'s'})).value;
    expect((await request('canonicalRead',{scope:'s',expected:generation,coverage:{tasks:{key:'_id',complete:true}},read:{table:'tasks',where:{column:'name',op:'eq',value:'Updated needle'}}})).value.value.rows[0].name).toBe('Updated needle');
    expect(changes.length).toBe(published+1); // Replay returns the receipt without another change event.
    const {replicaSumDigest,replicaRowsHashes}=await import('./replica-integrity.js');
    const rows=[{_id:'a',name:'old snapshot'}],digest=replicaSumDigest(await replicaRowsHashes(rows,'_id'));
    const stage={type:'replica.snapshotPage',id:'source',snapshotId:'disk',key:'_id',scope:'tenant',visibilityScope:'s',cursor:{epoch:'e',revision:1},sequence:0,last:true,result:rows,digest};
    expect((await request('snapshotAppend',{scope:'s',page:stage})).value.complete).toBe(true);
    expect((await request('snapshotRead',{scope:'s',subscription:'source',snapshot:'disk'})).value.rows[0].row.name).toBe('old snapshot');
    expect((await request('canonicalCall',canonicalPage)).value.value.rows[0].name).toBe('Updated needle');
    expect(changes.length).toBe(published+1); // Isolated stages do not publish canonical changes.
    expect((await request('snapshotDiscard',{scope:'s',subscription:'source',snapshot:'disk'})).value.removed).toBe(true);
    const encodedSnapshot=new TextEncoder().encode(JSON.stringify({...stage,type:'replica.snapshotEncodedPage',result:undefined,canonicalRows:rows.map(row=>JSON.stringify(row))}));
    expect((await request('snapshotAppendEncoded',{scope:'s',encoded:encodedSnapshot})).value.complete).toBe(true);
    expect((await request('snapshotRead',{scope:'s',subscription:'source',snapshot:'disk'})).value.rows[0].row.name).toBe('old snapshot');
    expect(changes.length).toBe(published+1);
    const header={signature:'native-collection',entity:'tasks',key:'_id',kind:'replica',source:'server',completeness:'complete',cursor:stage.cursor,aggregateDigest:digest};
    const activation=await request('snapshotActivate',{scope:'s',subscription:'source',snapshot:'disk',header});
    expect(activation.error).toBeUndefined();expect(activation.value.changes).toMatchObject({reset:true,entities:{},windows:{}});
    expect((await request('windowPage',{scope:'s',signature:header.signature,generation:'disk',offset:0})).value.ids).toEqual(['a']);
    expect((await request('canonicalCall',canonicalPage)).value.value.rows[0].name).toBe('Updated needle');
    expect(changes.length).toBe(published+2);
    expect(changes.at(-1).sequence).toBe((await request('canonicalGeneration',{scope:'s'})).value.sequence);
    const beforeReads=changes.length;const native=await request('nativeWindowPage',{scope:'s',signature:header.signature,limit:50});
    expect(native.error).toBeUndefined();expect(native.value).toMatchObject({window:{generation:'disk',total:1,header},offset:0,nextOffset:null,rows:[{id:'a',value:{_id:'a',name:'Updated needle'}}]});
    expect((await request('nativeEntityPage',{scope:'s',entity:'tasks',ids:['missing','a'],expected:native.value.generation})).value.rows).toEqual([{id:'missing',value:null},{id:'a',value:{_id:'a',name:'Updated needle'}}]);
    expect((await request('nativeWindowPage',{scope:'s',signature:header.signature,generation:'old'})).error.code).toBe('PAGE_CHANGED');
    expect((await request('nativeEntityPage',{scope:'s',entity:'tasks',ids:['a'],expected:{epoch:999,sequence:0}})).error.code).toBe('PAGE_CHANGED');expect(changes.length).toBe(beforeReads);
    const oldClock=(await request('canonicalGeneration',{scope:'identity'})).value;
    const session={scope:'identity',method:'saveSession',args:['identity',null]};
    await request('canonicalCall',session,'session-transport-one','session-stable');
    const newClock=(await request('canonicalGeneration',{scope:'identity'})).value;expect(newClock.sequence).toBe(oldClock.sequence+1);
    const beforeReplay=changes.length;await request('canonicalCall',session,'session-transport-two','session-stable');
    expect((await request('canonicalGeneration',{scope:'identity'})).value).toEqual(newClock);expect(changes.length).toBe(beforeReplay);

  } finally {
    interval.mockRestore(); vi.unstubAllGlobals(); delete (globalThis as any).onmessage; db.close();
  }
});
