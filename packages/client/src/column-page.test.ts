import { describe, it, expect, vi } from 'vitest';
import { packColumnPage, columnPageReader, columnPageTransfers, type ColumnProjection } from './column-page.js';
import { LocalReplica } from './local-replica.js';

const limits = { maxRows: 200, maxPayloadBytes: 64 * 1024 };
const projection: ColumnProjection = { title: { kind: 'string', nullable: true }, count: { kind: 'u32' }, active: { kind: 'boolean', nullable: true } };

describe('column pages', () => {
  it('preserves order, duplicates, absent rows, absent fields, NULL and empty/zero/false', () => {
    const rows = new Map<string, Record<string, unknown>>([
      ['a', { title: '', count: 0, active: false }], ['b', { title: null, active: null }],
    ]);
    const page = packColumnPage(['b','a','missing','a'], projection, id => rows.get(id), limits);
    const read = columnPageReader(page);
    expect(page.ids).toEqual(['b','a','missing','a']);
    expect(page.completeProjection).toBe(false);
    expect(read.value('title',0)).toBeNull();
    expect(read.value('count',0)).toBeUndefined();
    expect(read.value('active',0)).toBeNull();
    expect(read.value('title',1)).toBe('');
    expect(read.value('count',1)).toBe(0);
    expect(read.value('active',1)).toBe(false);
    expect(read.value('title',2)).toBeUndefined();
    expect(read.value('title',3)).toBe('');
  });

  it('stores repeated Unicode text once and never touches cold properties', () => {
    const cold = vi.fn(() => { throw new Error('cold content was read'); });
    const row = { title: 'Revisión 🚀' };
    Object.defineProperty(row, 'description', { get: cold, enumerable: true });
    const page = packColumnPage(['a','b'], { title: { kind: 'string' } }, () => row, limits);
    expect(page.completeProjection).toBe(true);
    expect(page.columns.title!.values).toBeInstanceOf(Uint8Array);
    expect(page.columns.title!.dictionary!.offsets.length).toBe(2);
    expect(columnPageReader(page).value('title',1)).toBe(row.title);
    expect(cold).not.toHaveBeenCalled();
  });

  it('preserves exact signed 64-bit boundaries and rejects already-rounded numbers', () => {
    const rows = [{ value: -(1n<<63n) }, { value: (1n<<63n)-1n }];
    const page = packColumnPage(['0','1'], { value: { kind:'i64' } }, id => rows[Number(id)], limits);
    const read = columnPageReader(page);
    expect(read.value('value',0)).toBe(-(1n<<63n));
    expect(read.value('value',1)).toBe((1n<<63n)-1n);
    expect(() => packColumnPage(['a'], { value:{kind:'i64'} }, () => ({value:Number.MAX_SAFE_INTEGER+1}),limits)).toThrow(/exact/);
    expect(() => packColumnPage(['a'], { value:{kind:'i64'} }, () => ({value:1n<<63n}),limits)).toThrow(/64-bit range/);
  });

  it('rejects overflow, nonfinite values, malformed text and invalid nulls instead of narrowing silently', () => {
    for (const [kind,value] of [['u32',-1],['u32',2**32],['i32',2**31],['i32',1.5],['f64',NaN],['f64',Infinity],['boolean',1],['string','\ud800']] as const)
      expect(() => packColumnPage(['a'],{value:{kind}},()=>({value}),limits)).toThrow();
    expect(() => packColumnPage(['a'],{value:{kind:'string'}},()=>({value:null}),limits)).toThrow(/nullable/);
    const page=packColumnPage(['a'],{value:{kind:'f64'}},()=>({value:-0}),limits);
    expect(Object.is(columnPageReader(page).value('value',0),-0)).toBe(true);
  });

  it('checks row/byte budgets before reading and widens dictionary slots at the exact boundary', () => {
    const read=vi.fn(()=>({title:'x'}));
    expect(()=>packColumnPage(['a'],projection,read,{maxRows:0,maxPayloadBytes:1000})).toThrow(/row budget/);
    expect(()=>packColumnPage(['a'],projection,read,{maxRows:1,maxPayloadBytes:0})).toThrow(/payload budget/);
    expect(read).not.toHaveBeenCalled();
    expect(()=>packColumnPage(['a'],{title:{kind:'string'}},()=>({title:'x'.repeat(1000)}),{maxRows:1,maxPayloadBytes:100})).toThrow(/payload budget/);
    const ids=Array.from({length:257},(_,i)=>String(i));
    const page=packColumnPage(ids,{title:{kind:'string'}},id=>({title:id}),{maxRows:257,maxPayloadBytes:100_000});
    expect(page.columns.title!.values).toBeInstanceOf(Uint16Array);
    expect(columnPageReader(page).value('title',256)).toBe('256');
    const bytes = columnPageTransfers(page).reduce((sum, buffer) => sum + buffer.byteLength, 0)
      + ids.reduce((sum, id) => sum + new TextEncoder().encode(id).byteLength, 0) + new TextEncoder().encode('title').byteLength;
    expect(page.payloadBytes).toBe(bytes);
    expect(() => packColumnPage(ids,{title:{kind:'string'}},id=>({title:id}),{maxRows:257,maxPayloadBytes:bytes-1})).toThrow(/payload budget/);
  });

  it('transfers only projection-owned buffers and detects use after transfer', () => {
    const source={title:'shared'};
    const page=packColumnPage(['a'],{title:{kind:'string'}},()=>source,limits);
    const before=columnPageReader(page);
    const moved=structuredClone(page,{transfer:columnPageTransfers(page)});
    expect(columnPageReader(moved).value('title',0)).toBe('shared');
    expect(()=>before.value('title',0)).toThrow(/Detached/);
    expect(source.title).toBe('shared');
  });
});

describe('Local Replica column projection', () => {
  it('reads the same replica and optimistic journal without cloning cold records', async () => {
    const replica=new LocalReplica();
    await replica.replaceWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'_id',rows:[{_id:'a',title:'Before',count:1,active:true,description:{large:'cold'}}],completeness:'complete',source:'server'});
    const cloning=vi.spyOn(globalThis,'structuredClone');
    const page=replica.entityColumns('tasks',['a'],projection,limits);
    expect(cloning).not.toHaveBeenCalled();cloning.mockRestore();
    expect(page.completeProjection).toBe(true);
    expect(columnPageReader(page).value('title',0)).toBe('Before');
    replica.applyOptimistic('edit',[{entity:'tasks',rowId:'a',op:'patch',fields:{title:'Predicted'}}]);
    const predicted=replica.entityColumns('tasks',['a'],projection,limits);
    expect(columnPageReader(predicted).value('title',0)).toBe('Predicted');
    expect(columnPageReader(page).value('title',0)).toBe('Before');
    replica.rejectCommand('edit');
    expect(columnPageReader(replica.entityColumns('tasks',['a'],projection,limits)).value('title',0)).toBe('Before');
  });

  it('keeps projected-field coverage separate from entity/query completeness', async () => {
    const replica=new LocalReplica();
    await replica.replaceWindow({signature:'partial',kind:'live',entity:'tasks',key:'_id',rows:[{_id:'a',title:'Cached'}],completeness:'partial',source:'cache'});
    expect(replica.entityCompleteness('tasks')).toBe('partial');
    expect(replica.entityColumns('tasks',['a'],{title:{kind:'string'}},limits).completeProjection).toBe(true);
    expect(replica.entityColumns('tasks',['a'],projection,limits).completeProjection).toBe(false);
    replica.applyOptimistic('delete',[{entity:'tasks',rowId:'a',op:'delete'}]);
    expect(columnPageReader(replica.entityColumns('tasks',['a'],projection,limits)).value('title',0)).toBeUndefined();
    replica.rejectCommand('delete');
    const switched = replica.activateScope('another-tenant');
    // Fail closed immediately, including while persistence is still loading.
    expect(replica.entityColumns('tasks',['a'],projection,limits).completeProjection).toBe(false);
    await switched;
    expect(replica.entityColumns('tasks',['a'],projection,limits).completeProjection).toBe(false);
  });

  it('matches ordinary entity reads across ordered insert, replace, patch and delete intents', async () => {
    const replica = new LocalReplica();
    await replica.replaceWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'_id',rows:[{_id:'a',title:'Initial',count:1,active:true}],completeness:'complete',source:'server'});
    replica.applyOptimistic('insert',[{entity:'tasks',rowId:'b',op:'insert',fields:{title:'New',count:2,active:false}}]);
    replica.applyOptimistic('replace',[{entity:'tasks',rowId:'a',op:'upsert',fields:{title:'Replacement',count:3}}]);
    replica.applyOptimistic('patch',[{entity:'tasks',rowId:'a',op:'patch',fields:{active:null}}]);
    replica.applyOptimistic('delete',[{entity:'tasks',rowId:'b',op:'delete'}]);
    const ids = ['b','a','a','missing'];
    const page = replica.entityColumns('tasks',ids,projection,limits);
    const read = columnPageReader(page);
    const ordinary = replica.entityBatch('tasks',ids);
    for (const [i, row] of ordinary.entries()) for (const column of Object.keys(projection))
      expect(read.value(column,i)).toEqual(row?.[column]);
    expect(page.replicaVersion).toBe(replica.version());
    expect(page.freshness).toBe(replica.freshness());
    // Caller mutation of a projection cannot change the authoritative replica.
    (page.columns.count!.values as Uint32Array)[1] = 99;
    expect(replica.entity('tasks','a')?.count).toBe(3);
  });
});
