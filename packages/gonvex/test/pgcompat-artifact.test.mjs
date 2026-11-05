import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, mkdir, readFile, writeFile, symlink, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { PGlite } from '@electric-sql/pglite';

const baselineSDK = process.env.GONVEX_PG_BASELINE_SDK;
const baselineCLI = process.env.GONVEX_PG_BASELINE_CLI;
const pairedSDK = process.env.GONVEX_PG_PAIRED_SDK;
const source = `import { reducer, schema, visibility, selectRows, selectDataBatch, insertDataRows, updateDataRows, updateDataWhere, deleteDataRows } from '@gonvex/module-sdk';
export const policy = visibility({table:'items',key:'_id',sets:{owned:{kind:'grant',table:'items',select:'owner',joins:[],where:[{table:'items',column:'owner',context:'member.id'}]}},where:{operator:'inSet',column:'owner',set:'owned'}});
export const read = reducer({args:schema.any(),result:schema.any(),run: async ctx => {
 const reads = [{table:'items',columns:['_id','title','tags'],where:{and:[{column:'_id',op:'in',values:['a','b','c','d']},{or:[{column:'tags',op:'arrayContains',value:'blue'},{column:'title',transform:'lowerTrim',op:'eq',value:'mixed'}]}]},orderBy:[{column:'_id'}]},
 {table:'items',columns:['_id'],where:{column:'_id',op:'in',values:[]}},
 {table:'items',columns:['_id'],where:{column:'title',op:'matches',pattern:'^[a-z]+$'}},
 {table:'items',columns:['_id'],where:{column:'owner',op:'isNull'}}];
 return selectDataBatch(ctx.db,reads);
}});
export const mutate = reducer({args:schema.any(),result:schema.any(),run: async ctx => {
 await insertDataRows(ctx,'items',[{_id:'z',owner:'member',title:'inserted',tags:['blue'],count:1}]);
 await updateDataRows(ctx.db,'items',[{id:'a',fields:{title:'updated',tags:['green'],count:2}}]);
 const updated=await updateDataWhere(ctx.db,{table:'items',where:{column:'_id',op:'in',values:['b','z']}},{count:9});
 const deleted=await deleteDataRows(ctx.db,'items',['c','missing','c']);
 return {updated,deleted,rows:await selectRows(ctx.db,{table:'items',orderBy:[{column:'_id'}]})};
}});`;
const migration = `CREATE TABLE items ("_id" text PRIMARY KEY, owner text, title text NOT NULL, tags jsonb, count integer NOT NULL DEFAULT 0);`;

// Explicit external artifacts only. Ordinary SQLite framework tests never load
// published PG helpers, access the network or rewrite their schema to pass.
test('paired PG extension preserves baseline closure and actual generated SQL execution', {skip: !baselineSDK || !baselineCLI || !pairedSDK}, async t => {
  const newCLI = new URL('../dist/', import.meta.url);
  const oldCLI = pathToFileURL(resolve(baselineCLI) + '/');
  const results = [];
  for (const [name, sdk, cli] of [['baseline',baselineSDK,oldCLI],['paired',pairedSDK,newCLI]]) {
    const root = await mkdtemp(join(tmpdir(),'gonvex-pgcompat-'+name+'-'));
    t.after(()=>rm(root,{recursive:true,force:true}));
    await mkdir(join(root,'gonvex')); await mkdir(join(root,'migrations'));
    await mkdir(join(root,'node_modules','@gonvex'),{recursive:true});
    await symlink(sdk,join(root,'node_modules','@gonvex','module-sdk'));
    await writeFile(join(root,'gonvex.json'),JSON.stringify({module:{schemaDialect:'postgres'}}));
    await writeFile(join(root,'migrations','0000.sql'),migration);
    const file=join(root,'gonvex','index.ts'); await writeFile(file,source);
    const {buildModuleArtifact,moduleManifestFunctions}=await import(new URL('module-artifact.js',cli));
    const {localBindings}=await import(new URL('local-bindings.js',cli));
    const module=await buildModuleArtifact({root,backendDir:join(root,'gonvex'),files:[file],migrations:[]});
    const functions=moduleManifestFunctions(module);
    const generated=await localBindings(root,{module,functions});
    for(const [name,body] of Object.entries(generated))await writeFile(join(root,'gonvex',name),body);
    const reducers=await import(pathToFileURL(join(root,'gonvex','local-reducers.js')));
    const pg=new PGlite(); const sql=[];
    try {
      await pg.exec(migration);
      await pg.exec(`INSERT INTO items VALUES ('a','member','alpha','["blue"]',0),('b',NULL,' MIXED ','[]',0),('c','other','gamma','["red"]',0),('d','member','delta',NULL,0)`);
      const db={query:async(statement,parameters=[])=>{sql.push({statement,parameters});return (await pg.query(statement,parameters)).rows}};
      const ctx={db};
      const read=await(await reducers.localReducers.read()).handler(ctx,{});
      const mutate=await(await reducers.localReducers.mutate()).handler(ctx,{});
      results.push({read,mutate,sql,visibility:module.visibility});
      assert.deepEqual(read[0].map(x=>x._id),['a','b']);
      assert.deepEqual(read[1],[]);
      assert.equal(mutate.updated,2); assert.equal(mutate.deleted,1);
      assert.deepEqual(mutate.rows.map(x=>x._id),['a','b','d','z']);
      assert.ok(sql.some(x=>x.statement.includes('ANY(')));
      assert.ok(sql.some(x=>x.statement.includes('jsonb_populate_recordset')));
      assert.ok(sql.some(x=>x.statement.includes('jsonb_populate_record(')));
      assert.ok(sql.some(x=>x.statement.includes('jsonb_agg')));
      assert.ok(!sql.some(x=>x.statement.includes('json_each')));
    } finally {await pg.close();}
  }
  assert.deepEqual(results[1],results[0], 'old CLI+SDK and new CLI+extended PG SDK must execute identical SQL/parameters/results');
  const baseMeta=JSON.parse(await readFile(join(baselineSDK,'package.json')));
  const pairedMeta=JSON.parse(await readFile(join(pairedSDK,'package.json')));
  baseMeta.version=pairedMeta.version; assert.deepEqual(pairedMeta,baseMeta);
  for(const folder of ['dist','runtime-contract']) {
    for(const file of await readdir(join(baselineSDK,folder))) {
      const old=await readFile(join(baselineSDK,folder,file));
      const next=await readFile(join(pairedSDK,folder,file));
      if(folder==='dist' && ['index.js','index.d.ts'].includes(file)) {
        assert.ok(next.subarray(0,old.length).equals(old));
        assert.match(next.subarray(old.length).toString(),/^\nexport (?:type )?\{/);
      } else assert.ok(next.equals(old),folder+'/'+file+' changed');
    }
  }
  const extension=await import(pathToFileURL(join(pairedSDK,'dist','permission-primitives.js')));
  assert.throws(()=>extension.permissionPrimitives({callback:()=>true}),/literal JSON/);
});
