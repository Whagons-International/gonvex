import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compileDataRead, selectRows, getRow, matchesDataPredicate } from '../dist/index.js';

test('structured reads bind values and preserve boolean grouping and null ordering', () => {
  assert.deepEqual(compileDataRead({table:'tasks',columns:['_id'],where:{and:[{column:'statusId',op:'in',values:['new','progress']},{or:[{column:'deletedAt',op:'isNull'},{column:'id',op:'gt',value:5}]}]},orderBy:[{column:'id',direction:'desc'}],limit:10}), {
    statement:'SELECT "_id" FROM "tasks" WHERE ("statusId" = ANY($1) AND ("deletedAt" IS NULL OR "id" > $2)) ORDER BY "id" DESC NULLS FIRST, "_id" ASC NULLS LAST LIMIT $3',parameters:[['new','progress'],5,10],
  });
});
test('server adapter and local adapter receive the same read intent', async () => {
  const calls=[];const row={_id:'task-1'};
  assert.deepEqual(await getRow({query:async(...args)=>{calls.push(args);return [row]}},'tasks','_id','task-1'),row);
  assert.deepEqual(calls,[[ 'SELECT * FROM "tasks" WHERE "_id" = $1 ORDER BY "_id" ASC NULLS LAST LIMIT $2',['task-1',1] ]]);
  const local=[];
  assert.deepEqual(await selectRows({query:()=>assert.fail('SQL must not execute locally'),select:async read=>{local.push(read);return [row]}},{table:'tasks'}),[row]);
  assert.deepEqual(local,[{table:'tasks',orderBy:[{column:'_id'}]}]);
});
test('invalid identifiers and values fail before either adapter executes', async () => {
  const db={query:()=>assert.fail(),select:()=>assert.fail()};
  for(const read of [{table:'tasks;DROP TABLE tasks'}, {table:'tasks',limit:-1}, {table:'tasks',columns:[]}, {table:'tasks',where:{column:'id',op:'eq',value:undefined}}, {table:'tasks',where:{column:'id',op:'eq',value:Infinity}}]) {
    await assert.rejects(()=>selectRows(db,read));
  }
});
test('null does not pass inequality and empty membership matches nothing', () => {
  assert.equal(matchesDataPredicate({value:null},{column:'value',op:'ne',value:'x'}),false);
  assert.equal(matchesDataPredicate({value:'x'},{column:'value',op:'ne',value:null}),false);
  assert.equal(matchesDataPredicate({value:'x'},{column:'value',op:'in',values:[]}),false);
  assert.equal(matchesDataPredicate({value:null},{column:'value',op:'isNull'}),true);
  assert.equal(matchesDataPredicate({value:3},{and:[{column:'value',op:'gte',value:2},{column:'value',op:'lt',value:4}]}),true);
});
