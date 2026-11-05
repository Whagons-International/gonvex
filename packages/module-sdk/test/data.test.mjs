import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compileDataRead, selectRows, getRow, matchesDataPredicate, lockData, existingDataTables, deleteDataRows } from '../dist/index.js';

test('structured reads bind values and preserve boolean grouping and null ordering', () => {
  assert.deepEqual(compileDataRead({table:'tasks',columns:['_id'],where:{and:[{column:'statusId',op:'in',values:['new','progress']},{or:[{column:'deletedAt',op:'isNull'},{column:'id',op:'gt',value:5}]}]},orderBy:[{column:'id',direction:'desc'}],limit:10}), {
    statement:'SELECT "_id" FROM "tasks" WHERE ("statusId" IN (SELECT value FROM json_each($1)) AND ("deletedAt" IS NULL OR "id" > $2)) ORDER BY "id" DESC NULLS FIRST, "_id" ASC NULLS LAST LIMIT $3',parameters:['["new","progress"]',5,10],
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
test('SQLite dialect: no row locks, JSON array membership and portable text transforms', () => {
  const read = compileDataRead({table:'tasks',columns:['_id'],where:{and:[{column:'tags',op:'arrayContains',value:'urgent'},{column:'name',transform:'lowerTrim',op:'eq',value:'x'},{column:'code',op:'matches',pattern:'^A'},{column:'id',op:'in',values:[]}]},orderBy:[{column:'name',transform:'numericSuffix'}],lock:'update'},{ordered:false});
  assert.equal(read.statement, `SELECT "_id" FROM "tasks" WHERE ((CASE WHEN json_type("tags") = 'array' THEN EXISTS (SELECT 1 FROM json_each("tags") AS element WHERE element.type = 'text' AND element.value = $1) ELSE 0 END) AND lower(trim("name")) = $2 AND "code" REGEXP $3 AND 0) ORDER BY CAST(NULLIF(substr("name", length(rtrim("name", '0123456789')) + 1), '') AS REAL) ASC NULLS LAST`);
  assert.deepEqual(read.parameters, ['urgent','x','^A']);
  assert.doesNotMatch(read.statement, /FOR UPDATE|::|ANY\(|jsonb|btrim/);
});
test('server writes and locks use SQLite statements only', async () => {
  const calls=[];
  const db={query:async(sql,parameters)=>{calls.push([sql,parameters]);return sql.startsWith('DELETE')?[{1:1},{1:1}]:[{name:'tasks'}]}};
  await lockData(db,'inventory');
  assert.deepEqual(calls,[],'single-writer tenants need no advisory lock statement');
  assert.deepEqual([...await existingDataTables(db,['tasks','missing'])],['tasks']);
  assert.equal(await deleteDataRows(db,'tasks',['a','b','a']),2);
  assert.deepEqual(calls,[
    [`SELECT name FROM sqlite_schema WHERE type IN ('table', 'view') AND name IN (SELECT value FROM json_each($1))`,['["tasks","missing"]']],
    ['DELETE FROM "tasks" WHERE "_id" IN (SELECT value FROM json_each($1)) RETURNING 1',['["a","b"]']],
  ]);
});
