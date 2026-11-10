import{test}from'node:test';import assert from'node:assert/strict';
import{compileLocalSchema}from'../dist/local-schema.js';import{renderLocalDataModel}from'../dist/local-data-model.js';
test('model includes unexposed SQL tables and gives real relationships distinct identity types',async()=>{
 const schema=await compileLocalSchema([{name:'tables.sql',sql:`CREATE TABLE tasks(_id TEXT PRIMARY KEY,name TEXT); CREATE TABLE notes(_id TEXT PRIMARY KEY,taskId TEXT REFERENCES tasks(_id),count INTEGER,enabled BOOLEAN); CREATE TABLE privateNumbers(id INTEGER PRIMARY KEY,value REAL);`}]);
 const source=renderLocalDataModel(schema,{});
 assert.match(source,/'?"notes"'?:\{/);assert.match(source,/"taskId":Id<"tasks">\|null/);
 assert.match(source,/"count":number\|null/);assert.match(source,/"enabled":boolean\|null/);
 assert.match(source,/IntegerKeyTables="privateNumbers"/);assert.doesNotMatch(source,/\bGo\b|manifest\.json/);
});
