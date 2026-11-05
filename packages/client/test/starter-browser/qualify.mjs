import assert from 'node:assert/strict';
import {deflateRawSync,inflateRawSync,constants} from 'node:zlib';
import {randomBytes} from 'node:crypto';
import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {qualify} from './codec-fixture.mjs';
const root=process.env.STARTER_BROWSER_ROOT;
assert(root,'explicit lane root');
const {createBrowserStarterInflater:create}=await import(pathToFileURL(root+'/codec.mjs'));
const corpus=[];
for(const size of [0,1,2,7,31,255,256,1023,4095,4096,4097,16383,65535,65536,262144]) {
 for(const strategy of [constants.Z_DEFAULT_STRATEGY,constants.Z_FIXED,constants.Z_HUFFMAN_ONLY]) {
  for(const raw of [Buffer.alloc(size,65),randomBytes(size),Buffer.from(('😀é\\u0000\n{"id":"x","body":"a\\\\b\\n\\\""}\n').repeat(Math.ceil(size/20))).subarray(0,size)]) {
   const wire=deflateRawSync(raw,{level:1,windowBits:12,strategy,finishFlush:constants.Z_SYNC_FLUSH});
   const encoded=wire.subarray(0,-4);
   if(encoded.length>262144)continue;
   assert.deepEqual(inflateRawSync(wire,{windowBits:12,finishFlush:constants.Z_SYNC_FLUSH}),raw);
   assert.deepEqual(inflateRawSync(Buffer.concat([wire,Buffer.from([1,0,0,255,255])])),raw);
   corpus.push({name:`${size}/${strategy}/${corpus.length}`,encoded:[...encoded],raw:[...raw]});
  }
 }
}
if(process.env.STARTER_BROWSER_RUST_ENCODER) {
 for(const c of [...corpus].filter((_,i)=>i%3===0)) {
  const raw=Buffer.from(c.raw), result=execFileSync(process.env.STARTER_BROWSER_RUST_ENCODER,[],{input:raw,maxBuffer:2*1024*1024});
  if(!result[0]) {assert.deepEqual(result.subarray(1),raw);continue;}
  const encoded=result.subarray(1), wire=Buffer.concat([encoded,Buffer.from([0,0,255,255])]);
  assert.deepEqual(inflateRawSync(wire,{windowBits:12,finishFlush:constants.Z_SYNC_FLUSH}),raw);
  assert.deepEqual(inflateRawSync(Buffer.concat([wire,Buffer.from([1,0,0,255,255])])),raw);
  corpus.push({name:'rust/'+c.name,encoded:[...encoded],raw:[...raw]});
 }
}
fs.writeFileSync(root+'/corpus.json',JSON.stringify(corpus));
console.log(JSON.stringify(await qualify(create,corpus)));
// The output limit also rejects a genuine compression bomb without native work.
const bomb=deflateRawSync(Buffer.alloc(4*1024*1024,65),{level:1,windowBits:12,finishFlush:constants.Z_SYNC_FLUSH}).subarray(0,-4);
await assert.rejects((await create())(bomb,{maxOutputBytes:262144,windowBits:12,flush:'sync'}),/output-limit/);
