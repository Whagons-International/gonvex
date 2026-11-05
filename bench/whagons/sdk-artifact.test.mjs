import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,writeFile,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { sdkArtifact } from './sdk-artifact.mjs';
test('experimental SDK selection rejects stale entries and changed payload members before execution',async()=>{
 const root=await mkdtemp(join(tmpdir(),'sdk-selection-'));
 try{
  const entry=join(root,'index.js'),part=join(root,'part.js'),provenance=join(root,'composition.json');
  const sha=x=>createHash('sha256').update(x).digest('hex');
  await writeFile(entry,'export const actual = 1;');await writeFile(part,'export const part = 2;');
  const expected=sha('export const actual = 1;');
  const env={LOAD_SDK_ENTRY:entry,LOAD_SDK_ENTRY_SHA256:expected,LOAD_SDK_PROVENANCE:provenance};
  await writeFile(provenance,JSON.stringify({composedClientJsSha256:expected,outputFiles:{'index.js':expected,'part.js':sha('export const part = 2;')},experimentalIdentity:'fixture-only'}));
  assert.equal((await sdkArtifact('unused',env)).experimentalIdentity,'fixture-only');
  await writeFile(part,'changed');await assert.rejects(sdkArtifact('unused',env),/payload mismatch/);
  await assert.rejects(sdkArtifact('unused',{LOAD_SDK_ENTRY:entry}),/exact SHA256/);
  await assert.rejects(sdkArtifact('unused',{...env,LOAD_SDK_ENTRY_SHA256:'0'.repeat(64)}),/exact SHA256/);
 }finally{await rm(root,{recursive:true,force:true});}
});
