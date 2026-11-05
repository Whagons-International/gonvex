import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, resolve } from 'node:path';
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
/** Select an explicitly pinned experimental SDK without modifying the app's
 * dependency graph. A composed artifact also verifies its complete payload. */
export async function sdkArtifact(installedEntry, env=process.env) {
 const entry=env.LOAD_SDK_ENTRY||installedEntry;
 const sha256=hash(await readFile(entry));
 if(env.LOAD_SDK_ENTRY&&(!/^[a-f0-9]{64}$/.test(env.LOAD_SDK_ENTRY_SHA256||'')||sha256!==env.LOAD_SDK_ENTRY_SHA256))throw Error('SDK entry override requires its exact SHA256');
 let composition;
 if(env.LOAD_SDK_PROVENANCE){
  const p=resolve(env.LOAD_SDK_PROVENANCE),root=dirname(p);composition=JSON.parse(await readFile(p,'utf8'));
  if(composition.composedClientJsSha256!==sha256||!composition.outputFiles)throw Error('SDK composition does not pin this entry');
  for(const [name,expected]of Object.entries(composition.outputFiles)){
   const path=resolve(root,name);if(!path.startsWith(root+'/')||hash(await readFile(path))!==expected)throw Error('SDK composition payload mismatch: '+name);
  }
 }
 return {entry,sha256,override:entry!==installedEntry,experimentalIdentity:composition?.experimentalIdentity,payloadTreeSHA256:composition?.outputPackageTreeHash,existingPatchSHA256:composition?.existingPatchSha256};
}
