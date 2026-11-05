/** Source metadata only. Never opens a tenant DB, server, or row dump. */
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const [manifestPath,catalogPath,outputPath]=process.argv.slice(2);
if(!outputPath)throw Error('usage: generate-whagons-provider-fixture.mjs MANIFEST PG_SOURCE_CATALOG OUTPUT');
const bytes=await readFile(manifestPath);const manifest=JSON.parse(bytes);
const catalog=JSON.parse(await readFile(catalogPath,'utf8'));
const functions=Object.fromEntries(Object.entries(manifest.functions).filter(([,f])=>f.replica).map(([path,f])=>[path,{...f}]));
const fixture={format:'whagons-source-policy-fixture-v1',sourcePin:'814415ca0b060c7513eca3c7ab6db17fe15304ab',sourceManifestSHA256:createHash('sha256').update(bytes).digest('hex'),qualification:'Complete static source definitions; synthetic physical schema profiles ONLY. Not production-copy identity/schema/timestamp qualification.',moduleSchema:manifest.schema,visibility:manifest.visibility,functions,sourceSchema:catalog};
await writeFile(outputPath,JSON.stringify(fixture,null,2)+'\n');
console.log(JSON.stringify({policies:Object.keys(fixture.visibility).length,replicaDefinitions:Object.keys(functions).length,sourceTables:Object.keys(catalog).length,qualification:fixture.qualification}));
