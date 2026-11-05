/** Source metadata only. Never opens a tenant DB, server, or row dump. */
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const [manifestPath,catalogPath,outputPath]=process.argv.slice(2);
if(!outputPath)throw Error('usage: generate-whagons-provider-fixture.mjs MANIFEST PG_SOURCE_CATALOG OUTPUT');
const bytes=await readFile(manifestPath);const manifest=JSON.parse(bytes);
const catalog=JSON.parse(await readFile(catalogPath,'utf8'));
const functions=Object.fromEntries(Object.entries(manifest.functions).filter(([,f])=>f.replica).map(([path,f])=>[path,{...f}]));
const fixture={format:'whagons-source-policy-fixture-v1',sourcePin:'814415ca0b060c7513eca3c7ab6db17fe15304ab',sourceManifestSHA256:createHash('sha256').update(bytes).digest('hex'),qualification:'Complete static source definitions; synthetic physical schema profiles ONLY. Not production-copy identity/schema/timestamp qualification.',moduleSchema:manifest.schema,visibility:manifest.visibility,functions,sourceSchema:catalog};
// Keep each named definition on one generated record line. The full canonical
// object is unchanged; this avoids over 100k lines of repeated indentation.
const sections=new Set(['visibility','functions','sourceSchema']);
const lines=Object.entries(fixture).map(([key,value])=>{
 if(!sections.has(key))return `  ${JSON.stringify(key)}: ${JSON.stringify(value)}`;
 return `  ${JSON.stringify(key)}: {\n`+Object.entries(value).map(([name,record])=>`    ${JSON.stringify(name)}: ${JSON.stringify(record)}`).join(',\n')+'\n  }';
});
await writeFile(outputPath,'{\n'+lines.join(',\n')+'\n}\n');
console.log(JSON.stringify({policies:Object.keys(fixture.visibility).length,replicaDefinitions:Object.keys(functions).length,sourceTables:Object.keys(catalog).length,qualification:fixture.qualification}));
