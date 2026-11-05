// Generate the signed cross-language fixture with the real source compiler.
import {readFile,writeFile} from 'node:fs/promises';
import {compilePermissionPrimitives} from '../dist/permission-primitives.js';
const source=new URL('../../../rust/crates/runtime/testdata/permission-source-groups.declaration.json',import.meta.url);
const output=new URL('../../../rust/crates/runtime/testdata/permission-source-groups.json',import.meta.url);
const compiled=JSON.stringify(compilePermissionPrimitives(JSON.parse(await readFile(source,'utf8'))),null,2)+'\n';
if(process.argv.includes('--write'))await writeFile(output,compiled);
else if(await readFile(output,'utf8')!==compiled)throw Error('permission source group fixture is stale; regenerate from declaration');
