import {buildModuleArtifact,moduleSourceFiles,moduleManifestFunctions} from '../../gonvex/dist/module-artifact.js';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {readFile,writeFile,readdir,cp,mkdir} from 'node:fs/promises';
// Run only from a copied framework inside the explicitly supplied owned evidence root.
const r=process.argv[2],framework=fileURLToPath(new URL('../../../',import.meta.url));
if(!r || !path.isAbsolute(r) || !framework.startsWith(path.resolve(r)+path.sep))throw Error('owned-evidence-root-required');
const v1=framework+'packages/client/test/explicit-server-fixture',v2=path.resolve(r,'source/native-v2');
await mkdir(v2,{recursive:true});
const {symlink,realpath}=await import('node:fs/promises');
try{if(await realpath(v2+'/node_modules')!==await realpath(framework+'node_modules'))throw Error('unexpected-dependency-link');}catch(error){if(error.code!=='ENOENT')throw error;await symlink(framework+'node_modules',v2+'/node_modules');}
await cp(v1+'/gonvex',v2+'/gonvex',{recursive:true,filter:p=>!p.includes('/_generated')&&!p.includes('/_build')});await cp(v1+'/migrations',v2+'/migrations',{recursive:true});await writeFile(v2+'/gonvex/board.ts',(await readFile(v2+'/gonvex/board.ts','utf8'))+'\n// Same-contract synthetic rebuild, separate exact artifact.\n');
const manifests={};
for(const [label,root] of [['v1',v1],['v2',v2]]){
 const artifact=await buildModuleArtifact({root,backendDir:root+'/gonvex',files:await moduleSourceFiles(root+'/gonvex'),migrations:(await readdir(root+'/migrations')).filter(p=>p.endsWith('.sql')).sort().map(p=>root+'/migrations/'+p),entrypoint:'gonvex/index.ts',split:true});
 manifests[label]={functions:moduleManifestFunctions(artifact),schema:{tables:{},tenantTables:{},controlPlaneTables:{}},module:artifact,visibility:artifact.visibility};
 const code=Buffer.from(artifact.javascript.code,'base64');await mkdir(v1+'/gonvex/_build',{recursive:true});await writeFile(v1+'/gonvex/_build/'+(label==='v1'?'native':'native-v2')+'.module.js',code);
 await writeFile(r+'/artifacts/native-'+label+'-manifest.json',JSON.stringify(manifests[label]));
 if(label==='v1'){const local=await readFile(v1+'/gonvex/_generated/local-executor.ts','utf8');if(!local.includes(JSON.stringify(artifact.hash)))throw Error('generated binding hash differs from actual artifact');}
 console.log(JSON.stringify({label,hash:artifact.hash,functions:Object.keys(artifact.functions).length,javascriptBytes:code.length}));
}
await writeFile(v1+'/gonvex/_build/native-manifests.json',JSON.stringify(manifests));
