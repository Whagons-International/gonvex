import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import crypto from 'node:crypto';
const artifact = fs.realpathSync(process.argv[2]);
const output = process.argv[3];
if (!artifact.startsWith('/mnt/data/work/') || !output?.startsWith('/mnt/data/work/') || fs.existsSync(output)) throw Error('fresh isolated paths required');
const core = '728b857e456d6d7a9f299883fdf1ba17c8e594b9';
const source = cp.execFileSync('git',['show',`${core}:packages/client/src/sdk-resume-hydration.test.ts`],{encoding:'utf8'});
let adapted = source;
for (const name of ['index','local-replica','replica-integrity']) {
  const from=`"./${name}.js"`; if (adapted.split(from).length !== 2) throw Error('import anchor drift');
  adapted=adapted.replace(from,JSON.stringify(path.join(artifact,'dist',name+'.js')));
}
if (/from ["']\.\//.test(adapted)) throw Error('unexpected relative SDK import');
fs.mkdirSync(output,{recursive:true});
fs.writeFileSync(path.join(output,'artifact-core.test.ts'),adapted);
fs.symlinkSync(path.resolve('packages/client/node_modules'),path.join(output,'node_modules'));
fs.writeFileSync(path.join(output,'vitest.config.mjs'),`export default ${JSON.stringify({root:output,cacheDir:path.join(output,'vite-cache'),test:{maxWorkers:2,fileParallelism:false,cache:false,include:['artifact-core.test.ts']}})};\n`);
fs.writeFileSync(path.join(output,'test-provenance.json'),JSON.stringify({coreTestSourceCommit:core,originalTestSha256:crypto.createHash('sha256').update(source).digest('hex'),adaptedTestSha256:crypto.createHash('sha256').update(adapted).digest('hex'),artifact,sourceFallback:false,rewrittenImports:['index.js','local-replica.js','replica-integrity.js']},null,2)+'\n');
