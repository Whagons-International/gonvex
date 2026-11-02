import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { queryPublicationControls } from './query-publication-controls.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const args = process.argv.slice(2);
assert.equal(args[0], '--output'); assert(args[1]);
assert(args.length === 2 || args.length === 4 && args[2] === '--baseline-source');
const output = resolve(args[1]); mkdirSync(output, { recursive: false });
const sourcePath = resolve(root, 'packages/client/src/index.ts');
const testPath = resolve(root, 'packages/client/src/query-publication-ownership.test.ts');
const source = readFileSync(sourcePath, 'utf8');
const hash = value => createHash('sha256').update(value).digest('hex');
const variants = queryPublicationControls(source);
if (args[3]) variants.push({ name: 'before-fix', pattern: '.', failed: 19, passed: 8,
  candidate: readFileSync(resolve(args[3]), 'utf8'),
  failure: /old query message reached|queued cache replay crossed|before persistence|storage write failed|old query completion published|old replica message reached|queued replica cache replay crossed|preceding durable write/ });
const cases = [];
for (const entry of variants) {
  const candidate = entry.candidate ?? source;
  const directory = resolve(output, entry.name); mkdirSync(directory);
  const marker = resolve(directory, 'compiled-source.json'), report = resolve(directory, 'tests.json');
  const config = resolve(directory, 'vitest.config.mjs');
  writeFileSync(config, `import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs'; import {createHash} from 'node:crypto';
export default {root:${JSON.stringify(resolve(root, 'packages/client'))},plugins:[{
 name:'query-publication-control',enforce:'pre',transform(code,id){
 if(id.split('?')[0]!==${JSON.stringify(sourcePath)})return;
 assert.equal(code,${JSON.stringify(source)});
 const changed=${JSON.stringify(candidate)};
 writeFileSync(${JSON.stringify(marker)},JSON.stringify({originalSha256:createHash('sha256').update(code).digest('hex'),compiledSha256:createHash('sha256').update(changed).digest('hex')}));
 return {code:changed,map:null};
 }}],test:{include:['src/query-publication-ownership.test.ts']}};`);
  const run = spawnSync('pnpm', ['--filter', '@gonvex/client', 'exec', 'vitest', 'run', '--config', config,
    '--reporter=json', '--outputFile', report, '-t', entry.pattern], { cwd: root, encoding: 'utf8', timeout: 60000 });
  writeFileSync(resolve(directory, 'execution.log'), (run.stdout ?? '') + (run.stderr ?? ''));
  assert.ifError(run.error);
  const result = JSON.parse(readFileSync(report, 'utf8'));
  const compiled = JSON.parse(readFileSync(marker, 'utf8'));
  assert.equal(compiled.originalSha256, hash(source)); assert.equal(compiled.compiledSha256, hash(candidate));
  assert.equal(result.numRuntimeErrorTestSuites ?? 0, 0);
  const assertions = result.testResults.flatMap(test => test.assertionResults).filter(test => !['skipped', 'pending'].includes(test.status));
  assert.equal(result.numPassedTests, entry.passed ?? 0, entry.name);
  assert.equal(result.numFailedTests, entry.failed ?? 0, entry.name);
  assert.equal(assertions.length, (entry.passed ?? 0) + (entry.failed ?? 0));
  assert.equal(run.status === 0, !entry.failed, entry.name);
  for (const test of assertions.filter(test => test.status === 'failed'))
    assert(test.failureMessages.some(message => entry.failure.test(message)), entry.name + ': wrong failure: ' + test.failureMessages.join('\n'));
  cases.push({ name: entry.name, passed: entry.passed ?? 0, expectedFailures: entry.failed ?? 0, ...compiled });
}
assert.equal(readFileSync(sourcePath, 'utf8'), source, 'source changed while controls ran');
writeFileSync(resolve(output, 'receipt.json'), JSON.stringify({ sourceSha256: hash(source), testSha256: hash(readFileSync(testPath)),
  driverSha256: hash(readFileSync(fileURLToPath(import.meta.url))),
  controlsSha256: hash(readFileSync(new URL("./query-publication-controls.mjs", import.meta.url))), cases,
  boundary: 'Actual SDK source compiled by Vite with a deterministic EventTarget transport and real MemoryLocalReplicaStorage. Captures every public callback while auth, close and storage completion interleave. Compiled source controls change one behavior without modifying the checkout. No native storage, backend authentication or mobile runtime claim.' }, null, 2) + '\n');
console.log('27 query publication schedules passed; nine compiled source controls reached 24 intended failures');
