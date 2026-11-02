import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { extractWatermarkPublication } from './watermark-publication.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const args = process.argv.slice(2);
assert.equal(args.length, 2); assert.equal(args[0], '--output');
const output = resolve(args[1]); mkdirSync(output, { recursive: false });
const sourcePath = resolve(root, 'packages/client/src/index.ts');
const testPath = resolve(root, 'packages/client/src/sdk-persistence-barrier.test.ts');
const source = readFileSync(sourcePath, 'utf8');
const extracted = extractWatermarkPublication(source);
const hash = value => createHash('sha256').update(value).digest('hex');
const cases = [
  { name: 'candidate', pattern: 'does not publish an old tenant|requires the current activation|does not let old ready persistence', count: 12 },
  { name: 'missing-post-await-guard', pattern: 'does not publish an old tenant', count: 4,
    before: extracted.guard, after: 'if (false) return;', failure: /outcome.*success/s },
  { name: 'missing-return-activation', pattern: 'requires the current activation', count: 4,
    before: 'activation !== this.receiptReconciliationGeneration', after: 'false', failure: /expected 1000 to be/ },
  { name: 'missing-ready-publication-guard', pattern: 'does not let old ready persistence', count: 4,
    ready: true, before: extracted.readyPublication.guard, after: 'if (false) return;', failure: /outcome.*pending/s },
];
const results = [];
for (const entry of cases) {
  const directory = resolve(output, entry.name); mkdirSync(directory);
  const originalMethod = entry.ready ? extracted.readyPublication.source : extracted.source;
  let method = originalMethod;
  if (entry.before) {
    assert.equal(method.split(entry.before).length, 2);
    method = method.replace(entry.before, entry.after);
  }
  const candidate = source.replace(originalMethod, method);
  const marker = resolve(directory, 'compiled-source.json');
  const report = resolve(directory, 'tests.json');
  const config = resolve(directory, 'vitest.config.mjs');
  // Vite compiles the selected source normally after this narrowly scoped
  // transform. The checkout and its built distribution are never modified.
  writeFileSync(config, `import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
export default {root:${JSON.stringify(resolve(root, 'packages/client'))},plugins:[{
name:'watermark-publication-control',enforce:'pre',transform(code,id){
 if(id.split('?')[0]!==${JSON.stringify(sourcePath)})return;
 assert.equal(code,${JSON.stringify(source)});
 const changed=code.replace(${JSON.stringify(originalMethod)},${JSON.stringify(method)});
 writeFileSync(${JSON.stringify(marker)},JSON.stringify({originalSha256:createHash('sha256').update(code).digest('hex'),compiledSha256:createHash('sha256').update(changed).digest('hex')}));
 return {code:changed,map:null};
}}],test:{include:['src/sdk-persistence-barrier.test.ts']}};
`);
  const run = spawnSync('pnpm', ['--filter', '@gonvex/client', 'exec', 'vitest', 'run', '--config', config,
    '--reporter=json', '--outputFile', report, '-t', entry.pattern], { cwd: root, encoding: 'utf8', timeout: 60000 });
  writeFileSync(resolve(directory, 'execution.log'), (run.stdout ?? '') + (run.stderr ?? ''));
  assert.ifError(run.error);
  const result = JSON.parse(readFileSync(report, 'utf8'));
  assert(existsSync(marker), 'SDK source was not compiled: ' + result.testResults.map(test => test.message).join('\n'));
  const compiled = JSON.parse(readFileSync(marker, 'utf8'));
  assert.equal(compiled.originalSha256, hash(source)); assert.equal(compiled.compiledSha256, hash(candidate));
  assert.equal(result.numRuntimeErrorTestSuites ?? 0, 0);
  const assertions = result.testResults.flatMap(test => test.assertionResults).filter(test => test.status !== 'skipped' && test.status !== 'pending');
  assert.equal(assertions.length, entry.count);
  if (entry.before) {
    assert.notEqual(run.status, 0);
    assert.equal(result.numFailedTests, entry.count);
    assert(assertions.every(test => test.status === 'failed' && test.failureMessages.some(message => entry.failure.test(message))),
      'source control did not reach the intended durable-watermark assertion');
  } else {
    assert.equal(run.status, 0); assert.equal(result.numPassedTests, entry.count); assert.equal(result.numFailedTests, 0);
  }
  results.push({ name: entry.name, assertions: entry.count, expectedFailure: !!entry.before, ...compiled });
}
assert.equal(readFileSync(sourcePath, 'utf8'), source, 'candidate source changed during controls');
writeFileSync(resolve(output, 'receipt.json'), JSON.stringify({ sourceSha256: hash(source), testSha256: hash(readFileSync(testPath)),
  driverSha256: hash(readFileSync(fileURLToPath(import.meta.url))), cases: results,
  boundary: 'Public SDK wire delivery with deterministic transport. Memory, fake IndexedDB through both actual browser adapters and actual in-memory Node SQLite. Holds real storage completion after its commit and rotates public authentication. No real backend, browser IndexedDB, Expo driver or OS process failure is claimed by this fixture.' }, null, 2) + '\n');
console.log('12 watermark/readiness ownership schedules passed; three compiled source controls failed their four intended assertions');
