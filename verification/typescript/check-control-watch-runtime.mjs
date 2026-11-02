import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { controlWatchControls } from './control-watch-controls.mjs';
import { extractControlWatch } from './control-watch-publication.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const args = process.argv.slice(2);
assert.equal(args[0], '--output'); assert(args[1]);
assert(args.length === 2 || args.length === 4 && args[2] === '--baseline-source');
const output = resolve(args[1]); mkdirSync(output, { recursive: false });
const sourcePath = resolve(root, 'packages/client/src/index.ts');
const testPath = resolve(root, 'packages/client/src/control-watch-ownership.test.ts');
const source = readFileSync(sourcePath, 'utf8');
const hash = value => createHash('sha256').update(value).digest('hex');
const variants = controlWatchControls(source, extractControlWatch(source).method);
if (args[3]) variants.push({ name: 'before-fix', pattern: '.', failed: 2, passed: 36,
  candidate: readFileSync(resolve(args[3]), 'utf8'),
  failure: /old Control Query|inactive Control Query|closed Control Query|resumed Control Query|scope observer read|native close suppressed/ });
const cases = [];
for (const entry of variants) {
  const candidate = entry.candidate ?? source;
  const directory = resolve(output, entry.name); mkdirSync(directory);
  const marker = resolve(directory, 'compiled-source.json'), report = resolve(directory, 'tests.json');
  const config = resolve(directory, 'vitest.config.mjs');
  const observations = resolve(directory, 'native-close.jsonl');
  writeFileSync(config, `import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs'; import {createHash} from 'node:crypto';
export default {root:${JSON.stringify(resolve(root, 'packages/client'))},plugins:[{
 name:'control-watch-control',enforce:'pre',transform(code,id){
 if(id.split('?')[0]!==${JSON.stringify(sourcePath)})return;
 assert.equal(code,${JSON.stringify(source)});
 const changed=${JSON.stringify(candidate)};
 writeFileSync(${JSON.stringify(marker)},JSON.stringify({originalSha256:createHash('sha256').update(code).digest('hex'),compiledSha256:createHash('sha256').update(changed).digest('hex')}));
 return {code:changed,map:null};
 }}],test:{include:['src/control-watch-ownership.test.ts','src/control-watch-close-order.test.ts']}};`);
  const run = spawnSync('pnpm', ['--filter', '@gonvex/client', 'exec', 'vitest', 'run', '--config', config,
    '--reporter=json', '--outputFile', report, '-t', entry.pattern], { cwd: root, encoding: 'utf8', timeout: 60000, env: { ...process.env, GONVEX_CONTROL_CLOSE_OBSERVATIONS: observations } });
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
  const nativeCloseObservations = existsSync(observations) ? readFileSync(observations, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line)) : [];
  cases.push({ nativeCloseObservations, name: entry.name, passed: entry.passed ?? 0, expectedFailures: entry.failed ?? 0, ...compiled });
}
assert.equal(readFileSync(sourcePath, 'utf8'), source, 'source changed while controls ran');
writeFileSync(resolve(output, 'receipt.json'), JSON.stringify({ sourceSha256: hash(source), testSha256: hash(readFileSync(testPath)),
  nativeCloseTestSha256: hash(readFileSync(resolve(root, "packages/client/src/control-watch-close-order.test.ts"))),
  driverSha256: hash(readFileSync(fileURLToPath(import.meta.url))),
  controlsSha256: hash(readFileSync(new URL("./control-watch-controls.mjs", import.meta.url))),
  extractorSha256: hash(readFileSync(new URL("./control-watch-publication.mjs", import.meta.url))), cases,
  boundary: 'Actual SDK source compiled by Vite with deterministic EventTarget transport. Records public Control Query watch callbacks and snapshots through account, tenant, visibility, close, inactive reuse and reconnect schedules. Four close/reconnect schedules use an actual loopback Node ws server and real close events. Control-role tests perform no storage writes; the visibility fixture uses the real memory adapter. Compiled source controls change one behavior without modifying the checkout. No native storage, backend authentication or mobile runtime claim.' }, null, 2) + '\n');
console.log('38 Control Query watch schedules passed; twelve compiled source controls reached 20 intended failures');
