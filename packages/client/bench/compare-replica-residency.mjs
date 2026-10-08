// Pair three hot-path runs against an unchanged revision with three current
// runs. No checkout or tracked-file mutation. Build the client first.
import { execFileSync } from 'node:child_process';
import { writeFileSync, unlinkSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';
const revision = process.argv[2] ?? 'f21f03d3c1fd60be201f70ae83f53eefd4674c19';
const root = fileURLToPath(new URL('../', import.meta.url));
const baseline = new URL(`../dist/.bench-baseline-${process.pid}.js`, import.meta.url);
const source = execFileSync('git', ['show', `${revision}:packages/client/src/local-replica.ts`], { cwd: root, encoding: 'utf8' });
writeFileSync(baseline, ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText);
const runs = { before: [], after: [] };
try {
  for (let i = 0; i < 3; i++) for (const phase of ['before', 'after']) {
    const env = { ...process.env };
    if (phase === 'before') env.REPLICA_BENCH_MODULE = baseline.href;
    else delete env.REPLICA_BENCH_MODULE;
    runs[phase].push(JSON.parse(execFileSync(process.execPath, ['--expose-gc', fileURLToPath(new URL('./replica-residency.mjs', import.meta.url)), '--hot-only'], { cwd: root, env, encoding: 'utf8' })));
  }
} finally { unlinkSync(baseline); }
const median = values => values.sort((a, b) => a - b)[Math.floor(values.length / 2)];
const medians = Object.fromEntries(['before', 'after'].map(phase => [phase, [10000, 50000].map(count => {
  const samples = runs[phase].map(run => run.samples.find(sample => sample.count === count));
  return { count, ...Object.fromEntries(['heapMB', 'upsertMs', 'wakesPerUpsert', 'entityReadNs'].map(key => [key, median(samples.map(sample => sample[key]))])) };
})]));
console.log(JSON.stringify({ baselineRevision: revision, node: process.version, medians, runs }, null, 2));
