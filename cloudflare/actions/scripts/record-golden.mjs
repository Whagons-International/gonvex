#!/usr/bin/env node
// Records the Rust runtime's answers for the K12 parity test.
//
//   node actions/scripts/record-golden.mjs <report.json>...   (from cloudflare/)
//
// Each report is `node conformance/src/cli.mjs --scenario actions --json <file>`
// run Rust against Rust. Every trace in every report must carry the same
// observations; the script then writes test/golden/actions-rust.json, and
// test/golden/fixture-functions.json from a fresh codegen build of the fixture.
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildFixture } from '../../conformance/src/fixture.mjs';

const ARTIFACT = /active artifact is \\"[0-9a-f]+\\"/g;
const normalize = (value) => JSON.parse(JSON.stringify(value).replace(ARTIFACT, 'active artifact is \\"<artifact>\\"'));
const observations = (trace) => Object.fromEntries(trace.steps.filter((step) => Object.keys(step.observations ?? {}).length).map((step) => [step.name, step.observations]));

const files = process.argv.slice(2);
if (!files.length) throw new Error('usage: record-golden.mjs <report.json>...');
let reference;
let traces = 0;
for (const file of files) {
  const report = JSON.parse(await readFile(file, 'utf8'));
  const scenario = report.scenarios.find((s) => s.name === 'actions');
  if (!scenario || scenario.status !== 'PASS') throw new Error(`${file}: scenario actions did not pass`);
  for (const trace of scenario.traces) {
    const steps = normalize(observations(trace));
    reference ??= steps;
    if (JSON.stringify(steps) !== JSON.stringify(reference)) throw new Error(`${file}: observations differ between traces`);
    traces++;
  }
}
const golden = new URL('../test/golden/', import.meta.url);
await writeFile(new URL('actions-rust.json', golden), JSON.stringify({
  note: `Observations of conformance scenario \`actions\` on the Rust runtime at 0bf3ad13 (rust-a against rust-b, ${files.length} runs, ${traces} identical traces). Normalized by the harness (<project>); the active artifact hash is replaced by <artifact>. Regenerate with actions/scripts/record-golden.mjs.`,
  steps: reference,
}, null, 1) + '\n');

const fixture = await buildFixture(await mkdtemp(join(tmpdir(), 'k12-fixture-')), 1);
const functions = Object.fromEntries(Object.entries(fixture.manifest.functions)
  .filter(([path]) => path.startsWith('actions.') || path.startsWith('board.'))
  .sort(([a], [b]) => (a < b ? -1 : 1)));
await writeFile(new URL('fixture-functions.json', golden), JSON.stringify({
  note: 'Manifest entries of the P02 conformance fixture (cloudflare/conformance/fixtures/app, generation 1) built with gonvex codegen; the K12 tests turn them into FunctionContracts as the Worker build does.',
  functions,
}, null, 1) + '\n');
console.log(`golden written from ${traces} traces`);
