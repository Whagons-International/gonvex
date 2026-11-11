#!/usr/bin/env node
/**
 * Conformance harness CLI.
 *
 *   node src/cli.mjs [--backend label=kind]... [--scenario name]... [--list]
 *                    [--json report.json] [--work dir] [--keep] [--verbose] [--serial]
 *
 * Default backends: rust-a=rust rust-b=rust (the same build twice). The first
 * backend is the reference; every other backend's trace is diffed against it.
 * Each scenario runs on all backends at once (--serial: one after another).
 * Exits 0 only when no scenario failed on any backend and no diff was found.
 */
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { buildFixture } from './fixture.mjs';
import { backendClass } from './backends/index.mjs';
import { runScenario } from './runner.mjs';
import { diffTraces } from './differ.mjs';
import { SCENARIOS } from '../scenarios/index.mjs';

const { values } = parseArgs({ options: {
  backend: { type: 'string', multiple: true }, scenario: { type: 'string', multiple: true }, list: { type: 'boolean' },
  json: { type: 'string' }, work: { type: 'string' }, keep: { type: 'boolean' }, verbose: { type: 'boolean' }, serial: { type: 'boolean' },
} });

if (values.list) { for (const s of SCENARIOS) console.log(`${s.name.padEnd(28)} ${s.description}`); process.exit(0); }

const selected = values.scenario?.length ? SCENARIOS.filter(s => values.scenario.includes(s.name)) : SCENARIOS;
const unknown = (values.scenario ?? []).filter(n => !SCENARIOS.some(s => s.name === n));
if (unknown.length) { console.error(`unknown scenario(s): ${unknown.join(', ')} (see --list)`); process.exit(2); }
const specs = (values.backend?.length ? values.backend : ['rust-a=rust', 'rust-b=rust']).map(spec => {
  const [label, kind] = spec.includes('=') ? spec.split(/=(.*)/s) : [spec, spec];
  return { label, kind };
});
if (specs.length < 1) { console.error('at least one --backend is required'); process.exit(2); }

const workDir = values.work ? resolve(values.work) : await mkdtemp(join(process.env.GONVEX_CONFORMANCE_TMP ?? tmpdir(), 'gonvex-conformance-'));
await mkdir(workDir, { recursive: true });
console.log(`work directory: ${workDir}`);

const started = Date.now();
const fixtures = { 1: await buildFixture(workDir, 1), 2: await buildFixture(workDir, 2) };
console.log(`fixture bundles: v1 ${fixtures[1].hash.slice(0, 12)}, v2 ${fixtures[2].hash.slice(0, 12)}`);

const backends = [];
for (const { label, kind } of specs) {
  const Class = await backendClass(kind);
  const dir = join(workDir, label);
  await mkdir(dir, { recursive: true });
  backends.push(new Class({ name: label, workDir: dir }));
}
const report = { backends: specs, scenarios: [], passed: 0, failed: 0, skipped: 0 };
let exitCode = 0;
try {
  await Promise.all(backends.map(b => b.start()));
  console.log(`backends: ${backends.map(b => `${b.name} (${b.wsUrl})`).join(', ')}\n`);
  for (const scenario of selected) {
    const at = Date.now();
    const traces = [];
    const one = (backend) => runScenario({ scenario, backend, fixtures, verbose: values.verbose })
      .catch(error => ({ backend: backend.name, scenario: scenario.name, failures: [`setup failed: ${error?.message ?? error}`], steps: [], checksums: {} }));
    if (values.serial) for (const backend of backends) traces.push(await one(backend));
    else traces.push(...await Promise.all(backends.map(one)));
    const ran = traces.filter(t => !t.skipped);
    const failures = ran.flatMap(t => t.failures.map(f => `[${t.backend}] ${f}`));
    const diffs = ran.slice(1).flatMap(t => { const d = diffTraces(ran[0], t); return d.length ? [`${ran[0].backend} vs ${t.backend}:`, ...d.map(l => '  ' + l)] : []; });
    const skipped = traces.filter(t => t.skipped).map(t => `${t.backend}: ${t.skipped}`);
    const ok = failures.length === 0 && diffs.length === 0 && ran.length > 0;
    const status = ran.length === 0 ? 'SKIP' : ok ? 'PASS' : 'FAIL';
    if (status === 'PASS') report.passed++; else if (status === 'SKIP') report.skipped++; else { report.failed++; exitCode = 1; }
    console.log(`${status} ${scenario.name} (${((Date.now() - at) / 1000).toFixed(1)} s, ${ran[0]?.steps.length ?? 0} steps)`);
    for (const line of [...failures, ...diffs]) console.log('  ' + line);
    for (const line of skipped) console.log('  skipped on ' + line);
    report.scenarios.push({ name: scenario.name, status, failures, diffs, skipped, traces });
  }
} finally {
  await Promise.allSettled(backends.map(b => b.stop()));
}
console.log(`\n${report.passed} passed, ${report.failed} failed, ${report.skipped} skipped of ${selected.length} scenarios in ${((Date.now() - started) / 1000).toFixed(1)} s`);
if (values.json) await writeFile(values.json, JSON.stringify(report, null, 1) + '\n');
if (!values.keep && !values.work && exitCode === 0) await rm(workDir, { recursive: true, force: true });
else console.log(`kept ${workDir}`);
process.exit(exitCode);
