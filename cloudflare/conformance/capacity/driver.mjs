import { Worker } from 'node:worker_threads';
import { writeFile, mkdir } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { preflight, Refusal } from './binding.mjs';
import { loadFixture } from './fixture.mjs';

export const kind = 'real-assembled-sdk';
const active = new Map();
let running = false;
/** Lead shell adapter: manifest.driverBinding and driverOutput are absolute, private local paths.
 * The manifest's assembled/autoscaling booleans are never consumed as runtime evidence. */
export async function runCell({ cell, manifest, signal, emit }) {
  if (running) throw new Refusal('NOT_ELIGIBLE', 'serial-cell-per-process');
  running = true;
  try { return await runBoundCell({ cell, manifest, signal, emit }); }
  finally { running = false; }
}
async function runBoundCell({ cell, manifest, signal, emit }) {
  if (!isAbsolute(manifest.driverBinding ?? '') || !isAbsolute(manifest.driverOutput ?? '')) throw new Refusal('NOT_ELIGIBLE', 'driver-paths');
  const prepared = await preflight(manifest.driverBinding), b = prepared.binding;
  if (prepared.state === 'DORMANT') throw new Refusal('UNSUPPORTED', prepared.missing.join(','));
  if (cell.id !== b.cell || active.has(b.runId) || signal.aborted) throw new Refusal('NOT_ELIGIBLE', 'cell/lifetime');
  // Validate all fixture schemas and rate allocations without credentials or executable imports.
  let cleanupMaximum = 0;
  for (const tenant of b.tenants) {
    const f = await loadFixture(tenant.fixture);
    if (b.mix.kind === 'source-derived' && f.operations.some(o => o.kind === 'action'))
      throw new Refusal('UNSUPPORTED', 'command-bound-action/background-completion-port');
    cleanupMaximum += f.cleanup.reduce((n, op) => n + op.cases.length, 0);
    for (const phase of b.phases) for (const t of phase.tenants.filter(t => t.alias === tenant.alias)) for (const r of t.rates) {
      const op = f.operations.find(o => o.alias === r.operation);
      if (!op) throw new Refusal('NOT_ELIGIBLE', 'fixture.operation');
      const count = b.phases.reduce((sum, p) => sum + p.tenants.filter(t => t.alias === tenant.alias).reduce((sum, t) => sum + t.rates.filter(r => r.operation === op.alias).reduce((s, r) => s + Math.ceil(p.durationMs * r.perSecond / 1000), 0), 0), 0);
      if (op.kind !== 'query' && op.cases.length < count) throw new Refusal('NOT_ELIGIBLE', 'fixture.case-budget');
    }
  }
  const offeredMaximum = b.phases.reduce((sum, phase) => sum + phase.tenants.reduce((sum, t) => sum + t.rates.reduce((sum, r) => sum + Math.ceil(phase.durationMs * r.perSecond / 1000), 0), 0), 0);
  if (offeredMaximum + cleanupMaximum > b.budget.maxOffered) throw new Refusal('NOT_ELIGIBLE', 'offered-including-cleanup');
  await mkdir(manifest.driverOutput, { recursive: true, mode: 0o700 });
  const out = join(manifest.driverOutput, b.runId);
  const worker = new Worker(new URL('./worker.mjs', import.meta.url), { workerData: { bindingPath: manifest.driverBinding, out, maxArtifactBytes: b.budget.maxArtifactBytes }, stdout: true, stderr: true });
  // SDK/custom-module diagnostics may contain credentials or customer content. Never forward them.
  worker.stdout.resume(); worker.stderr.resume();
  const record = { worker, result: undefined, termination: undefined }; active.set(b.runId, record);
  const stop = () => { worker.postMessage('stop'); };
  signal.addEventListener('abort', stop, { once: true });
  if (signal.aborted) stop();
  // Outer lifetime survives a stalled SDK/worker event loop. Termination is awaited; admitted server work stays UNKNOWN.
  const timer = setTimeout(() => { record.termination = worker.terminate(); }, b.budget.maxWallMs + b.budget.cleanupTimeoutMs);
  let outcome;
  try {
    outcome = await new Promise((resolve, reject) => {
      worker.on('message', row => {
        if (row.terminal) { record.result = row; return; }
        try { emit(row); worker.postMessage('ack'); } catch { stop(); reject(new Refusal('STOPPED', 'campaign-emit')); }
      });
      worker.once('error', () => reject(new Refusal('UNKNOWN', 'owned-worker-error')));
      worker.once('exit', code => resolve(record.result ?? { execution: 'STOPPED_OR_FAILED', code: 'UNKNOWN', field: `worker-exit-${code}`, cleanup: 'UNKNOWN' }));
    });
  } catch {
    outcome = { execution: 'STOPPED_OR_FAILED', code: 'UNKNOWN', field: 'parent-observation/worker-error', cleanup: 'UNKNOWN' };
  } finally {
    clearTimeout(timer); signal.removeEventListener('abort', stop);
    await (record.termination ?? worker.terminate());
    active.delete(b.runId);
  }
  outcome.futureEffects = 'OWNED_WORKER_EXITED'; outcome.admittedServerEffects = 'UNKNOWN';
  await writeFile(join(manifest.driverOutput, `${b.runId}.execution.json`), JSON.stringify(outcome) + '\n', { mode: 0o600, flag: 'wx' });
  if (outcome.execution !== 'EXECUTED_NOT_VERIFIED') throw new Refusal(outcome.code ?? 'UNKNOWN', outcome.field ?? 'cell-failed');
  return outcome;
}
export async function cleanup({ manifest }) {
  const { binding: b } = await preflight(manifest.driverBinding);
  const record = active.get(b.runId);
  if (record) { record.worker.postMessage('stop'); await record.worker.terminate(); active.delete(b.runId); }
  // Cross-process cleanup cannot prove old server commands finished or silently retry ambiguous writes.
  return { ownedWorker: record ? 'EXITED' : 'NONE_IN_THIS_PROCESS', admittedServerEffects: 'UNKNOWN', fixtureCleanup: 'SEE_PRIVATE_EXECUTION_RECEIPT_OR_UNKNOWN' };
}
