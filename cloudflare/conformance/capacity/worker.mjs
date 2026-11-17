import { parentPort, workerData } from 'node:worker_threads';
import { open, mkdir, readdir } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { preflight, jsonFile, shape, list, text, integer, refuse, Refusal } from './binding.mjs';
import { installEffects } from './effects.mjs';
import { loadFixture } from './fixture.mjs';
import { execute } from './engine.mjs';
import { sampleTelemetry, readCost, CONTRACT_SHA256 } from './telemetry.mjs';
import { sleep } from './schedule.mjs';
import { distribution } from './statistics.mjs';
import { versionUrl, readJSON, verifyIdentity } from './identity.mjs';

const controller = new AbortController();
parentPort.on('message', m => { if (m === 'stop') { controller.abort(); effects?.stop(); } if (m === 'ack') unacked = Math.max(0, unacked - 1); });
let effects, file, handle, loop, stopped, sequence = 0, bytes = 0, telemetry, unacked = 0, cleanupMs = 1000;
const confirmations = [], applications = [], totals = { offered: 0, dispatched: 0, completed: 0, errors: 0, timeouts: 0, failedAttempts: 0 };
const stages = Object.fromEntries(['warmup', 'measure', 'cooldown', 'cleanup'].map(stage => [stage, { durable: [], applied: [] }]));
const stageSummary = () => Object.fromEntries(Object.entries(stages).map(([stage, s]) => [stage, { durableConfirmation: distribution(s.durable), receiverApplied: distribution(s.applied) }]));
const emit = asyncRow => {
  const row = { at: new Date().toISOString(), sequence: ++sequence, ...asyncRow };
  const line = JSON.stringify(row) + '\n'; bytes += Buffer.byteLength(line);
  if (bytes > workerData.maxArtifactBytes) throw new Refusal('STOPPED', 'artifact-budget');
  // Synchronous ordered writes: no unbounded stream buffer, no payload-bearing messages to parent.
  file.writeSync(line);
  for (const k of ['offered', 'dispatched', 'completed', 'errors', 'timeouts']) totals[k] += row[k] ?? 0;
  if (['UNKNOWN', 'not-eligible'].includes(row.type)) totals.failedAttempts++;
  if (row.durableConfirmationMs !== undefined) confirmations.push(row.durableConfirmationMs);
  if (row.receiverDelayMs !== undefined) applications.push(row.receiverDelayMs);
  if (stages[row.stage] && row.durableConfirmationMs !== undefined) stages[row.stage].durable.push(row.durableConfirmationMs);
  if (stages[row.stage] && row.receiverDelayMs !== undefined) stages[row.stage].applied.push(row.receiverDelayMs);
  if (unacked >= 256) { stopped = new Refusal('STOPPED', 'parent-backpressure'); effects?.stop(); throw stopped; }
  unacked++;
  const projected = {};
  for (const key of ['at', 'type', 'tenantAlias', 'operation', 'offered', 'dispatched', 'completed', 'durableConfirmationMs', 'receiverDelayMs', 'timeouts', 'errors', 'generatorLagMs', 'digestMatch', 'measuredUsd']) if (key in row) projected[key] = row[key];
  parentPort.postMessage(projected);
};
try {
  const result = await preflight(workerData.bindingPath), b = result.binding;
  if (result.state === 'DORMANT') throw new Refusal('UNSUPPORTED', result.missing.join(','));
  if (b.telemetry.contractSha256 !== CONTRACT_SHA256) throw new Refusal('UNSUPPORTED', 'reviewed-capacity-adapter-contract');
  cleanupMs = b.budget.cleanupTimeoutMs;
  const start = performance.now(), deadline = start + b.budget.maxWallMs;
  const check = () => { effects?.check(); if (controller.signal.aborted || stopped) throw new Refusal('STOPPED', 'cancelled/monitor'); };
  const credentialPath = process.env[b.auth.fileEnv];
  if (!credentialPath) throw new Refusal('NOT_ELIGIBLE', 'Vault-supplied-principal-pool');
  const { value: credentials } = await jsonFile(credentialPath, 8 * 1024 * 1024, true);
  shape(credentials, ['sessions', 'monitorToken'], 'credential-pool'); text(credentials.monitorToken, 'monitorToken', 16384);
  list(credentials.sessions, 26000, 'credential.sessions');
  for (const c of credentials.sessions) {
    shape(c, ['principal', 'tenantId', 'accountId', 'memberId', 'token', 'expiresAtUnixMs'], 'credential');
    for (const k of ['principal', 'tenantId', 'accountId', 'memberId', 'token']) text(c[k], 'credential', 16384);
    integer(c.expiresAtUnixMs, Date.now() + b.budget.maxWallMs, Number.MAX_SAFE_INTEGER, 'credential.lifetime');
  }
  if (new Set(credentials.sessions.map(c => `${c.tenantId}:${c.principal}`)).size !== credentials.sessions.length) refuse('credential.duplicate');
  await mkdir(workerData.out, { mode: 0o700 });
  // FileHandle's synchronous descriptor avoids async log writes racing the receipt/effect boundary.
  handle = await open(`${workerData.out}/receipts.jsonl`, 'wx', 0o600);
  const { writeSync } = await import('node:fs'); file = { writeSync: line => { const data = Buffer.from(line); let n = 0; while (n < data.length) n += writeSync(handle.fd, data, n, data.length - n); } };
  effects = installEffects({ WebSocket: globalThis.WebSocket, fetch: globalThis.fetch, socketUrl: b.deployment.endpoint,
    httpUrls: [b.telemetry.url, versionUrl(b)], deadline, budget: b.budget, signal: controller.signal });
  globalThis.WebSocket = effects.WebSocket; globalThis.fetch = effects.fetch;
  const identity = await readJSON(await fetch(versionUrl(b), { redirect: 'error', signal: AbortSignal.any([controller.signal, AbortSignal.timeout(b.budget.maxTelemetryAgeMs)]), headers: { authorization: `Bearer ${credentials.monitorToken}` } }));
  verifyIdentity(identity, b); // Existing authenticated typed RUNTIME_CATALOG bridge, before any SDK client construction.
  const fixtures = new Map(); for (const t of b.tenants) fixtures.set(t.alias, await loadFixture(t.fixture));
  // Install the realm boundary BEFORE importing the genuine SDK or generated app modules.
  const sdk = await import(pathToFileURL(b.sources.sdk.path).href);
  const { api } = await import(pathToFileURL(b.sources.refs.path).href);
  const { localRuntime } = await import(pathToFileURL(b.sources.localRuntime.path).href);
  loop = monitorEventLoopDelay({ resolution: 20 }); loop.enable();
  telemetry = await sampleTelemetry(b, credentials.monitorToken, undefined, AbortSignal.any([controller.signal, AbortSignal.timeout(b.budget.maxTelemetryAgeMs)]));
  let cost = await readCost(b);
  emit({ type: 'telemetry', sample: telemetry, measuredUsd: cost.measuredUsd });
  const monitor = (async () => {
    while (!controller.signal.aborted) {
      await sleep(b.budget.telemetryIntervalMs, controller.signal); check();
      const before = performance.now(), cpu = process.cpuUsage(), memory = process.memoryUsage(), fds = (await readdir('/proc/self/fd')).length;
      if (memory.rss > b.budget.maxGeneratorRssBytes || fds > b.budget.maxGeneratorFds || loop.max / 1e6 > b.budget.maxLagMs) throw new Refusal('STOPPED', 'generator-resource-budget');
      const next = await sampleTelemetry(b, credentials.monitorToken, telemetry, AbortSignal.any([controller.signal, AbortSignal.timeout(b.budget.maxTelemetryAgeMs)]));
      const nextCost = await readCost(b, cost), deltaUsd = nextCost.measuredUsd - cost.measuredUsd; cost = nextCost;
      telemetry = next;
      emit({ type: 'telemetry', sample: next, generatorCpuMs: (cpu.user + cpu.system) / 1000, generatorRssBytes: memory.rss,
        generatorHeapBytes: memory.heapUsed, generatorFds: fds, generatorLagMs: loop.max / 1e6, telemetryRpcMs: performance.now() - before, wire: effects.snapshot(), measuredUsd: deltaUsd, costAsOfUnixMs: cost.costAsOfUnixMs });
      loop.reset();
    }
  })().catch(e => { if (!controller.signal.aborted) { stopped = e; controller.abort(); } });
  let outcome;
  try { outcome = await execute({ b, sdk, api, localRuntime, credentials, fixtures, check, signal: controller.signal, emit,
    beginCleanup: () => effects.limitLifetime(b.budget.cleanupTimeoutMs) }); }
  finally { controller.abort(); effects.stop(); await monitor; }
  await handle.sync(); await handle.close(); handle = undefined; file = undefined;
  parentPort.postMessage({ terminal: true, ...outcome, totals, byStage: stageSummary(), allStagesDurableConfirmation: distribution(confirmations), allStagesReceiverApplied: distribution(applications), futureEffects: 'FENCED_WORKER_TERMINATION_PENDING', admittedServerEffects: 'UNKNOWN' });
} catch (e) {
  parentPort.postMessage({ terminal: true, execution: 'STOPPED_OR_FAILED', code: e instanceof Refusal ? e.code : 'UNKNOWN',
    field: e instanceof Refusal ? e.field : 'worker-error', totals, byStage: stageSummary(), allStagesDurableConfirmation: distribution(confirmations), allStagesReceiverApplied: distribution(applications), cleanup: 'UNKNOWN', admittedServerEffects: 'UNKNOWN' });
} finally { controller.abort(); effects?.stop(); await effects?.settle(cleanupMs); loop?.disable(); if (handle) { await handle.sync().catch(() => {}); await handle.close().catch(() => {}); } parentPort.close(); }
