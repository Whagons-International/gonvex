// Uses the repository's existing native Playwright benchmark workflow.
import { wireCounter } from './wire-counter.mjs';
import { chromium } from '@playwright/test';
import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { spawn, execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { cpus, loadavg, release } from 'node:os';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const directory = dirname(fileURLToPath(import.meta.url));
const output = resolve(process.argv[2] ?? `${directory}/results`);
const binary = process.env.GONVEX_BENCH_BINARY;
if (!binary) throw new Error('Set GONVEX_BENCH_BINARY to the release benchmark binary');
await mkdir(output, { recursive: true });
await build({ entryPoints: [`${directory}/browser-entry.mjs`], outfile: `${output}/benchmark.js`, bundle: true,
  format: 'esm', platform: 'browser', minify: true, define: { global: 'globalThis' } });
const server = spawn(binary, ['serve', `${output}/fixtures.json`, `${output}/benchmark.js`], { stdio: ['ignore', 'pipe', 'inherit'] });
let browser, wire;
const resume = process.env.GONVEX_BENCH_RESUME === '1';
let results = { environment: { node: process.version, os: release(), cpu: cpus()[0].model, loadBefore: loadavg(),
  commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8', cwd: resolve(directory, '../..') }).trim(), fixtureSha256: (await readFile(`${output}/fixture-sha256.txt`, 'utf8')).trim() }, codec: [], pipeline: [] };
if (resume) {
  const previous = JSON.parse(await readFile(`${output}/browser.json`, 'utf8'));
  if (previous.environment.fixtureSha256 !== results.environment.fixtureSha256) throw new Error('Cannot resume with changed fixtures');
  results = previous;
}
results.complete = false;
results.runs ??= [];
results.runs.push({ at: new Date().toISOString(), load: loadavg(), bundleSha256: createHash('sha256').update(await readFile(`${output}/benchmark.js`)).digest('hex'), binarySha256: createHash('sha256').update(await readFile(binary)).digest('hex') });
try {
  const lines = createInterface({ input: server.stdout });
  const [url] = await Promise.race([once(lines, 'line'), once(server, 'exit').then(() => { throw new Error('server exited before listen'); })]);
  browser = await chromium.launch({ headless: true, env: { ...process.env, DBUS_SESSION_BUS_ADDRESS: 'unix:path=/dev/null/gonvex-benchmark-no-bus' }, args: ['--enable-precise-memory-info', '--js-flags=--expose-gc'],
    ...(process.env.GONVEX_BENCH_CHROMIUM ? { executablePath: process.env.GONVEX_BENCH_CHROMIUM } : {}) });
  results.environment.browser = browser.version();
  wire = await wireCounter(url);
  const page = await browser.newPage();
  await page.addInitScript(url => { window.benchWsBase = url; }, wire.url);
  page.on('pageerror', error => console.error(error));
  await page.goto(url); await page.waitForFunction(() => window.benchmarkReady, { timeout: 60000 });
  results.contract = await page.evaluate(() => window.contractBench());
  const smoke = process.env.GONVEX_BENCH_SMOKE === '1';
  results.environment.nodelay = process.env.GONVEX_BENCH_NODELAY === '1';
  if (!smoke && !resume && process.env.GONVEX_BENCH_SKIP_CODECS !== '1') results.codec = await page.evaluate(() => window.codecBench());
  await page.close();
  const fixtures = JSON.parse(await readFile(`${output}/fixtures.json`, 'utf8')).fixtures;
  for (const fixture of fixtures) {
    // The expired resume fixture intentionally reuses a complete snapshot.
    if (process.env.GONVEX_BENCH_WORKLOADS && !process.env.GONVEX_BENCH_WORKLOADS.split(',').includes(fixture.name)) continue;
    if (fixture.name === 'reconnect-expired-cursor' || (smoke && fixture.name !== 'small-update')) continue;
    const variants = fixture.before.length ? ['baseline', 'no-old', 'patch'] : ['baseline', 'paged', 'canonical'];
    const candidates = variants.map(variant => ({ variant, codec: 'json', compression: 'deflate1-1024' }));
    candidates.push({ variant: 'baseline', codec: 'msgpack', compression: 'deflate1-1024' },
      { variant: fixture.before.length ? 'no-old' : 'paged', codec: 'msgpack', compression: 'deflate1-1024' },
      { variant: 'baseline', codec: 'json', compression: 'zstd3' },
      { variant: 'baseline', codec: 'json', compression: 'brotli4' });
    for (const storage of smoke ? ['memory'] : (process.env.GONVEX_BENCH_STORES?.split(',') ?? ['memory', 'indexeddb'])) for (const candidate of smoke ? candidates.slice(0, 1) : candidates) {
      if (process.env.GONVEX_BENCH_CANDIDATES && !process.env.GONVEX_BENCH_CANDIDATES.split(',').includes(`${candidate.variant}/${candidate.codec}/${candidate.compression}`)) continue;
      if (resume && results.pipeline.some(row => row.workload === fixture.name && row.storage === storage && ['variant', 'codec', 'compression'].every(key => row[key] === candidate[key]))) continue;
      const page = await browser.newPage();
      await page.addInitScript(url => { window.benchWsBase = url; }, wire.url);
      await page.goto(url); await page.waitForFunction(() => window.benchmarkReady, { timeout: 60000 });
      const cdp = await page.context().newCDPSession(page);
      await cdp.send('Performance.enable');
      await cdp.send('HeapProfiler.collectGarbage');
      const before = await cdp.send('Performance.getMetrics');
      const wireBefore = { ...wire.totals };
      const gc = [];
      const tracing = ({ value }) => { for (const e of value) if (['MinorGC', 'MajorGC'].includes(e.name) && e.ph === 'X') gc.push({ name: e.name, us: e.dur }); };
      cdp.on('Tracing.dataCollected', tracing);
      await cdp.send('Tracing.start', { categories: 'v8,devtools.timeline', transferMode: 'ReportEvents' });
      const result = await page.evaluate(options => window.pipelineBench(options), { workload: fixture.name, ...candidate, storage,
        samples: fixture.after.length > 1000 ? 3 : fixture.after.length === 1000 ? 30 : 100, warmups: fixture.after.length > 1000 ? 1 : 5 });
      const after = await cdp.send('Performance.getMetrics');
      const traceEnd = once(cdp, 'Tracing.tracingComplete');
      await cdp.send('Tracing.end'); await traceEnd; cdp.off('Tracing.dataCollected', tracing);
      result.gc = { count: gc.length, totalUs: gc.reduce((sum, e) => sum + e.us, 0), includesUntimedSetup: true };
      result.actualWirePerTrial = Object.fromEntries(Object.entries(wire.totals).map(([k, v]) => [k, (v - wireBefore[k]) / (result.samples + (result.warmups ?? 5))]));
      const metric = (snapshot, name) => snapshot.metrics.find(m => m.name === name)?.value;
      result.rendererTaskDurationSeconds = metric(after, 'TaskDuration') - metric(before, 'TaskDuration');
      result.heapAfterBytes = metric(after, 'JSHeapUsedSize');
      await cdp.send('HeapProfiler.collectGarbage');
      result.retainedHeapAfterGcBytes = metric(await cdp.send('Performance.getMetrics'), 'JSHeapUsedSize');
      result.independentBrowserContext = true;
      results.pipeline.push(result);
      await page.close();
      await writeFile(`${output}/browser.json`, JSON.stringify(results, null, 2) + '\n');
      console.log(`${fixture.name} ${storage} ${candidate.variant} ${candidate.codec} ${candidate.compression}: ${result.latencyMs.p50.toFixed(2)} ms p50`);
    }
  }
  results.environment.loadAfter = loadavg();
  if (results.pipeline.length === 0) throw new Error('No benchmark cases selected');
  results.complete = true;
  await writeFile(`${output}/browser.json`, JSON.stringify(results, null, 2) + '\n');
} finally { wire?.close(); await browser?.close(); server.kill('SIGTERM'); }
