import { chromium } from '@playwright/test';
import { createHash } from 'node:crypto';
import { globSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { assertBudget, budget } from './gates.mjs';

const harnessStarted = performance.now();
if (process.platform !== 'linux') throw new Error('Private footprint requires Linux /proc');
const directory = dirname(fileURLToPath(import.meta.url)), root = resolve(directory, '../../../..');
const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
if (!bundler) throw new Error('Install frozen workspace dependencies first');
const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
const temp = await mkdtemp(join(tmpdir(), 'gonvex-memory-'));
const output = resolve(process.env.GONVEX_MEMORY_OUTPUT || join(directory, 'results.json'));
let browser, memoryTimer, pollInFlight, pollError, deadlineTimer, timeoutPhase;
const samples = [], visits = [];
const control = process.env.GONVEX_MEMORY_CONTROL_LATE_PROJECTION === '1';
try {
  const plugins = control ? [{ name: 'late-projection-negative-control', setup(bundler) {
    bundler.onLoad({ filter: /indexeddb-read-view\.js$/ }, async args => {
      let contents = await readFile(args.path, 'utf8');
      const before = contents;
      contents = contents.replace(/const retainedColumns = [^\n]+;/, 'const retainedColumns = undefined;')
        .replace(/const needsFinalProjection = [^\n]+;/, 'const needsFinalProjection = Boolean(read.columns);');
      if (contents === before || !contents.includes('const retainedColumns = undefined;') || !contents.includes('const needsFinalProjection = Boolean(read.columns);')) throw new Error('Negative control no longer matches the compiled query path');
      return { contents, loader: 'js' };
    });
  } }] : [];
  await build({ entryPoints: [join(directory, 'entry.mjs')], bundle: true, minify: true, platform: 'browser', format: 'esm', outfile: join(temp, 'bundle.js'), plugins });
  const javascript = await readFile(join(temp, 'bundle.js'));
  const fixtureHash = createHash('sha256').update(await readFile(join(directory, 'fixture.mjs'))).digest('hex');
  const context = await chromium.launchPersistentContext(process.env.GONVEX_MEMORY_PROFILE ? resolve(process.env.GONVEX_MEMORY_PROFILE) : join(temp, 'profile'));
  browser = context.browser();
  const page = context.pages()[0] ?? await context.newPage();
  await page.route('http://sdk-memory.localhost/**', route => route.fulfill({
    contentType: new URL(route.request().url()).pathname === '/bundle.js' ? 'text/javascript' : 'text/html',
    body: new URL(route.request().url()).pathname === '/bundle.js' ? javascript : '<!doctype html><title>SDK memory smoke</title><script type="module" src="/bundle.js"></script>',
  }));
  page.setDefaultTimeout(120_000);
  const cdp = await page.context().newCDPSession(page), processCdp = await browser.newBrowserCDPSession();
  const errors = [];
  let rendererIds = [];
  page.on('pageerror', error => errors.push(error.message));
  async function sample(label) {
    const heap = await cdp.send('Runtime.getHeapUsage');
    const { processInfo } = await processCdp.send('SystemInfo.getProcessInfo');
    rendererIds = processInfo.filter(process => process.type === 'renderer').map(process => process.id);
    let privateMB = 0, rssMB = 0, rendererCount = 0;
    for (const process of processInfo) {
      if (process.type !== 'renderer') continue;
      const status = await readFile(`/proc/${process.id}/status`, 'utf8');
      const kib = name => Number(status.match(new RegExp(`^${name}:\\s+(\\d+)`, 'm'))?.[1] ?? NaN);
      privateMB += (kib('RssAnon') + kib('VmSwap')) * 1024 / 1e6;
      rssMB += kib('VmRSS') * 1024 / 1e6; rendererCount++;
    }
    if (!rendererCount || !Number.isFinite(privateMB) || privateMB <= 0) throw new Error('Missing renderer footprint');
    samples.push({ label, privateMB, rssMB, heapMiB: heap.usedSize / 1048576 });
  }
  const setupStart = performance.now();
  deadlineTimer = setTimeout(() => { timeoutPhase = 'fixture setup'; void browser.close().catch(() => {}); }, 60_000);
  await page.goto('http://sdk-memory.localhost/');
  await page.waitForFunction(() => typeof window.seed === 'function');
  const fixture = await page.evaluate(hash => window.seed(hash), fixtureHash);
  await page.evaluate(() => window.navigate(0));
  await cdp.send('HeapProfiler.collectGarbage');
  const setupMs = performance.now() - setupStart;
  clearTimeout(deadlineTimer);
  await sample('warm-after-gc');
  await cdp.send('Performance.enable');
  const before = await cdp.send('Performance.getMetrics');
  const started = performance.now();
  deadlineTimer = setTimeout(() => { timeoutPhase = 'active workload'; void browser.close().catch(() => {}); }, 25_000);
  memoryTimer = setInterval(() => {
    if (pollInFlight || pollError) return;
    pollInFlight = (async () => {
      let privateMB = 0, rssMB = 0;
      for (const pid of rendererIds) {
        const status = await readFile(`/proc/${pid}/status`, 'utf8');
        const kib = name => Number(status.match(new RegExp(`^${name}:\\s+(\\d+)`, 'm'))?.[1] ?? NaN);
        privateMB += (kib('RssAnon') + kib('VmSwap')) * 1024 / 1e6; rssMB += kib('VmRSS') * 1024 / 1e6;
      }
      if (!Number.isFinite(privateMB) || privateMB <= 0) throw new Error('Invalid renderer memory poll');
      samples.push({ label: 'active-poll', elapsedMs: performance.now() - started, privateMB, rssMB });
    })().catch(error => { pollError = error; }).finally(() => { pollInFlight = undefined; });
  }, 100);
  // Same five spaces, two laps, and 20-second active budget as the app smoke.
  // This isolates the SDK storage/window path; it does not prove the app's cap.
  for (let visit = 0; visit < 10; visit++) {
    const workspace = (visit + 1) % 5;
    await page.evaluate(workspace => window.navigate(workspace), workspace);
    let steps = 0, state;
    while (performance.now() < started + (visit + 1) * 2000) {
      state = await page.evaluate(() => window.scroll());
      if (++steps % 5 === 0) await sample(`visit-${visit + 1}`);
      await page.waitForTimeout(100);
    }
    if (steps < 3 || state?.residentRows !== 200 || state.metadataIds !== 2000 || state.queryRows !== 200) throw new Error('Incomplete SDK workload');
    visits.push({ workspace, steps, ...state });
  }
  const activeMs = performance.now() - started;
  clearTimeout(deadlineTimer); deadlineTimer = undefined;
  clearInterval(memoryTimer); memoryTimer = undefined;
  await pollInFlight;
  if (pollError) throw pollError;
  const after = await cdp.send('Performance.getMetrics');
  await sample('natural-end');
  await cdp.send('HeapProfiler.collectGarbage');
  await sample('settled-after-gc');
  const metric = (value, name) => value.metrics.find(metric => metric.name === name)?.value ?? NaN;
  const summary = { scope: 'Gonvex IndexedDB, offline queries and fixed row-window only; no application UI or backend', fixtureHash, fixtureReused: fixture.reused, budget, control: control ? 'late-projection' : null, chromium: browser.version(), tasks: 10_000, workspaces: 5,
    setupMs, activeMs, harnessMs: performance.now() - harnessStarted, visits, samples, peakPrivateMB: Math.max(...samples.map(sample => sample.privateMB)), peakRssMB: Math.max(...samples.map(sample => sample.rssMB)),
    retainedGrowthMiB: samples.at(-1).heapMiB - samples[0].heapMiB,
    taskCpuSeconds: metric(after, 'TaskDuration') - metric(before, 'TaskDuration'), errors,
    note: 'Cold seed/setup excluded. No forced GC, heap snapshots or allocation sampling during the active workload.' };
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, JSON.stringify(summary, null, 2) + '\n');
  console.log(JSON.stringify({ setupMs, activeMs, harnessMs: summary.harnessMs, peakPrivateMB: summary.peakPrivateMB, peakRssMB: summary.peakRssMB, retainedGrowthMiB: summary.retainedGrowthMiB, taskCpuSeconds: summary.taskCpuSeconds }));
  assertBudget(summary);
} catch (error) {
  if (timeoutPhase) throw new Error(`SDK memory ${timeoutPhase} exceeded its deadline`, { cause: error });
  throw error;
} finally {
  clearTimeout(deadlineTimer);
  clearInterval(memoryTimer);
  await pollInFlight;
  await browser?.close();
  await rm(temp, { recursive: true, force: true });
}
