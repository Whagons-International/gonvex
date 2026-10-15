import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { readFile, mkdir, writeFile, realpath } from 'node:fs/promises';
import { tmpdir, cpus, totalmem, loadavg, release } from 'node:os';
import { execFileSync } from 'node:child_process';
import { resolve, dirname } from 'node:path';
import { WebSocketServer } from 'ws';
import { installFirefoxWorkerWebSocketWorkaround } from './firefox-websocket-workaround.mjs';
const workaroundApplied = process.env.BROWSER === 'firefox' ? installFirefoxWorkerWebSocketWorkaround() : () => false;
const { chromium, firefox, webkit } = await import('@playwright/test');
import { replicaRowsHashes, replicaAggregateDigest } from '../../dist/replica-integrity.js';
const require = createRequire(await realpath(new URL('../../node_modules/vitest/package.json', import.meta.url)));
const viteRequire = createRequire(require.resolve('vite/package.json'));
const { build } = viteRequire('esbuild');
const root = dirname(new URL(import.meta.url).pathname);
const output = resolve(tmpdir(), process.env.PROFILE_SET ?? 'gonvex-engine-worker-round2'); await mkdir(output, { recursive: true });
const revision = process.env.ENGINE_REVISION;
const revisionFiles = revision ? new Set(execFileSync('git', ['ls-tree', '-r', '--name-only', revision, 'packages/client/src'], { encoding: 'utf8' }).trim().split('\n')) : undefined;
const sourceRoot = execFileSync('git', ['rev-parse', '--show-toplevel'], { encoding: 'utf8' }).trim();
const revisionPlugin = { name: 'engine-revision', setup(build) {
  build.onLoad({ filter: /packages\/client\/src\/.*\.ts$/ }, args => {
    const file = args.path.slice(sourceRoot.length + 1);
    if (!revisionFiles?.has(file)) return;
    return { contents: execFileSync('git', ['show', `${revision}:${file}`], { encoding: 'utf8' }), loader: 'ts', resolveDir: dirname(args.path) };
  });
} };
for (const entry of ['browser', 'worker']) await build({ plugins: revision ? [revisionPlugin] : [], entryPoints: [resolve(root, `${entry}.ts`)], bundle: true, splitting: true, format: 'esm', target: 'es2022', minify: true, outdir: output, entryNames: '[name]', platform: 'browser' });
await build({ entryPoints: [resolve(root, 'fixture.ts')], bundle: true, format: 'esm', platform: 'node', outfile: resolve(output, 'fixture.mjs') });
const { row, columns } = await import(resolve(output, 'fixture.mjs'));
const rows = Array.from({ length: 20_000 }, (_, index) => row(index));
let offline = false, serverRevision = 1, denied = 0;
let wireProfile = { callFrames: 0, callManyFrames: 0, calls: 0, batchSizes: [] };
const sockets = new Set(); const opens = new Map(); const effects = new Map();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const send = (socket, frame) => { if (socket.readyState === 1) socket.send(JSON.stringify(frame)); };
const directive = { protocolVersion: 1, scope: 'tenant', visibilityScope: 'visible', epoch: 'fixture' };
const identity = { auth: { account: { id: 'account' } }, tenant: { id: 'tenant' }, member: { id: 'member', accountId: 'account', permissions: {} } };
const server = createServer(async (request, response) => {
  try {
    if (request.url === '/offline' || request.url === '/disconnect') { offline = request.url === '/offline'; for (const socket of sockets) socket.close(); response.end('ok'); return; }
    if (request.url === '/revision') { serverRevision = Math.max(serverRevision, Number(new URL(request.url, 'http://fixture').searchParams.get('value') ?? 1)); response.end(String(serverRevision)); return; }
    if (request.url?.startsWith('/revision?')) { serverRevision = Math.max(serverRevision, Number(new URL(request.url, 'http://fixture').searchParams.get('value') ?? 1)); response.end(String(serverRevision)); return; }
    if (request.url === '/online') { offline = false; response.end('ok'); return; }
    if (request.url === '/must-not-fetch') { denied++; response.end('escaped'); return; }
    if (request.url === '/denied-count') { response.end(JSON.stringify(denied)); return; }
    if (request.url === '/reset-wire-profile') { wireProfile = { callFrames: 0, callManyFrames: 0, calls: 0, batchSizes: [] }; response.end('ok'); return; }
    if (request.url === '/wire-profile') { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(wireProfile)); return; }
    if (request.url === '/reset-edits') { effects.clear(); response.end('ok'); return; }
    if (request.url === '/reset-burst') { response.end('ok'); return; }
    if (request.url === '/burst') {
      for (let tick = 0; tick < 20; tick++) {
        const changed = rows.slice(tick * 50, tick * 50 + 50).map(value => ({ ...value, count: value.count + 1 }));
        for (const value of changed) rows[Number(value.id)] = value;
        serverRevision++;
        for (const [socket, subscriptions] of opens) for (const subscription of subscriptions) {
          if (!subscription.path.startsWith("tasks.")) continue;
          const visible = subscription.path === 'tasks.visible' ? changed.filter(value => Number(value.id) < 250) : changed;
          send(socket, { type: 'replica.delta', id: subscription.id, cursor: { epoch: 'fixture', revision: serverRevision }, upserts: visible });
        }
        await sleep(50);
      }
      response.end('ok'); return;
    }
    if (new URL(request.url, 'http://fixture').pathname === '/') { response.setHeader('content-type', 'text/html'); response.end('<main></main><button>click</button><script type="module" src="/browser.js"></script>'); return; }
    const path = resolve(output, new URL(request.url, 'http://fixture').pathname.slice(1)); if (!path.startsWith(output + '/')) throw new Error('Invalid path');
    response.setHeader('content-type', 'text/javascript'); response.end(await readFile(path));
  } catch (error) { response.statusCode = 500; response.end(String(error)); }
});
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', socket => {
  if (offline) { socket.close(); return; }
  sockets.add(socket); opens.set(socket, []);
  send(socket, { type: 'session.ready', replica: directive, capabilities: { replicaSnapshotPages: 1, replicaWatermark: 1, reducerBatch: 1 } });
  let lane = Promise.resolve();
  socket.on('message', bytes => { lane = lane.then(async () => {
    const frame = JSON.parse(String(bytes));
    if (process.env.DEBUG_FIXTURE) process.stdout.write(`WIRE ${frame.type} ${frame.path ?? frame.opens?.map(open => open.path).join(",")}\n`);
    if (frame.type === 'auth') {
      await sleep(50); send(socket, { type: 'auth.result', id: frame.id, result: { replica: directive, localIdentity: identity, artifactHash: 'engine-worker-fixture-v1', accountId: 'account', tenantId: 'tenant' } });
    } else if (frame.type === 'replica.open' || frame.type === 'replica.openMany') {
      for (const subscription of frame.opens ?? [frame]) {
        if (!opens.has(socket)) return;
        opens.get(socket).push(subscription);
        const result = subscription.path === '__local.edits' ? [] : subscription.path === 'tasks.full' ? rows : rows.slice(0, subscription.path === 'tasks.source' ? 1_000 : 250);
        const hashes = await replicaRowsHashes(result, 'id'); const digest = replicaAggregateDigest(hashes);
        const base = { id: subscription.id, path: subscription.path, key: 'id', cursor: { epoch: 'fixture', revision: serverRevision }, mode: 'eager', maxRows: result.length, maxBytes: 100_000_000, truncated: false };
        if (subscription.path === 'tasks.full') {
          for (let offset = 0, sequence = 0; offset < result.length; offset += 100, sequence++) {
            const last = offset + 100 >= result.length;
            const page = { ...base, type: 'replica.snapshotPage', snapshotId: 'snapshot', scope: 'tenant', visibilityScope: 'visible', sequence, last, result: result.slice(offset, offset + 100), ...(last ? { digest } : {}) };
            await sleep(JSON.stringify(page).length / 2_500_000 * 1_000); send(socket, page);
          }
        } else send(socket, { ...base, type: 'replica.snapshot', result, hashes, digest });
        send(socket, { type: 'replica.ready', id: subscription.id, cursor: base.cursor, digest, key: 'id', complete: true });
      }
    } else if (frame.type === 'replica.close') { opens.set(socket, opens.get(socket).filter(value => value.id !== frame.id)); }
    else if (frame.type === 'reducer.call' || frame.type === 'reducer.callMany') {
      const calls = frame.calls ?? [frame];
      wireProfile[frame.type === 'reducer.callMany' ? 'callManyFrames' : 'callFrames']++;
      wireProfile.calls += calls.length; wireProfile.batchSizes.push(calls.length);
      await sleep(50);
      for (const call of frame.calls ?? [frame]) {
        const key = call.idempotencyKey ?? call.id; if (!effects.has(key)) effects.set(key, call.args);
        serverRevision++;
        for (const subscription of opens.get(socket) ?? []) if (subscription.path === '__local.edits') send(socket, { type: 'replica.delta', id: subscription.id, cursor: { epoch: 'fixture', revision: serverRevision }, upserts: [{ id: call.args.id, count: 1 }], originCommandIds: [key] });
        send(socket, { type: 'reducer.result', id: call.id, originCommandId: key, result: 1, committedRevision: serverRevision });
        send(socket, { type: 'replica.watermark', revision: serverRevision });
      }
    }
  }).catch(error => { process.stderr.write(String(error) + '\n'); }); });
  socket.on('close', () => { sockets.delete(socket); opens.delete(socket); });
});
await new Promise(resolve => server.listen(Number(process.env.PORT ?? 48839), '127.0.0.1', resolve)); const query = new URLSearchParams();
if (process.env.PIPELINE) query.set("pipeline", "");
if (process.env.PROFILE_IO) query.set("io", "");
const url = `http://127.0.0.1:${server.address().port}${query.size ? `?${query}` : ""}`;
const browserName = process.env.BROWSER ?? 'chromium';
const context = await ({ chromium, firefox, webkit }[browserName]).launchPersistentContext(resolve(output, `profile-${browserName}`), { headless: true });
const browser = context.browser(); const page = await context.newPage();
page.on('console', message => { if (/^(Seed |Engine )/.test(message.text())) process.stdout.write(message.text() + '\n'); });
page.on('pageerror', error => process.stderr.write(`PAGE: ${error}\n`));
const loadLimit = Number(process.env.LOAD_LIMIT ?? .5);
const startLoadLimit = Math.min(loadLimit, Number(process.env.START_LOAD_LIMIT ?? loadLimit));
const measured = Number(process.env.ITERATIONS ?? 20), warmups = Number(process.env.WARMUPS ?? 5);
const samples = []; const metadata = { browser: browserName, version: browser.version(), sdkCommit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(), fixture: 'engine-worker-fixture-v1', admission: process.env.PIPELINE ? 'pipelined' : 'sequential', cpu: cpus()[0].model, cores: cpus().length, ram: totalmem(), os: `${process.platform} ${release()}`, network: 'local websocket fixture; auth/reducer 50ms modeled RTT, pages paced at 20Mbit/s; storage separate', load: loadavg(), production: true, coldSamplingAligned: true, engineRevision: revision ?? "checkout", profileIO: Boolean(process.env.PROFILE_IO), loadLimit, startLoadLimit, loadChecks: [], fixtureIsolation: process.env.KEEP_EDITS ? "edits-accumulate-across-modes" : "edits-reset-before-each-mode", firefoxWorkerWebSocketWorkaround: workaroundApplied() };
try {
  await page.goto(url); await page.waitForFunction(() => window.benchmark);
  process.stdout.write(`Preparing ${process.env.SEED_ROWS ?? 50_000}-row canonical IDB fixture...\n`);
  metadata.seed = await page.evaluate(count => window.benchmark.seed(count), Number(process.env.SEED_ROWS ?? 50_000));
  process.stdout.write(`Seed ready: ${metadata.seed.rowBytes} bytes/row\n`);
  const waitForQuiet = async () => {
    while (loadavg()[0] / cpus().length > startLoadLimit) {
      process.stdout.write(`Waiting for quiet machine: load ${loadavg()[0].toFixed(2)} / ${cpus().length} cores\n`);
      await sleep(30_000);
    }
  };
  metadata.discardedPairs = [];
  for (let index = -warmups; index < measured; index++) {
    let complete = false;
    while (!complete) {
      if (process.env.WAIT_LOAD) await waitForQuiet();
      const pair = [];
      let session;
      try {
        for (const mode of index % 2 === 0 ? ['legacy', 'worker'] : ['worker', 'legacy']) {
          const checkStart = metadata.loadChecks.length;
          const load = loadavg(); metadata.loadChecks.push({ index, mode, load });
          if (load[0] / cpus().length > loadLimit) throw new Error("Load check failed before mode");
          await page.goto(url); await page.waitForFunction(() => window.benchmark);
          if (!process.env.KEEP_EDITS) await page.evaluate(() => window.benchmark.resetEdits());
          session = browserName === 'chromium' ? await context.newCDPSession(page) : null;
          await session?.send('Performance.enable');
          const metrics = async () => { const values = await session?.send('Performance.getMetrics'); return values?.metrics.find(value => value.name === 'ScriptDuration')?.value ?? null; };
          const run = async name => {
            process.stdout.write(`${index} ${mode} ${name}...\n`);
            const before = await metrics(); let overloaded = false;
            const observeLoad = () => {
              const load = loadavg(); metadata.loadChecks.push({ index, mode, workload: name, load });
              if (load[0] / cpus().length > loadLimit) overloaded = true;
            };
            const monitor = setInterval(observeLoad, 10_000);
            let value;
            try { value = await page.evaluate(async ({ name, mode }) => name === 'cold' ? window.benchmark.start(mode) : window.benchmark[name](), { name, mode }); }
            finally { clearInterval(monitor); }
            observeLoad();
            if (overloaded) throw new Error(`Load check failed during ${mode} ${name}; entire pair excluded`);
            const after = await metrics(); process.stdout.write(`${name}: ${value.wallMs.toFixed(1)}ms\n`);
            return { ...value, scriptMs: before === null ? null : (after - before) * 1_000 };
          };
          const cold = await run('cold'); if (cold.mode !== mode) throw new Error(`Mode silently fell back: ${mode} => ${cold.mode}`);
          await page.evaluate(() => window.benchmark.prepare());
          if (index === -warmups && mode === 'worker' && !metadata.correctness) metadata.correctness = await page.evaluate(() => window.benchmark.parity());
          const burst = await run('burst');
          const snapshot = process.env.SKIP_SNAPSHOT ? null : await run('snapshot');
          const drain = process.env.SKIP_DRAIN ? null : await run('drain');
          await page.evaluate(() => window.benchmark.stop()); await session?.detach(); session = null;
          pair.push({ index, mode, cold, burst, snapshot, drain, load: loadavg(), loadChecks: metadata.loadChecks.slice(checkStart) });
          process.stdout.write(`${index} ${mode}: cold ${cold.wallMs.toFixed(1)}ms, burst script ${burst.scriptMs?.toFixed(1)}ms\n`);
        }
        if (index >= 0) samples.push(...pair);
        complete = true;
      } catch (error) {
        await page.evaluate(() => window.benchmark.stop()).catch(() => undefined); await session?.detach().catch(() => undefined);
        if (!process.env.WAIT_LOAD || !String(error).includes('Load check failed')) throw error;
        metadata.discardedPairs.push({ index, reason: String(error), load: loadavg() });
        process.stdout.write(`Discarding pair ${index}; will retry after load drops\n`);
      }
      await writeFile(resolve(root, process.env.RESULTS ?? 'results.json'), JSON.stringify({ metadata, samples }, null, 2));
    }
  }
  // A second engine-capable tab makes both foreground tabs retain legacy mode.
  await page.goto(url); await page.waitForFunction(() => window.benchmark);
  process.stdout.write("Multi-tab: first worker start\n"); await page.bringToFront();
  await page.evaluate(() => window.benchmark.start("worker"));
  await page.evaluate(() => window.benchmark.prepare());
  process.stdout.write("Multi-tab: second tab start\n");
  const second = await context.newPage(); await second.goto(url); await second.waitForFunction(() => window.benchmark); await second.bringToFront();
  second.on("console", message => process.stdout.write(message.text() + "\n"));
  await second.evaluate(() => window.benchmark.start("worker"));
  await second.evaluate(() => window.benchmark.prepare());
  await page.waitForFunction(() => window.benchmark.mode() === "legacy");
  metadata.multiTab = { first: await page.evaluate(() => window.benchmark.mode()), second: await second.evaluate(() => window.benchmark.mode()) };
  await second.evaluate(() => window.benchmark.stop()); await second.close(); await page.evaluate(() => window.benchmark.stop());
  await writeFile(resolve(root, process.env.RESULTS ?? 'results.json'), JSON.stringify({ metadata, samples }, null, 2));
} catch (error) {
  metadata.failure = String(error);
  await writeFile(resolve(root, process.env.RESULTS ?? 'results.json'), JSON.stringify({ metadata, samples }, null, 2));
  throw error;
} finally { await context.close(); for (const socket of sockets) socket.terminate(); wss.close(); server.close(); }
