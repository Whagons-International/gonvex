import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { globSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from '@playwright/test';
import { WebSocketServer, WebSocket } from 'ws';
import { controlWatchControls } from '../../../verification/typescript/control-watch-controls.mjs';
import { verifyControlWatch } from '../../../verification/typescript/control-watch-publication.mjs';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const hash = value => createHash('sha256').update(value).digest('hex');
test('actual Chromium WebSocket close ordering cannot hide owner invalidation', { timeout: 90000 }, async () => {
  const parent = process.env.GONVEX_BROWSER_TEST_DIR; assert(parent, 'owned browser artifact directory is required');
  await mkdir(parent, { recursive: true }); const temporary = await mkdtemp(join(parent, 'gonvex-control-close-'));
  const sourcePath = resolve(root, 'packages/client/src/index.ts'), source = await readFile(sourcePath, 'utf8');
  const structural = verifyControlWatch(source), controls = controlWatchControls(source, structural.method);
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert(bundler); const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const orders = ['before-sdk', 'after-sdk', 'after-sdk-reconnect', 'unchanged'];
  const variants = [{ ...controls[0], orders },
    { ...controls.find(control => control.name === 'clear-uses-transport-owner'), orders: ['after-sdk-reconnect'], broken: true }];
  if (process.env.GONVEX_CONTROL_CLOSE_BASELINE_SOURCE) {
    variants.push({ ...controls.find(control => control.name === 'clear-uses-receipt-activation'), orders: ['before-sdk'], probe: true });
    variants.push({ name: 'before-fix',
    candidate: await readFile(process.env.GONVEX_CONTROL_CLOSE_BASELINE_SOURCE, 'utf8'), orders, baseline: true });
  }
  const receipt = { complete: false, sourceSha256: hash(source), structural, cases: [], bundledInputs: {},
    boundary: 'Actual Chromium native WebSocket and Node ws loopback server. Remote close travels over TCP. A standard constructor wrapper installs its close observer before or after SDK registration; a public watch observer may call connect. Server admission of the second connection is held until the renderer-state oracle runs, then fresh-owner progress is required. No SDK private fields or synthetic close dispatch, backend authorization or storage durability claim.' };
  const states = new Map(); let server, websocket;
  try {
    const bundles = {};
    for (const variant of variants) {
      let transformed = 0;
      const bundle = await build({ entryPoints: [resolve(root, 'packages/client/test/browser-control-close-entry.mjs')],
        bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true,
        plugins: [{ name: 'native-control-close-input', setup(builder) {
          builder.onLoad({ filter: /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
            assert.equal(path, sourcePath); transformed++; assert.equal(await readFile(path, 'utf8'), source);
            return { contents: variant.candidate, loader: 'ts' };
          });
        } }] });
      assert.equal(transformed, 1); bundles['/' + variant.name + '.js'] = bundle.outputFiles[0].contents;
      receipt[variant.name + 'SourceSha256'] = hash(variant.candidate);
      receipt[variant.name + 'BundleSha256'] = hash(bundle.outputFiles[0].contents);
      for (const input of Object.keys(bundle.metafile.inputs)) {
        const label = relative(root, resolve(input));
        if (label.startsWith('packages/') && !label.includes('/node_modules/')) receipt.bundledInputs[label] = hash(await readFile(resolve(input)));
      }
    }
    server = createServer((req, res) => {
      if (bundles[req.url]) { res.setHeader('Content-Type', 'text/javascript'); res.end(bundles[req.url]); }
      else { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>Native control close</title>'); }
    });
    websocket = new WebSocketServer({ server });
    websocket.on('connection', (peer, request) => {
      const id = new URL(request.url, 'http://fixture').searchParams.get('case'), state = states.get(id);
      assert(state, 'unknown fixture connection'); const number = state.connections++; state.peers.push(peer);
      peer.on('message', async data => {
        const frame = JSON.parse(String(data));
        if (frame.type === 'auth') {
          if (number > 0) await state.ready;
          if (peer.readyState === WebSocket.OPEN) peer.send(JSON.stringify({ type: 'auth.result', id: frame.id, result: {} }));
        } else if (frame.type === 'query.subscribe' && peer.readyState === WebSocket.OPEN) {
          peer.send(JSON.stringify({ type: 'query.result', id: frame.id,
            result: { private: number === 0 ? 'Alice control data' : 'Bob control data' } }));
        }
      });
    });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
    const origin = `http://127.0.0.1:${server.address().port}`;
    for (const variant of variants) {
      const context = await chromium.launchPersistentContext(join(temporary, variant.name), { headless: true, args: ['--no-sandbox'], serviceWorkers: 'block' });
      try {
        const page = context.pages()[0], errors = []; page.on('pageerror', error => errors.push(String(error)));
        await page.exposeFunction('controlCloseFixture', ({ id, action }) => {
          const state = states.get(id); assert(state);
          if (action === 'disconnect') state.peers[0].close(1000, 'fixture remote disconnect');
          else if (action === 'release-auth') state.release();
          else { assert.equal(action, 'cleanup'); state.release(); for (const peer of state.peers) peer.terminate(); states.delete(id); }
        });
        await page.goto(origin); await page.addScriptTag({ url: origin + '/' + variant.name + '.js' });
        receipt.browserVersion ??= context.browser().version();
        for (const order of variant.orders) {
          const id = variant.name + '-' + order; let release;
          const ready = new Promise(done => { release = done; }); states.set(id, { release, ready, connections: 0, peers: [] });
          const result = await page.evaluate(args => window.controlCloseOwnership(args), { id, order, url: origin.replace('http:', 'ws:') + '/ws?case=' + id });
          assert(result.nativeConstructor); assert.equal(result.afterClose.closeCode, 1000);
          const trace = result.afterClose.trace, external = trace.indexOf('external-close'), sdk = trace.indexOf('sdk-close');
          assert(external >= 0 && sdk >= 0);
          assert.equal(external < sdk, !order.startsWith('after-sdk'));
          const unsafe = order !== 'unchanged' && result.afterClose.rendered !== null;
          const expectedUnsafe = !!variant.broken || !!variant.baseline && order === 'after-sdk-reconnect';
          if (variant.probe) assert(trace.indexOf('clear-notification') < sdk, 'Chromium receipt probe requires its observed inter-listener clear checkpoint');
          assert.equal(unsafe, expectedUnsafe, JSON.stringify({ variant: variant.name, ...result }));
          if (order === 'unchanged') { assert(result.afterClose.sameSnapshot); assert.deepEqual(result.final, { private: 'Alice control data' }); }
          else {
            assert.equal(result.afterClose.snapshot, null);
            assert.deepEqual(result.final, { private: 'Bob control data' });
            if (!unsafe) assert(result.afterClose.observed.includes(null));
            if (order === 'after-sdk-reconnect') assert(result.afterClose.forced);
          }
          receipt.cases.push({ variant: variant.name, expectedUnsafeControl: expectedUnsafe, engineOrderingProbe: !!variant.probe, ...result });
        }
        assert.deepEqual(errors, []);
      } finally { await context.close(); }
    }
    assert.equal(receipt.cases.length, process.env.GONVEX_CONTROL_CLOSE_BASELINE_SOURCE ? 10 : 5);
    assert.equal(await readFile(sourcePath, 'utf8'), source);
    receipt.complete = true; receipt.driverSha256 = hash(await readFile(fileURLToPath(import.meta.url)));
    receipt.controlsSha256 = hash(await readFile(new URL('../../../verification/typescript/control-watch-controls.mjs', import.meta.url)));
    if (process.env.GONVEX_CONTROL_CLOSE_BROWSER_OUTPUT) await writeFile(process.env.GONVEX_CONTROL_CLOSE_BROWSER_OUTPUT, JSON.stringify(receipt, null, 2) + '\n');
  } finally {
    for (const state of states.values()) { state.release(); for (const peer of state.peers) peer.terminate(); }
    if (websocket) await new Promise(done => websocket.close(done));
    if (server) await new Promise(done => server.close(done));
    await rm(temporary, { recursive: true, force: true });
  }
});
