import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { globSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from '@playwright/test';
import { queryPublicationControls } from '../../../verification/typescript/query-publication-controls.mjs';
import { verifyQueryPublication } from '../../../verification/typescript/query-publication.mjs';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const hash = value => createHash('sha256').update(value).digest('hex');
test('native IndexedDB query publication retains owner and durable ordering', { timeout: 180000 }, async () => {
  const parent = process.env.GONVEX_BROWSER_TEST_DIR;
  assert(parent, 'owned browser artifact directory is required');
  await mkdir(parent, { recursive: true });
  const temporary = await mkdtemp(join(parent, 'gonvex-query-publication-'));
  const sourcePath = resolve(root, 'packages/client/src/index.ts'), source = await readFile(sourcePath, 'utf8');
  const structural = verifyQueryPublication(source);
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert(bundler); const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const controls = queryPublicationControls(source);
  const candidateCases = [];
  for (const family of ['reentrant', 'cache']) for (const collection of [false, true])
    for (const change of ['unchanged', 'account', 'tenant', 'return', 'close']) candidateCases.push({ family, collection, change });
  candidateCases.push({ family: 'durability' }, { family: 'failure' },
    ...['unchanged', 'return', 'close'].map(change => ({ family: 'deferred', change })),
    ...['error', 'result'].map(change => ({ family: 'order', change })));
  assert.equal(candidateCases.length, 27);
  const brokenCases = [
    ['query-listener-once', { family: 'reentrant', change: 'account' }],
    ['query-cache-without-owner', { family: 'cache', change: 'close' }],
    ['received-cache-before-commit', { family: 'durability' }],
    ['publish-failed-write', { family: 'failure' }],
    ['query-scope-only', { family: 'deferred', change: 'return' }],
    ['replica-listener-once', { family: 'reentrant', collection: true, change: 'close' }],
    ['replica-cache-without-owner', { family: 'cache', collection: true, change: 'close' }],
    ['bypass-publication-order', { family: 'order', change: 'error' }],
  ];
  const variants = [{ ...controls[0], schedules: candidateCases }, ...brokenCases.map(([name, schedule]) => ({
    ...controls.find(control => control.name === name), schedules: [schedule], broken: true,
  }))];
  const receipt = { complete: false, sourceSha256: hash(source), bundledInputs: {}, cases: [], structural,
    boundary: 'Actual Chromium, native IndexedDB and both shipping browser storage adapters. Public SDK callbacks use deterministic wire frames. Held writes are checked by an independent storage handle; failure aborts the native transaction after an entity put succeeds. Eight mutated source bundles must expose the intended callback or ordering violation. No backend authentication, process crash or mobile runtime claim.' };
  let server;
  try {
    const bundles = {};
    for (const variant of variants) {
      let changes = 0;
      const bundle = await build({ entryPoints: [resolve(root, 'packages/client/test/browser-query-publication-entry.mjs')],
        bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true,
        plugins: [{ name: 'query-publication-input', setup(builder) {
          builder.onLoad({ filter: /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
            assert.equal(path, sourcePath); changes++; assert.equal(await readFile(path, 'utf8'), source);
            return { contents: variant.candidate ?? source, loader: 'ts' };
          });
        } }] });
      assert.equal(changes, 1);
      for (const input of Object.keys(bundle.metafile.inputs)) {
        const label = relative(root, resolve(input));
        if (label.startsWith('packages/') && !label.includes('/node_modules/')) receipt.bundledInputs[label] = hash(await readFile(resolve(input)));
      }
      bundles['/' + variant.name + '.js'] = bundle.outputFiles[0].contents;
      receipt[variant.name + 'BundleSha256'] = hash(bundle.outputFiles[0].contents);
    }
    server = createServer((req, res) => {
      if (bundles[req.url]) { res.setHeader('Content-Type', 'text/javascript'); res.end(bundles[req.url]); }
      else { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>Query publication contract</title>'); }
    });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
    const origin = `http://127.0.0.1:${server.address().port}`;
    for (const variant of variants) {
      const context = await chromium.launchPersistentContext(join(temporary, variant.name), { headless: true, args: ['--no-sandbox'], serviceWorkers: 'block' });
      try {
        const page = context.pages()[0]; await page.goto(origin); await page.addScriptTag({ url: origin + '/' + variant.name + '.js' });
        receipt.browserVersion ??= context.browser().version();
        for (const kind of ['legacy', 'engine']) for (const [index, schedule] of variant.schedules.entries()) {
          const result = await page.evaluate(args => window.queryPublicationOwnership(args), { kind,
            name: variant.name + '-' + kind + '-' + index, ...schedule });
          const { family, change } = result;
          let unsafe;
          if (family === 'reentrant' || family === 'cache') {
            unsafe = change !== 'unchanged' && result.stale.length > 0;
            if (change === 'unchanged') assert.equal(result.stale.length, 1);
            if (!['close', 'unchanged'].includes(change)) assert(result.observed.some(row => row.result?.[0]?.title === 'Fresh data'));
          } else if (family === 'durability' || family === 'failure') {
            assert.equal(result.durableBefore, null, 'independent reader found a row before commit or after abort');
            unsafe = result.before.length > 0;
            assert.equal(result.durable.title, family === 'failure' ? 'Recovered data' : 'Old data');
            if (family === 'failure') assert.deepEqual(result.nativeFailure, { witnessed: 1, aborts: 1 });
          } else if (family === 'deferred') {
            unsafe = change !== 'unchanged' && result.before.length > 0;
            if (change === 'unchanged') assert.equal(result.before.length, 1);
            if (change === 'return') assert(result.initial.some(row => row.result?.[0]?.title === 'Fresh data'));
          } else {
            unsafe = result.before.length > 0;
            if (!variant.broken) assert.equal(result.initial[0].result[0].title, 'Old data');
          }
          assert.equal(unsafe, !!variant.broken, variant.name + ': unexpected publication outcome ' + JSON.stringify(result));
          receipt.cases.push({ variant: variant.name, expectedUnsafeControl: !!variant.broken, ...result });
        }
      } finally { await context.close(); }
    }
    assert.equal(receipt.cases.length, 70); assert.equal(await readFile(sourcePath, 'utf8'), source);
    receipt.complete = true; receipt.driverSha256 = hash(await readFile(fileURLToPath(import.meta.url)));
    receipt.controlsSha256 = hash(await readFile(new URL("../../../verification/typescript/query-publication-controls.mjs", import.meta.url)));
    if (process.env.GONVEX_QUERY_BROWSER_OUTPUT) await writeFile(process.env.GONVEX_QUERY_BROWSER_OUTPUT, JSON.stringify(receipt, null, 2) + '\n');
  } finally {
    if (server) await new Promise(done => server.close(done));
    await rm(temporary, { recursive: true, force: true });
  }
});
