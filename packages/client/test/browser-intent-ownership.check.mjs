import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { globSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from '@playwright/test';
import { extractIntentPublication } from '../../../verification/typescript/intent-publication.mjs';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const hash = value => createHash('sha256').update(value).digest('hex');
test('native IndexedDB intent observers fence superseded read completions', { timeout: 60000 }, async () => {
  const parent = process.env.GONVEX_BROWSER_TEST_DIR;
  assert(parent, 'owned browser artifact directory is required');
  await mkdir(parent, { recursive: true });
  const temporary = await mkdtemp(join(parent, 'gonvex-intents-'));
  const sourcePath = resolve(root, 'packages/client/src/index.ts');
  const source = await readFile(sourcePath, 'utf8');
  const method = extractIntentPublication(source).method;
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert(bundler, 'frozen esbuild is required');
  const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const variants = [
    ['candidate', null, null, ['unchanged', 'account', 'tenant', 'return', 'close']],
    ['ignore-activation', 'scope, this.outboxScope, generation,', 'scope, this.outboxScope, this.outboxScopeGeneration,', ['return']],
    ['scope-only-guard', '!ownsCurrentScope(scope, this.outboxScope, generation,\n            this.outboxScopeGeneration, this.manuallyClosed)', 'scope !== this.outboxScope', ['close']],
  ];
  const receipt = { complete: false, sourceSha256: hash(source), bundledInputs: {}, cases: [],
    boundary: 'Actual Chromium and native IndexedDB through DexieReducerOutbox, public SDK authentication and intent observer. Holds completion after an actual native journal read. No backend, process crash, mobile driver or power-loss proof.' };
  let server;
  try {
    const bundles = {};
    for (const [name, before, after] of variants) {
      let changes = 0;
      const candidateMethod = before ? method.replace(before, after) : method;
      if (before) assert.notEqual(candidateMethod, method);
      const bundle = await build({ entryPoints: [resolve(root, 'packages/client/test/browser-intent-ownership-entry.mjs')],
        bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true,
        plugins: before ? [{ name: 'intent-ownership-control', setup(builder) {
          builder.onLoad({ filter: /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
            assert.equal(path, sourcePath); changes++;
            assert.equal(await readFile(path, 'utf8'), source);
            return { contents: source.replace(method, candidateMethod), loader: 'ts' };
          });
        } }] : [] });
      assert.equal(changes, before ? 1 : 0);
      for (const input of Object.keys(bundle.metafile.inputs)) {
        const label = relative(root, resolve(input));
        if (label.startsWith('packages/') && !label.includes('/node_modules/')) receipt.bundledInputs[label] = hash(await readFile(resolve(input)));
      }
      bundles['/' + name + '.js'] = bundle.outputFiles[0].contents;
      receipt[name + 'BundleSha256'] = hash(bundle.outputFiles[0].contents);
    }
    server = createServer((req, res) => {
      if (bundles[req.url]) { res.setHeader('Content-Type', 'text/javascript'); res.end(bundles[req.url]); }
      else { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>Intent ownership contract</title>'); }
    });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
    const origin = `http://127.0.0.1:${server.address().port}`;
    for (const [name, before, , cases] of variants) {
      const context = await chromium.launchPersistentContext(join(temporary, name), { headless: true, args: ['--no-sandbox'], serviceWorkers: 'block' });
      try {
        const page = context.pages()[0]; await page.goto(origin);
        await page.addScriptTag({ url: origin + '/' + name + '.js' });
        receipt.browserVersion ??= context.browser().version();
        for (const change of cases) {
          const result = await page.evaluate(({ name, change }) => window.intentRefreshOwnership(name + '-' + change, change), { name, change });
          assert(result.nativeReadCompletedBeforeRotation);
          const stale = result.observed.some(rows => rows.includes('tasks.old'));
          assert.equal(stale, !!before || change === 'unchanged', 'unexpected stale intent notification: ' + JSON.stringify(result));
          if (!before) assert.deepEqual(result.final, change === 'unchanged' ? ['tasks.old'] : change === 'return' ? ['tasks.latest'] : []);
          assert.equal(result.durable.length, 1);
          assert.equal(result.durable[0].path, change === 'return' ? 'tasks.latest' : 'tasks.old');
          assert.equal(result.durable[0].key, result.durable[0].path);
          assert.equal(result.durable[0].state, 'rejected');
          receipt.cases.push({ variant: name, ...result, expectedStaleControl: !!before });
        }
      } finally { await context.close(); }
    }
    assert.equal(receipt.cases.length, 7);
    assert.equal(await readFile(sourcePath, 'utf8'), source);
    receipt.complete = true;
    receipt.driverSha256 = hash(await readFile(fileURLToPath(import.meta.url)));
    if (process.env.GONVEX_INTENT_BROWSER_OUTPUT) await writeFile(process.env.GONVEX_INTENT_BROWSER_OUTPUT, JSON.stringify(receipt, null, 2) + '\n');
  } finally {
    if (server) await new Promise(done => server.close(done));
    await rm(temporary, { recursive: true, force: true });
  }
});
