// Built package + native Chrome IndexedDB, with the existing root Playwright dependency.
import { chromium } from '@playwright/test';
import { createRequire } from 'node:module';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
const clientRequire = createRequire(new URL('../package.json', import.meta.url));
const vitestRequire = createRequire(clientRequire.resolve('vitest/package.json'));
const viteRequire = createRequire(vitestRequire.resolve('vite/package.json'));
const { build } = viteRequire('esbuild');
const temporary = await mkdtemp(join(tmpdir(), 'replica-chrome-'));
let browser, server;
try {
  const entry = fileURLToPath(new URL('./replica-idb-browser-entry.mjs', import.meta.url));
  await build({ entryPoints: [entry], bundle: true, format: 'esm', platform: 'browser', outfile: join(temporary, 'bench.js'), plugins: process.env.CLIENT_DIST ? [{ name: 'client-dist', setup(builder) { builder.onResolve({ filter: /^\.\.\/dist\// }, args => ({ path: resolve(process.env.CLIENT_DIST, args.path.split('/').at(-1)) })); } }] : [] });
  const bundle = await readFile(join(temporary, 'bench.js'));
  server = createServer((request, response) => { response.setHeader('Content-Type', request.url === '/bench.js' ? 'text/javascript' : 'text/html'); response.end(request.url === '/bench.js' ? bundle : '<!doctype html><script type="module" src="/bench.js"></script>'); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ executablePath: '/opt/google/chrome/chrome', headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage(); page.on('console', message => process.stderr.write(`${message.text()}\n`));
  await page.goto(`http://127.0.0.1:${server.address().port}`); await page.waitForFunction(() => typeof runBenchmark === 'function');
  const result = await page.evaluate(counts => runBenchmark(counts), (process.env.ROWS ?? '10000,50000').split(',').map(Number));
  console.log(JSON.stringify({ measuredAt: new Date().toISOString(), chrome: await browser.version(), ...result }, null, 2));
} finally { await browser?.close(); server?.close(); await rm(temporary, { recursive: true, force: true }); }
