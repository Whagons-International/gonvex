import assert from 'node:assert/strict';
import { before, after, test } from 'node:test';
import { createServer } from 'node:http';
import { globSync } from 'node:fs';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { chromium } from '@playwright/test';

const directory = dirname(fileURLToPath(import.meta.url)), root = resolve(directory, '../../..');
const receipt = { complete: false, scope: 'Real Chromium IndexedDB with owned browser process crash. No server, native SQLite or power-loss claim.', cases: [] };
let temporary, server, origin;
before(async () => {
  const parent = process.env.GONVEX_BROWSER_TEST_DIR;
  assert.ok(parent, 'GONVEX_BROWSER_TEST_DIR must select an owned build directory');
  await mkdir(parent, { recursive: true }); temporary = await mkdtemp(join(parent, 'upgrade-chromium-'));
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert.ok(bundler);
  const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const result = await build({ entryPoints: [join(directory, 'browser-upgrade-recovery-entry.mjs')], bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true });
  receipt.bundledProjectInputs ??= {};
  for (const input of Object.keys(result.metafile.inputs).sort()) {
    const path = resolve(input), label = relative(root, path);
    if (label.startsWith('packages/') && !label.includes('/node_modules/'))
      receipt.bundledProjectInputs[label] = createHash('sha256').update(await readFile(path)).digest('hex');
  }
  const code = result.outputFiles[0].contents;
  receipt.bundleSha256 = createHash('sha256').update(code).digest('hex');
  receipt.testDriverSha256 = createHash('sha256').update(await readFile(fileURLToPath(import.meta.url))).digest('hex');
  receipt.fixtureSha256 = createHash('sha256').update(await readFile(join(directory, 'browser-upgrade-recovery-entry.mjs'))).digest('hex');
  receipt.sourceSha256 = {};
  for (const name of ['browser-upgrades.ts', 'client-upgrades.ts', 'outbox.ts', 'outbox-policy.ts', 'outbox-recovery.ts', 'coalescing-policy.ts', 'kv-stores.ts', 'kv-sequence.ts'])
    receipt.sourceSha256[name] = createHash('sha256').update(await readFile(join(directory, '../src', name))).digest('hex');
  server = createServer((request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    response.setHeader('Content-Type', request.url === '/fixture.js' ? 'text/javascript' : 'text/html');
    response.end(request.url === '/fixture.js' ? code : '<!doctype html><title>Upgrade storage fixture</title>');
  });
  await new Promise(done => server.listen(0, '127.0.0.1', done)); origin = `http://127.0.0.1:${server.address().port}`;
});
after(async () => {
  receipt.complete = receipt.cases.length === 4 && receipt.cases.every(item => item.complete);
  if (process.env.GONVEX_BROWSER_UPGRADE_OUTPUT) await writeFile(process.env.GONVEX_BROWSER_UPGRADE_OUTPUT, JSON.stringify(receipt, null, 2) + '\n');
  if (server) await new Promise(done => server.close(done));
  if (temporary) await rm(temporary, { recursive: true, force: true });
  assert.ok(receipt.complete, 'all four browser upgrade/recovery contracts must pass');
});
async function launch(profile) {
  const context = await chromium.launchPersistentContext(profile, { headless: true, args: ['--no-sandbox'], serviceWorkers: 'block' });
  const page = context.pages()[0]; await page.goto(origin); await page.addScriptTag({ url: origin + '/fixture.js' });
  receipt.browserVersion ??= context.browser().version(); return { context, page };
}
async function crash(context) {
  const browser = context.browser(), session = await browser.newBrowserCDPSession();
  const { processInfo } = await session.send('SystemInfo.getProcessInfo');
  const process = processInfo.find(info => info.type === 'browser');
  assert.ok(Number.isInteger(process?.id) && process.id > 1);
  const command = await readFile(`/proc/${process.id}/cmdline`, 'utf8');
  assert.ok(command.includes(temporary + '/') && command.includes('--user-data-dir='), 'refuse to crash an unowned browser');
  const exited = new Promise((done, reject) => {
    const timer = setTimeout(() => reject(Error('browser did not disconnect')), 10000);
    browser.once('disconnected', () => { clearTimeout(timer); done(); });
  });
  globalThis.process.kill(process.id, 'SIGKILL'); await exited; assert.equal(browser.isConnected(), false);
}

test('accepted migration survives a browser process crash without rearming or rerunning', async () => {
  const profile = join(temporary, 'accepted'); let { context, page } = await launch(profile);
  try {
    await page.evaluate(() => upgradeRecovery.seed());
    const pending = page.evaluate(() => upgradeRecovery.pauseUpgrade()).then(() => ({ completed: true }), error => ({ error: String(error) }));
    await page.waitForFunction(() => window.upgradeWriteObserved === true);
    assert.equal(await page.evaluate(() => window.upgradeTransformCount), 1);
    await crash(context); assert.match((await pending).error, /closed|crash/i);
    ({ context, page } = await launch(profile));
    const result = await page.evaluate(() => upgradeRecovery.resumeUpgrade());
    assert.equal(result.entries.length, 1); assert.equal(result.entries[0].state, 'committed');
    assert.equal(result.entries[0].idempotencyKey, 'stable-command'); assert.equal(result.entries[0].receiptPath, 'tasks.save');
    assert.equal(result.entries[0].path, 'tasks.v2'); assert.equal(result.entries[0].id, 1);
    assert.deepEqual(result.snapshot.entities.tasks.a, { name: 'durable' }); assert.equal(result.snapshot.cursor, undefined);
    receipt.cases.push({ name: 'accepted-upgrade-crash', complete: true });
  } finally { if (context.browser()?.isConnected()) await context.close(); }
});
test('ownerless legacy history remains in the aborted native IndexedDB upgrade', async () => {
  const { context, page } = await launch(join(temporary, 'ownerless'));
  try {
    const result = await page.evaluate(() => upgradeRecovery.legacyOwnerless());
    assert.match(result.error, /scope|owner|recovery/i); assert.equal(result.version, 1);
    assert.equal(result.rows.length, 1); assert.equal(result.rows[0].state, 'committed'); assert.equal(result.rows[0].idempotencyKey, 'stable-command');
    receipt.cases.push({ name: 'ownerless-upgrade-rollback', complete: true });
  } finally { await context.close(); }
});
test('scoped version-1 history preserves identity, ordering and acceptance across upgrade and browser restart', async () => {
  const profile = join(temporary, 'legacy-scoped');
  let { context, page } = await launch(profile);
  try {
    const migrated = await page.evaluate(damage => upgradeRecovery.legacyScoped(damage),
      process.env.GONVEX_BROWSER_UPGRADE_DAMAGE === 'alter-intent-key');
    const expected = migrated.original.map(entry => entry.state === 'inflight'
      ? { ...entry, state: 'pending', knownUnsent: false } : entry);
    assert.equal(migrated.version, 2);
    assert.deepEqual(migrated.rows, expected, 'native upgrade changed durable history');
    // Public reads clone optional predictions; absent durable fields remain
    // absent above, while the API exposes an explicit undefined property.
    const publicRows = expected.map(entry => ({ ...entry, patches: undefined }));
    assert.deepEqual(migrated.alice, publicRows.filter(entry => entry.scope === 'alice'));
    assert.deepEqual(migrated.bob, publicRows.filter(entry => entry.scope === 'bob'));
    await context.close();
    ({ context, page } = await launch(profile));
    const reopened = await page.evaluate(() => upgradeRecovery.inspectLegacyScoped());
    assert.equal(reopened.version, 2);
    assert.deepEqual(reopened.rows, expected, 'browser restart changed durable history');
    assert.deepEqual(reopened.alice, migrated.alice);
    assert.deepEqual(reopened.bob, migrated.bob);
    await context.close();
    ({ context, page } = await launch(join(temporary, 'legacy-scoped-key-control')));
    const damaged = await page.evaluate(() => upgradeRecovery.legacyScoped(true));
    const expectedControl = damaged.original.map(entry => entry.state === 'inflight'
      ? { ...entry, state: 'pending', knownUnsent: false } : entry);
    assert.throws(() => assert.deepEqual(damaged.rows, expectedControl,
      'native upgrade changed durable history'), /native upgrade changed durable history/);
    receipt.cases.push({ name: 'scoped-v1-upgrade-restart', complete: true, changedAcceptedKeyRejected: true });
  } finally { await context.close(); }
});
test('corrupt native IndexedDB KV bytes prevent recovery and new admission', async () => {
  const { context, page } = await launch(join(temporary, 'corrupt'));
  try {
    const result = await page.evaluate(() => upgradeRecovery.corruptJournal());
    assert.equal(result.errors.length, 2); for (const error of result.errors) assert.match(error, /outbox|journal|recovery/i);
    assert.equal(result.raw, '{invalid accepted bytes'); assert.equal(result.count, 1);
    receipt.cases.push({ name: 'corrupt-journal-preserved', complete: true });
  } finally { await context.close(); }
});
