import assert from 'node:assert/strict';
import { before, after, test } from 'node:test';
import { createServer } from 'node:http';
import { globSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { chromium } from '@playwright/test';

const directory = dirname(fileURLToPath(import.meta.url)), root = resolve(directory, '../../..');
let temporary, server, origin;
const receipt = { complete: false, scope: 'Real Chromium IndexedDB, controlled protocol transport; no server or power-loss proof', cases: [] };
before(async () => {
  const parent = process.env.GONVEX_BROWSER_TEST_DIR ?? tmpdir();
  await mkdir(parent, { recursive: true });
  temporary = await mkdtemp(join(parent, 'gonvex-idb-'));
  receipt.sourceHead = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim();
  receipt.nodeVersion = process.version;
  receipt.sourceSha256 = {};
  for (const file of ['index.ts', 'indexeddb-replica.ts', 'indexeddb-engine.ts', 'outbox.ts', 'outbox-policy.ts']) {
    receipt.sourceSha256[file] = createHash('sha256').update(await readFile(join(directory, '../src', file))).digest('hex');
  }
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert.ok(bundler, 'frozen workspace esbuild is required');
  const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const bundles = {};
  for (const mutant of [false, true]) {
    let mutations = 0;
    const result = await build({ entryPoints: [join(directory, 'browser-storage-entry.mjs')],
      bundle: true, platform: 'browser', format: 'iife', write: false,
      plugins: mutant ? [{ name: 'remove-snapshot-barrier', setup(build) {
        build.onLoad({ filter: /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
          const source = await readFile(path, 'utf8');
          const guard = 'if (this.snapshotPageSubscriptions.size === 0) return false;';
          assert.equal(source.split(guard).length, 2, 'negative-control source guard changed');
          mutations++;
          return { contents: source.replace(guard, 'return false;'), loader: 'ts' };
        });
      } }] : [] });
    assert.equal(mutations, mutant ? 1 : 0);
    const code = result.outputFiles[0].contents;
    bundles[mutant ? '/mutant.js' : '/candidate.js'] = code;
    receipt[mutant ? 'mutantBundleSha256' : 'candidateBundleSha256'] = createHash('sha256').update(code).digest('hex');
  }
  server = createServer((request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    if (bundles[request.url]) {
      response.setHeader('Content-Type', 'text/javascript'); response.end(bundles[request.url]);
    } else {
      response.setHeader('Content-Type', 'text/html');
      response.end('<!doctype html><title>IndexedDB contract fixture</title>');
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
});
after(async () => {
  receipt.complete = receipt.cases.length === 5 && receipt.cases.filter(c => c.complete).length === 3
    && receipt.cases.some(c => c.contract === 'accepted-outbox-survives-crash-and-late-settlement' && c.complete)
    && receipt.cases.filter(c => c.observedViolation === 'checkpoint-passes-missing-row').length === 2;
  if (process.env.GONVEX_BROWSER_TEST_OUTPUT) await writeFile(process.env.GONVEX_BROWSER_TEST_OUTPUT, JSON.stringify(receipt, null, 2) + '\n');
  if (server) await new Promise(resolve => server.close(resolve));
  if (temporary) await rm(temporary, { recursive: true, force: true });
  assert.equal(receipt.complete, true, 'all declared browser contracts and negative controls must complete');
});

async function launch(profile, mutant = false) {
  const context = await chromium.launchPersistentContext(profile, { headless: true, args: ['--no-sandbox'], serviceWorkers: 'block' });
  const page = context.pages()[0];
  await page.goto(origin);
  await page.addScriptTag({ url: origin + (mutant ? '/mutant.js' : '/candidate.js') });
  receipt.browserVersion ??= context.browser().version();
  return { context, page };
}
async function crash(context) {
  const browser = context.browser(), session = await browser.newBrowserCDPSession();
  const { processInfo } = await session.send('SystemInfo.getProcessInfo');
  const owned = processInfo.find(info => info.type === 'browser');
  assert.ok(Number.isInteger(owned?.id) && owned.id > 1, 'owned Chromium PID required');
  const command = await readFile(`/proc/${owned.id}/cmdline`, 'utf8');
  assert.ok(command.includes(temporary + '/') && command.includes('--user-data-dir='), 'refuse to kill an unowned browser');
  const disconnected = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(Error('owned browser did not crash')), 10000);
    browser.once('disconnected', () => { clearTimeout(timeout); resolve(); });
  });
  process.kill(owned.id, 'SIGKILL');
  await disconnected;
  assert.equal(browser.isConnected(), false, 'browser crash must be witnessed');
  // A killed persistent context has no normal context-close
  // notification. Its browser process has already exited; do not await close.
}
function base(snapshot) {
  assert.deepEqual(snapshot.cursor, { epoch: 'browser-epoch', revision: 7 });
  assert.deepEqual(snapshot.entities.tasks, { a: { id: 'a', name: 'Original' }, b: { id: 'b', name: 'Keep' } });
}
function committed(snapshot) {
  assert.deepEqual(snapshot.cursor, { epoch: 'browser-epoch', revision: 8 });
  assert.deepEqual(snapshot.entities.tasks, { a: { id: 'a', name: 'Changed' }, c: { id: 'c', name: 'Inserted' } });
}

for (const kind of ['legacy', 'engine']) test(`${kind}: native IndexedDB rollback, crash and SDK barriers`, { timeout: 120000 }, async t => {
  const profile = join(temporary, kind);
  let { context, page } = await launch(profile);
  const evidence = { adapter: kind, checks: [] }; receipt.cases.push(evidence);
  try {
    await page.evaluate(([kind, name]) => browserGuarantees.seed(kind, name), [kind, 'atomic']);
    await t.test('native transaction abort rolls back successful row requests and checkpoint', async () => {
      const result = await page.evaluate(kind => browserGuarantees.abortTransaction(kind, 'atomic'), kind);
      assert.equal(result.witnessed, 1); assert.equal(result.rejected, true); base(result.saved);
      assert.equal(result.other.cursor.revision, 3);
      assert.equal(result.other.entities.tasks.a.name, 'Original');
      evidence.checks.push('witnessed-native-abort');
    });
    await t.test('browser process crash rolls back an uncommitted transaction', async () => {
      let observed;
      const witness = new Promise(resolve => { observed = resolve; });
      await page.exposeFunction('transactionWriteWitness', observed);
      const invocation = page.evaluate(kind => browserGuarantees.crashTransaction(kind, 'atomic'), kind)
        .then(() => ({ completed: true }), error => ({ error: String(error) }));
      await Promise.race([witness, invocation.then(result => { throw Error('transaction ended before witness: ' + JSON.stringify(result)); })]);
      t.diagnostic('native row write witnessed; crashing owned browser');
      await crash(context);
      t.diagnostic('browser disconnected; reopening persistent profile');
      assert.match((await invocation).error, /closed|crash/i);
      ({ context, page } = await launch(profile));
      base(await page.evaluate(kind => browserGuarantees.read(kind, 'atomic'), kind));
      evidence.checks.push('witnessed-mid-transaction-browser-crash');
    });
    await t.test('acknowledged writes and tombstones survive abrupt browser restart', async () => {
      committed(await page.evaluate(kind => browserGuarantees.commitTransaction(kind, 'atomic'), kind));
      await crash(context);
      ({ context, page } = await launch(profile));
      committed(await page.evaluate(kind => browserGuarantees.read(kind, 'atomic'), kind));
      assert.equal((await page.evaluate(kind => browserGuarantees.read(kind, 'atomic', 'account-b'), kind)).cursor.revision, 3);
      evidence.checks.push('committed-state-after-browser-crash', 'stale-frame-and-subject-isolation');
    });
    await t.test('watermark cannot retire an intent before paged snapshot persistence', async () => {
      const result = await page.evaluate(kind => browserGuarantees.snapshotBarrier(kind, 'sdk-success'), kind);
      assert.equal(result.before.outcome, 'pending'); assert.equal(result.before.intents.length, 1);
      assert.equal(result.before.saved.cursor.revision, 7);
      assert.equal(result.before.saved.entities.tasks.c, undefined);
      assert.equal(result.after.outcome, 'success'); assert.equal(result.after.intents.length, 0);
      assert.equal(result.after.saved.cursor.revision, 8);
      assert.equal(result.after.saved.entities.tasks.c.name, 'New snapshot row');
      evidence.checks.push('snapshot-before-watermark-retirement');
    });
    await t.test('failed snapshot promotion preserves the checkpoint and retry identity after crash', async () => {
      const result = await page.evaluate(kind => browserGuarantees.snapshotBarrier(kind, 'sdk-failure', true), kind);
      assert.equal(result.before.outcome, 'pending'); assert.equal(result.after.witnessed, 1);
      assert.notEqual(result.after.outcome, 'success'); assert.equal(result.after.intents.length, 1);
      assert.equal(result.after.saved.cursor.revision, 7); assert.equal(result.after.saved.entities.tasks.c, undefined);
      await crash(context);
      ({ context, page } = await launch(profile));
      const saved = await page.evaluate(kind => browserGuarantees.read(kind, 'sdk-failure', 'visible'), kind);
      assert.equal(saved.cursor.revision, 7); assert.equal(saved.entities.tasks.c, undefined);
      const queue = await page.evaluate(() => browserGuarantees.recoveredQueue('sdk-failure'));
      assert.equal(queue.length, 1); assert.equal(queue[0].state, 'pending');
      assert.equal(queue[0].idempotencyKey, result.before.intents[0].id);
      assert.equal(queue[0].id, result.before.intents[0].entryId);
      assert.deepEqual(queue[0].args, { id: 'c' });
      evidence.checks.push('failed-promotion-keeps-checkpoint', 'durable-intent-identity-after-browser-crash');
    });
  } finally { if (context.browser()?.isConnected()) await context.close(); }
  evidence.complete = evidence.checks.length === 7;
});

test('accepted outbox survives browser crash without late settlement re-arming it', { timeout: 60000 }, async () => {
  const profile = join(temporary, 'accepted-outbox');
  let { context, page } = await launch(profile);
  try {
    const accepted = await page.evaluate(() => browserGuarantees.acceptedQueue('accepted-outbox', true));
    assert.equal(accepted.length, 1); assert.equal(accepted[0].state, 'committed');
    await crash(context);
    ({ context, page } = await launch(profile));
    const result = await page.evaluate(() => browserGuarantees.acceptedQueue('accepted-outbox'));
    assert.deepEqual(result.before, accepted); assert.deepEqual(result.after, accepted);
    assert.equal(result.retried, null); assert.equal(result.ready, null);
    receipt.cases.push({ contract: 'accepted-outbox-survives-crash-and-late-settlement', complete: true });
  } finally { if (context.browser()?.isConnected()) await context.close(); }
});

test('negative control: omitting snapshot deferral certifies a missing row', { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, 'mutant'), true);
  try {
    for (const kind of ['legacy', 'engine']) {
      const result = await page.evaluate(kind => browserGuarantees.snapshotBarrier(kind, 'mutant-' + kind), kind);
      assert.equal(result.before.saved.cursor.revision, 8, 'mutant must reach the bad certificate');
      assert.equal(result.before.saved.entities.tasks.c, undefined, 'mutant must certify an unpersisted snapshot row');
      receipt.cases.push({ adapter: kind, negativeControl: 'snapshot-deferral-omitted', observedViolation: 'checkpoint-passes-missing-row' });
    }
  } finally { if (context.browser()?.isConnected()) await context.close(); }
});
