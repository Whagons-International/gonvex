import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { globSync } from 'node:fs';
import { mkdir, readFile, writeFile, rename } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from '@playwright/test';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const digest = data => createHash('sha256').update(data).digest('hex');
async function until(check, label) {
  const deadline = Date.now() + 20_000;
  while (!await check()) { assert(Date.now() < deadline, 'SDK upgrade fixture did not reach ' + label); await delay(10); }
}

export async function runSDKBrowserUpgrade(url, output) {
  await mkdir(output, { recursive: false });
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert(bundler, 'frozen esbuild required');
  const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const damage = process.env.GONVEX_SDK_UPGRADE_DAMAGE;
  assert(!damage || damage === 'omit-receipt-path', 'unknown SDK upgrade source control');
  const sourceMutation = { active: Boolean(damage), mutations: 0 };
  const bundle = await build({ entryPoints: [join(root, 'verification/backend-receipts/browser_rollback_upgrade_entry.mjs')],
    bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true,
    plugins: damage ? [{ name: 'omit-upgraded-receipt-namespace', setup(builder) {
      builder.onResolve({ filter: /packages\/client\/dist\/index\.js$/ }, () => ({ path: join(root, 'packages/client/src/index.ts') }));
      builder.onLoad({ filter: /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
        const source = await readFile(path, 'utf8');
        const ts = createRequire(join(root, 'packages/gonvex/package.json'))('typescript-api');
        assert.equal(ts.version, '6.0.3');
        const file = ts.createSourceFile('index.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
        assert.equal(file.parseDiagnostics.length, 0);
        const cls = file.statements.filter(node => ts.isClassDeclaration(node) && node.name?.text === 'GonvexClient');
        assert.equal(cls.length, 1);
        const methods = cls[0].members.filter(node => ts.isMethodDeclaration(node) && node.name.getText(file) === 'deliverOutboxBatch');
        assert.equal(methods.length, 1);
        const method = methods[0], original = method.getText(file), field = 'receiptPath: entry.receiptPath';
        assert.equal(original.split(field).length, 3, 'direct and batched receipt namespace fields changed');
        const changed = original.replaceAll(field, 'receiptPath: undefined');
        const contents = source.slice(0, method.getStart(file)) + changed + source.slice(method.end);
        sourceMutation.mutations += 2;
        sourceMutation.originalIndexSha256 = digest(source);
        sourceMutation.mutatedIndexSha256 = digest(contents);
        sourceMutation.method = 'deliverOutboxBatch';
        sourceMutation.parser = ts.version;
        return { contents, loader: 'ts' };
      });
    } }] : [] });
  assert.equal(sourceMutation.mutations, damage ? 2 : 0);
  const bundledInputs = {};
  for (const path of Object.keys(bundle.metafile.inputs)) {
    const absolute = resolve(path);
    if (absolute.startsWith(join(root, 'packages') + '/') && !absolute.includes('/node_modules/'))
      bundledInputs[absolute.slice(root.length + 1)] = digest(await readFile(absolute));
  }
  const bundleSha256 = digest(bundle.outputFiles[0].contents);
  const server = createServer((request, response) => {
    response.setHeader('Cache-Control', 'no-store');
    response.setHeader('Content-Type', request.url === '/fixture.js' ? 'text/javascript' : 'text/html');
    response.end(request.url === '/fixture.js' ? bundle.outputFiles[0].contents : '<!doctype html><title>SDK rollback upgrade fixture</title>');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = 'http://127.0.0.1:' + server.address().port;
  let context, page, lastCompletedState;
  const browserErrors = [];
  const launch = async () => {
    context = await chromium.launchPersistentContext(join(output, 'profile'), { headless: true, args: ['--no-sandbox'] });
    const opened = context.pages()[0];
    opened.on('pageerror', error => browserErrors.push(error.message));
    opened.on('console', message => { if (message.type() === 'error') browserErrors.push(message.text()); });
    await opened.goto(origin); await opened.addScriptTag({ url: origin + '/fixture.js' }); return opened;
  };
  try {
    page = await launch();
    await page.evaluate(url => rollbackUpgrade.setup(url, 1), url);
    const queued = await page.evaluate(() => rollbackUpgrade.enqueueAbort());
    assert.equal(queued.status, 'queued');
    await page.evaluate(() => rollbackUpgrade.connect());
    await until(async () => {
      const state = await page.evaluate(() => rollbackUpgrade.state());
      lastCompletedState = state;
      return state.ready && state.results.some(result => result.type === 'reducer.error') && state.entries[0]?.lastError;
    }, 'durable failed intent');
    const before = await page.evaluate(() => rollbackUpgrade.stop());
    lastCompletedState = before;
    assert.equal(before.entries.length, 1, 'failed reducer lost its durable intent');
    const original = before.entries[0];
    assert.equal(original.idempotencyKey, queued.reducerId);
    const deliveries = calls => calls.flatMap(call => call.type === 'reducer.callMany' ? call.calls : call.type === 'reducer.call' ? [call] : []);
    assert.equal(deliveries(before.calls).length, 1);
    assert(!before.results.some(result => result.type === 'reducer.result'), 'rollback reported acceptance');
    await context.close(); context = undefined;
    // The Rust fixture observes the actual PostgreSQL row and receipt before
    // allowing the supported browser contract migration to begin.
    await writeFile(join(output, 'rollback-request.tmp'), JSON.stringify({ key: original.idempotencyKey, rowId: original.args.id }) + '\n');
    await rename(join(output, 'rollback-request.tmp'), join(output, 'rollback-request.json'));
    let backend;
    await until(async () => {
      try { backend = JSON.parse(await readFile(join(output, 'rollback-observed.json'), 'utf8')); return true; }
      catch (error) { if (error.code === 'ENOENT' || error instanceof SyntaxError) return false; throw error; }
    }, 'real backend rollback witness');
    assert.equal(backend.business, 0, 'rollback left committed business effect');
    assert.equal(backend.receipts, 0, 'rollback left committed receipt');
    page = await launch();
    const upgraded = await page.evaluate(url => rollbackUpgrade.setup(url, 2), url);
    lastCompletedState = upgraded;
    await writeFile(join(output, 'before.json'), JSON.stringify(before, null, 2) + '\n');
    await writeFile(join(output, 'upgraded.json'), JSON.stringify(upgraded, null, 2) + '\n');
    assert.equal(upgraded.entries.length, 1);
    const entry = upgraded.entries[0];
    for (const field of ['id', 'scope', 'idempotencyKey', 'createdAt', 'attempts']) assert.deepEqual(entry[field], original[field], 'migration changed ' + field);
    assert.deepEqual(entry.args, original.args);
    assert.equal(entry.path, 'write'); assert.equal(entry.receiptPath, 'abort');
    assert.equal(entry.state, original.state === 'inflight' ? 'pending' : original.state);
    let confirmedSnapshots = 0;
    for (const [scope, snapshot] of Object.entries(before.snapshots)) {
      // Session-only owner scopes have metadata but no replica snapshot.
      if (snapshot === undefined) { assert.equal(upgraded.snapshots[scope], undefined); continue; }
      confirmedSnapshots++;
      assert.deepEqual(upgraded.snapshots[scope].entities, snapshot.entities, 'upgrade changed confirmed rows');
      assert.equal(upgraded.snapshots[scope].cursor, undefined, 'upgrade retained an uncertified cursor');
      assert.deepEqual(upgraded.snapshots[scope].liveQueries, {}, 'upgrade retained old membership contracts');
    }
    assert(confirmedSnapshots > 0, 'upgrade fixture did not preserve an actual confirmed snapshot');
    if (['failed', 'rejected'].includes(entry.state)) assert.equal(await page.evaluate(key => rollbackUpgrade.retry(key), entry.idempotencyKey), true);
    await page.evaluate(() => rollbackUpgrade.loseRenamedReply());
    await page.evaluate(() => rollbackUpgrade.connect());
    let interrupted;
    await until(async () => {
      interrupted = await page.evaluate(() => rollbackUpgrade.state());
      lastCompletedState = interrupted;
      return interrupted.droppedResult && !interrupted.connection.isWebSocketConnected && interrupted.entries.length === 1;
    }, 'lost committed reply after handler rename');
    assert(interrupted.droppedResult.committedRevision > 0, 'lost reply did not follow backend commit');
    const retained = interrupted.entries[0];
    for (const field of ['id', 'scope', 'idempotencyKey', 'createdAt', 'receiptPath', 'path', 'args'])
      assert.deepEqual(retained[field], entry[field], 'post-upgrade interruption changed ' + field);
    assert.equal(retained.knownUnsent, false, 'committed renamed call lost its previously-sent marker');
    assert.notEqual(retained.state, 'committed', 'dropped result reached SDK acceptance dispatch');
    await writeFile(join(output, 'postcommit-interrupted.json'), JSON.stringify(interrupted, null, 2) + '\n');
    // Kill only this persistent browser, after its journal transaction has
    // completed. The original runtime and PostgreSQL remain alive.
    const browser = context.browser(), session = await browser.newBrowserCDPSession();
    const { processInfo } = await session.send('SystemInfo.getProcessInfo');
    const owned = processInfo.find(info => info.type === 'browser');
    assert(Number.isInteger(owned?.id) && owned.id > 1);
    const command = await readFile(`/proc/${owned.id}/cmdline`, 'utf8');
    assert(command.includes(join(output, 'profile')) && command.includes('--user-data-dir='), 'refuse to crash an unowned upgrade browser');
    const disconnected = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(Error('upgrade browser did not crash')), 10_000);
      browser.once('disconnected', () => { clearTimeout(timeout); resolve(); });
    });
    process.kill(owned.id, 'SIGKILL');
    await disconnected;
    assert.equal(browser.isConnected(), false);
    context = undefined;
    page = await launch();
    const reopened = await page.evaluate(url => rollbackUpgrade.setup(url, 2), url);
    assert.equal(reopened.entries.length, 1, 'browser restart lost the original upgraded intent');
    for (const field of ['id', 'scope', 'idempotencyKey', 'createdAt', 'receiptPath', 'path', 'args'])
      assert.deepEqual(reopened.entries[0][field], entry[field], 'browser restart changed ' + field);
    await page.evaluate(() => rollbackUpgrade.connect());
    await until(async () => {
      const state = await page.evaluate(() => rollbackUpgrade.state());
      lastCompletedState = state;
      const lookups = state.calls.filter(call => call.type === 'query.call' && call.path === '__gonvex.reducerReceipt'
        && call.args.idempotencyKey === original.idempotencyKey);
      const rejected = state.events.find(event => event.type === 'query.error' && lookups.some(call => call.id === event.id));
      if (rejected) {
        assert.match(rejected.error, /idempotency key .*already used by reducer.*refusing to replay/i, 'unexpected receipt recovery query error');
        throw new Error('renamed receipt namespace was lost before restart recovery');
      }
      return state.ready && state.entries.length === 0 && state.row?.value === 7;
    }, 'renamed intent commit and replica recovery');
    const after = await page.evaluate(() => rollbackUpgrade.stop());
    const delivery = [...deliveries(interrupted.calls), ...deliveries(after.calls)];
    assert.equal(delivery.length, 1);
    assert.equal(deliveries(after.calls).length, 0, 'browser recovery resent a committed renamed intent');
    assert.equal(delivery[0].path, 'write'); assert.equal(delivery[0].receiptPath, 'abort');
    assert.equal(delivery[0].idempotencyKey, original.idempotencyKey);
    const accepted = interrupted.droppedResult;
    assert(accepted?.committedRevision > 0, 'renamed reducer did not produce a commit barrier');
    const saved = Object.values(after.snapshots).find(snapshot => snapshot?.entities.receipt_records?.['sdk-upgrade-rollback']);
    assert.deepEqual(saved?.entities.receipt_records['sdk-upgrade-rollback'], { id: 'sdk-upgrade-rollback', value: 7 });
    assert(saved.cursor?.revision >= accepted.committedRevision, 'retired upgraded intent has no durable commit cursor');
    assert(Object.values(saved.liveQueries).some(window => window.ids.includes('sdk-upgrade-rollback')), 'retired upgraded intent has no durable membership');
    const lookups = after.calls.filter(call => call.type === 'query.call' && call.path === '__gonvex.reducerReceipt');
    assert(lookups.length > 0 && lookups.every(call => call.args.receiptPath === 'abort' && call.args.idempotencyKey === original.idempotencyKey),
      'fresh browser did not use the original receipt namespace');
    // Reporting must remain usable after storage closes, including when a
    // failing storage lane cannot complete another IndexedDB transaction.
    const diagnostics = await page.evaluate(() => rollbackUpgrade.diagnostics());
    assert.equal(diagnostics.lastCompletedStorageRead.entries.length, 0);
    assert(Array.isArray(diagnostics.locks.held) && Array.isArray(diagnostics.locks.pending));
    await context.close(); context = undefined;
    page = await launch();
    const downgrade = await page.evaluate(async url => {
      try { await rollbackUpgrade.setup(url, 1); return { allowed: true }; }
      catch (error) { return { error: error.message, calls: rollbackUpgrade.transportLog() }; }
    }, url);
    assert.match(downgrade.error ?? '', /Stored data requires a newer application build/);
    assert.deepEqual(downgrade.calls, [], 'old contract connected before upgrade refusal');
    const result = { browser: context.browser().version(), backend, before, upgraded, after, downgradeRefused: true,
      lostReplyRecovery: { killSignal: 'SIGKILL', interrupted, reopened, recoveredOrdinaryCalls: deliveries(after.calls).length, lookups },
      bundleSha256, bundledInputs, sourceMutation,
      diagnosticsAfterStorageClose: true,
      boundary: 'Real authenticated SDK/browser IndexedDB rollback, production browserUpgradeStorage 1->2 rename, lost committed reply and owned browser SIGKILL. Fresh browser uses the retained receipt namespace without resending. Does not establish native SQLite application-contract migration, backend module-contract replacement, every historical version or machine power-loss durability.' };
    await writeFile(join(output, 'receipt.json'), JSON.stringify(result, null, 2) + '\n');
    return result;
  } catch (error) {
    let state;
    try {
      state = page.isClosed() ? { pageClosed: true, lastCompletedState }
        : await page.evaluate(() => rollbackUpgrade.diagnostics());
    } catch (diagnosticError) { state = { diagnosticError: diagnosticError.message, lastCompletedState }; }
    await writeFile(join(output, 'failure.json'), JSON.stringify({ error: error.message, browserErrors, state, sourceMutation, bundleSha256, bundledInputs }, null, 2) + '\n');
    throw error;
  } finally { if (context) await context.close(); await new Promise(resolve => server.close(resolve)); }
}
