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
const args = process.argv.slice(2);
assert.equal(args.length, 4); assert.equal(args[0], '--url'); assert.equal(args[2], '--output');
const url = args[1], output = resolve(args[3]);
await mkdir(output, { recursive: false });
const hash = value => createHash('sha256').update(value).digest('hex');
const save = (name, value) => writeFile(join(output, name), JSON.stringify(value, null, 2) + '\n');
const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
assert(bundler);
const { build } = await import(pathToFileURL(join(root, bundler)).href);
const scenario = process.env.GONVEX_SDK_MODULE_UPGRADE_SCENARIO ?? 'lost-reply';
assert(['lost-reply', 'counter-failure'].includes(scenario));
const damage = process.env.GONVEX_SDK_MODULE_UPGRADE_DAMAGE;
assert(!damage || ['omit-original-receipt', 'omit-replica-barrier'].includes(damage));
assert(damage !== 'omit-replica-barrier' || scenario === 'counter-failure');
const mutation = { active: Boolean(damage), mutations: 0 };
const bundle = await build({ entryPoints: [join(root, 'verification/backend-receipts/browser_module_upgrade_entry.mjs')],
  bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true,
  plugins: damage ? [{ name: 'forget-original-receipt', setup(builder) {
    if (damage === 'omit-original-receipt')
      builder.onResolve({ filter: /client-upgrades\.js$/ }, () => ({ path: join(root, 'packages/client/src/client-upgrades.ts') }));
    else builder.onResolve({ filter: /packages\/client\/dist\/index\.js$/ }, () => ({ path: join(root, 'packages/client/src/index.ts') }));
    builder.onLoad({ filter: damage === 'omit-original-receipt' ? /packages\/client\/src\/client-upgrades\.ts$/ : /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
      const source = await readFile(path, 'utf8');
      const ts = createRequire(join(root, 'packages/gonvex/package.json'))('typescript-api');
      assert.equal(ts.version, '6.0.3');
      const file = ts.createSourceFile(path, source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
      assert.equal(file.parseDiagnostics.length, 0);
      let fn, guard, replacement;
      if (damage === 'omit-original-receipt') {
        const functions = file.statements.filter(node => ts.isFunctionDeclaration(node) && node.name?.text === 'migrateClientData');
        assert.equal(functions.length, 1); fn = functions[0];
        guard = 'receiptPath: originalPath'; replacement = 'receiptPath: undefined';
        mutation.function = 'migrateClientData';
      } else {
        const classes = file.statements.filter(node => ts.isClassDeclaration(node) && node.name?.text === 'GonvexClient');
        assert.equal(classes.length, 1);
        const methods = classes[0].members.filter(node => ts.isMethodDeclaration(node) && node.name.getText(file) === 'registerCall');
        assert.equal(methods.length, 1); fn = methods[0];
        const regions = [];
        const visit = node => {
          if (ts.isIfStatement(node) && node.expression.getText(file).replace(/\s+/g, '') === 'kind==="reducer"&&message.type==="reducer.result"') regions.push(node);
          ts.forEachChild(node, visit);
        };
        visit(fn); assert.equal(regions.length, 1); fn = regions[0];
        guard = '&& committedRevision > this.processedReplicaWatermarkRevision'; replacement = '&& false';
        mutation.method = 'registerCall'; mutation.region = 'reducer.result';
      }
      const original = fn.getText(file);
      assert.equal(original.split(guard).length, 2);
      const contents = source.slice(0, fn.getStart(file)) + original.replace(guard, replacement) + source.slice(fn.end);
      Object.assign(mutation, { mutations: 1, parser: ts.version,
        originalSha256: hash(source), mutatedSha256: hash(contents) });
      return { contents, loader: 'ts' };
    });
  } }] : [] });
assert.equal(mutation.mutations, damage ? 1 : 0);
const inputs = {};
for (const path of Object.keys(bundle.metafile.inputs)) {
  const absolute = resolve(path);
  if (absolute.startsWith(join(root, 'packages') + '/') && !absolute.includes('/node_modules/'))
    inputs[absolute.slice(root.length + 1)] = hash(await readFile(absolute));
}
const provenance = { scenario, mutation, bundledInputs: inputs, bundleSha256: hash(bundle.outputFiles[0].contents) };
const server = createServer((request, response) => {
  response.setHeader('Cache-Control', 'no-store');
  response.setHeader('Content-Type', request.url === '/fixture.js' ? 'text/javascript' : 'text/html');
  response.end(request.url === '/fixture.js' ? bundle.outputFiles[0].contents : '<!doctype html><title>Backend module upgrade recovery</title>');
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
let context, page, lastState;
const errors = [];
async function launch() {
  context = await chromium.launchPersistentContext(join(output, 'profile'), { headless: true, args: ['--no-sandbox'] });
  page = context.pages()[0]; page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin); await page.addScriptTag({ url: origin + '/fixture.js' });
}
async function until(check, label) {
  const deadline = Date.now() + 20_000;
  while (!await check()) { assert(Date.now() < deadline, 'module upgrade did not reach ' + label); await delay(10); }
}
const ordinary = state => state.calls.filter(call => call.owner === 'main').flatMap(({ message }) =>
  message.type === 'reducer.callMany' ? message.calls : message.type === 'reducer.call' ? [message] : []);
function assertCounterBarrier(state, original) {
  const acknowledged = state.acknowledgements.some(ack => ack.key === original.idempotencyKey);
  const retired = state.retirementAttempts.some(attempt => attempt.id === original.id);
  if (acknowledged || retired || !state.entries.some(entry => entry.id === original.id))
    throw Error('accepted browser intent passed durable replica data');
  assert(state.processedWatermark < state.lostResult.committedRevision, 'counter failure advanced its durable watermark');
  assert.equal(state.row, undefined, 'counter failure published unpersisted business data');
}
try {
  await launch();
  await page.evaluate(url => moduleUpgrade.setup(url, 1), url);
  await page.evaluate(() => moduleUpgrade.connect(1));
  await until(async () => { lastState = await page.evaluate(() => moduleUpgrade.state()); return lastState.ready && lastState.confirmed?.value === 11; }, 'original confirmed replica');
  await page.evaluate(url => moduleUpgrade.observeOld(url), url);
  await until(async () => { lastState = await page.evaluate(() => moduleUpgrade.state()); return lastState.events.some(event => event.owner === 'observer' && event.message.type === 'auth.result'); }, 'old-contract observer authentication');
  const staged = await page.evaluate(() => moduleUpgrade.stage());
  assert.equal(staged.first.status, 'queued'); assert.equal(staged.second.status, 'queued');
  assert.equal(staged.entries.length, 2); assert(staged.entries[0].id < staged.entries[1].id);
  let counterFailure;
  if (scenario === 'counter-failure') {
    const fault = await page.evaluate(() => moduleUpgrade.armCounterFault());
    await save('counter-fault-armed.json', fault);
  }
  await page.evaluate(() => moduleUpgrade.deliver());
  if (scenario === 'counter-failure') {
    await until(async () => {
      lastState = await page.evaluate(() => moduleUpgrade.counterState());
      if (!lastState.lostResult) return false;
      assertCounterBarrier(lastState, staged.entries[0]);
      const accepted = lastState.entries.find(entry => entry.id === staged.entries[0].id);
      const heldData = lastState.heldFrames.some(frame => ['replica.transaction', 'replica.delta', 'replica.snapshotPage'].includes(frame.type)
        && frame.cursor?.revision >= lastState.lostResult.committedRevision);
      return accepted?.state === 'committed' && accepted.awaitingReplica === true && heldData;
    }, 'independently durable acceptance with actual replica data withheld');
    const accepted = lastState;
    await save('counter-accepted-held.json', accepted);
    const released = await page.evaluate(() => moduleUpgrade.releaseCounterFrames());
    await until(async () => {
      lastState = await page.evaluate(() => moduleUpgrade.counterState());
      assertCounterBarrier(lastState, staged.entries[0]);
      return lastState.storageFailures.some(failure => failure.method !== 'advanceWatermark' && /Invalid replica storage sequence/.test(failure.error))
        && !lastState.connection.isWebSocketConnected;
    }, 'actual data persistence rejection and SDK transport fence');
    await page.evaluate(() => moduleUpgrade.freezeCounterFailure());
    await until(async () => { lastState = await page.evaluate(() => moduleUpgrade.counterState()); return !lastState.drainingOutbox; }, 'finished accepted-intent failure bookkeeping');
    const failed = lastState;
    assertCounterBarrier(failed, staged.entries[0]);
    assert.deepEqual(failed.rawReplica, accepted.counterFault.corruptedReplica, 'failed replica write changed durable scope data');
    assert.equal(failed.entries[0].state, 'committed'); assert.equal(failed.entries[0].awaitingReplica, true);
    assert.equal(ordinary(failed).length, 1, 'counter failure resent or overtook the accepted intent');
    await save('counter-persistence-failed.json', failed);
    const repaired = await page.evaluate(() => moduleUpgrade.repairCounter());
    assert.deepEqual(repaired, accepted.counterFault.beforeReplica, 'repair changed data beyond the injected counter fault');
    counterFailure = { accepted, failed, released, repaired, independentJournalReader: true,
      actualAcceptanceDelivered: true, replicaDataAcknowledgedBeforePersistence: false };
  }
  await until(async () => { lastState = await page.evaluate(() => moduleUpgrade.state()); return lastState.lostResult && !lastState.connection.isWebSocketConnected && !lastState.drainingOutbox; }, 'finished interrupted intent bookkeeping');
  const before = lastState;
  assert.equal(before.entries.length, 2); assert.equal(ordinary(before).length, 1, 'dependent edit passed the first unresolved intent');
  assert(before.lostResult.committedRevision > 0);
  assert.equal(before.entries[0].knownUnsent, false);
  assert.equal(before.entries[1].state, 'pending');
  if (scenario === 'lost-reply') assert.notEqual(before.entries[0].state, 'committed', 'lost reply reached local acceptance');
  else { assert.equal(before.entries[0].state, 'committed'); assert.equal(before.entries[0].awaitingReplica, true); }
  await save('before.json', before);
  await writeFile(join(output, 'replace-request.tmp'), JSON.stringify({ keys: before.entries.map(entry => entry.idempotencyKey) }) + '\n');
  await rename(join(output, 'replace-request.tmp'), join(output, 'replace-request.json'));
  let replacement;
  await until(async () => {
    try { replacement = JSON.parse(await readFile(join(output, 'replaced.json'), 'utf8')); return true; }
    catch (error) { if (error.code === 'ENOENT' || error instanceof SyntaxError) return false; throw error; }
  }, 'actual runtime module and SQL replacement');
  await until(async () => { lastState = await page.evaluate(() => moduleUpgrade.state()); return lastState.observerUpdate && lastState.events.some(event => event.owner === 'observer' && event.message.type === 'client.updateRequired' && event.message.contract === 2); }, 'old client contract gate');
  assert.deepEqual(lastState.entries, before.entries, 'backend replacement changed the retained local journal');
  await save('old-client-fenced.json', lastState);
  const browser = context.browser(), session = await browser.newBrowserCDPSession();
  const { processInfo } = await session.send('SystemInfo.getProcessInfo');
  const owned = processInfo.find(info => info.type === 'browser');
  assert(Number.isInteger(owned?.id) && owned.id > 1);
  const command = await readFile(`/proc/${owned.id}/cmdline`, 'utf8');
  assert(command.includes(join(output, 'profile')) && command.includes('--user-data-dir='), 'refuse to kill another browser');
  const disconnected = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('owned browser did not crash')), 10_000);
    browser.once('disconnected', () => { clearTimeout(timer); resolve(); });
  });
  process.kill(owned.id, 'SIGKILL'); await disconnected; assert.equal(browser.isConnected(), false); context = undefined;
  await launch();
  const upgraded = await page.evaluate(url => moduleUpgrade.setup(url, 2), url);
  await save('upgraded.json', upgraded);
  assert.equal(upgraded.entries.length, 2);
  if (scenario === 'counter-failure') { assert.equal(upgraded.entries[0].state, 'committed'); assert.equal(upgraded.entries[0].awaitingReplica, true); }
  for (let index = 0; index < 2; index++) {
    for (const field of ['id', 'scope', 'idempotencyKey', 'createdAt', 'attempts'])
      assert.deepEqual(upgraded.entries[index][field], before.entries[index][field], 'upgrade changed intent ' + field);
    assert.equal(upgraded.entries[index].path, index === 0 ? 'writeV2' : 'editV2');
    assert.deepEqual(upgraded.entries[index].args, { id: 'upgrade-intent', count: index === 0 ? 7 : 8 });
  }
  assert(Object.values(upgraded.snapshots).some(snapshot => snapshot?.entities.receipt_records?.['confirmed-before-upgrade']?.count === 11));
  for (const snapshot of Object.values(upgraded.snapshots)) if (snapshot) { assert.equal(snapshot.cursor, undefined); assert.deepEqual(snapshot.liveQueries, {}); }
  await page.evaluate(() => moduleUpgrade.connect(2));
  await until(async () => {
    lastState = await page.evaluate(() => moduleUpgrade.state());
    const lookups = lastState.calls.filter(call => call.owner === 'main' && call.message.type === 'query.call' && call.message.path === '__gonvex.reducerReceipt');
    const rejected = lastState.events.find(event => event.owner === 'main' && event.message.type === 'query.error' && lookups.some(call => call.message.id === event.message.id));
    if (rejected) {
      assert.match(rejected.message.error, /idempotency key .*already used by reducer.*refusing to replay/i);
      throw Error('backend module upgrade lost original receipt identity');
    }
    return lastState.ready && lastState.entries.length === 0 && lastState.row?.count === 8;
  }, 'receipt recovery, ordered edit and complete new replica');
  const after = lastState, sent = ordinary(after);
  assert.equal(sent.length, 1, 'upgraded SDK resent the committed write');
  assert.equal(sent[0].path, 'editV2'); assert.equal(sent[0].receiptPath, 'edit');
  assert.equal(sent[0].idempotencyKey, before.entries[1].idempotencyKey);
  const lookups = after.calls.filter(call => call.owner === 'main' && call.message.type === 'query.call' && call.message.path === '__gonvex.reducerReceipt').map(call => call.message);
  assert(lookups.some(call => call.args.idempotencyKey === before.entries[0].idempotencyKey && call.args.receiptPath === 'write'));
  const result = after.events.find(event => event.owner === 'main' && event.message.type === 'reducer.result' && event.message.path === 'editV2').message;
  assert(result.committedRevision > before.lostResult.committedRevision);
  const saved = Object.values(after.snapshots).find(snapshot => snapshot?.cursor?.revision >= result.committedRevision && snapshot.entities.receipt_records?.['upgrade-intent']?.count === 8);
  assert(saved, 'intent retirement passed durable upgraded data');
  assert.equal(saved.entities.receipt_records['confirmed-before-upgrade'].count, 11);
  assert.equal(saved.entities.receipt_records['upgrade-intent'].value, undefined);
  assert(Object.values(saved.liveQueries).some(window => window.ids.includes('upgrade-intent') && window.ids.includes('confirmed-before-upgrade') && window.completeness === 'complete'), 'upgraded replica has no complete durable membership');
  await page.evaluate(() => moduleUpgrade.close());
  await context.close(); context = undefined;
  await launch();
  const downgrade = await page.evaluate(async url => {
    try { await moduleUpgrade.setup(url, 1); return { allowed: true }; }
    catch (error) { return { error: error.message }; }
  }, url);
  assert.match(downgrade.error ?? '', /Stored data requires a newer application build/);
  await save('receipt.json', { ...provenance, counterFailure, browser: context.browser().version(), killSignal: 'SIGKILL', staged, before, upgraded, replacement, after,
    committedIntentOrdinaryRedeliveries: 0, orderedNewIntentDeliveries: 1, downgradeRefused: true,
    boundary: 'Real authenticated SDK and Chromium IndexedDB, production apply_dev_sync SQL migration and V8 module replacement, client contract 1->2, renamed handlers and value->count data/argument transform. Does not establish arbitrary migrations, native Expo/Hermes or machine power loss.' });
  console.log('Backend module/SQL upgrade preserved receipt identity, causal order and complete durable replica after browser SIGKILL');
} catch (error) {
  await save('failure.json', { ...provenance, error: error.message, lastState, errors }); throw error;
} finally { if (context) await context.close(); await new Promise(resolve => server.close(resolve)); }
