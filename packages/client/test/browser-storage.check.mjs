import assert from 'node:assert/strict';
import { before, after, test } from 'node:test';
import { createServer } from 'node:http';
import { globSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
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
  for (const file of ['index.ts', 'indexeddb-replica.ts', 'indexeddb-engine.ts', 'indexeddb-version.ts', 'indexeddb-sequence.ts', 'outbox.ts', 'outbox-policy.ts', 'outbox-recovery.ts', 'coalescing-policy.ts', 'kv-stores.ts', 'kv-sequence.ts']) {
    receipt.sourceSha256[file] = createHash('sha256').update(await readFile(join(directory, '../src', file))).digest('hex');
  }
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert.ok(bundler, 'frozen workspace esbuild is required');
  const { build } = await import(pathToFileURL(resolve(root, bundler)).href);
  const bundles = {};
  for (const mutant of [false, 'snapshot', 'both', 'legacy-ambiguity', 'engine-sequence', 'startup-validation', 'activation-owner', 'activation-retention', 'activation-domain', 'activation-reentrancy', 'activation-cache', 'future-version', 'counter-integrity']) {
    let mutations = 0;
    const result = await build({ entryPoints: [join(directory, 'browser-storage-entry.mjs')],
      bundle: true, platform: 'browser', format: 'iife', write: false, metafile: true,
      plugins: mutant ? [{ name: 'remove-snapshot-barrier', setup(build) {
        build.onLoad({ filter: mutant === 'future-version' ? /packages\/client\/src\/indexeddb-version\.ts$/
          : mutant === 'counter-integrity' ? /packages\/client\/src\/indexeddb-sequence\.ts$/
          : mutant === 'startup-validation' ? /packages\/client\/src\/(?:local-replica|indexeddb-sequence)\.ts$/
          : mutant === 'legacy-ambiguity' ? /packages\/client\/src\/indexeddb-replica\.ts$/
          : mutant === 'engine-sequence' ? /packages\/client\/src\/(?:indexeddb-engine|indexeddb-sequence)\.ts$/
          : ['activation-owner', 'activation-retention', 'activation-domain', 'activation-reentrancy', 'activation-cache'].includes(mutant) ? /packages\/client\/src\/local-replica\.ts$/
          : /packages\/client\/src\/index\.ts$/ }, async ({ path }) => {
          const source = await readFile(path, 'utf8');
          if (path.endsWith('/indexeddb-sequence.ts')) {
            assert(['counter-integrity', 'startup-validation', 'engine-sequence'].includes(mutant));
            const start = source.indexOf('export async function assertReplicaSequence(');
            const end = source.indexOf('\nexport async function readReplicaSequence(', start);
            assert(start >= 0 && end > start);
            const original = source.slice(start, end), body = '): Promise<void> {';
            assert.equal(original.split(body).length, 2);
            const replacement = original.slice(0, original.indexOf(body) + body.length) + '\n  return;\n}\n';
            mutations++;
            return { contents: source.slice(0, start) + replacement + source.slice(end), loader: 'ts' };
          }
          if (mutant === 'future-version') {
            const guard = 'version ?? schemaVersion * 10';
            assert.equal(source.split(guard).length, 2, 'versioned native-open fallback changed');
            mutations++;
            return { contents: source.replace(guard, 'version'), loader: 'ts' };
          }
          if (mutant === 'activation-cache') {
            const capture = 'const generation = ++this.scopeActivationGeneration;';
            const clear = capture + '\n    this.scopeActivation = undefined;\n    this.scopeActivationName = undefined;';
            assert.equal(source.split(clear).length, 2, 'early activation cache retirement changed');
            mutations++;
            return { contents: source.replace(clear, capture), loader: 'ts' };
          }
          if (['activation-domain', 'activation-reentrancy'].includes(mutant)) {
            const domain = 'if (!isTransactionRevision(generation)) return Promise.reject(new Error("Replica scope activation sequence exhausted."));';
            assert.equal(source.split(domain).length, 2, 'activation numeric domain changed');
            if (mutant === 'activation-domain') { mutations++; return { contents: source.replace(domain, ''), loader: 'ts' }; }
            const capture = 'const generation = ++this.scopeActivationGeneration;';
            const fence = 'if (generation !== this.scopeActivationGeneration) return Promise.resolve();';
            assert.equal(source.split(capture).length, 2); assert.equal(source.split(fence).length, 2);
            const contents = source.replace(capture, '').replace(domain, capture + '\n    ' + domain).replace(fence, '');
            mutations += 3;
            return { contents, loader: 'ts' };
          }
          if (['startup-validation', 'activation-owner', 'activation-retention'].includes(mutant)) {
            const guard = mutant === 'startup-validation' ? 'if (!isReplicaRevision(storageSequence)) throw new Error("Invalid replica storage sequence.");'
              : mutant === 'activation-owner' ? 'if (this.scopeActivation !== activation) return;'
              : 'void activation.then(clearActivation, clearActivation);';
            assert.equal(source.split(guard).length, 2, 'startup/activation source guard changed');
            mutations++;
            return { contents: source.replace(guard, ''), loader: 'ts' };
          }
          if (mutant === 'engine-sequence') {
            const guard = "if (!Number.isSafeInteger(previousSequence) || previousSequence < 0 || previousSequence === Number.MAX_SAFE_INTEGER) {\n      throw new Error('Invalid or exhausted replica storage sequence.');\n    }";
            assert.equal(source.split(guard).length, 2, 'engine sequence guard changed');
            mutations++;
            return { contents: source.replace(guard, ''), loader: 'ts' };
          }
          if (mutant === 'legacy-ambiguity') {
            const guard = 'if (records.length > 1) throw new Error("Ambiguous ownerless replica snapshots require explicit recovery.");';
            assert.equal(source.split(guard).length, 2, 'ownerless snapshot ambiguity guard changed');
            mutations++;
            return { contents: source.replace(guard, ''), loader: 'ts' };
          }
          const guard = 'if (this.snapshotPageSubscriptions.size === 0) return false;';
          assert.equal(source.split(guard).length, 2, 'negative-control source guard changed');
          mutations++;
          let contents = source.replace(guard, 'return false;');
          if (mutant === 'both') {
            const openingGuard = 'if (opening.length > 0) {';
            assert.equal(contents.split(openingGuard).length, 2, 'opening watermark guard changed');
            contents = contents.replace(openingGuard, 'if (false) {');
            mutations++;
          }
          return { contents, loader: 'ts' };
        });
      } }] : [] });
    assert.equal(mutations, ['both', 'startup-validation', 'engine-sequence'].includes(mutant) ? 2 : mutant === 'activation-reentrancy' ? 3 : mutant ? 1 : 0);
    receipt.bundledProjectInputs ??= {};
    for (const input of Object.keys(result.metafile.inputs).sort()) {
      const path = resolve(input), label = relative(root, path);
      if (label.startsWith('packages/') && !label.includes('/node_modules/'))
        receipt.bundledProjectInputs[label] = createHash('sha256').update(await readFile(path)).digest('hex');
    }
    const code = result.outputFiles[0].contents;
    bundles[mutant ? `/${mutant}.js` : '/candidate.js'] = code;
    receipt[mutant ? `${mutant}BundleSha256` : 'candidateBundleSha256'] = createHash('sha256').update(code).digest('hex');
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
  receipt.complete = receipt.cases.length === 41 && receipt.cases.filter(c => c.complete).length === 23
    && receipt.cases.filter(c => c.observedViolation === 'regressed-counter-hides-committed-peer-update').length === 2
    && receipt.cases.filter(c => c.observedViolation === 'future-format-hydrated-by-old-reader').length === 5
    && receipt.cases.some(c => c.contract === 'accepted-outbox-survives-crash-and-late-settlement' && c.complete)
    && receipt.cases.filter(c => c.observedViolation === 'checkpoint-passes-missing-row').length === 2
    && receipt.cases.some(c => c.observedViolation === 'ambiguous-ownerless-checkpoint-overwritten')
    && receipt.cases.some(c => c.observedViolation === 'peer-update-hidden-by-reused-sequence')
    && receipt.cases.filter(c => c.observedViolation === 'invalid-startup-state-published').length === 2
    && receipt.cases.some(c => c.observedViolation === 'old-activation-cleared-new-owner')
    && receipt.cases.some(c => c.observedViolation === 'settled-activation-prevents-recovery')
    && receipt.cases.some(c => c.observedViolation === 'unsafe-activation-published-old-owner')
    && receipt.cases.some(c => c.observedViolation === 'older-activation-overrode-listener-owner')
    && receipt.cases.some(c => c.observedViolation === 'superseded-handle-reused-for-new-owner');
  if (process.env.GONVEX_BROWSER_TEST_OUTPUT) await writeFile(process.env.GONVEX_BROWSER_TEST_OUTPUT, JSON.stringify(receipt, null, 2) + '\n');
  if (server) await new Promise(resolve => server.close(resolve));
  if (temporary) await rm(temporary, { recursive: true, force: true });
  assert.equal(receipt.complete, true, 'all declared browser contracts and negative controls must complete');
});

async function launch(profile, mutant = false) {
  const context = await chromium.launchPersistentContext(profile, { headless: true, args: ['--no-sandbox'], serviceWorkers: 'block' });
  const page = context.pages()[0];
  await page.goto(origin);
  await page.addScriptTag({ url: origin + (mutant ? `/${mutant}.js` : '/candidate.js') });
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
for (const version of [1, 2]) test(`historical replica schema ${version} preserves complete owner snapshots across migration and browser restart`, { timeout: 60000 }, async () => {
  const profile = join(temporary, `historical-replica-${version}`), name = `historical-replica-${version}`;
  let running = await launch(profile);
  try {
    const rows = await running.page.evaluate(({ version, name }) => window.browserGuarantees.seedHistoricalReplica(version, name), { version, name });
    const original = { version, tables: ['snapshots'], rows };
    const aborted = await running.page.evaluate(async name => {
      try { return await window.browserGuarantees.abortHistoricalReplicaUpgrade(name); }
      catch (error) { throw Error(`${error.name}: ${error.message}`); }
    }, name);
    assert.equal(aborted.witnessed, 1, 'upgrade must write a normalized row before abort');
    assert.equal(aborted.aborted, 1, 'the native version-change transaction must actually abort');
    assert.equal(aborted.rejected, true);
    assert.deepEqual(aborted.raw, original, 'aborted upgrade must retain the exact original schema and snapshots');
    let observe;
    const witness = new Promise(resolve => { observe = resolve; });
    await running.page.exposeFunction('transactionWriteWitness', observe);
    const invocation = running.page.evaluate(name => window.browserGuarantees.crashHistoricalReplicaUpgrade(name), name)
      .then(() => ({ completed: true }), error => ({ error: String(error) }));
    await Promise.race([witness, invocation.then(result => { throw Error('historical upgrade ended before row witness: ' + JSON.stringify(result)); })]);
    await crash(running.context);
    assert.match((await invocation).error, /closed|crash/i);
    running = await launch(profile);
    const afterCrash = await running.page.evaluate(name => window.browserGuarantees.inspectHistoricalReplica(name), name);
    assert.deepEqual(afterCrash, original, 'browser crash during upgrade must retain the exact original schema and snapshots');
    const expected = version === 1 ? { default: { ...rows[0].snapshot, storageSequence: 0 }, a: undefined, b: undefined }
      : { default: undefined, a: { ...rows[0].snapshot, storageSequence: 0 }, b: { ...rows[1].snapshot, storageSequence: 0 } };
    const upgraded = await running.page.evaluate(async name => {
      try { return await window.browserGuarantees.readHistoricalReplica(name); }
      catch (error) { throw Error(`${error.name}: ${error.message}`); }
    }, name);
    assert.deepEqual(upgraded, expected, 'historical replica migration changed complete durable state or owner');
    await running.context.close();
    running = await launch(profile);
    const reopened = await running.page.evaluate(name => window.browserGuarantees.readHistoricalReplica(name), name);
    assert.deepEqual(reopened, expected, 'restarted historical replica lost migrated durable state or owner');
    receipt.cases.push({ contract: 'historical-replica', storedVersion: version, complete: true,
      upgradeAbort: { successfulRowRequests: aborted.witnessed, nativeAbortEvents: aborted.aborted, rejection: aborted.failure },
      browserKilledAfterSuccessfulRowRequest: true, originalSchemaAndRowsRestoredAfterCrash: true,
      checks: ['native-abort-after-normalized-row', 'browser-crash-after-normalized-row', 'original-schema-and-rows-retained', 'complete-migration', 'fresh-browser-reopen'] });
  } finally { await running.context.close(); }
});

for (const mutant of [false, 'legacy-ambiguity']) test(`${mutant ? 'negative control' : 'refusal'}: ambiguous ownerless replica snapshots`, { timeout: 60000 }, async () => {
  const profile = join(temporary, `ambiguous-${mutant || 'candidate'}`), name = 'ambiguous-ownerless';
  const { context, page } = await launch(profile, mutant);
  try {
    const rows = await page.evaluate(name => window.browserGuarantees.seedHistoricalReplica(1, name, true), name);
    const outcome = await page.evaluate(async name => {
      try { return { loaded: await window.browserGuarantees.readHistoricalReplica(name) }; }
      catch (error) { return { error: `${error.name}: ${error.message}` }; }
    }, name);
    if (mutant) {
      assert.equal(outcome.error, undefined, 'source mutant must reach actual migration');
      assert.equal(outcome.loaded.default.cursor.revision, 7, 'source mutant overwrites the other checkpoint with the last ownerless row');
      assert.equal(outcome.loaded.default.entities.tasks.a.name, 'Original');
      receipt.cases.push({ contract: 'historical-replica-ambiguity-control', observedViolation: 'ambiguous-ownerless-checkpoint-overwritten' });
    } else {
      assert.match(outcome.error, /Ambiguous ownerless replica snapshots require explicit recovery/);
      const raw = await page.evaluate(name => window.browserGuarantees.inspectHistoricalReplica(name), name);
      // IndexedDB key order is deterministic; the seed's insertion order differs.
      assert.deepEqual(raw, { version: 1, tables: ['snapshots'], rows: [...rows].sort((a, b) => a.key.localeCompare(b.key)) });
      receipt.cases.push({ contract: 'historical-replica-ambiguity-refusal', complete: true });
    }
  } finally { await context.close(); }
});

for (const version of [3, 4]) test(`historical normalized replica schema ${version} survives interrupted index upgrade and fresh browser restart`, { timeout: 60000 }, async () => {
  const profile = join(temporary, `normalized-historical-${version}`), name = `normalized-historical-${version}`;
  let running = await launch(profile);
  try {
    const original = await running.page.evaluate(({ version, name }) => window.browserGuarantees.seedNormalizedHistoricalReplica(version, name), { version, name });
    const aborted = await running.page.evaluate(name => window.browserGuarantees.interruptNormalizedHistoricalUpgrade(name, false), name);
    assert.equal(aborted.witnessed, 1, 'the new native index must answer a request before interruption');
    assert.equal(aborted.aborted, 1, 'the native upgrade transaction must actually abort');
    assert(aborted.error, 'adapter must reject the aborted upgrade');
    assert.deepEqual(aborted.raw, original.raw, 'abort changed historical schema, data or membership');
    let observe;
    const witness = new Promise(resolve => { observe = resolve; });
    await running.page.exposeFunction('transactionWriteWitness', observe);
    const invocation = running.page.evaluate(name => window.browserGuarantees.interruptNormalizedHistoricalUpgrade(name, true), name)
      .then(() => ({ completed: true }), error => ({ error: String(error) }));
    await Promise.race([witness, invocation.then(result => { throw Error('index upgrade ended before crash witness: ' + JSON.stringify(result)); })]);
    await crash(running.context);
    assert.match((await invocation).error, /closed|crash/i);
    running = await launch(profile);
    assert.deepEqual(await running.page.evaluate(name => window.browserGuarantees.inspectNamedDatabase(name), name), original.raw,
      'browser crash changed the historical schema, rows or metadata');
    const expected = { snapshots: original.expected, predicate: { rows: [{ id: 'a', name: 'Original' }], complete: true } };
    assert.deepEqual(await running.page.evaluate(name => window.browserGuarantees.readNormalizedHistoricalReplica(name), name), expected,
      'upgrade lost owner state, completeness or indexed predicate results');
    await running.context.close(); running = await launch(profile);
    assert.deepEqual(await running.page.evaluate(name => window.browserGuarantees.readNormalizedHistoricalReplica(name), name), expected,
      'fresh browser lost the upgraded owner state or predicate completeness');
    receipt.cases.push({ contract: 'historical-normalized-replica', storedVersion: version, complete: true,
      nativeIndexRequestBeforeAbort: true, nativeAbortEvents: aborted.aborted, browserKilledDuringVersionTransaction: true,
      rawSchemaAndDataRetained: true, predicateCompletenessAfterUpgradeAndRestart: true });
  } finally { await running.context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`${kind}: future replica format refuses before reading or repairing durable state`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `future-format-${kind}`));
  const name = `future-format-${kind}`;
  try {
    const before = await page.evaluate(({ kind, name }) => window.browserGuarantees.seedFutureReplica(kind, name), { kind, name });
    const after = await page.evaluate(({ kind, name }) => window.browserGuarantees.readFutureReplica(kind, name), { kind, name });
    assert.match(after.error ?? '', /VersionError|newer.*version|unsupported.*version/i,
      `${kind} read an unsupported future schema: ${JSON.stringify({ loaded: after.loaded, beforeVersion: before.version, afterVersion: after.raw.version })}`);
    assert.equal(after.loaded, undefined, 'future schema must not publish a snapshot');
    assert.deepEqual(after.raw, before, 'old reader must retain future schema, indexes, rows and accepted-history sentinel');
    receipt.cases.push({ adapter: kind, contract: 'future-replica-format-refusal', complete: true,
      nativeVersion: before.version, unchangedSchemaAndRows: true });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`${kind}: noncanonical persisted counters refuse every checkpoint read and write`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `counter-encoding-${kind}`));
  const invalid = ['', ' ', '01', '0x1', '1e0', '1.0', '+1', 1, true, null, [1], {}];
  try {
    for (const [index, value] of invalid.entries()) {
      const result = await page.evaluate(({ kind, name, value }) => window.browserGuarantees.replicaCounterIntegrity(kind, name, value),
        { kind, name: `counter-encoding-${kind}-${index}`, value });
      (receipt.counterIntegrityObservations ??= []).push({ adapter: kind, value, mode: 'encoded',
        priorSequence: result.priorSequence, commit: result.commit, peer: result.peer,
        durableRow: result.after.tables.entities.rows.find(row => row.scope === 'account-a' && row.id === 'a') });
      for (const [read, outcome] of Object.entries(result.reads))
        assert.match(outcome.error ?? '', /Invalid.*sequence/i, `${kind} ${read} accepted noncanonical counter ${JSON.stringify(value)}`);
      assert.match(result.writeError ?? '', /Invalid.*sequence/i, 'noncanonical counter admitted a durable write');
      assert.equal(result.commit, undefined);
      assert.deepEqual(result.after, result.before, 'refusal changed schema, rows, membership or counter bytes');
      assert.equal(result.other.cursor.revision, 3);
    }
    receipt.cases.push({ adapter: kind, contract: 'replica-counter-encoding', complete: true,
      invalidEncodingsRejected: invalid.length, readEntryPoints: 6, rawStatePreserved: true });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`${kind}: counter metadata cannot precede durable rows, windows or reset positions`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `counter-regression-${kind}`));
  try {
    for (const [mode, value] of [['encoded', '0'], ['window', '0'], ['missing', null], ['reset', '2']]) {
      const result = await page.evaluate(({ kind, name, value, mode }) => window.browserGuarantees.replicaCounterIntegrity(kind, name, value, mode),
        { kind, name: `counter-regression-${kind}-${mode}`, value, mode });
      (receipt.counterIntegrityObservations ??= []).push({ adapter: kind, value, mode,
        priorSequence: result.priorSequence, commit: result.commit, peer: result.peer,
        durableRow: result.after.tables.entities.rows.find(row => row.scope === 'account-a' && row.id === 'a') });
      for (const [read, outcome] of Object.entries(result.reads))
        assert.match(outcome.error ?? '', /Invalid.*sequence/i, `${kind} ${read} accepted a counter behind durable ${mode} state`);
      assert.match(result.writeError ?? '', /Invalid.*sequence/i, 'regressed counter reused a durable position');
      assert.equal(result.commit, undefined);
      assert.deepEqual(result.after, result.before);
      assert.equal(result.other.cursor.revision, mode === 'window' ? 7 : 3);
    }
    const isolated = await page.evaluate(({ kind, name }) => window.browserGuarantees.replicaCounterIntegrity(kind, name, '1', 'other-owner-ahead'),
      { kind, name: `counter-owner-progress-${kind}` });
    for (const outcome of Object.values(isolated.reads)) assert.equal(outcome.error, undefined, 'another owner counter prevented a valid scoped read');
    assert.equal(isolated.writeError, undefined);
    assert.equal(isolated.commit.sequence, 2);
    assert.equal(isolated.other.storageSequence, 1001);
    assert.equal(isolated.peer.value.entities.tasks.a.name, 'Changed');
    receipt.cases.push({ adapter: kind, contract: 'replica-counter-provenance', complete: true,
      regressedMetadataRejected: 4, rawStatePreserved: true, independentOwnerProgress: true });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`negative control: ${kind} loses a peer update when persisted counter integrity is omitted`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `counter-integrity-control-${kind}`), 'counter-integrity');
  try {
    const result = await page.evaluate(kind => window.browserGuarantees.replicaCounterIntegrity(kind, 'counter-integrity-control', '0'), kind);
    assert.equal(result.writeError, undefined);
    assert.equal(result.commit.sequence, result.priorSequence);
    const durable = result.after.tables.entities.rows.find(row => row.scope === 'account-a' && row.id === 'a');
    assert.equal(JSON.parse(durable.value).name, 'Changed');
    assert.equal(durable.authority.fields.name, 8);
    assert.equal(result.peer.error, undefined);
    assert.equal(result.peer.value.entities.tasks?.a, undefined, 'source control must actually omit the newly committed peer row');
    receipt.cases.push({ adapter: kind, observedViolation: 'regressed-counter-hides-committed-peer-update',
      previousSequence: result.priorSequence, reusedSequence: result.commit.sequence, durableRowRevision: 8 });
  } finally { await context.close(); }
});

for (const kind of ['direct-outbox', 'upgrade-queue', 'upgrade-meta']) test(`${kind}: future journal format preserves accepted history and refuses hydration`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `future-format-${kind}`));
  const name = `future-format-${kind}`;
  try {
    const before = await page.evaluate(({ kind, name }) => window.browserGuarantees.seedFutureJournal(kind, name), { kind, name });
    const after = await page.evaluate(({ kind, name }) => window.browserGuarantees.readFutureJournal(kind, name), { kind, name });
    assert.match(after.error ?? '', /VersionError|newer.*version|unsupported.*version/i,
      `${kind} hydrated unsupported future history: ${JSON.stringify({ loaded: after.loaded, beforeVersion: before.version, afterVersion: after.raw.version })}`);
    assert.equal(after.loaded, undefined, 'future history must not hydrate a queue');
    assert.deepEqual(after.raw, before, 'future journal bytes and schema must survive refusal');
    receipt.cases.push({ adapter: kind, contract: 'future-journal-format-refusal', complete: true,
      nativeVersion: before.version, unchangedSchemaAndRows: true });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine', 'direct-outbox', 'upgrade-queue', 'upgrade-meta']) test(`negative control: ${kind} accepts a future format when version retry loses its bound`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `future-version-control-${kind}`), 'future-version');
  const name = `future-version-control-${kind}`, replica = ['legacy', 'engine'].includes(kind);
  try {
    const before = await page.evaluate(({ kind, name, replica }) => replica
      ? window.browserGuarantees.seedFutureReplica(kind, name) : window.browserGuarantees.seedFutureJournal(kind, name), { kind, name, replica });
    const after = await page.evaluate(({ kind, name, replica }) => replica
      ? window.browserGuarantees.readFutureReplica(kind, name) : window.browserGuarantees.readFutureJournal(kind, name), { kind, name, replica });
    assert.equal(after.error, undefined, 'source control must reach actual future-format hydration');
    if (replica) assert.equal(after.loaded.cursor.revision, 7);
    else assert.equal(after.loaded[0].idempotencyKey, 'accepted-before-upgrade');
    if (kind !== 'upgrade-meta') {
      assert.equal(after.raw.version, before.version + 1, 'unbounded Dexie retry must actually repair a future schema');
      assert.notDeepEqual(after.raw, before);
    }
    receipt.cases.push({ adapter: kind, observedViolation: 'future-format-hydrated-by-old-reader',
      nativeVersionBefore: before.version, nativeVersionAfter: after.raw.version });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`${kind}: malformed or exhausted replica counters cannot commit a later checkpoint`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `sequence-${kind}`));
  const invalid = [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER + 1, -1, 1.5, 'malformed', 'Infinity'];
  try {
    for (const [index, sequence] of invalid.entries()) {
      const result = await page.evaluate(({ kind, name, sequence }) => window.browserGuarantees.replicaSequenceGuard(kind, name, sequence),
        { kind, name: `invalid-sequence-${kind}-${index}`, sequence });
      assert.match(result.error ?? '', /Invalid|exhausted/, `${kind} accepted invalid or exhausted sequence ${sequence}: ${JSON.stringify({ returnedSequence: result.result?.sequence, afterCursor: result.after?.cursor })}`);
      assert.equal(result.result, undefined);
      assert.deepEqual(result.rawAfter, result.rawBefore, 'rejected sequence must not change raw rows, membership or cursor');
      assert.equal(result.counter.value, String(sequence), 'rejected sequence must preserve the prior counter');
      assert.equal(result.other.cursor.revision, 3);
    }
    const last = await page.evaluate(({ kind, sequence }) => window.browserGuarantees.replicaSequenceGuard(kind, `last-sequence-${kind}`, sequence),
      { kind, sequence: Number.MAX_SAFE_INTEGER - 1 });
    assert.equal(last.error, undefined, 'last safe sequence must permit progress');
    assert.equal(last.result.sequence, Number.MAX_SAFE_INTEGER);
    assert.equal(last.counter.value, String(Number.MAX_SAFE_INTEGER));
    committed(last.after);
    receipt.cases.push({ adapter: kind, contract: 'replica-sequence-domain', invalidCountersRejected: invalid.length, lastSafeSequenceCommitted: true, complete: true });
  } finally { await context.close(); }
});

test('negative control: removing engine sequence admission hides a committed peer update', { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, 'engine-sequence-control'), 'engine-sequence');
  try {
    const result = await page.evaluate(sequence => window.browserGuarantees.replicaSequenceGuard('engine', 'reused-sequence', sequence, true), Number.MAX_SAFE_INTEGER);
    assert.equal(result.error, undefined);
    assert.equal(result.result.sequence, Number.MAX_SAFE_INTEGER + 1);
    assert.equal(result.second.sequence, result.result.sequence, 'source mutant must actually reuse a committed position');
    assert.equal(result.after.entities.tasks.a.name, 'Second');
    assert.equal(result.after.cursor.revision, 9);
    assert.equal(result.peerChanges.entities.tasks?.a, undefined, 'peer change reader must actually omit the later committed row');
    receipt.cases.push({ adapter: 'engine', negativeControl: 'sequence-and-integrity-admission-omitted', observedViolation: 'peer-update-hidden-by-reused-sequence',
      firstSequence: result.result.sequence, secondSequence: result.second.sequence, durableRevision: result.after.cursor.revision });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`${kind}: replica startup refuses invalid persisted sequence before publishing rows`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `startup-sequence-${kind}`));
  const invalid = [Number.MAX_SAFE_INTEGER + 1, -1, 1.5, 'malformed', 'Infinity', 'NaN'];
  try {
    for (const [index, sequence] of invalid.entries()) {
      const result = await page.evaluate(({ kind, name, sequence }) => window.browserGuarantees.replicaStartupSequence(kind, name, sequence),
        { kind, name: `invalid-startup-${kind}-${index}`, sequence });
      assert.match(result.error ?? '', /Invalid.*sequence/i, `${kind} published an invalid startup sequence ${sequence}: ${JSON.stringify({ row: result.row, cursor: result.cursor })}`);
      assert.equal(result.row, null);
      assert.equal(result.cursor, null);
      assert(result.publications.every(p => p.row === null && p.cursor === null), 'listener observed invalid persisted state');
      assert.deepEqual(result.after, result.before, 'failed startup changed persisted replica bytes');
      assert.equal(result.recoveryError, undefined, 'repaired metadata must permit a new activation');
      assert.equal(result.recovered.row.name, 'Original');
      assert.equal(result.recovered.cursor.revision, 7);
    }
    for (const sequence of [1, Number.MAX_SAFE_INTEGER]) {
      const result = await page.evaluate(({ kind, sequence }) => window.browserGuarantees.replicaStartupSequence(kind, `valid-startup-${kind}-${sequence}`, sequence), { kind, sequence });
      assert.equal(result.error, undefined, 'a valid persisted sequence must hydrate');
      assert.equal(result.row.name, 'Original');
      assert.equal(result.cursor.revision, 7);
    }
    receipt.cases.push({ adapter: kind, contract: 'replica-startup-sequence', invalidSequencesRejected: invalid.length,
      persistedStateRetained: true, sameReplicaRecoversAfterFaultRemoval: true, lastSafeSequenceReadable: true, complete: true });
  } finally { await context.close(); }
});

for (const kind of ['legacy', 'engine']) test(`negative control: unchecked ${kind} startup publishes invalid persisted state`, { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, `unchecked-startup-${kind}`), 'startup-validation');
  try {
    const result = await page.evaluate(kind => window.browserGuarantees.replicaStartupSequence(kind, 'unchecked-startup', Number.MAX_SAFE_INTEGER + 1), kind);
    assert.equal(result.error, undefined);
    assert.equal(result.row.name, 'Original');
    assert.equal(result.cursor.revision, 7);
    assert(result.publications.some(p => p.row?.name === 'Original' && p.cursor?.revision === 7));
    receipt.cases.push({ adapter: kind, negativeControl: 'startup-and-adapter-validation-omitted', observedViolation: 'invalid-startup-state-published' });
  } finally { await context.close(); }
});

test('negative control: old activation cleanup clears a newer owner handle', { timeout: 60000 }, async () => {
  for (const mutant of [false, 'activation-owner']) {
    const { context, page } = await launch(join(temporary, `activation-owner-${mutant || 'candidate'}`), mutant);
    try {
      const result = await page.evaluate(() => window.browserGuarantees.activationOwnerRace());
      assert.equal(result.row.owner, 'bob');
      assert.equal(result.sameInFlight, !mutant);
      assert.deepEqual(result.calls, mutant ? ['alice', 'bob', 'bob'] : ['alice', 'bob']);
      if (mutant) receipt.cases.push({ contract: 'activation-handle-ownership', storage: 'MemoryLocalReplicaStorage', positiveOwnedHandleRetained: true,
        negativeControl: mutant, observedViolation: 'old-activation-cleared-new-owner' });
    } finally { await context.close(); }
  }
});

test('negative control: retaining a settled activation prevents recovery after metadata repair', { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, 'activation-retention'), 'activation-retention');
  try {
    const result = await page.evaluate(() => window.browserGuarantees.replicaStartupSequence('engine', 'retained-activation', 'malformed'));
    assert.match(result.error, /Invalid replica storage sequence/);
    assert.equal(result.row, null);
    assert.equal(result.restoredCounter.value, '1', 'actual persisted fault must be removed before retry');
    assert.match(result.recoveryError, /Invalid replica storage sequence/);
    assert.equal(result.recovered, undefined);
    receipt.cases.push({ contract: 'activation-handle-retirement', negativeControl: 'activation-retention',
      observedViolation: 'settled-activation-prevents-recovery', restoredCounter: result.restoredCounter.value });
  } finally { await context.close(); }
});

test('negative control: unchecked activation identities publish an older owner at exhaustion', { timeout: 60000 }, async () => {
  for (const mutant of [false, 'activation-domain']) {
    const { context, page } = await launch(join(temporary, `activation-domain-${mutant || 'candidate'}`), mutant);
    try {
      const result = await page.evaluate(() => window.browserGuarantees.activationIdentityDomain());
      if (!mutant) {
        assert(result.outcomes.every(outcome => /scope activation.*exhausted/i.test(outcome.error)));
        assert.deepEqual(result.loads, ['alice']);
        assert.equal(result.row, null);
        assert(result.publications.every(owner => owner === null));
      } else {
        assert(result.outcomes.every(outcome => outcome.error === null));
        assert.deepEqual(result.loads, ['alice', 'bob', 'carol']);
        assert(result.publications.includes('bob'), 'source mutant must publish Bob after Carol was requested');
        assert.equal(result.row.owner, 'carol');
        receipt.cases.push({ contract: 'activation-identity-domain', storage: 'MemoryLocalReplicaStorage', negativeControl: mutant, positiveExhaustionRejected: true,
          observedViolation: 'unsafe-activation-published-old-owner', publications: result.publications });
      }
    } finally { await context.close(); }
  }
});

test('negative control: late identity capture overrides an owner requested by a listener', { timeout: 60000 }, async () => {
  for (const mutant of [false, 'activation-reentrancy']) {
    const { context, page } = await launch(join(temporary, `activation-reentrancy-${mutant || 'candidate'}`), mutant);
    try {
      const result = await page.evaluate(() => window.browserGuarantees.activationListenerRace());
      assert.equal(result.requestedOwner, 'bob');
      assert.equal(result.row.owner, mutant ? 'alice' : 'bob');
      assert.deepEqual(result.loads, mutant ? ['bob', 'alice'] : ['bob']);
      if (!mutant) assert(!result.publications.includes('alice'));
      else receipt.cases.push({ contract: 'activation-listener-ownership', storage: 'MemoryLocalReplicaStorage', negativeControl: mutant, positiveListenerOwnerPreserved: true,
        observedViolation: 'older-activation-overrode-listener-owner', requestedOwner: result.requestedOwner, publishedOwner: result.row.owner });
    } finally { await context.close(); }
  }
});

test('negative control: coalescing a superseded handle loses the latest return-to-owner request', { timeout: 60000 }, async () => {
  for (const mutant of [false, 'activation-cache']) {
    const { context, page } = await launch(join(temporary, `activation-cache-${mutant || 'candidate'}`), mutant);
    try {
      const result = await page.evaluate(() => window.browserGuarantees.activationReturnRace());
      assert.equal(result.phase, 2);
      assert.equal(result.requestedOwner, 'alice');
      assert.equal(result.reusedOldPromise, Boolean(mutant));
      assert.equal(result.row.owner, mutant ? 'bob' : 'alice');
      assert.deepEqual(result.loads, mutant ? ['alice', 'bob'] : ['alice', 'alice']);
      if (!mutant) assert(!result.publications.includes('bob'));
      else receipt.cases.push({ contract: 'activation-cache-ownership', storage: 'MemoryLocalReplicaStorage', negativeControl: mutant,
        positiveReturnedOwnerPreserved: true, observedViolation: 'superseded-handle-reused-for-new-owner',
        requestedOwner: result.requestedOwner, publishedOwner: result.row.owner, reusedOldPromise: result.reusedOldPromise });
    } finally { await context.close(); }
  }
});
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
      assert.equal(queue.length, 1); assert.equal(queue[0].state, 'committed');
      assert.equal(queue[0].awaitingReplica, true);
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

test('opening watermark deferral still protects data when only snapshot deferral is omitted', { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, 'snapshot-only'), 'snapshot');
  try {
    for (const kind of ['legacy', 'engine']) {
      const result = await page.evaluate(kind => browserGuarantees.snapshotBarrier(kind, 'snapshot-only-' + kind), kind);
      assert.equal(result.before.saved.cursor.revision, 7);
      assert.equal(result.before.saved.entities.tasks.c, undefined);
      assert.equal(result.before.outcome, 'pending');
      assert.equal(result.after.outcome, 'success');
      assert.equal(result.after.saved.entities.tasks.c.name, 'New snapshot row');
      receipt.cases.push({ adapter: kind, control: 'snapshot-only-deferral-omitted', complete: true });
    }
  } finally { if (context.browser()?.isConnected()) await context.close(); }
});

test('negative control: omitting both snapshot and opening deferral certifies a missing row', { timeout: 60000 }, async () => {
  const { context, page } = await launch(join(temporary, 'mutant'), 'both');
  try {
    for (const kind of ['legacy', 'engine']) {
      const result = await page.evaluate(kind => browserGuarantees.snapshotBarrier(kind, 'mutant-' + kind), kind);
      assert.equal(result.before.saved.cursor.revision, 8, 'mutant must reach the bad certificate');
      assert.equal(result.before.saved.entities.tasks.c, undefined, 'mutant must certify an unpersisted snapshot row');
      receipt.cases.push({ adapter: kind, negativeControl: 'snapshot-and-opening-deferral-omitted', observedViolation: 'checkpoint-passes-missing-row' });
    }
  } finally { if (context.browser()?.isConnected()) await context.close(); }
});
