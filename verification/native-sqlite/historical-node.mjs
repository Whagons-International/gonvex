import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { globSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { historicalPhase } from './historical.mjs';
import { corruptJournalPhase } from './corrupt-journal.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
const directory = resolve(args.get('--directory'));
mkdirSync(directory, { recursive: true });
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
if (args.get('--build-mutant') === 'yes') {
  const sourcePath = resolve(root, 'packages/expo-sqlite/src/outbox.ts');
  const original = readFileSync(sourcePath, 'utf8');
  const start = original.indexOf('  importLegacy('), end = original.indexOf('  private initialize()', start);
  assert(start >= 0 && end > start);
  const method = original.slice(start, end), before = '...(entry.state === "inflight" ? { state: "pending" as const } : {}),';
  assert.equal(method.split(before).length, 2);
  const changed = original.slice(0, start) + method.replace(before,
    before + '\n          ...(entry.state === "committed" ? { state: "pending" as const, awaitingReplica: undefined } : {}),') + original.slice(end);
  const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
  assert(bundler, 'frozen workspace esbuild required');
  const esbuild = await import(pathToFileURL(resolve(root, bundler)).href);
  const outfile = resolve(directory, 'adapter-mutant.mjs');
  const build = await esbuild.build({
    absWorkingDir: root, entryPoints: ['packages/expo-sqlite/src/index.ts'], outfile,
    bundle: true, platform: 'node', format: 'esm', packages: 'external', metafile: true,
    plugins: [{ name: 'accepted-import-source-control', setup(build) {
      build.onResolve({ filter: /^@gonvex\/client$/ }, () => ({ path: resolve(root, 'packages/client/dist/index.js'), external: true }));
      build.onLoad({ filter: /\/packages\/expo-sqlite\/src\/outbox\.ts$/ }, args => {
        assert.equal(args.path, sourcePath); return { contents: changed, loader: 'ts' };
      });
    } }],
  });
  // The control uses the same built client as the positive adapter. Resolve it
  // explicitly so the owned result directory cannot select a foreign package.
  const sources = Object.fromEntries(Object.keys(build.metafile.inputs).map(name => [name, hash(readFileSync(resolve(root, name)))]));
  writeFileSync(resolve(directory, 'mutation.json'), JSON.stringify({ mutations: 1, method: 'importLegacy', changedFields: ['state', 'awaitingReplica'],
    originalSha256: hash(original), mutatedSha256: hash(changed), bundleSha256: hash(readFileSync(outfile)),
    esbuild: esbuild.version, sources }, null, 2) + '\n');
} else {
  const phase = args.get('--phase'), runId = args.get('--run-id');
  assert(/^[a-zA-Z0-9_-]{1,80}$/.test(runId));
  const adapterPath = process.env.GONVEX_HISTORICAL_ADAPTER;
  const { ExpoSQLiteLocalReplicaStorage, ExpoSQLiteOutboxStore, migrateLegacyOutbox } = await import(adapterPath
    ? pathToFileURL(resolve(adapterPath)).href : new URL('../../packages/expo-sqlite/dist/index.js', import.meta.url).href);
  const { StoreReducerOutbox } = await import('../../packages/client/dist/outbox.js');
  const versions = new Set();
  const SQLite = {
    async deleteDatabaseAsync(name) { unlinkSync(resolve(directory, name)); },
    async openDatabaseAsync(name) {
      assert(/^gonvex-historical-[a-zA-Z0-9_-]+\.db$/.test(name));
      const db = new DatabaseSync(resolve(directory, name));
      // A targeted fault model for the bound bytes observed on Android. This
      // does not claim native execution; native CI runs the same schedule.
      const bind = params => params.map(value => process.env.GONVEX_HISTORICAL_TRUNCATE_BINDINGS === '1' && typeof value === 'string'
        ? value.split('\0', 1)[0] : value);
      versions.add(db.prepare('SELECT sqlite_version() AS version').get().version);
      return {
        async execAsync(sql) { db.exec(sql); },
        async runAsync(sql, ...params) { return db.prepare(sql).run(...bind(params)); },
        async getAllAsync(sql, ...params) { return db.prepare(sql).all(...bind(params)); },
        async getFirstAsync(sql, ...params) { return db.prepare(sql).get(...bind(params)) ?? null; },
        async withTransactionAsync(run) {
          db.exec('BEGIN');
          try { await run(); db.exec('COMMIT'); } catch (error) { db.exec('ROLLBACK'); throw error; }
        },
        async closeAsync() { db.close(); },
      };
    },
  };
  // The mobile host has a live event loop. Retain one in this CLI while a
  // deliberate native transaction hold awaits its parent's SIGKILL.
  const keepAlive = setInterval(() => {}, 1000);
  try {
    const execute = phase.startsWith('journal-corrupt-') ? corruptJournalPhase : historicalPhase;
    await execute({ phase, runId }, async event => {
      const path = resolve(directory, phase + '.json');
      writeFileSync(path + '.tmp', JSON.stringify({ ...event, runtime: { node: process.version, sqlite: [...versions], textBindings: process.env.GONVEX_HISTORICAL_TRUNCATE_BINDINGS === '1' ? 'nul-truncation-fault-model' : 'native-Node' } }) + '\n');
      renameSync(path + '.tmp', path);
    }, { SQLite, Replica: ExpoSQLiteLocalReplicaStorage, Outbox: ExpoSQLiteOutboxStore, Queue: StoreReducerOutbox, migrateLegacyOutbox });
  } finally { clearInterval(keepAlive); }
}
