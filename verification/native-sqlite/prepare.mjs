import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { globSync } from 'node:fs';
import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const project = dirname(fileURLToPath(import.meta.url));
const root = resolve(project, '../..');
const work = join(project, '.work');
const tool = JSON.parse(await readFile(join(project, 'toolchain.json'), 'utf8'));
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const collectorPort = Number(process.env.GONVEX_NATIVE_COLLECTOR_PORT ?? 18561);
assert.ok(Number.isInteger(collectorPort) && collectorPort >= 1024 && collectorPort <= 65535);
await rm(work, { recursive: true, force: true });
await mkdir(join(work, 'sdk/baseline'), { recursive: true });
const inputs = {};
for (const [name, expected] of Object.entries(tool.baseline.files)) {
  const bytes = execFileSync('git', ['show', `${tool.baseline.commit}:packages/expo-sqlite/src/${name}`], { cwd: root });
  assert.equal(digest(bytes), expected, `baseline ${name} changed`);
  await writeFile(join(work, 'sdk/baseline', name), bytes);
  inputs[`baseline/${name}`] = expected;
}
await cp(join(root, 'packages/expo-sqlite/src'), join(work, 'sdk/fixed'), {
  recursive: true,
  filter: file => !/\.(?:test|spec)\.[^.]+$/.test(file),
});
await writeFile(join(work, 'config.js'), `export const collectorURL = ${JSON.stringify(`http://127.0.0.1:${collectorPort}`)};\n`);

const [bundler] = globSync('node_modules/.pnpm/esbuild@*/node_modules/esbuild/lib/main.js', { cwd: root });
assert.ok(bundler, 'install frozen monorepo dependencies first');
const { build } = await import(pathToFileURL(join(root, bundler)).href);
const result = await build({
  absWorkingDir: project,
  entryPoints: ['proof-entry.js'],
  outfile: join(work, 'proof.js'),
  bundle: true,
  platform: 'neutral',
  format: 'esm',
  mainFields: ['react-native', 'module', 'main'],
  conditions: ['react-native'],
  external: ['expo-sqlite', 'react-native'],
  alias: {
    '@gonvex/client': join(root, 'packages/client/src/index.ts'),
    '@proof/queue': join(root, 'packages/client/src/outbox.ts'),
  },
  metafile: true,
});
for (const name of Object.keys(result.metafile.inputs)) inputs[name] = digest(await readFile(resolve(project, name)));
const fixtureInputs = {};
for (const name of ['index.js', 'metro.config.js', 'app.json', 'package.json', 'toolchain.json', 'prepare.mjs', 'run.py', 'contract.py']) {
  fixtureInputs[name] = digest(await readFile(join(project, name)));
}
const manifest = {
  head: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
  baseline: tool.baseline.commit,
  sourceSha256: inputs,
  fixtureSha256: fixtureInputs,
  bundleSha256: digest(await readFile(join(work, 'proof.js'))),
  workspaceLockSha256: digest(await readFile(join(root, 'pnpm-lock.yaml'))),
  fixtureLockSha256: digest(await readFile(join(project, 'package-lock.json'))),
};
await writeFile(join(work, 'source.json'), JSON.stringify(manifest, null, 2) + '\n');
console.log(`Prepared actual SDK source and ${Object.keys(inputs).length} hashed inputs`);
