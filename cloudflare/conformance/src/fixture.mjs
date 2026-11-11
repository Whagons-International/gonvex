/**
 * Builds the conformance fixture bundle with the repository's own CLI
 * (`gonvex codegen`), so every backend receives byte-identical `/dev/sync`
 * payloads. Generation 1 is `fixtures/app`; generation 2 is the same tree with
 * `fixtures/app-v2` copied on top.
 *
 * Nothing generated is committed: bundles are built into a temp directory once
 * per run and shared by all backends.
 */
import { cp, mkdir, readFile, symlink, rm } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { REPO, CONFORMANCE } from './paths.mjs';

const run = promisify(execFile);

/**
 * @param {string} workDir scratch directory for this run
 * @param {1|2} generation
 * @returns {Promise<{ generation: number, manifest: any, api: any, hash: string }>}
 */
export async function buildFixture(workDir, generation) {
  const dir = join(workDir, `fixture-v${generation}`);
  await rm(dir, { recursive: true, force: true });
  await cp(join(CONFORMANCE, 'fixtures/app'), dir, { recursive: true });
  if (generation === 2) await cp(join(CONFORMANCE, 'fixtures/app-v2'), dir, { recursive: true, force: true });
  await mkdir(join(dir, 'node_modules/@gonvex'), { recursive: true });
  for (const pkg of ['module-sdk', 'client', 'protocol']) {
    await symlink(join(REPO, 'packages', pkg), join(dir, 'node_modules/@gonvex', pkg), 'dir');
  }
  const cli = join(REPO, 'packages/gonvex/dist/index.js');
  try {
    await run(process.execPath, [cli, 'codegen', '--project', dir], { cwd: dir, env: { ...process.env, NODE_NO_WARNINGS: '1' } });
  } catch (error) {
    throw new Error(`gonvex codegen failed for fixture v${generation} (run \`pnpm build:packages\` at the repo root first):\n${error.stderr || error.message}`);
  }
  const manifest = JSON.parse(await readFile(join(dir, 'gonvex/_generated/manifest.json'), 'utf8'));
  // The generated API is TypeScript with type-only syntax; Node strips it.
  const { api } = await import(pathToFileURL(join(dir, 'gonvex/_generated/api.ts')).href);
  return { generation, manifest, api, hash: manifest.module?.hash };
}
