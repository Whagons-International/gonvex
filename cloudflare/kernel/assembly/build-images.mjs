// 0060 hosted-CI fallback. The arch3 Docker daemon must not accumulate a
// fresh task build cache outside /mnt or escape the shared workload slice.
// This entry accepts no arbitrary command, Dockerfile, registry or source ref.
import { spawn, execFileSync } from 'node:child_process';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readFile, writeFile, rm, stat } from 'node:fs/promises';
import { resolve, join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { cloudflareCredentials, developmentName } from './deploy.mjs';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const packageDir = join(root, 'cloudflare');

async function run(command, args, env, log, capture = false) {
  return new Promise((ok, fail) => {
    const outputFile = createWriteStream(log, { mode: 0o600 });
    const child = spawn(command, args, { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '', excessive = false;
    child.stdout.pipe(outputFile, { end: false }); child.stderr.pipe(outputFile, { end: false });
    if (capture) child.stdout.on('data', part => {
      if (output.length + part.length > 4 * 1024 * 1024) { excessive = true; child.kill('SIGTERM'); }
      else output += part;
    });
    outputFile.on('error', fail); child.on('error', fail);
    child.on('exit', code => outputFile.end(() => {
      code === 0 && !excessive ? ok(output) : fail(new Error(`${command} ${args[0]} exited ${code}; see private build log ${log}`));
    }));
  });
}

export async function buildAssemblyImages({ name, directory, exportOnly = false }) {
  developmentName(name);
  if (process.env.GITHUB_ACTIONS !== 'true' || !process.env.RUNNER_TEMP) throw new Error('fresh Docker builds require the approved hosted CI lane; no arch3 daemon/cache workaround');
  directory = resolve(directory); await mkdir(directory, { recursive: true, mode: 0o700 });
  const credentials = exportOnly ? null : await cloudflareCredentials();
  // Docker receives no CI token, Vault file, or Cloudflare credential. The
  // build context below is only the tracked immutable source, without .git.
  const env = { PATH: process.env.PATH, HOME: process.env.HOME, TMPDIR: process.env.RUNNER_TEMP,
    DOCKER_BUILDKIT: '1', DOCKER_CONFIG: join(directory, 'docker-auth') };
  await mkdir(env.DOCKER_CONFIG, { recursive: true, mode: 0o700 });
  const source = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim();
  const dirty = execFileSync('git', ['status', '--porcelain', '--untracked-files=no'], { cwd: root, encoding: 'utf8' });
  if (dirty) throw new Error('immutable committed build inputs required');
  const paths = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8' }).split('\0').filter(Boolean);
  const inputs = [];
  for (const path of paths) inputs.push({ path, sha256: createHash('sha256').update(await readFile(join(root, path))).digest('hex') });
  await writeFile(join(directory, 'inputs.json'), JSON.stringify({ source, inputs }, null, 2));
  const context = join(directory, 'context');
  await mkdir(context, { recursive: true });
  const sourceArchive = join(directory, 'source.tar');
  await run('git', ['archive', '--format=tar', '--output', sourceArchive, source], env, join(directory, 'source-archive.log'));
  await run('tar', ['-xf', sourceArchive, '-C', context], env, join(directory, 'source-extract.log'));
  await rm(sourceArchive);
  const images = {}, receipts = [];
  let archiveBytes = 0;
  try {
  for (const [kind, dockerfile] of [['sandbox', 'cloudflare/sandbox/container/Dockerfile'], ['builder', 'cloudflare/control/operator/builder/Dockerfile']]) {
    const tag = `${name}-${kind}:${source}`;
    let recipe = await readFile(join(root, dockerfile), 'utf8');
    const bases = [];
    const refs = [...recipe.matchAll(/^(?:FROM\s+|#\s*syntax=)([^\s]+)(.*)$/gm)];
    for (const match of refs) {
      const image = match[1];
      const output = await run('docker', ['buildx', 'imagetools', 'inspect', image, '--format', '{{json .Manifest.Digest}}'], env, join(directory, `${kind}-base-${bases.length}.log`), true);
      const digest = JSON.parse(output.trim());
      if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`immutable base digest missing for ${kind}`);
      const pinned = `${image.split('@')[0]}@${digest}`;
      recipe = recipe.replace(match[0], match[0].replace(image, pinned));
      bases.push({ original: image, pinned });
    }
    const pinnedRecipe = join(directory, `${kind}.Dockerfile`);
    await writeFile(pinnedRecipe, recipe);
    await run('docker', ['build', '--platform', 'linux/amd64', '--provenance=mode=max', '--label', `org.opencontainers.image.revision=${source}`, '--file', pinnedRecipe, '--tag', tag, context], env, join(directory, `${kind}-build.log`));
    const beforePush = JSON.parse(await run('docker', ['image', 'inspect', tag], env, join(directory, `${kind}-local.json`), true))[0];
    const archive = `${kind}.docker.tar`, archivePath = join(directory, archive);
    await run('docker', ['image', 'save', '--output', archivePath, tag], env, join(directory, `${kind}-export.log`));
    const bytes = (await stat(archivePath)).size;
    archiveBytes += bytes;
    // Finite hosted artifact envelope; exceeding it is a recorded build failure.
    if (bytes > 8 * 1024 ** 3 || archiveBytes > 12 * 1024 ** 3) throw new Error('development image archives exceed the 8 GiB/image or 12 GiB/job export envelope');
    const hash = createHash('sha256');
    for await (const part of createReadStream(archivePath)) hash.update(part);
    let digest;
    if (credentials) {
      const publishEnv = { ...env, CLOUDFLARE_ACCOUNT_ID: credentials.accountId, CLOUDFLARE_API_TOKEN: credentials.apiToken, WRANGLER_SEND_METRICS: 'false' };
      await run(join(packageDir, 'node_modules/.bin/wrangler'), ['containers', 'push', tag], publishEnv, join(directory, `${kind}-push.log`));
      const image = JSON.parse(await run('docker', ['image', 'inspect', tag], env, join(directory, `${kind}-pushed.json`), true))[0];
      digest = image.RepoDigests.find((value) => value.startsWith(`registry.cloudflare.com/${credentials.accountId}/`) && value.includes(`/${name}-${kind}@sha256:`));
      if (!digest) throw new Error(`immutable pushed ${kind} registry digest was not reported`);
      images[kind] = digest;
    }
    receipts.push({ kind, source, dockerfile, pinnedDockerfileSha256: createHash('sha256').update(recipe).digest('hex'), bases,
      imageId: beforePush.Id, configDigest: beforePush.Id, labels: beforePush.Config.Labels,
      archive: { format: 'docker-save-v1', file: archive, bytes, sha256: hash.digest('hex'), tag }, ...(digest ? { registryDigest: digest } : {}) });
  }
  } finally { await rm(env.DOCKER_CONFIG, { recursive: true, force: true }); }
  // Build logs preserve BuildKit's resolved base-image digests and build steps;
  // no raw auth or logs are published by this helper.
  await writeFile(join(directory, 'images.json'), JSON.stringify({ source, images, receipts }, null, 2));
  return { source, images, receipts };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [name, directory, mode] = process.argv.slice(2);
  if (mode !== undefined && mode !== '--export-only') throw new Error('unknown image build mode');
  buildAssemblyImages({ name, directory, exportOnly: mode === '--export-only' }).then((result) => process.stdout.write(`${JSON.stringify(result)}\n`)).catch((error) => { console.error(error.message); process.exitCode = 1; });
}
