// 0060 hosted-CI fallback. The arch3 Docker daemon must not accumulate a
// fresh task build cache outside /mnt or escape the shared workload slice.
// This entry accepts no arbitrary command, Dockerfile, registry or source ref.
import { spawn, execFileSync } from 'node:child_process';
import { mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { resolve, join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { cloudflareCredentials, developmentName } from './deploy.mjs';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const packageDir = join(root, 'cloudflare');

async function run(command, args, env, log) {
  return new Promise((ok, fail) => {
    const child = spawn(command, args, { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (part) => { output += part; }); child.stderr.on('data', (part) => { output += part; });
    child.on('error', fail);
    child.on('exit', async (code) => {
      await writeFile(log, output, { mode: 0o600 });
      code === 0 ? ok(output) : fail(new Error(`${command} ${args[0]} exited ${code}; see private build log ${log}`));
    });
  });
}

export async function buildAssemblyImages({ name, directory }) {
  developmentName(name);
  if (process.env.GITHUB_ACTIONS !== 'true' || !process.env.RUNNER_TEMP) throw new Error('fresh Docker builds require the approved hosted CI lane; no arch3 daemon/cache workaround');
  directory = resolve(directory); await mkdir(directory, { recursive: true, mode: 0o700 });
  const credentials = await cloudflareCredentials();
  const env = { ...process.env, CLOUDFLARE_ACCOUNT_ID: credentials.accountId, CLOUDFLARE_API_TOKEN: credentials.apiToken,
    WRANGLER_SEND_METRICS: 'false', DOCKER_CONFIG: join(directory, 'docker-auth') };
  delete env.WHAGONS_CREDENTIAL_FILE;
  await mkdir(env.DOCKER_CONFIG, { recursive: true, mode: 0o700 });
  const source = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim();
  const dirty = execFileSync('git', ['status', '--porcelain', '--untracked-files=no'], { cwd: root, encoding: 'utf8' });
  if (dirty) throw new Error('immutable committed build inputs required');
  const paths = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8' }).split('\0').filter(Boolean);
  const inputs = [];
  for (const path of paths) inputs.push({ path, sha256: createHash('sha256').update(await readFile(join(root, path))).digest('hex') });
  await writeFile(join(directory, 'inputs.json'), JSON.stringify({ source, inputs }, null, 2));
  const images = {}, receipts = [];
  try {
  for (const [kind, dockerfile] of [['sandbox', 'cloudflare/sandbox/container/Dockerfile'], ['builder', 'cloudflare/control/operator/builder/Dockerfile']]) {
    const tag = `${name}-${kind}:${source}`;
    let recipe = await readFile(join(root, dockerfile), 'utf8');
    const bases = [];
    const refs = [...recipe.matchAll(/^(?:FROM\s+|#\s*syntax=)([^\s]+)(.*)$/gm)];
    for (const match of refs) {
      const image = match[1];
      const output = await run('docker', ['buildx', 'imagetools', 'inspect', image, '--format', '{{json .Manifest.Digest}}'], env, join(directory, `${kind}-base-${bases.length}.log`));
      const digest = JSON.parse(output.trim());
      if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`immutable base digest missing for ${kind}`);
      const pinned = `${image.split('@')[0]}@${digest}`;
      recipe = recipe.replace(match[0], match[0].replace(image, pinned));
      bases.push({ original: image, pinned });
    }
    const pinnedRecipe = join(directory, `${kind}.Dockerfile`);
    await writeFile(pinnedRecipe, recipe);
    await run('docker', ['build', '--platform', 'linux/amd64', '--provenance=mode=max', '--label', `org.opencontainers.image.revision=${source}`, '--file', pinnedRecipe, '--tag', tag, '.'], env, join(directory, `${kind}-build.log`));
    const beforePush = JSON.parse(await run('docker', ['image', 'inspect', tag], env, join(directory, `${kind}-local.json`)))[0];
    await run(join(packageDir, 'node_modules/.bin/wrangler'), ['containers', 'push', tag], env, join(directory, `${kind}-push.log`));
    const image = JSON.parse(await run('docker', ['image', 'inspect', tag], env, join(directory, `${kind}-pushed.json`)))[0];
    const digest = image.RepoDigests.find((value) => value.startsWith(`registry.cloudflare.com/${credentials.accountId}/`) && value.includes(`/${name}-${kind}@sha256:`));
    if (!digest) throw new Error(`immutable pushed ${kind} registry digest was not reported`);
    images[kind] = digest;
    receipts.push({ kind, source, dockerfile, pinnedDockerfileSha256: createHash('sha256').update(recipe).digest('hex'), bases, imageId: beforePush.Id, registryDigest: digest, labels: image.Config.Labels });
  }
  } finally { await rm(env.DOCKER_CONFIG, { recursive: true, force: true }); }
  // Build logs preserve BuildKit's resolved base-image digests and build steps;
  // no raw auth or logs are published by this helper.
  await writeFile(join(directory, 'images.json'), JSON.stringify({ source, images, receipts }, null, 2));
  return { source, images, receipts };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [name, directory] = process.argv.slice(2);
  buildAssemblyImages({ name, directory }).then((result) => process.stdout.write(`${JSON.stringify(result)}\n`)).catch((error) => { console.error(error.message); process.exitCode = 1; });
}
