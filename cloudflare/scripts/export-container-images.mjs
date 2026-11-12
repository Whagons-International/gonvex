// 0066: credential-free hosted builds only. Publication is a separate Vault-owned step.
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readFile, writeFile, readdir, lstat, readlink, rm, stat, realpath, statfs } from 'node:fs/promises';
import { resolve, join, dirname, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const exec = promisify(execFile);
const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const RECIPES = [['sandbox', 'cloudflare/sandbox/container/Dockerfile'], ['builder', 'cloudflare/control/operator/builder/Dockerfile']];
const sha = value => createHash('sha256').update(value).digest('hex');
const json = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + '\n');
async function output(command, args, cwd) {
  return (await exec(command, args, { cwd, maxBuffer: 4 * 1024 * 1024, encoding: 'utf8' })).stdout;
}
async function fileHash(path) {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
}

export function dockerEnvironment(env, directory) {
  return { PATH: env.PATH, HOME: env.HOME, TMPDIR: env.RUNNER_TEMP,
    DOCKER_BUILDKIT: '1', DOCKER_CONFIG: join(directory, 'docker-auth') };
}

export async function prepareContext(root, directory, expectedSource) {
  if (!/^[a-f0-9]{40}$/.test(expectedSource ?? '')) throw new Error('expected full PR head SHA required');
  const source = (await output('git', ['rev-parse', 'HEAD'], root)).trim();
  if (source !== expectedSource) throw new Error('checkout differs from expected PR head');
  if (await output('git', ['status', '--porcelain', '--untracked-files=no'], root)) throw new Error('tracked source is dirty');
  const tracked = (await output('git', ['ls-tree', '-rz', '--full-tree', source], root)).split('\0').filter(Boolean).map(line => {
    const match = /^(\d+) (\w+) ([a-f0-9]+)\t([\s\S]+)$/.exec(line);
    if (!match || match[2] !== 'blob') throw new Error('unsupported tracked input type');
    return { path: match[4], mode: match[1], gitBlob: match[3] };
  });
  const context = join(directory, 'context'), archive = join(directory, 'source.tar');
  await mkdir(context); // Never reuse a context containing leftovers.
  await output('git', ['archive', '--format=tar', '--output', archive, source], root);
  const archiveSha256 = await fileHash(archive);
  await output('tar', ['-xf', archive, '-C', context], root);
  const inputs = [];
  const walk = async (path = '') => {
    for (const name of (await readdir(join(context, path))).sort()) {
      const item = path ? `${path}/${name}` : name, absolute = join(context, item), info = await lstat(absolute);
      if (info.isDirectory()) await walk(item);
      else if (info.isSymbolicLink()) inputs.push({ path: item, type: 'symlink', sha256: sha(await readlink(absolute)) });
      else if (info.isFile()) inputs.push({ path: item, type: 'file', bytes: info.size, executable: !!(info.mode & 0o111), sha256: await fileHash(absolute) });
      else throw new Error('unsupported effective context entry');
    }
  };
  await walk();
  await json(join(directory, 'inputs.json'), { source, expectedSource, archiveSha256, tracked, inputs });
  await rm(archive);
  return { source, context, archiveSha256, contextManifestSha256: await fileHash(join(directory, 'inputs.json')) };
}

export async function pinRecipe(recipe, digestFor) {
  const bases = [];
  for (const match of [...recipe.matchAll(/^(?:FROM\s+|#\s*syntax=)([^\s]+)(.*)$/gm)]) {
    const digest = await digestFor(match[1]);
    if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error('invalid immutable base digest');
    const pinned = `${match[1].split('@')[0]}@${digest}`;
    recipe = recipe.replace(match[0], match[0].replace(match[1], pinned));
    bases.push({ original: match[1], pinned });
  }
  if (!bases.length) throw new Error('recipe has no pinned base');
  return { recipe, bases };
}

export function archiveBudget(bytes, accumulated) {
  if (!Number.isSafeInteger(bytes) || !Number.isSafeInteger(accumulated) || accumulated < 0 || bytes <= 0 || bytes > 8 * 1024 ** 3 || accumulated + bytes > 12 * 1024 ** 3) {
    throw new Error('image archives exceed 8 GiB/image or 12 GiB/job envelope');
  }
  return accumulated + bytes;
}

export async function verifyArchive(path, tag, expected, report = async () => {}) {
  expected = typeof expected === 'string' ? { id: expected } : expected;
  const manifest = JSON.parse(await output('tar', ['-xOf', path, 'manifest.json']));
  if (!Array.isArray(manifest) || manifest.length !== 1 || !manifest[0].RepoTags?.includes(tag)) throw new Error('unexpected archive image/tag');
  const config = manifest[0].Config;
  if (!/^(?:[a-f0-9]{64}\.json|blobs\/sha256\/[a-f0-9]{64})$/.test(config)) throw new Error('invalid archive config path');
  const read = async name => (await exec('tar', ['-xOf', path, name], { maxBuffer: 4 * 1024 * 1024, encoding: 'buffer' })).stdout;
  const decode = bytes => JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  const bytes = await read(config), configDigest = `sha256:${sha(bytes)}`, parsed = decode(bytes);
  const audit = { verified: false, builtId: expected.id, builtDescriptor: expected.descriptor,
    configDigest, configBytes: bytes.length, archiveConfigPath: config, graph: [] };
  await report(audit);
  if (!config.includes(configDigest.slice(7))) throw new Error('archive config filename differs from its bytes');
  if (parsed.os !== 'linux' || parsed.architecture !== 'amd64') throw new Error('unexpected image platform');
  if (expected.configDigest && expected.configDigest !== configDigest) throw new Error('build config digest differs from archive');
  if (expected.diffIds && JSON.stringify(expected.diffIds) !== JSON.stringify(parsed.rootfs?.diff_ids)) throw new Error('built rootfs differs from archive config');
  if (expected.id === configDigest) {
    // Classic Docker stores use the config digest as Id. Containerd stores
    // may instead return an image/index digest, so never equate these blindly.
    audit.mapping = 'built-config';
  } else {
    const indexTypes = new Set(['application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json']);
    const imageTypes = new Set(['application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json']);
    const descriptor = expected.descriptor;
    if (!descriptor || descriptor.digest !== expected.id || !indexTypes.has(descriptor.mediaType) && !imageTypes.has(descriptor.mediaType)) {
      throw new Error('archive config differs from built image and no matching built descriptor exists');
    }
    let consumed = 0;
    const cache = new Map();
    const blob = async desc => {
      if (!/^sha256:[a-f0-9]{64}$/.test(desc.digest) || !Number.isSafeInteger(desc.size) || desc.size <= 0 || desc.size > 4 * 1024 * 1024) throw new Error('invalid image descriptor');
      if (cache.has(desc.digest)) {
        const value = cache.get(desc.digest);
        if (value.size !== desc.size || value.type !== desc.mediaType) throw new Error('conflicting image descriptor');
        return value.body;
      }
      if (cache.size >= 128 || consumed + desc.size > 16 * 1024 * 1024) throw new Error('image metadata graph budget exceeded');
      const raw = await read(`blobs/sha256/${desc.digest.slice(7)}`); consumed += raw.length;
      if (raw.length !== desc.size || `sha256:${sha(raw)}` !== desc.digest) throw new Error('archive descriptor bytes differ');
      const body = decode(raw); cache.set(desc.digest, { size: desc.size, type: desc.mediaType, body });
      audit.graph.push({ digest: desc.digest, size: desc.size, mediaType: desc.mediaType });
      return body;
    };
    const outerBytes = await read('index.json'), outer = decode(outerBytes);
    const find = async (index, depth = 0) => {
      if (depth > 8 || index.schemaVersion !== 2 || !Array.isArray(index.manifests) || index.manifests.length > 128) throw new Error('invalid archive index');
      const matches = [];
      for (const desc of index.manifests) {
        if (desc.digest === descriptor.digest) {
          if (desc.mediaType !== descriptor.mediaType || desc.size !== descriptor.size) throw new Error('built descriptor metadata differs');
          matches.push({ desc, body: await blob(desc) });
        } else if (indexTypes.has(desc.mediaType)) matches.push(...await find(await blob(desc), depth + 1));
      }
      return matches;
    };
    const roots = `sha256:${sha(outerBytes)}` === descriptor.digest
      ? [{ desc: descriptor, body: outer }] : await find(outer);
    if (roots.length !== 1) throw new Error('built descriptor absent or ambiguous in archive');
    if (`sha256:${sha(outerBytes)}` === descriptor.digest && outerBytes.length !== descriptor.size) throw new Error('built index size differs');
    const leaves = async (desc, body, depth = 0) => {
      if (depth > 8 || body.schemaVersion !== 2) throw new Error('invalid platform manifest graph');
      if (imageTypes.has(desc.mediaType)) return [{ desc, body }];
      if (!indexTypes.has(desc.mediaType) || !Array.isArray(body.manifests) || body.manifests.length > 128) throw new Error('invalid platform index');
      const result = [];
      for (const next of body.manifests) result.push(...await leaves(next, await blob(next), depth + 1));
      return result;
    };
    const candidates = (await leaves(roots[0].desc, roots[0].body)).filter(value => value.body.config?.digest === configDigest);
    if (candidates.length !== 1) throw new Error('built image does not identify exactly one archive config');
    const selected = candidates[0];
    if (selected.body.config.size !== bytes.length) throw new Error('platform config size differs');
    if (!Array.isArray(selected.body.layers) || selected.body.layers.length !== manifest[0].Layers?.length) throw new Error('platform layers differ from Docker-save view');
    audit.layers = selected.body.layers.map((layer, i) => {
      if (!/^sha256:[a-f0-9]{64}$/.test(layer.digest) || !Number.isSafeInteger(layer.size) || layer.size < 0
        || typeof manifest[0].Layers[i] !== 'string' || manifest[0].Layers[i].startsWith('/') || manifest[0].Layers[i].split('/').includes('..')) throw new Error('invalid archive layer mapping');
      return { descriptor: layer, dockerSavePath: manifest[0].Layers[i], diffId: parsed.rootfs?.diff_ids?.[i] };
    });
    audit.mapping = 'built-descriptor-to-platform-config';
    audit.archiveRootDigest = descriptor.digest;
    audit.platformManifestDigest = selected.desc.digest;
  }
  audit.verified = true; await report(audit);
  return { config: parsed, configDigest, identity: audit };
}

async function docker(args, env, log, capture = false) {
  return new Promise((ok, fail) => {
    const stream = createWriteStream(log, { mode: 0o600 }), child = spawn('docker', args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
    const chunks = []; let count = 0;
    child.stdout.pipe(stream, { end: false }); child.stderr.pipe(stream, { end: false });
    if (capture) child.stdout.on('data', part => {
      count += part.length;
      if (count > 4 * 1024 * 1024) child.kill('SIGTERM');
      else chunks.push(part);
    });
    child.on('error', error => { stream.end(); fail(error); }); stream.on('error', error => { child.kill('SIGTERM'); fail(error); });
    child.on('close', code => stream.end(() => code === 0 && count <= 4 * 1024 * 1024 ? ok(Buffer.concat(chunks).toString('utf8')) : fail(new Error(`docker ${args[0]} failed; private log ${log}`))));
  });
}

export function validateHosted(env) {
  if (env.GITHUB_ACTIONS !== 'true' || env.RUNNER_ENVIRONMENT !== 'github-hosted' || !env.RUNNER_TEMP) {
    throw new Error('fresh Docker builds require the hosted CI lane; no arch3 daemon workaround');
  }
}

export async function exportImages(name, directory) {
  if (!/^gonvex-cf-dev-[a-z0-9-]+$/.test(name ?? '') || name.length > 63) throw new Error('development image prefix/name required');
  validateHosted(process.env);
  directory = resolve(directory); await mkdir(directory, { recursive: true, mode: 0o700 });
  const within = relative(await realpath(process.env.RUNNER_TEMP), await realpath(directory));
  if (!within || within.startsWith('..') || within.startsWith('/')) throw new Error('output must be inside RUNNER_TEMP');
  const env = dockerEnvironment(process.env, directory), startedAt = new Date().toISOString();
  await mkdir(env.DOCKER_CONFIG, { mode: 0o700 });
  try {
    const context = await prepareContext(ROOT, directory, process.env.EXPECTED_SOURCE_SHA);
    const tools = { docker: (await docker(['version', '--format', '{{.Client.Version}}'], env, join(directory, 'docker-version.log'), true)).trim(),
      buildx: (await docker(['buildx', 'version'], env, join(directory, 'buildx-version.log'), true)).trim() };
    const before = await statfs(directory), receipts = []; let total = 0;
    for (const [kind, path] of RECIPES) {
      const pinned = await pinRecipe(await readFile(join(context.context, path), 'utf8'), async image => JSON.parse((await docker(
        ['buildx', 'imagetools', 'inspect', image, '--format', '{{json .Manifest.Digest}}'], env, join(directory, `${kind}-base-${sha(image)}.log`), true)).trim()));
      const recipe = join(directory, `${kind}.Dockerfile`), metadata = join(directory, `${kind}.metadata.json`), tag = `${name}-${kind}:${context.source}`;
      await writeFile(recipe, pinned.recipe);
      await docker(['buildx', 'build', '--load', '--platform', 'linux/amd64', '--provenance=mode=max', '--metadata-file', metadata,
        '--label', `org.opencontainers.image.revision=${context.source}`, '--file', recipe, '--tag', tag, context.context], env, join(directory, `${kind}-build.log`));
      const [image] = JSON.parse(await docker(['image', 'inspect', tag], env, join(directory, `${kind}-inspect.log`), true));
      const file = `${kind}.docker.tar`, archive = join(directory, file);
      await docker(['image', 'save', '--output', archive, tag], env, join(directory, `${kind}-export.log`));
      const bytes = (await stat(archive)).size; total = archiveBudget(bytes, total);
      const build = JSON.parse(await readFile(metadata, 'utf8'));
      if (image.Descriptor && build['containerimage.descriptor'] && ['digest', 'mediaType', 'size'].some(key => image.Descriptor[key] !== build['containerimage.descriptor'][key])) throw new Error('built/inspected descriptors differ');
      const verified = await verifyArchive(archive, tag, { id: image.Id, descriptor: image.Descriptor ?? build['containerimage.descriptor'],
        configDigest: build['containerimage.config.digest'], diffIds: image.RootFS?.Layers }, audit => json(join(directory, `${kind}.archive.json`), audit));
      if (verified.config.config?.Labels?.['org.opencontainers.image.revision'] !== context.source) throw new Error('image source label differs');
      receipts.push({ kind, source: context.source, dockerfile: path, pinnedDockerfileSha256: sha(pinned.recipe), bases: pinned.bases,
        imageId: image.Id, configDigest: verified.configDigest, identity: verified.identity, buildMetadataSha256: await fileHash(metadata),
        archive: { format: 'docker-save-v1', file, bytes, sha256: await fileHash(archive), tag } });
    }
    const after = await statfs(directory);
    const result = { source: context.source, expectedSource: process.env.EXPECTED_SOURCE_SHA, context, images: {}, receipts, tools,
      github: { repository: process.env.GITHUB_REPOSITORY, runId: process.env.GITHUB_RUN_ID, attempt: process.env.GITHUB_RUN_ATTEMPT,
        workflowSha: process.env.GITHUB_WORKFLOW_SHA, eventMergeSha: process.env.GITHUB_SHA },
      startedAt, finishedAt: new Date().toISOString(), archiveBytes: total,
      diskFreeBefore: before.bavail * before.bsize, diskFreeAfter: after.bavail * after.bsize,
      proof: 'credential-free archives only; not published or deployed; not final K22 runtime evidence' };
    await json(join(directory, 'images.json'), result);
    return result;
  } catch (error) {
    await json(join(directory, 'failure.json'), { failedAt: new Date().toISOString(), reason: String(error.message).slice(0, 2048) });
    throw error;
  } finally { await rm(env.DOCKER_CONFIG, { recursive: true, force: true }); }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [name, directory, ...extra] = process.argv.slice(2);
  if (!directory || extra.length) throw new Error('usage: export-container-images.mjs development-name output-directory');
  exportImages(name, directory).then(result => console.log(JSON.stringify({ source: result.source, archiveBytes: result.archiveBytes, images: result.receipts.length })))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}
