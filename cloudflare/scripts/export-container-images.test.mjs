import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { prepareContext, pinRecipe, dockerEnvironment, archiveBudget, verifyArchive, validateHosted, exportImages } from './export-container-images.mjs';
const exec = promisify(execFile), hash = text => createHash('sha256').update(text).digest('hex');
async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'gonvex-image-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}
async function repository(t) {
  const dir = await fixture(t), root = join(dir, 'repo'), out = join(dir, 'out');
  await mkdir(root); await mkdir(out);
  const git = async (...args) => (await exec('git', args, { cwd: root })).stdout.trim();
  await git('init', '-q'); await git('config', 'user.name', 'Image fixture'); await git('config', 'user.email', 'fixture@example.invalid');
  await writeFile(join(root, 'source.txt'), 'committed source'); await symlink('source.txt', join(root, 'link'));
  await git('add', '.'); await git('commit', '-qm', 'fixture');
  return { root, out, source: await git('rev-parse', 'HEAD') };
}
test('archive contains committed bytes and link identity, excluding untracked files and .git', async t => {
  const f = await repository(t); await writeFile(join(f.root, 'untracked.txt'), 'not a build input');
  const built = await prepareContext(f.root, f.out, f.source);
  const manifest = JSON.parse(await readFile(join(f.out, 'inputs.json'), 'utf8'));
  assert.equal(await readFile(join(built.context, 'source.txt'), 'utf8'), 'committed source');
  assert.deepEqual(manifest.inputs.map(i => [i.path, i.sha256]), [['link', hash('source.txt')], ['source.txt', hash('committed source')]]);
  assert.equal(manifest.tracked.length, 2); assert.equal(manifest.source, f.source);
  await assert.rejects(access(join(built.context, 'untracked.txt'))); await assert.rejects(access(join(built.context, '.git')));
  await assert.rejects(prepareContext(f.root, f.out, f.source), /EEXIST/);
});
test('foreign head and dirty tracked source refuse before producing context', async t => {
  const f = await repository(t);
  await assert.rejects(prepareContext(f.root, f.out, '0'.repeat(40)), /differs/);
  await writeFile(join(f.root, 'source.txt'), 'changed');
  await assert.rejects(prepareContext(f.root, f.out, f.source), /dirty/);
  await assert.rejects(access(join(f.out, 'context')));
});
test('frontend and every external FROM are pinned, retaining stage and COPY semantics', async () => {
  const d = `sha256:${'a'.repeat(64)}`, seen = [];
  const result = await pinRecipe('# syntax=docker/dockerfile:1\nFROM rust:1 AS build\nCOPY --from=build /out /out\nFROM node:22\n', async ref => { seen.push(ref); return d; });
  assert.deepEqual(seen, ['docker/dockerfile:1', 'rust:1', 'node:22']);
  assert.match(result.recipe, new RegExp(`FROM rust:1@${d} AS build`));
  assert.match(result.recipe, /COPY --from=build \/out \/out/); assert.equal(result.bases.length, 3);
  await assert.rejects(pinRecipe('FROM node:22\n', async () => 'latest'), /immutable/);
});
test('Docker cannot inherit CI, Vault, Cloudflare, SSH or npm credential variables', () => {
  const env = dockerEnvironment({ PATH: '/bin', HOME: '/runner', RUNNER_TEMP: '/tmp/runner', GITHUB_TOKEN: 'fixture',
    CLOUDFLARE_API_TOKEN: 'fixture', WHAGONS_CREDENTIALS_FILE: 'fixture', SSH_AUTH_SOCK: 'fixture', NPM_TOKEN: 'fixture' }, '/private');
  assert.deepEqual(Object.keys(env).sort(), ['DOCKER_BUILDKIT', 'DOCKER_CONFIG', 'HOME', 'PATH', 'TMPDIR']);
  assert.equal(env.DOCKER_CONFIG, '/private/docker-auth');
  assert.throws(() => validateHosted({ GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'self-hosted', RUNNER_TEMP: '/tmp' }), /hosted CI/);
  assert.throws(() => validateHosted({}), /hosted CI/);
  validateHosted({ GITHUB_ACTIONS: 'true', RUNNER_ENVIRONMENT: 'github-hosted', RUNNER_TEMP: '/tmp' });
});
test('export admission rejects non-development names and archive excesses', async () => {
  await assert.rejects(exportImages('production-image', '/unused'), /development/);
  assert.equal(archiveBudget(8 * 1024 ** 3, 0), 8 * 1024 ** 3);
  assert.equal(archiveBudget(4 * 1024 ** 3, 8 * 1024 ** 3), 12 * 1024 ** 3);
  for (const [bytes, prior] of [[8 * 1024 ** 3 + 1, 0], [5 * 1024 ** 3, 8 * 1024 ** 3], [0, 0], [1, NaN]]) assert.throws(() => archiveBudget(bytes, prior), /envelope/);
});
test('Docker-save manifest/config tampering and foreign tag fail before publication', async t => {
  const dir = await fixture(t), tag = 'gonvex-cf-dev-test:fixture';
  const config = JSON.stringify({ architecture: 'amd64', os: 'linux', config: { Labels: {} } }), digest = hash(config), file = `${digest}.json`;
  await writeFile(join(dir, file), config);
  const pack = async entry => { await writeFile(join(dir, 'manifest.json'), JSON.stringify([entry])); await exec('tar', ['-cf', join(dir, 'image.tar'), 'manifest.json', file], { cwd: dir }); };
  await pack({ Config: file, RepoTags: [tag], Layers: [] });
  assert.equal((await verifyArchive(join(dir, 'image.tar'), tag, `sha256:${digest}`)).config.os, 'linux');
  await assert.rejects(verifyArchive(join(dir, 'image.tar'), tag, `sha256:${'0'.repeat(64)}`), /differs/);
  await assert.rejects(verifyArchive(join(dir, 'image.tar'), 'foreign:tag', `sha256:${digest}`), /tag/);
  await writeFile(join(dir, file), config.replace('"Labels":{}', '"Labels":{"changed":"true"}'));
  await pack({ Config: file, RepoTags: [tag], Layers: [] });
  await assert.rejects(verifyArchive(join(dir, 'image.tar'), tag, `sha256:${digest}`), /differs/);
  await pack({ Config: '../outside', RepoTags: [tag], Layers: [] });
  await assert.rejects(verifyArchive(join(dir, 'image.tar'), tag, `sha256:${digest}`), /config path/);
});

// A real OCI-layout + Docker-save compatibility archive: containerd-backed
// Docker 29 returns this index Id, NOT the selected platform config digest.
async function ociFixture(t) {
  const dir = await fixture(t), tag = 'gonvex-cf-dev-oci:fixture';
  await mkdir(join(dir, 'blobs/sha256'), { recursive: true });
  const put = async (body, mediaType) => {
    const text = JSON.stringify(body), digest = hash(text);
    await writeFile(join(dir, 'blobs/sha256', digest), text);
    return { mediaType, digest: `sha256:${digest}`, size: Buffer.byteLength(text) };
  };
  const config = await put({ architecture: 'amd64', os: 'linux', config: { Labels: { 'org.opencontainers.image.revision': 'fixture' } }, rootfs: { type: 'layers', diff_ids: [] } }, 'application/vnd.oci.image.config.v1+json');
  const manifest = await put({ schemaVersion: 2, mediaType: 'application/vnd.oci.image.manifest.v1+json', config, layers: [] }, 'application/vnd.oci.image.manifest.v1+json');
  const attestConfig = await put({ architecture: 'unknown', os: 'unknown', rootfs: { type: 'layers', diff_ids: [] } }, 'application/vnd.oci.image.config.v1+json');
  const attestation = await put({ schemaVersion: 2, mediaType: 'application/vnd.oci.image.manifest.v1+json', config: attestConfig, layers: [] }, 'application/vnd.oci.image.manifest.v1+json');
  const index = await put({ schemaVersion: 2, mediaType: 'application/vnd.oci.image.index.v1+json', manifests: [{ ...manifest, platform: { os: 'linux', architecture: 'amd64' } }, { ...attestation, platform: { os: 'unknown', architecture: 'unknown' } }] }, 'application/vnd.oci.image.index.v1+json');
  await writeFile(join(dir, 'oci-layout'), '{"imageLayoutVersion":"1.0.0"}');
  await writeFile(join(dir, 'index.json'), JSON.stringify({ schemaVersion: 2, manifests: [index] }));
  await writeFile(join(dir, 'manifest.json'), JSON.stringify([{ Config: `blobs/sha256/${config.digest.slice(7)}`, RepoTags: [tag], Layers: [] }]));
  const archive = join(dir, 'modern.tar');
  const pack = () => exec('tar', ['-cf', archive, 'oci-layout', 'index.json', 'manifest.json', 'blobs'], { cwd: dir });
  await pack();
  return { dir, archive, tag, config, manifest, index, pack, put, expected: { id: index.digest, descriptor: index, diffIds: [] } };
}
test('Docker 29 index Id maps through verified OCI descriptors to the archive config', async t => {
  const f = await ociFixture(t); assert.notEqual(f.index.digest, f.config.digest);
  const result = await verifyArchive(f.archive, f.tag, f.expected);
  assert.equal(result.configDigest, f.config.digest);
  assert.equal(result.identity.mapping, 'built-descriptor-to-platform-config');
  assert.equal(result.identity.archiveRootDigest, f.index.digest);
  assert.equal(result.identity.platformManifestDigest, f.manifest.digest);
  assert.equal(result.identity.verified, true);
  if (process.env.GONVEX_IMAGE_FIXTURE_OUTPUT) {
    const destination = process.env.GONVEX_IMAGE_FIXTURE_OUTPUT;
    await mkdir(destination, { recursive: true });
    await exec('cp', [f.archive, join(destination, 'docker29-oci-save.tar')]);
    await writeFile(join(destination, 'expected.json'), JSON.stringify({ ...f.expected, configDigest: f.config.digest }));
  }
});
test('matching tag and source labels cannot excuse a foreign built index', async t => {
  const f = await ociFixture(t);
  await assert.rejects(verifyArchive(f.archive, f.tag, { ...f.expected, id: `sha256:${'a'.repeat(64)}`, descriptor: { ...f.index, digest: `sha256:${'a'.repeat(64)}` } }), /absent/);
});
test('modified OCI descriptor/config bytes, conflicting config and rootfs are rejected', async t => {
  const f = await ociFixture(t);
  await assert.rejects(verifyArchive(f.archive, f.tag, { ...f.expected, configDigest: `sha256:${'b'.repeat(64)}` }), /config digest differs/);
  await assert.rejects(verifyArchive(f.archive, f.tag, { ...f.expected, diffIds: ['foreign'] }), /rootfs differs/);
  await writeFile(join(f.dir, 'blobs/sha256', f.manifest.digest.slice(7)), '{"schemaVersion":2}'); await f.pack();
  await assert.rejects(verifyArchive(f.archive, f.tag, f.expected), /descriptor bytes differ/);
});
test('OCI wrapper cannot duplicate the built root or hide modified config bytes', async t => {
  const f = await ociFixture(t);
  await writeFile(join(f.dir, 'index.json'), JSON.stringify({ schemaVersion: 2, manifests: [f.index, f.index] })); await f.pack();
  await assert.rejects(verifyArchive(f.archive, f.tag, f.expected), /ambiguous/);
  await writeFile(join(f.dir, 'blobs/sha256', f.config.digest.slice(7)), '{"architecture":"amd64","os":"linux"}'); await f.pack();
  await assert.rejects(verifyArchive(f.archive, f.tag, f.expected), /filename differs/);
});
test('valid foreign config with the same tag/source label cannot bypass the built descriptor', async t => {
  const f = await ociFixture(t);
  const foreign = await f.put({ architecture: 'amd64', os: 'linux', config: { Env: ['MODIFIED=1'], Labels: { 'org.opencontainers.image.revision': 'fixture' } }, rootfs: { type: 'layers', diff_ids: [] } }, 'application/vnd.oci.image.config.v1+json');
  await writeFile(join(f.dir, 'manifest.json'), JSON.stringify([{ Config: `blobs/sha256/${foreign.digest.slice(7)}`, RepoTags: [f.tag], Layers: [] }])); await f.pack();
  await assert.rejects(verifyArchive(f.archive, f.tag, f.expected), /exactly one archive config/);
});
