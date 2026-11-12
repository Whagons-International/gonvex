#!/usr/bin/env node
// The K19 release builder as an HTTP server: the same process runs in a
// Container on the platform (Dockerfile) and as a local Node process for the
// local kernel (router/dev/local/start.mjs). Node 22.18+ (TypeScript type
// stripping loads the shared builder core directly).
//
//   POST /inspect  {incoming, headerProject}   -> Inspection
//   POST /prepare  {incoming, project}         -> {ok, summary} | {ok:false, status, error}
//   POST /activate {releases}                  -> {ok, version} | {ok:false, error}
//   POST /deploy   {version}                   -> {ok} | {ok:false, error}
//   GET  /healthz
//
// Every request needs `authorization: Bearer $GONVEX_RELEASE_BUILDER_TOKEN`.
//
// Environment:
//   GONVEX_RELEASE_BUILDER_TOKEN   shared secret with the kernel (required)
//   GONVEX_KERNEL_URL              the kernel's router, for the release bucket (its /_gonvex/k19/blobs route)
//   GONVEX_BUILDER_PORT            listen port (default 8790); GONVEX_BUILDER_HOST (default 127.0.0.1, 0.0.0.0 in a Container)
//   GONVEX_BUILDER_TARGET          "local" (default) or "wrangler"
//   GONVEX_KERNEL_GENERATED_DIR    local: the directory the kernel's `gonvex:releases` alias points into
//   GONVEX_KERNEL_STAGING_DIR      local: where release modules are staged (default: the generated dir). Keep it
//                                  outside the tree `wrangler dev` watches, or staging reloads workerd mid-sync.
//   GONVEX_DEPLOY_DEFER_MS         local: delay before the swap that reloads workerd (default 1500)
//   GONVEX_KERNEL_WRANGLER_CONFIG  wrangler: the kernel's wrangler config; GONVEX_KERNEL_SCRIPT its name
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { timingSafeEqual } from 'node:crypto';
import { PRELUDE_SPECIFIER, ReleaseBuilderCore, RELEASES_ENTRY, ACTION_RELEASES_ENTRY } from './core.ts';

const here = dirname(fileURLToPath(import.meta.url));

const token = process.env.GONVEX_RELEASE_BUILDER_TOKEN ?? '';
if (!token) {
  console.error('GONVEX_RELEASE_BUILDER_TOKEN is required');
  process.exit(2);
}
const kernelUrl = (process.env.GONVEX_KERNEL_URL ?? '').replace(/\/+$/, '');
if (!kernelUrl) {
  console.error('GONVEX_KERNEL_URL is required');
  process.exit(2);
}
const port = Number(process.env.GONVEX_BUILDER_PORT ?? '8790');
const targetKind = process.env.GONVEX_BUILDER_TARGET ?? 'local';
const log = (message) => console.log(`[release-builder] ${new Date().toISOString()} ${message}`);

/** The release bucket through the kernel's blob route (a Container cannot bind R2). */
class HttpBlobStore {
  constructor(base, secret) {
    this.base = base;
    this.secret = secret;
  }
  url(key) {
    return `${this.base}/_gonvex/k19/blobs/${encodeURIComponent(key)}`;
  }
  async get(key) {
    const response = await fetch(this.url(key), { headers: { authorization: `Bearer ${this.secret}` } });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`blob GET ${key}: HTTP ${response.status}`);
    const text = await response.text();
    return { text: async () => text };
  }
  async put(key, value) {
    const response = await fetch(this.url(key), { method: 'PUT', headers: { authorization: `Bearer ${this.secret}` }, body: value });
    if (!response.ok) throw new Error(`blob PUT ${key}: HTTP ${response.status}`);
  }
  async delete(key) {
    await fetch(this.url(key), { method: 'DELETE', headers: { authorization: `Bearer ${this.secret}` } });
  }
}

/**
 * Local target: files go under the kernel's generated directory. Staging
 * writes the release modules (nothing imports them yet, so workerd does not
 * reload); deploy rewrites `releases.js`, the module the kernel imports, which
 * makes `wrangler dev` rebuild and restart workerd with the new version.
 */
class LocalTarget {
  constructor(dir, deferMs, stagingDir = dir) {
    this.dir = dir;
    this.stagingDir = stagingDir;
    this.deferMs = deferMs;
    this.staged = new Map();
  }
  async stage(files, version) {
    for (const [path, content] of files) {
      if (path === RELEASES_ENTRY || path === ACTION_RELEASES_ENTRY) continue;
      const file = join(this.stagingDir, path);
      await mkdir(dirname(file), { recursive: true });
      await writeFile(file, content);
    }
    // The entry imports `./releases/...`; point it at the staging directory.
    const base = relative(this.dir, this.stagingDir).split('\\').join('/') || '.';
    const entry = files.get(RELEASES_ENTRY).replaceAll('from "./releases/', `from "${base.startsWith('.') ? base : `./${base}`}/releases/`);
    const actions = files.get(ACTION_RELEASES_ENTRY)?.replaceAll('from "./releases/', `from "${base.startsWith('.') ? base : `./${base}`}/releases/`);
    this.staged.set(version, { kernel: entry, actions });
  }
  async deploy(version) {
    const entry = this.staged.get(version);
    if (entry === undefined) throw new Error(`version ${version} was not staged`);
    // workerd restarts on the swap and cuts every request in flight, including
    // the /dev/sync response; swap after it has gone out.
    setTimeout(async () => {
      const file = join(this.dir, RELEASES_ENTRY);
      if (entry.actions) {
        const actions = join(this.dir, ACTION_RELEASES_ENTRY);
        await writeFile(`${actions}.tmp`, entry.actions);
        await rename(`${actions}.tmp`, actions);
      }
      await writeFile(`${file}.tmp`, entry.kernel);
      await rename(`${file}.tmp`, file);
      log(`version ${version} is live (workerd reloads)`);
    }, this.deferMs);
  }
}

/** Platform target: `wrangler deploy` of the kernel with the generated releases (Container image has the kernel source). */
class WranglerTarget extends LocalTarget {
  constructor(dir, config, script) {
    super(dir, 0);
    this.config = config;
    this.script = script;
  }
  async deploy(version) {
    const entry = this.staged.get(version);
    if (entry === undefined) throw new Error(`version ${version} was not staged`);
    if (!this.script.startsWith('gonvex-cf-dev-')) throw new Error('development script name required');
    await writeFile(join(this.dir, RELEASES_ENTRY), entry.kernel);
    if (entry.actions) await writeFile(join(this.dir, ACTION_RELEASES_ENTRY), entry.actions);
    const load = async (path) => JSON.parse((await readFile(path, 'utf8')).replace(/^\s*\/\/.*$/gm, ''));
    const kernel = await load(this.config);
    const root = dirname(this.config);
    const actionPath = join(root, 'kernel/assembly/actions.wrangler.jsonc');
    const actions = await load(actionPath);
    for (const [config, dir, name, alias, generatedEntry] of [
      [actions, dirname(actionPath), `${this.script}-actions`, 'gonvex:action-releases', ACTION_RELEASES_ENTRY],
      [kernel, root, this.script, 'gonvex:releases', RELEASES_ENTRY],
    ]) {
      config.name = name;
      config.main = resolve(dir, config.main);
      config.alias = Object.fromEntries(Object.entries(config.alias ?? {}).map(([key, value]) => [key, resolve(dir, value)]));
      config.alias[alias] = join(this.dir, generatedEntry);
      for (const binding of config.durable_objects?.bindings ?? []) if (binding.script_name) binding.script_name = this.script;
      for (const service of config.services ?? []) service.service = service.binding === 'SANDBOX' ? `${this.script}-sandbox` : service.binding === 'ACTIONS' ? `${this.script}-actions` : `${this.script}-builder`;
      for (const workflow of config.workflows ?? []) { workflow.name = `${this.script}-actions`; if (workflow.script_name) workflow.script_name = `${this.script}-actions`; }
      for (const bucket of config.r2_buckets ?? []) bucket.bucket_name = `${this.script}-${bucket.binding.toLowerCase()}`;
      for (const q of [...(config.queues?.producers ?? []), ...(config.queues?.consumers ?? [])]) q.queue = `${this.script}-rollout`;
      for (const dataset of config.analytics_engine_datasets ?? []) dataset.dataset = `${this.script}-telemetry`;
      const path = join(this.dir, `${name}.json`);
      await writeFile(path, JSON.stringify(config));
      await new Promise((resolveDeploy, reject) => {
        const child = spawn(resolve(here, '../../../node_modules/.bin/wrangler'), ['deploy', '--config', path, '--keep-vars'], { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, WRANGLER_SEND_METRICS: 'false' } });
        // Raw Wrangler output can contain request data. Report only the exit status.
        child.stdout.resume(); child.stderr.resume();
        child.on('error', reject);
        child.on('exit', (code) => code === 0 ? resolveDeploy() : reject(new Error(`wrangler deploy exited ${code}`)));
      });
    }
    log(`version ${version} deployed`);
  }
}

const generatedDir = resolve(process.env.GONVEX_KERNEL_GENERATED_DIR ?? resolve(here, '../../../router/dev/local/.generated'));
const stagingDir = resolve(process.env.GONVEX_KERNEL_STAGING_DIR ?? generatedDir);
await mkdir(generatedDir, { recursive: true });
await mkdir(stagingDir, { recursive: true });
// Before the first sync the kernel must still build: an empty releases module.
const entryFile = join(generatedDir, RELEASES_ENTRY);
try {
  await readFile(entryFile);
} catch {
  await writeFile(entryFile, 'export const releases = {};\n');
}
const target =
  targetKind === 'wrangler'
    ? new WranglerTarget(generatedDir, process.env.GONVEX_KERNEL_WRANGLER_CONFIG ?? '', process.env.GONVEX_KERNEL_SCRIPT ?? '')
    : new LocalTarget(generatedDir, Number(process.env.GONVEX_DEPLOY_DEFER_MS ?? '1500'), stagingDir);

// app.js lives at <generated>/releases/<project>/<hash>/app.js: three levels below the generated dir.
const builder = new ReleaseBuilderCore({ blobs: new HttpBlobStore(kernelUrl, token), target, preludeSpecifier: PRELUDE_SPECIFIER, log });

const authorized = (request) => {
  const given = Buffer.from(request.headers.authorization ?? '');
  const expected = Buffer.from(`Bearer ${token}`);
  return given.length === expected.length && timingSafeEqual(given, expected);
};

async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
}

// One build at a time: a release build holds the parsed manifest and the transform in memory.
let queue = Promise.resolve();
const serial = (fn) => {
  const run = queue.then(fn, fn);
  queue = run.catch(() => undefined);
  return run;
};

createServer(async (request, response) => {
  const send = (status, body) => {
    response.writeHead(status, { 'content-type': 'application/json' });
    response.end(JSON.stringify(body));
  };
  if (request.method === 'GET' && request.url === '/healthz') return send(200, { ok: true, target: targetKind });
  if (!authorized(request)) return send(401, { error: 'release builder token required' });
  if (request.method !== 'POST') return send(405, { error: 'method not allowed' });
  try {
    const body = await readJson(request);
    const started = Date.now();
    let result;
    switch (request.url) {
      case '/inspect':
        result = await serial(() => builder.inspect(String(body.incoming), String(body.headerProject ?? '')));
        break;
      case '/prepare':
        result = await serial(() => builder.prepare(String(body.incoming), String(body.project)));
        break;
      case '/activate':
        result = await serial(() => builder.activate(body.releases ?? []));
        break;
      case '/deploy':
        result = await serial(() => builder.deploy(String(body.version)));
        break;
      default:
        return send(404, { error: 'not found' });
    }
    log(`${request.url} ${result.ok ? 'ok' : `failed: ${result.error}`} in ${Date.now() - started} ms (heap ${Math.round(process.memoryUsage().heapUsed / 1e6)} MB)`);
    send(200, result);
  } catch (cause) {
    log(`${request.url} crashed: ${cause?.stack ?? cause}`);
    send(500, { error: String(cause?.message ?? cause) });
  }
}).listen(port, process.env.GONVEX_BUILDER_HOST ?? '127.0.0.1', () => log(`listening on ${process.env.GONVEX_BUILDER_HOST ?? '127.0.0.1'}:${port} (target ${targetKind}, generated ${generatedDir})`));

process.on('SIGTERM', () => process.exit(0));
