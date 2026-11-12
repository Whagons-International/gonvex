// Development-only composition deploy. Run solely under the approved Vault
// wrapper (or CI's existing Cloudflare secrets); never print credential values.
import { spawn } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, randomBytes } from 'node:crypto';
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
const load = async (file) => JSON.parse((await readFile(file, 'utf8')).replace(/^\s*\/\/.*$/gm, ''));
export const developmentName = (name) => {
  if (!/^gonvex-cf-dev-[a-z0-9-]+$/.test(name) || name.length > 54) throw new Error('development name must start gonvex-cf-dev- and fit all companion names (54 characters)');
  return name;
};
export async function cloudflareCredentials() {
  const credential = process.env.WHAGONS_CREDENTIAL_FILE ? JSON.parse(await readFile(process.env.WHAGONS_CREDENTIAL_FILE, 'utf8'))
    : { accountId: process.env.CLOUDFLARE_ACCOUNT_ID, apiToken: process.env.CLOUDFLARE_API_TOKEN };
  if (!credential.accountId || !credential.apiToken) throw new Error('approved Cloudflare credential wrapper required');
  return credential;
}
export function accountApi(credentials) {
  return async (path, { method = 'GET', body, missing = false } = {}) => {
    const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${credentials.accountId}${path}`, {
      method, headers: { authorization: `Bearer ${credentials.apiToken}`, 'content-type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (missing && response.status === 404) return null;
    const reply = await response.json();
    if (!response.ok || !reply.success) {
      if (missing && reply.errors?.some((e) => [10007, 10006, 10004].includes(e.code))) return null;
      throw new Error(`Cloudflare ${method} ${path} failed HTTP ${response.status}, codes ${(reply.errors ?? []).map((e) => e.code).join(',')}`);
    }
    return reply.result;
  };
}

export async function servingConfigs(name, directory, { kernelUrl, images = {} } = {}) {
  developmentName(name); await mkdir(directory, { recursive: true, mode: 0o700 });
  const configs = {};
  for (const [kind, file] of [['kernel', join(root, 'wrangler.jsonc')], ...['actions', 'sandbox', 'builder'].map((kind) => [kind, join(here, `${kind}.wrangler.jsonc`)])]) {
    const config = await load(file), dir = dirname(file);
    config.name = kind === 'kernel' ? name : `${name}-${kind}`;
    config.main = resolve(dir, config.main);
    config.alias = Object.fromEntries(Object.entries(config.alias ?? {}).map(([key, value]) => [key, resolve(dir, value)]));
    for (const binding of config.durable_objects?.bindings ?? []) if (binding.script_name) binding.script_name = name;
    for (const service of config.services ?? []) service.service = `${name}-${service.binding === 'RELEASE_BUILDER' ? 'builder' : service.binding.toLowerCase()}`;
    for (const workflow of config.workflows ?? []) { workflow.name = `${name}-actions`; if (workflow.script_name) workflow.script_name = `${name}-actions`; }
    for (const bucket of config.r2_buckets ?? []) bucket.bucket_name = `${name}-${bucket.binding.toLowerCase()}`;
    for (const queue of [...(config.queues?.producers ?? []), ...(config.queues?.consumers ?? [])]) queue.queue = `${name}-rollout`;
    for (const dataset of config.analytics_engine_datasets ?? []) dataset.dataset = `${name}-telemetry`;
    for (const container of config.containers ?? []) {
      if (images[kind]) container.image = images[kind];
      else { container.image = resolve(dir, container.image); container.image_build_context = resolve(dir, container.image_build_context); }
    }
    config.vars = { ...config.vars, GONVEX_STORAGE_BUCKET: `${name}-files`, ...(kernelUrl ? { GONVEX_PUBLIC_URL: kernelUrl } : {}) };
    if (kind === 'builder') Object.assign(config.vars, { GONVEX_KERNEL_SCRIPT: name, GONVEX_KERNEL_URL: kernelUrl });
    configs[kind] = { config, path: join(directory, `${kind}.json`) };
  }
  for (const { config, path } of Object.values(configs)) await writeFile(path, JSON.stringify(config), { mode: 0o600 });
  const bootstrap = { ...configs.kernel.config, services: [], workflows: [] };
  configs.bootstrap = { config: bootstrap, path: join(directory, 'bootstrap.json') };
  await writeFile(configs.bootstrap.path, JSON.stringify(bootstrap), { mode: 0o600 });
  return configs;
}

async function wrangler(args, credentials, directory) {
  const env = { ...process.env, CLOUDFLARE_ACCOUNT_ID: credentials.accountId, CLOUDFLARE_API_TOKEN: credentials.apiToken,
    WRANGLER_SEND_METRICS: 'false', CI: '1', TMPDIR: join(directory, 'tmp') };
  delete env.WHAGONS_CREDENTIAL_FILE;
  await mkdir(env.TMPDIR, { recursive: true });
  return new Promise((ok, fail) => {
    const child = spawn(join(root, 'node_modules/.bin/wrangler'), args, { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (part) => { output += part; }); child.stderr.on('data', (part) => { output += part; });
    child.on('error', fail);
    child.on('exit', async (code) => {
      // Keep raw deployment diagnostics private. API/credential failure text can include context.
      await writeFile(join(directory, `wrangler-${Date.now()}.log`), output, { mode: 0o600 });
      code === 0 ? ok(output) : fail(new Error(`wrangler ${args[0]} exited ${code}; private diagnostics in ${directory}`));
    });
  });
}

export async function deployAssembly({ name, directory, images, secrets = {} }) {
  developmentName(name); directory = resolve(directory);
  const credentials = await cloudflareCredentials(), api = accountApi(credentials);
  // Require explicit immutable Container identities. Build is a separate admitted phase.
  for (const kind of ['sandbox', 'builder']) if (!images?.[kind]?.includes('@sha256:')) throw new Error(`immutable ${kind} Container image digest required before deployment`);
  const subdomain = await api('/workers/subdomain');
  const url = `https://${name}.${subdomain.subdomain}.workers.dev`;
  const configs = await servingConfigs(name, directory, { images, kernelUrl: url });
  const resources = [];
  for (const suffix of ['releases', 'files']) {
    const bucket = `${name}-${suffix}`;
    if (!await api(`/r2/buckets/${bucket}`, { missing: true })) await api('/r2/buckets', { method: 'POST', body: { name: bucket } });
    resources.push({ kind: 'r2', name: bucket });
  }
  const queueName = `${name}-rollout`;
  const queues = await api(`/queues?name=${encodeURIComponent(queueName)}`);
  const queue = queues.find((q) => q.queue_name === queueName) ?? await api('/queues', { method: 'POST', body: { queue_name: queueName } });
  resources.push({ kind: 'queue', name: queueName, id: queue.queue_id });
  const existing = await api(`/workers/scripts/${name}/settings`, { missing: true });
  if (!existing) await wrangler(['deploy', '--config', configs.bootstrap.path], credentials, directory);
  const token = randomBytes(32).toString('hex');
  const storageSecret = randomBytes(32).toString('hex');
  const rootSecrets = { GONVEX_STORAGE_SECRET: storageSecret, ...secrets, GONVEX_RELEASE_BUILDER_TOKEN: token };
  for (const [key, value] of Object.entries(rootSecrets)) await api(`/workers/scripts/${name}/secrets`, { method: 'PUT', body: { name: key, text: value, type: 'secret_text' } });
  for (const kind of ['sandbox', 'actions', 'builder']) await wrangler(['deploy', '--config', configs[kind].path, '--keep-vars'], credentials, directory);
  for (const kind of ['actions', 'sandbox']) await api(`/workers/scripts/${name}-${kind}/secrets`, { method: 'PUT', body: { name: 'GONVEX_STORAGE_SECRET', text: rootSecrets.GONVEX_STORAGE_SECRET, type: 'secret_text' } });
  const builderSecrets = { GONVEX_RELEASE_BUILDER_TOKEN: token, CLOUDFLARE_ACCOUNT_ID: credentials.accountId, CLOUDFLARE_API_TOKEN: credentials.apiToken };
  for (const [key, value] of Object.entries(builderSecrets)) await api(`/workers/scripts/${name}-builder/secrets`, { method: 'PUT', body: { name: key, text: value, type: 'secret_text' } });
  await wrangler(['deploy', '--config', configs.kernel.path, '--keep-vars'], credentials, directory);
  const workers = [];
  for (const kind of ['kernel', 'actions', 'sandbox', 'builder']) {
    const script = configs[kind].config.name;
    const deployments = await api(`/workers/scripts/${script}/deployments`);
    const settings = await api(`/workers/scripts/${script}/settings`);
    workers.push({ kind, name: script, deployments, configHash: createHash('sha256').update(await readFile(configs[kind].path)).digest('hex'),
      bindings: settings.bindings?.map(({ name, type, class_name, script_name, service, dataset, bucket_name }) => ({ name, type, class_name, script_name, service, dataset, bucket_name })), limits: settings.limits });
  }
  const evidence = { name, url, at: new Date().toISOString(), images, resources, workers };
  await writeFile(join(directory, 'deployment.json'), JSON.stringify(evidence, null, 2));
  return evidence;
}
