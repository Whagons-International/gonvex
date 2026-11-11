#!/usr/bin/env node
// K17 done_when: the API gateway's consumer contract passes against the kernel.
//
// The gateway builds its function catalog from `GET /dev/manifest` with its
// own `gvx_svc_` credential (whagons-api-gateway `src/runtime/manifest.rs`,
// `src/catalog.rs`), and the app repository checks every function the gateway
// and the CLI call against a manifest (`contracts/consumers/*.json`,
// `scripts/lib/consumer-contracts.mjs`). This script deploys the app's
// committed release to both runtimes through `/dev/sync` (the CLI's request),
// reads `/dev/manifest` as the gateway principal and runs the app's own checker
// on that body:
//
//   - Rust: the reference runtime (`cloudflare/conformance` adapter).
//   - kernel: K19's local kernel (router `/dev/sync` → R2 release → builder),
//     whose `/dev/manifest` route answers service principals through
//     `kernel/principals/manifest.ts`.
//
// Pass: no contract errors on either runtime, the same warnings, and the same
// catalog (functions and module hash) from both. A principal without
// `manifest:true` gets 403 on both. The app files are read from git, never
// written (`git show <ref>:<path>`).
//
//   node kernel/principals/verify/gateway-contract.mjs --app <whagons5-client checkout> \
//     [--ref origin/staging] [--work <dir>] [--out kernel/principals/verify/results/gateway-contract.json]
import { execFileSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';

const here = dirname(fileURLToPath(import.meta.url));
const CLOUDFLARE = resolve(here, '../../..');
const flag = (name, fallback) => {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : fallback;
};
const app = resolve(flag('--app', ''));
const ref = flag('--ref', 'origin/staging');
const work = resolve(flag('--work', join(tmpdir(), `k17-gateway-contract-${process.pid}`)));
const out = flag('--out');
await mkdir(work, { recursive: true });

const git = (...args) => execFileSync('git', ['-C', app, ...args], { maxBuffer: 1 << 30 });
const commit = git('rev-parse', ref).toString().trim();
const show = (path) => git('show', `${ref}:${path}`);
const files = ['gonvex/_generated/manifest.json', 'contracts/consumers/whagons-api-gateway.json', 'contracts/consumers/whagons-cli.json', 'scripts/lib/consumer-contracts.mjs'];
for (const path of files) {
  await mkdir(dirname(join(work, 'app', path)), { recursive: true });
  await writeFile(join(work, 'app', path), show(path));
}
const { checkConsumerContract, validateConsumerContract } = await import(pathToFileURL(join(work, 'app/scripts/lib/consumer-contracts.mjs')).href);
const manifest = JSON.parse(await readFile(join(work, 'app/gonvex/_generated/manifest.json'), 'utf8'));
const contracts = [];
for (const name of ['whagons-api-gateway', 'whagons-cli']) {
  const path = `contracts/consumers/${name}.json`;
  contracts.push(validateConsumerContract(JSON.parse(await readFile(join(work, 'app', path), 'utf8')), path));
}

const token = () => `gvx_svc_${randomBytes(32).toString("hex")}`;
const GATEWAY = token();
const NO_MANIFEST = token();
const principals = (project) => JSON.stringify([
  // The gateway's production entry (docs/service-principals.md).
  { id: 'whagons-api-gateway', tokenSha256: createHash('sha256').update(GATEWAY).digest('hex'), projects: [project], functions: ['apiKeys.authenticate'], maxDelegationSeconds: 900, manifest: true },
  { id: 'no-manifest', tokenSha256: createHash('sha256').update(NO_MANIFEST).digest('hex'), projects: [project], functions: ['apiKeys.authenticate'] },
]);

async function http(url, { method = 'GET', headers = {}, body } = {}) {
  const response = await fetch(url, { method, headers, ...(body !== undefined ? { body } : {}) });
  const text = await response.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { raw: text.slice(0, 500) }; }
  return { status: response.status, json };
}

/** `gonvex dev --once`'s request (packages/gonvex `syncRuntime`), with the release retargeted to the new project. */
async function sync(base, project, key) {
  const body = JSON.stringify({ ...manifest, project });
  const r = await http(`${base}/dev/sync`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-gonvex-project-id': project, authorization: `Bearer ${key}`, 'x-gonvex-key': key }, body });
  if (r.status !== 200) throw new Error(`/dev/sync -> ${r.status}: ${JSON.stringify(r.json).slice(0, 500)}`);
  return r.json;
}

async function readCatalog(base, project) {
  const read = (credential) => http(`${base}/dev/manifest?project=${encodeURIComponent(project)}`, { headers: { authorization: `Bearer ${credential}` } });
  const allowed = await read(GATEWAY);
  const denied = await read(NO_MANIFEST);
  if (allowed.status !== 200) throw new Error(`/dev/manifest (gateway) -> ${allowed.status}: ${JSON.stringify(allowed.json)}`);
  const checks = contracts.map((contract) => ({ consumer: contract.consumer, functions: Object.keys(contract.functions).length, ...checkConsumerContract(allowed.json, contract) }));
  return {
    status: allowed.status,
    keys: Object.keys(allowed.json).sort(),
    functions: Object.keys(allowed.json.functions ?? {}).length,
    moduleHash: allowed.json.module?.hash,
    catalogSha256: createHash('sha256').update(JSON.stringify(allowed.json.functions)).digest('hex'),
    denied: { status: denied.status, body: denied.json },
    checks,
    body: allowed.json,
  };
}

const report = { app: { ref, commit }, startedAt: new Date().toISOString(), runtimes: {} };

// ---- Rust (reference) ------------------------------------------------------------------
{
  const { RustBackend } = await import(pathToFileURL(join(CLOUDFLARE, 'conformance/src/backends/rust.mjs')).href);
  const rust = new RustBackend({ name: 'rust', workDir: join(work, 'rust') });
  await mkdir(rust.workDir, { recursive: true });
  try {
    await rust.start();
    const { projectId, projectKey } = await rust.createProject('whagons-gateway-contract');
    await rust.configureServicePrincipals(JSON.parse(principals(projectId)));
    const started = Date.now();
    await sync(rust.httpUrl, projectId, projectKey);
    report.runtimes.rust = { syncMs: Date.now() - started, ...(await readCatalog(rust.httpUrl, projectId)) };
  } finally {
    await rust.stop();
  }
}

// ---- kernel (K19 local kernel, K17 manifest access) ---------------------------------------------
{
  const { startLocalKernel } = await import(pathToFileURL(join(CLOUDFLARE, 'router/dev/local/start.mjs')).href);
  const state = join(work, 'kernel-state');
  const vars = { GONVEX_SYNC_DEADLINE_MS: '1200000' };
  let kernel = await startLocalKernel({ state, logs: join(work, 'kernel-logs'), vars });
  let project;
  try {
    const created = await http(`${kernel.url}/dev/projects`, { method: 'POST', headers: { authorization: `Bearer ${kernel.adminKey}`, 'content-type': 'application/json' }, body: JSON.stringify({ name: 'whagons-gateway-contract', databaseMode: 'multiTenant' }) });
    if (created.status !== 200 && created.status !== 201) throw new Error(`/dev/projects -> ${created.status}: ${JSON.stringify(created.json)}`);
    project = { id: created.json.project.id, key: created.json.projectKey };
  } finally {
    await kernel.stop();
  }
  // GONVEX_SERVICE_PRINCIPALS is read when the control object starts.
  kernel = await startLocalKernel({ state, logs: join(work, 'kernel-logs-2'), vars: { ...vars, GONVEX_ADMIN_KEY: kernel.adminKey, GONVEX_SERVICE_PRINCIPALS: principals(project.id) } });
  try {
    const started = Date.now();
    await sync(kernel.url, project.id, project.key);
    report.runtimes.kernel = { syncMs: Date.now() - started, ...(await readCatalog(kernel.url, project.id)) };
  } finally {
    await kernel.stop();
  }
}

// ---- verdict ------------------------------------------------------------------------------------
const { rust, kernel } = report.runtimes;
const strip = ({ body: _body, ...rest }) => rest;
const sameCatalog = JSON.stringify(rust.body.functions) === JSON.stringify(kernel.body.functions) && rust.moduleHash === kernel.moduleHash;
const errors = [...rust.checks, ...kernel.checks].flatMap((check) => check.errors);
const sameWarnings = JSON.stringify(rust.checks.map((c) => c.warnings)) === JSON.stringify(kernel.checks.map((c) => c.warnings));
const sameDenied = JSON.stringify(rust.denied) === JSON.stringify(kernel.denied);
report.verdict = {
  pass: errors.length === 0 && sameCatalog && sameWarnings && sameDenied && JSON.stringify(rust.keys) === JSON.stringify(kernel.keys),
  contractErrors: errors,
  sameCatalog,
  sameWarnings,
  sameDenied,
};
report.runtimes = { rust: strip(rust), kernel: strip(kernel) };
const text = JSON.stringify(report, null, 2);
if (out) await writeFile(resolve(out), `${text}\n`);
console.log(text);
process.exit(report.verdict.pass ? 0 : 1);
