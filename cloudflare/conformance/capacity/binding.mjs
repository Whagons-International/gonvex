import { readFile, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { isAbsolute } from 'node:path';

export class Refusal extends Error {
  constructor(code, field) { super(`${code}: ${field}`); this.code = code; this.field = field; }
}
export const refuse = field => { throw new Refusal('NOT_ELIGIBLE', field); };
export function shape(v, keys, field) {
  if (!v || typeof v !== 'object' || Array.isArray(v) || Object.keys(v).some(k => !keys.includes(k))
    || keys.some(k => !Object.hasOwn(v, k))) refuse(field);
}
export function integer(n, low, high, field) { if (!Number.isSafeInteger(n) || n < low || n > high) refuse(field); }
export function text(s, field, max = 256) { if (typeof s !== 'string' || !s.length || s.length > max) refuse(field); }
export function list(v, max, field, min = 1) { if (!Array.isArray(v) || v.length < min || v.length > max) refuse(field); }
export function unique(v, field) { if (new Set(v).size !== v.length) refuse(field); }
export const hash = value => createHash('sha256').update(value).digest('hex');
export function hex(s, length, field) { if (typeof s !== 'string' || !new RegExp(`^[0-9a-f]{${length}}$`).test(s)) refuse(field); }
export function endpoint(s, socket = false) {
  let u; try { u = new URL(s); } catch { refuse('endpoint'); }
  if (u.protocol !== (socket ? 'wss:' : 'https:') || !/^gonvex-cf-dev-[a-z0-9-]+\.whagons-international\.workers\.dev$/.test(u.hostname)
    || u.username || u.password || u.search || u.hash || u.port) refuse('endpoint.dev-only');
  return u;
}
export async function jsonFile(path, maxBytes = 8 * 1024 * 1024, privateFile = false) {
  if (!isAbsolute(path)) refuse('absolute-file');
  const s = await stat(path);
  if (!s.isFile() || s.size > maxBytes || (privateFile && (s.mode & 0o077))) refuse('file.bounds/mode');
  const raw = await readFile(path);
  return { value: JSON.parse(raw.toString('utf8')), hash: hash(raw) };
}
export async function checkedFile(v) {
  shape(v, ['path', 'sha256'], 'file'); text(v.path, 'file.path', 4096); hex(v.sha256, 64, 'file.sha256');
  if (!isAbsolute(v.path) || (await stat(v.path)).size > 32 * 1024 * 1024 || hash(await readFile(v.path)) !== v.sha256) refuse('file.identity');
}
export function validateBinding(b) {
  shape(b, ['v', 'runId', 'cell', 'deployment', 'sources', 'auth', 'tenants', 'phases', 'mix', 'telemetry', 'budget', 'resourceAdmission'], 'binding');
  if (b.v !== 1) refuse('version');
  if (!/^[a-zA-Z0-9-]{1,64}$/.test(b.runId)) refuse('runId'); text(b.cell, 'cell');
  const d = b.deployment;
  shape(d, ['accountId', 'sourceSha', 'moduleSha256', 'configSha256', 'endpoint', 'resources'], 'deployment');
  if (d.accountId !== '30962192f9607a89eda812daa15e55fa') refuse('account');
  hex(d.sourceSha, 40, 'sourceSha'); hex(d.moduleSha256, 64, 'moduleSha256'); hex(d.configSha256, 64, 'configSha256'); endpoint(d.endpoint, true);
  list(d.resources, 4, 'resources', 4); unique(d.resources.map(r => r.role), 'resource.roles');
  for (const r of d.resources) { shape(r, ['role', 'name', 'version'], 'resource');
    if (!['kernel', 'actions', 'builder', 'sandbox'].includes(r.role) || !/^gonvex-cf-dev-[a-z0-9-]+$/.test(r.name)) refuse('resource.identity'); text(r.version, 'resource.version'); }
  const kernel = d.resources.find(r => r.role === 'kernel').name;
  if (endpoint(d.endpoint, true).hostname.split('.')[0] !== kernel || d.resources.some(r => r.role !== 'kernel' && !r.name.startsWith(`${kernel}-`))) refuse('resource.scope');
  shape(b.sources, ['sdk', 'refs', 'localRuntime', 'sdkSourceSha', 'webSourceSha', 'closure'], 'sources');
  hex(b.sources.sdkSourceSha, 40, 'sdkSourceSha'); hex(b.sources.webSourceSha, 40, 'webSourceSha');
  shape(b.auth, ['fileEnv'], 'auth'); if (b.auth.fileEnv !== 'WHAGONS_CREDENTIAL_FILE') refuse('auth.Vault-file-only');
  shape(b.mix, ['kind', 'evidence'], 'mix');
  if (!['synthetic-stress', 'source-derived'].includes(b.mix.kind) || (b.mix.kind === 'source-derived' && !b.mix.evidence)) refuse('mix.evidence');
  list(b.tenants, 26, 'tenants'); unique(b.tenants.map(t => t.alias), 'tenant.alias'); unique(b.tenants.map(t => t.tenantId), 'tenant.id');
  for (const t of b.tenants) { shape(t, ['alias', 'tenantId', 'project', 'principals', 'fixture', 'dataProvenance'], 'tenant');
    for (const k of ['alias', 'tenantId', 'project']) text(t[k], `tenant.${k}`);
    list(t.principals, 1000, 'principals'); unique(t.principals, 'principals'); t.principals.forEach(p => text(p, 'principal.alias')); }
  const limits = { maxWallMs: [100, 3600000], maxConnections: [2, 26000], maxPending: [1, 1000], maxOffered: [1, 100000],
    maxSentBytes: [1, 2 ** 40], maxReceivedBytes: [1, 2 ** 40], maxArtifactBytes: [1024, 2 ** 30],
    maxResidentRows: [1, 100000], maxResidentBytes: [1024, 2 ** 30], maxGeneratorRssBytes: [1024, 2 ** 40], maxGeneratorFds: [16, 100000],
    maxLagMs: [1, 60000], operationTimeoutMs: [1, 60000], setupTimeoutMs: [1, 300000], cleanupTimeoutMs: [1, 60000],
    opensPerSecond: [1, 1000], telemetryIntervalMs: [100, 60000], maxTelemetryAgeMs: [100, 60000], maxCostAgeMs: [100, 60000] };
  shape(b.budget, [...Object.keys(limits), 'maxUsd'], 'budget');
  for (const [k, [low, high]] of Object.entries(limits)) integer(b.budget[k], low, high, k);
  if (!Number.isFinite(b.budget.maxUsd) || b.budget.maxUsd <= 0) refuse('maxUsd');
  shape(b.telemetry, ['url', 'contractSha256', 'providerFile'], 'telemetry');
  if (b.telemetry.url !== null && endpoint(b.telemetry.url).host !== endpoint(d.endpoint, true).host) refuse('telemetry.resource-scope');
  if (b.telemetry.contractSha256 !== null) hex(b.telemetry.contractSha256, 64, 'telemetry.contractSha256');
  if (b.telemetry.providerFile !== null && !isAbsolute(b.telemetry.providerFile)) refuse('providerFile');
  list(b.phases, 32, 'phases'); let duration = 0, offered = 0;
  for (const p of b.phases) {
    shape(p, ['name', 'stage', 'durationMs', 'tenants'], 'phase'); text(p.name, 'phase.name');
    if (!['warmup', 'measure', 'cooldown'].includes(p.stage)) refuse('phase.stage');
    integer(p.durationMs, 1, 1800000, 'durationMs'); duration += p.durationMs;
    list(p.tenants, 26, 'phase.tenants'); unique(p.tenants.map(t => t.alias), 'phase.alias'); let total = 0;
    for (const t of p.tenants) {
      shape(t, ['alias', 'connections', 'active', 'reconnect', 'rates'], 'phase.tenant');
      if (!b.tenants.some(v => v.alias === t.alias)) refuse('phase.tenant.binding');
      integer(t.connections, 2, 1000, 'connections'); integer(t.active, 0, t.connections - 1, 'active'); integer(t.reconnect, 0, t.connections, 'reconnect'); total += t.connections;
      list(t.rates, 16, 'rates', 0); unique(t.rates.map(r => r.operation), 'rates.operation');
      for (const r of t.rates) { shape(r, ['operation', 'perSecond'], 'rate'); text(r.operation, 'operation');
        if (!Number.isFinite(r.perSecond) || r.perSecond <= 0 || r.perSecond > 10000 || !t.active) refuse('rate');
        offered += Math.ceil(p.durationMs * r.perSecond / 1000); }
    }
    if (total > b.budget.maxConnections) refuse('connection-budget');
  }
  if (duration + b.budget.setupTimeoutMs + b.budget.cleanupTimeoutMs > b.budget.maxWallMs) refuse('wall-budget');
  if (offered > b.budget.maxOffered) refuse('offered-budget');
  return b;
}

/** Local checks only: never imports executable modules, reads credentials or touches a target. */
export async function preflight(path) {
  const { value: b } = await jsonFile(path); validateBinding(b);
  await checkedFile(b.sources.sdk); await checkedFile(b.sources.refs);
  list(b.sources.closure, 4096, 'sources.closure'); unique(b.sources.closure.map(f => f.path), 'closure.paths');
  let closureBytes = 0;
  for (const f of b.sources.closure) { closureBytes += (await stat(f.path)).size; if (closureBytes > 128 * 1024 * 1024) refuse('closure.bytes'); await checkedFile(f); }
  for (const f of [b.sources.sdk, b.sources.refs, b.sources.localRuntime].filter(Boolean))
    if (!b.sources.closure.some(c => c.path === f.path && c.sha256 === f.sha256)) refuse('closure.entry');
  for (const f of [b.sources.localRuntime, b.mix.evidence, b.resourceAdmission, ...b.tenants.flatMap(t => [t.fixture, t.dataProvenance])].filter(Boolean)) await checkedFile(f);
  const missing = [];
  if (!b.sources.localRuntime) missing.push('generated-localRuntime-for-localExecution-evidence');
  if (!b.telemetry.url) missing.push('authenticated-assembled-capacity-telemetry-v1');
  if (!b.telemetry.contractSha256) missing.push('reviewed-capacity-adapter-contract');
  if (!b.telemetry.providerFile) missing.push('actual-provider-run-attribution-and-cost-collector');
  if (b.telemetry.contractSha256 && b.telemetry.contractSha256 !== hash(await readFile(new URL('./telemetry.mjs', import.meta.url)))) missing.push('driver-input-profile-mismatch');
  if (!b.resourceAdmission) missing.push('normal-resource-admission-receipt');
  for (const t of b.tenants) if (!t.dataProvenance) missing.push('tenant-data-provenance');
  return { state: missing.length ? 'DORMANT' : 'BOUND_NOT_RUNTIME_VERIFIED', missing, binding: b };
}
