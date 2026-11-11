/**
 * The K17 harness on local workerd (`cloudflare/kernel/principals/harness`):
 * K05's socket path in front of K06's control object and K07's tenant object,
 * with service principals wired through. It runs `service-principals`
 * against the kernel's own control code: credentials, delegation grants and
 * their redemption are K06's, the service socket is K05 + K17, allowlisted
 * Reducers run on K07's path.
 *
 * The fixture bundles are part of the Worker version (0016 G0-2): the adapter
 * builds generations 1-3 with the CLI, writes them with K07's
 * `generate-bundles.mjs`, then starts `wrangler dev`. Seeding uses the
 * harness's development routes (the reference's `/dev/*` seeding routes,
 * admin key). `configureServicePrincipals` restarts workerd with
 * `GONVEX_SERVICE_PRINCIPALS`, keeping the persisted state.
 *
 * Only Reducers run on the tenant object today; Replica Collections, Live
 * Queries and Actions need the tenant-object composition, so run only
 * `service-principals` (and the control-plane scenarios) against it.
 */
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { createWriteStream } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Backend } from '../backend.mjs';
import { buildFixture } from '../fixture.mjs';
import { REPO } from '../paths.mjs';

const CLOUDFLARE = join(REPO, 'cloudflare');
const HARNESS = join(CLOUDFLARE, 'kernel/principals/harness');
const BUNDLES = join(CLOUDFLARE, 'kernel/reducer/harness/generate-bundles.mjs');

const freePort = () => new Promise((resolve, reject) => {
  const server = createServer();
  server.unref();
  server.on('error', reject);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

export class CloudflarePrincipalsBackend extends Backend {
  constructor(options) {
    super(options);
    this.adminKey = 'conformance-admin-' + randomBytes(12).toString('hex');
    this.keys = new Map();
    this.restarts = 0;
    this.servicePrincipals = undefined;
  }

  get capabilities() { return { restart: true, crash: true, servicePrincipals: true }; }

  async start() {
    const fixtures = join(this.workDir, 'k17-fixtures');
    await mkdir(fixtures, { recursive: true });
    const manifests = [];
    for (const generation of [1, 2, 3]) {
      const { manifest } = await buildFixture(fixtures, generation);
      const path = join(fixtures, `manifest-v${generation}.json`);
      await writeFile(path, JSON.stringify(manifest));
      manifests.push(path);
    }
    const { generateBundles } = await import(BUNDLES);
    await generateBundles(manifests);
    this.state = join(this.workDir, 'wrangler-state');
    await mkdir(this.state, { recursive: true });
    this.port ??= await freePort();
    this.httpUrl = `http://127.0.0.1:${this.port}`;
    this.wsUrl = `ws://127.0.0.1:${this.port}/ws`;
    await this.spawn();
  }

  async spawn() {
    this.inspectorPort = await freePort();
    const log = createWriteStream(join(this.workDir, `wrangler-${this.restarts}.log`), { flags: 'a' });
    const vars = { GONVEX_ADMIN_KEY: this.adminKey, ...(this.servicePrincipals ? { GONVEX_SERVICE_PRINCIPALS: this.servicePrincipals } : {}) };
    const args = ['dev', '--config', join(HARNESS, 'wrangler.jsonc'), '--ip', '127.0.0.1', '--port', String(this.port), '--inspector-port', String(this.inspectorPort),
      '--persist-to', this.state, '--show-interactive-dev-session=false'];
    for (const [key, value] of Object.entries(vars)) args.push('--var', `${key}:${value}`);
    this.child = spawn(join(CLOUDFLARE, 'node_modules/.bin/wrangler'), args, {
      cwd: CLOUDFLARE, env: { ...process.env, WRANGLER_SEND_METRICS: 'false', CI: '1', NO_COLOR: '1' }, stdio: ['ignore', 'pipe', 'pipe'], detached: true,
    });
    this.child.stdout.pipe(log); this.child.stderr.pipe(log);
    this.exited = new Promise(resolve => this.child.once('exit', (code, signal) => resolve({ code, signal })));
    const deadline = Date.now() + 90_000;
    for (;;) {
      const early = await Promise.race([this.exited, delay(200).then(() => undefined)]);
      if (early) throw new Error(`wrangler dev exited during startup (${JSON.stringify(early)}); see ${join(this.workDir, `wrangler-${this.restarts}.log`)}`);
      try { const r = await fetch(this.httpUrl + '/healthz'); if (r.ok) return; } catch {}
      if (Date.now() > deadline) throw new Error('wrangler dev did not serve within 90 s');
    }
  }

  kill(signal) {
    // wrangler dev runs workerd as a child: signal the whole process group.
    try { process.kill(-this.child.pid, signal); } catch { try { this.child.kill(signal); } catch {} }
  }

  async stop() {
    if (!this.child || this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.kill('SIGTERM');
    const done = await Promise.race([this.exited, delay(10_000).then(() => undefined)]);
    if (!done) { this.kill('SIGKILL'); await this.exited; }
  }

  async restart({ crash = false } = {}) {
    if (crash) { this.kill('SIGKILL'); await this.exited; } else await this.stop();
    this.restarts++;
    // workerd may still hold the port for a moment after its parent exits.
    for (let attempt = 0; ; attempt++) {
      try { await this.spawn(); return; } catch (error) { if (attempt >= 4) throw error; await delay(500); }
    }
  }

  /** `GONVEX_SERVICE_PRINCIPALS` is read when the control object starts: restart with it. */
  async configureServicePrincipals(principals) {
    this.servicePrincipals = JSON.stringify(principals);
    await this.restart();
  }

  async request(method, path, { key = this.adminKey, body } = {}) {
    const r = await fetch(this.httpUrl + path, { method, headers: { authorization: `Bearer ${key}`, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
    const text = await r.text();
    let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
    if (!r.ok) throw new Error(`${method} ${path} -> ${r.status}: ${text.slice(0, 300)}`);
    return json;
  }

  async createProject(name) {
    const r = await this.request('POST', '/dev/projects', { body: { name, databaseMode: 'multiTenant' } });
    this.keys.set(r.project.id, r.projectKey);
    return { projectId: r.project.id, projectKey: r.projectKey };
  }

  async deploy(project, manifest) {
    const r = await this.request('POST', '/dev/sync', { key: this.keys.get(project), body: { ...manifest, project } });
    return { ok: r.ok === true, functionCount: r.functionCount };
  }

  async createTenant(project, tenantId, name) {
    const r = await this.request('POST', '/dev/tenants', { body: { projectId: project, id: tenantId, name } });
    return { tenantId: r.id };
  }

  async createMember(project, tenantId, { email, password, name }) {
    const r = await this.request('POST', '/dev/internal/e2e/members', { body: { projectId: project, tenantId, email, password, name } });
    return { accountId: r.accountId, memberId: r.memberId };
  }

  async readTable(project, tenantId, table) {
    const q = new URLSearchParams({ project, tenant: tenantId });
    return (await this.request('GET', `/dev/data/tables/${encodeURIComponent(table)}/rows?${q}`, { key: this.keys.get(project) })).rows;
  }
}

export default CloudflarePrincipalsBackend;
