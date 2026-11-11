/**
 * The K06 control object in local workerd (`wrangler dev`), with the stub
 * tenant object and the minimal control socket owner from
 * cloudflare/control/test-support. It serves `auth` and `scope:"control"`
 * frames and the native OAuth routes; tenant-scope frames are refused, so
 * run only the control-plane scenarios against it (README, "Control plane").
 *
 * Seeding goes through the same development routes as the Rust adapter
 * (POST /dev/projects, /dev/sync, /dev/tenants, /dev/internal/e2e/members),
 * served by cloudflare/control/test-support/worker.ts.
 */
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Backend } from '../backend.mjs';
import { REPO } from '../paths.mjs';
import { firebaseEmulatorEnv, CONFORMANCE_SECRETS } from './rust.mjs';

const freePort = () => new Promise((resolve, reject) => {
  const server = createServer();
  server.unref();
  server.on('error', reject);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

const CONTROL_DIR = join(REPO, 'cloudflare/control');

export class CloudflareControlBackend extends Backend {
  constructor(options) {
    super(options);
    this.adminKey = 'conformance-admin-' + randomBytes(12).toString('hex');
    this.persist = join(this.workDir, 'state');
    this.keys = new Map();
    this.restarts = 0;
  }

  /** Control-plane only: tenant data, replicas and module code are other packages. */
  get capabilities() { return { restart: true, crash: true, firebaseEmulator: !!process.env.GONVEX_CONFORMANCE_FIREBASE_EMULATOR }; }

  async start() {
    await mkdir(this.persist, { recursive: true });
    this.port ??= await freePort();
    this.inspector ??= await freePort();
    this.httpUrl = `http://127.0.0.1:${this.port}`;
    this.wsUrl = `ws://127.0.0.1:${this.port}/ws`;
    await this.spawn();
  }

  async spawn() {
    const log = createWriteStream(join(this.workDir, `wrangler-${this.restarts}.log`));
    const vars = {
      GONVEX_ADMIN_KEY: this.adminKey,
      GONVEX_AUTH_URL: this.httpUrl,
      ...CONFORMANCE_SECRETS,
      ...firebaseEmulatorEnv(),
    };
    const args = ['dev', '--config', join(CONTROL_DIR, 'wrangler.jsonc'), '--ip', '127.0.0.1', '--port', String(this.port),
      '--inspector-port', String(this.inspector), '--persist-to', this.persist, '--log-level', 'warn', '--show-interactive-dev-session=false'];
    for (const [key, value] of Object.entries(vars)) args.push('--var', `${key}:${value}`);
    this.child = spawn(join(REPO, 'cloudflare/node_modules/.bin/wrangler'), args, {
      cwd: CONTROL_DIR,
      env: { ...process.env, WRANGLER_SEND_METRICS: 'false', CI: '1', NO_COLOR: '1' },
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: true,
    });
    this.child.stdout.pipe(log); this.child.stderr.pipe(log);
    const exited = new Promise(resolve => this.child.once('exit', (code, signal) => resolve({ code, signal })));
    this.exited = exited;
    const deadline = Date.now() + 90_000;
    for (;;) {
      const early = await Promise.race([exited, delay(200).then(() => undefined)]);
      if (early) throw new Error(`wrangler dev exited during startup (${JSON.stringify(early)}); see ${join(this.workDir, `wrangler-${this.restarts}.log`)}`);
      try { const r = await fetch(this.httpUrl + '/healthz'); if (r.ok) return; } catch {}
      if (Date.now() > deadline) throw new Error('wrangler dev did not become healthy within 90 s');
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
    await this.spawn();
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

  async createAccount(project, { email, password, name }) {
    const r = await this.request('POST', '/dev/internal/e2e/members', { body: { projectId: project, email, password, name, accountOnly: true } });
    return { accountId: r.accountId };
  }

  async addProjectMember(project, { email, name = '', role = 'dev' }) {
    await this.request('POST', `/dev/projects/${encodeURIComponent(project)}/members`, { body: { email, name, role } });
  }

  async configureGoogleAuth(project, { redirectUri, signupMode = 'personal' }) {
    await this.request('PUT', `/dev/projects/${encodeURIComponent(project)}/auth/google`, { body: { redirectUri, signupMode } });
  }

  /** Tenant module tables live in the tenant object (K01+); the stub holds `members` only. */
  async readTable(project, tenantId, table) {
    const q = new URLSearchParams({ project, tenant: tenantId });
    const page = await this.request('GET', `/dev/data/tables/${encodeURIComponent(table)}/rows?${q}`);
    return page.rows;
  }
}

export default CloudflareControlBackend;
