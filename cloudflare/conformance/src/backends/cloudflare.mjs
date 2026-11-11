/**
 * The Cloudflare kernel on local workerd (`wrangler dev` in `cloudflare/`).
 *
 * Seeding goes through the session layer's stub routes (`/_stub/session/*`,
 * `cloudflare/kernel/session/stub/routes.ts`), which mirror the Rust
 * development routes the Rust adapter uses. They stand in for the control
 * object (K06) and the deploy path (K19) until those packages land; the
 * adapter then switches to their routes and nothing in the scenarios changes.
 *
 * Today the kernel serves the session layer (K05): handshake, auth,
 * capabilities, close codes and limits. Scenarios that need Reducers, Queries
 * or replicas fail on it until K07, K08 and K10 land.
 *
 * One `wrangler dev` per backend instance, with its own state directory and
 * port. `GONVEX_CONFORMANCE_WRANGLER_LOG` keeps wrangler's output (default:
 * `<workDir>/wrangler.log`).
 *
 * Remote mode: with `GONVEX_CONFORMANCE_CF_URL` (an `https://gonvex-cf-dev-*`
 * deploy) and `GONVEX_CONFORMANCE_CF_KEY` (its `GONVEX_STUB_ADMIN_KEY`), the
 * adapter starts nothing and runs against that deploy. Each scenario uses its
 * own project, so runs do not interfere.
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

const CLOUDFLARE = join(REPO, 'cloudflare');

const freePort = () => new Promise((resolve, reject) => {
  const server = createServer();
  server.unref();
  server.on('error', reject);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

export class CloudflareBackend extends Backend {
  constructor(options) {
    super(options);
    this.adminKey = 'conformance-stub-' + randomBytes(12).toString('hex');
  }

  get capabilities() { return { restart: false, crash: false }; }

  async start() {
    if (process.env.GONVEX_CONFORMANCE_CF_URL) {
      this.httpUrl = process.env.GONVEX_CONFORMANCE_CF_URL.replace(/\/$/, '');
      this.wsUrl = this.httpUrl.replace(/^http/, 'ws') + '/ws';
      this.adminKey = process.env.GONVEX_CONFORMANCE_CF_KEY;
      if (!this.adminKey) throw new Error('GONVEX_CONFORMANCE_CF_KEY is required with GONVEX_CONFORMANCE_CF_URL');
      return;
    }
    const state = join(this.workDir, 'wrangler-state');
    await mkdir(state, { recursive: true });
    this.port = await freePort();
    this.inspectorPort = await freePort();
    this.httpUrl = `http://127.0.0.1:${this.port}`;
    this.wsUrl = `ws://127.0.0.1:${this.port}/ws`;
    const log = createWriteStream(process.env.GONVEX_CONFORMANCE_WRANGLER_LOG ?? join(this.workDir, 'wrangler.log'));
    this.child = spawn(join(CLOUDFLARE, 'node_modules/.bin/wrangler'), [
      'dev', '--ip', '127.0.0.1', '--port', String(this.port), '--inspector-port', String(this.inspectorPort),
      '--persist-to', state, '--var', `GONVEX_STUB_ADMIN_KEY:${this.adminKey}`, '--show-interactive-dev-session=false',
    ], { cwd: CLOUDFLARE, env: { ...process.env, WRANGLER_SEND_METRICS: 'false', CI: '1' }, stdio: ['ignore', 'pipe', 'pipe'], detached: true });
    this.child.stdout.pipe(log); this.child.stderr.pipe(log);
    this.exited = new Promise(resolve => this.child.once('exit', (code, signal) => resolve({ code, signal })));
    const deadline = Date.now() + 90_000;
    for (;;) {
      const early = await Promise.race([this.exited, delay(200).then(() => undefined)]);
      if (early) throw new Error(`wrangler dev exited during startup (${JSON.stringify(early)}); see ${join(this.workDir, 'wrangler.log')}`);
      try { const r = await fetch(this.httpUrl + '/'); if (r.ok) return; } catch {}
      if (Date.now() > deadline) throw new Error('wrangler dev did not serve within 90 s');
    }
  }

  async stop() {
    if (!this.child) return;
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    // wrangler starts workerd as a child: signal the whole process group.
    try { process.kill(-this.child.pid, 'SIGTERM'); } catch {}
    const done = await Promise.race([this.exited, delay(10_000).then(() => undefined)]);
    if (!done) { try { process.kill(-this.child.pid, 'SIGKILL'); } catch {} await this.exited; }
  }

  async request(method, route, body) {
    const r = await fetch(`${this.httpUrl}/_stub/session/${route}`, {
      method, headers: { authorization: `Bearer ${this.adminKey}`, ...(body ? { 'content-type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await r.text();
    if (!r.ok) throw new Error(`${method} /_stub/session/${route} -> ${r.status}: ${text.slice(0, 300)}`);
    return JSON.parse(text);
  }

  createProject(name) { return this.request('POST', 'projects', { name }); }
  deploy(project, manifest) { return this.request('POST', 'sync', { ...manifest, project }); }
  async createTenant(project, tenantId, name) { const r = await this.request('POST', 'tenants', { projectId: project, id: tenantId, name }); return { tenantId: r.id }; }
  createMember(project, tenantId, { email, password, name }) { return this.request('POST', 'members', { projectId: project, tenantId, email, password, name }); }
  async readTable(project, tenantId, table) {
    const q = new URLSearchParams({ project, tenant: tenantId });
    return (await this.request('GET', `tables/${encodeURIComponent(table)}?${q}`)).rows;
  }
}
