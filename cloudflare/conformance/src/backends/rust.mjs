/**
 * The Rust runtime from this repository (`rust/crates/runtime`), the reference.
 *
 * One process per backend instance, SQLite only (no Postgres, Docker or auth
 * emulator), V8 in process. Binary: $GONVEX_CONFORMANCE_RUST_BINARY, else
 * $CARGO_TARGET_DIR/debug/gonvex-runtime, else rust/target/debug/gonvex-runtime.
 * Build it with `cargo build --locked -p gonvex-runtime` in rust/.
 *
 * Seeding uses the runtime's development routes:
 *   POST /dev/projects, POST /dev/sync, POST /dev/tenants,
 *   POST /dev/internal/e2e/members, GET /dev/data/tables/{table}/rows.
 */
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { existsSync, createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Backend } from '../backend.mjs';
import { REPO } from '../paths.mjs';

export function rustBinary() {
  const candidates = [process.env.GONVEX_CONFORMANCE_RUST_BINARY,
    process.env.CARGO_TARGET_DIR && join(process.env.CARGO_TARGET_DIR, 'debug/gonvex-runtime'),
    join(REPO, 'rust/target/debug/gonvex-runtime'), join(REPO, 'rust/target/release/gonvex-runtime')].filter(Boolean);
  const found = candidates.find(p => existsSync(p));
  if (!found) throw new Error(`gonvex-runtime binary not found (tried ${candidates.join(', ')}). Build it: (cd rust && cargo build --locked -p gonvex-runtime), or set GONVEX_CONFORMANCE_RUST_BINARY.`);
  return found;
}

/**
 * Fixed configuration both backends get, so control-plane scenarios can use
 * provider secrets (C.3.3) and the native OAuth routes (C.3.5).
 */
export const CONFORMANCE_SECRETS = {
  GONVEX_DASHBOARD_SESSION_SECRET: 'conformance-control-secret',
  GONVEX_GOOGLE_CLIENT_ID: 'conformance-google-client',
  GONVEX_GOOGLE_CLIENT_SECRET: 'conformance-google-secret',
};

/**
 * Firebase Auth Emulator test decoder (docs/firebase-auth-emulator.md), on
 * when GONVEX_CONFORMANCE_FIREBASE_EMULATOR names the emulator (host:port).
 * The Rust binary must be built with `--features test-firebase-auth-emulator`.
 */
export function firebaseEmulatorEnv() {
  const host = process.env.GONVEX_CONFORMANCE_FIREBASE_EMULATOR;
  return host ? { GONVEX_TEST_AUTH_EMULATOR: 'true', GONVEX_ENV: 'test', FIREBASE_AUTH_EMULATOR_HOST: host } : {};
}

const freePort = () => new Promise((resolve, reject) => {
  const server = createServer();
  server.unref();
  server.on('error', reject);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

export class RustBackend extends Backend {
  constructor(options) {
    super(options);
    this.binary = options.binary ?? rustBinary();
    this.adminKey = 'conformance-admin-' + randomBytes(12).toString('hex');
    this.dataDir = join(this.workDir, 'data');
    this.keys = new Map(); // projectId -> projectKey
    this.restarts = 0;
  }

  get capabilities() { return { restart: true, crash: true, firebaseEmulator: !!process.env.GONVEX_CONFORMANCE_FIREBASE_EMULATOR }; }

  async start() {
    await mkdir(this.dataDir, { recursive: true });
    this.port ??= await freePort();
    this.httpUrl = `http://127.0.0.1:${this.port}`;
    this.wsUrl = `ws://127.0.0.1:${this.port}/ws`;
    await this.spawn();
  }

  async spawn() {
    const log = createWriteStream(join(this.workDir, `runtime-${this.restarts}.log`));
    this.child = spawn(this.binary, [], {
      cwd: this.workDir,
      env: {
        PATH: process.env.PATH, HOME: process.env.HOME, RUST_LOG: process.env.GONVEX_CONFORMANCE_RUST_LOG ?? 'warn',
        GONVEX_ADDR: `127.0.0.1:${this.port}`,
        GONVEX_DATA_DIR: this.dataDir,
        GONVEX_ADMIN_KEY: this.adminKey,
        GONVEX_SQLITE_SYNCHRONOUS: 'full',
        // A Reducer that never returns fails `transient` within the scenario budget.
        GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS: '2000',
        GONVEX_AUTH_URL: this.httpUrl,
        ...CONFORMANCE_SECRETS,
        ...firebaseEmulatorEnv(),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    this.child.stdout.pipe(log); this.child.stderr.pipe(log);
    const exited = new Promise(resolve => this.child.once('exit', (code, signal) => resolve({ code, signal })));
    this.exited = exited;
    const deadline = Date.now() + 60_000;
    for (;;) {
      const early = await Promise.race([exited, delay(100).then(() => undefined)]);
      if (early) throw new Error(`gonvex-runtime exited during startup (${JSON.stringify(early)}); see ${join(this.workDir, `runtime-${this.restarts}.log`)}`);
      try { const r = await fetch(this.httpUrl + '/healthz'); if (r.ok) return; } catch {}
      if (Date.now() > deadline) throw new Error('gonvex-runtime did not become healthy within 60 s');
    }
  }

  async stop() {
    if (!this.child || this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.kill('SIGTERM');
    const done = await Promise.race([this.exited, delay(10_000).then(() => undefined)]);
    if (!done) { this.child.kill('SIGKILL'); await this.exited; }
  }

  async restart({ crash = false } = {}) {
    if (crash) { this.child.kill('SIGKILL'); await this.exited; }
    else await this.stop();
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
    const r = await this.request('POST', '/dev/tenants', { key: this.keys.get(project), body: { projectId: project, id: tenantId, name } });
    return { tenantId: r.id };
  }

  async createMember(project, tenantId, { email, password, name }) {
    const r = await this.request('POST', '/dev/internal/e2e/members', { body: { projectId: project, tenantId, email, password, name } });
    return { accountId: r.accountId, memberId: r.memberId };
  }

  /** An account with a password login and no membership. */
  async createAccount(project, { email, password, name }) {
    const r = await this.request('POST', '/dev/internal/e2e/members', { body: { projectId: project, email, password, name, accountOnly: true } });
    return { accountId: r.accountId };
  }

  /** A project developer (`gonvex_project_members`): grants `dev` / `pAdmin` Control access. */
  async addProjectMember(project, { email, name = '', role = 'dev' }) {
    await this.request('POST', `/dev/projects/${encodeURIComponent(project)}/members`, { body: { email, name, role } });
  }

  /** Native Google OAuth with a registered app redirect URI (operator route). */
  async configureGoogleAuth(project, { redirectUri, signupMode = 'personal' }) {
    await this.request('PUT', `/dev/projects/${encodeURIComponent(project)}/auth/google`, { body: { redirectUri, signupMode } });
  }

  async readTable(project, tenantId, table) {
    const rows = [];
    for (let offset = 0; ; offset += 1000) {
      const q = new URLSearchParams({ project, tenant: tenantId, limit: '1000', offset: String(offset) });
      const page = await this.request('GET', `/dev/data/tables/${encodeURIComponent(table)}/rows?${q}`, { key: this.keys.get(project) });
      rows.push(...page.rows);
      if (page.rows.length < 1000 || rows.length >= page.total) return rows;
    }
  }
}
