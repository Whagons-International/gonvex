/**
 * The kernel with the K07 Reducer path on local workerd: the K05 socket path
 * (router, gateways, the control stand-in) and the K07 tenant object, from
 * `cloudflare/kernel/reducer/harness/wrangler.jsonc`.
 *
 * The fixture bundles are part of the Worker version (0016 G0-2), so this
 * adapter builds generations 1-3 with the CLI first (the same bytes every
 * backend deploys), writes them into the harness with
 * `generate-bundles.mjs`, and only then starts `wrangler dev`. `/dev/sync`
 * then activates the generation whose artifact hash the manifest names.
 *
 * Seeding uses the same stub routes as the `cloudflare` backend; `readTable`
 * reads the tenant object's real tables. `restart({ crash })` kills workerd
 * (SIGKILL for a crash) and starts it again on the same port and state
 * directory, so connected clients reconnect to the same URL.
 *
 * Today it serves Reducers and the receipt query. Replica collections and
 * Live Queries (K08, K10) are not there yet, so scenario steps that watch a
 * collection fail on this backend until they land.
 */
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { createWriteStream } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { CloudflareBackend } from './cloudflare.mjs';
import { buildFixture } from '../fixture.mjs';
import { REPO } from '../paths.mjs';

const CLOUDFLARE = join(REPO, 'cloudflare');
const HARNESS = join(CLOUDFLARE, 'kernel/reducer/harness');

const freePort = () => new Promise((resolve, reject) => {
  const server = createServer();
  server.unref();
  server.on('error', reject);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => resolve(port)); });
});

export class CloudflareReducerBackend extends CloudflareBackend {
  /** `receiptExpiry`: receipts expire after the offline window (0009); Rust keeps them forever. */
  get capabilities() { return { restart: true, crash: true, receiptExpiry: true, publicCalls: true }; }

  /**
   * Runs the tenant's receipt expiry (0009) as the alarm would at time `now`,
   * with `offlineMaxAgeMs` standing in for the module's window (the fixture
   * declares none).
   */
  async expireReceipts(project, tenantId, { now, offlineMaxAgeMs }) {
    const r = await fetch(`${this.httpUrl}/_stub/reducer/prune`, {
      method: 'POST', headers: { authorization: `Bearer ${this.adminKey}`, 'content-type': 'application/json' },
      body: JSON.stringify({ tenant: tenantId, now, offlineMaxAgeMs }),
    });
    if (!r.ok) throw new Error(`POST /_stub/reducer/prune -> ${r.status}: ${(await r.text()).slice(0, 300)}`);
    return r.json();
  }

  async start() {
    const fixtures = join(this.workDir, 'k07-fixtures');
    await mkdir(fixtures, { recursive: true });
    const manifests = [];
    for (const generation of [1, 2, 3]) {
      const { manifest } = await buildFixture(fixtures, generation);
      const path = join(fixtures, `manifest-v${generation}.json`);
      await writeFile(path, JSON.stringify(manifest));
      manifests.push(path);
    }
    const { generateBundles } = await import(join(HARNESS, 'generate-bundles.mjs'));
    await generateBundles(manifests);
    this.state = join(this.workDir, 'wrangler-state');
    await mkdir(this.state, { recursive: true });
    this.port = await freePort();
    this.httpUrl = `http://127.0.0.1:${this.port}`;
    this.wsUrl = `ws://127.0.0.1:${this.port}/ws`;
    await this.spawnWorkerd();
  }

  async spawnWorkerd() {
    this.inspectorPort = await freePort();
    const log = createWriteStream(process.env.GONVEX_CONFORMANCE_WRANGLER_LOG ?? join(this.workDir, 'wrangler.log'), { flags: 'a' });
    this.child = spawn(join(CLOUDFLARE, 'node_modules/.bin/wrangler'), [
      'dev', '--config', join(HARNESS, 'wrangler.jsonc'), '--ip', '127.0.0.1', '--port', String(this.port), '--inspector-port', String(this.inspectorPort),
      '--persist-to', this.state, '--var', `GONVEX_STUB_ADMIN_KEY:${this.adminKey}`, '--show-interactive-dev-session=false',
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

  async restart({ crash = false } = {}) {
    if (this.child && this.child.exitCode === null && this.child.signalCode === null) {
      try { process.kill(-this.child.pid, crash ? 'SIGKILL' : 'SIGTERM'); } catch {}
      const done = await Promise.race([this.exited, delay(10_000).then(() => undefined)]);
      if (!done) { try { process.kill(-this.child.pid, 'SIGKILL'); } catch {} await this.exited; }
    }
    // workerd may still hold the port for a moment after its parent exits.
    for (let attempt = 0; ; attempt++) {
      try { await this.spawnWorkerd(); return; } catch (error) { if (attempt >= 4) throw error; await delay(500); }
    }
  }
}
