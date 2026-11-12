#!/usr/bin/env node
/**
 * K21: proves the bridge's replacement for its direct `boardMessages` INSERT
 * against a runtime with service principals (docs/service-principals.md):
 *
 *  1. builds `verify/app` (a stand-in with Whagons' `legacyBridge.createBoardMessage`
 *     signature) with the repository CLI;
 *  2. starts the Rust reference, creates a project, and restarts it with one
 *     principal `whagons-legacy-bridge` allowed only that function;
 *  3. checks the service socket at frame level: auth bound to project and tenant, the
 *     allowlist, refusal of subscriptions and replicas, an idempotent keyed call;
 *  4. runs the patched bridge's own writer (`TestServicePrincipalE2E`) against it;
 *  5. reads the table: one row per intent, written as `_gonvex_service:whagons-legacy-bridge`
 *     on the `api` channel.
 *
 * The kernel has no service principals until K17 lands. Point K17's acceptance at the same
 * frames (`--ws` is the only thing that differs once the kernel can be provisioned this way).
 *
 * Usage (repo root built with `pnpm build:packages`):
 *   node cloudflare/bridge-support/verify/service-principal.mjs \
 *     --runtime <gonvex-runtime binary> --bridge <whagons5-client>/compatibility/legacy-convex-v5.1.29-bridge
 * `--bridge` is optional: without it only the frame-level checks run.
 */
import { spawn, execFile } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { cp, mkdir, mkdtemp, readFile, rm, symlink } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';

const run = promisify(execFile);
const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '../../..');
const PRINCIPAL = 'whagons-legacy-bridge';
const ALLOWED = 'legacyBridge.createBoardMessage';

const arg = (name) => { const i = process.argv.indexOf(`--${name}`); return i > 0 ? process.argv[i + 1] : undefined; };
const runtimeBinary = arg('runtime') ?? process.env.GONVEX_CONFORMANCE_RUST_BINARY;
const bridgeDir = arg('bridge');
if (!runtimeBinary) throw new Error('--runtime <gonvex-runtime> (or GONVEX_CONFORMANCE_RUST_BINARY) is required');

const freePort = () => new Promise((ok, fail) => {
  const server = createServer(); server.unref(); server.on('error', fail);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => ok(port)); });
});

const results = [];
const check = (name, ok, detail) => { results.push({ name, ok: !!ok, ...(detail === undefined ? {} : { detail }) }); console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${detail === undefined ? '' : ` ${JSON.stringify(detail)}`}`); };

async function buildApp(work) {
  const dir = join(work, 'app');
  await cp(join(HERE, 'app'), dir, { recursive: true });
  await mkdir(join(dir, 'node_modules/@gonvex'), { recursive: true });
  for (const pkg of ['module-sdk', 'client', 'protocol']) await symlink(join(REPO, 'packages', pkg), join(dir, 'node_modules/@gonvex', pkg), 'dir');
  await run(process.execPath, [join(REPO, 'packages/gonvex/dist/index.js'), 'codegen', '--project', dir], { cwd: dir, env: { ...process.env, NODE_NO_WARNINGS: '1' } });
  return JSON.parse(await readFile(join(dir, 'gonvex/_generated/manifest.json'), 'utf8'));
}

class Runtime {
  constructor(work) { this.work = work; this.adminKey = randomBytes(16).toString('hex'); this.extraEnv = {}; this.n = 0; }
  async start() {
    this.port ??= await freePort();
    this.http = `http://127.0.0.1:${this.port}`;
    this.ws = `ws://127.0.0.1:${this.port}/ws`;
    this.child = spawn(runtimeBinary, [], {
      cwd: this.work, stdio: 'ignore',
      env: { PATH: process.env.PATH, HOME: process.env.HOME, RUST_LOG: 'warn', GONVEX_ADDR: `127.0.0.1:${this.port}`, GONVEX_DATA_DIR: join(this.work, 'data'), GONVEX_ADMIN_KEY: this.adminKey, GONVEX_AUTH_URL: this.http, ...this.extraEnv },
    });
    this.exited = new Promise((ok) => this.child.once('exit', ok));
    for (let deadline = Date.now() + 60_000; ;) {
      try { if ((await fetch(`${this.http}/healthz`)).ok) return; } catch {}
      if (this.child.exitCode !== null) throw new Error('runtime exited during startup');
      if (Date.now() > deadline) throw new Error('runtime did not become healthy');
      await delay(100);
    }
  }
  async stop() { if (this.child?.exitCode === null) { this.child.kill('SIGTERM'); await this.exited; } }
  async request(method, path, { key = this.adminKey, body } = {}) {
    const r = await fetch(this.http + path, { method, headers: { authorization: `Bearer ${key}`, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
    const text = await r.text();
    if (!r.ok) throw new Error(`${method} ${path} -> ${r.status}: ${text.slice(0, 300)}`);
    return JSON.parse(text);
  }
}

/** A frame-level socket. */
async function socket(url) {
  const ws = new WebSocket(url);
  const inbox = []; let wake = () => {};
  ws.addEventListener('message', (e) => { inbox.push(JSON.parse(String(e.data))); wake(); });
  await new Promise((ok, fail) => { ws.addEventListener('open', ok, { once: true }); ws.addEventListener('error', fail, { once: true }); });
  const next = async (match, ms = 10_000) => {
    for (const deadline = Date.now() + ms; ;) {
      const i = inbox.findIndex(match); if (i >= 0) return inbox.splice(i, 1)[0];
      if (Date.now() > deadline) throw new Error('timed out');
      await Promise.race([new Promise((ok) => { wake = ok; }), delay(100)]);
    }
  };
  const request = (frame) => { ws.send(JSON.stringify(frame)); return next((m) => m.id === frame.id); };
  return { ws, next, request, close: () => ws.close() };
}

const work = await mkdtemp(join(process.env.K21_VERIFY_TMP ?? tmpdir(), 'k21-service-principal-'));
const runtime = new Runtime(work);
try {
  const manifest = await buildApp(work);
  await runtime.start();
  const { project, projectKey } = await runtime.request('POST', '/dev/projects', { body: { name: 'k21-verify', databaseMode: 'multiTenant' } });
  // Service principals are startup configuration: restart with one bound to this project.
  await runtime.stop();
  const token = `gvx_svc_${randomBytes(32).toString('hex')}`;
  runtime.extraEnv.GONVEX_SERVICE_PRINCIPALS = JSON.stringify([{ id: PRINCIPAL, tokenSha256: createHash('sha256').update(token).digest('hex'), projects: [project.id], functions: [ALLOWED] }]);
  await runtime.start();
  await runtime.request('POST', '/dev/sync', { key: projectKey, body: { ...manifest, project: project.id } });
  const tenant = 'k21-verify-a';
  await runtime.request('POST', '/dev/tenants', { key: projectKey, body: { projectId: project.id, id: tenant, name: 'K21 verify' } });
  await runtime.request('POST', '/dev/tenants', { key: projectKey, body: { projectId: project.id, id: 'k21-verify-b', name: 'K21 verify B' } });

  // 3. Frame level.
  const s = await socket(`${runtime.ws}?project=${project.id}&tenant=${tenant}`);
  await s.next((m) => m.type === 'session.ready');
  const auth = await s.request({ type: 'auth', id: randomUUID(), token, project: project.id, tenant });
  check('service auth binds the socket to the tenant', auth.type === 'auth.result', auth.type === 'auth.result' ? Object.keys(auth.result).sort() : auth.error);
  const entity = { _id: 'legacy-board-message-frames', boardId: 'board-1', createdBy: 'member-1', content: 'frames' };
  const first = await s.request({ type: 'reducer.call', id: randomUUID(), path: ALLOWED, args: { entity }, idempotencyKey: 'frames-intent' });
  check('allowlisted internal Reducer with an idempotencyKey', first.type === 'reducer.result' && first.result === entity._id, first.error ?? first.result);
  const replay = await s.request({ type: 'reducer.call', id: randomUUID(), path: ALLOWED, args: { entity }, idempotencyKey: 'frames-intent' });
  check('the same key replays the stored result', replay.type === 'reducer.result' && replay.result === entity._id, replay.error ?? replay.result);
  const refused = await s.request({ type: 'reducer.call', id: randomUUID(), path: 'legacyBridge.notAllowed', args: {} });
  check('a function outside the allowlist is refused', refused.type === 'reducer.error' && /is not allowed for this service principal/.test(refused.error), refused.error);
  const sub = await s.request({ type: 'query.subscribe', id: randomUUID(), path: ALLOWED, args: {} });
  check('subscriptions are refused', sub.type === 'query.error' && /service principals cannot subscribe/.test(sub.error), sub.error);
  const rep = await s.request({ type: 'replica.open', id: randomUUID(), path: ALLOWED, args: {} });
  check('replicas are refused', rep.type === 'replica.error' && /cannot open replicas/.test(rep.error), rep.error);
  s.close();
  const other = await socket(runtime.ws);
  await other.next((m) => m.type === 'session.ready');
  const wrong = await other.request({ type: 'auth', id: randomUUID(), token: `gvx_svc_${'0'.repeat(64)}`, project: project.id, tenant });
  check('an unknown service credential is refused', wrong.type === 'auth.error', wrong.error);
  other.close();

  // 4. The patched bridge's writer.
  if (bridgeDir) {
    const go = await run('go', ['test', '-count=1', '-run', '^TestServicePrincipalE2E$', '-v', '.'], {
      cwd: bridgeDir,
      env: { ...process.env, BRIDGE_SERVICE_E2E_WS_URL: runtime.ws, BRIDGE_SERVICE_E2E_PROJECT: project.id, BRIDGE_SERVICE_E2E_TENANT: tenant, BRIDGE_SERVICE_E2E_TOKEN: token },
    }).then((r) => ({ ok: true, out: r.stdout }), (e) => ({ ok: false, out: `${e.stdout}${e.stderr}` }));
    check('bridge writer (go test TestServicePrincipalE2E)', go.ok && /--- PASS: TestServicePrincipalE2E/.test(go.out), go.out.split('\n').filter((l) => /---|E2E message|FAIL|Error/.test(l)).map((l) => l.trim()));
  }

  // 5. What was written, and as whom.
  const { rows } = await runtime.request('GET', `/dev/data/tables/boardMessages/rows?project=${project.id}&tenant=${tenant}&limit=100&offset=0`, { key: projectKey });
  check(`one row per intent (${bridgeDir ? 2 : 1} intents)`, rows.length === (bridgeDir ? 2 : 1), rows.map((r) => r._id));
  check('written as the service principal on the api channel', rows.every((r) => r.caller === `_gonvex_service:${PRINCIPAL}` && r.channel === 'api'), rows.map((r) => [r.caller, r.channel]));
  const { rows: otherRows } = await runtime.request('GET', `/dev/data/tables/boardMessages/rows?project=${project.id}&tenant=k21-verify-b&limit=100&offset=0`, { key: projectKey });
  check('nothing reached the other tenant', otherRows.length === 0, otherRows.length);
} finally {
  await runtime.stop();
  if (!process.argv.includes('--keep')) await rm(work, { recursive: true, force: true });
}
const failed = results.filter((r) => !r.ok).length;
console.log(`${results.length - failed}/${results.length} checks passed`);
process.exit(failed ? 1 : 0);
