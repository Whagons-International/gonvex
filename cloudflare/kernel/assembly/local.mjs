// Local workerd uses the serving classes and K19's real builder core.
// Only the builder's transport changes: Node locally, Container on the platform.
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { createWriteStream } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
export const freePort = () => new Promise((ok, fail) => {
  const server = createServer(); server.unref(); server.on('error', fail);
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => ok(port)); });
});
const load = async (path) => JSON.parse((await readFile(path, 'utf8')).replace(/^\s*\/\/.*$/gm, ''));
export async function startLocalAssembly({ state, port, vars = {}, deferMs = 1500 } = {}) {
  if (!state) throw new Error('an owned state directory is required');
  state = resolve(state); await mkdir(state, { recursive: true, mode: 0o700 });
  const generated = join(state, 'generated'); await mkdir(generated, { recursive: true });
  for (const entry of ['releases.js', 'action-releases.js']) {
    try { await readFile(join(generated, entry)); } catch { await writeFile(join(generated, entry), 'export const releases = {};\n'); }
  }
  port ??= await freePort();
  const url = `http://127.0.0.1:${port}`;
  const builderPort = await freePort();
  const token = randomBytes(24).toString('hex');
  const adminKey = vars.GONVEX_ADMIN_KEY ?? randomBytes(24).toString('hex');
  const common = { GONVEX_RUNTIME_VERSION: 'development', GONVEX_ADMIN_KEY: adminKey, GONVEX_PUBLIC_URL: url, GONVEX_ASSEMBLY_FIXTURE_KEY: adminKey, ...vars };
  const paths = [];
  for (const [name, file, entry] of [['kernel', join(root, 'wrangler.jsonc'), 'releases.js'], ['actions', join(here, 'actions.wrangler.jsonc'), 'action-releases.js'], ['sandbox', join(here, 'sandbox.wrangler.jsonc'), null]]) {
    const config = await load(file), dir = dirname(file);
    config.main = resolve(dir, config.main);
    config.alias = Object.fromEntries(Object.entries(config.alias ?? {}).map(([key, value]) => [key, resolve(dir, value)]));
    if (entry) config.alias[name === 'kernel' ? 'gonvex:releases' : 'gonvex:action-releases'] = join(generated, entry);
    config.vars = { ...config.vars, ...common };
    if (name === 'kernel') {
      config.services = config.services.filter((service) => service.binding !== 'RELEASE_BUILDER');
      Object.assign(config.vars, { GONVEX_RELEASE_BUILDER_URL: `http://127.0.0.1:${builderPort}`, GONVEX_RELEASE_BUILDER_TOKEN: token });
    }
    if (name === 'sandbox') { delete config.containers; config.vars.GONVEX_SANDBOX_ENABLED = vars.SANDBOX_CONTAINER_URL ? 'true' : 'false'; }
    const path = join(state, `${name}.json`);
    await writeFile(path, JSON.stringify(config), { mode: 0o600 }); paths.push(path);
  }
  const children = [];
  const logs = [];
  const launch = (name, command, args, env) => {
    const log = createWriteStream(join(state, `${name}.log`), { mode: 0o600 }); logs.push(log);
    const child = spawn(command, args, { cwd: root, env: { ...process.env, WRANGLER_SEND_METRICS: 'false', CI: '1', NO_COLOR: '1', ...env }, stdio: ['ignore', 'pipe', 'pipe'], detached: true });
    child.stdout.pipe(log); child.stderr.pipe(log); children.push(child); return child;
  };
  const kill = (signal) => { for (const child of children) { try { process.kill(-child.pid, signal); } catch {} } };
  const onExit = () => kill('SIGKILL'); process.once('exit', onExit);
  const stop = async () => { kill('SIGTERM'); await delay(500); kill('SIGKILL'); for (const log of logs) log.end(); process.removeListener('exit', onExit); };
  try {
    launch('builder', process.execPath, [join(root, 'control/operator/builder/server.mjs')], {
      GONVEX_RELEASE_BUILDER_TOKEN: token, GONVEX_KERNEL_URL: url, GONVEX_BUILDER_PORT: String(builderPort),
      GONVEX_KERNEL_GENERATED_DIR: generated, GONVEX_KERNEL_STAGING_DIR: join(state, 'release-pieces'), GONVEX_DEPLOY_DEFER_MS: String(deferMs),
    });
    const args = ['dev', ...paths.flatMap((path) => ['--config', path]), '--ip', '127.0.0.1', '--port', String(port), '--inspector-port', String(await freePort()), '--persist-to', join(state, 'persist'), '--log-level', 'warn', '--show-interactive-dev-session=false'];
    const worker = launch('workerd', join(root, 'node_modules/.bin/wrangler'), args, {});
    const deadline = Date.now() + 60_000;
    for (;;) {
      if (worker.exitCode !== null) throw new Error(`workerd exited ${worker.exitCode}; see ${join(state, 'workerd.log')}`);
      try { if ((await fetch(`${url}/healthz`)).ok) break; } catch {}
      if (Date.now() > deadline) throw new Error(`assembly did not become healthy; see ${join(state, 'workerd.log')}`);
      await delay(100);
    }
    return { url, adminKey, state, stop };
  } catch (error) { await stop(); throw error; }
}
