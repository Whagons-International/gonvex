#!/usr/bin/env node
// Starts the K19 local kernel: the release builder (Node) and `wrangler dev`
// of local/wrangler.jsonc, wired to each other. Prints the runtime URL and the
// admin key, then keeps running until interrupted.
//
//   node router/dev/local/start.mjs [--port 8787] [--state <dir>]
//
// Then, from an app checkout:  gonvex dev --once --runtime-url <url> --project-id <id> --key <project key>
// (create the project first: POST <url>/dev/projects with the admin key).
import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { createServer } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

const here = dirname(fileURLToPath(import.meta.url));
const CLOUDFLARE = resolve(here, '../../..');

export const freePort = () =>
  new Promise((resolvePort, reject) => {
    const server = createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(() => resolvePort(port));
    });
  });

/**
 * Starts the builder and the kernel. `vars` are added to the kernel's vars
 * (dashboard account, session secret, ...). Returns the URLs, keys and a stop().
 */
export async function startLocalKernel({ port, state, logs, vars = {}, deferMs = 1500 } = {}) {
  port ??= await freePort();
  const builderPort = await freePort();
  const inspector = await freePort();
  // Outside the tree `wrangler dev` watches: a write there reloads workerd.
  state = resolve(state ?? join(process.env.XDG_CACHE_HOME ?? join(homedir(), '.cache'), 'gonvex-k19-local'));
  logs = resolve(logs ?? state);
  await mkdir(state, { recursive: true });
  await mkdir(logs, { recursive: true });
  const adminKey = vars.GONVEX_ADMIN_KEY ?? `k19-admin-${randomBytes(12).toString('hex')}`;
  const builderToken = randomBytes(24).toString('hex');
  const url = `http://127.0.0.1:${port}`;
  const generated = join(here, '.generated');
  const children = [];
  // Children run in their own process groups; never leave them behind when this process ends.
  process.once('exit', () => {
    for (const child of children) {
      try {
        process.kill(-child.pid, 'SIGKILL');
      } catch {}
    }
  });

  const builderLog = createWriteStream(join(logs, 'release-builder.log'));
  const builder = spawn(process.execPath, [join(CLOUDFLARE, 'control/operator/builder/server.mjs')], {
    cwd: CLOUDFLARE,
    env: {
      ...process.env,
      GONVEX_RELEASE_BUILDER_TOKEN: builderToken,
      GONVEX_KERNEL_URL: url,
      GONVEX_BUILDER_PORT: String(builderPort),
      GONVEX_KERNEL_GENERATED_DIR: generated,
      GONVEX_KERNEL_STAGING_DIR: join(state, 'releases-staging'),
      GONVEX_DEPLOY_DEFER_MS: String(deferMs),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
  });
  builder.stdout.pipe(builderLog);
  builder.stderr.pipe(builderLog);
  children.push(builder);
  for (let i = 0; ; i += 1) {
    try {
      if ((await fetch(`http://127.0.0.1:${builderPort}/healthz`)).ok) break;
    } catch {}
    if (i > 100) throw new Error(`release builder did not start; see ${join(logs, 'release-builder.log')}`);
    await delay(100);
  }

  const kernelVars = {
    GONVEX_ADMIN_KEY: adminKey,
    GONVEX_RELEASE_BUILDER_URL: `http://127.0.0.1:${builderPort}`,
    GONVEX_RELEASE_BUILDER_TOKEN: builderToken,
    ...vars,
  };
  const args = ['dev', '--config', join(here, 'wrangler.jsonc'), '--ip', '127.0.0.1', '--port', String(port), '--inspector-port', String(inspector), '--persist-to', state, '--log-level', 'warn', '--show-interactive-dev-session=false'];
  for (const [key, value] of Object.entries(kernelVars)) args.push('--var', `${key}:${value}`);
  const kernelLog = createWriteStream(join(logs, 'wrangler.log'));
  const kernel = spawn(join(CLOUDFLARE, 'node_modules/.bin/wrangler'), args, {
    cwd: here,
    env: { ...process.env, WRANGLER_SEND_METRICS: 'false', CI: '1', NO_COLOR: '1' },
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: true,
  });
  kernel.stdout.pipe(kernelLog);
  kernel.stderr.pipe(kernelLog);
  children.push(kernel);
  const deadline = Date.now() + 120_000;
  for (;;) {
    if (kernel.exitCode !== null) throw new Error(`wrangler dev exited (${kernel.exitCode}); see ${join(logs, 'wrangler.log')}`);
    try {
      if ((await fetch(`${url}/healthz`)).ok) break;
    } catch {}
    if (Date.now() > deadline) throw new Error('the local kernel did not become healthy within 120 s');
    await delay(250);
  }

  const stop = async () => {
    for (const child of children) {
      try {
        process.kill(-child.pid, 'SIGTERM');
      } catch {}
    }
    await delay(500);
    for (const child of children) {
      try {
        process.kill(-child.pid, 'SIGKILL');
      } catch {}
    }
  };
  return { url, adminKey, builderUrl: `http://127.0.0.1:${builderPort}`, state, logs, stop };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const flag = (name) => {
    const index = process.argv.indexOf(name);
    return index >= 0 ? process.argv[index + 1] : undefined;
  };
  const kernel = await startLocalKernel({ port: flag('--port') ? Number(flag('--port')) : 8787, state: flag('--state') });
  console.log(JSON.stringify({ url: kernel.url, adminKey: kernel.adminKey, logs: kernel.logs }, null, 2));
  const shutdown = async () => {
    await kernel.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
