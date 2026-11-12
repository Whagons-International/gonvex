#!/usr/bin/env node
/**
 * K21: runs the bridge's own live Go tests (`TestBridgeE2E*` in whagons5-client
 * `compatibility/legacy-convex-v5.1.29-bridge/`) against a local backend, and
 * reports where each one stops.
 *
 * Starts a conformance backend (`cloudflare-principals` by default: K05 sockets in
 * front of K06's control object and K07's tenant object; or `rust`), deploys fixture
 * generation 1, creates a tenant, and runs
 *   go test -count=1 -run '^TestBridgeE2E' -v
 * with BRIDGE_E2E_GONVEX_URL, _PROJECT_ID and _TENANT pointing at it, and
 * BRIDGE_E2E_REQUIRED=1, so a skip is a failure.
 *
 * BRIDGE_E2E_TOKEN must be a Firebase ID token for a Member of that tenant. The
 * bridge signs in only through `control.auth.exchangeExternalToken` with
 * provider `firebase`. Pass a real token with `--token` (for example from the
 * Firebase Auth Emulator, against a Rust runtime built with
 * `test-firebase-auth-emulator`). Without one, a JWT-shaped placeholder shows
 * the first step each backend refuses.
 *
 * Usage (repo root built with `pnpm build:packages`; cloudflare deps installed):
 *   node cloudflare/bridge-support/verify/bridge-e2e.mjs \
 *     --bridge <whagons5-client>/compatibility/legacy-convex-v5.1.29-bridge [--backend cloudflare-principals|rust] [--token <id token>]
 */
import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';

const run = promisify(execFile);
const HERE = dirname(fileURLToPath(import.meta.url));
const CONFORMANCE = resolve(HERE, '../../conformance');

const arg = (name, fallback) => { const i = process.argv.indexOf(`--${name}`); return i > 0 ? process.argv[i + 1] : fallback; };
const bridgeDir = arg('bridge');
const kind = arg('backend', 'cloudflare-principals');
if (!bridgeDir) throw new Error('--bridge <whagons5-client>/compatibility/legacy-convex-v5.1.29-bridge is required');
// A JWT-shaped placeholder, not a credential: header {"alg":"none"}, payload {"sub":"k21-placeholder"}.
const token = arg('token', 'eyJhbGciOiJub25lIn0.eyJzdWIiOiJrMjEtcGxhY2Vob2xkZXIifQ.placeholder');

const { buildFixture } = await import(pathToFileURL(join(CONFORMANCE, 'src/fixture.mjs')).href);
const registry = await import(pathToFileURL(join(CONFORMANCE, 'src/backends/index.mjs')).href);
const Backend = (registry.BACKENDS ?? registry.default ?? registry)[kind];
if (!Backend) throw new Error(`unknown backend ${kind}`);

const work = await mkdtemp(join(process.env.K21_VERIFY_TMP ?? tmpdir(), 'k21-bridge-e2e-'));
const backend = new Backend({ name: kind, workDir: join(work, kind) });
let exitCode = 1;
try {
  const { manifest } = await buildFixture(work, 1);
  await backend.start();
  const { projectId } = await backend.createProject('k21-bridge-e2e');
  await backend.deploy(projectId, { ...manifest, project: projectId });
  const tenant = 'k21-bridge-e2e-a';
  await backend.createTenant(projectId, tenant, 'K21 bridge e2e');
  await backend.createMember(projectId, tenant, { email: 'bridge-e2e@conformance.invalid', password: 'k21-bridge-e2e-password', name: 'bridge' });
  const go = await run('go', ['test', '-count=1', '-run', '^TestBridgeE2E', '-v', '.'], {
    cwd: bridgeDir, maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, BRIDGE_E2E_REQUIRED: '1', BRIDGE_E2E_GONVEX_URL: backend.httpUrl, BRIDGE_E2E_PROJECT_ID: projectId, BRIDGE_E2E_TENANT: tenant, BRIDGE_E2E_TOKEN: token },
  }).then((r) => ({ ok: true, out: r.stdout }), (e) => ({ ok: false, out: `${e.stdout ?? ''}${e.stderr ?? ''}` }));
  const lines = go.out.split('\n').filter((l) => /^(=== RUN|--- (PASS|FAIL|SKIP)|\s+\S+_test\.go:\d+:)/.test(l));
  console.log(`backend ${kind}, project ${projectId}, tenant ${tenant}`);
  for (const line of lines) console.log(line.replace(token, '<token>'));
  console.log(go.ok ? 'TestBridgeE2E: PASS' : 'TestBridgeE2E: FAIL');
  exitCode = go.ok ? 0 : 1;
} finally {
  await backend.stop();
  if (!process.argv.includes('--keep')) await rm(work, { recursive: true, force: true });
}
process.exit(exitCode);
