#!/usr/bin/env node
// K19 done_when, part 1: `gonvex dev --once` deploys an app (Whagons) to the
// local kernel. Starts the kernel and the builder, creates a project, syncs
// with the repository's own CLI, provisions a tenant, syncs again (the queued
// rollout), waits for the new Worker version and checks what it serves, then
// exercises the operator routes the dashboard reads. Writes a JSON report.
//
//   node router/dev/local/e2e.mjs --app <app checkout> --work <scratch dir> [--keep]
//
// The app checkout needs its dependencies installed (the CLI bundles its module).
import { spawn } from 'node:child_process';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { startLocalKernel } from './start.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(here, '../../../..');
const CLI = join(REPO, 'packages/gonvex/dist/index.js');

const flag = (name) => {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
};
const app = resolve(flag('--app') ?? '');
const work = resolve(flag('--work') ?? join(here, '.e2e'));
const keep = process.argv.includes('--keep');
const tenantId = flag('--tenant') ?? 'whagons-local';
const report = { startedAt: new Date().toISOString(), app, steps: [] };

async function step(name, fn) {
  const started = Date.now();
  process.stdout.write(`- ${name} ... `);
  try {
    const value = await fn();
    const ms = Date.now() - started;
    report.steps.push({ name, ok: true, ms, ...(value && typeof value === 'object' ? { value } : {}) });
    console.log(`ok (${ms} ms)`);
    return value;
  } catch (error) {
    report.steps.push({ name, ok: false, ms: Date.now() - started, error: String(error?.stack ?? error) });
    console.log(`FAILED\n${error?.stack ?? error}`);
    throw error;
  }
}

function run(command, args, options) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(command, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (chunk) => (output += chunk));
    child.stderr.on('data', (chunk) => (output += chunk));
    child.on('exit', (code) => (code === 0 ? resolveRun(output) : reject(new Error(`${command} ${args.join(' ')} exited ${code}:\n${output.slice(-4000)}`))));
  });
}

await rm(work, { recursive: true, force: true });
await mkdir(work, { recursive: true });
// A fresh kernel: no Worker version from an earlier run.
await rm(join(here, '.generated'), { recursive: true, force: true });
const dashboardAccount = 'operator@k19.local';
const dashboardPassword = 'k19-local-dashboard-password';
const kernel = await startLocalKernel({
  state: join(work, 'state'),
  logs: work,
  vars: { DASHBOARD_AUTH_ACCOUNT: dashboardAccount, DASHBOARD_AUTH_PASSWORD: dashboardPassword, GONVEX_DASHBOARD_SESSION_SECRET: 'k19-local-session-secret', GONVEX_SYNC_DEADLINE_MS: '1200000' },
});
report.kernel = { url: kernel.url };
const api = async (method, path, { token = kernel.adminKey, body } = {}) => {
  const response = await fetch(kernel.url + path, { method, headers: { authorization: `Bearer ${token}`, ...(body ? { 'content-type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
  const text = await response.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }
  if (!response.ok) throw new Error(`${method} ${path} -> ${response.status}: ${text.slice(0, 500)}`);
  return json;
};

let exitCode = 0;
try {
  const project = await step('create project (POST /dev/projects, admin key)', async () => {
    const created = await api('POST', '/dev/projects', { body: { name: 'Whagons (K19 local)', databaseMode: 'multiTenant' } });
    return { id: created.project.id, key: created.projectKey };
  });
  report.project = project.id;
  // A dashboard operator signed in with a session is not a global admin (D.3): it sees projects it owns or is a member of.
  await step('the dashboard operator becomes a project owner', async () => api('POST', `/dev/projects/${project.id}/members`, { body: { email: dashboardAccount, name: 'Operator', role: 'owner' } }));
  const cliArgs = [CLI, 'dev', '--once', '--runtime-url', kernel.url, '--project-id', project.id, '--key', project.key];

  const firstSync = await step('gonvex dev --once (install, no tenant yet)', async () => ({ output: (await run(process.execPath, cliArgs, { cwd: app })).trim().split('\n').slice(-3) }));
  const manifest = await step('GET /dev/manifest (project key)', async () => {
    const value = await api('GET', `/dev/manifest?project=${project.id}`, { token: project.key });
    return { functions: Object.keys(value.functions).length, module: value.module };
  });
  await step('POST /dev/tenants (provision the full schema from the active release)', async () => api('POST', '/dev/tenants', { body: { projectId: project.id, id: tenantId, name: 'Whagons local' } }));
  await step('gonvex dev --once (queued rollout to the tenant)', async () => ({ output: (await run(process.execPath, cliArgs, { cwd: app })).trim().split('\n').slice(-3) }));
  const served = await step('the new Worker version serves the release (K04 registry install)', async () => {
    for (let i = 0; i < 240; i += 1) {
      try {
        const response = await fetch(`${kernel.url}/_gonvex/k19/served?tenant=${tenantId}&project=${project.id}`, { headers: { authorization: `Bearer ${kernel.adminKey}` } });
        if (response.ok) {
          const value = await response.json();
          if (value.served?.hash === manifest.module.hash) {
            if (value.served.error) throw new Error(`the bundle did not load: ${value.served.error}`);
            return { served: value.served, migrationState: value.status.state, ledger: value.ledger.map((row) => row.name) };
          }
        }
      } catch (error) {
        if (String(error).includes('did not load')) throw error;
      }
      await delay(500);
    }
    throw new Error('the kernel never served the synced hash');
  });
  if (served.ledger.length === 0) throw new Error('the tenant ledger is empty');

  await step('operator routes the dashboard reads', async () => {
    const tables = await api('GET', `/dev/data/tables?project=${project.id}&tenant=${tenantId}`, { token: project.key });
    const sample = tables.tables.find((table) => table.name === 'tasks') ?? tables.tables[0];
    const rows = await api('GET', `/dev/data/tables/${sample.name}/rows?project=${project.id}&tenant=${tenantId}&limit=5`, { token: project.key });
    const tenants = await api('GET', `/dev/tenants?project=${project.id}`);
    const metrics = await api('GET', `/dev/metrics?project=${project.id}`);
    const errors = await api('GET', `/dev/errors/groups?project=${project.id}`);
    const projects = await api('GET', '/dev/projects');
    return {
      tables: tables.tables.length,
      sampleTable: sample.name,
      sampleColumns: rows.columns.length,
      sampleTotal: rows.total,
      tenants: tenants.tenants.map((t) => [t.id, t.provisioned]),
      metricFunctions: Object.keys(metrics.functions).length,
      errorGroups: errors.groups.length,
      projects: projects.projects.length,
    };
  });
  report.result = { firstSync, manifest, served };
  report.dashboard = { url: kernel.url, account: dashboardAccount, password: dashboardPassword, project: project.id, tenant: tenantId };
} catch {
  exitCode = 1;
} finally {
  report.finishedAt = new Date().toISOString();
  await writeFile(join(work, 'report.json'), JSON.stringify(report, null, 2));
  console.log(`report: ${join(work, 'report.json')}`);
  if (keep) {
    console.log(`kernel left running at ${kernel.url} (admin key in the report's dashboard block is not printed)`);
    report.adminKey = kernel.adminKey;
    await writeFile(join(work, 'report.json'), JSON.stringify(report, null, 2));
  } else {
    await kernel.stop();
  }
}
process.exitCode = exitCode;
if (!keep) process.exit(exitCode);
