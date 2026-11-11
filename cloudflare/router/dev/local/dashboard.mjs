#!/usr/bin/env node
// K19 done_when, part 2: the Gonvex dashboard works against the local kernel.
// Builds apps/dashboard, serves it with its own server.mjs (which proxies
// /dev/* and the operator sockets to the kernel, as in production), signs in
// with the kernel's configured dashboard account in headless Chromium, and
// visits the pages backed by K19's routes: projects, data (tables and rows of
// a tenant), logs, errors and settings. Every /dev response the browser sees
// and every console error is recorded; screenshots go next to the report.
//
//   node router/dev/local/dashboard.mjs --report <e2e report.json> --playwright <path to the playwright package>
//
// Needs the kernel from `e2e.mjs --keep` still running. Headless Chromium runs
// with --disable-gpu (arch3 rule).
import { spawn } from 'node:child_process';
import { readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { freePort } from './start.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(here, '../../../..');
const DASHBOARD = join(REPO, 'apps/dashboard');
const flag = (name) => {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
};
const reportPath = resolve(flag('--report') ?? '');
const e2e = JSON.parse(await readFile(reportPath, 'utf8'));
const out = dirname(reportPath);
const { url: kernelUrl, account, password, project, tenant } = e2e.dashboard;
const require = createRequire(resolve(flag('--playwright') ?? '') + '/');
const loadPlaywright = () => {
  try {
    return require('playwright');
  } catch {
    return require('@playwright/test');
  }
};
const { chromium } = loadPlaywright();

const run = (command, args, options) =>
  new Promise((resolveRun, reject) => {
    const child = spawn(command, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (chunk) => (output += chunk));
    child.stderr.on('data', (chunk) => (output += chunk));
    child.on('exit', (code) => (code === 0 ? resolveRun(output) : reject(new Error(`${command} exited ${code}:\n${output.slice(-3000)}`))));
  });

const port = await freePort();
const origin = `http://127.0.0.1:${port}`;
const dist = join(out, 'dashboard-dist');
console.log('- building the dashboard');
await run(join(DASHBOARD, 'node_modules/.bin/vite'), ['build', '--outDir', dist, '--emptyOutDir'], {
  cwd: DASHBOARD,
  env: { ...process.env, VITE_GONVEX_URL: origin, VITE_GONVEX_PASSWORD_LOGIN_ENABLED: 'true', VITE_GONVEX_AUTH_ENABLED: 'true' },
});
console.log('- serving it with apps/dashboard/server.mjs');
const server = spawn(process.execPath, [join(DASHBOARD, 'server.mjs')], {
  cwd: DASHBOARD,
  env: { ...process.env, PORT: String(port), DASHBOARD_STATIC_ROOT: dist, DASHBOARD_AUTH_ENABLED: 'true', DASHBOARD_SESSION_SECRET: 'k19-dashboard-cookie-secret', DASHBOARD_COOKIE_SECURE: 'false', GONVEX_RUNTIME_URL: kernelUrl },
  stdio: ['ignore', 'pipe', 'pipe'],
});
for (let i = 0; ; i += 1) {
  try {
    if ((await fetch(`${origin}/healthz`)).ok) break;
  } catch {}
  if (i > 100) throw new Error('dashboard server did not start');
  await delay(100);
}

// The tenant table with the most rows, to open in the data browser.
const tables = await (await fetch(`${kernelUrl}/dev/data/tables?project=${project}&tenant=${tenant}`, { headers: { authorization: `Bearer ${e2e.adminKey}` } })).json();
const busiest = [...(tables.tables ?? [])].sort((a, b) => b.rowCount - a.rowCount)[0];
const result = { origin, kernelUrl, tenantTables: tables.tables?.length ?? 0, openedTable: busiest ? { name: busiest.name, rowCount: busiest.rowCount } : null, pages: [], devResponses: [], consoleErrors: [], sockets: [] };
const browser = await chromium.launch({ args: ['--disable-gpu'] });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on('console', (message) => {
    if (message.type() === 'error') result.consoleErrors.push(message.text().slice(0, 300));
  });
  page.on('response', (response) => {
    const url = new URL(response.url());
    if (url.pathname.startsWith('/dev/') || url.pathname.startsWith('/api/dashboard/')) result.devResponses.push({ method: response.request().method(), path: `${url.pathname}${url.search}`.slice(0, 160), status: response.status() });
  });
  page.on('websocket', (socket) => {
    const entry = { url: socket.url().replace(/key=[^&]+/, 'key=…'), framesReceived: 0, firstFrame: '' };
    result.sockets.push(entry);
    socket.on('framereceived', (frame) => {
      entry.framesReceived += 1;
      if (!entry.firstFrame) entry.firstFrame = String(frame.payload).slice(0, 160);
    });
  });

  const shot = async (name) => {
    const file = join(out, `dashboard-${name}.png`);
    await page.screenshot({ path: file, fullPage: false });
    result.pages.push({ name, screenshot: file, url: page.url() });
  };

  try {
  await page.goto(origin, { waitUntil: 'networkidle' });
  await page.fill('input[type="email"]', account);
  await page.fill('input[type="password"]', password);
  await page.click('button[type="submit"]');
  await page.waitForSelector('text=Whagons (K19 local)', { timeout: 30_000 });
  await delay(1500);
  await shot('1-projects');

  await page.getByRole('button', { name: 'Open project' }).first().click();
  await delay(4000);
  await shot('2-overview');
  result.navigation = await page.locator('nav a, nav button, aside a, aside button').allInnerTexts().catch(() => []);

  for (const [label, name] of [['Data', '3-data'], ['Logs', '5-logs'], ['Errors', '6-errors'], ['Settings', '7-settings']]) {
    const link = page.getByRole('button', { name: label, exact: true }).or(page.getByRole('link', { name: label, exact: true })).or(page.getByText(label, { exact: true })).first();
    await link.click();
    await delay(2500);
    await shot(name);
    if (name === '3-data' && busiest) {
      // The tenant database and its busiest table, as the data page's own URL state selects them.
      const target = new URL(page.url());
      target.searchParams.set('db', tenant);
      target.searchParams.set('table', busiest.name);
      await page.goto(target.toString(), { waitUntil: 'networkidle' });
      await delay(3000);
      result.dataStatus = (await page.locator('body').innerText()).match(/Viewing tenant database[^\n]*|Tenant database unavailable[^\n]*/i)?.[0] ?? null;
      await shot('4-data-table');
    }
  }
  } catch (error) {
    result.error = String(error?.message ?? error);
    await shot('failure').catch(() => undefined);
    result.bodyText = (await page.locator('body').innerText().catch(() => '')).slice(0, 2000);
    process.exitCode = 1;
  }
} finally {
  await browser.close();
  server.kill('SIGTERM');
}
result.failedDev = result.devResponses.filter((r) => r.status >= 400);
await writeFile(join(out, 'dashboard-report.json'), JSON.stringify(result, null, 2));
console.log(JSON.stringify({ error: result.error, bodyText: result.bodyText, tenantTables: result.tenantTables, openedTable: result.openedTable, dataStatus: result.dataStatus, pages: result.pages.map((p) => p.name), devResponses: result.devResponses.length, failedDev: result.failedDev, consoleErrors: result.consoleErrors.length, sockets: result.sockets.map((s) => [s.url, s.framesReceived]) }, null, 2));
console.log(`report: ${join(out, 'dashboard-report.json')}`);
void project;
