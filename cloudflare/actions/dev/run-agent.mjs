#!/usr/bin/env node
// Runs one agent Action locally in a Workflow step for a long time (K12
// done_when: "a 30-minute agent Action completes locally"), the way S4's
// scenario 1 runs it: event-stream turns of 20 s (100 events every 200 ms)
// with 10 s pauses, renewals every 30 s on a 5-minute lease.
//
//   node actions/dev/run-agent.mjs [seconds=1800] [out.json]     (from cloudflare/)
//
// Starts the conformance upstream on 127.0.0.1:47812 and `wrangler dev` on a
// free port with a fresh state directory, polls the run every 30 s, and writes
// the outcome, the renewal cadence and the tenant's rows to out.json.
//
// `wrangler dev` rebuilds and restarts on any source change, which kills
// in-flight work (S4 saw the same). So it runs from a frozen copy of the
// sources (reflinked), never from the working tree.
import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { cp, mkdir, mkdtemp, symlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { startUpstream } from '../../conformance/scenarios/actions-upstream.mjs';

const seconds = Number(process.argv[2] ?? 1800);
const out = process.argv[3] ?? join(tmpdir(), `k12-agent-${seconds}s.json`);
const source = fileURLToPath(new URL('../../', import.meta.url));
const work = await mkdtemp(join(tmpdir(), 'k12-dev-'));
// Frozen sources: cloudflare/{actions,kernel,conformance} and the SDK they import, laid out as in the repo.
const cloudflare = join(work, 'repo/cloudflare');
for (const dir of ['actions', 'kernel', 'conformance/fixtures', 'conformance/scenarios']) await cp(join(source, dir), join(cloudflare, dir), { recursive: true });
for (const dir of ['src', 'runtime-contract']) await cp(join(source, '../packages/module-sdk', dir), join(work, 'repo/packages/module-sdk', dir), { recursive: true });
await symlink(join(source, 'node_modules'), join(cloudflare, 'node_modules'));
await mkdir(join(work, 'state'));
const port = await new Promise((resolve) => { const s = createServer(); s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); }); });
const upstream = await startUpstream();
const log = createWriteStream(join(work, 'wrangler.log'));
const child = spawn(join(cloudflare, 'node_modules/.bin/wrangler'), ['dev', '--config', 'actions/dev/wrangler.jsonc', '--ip', '127.0.0.1', '--port', String(port), '--persist-to', join(work, 'state'), '--show-interactive-dev-session=false'],
  { cwd: cloudflare, env: { ...process.env, WRANGLER_SEND_METRICS: 'false', CI: '1' }, stdio: ['ignore', 'pipe', 'pipe'], detached: true });
child.stdout.pipe(log); child.stderr.pipe(log);
const base = `http://127.0.0.1:${port}`;
const stop = async () => { try { process.kill(-child.pid, 'SIGTERM'); } catch {} await upstream.close(); };
try {
  for (let i = 0; ; i++) {
    try { if ((await fetch(base + '/run?id=none')).status) break; } catch {}
    if (i > 300) throw new Error(`wrangler dev did not start; see ${work}/wrangler.log`);
    await sleep(200);
  }
  const tenant = `agent-${Date.now()}`;
  const started = Date.now();
  const enqueued = await (await fetch(`${base}/enqueue?tenant=${tenant}`, { method: 'POST', body: JSON.stringify({ path: 'long.agent', args: { seconds, events: 100, delayMs: 200, pauseMs: 10_000 } }) })).json();
  console.log(`enqueued ${enqueued.id} (workflow ${enqueued.instance}); log ${work}/wrangler.log`);
  let status;
  for (;;) {
    await sleep(30_000);
    status = await (await fetch(`${base}/workflow?instance=${enqueued.instance}`)).json();
    const run = await (await fetch(`${base}/run?tenant=${tenant}&id=${enqueued.id}`)).json();
    console.log(`${Math.round((Date.now() - started) / 1000)} s: workflow ${status.status}; row ${JSON.stringify(run.row && { state: run.row.state, status: run.row.status, attempts: run.row.attempts, locked_at: run.row.locked_at })}`);
    if (['complete', 'errored', 'terminated'].includes(status.status)) {
      const result = { seconds, wallSeconds: (Date.now() - started) / 1000, enqueued, workflow: status, tenant: run, measuredAt: new Date().toISOString() };
      await writeFile(out, JSON.stringify(result, null, 1));
      console.log(`wrote ${out}`);
      console.log(JSON.stringify(status.output, null, 1));
      break;
    }
  }
  if (status.status !== 'complete') process.exitCode = 1;
} finally {
  await stop();
}
