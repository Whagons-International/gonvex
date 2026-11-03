/**
 * Single-path load scenarios on thin protocol clients.
 *
 * Connects LOAD_USERS users (the same accounts, tenants and collections as
 * run.ts) and then drives exactly one operation at a fixed offered rate, so
 * each server path gets a stable number before mixed runs:
 *
 *   LOAD_SCENARIO=heartbeat  presence.beat from random users
 *   LOAD_SCENARIO=snapshot   open a task detail window (5 collections) on a
 *                            random visible task, wait until all are ready,
 *                            close it after LOAD_DWELL_MS (default 0)
 *   LOAD_SCENARIO=edit       tasks.rename into the largest tenant only; also
 *                            measures delivery of the new name to every other
 *                            connected user who can see the task
 *
 * Arrivals are Poisson at LOAD_RATE operations/second, independent of server
 * speed (open loop). Latency counts from each operation's planned start, so
 * server queueing shows up instead of lowering the load. A plan that finds
 * LOAD_MAX_INFLIGHT (default 2000) operations still running counts as a
 * failed operation.
 *
 *   LOAD_SCENARIO=edit LOAD_RATE=20 LOAD_USERS=200 LOAD_SECONDS=180 node paths.ts
 */
import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { quantiles, loopback } from './safety.mjs';
import { ThinClient, defaultPool } from './thin-client.mjs';

const scenario = process.env.LOAD_SCENARIO || 'heartbeat';
if (!['heartbeat', 'snapshot', 'edit'].includes(scenario)) throw Error('LOAD_SCENARIO must be heartbeat, snapshot or edit');
const target = Number(process.env.LOAD_USERS || 26), rate = Number(process.env.LOAD_RATE || 10);
const seconds = Number(process.env.LOAD_SECONDS || 180), ramp = Number(process.env.LOAD_RAMP_SECONDS || 60);
const dwellMs = Number(process.env.LOAD_DWELL_MS || 0), maxInFlight = Number(process.env.LOAD_MAX_INFLIGHT || 2000);
const mounts = process.env.LOAD_PATH_MOUNTS || 'standard';
if (!(target >= 1 && rate > 0 && seconds > 0)) throw Error('Invalid load bounds');

const app = process.env.WHAGONS_CHECKOUT!;
const require = createRequire(app + '/package.json');
const { control } = await import(pathToFileURL(require.resolve('@gonvex/client')).href);
const { api } = await import(pathToFileURL(app + '/gonvex/_generated/api.ts').href);
const executor = await readFile(app + '/gonvex/_generated/local-executor.ts', 'utf8');
const clientContract = JSON.parse(executor.match(/clientContract: (\{[^}]+\})/)![1]);
const url = loopback(process.env.LOAD_URL || 'http://127.0.0.1:8080').origin;
const auth = loopback(process.env.LOAD_AUTH_URL || 'http://127.0.0.1:9099').origin;
const { projectId, fixtures } = JSON.parse(await readFile(process.env.LOAD_STATE!, 'utf8'));
const pause = (ms: number) => new Promise(r => setTimeout(r, ms));
const until = async (cond: () => boolean, ms: number) => { const end = Date.now() + ms; while (!cond()) { if (Date.now() > end) return false; await pause(20); } return true; };

// Same tenant assignment as run.ts (production write skew).
const weights = fixtures.map((_: any, i: number) => process.env.LOAD_ALL_TENANTS_ACTIVE === 'true' ? 1 : ([39, 39, 11, 6, 2, 2, 1][i] || 0));
const total = weights.reduce((a: number, b: number) => a + b, 0);
const chooseTenant = (i: number) => { let x = ((i * 0.61803398875) % 1) * total; return fixtures[weights.findIndex((w: number) => (x -= w) < 0)] || fixtures[0]; };
// Edits target the largest tenant only: pick its users from the prepared pool.
const largest = fixtures[0];
const userIndexes: number[] = [];
for (let i = 0; userIndexes.length < target && i < 100000; i++) if (scenario !== 'edit' || chooseTenant(i) === largest) userIndexes.push(i);
const prepared = Number(process.env.LOAD_PREPARED_USERS || 100000);
if (userIndexes.some(i => i >= prepared)) throw Error(`Scenario needs accounts above LOAD_PREPARED_USERS=${prepared}; run run.ts prepare with more users`);

const samples: Record<string, { at: number; ms: number }[]> = {};
const errors: Record<string, number> = {};
const sample = (kind: string, ms: number, at: number) => (samples[kind] ??= []).push({ at, ms });
const error = (e: any) => { const k = String(e?.message || e).slice(0, 500); errors[k] = (errors[k] || 0) + 1; };
const identity = async (email: string) => {
  const body = { email, password: 'local-synthetic-password', returnSecureToken: true };
  const r = await fetch(auth + '/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=local', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  const b: any = await r.json(); if (!r.ok) throw Error('Emulator identity failed'); return b.idToken;
};
const call = (c: any, ref: any, args: any) => c.reducer({ ...ref, localExecution: undefined }, args, { offline: 'reject' });
const baseRefs = [api.presence.list, api.workspaces.list, api.statuses.statuses, api.tags.tags, api.members.listMembers, api.members.listMemberTeams,
  api.taskResources.taskUsers, api.taskResources.taskTags, api.workspacePreferences.workspacePreferences, api.approvals.taskApprovalInstances];

type User = { i: number; c: ThinClient; tenant: string; member: string; tasks: any; ready: boolean };
const users: User[] = [];
const pendingEdits = new Map<string, { name: string; at: number; tenant: string; waiting: Set<User> }>();
const cpu0 = process.cpuUsage(), wall0 = performance.now();
const loop = monitorEventLoopDelay({ resolution: 10 }); loop.enable();
let peakRss = 0; const rssTimer = setInterval(() => { peakRss = Math.max(peakRss, process.memoryUsage().rss); }, 2000); rssTimer.unref();

const connectUser = async (i: number) => {
  const f = chooseTenant(i), email = `load-user-${i}@example.invalid`, at = performance.now();
  const c = new ThinClient(url.replace('http:', 'ws:') + '/ws', { project: projectId, clientContract });
  c.connect();
  try {
    const grant = await c.action(control.auth.exchangeExternalToken, { provider: 'firebase', token: await identity(email), tenantId: f.tenant });
    await c.authenticate({ project: projectId, token: grant.accessToken, tenant: f.tenant });
    const u: User = { i, c, tenant: f.tenant, member: grant.tenants.find((t: any) => t.id === f.tenant)?.memberId, tasks: undefined, ready: false };
    if (mounts === 'standard') for (const ref of baseRefs) c.watchReplica(ref, {}).onUpdate(() => {});
    u.tasks = c.watchReplica(api.tasks.recentWorkspaceTasks, {});
    u.tasks.onUpdate(() => {
      if (!u.ready && u.tasks.status().isUpToDate) { u.ready = true; sample('initialLoad', performance.now() - at, Date.now()); }
      if (!pendingEdits.size) return;
      for (const [id, edit] of pendingEdits) {
        if (!edit.waiting.has(u)) continue;
        const row = u.tasks.localReplicaGet(id);
        if (row && row.name === edit.name) { edit.waiting.delete(u); sample('taskDelivery', Date.now() - edit.at, edit.at); }
      }
    });
    users.push(u);
    if (!await until(() => u.ready, 120000)) error(Error('Initial task replica did not complete'));
  } catch (e) { error(e); c.close(); }
};

// ---- ramp ---------------------------------------------------------------------
console.log(JSON.stringify({ scenario, target, rate, seconds, phase: 'ramp' }));
await Promise.all(userIndexes.map(async (i, k) => { await pause(k * ramp * 1000 / userIndexes.length); await connectUser(i); }));
const connected = users.filter(u => u.ready).length;
console.log(JSON.stringify({ phase: 'hold', connected }));

// ---- operations ----------------------------------------------------------------
const pick = <T,>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];
const open = (u: User) => ((u.tasks.localReplicaResult() || []) as any[]).filter(r => r.statusId !== chooseTenant(u.i).completedStatusId);
const operation = async (plannedPerf: number, plannedWall: number) => {
  const u = pick(users.filter(x => x.ready));
  if (!u) throw Error('No connected user');
  if (scenario === 'heartbeat') {
    await call(u.c, api.presence.beat, {});
    sample('heartbeat', performance.now() - plannedPerf, plannedWall);
  } else if (scenario === 'snapshot') {
    const t = pick((u.tasks.localReplicaResult() || []) as any[]);
    if (!t) throw Error('User has no visible task');
    const refs: [any, any][] = [[api.tasks.get, { id: t._id }], [api.taskResources.listTaskNotes, { taskId: t._id }], [api.taskResources.listTaskLogs, { taskId: t._id }],
      [api.taskResources.listTaskViewsByTaskId, { taskId: t._id }], [api.approvals.listTaskApprovals, { taskId: t._id }]];
    const ws = refs.map(([ref, args]) => { const w = u.c.watchReplica(ref, args); const stop = w.onUpdate(() => {}); return { w, stop }; });
    try {
      if (!await until(() => ws.every(x => x.w.status().isUpToDate), 30000)) throw Error('Detail window never became current');
      sample('snapshotOpen', performance.now() - plannedPerf, plannedWall);
      if (dwellMs) await pause(dwellMs);
    } finally { for (const x of ws) x.stop(); }
  } else {
    const t = pick(open(u));
    if (!t) throw Error('User has no open task');
    const name = 'Path edit ' + randomUUID().slice(0, 8);
    const waiting = new Set(users.filter(x => x !== u && x.ready && x.tenant === u.tenant && x.tasks.localReplicaGet(t._id)));
    const edit = { name, at: plannedWall, tenant: u.tenant, waiting };
    pendingEdits.set(t._id, edit);
    await call(u.c, api.tasks.rename, { taskId: t._id, name });
    sample('save', performance.now() - plannedPerf, plannedWall);
    // Delivery is sampled by subscribers; leave 15 s for stragglers, then count misses.
    setTimeout(() => { if (pendingEdits.get(t._id) === edit) pendingEdits.delete(t._id); if (edit.waiting.size) error(Error(`Edit not delivered within 15 s to ${edit.waiting.size} subscribers`)); }, 15000).unref();
  }
};

const start = Date.now(), end = start + seconds * 1000;
let inFlight = 0, planned = 0, completed = 0, overflow = 0;
const pending = new Set<Promise<void>>();
let next = performance.now();
while (true) {
  next += -Math.log(Math.max(Math.random(), 1e-9)) * 1000 / rate;
  const wait = next - performance.now();
  if (wait > 0) await pause(wait);
  if (Date.now() >= end) break;
  planned++;
  if (inFlight >= maxInFlight) { overflow++; error(Error('Open-loop overflow: too many operations in flight')); continue; }
  inFlight++;
  const plannedPerf = next, plannedWall = Date.now() - (performance.now() - next);
  const p = operation(plannedPerf, plannedWall).then(() => { completed++; }, e => { error(e); }).finally(() => { inFlight--; pending.delete(p); });
  pending.add(p);
}
await Promise.race([Promise.all([...pending]), pause(30000)]);
await pause(16000); // let delivery stragglers time out into errors
loop.disable();
for (const u of users) u.c.close();

const steady = (kind: string) => quantiles((samples[kind] || []).filter(x => kind === "initialLoad" || (x.at >= start && x.at < end)).map(x => x.ms));
const kinds = Object.keys(samples);
const failed = Object.values(errors).reduce((a, b) => a + b, 0);
const latencyMs = Object.fromEntries(kinds.map(k => [k, { samples: (samples[k] || []).filter(x => k === "initialLoad" || (x.at >= start && x.at < end)).length, ...steady(k) }]));
const p95 = Number(process.env.LOAD_P95_MS || 500), p99 = Number(process.env.LOAD_P99_MS || 1000);
const gated = kinds.filter(k => k !== 'initialLoad');
const failingKinds = gated.filter(k => (latencyMs[k].p95 ?? Infinity) >= p95 || (latencyMs[k].p99 ?? Infinity) >= p99);
const errorRate = planned ? failed / planned : 1;
const cpu = process.cpuUsage(cpu0);
const result = {
  scenario, users: { requested: target, connected }, offered: { ratePerSecond: rate, planned, achievedPerSecond: completed / seconds }, completed, overflow,
  latencyMs, errors, errorRate, failingKinds, passed: connected === target && failingKinds.length === 0 && errorRate < 0.01,
  generator: { cpuSeconds: (cpu.user + cpu.system) / 1e6, wallSeconds: (performance.now() - wall0) / 1000, peakRssBytes: peakRss, eventLoopP99Ms: loop.percentile(99) / 1e6, rowPool: { distinct: defaultPool.size, ...defaultPool.stats }, thin: ThinClient.stats },
  steadyStartMs: start, steadyEndMs: end, rawSamples: samples,
};
console.log(JSON.stringify({ ...result, rawSamples: undefined }));
await writeFile(process.env.LOAD_RESULT || `paths-${scenario}.json`, JSON.stringify(result, null, 1));
process.exit(0);
