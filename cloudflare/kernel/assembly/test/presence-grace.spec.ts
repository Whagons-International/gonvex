// 505: a Member's offline is published at the grace end on the real AssemblyTenant (DO SQLite, its
// multiplexed TenantAlarm), even while another alarm owner is firing and with no presence timer at all.
// On dev the offline was sometimes never published (-12) or late (-11 27 s) and no tenant alarm covered
// the grace end. Presence timers are dropped here, so the alarm is the only thing that can publish it.
import { env, runDurableObjectAlarm, runInDurableObject } from 'cloudflare:test';
import { afterEach, beforeEach, expect, it } from 'vitest';
import { buildReleasePlan, migrationsFromFiles } from '../../migrate';
import { manifest, files } from './catalog';
import { plans, type CompositionTenant } from './worker';
import type { TenantSession } from '../../session/ports';
import { setSpanTiming } from '../../delivery/spans';

type Stub = { dispatch(s: unknown, f: unknown): Promise<string[]>; presenceLeave(id: string, host: string, previous: unknown): Promise<void> };
type Diag = Record<string, unknown> & { event: string; t: number };
interface Internals {
 alarms: { register(source: unknown): void };
 presence: { options: { setTimer?: unknown }; hub: { config: { offlineGraceMs: number } }; nextAlarm(): number | null };
}
const bindings = env as unknown as { TENANT: DurableObjectNamespace };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const GATEWAY = 'gw-presence';

function session(tenantId: string, memberId: string): TenantSession {
 return {
  identity: { projectId: 'p', account: { id: `acc-${memberId}`, email: '', emailVerified: false, name: '', avatarUrl: '', provider: '' } },
  route: { projectId: 'p', tenantId, databaseUrl: '' },
  member: { id: memberId, accountId: `acc-${memberId}`, status: 'active', displayName: '', avatarUrl: '', role: 'admin', permissions: null, membershipRevision: 1 },
  admissionRevision: 1, delegation: null, developerSession: null, anonymous: false,
 } as TenantSession;
}
const view = (tenantId: string, connectionId: string, memberId: string) => ({ connectionId, host: GATEWAY, tenant: session(tenantId, memberId) });

// The tenant runs in this isolate: its presence-diag lines show what published B's offline, and when.
let lines: Diag[] = [];
const log = console.log;
beforeEach(() => {
 lines = [];
 setSpanTiming(true);
 console.log = (...args: unknown[]) => {
  try { const m = JSON.parse(String(args[0])); if (m?.diag === 'presence') { lines.push({ ...m, t: Date.now() }); return; } } catch { /* not a diag line */ }
  log(...args);
 };
});
afterEach(() => { setSpanTiming(false); console.log = log; });

const published = (member: string) => lines.find((l) => l.event === 'publish' && JSON.stringify(l.changes).includes(`${member}:offline`));

/**
 * A provisioned tenant whose presence timers never run, plus another alarm owner ("busy") that holds the
 * tenant alarm inside its run while `holding` is set, as the Action-call sweeps do on -12 and -11.
 */
async function tenant(opts: { graceMs?: number; provision?: boolean } = {}) {
 const name = `presence-${crypto.randomUUID()}`;
 const stub = bindings.TENANT.getByName(name);
 const busy = { next: null as number | null, holding: false, inside: false };
 let state!: DurableObjectState; let object!: CompositionTenant;
 await runInDurableObject(stub, async (o, s) => {
  object = o as CompositionTenant; state = s;
  const internals = object as unknown as Internals;
  if (opts.provision !== false) {
   const release = await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
   await object.activate('p', manifest, release.id);
   await (object as unknown as { setupScheduled(): Promise<void> }).setupScheduled();
   // X17m: the tenant checks a session's Member at attach; the fixture sessions are these Members' admissions.
   for (const id of ['A', 'B', 'C']) await object.addMember({ id, accountId: `acc-${id}`, displayName: '' });
   s.storage.sql.exec("UPDATE members SET role='admin', permissions='null', membership_revision=1");
   internals.alarms.register({ name: 'busy', nextAt: () => busy.next, run: async (now: number) => {
    busy.inside = true;
    while (busy.holding) await sleep(20);
    busy.inside = false; busy.next = now + 60_000; return 1;
   } });
  }
  internals.presence.options.setTimer = () => null; // every presence timer is lost
  if (opts.graceMs !== undefined) internals.presence.hub.config.offlineGraceMs = opts.graceMs;
 });
 return {
  name, stub: stub as unknown as Stub, busy,
  internals: () => object as unknown as Internals,
  alarmAt: () => runInDurableObject(stub, async () => state.storage.getAlarm()),
  async waitOffline(member: string, maxMs: number) {
   for (const end = Date.now() + maxMs; Date.now() < end && !published(member);) await sleep(50);
   return published(member);
  },
  /** Starts the busy owner's alarm and returns once it is inside its run. */
  async holdAlarm() {
   busy.holding = true; busy.next = Date.now();
   await runInDurableObject(stub, async () => { await state.storage.setAlarm(Date.now()); });
   while (!busy.inside) await sleep(10);
  },
 };
}

const graceOf = (connection: string) => (lines.find((l) => l.event === 'leave' && l.connection === connection)!.after as { graceUntil: number; due: number });

it('the leave lands while another alarm owner is firing: the alarm alone publishes B offline at the 10 s grace end', async () => {
 const t = await tenant();
 await t.stub.dispatch(view(t.name, 'a', 'A'), { type: 'presence.subscribe', id: 's' });
 await t.stub.dispatch(view(t.name, 'b', 'B'), { type: 'presence.set', state: 'active' });
 await t.holdAlarm();
 const left = Date.now();
 await t.stub.presenceLeave('b', GATEWAY, null);
 t.busy.holding = false;
 while (t.busy.inside) await sleep(10);
 await sleep(50);
 expect(await t.alarmAt()).toBeLessThanOrEqual(graceOf('b').graceUntil);
 const offline = await t.waitOffline('B', 13_000);
 expect(offline, 'B offline published').toBeDefined();
 expect(offline!.t - left).toBeGreaterThanOrEqual(10_000);
 expect(offline!.t - left).toBeLessThan(11_500);
 expect(lines.some((l) => l.event === 'timer')).toBe(false); // the alarm published it, not a timer
 expect(offline!.gateways).toEqual([GATEWAY]);
}, 30_000);

it('a window is pending when the socket closes (Away just before the close): the grace end is still on the alarm', async () => {
 const t = await tenant({ graceMs: 2_000 });
 await t.stub.dispatch(view(t.name, 'a', 'A'), { type: 'presence.subscribe', id: 's' });
 await t.stub.dispatch(view(t.name, 'b', 'B'), { type: 'presence.set', state: 'active' }); // leading edge
 await t.stub.dispatch(view(t.name, 'b', 'B'), { type: 'presence.set', state: 'idle' }); // the page hides: trailing window
 const left = Date.now();
 await t.stub.presenceLeave('b', GATEWAY, null); // then the socket closes inside the window
 const grace = graceOf('b');
 expect(grace.due).toBeLessThan(grace.graceUntil); // the next due time is the window, not the grace end
 expect(t.internals().presence.nextAlarm()).toBe(grace.graceUntil);
 expect(await t.alarmAt()).toBeLessThanOrEqual(grace.graceUntil);
 const offline = await t.waitOffline('B', 5_000);
 expect(offline, 'B offline published').toBeDefined();
 expect(offline!.t - left).toBeGreaterThanOrEqual(2_000);
 expect(offline!.t - left).toBeLessThan(3_500);
}, 30_000);

it('a presence alarm that runs less than a second before a grace end keeps that grace end on the alarm', async () => {
 const t = await tenant({ graceMs: 2_000 });
 await t.stub.dispatch(view(t.name, 'a', 'A'), { type: 'presence.subscribe', id: 's' });
 await t.stub.dispatch(view(t.name, 'c', 'C'), { type: 'presence.set', state: 'active' });
 await sleep(300);
 await t.stub.dispatch(view(t.name, 'b', 'B'), { type: 'presence.set', state: 'active' });
 await t.stub.presenceLeave('c', GATEWAY, null);
 await sleep(400);
 const left = Date.now();
 await t.stub.presenceLeave('b', GATEWAY, null); // B's grace ends 0.4 s after C's: C's alarm runs presence 0.4 s before it
 const offline = await t.waitOffline('B', 4_000);
 expect(published('C'), 'C offline published').toBeDefined();
 expect(offline, 'B offline published').toBeDefined();
 expect(offline!.t - left).toBeGreaterThanOrEqual(2_000);
 expect(offline!.t - left).toBeLessThan(3_500);
}, 30_000);

it('a tenant whose scheduler is not set up yet still publishes grace ends from its alarm', async () => {
 const t = await tenant({ provision: false, graceMs: 1_000 });
 await runInDurableObject(t.stub as never, async (object) => {
  const presence = (object as unknown as { presence: { frame(s: unknown, f: unknown): Promise<string[]>; leave(id: string, host: string): Promise<void> } }).presence;
  await presence.frame(view(t.name, 'a', 'A'), { type: 'presence.subscribe', id: 's' });
  await presence.frame(view(t.name, 'b', 'B'), { type: 'presence.set', state: 'active' });
  await presence.leave('b', GATEWAY);
 });
 expect(await t.alarmAt()).not.toBeNull();
 // The alarm fires on its own (no timer runs); run it by hand if it has not yet.
 const offline = await t.waitOffline('B', 2_500) ?? (await runDurableObjectAlarm(t.stub as never), await t.waitOffline('B', 500));
 expect(offline, 'B offline published').toBeDefined();
 expect(lines.some((l) => l.event === 'alarm')).toBe(true);
}, 30_000);
