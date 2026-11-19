// X13g (0094 B): a socket's agent Action runs in the tenant's resident ActionHost (Actions Worker script):
// warm reuse with one module evaluation, prewarm when the tenant gains its first subscriber, a cold path
// after eviction that still answers, standard Actions unchanged, and the host's limits and bindings.
import { env, evictDurableObject, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { buildReleasePlan, migrationsFromFiles } from '../../migrate';
import { manifest, files } from './catalog';
import { moduleEvaluations, plans, resetActionModule, type CompositionTenant } from './worker';
import { serviceSession } from '../../principals/grant';
import type { SocketView } from '../../session/ports';
const bindings = env as unknown as { TENANT: DurableObjectNamespace; ACTION_HOST: DurableObjectNamespace };
type HostState = { held: boolean; holding: boolean; prewarms: number };
const host = (tenant: string) => (bindings.ACTION_HOST as unknown as { getByName(n: string): { hostState(): Promise<HostState> } }).getByName(tenant);
async function fixture(run: (object: CompositionTenant, state: DurableObjectState) => Promise<void>) {
 const stub = bindings.TENANT.getByName(`x13g-${crypto.randomUUID()}`);
 await runInDurableObject(stub, async (object, state) => {
  const tenant = object as CompositionTenant;
  const release = await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
  await tenant.activate('p', manifest, release.id);
  await (tenant as unknown as { setupScheduled(): Promise<void> }).setupScheduled();
  await run(tenant, state);
 });
}
function view(tenantId: string): SocketView {
 const tenant = serviceSession({ principalId: 'x13g', route: { projectId: 'p', tenantId, databaseUrl: 'sqlite://test' }, functions: [], maxDelegationSeconds: 0 });
 return { connectionId: crypto.randomUUID(), host: 'fixture', actionGeneration: 1, tenant, connection: { projectId: 'p', identity: tenant.identity, tenant, impersonationId: '', authToken: '' }, subscriptionEpoch: 1, publicTransport: false,
  negotiated: { snapshotPages: false, noOldValues: false, fieldPatches: false, batchFrames: false, nativeRows: false, setDigest: false, telemetryBatch: false, presence: false } };
}
const runs = (state: DurableObjectState) => state.storage.sql.exec<{ state: string; attempts: number; dispatch_count: number }>('SELECT state,attempts,dispatch_count FROM _gonvex_st_action_runs').toArray();

it('X13g warm reuse: two agent Actions from a socket run in the ActionHost, with one module evaluation', async () => fixture(async (object, state) => {
 resetActionModule();
 const socket = view(state.id.name!);
 for (const id of ['first', 'second']) {
  const replies = await object.dispatch(socket, { type: 'action.call', id, path: 'probe.agentNoop', args: {} });
  expect(replies.map((r) => JSON.parse(r))).toEqual([{ type: 'action.result', id, path: 'probe.agentNoop', result: null }]);
 }
 expect(moduleEvaluations()).toBe(1);
 expect(await host(state.id.name!).hostState()).toMatchObject({ held: true, holding: true });
 // Still durable inline runs (X13), claimed once each.
 expect(runs(state).filter((r) => r.state === 'done').length).toBe(2);
}));

it('X13g prewarm: the tenant\'s first subscriber holds the host and evaluates the module before any Action', async () => fixture(async (object, state) => {
 resetActionModule();
 (object as unknown as { subscribersChanged(holding: boolean): void }).subscribersChanged(true);
 let s: HostState = { held: false, holding: false, prewarms: 0 };
 for (let i = 0; i < 100 && !s.prewarms; i++) { await new Promise((r) => setTimeout(r, 5)); s = await host(state.id.name!).hostState(); }
 expect(s).toEqual({ held: true, holding: true, prewarms: 1 });
 expect(moduleEvaluations()).toBe(1);
 const replies = await object.dispatch(view(state.id.name!), { type: 'action.call', id: 'after-prewarm', path: 'probe.agentNoop', args: {} });
 expect(JSON.parse(replies[0]!)).toMatchObject({ type: 'action.result', result: null });
 expect(moduleEvaluations()).toBe(1);
 // The last subscriber went: the host is released and may hibernate.
 (object as unknown as { subscribersChanged(holding: boolean): void }).subscribersChanged(false);
 for (let i = 0; i < 100 && s.held; i++) { await new Promise((r) => setTimeout(r, 5)); s = await host(state.id.name!).hostState(); }
 expect(s).toMatchObject({ held: false, holding: false });
}));

it('X13g a held host with no subscribers left at the tenant lets itself go at its next check', async () => fixture(async (object, state) => {
 // The tenant never sent `release` (it went away); its check answers "not wanted" (no subscriptions held).
 (object as unknown as { subscribersChanged(holding: boolean): void }).subscribersChanged(true);
 let s = await host(state.id.name!).hostState();
 for (let i = 0; i < 100 && !s.held; i++) { await new Promise((r) => setTimeout(r, 5)); s = await host(state.id.name!).hostState(); }
 expect(s.held).toBe(true);
 expect(await object.actionHostWanted()).toBe(false);
 for (let i = 0; i < 200 && s.held; i++) { await new Promise((r) => setTimeout(r, 10)); s = await host(state.id.name!).hostState(); }
 expect(s).toMatchObject({ held: false, holding: false });
}));

it('X13g eviction fallback: after the host is evicted, the next agent Action takes the cold path and answers correctly', async () => fixture(async (object, state) => {
 const socket = view(state.id.name!);
 expect(JSON.parse((await object.dispatch(socket, { type: 'action.call', id: 'before', path: 'probe.agentNoop', args: {} }))[0]!)).toMatchObject({ result: null });
 await evictDurableObject(host(state.id.name!) as unknown as DurableObjectStub);
 resetActionModule(); // a new isolate would evaluate again
 expect((await host(state.id.name!).hostState()).held).toBe(false);
 expect(JSON.parse((await object.dispatch(socket, { type: 'action.call', id: 'after', path: 'probe.agentNoop', args: {} }))[0]!)).toEqual({ type: 'action.result', id: 'after', path: 'probe.agentNoop', result: null });
 expect(moduleEvaluations()).toBe(1);
 expect(await host(state.id.name!).hostState()).toMatchObject({ held: true, holding: true });
}));

it('X13g limits: the host applies the same capability checks; standard Actions keep the ActionWorker', async () => fixture(async (object, state) => {
 const socket = view(state.id.name!);
 const refused = JSON.parse((await object.dispatch(socket, { type: 'action.call', id: 'secret', path: 'probe.agentSecret', args: {} }))[0]!);
 expect(refused).toMatchObject({ type: 'action.error', id: 'secret' });
 expect(refused.error).toContain('project secrets require the Control Plane');
 const before = await host(state.id.name!).hostState();
 expect(JSON.parse((await object.dispatch(socket, { type: 'action.call', id: 'std', path: 'probe.noop', args: {} }))[0]!)).toMatchObject({ type: 'action.result', result: null });
 expect(await host(state.id.name!).hostState()).toEqual(before);
}));

// Review of #509 (a): the fallback follows evidence (did the run start here?), not an error message.
function withHost(object: CompositionTenant, host: Record<string, unknown>) {
 (object as unknown as { env: Record<string, unknown> }).env = { ...(object as unknown as { env: Record<string, unknown> }).env, ACTION_HOST: { getByName: () => host } };
}

it('X13g review (a): a host that fails before starting the run (e.g. an Actions script rolled back without ActionHost) falls back to the ActionWorker', async () => fixture(async (object, state) => {
 withHost(object, { call: async () => { throw new Error('Durable Object class "ActionHost" is not exported by script gonvex-cf-dev-k22-actions'); }, hold: async () => ({}), release: async () => ({}) });
 const replies = await object.dispatch(view(state.id.name!), { type: 'action.call', id: 'rolled-back', path: 'probe.agentNoop', args: {} });
 expect(replies.map((r) => JSON.parse(r))).toEqual([{ type: 'action.result', id: 'rolled-back', path: 'probe.agentNoop', result: null }]);
 for (let i = 0; i < 100 && runs(state).some((r) => r.state !== 'done'); i++) await new Promise((r) => setTimeout(r, 10)); // completion follows the reply (X13b)
 expect(runs(state).map((r) => [r.state, r.attempts])).toEqual([['done', 1]]); // one run, by the ActionWorker
}));

it('X13g review (a): a host that fails after its run started is not retried (the Action runs at most once)', async () => fixture(async (object, state) => {
 withHost(object, { call: async (input: { session: unknown; path: string; args: unknown; inlineOwner: string }) => {
  await object.startCall({ v: 1, session: input.session as never, path: input.path, args: input.args, owner: input.inlineOwner });
  throw new Error('Durable Object reset because its code was updated');
 }, hold: async () => ({}), release: async () => ({}) });
 await expect(object.dispatch(view(state.id.name!), { type: 'action.call', id: 'reset', path: 'probe.agentNoop', args: {} })).rejects.toThrow('reset');
 // Exactly one run: started by the host, left for the X13b sweep; no second run by the ActionWorker.
 expect(runs(state).map((r) => [r.state, r.attempts, r.dispatch_count])).toEqual([['live', 1, 0]]);
}));

it('X13g review (c): a reordered older release cannot drop a newer hold', async () => fixture(async (object, state) => {
 const stub = (bindings.ACTION_HOST as unknown as { getByName(n: string): { hold(r: unknown): Promise<unknown>; release(r?: unknown): Promise<unknown>; hostState(): Promise<HostState> } }).getByName(state.id.name!);
 await stub.hold({ v: 1, projectId: 'p', tenantId: state.id.name!, generation: 5 });
 await stub.release({ v: 1, generation: 4 }); // sent before the hold, delivered after it
 expect(await stub.hostState()).toMatchObject({ held: true, holding: true });
 await stub.hold({ v: 1, projectId: 'p', tenantId: state.id.name!, generation: 3 }); // stale hold changes nothing either
 await stub.release({ v: 1, generation: 6 });
 expect(await stub.hostState()).toMatchObject({ held: false, holding: false });
 // A legacy caller without generations still works.
 await stub.hold({ v: 1, projectId: 'p', tenantId: state.id.name! });
 await stub.release();
 expect(await stub.hostState()).toMatchObject({ held: false });
}));
