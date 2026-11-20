// X17o: the tenant runs a Reducer call forwarded behind a socket's Action only after that Action's inline result,
// at execution-time scope, and refuses it (FORWARD_REFUSED) for an unknown, reused, pending or failed Action.
import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { buildReleasePlan, migrationsFromFiles } from '../../migrate';
import { manifest, files } from './catalog';
import { plans, resetActionModule, type CompositionTenant } from './worker';
import { serviceSession } from '../../principals/grant';
import type { SocketView } from '../../session/ports';
import { FORWARD_REFUSED } from '../../session/forward';
const bindings = env as unknown as { TENANT: DurableObjectNamespace };
type Internals = { trackAction(connectionId: string, actionId: string, generation: number | undefined, inline: Promise<boolean>): void; socketAction: (input: unknown) => Promise<unknown> };
async function fixture(run: (object: CompositionTenant) => Promise<void>) {
  const stub = bindings.TENANT.getByName(`x17o-${crypto.randomUUID()}`);
  await runInDurableObject(stub, async (object) => {
    const tenant = object as CompositionTenant;
    const release = await buildReleasePlan(await migrationsFromFiles(files)); plans.add(release);
    await tenant.activate('p', manifest, release.id);
    await (tenant as unknown as { setupScheduled(): Promise<void> }).setupScheduled();
    await run(tenant);
  });
}
function view(tenantId: string, options: { connectionId?: string; afterAction?: string; actionGeneration?: number; subscriptionEpoch?: number; forwardContract?: string | null } = {}): SocketView {
  const tenant = serviceSession({ principalId: 'x17o', route: { projectId: 'p', tenantId, databaseUrl: 'sqlite://test' }, functions: [], maxDelegationSeconds: 0 });
  const { connectionId = 'conn-1', afterAction, actionGeneration = 1, subscriptionEpoch = 1, forwardContract = null } = options;
  return { connectionId, host: 'fixture', actionGeneration, tenant, connection: { projectId: 'p', identity: tenant.identity, tenant, impersonationId: '', authToken: '' }, subscriptionEpoch, publicTransport: false,
    negotiated: { snapshotPages: false, noOldValues: false, fieldPatches: false, batchFrames: false, nativeRows: false, setDigest: false, telemetryBatch: false, presence: false },
    ...(afterAction === undefined ? {} : { afterAction, forwardContract }) };
}
const call = (id: string) => ({ type: 'reducer.call' as const, id, path: 'probe.write', args: {}, idempotencyKey: `k-${id}` });
const tenantName = (object: CompositionTenant) => (object as unknown as { ctx: DurableObjectState }).ctx.id.name!;
const reachedReducer = (out: unknown) => Array.isArray(out) && out.length > 0 && out[0] !== FORWARD_REFUSED;
const within = <T>(promise: Promise<T>, ms: number) => Promise.race([promise, new Promise<'timeout'>((r) => setTimeout(() => r('timeout'), ms))]);

it('X17o holds a forwarded call until its Action returns an inline result, then runs it', async () => fixture(async (object) => {
  let settle!: (inline: boolean) => void;
  (object as unknown as Internals).trackAction('conn-1', 'a1', 1, new Promise<boolean>((r) => { settle = r; }));
  let done = false;
  const reply = object.dispatch(view(tenantName(object), { afterAction: 'a1' }), call('c1')).then((out) => { done = true; return out; });
  await new Promise((r) => setTimeout(r, 50));
  expect(done).toBe(false);
  settle(true);
  const out = await reply;
  expect(reachedReducer(out)).toBe(true);
  expect(JSON.parse(out[0]!)).toMatchObject({ id: 'c1' });
}));

it('X17o refuses a forwarded call behind an unknown Action, another socket\'s Action, or for a non-Reducer frame', async () => fixture(async (object) => {
  const name = tenantName(object), internals = object as unknown as Internals;
  expect(await object.dispatch(view(name, { afterAction: 'unknown' }), call('c1'))).toEqual([FORWARD_REFUSED]);
  internals.trackAction('conn-2', 'a1', 1, Promise.resolve(true));
  expect(await object.dispatch(view(name, { afterAction: 'a1' }), call('c3'))).toEqual([FORWARD_REFUSED]);
  internals.trackAction('conn-1', 'a2', 1, Promise.resolve(true));
  expect(await object.dispatch(view(name, { afterAction: 'a2' }), { type: 'query.call', id: 'q1', path: 'probe.noop', args: {} })).toEqual([FORWARD_REFUSED]);
}));

it('X17o a real socket Action registers its dispatch: a call forwarded behind it runs after its result', async () => fixture(async (object) => {
  resetActionModule();
  const name = tenantName(object);
  const order: string[] = [];
  const action = object.dispatch(view(name), { type: 'action.call', id: 'a1', path: 'probe.agentNoop', args: {} }).then((out) => { order.push('action'); return out; });
  const forwarded = object.dispatch(view(name, { afterAction: 'a1' }), call('c1')).then((out) => { order.push('reducer'); return out; });
  const [a, c] = await Promise.all([action, forwarded]);
  expect(JSON.parse(a[0]!)).toMatchObject({ type: 'action.result', id: 'a1' });
  expect(reachedReducer(c)).toBe(true);
  expect(order).toEqual(['action', 'reducer']);
}));

it('X17o a real pending Action (its dispatch returns nothing) refuses the forward', async () => fixture(async (object) => {
  const name = tenantName(object);
  (object as unknown as Internals).socketAction = async () => ({ pending: true, id: 'run-1' });
  const action = object.dispatch(view(name), { type: 'action.call', id: 'a1', path: 'probe.agentNoop', args: {} });
  const forwarded = object.dispatch(view(name, { afterAction: 'a1' }), call('c1'));
  expect(await action).toEqual([]);
  expect(await forwarded).toEqual([FORWARD_REFUSED]);
}));

it('X17o a real throwing Action releases the forward (refused), never leaving it held', async () => fixture(async (object) => {
  const name = tenantName(object);
  (object as unknown as Internals).socketAction = async () => { await new Promise((r) => setTimeout(r, 20)); throw new Error('action host failed'); };
  const action = object.dispatch(view(name), { type: 'action.call', id: 'a1', path: 'probe.agentNoop', args: {} }).then(() => 'resolved', () => 'threw');
  const forwarded = object.dispatch(view(name, { afterAction: 'a1' }), call('c1'));
  expect(await action).toBe('threw');
  expect(await within(forwarded, 2000)).toEqual([FORWARD_REFUSED]);
}));

it('review A: a forwarded call whose socket closed (connectionReset) while it was held never reaches the Reducer', async () => fixture(async (object) => {
  const name = tenantName(object);
  let settle!: (inline: boolean) => void;
  (object as unknown as Internals).trackAction('conn-1', 'a1', 1, new Promise<boolean>((r) => { settle = r; }));
  const reply = object.dispatch(view(name, { afterAction: 'a1' }), call('c1')).then((out) => out, (error: unknown) => error);
  await new Promise((r) => setTimeout(r, 20));
  await object.connectionReset('conn-1', 2); // gateway close -> resetSubscriptions -> connectionReset(next epoch)
  settle(true);
  expect(reachedReducer(await reply)).toBe(false);
}));

it("review B: a forward behind a reused Action id does not run on the earlier dispatch's settled entry", async () => fixture(async (object) => {
  resetActionModule();
  const name = tenantName(object);
  const first = await object.dispatch(view(name, { actionGeneration: 1 }), { type: 'action.call', id: 'a1', path: 'probe.agentNoop', args: {} });
  expect(JSON.parse(first[0]!)).toMatchObject({ type: 'action.result', id: 'a1' });
  // id a1 reused (generation 2); its dispatch has not reached the tenant yet, the forward has.
  const out = await object.dispatch(view(name, { actionGeneration: 2, afterAction: 'a1' }), call('c1'));
  expect(out).toEqual([FORWARD_REFUSED]);
}));

it('review R5 (fix a): a forward checked against another client contract than the active module is refused, nothing committed', async () => fixture(async (object) => {
  const name = tenantName(object);
  (object as unknown as Internals).trackAction('conn-1', 'a1', 1, Promise.resolve(true));
  expect(await object.dispatch(view(name, { afterAction: 'a1', forwardContract: '7' }), call('c1'))).toEqual([FORWARD_REFUSED]);
  (object as unknown as Internals).trackAction('conn-1', 'a2', 1, Promise.resolve(true));
  expect(reachedReducer(await object.dispatch(view(name, { afterAction: 'a2' }), call('c2')))).toBe(true); // same contract (none): runs
}));
