// X17o: the tenant runs a Reducer call forwarded behind a socket's Action only after that Action's inline result,
// and refuses it (FORWARD_REFUSED) for an unknown, pending or failed Action.
import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { buildReleasePlan, migrationsFromFiles } from '../../migrate';
import { manifest, files } from './catalog';
import { plans, resetActionModule, type CompositionTenant } from './worker';
import { serviceSession } from '../../principals/grant';
import type { SocketView } from '../../session/ports';
import { FORWARD_REFUSED } from '../../session/forward';
const bindings = env as unknown as { TENANT: DurableObjectNamespace };
type Internals = { trackAction(connectionId: string, actionId: string, inline: Promise<boolean>): void };
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
function view(tenantId: string, connectionId = 'conn-1', afterAction?: string): SocketView {
  const tenant = serviceSession({ principalId: 'x17o', route: { projectId: 'p', tenantId, databaseUrl: 'sqlite://test' }, functions: [], maxDelegationSeconds: 0 });
  return { connectionId, host: 'fixture', actionGeneration: 1, tenant, connection: { projectId: 'p', identity: tenant.identity, tenant, impersonationId: '', authToken: '' }, subscriptionEpoch: 1, publicTransport: false,
    negotiated: { snapshotPages: false, noOldValues: false, fieldPatches: false, batchFrames: false, nativeRows: false, setDigest: false, telemetryBatch: false, presence: false },
    ...(afterAction === undefined ? {} : { afterAction }) };
}
const call = (id: string) => ({ type: 'reducer.call' as const, id, path: 'probe.write', args: {}, idempotencyKey: `k-${id}` });
const tenantName = (object: CompositionTenant) => (object as unknown as { ctx: DurableObjectState }).ctx.id.name!;

it('X17o holds a forwarded call until its Action returns an inline result, then runs it', async () => fixture(async (object) => {
  let settle!: (inline: boolean) => void;
  (object as unknown as Internals).trackAction('conn-1', 'a1', new Promise<boolean>((r) => { settle = r; }));
  let done = false;
  const reply = object.dispatch(view(tenantName(object), 'conn-1', 'a1'), call('c1')).then((out) => { done = true; return out; });
  await new Promise((r) => setTimeout(r, 50));
  expect(done).toBe(false); // held while the Action's dispatch is out
  settle(true);
  const out = await reply;
  expect(out).not.toEqual([FORWARD_REFUSED]);
  expect(JSON.parse(out[0]!)).toMatchObject({ id: 'c1' }); // reached the Reducer path (its own result or error)
}));

it('X17o refuses a forwarded call behind an unknown, pending or failed Action, or for a non-Reducer frame', async () => fixture(async (object) => {
  const name = tenantName(object), internals = object as unknown as Internals;
  expect(await object.dispatch(view(name, 'conn-1', 'unknown'), call('c1'))).toEqual([FORWARD_REFUSED]);
  internals.trackAction('conn-1', 'pending', Promise.resolve(false)); // went pending or failed: only an inline result releases
  expect(await object.dispatch(view(name, 'conn-1', 'pending'), call('c2'))).toEqual([FORWARD_REFUSED]);
  internals.trackAction('conn-2', 'a1', Promise.resolve(true)); // another socket's Action of the same id
  expect(await object.dispatch(view(name, 'conn-1', 'a1'), call('c3'))).toEqual([FORWARD_REFUSED]);
  internals.trackAction('conn-1', 'a2', Promise.resolve(true));
  expect(await object.dispatch(view(name, 'conn-1', 'a2'), { type: 'query.call', id: 'q1', path: 'probe.noop', args: {} })).toEqual([FORWARD_REFUSED]);
}));

it('X17o a real socket Action registers its dispatch: a call forwarded behind it runs after its result', async () => fixture(async (object) => {
  resetActionModule();
  const name = tenantName(object);
  const order: string[] = [];
  const action = object.dispatch(view(name), { type: 'action.call', id: 'a1', path: 'probe.agentNoop', args: {} }).then((out) => { order.push('action'); return out; });
  const forwarded = object.dispatch(view(name, 'conn-1', 'a1'), call('c1')).then((out) => { order.push('reducer'); return out; });
  const [a, c] = await Promise.all([action, forwarded]);
  expect(JSON.parse(a[0]!)).toMatchObject({ type: 'action.result', id: 'a1' });
  expect(c).not.toEqual([FORWARD_REFUSED]);
  expect(order).toEqual(['action', 'reducer']);
}));
