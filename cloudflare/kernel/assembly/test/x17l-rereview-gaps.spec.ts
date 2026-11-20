// #542 re-review (head 04a8e83e): two paths still let a pre-change cached read reach a subscription after the
// invalidation it should have lost to. Both tests FAIL on 04a8e83e.
import { describe, expect, it } from 'vitest';
import { AssemblyGateway, assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { controlReadsFor } from '../../session/control-read-cache';

const socket = (account: string) => ({ connectionId: 'c', host: 'h', connection: { projectId: 'p', identity: { account: { id: account } }, tenant: null, impersonationId: '', authToken: '' } });
const value = (replies: string[]) => (JSON.parse(replies[0]!) as { result: { version: number } }).result.version;

describe('#542 re-review: a cached reply is never returned after an invalidation it straddles', () => {
  it('subscribe hit whose watch lapsed: controlChanged during the awaited watch -> the reply is live, not the dropped entry', async () => {
    let version = 1;
    let holdWatch = false;
    const held: Array<() => void> = [];
    const control = {
      watch: async () => { if (holdWatch) await new Promise<void>((resolve) => held.push(resolve)); },
      sessionDispatch: async (_s: unknown, frame: { id: string; path: string }) =>
        [JSON.stringify({ type: 'query.result', id: frame.id, path: frame.path, reason: 'initial', result: { version } })],
    };
    const env = { CONTROL: { getByName: () => control }, TENANT: { getByName: () => ({}) }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
    const ports = assemblyPorts(env);
    // Socket 1 subscribes (miss): version 1 is cached under the subscribe key.
    await ports.control.dispatch(socket('a') as never, { type: 'query.subscribe', id: 's0', path: 'control.tenants.mine', args: {} } as never);
    // Its registration lapses (watchedRecently expired a read-latency before the entry, or the 1024-project map evicted it).
    controlReadsFor(env).forgetWatched('p');
    // Socket 2 subscribes: a hit that is not recently registered, so it awaits Control's watch before the cached reply.
    holdWatch = true;
    const subscribe = ports.control.dispatch(socket('a') as never, { type: 'query.subscribe', id: 's1', path: 'control.tenants.mine', args: {} } as never);
    await new Promise((resolve) => setTimeout(resolve, 0));
    // Meanwhile another client's write commits and its notice arrives (AssemblyGateway.controlChanged's first step).
    // The subscription is not yet in controlQueries, so that notice's refresh does not cover it.
    version = 2;
    controlReadsFor(env).invalidate('p');
    for (const resolve of held.splice(0)) resolve();
    // Returning the pre-invalidation entry makes subscription.last = version 1 with no later refresh: stale until the
    // project's NEXT notify (no TTL bounds a subscription).
    expect(value(await subscribe)).toBe(2);
  });

  it("Control restart (refreshControlWatches 'restarted'): cached reads are dropped before the sessions refresh", async () => {
    const control = { runtimeInstance: async () => 'a-new-control-instance', watch: async () => {} };
    const env = { CONTROL: { getByName: () => control } };
    const reads = controlReadsFor(env);
    // A pre-change read is cached; the write's notice sat in Control's in-memory outbox and was lost in the reset.
    reads.put('kp', 'p', { version: 1 }, reads.generation('p'));
    let refreshedWhileCached: boolean | undefined;
    const session = { closed: false, record: { connection: { projectId: 'p' } }, controlChanged: async () => { refreshedWhileCached ??= reads.get('kp') !== undefined; } };
    const waits: Promise<unknown>[] = [];
    const self = { env, controlInstance: 'a-previous-control-instance', host: { sessions: new Map([[{}, session]]) }, ctx: { id: { toString: () => 'g' }, waitUntil: (p: Promise<unknown>) => waits.push(p) } };
    await (AssemblyGateway.prototype as unknown as { refreshControlWatches(this: unknown): Promise<void> }).refreshControlWatches.call(self);
    await Promise.all(waits);
    // The restart refresh exists to recover lost notices. Its query.calls go through the cached port, so with the
    // entry still present they return version 1, match subscription.last and send nothing: stale until the next notify.
    expect(refreshedWhileCached).toBe(false);
  });
});
