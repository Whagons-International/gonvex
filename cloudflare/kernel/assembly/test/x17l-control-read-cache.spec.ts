// X17l: the gateway caches the Control reads a cold load repeats; notify, writes and the TTL invalidate.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { CONTROL_READ_MS, ControlReadCache, controlReadsFor } from '../../session/control-read-cache';

const socket = (account: string, extra: Record<string, unknown> = {}) => ({ connectionId: 'c', host: 'h', connection: { projectId: 'p', identity: { account: { id: account } }, tenant: null, impersonationId: '', authToken: '', ...extra } });
function world() {
  const calls = { dispatch: 0, watch: 0 };
  let version = 1, failing = false;
  const control = {
    watch: async () => { calls.watch++; },
    sessionDispatch: async (_s: unknown, frame: { type: string; id: string; path: string }) => {
      calls.dispatch++;
      if (frame.type === 'action.call') { version++; return [JSON.stringify({ type: 'action.result', id: frame.id, result: null })]; }
      return [failing ? JSON.stringify({ type: 'query.error', id: frame.id, path: frame.path, error: 'no' })
        : JSON.stringify({ type: 'query.result', id: frame.id, path: frame.path, reason: 'initial', result: { version } })];
    },
  };
  const env = { CONTROL: { getByName: () => control }, TENANT: { getByName: () => ({}) }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
  return { env, ports: assemblyPorts(env), calls, bump: () => { version++; }, fail: (f: boolean) => { failing = f; } };
}
const call = (id: string, path = 'control.tenants.mine') => ({ type: 'query.call', id, path, args: {} }) as never;
const sub = (id: string, path = 'control.tenants.mine') => ({ type: 'query.subscribe', id, path, args: {} }) as never;
const result = (replies: string[]) => JSON.parse(replies[0]!) as { id: string; result: { version: number } };

afterEach(() => { vi.useRealTimers(); });

describe('X17l gateway Control read cache', () => {
  it('a repeated read of the same caller is served from the cache, under its own id', async () => {
    const w = world();
    expect(result(await w.ports.control.dispatch(socket('a') as never, call('q1')))).toMatchObject({ id: 'q1', result: { version: 1 } });
    expect(result(await w.ports.control.dispatch(socket('a') as never, call('q2')))).toMatchObject({ id: 'q2', result: { version: 1 } });
    expect(w.calls.dispatch).toBe(1);
    // Another account, another path, or a subscribe (it is permission-checked) is not served from it.
    await w.ports.control.dispatch(socket('b') as never, call('q3'));
    await w.ports.control.dispatch(socket('a') as never, call('q4', 'control.developer.status'));
    await w.ports.control.dispatch(socket('a') as never, sub('q5'));
    expect(w.calls.dispatch).toBe(4);
    expect(w.calls.watch).toBe(1); // the miss registered the watcher
    // A cached subscribe within the registration window does not register again (#542 review: not vacuous)...
    await w.ports.control.dispatch(socket('a') as never, sub('q6'));
    expect(w.calls.dispatch).toBe(4);
    expect(w.calls.watch).toBe(1);
    // ...and one whose registration lapsed does, before the cached reply.
    controlReadsFor(w.env).forgetWatched('p');
    await w.ports.control.dispatch(socket('a') as never, sub('q7'));
    expect(w.calls.dispatch).toBe(4);
    expect(w.calls.watch).toBe(2);
  });

  it('read-your-writes: a Control write through this gateway drops the project first', async () => {
    const w = world();
    await w.ports.control.dispatch(socket('a') as never, call('q1'));
    await w.ports.control.dispatch(socket('a') as never, { type: 'action.call', id: 'w1', path: 'control.tenants.setActive', args: {} } as never);
    expect(result(await w.ports.control.dispatch(socket('a') as never, call('q2'))).result.version).toBe(2);
  });

  it("another client's change: a notify (controlChanged) drops it; a lost notify expires within CONTROL_READ_MS", async () => {
    const w = world();
    await w.ports.control.dispatch(socket('a') as never, call('q1'));
    w.bump();
    controlReadsFor(w.env).invalidate('p'); // what AssemblyGateway.controlChanged does first
    expect(result(await w.ports.control.dispatch(socket('a') as never, call('q2'))).result.version).toBe(2);
    vi.useFakeTimers({ toFake: ['Date'] });
    w.bump(); // no notify this time
    expect(result(await w.ports.control.dispatch(socket('a') as never, call('q3'))).result.version).toBe(2);
    vi.setSystemTime(Date.now() + CONTROL_READ_MS);
    expect(result(await w.ports.control.dispatch(socket('a') as never, call('q4'))).result.version).toBe(3);
  });

  it('never caches errors, impersonated sessions or other paths', async () => {
    const w = world();
    w.fail(true);
    await w.ports.control.dispatch(socket('a') as never, call('q1'));
    await w.ports.control.dispatch(socket('a') as never, call('q2'));
    expect(w.calls.dispatch).toBe(2);
    w.fail(false);
    await w.ports.control.dispatch(socket('a', { impersonationId: 'imp' }) as never, call('q3'));
    await w.ports.control.dispatch(socket('a', { impersonationId: 'imp' }) as never, call('q4'));
    await w.ports.control.dispatch(socket('a') as never, call('q5', 'control.support.listTenants'));
    await w.ports.control.dispatch(socket('a') as never, call('q6', 'control.support.listTenants'));
    expect(w.calls.dispatch).toBe(6);
  });

  it('each gateway object has its own cache', () => {
    expect(controlReadsFor(world().env)).not.toBe(controlReadsFor(world().env));
  });

  it('the TTL bound (unit)', () => {
    let now = 0; const cache = new ControlReadCache(() => now);
    cache.put('k', 'p', 1, cache.generation('p')); now = CONTROL_READ_MS - 1; expect(cache.get('k')).toEqual({ result: 1 }); now = CONTROL_READ_MS; expect(cache.get('k')).toBeUndefined();
  });
});

describe('#542 review: generation and byte bound (unit)', () => {
  it('a read started before an invalidation is not stored', () => {
    const cache = new ControlReadCache();
    const before = cache.generation('p');
    cache.invalidate('p');
    expect(cache.put('k', 'p', 1, before)).toBe(false);
    expect(cache.get('k')).toBeUndefined();
    expect(cache.put('k', 'p', 1, cache.generation('p'))).toBe(true);
    expect(cache.get('k')).toEqual({ result: 1 });
  });
  it('the memory bound is the serialized size', () => {
    const cache = new ControlReadCache(() => 0, CONTROL_READ_MS, 8_000);
    const big = 'x'.repeat(400); // ~0.8 KB per entry
    for (let i = 0; i < 50; i++) cache.put(`k${i}`, 'p', big, 0);
    expect(cache.totalBytes).toBeLessThanOrEqual(8_000);
    expect(cache.size).toBeLessThan(50);
    expect(cache.get('k49')).toEqual({ result: big }); // newest kept, oldest evicted
    expect(cache.get('k0')).toBeUndefined();
    expect(cache.put('huge', 'p', 'y'.repeat(2_000), 0)).toBe(false); // one entry is at most an eighth of the budget
  });
});

describe("X17l the gateway drops a project's cached reads on controlChanged", () => {
  it('AssemblyGateway.controlChanged(project) invalidates that project only', async () => {
    const { env, runInDurableObject } = await import('cloudflare:test');
    const gateways = (env as unknown as { GATEWAY: DurableObjectNamespace }).GATEWAY;
    await runInDurableObject(gateways.getByName('x17l-gw'), async (instance: unknown) => {
      const gateway = instance as { controlChanged(p: string): Promise<void>; env: object };
      const reads = controlReadsFor(gateway.env);
      reads.put('kp', 'p', 1, reads.generation('p')); reads.put('kq', 'q', 2, reads.generation('q'));
      await gateway.controlChanged('p');
      expect(reads.get('kp')).toBeUndefined();
      expect(reads.get('kq')).toEqual({ result: 2 });
    });
  });
});
