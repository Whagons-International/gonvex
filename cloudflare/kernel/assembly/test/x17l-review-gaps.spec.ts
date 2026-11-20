// #542 independent review: a Control read that was in flight when the project was invalidated must not be cached.
// Every test in the first block FAILS on 3d82470a.
import { describe, expect, it } from 'vitest';
import { assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { controlReadsFor } from '../../session/control-read-cache';

const socket = (account: string) => ({ connectionId: 'c', host: 'h', connection: { projectId: 'p', identity: { account: { id: account } }, tenant: null, impersonationId: '', authToken: '' } });

/** A Control whose reads take the state at call time and can be held before they reply (tenants.mine awaits tenant admits). */
function world() {
  let version = 1;
  const held: Array<() => void> = [];
  let hold = false;
  const control = {
    watch: async () => {},
    sessionDispatch: async (_s: unknown, frame: { type: string; id: string; path: string }) => {
      if (frame.type === 'action.call' || frame.type === 'reducer.call') {
        version++;
        return [JSON.stringify({ type: 'action.result', id: frame.id, path: frame.path, result: null }), JSON.stringify({ type: 'control.watermark', id: frame.id })];
      }
      const read = version; // Control read the rows now
      if (hold) await new Promise<void>((resolve) => held.push(resolve));
      return [JSON.stringify({ type: 'query.result', id: frame.id, path: frame.path, reason: 'initial', result: { version: read } })];
    },
  };
  const env = { CONTROL: { getByName: () => control }, TENANT: { getByName: () => ({}) }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
  return {
    env, ports: assemblyPorts(env),
    bump: () => { version++; },
    holdReads: (on: boolean) => { hold = on; },
    release: () => { for (const resolve of held.splice(0)) resolve(); },
  };
}
const call = (id: string) => ({ type: 'query.call', id, path: 'control.tenants.mine', args: {} }) as never;
const value = (replies: string[]) => (JSON.parse(replies[0]!) as { result: { version: number } }).result.version;
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('#542 review: a read in flight across an invalidation is not cached', () => {
  it('controlChanged during an in-flight read: the next read (and every subscription refresh) sees the change', async () => {
    const w = world();
    w.holdReads(true);
    const inflight = w.ports.control.dispatch(socket('a') as never, call('q1')); // socket 1: Control read version 1
    await tick();
    w.bump();                              // another client's write commits in Control
    controlReadsFor(w.env).invalidate('p'); // its notice arrives: AssemblyGateway.controlChanged's first step
    w.holdReads(false);
    w.release();
    expect(value(await inflight)).toBe(1);  // that reply is correct for its own frame
    // A refresh is a query.call through the same port. If it is served version 1, `subscription.last` matches and the
    // subscription never sees version 2: no TTL bounds that, only the next notify of the project does.
    expect(value(await w.ports.control.dispatch(socket('a') as never, call('refresh')))).toBe(2);
  });

  it("read-your-writes across a client's two sockets on one gateway: a read in flight during a write is not cached", async () => {
    const w = world();
    w.holdReads(true);
    const other = w.ports.control.dispatch(socket('a') as never, call('q1')); // socket A reads (version 1)
    await tick();
    w.holdReads(false);
    // socket B writes (e.g. control.tenants.create): dropped before and after, reply returned.
    await w.ports.control.dispatch(socket('a') as never, { type: 'reducer.call', id: 'w1', path: 'control.tenants.create', args: {} } as never);
    w.release();
    await other;                           // A's pre-write reply lands after B's post-write drop
    expect(value(await w.ports.control.dispatch(socket('a') as never, call('q2')))).toBe(2); // B reads its own write
  });
});
