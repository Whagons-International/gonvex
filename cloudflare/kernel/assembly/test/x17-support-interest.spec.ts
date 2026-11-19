// X17: presence and error-session/telemetry changes reach only the gateways whose sessions read `control.support.*`
// (and every non-gateway watcher, as before). Other gateways would only pay for them (a peer request defers their next
// outgoing call, X02 CallOrigin). Ordinary Control changes still reach every watcher.
import { env, evictDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';

type Control = {
  watch(project: string, binding: string, id: string): Promise<void>;
  sessionDispatch(socket: unknown, frame: unknown): Promise<string[]>;
  fixtureSupportWatchers(project: string): Promise<Array<{ binding: string; name: string }>>;
  fixtureWatchers(project: string): Promise<Array<{ binding: string; name: string }>>;
};
// PUB_GATEWAY is the stable Worker's TestGateway class: the namespace Control's own GATEWAY binding names.
const bindings = env as unknown as { PUB_CONTROL: DurableObjectNamespace; PUB_GATEWAY: DurableObjectNamespace };
const socket = (host: string) => ({ connectionId: 'c-' + host, host, connection: { projectId: 'p', identity: null, tenant: null, impersonationId: '', authToken: '' } });

describe('X17 support-scoped Control notifications', () => {
  it('notify only gateways that read control.support.*, plus every non-gateway watcher; the interest survives a restart', async () => {
    const stub = bindings.PUB_CONTROL.getByName('x17-' + crypto.randomUUID());
    const control = stub as unknown as Control;
    const a = bindings.PUB_GATEWAY.idFromName('gw-a').toString(), b = bindings.PUB_GATEWAY.idFromName('gw-b').toString();
    await control.watch('p', 'GATEWAY', a);
    await control.watch('p', 'GATEWAY', b);
    await control.watch('p', 'FIXTURE_OWNER', 'fixture-owner');
    expect(await control.fixtureSupportWatchers('p')).toEqual([{ binding: 'FIXTURE_OWNER', name: 'fixture-owner' }]);

    // An ordinary Control query does not register interest; a support query does, even when it is refused.
    await control.sessionDispatch(socket('gw-b'), { type: 'query.call', id: 'q1', path: 'control.developer.status', args: {} });
    await control.sessionDispatch(socket('gw-a'), { type: 'query.subscribe', id: 'q2', path: 'control.support.listTenants', args: {} });
    const expected = [{ binding: 'GATEWAY', name: a }, { binding: 'FIXTURE_OWNER', name: 'fixture-owner' }];
    expect(await control.fixtureSupportWatchers('p')).toEqual(expected);
    // Every watcher still gets ordinary changes.
    expect((await control.fixtureWatchers('p')).map(w => w.name).sort()).toEqual([a, b, 'fixture-owner'].sort());

    await evictDurableObject(stub);
    expect(await control.fixtureSupportWatchers('p')).toEqual(expected);
  });
});
