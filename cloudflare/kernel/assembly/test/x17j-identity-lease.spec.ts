// X17j: identity leases on the gateway: repeated auths of the same token skip Control, revocation is pushed by
// controlChanged, and the 10 s TTL is the backstop when a notification is lost.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { IDENTITY_LEASE_MS, IdentityLeases, identityLeases } from '../../session/identity-lease';

const identity = { projectId: 'p', account: { id: 'acct-1', email: 'a@x', emailVerified: true, name: 'A', avatarUrl: '', provider: 'password' } };
const route = { projectId: 'p', tenantId: 't1', databaseUrl: 'db' };
const member = { id: 'm-1', accountId: 'acct-1', status: 'active', displayName: 'A', avatarUrl: '', role: 'admin', permissions: {}, membershipRevision: 1 };
const session = { identity, route, member, admissionRevision: 3, delegation: null, developerSession: null, anonymous: false };

function world() {
  const calls = { identity: 0, authenticate: 0, admit: 0 };
  let revoked = false;
  let admits: typeof member | null = member;
  const control = {
    sessionIdentity: async () => { calls.identity++; return revoked ? { ok: false, error: 'session is revoked' } : { ok: true, value: identity }; },
    sessionAuthenticate: async () => { calls.authenticate++; return revoked ? { ok: false, error: 'session is revoked' } : { ok: true, value: session }; },
  };
  const tenant = { admit: async () => { calls.admit++; return admits ? { ...admits, admissionRevision: 4 } : null; } };
  const env = { CONTROL: { getByName: () => control }, TENANT: { getByName: () => tenant }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
  return { ports: assemblyPorts(env), calls, revoke: () => { revoked = true; }, stopAdmitting: () => { admits = null; } };
}

afterEach(() => { identityLeases.invalidate('p'); vi.useRealTimers(); });

describe('X17j identity leases', () => {
  it('a repeated control-only auth of the same token is answered from the lease', async () => {
    const w = world();
    expect(await w.ports.control.loadSessionIdentity('tok-a', 'p')).toMatchObject({ ok: true });
    expect(await w.ports.control.loadSessionIdentity('tok-a', 'p')).toMatchObject({ ok: true, value: identity });
    expect(w.calls.identity).toBe(1);
    // Another token never uses it.
    await w.ports.control.loadSessionIdentity('tok-b', 'p');
    expect(w.calls.identity).toBe(2);
  });

  it('a repeated tenant auth reuses identity and route but admits the member live (one tenant call, no Control)', async () => {
    const w = world();
    await w.ports.control.authenticateSession('tok-a', 'p', 't1');
    const again = await w.ports.control.authenticateSession('tok-a', 'p', 't1');
    expect(again).toMatchObject({ ok: true, value: { route, member: { id: 'm-1' }, admissionRevision: 4 } });
    expect(w.calls).toMatchObject({ authenticate: 1, admit: 1 });
    // Another tenant is never served from this lease.
    await w.ports.control.authenticateSession('tok-a', 'p', 't2');
    expect(w.calls.authenticate).toBe(2);
  });

  it('a member the tenant no longer admits falls back to Control (and its answer)', async () => {
    const w = world();
    await w.ports.control.authenticateSession('tok-a', 'p', 't1');
    w.stopAdmitting(); w.revoke();
    expect(await w.ports.control.authenticateSession('tok-a', 'p', 't1')).toMatchObject({ ok: false });
    expect(w.calls.authenticate).toBe(2);
  });

  it('revocation is pushed: after controlChanged the next auth on the gateway goes to Control and fails', async () => {
    const w = world();
    await w.ports.control.loadSessionIdentity('tok-a', 'p');
    await w.ports.control.authenticateSession('tok-a', 'p', 't1');
    w.revoke();
    identityLeases.invalidate('p'); // what AssemblyGateway.controlChanged does first
    expect(await w.ports.control.loadSessionIdentity('tok-a', 'p')).toMatchObject({ ok: false, error: 'session is revoked' });
    expect(await w.ports.control.authenticateSession('tok-a', 'p', 't1')).toMatchObject({ ok: false, error: 'session is revoked' });
  });

  it('a lost notification: the lease expires within IDENTITY_LEASE_MS and the revoked token fails', async () => {
    let now = 1_000_000;
    const leases = new IdentityLeases(() => now);
    leases.putIdentity('tok-a', 'p', identity);
    expect(leases.identity('tok-a', 'p')).toEqual(identity);
    now += IDENTITY_LEASE_MS - 1;
    expect(leases.identity('tok-a', 'p')).toEqual(identity);
    now += 1;
    expect(leases.identity('tok-a', 'p')).toBeUndefined();
    // Through the ports: with the clock past the TTL, the revoked token reaches Control and fails.
    vi.useFakeTimers({ toFake: ['Date'] });
    const w = world();
    await w.ports.control.loadSessionIdentity('tok-c', 'p');
    w.revoke();
    vi.setSystemTime(Date.now() + IDENTITY_LEASE_MS);
    expect(await w.ports.control.loadSessionIdentity('tok-c', 'p')).toMatchObject({ ok: false });
  });

  it('never leases impersonation/developer grants, delegated, developer or anonymous sessions', () => {
    const leases = new IdentityLeases();
    leases.putIdentity('gvx_imp_x', 'p', identity);
    leases.putSession('gvx_dev_x', 'p', 't1', session);
    leases.putSession('tok', 'p', 't1', { ...session, delegation: { principal: 's' } as never });
    leases.putSession('tok', 'p', 't2', { ...session, developerSession: {} as never });
    leases.putSession('tok', 'p', 't3', { ...session, anonymous: true });
    leases.putSession('tok', 'p', '', session);
    expect(leases.size).toBe(0);
  });
});

describe('X17j the gateway drops a project\'s leases on controlChanged', () => {
  it('AssemblyGateway.controlChanged(project) invalidates that project only', async () => {
    const { env, runInDurableObject } = await import('cloudflare:test');
    const gateways = (env as unknown as { GATEWAY: DurableObjectNamespace }).GATEWAY;
    await runInDurableObject(gateways.getByName('x17j-gw'), async (gateway: { controlChanged(p: string): Promise<void> }) => {
      const { identityLeases: leases } = await import('../../session/identity-lease');
      leases.putIdentity('tok-p', 'p', identity);
      leases.putIdentity('tok-q', 'q', { ...identity, projectId: 'q' });
      await gateway.controlChanged('p');
      expect(leases.identity('tok-p', 'p')).toBeUndefined();
      expect(leases.identity('tok-q', 'q')).toEqual({ ...identity, projectId: 'q' });
      leases.invalidate('q');
    });
  });
});
