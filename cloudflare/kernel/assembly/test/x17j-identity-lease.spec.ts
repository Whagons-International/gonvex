// X17j: identity leases on the gateway (gvx_session_* tokens): repeated auths skip Control, revocation is pushed by
// controlChanged and by this gateway's own forwarded writes, a lease never outlives the session, and the 10 s TTL is
// the backstop when a notification is lost.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { IDENTITY_LEASE_MS, IdentityLeases, leasesFor } from '../../session/identity-lease';

const identity = { projectId: 'p', account: { id: 'acct-1', email: 'a@x', emailVerified: true, name: 'A', avatarUrl: '', provider: 'password' } };
const route = { projectId: 'p', tenantId: 't1', databaseUrl: 'db' };
const member = { id: 'm-1', accountId: 'acct-1', status: 'active', displayName: 'A', avatarUrl: '', role: 'admin', permissions: {}, membershipRevision: 1 };
const session = { identity, route, member, admissionRevision: 3, delegation: null, developerSession: null, anonymous: false };
const TOKEN = 'gvx_session_aaaa', OTHER = 'gvx_session_bbbb';

function world(expiresAt = () => Date.now() + 3_600_000) {
  const calls = { identity: 0, authenticate: 0, admit: 0, dispatch: 0, liveOnly: false };
  let revoked = false;
  let admits: typeof member | null = member;
  const control = {
    sessionIdentity: async () => { calls.identity++; return revoked ? { ok: false, error: 'session is revoked' } : { ok: true, value: identity, expiresAt: expiresAt() }; },
    sessionAuthenticate: async (_t: string, _p: string, _i: string, options?: { liveOnly?: boolean }) => { calls.authenticate++; calls.liveOnly = options?.liveOnly === true; return revoked ? { ok: false, error: 'session is revoked' } : { ok: true, value: session, expiresAt: expiresAt() }; },
    sessionDispatch: async (_s: unknown, frame: { id: string }) => { calls.dispatch++; revoked = true; return [JSON.stringify({ type: 'action.result', id: frame.id, result: null })]; },
  };
  const tenant = { admit: async () => { calls.admit++; return admits ? { ...admits, admissionRevision: 4 } : null; } };
  const env = { CONTROL: { getByName: () => control }, TENANT: { getByName: () => tenant }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
  return { env, ports: assemblyPorts(env), calls, revoke: () => { revoked = true; }, stopAdmitting: () => { admits = null; } };
}
const socket = { connectionId: 'c', host: 'h', connection: { projectId: 'p', identity, tenant: null, impersonationId: '', authToken: TOKEN } } as never;

afterEach(() => { vi.useRealTimers(); });

describe('X17j identity leases', () => {
  it('a repeated control-only auth of the same session token is answered from the lease', async () => {
    const w = world();
    expect(await w.ports.control.loadSessionIdentity(TOKEN, 'p')).toMatchObject({ ok: true });
    expect(await w.ports.control.loadSessionIdentity(` ${TOKEN} `, 'p')).toMatchObject({ ok: true, value: identity }); // trimmed, as Control
    expect(w.calls.identity).toBe(1);
    await w.ports.control.loadSessionIdentity(OTHER, 'p'); // another token never uses it
    expect(w.calls.identity).toBe(2);
  });

  it('a repeated tenant auth reuses identity and route but admits the member live (one tenant call, no Control)', async () => {
    const w = world();
    await w.ports.control.authenticateSession(TOKEN, 'p', 't1');
    const again = await w.ports.control.authenticateSession(TOKEN, 'p', 't1');
    expect(again).toMatchObject({ ok: true, value: { route, member: { id: 'm-1' }, admissionRevision: 4 } });
    expect(w.calls).toMatchObject({ authenticate: 1, admit: 1 });
    await w.ports.control.authenticateSession(TOKEN, 'p', 't2'); // another tenant is never served from this lease
    expect(w.calls.authenticate).toBe(2);
  });

  it('a member the tenant no longer admits falls back to Control (and its answer)', async () => {
    const w = world();
    await w.ports.control.authenticateSession(TOKEN, 'p', 't1');
    w.stopAdmitting(); w.revoke();
    expect(await w.ports.control.authenticateSession(TOKEN, 'p', 't1')).toMatchObject({ ok: false });
    expect(w.calls.authenticate).toBe(2);
    expect(w.calls.liveOnly).toBe(true); // #540 review: Control admits live, never from a recent admission
  });

  it('revocation is pushed: after controlChanged the next auth on the gateway goes to Control and fails', async () => {
    const w = world();
    await w.ports.control.loadSessionIdentity(TOKEN, 'p');
    await w.ports.control.authenticateSession(TOKEN, 'p', 't1');
    w.revoke();
    leasesFor(w.env).invalidate('p'); // what AssemblyGateway.controlChanged does first
    expect(await w.ports.control.loadSessionIdentity(TOKEN, 'p')).toMatchObject({ ok: false, error: 'session is revoked' });
    expect(await w.ports.control.authenticateSession(TOKEN, 'p', 't1')).toMatchObject({ ok: false, error: 'session is revoked' });
  });

  it('a Control write this gateway forwards (a logout) drops the leases synchronously, before any notice', async () => {
    const w = world();
    await w.ports.control.loadSessionIdentity(TOKEN, 'p');
    await w.ports.control.dispatch(socket, { type: 'action.call', id: 'w1', path: 'control.auth.logout', args: {} } as never); // revokes in the fake
    expect(await w.ports.control.loadSessionIdentity(TOKEN, 'p')).toMatchObject({ ok: false });
  });

  it('a lost notification: the lease expires within IDENTITY_LEASE_MS and the revoked token fails', async () => {
    let now = 1_000_000;
    const leases = new IdentityLeases(() => now);
    leases.putIdentity(TOKEN, 'p', identity, now + 3_600_000);
    expect(leases.identity(TOKEN, 'p')).toEqual(identity);
    now += IDENTITY_LEASE_MS - 1;
    expect(leases.identity(TOKEN, 'p')).toEqual(identity);
    now += 1;
    expect(leases.identity(TOKEN, 'p')).toBeUndefined();
    vi.useFakeTimers({ toFake: ['Date'] });
    const w = world();
    await w.ports.control.loadSessionIdentity(OTHER, 'p');
    w.revoke();
    vi.setSystemTime(Date.now() + IDENTITY_LEASE_MS);
    expect(await w.ports.control.loadSessionIdentity(OTHER, 'p')).toMatchObject({ ok: false });
  });

  it('a lease never outlives the session: it ends at expires_at, and an answer without expiresAt is not leased', () => {
    let now = 1_000_000;
    const leases = new IdentityLeases(() => now);
    leases.putIdentity(TOKEN, 'p', identity, now + 2_000);
    now += 2_000;
    expect(leases.identity(TOKEN, 'p')).toBeUndefined();
    leases.putIdentity(OTHER, 'p', identity, undefined);
    expect(leases.identity(OTHER, 'p')).toBeUndefined();
  });

  it('never leases non-session tokens, delegated, developer or anonymous sessions', () => {
    const leases = new IdentityLeases();
    const far = Date.now() + 3_600_000;
    leases.putIdentity('gvx_imp_x', 'p', identity, far);
    leases.putIdentity('tok-x', 'p', identity, far);
    leases.putSession('gvx_dev_x', 'p', 't1', session, far);
    leases.putSession(TOKEN, 'p', 't1', { ...session, delegation: { principal: 's' } as never }, far);
    leases.putSession(TOKEN, 'p', 't2', { ...session, developerSession: {} as never }, far);
    leases.putSession(TOKEN, 'p', 't3', { ...session, anonymous: true }, far);
    leases.putSession(TOKEN, 'p', '', session, far);
    expect(leases.size).toBe(0);
  });

  it('each gateway object has its own store', () => {
    const a = world(), b = world();
    expect(leasesFor(a.env)).not.toBe(leasesFor(b.env));
  });
});

describe("X17j the gateway drops a project's leases on controlChanged", () => {
  it('AssemblyGateway.controlChanged(project) invalidates that project only, in its own store', async () => {
    const { env, runInDurableObject } = await import('cloudflare:test');
    const gateways = (env as unknown as { GATEWAY: DurableObjectNamespace }).GATEWAY;
    await runInDurableObject(gateways.getByName('x17j-gw'), async (instance: unknown) => {
      const gateway = instance as { controlChanged(p: string): Promise<void>; env: object };
      const leases = leasesFor(gateway.env), far = Date.now() + 3_600_000;
      leases.putIdentity(TOKEN, 'p', identity, far);
      leases.putIdentity(OTHER, 'q', { ...identity, projectId: 'q' }, far);
      await gateway.controlChanged('p');
      expect(leases.identity(TOKEN, 'p')).toBeUndefined();
      expect(leases.identity(OTHER, 'q')).toEqual({ ...identity, projectId: 'q' });
    });
  });
});
