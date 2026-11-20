// #539 re-review (head 35e47518): the lease against a REAL Control and a token issued the way Control issues it
// (`issueSession`: secureToken("session"), expires_at = now + ACCESS_TOKEN_MS, in ms). The PR's own tests use fake
// Controls; the expiry test in x17j-review-gaps.spec.ts uses `tok-exp`, which is never leased, so it passes vacuously.
import { env, runInDurableObject } from 'cloudflare:test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { ACCESS_TOKEN_MS, issueSession, newFamily } from '../../../control/src/sessions';
import { IDENTITY_LEASE_MS } from '../../session/identity-lease';
import { SessionControlObject } from '../control';
import { createProject } from '../../../control/src/dev';

type Real = Pick<SessionControlObject, 'sessionIdentity'> & { sql: { run(sql: string, ...a: unknown[]): void; tx<T>(fn: () => T): T }; context(): Parameters<typeof issueSession>[0] };
// REAL_CONTROL is K06's ControlObject in this worker; SessionControlObject's own `sessionIdentity` (with this PR's
// `sessionExpiry`) runs on it unchanged: the methods are taken from SessionControlObject's prototype.
const REAL_CONTROL = (env as unknown as { REAL_CONTROL: DurableObjectNamespace }).REAL_CONTROL;
const proto = SessionControlObject.prototype as unknown as Record<string, unknown>;
function asSessionControl(instance: object): Real {
  const shim = Object.create(instance) as Record<string, unknown>;
  for (const name of ['auth', 'sessionExpiry', 'sessionIdentity']) shim[name] = proto[name];
  return shim as unknown as Real;
}

async function withRealControl(test: (control: Real, project: string, token: string, expiresAt: number) => Promise<void>) {
  await runInDurableObject(REAL_CONTROL.getByName('x17j-rereview-' + crypto.randomUUID()), async (instance: unknown) => {
    const control = asSessionControl(instance as object);
    const { projectId: project } = createProject(control.context() as never, { name: 'X17j re-review' });
    const accountId = 'acct-' + crypto.randomUUID();
    control.sql.run("INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at) VALUES($1,$2,'lease@fixture.invalid','L',NULL,0)", accountId, project);
    const grant = control.sql.tx(() => issueSession(control.context(), project, accountId, newFamily(), Date.now() + 86_400_000));
    await test(control, project, grant.accessToken, grant.accessExpiresAt);
  });
}

/** A gateway's control port whose Control is the real object (counts its auth calls). */
function portsOver(control: Real) {
  const calls = { identity: 0 };
  const counted = { sessionIdentity: (t: string, p: string | undefined) => { calls.identity++; return control.sessionIdentity(t, p); } };
  const environment = { CONTROL: { getByName: () => counted }, TENANT: { getByName: () => ({}) }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
  return { ports: assemblyPorts(environment), calls };
}

afterEach(() => { vi.useRealTimers(); });

describe('#539 re-review: a real Control-issued session token', () => {
  it('Control returns the row expires_at in ms with sessionIdentity, and the gateway leases the token', async () => {
    await withRealControl(async (control, project, token, expiresAt) => {
      expect(token.startsWith('gvx_session_')).toBe(true);
      const outcome = await control.sessionIdentity(token, project) as { ok: boolean; expiresAt?: number };
      expect(outcome).toMatchObject({ ok: true, expiresAt });
      expect(outcome.expiresAt! - Date.now()).toBeGreaterThan(ACCESS_TOKEN_MS - 60_000); // ms, not s
      expect(outcome.expiresAt! - Date.now()).toBeLessThanOrEqual(ACCESS_TOKEN_MS);
      const w = portsOver(control);
      expect(await w.ports.control.loadSessionIdentity(token, project)).toMatchObject({ ok: true });
      expect(await w.ports.control.loadSessionIdentity(token, project)).toMatchObject({ ok: true });
      expect(w.calls.identity).toBe(1);
    });
  });

  it('a real lease ends at the session row expires_at, inside IDENTITY_LEASE_MS', async () => {
    await withRealControl(async (control, project, token) => {
      // Real time: Control's expiry check is SQLite's clock (NOW_MS), which fake timers do not move.
      const start = Date.now();
      control.sql.run('UPDATE gonvex_auth_sessions SET expires_at=$1', start + 1_500);
      const w = portsOver(control);
      expect(await w.ports.control.loadSessionIdentity(token, project)).toMatchObject({ ok: true });
      expect(await w.ports.control.loadSessionIdentity(token, project)).toMatchObject({ ok: true });
      expect(w.calls.identity).toBe(1); // leased
      await new Promise((resolve) => setTimeout(resolve, Math.max(0, start + 1_700 - Date.now())));
      expect(Date.now() - start).toBeLessThan(IDENTITY_LEASE_MS); // still inside the lease window
      const after = await w.ports.control.loadSessionIdentity(token, project);
      expect(w.calls.identity).toBe(2); // the lease ended at expires_at: Control answered
      expect(after).toMatchObject({ ok: false });
    });
  });

  it('a revoked session returns no expiresAt (never leased)', async () => {
    await withRealControl(async (control, project, token) => {
      // sessionExpiry reads the row after the auth; the auth itself fails for a revoked row.
      control.sql.run('UPDATE gonvex_auth_sessions SET revoked_at=$1', Date.now());
      const outcome = await control.sessionIdentity(token, project) as { ok: boolean; expiresAt?: number };
      expect(outcome.ok).toBe(false);
      expect(outcome.expiresAt).toBeUndefined();
    });
  });
});
