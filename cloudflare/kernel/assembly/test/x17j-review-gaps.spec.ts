// #539 independent review: gaps in the X17j identity lease (failed on c59f3dd5). Adapted: Control returns the
// session's expiresAt with an auth (the lease's bound), and leases are per gateway object.
import { env, runInDurableObject } from 'cloudflare:test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { assemblyPorts, type AssemblyGatewayEnv } from '../gateway';
import { createProject } from '../../../control/src/dev';
import { sha256Hex } from '../../../control/src/util';

import { handleOperatorRoute, type Op } from '../../../control/operator/routes';
import { OperatorAuth } from '../../../control/operator/auth';
import { operatorConfig } from '../../../control/operator/config';

const identity = { projectId: 'p', account: { id: 'acct-1', email: 'a@x', emailVerified: true, name: 'A', avatarUrl: '', provider: 'password' } };

function world(validUntil = Infinity) {
  const calls = { identity: 0 };
  const control = {
    // Control's load_session_identity: `gvx_session_` only, and `expires_at > now`.
    sessionIdentity: async (token: string) => {
      calls.identity++;
      if (!token.trim().startsWith('gvx_session_') && !token.startsWith('tok-')) return { ok: false, error: 'session is invalid' };
      return Date.now() < validUntil ? { ok: true, value: identity, expiresAt: Number.isFinite(validUntil) ? validUntil : Date.now() + 3_600_000 } : { ok: false, error: 'session is invalid' };
    },
  };
  const environment = { CONTROL: { getByName: () => control }, TENANT: { getByName: () => ({}) }, GATEWAY: { idFromName: () => ({ toString: () => 'g' }) } } as unknown as AssemblyGatewayEnv;
  return { ports: assemblyPorts(environment), calls };
}

afterEach(() => { vi.useRealTimers(); });

describe('#539 review: lease keying and expiry', () => {
  it('a real session token (gvx_session_*) is leased: the second control-only auth skips Control', async () => {
    // Control rejects every token that does not start with `gvx_session_` (control/src/directory.ts:155), and
    // putIdentity/putSession refuse every `gvx_` token (identity-lease.ts:45,52): the lease never applies in production.
    const w = world();
    await w.ports.control.loadSessionIdentity('gvx_session_abc', 'p');
    await w.ports.control.loadSessionIdentity('gvx_session_abc', 'p');
    expect(w.calls.identity).toBe(1);
  });

  it('a lease never outlives the token: a session that expires inside the window fails at its expires_at', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const start = Date.now();
    const w = world(start + 2_000); // the session row expires 2 s after this auth
    expect(await w.ports.control.loadSessionIdentity('tok-exp', 'p')).toMatchObject({ ok: true });
    vi.setSystemTime(start + 3_000); // past expires_at, inside IDENTITY_LEASE_MS
    expect(await w.ports.control.loadSessionIdentity('tok-exp', 'p')).toMatchObject({ ok: false });
  });
});

type Internals = { sql: { run(sql: string, ...args: unknown[]): void }; notify(project: string): void; fetch(request: Request): Promise<Response>; context(): never };
const REAL_CONTROL = (env as unknown as { REAL_CONTROL: DurableObjectNamespace }).REAL_CONTROL;

async function inControl(test: (instance: Internals, project: string, notified: string[]) => Promise<void>) {
  await runInDurableObject(REAL_CONTROL.getByName('x17j-gap-' + crypto.randomUUID()), async (instance: unknown) => {
    const control = instance as Internals;
    const notified: string[] = [];
    control.notify = (project: string) => { notified.push(project); };
    const { projectId } = createProject((control as unknown as { context(): Parameters<typeof createProject>[0] }).context(), { name: 'x17j' });
    control.sql.run("INSERT INTO accounts(id,auth_realm_id,email,name,disabled_at,updated_at) VALUES('acct-1',$1,'a@x','A',NULL,0)", projectId);
    control.sql.run("INSERT INTO gonvex_auth_sessions(token_hash,project_id,account_id,family_id,expires_at) VALUES($1,$2,'acct-1','fam-1',$3)",
      sha256Hex('gvx_session_live'), projectId, Date.now() + 3_600_000);
    await test(control, projectId, notified);
  });
}

describe('#539 review: every revocation notifies the gateways', () => {
  it('operator PATCH /dev/projects/:p/auth/accounts/:a {disabled:true} (revokes every session) notifies', async () => {
    await inControl(async (control, project, notified) => {
      const key = String((control as unknown as { sql: { value(sql: string, ...a: unknown[]): unknown } }).sql.value('SELECT project_key FROM gonvex_runtime_projects WHERE id=$1', project));
      const ctx = (control as unknown as { context(): Op['ctx'] }).context();
      // The real operator wiring (control/operator/object.ts:200-215): op.controlChanged is K06's notify.
      const op = { ctx, auth: new OperatorAuth(ctx, operatorConfig({} as never)), controlChanged: (p: string) => control.notify(p) } as unknown as Op;
      const response = await handleOperatorRoute(op, new Request(`https://control/dev/projects/${project}/auth/accounts/acct-1`, {
        method: 'PATCH', headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' }, body: JSON.stringify({ disabled: true }),
      }));
      expect(response.status).toBe(200);
      expect(notified).toContain(project); // control/operator/routes.ts:867-885 never calls op.controlChanged
    });
  });

  it('HTTP POST /auth/logout (revokes the family) notifies', async () => {
    await inControl(async (control, project, notified) => {
      const response = await control.fetch(new Request('https://control/auth/logout', {
        method: 'POST', headers: { authorization: 'Bearer gvx_session_live', 'content-type': 'application/json' }, body: JSON.stringify({ refreshToken: '' }),
      }));
      expect(response.status).toBe(200);
      expect(notified).toContain(project); // control/src/native-auth.ts:483-508 never calls ctx.controlChanged
    });
  });
});

describe('X17j: the other revoking paths notify the gateways', () => {
  const allow = { authorize: async () => ({}), authorizeProject: async () => {}, authorizeProjectResource: async () => {}, canManageProject: () => true };
  it('HTTP POST /auth/token with a reused refresh token (revokes the family) notifies', async () => {
    await inControl(async (control, project, notified) => {
      control.sql.run("INSERT INTO gonvex_auth_refresh_tokens(token_hash,family_id,project_id,account_id,expires_at,used_at) VALUES($1,'fam-1',$2,'acct-1',$3,1)",
        sha256Hex('gvx_refresh_reused'), project, Date.now() + 3_600_000);
      const response = await control.fetch(new Request('https://control/auth/token', {
        method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ grantType: 'refresh_token', project, refreshToken: 'gvx_refresh_reused' }),
      }));
      expect(response.status).toBe(401);
      expect(notified).toContain(project);
    });
  });

  it('operator tenant delete notifies (before the tenant storage goes)', async () => {
    await inControl(async (control, project, notified) => {
      control.sql.run("UPDATE gonvex_runtime_projects SET database_mode='multiTenant' WHERE id=$1", project);
      control.sql.run("INSERT INTO gonvex_runtime_tenants(tenant_id,project_id,name,domain,runtime_created,status) VALUES('t-del',$1,'T','t-del',1,'active')", project);
      const ctx = (control as unknown as { context(): Op['ctx'] }).context();
      let storageDeleted = false, notifiedBefore = false;
      const op = { ctx, auth: allow, controlChanged: (p: string) => control.notify(p), deleteTenantStorage: async () => { notifiedBefore = notified.includes(project); storageDeleted = true; } } as unknown as Op;
      const response = await handleOperatorRoute(op, new Request(`https://control/dev/tenants/t-del?project=${project}`, { method: 'DELETE' }));
      expect(response.status).toBe(200);
      expect(storageDeleted).toBe(true);
      expect(notifiedBefore).toBe(true);
    });
  });

  it('operator project delete notifies', async () => {
    await inControl(async (control, project, notified) => {
      const { OPERATOR_SCHEMA } = await import('../../../control/operator/sync');
      const { installDeploymentSchema } = await import('../../../control/operator/deployment');
      (control.sql as unknown as { script(sql: string): void }).script(OPERATOR_SCHEMA);
      installDeploymentSchema(control.sql as never);
      control.sql.run("UPDATE gonvex_runtime_projects SET runtime_created=1 WHERE id=$1", project);
      const ctx = (control as unknown as { context(): Op['ctx'] }).context();
      const op = { ctx, auth: allow, controlChanged: (p: string) => control.notify(p), deleteTenantStorage: async () => {}, releases: undefined } as unknown as Op;
      const response = await handleOperatorRoute(op, new Request(`https://control/dev/projects/${project}`, { method: 'DELETE' }));
      expect(response.status).toBe(200);
      expect(notified).toContain(project);
    });
  });
});
