/**
 * Control-plane auth flow (control.rs, packages/client/src/control.ts):
 * public settings, password login (wrong password, unknown account, good),
 * account query, tenant auth with the session, refresh rotation, logout and
 * the refused refresh after it, and tenant auth with invalid tokens.
 */
import { control } from '../src/sessions.mjs';

const settle = (promise) => promise.then(result => ({ result }), error => ({ error: String(error?.message ?? error), code: error?.code }));
const sessionShape = (s) => s && ({ hasAccess: typeof s.accessToken === 'string', hasRefresh: typeof s.refreshToken === 'string', account: s.account, tenants: s.tenants });

export default {
  name: 'control-auth',
  description: 'Control-plane auth: login failures, login, me, tenant auth, refresh, logout, invalid tokens',
  tenants: { a: ['alice'], b: ['alice'] },
  async run(ctx) {
    let c, session;
    const email = ctx.members.alice.email;
    await ctx.step('public settings on a control-only socket', async () => {
      c = await ctx.sdk('control');
      ctx.observe('settings', await settle(c.query(control.auth.publicSettings, {})));
    });
    await ctx.step('login failures', async () => {
      ctx.observe('wrongPassword', await settle(c.action(control.auth.passwordLogin, { email, password: 'not-the-password-123' })));
      ctx.observe('unknownAccount', await settle(c.action(control.auth.passwordLogin, { email: 'nobody@conformance.invalid', password: ctx.PASSWORD })));
      ctx.observe('extraArgument', await settle(c.action(control.auth.passwordLogin, { email, password: ctx.PASSWORD, remember: true })));
    });
    await ctx.step('login', async () => {
      const r = await settle(c.action(control.auth.passwordLogin, { email, password: ctx.PASSWORD }));
      session = r.result;
      ctx.observe('login', r.error ? r : sessionShape(session));
    });
    await ctx.step('tenant auth and account query', async () => {
      await c.authenticate({ project: ctx.project, token: session.accessToken, tenant: ctx.tenants.a });
      ctx.observe('me', await settle(c.query(control.accounts.me, {})));
    });
    let refreshed;
    await ctx.step('refresh rotates the session', async () => {
      const r = await settle(c.action(control.auth.refreshSession, { refreshToken: session.refreshToken }));
      refreshed = r.result;
      ctx.observe('refresh', r.error ? r : sessionShape(refreshed));
      ctx.observe('rotated', !!refreshed && refreshed.refreshToken !== session.refreshToken);
      ctx.observe('oldRefreshAgain', await settle(c.action(control.auth.refreshSession, { refreshToken: session.refreshToken })));
    });
    await ctx.step('tenant auth with the refreshed token, other tenant', async () => {
      await c.authenticate({ project: ctx.project, token: refreshed.accessToken, tenant: ctx.tenants.b });
      ctx.observe('me', await settle(c.query(control.accounts.me, {})));
    });
    await ctx.step('logout revokes the refresh token', async () => {
      ctx.observe('logout', await settle(c.reducer(control.auth.logout, { refreshToken: refreshed.refreshToken, all: false })));
      ctx.observe('refreshAfterLogout', await settle(c.action(control.auth.refreshSession, { refreshToken: refreshed.refreshToken })));
    });
    await ctx.step('tenant auth with invalid tokens', async () => {
      const raw = await ctx.raw('raw');
      const attempt = (frame) => { raw.send(frame); return raw.next(m => (m.type === 'auth.result' || m.type === 'auth.error') && m.id === frame.id, { label: 'auth reply' }); };
      const shape = (m) => ({ type: m.type, code: m.code, error: m.error });
      ctx.observe('garbage', shape(await attempt({ type: 'auth', id: 'auth-garbage', project: ctx.project, tenant: ctx.tenants.a, token: 'gvx_session_not_a_real_token', clientContract: 1 })));
      ctx.observe('unknownTenant', shape(await attempt({ type: 'auth', id: 'auth-unknown-tenant', project: ctx.project, tenant: 'tenant-zz', token: session.accessToken, clientContract: 1 })));
      ctx.observe('unknownProject', shape(await attempt({ type: 'auth', id: 'auth-unknown-project', project: 'no-such-project', controlOnly: true, clientContract: 1 })));
      raw.close();
    });
  },
};
