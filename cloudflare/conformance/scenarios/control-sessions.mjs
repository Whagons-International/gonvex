/**
 * Control Plane sessions (CONTRACT.md C.0 idempotency; C.3.6 refresh and
 * rotation; C.3.7 revocation; C.3.8 auth branches; C.1 rows 2, 5, 7, 8):
 * replayed logins, key reuse across paths, refresh reuse after the grace
 * period revoking the family, logout of every session, password change, and
 * the `auth` frame's control-only, mismatch and default-tenant branches.
 */
import { action, auth, query, reducer, sessionShape } from './control-helpers.mjs';

const NEW_PASSWORD = 'alice-new-password-1';

export default {
  name: 'control-sessions',
  description: 'Control sessions: login replay, key reuse, refresh reuse revocation, logout all, password change, auth branches',
  tenants: { a: ['alice'], b: ['alice', 'bob'] },
  tables: [],
  async run(ctx) {
    let socket, first;
    const email = ctx.members.alice.email;
    await ctx.step('auth frame branches', async () => {
      socket = await ctx.raw('alice');
      ctx.observe('noProject', await auth(socket, { controlOnly: true }));
      ctx.observe('noToken', await auth(socket, { project: ctx.project }));
      ctx.observe('anonymous', await auth(socket, { project: ctx.project, controlOnly: true }));
      ctx.observe('meAnonymous', await query(socket, 'control.accounts.me', {}));
      ctx.observe('noKey', await action(socket, 'control.auth.passwordLogin', { email, password: ctx.PASSWORD }, null));
      const login = await action(socket, 'control.auth.passwordLogin', { email: ` ${email.toUpperCase()} `, password: ctx.PASSWORD }, 'login-1');
      first = login.result;
      ctx.observe('login', sessionShape(login));
      ctx.observe('controlOnlyToken', await auth(socket, { project: ctx.project, token: first.accessToken, controlOnly: true }));
      ctx.observe('controlOnlyTenantIgnored', await auth(socket, { project: ctx.project, token: first.accessToken, controlOnly: true, tenant: ctx.tenants.b }));
      ctx.observe('defaultTenant', await auth(socket, { project: ctx.project, token: first.accessToken }));
      ctx.observe('projectMismatch', await auth(socket, { project: 'no-such-project', token: first.accessToken, controlOnly: true }));
      ctx.observe('notAMember', await auth(socket, { project: ctx.project, token: first.accessToken, tenant: 'nope' }));
      ctx.observe('notSessionToken', await auth(socket, { project: ctx.project, token: 'gvx_refresh_x', controlOnly: true }));
      const bob = await ctx.raw('bob');
      const bobSession = await ctx.rawLogin(bob, 'bob');
      ctx.observe('bobDefaultTenant', await auth(bob, { project: ctx.project, token: bobSession.accessToken }));
      ctx.observe('bobNotMember', await auth(bob, { project: ctx.project, token: bobSession.accessToken, tenant: ctx.tenants.a }));
    });
    await ctx.step('login replay and key reuse', async () => {
      await auth(socket, { project: ctx.project, controlOnly: true });
      const replay = await action(socket, 'control.auth.passwordLogin', { email, password: ctx.PASSWORD }, 'login-1');
      ctx.observe('replaySameTokens', replay.result?.accessToken === first.accessToken && replay.result?.refreshToken === first.refreshToken);
      ctx.observe('replayWrongPassword', sessionShape(await action(socket, 'control.auth.passwordLogin', { email, password: 'wrong-password-123' }, 'login-1')));
      await auth(socket, { project: ctx.project, token: first.accessToken, controlOnly: true });
      ctx.observe('logoutKey', await reducer(socket, 'control.auth.logout', { refreshToken: 'gvx_refresh_unknown', all: false }, 'shared-key'));
      ctx.observe('otherPathSameKey', await reducer(socket, 'control.accounts.updatePassword', { currentPassword: ctx.PASSWORD, newPassword: NEW_PASSWORD }, 'shared-key'));
      ctx.observe('logoutNeedsToken', await reducer(socket, 'control.auth.logout', { all: true }, 'logout-x'));
    });
    await ctx.step('refresh reuse after the grace period revokes the family', async () => {
      await auth(socket, { project: ctx.project, controlOnly: true });
      const second = await action(socket, 'control.auth.refreshSession', { refreshToken: first.refreshToken }, 'refresh-1');
      ctx.observe('rotated', sessionShape(second));
      ctx.observe('withinGrace', await action(socket, 'control.auth.refreshSession', { refreshToken: first.refreshToken }, 'refresh-2'));
      await ctx.delay(5600);
      ctx.observe('reuse', await action(socket, 'control.auth.refreshSession', { refreshToken: first.refreshToken }, 'refresh-3'));
      ctx.observe('reuseReplay', await action(socket, 'control.auth.refreshSession', { refreshToken: first.refreshToken }, 'refresh-3'));
      ctx.observe('latestRevoked', await action(socket, 'control.auth.refreshSession', { refreshToken: second.result?.refreshToken }, 'refresh-4'));
      ctx.observe('accessRevoked', await auth(socket, { project: ctx.project, token: second.result?.accessToken, controlOnly: true }));
      ctx.observe('garbage', await action(socket, 'control.auth.refreshSession', { refreshToken: 'gvx_refresh_nope' }, 'refresh-5'));
    });
    await ctx.step('logout of every session', async () => {
      await auth(socket, { project: ctx.project, controlOnly: true });
      const one = (await action(socket, 'control.auth.passwordLogin', { email, password: ctx.PASSWORD }, 'login-2')).result;
      const two = (await action(socket, 'control.auth.passwordLogin', { email, password: ctx.PASSWORD }, 'login-3')).result;
      await auth(socket, { project: ctx.project, token: one.accessToken, controlOnly: true });
      ctx.observe('logoutAll', await reducer(socket, 'control.auth.logout', { refreshToken: one.refreshToken, all: true }, 'logout-all'));
      const check = await ctx.raw('check');
      ctx.observe('otherAccess', await auth(check, { project: ctx.project, token: two.accessToken, controlOnly: true }));
      ctx.observe('otherRefresh', await action(check, 'control.auth.refreshSession', { refreshToken: two.refreshToken }, 'refresh-6'));
      ctx.observe('callAfterRevocation', await query(socket, 'control.accounts.me', {}));
    });
    await ctx.step('password change', async () => {
      await auth(socket, { project: ctx.project, controlOnly: true });
      const session = (await action(socket, 'control.auth.passwordLogin', { email, password: ctx.PASSWORD }, 'login-4')).result;
      await auth(socket, { project: ctx.project, token: session.accessToken, controlOnly: true });
      ctx.observe('wrongCurrent', await reducer(socket, 'control.accounts.updatePassword', { currentPassword: 'wrong-password-123', newPassword: NEW_PASSWORD }, 'pw-1'));
      ctx.observe('tooShort', await reducer(socket, 'control.accounts.updatePassword', { currentPassword: ctx.PASSWORD, newPassword: 'short' }, 'pw-2'));
      ctx.observe('changed', await reducer(socket, 'control.accounts.updatePassword', { currentPassword: ctx.PASSWORD, newPassword: NEW_PASSWORD }, 'pw-3'));
      ctx.observe('replay', await reducer(socket, 'control.accounts.updatePassword', { currentPassword: 'anything-at-all', newPassword: 'x' }, 'pw-3'));
      ctx.observe('sessionKept', await query(socket, 'control.accounts.me', {}));
      const fresh = await ctx.raw('fresh');
      await auth(fresh, { project: ctx.project, controlOnly: true });
      ctx.observe('oldPassword', await action(fresh, 'control.auth.passwordLogin', { email, password: ctx.PASSWORD }, 'login-5'));
      ctx.observe('newPassword', sessionShape(await action(fresh, 'control.auth.passwordLogin', { email, password: NEW_PASSWORD }, 'login-6')));
    });
  },
};
