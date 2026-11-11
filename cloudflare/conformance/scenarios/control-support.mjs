/**
 * Control Plane support and developer functions (CONTRACT.md C.1 rows 27-57;
 * C.3.1 grants; C.3.8 item 5): support sessions and commands, the tenant
 * overview, support impersonation and its single-use grant, developer mode,
 * self-provisioned developer membership, agent claim tokens, assistant and
 * voice settings, project developers and demo accounts.
 */
import { action, auth, memberSocket, passwordLogin, pick, query, reducer } from './control-helpers.mjs';

const ADMIN = 'alice@conformance.invalid';

export default {
  name: 'control-support',
  description: 'Control support: sessions, commands, tenant overview, impersonation, developer mode, agent tokens, settings, demos',
  tenants: { a: ['alice', 'bob'], b: ['bob'] },
  tables: [],
  async run(ctx) {
    let alice, bob, session;
    await ctx.step('bob reports a support session', async () => {
      bob = await memberSocket(ctx, 'bob', 'bob');
      const first = await reducer(bob, 'control.support.heartbeat', { release: ' 1.2.3 ', environment: 'test' }, 'hb-1');
      if (first.result) session = ctx.alias(first.result.sessionId, 'support:bob');
      ctx.observe('heartbeat', first);
      ctx.observe('again', await reducer(bob, 'control.support.heartbeat', { release: '1.2.3', environment: 'test' }, 'hb-2'));
      ctx.observe('extraField', await reducer(bob, 'control.support.heartbeat', { release: '1', environment: 'x', tab: 1 }, 'hb-3'));
    });
    await ctx.step('a project admin reads support data', async () => {
      alice = await memberSocket(ctx, 'alice', 'alice');
      ctx.observe('beforeGrant', await query(alice, 'control.support.listSessions', {}));
      await ctx.backend.addProjectMember(ctx.project, { email: ADMIN, name: 'Alice', role: 'admin' });
      ctx.observe('sessions', await query(alice, 'control.support.listSessions', {}));
      ctx.observe('session', pick((await query(alice, 'control.support.getSession', { id: session })).result ?? {}, ['id', 'tenantId', 'accountId', 'release', 'environment']));
      ctx.observe('unknownSession', await query(alice, 'control.support.getSession', { id: 'support_nope' }));
      ctx.observe('tenants', await query(alice, 'control.support.listTenants', {}));
      ctx.observe('tenant', pick((await query(alice, 'control.support.getTenant', { tenantId: ctx.tenants.b })).result ?? {}, ['id', 'name', 'status', 'seatLimit']));
      ctx.observe('unknownTenant', await query(alice, 'control.support.getTenant', { tenantId: 'nope' }));
      ctx.observe('errors', await query(alice, 'control.support.listErrors', {}));
      ctx.observe('unknownError', await query(alice, 'control.support.getError', { fingerprint: 'nope' }));
    });
    await ctx.step('support commands', async () => {
      const sent = await reducer(alice, 'control.support.sendCommand', { sessionId: session, kind: 'reload', payload: { reason: 'test' } }, 'cmd-1');
      if (sent.result) ctx.alias(sent.result.id, 'command:1');
      ctx.observe('sent', sent);
      ctx.observe('unknownSession', await reducer(alice, 'control.support.sendCommand', { sessionId: 'support_nope', kind: 'reload', payload: {} }, 'cmd-2'));
      ctx.observe('notOwner', await reducer(alice, 'control.support.ackCommand', { id: sent.result?.id }, 'ack-alice'));
      ctx.observe('ack', await reducer(bob, 'control.support.ackCommand', { id: sent.result?.id }, 'ack-1'));
      ctx.observe('ackAgain', await reducer(bob, 'control.support.ackCommand', { id: sent.result?.id }, 'ack-2'));
      ctx.observe('memberCannotSend', await reducer(bob, 'control.support.sendCommand', { sessionId: session, kind: 'reload', payload: {} }, 'cmd-bob'));
      ctx.observe('pruneRange', await reducer(alice, 'control.support.pruneSessions', { olderThanSeconds: 10 }, 'prune-0'));
      ctx.observe('prune', await reducer(alice, 'control.support.pruneSessions', { olderThanSeconds: 60 }, 'prune-1'));
    });
    let grant;
    await ctx.step('support impersonation', async () => {
      ctx.observe('notMember', await reducer(alice, 'control.support.createImpersonation', { accountId: 'acct_nope', tenantId: ctx.tenants.a, reason: 'help' }, 'imp-0'));
      const r = await reducer(alice, 'control.support.createImpersonation', { accountId: ctx.members.bob.accountId, tenantId: ctx.tenants.a, reason: 'help bob' }, 'imp-1');
      grant = r.result;
      if (grant) ctx.alias(grant.id, 'grant:imp');
      ctx.observe('created', r);
      ctx.observe('replay', pick((await reducer(alice, 'control.support.createImpersonation', { accountId: ctx.members.bob.accountId, tenantId: ctx.tenants.a, reason: 'help bob' }, 'imp-1')).result ?? {}, ['id']));
      const wrongTenant = await ctx.raw('imp-wrong');
      ctx.observe('wrongTenant', await auth(wrongTenant, { project: ctx.project, token: grant.token, tenant: ctx.tenants.b, controlOnly: true, capabilities: { tenantControl: 1 } }));
      const imp = await ctx.raw('imp');
      const redeemed = await auth(imp, { project: ctx.project, token: grant.token, tenant: ctx.tenants.a, controlOnly: true, capabilities: { tenantControl: 1 } });
      ctx.observe('redeem', redeemed);
      ctx.observe('me', await query(imp, 'control.accounts.me', {}));
      ctx.observe('status', await query(imp, 'control.developer.status', {}));
      ctx.observe('adminBlocked', await query(imp, 'control.support.listTenants', {}));
      ctx.observe('tenantAdminAllowed', await query(imp, 'control.invitations.list', {}));
      const again = await ctx.raw('imp-again');
      ctx.observe('reuse', await auth(again, { project: ctx.project, token: grant.token, tenant: ctx.tenants.a, controlOnly: true, capabilities: { tenantControl: 1 } }));
      ctx.observe('noTenant', await auth(again, { project: ctx.project, token: grant.token, controlOnly: true }));
      const reconnect = await auth(again, { project: ctx.project, token: redeemed.result?.developerSessionToken, tenant: ctx.tenants.a, controlOnly: true, capabilities: { tenantControl: 1 } });
      ctx.observe('reconnect', reconnect);
      ctx.expect(reconnect.result && reconnect.result.developerSessionToken !== redeemed.result?.developerSessionToken, 'the reconnect credential rotates');
      const stale = await ctx.raw('imp-stale');
      ctx.observe('staleReconnect', await auth(stale, { project: ctx.project, token: redeemed.result?.developerSessionToken, tenant: ctx.tenants.a, controlOnly: true, capabilities: { tenantControl: 1 } }));
    });
    let dev;
    await ctx.step('developer mode', async () => {
      ctx.observe('status', await query(alice, 'control.developer.status', {}));
      ctx.observe('unknownTenant', await reducer(alice, 'control.developer.enter', { tenantId: 'nope' }, 'dev-0'));
      const r = await reducer(alice, 'control.developer.enter', { tenantId: ctx.tenants.b }, 'dev-1');
      dev = r.result;
      if (dev) ctx.alias(dev.id, 'grant:dev');
      ctx.observe('enter', r);
      const socket = await ctx.raw('devmode');
      ctx.observe('redeem', await auth(socket, { project: ctx.project, token: dev.token, tenant: ctx.tenants.b, controlOnly: true, capabilities: { tenantControl: 1 } }));
      ctx.observe('status', await query(socket, 'control.developer.status', {}));
      ctx.observe('mine', await query(socket, 'control.tenants.mine', {}));
      ctx.observe('memberCannotEnter', await reducer(bob, 'control.developer.enter', { tenantId: ctx.tenants.a }, 'dev-bob'));
      ctx.observe('exit', await reducer(alice, 'control.developer.exit', { grantId: dev.id }, 'exit-1'));
      ctx.observe('exitAgain', await reducer(alice, 'control.developer.exit', { grantId: dev.id }, 'exit-2'));
    });
    await ctx.step('self-provisioned developer membership', async () => {
      const r = await reducer(alice, 'control.developer.provisionSelf', { tenantId: ctx.tenants.b }, 'self-1');
      if (r.result) ctx.alias(r.result.memberId, 'member:alice@b');
      ctx.observe('provision', r);
      ctx.observe('mine', await query(alice, 'control.tenants.mine', {}));
      ctx.observe('remove', await reducer(alice, 'control.developer.removeSelf', { tenantId: ctx.tenants.b }, 'self-2'));
      ctx.observe('removeAgain', await reducer(alice, 'control.developer.removeSelf', { tenantId: ctx.tenants.b }, 'self-3'));
      ctx.observe('mineAfter', await query(alice, 'control.tenants.mine', {}));
      ctx.observe('unknownTenant', await reducer(alice, 'control.developer.provisionSelf', { tenantId: 'nope' }, 'self-4'));
    });
    await ctx.step('agent claim tokens', async () => {
      ctx.observe('badExpiry', await reducer(alice, 'control.agentAuth.issue', { permissions: ['read'], expiresInSeconds: 0 }, 'agent-0'));
      const r = await reducer(alice, 'control.agentAuth.issue', { permissions: ['write', 'read', 'read'], expiresInSeconds: 60 }, 'agent-1');
      if (r.result) ctx.alias(r.result.id, 'agent:1');
      ctx.observe('issued', r);
      ctx.observe('claim', await reducer(bob, 'control.agentAuth.claim', { token: r.result?.token }, 'claim-1'));
      ctx.observe('claimAgain', await reducer(bob, 'control.agentAuth.claim', { token: r.result?.token }, 'claim-2'));
      ctx.observe('revoke', await reducer(alice, 'control.agentAuth.revoke', { id: r.result?.id }, 'agent-revoke-1'));
      ctx.observe('revokeAgain', await reducer(alice, 'control.agentAuth.revoke', { id: r.result?.id }, 'agent-revoke-2'));
    });
    await ctx.step('assistant and voice settings', async () => {
      ctx.observe('defaults', await query(alice, 'control.assistant.getDefaults', {}));
      ctx.observe('setDefaults', await reducer(alice, 'control.assistant.setDefaults', { scopeId: '', value: { model: 'small' } }, 'set-1'));
      ctx.observe('scoped', await reducer(alice, 'control.assistant.setDefaults', { scopeId: 'tenant-x', value: { model: 'large' } }, 'set-2'));
      ctx.observe('defaultsAfter', await query(alice, 'control.assistant.getDefaults', {}));
      ctx.observe('rateCard', await reducer(alice, 'control.voice.setRateCard', { scopeId: 'default', value: { perMinute: 2 } }, 'set-3'));
      ctx.observe('entitlement', await reducer(alice, 'control.voice.setTenantEntitlement', { scopeId: ctx.tenants.a, value: { minutes: 100 } }, 'set-4'));
      ctx.observe('override', await reducer(alice, 'control.voice.setUserOverride', { scopeId: 'bob', value: null }, 'set-5'));
      ctx.observe('voice', await query(alice, 'control.voice.getConfiguration', {}));
      ctx.observe('memberBlocked', await query(bob, 'control.voice.getConfiguration', {}));
    });
    await ctx.step('project developers', async () => {
      ctx.observe('list', await query(alice, 'control.project.developers.list', {}));
      ctx.observe('badRole', await reducer(alice, 'control.project.developers.invite', { email: 'carol@conformance.invalid', name: 'Carol', role: 'boss' }, 'devs-0'));
      ctx.observe('invite', await reducer(alice, 'control.project.developers.invite', { email: 'Carol@Conformance.invalid', name: 'Carol', role: 'dev' }, 'devs-1'));
      ctx.observe('listAfter', await query(alice, 'control.project.developers.list', {}));
      ctx.observe('remove', await reducer(alice, 'control.project.developers.remove', { email: 'carol@conformance.invalid' }, 'devs-2'));
      ctx.observe('removeAgain', await reducer(alice, 'control.project.developers.remove', { email: 'carol@conformance.invalid' }, 'devs-3'));
    });
    await ctx.step('demo accounts', async () => {
      const email = 'demo@conformance.invalid';
      ctx.observe('nonDemo', await reducer(alice, 'control.demos.create', { tenantId: ctx.tenants.a, email: 'bob@conformance.invalid', name: 'Bob', password: 'demo-password-1', label: 'x' }, 'demo-0'));
      const r = await reducer(alice, 'control.demos.create', { tenantId: ctx.tenants.a, email, name: 'Demo', password: 'demo-password-1', label: 'Demo' }, 'demo-1');
      if (r.result) { ctx.alias(r.result.accountId, 'account:demo'); ctx.alias(r.result.memberId, 'member:demo@a'); }
      ctx.observe('created', r);
      ctx.observe('replay', await reducer(alice, 'control.demos.create', { tenantId: ctx.tenants.a, email, name: 'Demo', password: 'demo-password-1', label: 'Demo' }, 'demo-1'));
      ctx.observe('resetPassword', await reducer(alice, 'control.demos.resetPassword', { accountId: r.result?.accountId, password: 'demo-password-2' }, 'demo-2'));
      ctx.observe('resetNonDemo', await reducer(alice, 'control.demos.resetPassword', { accountId: ctx.members.bob.accountId, password: 'demo-password-2' }, 'demo-3'));
      const login = await passwordLogin(ctx, 'demo', email, 'demo-password-2');
      ctx.observe('login', login.error ? login : pick(login.result, ['account', 'tenants', 'activeTenantId']));
      ctx.observe('delete', await reducer(alice, 'control.demos.delete', { accountId: r.result?.accountId }, 'demo-4'));
      ctx.observe('deleteUnknown', await reducer(alice, 'control.demos.delete', { accountId: 'acct_nope' }, 'demo-5'));
      ctx.observe('loginAfter', pick(await passwordLogin(ctx, 'demo-after', email, 'demo-password-2'), ['error']));
      ctx.observe('refreshAfter', await action(login.socket, 'control.auth.refreshSession', { refreshToken: login.result?.refreshToken }, 'demo-refresh'));
    });
  },
};
