/**
 * Control Plane tenants (CONTRACT.md C.1 rows 13-20, 58; C.4): the live
 * `control.tenants.mine`, creation with domain rules, idempotent replay and
 * the tenant directory, profile and timezone updates, member provisioning,
 * tenant-admin and project-admin refusals, project settings and soft delete.
 */
import { auth, controlCall, memberSocket, nextUpdate, passwordLogin, pick, query, reducer, subscribe, tenantControl } from './control-helpers.mjs';

export default {
  name: 'control-tenants',
  description: 'Control tenants: mine (live), create, domain rules, replay, profile, timezone, provisioning, refusals, delete',
  tenants: { a: ['alice'] },
  tables: [],
  async run(ctx) {
    let alice, sub, created;
    await ctx.step('alice watches her tenants', async () => {
      alice = await memberSocket(ctx, 'alice', 'alice');
      sub = await subscribe(alice, 'control.tenants.mine');
      ctx.observe('mine', sub.initial);
      ctx.observe('myTenants', await query(alice, 'users.myTenants', {}));
      ctx.observe('developerStatus', await query(alice, 'control.developer.status', {}));
      ctx.observe('notSubscribable', (await subscribe(alice, 'control.accounts.me')).initial);
      ctx.observe('meExtraArgument', await query(alice, 'control.accounts.me', { x: 1 }));
      ctx.observe('unknownFunction', await query(alice, 'control.nope', {}));
      ctx.observe('wrongKind', await query(alice, 'control.tenants.create', { name: 'x' }));
    });
    await ctx.step('tenant creation refusals', async () => {
      ctx.observe('badDomain', await reducer(alice, 'control.tenants.create', { name: 'Acme', domain: 'Bad_Domain' }, 'create-bad'));
      ctx.observe('leadingDash', await reducer(alice, 'control.tenants.create', { name: 'Acme', domain: '-acme' }, 'create-dash'));
      ctx.observe('noName', await reducer(alice, 'control.tenants.create', { name: '   ' }, 'create-noname'));
      ctx.observe('extraField', await reducer(alice, 'control.tenants.create', { name: 'Acme', databaseUrl: 'x' }, 'create-extra'));
      ctx.observe('noKey', await controlCall(alice, 'reducer.call', 'control.tenants.create', { name: 'Acme' }, { key: null }));
    });
    await ctx.step('alice creates a tenant; her live list refreshes first', async () => {
      const r = await reducer(alice, 'control.tenants.create', { name: 'Acme Works' }, 'create-1');
      if (r.result?.id) created = ctx.alias(r.result.id, 'tenant:created');
      ctx.observe('created', r);
      ctx.observe('refresh', await nextUpdate(alice, sub.id));
      const order = ctx.frames('alice', { dir: 'in' }).filter(f => f.id === sub.id || f.path === 'control.tenants.create').map(f => f.type);
      ctx.expect(order.indexOf('query.result') < order.lastIndexOf('reducer.result'), 'subscription refresh precedes the reducer result');
    });
    await ctx.step('replay, domain conflict and lookup by domain', async () => {
      ctx.observe('replay', await reducer(alice, 'control.tenants.create', { name: 'Acme Works' }, 'create-1'));
      ctx.observe('domainInUse', await reducer(alice, 'control.tenants.create', { name: 'Other', domain: 'acme-works' }, 'create-2'));
      ctx.observe('keyForOtherPath', await reducer(alice, 'control.tenants.updateTimezone', { timezone: 'UTC' }, 'create-1'));
      const anon = await ctx.raw('anon');
      ctx.observe('anonAuth', await auth(anon, { project: ctx.project, controlOnly: true }));
      ctx.observe('byDomain', await query(anon, 'control.tenants.getByDomain', { domain: 'ACME-works' }));
      ctx.observe('unknownDomain', await query(anon, 'control.tenants.getByDomain', { domain: 'nope' }));
      ctx.observe('anonMine', await query(anon, 'control.tenants.mine', {}));
      ctx.observe('anonCreate', await reducer(anon, 'control.tenants.create', { name: 'Anon' }, 'anon-create'));
    });
    await ctx.step('alice works in the created tenant', async () => {
      const session = await ctx.rawLogin(alice, 'alice');
      ctx.observe('auth', await tenantControl(ctx, alice, session.accessToken, created));
      ctx.observe('mine', await query(alice, 'control.tenants.mine', {}));
      ctx.observe('profile', await reducer(alice, 'control.tenants.updateProfile', { name: 'Acme', domain: 'Acme-Two', description: ' Our team ' }, 'profile-1'));
      ctx.observe('badTimezone', await reducer(alice, 'control.tenants.updateTimezone', { timezone: 'Mars' }, 'tz-bad'));
      ctx.observe('timezone', await reducer(alice, 'control.tenants.updateTimezone', { timezone: 'Europe/Paris' }, 'tz-1'));
      ctx.observe('mineAfter', await query(alice, 'control.tenants.mine', {}));
      ctx.observe('byNewDomain', await query(alice, 'control.tenants.getByDomain', { domain: 'acme-two' }));
    });
    let carol;
    await ctx.step('a tenant admin provisions a member login', async () => {
      const session = await ctx.rawLogin(alice, 'alice');
      await tenantControl(ctx, alice, session.accessToken, ctx.tenants.a);
      ctx.observe('adminCannotGrantAdmin', await reducer(alice, 'control.accounts.provisionMemberLogin',
        { email: 'carol@conformance.invalid', name: 'Carol', password: 'carol-password-1', role: 'admin', permissions: {} }, 'prov-admin'));
      ctx.observe('shortPassword', await reducer(alice, 'control.accounts.provisionMemberLogin',
        { email: 'carol@conformance.invalid', name: 'Carol', password: 'short', role: 'member', permissions: {} }, 'prov-short'));
      ctx.observe('viewerRole', await reducer(alice, 'control.accounts.provisionMemberLogin',
        { email: 'carol@conformance.invalid', name: 'Carol', password: 'carol-password-1', role: 'viewer', permissions: {} }, 'prov-viewer'));
      const r = await reducer(alice, 'control.accounts.provisionMemberLogin',
        { email: 'Carol@Conformance.invalid', name: 'Carol', password: 'carol-password-1', role: 'member', permissions: { notes: true } }, 'prov-1');
      if (r.result) { ctx.alias(r.result.accountId, 'account:carol'); ctx.alias(r.result.memberId, 'member:carol@a'); }
      ctx.observe('provisioned', r);
      ctx.observe('replay', await reducer(alice, 'control.accounts.provisionMemberLogin',
        { email: 'carol@conformance.invalid', name: 'Carol', password: 'carol-password-1', role: 'member', permissions: { notes: true } }, 'prov-1'));
      ctx.observe('existingAccount', await reducer(alice, 'control.accounts.provisionMemberLogin',
        { email: 'alice@conformance.invalid', name: 'Alice', password: 'alice-password-1', role: 'member', permissions: {} }, 'prov-alice'));
      const login = await passwordLogin(ctx, 'carol-login', 'carol@conformance.invalid', 'carol-password-1');
      ctx.observe('carolLogin', login.error ? login : { account: login.result.account, tenants: login.result.tenants });
      carol = login.socket;
      if (login.result) ctx.observe('carolAuth', await tenantControl(ctx, carol, login.result.accessToken, ctx.tenants.a));
    });
    await ctx.step('a plain member is refused tenant and project administration', async () => {
      ctx.observe('timezone', await reducer(carol, 'control.tenants.updateTimezone', { timezone: 'UTC' }, 'carol-tz'));
      ctx.observe('invitations', await query(carol, 'control.invitations.list', {}));
      ctx.observe('seatLimit', await reducer(carol, 'control.tenants.setSeatLimit', { tenantId: ctx.tenants.a, seatLimit: 3 }, 'carol-seats'));
      ctx.observe('supportTenants', await query(carol, 'control.support.listTenants', {}));
      ctx.observe('mine', await query(carol, 'control.tenants.mine', {}));
    });
    await ctx.step('a project admin sets tenant limits', async () => {
      ctx.observe('beforeGrant', await reducer(alice, 'control.tenants.setSeatLimit', { tenantId: ctx.tenants.a, seatLimit: 25 }, 'seats-0'));
      await ctx.backend.addProjectMember(ctx.project, { email: 'alice@conformance.invalid', name: 'Alice', role: 'admin' });
      ctx.observe('seatLimit', await reducer(alice, 'control.tenants.setSeatLimit', { tenantId: ctx.tenants.a, seatLimit: 25 }, 'seats-1'));
      ctx.observe('negative', await reducer(alice, 'control.tenants.setSeatLimit', { tenantId: ctx.tenants.a, seatLimit: -1 }, 'seats-2'));
      ctx.observe('fraction', await reducer(alice, 'control.tenants.setSeatLimit', { tenantId: ctx.tenants.a, seatLimit: 1.5 }, 'seats-3'));
      ctx.observe('unknownTenant', await reducer(alice, 'control.tenants.setSeatLimit', { tenantId: 'nope', seatLimit: 1 }, 'seats-4'));
      ctx.observe('exception', await reducer(alice, 'control.tenants.setException', { tenantId: ctx.tenants.a, value: { reason: 'pilot' } }, 'exc-1'));
      ctx.observe('tenant', pick((await query(alice, 'control.support.getTenant', { tenantId: ctx.tenants.a })).result ?? {}, ['id', 'name', 'domain', 'status', 'timezone', 'description', 'profile', 'seatLimit']));
      ctx.observe('clearSeats', await reducer(alice, 'control.tenants.setSeatLimit', { tenantId: ctx.tenants.a, seatLimit: null }, 'seats-5'));
    });
    await ctx.step('the created tenant is deleted', async () => {
      const session = await ctx.rawLogin(alice, 'alice');
      await tenantControl(ctx, alice, session.accessToken, created);
      ctx.observe('delete', await reducer(alice, 'control.tenants.delete', {}, 'delete-1'));
      ctx.observe('deleteAgain', await reducer(alice, 'control.tenants.delete', {}, 'delete-2'));
      ctx.observe('mine', await query(alice, 'control.tenants.mine', {}));
      ctx.observe('byDomain', await query(alice, 'control.tenants.getByDomain', { domain: 'acme-two' }));
      ctx.observe('authDeleted', await tenantControl(ctx, alice, session.accessToken, created));
      // A full tenant session (not control-only) to the deleted tenant fails the same way.
      ctx.observe('fullAuthDeleted', await auth(alice, { project: ctx.project, token: session.accessToken, tenant: created }));
    });
  },
};
