/**
 * Control Plane invitations (CONTRACT.md C.1 rows 21-26, 59-60; rows 3, 12):
 * create with validation, the live list refreshed on another socket, lookup
 * (and the legacy lookup), update, revoke, provider policy, wrong account,
 * acceptance with replay and a stale key, an application payload without a
 * declared acceptance Reducer, member providers and member password reset.
 */
import { action, auth, memberSocket, nextUpdate, passwordLogin, pick, query, reducer, subscribe, tenantControl } from './control-helpers.mjs';

const DAVE = 'dave@conformance.invalid';
const ERIN = 'erin@conformance.invalid';
const NEW_PASSWORD = 'dave-new-password-1';

export default {
  name: 'control-invitations',
  description: 'Control invitations: create, live list, lookup, update, revoke, provider policy, accept, replay, payload, reset password',
  tenants: { a: ['alice', 'bob'] },
  tables: [],
  async run(ctx) {
    let alice, bob, list, dave, erin, invite;
    await ctx.step('accounts without a membership', async () => {
      ctx.alias((await ctx.backend.createAccount(ctx.project, { email: DAVE, password: ctx.PASSWORD, name: 'dave' })).accountId, 'account:dave');
      ctx.alias((await ctx.backend.createAccount(ctx.project, { email: ERIN, password: ctx.PASSWORD, name: 'erin' })).accountId, 'account:erin');
      const d = await passwordLogin(ctx, 'dave', DAVE);
      dave = d.socket;
      ctx.observe('daveLogin', d.error ? d : pick(d.result, ['account', 'tenants', 'activeTenantId']));
      await auth(dave, { project: ctx.project, token: d.result.accessToken, controlOnly: true });
      dave.session = d.result;
      const e = await passwordLogin(ctx, 'erin', ERIN);
      erin = e.socket;
      await auth(erin, { project: ctx.project, token: e.result.accessToken, controlOnly: true });
      ctx.observe('daveMine', await query(dave, 'control.tenants.mine', {}));
    });
    await ctx.step('alice watches the invitation list', async () => {
      alice = await memberSocket(ctx, 'alice', 'alice');
      list = await subscribe(alice, 'control.invitations.list');
      ctx.observe('initial', list.initial);
      bob = await memberSocket(ctx, 'bob', 'bob');
    });
    await ctx.step('invitation validation', async () => {
      const base = { email: DAVE, role: 'member', teamIds: [], allowedAuthProviders: [] };
      ctx.observe('badEmail', await reducer(bob, 'control.invitations.create', { ...base, email: 'nope' }, 'inv-bad-email'));
      ctx.observe('badRole', await reducer(bob, 'control.invitations.create', { ...base, role: 'boss' }, 'inv-bad-role'));
      ctx.observe('noTeams', await reducer(bob, 'control.invitations.create', { email: DAVE, role: 'member', allowedAuthProviders: [] }, 'inv-no-teams'));
      ctx.observe('badTeam', await reducer(bob, 'control.invitations.create', { ...base, teamIds: ['ok', ' '] }, 'inv-bad-team'));
      ctx.observe('badProvider', await reducer(bob, 'control.invitations.create', { ...base, allowedAuthProviders: ['github'] }, 'inv-bad-provider'));
      ctx.observe('badPermissions', await reducer(bob, 'control.invitations.create', { ...base, permissions: [] }, 'inv-bad-perms'));
      ctx.observe('adminGrantsAdmin', await reducer(bob, 'control.invitations.create', { ...base, role: 'admin' }, 'inv-admin'));
      ctx.observe('memberCannot', await reducer(dave, 'control.invitations.create', base, 'inv-dave'));
    });
    await ctx.step('bob invites dave; alice sees it live', async () => {
      const r = await reducer(bob, 'control.invitations.create', { email: ' Dave@Conformance.invalid ', role: 'member', permissions: { notes: true }, teamIds: [], allowedAuthProviders: [] }, 'inv-1');
      invite = r.result;
      if (invite) ctx.alias(invite.id, 'invitation:dave-1');
      ctx.observe('created', r);
      ctx.observe('aliceRefresh', await nextUpdate(alice, list.id, 'invitation list refresh'));
      ctx.observe('replay', pick((await reducer(bob, 'control.invitations.create', { email: DAVE, role: 'member', teamIds: [], allowedAuthProviders: [] }, 'inv-1')).result ?? {}, ['id']));
    });
    await ctx.step('lookups', async () => {
      const anon = await ctx.raw('anon');
      await auth(anon, { project: ctx.project, controlOnly: true });
      ctx.observe('lookup', await query(anon, 'control.invitations.lookup', { token: invite.token }));
      ctx.observe('legacyLookup', await query(anon, 'tenants.getInvitationByToken', { invitationToken: invite.token }));
      ctx.observe('badToken', await query(anon, 'control.invitations.lookup', { token: 'gvx_invite_nope' }));
      ctx.observe('noToken', await query(anon, 'control.invitations.lookup', {}));
      ctx.observe('anonAccept', await reducer(anon, 'control.invitations.accept', { token: invite.token }, 'anon-accept'));
    });
    await ctx.step('update, revoke and provider policy', async () => {
      ctx.observe('update', await reducer(bob, 'control.invitations.update', { id: invite.id, role: 'viewer', teamIds: [], allowedAuthProviders: ['password'] }, 'upd-1'));
      ctx.observe('afterUpdate', pick((await query(dave, 'control.invitations.lookup', { token: invite.token })).result ?? {}, ['role', 'allowedAuthProviders', 'email']));
      ctx.observe('updateUnknown', await reducer(bob, 'control.invitations.update', { id: 'invite_nope', role: 'member', teamIds: [], allowedAuthProviders: [] }, 'upd-2'));
      ctx.observe('revokeNeither', await reducer(bob, 'control.invitations.revoke', {}, 'rev-0'));
      ctx.observe('revoke', await reducer(bob, 'control.invitations.revoke', { email: 'DAVE@conformance.invalid' }, 'rev-1'));
      ctx.observe('lookupRevoked', await query(dave, 'control.invitations.lookup', { token: invite.token }));
      ctx.observe('acceptRevoked', await reducer(dave, 'control.invitations.accept', { token: invite.token }, 'acc-revoked'));
      const again = await reducer(bob, 'control.invitations.create', { email: DAVE, role: 'member', teamIds: [], allowedAuthProviders: ['google'] }, 'inv-2');
      invite = again.result;
      if (invite) ctx.alias(invite.id, 'invitation:dave-2');
      ctx.observe('recreated', again);
      ctx.observe('needsProvider', await reducer(dave, 'control.invitations.accept', { token: invite.token }, 'acc-provider'));
      ctx.observe('relaxProviders', await reducer(bob, 'control.invitations.update', { id: invite.id, role: 'member', teamIds: [], allowedAuthProviders: ['password', 'google'] }, 'upd-3'));
      ctx.observe('wrongAccount', await reducer(erin, 'control.invitations.accept', { token: invite.token }, 'acc-erin'));
      await ctx.quiesce();
    });
    await ctx.step('dave accepts', async () => {
      const r = await reducer(dave, 'control.invitations.accept', { token: invite.token }, 'acc-1');
      if (r.result) ctx.alias(r.result.memberId, 'member:dave@a');
      ctx.observe('accepted', r);
      ctx.observe('replay', await reducer(dave, 'control.invitations.accept', { token: invite.token }, 'acc-1'));
      ctx.observe('legacyReplay', await reducer(dave, 'tenants.acceptInvitation', { invitationToken: invite.token }, 'acc-1'));
      ctx.observe('otherKey', await reducer(dave, 'control.invitations.accept', { token: invite.token }, 'acc-2'));
      ctx.observe('daveMine', await query(dave, 'control.tenants.mine', {}));
      await ctx.until(async () => {
        const rows = (await query(alice, 'control.invitations.list', {})).result ?? [];
        return rows.some(row => row.accepted);
      }, 'accepted invitation listed');
      const rows = (await query(alice, 'control.invitations.list', {})).result ?? [];
      ctx.observe('list', rows.map(row => pick(row, ['id', 'email', 'role', 'permissions', 'teamIds', 'allowedAuthProviders', 'revoked', 'accepted', 'state', 'expiresAt'])));
      ctx.observe('lookupAccepted', await query(dave, 'control.invitations.lookup', { token: invite.token }));
    });
    await ctx.step('an application payload needs a declared acceptance Reducer', async () => {
      // A failed acceptance leaves its claim. Rust's live list may show that
      // only through its delayed notify echo (README, "Reference behaviours"),
      // so the list is read on demand from here on.
      alice.send({ type: 'query.unsubscribe', id: list.id });
      const r = await reducer(bob, 'control.invitations.create', { email: ERIN, role: 'member', teamIds: ['team-1'], allowedAuthProviders: [], payload: { seat: 'a' } }, 'inv-erin');
      if (r.result) ctx.alias(r.result.id, 'invitation:erin');
      ctx.observe('created', pick(r.result ?? r, ['id', 'error']));
      ctx.observe('accept', await reducer(erin, 'control.invitations.accept', { token: r.result.token }, 'erin-1'));
      ctx.observe('retrySameKey', await reducer(erin, 'control.invitations.accept', { token: r.result.token }, 'erin-1'));
      ctx.observe('retryOtherKey', await reducer(erin, 'control.invitations.accept', { token: r.result.token }, 'erin-2'));
      const rows = (await query(alice, 'control.invitations.list', {})).result ?? [];
      ctx.observe('erinState', rows.filter(row => row.email === ERIN).map(row => pick(row, ['state', 'accepted', 'teamIds'])));
    });
    await ctx.step('member providers and a member password reset', async () => {
      const accepted = (await reducer(dave, 'control.invitations.accept', { token: invite.token }, 'acc-1')).result;
      ctx.observe('providers', await query(alice, 'control.auth.memberProviders', { memberIds: [accepted.memberId, ctx.members.alice.memberIds.a, 'member_nope', accepted.memberId] }));
      ctx.observe('providersNotArray', await query(alice, 'control.auth.memberProviders', { memberIds: 'x' }));
      ctx.observe('resetAdmin', await reducer(alice, 'control.accounts.resetMemberPassword', { memberId: ctx.members.bob.memberIds.a, newPassword: NEW_PASSWORD }, 'reset-bob'));
      ctx.observe('resetUnknown', await reducer(alice, 'control.accounts.resetMemberPassword', { memberId: 'member_nope', newPassword: NEW_PASSWORD }, 'reset-nope'));
      ctx.observe('resetShort', await reducer(alice, 'control.accounts.resetMemberPassword', { memberId: accepted.memberId, newPassword: 'short' }, 'reset-short'));
      ctx.observe('reset', await reducer(alice, 'control.accounts.resetMemberPassword', { memberId: accepted.memberId, newPassword: NEW_PASSWORD }, 'reset-dave'));
      // Every session of dave is revoked: his old token no longer authenticates.
      const check = await ctx.raw('dave-check');
      ctx.observe('oldToken', await tenantControl(ctx, check, dave.session.accessToken, ctx.tenants.a));
      ctx.observe('oldRefresh', await action(check, 'control.auth.refreshSession', { refreshToken: dave.session.refreshToken }, 'dave-refresh'));
      ctx.observe('oldPassword', pick(await passwordLogin(ctx, 'dave-old', DAVE), ['error']));
      const login = await passwordLogin(ctx, 'dave-new', DAVE, NEW_PASSWORD);
      ctx.observe('newPassword', login.error ? login : pick(login.result, ['account', 'tenants', 'activeTenantId']));
    });
  },
};
