/**
 * External identity sign-in with the Firebase Auth Emulator (CONTRACT.md C.1
 * rows 6, 9-11; C.3.4; docs/firebase-auth-emulator.md): realm configuration
 * and its validation, `exchangeExternalToken` for a new account, a returning
 * identity, verified-email linking to an existing account, invite-only signup,
 * `previousRefreshToken` revocation, token and provider failures, and a
 * Firebase-only project refusing passwords.
 *
 * Needs `GONVEX_CONFORMANCE_FIREBASE_EMULATOR=127.0.0.1:9099` (both backends
 * then enable the emulator decoder) and a Rust binary built with
 * `--features test-firebase-auth-emulator`. The emulator's project is
 * `GONVEX_CONFORMANCE_FIREBASE_PROJECT` (default `demo-gonvex-k06`).
 */
import { action, auth, memberSocket, pick, query, reducer, sessionShape } from './control-helpers.mjs';

const EMULATOR = process.env.GONVEX_CONFORMANCE_FIREBASE_EMULATOR;
const FIREBASE_PROJECT = process.env.GONVEX_CONFORMANCE_FIREBASE_PROJECT || 'demo-gonvex-k06';
const PASSWORD = 'firebase-password-1';

async function emulator(path, body, headers = {}) {
  const r = await fetch(`http://${EMULATOR}/identitytoolkit.googleapis.com/v1/${path}`, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) });
  const json = await r.json();
  if (!r.ok) throw Object.assign(new Error(json?.error?.message ?? `emulator ${r.status}`), { emulator: json?.error?.message });
  return json;
}

/** A Firebase user (created once; both backends share the emulator) and a fresh ID token. */
async function firebaseUser(email, { verified = false } = {}) {
  let user;
  try {
    user = await emulator('accounts:signUp?key=conformance', { email, password: PASSWORD, returnSecureToken: true });
  } catch (error) {
    if (error.emulator !== 'EMAIL_EXISTS') throw error;
    user = await emulator('accounts:signInWithPassword?key=conformance', { email, password: PASSWORD, returnSecureToken: true });
  }
  if (verified) {
    await emulator(`projects/${FIREBASE_PROJECT}/accounts:update`, { localId: user.localId, emailVerified: true }, { authorization: 'Bearer owner' });
    user = await emulator('accounts:signInWithPassword?key=conformance', { email, password: PASSWORD, returnSecureToken: true });
  }
  return user.idToken;
}

export default {
  name: 'control-external-auth',
  description: 'Control external auth (Firebase emulator): realms, exchange, linking, invite-only, previous refresh token, failures',
  tenants: { a: ['alice', 'bob'] },
  requires: ['firebaseEmulator'],
  tables: [],
  async run(ctx) {
    let alice, anon, first;
    const realm = (fields) => ({ provider: 'firebase', authMode: 'hybrid', enabled: true, signupMode: 'personal', firebaseProjectId: FIREBASE_PROJECT, ...fields });
    await ctx.step('a project admin configures a Firebase realm', async () => {
      alice = await memberSocket(ctx, 'alice', 'alice');
      ctx.observe('notAdmin', await reducer(alice, 'control.auth.realms.configure', realm(), 'realm-0'));
      await ctx.backend.addProjectMember(ctx.project, { email: 'alice@conformance.invalid', name: 'Alice', role: 'admin' });
      ctx.observe('badProvider', await reducer(alice, 'control.auth.realms.configure', realm({ provider: 'github' }), 'realm-1'));
      ctx.observe('badSignup', await reducer(alice, 'control.auth.realms.configure', realm({ signupMode: 'open' }), 'realm-2'));
      ctx.observe('enabledNotBool', await reducer(alice, 'control.auth.realms.configure', realm({ enabled: 'yes' }), 'realm-3'));
      ctx.observe('modeMismatch', await reducer(alice, 'control.auth.realms.configure', realm({ authMode: 'gonvex-native' }), 'realm-4'));
      ctx.observe('noProject', await reducer(alice, 'control.auth.realms.configure', realm({ firebaseProjectId: '' }), 'realm-5'));
      ctx.observe('oidcIncomplete', await reducer(alice, 'control.auth.realms.configure', { provider: 'external-oidc', authMode: 'hybrid', enabled: true, signupMode: 'personal', issuer: 'https://issuer.example' }, 'realm-6'));
      ctx.observe('httpJwks', await reducer(alice, 'control.auth.realms.configure', { provider: 'external-oidc', authMode: 'hybrid', enabled: false, signupMode: 'personal', jwksUrl: 'http://keys.example/jwks' }, 'realm-7'));
      ctx.observe('adminCredsWrongProvider', await reducer(alice, 'control.auth.realms.configure', { provider: 'google', enabled: false, signupMode: 'personal', adminCredentials: '{}' }, 'realm-8'));
      ctx.observe('adminCredsMismatch', await reducer(alice, 'control.auth.realms.configure', realm({ enabled: false, adminCredentials: '{"project_id":"other"}' }), 'realm-9'));
      ctx.observe('microsoftNoTenant', await reducer(alice, 'control.auth.realms.configure', { provider: 'microsoft', enabled: true, signupMode: 'personal', clientId: 'c', clientSecret: 's' }, 'realm-10'));
      ctx.observe('configure', await reducer(alice, 'control.auth.realms.configure', realm(), 'realm-11'));
      ctx.observe('secretStored', await reducer(alice, 'control.auth.realms.configure', { provider: 'apple', authMode: 'hybrid', enabled: true, signupMode: 'personal', clientId: 'apple-client', clientSecret: 'apple-secret' }, 'realm-12'));
      ctx.observe('secretKept', await reducer(alice, 'control.auth.realms.configure', { provider: 'apple', authMode: 'hybrid', enabled: true, signupMode: 'personal', clientId: 'apple-client' }, 'realm-13'));
      ctx.observe('realms', await query(alice, 'control.auth.realms.list', {}));
      anon = await ctx.raw('anon');
      await auth(anon, { project: ctx.project, controlOnly: true });
      ctx.observe('publicSettings', await query(anon, 'control.auth.publicSettings', {}));
    });
    await ctx.step('a new Firebase identity signs in', async () => {
      const token = await firebaseUser('fern@conformance.invalid');
      const r = await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token }, 'fx-1');
      if (r.result) ctx.alias(r.result.account.id, 'account:fern');
      first = r.result;
      ctx.observe('exchange', sessionShape(r));
      const again = await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: await firebaseUser('fern@conformance.invalid'), tenantId: 'ignored' }, 'fx-2');
      ctx.observe('returning', sessionShape(again));
      const fern = await ctx.raw('fern');
      ctx.observe('fernAuth', await auth(fern, { project: ctx.project, token: again.result?.accessToken, controlOnly: true }));
      ctx.observe('fernMe', await query(fern, 'control.accounts.me', {}));
    });
    await ctx.step('token and provider failures', async () => {
      ctx.observe('garbage', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: 'not-a-token' }, 'fx-bad-1'));
      ctx.observe('signed', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: 'eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln' }, 'fx-bad-2'));
      ctx.observe('wrongProvider', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'github', token: 'x' }, 'fx-bad-3'));
      ctx.observe('oidcDisabled', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'external-oidc', token: 'x' }, 'fx-bad-4'));
      ctx.observe('extraField', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: 'x', scope: 'all' }, 'fx-bad-5'));
      ctx.observe('noToken', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase' }, 'fx-bad-6'));
    });
    await ctx.step('a verified email links to the existing account', async () => {
      const token = await firebaseUser('bob@conformance.invalid', { verified: true });
      const r = await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token, tenantId: ctx.tenants.a }, 'fx-link');
      ctx.observe('linked', sessionShape(r));
      ctx.observe('providers', await query(alice, 'control.auth.memberProviders', { memberIds: [ctx.members.bob.memberIds.a] }));
    });
    await ctx.step('invite-only signup', async () => {
      ctx.observe('inviteOnly', await reducer(alice, 'control.auth.realms.configure', realm({ signupMode: 'inviteOnly' }), 'realm-14'));
      ctx.observe('unverified', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: await firebaseUser('una@conformance.invalid') }, 'fx-una-1'));
      // Separate users per state: the backends share the emulator, so no user's verification ever changes.
      const verified = await firebaseUser('ivy@conformance.invalid', { verified: true });
      ctx.observe('notInvited', await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: verified }, 'fx-ivy-2'));
      const invite = await reducer(alice, 'control.invitations.create', { email: 'ivy@conformance.invalid', role: 'member', teamIds: [], allowedAuthProviders: ['firebase'] }, 'inv-ivy');
      if (invite.result) ctx.alias(invite.result.id, 'invitation:ivy');
      const r = await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: verified }, 'fx-ivy-3');
      if (r.result) ctx.alias(r.result.account.id, 'account:ivy');
      ctx.observe('invited', sessionShape(r));
      const ivy = await ctx.raw('ivy');
      await auth(ivy, { project: ctx.project, token: r.result?.accessToken, controlOnly: true });
      const accepted = await reducer(ivy, 'control.invitations.accept', { token: invite.result?.token }, 'ivy-accept');
      if (accepted.result) ctx.alias(accepted.result.memberId, 'member:ivy@a');
      ctx.observe('accepted', accepted);
      ctx.observe('providers', await query(alice, 'control.auth.memberProviders', { memberIds: [accepted.result?.memberId] }));
      ctx.observe('tenantCreationBlocked', await reducer(ivy, 'control.tenants.create', { name: 'Ivy Co' }, 'ivy-create'));
    });
    await ctx.step('a new exchange revokes the previous refresh token', async () => {
      const r = await action(anon, 'control.auth.exchangeExternalToken', { provider: 'firebase', token: await firebaseUser('fern@conformance.invalid'), previousRefreshToken: first.refreshToken }, 'fx-3');
      ctx.observe('exchange', sessionShape(r));
      ctx.observe('oldRefresh', await action(anon, 'control.auth.refreshSession', { refreshToken: first.refreshToken }, 'fx-refresh-old'));
      ctx.observe('newRefresh', sessionShape(await action(anon, 'control.auth.refreshSession', { refreshToken: r.result?.refreshToken }, 'fx-refresh-new')));
    });
    await ctx.step('a Firebase-only project refuses passwords', async () => {
      ctx.observe('firebaseOnly', await reducer(alice, 'control.auth.realms.configure', realm({ authMode: 'firebase', signupMode: 'personal' }), 'realm-15'));
      ctx.observe('publicSettings', await query(anon, 'control.auth.publicSettings', {}));
      ctx.observe('passwordLogin', await action(anon, 'control.auth.passwordLogin', { email: 'bob@conformance.invalid', password: ctx.PASSWORD }, 'pw-1'));
      ctx.observe('updatePassword', await reducer(alice, 'control.accounts.updatePassword', { currentPassword: ctx.PASSWORD, newPassword: 'another-password-1' }, 'pw-2'));
      ctx.observe('provision', await reducer(alice, 'control.accounts.provisionMemberLogin', { email: 'hal@conformance.invalid', name: 'Hal', password: 'hal-password-12', role: 'member', permissions: {} }, 'pw-3'));
      ctx.observe('realms', (await query(alice, 'control.auth.realms.list', {})).result?.map(row => pick(row, ['provider', 'enabled', 'authMode', 'signupMode'])));
    });
  },
};
