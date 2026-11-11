/**
 * Native OAuth HTTP routes (CONTRACT.md C.3.5, D.1 "Native OAuth";
 * native_auth.rs): `/auth/config`, `/auth/{provider}/authorize` with PKCE and
 * redirect registration, the callback's state handling, `/auth/token` (both
 * grants and their failures), `/auth/logout` and CORS. The provider round trip
 * itself needs Google, so the callback is driven through its error branch.
 *
 * Both backends get GONVEX_AUTH_URL (their own base URL) and the fallback
 * Google client of CONFORMANCE_SECRETS; the authorize redirect is observed
 * with the backend's base URL replaced by `<base>`.
 */
import { createHash } from 'node:crypto';
import { action, auth } from './control-helpers.mjs';

const APP = 'http://localhost:5173/auth/callback';
const STATE = 'conformance-app-state-0001';
const VERIFIER = 'v'.repeat(43);
const CHALLENGE = createHash('sha256').update(VERIFIER).digest('base64url');

/** One HTTP exchange, shaped for observation (body parsed when JSON). */
async function http(ctx, method, path, { body, json, headers = {} } = {}) {
  const init = { method, headers: { ...headers }, redirect: 'manual' };
  if (json !== undefined) { init.headers['content-type'] = 'application/json'; init.body = JSON.stringify(json); }
  else if (body !== undefined) init.body = body;
  const r = await fetch(ctx.backend.httpUrl + path, init);
  const text = await r.text();
  let parsed; try { parsed = JSON.parse(text); } catch { parsed = text; }
  const base = ctx.backend.httpUrl;
  const location = r.headers.get('location');
  return {
    status: r.status,
    ...(location ? { location: location.split(base).join('<base>') } : {}),
    cors: r.headers.get('access-control-allow-origin'),
    body: parsed === '' ? null : parsed,
  };
}

/** A redirect URL split into origin+path and query pairs (random tokens masked). */
function redirectShape(location, base) {
  if (!location) return null;
  const url = new URL(location.replace('<base>', 'http://base.invalid'));
  const value = (v) => (/^gvx_[a-z]+_/.test(v) ? v.replace(/^(gvx_[a-z]+_).*/, '$1…') : v.split(base).join('<base>'));
  const query = Object.fromEntries([...url.searchParams].map(([k, v]) => [k, value(v)]));
  return { target: `${url.origin}${url.pathname}`.replace('http://base.invalid', '<base>'), query };
}

export default {
  name: 'control-native-auth',
  description: 'Native OAuth routes: config, authorize (PKCE, redirects), callback state, token grants, logout, CORS',
  tenants: { a: ['alice'] },
  tables: [],
  async run(ctx) {
    const p = encodeURIComponent(ctx.project);
    const authorize = (provider, fields = {}) => {
      const q = new URLSearchParams({ project: ctx.project, redirect_uri: APP, state: STATE, code_challenge: CHALLENGE, code_challenge_method: 'S256', ...fields });
      return http(ctx, 'GET', `/auth/${provider}/authorize?${q}`);
    };
    await ctx.step('configuration before any provider', async () => {
      ctx.observe('config', await http(ctx, 'GET', `/auth/config?project=${p}`));
      ctx.observe('noProject', await http(ctx, 'GET', '/auth/config?project='));
      ctx.observe('unknownProject', await http(ctx, 'GET', '/auth/config?project=nope'));
      ctx.observe('notRegistered', await authorize('google'));
    });
    let state;
    await ctx.step('Google with a registered redirect', async () => {
      await ctx.backend.configureGoogleAuth(ctx.project, { redirectUri: APP });
      ctx.observe('config', await http(ctx, 'GET', `/auth/config?project=${p}`));
      ctx.observe('unsupported', await authorize('github'));
      ctx.observe('shortState', await authorize('google', { state: 'short' }));
      ctx.observe('plainPkce', await authorize('google', { code_challenge_method: 'plain' }));
      ctx.observe('httpRedirect', await authorize('google', { redirect_uri: 'http://app.example/cb' }));
      ctx.observe('fragment', await authorize('google', { redirect_uri: 'https://app.example/cb#x' }));
      ctx.observe('otherRedirect', await authorize('google', { redirect_uri: 'https://app.example/cb' }));
      ctx.observe('microsoftUnconfigured', await authorize('microsoft'));
      const ok = await authorize('google');
      ctx.observe('authorize', { status: ok.status, cors: ok.cors, redirect: redirectShape(ok.location, ctx.backend.httpUrl) });
      state = ok.location ? new URL(ok.location.replace('<base>', 'http://base.invalid')).searchParams.get('state') : null;
    });
    await ctx.step('the callback consumes its state once', async () => {
      ctx.observe('unknownState', await http(ctx, 'GET', '/auth/google/callback?state=gvx_oauth_nope&code=x'));
      const r = await http(ctx, 'GET', `/auth/google/callback?state=${encodeURIComponent(state)}&error=access_denied`);
      ctx.observe('providerError', { status: r.status, redirect: redirectShape(r.location, ctx.backend.httpUrl) });
      ctx.observe('stateReused', await http(ctx, 'GET', `/auth/google/callback?state=${encodeURIComponent(state)}&error=access_denied`));
      const again = await authorize('google');
      const second = new URL(again.location.replace('<base>', 'http://base.invalid')).searchParams.get('state');
      const mismatch = await http(ctx, 'GET', `/auth/apple/callback?state=${encodeURIComponent(second)}&code=x`);
      ctx.observe('providerMismatch', { status: mismatch.status, redirect: redirectShape(mismatch.location, ctx.backend.httpUrl) });
      const form = await http(ctx, 'POST', '/auth/google/callback', { body: 'state=nope', headers: { 'content-type': 'text/plain' } });
      ctx.observe('formContentType', { status: form.status });
    });
    await ctx.step('token grants', async () => {
      const socket = await ctx.raw('login');
      await auth(socket, { project: ctx.project, controlOnly: true });
      const login = await action(socket, 'control.auth.passwordLogin', { email: ctx.members.alice.email, password: ctx.PASSWORD }, 'login-native');
      const refreshToken = login.result.refreshToken;
      ctx.observe('preflight', await http(ctx, 'OPTIONS', '/auth/token'));
      ctx.observe('noProject', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'refresh_token', project: ' ', refreshToken } }));
      ctx.observe('badGrant', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'password', project: ctx.project } }));
      ctx.observe('missingGrant', (await http(ctx, 'POST', '/auth/token', { json: { project: ctx.project } })).status);
      ctx.observe('notJson', (await http(ctx, 'POST', '/auth/token', { body: 'grantType=refresh_token', headers: { 'content-type': 'application/x-www-form-urlencoded' } })).status);
      ctx.observe('badCode', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'authorization_code', project: ctx.project, code: 'gvx_authcode_nope', codeVerifier: VERIFIER, redirectUri: APP } }));
      ctx.observe('shortVerifier', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'authorization_code', project: ctx.project, code: 'x', codeVerifier: 'short', redirectUri: APP } }));
      const refreshed = await http(ctx, 'POST', '/auth/token', { json: { grantType: 'refresh_token', project: ctx.project, refreshToken } });
      ctx.observe('refresh', { status: refreshed.status, cors: refreshed.cors, account: refreshed.body?.account, tenants: refreshed.body?.tenants, activeTenantId: refreshed.body?.activeTenantId, tokenType: refreshed.body?.tokenType });
      ctx.observe('rotatedAgain', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'refresh_token', project: ctx.project, refreshToken } }));
      ctx.observe('wrongProject', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'refresh_token', project: 'nope', refreshToken: refreshed.body?.refreshToken } }));
      ctx.observe('logout', await http(ctx, 'POST', '/auth/logout', { json: { refreshToken: refreshed.body?.refreshToken } }));
      ctx.observe('afterLogout', await http(ctx, 'POST', '/auth/token', { json: { grantType: 'refresh_token', project: ctx.project, refreshToken: refreshed.body?.refreshToken } }));
      ctx.observe('logoutUnknown', await http(ctx, 'POST', '/auth/logout', { json: { refreshToken: 'gvx_refresh_nope' } }));
      const second = await action(socket, 'control.auth.passwordLogin', { email: ctx.members.alice.email, password: ctx.PASSWORD }, 'login-native-2');
      ctx.observe('logoutAll', await http(ctx, 'POST', '/auth/logout', { json: { all: true }, headers: { authorization: `Bearer ${second.result.accessToken}` } }));
      const check = await ctx.raw('check');
      ctx.observe('accessAfterLogoutAll', await auth(check, { project: ctx.project, token: login.result.accessToken, controlOnly: true }));
    });
  },
};
