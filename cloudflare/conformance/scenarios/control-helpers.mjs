/**
 * Frame-level helpers for the control-plane scenarios: `scope:"control"`
 * calls, live Control subscriptions and logins on raw sockets.
 */
import { randomUUID } from 'node:crypto';

const REPLY = {
  'query.call': ['query.result', 'query.error'],
  'query.subscribe': ['query.result', 'query.error'],
  'reducer.call': ['reducer.result', 'reducer.error'],
  'action.call': ['action.result', 'action.error'],
};

/**
 * One `scope:"control"` call. Writes carry `key` as the idempotency key
 * (omit with `key: null`). Resolves to `{ result }` or `{ error, class? }`.
 */
export async function controlCall(socket, type, path, args = {}, { key, id = randomUUID() } = {}) {
  const frame = { type, id, path, args, scope: 'control' };
  if (type !== 'query.call' && type !== 'query.subscribe' && key !== null) frame.idempotencyKey = key ?? id;
  socket.send(frame);
  const [ok, fail] = REPLY[type];
  const reply = await socket.next(m => (m.type === ok || m.type === fail) && m.id === id, { label: `${path} reply` });
  if (reply.type === ok) return { result: reply.result };
  return { error: reply.error, ...(reply.class ? { class: reply.class } : {}), ...(reply.retryable !== undefined ? { retryable: reply.retryable } : {}) };
}

export const query = (socket, path, args, options) => controlCall(socket, 'query.call', path, args, options);
export const reducer = (socket, path, args, key, options = {}) => controlCall(socket, 'reducer.call', path, args, { ...options, key });
export const action = (socket, path, args, key, options = {}) => controlCall(socket, 'action.call', path, args, { ...options, key });

/** Opens a live Control Query; resolves to `{ id, initial }` (`initial` is `{ result }` or `{ error }`). */
export async function subscribe(socket, path, args = {}) {
  const id = randomUUID();
  const initial = await controlCall(socket, 'query.subscribe', path, args, { id });
  return { id, initial };
}

/** The next `query.result` / `query.error` for a subscription (a refresh). */
export async function nextUpdate(socket, id, label = 'subscription refresh') {
  const m = await socket.next(x => (x.type === 'query.result' || x.type === 'query.error') && x.id === id, { label });
  return m.type === 'query.result' ? { reason: m.reason, result: m.result } : { error: m.error };
}

/** Sends `auth` and resolves to the `auth.result` / `auth.error` frame, shaped for observation. */
export async function auth(socket, fields) {
  const id = randomUUID();
  socket.send({ type: 'auth', id, clientContract: 1, ...fields });
  const m = await socket.next(x => (x.type === 'auth.result' || x.type === 'auth.error') && x.id === id, { label: 'auth reply' });
  return m.type === 'auth.result' ? { result: m.result } : { error: m.error, code: m.code };
}

/** Password login on a control-only socket of its own; returns the session. */
export async function passwordLogin(ctx, tag, email, password = ctx.PASSWORD) {
  const socket = await ctx.raw(tag);
  await auth(socket, { project: ctx.project, controlOnly: true });
  const r = await action(socket, 'control.auth.passwordLogin', { email, password }, `login-${tag}`);
  return { socket, ...r };
}

/**
 * A raw socket authenticated as a seeded Member for Control calls in a
 * tenant: control-only with `tenantControl=1`, as the dashboard does, so the
 * connection carries the tenant for TenantAdmin calls but has no tenant data
 * session (no change feed, so no `replica.watermark` for tenant commits).
 */
export async function memberSocket(ctx, tag, member, tenant = 'a') {
  const socket = await ctx.raw(tag);
  const session = await ctx.rawLogin(socket, member);
  const r = await tenantControl(ctx, socket, session.accessToken, ctx.tenants[tenant] ?? tenant);
  if (r.error) throw new Error(`auth as ${member}: ${r.error}`);
  return socket;
}

/** Control-only auth that keeps a tenant (`capabilities.tenantControl=1`). */
export const tenantControl = (ctx, socket, token, tenant) =>
  auth(socket, { project: ctx.project, token, tenant, controlOnly: true, capabilities: { tenantControl: 1 } });

/** Keeps only the listed keys of each object (for observations of result rows). */
export const pick = (value, keys) => Array.isArray(value) ? value.map(v => pick(v, keys)) : value && typeof value === 'object'
  ? Object.fromEntries(keys.filter(k => k in value).map(k => [k, value[k]])) : value;

/** Session result without token values (shape plus account and tenants). */
export const sessionShape = (r) => r.error ? r : {
  hasAccess: typeof r.result.accessToken === 'string' && r.result.accessToken.startsWith('gvx_session_'),
  hasRefresh: typeof r.result.refreshToken === 'string' && r.result.refreshToken.startsWith('gvx_refresh_'),
  tokenType: r.result.tokenType, expiresIn: r.result.expiresIn,
  account: r.result.account, tenants: r.result.tenants, activeTenantId: r.result.activeTenantId,
};
