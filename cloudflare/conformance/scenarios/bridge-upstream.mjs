/**
 * K21: the Go compatibility bridge's upstream protocol, frame for frame.
 *
 * Released Whagons mobile apps (Gonvex 0.1.x SDK, 5.1.31+) never talk to the
 * runtime. They talk to the Go bridge in whagons5-client
 * `compatibility/legacy-convex-v5.1.29-bridge/`, and the bridge holds one
 * upstream socket per phone and tenant. This scenario sends exactly what that
 * upstream client sends (`bridge.go` and `upstream_lifecycle.go` at
 * whagons5-client staging a6de676951; line numbers below are from there), on
 * the fixture app instead of Whagons paths, so each feature the bridge needs
 * is compared between Rust and the kernel on its own:
 *
 *  - dial with `?project=&tenant=` and read `session.ready` first (connect, bridge.go:5097-5131);
 *  - the public Control Plane auth with `clientContract: 3` and no capabilities
 *    (authenticatePublicControl, bridge.go:5153-5171);
 *  - `control.auth.publicSettings` as a one-shot `query.call` with `scope: "control"` (bridge.go:2244-2266);
 *  - the session grant fields the bridge reads (rememberSessionGrant, upstream_lifecycle.go:240-247;
 *    installSessionGrant, :406-418). Sign-in uses `control.auth.passwordLogin`; the bridge's own call,
 *    `exchangeExternalToken` with `provider: "firebase"` (:368-404), is covered by `control-external-auth`;
 *  - tenant auth on the same socket without capabilities (authenticateSession, bridge.go:5191-5213),
 *    so every collection is delivered as one legacy `replica.snapshot`, then `replica.delta`
 *    with `upserts` / `deleted` (replaceReplicaSnapshot / applyReplicaDelta, bridge.go:5573-5640);
 *  - `control.tenants.mine` as a `query.subscribe` with `scope: "control"` (subscribe, bridge.go:5215-5255);
 *  - `reducer.call` with an `idempotencyKey` and its exact replay (callOnce, bridge.go:5280-5320);
 *  - a Live Query through `query.subscribe` (every path outside `legacyReplicaCollectionPaths`);
 *  - `query.unsubscribe` and `replica.close` (bridge.go:5257-5270);
 *  - `control.auth.refreshSession` as an `action.call`, re-auth in place, and reopening the
 *    collection under its old id (refreshSessionGrant / resubscribeAll, upstream_lifecycle.go:338-450);
 *  - `error.register` and `error.envelope`, whose `error.ack` the bridge waits for (nativeError,
 *    bridge.go:5331-5346; techSupport.recordHeartbeat / recordClientError, bridge.go:3720-3756);
 *  - a WebSocket protocol ping, which the bridge sends every 25 s and whose pong keeps the socket
 *    alive (startKeepalive, upstream_lifecycle.go:193-220).
 *
 * The bridge's direct database write (`boardMessages`) has no runtime equivalent; it becomes a
 * service-principal call (`cloudflare/bridge-support/`, contract-change note of K21).
 */
import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';

/** bridge.go:35 `bridgeClientContractVersion`. */
const BRIDGE_CLIENT_CONTRACT = 3;

const sortedKeys = (value) => (value && typeof value === 'object' && !Array.isArray(value) ? Object.keys(value).sort() : typeof value);

/** Sends `frame` and waits for the reply with its id (bridge.go `request`). */
async function request(socket, frame, label) {
  socket.send(frame);
  return socket.next((m) => m.id === frame.id && m.type !== 'replica.watermark', { label });
}

const reply = (m) => (m.error !== undefined ? { type: m.type, error: m.error, ...(m.class ? { class: m.class } : {}) } : { type: m.type, keys: sortedKeys(m.result) });

export default {
  name: 'bridge-upstream',
  description: 'K21: the Go mobile bridge\'s upstream frames (auth, public control, grants, legacy snapshots, keyed writes, refresh, error.ack, ping)',
  tenants: { a: ['alice'] },
  async run(ctx) {
    let socket, grant;
    const replicaId = 'bridge-replica-board';

    await ctx.step('dial with the bridge query parameters', async () => {
      socket = await ctx.raw('bridge', { query: { project: ctx.project, tenant: ctx.tenants.a } });
      const ready = await socket.next({ type: 'session.ready' }, { label: 'session.ready' });
      ctx.observe('firstFrame', ctx.frames('bridge', { dir: 'in' })[0]?.type);
      ctx.expect(ready.type === 'session.ready', 'session.ready is the first frame');
    });

    await ctx.step('public Control Plane auth (controlOnly, clientContract 3)', async () => {
      const m = await request(socket, { type: 'auth', id: randomUUID(), project: ctx.project, controlOnly: true, clientContract: BRIDGE_CLIENT_CONTRACT }, 'public auth');
      ctx.observe('reply', reply(m));
    });

    await ctx.step('publicSettings as a one-shot control query.call', async () => {
      const m = await request(socket, { type: 'query.call', id: randomUUID(), path: 'control.auth.publicSettings', args: {}, scope: 'control' }, 'publicSettings');
      ctx.observe('reply', reply(m));
    });

    await ctx.step('sign in and read the grant fields the bridge uses', async () => {
      const id = randomUUID();
      const m = await request(socket, { type: 'action.call', id, path: 'control.auth.passwordLogin', scope: 'control', idempotencyKey: id, args: { email: ctx.members.alice.email, password: ctx.PASSWORD } }, 'passwordLogin');
      ctx.expect(m.type === 'action.result', `sign-in failed: ${m.error}`);
      grant = m.result;
      ctx.observe('bridgeFields', {
        accessToken: typeof grant.accessToken,
        refreshToken: typeof grant.refreshToken,
        expiry: typeof (grant.expiresAt ?? grant.expiresIn),
        accountId: typeof (grant.accountId ?? grant.account?.id),
      });
    });

    await ctx.step('tenant auth on the same socket, no capabilities', async () => {
      const m = await request(socket, { type: 'auth', id: randomUUID(), token: grant.accessToken, project: ctx.project, clientContract: BRIDGE_CLIENT_CONTRACT, tenant: ctx.tenants.a }, 'tenant auth');
      ctx.observe('reply', reply(m));
      ctx.expect(m.type === 'auth.result', `tenant auth failed: ${m.error}`);
    });

    await ctx.step('control.tenants.mine as a control query.subscribe', async () => {
      const id = randomUUID();
      const m = await request(socket, { type: 'query.subscribe', id, path: 'control.tenants.mine', args: {}, scope: 'control' }, 'tenants.mine');
      ctx.observe('reply', { type: m.type, ...(m.error ? { error: m.error } : { rows: Array.isArray(m.result) ? m.result.length : sortedKeys(m.result) }) });
      socket.send({ type: 'query.unsubscribe', id });
    });

    await ctx.step('replica.open delivers one legacy replica.snapshot', async () => {
      socket.send({ type: 'replica.open', id: replicaId, path: 'board.all', args: {} });
      const snapshot = await socket.next((m) => m.id === replicaId && ['replica.snapshot', 'replica.snapshotPage', 'replica.error'].includes(m.type), { label: 'replica.snapshot' });
      ctx.observe('frame', snapshot.type);
      ctx.expect(snapshot.type === 'replica.snapshot', `the bridge reads only replica.snapshot, got ${snapshot.type}: ${snapshot.error ?? ''}`);
      ctx.observe('bridgeFields', { key: snapshot.key, orderBy: snapshot.orderBy, orderDirection: snapshot.orderDirection, rows: Array.isArray(snapshot.result) ? snapshot.result.length : typeof snapshot.result });
      await socket.next({ type: 'replica.ready', id: replicaId }, { label: 'replica.ready' });
    });

    await ctx.step('a keyed reducer.call and its replica.delta', async () => {
      const m = await request(socket, { type: 'reducer.call', id: randomUUID(), path: 'board.add', args: { id: 'bridge-1', title: 'from the bridge', position: 1 }, idempotencyKey: 'bridge-intent-1' }, 'board.add');
      ctx.observe('reply', m.type === 'reducer.result' ? { type: m.type, result: m.result } : reply(m));
      const delta = await socket.next((x) => x.id === replicaId && x.type === 'replica.delta', { label: 'replica.delta' });
      ctx.observe('delta', { upserts: (delta.upserts ?? []).map((row) => row.id), deleted: delta.deleted ?? [] });
    });

    await ctx.step('the exact replay of that call (new call id, same key)', async () => {
      const m = await request(socket, { type: 'reducer.call', id: randomUUID(), path: 'board.add', args: { id: 'bridge-1', title: 'from the bridge', position: 1 }, idempotencyKey: 'bridge-intent-1' }, 'board.add replay');
      ctx.observe('reply', m.type === 'reducer.result' ? { type: m.type, result: m.result } : reply(m));
      await ctx.quiesce();
      ctx.observe('rows', (await ctx.backend.readTable(ctx.project, ctx.tenants.a, 'board')).filter((row) => row.id === 'bridge-1').length);
    });

    await ctx.step('a Live Query through query.subscribe follows a write', async () => {
      const id = randomUUID();
      const first = await request(socket, { type: 'query.subscribe', id, path: 'board.page', args: { offset: 0, limit: 3 } }, 'board.page');
      ctx.observe('first', first.type === 'query.result' ? first.result.map((row) => row.title) : reply(first));
      await request(socket, { type: 'reducer.call', id: randomUUID(), path: 'board.rename', args: { id: 'bridge-1', title: 'renamed' }, idempotencyKey: 'bridge-intent-2' }, 'board.rename');
      const next = await socket.next((x) => x.id === id && (x.type === 'query.result' || x.type === 'query.error'), { label: 'board.page update' });
      ctx.observe('next', next.type === 'query.result' ? next.result.map((row) => row.title) : reply(next));
      socket.send({ type: 'query.unsubscribe', id });
    });

    await ctx.step('refresh the session, re-auth in place, reopen under the same id', async () => {
      const id = randomUUID();
      const refreshed = await request(socket, { type: 'action.call', id, path: 'control.auth.refreshSession', scope: 'control', idempotencyKey: `${id}-1`, args: { refreshToken: grant.refreshToken } }, 'refreshSession');
      ctx.observe('refresh', refreshed.type === 'action.result' ? { type: refreshed.type, accessToken: typeof refreshed.result.accessToken, refreshToken: typeof refreshed.result.refreshToken } : reply(refreshed));
      ctx.expect(refreshed.type === 'action.result', `refresh failed: ${refreshed.error}`);
      const auth = await request(socket, { type: 'auth', id: randomUUID(), token: refreshed.result.accessToken, project: ctx.project, clientContract: BRIDGE_CLIENT_CONTRACT, tenant: ctx.tenants.a }, 're-auth');
      ctx.observe('reauth', reply(auth));
      socket.send({ type: 'replica.open', id: replicaId, path: 'board.all', args: {} });
      const snapshot = await socket.next((m) => m.id === replicaId && ['replica.snapshot', 'replica.snapshotPage', 'replica.error'].includes(m.type), { label: 'reopened snapshot' });
      ctx.observe('reopened', { type: snapshot.type, rows: (snapshot.result ?? []).map((row) => row.title) });
      await socket.next({ type: 'replica.ready', id: replicaId }, { label: 'replica.ready' });
      socket.send({ type: 'replica.close', id: replicaId });
    });

    await ctx.step('error.register and error.envelope are acknowledged', async () => {
      const register = await request(socket, { type: 'error.register', id: randomUUID(), release: '5.1.43', environment: 'legacy-mobile' }, 'error.ack (register)').catch((error) => ({ type: 'none', error: String(error.message).replace(/ for .*/, '') }));
      ctx.observe('register', { type: register.type, error: register.error ?? null });
      const event = { eventId: 'bridge-event-1', timestamp: '2026-11-11T00:00:00.000Z', level: 'error', message: 'conformance', name: 'LegacyClientError', stack: null, culprit: null, release: '5.1.43', url: null, userAgent: null, deviceId: null, sessionId: null, context: {}, breadcrumbs: [] };
      const envelope = await request(socket, { type: 'error.envelope', id: randomUUID(), events: [event] }, 'error.ack (envelope)').catch((error) => ({ type: 'none', error: String(error.message).replace(/ for .*/, '') }));
      ctx.observe('envelope', { type: envelope.type, error: envelope.error ?? null });
    });

    await ctx.step('a WebSocket protocol ping gets a pong', async () => {
      // Not recorded: the frame recorder sees text frames only. The bridge treats 2.5 ping windows
      // without a pong or frame as a dead upstream (upstream_lifecycle.go:198-204).
      const ws = new WebSocket(`${ctx.wsUrl}?project=${encodeURIComponent(ctx.project)}`);
      try {
        await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
        const pong = new Promise((resolve) => { ws.once('pong', () => resolve(true)); setTimeout(() => resolve(false), 5_000); });
        ws.ping();
        ctx.observe('pong', await pong);
      } finally { ws.terminate(); }
    });

    socket.close();
  },
};
