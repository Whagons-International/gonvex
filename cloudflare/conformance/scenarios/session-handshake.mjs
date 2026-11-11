/**
 * Socket handshake and authentication (CONTRACT.md A.1.2-A.1.5, C.3.8, C.4;
 * package K05). Frame by frame on raw sockets, then the real SDK:
 *
 *  - `session.ready` is the first frame, unsolicited, capabilities only;
 *  - every `auth` branch the shipped clients reach: control-only with and
 *    without a token, `tenantControl`, no token, a full tenant session, the
 *    default tenant, and the errors with their `code`;
 *  - the replica directive's `scope` and `visibilityScope` recomputed here
 *    from the result, so both backends must hash the same inputs;
 *  - re-auth in place: `subscriptionsPreserved` only with the capability and
 *    an identical scope, a tenant switch, and a failed re-auth tearing the
 *    session down;
 *  - `clientContract`: a mismatch on tenant auth sends
 *    `client.updateRequired` and closes; control-only auth is not gated;
 *  - the real SDK connects, logs in and authenticates.
 */
import { createHash } from 'node:crypto';

const sha256 = (text) => createHash('sha256').update(text).digest('hex');
/** modules.rs `replica_directive`: serde_json bytes of these objects, keys in this order. */
function expectedDirective({ project, tenant, account, permissions, epoch }) {
  const permissionsHash = sha256(JSON.stringify(permissions));
  const accountId = account.trim() === '' ? 'anonymous' : account;
  return {
    scope: sha256(JSON.stringify({ protocolVersion: 1, projectId: project, tenantId: tenant, accountId, permissionsHash, epoch })),
    visibilityScope: sha256(JSON.stringify({ protocolVersion: 1, kind: 'replica-visibility', projectId: project, tenantId: tenant, accountId, permissionsHash })),
  };
}

/** The keys of a result, in the order the backend wrote them (A.1.4 lists them in serde order). */
const keysOf = (value) => value && typeof value === 'object' ? Object.keys(value) : value;
const errorShape = (m) => ({ type: m.type, id: m.id, code: m.code ?? null, error: m.error });

export default {
  name: 'session-handshake',
  description: 'Handshake and auth: session.ready, every auth branch and error code, directive hashes, re-auth in place, preservation, tenant switch, clientContract, the real SDK',
  tenants: { a: ['alice', 'bob'], b: ['alice'] },
  async run(ctx) {
    const project = ctx.project;
    const auth = async (socket, frame) => {
      socket.send({ type: 'auth', clientContract: 1, ...frame });
      return socket.next(m => ((m.type === 'auth.result' || m.type === 'auth.error') && m.id === frame.id) || m.type === 'client.updateRequired', { label: `${frame.id} reply` });
    };
    let raw, alice, bob;

    await ctx.step('session.ready is the first frame', async () => {
      raw = await ctx.raw('raw');
      const ready = await raw.next({ type: 'session.ready' });
      ctx.expect(ctx.frames('raw', { dir: 'in' })[0]?.type === 'session.ready', 'session.ready must be the first frame');
      ctx.observe('keys', keysOf(ready));
      ctx.observe('capabilityKeys', Object.keys(ready.capabilities).filter(k => !['replicaNativeRows', 'replicaSetDigest', 'replicaFieldPatches'].includes(k)));
      ctx.observe('protocolVersion', ready.capabilities.protocolVersion);
      ctx.expect(typeof ready.capabilities.runtimeVersion === 'string' && ready.capabilities.runtimeVersion.length > 0, 'runtimeVersion is a non-empty string');
    });

    await ctx.step('control-only auth without a token', async () => {
      ctx.observe('noProject', errorShape(await auth(raw, { id: 'c-no-project', controlOnly: true })));
      const ok = await auth(raw, { id: 'c-project', controlOnly: true, project });
      ctx.observe('withProject', { type: ok.type, keys: keysOf(ok.result), accountId: ok.result?.accountId, tenantId: ok.result?.tenantId });
      ctx.observe('unknownProject', errorShape(await auth(raw, { id: 'c-unknown-project', controlOnly: true, project: 'no-such-project' })));
      ctx.observe('blankProject', errorShape(await auth(raw, { id: 'c-blank-project', controlOnly: true, project: '   ' })));
    });

    await ctx.step('auth without a token', async () => {
      ctx.observe('tenantNoToken', errorShape(await auth(raw, { id: 'n-tenant', project, tenant: ctx.tenants.a })));
      ctx.observe('blankToken', errorShape(await auth(raw, { id: 'n-blank', project, tenant: ctx.tenants.a, token: '  ' })));
      ctx.observe('nothing', errorShape(await auth(raw, { id: 'n-nothing' })));
    });

    let session, bobSession;
    await ctx.step('password login on the socket', async () => {
      session = await ctx.rawLogin(raw, 'alice');
      ctx.expect(typeof session.accessToken === 'string', 'login returns an access token');
    });

    await ctx.step('control-only auth with a token', async () => {
      const plain = await auth(raw, { id: 'c-token', controlOnly: true, project, token: session.accessToken });
      ctx.observe('identity', { type: plain.type, keys: keysOf(plain.result), tenantId: plain.result?.tenantId });
      const dropped = await auth(raw, { id: 'c-token-tenant', controlOnly: true, project, token: session.accessToken, tenant: ctx.tenants.a });
      ctx.observe('tenantWithoutCapability', { type: dropped.type, keys: keysOf(dropped.result), tenantId: dropped.result?.tenantId });
      const kept = await auth(raw, { id: 'c-tenant-control', controlOnly: true, project, token: session.accessToken, tenant: ctx.tenants.a, capabilities: { tenantControl: 1 } });
      ctx.observe('tenantControl', { type: kept.type, keys: keysOf(kept.result), tenantIsA: kept.result?.tenantId === ctx.tenants.a });
      const blankTenant = await auth(raw, { id: 'c-tenant-control-blank', controlOnly: true, project, token: session.accessToken, tenant: ' ', capabilities: { tenantControl: 1 } });
      ctx.observe('tenantControlBlankTenant', { type: blankTenant.type, keys: keysOf(blankTenant.result), tenantId: blankTenant.result?.tenantId });
    });

    await ctx.step('tenant auth', async () => {
      const m = await auth(raw, { id: 't-a', project, token: session.accessToken, tenant: ctx.tenants.a });
      ctx.observe('type', m.type);
      ctx.observe('keys', keysOf(m.result));
      ctx.observe('localIdentity', m.result?.localIdentity);
      ctx.observe('replicaKeys', keysOf(m.result?.replica));
      ctx.observe('replicaProtocolVersion', m.result?.replica?.protocolVersion);
      const want = expectedDirective({ project, tenant: ctx.tenants.a, account: m.result.accountId, permissions: m.result.localIdentity.member.permissions, epoch: m.result.replica.epoch });
      ctx.expect(m.result.replica.visibilityScope === want.visibilityScope, `visibilityScope ${m.result.replica.visibilityScope} != ${want.visibilityScope}`);
      ctx.expect(m.result.replica.scope === want.scope, `scope ${m.result.replica.scope} != ${want.scope}`);
      ctx.expect(/^[0-9a-f]{64}$/.test(m.result.replica.epoch), 'epoch is a sha256 hex');
    });

    await ctx.step('tenant auth without a tenant picks the first admitted tenant', async () => {
      const m = await auth(raw, { id: 't-default', project, token: session.accessToken });
      ctx.observe('type', m.type);
      ctx.observe('tenantIsA', m.result?.tenantId === ctx.tenants.a);
      ctx.observe('subscriptionsPreserved', m.result?.subscriptionsPreserved ?? null);
    });

    await ctx.step('auth errors and their codes', async () => {
      ctx.observe('garbageSession', errorShape(await auth(raw, { id: 'e-garbage', project, tenant: ctx.tenants.a, token: 'gvx_session_not_a_real_token' })));
      ctx.observe('notAToken', errorShape(await auth(raw, { id: 'e-not-token', project, tenant: ctx.tenants.a, token: 'hello' })));
      ctx.observe('otherProject', errorShape(await auth(raw, { id: 'e-other-project', project: 'no-such-project', tenant: ctx.tenants.a, token: session.accessToken })));
      ctx.observe('unknownTenant', errorShape(await auth(raw, { id: 'e-unknown-tenant', project, tenant: 'tenant-zz', token: session.accessToken })));
      ctx.observe('garbageControlOnly', errorShape(await auth(raw, { id: 'e-garbage-control', project, controlOnly: true, token: 'gvx_session_not_a_real_token' })));
      ctx.observe('badDeveloperGrant', errorShape(await auth(raw, { id: 'e-dev', project, tenant: ctx.tenants.a, token: 'gvx_dev_not_a_grant' })));
      ctx.observe('badDeveloperGrantControlOnly', errorShape(await auth(raw, { id: 'e-dev-control', project, controlOnly: true, token: 'gvx_imp_not_a_grant' })));
      const bobRaw = await ctx.raw('bob');
      await bobRaw.next({ type: 'session.ready' });
      bobSession = await ctx.rawLogin(bobRaw, 'bob');
      ctx.observe('notAMember', errorShape(await auth(bobRaw, { id: 'e-not-member', project, tenant: ctx.tenants.b, token: bobSession.accessToken })));
      bobRaw.close();
    });

    await ctx.step('a failed re-auth tears the session down', async () => {
      ctx.observe('reauth', (await auth(raw, { id: 'r-good', project, token: session.accessToken, tenant: ctx.tenants.a })).type);
      ctx.observe('failed', errorShape(await auth(raw, { id: 'r-bad', project, tenant: ctx.tenants.a, token: 'gvx_session_not_a_real_token' })));
      raw.send({ type: 'query.call', id: 'q-after-failed-auth', path: 'notes:list', args: {} });
      const q = await raw.next({ id: 'q-after-failed-auth' });
      ctx.observe('queryAfter', { type: q.type, error: q.error });
      raw.send({ type: 'reducer.call', id: 'r-after-failed-auth', path: 'notes:add', args: {} });
      const r = await raw.next({ id: 'r-after-failed-auth' });
      ctx.observe('reducerAfter', { type: r.type, error: r.error, class: r.class, retryable: r.retryable });
      raw.send({ type: 'action.call', id: 'a-after-failed-auth', path: 'notes:act', args: {} });
      const a = await raw.next({ id: 'a-after-failed-auth' });
      ctx.observe('actionAfter', { type: a.type, error: a.error });
      raw.send({ type: 'query.subscribe', id: 's-after-failed-auth', path: 'notes:list', args: {} });
      const s = await raw.next({ id: 's-after-failed-auth' });
      ctx.observe('subscribeAfter', { type: s.type, error: s.error });
      raw.send({ type: 'replica.open', id: 'o-after-failed-auth', path: 'notes:all', args: {} });
      const o = await raw.next({ id: 'o-after-failed-auth' });
      ctx.observe('openAfter', { type: o.type, error: o.error });
      raw.send({ type: 'presence.subscribe', id: 'p-after-failed-auth' });
      const p = await raw.next({ id: 'p-after-failed-auth' });
      ctx.observe('presenceAfter', { type: p.type, error: p.error });
    });

    await ctx.step('re-auth in place: preservation and tenant switch', async () => {
      const capabilities = { preserveSubscriptions: 1 };
      ctx.observe('first', (await auth(raw, { id: 'p-1', project, token: session.accessToken, tenant: ctx.tenants.a, capabilities })).result?.subscriptionsPreserved ?? null);
      ctx.observe('same', (await auth(raw, { id: 'p-2', project, token: session.accessToken, tenant: ctx.tenants.a, capabilities })).result?.subscriptionsPreserved ?? null);
      ctx.observe('sameWithoutCapability', (await auth(raw, { id: 'p-3', project, token: session.accessToken, tenant: ctx.tenants.a })).result?.subscriptionsPreserved ?? null);
      ctx.observe('capabilityValue2', (await auth(raw, { id: 'p-4', project, token: session.accessToken, tenant: ctx.tenants.a, capabilities: { preserveSubscriptions: 2 } })).result?.subscriptionsPreserved ?? null);
      ctx.observe('againWithCapability', (await auth(raw, { id: 'p-5', project, token: session.accessToken, tenant: ctx.tenants.a, capabilities })).result?.subscriptionsPreserved ?? null);
      const b = await auth(raw, { id: 'p-6', project, token: session.accessToken, tenant: ctx.tenants.b, capabilities });
      ctx.observe('switchToB', { type: b.type, tenantIsB: b.result?.tenantId === ctx.tenants.b, preserved: b.result?.subscriptionsPreserved ?? null });
      const a = await auth(raw, { id: 'p-7', project, token: session.accessToken, tenant: ctx.tenants.a, capabilities });
      ctx.observe('switchBackToA', { type: a.type, tenantIsA: a.result?.tenantId === ctx.tenants.a, preserved: a.result?.subscriptionsPreserved ?? null });
      const control = await auth(raw, { id: 'p-8', project, token: session.accessToken, tenant: ctx.tenants.a, controlOnly: true, capabilities: { ...capabilities, tenantControl: 1 } });
      ctx.observe('controlOnly', control.result?.subscriptionsPreserved ?? null);
      const back = await auth(raw, { id: 'p-9', project, token: session.accessToken, tenant: ctx.tenants.a, capabilities });
      ctx.observe('afterControlOnly', back.result?.subscriptionsPreserved ?? null);
    });

    await ctx.step('client contract', async () => {
      const ok = await auth(raw, { id: 'k-1', project, token: session.accessToken, tenant: ctx.tenants.a, clientContract: 1 });
      ctx.observe('matching', ok.type);
      const control = await auth(raw, { id: 'k-control', project, token: session.accessToken, controlOnly: true, clientContract: 7 });
      ctx.observe('controlOnlyMismatch', control.type);
      const noContract = await ctx.raw('no-contract');
      await noContract.next({ type: 'session.ready' });
      noContract.send({ type: 'auth', id: 'k-none', project, token: session.accessToken, tenant: ctx.tenants.a });
      const none = await noContract.next(m => m.id === 'k-none' || m.type === 'client.updateRequired', { label: 'k-none reply' });
      ctx.observe('withoutContract', none.type === 'client.updateRequired' ? { type: none.type, reason: none.reason, contract: none.contract } : none.type);
      const stale = await ctx.raw('stale');
      await stale.next({ type: 'session.ready' });
      stale.send({ type: 'auth', id: 'k-stale', project, token: session.accessToken, tenant: ctx.tenants.a, clientContract: 7 });
      const m = await stale.next(x => x.id === 'k-stale' || x.type === 'client.updateRequired', { label: 'k-stale reply' });
      ctx.observe('mismatch', m.type === 'client.updateRequired' ? { type: m.type, reason: m.reason, contract: m.contract } : m.type);
      // No close frame in Rust (the task ends); the kernel closes after the frame (A.1.9 "none").
      ctx.observe('mismatchClosed', m.type === 'client.updateRequired' ? !!(await stale.whenClosed(5_000)) : null);
    });

    await ctx.step('the real SDK connects, logs in and authenticates', async () => {
      alice = await ctx.sdk('sdk-alice', { member: 'alice' });
      bob = await ctx.sdk('sdk-bob', { member: 'bob' });
      for (const [name, client] of [['alice', alice], ['bob', bob]]) {
        const state = client.connectionState();
        ctx.observe(name, { connected: state.isWebSocketConnected ?? null, connections: state.connectionCount ?? null });
      }
    });

    await ctx.step('the SDK switches tenant in place', async () => {
      await alice.authenticate({ project, token: alice.session.accessToken, tenant: ctx.tenants.b });
      ctx.observe('connections', alice.connectionState().connectionCount ?? null);
      await alice.authenticate({ project, token: alice.session.accessToken, tenant: ctx.tenants.a });
      ctx.observe('connectionsAfterSwitchBack', alice.connectionState().connectionCount ?? null);
    });

    await ctx.step('a client contract deployed while sockets are connected', async () => {
      const tenantSocket = await ctx.raw('reload-tenant');
      await tenantSocket.next({ type: 'session.ready' });
      ctx.observe('tenantAuth', (await auth(tenantSocket, { id: 'd-tenant', project, token: session.accessToken, tenant: ctx.tenants.a })).type);
      const controlSocket = await ctx.raw('reload-control');
      await controlSocket.next({ type: 'session.ready' });
      ctx.observe('controlAuth', (await auth(controlSocket, { id: 'd-control', project, token: session.accessToken, controlOnly: true })).type);
      // Generation 3: generation 1 plus client-contract.json at version 2 (fixtures/app-v3).
      ctx.observe('deployed', (await ctx.deploy(3)).ok);
      const update = await tenantSocket.next({ type: 'client.updateRequired' });
      ctx.observe('tenantSocket', { frame: update, closed: !!(await tenantSocket.whenClosed(5_000)) });
      const reload = await controlSocket.next({ type: 'system.reload' });
      ctx.observe('controlSocket', { type: reload.type, reason: reload.reason, newArtifact: reload.artifactHash === ctx.fixtures[3].hash, open: !controlSocket.closed });
      await ctx.until(() => alice.connectionState().isWebSocketConnected === false || alice.connectionState().updateRequired === true, 'the SDK sees the update', 5_000).catch(() => {});

      const stale = await ctx.raw('contract-stale');
      await stale.next({ type: 'session.ready' });
      const m = await auth(stale, { id: 'd-stale', project, token: session.accessToken, tenant: ctx.tenants.a, clientContract: 1 });
      ctx.observe('staleAuth', { frame: m, closed: !!(await stale.whenClosed(5_000)) });
      const current = await ctx.raw('contract-current');
      await current.next({ type: 'session.ready' });
      ctx.observe('currentAuth', (await auth(current, { id: 'd-current', project, token: session.accessToken, tenant: ctx.tenants.a, clientContract: 2 })).type);
      ctx.observe('staleControlOnly', (await auth(current, { id: 'd-control-stale', project, token: session.accessToken, controlOnly: true, clientContract: 1 })).type);
      ctx.observe('staleControlOnlyThenTenantFrame', await (async () => {
        current.send({ type: 'query.call', id: 'd-q', path: 'notes:list', args: {} });
        const q = await current.next(x => x.id === 'd-q' || x.type === 'client.updateRequired');
        return q.type;
      })());
      current.close(); controlSocket.close();
    });
    raw.close();
  },
};
