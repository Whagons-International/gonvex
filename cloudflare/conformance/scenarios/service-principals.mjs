/**
 * Service principals (CONTRACT A.1.4 "Service principal", A.2 notes, C.1 rows
 * 61-62, C.3.1, C.3.8 items 1 and 11, D.1 `/dev/manifest`; reference
 * rust/crates/runtime/src/service_principal.rs and docs/service-principals.md).
 *
 * The backend is restarted with three principals for the scenario's project
 * (`configureServicePrincipals`, capability `servicePrincipals`):
 *  - `gateway`: the API gateway's shape. Three fixture functions (one
 *    internal), delegation up to 600 s, manifest access.
 *  - `reader`: one function, no manifest access.
 *  - `elsewhere`: lists another project only.
 *
 * The steps follow the gateway's own use (whagons-api-gateway
 * `src/runtime/pool.rs`): a service socket per tenant, the internal key check
 * without an idempotency key, `control.servicePrincipals.delegate` with an
 * actor, the grant redeemed on a member socket with `clientContract`, calls
 * there, revocation, and `/dev/manifest` with the service credential.
 */
import { createHash, randomUUID } from 'node:crypto';
import { RAW_CAPABILITIES } from './helpers.mjs';

const credential = (name) => `gvx_svc_${createHash('sha256').update(`k17-conformance-${name}`).digest('hex')}`;
const TOKENS = { gateway: credential('gateway'), reader: credential('reader'), elsewhere: credential('elsewhere'), unknown: credential('unknown') };
const digest = (token) => createHash('sha256').update(token).digest('hex');

const DELEGATE = 'control.servicePrincipals.delegate';
const REVOKE = 'control.servicePrincipals.revokeDelegation';

// Client-minted ids are UUIDs, which the normalizer aliases per session (both backends run at once).
const nextId = () => randomUUID();

/** One frame and its reply, matched by id (`reply` picks the reply types). */
async function exchange(socket, frame, reply = (m) => m.id === frame.id) {
  socket.send(frame);
  return socket.next(reply, { label: `${frame.type} ${frame.path ?? ''} reply` });
}

/** The reply frame without its id (ids are aliased per backend, the rest is compared). */
const strip = ({ id: _id, ...rest }) => rest;

async function serviceAuth(socket, { token, project, tenant, id = nextId('svc-auth') }) {
  const frame = { type: 'auth', id, token, ...(project !== undefined ? { project } : {}), ...(tenant !== undefined ? { tenant } : {}) };
  return strip(await exchange(socket, frame, (m) => (m.type === 'auth.result' || m.type === 'auth.error') && m.id === id));
}

const reducer = (socket, path, args, extra = {}) => exchange(socket, { type: 'reducer.call', id: nextId('r'), path, args, ...extra }).then(strip);
const control = (socket, path, args) => reducer(socket, path, args, { scope: 'control' });

export default {
  name: 'service-principals',
  description: 'gvx_svc_ admission and tenant binding, the allowlist and narrow frame set, delegation with provenance, revocation, /dev/manifest',
  tenants: { a: ['alice', 'bob'], b: ['carol'] },
  requires: ['servicePrincipals'],
  async run(ctx) {
    const alice = ctx.members.alice, bob = ctx.members.bob, carol = ctx.members.carol;
    await ctx.step('the backend is configured with three principals', async () => {
      await ctx.backend.configureServicePrincipals([
        { id: 'gateway', tokenSha256: digest(TOKENS.gateway), projects: [ctx.project], functions: ['principals.verifyKey', 'principals.whoami', 'principals.record'], maxDelegationSeconds: 600, manifest: true },
        { id: 'reader', tokenSha256: digest(TOKENS.reader), projects: [ctx.project], functions: ['principals.whoami'] },
        { id: 'elsewhere', tokenSha256: digest(TOKENS.elsewhere), projects: ['k17-some-other-project'], functions: ['principals.whoami'], manifest: true },
      ]);
    });

    let service;
    await ctx.step('a credential is admitted only for its project and an existing tenant', async () => {
      service = await ctx.raw('service');
      ctx.observe('unknown credential', await serviceAuth(service, { token: TOKENS.unknown, project: ctx.project, tenant: ctx.tenants.a }));
      ctx.observe('project outside the allowlist', await serviceAuth(service, { token: TOKENS.elsewhere, project: ctx.project, tenant: ctx.tenants.a }));
      ctx.observe('no tenant', await serviceAuth(service, { token: TOKENS.gateway, project: ctx.project }));
      ctx.observe('no project', await serviceAuth(service, { token: TOKENS.gateway, tenant: ctx.tenants.a }));
      ctx.observe('blank tenant', await serviceAuth(service, { token: TOKENS.gateway, project: ctx.project, tenant: '  ' }));
      ctx.observe('unknown tenant', await serviceAuth(service, { token: TOKENS.gateway, project: ctx.project, tenant: 'service-principals-nowhere' }));
      ctx.observe('bound to tenant a', await serviceAuth(service, { token: ` ${TOKENS.gateway} `, project: ctx.project, tenant: ctx.tenants.a }));
    });

    await ctx.step('allowlisted calls run as the principal on the api channel', async () => {
      ctx.observe('internal key check', await reducer(service, 'principals.verifyKey', { keyHash: 'k17-valid-key-digest' }));
      ctx.observe('wrong key', await reducer(service, 'principals.verifyKey', { keyHash: 'nope' }));
      ctx.observe('arguments checked', await reducer(service, 'principals.verifyKey', { keyHash: 7 }));
      ctx.observe('write', await reducer(service, 'principals.record', { id: 'svc-row', title: 'by the service' }, { idempotencyKey: 'k17-service-key-1' }));
      ctx.observe('replayed write', await reducer(service, 'principals.record', { id: 'svc-row', title: 'by the service' }, { idempotencyKey: 'k17-service-key-1' }));
    });

    await ctx.step('everything outside the allowlist and the narrow frame set is refused', async () => {
      ctx.observe('reducer outside the allowlist', await reducer(service, 'board.add', { id: 'x', title: 'x', position: 1 }));
      ctx.observe('unknown function', await reducer(service, 'principals.missing', {}));
      ctx.observe('query outside the allowlist', strip(await exchange(service, { type: 'query.call', id: nextId('q'), path: 'board.page', args: { offset: 0, limit: 3 } })));
      ctx.observe('action outside the allowlist', strip(await exchange(service, { type: 'action.call', id: nextId('a'), path: 'scheduler.notify', args: {} })));
      ctx.observe('control query', strip(await exchange(service, { type: 'query.call', id: nextId('q'), path: 'control.tenants.mine', args: {}, scope: 'control' })));
      ctx.observe('control action', strip(await exchange(service, { type: 'action.call', id: nextId('a'), path: 'control.auth.refreshSession', args: {}, scope: 'control' })));
      ctx.observe('other control reducer', await control(service, 'control.tenants.create', { name: 'x' }));
      ctx.observe('subscribe', strip(await exchange(service, { type: 'query.subscribe', id: nextId('s'), path: 'board.page', args: { offset: 0, limit: 3 } })));
      ctx.observe('replica', strip(await exchange(service, { type: 'replica.open', id: nextId('o'), path: 'board.all', args: {} })));
      for (const frame of [
        { type: 'reducer.callMany', calls: [{ id: nextId('r'), path: 'principals.whoami', args: {} }] },
        { type: 'query.subscribeMany', subscribes: [{ id: nextId('s'), path: 'board.page', args: { offset: 0, limit: 3 } }] },
        { type: 'replica.openMany', opens: [{ id: nextId('o'), path: 'board.all', args: {} }] },
      ]) ctx.observe(`batch ${frame.type}`, strip(await exchange(service, frame, (m) => m.type === 'reducer.error' && m.id === '')));
      // Ignored frames get no reply: the next call's reply is the next frame.
      service.send({ type: 'presence.set', state: 'active' });
      service.send({ type: 'query.unsubscribe', id: 'nothing' });
      service.send({ type: 'error.heartbeat', id: nextId('e') });
      service.send({ type: 'replica.close', id: 'nothing' });
      ctx.observe('after ignored frames', await reducer(service, 'principals.whoami', {}));
      const reader = await ctx.raw('reader');
      ctx.observe('reader bound', await serviceAuth(reader, { token: TOKENS.reader, project: ctx.project, tenant: ctx.tenants.a }));
      ctx.observe('reader: another principal\'s function', await reducer(reader, 'principals.verifyKey', { keyHash: 'k17-valid-key-digest' }));
      ctx.observe('reader: its own function', await reducer(reader, 'principals.whoami', {}));
    });

    let grant, plain;
    await ctx.step('delegation is checked and bound to the socket\'s tenant', async () => {
      const valid = { accountId: alice.accountId, memberId: alice.memberIds.a, reason: 'api-key:k1' };
      ctx.observe('missing account', await control(service, DELEGATE, { memberId: alice.memberIds.a, reason: 'r' }));
      ctx.observe('blank reason', await control(service, DELEGATE, { ...valid, reason: '  ' }));
      ctx.observe('reason over 200 bytes', await control(service, DELEGATE, { ...valid, reason: 'é'.repeat(101) }));
      ctx.observe('too short', await control(service, DELEGATE, { ...valid, expiresInSeconds: 10 }));
      ctx.observe('over the maximum', await control(service, DELEGATE, { ...valid, expiresInSeconds: 601 }));
      ctx.observe('not an integer', await control(service, DELEGATE, { ...valid, expiresInSeconds: 60.5 }));
      ctx.observe('unknown field', await control(service, DELEGATE, { ...valid, scope: 'all' }));
      ctx.observe('not an object', await control(service, DELEGATE, ['x']));
      ctx.observe('bad actor kind', await control(service, DELEGATE, { ...valid, actor: { kind: 'API Key', name: 'n' } }));
      ctx.observe('unknown actor field', await control(service, DELEGATE, { ...valid, actor: { kind: 'agent', name: 'n', id: 1 } }));
      ctx.observe('actor not an object', await control(service, DELEGATE, { ...valid, actor: 'agent' }));
      ctx.observe('member of another tenant', await control(service, DELEGATE, { accountId: carol.accountId, memberId: carol.memberIds.b, reason: 'r' }));
      ctx.observe('member id of another account', await control(service, DELEGATE, { ...valid, memberId: bob.memberIds.a }));
      const minted = await control(service, DELEGATE, { ...valid, expiresInSeconds: 300, actor: { kind: 'agent', name: '  CI deploy key  ', reference: 'apiKey:k1' } });
      ctx.expect(minted.type === 'reducer.result', `delegate: ${JSON.stringify(minted)}`);
      grant = minted.result;
      ctx.alias(grant.id, 'grant-1');
      ctx.expect(/^svcdel_[0-9a-f]{32}$/.test(grant.id) && grant.token.startsWith('gvx_imp_'), `grant shape: ${JSON.stringify(grant)}`);
      ctx.expect(grant.expiresInSeconds === 300, `expiresInSeconds: ${grant.expiresInSeconds}`);
      ctx.observe('delegated', minted);
      const second = await control(service, DELEGATE, { accountId: bob.accountId, memberId: bob.memberIds.a, reason: 'api-key:k2', actor: { kind: 'agent', name: 'n', reference: '   ' } });
      plain = second.result;
      ctx.alias(plain.id, 'grant-2');
      ctx.expect(plain.expiresInSeconds === 600, `default expiresInSeconds is the principal's maximum: ${plain.expiresInSeconds}`);
      ctx.observe('delegated with the maximum and an empty reference', second);
    });

    let member;
    await ctx.step('the grant opens the member\'s own session, driven on the api channel', async () => {
      member = await ctx.raw('delegated');
      member.send({ type: 'auth', id: 'member', token: grant.token, project: ctx.project, tenant: ctx.tenants.a, clientContract: 1, capabilities: RAW_CAPABILITIES });
      const auth = await member.next((m) => m.id === 'member', { label: 'delegated auth' });
      ctx.expect(auth.type === 'auth.result', `delegated auth: ${JSON.stringify(auth)}`);
      ctx.observe('auth', strip(auth));
      ctx.observe('whoami', await reducer(member, 'principals.whoami', {}));
      ctx.observe('a function the service may not call', await reducer(member, 'board.add', { id: 'member-row', title: 'by alice through the gateway', position: 2 }));
      ctx.observe('the internal key check', await reducer(member, 'principals.verifyKey', { keyHash: 'k17-valid-key-digest' }));
      const again = await ctx.raw('replay');
      ctx.observe('single use', await serviceAuth(again, { token: grant.token, project: ctx.project, tenant: ctx.tenants.a }));
      // A redemption in another tenant is refused and does not consume the grant.
      ctx.observe('wrong tenant', await serviceAuth(await ctx.raw('wrong-tenant'), { token: plain.token, project: ctx.project, tenant: ctx.tenants.b }));
      const bobs = await ctx.raw('delegated-plain');
      bobs.send({ type: 'auth', id: 'plain', token: plain.token, project: ctx.project, tenant: ctx.tenants.a, clientContract: 1 });
      ctx.observe('second grant auth', strip(await bobs.next((m) => m.id === 'plain', { label: 'second grant auth' })));
      ctx.observe('second grant whoami', await reducer(bobs, 'principals.whoami', {}));
      bobs.close();
    });

    let reconnected;
    await ctx.step('the rotating reconnect credential resumes the delegated session', async () => {
      const { developerSessionToken } = ctx.frames('delegated', { dir: 'in', type: 'auth.result' }).at(-1)?.result ?? {};
      ctx.expect(typeof developerSessionToken === 'string' && developerSessionToken.startsWith('gvx_dev_'), 'a gvx_dev_ reconnect credential');
      member.close();
      reconnected = await ctx.raw('reconnected');
      reconnected.send({ type: 'auth', id: 'reconnect', token: developerSessionToken, project: ctx.project, tenant: ctx.tenants.a, clientContract: 1 });
      ctx.observe('reconnect', strip(await reconnected.next((m) => m.id === 'reconnect', { label: 'reconnect auth' })));
      ctx.observe('whoami', await reducer(reconnected, 'principals.whoami', {}));
    });

    await ctx.step('a principal revokes only its own grants, and the member session ends', async () => {
      const reader = await ctx.raw('reader-revoke');
      await serviceAuth(reader, { token: TOKENS.reader, project: ctx.project, tenant: ctx.tenants.a });
      ctx.observe('another principal\'s grant', await control(reader, REVOKE, { id: grant.id }));
      ctx.observe('extra field', await control(service, REVOKE, { id: grant.id, reason: 'x' }));
      ctx.observe('unknown grant', await control(service, REVOKE, { id: 'svcdel_00000000000000000000000000000000' }));
      ctx.observe('revoked', await control(service, REVOKE, { id: grant.id }));
      ctx.observe('revoked again', await control(service, REVOKE, { id: grant.id }));
      // The 30 s revalidation round ends the delegated session (C.3.8 item 10).
      const expired = await reconnected.next((m) => m.type === 'auth.error' && m.id === 'session-expired', { timeoutMs: 40_000, label: 'session-expired' });
      ctx.observe('session expired', expired);
      ctx.observe('after expiry', await reducer(reconnected, 'principals.whoami', {}));
    });

    await ctx.step('re-auth moves a service socket to another tenant, or ends the grant', async () => {
      ctx.observe('tenant b', await serviceAuth(service, { token: TOKENS.gateway, project: ctx.project, tenant: ctx.tenants.b }));
      ctx.observe('tenant b write', await reducer(service, 'principals.record', { id: 'svc-row-b', title: 'by the service in b' }));
      const session = (await ctx.rawLogin(await ctx.raw('login'), 'carol')).accessToken;
      service.send({ type: 'auth', id: 'as-carol', token: session, project: ctx.project, tenant: ctx.tenants.b, clientContract: 1 });
      const auth = await service.next((m) => m.id === 'as-carol', { label: 'member auth' });
      ctx.observe('member auth', auth.type);
      ctx.observe('internal function as a member', await reducer(service, 'principals.verifyKey', { keyHash: 'k17-valid-key-digest' }));
      ctx.observe('whoami as a member', await reducer(service, 'principals.whoami', {}));
    });

    await ctx.step('a frame that does not decode closes a service socket 1003', async () => {
      const socket = await ctx.raw('service-close');
      await serviceAuth(socket, { token: TOKENS.gateway, project: ctx.project, tenant: ctx.tenants.a });
      socket.sendRaw('{"type":"reducer.call","id":"r","path":7,"args":{}}');
      const closed = await socket.whenClosed(10_000);
      ctx.observe('close', closed ? { code: closed.code, reason: closed.reason } : null);
    });

    await ctx.step('/dev/manifest answers a principal only with manifest:true for a listed project', async () => {
      const get = async (query, headers) => {
        const r = await fetch(`${ctx.backend.httpUrl}/dev/manifest${query}`, { headers });
        const text = await r.text();
        let body; try { body = JSON.parse(text); } catch { body = { raw: text }; }
        return { status: r.status, body };
      };
      const summary = ({ status, body }) => status === 200
        ? { status, keys: Object.keys(body).sort(), project: body.project === ctx.project, functions: Object.keys(body.functions ?? {}).sort(), module: Object.keys(body.module ?? {}).sort(), generation: typeof body.module?.generation }
        : { status, body };
      const q = `?project=${encodeURIComponent(ctx.project)}`;
      const bearer = (token) => ({ authorization: `Bearer ${token}` });
      const allowed = await get(q, bearer(TOKENS.gateway));
      ctx.observe('gateway', summary(allowed));
      const header = await get('', { ...bearer(TOKENS.gateway), 'x-gonvex-project-id': ctx.project });
      ctx.expect(JSON.stringify(header.body) === JSON.stringify(allowed.body), 'the header form returns the same body');
      ctx.observe('padded bearer', summary(await get(q, { authorization: `Bearer  ${TOKENS.gateway} ` })));
      ctx.observe('without manifest access', summary(await get(q, bearer(TOKENS.reader))));
      ctx.observe('project not listed', summary(await get(q, bearer(TOKENS.elsewhere))));
      ctx.observe('unknown service credential', summary(await get(q, bearer(TOKENS.unknown))));
      ctx.observe('no project', summary(await get('', bearer(TOKENS.gateway))));
    });
  },
};
