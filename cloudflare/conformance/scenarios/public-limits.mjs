/**
 * Public functions (K16): the anonymous transport's budgets and admission,
 * at Rust's default limits, through raw sockets:
 *
 *  - public auth by id and by re-auth to the same scope, an unknown tenant;
 *  - 30 calls per connection per 60 s (auth frames count), then each frame
 *    kind's correlated `public call rate limit exceeded` error, batches
 *    costing their length;
 *  - 120 calls per address across connections;
 *  - 20 public sockets per address (HTTP 429 at the 21st), a slot freed by a close;
 *  - 100 pending sockets per address across both transports (429 at the
 *    101st), a pending slot freed by a successful auth.
 *
 * `public-calls` (same helpers) covers anonymous calls on backends that run Reducers.
 *
 * Every step uses its own client address through `CF-Connecting-IP`, so no
 * budget carries over from another step or scenario. The Rust backend trusts
 * that header from 127.0.0.1 (`GONVEX_TRUSTED_PROXY_CIDRS`); on Cloudflare the
 * edge sets it (contract-changes 0042).
 */
import WebSocket from 'ws';
import { setTimeout as delay } from 'node:timers/promises';

let addresses = 0;
/** A fresh documentation address (RFC 5737) per step and run. */
export const nextAddress = () => `203.0.113.${(++addresses % 250) + 1}`;

/** A socket with a client address. Resolves to the socket, or `{ refused: { status, body } }`. */
export function open(wsUrl, address, { public: publicTransport = false } = {}) {
  const url = publicTransport ? `${wsUrl}?public=1` : wsUrl;
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers: { 'cf-connecting-ip': address } });
    const socket = { ws, inbox: [], waiters: [], close: undefined };
    const wake = () => { for (const w of socket.waiters.splice(0)) w(); };
    ws.on('message', (data) => { try { socket.inbox.push(JSON.parse(String(data))); } catch {} wake(); });
    ws.on('close', (code, reason) => { socket.close = { code, reason: String(reason) }; wake(); });
    ws.on('unexpected-response', (_req, res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => resolve({ refused: { status: res.statusCode, body } }));
    });
    ws.on('error', (error) => { if (ws.readyState !== WebSocket.OPEN) reject(error); });
    ws.on('open', () => resolve(socket));
  });
}

export async function next(socket, match, label, timeoutMs = 10_000) {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const i = socket.inbox.findIndex(match);
    if (i >= 0) return socket.inbox.splice(i, 1)[0];
    if (socket.close) throw new Error(`closed (${socket.close.code}) while waiting for ${label}`);
    if (Date.now() > end) throw new Error(`timed out waiting for ${label}`);
    await Promise.race([new Promise(r => socket.waiters.push(r)), delay(50)]);
  }
}

export const send = (socket, frame) => socket.ws.send(JSON.stringify(frame));
export const shut = (socket) => { try { socket.ws.close(); } catch {} };
/** What is compared of a reply: no backend-assigned ids. */
export const shape = (m) => {
  const out = { type: m.type };
  for (const k of ['id', 'path', 'reason', 'error', 'code', 'class', 'retryable', 'committedRevision']) if (m[k] !== undefined) out[k] = m[k];
  if (m.type === 'auth.result') out.result = { accountId: m.result.accountId, public: m.result.public };
  else if (m.result !== undefined) out.result = m.result;
  return out;
};

export default {
  name: 'public-limits',
  description: 'Public transport (K16): auth and scope, 30 calls per connection, 120 per address, 20 public and 100 pending sockets per address (429), slot release',
  tenants: { a: ['alice'] },
  async run(ctx) {
    const { wsUrl } = ctx.backend;
    const project = ctx.project, tenant = ctx.tenants.a;
    const ready = async (socket) => { await next(socket, m => m.type === 'session.ready', 'session.ready'); return socket; };
    const opened = async (address, options) => {
      const s = await open(wsUrl, address, options);
      if (s.refused) throw new Error(`upgrade refused: ${s.refused.status} ${s.refused.body}`);
      return ready(s);
    };
    const auth = async (socket, id, tenantRef = tenant) => {
      send(socket, { type: 'auth', id, project, tenant: tenantRef, public: true, clientContract: 1 });
      return next(socket, m => m.id === id, `${id} reply`);
    };

    await ctx.step('public auth and scope', async () => {
      const s = await opened(nextAddress(), { public: true });
      ctx.observe('first', shape(await auth(s, 'p1')));
      ctx.observe('again', shape(await auth(s, 'p2')));
      ctx.observe('unknownTenant', shape(await auth(s, 'p3', 'no-such-tenant')));
      ctx.observe('open', s.close === undefined);
      shut(s);
    });

    await ctx.step('30 calls per connection', async () => {
      const s = await opened(nextAddress(), { public: true });
      const replies = [];
      for (let i = 0; i < 30; i++) replies.push((await auth(s, `c${i}`)).type);
      ctx.observe('first30', replies.every(t => t === 'auth.result') ? 'all auth.result' : replies);
      ctx.observe('auth31', shape(await auth(s, 'c30')));
      const refused = [
        { type: 'query.call', id: 'q', path: 'host.querySurface', args: {} },
        { type: 'query.subscribe', id: 's', path: 'notes.list', args: {} },
        { type: 'reducer.call', id: 'r', path: 'board.create', args: {} },
        { type: 'reducer.callMany', calls: [{ id: 'm1', path: 'board.create', args: {} }, { id: 'm2', path: 'board.create', args: {} }] },
        { type: 'replica.open', id: 'o', path: 'notes.byBoard', args: {}, keys: [], hashes: {}, fullIntegrity: false },
        { type: 'action.call', id: 'a', path: 'errors.action', args: {} },
        { type: 'query.unsubscribe', id: 'u' },
      ];
      for (const frame of refused) send(s, frame);
      const ids = ['q', 's', 'r', 'm1', 'm2', 'o', 'a', 'public-rate-limit'];
      const out = {};
      for (const id of ids) out[id] = shape(await next(s, m => m.id === id, id));
      ctx.observe('refused', out);
      ctx.observe('open', s.close === undefined);
      shut(s);
    });

    await ctx.step('120 calls per address across connections', async () => {
      const address = nextAddress();
      const sockets = [];
      for (let n = 0; n < 4; n++) {
        const s = await opened(address, { public: true });
        sockets.push(s);
        let ok = 0;
        for (let i = 0; i < 30; i++) if ((await auth(s, `s${n}-${i}`)).type === 'auth.result') ok++;
        ctx.observe(`socket${n}`, ok);
      }
      const fifth = await opened(address, { public: true });
      sockets.push(fifth);
      ctx.observe('call121', shape(await auth(fifth, 'over')));
      for (const s of sockets) shut(s);
    });

    await ctx.step('20 public sockets per address', async () => {
      const address = nextAddress();
      const sockets = [];
      for (let i = 0; i < 20; i++) sockets.push(await opened(address, { public: true }));
      const refused = await open(wsUrl, address, { public: true });
      ctx.observe('socket21', refused.refused ?? 'admitted');
      // Another address is unaffected.
      const other = await opened(nextAddress(), { public: true });
      shut(other);
      shut(sockets.pop());
      let admitted;
      for (let i = 0; i < 100 && !admitted; i++) {
        const s = await open(wsUrl, address, { public: true });
        if (s.refused) await delay(50);
        else admitted = s;
      }
      ctx.observe('afterClose', admitted ? 'admitted' : 'refused');
      if (admitted) sockets.push(admitted);
      for (const s of sockets) shut(s);
    });

    await ctx.step('100 pending sockets per address, both transports', async () => {
      const address = nextAddress();
      const anonymous = await opened(address, { public: true });
      const sockets = [anonymous];
      for (let i = 0; i < 99; i++) sockets.push(await opened(address));
      const refused = await open(wsUrl, address);
      ctx.observe('socket101', refused.refused ?? 'admitted');
      const refusedPublic = await open(wsUrl, address, { public: true });
      ctx.observe('socket101public', refusedPublic.refused ?? 'admitted');
      // A successful auth frees the pending slot; the socket stays open.
      ctx.observe('auth', (await auth(anonymous, 'pending-auth')).type);
      let admitted;
      for (let i = 0; i < 100 && !admitted; i++) {
        const s = await open(wsUrl, address);
        if (s.refused) await delay(50);
        else admitted = s;
      }
      ctx.observe('afterAuth', admitted ? 'admitted' : 'refused');
      if (admitted) sockets.push(admitted);
      for (const s of sockets) shut(s);
    });
  },
};
