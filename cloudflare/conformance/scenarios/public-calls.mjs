/**
 * Public functions (K16) on backends that run Reducers (`publicCalls`): an
 * anonymous session reaches only functions declared `public: true`. A
 * non-public Reducer is refused with Rust's text, alone and in a batch, and
 * the idempotency key of an anonymous call is ignored (no receipt).
 * Helpers and addressing as in `public-limits`.
 */
import { nextAddress, next, open, send, shape, shut } from './public-limits.mjs';

export default {
  name: 'public-calls',
  description: 'Public functions (K16): an anonymous session is refused non-public Reducers, alone and in a batch',
  tenants: { a: ['alice'] },
  requires: ['publicCalls'],
  async run(ctx) {
    const { wsUrl } = ctx.backend;
    await ctx.step('anonymous sessions call only public functions', async () => {
      const s = await open(wsUrl, nextAddress(), { public: true });
      if (s.refused) throw new Error(`upgrade refused: ${s.refused.status}`);
      await next(s, m => m.type === 'session.ready', 'session.ready');
      send(s, { type: 'auth', id: 'anon', project: ctx.project, tenant: ctx.tenants.a, public: true, clientContract: 1 });
      ctx.observe('auth', (await next(s, m => m.id === 'anon', 'anon')).type);
      send(s, { type: 'reducer.call', id: 'np', path: 'board.add', args: { id: 'x', title: 'x' }, idempotencyKey: 'k' });
      ctx.observe('nonPublicReducer', shape(await next(s, m => m.id === 'np', 'np')));
      send(s, { type: 'reducer.callMany', calls: [{ id: 'b1', path: 'board.add', args: {} }, { id: 'b2', path: 'no.such', args: {} }] });
      ctx.observe('batch', [shape(await next(s, m => m.id === 'b1', 'b1')), shape(await next(s, m => m.id === 'b2', 'b2'))]);
      ctx.observe('open', s.close === undefined);
      shut(s);
    });
  },
};
