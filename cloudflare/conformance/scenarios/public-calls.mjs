/**
 * Public functions (K16) on backends that run module calls (`publicCalls`):
 * an anonymous session runs functions declared `public: true` and is refused
 * the rest with Rust's text.
 *
 *  - passing: the public Queries `actions.boardCount` and `host.querySurface`
 *    (the ctx surface an anonymous Query sees), and the public Action
 *    `actions.publicEcho` (no account in its context);
 *  - refused: a non-public Reducer, alone and in a batch with an unknown
 *    path, and a non-public Action.
 *
 * Helpers and addressing as in `public-limits`.
 */
import { nextAddress, next, open, send, shape, shut } from './public-limits.mjs';

export default {
  name: 'public-calls',
  description: 'Public functions (K16): an anonymous session runs public Queries and Actions and is refused non-public functions',
  tenants: { a: ['alice'] },
  requires: ['publicCalls'],
  async run(ctx) {
    const { wsUrl } = ctx.backend;
    let s;
    await ctx.step('anonymous auth', async () => {
      s = await open(wsUrl, nextAddress(), { public: true });
      if (s.refused) throw new Error(`upgrade refused: ${s.refused.status}`);
      await next(s, m => m.type === 'session.ready', 'session.ready');
      send(s, { type: 'auth', id: 'anon', project: ctx.project, tenant: ctx.tenants.a, public: true, clientContract: 1 });
      ctx.observe('auth', (await next(s, m => m.id === 'anon', 'anon')).type);
    });
    await ctx.step('public functions pass', async () => {
      send(s, { type: 'query.call', id: 'count', path: 'actions.boardCount', args: {} });
      ctx.observe('boardCount', shape(await next(s, m => m.id === 'count', 'count')));
      send(s, { type: 'query.call', id: 'surface', path: 'host.querySurface', args: {} });
      ctx.observe('querySurface', shape(await next(s, m => m.id === 'surface', 'surface')));
      send(s, { type: 'action.call', id: 'echo', path: 'actions.publicEcho', args: { text: 'hi' } });
      ctx.observe('publicEcho', shape(await next(s, m => m.id === 'echo', 'echo')));
    });
    await ctx.step('non-public functions are refused', async () => {
      send(s, { type: 'reducer.call', id: 'np', path: 'board.add', args: { id: 'x', title: 'x' } });
      ctx.observe('nonPublicReducer', shape(await next(s, m => m.id === 'np', 'np')));
      send(s, { type: 'reducer.callMany', calls: [{ id: 'b1', path: 'board.add', args: {} }, { id: 'b2', path: 'no.such', args: {} }] });
      ctx.observe('batch', [shape(await next(s, m => m.id === 'b1', 'b1')), shape(await next(s, m => m.id === 'b2', 'b2'))]);
      send(s, { type: 'action.call', id: 'na', path: 'actions.fails', args: {} });
      ctx.observe('nonPublicAction', shape(await next(s, m => m.id === 'na', 'na')));
      ctx.observe('open', s.close === undefined);
      shut(s);
    });
  },
};
