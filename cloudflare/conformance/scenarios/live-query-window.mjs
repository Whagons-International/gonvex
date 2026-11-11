/**
 * A structured Live Query window (board.page: sort by position, offset and
 * limit). Watched by the real SDK (watchLiveQuery) and a raw subscription:
 * initial result, a row entering at the top and pushing one out, an update
 * outside the window, a delete inside it, moving the window and unsubscribing.
 */
import { randomUUID } from 'node:crypto';
import { rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const ids = (rows) => (Array.isArray(rows) ? rows.map(r => `${r.id}:${r.position}:${r.title}`) : rows);

export default {
  name: 'live-query-window',
  description: 'Live Query window: initial, enter/leave, outside update, delete, window move, unsubscribe',
  tenants: { a: ['alice', 'bob'] },
  async run(ctx) {
    const { api } = ctx;
    let writer, sdk, live, raw, subId;
    const sdkRows = () => { try { return live.localLiveQueryResult(); } catch (error) { return { error: error.message }; } };
    const expectSdk = async (expected, label) => {
      await ctx.until(() => JSON.stringify(ids(sdkRows())) === JSON.stringify(expected), label);
      ctx.observe('sdk', ids(sdkRows()));
    };
    // Every query frame the raw subscriber received in this step, in order.
    const rawUpdates = async () => { await ctx.quiesce(); const out = []; for (;;) { const m = raw.inbox.findIndex(x => x.id === subId && x.type?.startsWith('query.')); if (m < 0) break; out.push(raw.inbox.splice(m, 1)[0].type); } return out; };
    await ctx.step('seed five rows', async () => {
      writer = await ctx.raw('writer');
      const s = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      for (let p = 1; p <= 5; p++) await rawReducer(writer, 'board.add', { id: `p${p}`, title: `row ${p}`, position: p * 10 });
    });
    await ctx.step('subscribe to the first window', async () => {
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      live = sdk.watchLiveQuery(api.board.page, { offset: 0, limit: 3 });
      live.onUpdate(() => {});
      raw = await ctx.raw('raw');
      const s = await ctx.rawLogin(raw, 'bob');
      await raw.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: { ...RAW_CAPABILITIES, queryPagePatch: 1 } });
      subId = randomUUID();
      raw.send({ type: 'query.subscribe', id: subId, path: 'board.page', args: { offset: 0, limit: 3 } });
      const first = await raw.next(m => m.id === subId && (m.type === 'query.result' || m.type === 'query.error'), { label: 'raw initial result' });
      ctx.observe('raw', { type: first.type, rows: ids(first.result), error: first.error });
      await expectSdk(['p1:10:row 1', 'p2:20:row 2', 'p3:30:row 3'], 'initial window');
    });
    await ctx.step('a row enters at the top', async () => {
      await rawReducer(writer, 'board.add', { id: 'p0', title: 'row 0', position: 5 });
      await expectSdk(['p0:5:row 0', 'p1:10:row 1', 'p2:20:row 2'], 'p0 entered, p3 left');
      ctx.observe('rawFrames', await rawUpdates());
    });
    await ctx.step('an update outside the window', async () => {
      await rawReducer(writer, 'board.rename', { id: 'p5', title: 'row 5 renamed' });
      await ctx.quiesce();
      ctx.observe('sdk', ids(sdkRows()));
      ctx.observe('rawFrames', await rawUpdates());
    });
    await ctx.step('an update inside the window', async () => {
      await rawReducer(writer, 'board.rename', { id: 'p1', title: 'row 1 renamed' });
      await expectSdk(['p0:5:row 0', 'p1:10:row 1 renamed', 'p2:20:row 2'], 'p1 renamed');
      ctx.observe('rawFrames', await rawUpdates());
    });
    await ctx.step('a delete inside the window', async () => {
      await rawReducer(writer, 'board.remove', { id: 'p0' });
      await expectSdk(['p1:10:row 1 renamed', 'p2:20:row 2', 'p3:30:row 3'], 'p0 deleted, p3 back');
      ctx.observe('rawFrames', await rawUpdates());
    });
    await ctx.step('move the window', async () => {
      raw.send({ type: 'query.unsubscribe', id: subId });
      subId = randomUUID();
      raw.send({ type: 'query.subscribe', id: subId, path: 'board.page', args: { offset: 2, limit: 2 } });
      const moved = await raw.next(m => m.id === subId && (m.type === 'query.result' || m.type === 'query.error'), { label: 'moved window' });
      ctx.observe('raw', { type: moved.type, rows: ids(moved.result), error: moved.error });
      const second = sdk.watchLiveQuery(api.board.page, { offset: 2, limit: 2 });
      second.onUpdate(() => {});
      await ctx.until(() => JSON.stringify(ids(second.localLiveQueryResult())) === JSON.stringify(['p3:30:row 3', 'p4:40:row 4']), 'second window');
      ctx.observe('sdk', ids(second.localLiveQueryResult()));
    });
    await ctx.step('limits and unsubscribe', async () => {
      const over = randomUUID();
      raw.send({ type: 'query.subscribe', id: over, path: 'board.page', args: { offset: 0, limit: 500 } });
      const r = await raw.next(m => m.id === over && (m.type === 'query.result' || m.type === 'query.error'), { label: 'over-limit window' });
      ctx.observe('overLimit', { type: r.type, rows: ids(r.result), error: r.error });
      raw.send({ type: 'query.unsubscribe', id: over });
      raw.send({ type: 'query.unsubscribe', id: subId });
      await rawReducer(writer, 'board.rename', { id: 'p3', title: 'after unsubscribe' });
      ctx.observe('afterUnsubscribe', await rawUpdates());
    });
  },
};
