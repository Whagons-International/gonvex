/**
 * The tenant database gets a new replica epoch under live subscriptions (a
 * restore, or an import without `--keep-epoch`; 0008), at frame level and
 * through the SDK. The feed keeps going: the next transaction carries the new
 * epoch while collection deltas keep their subscription's cursor epoch, and
 * every client converges. A resume with the old epoch's cursor is no longer
 * resumable and gets a fresh snapshot; an SDK reconnecting across the change
 * does too. Backends without `epochReset` skip it.
 */
import { watch, call, rowsWhen, project, integrity, rawOpen, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

const COLUMNS = ['id', 'title', 'position', 'bumps'];
const NEW_EPOCH = 'conformance-replica-reset-epoch-0001';

export default {
  name: 'replica-reset',
  description: 'A new database epoch under live subscriptions: the feed continues, old-epoch resumes reload, the SDK converges',
  tenants: { a: ['alice', 'bob'] },
  requires: ['epochReset'],
  async run(ctx) {
    const { api } = ctx;
    let writer, reader, opened, sdk, view;
    await ctx.step('a raw reader and the SDK watch the board', async () => {
      writer = await ctx.raw('writer');
      const s = await ctx.rawLogin(writer, 'alice');
      await writer.auth({ project: ctx.project, tenant: ctx.tenants.a, token: s.accessToken, capabilities: RAW_CAPABILITIES });
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 'e1', title: 'before', position: 1 }));
      reader = await ctx.raw('reader');
      const r = await ctx.rawLogin(reader, 'bob');
      await reader.auth({ project: ctx.project, tenant: ctx.tenants.a, token: r.accessToken, capabilities: RAW_CAPABILITIES });
      opened = await rawOpen(reader, { path: 'board.all' });
      ctx.observe('open', { rows: project(opened.rows ?? [], COLUMNS), types: opened.types });
      sdk = await ctx.sdk('sdk', { member: 'bob' });
      view = watch(sdk, api.board.all);
      ctx.observe('sdk', project(await rowsWhen(ctx, view, (rows) => rows.length === 1, 'sdk snapshot'), COLUMNS));
    });
    await ctx.step('the epoch changes, then a commit', async () => {
      await ctx.backend.resetEpoch(ctx.project, ctx.tenants.a, NEW_EPOCH);
      ctx.observe('add', await rawReducer(writer, 'board.add', { id: 'e2', title: 'after', position: 2 }));
      const delta = await reader.next((m) => m.type === 'replica.delta' && m.id === opened.id, { label: 'delta after the epoch change' });
      const ready = await reader.next((m) => m.type === 'replica.ready' && m.id === opened.id, { label: 'ready after the epoch change' });
      ctx.expect(delta.cursor.epoch === opened.ready.cursor.epoch && ready.cursor.epoch === opened.ready.cursor.epoch, 'deltas keep the subscription cursor epoch');
      opened = { ...opened, rows: [...opened.rows, ...delta.upserts].sort((a, b) => a.id.localeCompare(b.id)), ready };
      ctx.observe('sdk', project(await rowsWhen(ctx, view, (rows) => rows.length === 2, 'sdk sees the commit'), COLUMNS));
    });
    await ctx.step('a resume with the old epoch reloads', async () => {
      // The same id replaces the subscription: a socket holding collections of two epochs is closed (lib.rs 1859-1866).
      const again = await rawOpen(reader, { id: opened.id, path: 'board.all', cursor: opened.ready.cursor, ...integrity(opened.rows, 'id', COLUMNS), fullIntegrity: true }, opened.rows);
      ctx.observe('resume', { types: again.types, rows: project(again.rows ?? [], COLUMNS) });
      ctx.expect(again.ready?.cursor?.epoch !== opened.ready.cursor.epoch, 'the reload is in the new epoch');
    });
    await ctx.step('the SDK reconnects across the change', async () => {
      sdk.socket.close(4001, 'conformance network drop');
      ctx.observe('rename', await rawReducer(writer, 'board.rename', { id: 'e1', title: 'renamed after the epoch change' }));
      ctx.observe('sdk', project(await rowsWhen(ctx, view, (rows) => rows.some((r) => r.title === 'renamed after the epoch change'), 'sdk resumed'), COLUMNS));
      ctx.observe('call', await call(sdk, api.board.bump, { id: 'e2' }));
      ctx.observe('sdk', project(await rowsWhen(ctx, view, (rows) => rows.some((r) => r.id === 'e2' && r.bumps === 1), 'sdk sees its own call'), COLUMNS));
    });
  },
};
